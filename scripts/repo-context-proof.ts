#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { gitContextCacheSizeForProof } from "../packages/collector-cli/src/git-context";
import { runRepoEnrichmentMaintenance } from "../packages/collector-cli/src/maintenance";
import { resolveMaintenanceRepoContexts } from "../packages/collector-cli/src/maintenance-worker";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import {
  attachRepoContextId,
  attachRepoContextSidecar,
  peekRepoContextSidecar,
  REPO_CONTEXT_RESOLVER_VERSION,
  resolveRepoContextRequests,
} from "../packages/collector-cli/src/repo-context";
import {
  aiInteractionEventSchema,
  remoteLinkageHash,
  type AiInteractionEvent,
} from "../packages/shared/src/index";

const checks: string[] = [];
const MAX_APPEND_MANY_EVENTS = 2_048;
const MAX_APPEND_MANY_MS = 15_000;
const RESULT_RETENTION_LIMIT = 4_096;
const RESULT_PLATEAU_PAGE_BUDGET = 16;
const LEGACY_UPGRADE_ROWS = 65_536;

function check(name: string, condition: unknown) {
  assert.ok(condition, name);
  checks.push(name);
}

function write(file: string, contents: string) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, contents, { mode: 0o600 });
}

function repo(root: string, name: string, remote: string) {
  const cwd = path.join(root, name);
  write(path.join(cwd, ".git", "HEAD"), "ref: refs/heads/main\n");
  write(path.join(cwd, ".git", "refs", "heads", "main"), "a".repeat(40) + "\n");
  write(path.join(cwd, ".git", "config"), `[remote "origin"]\n\turl = ${remote}\n`);
  return cwd;
}

function event(index: number, sessionId = "repo-context-session"): AiInteractionEvent {
  return aiInteractionEventSchema.parse({
    id: deterministicEventId(["repo-context-proof", String(index)]),
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: new Date(Date.UTC(2026, 6, 20, 10, 0, index % 60)).toISOString(),
    sessionId,
    inputTokens: index + 1,
    outputTokens: 1,
    actionClass: "other",
  });
}

function contextId(buffer: LocalEventBuffer, eventId: string) {
  const row = buffer.database
    .prepare(`select context_id as contextId from repo_context_event_links where event_id = ?`)
    .get(eventId) as { contextId: string | null } | undefined;
  assert.match(row?.contextId ?? "", /^repoctx:v1:[0-9a-f]{64}$/);
  return row!.contextId!;
}

function artifactContains(file: string, sentinel: string) {
  return fs.existsSync(file) && fs.readFileSync(file).includes(Buffer.from(sentinel));
}

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-repo-context-proof-"));
try {
  const privateCwdSentinel = "REPO_CONTEXT_PRIVATE_CWD_SENTINEL";
  const cwdA = repo(root, privateCwdSentinel, "https://example.invalid/team/repo-a.git");
  const cwdB = repo(root, "repo-b", "https://example.invalid/team/repo-b.git");
  const cfg = collectorConfigSchema.parse({});
  const ledger = path.join(root, "ledger.sqlite");
  const bufferA = new LocalEventBuffer(ledger, { delivery: { enabled: true } });
  const bufferB = new LocalEventBuffer(ledger, { delivery: { enabled: true } });
  const eagerKey = bufferA.database
    .prepare(`select count(*) as count, length(max(hmac_key)) as bytes from repo_context_identity_key`)
    .get() as { count: number; bytes: number };
  check(
    "hmac_winner_exists_before_first_admission_and_second_connection",
    eagerKey.count === 1 && eagerKey.bytes === 32,
  );

  const sharedOccurrence = "hook-or-otlp-event-id";
  const first = event(1);
  const second = event(2);
  assert.equal(attachRepoContextSidecar(first, sharedOccurrence, cwdA), true);
  assert.equal(
    attachRepoContextSidecar(second, sharedOccurrence, `${cwdA}${path.sep}.${path.sep}`),
    true,
  );
  assert.equal(bufferA.append(first), true);
  assert.equal(bufferB.append(second), true);
  const firstContextId = contextId(bufferA, first.id);
  const secondContextId = contextId(bufferA, second.id);
  check("same_ledger_winner_key_derives_same_occurrence_context", firstContextId === secondContextId);
  const keyCount = bufferA.database
    .prepare(`select count(*) as count, length(max(hmac_key)) as bytes from repo_context_identity_key`)
    .get() as { count: number; bytes: number };
  check("per_ledger_hmac_key_has_one_32_byte_winner", keyCount.count === 1 && keyCount.bytes === 32);

  const otherLedger = path.join(root, "other-ledger.sqlite");
  const other = new LocalEventBuffer(otherLedger);
  const otherEvent = event(3);
  attachRepoContextSidecar(otherEvent, sharedOccurrence, cwdA);
  other.append(otherEvent);
  check(
    "different_ledgers_do_not_share_context_identifiers",
    contextId(other, otherEvent.id) !== firstContextId,
  );
  other.close();

  // Full EXPLAIN is the structural migration gate: first-install index builds
  // may walk only the new, empty sidecar root page, never buffered_events.
  const explainDatabase = new Database(":memory:");
  explainDatabase.exec(`
    create table buffered_events (id text primary key, payload_json text not null);
    insert into buffered_events values ('legacy-proof-row', '{}');
    create table repo_context_event_links (
      event_id text primary key,
      context_id text not null,
      fill_pending integer not null,
      context_conflict integer not null,
      suppression_cleaned integer not null
    ) without rowid;
  `);
  const explainRoots = explainDatabase.prepare(
    `select name, rootpage from sqlite_schema
     where type = 'table' and name in ('buffered_events', 'repo_context_event_links')`,
  ).all() as Array<{ name: string; rootpage: number }>;
  const bufferedRoot = explainRoots.find((row) => row.name === "buffered_events")!.rootpage;
  const linkRoot = explainRoots.find((row) => row.name === "repo_context_event_links")!.rootpage;
  const indexPrograms = [
    `create index proof_repo_context_pending_context
       on repo_context_event_links (context_id, event_id) where fill_pending = 1`,
    `create index proof_repo_context_cleanup
       on repo_context_event_links (context_id, event_id) where suppression_cleaned = 0`,
  ].map((sql) => explainDatabase.prepare(`explain ${sql}`).all() as Array<{
    opcode: string;
    p2: number;
  }>);
  const indexOpenReadRoots = indexPrograms.flatMap((program) =>
    program.filter((operation) => operation.opcode === "OpenRead").map((operation) => operation.p2)
  );
  check(
    "first_install_index_bytecode_reads_only_the_empty_new_sidecar",
    indexOpenReadRoots.length === 2 &&
      indexOpenReadRoots.every((rootPage) => rootPage === linkRoot) &&
      !indexOpenReadRoots.includes(bufferedRoot),
  );
  explainDatabase.close();

  // Scale the pre-#151 shape enough that an accidental legacy index build or
  // constrained ALTER is material, then prove constructor and reopen leave the
  // event table and its existing indexes byte-for-byte unchanged.
  const legacyLedger = path.join(root, "legacy-upgrade.sqlite");
  new LocalEventBuffer(legacyLedger).close();
  const legacyDatabase = new Database(legacyLedger);
  legacyDatabase.exec(`
    drop trigger if exists trg_repo_context_event_link_identity_immutable;
    drop trigger if exists trg_repo_context_event_link_state_monotonic;
    drop trigger if exists trg_events_repo_context_link_delete;
    drop trigger if exists trg_repo_context_result_immutable_update;
    drop trigger if exists trg_repo_context_result_delete_guard;
    drop table if exists repo_context_event_links;
    drop table if exists repo_context_handoffs;
    drop table if exists repo_context_inflight;
    drop table if exists repo_context_unknown_counters;
    drop table if exists repo_context_suppressions;
    drop table if exists repo_context_conflicts;
    drop table if exists repo_context_results;
    drop table if exists repo_context_identity_key;
    with recursive legacy_rows(ordinal) as (
      values(1)
      union all select ordinal + 1 from legacy_rows where ordinal < ${LEGACY_UPGRADE_ROWS}
    )
    insert into buffered_events
      (id, source, event_type, data_mode, observed_at, payload_json,
       suppressed_fields_json, created_at, privacy_generation)
    select
      printf('legacy-%06d', ordinal), 'codex', 'assistant_response', 'metadata',
      '2026-01-01T00:00:00.000Z',
      printf('{"legacy":"%0480d"}', ordinal), '[]',
      '2026-01-01T00:00:00.000Z', lower(hex(randomblob(16)))
    from legacy_rows;
    create trigger proof_legacy_upgrade_no_insert
    before insert on buffered_events begin
      select raise(abort, 'legacy_upgrade_inserted_event');
    end;
    create trigger proof_legacy_upgrade_no_update
    before update on buffered_events begin
      select raise(abort, 'legacy_upgrade_updated_event');
    end;
    create trigger proof_legacy_upgrade_no_delete
    before delete on buffered_events begin
      select raise(abort, 'legacy_upgrade_deleted_event');
    end;
  `);
  legacyDatabase.pragma("wal_checkpoint(TRUNCATE)");
  const legacyColumnsBefore = legacyDatabase.pragma("table_info(buffered_events)");
  const legacyTableBefore = legacyDatabase.prepare(
    `select sql, rootpage from sqlite_schema where type = 'table' and name = 'buffered_events'`,
  ).get();
  const legacyIndexesBefore = legacyDatabase.prepare(
    `select name, sql, rootpage from sqlite_schema
     where type = 'index' and tbl_name = 'buffered_events' order by name`,
  ).all();
  const legacyFactsBefore = legacyDatabase.prepare(
    `select count(*) as rows, sum(length(payload_json)) as payloadBytes,
       min(id) as firstId, max(id) as lastId from buffered_events`,
  ).get();
  const legacyPagesBefore = Number(legacyDatabase.pragma("page_count", { simple: true }));
  legacyDatabase.close();

  const upgradeCpuStarted = process.cpuUsage();
  const upgradeWallStarted = performance.now();
  const upgradedLegacy = new LocalEventBuffer(legacyLedger);
  const legacyUpgradeWallMs = performance.now() - upgradeWallStarted;
  const legacyUpgradeCpu = process.cpuUsage(upgradeCpuStarted);
  const legacyColumnsAfter = upgradedLegacy.database.pragma("table_info(buffered_events)");
  const legacyTableAfter = upgradedLegacy.database.prepare(
    `select sql, rootpage from sqlite_schema where type = 'table' and name = 'buffered_events'`,
  ).get();
  const legacyIndexesAfter = upgradedLegacy.database.prepare(
    `select name, sql, rootpage from sqlite_schema
     where type = 'index' and tbl_name = 'buffered_events' order by name`,
  ).all();
  const legacyFactsAfter = upgradedLegacy.database.prepare(
    `select count(*) as rows, sum(length(payload_json)) as payloadBytes,
       min(id) as firstId, max(id) as lastId from buffered_events`,
  ).get();
  const legacyLinksAfter = Number((upgradedLegacy.database.prepare(
    `select count(*) as count from repo_context_event_links`,
  ).get() as { count: number }).count);
  const linkIndexesBeforeReopen = upgradedLegacy.database.prepare(
    `select name, rootpage from sqlite_schema
     where type = 'index' and tbl_name = 'repo_context_event_links' order by name`,
  ).all();
  const reopenedLegacy = new LocalEventBuffer(legacyLedger);
  const linkIndexesAfterReopen = reopenedLegacy.database.prepare(
    `select name, rootpage from sqlite_schema
     where type = 'index' and tbl_name = 'repo_context_event_links' order by name`,
  ).all();
  reopenedLegacy.close();
  upgradedLegacy.database.pragma("wal_checkpoint(TRUNCATE)");
  const legacyPagesAfter = Number(upgradedLegacy.database.pragma("page_count", { simple: true }));
  check(
    "scaled_legacy_first_upgrade_never_mutates_or_indexes_raw_history",
    JSON.stringify(legacyColumnsAfter) === JSON.stringify(legacyColumnsBefore) &&
      JSON.stringify(legacyTableAfter) === JSON.stringify(legacyTableBefore) &&
      JSON.stringify(legacyIndexesAfter) === JSON.stringify(legacyIndexesBefore) &&
      JSON.stringify(legacyFactsAfter) === JSON.stringify(legacyFactsBefore) &&
      legacyLinksAfter === 0 &&
      legacyPagesAfter <= legacyPagesBefore + 64,
  );
  check(
    "sidecar_indexes_are_created_once_and_reopen_does_not_rebuild_them",
    linkIndexesBeforeReopen.length === 2 &&
      JSON.stringify(linkIndexesAfterReopen) === JSON.stringify(linkIndexesBeforeReopen),
  );
  upgradedLegacy.database.exec(`
    drop trigger proof_legacy_upgrade_no_insert;
    drop trigger proof_legacy_upgrade_no_update;
    drop trigger proof_legacy_upgrade_no_delete;
  `);
  const postUpgradeEvent = event(55_000, "post-legacy-upgrade");
  attachRepoContextSidecar(postUpgradeEvent, "post-legacy-upgrade", cwdA);
  assert.equal(upgradedLegacy.append(postUpgradeEvent), true);
  const postUpgradeContextId = contextId(upgradedLegacy, postUpgradeEvent.id);
  const duplicatePostUpgradeEvent = event(55_000, "post-legacy-upgrade");
  attachRepoContextSidecar(duplicatePostUpgradeEvent, "hostile-duplicate", cwdB);
  assert.equal(upgradedLegacy.append(duplicatePostUpgradeEvent), false);
  const postUpgradeLinks = upgradedLegacy.database.prepare(
    `select context_id as contextId from repo_context_event_links where event_id = ?`,
  ).all(postUpgradeEvent.id) as Array<{ contextId: string }>;
  check(
    "new_admission_links_atomically_and_duplicate_cannot_replace_identity",
    postUpgradeLinks.length === 1 && postUpgradeLinks[0]!.contextId === postUpgradeContextId,
  );
  upgradedLegacy.database.prepare(`delete from buffered_events where id = ?`).run(postUpgradeEvent.id);
  check(
    "raw_retention_delete_removes_only_its_new_sidecar_link",
    !upgradedLegacy.database.prepare(
      `select 1 from repo_context_event_links where event_id = ?`,
    ).get(postUpgradeEvent.id),
  );
  upgradedLegacy.close();

  const cachedPathsBefore = gitContextCacheSizeForProof();
  for (let batch = 0; batch < 128; batch += 1) {
    const cacheHostileRequests = Array.from({ length: 8 }, (_, offset) => {
      const ordinal = batch * 8 + offset;
      return {
        contextId: `repoctx:v1:${ordinal.toString(16).padStart(64, "0")}`,
        source: "unknown" as const,
        cwd: path.join(root, `cache-disabled-miss-${ordinal}`),
      };
    });
    const cacheHostileResults = resolveRepoContextRequests(cacheHostileRequests);
    assert.equal(cacheHostileResults.length, 8);
    assert.ok(cacheHostileResults.every((result) => result.repoHash === null));
  }
  check(
    "deferred_unique_cwds_never_enter_raw_path_cache",
    gitContextCacheSizeForProof() === cachedPathsBefore,
  );

  const hookA = appendForwardedHook(
    { id: "hook-a", event_type: "UserPromptSubmit", session_id: "multi-repo", cwd: cwdA },
    { config: cfg, buffer: bufferA, source: "codex" },
  );
  const hookB = appendForwardedHook(
    { id: "hook-b", event_type: "UserPromptSubmit", session_id: "multi-repo", cwd: cwdB },
    { config: cfg, buffer: bufferA, source: "codex" },
  );
  const unknownRows = bufferA.database
    .prepare(
      `select id, repo_hash as repoHash, branch_hash as branchHash, head_sha as headSha
       from buffered_events where id in (?, ?) order by id`,
    )
    .all(hookA.event.id, hookB.event.id) as Array<Record<string, unknown>>;
  check(
    "hook_capture_appends_unknown_without_raw_cwd",
    unknownRows.length === 2 && unknownRows.every((row) =>
      row.repoHash === null && row.branchHash === null && row.headSha === null
    ),
  );

  const exploded = explodeOtlpPayload(
    {
      resourceLogs: [{
        resource: { attributes: [{ key: "service.name", value: { stringValue: "codex_exec" } }] },
        scopeLogs: [{ logRecords: [{
          timeUnixNano: "1784541600000000000",
          attributes: [
            { key: "cwd", value: { stringValue: cwdA } },
            { key: "gen_ai.usage.input_tokens", value: { intValue: "17" } },
            { key: "gen_ai.usage.output_tokens", value: { intValue: "3" } },
            { key: "session.id", value: { stringValue: "otlp-context-session" } },
          ],
        }] }],
      }],
    },
    { policy: cfg.policy, source: "codex", transportPath: "/v1/logs" },
  );
  bufferA.appendMany(exploded.events, exploded.metricSamples, exploded.admissionDrops);
  const otlpId = exploded.events[0]?.event.id;
  check(
    "otlp_capture_keeps_value_unknown_and_queues_sidecar",
    Boolean(otlpId) && contextId(bufferA, otlpId!) !== "" &&
      (bufferA.database.prepare(`select repo_hash as repoHash from buffered_events where id = ?`)
        .get(otlpId) as { repoHash: string | null }).repoHash === null,
  );

  const requests = bufferA.takeRepoContextBatch();
  const begunRequests = bufferA.beginRepoContextResolution(requests);
  const results = resolveRepoContextRequests(begunRequests);
  const applied = bufferA.applyRepoContextResults(results);
  check(
    "bounded_results_fill_exact_context_rows",
    applied.resultsInserted > 0 && applied.rowsFilled > 0 && applied.rowsVisited <= 128,
  );
  const isolated = bufferA.database
    .prepare(
      `select l.context_id as contextId, e.repo_hash as repoHash,
         e.branch_hash as branchHash, e.head_sha as headSha
       from buffered_events e join repo_context_event_links l on l.event_id = e.id
       where e.id in (?, ?) order by e.id`,
    )
    .all(hookA.event.id, hookB.event.id) as Array<{
      contextId: string;
      repoHash: string | null;
      branchHash: string | null;
      headSha: string | null;
    }>;
  check(
    "two_contexts_in_one_session_do_not_smear",
    isolated.length === 2 && isolated[0]!.contextId !== isolated[1]!.contextId &&
      isolated[0]!.repoHash !== isolated[1]!.repoHash &&
      isolated.every((row) => row.repoHash && row.branchHash && row.headSha === "a".repeat(40)),
  );

  const replayResult = results.find((result) => result.contextId === firstContextId)!;
  const replayRequest = requests.find((request) => request.contextId === firstContextId)!;
  assert.ok(replayResult);
  assert.ok(replayRequest);
  const changesBeforeReplay = bufferA.database.prepare(`select total_changes() as n`).get() as { n: number };
  const replayBegun = bufferA.beginRepoContextResolution([replayRequest]);
  const replay = bufferA.applyRepoContextResults([replayResult]);
  const changesAfterReplay = bufferA.database.prepare(`select total_changes() as n`).get() as { n: number };
  check(
    "retained_result_begin_and_identical_replay_write_zero",
    replayBegun.length === 0 && bufferA.repoContextInflightCount() === 0 &&
      replay.resultReplays === 1 && changesAfterReplay.n === changesBeforeReplay.n,
  );
  const conflictingHash = remoteLinkageHash("https://example.invalid/team/conflict.git")!;
  bufferA.database
    .prepare(`insert into repo_context_inflight (context_id, started_at) values (?, ?)`)
    .run(firstContextId, new Date().toISOString());
  const conflict = bufferA.applyRepoContextResults([{ ...replayResult, repoHash: conflictingHash }]);
  const immutable = bufferA.database
    .prepare(`select repo_hash as repoHash from repo_context_results where context_id = ?`)
    .get(firstContextId) as { repoHash: string | null };
  check(
    "conflicting_result_never_overwrites_and_increments_bounded_evidence",
    conflict.resultConflicts === 1 && immutable.repoHash === replayResult.repoHash &&
      bufferA.repoContextUnknownCounters().some((row) =>
        row.reason === "result_conflict" && row.droppedCount === 1
      ),
  );

  assert.equal(bufferA.repoContextQueueStatus().queued, 0);
  const afterResult = event(4);
  attachRepoContextSidecar(afterResult, sharedOccurrence, cwdA);
  bufferA.append(afterResult);
  const afterResultRow = bufferA.database
    .prepare(
      `select repo_hash as repoHash, branch_hash as branchHash, head_sha as headSha
       from buffered_events where id = ?`,
    )
    .get(afterResult.id) as {
      repoHash: string | null;
      branchHash: string | null;
      headSha: string | null;
    };
  const retainedOutboxBeforeSeal = bufferA.database.prepare(
    `select repo_hash as repoHash, branch_hash as branchHash,
       sealed_envelope_json as sealed, attempt_count as attempts
     from upload_outbox where delivery_id = ?`,
  ).get(afterResult.id) as {
    repoHash: string | null;
    branchHash: string | null;
    sealed: string | null;
    attempts: number;
  };
  const retainedLease = bufferA.delivery.lease({ leaseId: "retained-result-proof" });
  const retainedItem = retainedLease.items.find((item) => item.deliveryId === afterResult.id);
  const retainedEnvelope = retainedItem
    ? JSON.parse(retainedItem.envelopeJson) as {
        event: { projectKey?: string; metadata: { branchHash?: string } };
      }
    : null;
  check(
    "retained_result_before_event_fills_all_linkage_without_new_filesystem_work",
    afterResultRow.repoHash === replayResult.repoHash &&
      afterResultRow.branchHash === replayResult.branchHash &&
      afterResultRow.headSha === replayResult.headSha &&
      retainedOutboxBeforeSeal.repoHash === replayResult.repoHash &&
      retainedOutboxBeforeSeal.branchHash === replayResult.branchHash &&
      retainedOutboxBeforeSeal.sealed === null && retainedOutboxBeforeSeal.attempts === 0 &&
      retainedEnvelope?.event.projectKey === replayResult.repoHash &&
      retainedEnvelope.event.metadata.branchHash === replayResult.branchHash &&
      bufferA.repoContextQueueStatus().queued === 0,
  );

  const partialBranch = remoteLinkageHash(
    "https://example.invalid/team/existing-non-null-branch.git",
  )!;
  const partial = aiInteractionEventSchema.parse({
    ...event(5, "partial-linkage"),
    metadata: {
      git: {
        remoteUrlHash: replayResult.repoHash,
        branchHash: partialBranch,
      },
    },
  });
  attachRepoContextSidecar(partial, "partial-linkage-occurrence", cwdA);
  bufferA.append(partial);
  const partialRequest = bufferA.takeRepoContextBatch()[0]!;
  const partialResult = resolveRepoContextRequests(
    bufferA.beginRepoContextResolution([partialRequest]),
  )[0]!;
  const partialApply = bufferA.applyRepoContextResults([partialResult]);
  const partialRow = bufferA.database.prepare(
    `select repo_hash as repoHash, branch_hash as branchHash, head_sha as headSha
     from buffered_events where id = ?`,
  ).get(partial.id) as {
    repoHash: string | null;
    branchHash: string | null;
    headSha: string | null;
  };
  check(
    "partial_linkage_fills_each_missing_field_without_overwriting_non_null_values",
    partialApply.rowsFilled === 1 &&
      partialRow.repoHash === replayResult.repoHash &&
      partialRow.branchHash === partialBranch &&
      partialRow.headSha === partialResult.headSha,
  );

  const alreadyComplete = aiInteractionEventSchema.parse({
    ...event(5_001, "already-complete-linkage"),
    metadata: {
      git: {
        remoteUrlHash: replayResult.repoHash,
        branchHash: replayResult.branchHash,
        headSha: replayResult.headSha,
      },
    },
  });
  attachRepoContextSidecar(alreadyComplete, "already-complete-linkage", cwdA);
  bufferA.append(alreadyComplete);
  const completeRequest = bufferA.takeRepoContextBatch()[0]!;
  const completeResult = resolveRepoContextRequests(
    bufferA.beginRepoContextResolution([completeRequest]),
  )[0]!;
  const completeApply = bufferA.applyRepoContextResults([completeResult]);
  const completeLink = bufferA.database.prepare(
    `select fill_pending as fillPending, context_conflict as contextConflict
     from repo_context_event_links where event_id = ?`,
  ).get(alreadyComplete.id) as { fillPending: number; contextConflict: number };
  check(
    "matching_complete_tuple_is_validated_once_and_leaves_pending_index",
    completeApply.rowsVisited === 1 && completeApply.rowsFilled === 0 &&
      completeLink.fillPending === 0 && completeLink.contextConflict === 0,
  );

  const mismatch = aiInteractionEventSchema.parse({
    ...event(6, "repo-context-row-mismatch"),
    metadata: { git: { remoteUrlHash: replayResult.repoHash } },
  });
  attachRepoContextSidecar(mismatch, "repo-context-row-mismatch", cwdB);
  bufferA.append(mismatch);
  const mismatchRequest = bufferA.takeRepoContextBatch()[0]!;
  const mismatchResult = resolveRepoContextRequests(
    bufferA.beginRepoContextResolution([mismatchRequest]),
  )[0]!;
  assert.notEqual(mismatchResult.repoHash, replayResult.repoHash);
  const mismatchRawBefore = bufferA.database.prepare(
    `select e.rowid, e.input_tokens as inputTokens, e.output_tokens as outputTokens,
       e.repo_hash as repoHash, e.branch_hash as branchHash, e.head_sha as headSha,
       l.context_conflict as contextConflict
     from buffered_events e join repo_context_event_links l on l.event_id = e.id
     where e.id = ?`,
  ).get(mismatch.id) as {
    rowid: number;
    inputTokens: number;
    outputTokens: number;
    repoHash: string | null;
    branchHash: string | null;
    headSha: string | null;
    contextConflict: number;
  };
  const mismatchProjectionBefore = bufferA.database.prepare(
    `select repo_hash as repoHash, branch_hash as branchHash, head_hash as headHash,
       input_tokens as inputTokens, output_tokens as outputTokens
     from dashboard_event_facts where raw_rowid = ?`,
  ).get(mismatchRawBefore.rowid);
  const mismatchLease = bufferA.delivery.lease({ leaseId: "row-mismatch-proof" });
  const mismatchSealedBefore = mismatchLease.items
    .find((item) => item.deliveryId === mismatch.id)?.envelopeJson;
  assert.ok(mismatchSealedBefore);
  const conflictCountBefore = bufferA.repoContextUnknownCounters()
    .find((row) => row.reason === "result_conflict")?.droppedCount ?? 0;
  const mismatchApply = bufferA.applyRepoContextResults([mismatchResult]);
  const mismatchRawAfter = bufferA.database.prepare(
    `select e.input_tokens as inputTokens, e.output_tokens as outputTokens,
       e.repo_hash as repoHash, e.branch_hash as branchHash, e.head_sha as headSha,
       l.context_conflict as contextConflict
     from buffered_events e join repo_context_event_links l on l.event_id = e.id
     where e.id = ?`,
  ).get(mismatch.id) as Omit<typeof mismatchRawBefore, "rowid">;
  const mismatchProjectionAfter = bufferA.database.prepare(
    `select repo_hash as repoHash, branch_hash as branchHash, head_hash as headHash,
       input_tokens as inputTokens, output_tokens as outputTokens
     from dashboard_event_facts where raw_rowid = ?`,
  ).get(mismatchRawBefore.rowid);
  const mismatchOutboxAfter = bufferA.database.prepare(
    `select sealed_envelope_json as sealed from upload_outbox where delivery_id = ?`,
  ).get(mismatch.id) as { sealed: string };
  const conflictCountAfter = bufferA.repoContextUnknownCounters()
    .find((row) => row.reason === "result_conflict")?.droppedCount ?? 0;
  check(
    "mismatched_existing_repo_never_synthesizes_branch_or_head_tuple",
    mismatchApply.rowsVisited === 1 && mismatchApply.rowsFilled === 0 &&
      mismatchRawBefore.repoHash === replayResult.repoHash &&
      mismatchRawAfter.repoHash === mismatchRawBefore.repoHash &&
      mismatchRawAfter.branchHash === null && mismatchRawAfter.headSha === null &&
      mismatchRawAfter.contextConflict === 1 &&
      mismatchRawAfter.inputTokens === mismatchRawBefore.inputTokens &&
      mismatchRawAfter.outputTokens === mismatchRawBefore.outputTokens &&
      Boolean(mismatchProjectionBefore) &&
      JSON.stringify(mismatchProjectionAfter) === JSON.stringify(mismatchProjectionBefore) &&
      mismatchOutboxAfter.sealed === mismatchSealedBefore &&
      conflictCountAfter === conflictCountBefore + 1 &&
      bufferA.drainRepoContextFills().rowsFilled === 0,
  );

  const resolverRequests = [
    {
      contextId: `repoctx:v1:${"d".repeat(64)}`,
      source: "codex" as const,
      cwd: cwdA,
    },
    {
      contextId: `repoctx:v1:${"e".repeat(64)}`,
      source: "claude_code" as const,
      cwd: cwdB,
    },
  ];
  let resolverCalls = 0;
  const contained = resolveMaintenanceRepoContexts(resolverRequests, {
    quarantine: null,
    reportProgress: () => true,
    recordRepoLabel: () => undefined,
    resolveRequests: (batch, options) => {
      resolverCalls += 1;
      if (resolverCalls === 1) throw new Error("proof_resolver_fault");
      return resolveRepoContextRequests(batch, options);
    },
  });
  bufferA.database.prepare(
    `insert into repo_context_inflight (context_id, started_at, owner)
     values (?, ?, 'child')`,
  ).run(contained[0]!.contextId, "2026-07-20T19:00:00.000Z");
  const resolutionFailuresBefore = bufferA.repoContextUnknownCounters()
    .find((row) => row.reason === "resolution_failed")?.droppedCount ?? 0;
  const containedApply = bufferA.applyRepoContextResults([contained[0]!]);
  const resolutionFailuresAfter = bufferA.repoContextUnknownCounters()
    .find((row) => row.reason === "resolution_failed")?.droppedCount ?? 0;
  check(
    "resolver_fault_is_contained_as_exact_unknown_and_later_requests_continue",
    contained.length === 2 && contained[0]!.repoHash === null &&
      contained[0]!.contextId === resolverRequests[0]!.contextId &&
      contained[1]!.repoHash !== null && resolverCalls === 2 &&
      containedApply.unknownResults === 1 &&
      resolutionFailuresAfter === resolutionFailuresBefore + 1 &&
      !bufferA.database.prepare(
        `select 1 from repo_context_inflight where context_id = ?`,
      ).get(contained[0]!.contextId),
  );

  const storedText = bufferA.database
    .prepare(
      `select group_concat(value, '|') as text from (
         select payload_json as value from buffered_events
         union all select context_id from repo_context_event_links
         union all select coalesce(repo_hash, '') || coalesce(branch_hash, '') || coalesce(head_sha, '')
           from repo_context_results
         union all select coalesce(base_envelope_json, '') || coalesce(sealed_envelope_json, '')
           from upload_outbox
       )`,
    )
    .get() as { text: string };
  const artifacts = [ledger, `${ledger}-wal`, `${ledger}-shm`];
  check(
    "raw_cwd_absent_from_rows_db_wal_shm_and_outbound_base",
    !storedText.text.includes(privateCwdSentinel) &&
      artifacts.every((file) => !artifactContains(file, privateCwdSentinel)),
  );

  bufferB.close();
  bufferA.close();

  const fillDrain = new LocalEventBuffer(path.join(root, "fill-drain.sqlite"));
  const fillEvents = Array.from({ length: 512 }, (_, index) => {
    const candidate = event(40_000 + index, "fill-drain-session");
    attachRepoContextSidecar(candidate, `fill-drain-occurrence-${index % 8}`, cwdA);
    return { event: candidate, suppressedFields: [] };
  });
  fillDrain.appendMany(fillEvents);
  const fillRequests = fillDrain.takeRepoContextBatch();
  assert.equal(fillRequests.length, 8);
  const fillResults = resolveRepoContextRequests(
    fillDrain.beginRepoContextResolution(fillRequests),
  );
  const firstFill = fillDrain.applyRepoContextResults(fillResults);
  const beforeReplayChanges = Number((fillDrain.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  const retainedReplay = fillDrain.applyRepoContextResults(fillResults);
  const afterReplayChanges = Number((fillDrain.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  const fillDrains = [
    fillDrain.drainRepoContextFills(),
    fillDrain.drainRepoContextFills(),
    fillDrain.drainRepoContextFills(),
  ];
  const unfilledAfterDrains = Number((fillDrain.database.prepare(
    `select count(*) as count
     from repo_context_event_links l join buffered_events e on e.id = l.event_id
     where l.fill_pending = 1 and e.repo_hash is null`,
  ).get() as { count: number }).count);
  const tokenTruthBeforeSuppression = fillDrain.database.prepare(
    `select count(*) as events, sum(input_tokens) as inputTokens,
       sum(output_tokens) as outputTokens from buffered_events`,
  ).get() as { events: number; inputTokens: number; outputTokens: number };
  fillDrain.transactionWithRepoContextHandoffs(() => {
    for (const request of fillRequests) {
      assert.equal(fillDrain.suppressRepoContextId(request.contextId), true);
    }
  });
  const cleanupPlan = fillDrain.database.prepare(
    `explain query plan
     select l.event_id, e.rowid
     from repo_context_event_links l indexed by idx_repo_context_event_links_cleanup
     left join buffered_events e on e.id = l.event_id
     where l.context_id = ? and l.suppression_cleaned = 0
     order by l.event_id limit 128`,
  ).all(fillRequests[0]!.contextId) as Array<{ detail: string }>;
  const fillPlan = fillDrain.database.prepare(
    `explain query plan
     select l.event_id, e.repo_hash, r.repo_hash
     from repo_context_event_links l indexed by idx_repo_context_event_links_pending_context
     join buffered_events e on e.id = l.event_id
     join repo_context_results r on r.context_id = l.context_id
     where l.context_id = ? and l.fill_pending = 1
     order by l.event_id limit 128`,
  ).all(fillRequests[0]!.contextId) as Array<{ detail: string }>;
  const resultDriverPlan = fillDrain.database.prepare(
    `explain query plan
     select r.context_id
     from repo_context_results r
     where not exists (
       select 1 from repo_context_suppressions s where s.context_id = r.context_id
     ) and exists (
       select 1
       from repo_context_event_links l
         indexed by idx_repo_context_event_links_pending_context
       where l.context_id = r.context_id and l.fill_pending = 1
     )
     order by r.accepted_at, r.context_id limit 8`,
  ).all() as Array<{ detail: string }>;
  const suppressionDrains = [
    fillDrain.drainRepoContextSuppressions(),
    fillDrain.drainRepoContextSuppressions(),
    fillDrain.drainRepoContextSuppressions(),
    fillDrain.drainRepoContextSuppressions(),
  ];
  const linkedAfterSuppression = Number((fillDrain.database.prepare(
    `select count(*) as count from buffered_events where
       repo_hash is not null or branch_hash is not null or head_sha is not null`,
  ).get() as { count: number }).count);
  const tokenTruthAfterSuppression = fillDrain.database.prepare(
    `select count(*) as events, sum(input_tokens) as inputTokens,
       sum(output_tokens) as outputTokens from buffered_events`,
  ).get() as { events: number; inputTokens: number; outputTokens: number };
  const suppressionComplete = Number((fillDrain.database.prepare(
    `select count(*) as count from repo_context_suppressions where cleanup_complete = 1`,
  ).get() as { count: number }).count);
  const removeResult = fillDrain.database.prepare(
    `delete from repo_context_results where context_id = ?`,
  );
  const resultsCollectable = fillDrain.database.transaction(() =>
    fillRequests.reduce(
      (changes, request) => changes + removeResult.run(request.contextId).changes,
      0,
    )
  )();
  check(
    "legal_512_event_context_converges_over_bounded_maintenance_fill_slices",
    firstFill.rowsVisited === 128 && firstFill.rowsFilled === 128 &&
      retainedReplay.resultReplays === 8 && retainedReplay.rowsFilled === 0 &&
      afterReplayChanges === beforeReplayChanges &&
      fillDrains.every((drain) => drain.rowsVisited <= 128 && drain.rowsFilled <= 128) &&
      fillDrains.reduce((sum, drain) => sum + drain.rowsFilled, 0) === 384 &&
      unfilledAfterDrains === 0,
  );
  check(
    "terminal_suppression_uses_new_only_context_index_and_converges_in_fixed_slices",
    cleanupPlan.some((row) => row.detail.includes("idx_repo_context_event_links_cleanup")) &&
      cleanupPlan.every((row) => !row.detail.includes("SCAN buffered_events")) &&
      suppressionDrains.every((drain) =>
        drain.contextsVisited <= 128 && drain.rowsVisited <= 128 && drain.rowsCleared <= 128
      ) &&
      suppressionDrains.reduce((sum, drain) => sum + drain.rowsCleared, 0) === 512 &&
      linkedAfterSuppression === 0 && suppressionComplete === 8 &&
      JSON.stringify(tokenTruthAfterSuppression) === JSON.stringify(tokenTruthBeforeSuppression) &&
      resultsCollectable === 8,
  );
  check(
    "fill_plan_searches_sidecar_then_existing_event_primary_key_without_raw_scan",
    fillPlan.some((row) =>
      row.detail.includes("idx_repo_context_event_links_pending_context")
    ) &&
      fillPlan.some((row) =>
        row.detail.includes("sqlite_autoindex_buffered_events_1")
      ) &&
      fillPlan.every((row) => !row.detail.includes("SCAN buffered_events")),
  );
  check(
    "global_fill_is_capped_result_driven_and_never_scans_unresolved_links",
    resultDriverPlan.some((row) => row.detail.includes("idx_repo_context_results_gc")) &&
      resultDriverPlan.some((row) =>
        row.detail.includes("idx_repo_context_event_links_pending_context")
      ) &&
      resultDriverPlan.every((row) => !row.detail.includes("SCAN l")),
  );
  fillDrain.close();

  const handoffLedger = path.join(root, "handoff.sqlite");
  const handoff = new LocalEventBuffer(handoffLedger);
  const handoffColumns = handoff.database.pragma("table_info(repo_context_handoffs)") as Array<{
    name: string;
  }>;
  check(
    "durable_handoff_schema_contains_only_context_id",
    handoffColumns.length === 1 && handoffColumns[0]?.name === "context_id",
  );
  handoff.database.exec(`
    create trigger proof_abort_repo_context_handoff
    before insert on repo_context_handoffs
    begin
      select raise(abort, 'proof_handoff_abort');
    end;
  `);
  const rollbackSingle = event(19, "handoff-rollback-single");
  attachRepoContextSidecar(rollbackSingle, rollbackSingle.id, cwdA);
  assert.throws(() => handoff.append(rollbackSingle), /proof_handoff_abort/);
  assert.equal(peekRepoContextSidecar(rollbackSingle), undefined);
  assert.equal(
    handoff.database.prepare(`select 1 from buffered_events where id = ?`).get(rollbackSingle.id),
    undefined,
  );
  const rollbackA = event(20, "handoff-rollback");
  const rollbackB = event(21, "handoff-rollback");
  attachRepoContextSidecar(rollbackA, rollbackA.id, cwdA);
  attachRepoContextSidecar(rollbackB, rollbackB.id, cwdB);
  assert.throws(
    () => handoff.appendMany([
      { event: rollbackA, suppressedFields: [] },
      { event: rollbackB, suppressedFields: [] },
    ]),
    /proof_handoff_abort/,
  );
  check(
    "append_and_append_many_roll_back_handoff_and_clear_sidecars_on_throw",
    Number((handoff.database.prepare(
      `select count(*) as count from buffered_events where id in (?, ?)`,
    ).get(rollbackA.id, rollbackB.id) as { count: number }).count) === 0 &&
      peekRepoContextSidecar(rollbackA) === undefined &&
      peekRepoContextSidecar(rollbackB) === undefined,
  );
  handoff.database.exec(`drop trigger proof_abort_repo_context_handoff`);
  const crashGapEvent = event(22, "handoff-crash-gap");
  attachRepoContextSidecar(crashGapEvent, crashGapEvent.id, cwdA);
  handoff.append(crashGapEvent);
  const crashGapRequest = handoff.takeRepoContextBatch()[0]!;
  assert.ok(crashGapRequest);
  const transitionEvent = event(24, "handoff-transition");
  attachRepoContextSidecar(transitionEvent, transitionEvent.id, cwdB);
  handoff.append(transitionEvent);
  const transitionContextId = contextId(handoff, transitionEvent.id);
  const transitionRequest = handoff.takeRepoContextBatch()
    .find((request) => request.contextId === transitionContextId)!;
  assert.ok(transitionRequest);
  check(
    "handoff_survives_commit_and_queue_take_until_begin",
    Number((handoff.database.prepare(
      `select count(*) as count from repo_context_handoffs`,
    ).get() as { count: number }).count) === 2,
  );
  const childView = new LocalEventBuffer(handoffLedger, { databaseBusyTimeoutMs: 900 });
  check(
    "maintenance_child_open_does_not_recover_parent_handoff",
    Number((childView.database.prepare(
      `select count(*) as count from repo_context_handoffs`,
    ).get() as { count: number }).count) === 2,
  );
  childView.close();

  const crashGapResult = resolveRepoContextRequests([crashGapRequest])[0]!;
  const handoffOnlyChangesBefore = Number((handoff.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  assert.throws(
    () => handoff.applyRepoContextResults([crashGapResult]),
    /repo_context_result_not_inflight/,
  );
  const handoffOnlyChangesAfter = Number((handoff.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  check(
    "unsolicited_handoff_only_result_is_zero_write_and_preserves_unknown_handoff",
    handoffOnlyChangesAfter === handoffOnlyChangesBefore &&
      handoff.repoContextQueueStatus().queued === 2 &&
      handoff.repoContextInflightCount() === 0 &&
      Boolean(handoff.database.prepare(
        `select 1 from repo_context_handoffs where context_id = ?`,
      ).get(crashGapRequest.contextId)) &&
      !handoff.database.prepare(
        `select 1 from repo_context_results where context_id = ?`,
      ).get(crashGapRequest.contextId) &&
      (handoff.database.prepare(
        `select repo_hash as repoHash from buffered_events where id = ?`,
      ).get(crashGapEvent.id) as { repoHash: string | null }).repoHash === null,
  );

  const transitioned = handoff.beginRepoContextResolution([transitionRequest]);
  check(
    "begin_moves_exact_handoff_to_inflight_once_after_child_open",
    transitioned.length === 1 && handoff.repoContextInflightCount() === 1 &&
      Number((handoff.database.prepare(
        `select count(*) as count from repo_context_handoffs`,
      ).get() as { count: number }).count) === 1,
  );
  const transitionResult = resolveRepoContextRequests(transitioned)[0]!;
  const duplicateChangesBefore = Number((handoff.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  assert.throws(
    () => handoff.applyRepoContextResults([transitionResult, transitionResult]),
    /repo_context_result_duplicate/,
  );
  const duplicateChangesAfter = Number((handoff.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  check(
    "duplicate_result_batch_rejects_before_any_durable_mutation",
    duplicateChangesAfter === duplicateChangesBefore &&
      handoff.repoContextInflightCount() === 1 &&
      !handoff.database.prepare(
        `select 1 from repo_context_results where context_id = ?`,
      ).get(transitionRequest.contextId),
  );
  const atomicChangesBefore = Number((handoff.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  assert.throws(
    () => handoff.applyRepoContextResults([transitionResult, crashGapResult]),
    /repo_context_result_not_inflight/,
  );
  const atomicChangesAfter = Number((handoff.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  check(
    "mixed_authorized_and_unsolicited_batch_is_all_or_nothing",
    atomicChangesAfter === atomicChangesBefore &&
      handoff.repoContextInflightCount() === 1 &&
      !handoff.database.prepare(
        `select 1 from repo_context_results where context_id in (?, ?)`,
      ).get(transitionRequest.contextId, crashGapRequest.contextId) &&
      Boolean(handoff.database.prepare(
        `select 1 from repo_context_handoffs where context_id = ?`,
      ).get(crashGapRequest.contextId)),
  );
  handoff.failRepoContextResolution(transitioned, "boundary_unavailable");
  handoff.close();
  const recoveredHandoff = new LocalEventBuffer(handoffLedger);
  const recoveredCount = recoveredHandoff.recoverRepoContextState();
  const recoveredAgain = recoveredHandoff.recoverRepoContextState();
  const workerCrashCount = recoveredHandoff.repoContextUnknownCounters()
    .find((row) => row.reason === "worker_crash")?.droppedCount;
  check(
    "parent_startup_recovers_unresolved_handoff_exactly_once",
    recoveredCount === 1 && recoveredAgain === 0 && workerCrashCount === 1 &&
      Number((recoveredHandoff.database.prepare(
        `select count(*) as count from repo_context_handoffs`,
      ).get() as { count: number }).count) === 0,
  );
  const resolvedStaleEvent = event(23, "resolved-stale-handoff");
  attachRepoContextSidecar(resolvedStaleEvent, resolvedStaleEvent.id, cwdA);
  recoveredHandoff.append(resolvedStaleEvent);
  const resolvedStaleRequest = recoveredHandoff.takeRepoContextBatch()[0]!;
  const resolvedStaleBegun = recoveredHandoff.beginRepoContextResolution([resolvedStaleRequest]);
  assert.equal(resolvedStaleBegun.length, 1);
  recoveredHandoff.applyRepoContextResults(resolveRepoContextRequests(resolvedStaleBegun));
  recoveredHandoff.database
    .prepare(`insert into repo_context_handoffs (context_id) values (?)`)
    .run(resolvedStaleRequest.contextId);
  check(
    "resolved_stale_handoff_clears_without_false_worker_crash",
    recoveredHandoff.recoverRepoContextState() === 0 &&
      recoveredHandoff.repoContextUnknownCounters()
        .find((row) => row.reason === "worker_crash")?.droppedCount === 1,
  );
  recoveredHandoff.close();

  const busyLedger = path.join(root, "begin-busy.sqlite");
  const busy = new LocalEventBuffer(busyLedger, { databaseBusyTimeoutMs: 0 });
  const busyEvent = event(25, "begin-busy");
  attachRepoContextSidecar(busyEvent, busyEvent.id, cwdA);
  busy.append(busyEvent);
  const busyRequest = busy.takeRepoContextBatch()[0]!;
  const busyCountersBefore = JSON.stringify(busy.repoContextUnknownCounters());
  const blocker = new Database(busyLedger, { timeout: 0 });
  blocker.pragma("journal_mode = WAL");
  blocker.exec("begin immediate");
  assert.throws(
    () => busy.beginRepoContextResolution([busyRequest]),
    (error: unknown) => Boolean(
      error && typeof error === "object" && "code" in error && error.code === "SQLITE_BUSY"
    ),
  );
  check(
    "sqlite_busy_begin_keeps_memory_request_id_and_durable_handoff_retryable",
    busy.repoContextQueueStatus().queued === 1 &&
      busy.takeRepoContextBatch()[0]?.contextId === busyRequest.contextId &&
      busy.repoContextInflightCount() === 0 &&
      JSON.stringify(busy.repoContextUnknownCounters()) === busyCountersBefore &&
      Boolean(busy.database.prepare(
        `select 1 from repo_context_handoffs where context_id = ?`,
      ).get(busyRequest.contextId)),
  );
  blocker.exec("rollback");
  blocker.close();
  const busyRetry = busy.beginRepoContextResolution(busy.takeRepoContextBatch());
  const busyApply = busy.applyRepoContextResults(resolveRepoContextRequests(busyRetry));
  check(
    "next_cadence_after_busy_commits_once_without_restart_or_counter_drift",
    busyRetry.length === 1 && busyApply.resultsInserted === 1 && busyApply.rowsFilled === 1 &&
      busy.repoContextQueueStatus().queued === 0 && busy.repoContextInflightCount() === 0 &&
      JSON.stringify(busy.repoContextUnknownCounters()) === busyCountersBefore &&
      !busy.database.prepare(
        `select 1 from repo_context_handoffs where context_id = ?`,
      ).get(busyRequest.contextId),
  );
  busy.close();

  const sealedLedger = path.join(root, "sealed.sqlite");
  const sealed = new LocalEventBuffer(sealedLedger, { delivery: { enabled: true } });
  const sealedEvent = event(10, "sealed-session");
  attachRepoContextSidecar(sealedEvent, sealedEvent.id, cwdB);
  sealed.append(sealedEvent);
  const sealedRequest = sealed.takeRepoContextBatch()[0]!;
  const lease = sealed.delivery.lease({ leaseId: "repo-context-proof-lease" });
  assert.equal(lease.items.length, 1);
  const sealedBefore = lease.items[0]!.envelopeJson;
  sealed.beginRepoContextResolution([sealedRequest]);
  const sealedApply = sealed.applyRepoContextResults(resolveRepoContextRequests([sealedRequest]));
  const sealedAfter = sealed.database
    .prepare(
      `select sealed_envelope_json as sealed, repo_hash as repoHash
       from upload_outbox where delivery_id = ?`,
    )
    .get(sealedEvent.id) as { sealed: string; repoHash: string | null };
  const localAfterSeal = sealed.database
    .prepare(`select repo_hash as repoHash from buffered_events where id = ?`)
    .get(sealedEvent.id) as { repoHash: string | null };
  check(
    "post_attempt_local_fill_never_rewrites_sealed_outbound_bytes",
    sealedApply.rowsFilled === 1 && localAfterSeal.repoHash !== null &&
      sealedAfter.repoHash === null && sealedAfter.sealed === sealedBefore,
  );
  sealed.close();

  const overflowLedger = path.join(root, "overflow.sqlite");
  const overflow = new LocalEventBuffer(overflowLedger);
  for (let index = 0; index < 129; index += 1) {
    const candidate = event(1_000 + index, `overflow-${index}`);
    attachRepoContextSidecar(candidate, candidate.id, cwdA);
    overflow.append(candidate);
  }
  const overflowStatus = overflow.repoContextQueueStatus();
  const firstBatch = overflow.takeRepoContextBatch();
  check(
    "transient_queue_is_bounded_128_and_drains_at_most_8",
    overflowStatus.queued === 128 && firstBatch.length === 8 &&
      overflow.repoContextUnknownCounters().some((row) =>
        row.reason === "queue_overflow" && row.droppedCount === 1
      ),
  );
  overflow.close();

  const childLedger = path.join(root, "child-inflight.sqlite");
  const parentOwner = new LocalEventBuffer(childLedger);
  const parentOwnedEvent = event(2_000, "parent-owned-handoff");
  attachRepoContextSidecar(parentOwnedEvent, parentOwnedEvent.id, cwdA);
  parentOwner.append(parentOwnedEvent);
  const parentOwnedRequest = parentOwner.takeRepoContextBatch()[0]!;
  assert.ok(parentOwnedRequest);
  const childOwner = new LocalEventBuffer(childLedger);
  childOwner.beginChildRepoContextRun();
  const childSession = "child-inflight-shared";
  const childDonor = aiInteractionEventSchema.parse({
    ...event(2_099, childSession),
    metadata: { git: { remoteUrlHash: remoteLinkageHash("https://example.invalid/donor.git") } },
  });
  childOwner.append(childDonor);
  for (let index = 0; index < 10; index += 1) {
    childOwner.transactionWithRepoContextHandoffs(() => {
      const candidate = event(2_100 + index, childSession);
      const request = childOwner.repoContextOccurrenceRequest(
        "codex",
        `child-inflight-occurrence-${index}`,
        index % 2 === 0 ? cwdA : cwdB,
      )!;
      const accepted = childOwner.stageRepoContextRequest(request);
      assert.equal(attachRepoContextId(candidate, request.contextId), true);
      childOwner.append(candidate);
      if (accepted) assert.equal(accepted, request.contextId);
    });
  }
  const childBatch = childOwner.finishChildRepoContextRun();
  const childBoundRows = Number((childOwner.database.prepare(
    `select count(*) as count
     from buffered_events e join repo_context_event_links l on l.event_id = e.id
     where e.session_id = ?`,
  ).get(childSession) as { count: number }).count);
  const childPendingRows = Number((childOwner.database.prepare(
    `select count(*) as count
     from buffered_events e join repo_context_event_links l on l.event_id = e.id
     where e.session_id = ? and l.fill_pending = 1`,
  ).get(childSession) as { count: number }).count);
  runRepoEnrichmentMaintenance(childOwner.database, {
    legacyBackfillLimit: 32,
    sessionLimit: 8,
    eventLimit: 32,
  });
  const exactUnknownAfterStitch = Number((childOwner.database.prepare(
    `select count(*) as count
     from buffered_events e join repo_context_event_links l on l.event_id = e.id
     where e.session_id = ? and e.repo_hash is null`,
  ).get(childSession) as { count: number }).count);
  const childOverflow = childOwner.repoContextUnknownCounters()
    .find((row) => row.reason === "queue_overflow")?.droppedCount;
  check(
    "child_run_commits_exactly_eight_inflight_and_counts_excess_unknown_once",
    childBatch.length === 8 && childBoundRows === 10 && childPendingRows === 8 &&
      exactUnknownAfterStitch === 10 &&
      childOwner.repoContextInflightCount() === 8 && childOverflow === 2 &&
      childOwner.repoContextQueueStatus().queued === 0,
  );
  childOwner.close();
  const recoveredChildInflight = parentOwner.failRepoContextRun([], "worker_crash");
  check(
    "parent_failure_gate_clears_only_child_inflight_and_preserves_parent_handoff",
    recoveredChildInflight.parentUnknown === 0 && recoveredChildInflight.childUnknown === 8 &&
      parentOwner.repoContextInflightCount() === 0 &&
      Boolean(parentOwner.database.prepare(
        `select 1 from repo_context_handoffs where context_id = ?`,
      ).get(parentOwnedRequest.contextId)) &&
      parentOwner.takeRepoContextBatch()[0]?.contextId === parentOwnedRequest.contextId,
  );
  parentOwner.close();

  const scopeLedger = path.join(root, "outer-scope-rollback.sqlite");
  const scope = new LocalEventBuffer(scopeLedger);
  scope.database.exec(`create table proof_cursor (value integer not null)`);
  scope.beginChildRepoContextRun();
  const scopedEvent = event(2_200, "outer-scope-rollback");
  assert.throws(
    () => scope.transactionWithRepoContextHandoffs(() => {
      const request = scope.repoContextOccurrenceRequest(
        "codex",
        "outer-scope-rollback-occurrence",
        cwdA,
      )!;
      const accepted = scope.stageRepoContextRequest(request);
      assert.ok(accepted);
      assert.equal(attachRepoContextId(scopedEvent, accepted), true);
      scope.append(scopedEvent);
      scope.database.prepare(`insert into proof_cursor (value) values (1)`).run();
      throw new Error("proof_outer_scope_abort");
    }),
    /proof_outer_scope_abort/,
  );
  const scopedBatch = scope.finishChildRepoContextRun();
  check(
    "outer_scope_rollback_leaves_zero_event_cursor_inflight_handoff_or_raw_queue",
    scopedBatch.length === 0 && scope.repoContextInflightCount() === 0 &&
      scope.repoContextQueueStatus().queued === 0 &&
      Number((scope.database.prepare(`select count(*) as count from proof_cursor`).get() as { count: number }).count) === 0 &&
      !scope.database.prepare(`select 1 from buffered_events where id = ?`).get(scopedEvent.id) &&
      Number((scope.database.prepare(
        `select count(*) as count from repo_context_handoffs`,
      ).get() as { count: number }).count) === 0,
  );
  scope.close();

  const stitchLedger = path.join(root, "legacy-stitch.sqlite");
  const stitch = new LocalEventBuffer(stitchLedger);
  const legacyDonorHash = remoteLinkageHash("https://example.invalid/team/legacy-donor.git")!;
  const legacyDonor = aiInteractionEventSchema.parse({
    id: deterministicEventId(["repo-context-proof", "legacy-donor"]),
    source: "codex",
    dataMode: "metadata",
    eventType: "user_prompt_submit",
    observedAt: "2026-07-20T18:00:00.000Z",
    sessionId: "legacy-stitch-hostile",
    actionClass: "other",
    metadata: { git: { remoteUrlHash: legacyDonorHash } },
  });
  const exactTarget = aiInteractionEventSchema.parse({
    id: deterministicEventId(["repo-context-proof", "exact-target"]),
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: "2026-07-20T18:00:01.000Z",
    sessionId: "legacy-stitch-hostile",
    inputTokens: 11,
    outputTokens: 2,
    actionClass: "other",
  });
  stitch.append(legacyDonor);
  attachRepoContextSidecar(exactTarget, exactTarget.id, cwdB);
  stitch.append(exactTarget);
  const exactTargetRequest = stitch.takeRepoContextBatch()[0]!;
  const stitchReceipt = runRepoEnrichmentMaintenance(stitch.database, {
    legacyBackfillLimit: 32,
    sessionLimit: 8,
    eventLimit: 32,
  });
  const beforeExactFill = stitch.database
    .prepare(`select repo_hash as repoHash from buffered_events where id = ?`)
    .get(exactTarget.id) as { repoHash: string | null };
  const exactTargetBegun = stitch.beginRepoContextResolution([exactTargetRequest]);
  const exactTargetResult = resolveRepoContextRequests(exactTargetBegun)[0]!;
  const exactTargetApply = stitch.applyRepoContextResults([exactTargetResult]);
  const afterExactFill = stitch.database
    .prepare(`select repo_hash as repoHash from buffered_events where id = ?`)
    .get(exactTarget.id) as { repoHash: string | null };
  check(
    "legacy_stitch_excludes_exact_context_donors_and_targets",
    stitchReceipt.backward === 0 && stitchReceipt.forward === 0 &&
      beforeExactFill.repoHash === null && exactTargetApply.rowsFilled === 1 &&
      afterExactFill.repoHash === exactTargetResult.repoHash &&
      afterExactFill.repoHash !== legacyDonorHash,
  );
  stitch.close();

  const retentionLedger = path.join(root, "retention.sqlite");
  const retention = new LocalEventBuffer(retentionLedger);
  const protectedEvent = event(30_000, "retention-protected");
  attachRepoContextSidecar(protectedEvent, protectedEvent.id, cwdB);
  retention.append(protectedEvent);
  const protectedRequest = retention.takeRepoContextBatch()[0]!;
  const protectedBegun = retention.beginRepoContextResolution([protectedRequest]);
  assert.equal(protectedBegun.length, 1);
  const protectedResult = resolveRepoContextRequests(protectedBegun)[0]!;
  retention.applyRepoContextResults([protectedResult], 0);
  assert.throws(
    () => retention.database
      .prepare(`delete from repo_context_results where context_id = ?`)
      .run(protectedRequest.contextId),
    /repo_context_result_still_referenced/,
  );
  const retentionHash = remoteLinkageHash("https://example.invalid/team/retention.git")!;
  const applyRetentionRange = (start: number, count: number) => {
    for (let offset = 0; offset < count; offset += 8) {
      const batchSize = Math.min(8, count - offset);
      const batch = Array.from({ length: batchSize }, (_, index) => {
        const ordinal = start + offset + index;
        return {
          contextId: `repoctx:v1:${ordinal.toString(16).padStart(64, "0")}`,
          repoHash: retentionHash,
          branchHash: null,
          headSha: null,
          resolvedAt: new Date(Date.UTC(2026, 6, 20, 20, 0, ordinal % 60)).toISOString(),
          resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
        };
      });
      const authorize = retention.database.prepare(
        `insert into repo_context_inflight (context_id, started_at) values (?, ?)`,
      );
      retention.database.transaction(() => {
        for (const result of batch) authorize.run(result.contextId, result.resolvedAt);
      })();
      retention.applyRepoContextResults(batch, 0);
    }
  };
  applyRetentionRange(1, RESULT_RETENTION_LIMIT + 128);
  retention.database.pragma("wal_checkpoint(TRUNCATE)");
  const plateauPagesBefore = Number(retention.database.pragma("page_count", { simple: true }));
  applyRetentionRange(RESULT_RETENTION_LIMIT + 129, 128);
  retention.database.pragma("wal_checkpoint(TRUNCATE)");
  const plateauPagesAfter = Number(retention.database.pragma("page_count", { simple: true }));
  const retentionStatus = retention.repoContextResultRetentionStatus();
  const protectedStillPending = retention.database
    .prepare(
      `select e.repo_hash as eventRepoHash,
         exists(
           select 1 from repo_context_event_links l
           join repo_context_results r on r.context_id = l.context_id
           where l.event_id = e.id
         ) as resultExists
       from buffered_events e where e.id = ?`,
    )
    .get(protectedEvent.id) as { eventRepoHash: string | null; resultExists: number };
  check(
    "successful_result_retention_is_capped_guarded_and_reuses_pages",
    retentionStatus.count === RESULT_RETENTION_LIMIT && !retentionStatus.capped &&
      protectedStillPending.eventRepoHash === null && protectedStillPending.resultExists === 1 &&
      plateauPagesAfter <= plateauPagesBefore + RESULT_PLATEAU_PAGE_BUDGET,
  );

  const directInsert = retention.database.prepare(
    `insert into repo_context_results
       (context_id, repo_hash, branch_hash, head_sha, resolved_at, resolver_version, accepted_at)
     values (?, ?, null, null, ?, ?, ?)`,
  );
  retention.database.transaction(() => {
    for (let index = 0; index < 129; index += 1) {
      const context = `repoctx:v1:${(100_000 + index).toString(16).padStart(64, "0")}`;
      const when = new Date(Date.UTC(2020, 0, 1, 0, 0, index % 60)).toISOString();
      directInsert.run(
        context,
        retentionHash,
        when,
        REPO_CONTEXT_RESOLVER_VERSION,
        when,
      );
    }
  })();
  const gcFirst = retention.runRepoContextResultGc(128);
  const gcSecond = retention.runRepoContextResultGc(128);
  check(
    "result_gc_is_bounded_128_and_converges_without_pending_deletion",
    gcFirst.visited <= 128 && gcFirst.deleted === 128 &&
      gcSecond.visited <= 128 && gcSecond.deleted === 1 &&
      retention.repoContextResultRetentionStatus().count === RESULT_RETENTION_LIMIT &&
      Boolean(retention.database.prepare(
        `select 1 from repo_context_results where context_id = ?`,
      ).get(protectedRequest.contextId)),
  );
  const protectedReplayChangesBefore = Number((retention.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  const protectedReplay = retention.applyRepoContextResults([protectedResult]);
  const protectedReplayChangesAfter = Number((retention.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  const protectedAfterReplay = retention.database
    .prepare(`select repo_hash as repoHash from buffered_events where id = ?`)
    .get(protectedEvent.id) as { repoHash: string | null };
  check(
    "late_identical_replay_without_inflight_is_literal_zero_write",
    protectedReplay.resultReplays === 1 && protectedReplay.rowsFilled === 0 &&
      protectedReplayChangesAfter === protectedReplayChangesBefore &&
      protectedAfterReplay.repoHash === null,
  );
  retention.close();

  const maxShapeLedger = path.join(root, "max-shape.sqlite");
  const maxShape = new LocalEventBuffer(maxShapeLedger);
  maxShape.database.exec(`
    create table proof_queue_counter_mutations (
      singleton integer primary key check (singleton = 1),
      mutations integer not null
    );
    insert into proof_queue_counter_mutations values (1, 0);
    create trigger proof_queue_counter_insert
    after insert on repo_context_unknown_counters
    when new.reason = 'queue_overflow'
    begin
      update proof_queue_counter_mutations set mutations = mutations + 1 where singleton = 1;
    end;
    create trigger proof_queue_counter_update
    after update on repo_context_unknown_counters
    when new.reason = 'queue_overflow'
    begin
      update proof_queue_counter_mutations set mutations = mutations + 1 where singleton = 1;
    end;
  `);
  const maxShapeEntries = Array.from({ length: MAX_APPEND_MANY_EVENTS }, (_, index) => {
    const candidate = event(10_000 + index, "max-shape-session");
    attachRepoContextSidecar(candidate, candidate.id, cwdA);
    return { event: candidate, suppressedFields: [] as string[] };
  });
  const expectedInputTokens = maxShapeEntries.reduce(
    (total, entry) => total + (entry.event.inputTokens ?? 0),
    0,
  );
  const maxShapeStarted = performance.now();
  maxShape.appendMany(maxShapeEntries);
  const maxShapeElapsedMs = performance.now() - maxShapeStarted;
  const unresolvedDrainChangesBefore = Number((maxShape.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  const unresolvedDrain = maxShape.drainRepoContextFills();
  const unresolvedDrainChangesAfter = Number((maxShape.database
    .prepare(`select total_changes() as count`).get() as { count: number }).count);
  const maxShapeFacts = maxShape.database
    .prepare(
      `select count(*) as count, coalesce(sum(input_tokens), 0) as inputTokens,
         coalesce(sum(output_tokens), 0) as outputTokens
       from buffered_events`,
    )
    .get() as { count: number; inputTokens: number; outputTokens: number };
  const maxShapeCounter = maxShape.repoContextUnknownCounters()
    .find((row) => row.reason === "queue_overflow")?.droppedCount;
  const maxShapeMutations = (maxShape.database
    .prepare(`select mutations from proof_queue_counter_mutations where singleton = 1`)
    .get() as { mutations: number }).mutations;
  const maxShapeHandoffs = (maxShape.database
    .prepare(`select count(*) as count from repo_context_handoffs`)
    .get() as { count: number }).count;
  const maxShapePendingLinks = (maxShape.database
    .prepare(`select count(*) as count from repo_context_event_links where fill_pending = 1`)
    .get() as { count: number }).count;
  check(
    "max_shape_append_many_is_exact_bounded_and_one_counter_mutation",
    maxShapeFacts.count === MAX_APPEND_MANY_EVENTS &&
      maxShapeFacts.inputTokens === expectedInputTokens &&
      maxShapeFacts.outputTokens === MAX_APPEND_MANY_EVENTS &&
      maxShape.repoContextQueueStatus().queued === 128 &&
      maxShapeHandoffs === 128 &&
      maxShapePendingLinks === 128 &&
      maxShapeCounter === MAX_APPEND_MANY_EVENTS - 128 &&
      maxShapeMutations === 1 &&
      unresolvedDrain.rowsVisited === 0 && unresolvedDrain.rowsFilled === 0 &&
      unresolvedDrainChangesAfter === unresolvedDrainChangesBefore &&
      maxShapeElapsedMs <= MAX_APPEND_MANY_MS &&
      peekRepoContextSidecar(maxShapeEntries[0]!.event) === undefined &&
      peekRepoContextSidecar(maxShapeEntries.at(-1)!.event) === undefined,
  );
  const invalidHandoffRows = (maxShape.database
    .prepare(
      `select count(*) as count from repo_context_handoffs
       where length(context_id) != 75 or substr(context_id, 1, 11) != 'repoctx:v1:'
         or substr(context_id, 12) glob '*[^0-9a-f]*'`,
    )
    .get() as { count: number }).count;
  assert.equal(invalidHandoffRows, 0);
  maxShape.close();
  check(
    "max_shape_raw_cwd_absent_from_durable_artifacts",
    [maxShapeLedger, `${maxShapeLedger}-wal`, `${maxShapeLedger}-shm`]
      .every((file) => !artifactContains(file, privateCwdSentinel)),
  );

  process.stdout.write(`${JSON.stringify({
    schema: "plimsoll.repo-context-proof.v1",
    gateReady: true,
    checks: checks.length,
    rawPathValuesPersisted: 0,
    legacyUpgrade: {
      rows: LEGACY_UPGRADE_ROWS,
      wallMs: Number(legacyUpgradeWallMs.toFixed(2)),
      cpuMs: Number(((legacyUpgradeCpu.user + legacyUpgradeCpu.system) / 1_000).toFixed(2)),
      eventPageDelta: legacyPagesAfter - legacyPagesBefore,
    },
  })}\n`);
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
