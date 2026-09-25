import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { markRawPrivacyDisposition } from "../packages/collector-cli/src/privacy-disposition";
import { isSqliteContentionError, SyncStorageBusyError } from "../packages/collector-cli/src/sqlite-contention";
import { asHttpBoundaryRejection } from "../packages/collector-cli/src/http-boundary";
import {
  buildSessionSyncRow,
  collectSessionSnapshots,
  emptyDaemonSessionSyncState,
  listLedgerSessionIds,
  listLedgerSessionIdsOffThread,
  planDaemonSessionSync,
  readLedgerOffThread,
  runSessionSync,
} from "../packages/collector-cli/src/session-sync";
import {
  ensureSessionSummarySchema,
  listSessionSummaryPendingIds,
  sessionSummaryCounters,
  updateSessionSummary,
} from "../packages/collector-cli/src/session-summary";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

const expect = process.env.EXPECT === "red" ? "red" : "green";
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-session-summary-proof-"));
const ledgerPath = path.join(root, "ledger.sqlite");
const tenantId = "00000000-0000-4000-8000-000000000741";
const installKey = "session-summary-proof-install";
const until = "2026-09-30T23:59:59.000Z";
const sessionA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa41";
const sessionB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbb41";
const sessionC = "cccccccc-cccc-4ccc-8ccc-cccccccccc41";
const hashA = `sha256:${"a".repeat(64)}`;
const hashB = `sha256:${"b".repeat(64)}`;

const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest",
  tenantId,
  installKey,
  uploadSigningSecret: "session-summary-proof-secret",
});

function uuid(index: number) {
  return `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`;
}

function insertRaw(
  buffer: LocalEventBuffer,
  input: {
    rowid?: number;
    id: string;
    sessionId: string;
    observedAt: string;
    createdAt: string;
    inputTokens: number;
    outputTokens: number;
    costUsd?: number | null;
    repoHash?: string | null;
    branchHash?: string | null;
    accountHash?: string | null;
  },
) {
  buffer.database.prepare(`
    insert into buffered_events
      (rowid, id, source, event_type, data_mode, observed_at, payload_json,
       suppressed_fields_json, created_at, session_id, input_tokens, output_tokens,
       cost_usd, repo_hash, branch_hash, account_hash, workspace_id,
       privacy_generation)
    values (?, ?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    input.rowid ?? null,
    input.id,
    input.observedAt,
    input.createdAt,
    input.sessionId,
    input.inputTokens,
    input.outputTokens,
    input.costUsd ?? null,
    input.repoHash ?? null,
    input.branchHash ?? null,
    input.accountHash ?? null,
    tenantId,
    `generation-${input.id}`,
  );
}

function expectedSessions(buffer: LocalEventBuffer, sessionIds: string[]) {
  return collectSessionSnapshots(buffer.database, { until, sessionIds })
    .map(buildSessionSyncRow)
    .flatMap((row) => row.ok ? [row.row] : [])
    .sort((a, b) => a.session.id.localeCompare(b.session.id));
}

async function runIncremental(buffer: LocalEventBuffer, sessionIds: string[]) {
  let fullRecomputes = 0;
  let rowsRead = 0;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    let wire = "";
    const fetchImpl = (async (_input, init) => {
      wire = String(init?.body ?? "");
      return new Response(JSON.stringify({
        ...acceptedFixtureDelivery(wire, installKey),
        inserted: JSON.parse(wire).sessions.length,
        updated: 0,
        skippedStale: 0,
      }), { status: 200, headers: { "content-type": "application/json" } });
    }) as typeof fetch;
    const result = await runSessionSync(config, {
      ledgerDb: buffer.database,
      incremental: true,
      sessionIds,
      until,
      fetchImpl,
      sleep: async () => undefined,
      delayMs: 0,
      maxAttemptsPerBatch: 1,
      summaryMaxRows: 100_000,
      summaryMaxMs: 5_000,
      log: () => undefined,
    });
    fullRecomputes += result.summaryStats.fullRecomputes;
    rowsRead += result.summaryStats.rowsRead;
    if (result.summaryComplete) {
      return {
        result: { ...result, summaryStats: { ...result.summaryStats, fullRecomputes, rowsRead } },
        sent: wire ? JSON.parse(wire).sessions : [],
      };
    }
  }
  throw new Error("session_summary_did_not_complete_within_20_bounded_slices");
}

async function runFenceMultiBatchCase(rttMs: number) {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), `p41-fence-${rttMs}-`));
  const buffer = new LocalEventBuffer(path.join(fixture, "ledger.sqlite"), { workspaceId: tenantId });
  const external = new Database(buffer.database.name, { fileMustExist: true, timeout: 0 });
  const sessionIds = Array.from({ length: 1_200 }, (_, index) => uuid(10_000 + index));
  for (const [index, id] of sessionIds.entries()) {
    const eventId = uuid(30_000 + index);
    insertRaw(buffer, {
      id: eventId,
      sessionId: id,
      observedAt: new Date(Date.parse("2026-09-20T00:00:00.000Z") + index * 1_000).toISOString(),
      createdAt: new Date(Date.parse("2026-09-20T00:00:00.000Z") + index * 1_000).toISOString(),
      inputTokens: 1,
      outputTokens: 1,
    });
  }
  // Keep the daemon-write probe outside the upload's leased session set. The
  // old fence still blocks it because SQLite's write reservation is database
  // wide; the repaired fence must leave this unrelated write free to commit.
  const unrelatedId = uuid(90_001);
  insertRaw(buffer, {
    id: unrelatedId,
    sessionId: uuid(90_000),
    observedAt: "2026-09-20T23:59:00.000Z",
    createdAt: "2026-09-20T23:59:00.000Z",
    inputTokens: 1,
    outputTokens: 1,
  });
  buffer.database.pragma("busy_timeout = 0");
  let fetchCalls = 0;
  let active = 0;
  let peak = 0;
  const daemonWrites: string[] = [];
  try {
    const result = await runSessionSync(config, {
      ledgerDb: buffer.database,
      incremental: true,
      sessionIds,
      until,
      batchSize: 500,
      concurrency: 2,
      delayMs: 100,
      maxAttemptsPerBatch: 1,
      fetchImpl: (async (_input, init) => {
        fetchCalls += 1;
        active += 1;
        peak = Math.max(peak, active);
        for (const db of [buffer.database, external]) {
          try {
            const write = db.prepare(
              "update buffered_events set output_tokens = output_tokens + 1 where id = ?",
            ).run(unrelatedId);
            daemonWrites.push(write.changes === 1 ? "succeeded" : `changes_${write.changes}`);
          } catch (error) {
            daemonWrites.push(error instanceof Error && "code" in error ? String(error.code) : String(error));
          }
        }
        await new Promise((resolve) => setTimeout(resolve, rttMs));
        active -= 1;
        const wire = String(init?.body ?? "");
        return new Response(JSON.stringify({
          ...acceptedFixtureDelivery(wire, installKey),
          inserted: JSON.parse(wire).sessions.length,
          updated: 0,
          skippedStale: 0,
        }), { status: 200, headers: { "content-type": "application/json" } });
      }) as typeof fetch,
      log: () => undefined,
    });
    console.log(JSON.stringify({ fenceProbe: { rttMs, fetchCalls, peak, daemonWrites,
      ok: result.ok, sentSessions: result.sentSessions, acceptedSessions: result.acceptedSessions,
      summaryComplete: result.summaryComplete, reason: result.reason } }));
    assert.equal(result.ok, true, JSON.stringify({ reason: result.reason, sentSessions: result.sentSessions }));
    assert.equal(result.summaryComplete, true, JSON.stringify(result));
    assert.equal(result.sentSessions, 1_200, JSON.stringify(result));
    assert.equal(result.acceptedSessions, 1_200, JSON.stringify(result));
    assert.equal(fetchCalls, 3, JSON.stringify({ fetchCalls, result }));
    assert.equal(peak, 2, JSON.stringify({ peak }));
    assert.deepEqual(daemonWrites, Array(6).fill("succeeded"));
    assert.deepEqual(result.pendingSummarySessionIds, []);
    return { rttMs, fetchCalls, peak, daemonWrites, result: {
      ok: result.ok,
      sentSessions: result.sentSessions,
      acceptedSessions: result.acceptedSessions,
      summaryComplete: result.summaryComplete,
      batches: result.batches,
    } };
  } finally {
    external.close();
    buffer.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

async function runFenceErasureCase(kind: "delete" | "privacy" | "receipt") {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "p41-fence-erasure-"));
  const buffer = new LocalEventBuffer(path.join(fixture, "ledger.sqlite"), { workspaceId: tenantId });
  const external = new Database(buffer.database.name, { fileMustExist: true, timeout: 0 });
  const targetSessionId = uuid(40_000);
  const eventId = uuid(40_001);
  insertRaw(buffer, {
    id: eventId,
    sessionId: targetSessionId,
    observedAt: "2026-09-20T00:00:00.000Z",
    createdAt: "2026-09-20T00:00:00.000Z",
    inputTokens: 1,
    outputTokens: 1,
  });
  const rowid = (buffer.database.prepare("select rowid from buffered_events where id = ?").get(eventId) as { rowid: number }).rowid;
  const erase = () => kind === "delete"
    ? external.prepare("delete from buffered_events where id = ?").run(eventId).changes
    : kind === "privacy"
      ? markRawPrivacyDisposition(external, rowid, "local_privacy_violation", new Date().toISOString())
      : external.prepare(`insert into upload_receipts
          (delivery_id, terminal_state, reason, status_class, attempt_count, created_at, terminal_at)
          values (?, 'dead', 'local_privacy_violation', 'local', 0, ?, ?)`)
        .run(eventId, until, until).changes;
  let firstBody = "";
  let erasureAttempt = "not_attempted";
  try {
    const first = await runSessionSync(config, {
      ledgerDb: buffer.database,
      incremental: true,
      sessionIds: [targetSessionId],
      until,
      batchSize: 500,
      concurrency: 1,
      delayMs: 0,
      maxAttemptsPerBatch: 1,
      fetchImpl: (async (_input, init) => {
        firstBody = String(init?.body ?? "");
        try {
          erasureAttempt = `committed_${erase()}`;
        } catch (error) {
          assert.equal(asHttpBoundaryRejection(error).reason, "storage_busy_retry");
          erasureAttempt = error instanceof Error && error.message === "session_sync_upload_lease"
            ? "deferred" : String(error);
          assert.equal(collectSessionSnapshots(buffer.database, { until, sessionIds: [targetSessionId] })[0]?.events, 1);

        }
        await new Promise((resolve) => setTimeout(resolve, 50));
        return new Response(JSON.stringify(acceptedFixtureDelivery(firstBody, installKey)), {
          status: 200, headers: { "content-type": "application/json" },
        });
      }) as typeof fetch,
      log: () => undefined,
    });
    assert.equal(first.ok, true, JSON.stringify(first));
    assert.equal(first.sentSessions, 1, JSON.stringify(first));
    assert.ok(firstBody.includes(targetSessionId), firstBody);
    assert.equal(erasureAttempt, "deferred", JSON.stringify({ erasureAttempt, first }));
    const committed = erase();
    assert.equal(committed, 1);

    let secondBody = "";
    const second = await runSessionSync(config, {
      ledgerDb: buffer.database,
      incremental: true,
      sessionIds: [targetSessionId],
      until,
      batchSize: 500,
      concurrency: 1,
      delayMs: 0,
      maxAttemptsPerBatch: 1,
      fetchImpl: (async (_input, init) => {
        secondBody = String(init?.body ?? "");
        return new Response(JSON.stringify(acceptedFixtureDelivery(secondBody, installKey)), {
          status: 200, headers: { "content-type": "application/json" },
        });
      }) as typeof fetch,
      log: () => undefined,
    });
    assert.equal(second.sentSessions, 0, JSON.stringify(second));
    assert.equal(secondBody, "", secondBody);
    return { kind, erasureAttempt, firstSent: first.sentSessions, committed, secondSent: second.sentSessions };
  } finally {
    external.close();
    buffer.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

async function runLeaseSafeIntakeCase() {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "p41-lease-intake-"));
  const buffer = new LocalEventBuffer(path.join(fixture, "ledger.sqlite"), { workspaceId: tenantId });
  const external = new Database(buffer.database.name, { fileMustExist: true, timeout: 0 });
  const sessionId = uuid(45_000);
  const originalId = uuid(45_001);
  insertRaw(buffer, {
    id: originalId, sessionId,
    observedAt: "2026-09-20T00:00:00.000Z", createdAt: "2026-09-20T00:00:00.000Z",
    inputTokens: 1, outputTokens: 1,
  });
  let firstBody = "";
  let spooled = 0;
  let erasure = "not_attempted";
  const intakeMs: number[] = [];
  try {
    const first = await runSessionSync(config, {
      ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until,
      delayMs: 0, maxAttemptsPerBatch: 1,
      fetchImpl: (async (_input, init) => {
        firstBody = String(init?.body ?? "");
        if (process.env.PROBE_MUTATION === "restore_intake_abort") {
          external.exec(`create trigger trg_session_sync_upload_lease_insert
            before insert on buffered_events
            when new.session_id is not null and exists (
              select 1 from session_sync_upload_leases where session_id = new.session_id
                and lease_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            begin select raise(abort, 'session_sync_upload_lease'); end`);
        }
        for (let index = 0; index < 3; index += 1) {
          await new Promise((resolve) => setTimeout(resolve, 300));
          const started = performance.now();
          try {
            assert.equal(buffer.append(aiInteractionEventSchema.parse({
              id: uuid(45_002 + index), sessionId, source: "codex",
              eventType: "assistant_response", observedAt: new Date().toISOString(),
              inputTokens: 2, outputTokens: 2,
            })), true);
          } catch (error) {
            assert.equal(asHttpBoundaryRejection(error).reason, "storage_busy_retry");
            spooled += 1;
          }
          intakeMs.push(performance.now() - started);
          if (index === 0) {
            try {
              external.prepare("delete from buffered_events where id = ?").run(originalId);
              erasure = "overtook_send";
            } catch (error) {
              erasure = error instanceof Error ? error.message : String(error);
            }
          }
        }
        return new Response(JSON.stringify(acceptedFixtureDelivery(firstBody, installKey)), {
          status: 200, headers: { "content-type": "application/json" },
        });
      }) as typeof fetch,
      log: () => undefined,
    });
    console.log(JSON.stringify({ leaseIntakeProbe: { spooled, intakeMs, erasure,
      firstBodyEvents: JSON.parse(firstBody).sessions[0].totals.events,
      firstOk: first.ok, pending: first.pendingSummarySessionIds } }));
    assert.equal(spooled, 0, "intake must not spool during a session-summary lease");
    assert.ok(intakeMs.every((ms) => ms < 200), JSON.stringify(intakeMs));
    assert.equal(erasure, "session_sync_upload_lease");
    assert.equal(JSON.parse(firstBody).sessions[0].totals.events, 1);
    assert.equal(first.ok, false, JSON.stringify(first));
    assert.ok(first.pendingSummarySessionIds.includes(sessionId), JSON.stringify(first));
    assert.equal(external.prepare("delete from buffered_events where id = ?").run(originalId).changes, 1);
    const second = await runIncremental(buffer, [sessionId]);
    assert.equal(second.result.ok, true, JSON.stringify(second.result));
    assert.equal(second.sent[0]?.totals.events, 3);
    compareExact(buffer, [sessionId], second.sent);
    return { spooled, maxIntakeMs: Math.max(...intakeMs), erasure,
      firstBodyEvents: 1, nextBodyEvents: second.sent[0]?.totals.events };
  } finally {
    external.close();
    buffer.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

async function runLeaseUpgradeCase() {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "p41-lease-upgrade-"));
  const ledgerPath = path.join(fixture, "ledger.sqlite");
  let buffer = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
  const sessionId = uuid(46_000);
  try {
    ensureSessionSummarySchema(buffer.database);
    // Recreate the 0.7.40 fences on a private fixture to prove the additive
    // schema upgrade removes the intake abort and replaces the dirty guards.
    buffer.database.exec(`
      drop trigger trg_session_sync_upload_lease_dirty_insert;
      drop trigger trg_session_sync_upload_lease_dirty_update;
      create trigger trg_session_sync_upload_lease_insert before insert on buffered_events
      when new.session_id is not null and exists (select 1 from session_sync_upload_leases
        where session_id = new.session_id and lease_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      begin select raise(abort, 'session_sync_upload_lease'); end;
      create trigger trg_session_sync_upload_lease_dirty_insert before insert on session_sync_summary_dirty
      when exists (select 1 from session_sync_upload_leases
        where session_id = new.session_id and lease_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      begin select raise(abort, 'session_sync_upload_lease'); end;
      create trigger trg_session_sync_upload_lease_dirty_update before update on session_sync_summary_dirty
      when exists (select 1 from session_sync_upload_leases
        where session_id = new.session_id and lease_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      begin select raise(abort, 'session_sync_upload_lease'); end;
    `);
    buffer.close();
    buffer = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
    assert.equal(buffer.database.prepare(
      "select 1 from sqlite_master where name = 'trg_session_sync_upload_lease_insert'",
    ).get(), undefined, "the old intake fence must be gone on the first ledger open");
    ensureSessionSummarySchema(buffer.database);
    assert.equal(buffer.database.prepare("select 1 from sqlite_master where name = 'trg_session_sync_upload_lease_insert'").get(), undefined);
    for (const name of ["trg_session_sync_upload_lease_dirty_insert", "trg_session_sync_upload_lease_dirty_update"]) {
      const trigger = buffer.database.prepare("select sql from sqlite_master where name = ?").get(name) as { sql: string };
      assert.ok(trigger.sql.includes("raw_insert_before_high_water"), name);
    }
    for (const [rowid, eventId] of [[100, uuid(46_001)], [200, uuid(46_002)]] as const) {
      insertRaw(buffer, { rowid, id: eventId, sessionId,
        observedAt: "2026-09-20T00:00:00.000Z", createdAt: "2026-09-20T00:00:00.000Z",
        inputTokens: 1, outputTokens: 1 });
    }
    let firstBody = "";
    const first = await runSessionSync(config, {
      ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until,
      delayMs: 0, maxAttemptsPerBatch: 1,
      fetchImpl: (async (_input, init) => {
        firstBody = String(init?.body ?? "");
        insertRaw(buffer, { rowid: 150, id: uuid(46_003), sessionId,
          observedAt: "2026-09-20T00:00:00.000Z", createdAt: "2026-09-20T00:00:00.000Z",
          inputTokens: 1, outputTokens: 1 });
        return new Response(JSON.stringify(acceptedFixtureDelivery(firstBody, installKey)), {
          status: 200, headers: { "content-type": "application/json" },
        });
      }) as typeof fetch,
      log: () => undefined,
    });
    assert.equal(JSON.parse(firstBody).sessions[0].totals.events, 2);
    assert.equal(first.ok, false, JSON.stringify(first));
    assert.ok(first.pendingSummarySessionIds.includes(sessionId), JSON.stringify(first));
    const next = await runIncremental(buffer, [sessionId]);
    assert.equal(next.result.ok, true, JSON.stringify(next.result));
    assert.equal(next.sent[0]?.totals.events, 3);
    compareExact(buffer, [sessionId], next.sent);
    return { firstBodyEvents: 2, nextBodyEvents: 3, migrated: true };
  } finally {
    buffer.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

async function runLeaseFutureAppendCase() {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "p41-lease-future-"));
  const buffer = new LocalEventBuffer(path.join(fixture, "ledger.sqlite"), { workspaceId: tenantId });
  const sessionId = uuid(47_000);
  const firstUntil = new Date(Date.now() - 1_000).toISOString();
  insertRaw(buffer, { id: uuid(47_001), sessionId,
    observedAt: "2026-09-20T00:00:00.000Z", createdAt: "2026-09-20T00:00:00.000Z",
    inputTokens: 1, outputTokens: 1 });
  let firstBody = "";
  let nextBody = "";
  try {
    const first = await runSessionSync(config, {
      ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until: firstUntil,
      delayMs: 0, maxAttemptsPerBatch: 1,
      fetchImpl: (async (_input, init) => {
        firstBody = String(init?.body ?? "");
        assert.equal(buffer.append(aiInteractionEventSchema.parse({
          id: uuid(47_002), sessionId, source: "codex",
          eventType: "assistant_response", observedAt: new Date().toISOString(),
          inputTokens: 2, outputTokens: 2,
        })), true);
        return new Response(JSON.stringify(acceptedFixtureDelivery(firstBody, installKey)), {
          status: 200, headers: { "content-type": "application/json" },
        });
      }) as typeof fetch, log: () => undefined,
    });
    assert.equal(first.ok, true, JSON.stringify(first));
    assert.equal(first.summaryComplete, true);
    assert.equal(JSON.parse(firstBody).sessions[0].totals.events, 1);
    const nextUntil = new Date(Date.now() + 1_000).toISOString();
    const nextIds = listLedgerSessionIds(buffer.database, { since: firstUntil, until: nextUntil });
    assert.ok(nextIds.includes(sessionId), JSON.stringify(nextIds));
    const next = await runSessionSync(config, {
      ledgerDb: buffer.database, incremental: true, sessionIds: nextIds, until: nextUntil,
      delayMs: 0, maxAttemptsPerBatch: 1,
      fetchImpl: (async (_input, init) => {
        nextBody = String(init?.body ?? "");
        return new Response(JSON.stringify(acceptedFixtureDelivery(nextBody, installKey)), {
          status: 200, headers: { "content-type": "application/json" },
        });
      }) as typeof fetch, log: () => undefined,
    });
    assert.equal(next.ok, true, JSON.stringify(next));
    assert.equal(JSON.parse(nextBody).sessions[0].totals.events, 2);
    return { firstBodyEvents: 1, nextBodyEvents: 2, nextPassFoundSession: true };
  } finally {
    buffer.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

async function runStaleBeforePostCase() {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "p41-fence-stale-"));
  const buffer = new LocalEventBuffer(path.join(fixture, "ledger.sqlite"), { workspaceId: tenantId });
  const targetSessionId = uuid(50_000);
  const eventId = uuid(50_001);
  insertRaw(buffer, {
    id: eventId,
    sessionId: targetSessionId,
    observedAt: "2026-09-20T00:00:00.000Z",
    createdAt: "2026-09-20T00:00:00.000Z",
    inputTokens: 1,
    outputTokens: 1,
  });
  let fetchCalls = 0;
  let mutateAtStart = true;
  try {
    const first = await runSessionSync(config, {
      ledgerDb: buffer.database,
      incremental: true,
      sessionIds: [targetSessionId],
      until,
      batchSize: 500,
      concurrency: 1,
      delayMs: 0,
      maxAttemptsPerBatch: 1,
      fetchImpl: (async () => {
        fetchCalls += 1;
        throw new Error("stale_snapshot_was_sent");
      }) as typeof fetch,
      sleep: async () => undefined,
      log: (line) => {
        if (mutateAtStart && line.includes('"status":"session_sync_start"')) {
          buffer.database.prepare("update buffered_events set output_tokens = output_tokens + 1 where id = ?").run(eventId);
          mutateAtStart = false;
        }
      },
    });
    assert.equal(fetchCalls, 0, JSON.stringify(first));
    assert.equal(first.sentSessions, 0, JSON.stringify(first));
    assert.equal(first.summaryComplete, false, JSON.stringify(first));
    assert.ok(first.pendingSummarySessionIds.includes(targetSessionId), JSON.stringify(first));
    const second = await runSessionSync(config, {
      ledgerDb: buffer.database,
      incremental: true,
      sessionIds: [targetSessionId],
      until,
      batchSize: 500,
      concurrency: 1,
      delayMs: 0,
      maxAttemptsPerBatch: 1,
      fetchImpl: (async (_input, init) => {
        fetchCalls += 1;
        const wire = String(init?.body ?? "");
        return new Response(JSON.stringify(acceptedFixtureDelivery(wire, installKey)), {
          status: 200, headers: { "content-type": "application/json" },
        });
      }) as typeof fetch,
      sleep: async () => undefined,
      log: () => undefined,
    });
    assert.equal(second.ok, true, JSON.stringify(second));
    assert.equal(second.sentSessions, 1, JSON.stringify(second));
    return { fetchCalls, firstSent: first.sentSessions, retriedSent: second.sentSessions, pending: first.pendingSummarySessionIds };
  } finally {
    buffer.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

function compareExact(buffer: LocalEventBuffer, sessionIds: string[], sent: unknown[]) {
  assert.deepEqual(
    sent.slice().sort((a, b) =>
      (a as { session: { id: string } }).session.id.localeCompare(
        (b as { session: { id: string } }).session.id,
      )),
    expectedSessions(buffer, sessionIds),
  );
}

async function runFenceContentionCase(point: "before" | "after" | "stale") {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "p40-fence-contention-"));
  const buffer = new LocalEventBuffer(path.join(fixture, "ledger.sqlite"), { workspaceId: tenantId });
  const writer = new Database(buffer.database.name, { fileMustExist: true, timeout: 0 });
  buffer.database.pragma("busy_timeout = 0");
  const sessionId = uuid(60_000);
  const eventId = uuid(60_001);
  insertRaw(buffer, {
    id: eventId, sessionId, observedAt: "2026-09-20T00:00:00.000Z",
    createdAt: "2026-09-20T00:00:00.000Z", inputTokens: 1, outputTokens: 1,
  });
  let fetches = 0;
  let waits = 0;
  try {
    const result = await runSessionSync(config, {
      ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until,
      delayMs: 0, maxAttemptsPerBatch: 1,
      log: (line) => {
        if (point !== "after" && line.includes('"status":"session_sync_start"')) {
          writer.exec("begin immediate");
        }
      },
      sleep: async () => {
        waits += 1;
        assert.equal(writer.inTransaction, true);
        if (point === "stale") {
          writer.prepare("update buffered_events set output_tokens = 42 where id = ?").run(eventId);
        }
        writer.exec("commit");
      },
      fetchImpl: (async (_url, init) => {
        fetches += 1;
        assert.equal(buffer.database.inTransaction, false, "network send held the daemon transaction");
        const wire = String(init?.body ?? "");
        if (point === "after") writer.exec("begin immediate");
        return new Response(JSON.stringify(acceptedFixtureDelivery(wire, installKey)), {
          status: 200, headers: { "content-type": "application/json" },
        });
      }) as typeof fetch,
    });
    assert.equal(waits, 1, JSON.stringify({ point, waits, result }));
    assert.equal(result.ok, point !== "stale", JSON.stringify(result));
    assert.equal(fetches, point === "stale" ? 0 : 1);
    assert.equal(result.summaryComplete, point !== "stale");
    const leases = buffer.database.prepare("select count(*) as n from session_sync_upload_leases").get() as { n: number };
    assert.equal(leases.n, 0);
    if (point === "stale") {
      const retried = await runIncremental(buffer, [sessionId]);
      assert.equal(retried.result.sentSessions, 1);
      compareExact(buffer, [sessionId], retried.sent);
    }
    // A process crash cannot leave a permanent mutation fence.
    writer.prepare(`insert into session_sync_upload_leases
      (session_id, lease_token, lease_expires_at, mutation_revision, high_water)
      values (?, 'expired-process', '2000-01-01T00:00:00.000Z', 0, 0)`).run(sessionId);
    assert.equal(writer.prepare("delete from buffered_events where id = ?").run(eventId).changes, 1);
    return { point, waits, fetches, ok: result.ok, summaryComplete: result.summaryComplete };
  } finally {
    if (writer.inTransaction) writer.exec("rollback");
    writer.close();
    buffer.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

async function reviewRegressions() {
  const selected = process.env.PROBE_CASE;
  const cases = [
    "first_read_insert", "fallback_checkpoint", "missing_dirty_marker", "privacy_before_send",
    "unrelated_revision", "retry_erasure", "hard_bounds", "session_id_paging",
    "privacy_lineage_first_read", "checkpoint_timeout", "future_horizon",
    "interleaved_initial_insert", "trigger_upgrade", "privacy_handoff_erasure",
    "backdated_queue_planner", "stale_send_fast_retry",
    "pending_id_bounds", "sync_id_deadline", "same_time_seek",
    "fence_multibatch", "fence_erasure", "lease_safe_intake", "lease_upgrade", "lease_future_append", "fence_stale_before_post", "fence_contention",
    "summary_busy_state", "summary_busy_stable", "summary_busy_daemon", "summary_busy_budget",
  ];
  for (const name of cases) {
    if (selected && selected !== name) continue;
    const fixture = fs.mkdtempSync(path.join(os.tmpdir(), `p41-${name}-`));
    const buffer = new LocalEventBuffer(path.join(fixture, "ledger.sqlite"), { workspaceId: tenantId });
    const sessionId = `dddddddd-dddd-4ddd-8ddd-${String(cases.indexOf(name) + 1).padStart(12, "0")}`;
    const add = (index: number) => insertRaw(buffer, {
      id: uuid(400 + cases.indexOf(name) * 10 + index), sessionId,
      observedAt: `2026-09-20T02:${String(index).padStart(2, "0")}:00.000Z`,
      createdAt: `2026-09-20T02:${String(index).padStart(2, "0")}:00.000Z`,
      inputTokens: 1, outputTokens: 1, costUsd: 0.01,
    });
    try {
      ensureSessionSummarySchema(buffer.database);
      const directRead = async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> =>
        queries.flatMap((query) => buffer.database.prepare(query.sql).all(query.params) as T[]);
      if (name === "first_read_insert") {
        add(1);
        let injected = false;
        const read = async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> => {
          const rows = await directRead<T>(queries);
          if (!injected && queries.some((query) => /order by (?:e\.observed_at, e\.rowid|scan\.sort_observed_at, scan\.raw_rowid) asc/.test(query.sql))) {
            injected = true;
            add(2);
          }
          return rows;
        };
        await updateSessionSummary(buffer.database, sessionId, until, { read });
        const resumed = await updateSessionSummary(buffer.database, sessionId, until, { read });
        assert.equal(injected, true);
        assert.equal(resumed.complete, true);
        assert.deepEqual(resumed.snapshot, collectSessionSnapshots(buffer.database, { until, sessionIds: [sessionId] })[0]);
      } else if (name === "fallback_checkpoint") {
        for (let index = 1; index <= 4; index += 1) add(index);
        await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        buffer.database.prepare("update buffered_events set input_tokens = 99 where id = ?").run(uuid(411));
        const partial = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead, maxRows: 1 });
        assert.equal(partial.complete, false);
        buffer.database.prepare("update session_sync_summary_state set high_water = high_water + 100 where session_id = ?").run(sessionId);
        const resumed = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        assert.equal(resumed.complete, true);
        assert.deepEqual(resumed.snapshot, collectSessionSnapshots(buffer.database, { until, sessionIds: [sessionId] })[0]);
      } else if (name === "missing_dirty_marker") {
        add(1);
        await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        buffer.database.prepare("update buffered_events set input_tokens = 77 where id = ?").run(uuid(421));
        buffer.database.prepare("delete from session_sync_summary_dirty where session_id = ?").run(sessionId);
        const plan = planDaemonSessionSync({
          db: buffer.database,
          state: { ...emptyDaemonSessionSyncState(), caughtUp: true, lastSuccessfulUntil: "2026-09-21T00:00:00.000Z" },
          uploadedBatches: [], until, ledgerSessionIds: [],
        });
        assert.equal(plan.skip, false);
        assert.ok(plan.sessionIds?.includes(sessionId));
      } else if (name === "privacy_before_send") {
        add(1);
        let sent = "";
        let erased = false;
        const result = await runSessionSync(config, {
          ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until,
          delayMs: 0, maxAttemptsPerBatch: 1,
          fetchImpl: (async (_input, init) => {
            sent = String(init?.body ?? "");
            return new Response(JSON.stringify(acceptedFixtureDelivery(sent, installKey)), {
              status: 200, headers: { "content-type": "application/json" },
            });
          }) as typeof fetch,
          log: (line) => {
            if (!erased && line.includes('"status":"session_sync_start"')) {
              buffer.database.prepare("delete from buffered_events where id = ?").run(uuid(431));
              erased = true;
            }
          },
        });
        assert.equal(erased, true);
        assert.equal(sent, "");
        assert.equal(result.summaryComplete, false);
        assert.ok(result.pendingSummarySessionIds.includes(sessionId));
      } else if (name === "unrelated_revision") {
        for (let index = 1; index <= 8; index += 1) add(index);
        await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        buffer.database.prepare("update buffered_events set output_tokens = 42 where id = ?").run(uuid(441));
        const first = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead, maxRows: 1 });
        assert.equal(first.complete, false);
        const unrelatedId = "eeeeeeee-eeee-4eee-8eee-000000000001";
        insertRaw(buffer, {
          id: uuid(470), sessionId: unrelatedId,
          observedAt: "2026-09-20T03:00:00.000Z", createdAt: "2026-09-20T03:00:00.000Z",
          inputTokens: 1, outputTokens: 1,
        });
        buffer.database.prepare("update buffered_events set output_tokens = 43 where id = ?").run(uuid(470));
        const second = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead, maxRows: 1 });
        assert.ok(second.highWater > first.highWater, JSON.stringify({ first, second }));
      } else if (name === "retry_erasure") {
        add(1);
        await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        let calls = 0;
        let result: Awaited<ReturnType<typeof runSessionSync>> | undefined;
        for (let attempt = 0; attempt < 20 && calls === 0; attempt += 1) {
          result = await runSessionSync(config, {
            ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until,
            delayMs: 0, maxAttemptsPerBatch: 2,
            fetchImpl: (async () => {
              calls += 1;
              return new Response("{}", { status: 503 });
            }) as typeof fetch,
            sleep: async () => {
              buffer.database.prepare("delete from buffered_events where id = ?").run(uuid(451));
            },
            log: () => undefined,
          });
        }
        assert.equal(calls, 1);
        assert.ok(result);
        assert.equal(result.sentSessions, 0);
        assert.equal(result.summaryComplete, false);
        assert.ok(result.pendingSummarySessionIds.includes(sessionId));
      } else if (name === "hard_bounds") {
        for (let index = 1; index <= 5_200; index += 1) {
          insertRaw(buffer, {
            id: uuid(10_000 + index), sessionId,
            observedAt: "2026-09-20T00:00:00.000Z", createdAt: "2026-09-20T00:00:00.000Z",
            inputTokens: 1, outputTokens: 1,
          });
        }
        const result = await updateSessionSummary(buffer.database, sessionId, until, {
          read: directRead, maxRows: 100_000, maxMs: 5_000,
        });
        assert.ok(result.rowsRead <= 5_000, JSON.stringify(result));
        assert.equal(result.complete, false);
      } else if (name === "session_id_paging") {
        for (let index = 1; index <= 5_001; index += 1) {
          insertRaw(buffer, {
            id: uuid(20_000 + index),
            sessionId: `eeeeeeee-eeee-4eee-8eee-${String(index).padStart(12, "0")}`,
            observedAt: "2026-09-20T00:00:00.000Z", createdAt: "2026-09-20T00:00:00.000Z",
            inputTokens: 1, outputTokens: 1,
          });
        }
        const ids = await listLedgerSessionIdsOffThread(buffer.database, {
          until, maxIds: Number.POSITIVE_INFINITY,
        });
        assert.equal(ids.length, 5_001);
        const initialIds = await listLedgerSessionIdsOffThread(buffer.database, {
          until, maxIds: Number.POSITIVE_INFINITY, allSessions: true,
        });
        assert.deepEqual(initialIds, ids);
      } else if (name === "privacy_lineage_first_read") {
        add(1);
        let changed = false;
        const read = async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> => {
          const rows = await directRead<T>(queries);
          if (!changed && queries.some((query) => /order by (?:e\.observed_at, e\.rowid|scan\.sort_observed_at, scan\.raw_rowid) asc/.test(query.sql))) {
            changed = true;
            buffer.database.prepare(`insert into upload_receipts
              (delivery_id, terminal_state, reason, status_class, attempt_count, created_at, terminal_at)
              values (?, 'dead', 'local_privacy_violation', 'local', 0, ?, ?)`).run(
              uuid(481), "2026-09-20T02:00:00.000Z", "2026-09-20T02:00:00.000Z",
            );
          }
          return rows;
        };
        const first = await updateSessionSummary(buffer.database, sessionId, until, { read });
        assert.equal(changed, true);
        assert.equal(first.complete, false);
        const next = await updateSessionSummary(buffer.database, sessionId, until, { read });
        assert.equal(next.complete, true);
        assert.equal(next.snapshot, null);
      } else if (name === "checkpoint_timeout") {
        for (let index = 1; index <= 4; index += 1) add(index);
        const first = await updateSessionSummary(buffer.database, sessionId, until, {
          read: directRead, maxRows: 1,
        });
        assert.equal(first.complete, false);
        const deferred = await updateSessionSummary(buffer.database, sessionId, until, {
          read: async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> => {
            if (queries.some((query) => query.sql.includes("from buffered_events where rowid = @rowid"))) {
              throw new Error("session_summary_read_interrupted");
            }
            return directRead<T>(queries);
          },
        });
        assert.equal(deferred.complete, false);
        assert.equal(deferred.fullRecompute, false);
        assert.equal(deferred.highWater, first.highWater);
        const resumed = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        assert.equal(resumed.complete, true);
        assert.deepEqual(resumed.snapshot, collectSessionSnapshots(buffer.database, { until, sessionIds: [sessionId] })[0]);
      } else if (name === "summary_busy_state" || name === "summary_busy_stable") {
        add(1);
        buffer.database.pragma("busy_timeout = 0");
        const writer = new Database(buffer.database.name, { fileMustExist: true, timeout: 0 });
        let release: ReturnType<typeof setTimeout> | undefined;
        let lockedAtScan = false;
        const unlock = () => { if (writer.inTransaction) writer.exec("commit"); };
        try {
          if (name === "summary_busy_state") {
            writer.exec("begin immediate");
            release = setTimeout(unlock, 90);
          }
          const read = async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> => {
            const rows = await directRead<T>(queries);
            if (name === "summary_busy_stable" && !lockedAtScan &&
                queries.some((query) => /order by (?:e\.observed_at, e\.rowid|scan\.sort_observed_at, scan\.raw_rowid) asc/.test(query.sql))) {
              lockedAtScan = true;
              writer.exec("begin immediate");
              release = setTimeout(unlock, 90);
            }
            return rows;
          };
          const result = await updateSessionSummary(buffer.database, sessionId, until, { read });
          assert.equal(result.complete, true);
          assert.deepEqual(result.snapshot, collectSessionSnapshots(buffer.database, {
            until, sessionIds: [sessionId],
          })[0]);
          if (name === "summary_busy_stable") assert.equal(lockedAtScan, true);
          console.log(JSON.stringify({ reviewCase: name, result: "PASS" }));
        } finally {
          if (release) clearTimeout(release);
          if (writer.inTransaction) writer.exec("rollback");
          writer.close();
        }
      } else if (name === "summary_busy_daemon" || name === "summary_busy_budget") {
        add(1);
        buffer.database.pragma("busy_timeout = 0");
        const writer = new Database(buffer.database.name, { fileMustExist: true, timeout: 0 });
        const bodies: string[] = [];
        const sync = () => runSessionSync(config, {
          ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until,
          delayMs: 0, maxAttemptsPerBatch: 1,
          fetchImpl: (async (_url, init) => {
            const body = String(init?.body ?? "");
            bodies.push(body);
            return new Response(JSON.stringify({
              ...acceptedFixtureDelivery(body, installKey), inserted: 1, updated: 0, skippedStale: 0,
            }), { status: 200, headers: { "content-type": "application/json" } });
          }) as typeof fetch,
          log: () => undefined,
        });
        try {
          writer.exec("begin immediate");
          if (name === "summary_busy_daemon") {
            const bursts = (async () => {
              for (let index = 0; index < 4; index += 1) {
                await new Promise<void>((resolve) => setTimeout(resolve, 35));
                if (writer.inTransaction) writer.exec("commit");
                if (index < 3) {
                  await new Promise<void>((resolve) => setTimeout(resolve, 5));
                  try { writer.exec("begin immediate"); }
                  catch (error) { if (!isSqliteContentionError(error)) throw error; }
                }
              }
            })();
            let result: Awaited<ReturnType<typeof sync>>;
            try { result = await sync(); } finally { await bursts; }
            assert.equal(result.ok, true);
            assert.equal(result.summaryComplete, true);
            assert.equal(result.sentSessions, 1);
            assert.equal(bodies.length, 1);
          } else {
            let failure: unknown;
            try { await sync(); } catch (error) { failure = error; }
            assert.ok(failure instanceof SyncStorageBusyError, String(failure));
            assert.equal(bodies.length, 0, "no body may leave while the summary is blocked");
            assert.equal((buffer.database.prepare("select count(*) as count from buffered_events where session_id = ?")
              .get(sessionId) as { count: number }).count, 1);
            writer.exec("commit");
            const result = await sync();
            assert.equal(result.ok, true);
            assert.equal(result.sentSessions, 1);
            assert.equal(bodies.length, 1);
          }
          console.log(JSON.stringify({ reviewCase: name, result: "PASS", sent: bodies.length }));
        } finally {
          if (writer.inTransaction) writer.exec("rollback");
          writer.close();
        }
      } else if (name === "future_horizon") {
        insertRaw(buffer, {
          id: uuid(501), sessionId,
          observedAt: "2026-09-19T00:00:00.000Z", createdAt: "2026-10-02T00:00:00.000Z",
          inputTokens: 10, outputTokens: 1,
        });
        add(2);
        const first = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        assert.equal(first.snapshot?.events, 1);
        const later = "2026-10-10T00:00:00.000Z";
        const second = await updateSessionSummary(buffer.database, sessionId, later, { read: directRead });
        assert.equal(second.fullRecompute, true);
        assert.deepEqual(second.snapshot, collectSessionSnapshots(buffer.database, {
          until: later, sessionIds: [sessionId],
        })[0]);
      } else if (name === "interleaved_initial_insert") {
        add(5);
        add(10);
        const first = await updateSessionSummary(buffer.database, sessionId, until, {
          read: directRead, maxRows: 1,
        });
        assert.equal(first.complete, false);
        add(1);
        add(20);
        await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        const final = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        assert.equal(final.complete, true);
        assert.deepEqual(final.snapshot, collectSessionSnapshots(buffer.database, {
          until, sessionIds: [sessionId],
        })[0]);
      } else if (name === "trigger_upgrade") {
        buffer.database.exec(`
          drop trigger trg_session_summary_raw_insert;
          create trigger trg_session_summary_raw_insert
          after insert on buffered_events
          when new.session_id is not null and exists (
            select 1 from session_sync_summary_state where session_id = new.session_id
          )
          begin
            insert or ignore into session_sync_summary_rows (raw_rowid, session_id, created_at)
              values (new.rowid, new.session_id, new.created_at);
          end;
        `);
        ensureSessionSummarySchema(buffer.database);
        const sparse = (index: number, rowid: number) => insertRaw(buffer, {
          rowid, id: uuid(520 + index), sessionId,
          observedAt: `2026-09-20T02:${String(index).padStart(2, "0")}:00.000Z`,
          createdAt: "2026-09-20T02:00:00.000Z", inputTokens: 1, outputTokens: 1,
        });
        sparse(5, 100);
        sparse(10, 200);
        const first = await updateSessionSummary(buffer.database, sessionId, until, {
          read: directRead, maxRows: 1,
        });
        assert.equal(first.highWater, 100);
        sparse(1, 150);
        const resumed = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        assert.equal(resumed.fullRecompute, true);
        assert.deepEqual(resumed.snapshot, collectSessionSnapshots(buffer.database, {
          until, sessionIds: [sessionId],
        })[0]);
      } else if (name === "privacy_handoff_erasure") {
        add(1);
        buffer.database.pragma("busy_timeout = 0");
        let transmitted: string | null = null;
        const result = await runSessionSync(config, {
          ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until,
          delayMs: 0, maxAttemptsPerBatch: 1,
          fetchImpl: (async (_url, init) => {
            const body = String(init?.body ?? "");
            // This is the original review interleaving: erasure attempts to
            // commit after body access but before the transport records send.
            buffer.database.prepare("delete from buffered_events where id = ?").run(uuid(531));
            transmitted = body;
            return new Response(JSON.stringify(acceptedFixtureDelivery(body, installKey)), {
              status: 200, headers: { "content-type": "application/json" },
            });
          }) as typeof fetch,
          log: () => undefined,
        });
        assert.equal(transmitted, null, "a body crossed the handoff after erasure");
        assert.equal(result.sentSessions, 0);
        const external = new Database(buffer.database.name, { fileMustExist: true, timeout: 0 });
        try {
          let externalErasureBlocked = false;
          let fullAtHandoff = 0;
          const sent = await runSessionSync(config, {
            ledgerDb: buffer.database, incremental: true, sessionIds: [sessionId], until,
            delayMs: 0, maxAttemptsPerBatch: 1,
            fetchImpl: (async (_url, init) => {
              const body = String(init?.body ?? "");
              try {
                external.prepare("delete from buffered_events where id = ?").run(uuid(531));
              } catch (error) {
                externalErasureBlocked = isSqliteContentionError(error);
              }
              fullAtHandoff = collectSessionSnapshots(buffer.database, { until, sessionIds: [sessionId] })[0]?.events ?? 0;
              return new Response(JSON.stringify(acceptedFixtureDelivery(body, installKey)), {
                status: 200, headers: { "content-type": "application/json" },
              });
            }) as typeof fetch,
            log: () => undefined,
          });
          assert.equal(externalErasureBlocked, true);
          assert.equal(fullAtHandoff, 1);
          assert.equal(sent.sentSessions, 1);
          external.prepare("delete from buffered_events where id = ?").run(uuid(531));
          assert.deepEqual(collectSessionSnapshots(buffer.database, { until, sessionIds: [sessionId] }), []);
        } finally {
          external.close();
        }
      } else if (name === "backdated_queue_planner") {
        add(1);
        const first = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        assert.equal(first.snapshot?.events, 1);
        add(2);
        const later = "2026-10-01T23:59:59.000Z";
        const discovered = await listLedgerSessionIdsOffThread(buffer.database, {
          since: until, until: later,
        });
        assert.deepEqual(discovered, []);
        const plan = planDaemonSessionSync({
          db: buffer.database,
          state: { ...emptyDaemonSessionSyncState(), caughtUp: true, lastSuccessfulUntil: until },
          uploadedBatches: [], until: later, ledgerSessionIds: discovered,
        });
        assert.equal(plan.skip, false);
        assert.ok(plan.sessionIds?.includes(sessionId));
        const updated = await updateSessionSummary(buffer.database, sessionId, later, { read: directRead });
        assert.equal(updated.snapshot?.events, 2);
        assert.deepEqual(updated.snapshot, collectSessionSnapshots(buffer.database, {
          until: later, sessionIds: [sessionId],
        })[0]);
      } else if (name === "stale_send_fast_retry") {
        add(1);
        const db = buffer.database;
        const originalPrepare = db.prepare.bind(db);
        let stateReads = 0;
        let fetchCalls = 0;
        (db as typeof db & { prepare: typeof db.prepare }).prepare = ((sql: string) => {
          if (sql.includes("from session_sync_summary_state where session_id = ?") && ++stateReads === 3) add(2);
          return originalPrepare(sql);
        }) as typeof db.prepare;
        let result: Awaited<ReturnType<typeof runSessionSync>>;
        try {
          result = await runSessionSync(config, {
            ledgerDb: db, incremental: true, sessionIds: [sessionId], until,
            delayMs: 0, maxAttemptsPerBatch: 1,
            fetchImpl: (async () => { fetchCalls += 1; throw new Error("unexpected_fetch"); }) as typeof fetch,
            log: () => undefined,
          });
        } finally {
          (db as typeof db & { prepare: typeof db.prepare }).prepare = originalPrepare;
        }
        assert.ok(stateReads >= 3);
        assert.equal(fetchCalls, 0);
        assert.equal(result.ok, false);
        assert.ok(result.pendingSummarySessionIds.includes(sessionId));
        const cliSource = fs.readFileSync(path.join(process.cwd(), "packages/collector-cli/src/cli.ts"), "utf8");
        const pending = cliSource.indexOf("const summaryPending = sessionResult.pendingSummarySessionIds;");
        const branch = cliSource.indexOf("if (sessionResult.ok", pending);
        assert.ok(pending >= 0 && branch > pending);
        assert.match(cliSource.slice(pending, branch), /summaryCatchUp = summaryPending\.length > 0/);
      } else if (name === "pending_id_bounds") {
        const insert = buffer.database.prepare(
          "insert into session_sync_summary_dirty (session_id, reason, updated_at) values (?, 'fixture', ?)",
        );
        buffer.database.transaction(() => {
          for (let index = 0; index < 8_050; index += 1) {
            insert.run(`eeeeeeee-eeee-4eee-8eee-${String(index).padStart(12, "0")}`, until);
          }
        })();
        let bounded = false;
        try {
          bounded = listSessionSummaryPendingIds(buffer.database, until, 8_000).length <= 8_000;
        } catch (error) {
          bounded = error instanceof Error && error.message.includes("bounded_sql_read");
        }
        assert.equal(bounded, true);
        const plan = planDaemonSessionSync({
          db: buffer.database,
          state: { ...emptyDaemonSessionSyncState(), caughtUp: true, lastSuccessfulUntil: until },
          uploadedBatches: [], until, ledgerSessionIds: [],
        });
        assert.equal(plan.reason, "full_catchup");
      } else if (name === "sync_id_deadline") {
        for (let index = 0; index < 180; index += 1) {
          insertRaw(buffer, {
            id: uuid(50_000 + index),
            sessionId: `ffffffff-ffff-4fff-8fff-${String(index).padStart(12, "0")}`,
            observedAt: "2026-09-20T02:00:00.000Z",
            createdAt: "2026-09-20T02:00:00.000Z",
            inputTokens: 1, outputTokens: 1,
          });
        }
        buffer.database.function("slow_session_id_probe", () => {
          const end = performance.now() + 2;
          while (performance.now() < end) { /* simulate a costly SQLite row */ }
          return 1;
        });
        const db = buffer.database;
        const originalPrepare = db.prepare.bind(db);
        (db as typeof db & { prepare: typeof db.prepare }).prepare = ((sql: string) =>
          originalPrepare(sql.includes("select distinct e.session_id as sessionId")
            ? sql.replace(" order by e.session_id asc", " and slow_session_id_probe() order by e.session_id asc")
            : sql)) as typeof db.prepare;
        let deadlineRaised = false;
        try {
          listLedgerSessionIds(db, { until });
        } catch (error) {
          deadlineRaised = error instanceof Error && error.message.includes("bounded_sql_read_deadline");
        } finally {
          (db as typeof db & { prepare: typeof db.prepare }).prepare = originalPrepare;
        }
        assert.equal(deadlineRaised, true);
      } else if (name === "same_time_seek") {
        for (let index = 1; index <= 3; index += 1) {
          insertRaw(buffer, {
            id: uuid(60_000 + index), sessionId,
            observedAt: "2026-09-20T04:00:00.000Z",
            createdAt: "2026-09-20T04:00:00.000Z",
            inputTokens: 1, outputTokens: 1,
          });
        }
        const first = await updateSessionSummary(buffer.database, sessionId, until, {
          read: directRead, maxRows: 1,
        });
        assert.equal(first.complete, false);
        const plans: string[] = [];
        const plannedRead = async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> => {
          for (const query of queries) {
            if (query.sql.includes("@cursorObservedAt")) {
              const explain = buffer.database.prepare(`explain query plan ${query.sql}`)
                .all(query.params) as Array<{ detail: string }>;
              plans.push(...explain.map((row) => row.detail));
            }
          }
          return directRead<T>(queries);
        };
        const second = await updateSessionSummary(buffer.database, sessionId, until, {
          read: plannedRead, maxRows: 1,
        });
        assert.ok(second.highWater > first.highWater);
        assert.ok(plans.some((detail) => /observed_at=\? AND rowid>\?/.test(detail)),
          `same-timestamp continuation did not seek by rowid: ${plans.join("; ")}`);
        const complete = await updateSessionSummary(buffer.database, sessionId, until, { read: directRead });
        assert.deepEqual(complete.snapshot, collectSessionSnapshots(buffer.database, {
          until, sessionIds: [sessionId],
        })[0]);
      } else if (name === "fence_multibatch") {
        buffer.close();
        const results = [];
        const failures: string[] = [];
        for (const rttMs of [300, 1_200]) {
          try { results.push(await runFenceMultiBatchCase(rttMs)); }
          catch (error) { failures.push(`${rttMs}: ${String(error)}`); }
        }
        assert.deepEqual(failures, []);
        console.log(JSON.stringify({ reviewCase: name, result: "PASS", results }));
        continue;
      } else if (name === "fence_erasure") {
        buffer.close();
        const result = [];
        for (const kind of ["delete", "privacy", "receipt"] as const) {
          result.push(await runFenceErasureCase(kind));
        }
        console.log(JSON.stringify({ reviewCase: name, result: "PASS", detail: result }));
        continue;
      } else if (name === "lease_safe_intake") {
        buffer.close();
        const result = await runLeaseSafeIntakeCase();
        console.log(JSON.stringify({ reviewCase: name, result: "PASS", detail: result }));
        continue;
      } else if (name === "lease_upgrade") {
        buffer.close();
        const result = await runLeaseUpgradeCase();
        console.log(JSON.stringify({ reviewCase: name, result: "PASS", detail: result }));
        continue;
      } else if (name === "lease_future_append") {
        buffer.close();
        const result = await runLeaseFutureAppendCase();
        console.log(JSON.stringify({ reviewCase: name, result: "PASS", detail: result }));
        continue;
      } else if (name === "fence_stale_before_post") {
        buffer.close();
        const result = await runStaleBeforePostCase();
        console.log(JSON.stringify({ reviewCase: name, result: "PASS", detail: result }));
        continue;
      } else if (name === "fence_contention") {
        buffer.close();
        const results = [];
        for (const point of ["before", "after", "stale"] as const) {
          results.push(await runFenceContentionCase(point));
        }
        console.log(JSON.stringify({ reviewCase: name, result: "PASS", results }));
        continue;
      }
      console.log(JSON.stringify({ reviewCase: name, result: "PASS" }));
    } finally {
      buffer.close();
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  }
}

async function main() {
  if (!fs.existsSync(path.join(process.cwd(), "packages/collector-cli/src/session-summary.ts"))) {
    console.error(JSON.stringify({ result: "RED", reason: "incremental_summary_module_missing" }));
    if (expect === "red") return;
    throw new Error("incremental_summary_module_missing");
  }

  const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
  try {
    ensureSessionSummarySchema(buffer.database);
    for (let index = 0; index < 6; index += 1) {
      insertRaw(buffer, {
        id: uuid(index + 1), sessionId: sessionA,
        observedAt: `2026-09-20T00:0${index}:00.000Z`,
        createdAt: `2026-09-20T00:0${index}:00.000Z`,
        inputTokens: 10 + index, outputTokens: 2,
        costUsd: index % 2 === 0 ? 0.01 * (index + 1) : null,
        repoHash: hashA, branchHash: hashA, accountHash: hashA,
      });
    }
    for (let index = 0; index < 3; index += 1) {
      insertRaw(buffer, {
        id: uuid(index + 20), sessionId: sessionB,
        observedAt: `2026-09-20T01:0${index}:00.000Z`,
        createdAt: `2026-09-20T01:0${index}:00.000Z`,
        inputTokens: 4, outputTokens: 1, costUsd: 0.02,
        repoHash: hashB, branchHash: hashB, accountHash: hashB,
      });
    }

    const first = await runIncremental(buffer, [sessionA, sessionB]);
    assert.equal(first.result.ok, true);
    assert.equal(first.result.summaryComplete, true);
    compareExact(buffer, [sessionA, sessionB], first.sent);
    assert.equal(first.result.summaryStats.fullRecomputes, 0);

    // Late/out-of-order observation and a new row with an older created_at
    // both receive a higher rowid and are therefore visible past the HWM.
    insertRaw(buffer, {
      id: uuid(40), sessionId: sessionA,
      observedAt: "2026-09-19T23:59:00.000Z",
      createdAt: "2026-09-19T23:59:00.000Z",
      inputTokens: 99, outputTokens: 7, costUsd: 0.09,
      repoHash: hashA, branchHash: hashA, accountHash: hashA,
    });
    insertRaw(buffer, {
      id: uuid(41), sessionId: sessionB,
      observedAt: "2026-09-19T22:59:00.000Z",
      createdAt: "2026-09-19T22:59:00.000Z",
      inputTokens: 3, outputTokens: 8, costUsd: null,
      repoHash: hashB, branchHash: hashB, accountHash: hashB,
    });
    const appended = await runIncremental(buffer, [sessionA, sessionB]);
    assert.equal(appended.result.summaryComplete, true);
    assert.equal(appended.result.summaryStats.fullRecomputes, 0);
    assert.ok(
      appended.result.summaryStats.rowsRead >= 2,
      JSON.stringify(appended.result.summaryStats),
    );
    compareExact(buffer, [sessionA, sessionB], appended.sent);

    const oldA = uuid(1);
    buffer.database.prepare("update buffered_events set input_tokens = 777 where id = ?").run(oldA);
    const edited = await runIncremental(buffer, [sessionA, sessionB]);
    assert.equal(edited.result.summaryComplete, true);
    assert.ok(edited.result.summaryStats.fullRecomputes >= 1);
    compareExact(buffer, [sessionA, sessionB], edited.sent);

    buffer.database.prepare("delete from buffered_events where id = ?").run(uuid(2));
    const deleted = await runIncremental(buffer, [sessionA, sessionB]);
    assert.equal(deleted.result.summaryComplete, true);
    assert.ok(deleted.result.summaryStats.fullRecomputes >= 1);
    compareExact(buffer, [sessionA, sessionB], deleted.sent);

    const privacyRow = buffer.database.prepare(
      "select rowid from buffered_events where id = ?",
    ).get(uuid(20)) as { rowid: number };
    assert.equal(markRawPrivacyDisposition(
      buffer.database, privacyRow.rowid, "local_privacy_violation", new Date().toISOString(),
    ), 1);
    const privacy = await runIncremental(buffer, [sessionA, sessionB]);
    assert.equal(privacy.result.summaryComplete, true);
    assert.ok(privacy.result.summaryStats.fullRecomputes >= 1);
    compareExact(buffer, [sessionA, sessionB], privacy.sent);

    // A bounded pass persists its HWM before the process is restarted.
    for (let index = 0; index < 14; index += 1) {
      insertRaw(buffer, {
        id: uuid(100 + index), sessionId: sessionC,
        observedAt: `2026-09-21T00:${String(index).padStart(2, "0")}:00.000Z`,
        createdAt: `2026-09-21T00:${String(index).padStart(2, "0")}:00.000Z`,
        inputTokens: 1, outputTokens: 1, costUsd: 0.001,
      });
    }
    const partial = await updateSessionSummary(buffer.database, sessionC, until, {
      maxRows: 3,
      maxMs: 500,
      read: (queries) => readLedgerOffThread(buffer.database, queries),
    });
    assert.equal(partial.complete, false);
    const savedHwm = buffer.database.prepare(
      "select high_water as highWater, complete from session_sync_summary_state where session_id = ?",
    ).get(sessionC) as { highWater: number; complete: number };
    assert.ok(savedHwm.highWater > 0 && savedHwm.complete === 0);
    buffer.close();

    const restarted = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
    try {
      const resumed = await runIncremental(restarted, [sessionC]);
      assert.equal(resumed.result.summaryComplete, true);
      compareExact(restarted, [sessionC], resumed.sent);

      // HWM corruption is detected from the durable checkpoint, not trusted.
      restarted.database.prepare(
        "update session_sync_summary_state set high_water = high_water + 100 where session_id = ?",
      ).run(sessionC);
      const hwmBroken = await runIncremental(restarted, [sessionC]);
      assert.equal(hwmBroken.result.summaryComplete, true);
      assert.ok(hwmBroken.result.summaryStats.fullRecomputes >= 1);
      compareExact(restarted, [sessionC], hwmBroken.sent);

      // Mutation-probe: delete the dirty marker after an edit. The global
      // mutation revision still forces a safe fallback instead of a wrong
      // incremental result.
      restarted.database.prepare("update buffered_events set output_tokens = 88 where id = ?").run(uuid(100));
      restarted.database.prepare("delete from session_sync_summary_dirty where session_id = ?").run(sessionC);
      const skippedFallback = await runIncremental(restarted, [sessionC]);
      assert.equal(skippedFallback.result.summaryComplete, true);
      assert.ok(skippedFallback.result.summaryStats.fullRecomputes >= 1);
      compareExact(restarted, [sessionC], skippedFallback.sent);

      const counters = sessionSummaryCounters(restarted.database);
      assert.ok(counters.fallbackRecomputes >= 5);

      // Optional real downgrade lane: a v0.7.37 LocalEventBuffer appends a
      // late row while the summary state is present, then the current code
      // resumes and must match a full recompute.
      const downgradeRoot = process.env.PLIMSOLL_0737_ROOT;
      if (downgradeRoot) {
        restarted.close();
        const legacy = await import(pathToFileURL(path.join(
          downgradeRoot, "packages/collector-cli/src/buffer.ts",
        )).href);
        const LegacyBuffer = legacy.LocalEventBuffer as typeof LocalEventBuffer;
        const oldBuffer = new LegacyBuffer(ledgerPath, { workspaceId: tenantId });
        try {
          insertRaw(oldBuffer, {
            id: uuid(200), sessionId: sessionC,
            observedAt: "2026-09-18T00:00:00.000Z",
            createdAt: "2026-09-18T00:00:00.000Z",
            inputTokens: 5, outputTokens: 6, costUsd: 0.005,
          });
        } finally {
          oldBuffer.close();
        }
        const afterDowngrade = new LocalEventBuffer(ledgerPath, { workspaceId: tenantId });
        try {
          const upgraded = await runIncremental(afterDowngrade, [sessionC]);
          assert.equal(upgraded.result.summaryComplete, true);
          compareExact(afterDowngrade, [sessionC], upgraded.sent);
        } finally {
          afterDowngrade.close();
        }
      }
    } finally {
      if (restarted.database.open) restarted.close();
    }
  } finally {
    try { buffer.close(); } catch { /* already closed by restart case */ }
    for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(ledgerPath + suffix, { force: true });
    fs.rmSync(root, { recursive: true, force: true });
  }

  await reviewRegressions();
  console.log(JSON.stringify({
    result: "PASS",
    proof: "session-summary-incremental",
    exactness: ["append", "late_row", "edit", "delete", "privacy_disposition", "restart", "hwm_mutation", "fallback_marker_mutation"],
    downgrade: Boolean(process.env.PLIMSOLL_0737_ROOT),
  }));
}

import { pathToFileURL } from "node:url";

main().catch((error) => {
  if (expect === "red") {
    console.log(JSON.stringify({ result: "RED", proof: "session-summary-incremental", reason: error instanceof Error ? error.message : String(error) }));
    process.exitCode = 0;
    return;
  }
  console.error(error);
  process.exitCode = 1;
});
