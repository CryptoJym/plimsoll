#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  codexReconciliationStatus,
  runCodexReconciliationMaintenance,
} from "../packages/collector-cli/src/codex-reconciliation";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { CoalescingMaintenanceScheduler } from "../packages/collector-cli/src/maintenance";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

function check(name: string, condition: unknown, detail: Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function seedLegacyLedger(file: string, rows: number) {
  const database = new Database(file);
  database.pragma("journal_mode = WAL");
  database.exec(`
    create table buffered_events (
      id text primary key,
      source text not null,
      event_type text not null,
      data_mode text not null,
      observed_at text not null,
      payload_json text not null,
      suppressed_fields_json text not null default '[]',
      created_at text not null,
      session_id text,
      action_class text,
      model text,
      input_tokens integer,
      output_tokens integer,
      cache_read_tokens integer,
      cache_creation_tokens integer,
      cost_usd real,
      uploaded_at text,
      repo_hash text,
      branch_hash text,
      head_sha text,
      machine text,
      account_hash text
    )
  `);
  database
    .prepare(
      `with recursive sequence(n) as (
         select 1
         union all select n + 1 from sequence where n < @rows
       )
       insert into buffered_events
         (id, source, event_type, data_mode, observed_at, payload_json,
          suppressed_fields_json, created_at)
       select printf('legacy-irrelevant-%09d', n), 'codex', 'otel_span', 'metadata',
         '2026-01-01T00:00:00.000Z', '{"eventType":"otel_span"}', '[]',
         '2026-01-01T00:00:00.000Z'
       from sequence`,
    )
    .run({ rows });
  database.close();
}

function usageEvent(id: string, observedAt: string) {
  return aiInteractionEventSchema.parse({
    id,
    tenantId: "local",
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt,
    actionClass: "other",
    inputTokens: 2_400,
    outputTokens: 510,
    cacheReadTokens: 1_800,
    metadata: {},
  });
}

function contextEvent(id: string, observedAt: string) {
  return aiInteractionEventSchema.parse({
    id,
    tenantId: "local",
    source: "codex",
    dataMode: "metadata",
    eventType: "tool_use",
    observedAt,
    sessionId: "019e9100-0000-7000-8000-000000000001",
    model: "gpt-5.5",
    actionClass: "shell",
    metadata: {},
  });
}

function windowState(buffer: LocalEventBuffer) {
  return buffer.database
    .prepare(
      `select revision, processing_revision as processingRevision,
         cursor_observed_at as cursorObservedAt,
         cursor_event_id as cursorEventId
       from codex_reconciliation_windows limit 1`,
    )
    .get() as
    | {
        revision: number;
        processingRevision: number;
        cursorObservedAt: string;
        cursorEventId: string;
      }
    | undefined;
}

async function proveRequestPathIsBounded(root: string) {
  const ledger = path.join(root, "request-path.sqlite");
  seedLegacyLedger(ledger, 50_000);
  const buffer = new LocalEventBuffer(ledger);
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer);
  try {
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;
    const before = codexReconciliationStatus(buffer.database);
    const observedAt = "2026-07-15T12:02:00.000Z";
    const envelope = {
      resourceSpans: [
        {
          resource: { attributes: [{ key: "service.name", value: { stringValue: "codex" } }] },
          scopeSpans: [
            {
              spans: [
                {
                  name: "codex.usage",
                  traceId: "11111111111111111111111111111111",
                  spanId: "2222222222222222",
                  startTimeUnixNano: String(BigInt(Date.parse(observedAt)) * 1_000_000n),
                  attributes: [
                    { key: "gen_ai.usage.input_tokens", value: { intValue: "2400" } },
                    { key: "gen_ai.usage.output_tokens", value: { intValue: "510" } },
                    { key: "gen_ai.usage.cached_tokens", value: { intValue: "1800" } },
                  ],
                },
              ],
            },
          ],
        },
      ],
    };
    const started = performance.now();
    const response = await fetch(`http://127.0.0.1:${port}/v1/traces`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-plimsoll-source": "codex" },
      body: JSON.stringify(envelope),
    });
    const durationMs = performance.now() - started;
    const body = (await response.json()) as Record<string, unknown>;
    const after = codexReconciliationStatus(buffer.database);
    check(
      "otlp_request_only_appends_and_enqueues",
      response.status === 202 &&
        body.events === 1 &&
        after.rowsVisited === before.rowsVisited &&
        after.candidateBacklog === before.candidateBacklog + 1,
      {
        status: response.status,
        events: body.events as number,
        legacyRows: 50_000,
        rowsVisitedBefore: before.rowsVisited,
        rowsVisitedAfter: after.rowsVisited,
        candidateBacklog: after.candidateBacklog,
        durationMs: Math.round(durationMs * 100) / 100,
      },
    );
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
  }
}

function proveUsefulLegacyCadence(root: string) {
  const syntheticRows = 300_000;
  const liveRows = 4_810_030;
  const ledger = path.join(root, "legacy-throughput.sqlite");
  seedLegacyLedger(ledger, syntheticRows);
  const buffer = new LocalEventBuffer(ledger);
  try {
    const results = [];
    const durations: number[] = [];
    for (let slice = 0; slice < 100; slice += 1) {
      const started = performance.now();
      const result = runCodexReconciliationMaintenance(buffer.database);
      durations.push(performance.now() - started);
      results.push(result);
      assert.ok(result.legacyRowsVisited <= 100_000);
      if (result.backfillComplete) break;
    }
    const visited = results.reduce((total, result) => total + result.legacyRowsVisited, 0);
    const productiveSlices = results.filter((result) => result.legacyRowsVisited > 0).length;
    const rowsPerMinute = visited / Math.max(1, productiveSlices);
    const projectedMinutes = Math.ceil(liveRows / rowsPerMinute);
    check(
      "legacy_high_water_finishes_at_useful_one_minute_cadence",
      visited === syntheticRows &&
        results.at(-1)?.backfillComplete === true &&
        projectedMinutes <= 120,
      {
        syntheticRows,
        slices: results.length,
        rowsPerMinute: Math.round(rowsPerMinute),
        projectedLiveRows: liveRows,
        projectedMinutes,
        maxSliceMs: Math.round(Math.max(...durations) * 100) / 100,
      },
    );
    const idle = runCodexReconciliationMaintenance(buffer.database, { timeLimitMs: 1_000 });
    check("completed_legacy_migration_visits_zero_rows", idle.rowsVisited === 0, idle);
  } finally {
    buffer.close();
  }
}

async function proveContextRevisionCrashRollbackAndOverlap(root: string) {
  const ledger = path.join(root, "context-window.sqlite");
  let buffer = new LocalEventBuffer(ledger);
  const candidateIds = Array.from({ length: 7 }, (_, index) => `candidate-${index}`);
  const candidateTimes = candidateIds.map(
    (_, index) => `2026-07-15T12:0${index}:00.000Z`,
  );
  try {
    buffer.database.exec(`
      create table reconciliation_update_audit (
        event_id text primary key,
        updates integer not null default 0
      );
      create trigger reconciliation_update_audit_trigger
      after update of session_id, model, cost_usd on buffered_events
      when new.id like 'candidate-%'
      begin
        insert into reconciliation_update_audit (event_id, updates) values (new.id, 1)
        on conflict(event_id) do update set updates = updates + 1;
      end;
    `);
    for (let index = 0; index < candidateIds.length; index += 1) {
      buffer.append(usageEvent(candidateIds[index]!, candidateTimes[index]!));
    }
    // Candidates fail closed once, then wait durably for later context.
    runCodexReconciliationMaintenance(buffer.database, {
      legacyRowLimit: 100,
      contextRowLimit: 100,
      candidateLimit: 100,
      timeLimitMs: 1_000,
    });
    const waiting = codexReconciliationStatus(buffer.database);
    check(
      "unresolved_candidates_wait_without_spinning",
      waiting.legacyComplete && waiting.candidateBacklog === 0,
      waiting,
    );

    buffer.append(contextEvent("context-one", "2026-07-15T12:05:30.000Z"));
    const beforeRollback = codexReconciliationStatus(buffer.database);
    buffer.database.exec(`
      create trigger reconciliation_injected_abort
      before update of session_id on buffered_events
      when new.id = 'candidate-2'
      begin
        select raise(abort, 'injected reconciliation rollback');
      end;
    `);
    let rollbackRaised = false;
    try {
      runCodexReconciliationMaintenance(buffer.database, {
        contextRowLimit: 100,
        candidateLimit: 100,
        timeLimitMs: 1_000,
      });
    } catch {
      rollbackRaised = true;
    }
    buffer.database.exec(`drop trigger reconciliation_injected_abort`);
    const unchanged = buffer.database
      .prepare(
        `select count(*) as n from buffered_events
         where id like 'candidate-%' and session_id is null and model is null and cost_usd is null`,
      )
      .get() as { n: number };
    const afterRollback = codexReconciliationStatus(buffer.database);
    check(
      "failed_slice_rolls_back_mutations_and_cursors",
      rollbackRaised &&
        unchanged.n === candidateIds.length &&
        afterRollback.candidateBacklog === beforeRollback.candidateBacklog &&
        afterRollback.contextWindowBacklog === beforeRollback.contextWindowBacklog &&
        afterRollback.degradedReason === "maintenance_failed",
      {
        rollbackRaised,
        unresolvedRows: unchanged.n,
        before: beforeRollback,
        after: afterRollback,
      },
    );

    const firstSlice = runCodexReconciliationMaintenance(buffer.database, {
      contextWindowLimit: 1,
      contextRowLimit: 2,
      candidateLimit: 1,
      timeLimitMs: 1_000,
    });
    const cursorBeforeRepeat = windowState(buffer);
    const firstRepeat = buffer.append(
      contextEvent("context-two", "2026-07-15T12:05:40.000Z"),
    );
    const afterUniqueRepeat = windowState(buffer);
    const duplicateRepeat = buffer.append(
      contextEvent("context-two", "2026-07-15T12:05:40.000Z"),
    );
    const afterDuplicateRepeat = windowState(buffer);
    check(
      "same_bucket_invalidation_increments_revision_without_cursor_reset",
      firstSlice.contextRowsVisited === 2 &&
        firstRepeat === true &&
        duplicateRepeat === false &&
        Boolean(cursorBeforeRepeat?.cursorEventId) &&
        afterUniqueRepeat?.cursorEventId === cursorBeforeRepeat?.cursorEventId &&
        afterUniqueRepeat?.revision === (cursorBeforeRepeat?.revision ?? 0) + 1 &&
        afterDuplicateRepeat?.revision === afterUniqueRepeat?.revision,
      {
        firstSlice,
        cursorBeforeRepeat,
        afterUniqueRepeat,
        afterDuplicateRepeat,
        duplicateRepeat,
      },
    );

    // A committed partial cursor and its revision survive process reopen.
    const durableBefore = codexReconciliationStatus(buffer.database);
    buffer.close();
    buffer = new LocalEventBuffer(ledger);
    const durableAfter = codexReconciliationStatus(buffer.database);
    check(
      "partial_window_and_backlog_survive_reopen",
      durableAfter.candidateBacklog === durableBefore.candidateBacklog &&
        durableAfter.contextWindowBacklog === durableBefore.contextWindowBacklog &&
        JSON.stringify(windowState(buffer)) === JSON.stringify(afterDuplicateRepeat),
      { before: durableBefore, after: durableAfter, window: windowState(buffer) },
    );

    const drainResults = [];
    for (let slice = 0; slice < 100; slice += 1) {
      const result = runCodexReconciliationMaintenance(buffer.database, {
        contextWindowLimit: 1,
        contextRowLimit: 2,
        candidateLimit: 1,
        timeLimitMs: 1_000,
      });
      drainResults.push(result);
      const status = codexReconciliationStatus(buffer.database);
      if (status.candidateBacklog === 0 && status.contextWindowBacklog === 0) break;
    }
    const resolved = buffer.database
      .prepare(
        `select id, session_id as sessionId, model, cost_usd as costUsd
         from buffered_events where id like 'candidate-%' order by id`,
      )
      .all() as Array<{
      id: string;
      sessionId: string | null;
      model: string | null;
      costUsd: number | null;
    }>;
    const audit = buffer.database
      .prepare(`select event_id as eventId, updates from reconciliation_update_audit order by event_id`)
      .all() as Array<{ eventId: string; updates: number }>;
    const finalStatus = codexReconciliationStatus(buffer.database);
    check(
      "later_context_drains_every_tail_row_once_after_revisions",
      resolved.length === candidateIds.length &&
        resolved.every(
          (row) =>
            row.sessionId === "019e9100-0000-7000-8000-000000000001" &&
            row.model === "gpt-5.5" &&
            row.costUsd !== null,
        ) &&
        audit.length === candidateIds.length &&
        audit.every((row) => row.updates === 1) &&
        finalStatus.candidateBacklog === 0 &&
        finalStatus.contextWindowBacklog === 0 &&
        finalStatus.degradedReason === null,
      { slices: drainResults.length, resolved: resolved.length, audit, finalStatus },
    );

    const idle = runCodexReconciliationMaintenance(buffer.database, { timeLimitMs: 1_000 });
    check(
      "unchanged_reconciliation_rerun_visits_zero_rows",
      idle.rowsVisited === 0 && idle.rowsChanged === 0,
      idle,
    );

    let firstStarted!: () => void;
    let releaseFirst!: () => void;
    const startedSignal = new Promise<void>((resolve) => (firstStarted = resolve));
    const releaseSignal = new Promise<void>((resolve) => (releaseFirst = resolve));
    let invocations = 0;
    const scheduler = new CoalescingMaintenanceScheduler(async () => {
      invocations += 1;
      if (invocations === 1) {
        firstStarted();
        await releaseSignal;
      }
      const reconciliation = runCodexReconciliationMaintenance(buffer.database, {
        timeLimitMs: 1_000,
      });
      return {
        recentOnly: true,
        rollout: {
          filesSeen: 0, filesRead: 0, filesParsed: 0, filesReset: 0, legacyRebuilds: 0,
          checkpointRebuilds: 0, bytesRead: 0, bytesDeferred: 0,
          sessionsSkippedOtlpCovered: 0, eventsAppended: 0,
          tokensAppended: { input: 0, cachedInput: 0, output: 0 }, parseErrors: 0,
        },
        transcript: {
          filesSeen: 0, filesRead: 0, filesParsed: 0, filesReset: 0, legacyRebuilds: 0,
          checkpointRebuilds: 0, bytesRead: 0, bytesDeferred: 0,
          sessionsSkippedLiveCovered: 0, eventsAppended: 0,
          tokensAppended: { input: 0, cacheRead: 0, output: 0 }, parseErrors: 0,
        },
        reconciliation,
        repricing: {
          catalogFingerprint: "proof", catalogChanged: false, backfillComplete: true,
          legacyRowsVisited: 0, candidateRowsVisited: 0, rowsVisited: 0, repriced: 0,
        },
        enrichment: {
          backfillComplete: true, legacyRowsVisited: 0, sessionsVisited: 0,
          candidateRowsVisited: 0, rowsVisited: 0, backward: 0, forward: 0,
        },
        rawEventWrites: 0,
      };
    });
    const first = scheduler.trigger(true);
    await startedSignal;
    const second = scheduler.trigger(true);
    const third = scheduler.trigger(false);
    releaseFirst();
    await Promise.all([first, second, third]);
    const schedulerStatus = scheduler.status();
    check(
      "overlapping_reconciliation_triggers_coalesce_to_one_inflight_job",
      invocations === 2 &&
        schedulerStatus.maxConcurrentJobs === 1 &&
        schedulerStatus.overlappingJobs === 0 &&
        schedulerStatus.coalescedTriggerCount === 2,
      { invocations, schedulerStatus },
    );

    const privateColumns = buffer.database
      .prepare(
        `select name from pragma_table_info('codex_reconciliation_pending')
         union all select name from pragma_table_info('codex_reconciliation_candidates')
         union all select name from pragma_table_info('codex_reconciliation_context')
         union all select name from pragma_table_info('codex_reconciliation_windows')`,
      )
      .all() as Array<{ name: string }>;
    check(
      "reconciliation_indexes_store_no_payload_or_raw_path_columns",
      privateColumns.every(
        (row) => !/(payload|content|prompt|response|arguments|path|url|email|token)/i.test(row.name),
      ),
      { columns: privateColumns.map((row) => row.name) },
    );
  } finally {
    buffer.close();
  }
}

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-codex-reconciliation-proof-"));
  try {
    await proveRequestPathIsBounded(root);
    proveUsefulLegacyCadence(root);
    await proveContextRevisionCrashRollbackAndOverlap(root);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
  process.stdout.write(
    `${JSON.stringify(
      {
        status: "pass",
        proof: "issue-91-bounded-codex-reconciliation",
        checks: checks.length,
        names: checks.map((entry) => entry.name),
        evidence: Object.fromEntries(checks.map((entry) => [entry.name, entry.detail])),
      },
      null,
      2,
    )}\n`,
  );
}

main().catch((error) => {
  process.stderr.write(
    `${JSON.stringify({
      status: "fail",
      proof: "issue-91-bounded-codex-reconciliation",
      error: error instanceof Error ? error.message : String(error),
    })}\n`,
  );
  process.exitCode = 1;
});
