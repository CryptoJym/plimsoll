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
import type { CodexReconciliationStatus } from "../packages/collector-cli/src/codex-reconciliation";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { CoalescingMaintenanceScheduler } from "../packages/collector-cli/src/maintenance";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

function check(name: string, condition: unknown, detail: Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

type LegacyFixtureShape = "irrelevant" | "sparse" | "dense-context" | "mixed";

function seedLegacyLedger(
  file: string,
  rows: number,
  shape: LegacyFixtureShape = "irrelevant",
) {
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
          suppressed_fields_json, created_at, session_id, model,
          input_tokens, output_tokens)
       select printf('legacy-${shape}-%09d', n), 'codex',
         case
           when (@candidateModulo > 0 and n % @candidateModulo = 0)
             or (@mixed = 1 and n % 3 != 0)
             then 'assistant_response'
           when (@denseContext = 1 and n % 3 != 0)
             or (@sparse = 1 and n % 1000 = 1)
             or (@mixed = 1 and n % 3 = 0)
             then 'tool_use'
           else 'otel_span'
         end,
         'metadata',
         strftime('%Y-%m-%dT%H:%M:%fZ', '2026-01-01', printf('+%d seconds', n % 3600)),
         '{"eventType":"legacy_fixture"}', '[]',
         '2026-01-01T00:00:00.000Z',
         case
           when (@denseContext = 1 and n % 3 != 0)
             or (@sparse = 1 and n % 1000 = 1)
             or (@mixed = 1 and n % 3 != 2)
             then '019e9100-0000-7000-8000-000000000001'
         end,
         case
           when (@denseContext = 1 and n % 3 != 0)
             or (@sparse = 1 and n % 1000 = 1)
             or (@mixed = 1 and n % 3 != 2)
             then 'gpt-5.5'
         end,
         case
           when (@candidateModulo > 0 and n % @candidateModulo = 0)
             or (@mixed = 1 and n % 3 != 0)
             then 2400
         end,
         case
           when (@candidateModulo > 0 and n % @candidateModulo = 0)
             or (@mixed = 1 and n % 3 != 0)
             then 510
         end
       from sequence`,
    )
    .run({
      rows,
      denseContext: shape === "dense-context" ? 1 : 0,
      sparse: shape === "sparse" ? 1 : 0,
      mixed: shape === "mixed" ? 1 : 0,
      candidateModulo:
        shape === "dense-context" ? 9_000 : shape === "sparse" ? 50_000 : 0,
    });
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

function proveRawContextLookupPlan(buffer: LocalEventBuffer) {
  const bindings = {
    eventId: "query-plan-candidate",
    start: "2026-01-01T00:00:00.000Z",
    observedAt: "2026-01-01T00:05:00.000Z",
    end: "2026-01-01T00:10:00.000Z",
  };
  const plans = ["session_id", "model"].flatMap((column) =>
    ["before", "after"].map((direction) => {
      const before = direction === "before";
      return buffer.database
        .prepare(
          `explain query plan
           select id, observed_at, ${column}
           from buffered_events indexed by idx_events_observed
           where source = 'codex' and ${column} is not null and id != @eventId
             and observed_at >= @start
             and observed_at <= ${before ? "@observedAt" : "@end"}
           order by observed_at ${before ? "desc" : "asc"}
           limit 1`,
        )
        .all(bindings)
        .map((row) => (row as { detail: string }).detail);
    }),
  );
  check(
    "nearest_context_uses_bounded_raw_observed_index",
    plans.length === 4 &&
      plans.every((plan) =>
        plan.some(
          (detail) =>
            detail.includes("SEARCH buffered_events USING INDEX idx_events_observed") &&
            detail.includes("observed_at>?") &&
            detail.includes("observed_at<?"),
        ),
      ) &&
      plans.every((plan) => plan.every((detail) => !detail.includes("SCAN buffered_events"))),
    { plans },
  );
}

function proveLegacyCadence(root: string, shape: "sparse" | "dense-context") {
  const syntheticRows = 300_000;
  const liveRows = 4_810_030;
  const ledger = path.join(root, `legacy-${shape}.sqlite`);
  seedLegacyLedger(ledger, syntheticRows, shape);
  const buffer = new LocalEventBuffer(ledger);
  try {
    const results: ReturnType<typeof runCodexReconciliationMaintenance>[] = [];
    const durations: number[] = [];
    let freshResolutionCycle: number | null = null;
    let freshResolvedBeforeLegacyCompletion = false;
    let cyclesSinceFresh = 0;
    for (let slice = 0; slice < 240; slice += 1) {
      const started = performance.now();
      const result = runCodexReconciliationMaintenance(buffer.database);
      durations.push(performance.now() - started);
      results.push(result);
      assert.ok(result.legacyRowsVisited <= 100_000);
      assert.ok(result.legacyRowsVisited === 0 || result.legacyRowsVisited % 500 === 0);

      if (shape === "dense-context" && slice === 0) {
        const partial = codexReconciliationStatus(buffer.database);
        assert.equal(partial.legacyComplete, false);
        buffer.append(usageEvent("fresh-during-backfill", "2026-07-15T12:02:00.000Z"));
        buffer.append(contextEvent("fresh-context", "2026-07-15T12:02:30.000Z"));
        continue;
      }
      if (shape === "dense-context" && freshResolutionCycle === null) {
        cyclesSinceFresh += 1;
        const fresh = buffer.database
          .prepare(
            `select session_id as sessionId, model, cost_usd as costUsd
             from buffered_events where id = 'fresh-during-backfill'`,
          )
          .get() as {
          sessionId: string | null;
          model: string | null;
          costUsd: number | null;
        };
        if (fresh.sessionId && fresh.model && fresh.costUsd !== null) {
          freshResolutionCycle = cyclesSinceFresh;
          freshResolvedBeforeLegacyCompletion = !codexReconciliationStatus(buffer.database)
            .legacyComplete;
        }
      }
      if (result.backfillComplete) break;
    }
    const visited = results.reduce((total, result) => total + result.legacyRowsVisited, 0);
    const productive = results
      .map((result, index) => ({ rows: result.legacyRowsVisited, durationMs: durations[index]! }))
      .filter((entry) => entry.rows > 0);
    const rowsPerCycle = visited / Math.max(1, productive.length);
    const projectedCycles = Math.ceil(liveRows / rowsPerCycle);
    const minRowsPerCycle = Math.min(...productive.map((entry) => entry.rows));
    const maxRowsPerCycle = Math.max(...productive.map((entry) => entry.rows));
    const minSliceMs = Math.min(...productive.map((entry) => entry.durationMs));
    const maxSliceMs = Math.max(...productive.map((entry) => entry.durationMs));
    check(
      `${shape}_legacy_high_water_has_measured_bounded_cadence`,
      visited === syntheticRows &&
        results.at(-1)?.backfillComplete === true &&
        projectedCycles <= 120 &&
        maxSliceMs <= 100,
      {
        shape,
        syntheticRows,
        cycles: results.length,
        productiveCycles: productive.length,
        rowsPerCycle: Math.round(rowsPerCycle),
        minRowsPerCycle,
        maxRowsPerCycle,
        minSliceMs: Math.round(minSliceMs * 100) / 100,
        maxSliceMs: Math.round(maxSliceMs * 100) / 100,
        projectedLiveRows: liveRows,
        projectedCycles,
      },
    );

    if (shape === "dense-context") {
      check(
        "fresh_usage_resolves_during_incomplete_dense_backfill",
        freshResolutionCycle !== null &&
          freshResolutionCycle <= 2 &&
          freshResolvedBeforeLegacyCompletion,
        { freshResolutionCycle, freshResolvedBeforeLegacyCompletion },
      );

      const sideTables = buffer.database
        .prepare(
          `select name from sqlite_master
           where type = 'table' and name like 'codex_reconciliation_%'
           order by name`,
        )
        .all()
        .map((row) => (row as { name: string }).name);
      const rowCounts = Object.fromEntries(
        [
          "codex_reconciliation_control",
          "codex_reconciliation_pending",
          "codex_reconciliation_candidates",
          "codex_reconciliation_windows",
        ].map((name) => [
          name,
          (
            buffer.database.prepare(`select count(*) as n from ${name}`).get() as {
              n: number;
            }
          ).n,
        ]),
      );
      const sideBytes = (
        buffer.database
          .prepare(
            `select coalesce(sum(pgsize), 0) as bytes from dbstat
             where name in (
               select name from sqlite_master
               where tbl_name like 'codex_reconciliation_%'
             )`,
          )
          .get() as { bytes: number }
      ).bytes;
      const rawTableBytes = (
        buffer.database
          .prepare(`select coalesce(sum(pgsize), 0) as bytes from dbstat where name = 'buffered_events'`)
          .get() as { bytes: number }
      ).bytes;
      check(
        "dense_history_has_no_context_mirror_or_side_state_amplification",
        !sideTables.includes("codex_reconciliation_context") &&
          sideBytes < rawTableBytes * 0.25 &&
          Object.values(rowCounts).reduce((total, count) => total + count, 0) < 1_000,
        {
          denseContextRowsInRawHistory: 200_000,
          sideTables,
          rowCounts,
          sideBytes,
          rawTableBytes,
          sideToRawRatio: Math.round((sideBytes / rawTableBytes) * 10_000) / 10_000,
        },
      );
      proveRawContextLookupPlan(buffer);
      const timingStatus = codexReconciliationStatus(buffer.database);
      const statusPlan = buffer.database
        .prepare(
          `explain query plan
           select candidate_backlog, max_slice_duration_ms
           from codex_reconciliation_control where singleton = 1`,
        )
        .all()
        .map((row) => (row as { detail: string }).detail);
      check(
        "singleton_status_reports_slice_timing_at_constant_cost",
        timingStatus.maxSliceDurationMs > 0 &&
          timingStatus.maxSliceDurationMs >= timingStatus.lastSliceDurationMs &&
          typeof timingStatus.lastTimeBudgetExhausted === "boolean" &&
          statusPlan.some(
            (detail) =>
              detail.includes("SEARCH codex_reconciliation_control USING INTEGER PRIMARY KEY"),
          ),
        { timingStatus, statusPlan },
      );
    }

    for (let drain = 0; drain < 10; drain += 1) {
      const status = codexReconciliationStatus(buffer.database);
      if (status.candidateBacklog === 0 && status.contextWindowBacklog === 0) break;
      runCodexReconciliationMaintenance(buffer.database, { timeLimitMs: 1_000 });
    }
    const idle = runCodexReconciliationMaintenance(buffer.database, { timeLimitMs: 1_000 });
    check(`${shape}_completed_backfill_visits_zero_idle_rows`, idle.rowsVisited === 0, idle);
  } finally {
    buffer.close();
  }
}

function proveAdversarialMixedBackfill(root: string) {
  const ledger = path.join(root, "legacy-mixed-adversarial.sqlite");
  seedLegacyLedger(ledger, 300_000, "mixed");
  const buffer = new LocalEventBuffer(ledger);
  try {
    const rawShape = buffer.database
      .prepare(
        `select
           sum(case when session_id is not null or model is not null then 1 else 0 end)
             as contextRows,
           sum(case when source = 'codex' and event_type = 'assistant_response'
             and (input_tokens is not null or output_tokens is not null)
             and (session_id is null or model is null or cost_usd is null)
             then 1 else 0 end) as candidateRows
         from buffered_events`,
      )
      .get() as { contextRows: number; candidateRows: number };
    assert.deepEqual(rawShape, { contextRows: 200_000, candidateRows: 200_000 });

    const slices: Array<{
      phase: string;
      durationMs: number;
      legacyRowsVisited: number;
      contextRowsVisited: number;
      candidateRowsVisited: number;
      candidateBacklog: number;
      contextWindowBacklog: number;
      legacyCursorRowid: number;
      legacyComplete: boolean;
    }> = [];
    const runSlice = (phase: string) => {
      const started = performance.now();
      const result = runCodexReconciliationMaintenance(buffer.database);
      const durationMs = performance.now() - started;
      const status = codexReconciliationStatus(buffer.database);
      slices.push({
        phase,
        durationMs: Math.round(durationMs * 100) / 100,
        legacyRowsVisited: result.legacyRowsVisited,
        contextRowsVisited: result.contextRowsVisited,
        candidateRowsVisited: result.candidateRowsVisited,
        candidateBacklog: status.candidateBacklog,
        contextWindowBacklog: status.contextWindowBacklog,
        legacyCursorRowid: status.legacyCursorRowid,
        legacyComplete: status.legacyComplete,
      });
      return { result, status, durationMs };
    };

    const first = runSlice("initial_dense_mixed_backfill");
    assert.equal(first.status.legacyComplete, false);
    buffer.append(usageEvent("mixed-fresh-usage", "2026-07-15T12:02:00.000Z"));
    runSlice("fresh_usage_before_context");
    const waiting = buffer.database
      .prepare(
        `select session_id as sessionId, model, cost_usd as costUsd
         from buffered_events where id = 'mixed-fresh-usage'`,
      )
      .get() as { sessionId: string | null; model: string | null; costUsd: number | null };
    assert.deepEqual(waiting, { sessionId: null, model: null, costUsd: null });

    buffer.append(contextEvent("mixed-later-context", "2026-07-15T12:02:30.000Z"));
    let freshResolutionCycle: number | null = null;
    let resolutionStatus: CodexReconciliationStatus | null = null;
    for (let cycle = 1; cycle <= 2; cycle += 1) {
      const afterContext = runSlice(`after_later_context_${cycle}`);
      const fresh = buffer.database
        .prepare(
          `select session_id as sessionId, model, cost_usd as costUsd
           from buffered_events where id = 'mixed-fresh-usage'`,
        )
        .get() as { sessionId: string | null; model: string | null; costUsd: number | null };
      if (fresh.sessionId && fresh.model && fresh.costUsd !== null) {
        freshResolutionCycle = cycle;
        resolutionStatus = afterContext.status;
        break;
      }
    }
    while (slices.length < 5) runSlice(`bounded_observation_${slices.length + 1}`);

    const sideTables = buffer.database
      .prepare(
        `select name from sqlite_master
         where type = 'table' and name like 'codex_reconciliation_%'
         order by name`,
      )
      .all()
      .map((row) => (row as { name: string }).name);
    const rowCounts = Object.fromEntries(
      [
        "codex_reconciliation_control",
        "codex_reconciliation_pending",
        "codex_reconciliation_candidates",
        "codex_reconciliation_windows",
      ].map((name) => [
        name,
        (
          buffer.database.prepare(`select count(*) as n from ${name}`).get() as {
            n: number;
          }
        ).n,
      ]),
    );
    const sideBytes = (
      buffer.database
        .prepare(
          `select coalesce(sum(pgsize), 0) as bytes from dbstat
           where name in (
             select name from sqlite_master
             where tbl_name like 'codex_reconciliation_%'
           )`,
        )
        .get() as { bytes: number }
    ).bytes;
    const rawTableBytes = (
      buffer.database
        .prepare(`select coalesce(sum(pgsize), 0) as bytes from dbstat where name = 'buffered_events'`)
        .get() as { bytes: number }
    ).bytes;
    const finalStatus = codexReconciliationStatus(buffer.database);
    const discoveredCandidates =
      finalStatus.legacyCursorRowid - Math.floor(finalStatus.legacyCursorRowid / 3);
    const trackedUsageRows =
      Number(rowCounts.codex_reconciliation_pending) +
      Number(rowCounts.codex_reconciliation_candidates);
    const maxSliceMs = Math.max(...slices.map((slice) => slice.durationMs));
    check(
      "mixed_dense_backfill_prioritizes_later_context_without_context_mirror",
      freshResolutionCycle !== null &&
        freshResolutionCycle <= 2 &&
        resolutionStatus?.legacyComplete === false &&
        !sideTables.includes("codex_reconciliation_context") &&
        trackedUsageRows <= discoveredCandidates * 2 + 2 &&
        sideBytes < rawTableBytes &&
        maxSliceMs <= 100,
      {
        rawShape,
        slices,
        freshResolutionCycle,
        legacyIncompleteAtResolution: resolutionStatus?.legacyComplete === false,
        finalObservedStatus: finalStatus,
        discoveredCandidates,
        rowCounts,
        sideTables,
        sideBytes,
        rawTableBytes,
        sideToRawRatio: Math.round((sideBytes / rawTableBytes) * 10_000) / 10_000,
        maxSliceMs,
        cadenceProjection: "not extrapolated: this shape contains 200000 genuine candidates",
      },
    );
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
    proveLegacyCadence(root, "sparse");
    proveLegacyCadence(root, "dense-context");
    proveAdversarialMixedBackfill(root);
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
