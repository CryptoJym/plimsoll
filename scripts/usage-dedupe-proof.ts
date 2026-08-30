#!/usr/bin/env node

/**
 * Issue #179/#193 regression proof: dashboard usage double-counting across
 * the two ingest paths.
 *
 * A legacy ledger can hold the same session's usage twice — once from live
 * capture (any non-tailer event type, e.g. `assistant_response` or
 * `otel_span`) and once from a backfill tailer (`usage_transcript` or
 * `usage_rollout`). Fixtures here insert both paths directly as raw rows
 * (bypassing the ingest-time authority gate, exactly like pre-gate data) and
 * prove the projection counts each unit of usage exactly once under the
 * documented preference rule: live wins, backfill is projected without usage,
 * regardless of event class pairing (#193 gap 1) or arrival order (#193 gap 2).
 */

import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { guardProofCompletion } from "./lib/proof-completion";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const NOW = new Date("2026-08-20T16:00:00.000Z");
const DAY_MS = 24 * 60 * 60 * 1000;
Date.now = () => NOW.getTime();

const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

let eventSequence = 1;

/**
 * Raw dual-path fixture row. Inserted straight into buffered_events (with a
 * privacy generation so it projects) to reproduce a legacy mixed ledger that
 * predates the ingest-time session_usage_authority gate.
 */
function rawUsageRow(
  db: LocalEventBuffer["database"],
  input: {
    source?: string;
    eventType: string;
    sessionId: string | null;
    model?: string;
    observedAt?: string;
    inputTokens?: number;
    outputTokens?: number;
    cacheReadTokens?: number;
    cacheCreationTokens?: number;
    costUsd?: number;
  },
) {
  db.prepare(
    `insert into buffered_events
      (id, source, event_type, data_mode, observed_at, payload_json,
       suppressed_fields_json, created_at, session_id, action_class, model,
       input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
       cost_usd, privacy_generation)
     values (?, ?, ?, 'metadata', ?, '{}', '[]', ?, ?, 'other', ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    `dedupe-fixture-${eventSequence++}`,
    input.source ?? "claude_code",
    input.eventType,
    input.observedAt ?? new Date(NOW.getTime() - DAY_MS).toISOString(),
    NOW.toISOString(),
    input.sessionId,
    input.model ?? null,
    input.inputTokens ?? null,
    input.outputTokens ?? null,
    input.cacheReadTokens ?? null,
    input.cacheCreationTokens ?? null,
    input.costUsd ?? null,
    crypto.randomUUID(),
  );
}

function settle(buffer: LocalEventBuffer, now = NOW, maxSlices = 100) {
  const receipts = [];
  for (let slice = 0; slice < maxSlices; slice += 1) {
    const state = buffer.projection.status();
    if (
      state.ready &&
      !state.dirty &&
      state.backfill.complete &&
      state.backfill.parityComplete &&
      Object.values(state.backlog).every((value) => value === 0)
    ) {
      return receipts;
    }
    receipts.push(buffer.projection.runMaintenance(now));
  }
  throw new Error(`projection did not settle: ${JSON.stringify(buffer.projection.status())}`);
}

function readySnapshot(buffer: LocalEventBuffer, days: number) {
  const read = buffer.projection.readSnapshot(days);
  assert.equal(read.kind, "ready", JSON.stringify(read));
  return read.kind === "ready" ? read.snapshot : assert.fail("snapshot not ready");
}

function windowCostUsd(db: LocalEventBuffer["database"], days: number) {
  return Number((db.prepare(
    `select cost_nanos as costNanos from dashboard_window_totals where days=?`,
  ).get(days) as { costNanos: number }).costNanos) / 1_000_000_000;
}

// Refuses the two silent-green modes: an early event-loop drain and a hang
// that never exits. See scripts/lib/proof-completion.ts. main() here is
// synchronous, so completion is a plain statement after the call below.
const guard = guardProofCompletion({ countChecks: () => checks.length });

function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-usage-dedupe-proof-"));
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
  try {
    const dual = "dedupe-session-dual-path";
    const tailerOnly = "dedupe-session-tailer-only";
    const liveOnly = "dedupe-session-live-only";

    // Session recorded by BOTH ingest paths (the issue's $2,845.61 vs
    // $2,986.22 shape): live says $3.80, backfill says $12.00 over the same
    // work. Naive summation reports $15.80 for one session.
    rawUsageRow(buffer.database, { eventType: "assistant_response", sessionId: dual,
      model: "claude-fable-5", inputTokens: 100, outputTokens: 10, costUsd: 1.1 });
    rawUsageRow(buffer.database, { eventType: "assistant_response", sessionId: dual,
      model: "claude-fable-5", inputTokens: 200, outputTokens: 20, costUsd: 2.2 });
    rawUsageRow(buffer.database, { eventType: "assistant_response", sessionId: dual,
      model: "claude-fable-5", inputTokens: 50, outputTokens: 5, costUsd: 0.5 });
    rawUsageRow(buffer.database, { eventType: "usage_transcript", sessionId: dual,
      model: "claude-fable-5", inputTokens: 900, outputTokens: 90, costUsd: 5 });
    rawUsageRow(buffer.database, { eventType: "usage_transcript", sessionId: dual,
      model: "claude-fable-5", inputTokens: 800, outputTokens: 80, costUsd: 7 });

    // Backfill remains authoritative when the live path never saw the session.
    rawUsageRow(buffer.database, { eventType: "usage_transcript", sessionId: tailerOnly,
      model: "claude-fable-5", inputTokens: 70, outputTokens: 7, costUsd: 0.9 });

    // Live-only and sessionless usage are unaffected by the rule.
    rawUsageRow(buffer.database, { eventType: "assistant_response", sessionId: liveOnly,
      model: "claude-fable-5", inputTokens: 30, outputTokens: 3, costUsd: 0.3 });
    rawUsageRow(buffer.database, { eventType: "usage_transcript", sessionId: null,
      model: "claude-fable-5", inputTokens: 10, outputTokens: 1, costUsd: 0.1 });

    // Reproduce: the raw evidence really does carry both paths for one session.
    const rawPaths = buffer.database.prepare(
      `select event_type as eventType, count(*) as n, sum(cost_usd) as cost
       from buffered_events where session_id=? group by event_type`,
    ).all(dual) as Array<{ eventType: string; n: number; cost: number }>;
    const naiveCombined = Number((buffer.database.prepare(
      `select sum(cost_usd) as cost from buffered_events`,
    ).get() as { cost: number }).cost);
    check("fixture_reproduces_dual_path_session",
      rawPaths.length === 2 && Math.abs(naiveCombined - 17.1) < 1e-6,
      { rawPaths, naiveCombined });

    settle(buffer);

    // Rule: live wins. Projected window cost equals the chosen path only:
    // 3.8 (dual live) + 0.9 (tailer-only) + 0.3 (live-only) + 0.1 (sessionless).
    const projected30 = windowCostUsd(buffer.database, 30);
    check("window_total_counts_each_unit_once",
      Math.abs(projected30 - 5.1) < 1e-6 && Math.abs(projected30 - naiveCombined) > 1,
      { projected30, naiveCombined });

    // Acceptance-criterion form: projection total == chosen path's raw sum,
    // computed independently from raw evidence with the ingest gate's class
    // definition (tailer = usage_rollout|usage_transcript, live = the rest).
    const chosenPathSum = Number((buffer.database.prepare(
      `select coalesce(sum(case when e.event_type in ('usage_rollout','usage_transcript')
         and e.session_id is not null
         and exists (select 1 from buffered_events live where live.source=e.source
           and live.session_id=e.session_id
           and live.event_type not in ('usage_rollout','usage_transcript')
           and (live.input_tokens is not null or live.output_tokens is not null
             or live.cache_read_tokens is not null or live.cache_creation_tokens is not null
             or live.cost_usd is not null)) then null else e.cost_usd end),0) as cost
       from buffered_events e`,
    ).get() as { cost: number }).cost);
    check("projection_equals_chosen_path_raw_sum",
      Math.abs(projected30 - chosenPathSum) < 1e-6, { projected30, chosenPathSum });

    const lifetime = readySnapshot(buffer, 30).status.stats as Record<string, number>;
    check("lifetime_totals_single_counted",
      Math.abs(Number(lifetime.totalCostUsd) - 5.1) < 1e-6,
      { totalCostUsd: lifetime.totalCostUsd });

    const totals = readySnapshot(buffer, 30).summary.totals as Record<string, number>;
    check("token_events_single_counted",
      Number(totals.tokenEvents) === 6 && Number(totals.inputTokens) === 460,
      { tokenEvents: totals.tokenEvents, inputTokens: totals.inputTokens });

    // The choice is legible on the projection surface itself.
    const authority = (readySnapshot(buffer, 30).summary as Record<string, unknown>).usageAuthority as
      { rule: string; backfillSessionsSuppressed: number };
    check("snapshot_exposes_usage_authority_rule",
      authority.rule === "live_capture_usage_wins" &&
      authority.backfillSessionsSuppressed === 1,
      authority);

    // Per-session surfaces follow the same rule without double counting.
    const sessions = readySnapshot(buffer, 30).sessions as Array<Record<string, unknown>>;
    const sessionDetail = (sessionId: string) => {
      const hash = `sha256:${crypto.createHash("sha256").update(sessionId).digest("hex")}`;
      return sessions.find((row) => row.sessionId === hash);
    };
    check("dual_path_session_counts_live_only",
      Math.abs(Number(sessionDetail(dual)?.costUsd) - 3.8) < 1e-6,
      { dual: sessionDetail(dual)?.costUsd });
    check("tailer_only_session_keeps_backfill_usage",
      Math.abs(Number(sessionDetail(tailerOnly)?.costUsd) - 0.9) < 1e-6,
      { tailerOnly: sessionDetail(tailerOnly)?.costUsd });

    // Parity reference windows use the same rule; a mismatch would surface as
    // degraded_reason='projection_parity_mismatch'.
    const state = buffer.projection.status();
    check("parity_settles_green_under_dedupe_rule",
      state.ready && state.parityReady && !state.degraded &&
      state.degradedReason !== "projection_parity_mismatch",
      { state });

    // Re-derivation stays stable: touching a suppressed backfill row replays
    // it through the rule and moves nothing.
    const before = windowCostUsd(buffer.database, 30);
    buffer.database.prepare(
      `update buffered_events set output_tokens=output_tokens+1
       where event_type='usage_transcript' and session_id=?`,
    ).run(dual);
    settle(buffer);
    const afterTouch = windowCostUsd(buffer.database, 30);
    check("suppressed_backfill_replay_moves_nothing",
      Math.abs(afterTouch - before) < 1e-6, { before, afterTouch });

    // Removing the live sibling does NOT retroactively promote stored
    // backfill facts (conservative under-count, never a new double count):
    // totals stay at the single-counted figure after maintenance re-runs.
    buffer.database.prepare(
      `delete from buffered_events where event_type='assistant_response' and session_id=?`,
    ).run(liveOnly);
    settle(buffer);
    const afterLiveDelete = windowCostUsd(buffer.database, 30);
    check("live_delete_keeps_single_counted_total",
      Math.abs(afterLiveDelete - (before - 0.3)) < 1e-6, { afterLiveDelete });

    // Reopen: persisted aggregates survive restart unchanged.
    const reopenPath = path.join(root, "ledger.sqlite");
    const reopened = new LocalEventBuffer(reopenPath);
    try {
      settle(reopened);
      check("reopen_preserves_single_counted_totals",
        Math.abs(windowCostUsd(reopened.database, 30) - (before - 0.3)) < 1e-6,
        { reopened: windowCostUsd(reopened.database, 30) });
      const reopenedAuthority =
        (readySnapshot(reopened, 30).summary as Record<string, unknown>).usageAuthority as
          { rule: string };
      check("reopen_exposes_same_rule", reopenedAuthority.rule === "live_capture_usage_wins",
        reopenedAuthority);
    } finally {
      reopened.close();
    }

    console.log(`usage-dedupe-proof: ${checks.length} checks green`);
    for (const entry of checks) console.log(`  ok ${entry.name}`);
  } finally {
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }

  issue193EventClassAndOrderGaps();
  issue193ReworkFixtures();
}

/**
 * Issue #193 adversarial fixtures: the merged #185 dedupe only suppressed
 * `usage_transcript` against `assistant_response` siblings and never
 * re-derived facts when the live sibling arrived later. Every check in this
 * section FAILS on that code and must PASS after the fix.
 */
function issue193EventClassAndOrderGaps() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-issue193-proof-"));
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
  try {
    // Gap 1a — legacy mixed CODEX session: tailer class is usage_rollout.
    const codexRollout = "i193-codex-rollout-mixed";
    rawUsageRow(buffer.database, { source: "codex", eventType: "usage_rollout",
      sessionId: codexRollout, model: "gpt-fable-5", inputTokens: 400, outputTokens: 40, costUsd: 4 });
    rawUsageRow(buffer.database, { source: "codex", eventType: "assistant_response",
      sessionId: codexRollout, model: "gpt-fable-5", inputTokens: 100, outputTokens: 10, costUsd: 1.5 });
    rawUsageRow(buffer.database, { source: "codex", eventType: "assistant_response",
      sessionId: codexRollout, model: "gpt-fable-5", inputTokens: 50, outputTokens: 5, costUsd: 0.5 });

    // Gap 1b — live usage arrived as otel_span; transcript must still lose.
    const otelLive = "i193-otel-span-live";
    rawUsageRow(buffer.database, { eventType: "otel_span", sessionId: otelLive,
      model: "claude-fable-5", inputTokens: 120, outputTokens: 12, costUsd: 0.7 });
    rawUsageRow(buffer.database, { eventType: "otel_span", sessionId: otelLive,
      model: "claude-fable-5", inputTokens: 30, outputTokens: 3, costUsd: 0.3 });
    rawUsageRow(buffer.database, { eventType: "usage_transcript", sessionId: otelLive,
      model: "claude-fable-5", inputTokens: 700, outputTokens: 70, costUsd: 3 });
    rawUsageRow(buffer.database, { eventType: "usage_transcript", sessionId: otelLive,
      model: "claude-fable-5", inputTokens: 600, outputTokens: 60, costUsd: 2 });

    // Negative control: transcript + live row WITHOUT usage is not dual-path;
    // the backfill stays authoritative and must keep counting its usage.
    const noUsageLive = "i193-live-without-usage";
    rawUsageRow(buffer.database, { eventType: "usage_transcript", sessionId: noUsageLive,
      model: "claude-fable-5", inputTokens: 90, outputTokens: 9, costUsd: 9.9 });
    rawUsageRow(buffer.database, { eventType: "assistant_response", sessionId: noUsageLive,
      model: "claude-fable-5" });

    settle(buffer);
    // Expected: 2.0 (rollout session, live only) + 1.0 (otel session, live only)
    //           + 9.9 (no-usage-live control keeps backfill) = 12.9
    let projected30 = windowCostUsd(buffer.database, 30);
    check("i193_rollout_and_otel_live_siblings_suppress_backfill",
      Math.abs(projected30 - 12.9) < 1e-6,
      { projected30, expected: 12.9 });

    let authority = (readySnapshot(buffer, 30).summary as Record<string, unknown>).usageAuthority as
      { rule: string; backfillSessionsSuppressed: number };
    check("i193_summary_counts_dual_path_sessions_by_class",
      authority.backfillSessionsSuppressed === 2 && authority.rule === "live_capture_usage_wins",
      authority);

    // Gap 2 — transcript-first arrival order. Project the tailer fact with no
    // live sibling present, THEN record live capture for the same session.
    const lateLive = "i193-transcript-first";
    rawUsageRow(buffer.database, { eventType: "usage_transcript", sessionId: lateLive,
      model: "claude-fable-5", inputTokens: 500, outputTokens: 50, costUsd: 6 });
    settle(buffer);
    const transcriptFirstAlone = windowCostUsd(buffer.database, 30);
    check("i193_transcript_first_counts_before_sibling",
      Math.abs(transcriptFirstAlone - 18.9) < 1e-6, // 12.9 + 6.0
      { transcriptFirstAlone });
    rawUsageRow(buffer.database, { eventType: "assistant_response", sessionId: lateLive,
      model: "claude-fable-5", inputTokens: 150, outputTokens: 15, costUsd: 2 });
    settle(buffer);

    // Same shape with the rollout tailer class and an otel_span live sibling.
    const rolloutFirst = "i193-rollout-first";
    rawUsageRow(buffer.database, { eventType: "usage_rollout", sessionId: rolloutFirst,
      model: "gpt-fable-5", inputTokens: 300, outputTokens: 30, costUsd: 5 });
    settle(buffer);
    rawUsageRow(buffer.database, { eventType: "otel_span", sessionId: rolloutFirst,
      model: "gpt-fable-5", inputTokens: 80, outputTokens: 8, costUsd: 1.25 });
    settle(buffer);

    projected30 = windowCostUsd(buffer.database, 30);
    // Converged total: 12.9 + 2.0 (late live wins) + 1.25 (otel wins) = 16.15
    check("i193_late_live_sibling_rederives_projected_backfill_fact",
      Math.abs(projected30 - 16.15) < 1e-6,
      { projected30, expected: 16.15 });

    const lifetime = readySnapshot(buffer, 30).status.stats as Record<string, number>;
    check("i193_converged_lifetime_single_counted",
      Math.abs(Number(lifetime.totalCostUsd) - 16.15) < 1e-6,
      { totalCostUsd: lifetime.totalCostUsd });

    authority = (readySnapshot(buffer, 30).summary as Record<string, unknown>).usageAuthority as
      { rule: string; backfillSessionsSuppressed: number };
    check("i193_summary_tracks_all_four_suppressed_sessions",
      authority.backfillSessionsSuppressed === 4,
      authority);

    const state = buffer.projection.status();
    check("i193_parity_settles_green_after_order_repairs",
      state.ready && state.parityReady && !state.degraded &&
      state.degradedReason !== "projection_parity_mismatch",
      { state });

    console.log(`issue-193 fixtures: ${checks.length - 13} checks green`);
  } finally {
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

// Referenced so the type import stays honest even though fixtures bypass it.
main();
guard.complete();

function repairFactsCounter(db: LocalEventBuffer["database"]) {
  return (db.prepare(
    `select repair_facts as n from dashboard_projection_control where singleton=1`,
  ).get() as { n: number }).n;
}

/**
 * Rework fixtures for the two post-audit gaps (issue #193):
 *
 * Gap 1 — repair amplification. The sibling-repair enqueue used to fire on
 * EVERY live-usage event, so a legacy session with N already-projected
 * tailer facts re-drained N repair rows for each new live row forever.
 * The fix enqueues only when a batch introduces the FIRST live-usage row
 * for a (source, session): first arrival drains exactly once, later live
 * rows drain zero sibling repairs, totals still converge.
 *
 * Gap 2 — summary/suppression mismatch. Suppression's live-sibling lookup
 * is unbounded in time, but usageAuthoritySummary required BOTH sides to be
 * inside the reporting window. The fix keeps the window on the tailer side
 * only; an out-of-window live sibling must still yield a suppressed AND
 * counted session.
 */
function issue193ReworkFixtures() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-i193-rework-proof-"));
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
  try {
    // --- Gap 1: sibling-repair amplification -----------------------------
    const amp = "i193r-amplified-session";
    const tailerFacts = 5;
    for (let i = 0; i < tailerFacts; i += 1) {
      rawUsageRow(buffer.database, { eventType: "usage_transcript", sessionId: amp,
        model: "claude-fable-5", inputTokens: 10 + i, outputTokens: i, costUsd: 1 });
    }
    settle(buffer);
    const baselineRepairs = repairFactsCounter(buffer.database);

    // FIRST live-usage row for the pair: the sibling repair is required and
    // must be drained exactly once — one per projected tailer fact — plus
    // the live row's own raw_insert repair.
    rawUsageRow(buffer.database, { eventType: "assistant_response", sessionId: amp,
      model: "claude-fable-5", inputTokens: 100, outputTokens: 10, costUsd: 2 });
    settle(buffer);
    const firstLiveDelta = repairFactsCounter(buffer.database) - baselineRepairs;
    check("i193r_first_live_row_drains_sibling_repairs_exactly_once",
      firstLiveDelta === tailerFacts + 1,
      { firstLiveDelta, expectedSiblingRepairs: tailerFacts, plusRawInsert: 1 });

    // SECOND live-usage row for the same pair: suppression cannot change
    // (the predicate is existential), so ZERO additional sibling repairs
    // may be enqueued — only the live row's own raw_insert repair drains.
    const afterFirst = repairFactsCounter(buffer.database);
    rawUsageRow(buffer.database, { eventType: "assistant_response", sessionId: amp,
      model: "claude-fable-5", inputTokens: 200, outputTokens: 20, costUsd: 2 });
    settle(buffer);
    const secondLiveDelta = repairFactsCounter(buffer.database) - afterFirst;
    check("i193r_second_live_row_enqueues_zero_sibling_repairs",
      secondLiveDelta === 1,
      { secondLiveDelta, expectedSiblingRepairs: 0 });

    // Totals converge to the suppressed value either way: 5 suppressed $1
    // tailers + 2 live rows at $2 = $4 counted exactly once.
    let projected30 = windowCostUsd(buffer.database, 30);
    check("i193r_totals_converge_to_suppressed_value",
      Math.abs(projected30 - 4) < 1e-6, { projected30, expected: 4 });

    // --- Gap 2: out-of-window live sibling suppresses AND counts ---------
    const stale = "i193r-stale-live-session";
    rawUsageRow(buffer.database, { eventType: "assistant_response", sessionId: stale,
      observedAt: new Date(NOW.getTime() - 61 * DAY_MS).toISOString(),
      model: "claude-fable-5", inputTokens: 900, outputTokens: 90, costUsd: 50 });
    rawUsageRow(buffer.database, { eventType: "usage_transcript", sessionId: stale,
      model: "claude-fable-5", inputTokens: 500, outputTokens: 50, costUsd: 50 });
    settle(buffer);

    const authority = (readySnapshot(buffer, 30).summary as Record<string, unknown>)
      .usageAuthority as { rule: string; backfillSessionsSuppressed: number };
    check("i193r_out_of_window_live_sibling_suppresses_and_counts",
      authority.backfillSessionsSuppressed === 2 && authority.rule === "live_capture_usage_wins",
      authority);

    // And the suppression really fired: the in-window $50 tailer contributes
    // nothing, so the window total stays at the converged $4 from above.
    projected30 = windowCostUsd(buffer.database, 30);
    check("i193r_stale_sibling_actually_suppresses_window_total",
      Math.abs(projected30 - 4) < 1e-6, { projected30, expected: 4 });

    console.log(`rework fixtures: ${checks.length - 20} checks green`);
  } finally {
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}
