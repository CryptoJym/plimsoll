#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const DAY_MS = 24 * 60 * 60 * 1_000;
const MINUTE_MS = 60 * 1_000;
const NOW = new Date("2026-07-15T12:00:00.000Z");
const LIVE_SESSION = "active-live-session";
const LIVE_HISTORICAL_FACTS = 12_001;
const LIVE_UPDATE_PASSES = 16;
const CUTOFF_SESSIONS = 5;
const CUTOFF_RETAINED_FACTS = 1_501;
const CUTOFF_STEPS = 24;

type Totals = {
  events: number;
  tokenEvents: number;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  costUsd: number;
  sessions: number;
  sessionsWithTokens: number;
  oldest: string | null;
  newest: string | null;
};

let clockMs = NOW.getTime();

function createLedger(databasePath: string) {
  const seed = new LocalEventBuffer(databasePath);
  seed.close();
  return new Database(databasePath);
}

function insertStatement(db: Database.Database) {
  return db.prepare(
    `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,
        created_at,session_id,action_class,input_tokens,output_tokens,cost_usd,privacy_generation)
     values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
  );
}

function insertFact(
  insert: Database.Statement,
  id: string,
  observedAt: string,
  sessionId: string,
  inputTokens = 1,
  outputTokens = 2,
) {
  insert.run(
    id,
    "codex",
    "assistant_response",
    "metadata",
    observedAt,
    JSON.stringify({ id, source: "codex", eventType: "assistant_response", observedAt, sessionId }),
    "[]",
    NOW.toISOString(),
    sessionId,
    "other",
    inputTokens,
    outputTokens,
    0.000001,
    `fixture-generation-${id}`,
  );
}

function settle(buffer: LocalEventBuffer, now: Date, maxPasses: number) {
  for (let pass = 0; pass < maxPasses; pass += 1) {
    const status = buffer.projection.status();
    if (
      status.ready &&
      status.parityReady &&
      !status.dirty &&
      Object.values(status.backlog).every((value) => value === 0)
    ) {
      return pass;
    }
    clockMs = now.getTime();
    buffer.projection.runMaintenance(now);
  }
  throw new Error(`projection did not settle in ${maxPasses} passes: ${JSON.stringify(buffer.projection.status())}`);
}

function publishedTotals(buffer: LocalEventBuffer, days: number) {
  const read = buffer.projection.readSnapshot(days);
  assert.equal(read.kind, "ready", JSON.stringify(read));
  if (read.kind !== "ready") throw new Error("snapshot unavailable");
  assert.equal(read.snapshot.projection.status, "ready", JSON.stringify(read.snapshot.projection));
  return {
    since: read.snapshot.window.since,
    totals: read.snapshot.summary.totals as Totals,
    generation: read.snapshot.generation,
  };
}

function recomputeTotals(db: Database.Database, since: string): Totals {
  const row = db.prepare(
    `select count(*) as events,
       coalesce(sum(case when input_tokens is not null or output_tokens is not null then 1 else 0 end),0) as tokenEvents,
       coalesce(sum(input_tokens),0) as inputTokens,
       coalesce(sum(output_tokens),0) as outputTokens,
       coalesce(sum(cache_read_tokens),0) as cacheReadTokens,
       coalesce(sum(cache_creation_tokens),0) as cacheCreationTokens,
       coalesce(sum(round(coalesce(cost_usd,0)*1000000000)),0)/1000000000.0 as costUsd,
       count(distinct session_id) as sessions,
       count(distinct case when input_tokens is not null or output_tokens is not null then session_id end) as sessionsWithTokens,
       min(observed_at) as oldest,max(observed_at) as newest
     from buffered_events
     where data_mode<>'evidence' and privacy_disposition is null
       and privacy_generation is not null and observed_at>=?`,
  ).get(since) as Totals;
  return {
    events: Number(row.events),
    tokenEvents: Number(row.tokenEvents),
    inputTokens: Number(row.inputTokens),
    outputTokens: Number(row.outputTokens),
    cacheReadTokens: Number(row.cacheReadTokens),
    cacheCreationTokens: Number(row.cacheCreationTokens),
    costUsd: Number(row.costUsd),
    sessions: Number(row.sessions),
    sessionsWithTokens: Number(row.sessionsWithTokens),
    oldest: row.oldest,
    newest: row.newest,
  };
}

function assertPublishedParity(buffer: LocalEventBuffer, days: number) {
  const published = publishedTotals(buffer, days);
  const recomputed = recomputeTotals(buffer.database, published.since);
  assert.deepEqual(published.totals, recomputed, JSON.stringify({ days, published, recomputed }));
  return { days, since: published.since, generation: published.generation, totals: recomputed };
}

function assertPublishedSessionParity(buffer: LocalEventBuffer) {
  const checked: number[] = [];
  for (const days of [30, 90, 182, 365, 1825]) {
    const read = buffer.projection.readSnapshot(days);
    assert.equal(read.kind, "ready", JSON.stringify(read));
    if (read.kind !== "ready") throw new Error("snapshot unavailable");
    assert.equal(
      read.snapshot.window.since,
      new Date(clockMs - days * DAY_MS).toISOString(),
      JSON.stringify({days,window:read.snapshot.window}),
    );
    const expected = buffer.database.prepare(
      `select count(*) as events,coalesce(sum(input_tokens),0) as inputTokens,
        coalesce(sum(output_tokens),0) as outputTokens,
        coalesce(sum(cache_read_tokens),0) as cacheReadTokens,
        coalesce(sum(round(coalesce(cost_usd,0)*1000000000)),0)/1000000000.0 as costUsd
       from buffered_events where session_id=? and observed_at>=?`,
    ).get(LIVE_SESSION, read.snapshot.window.since) as Record<string,number>;
    assert.equal(read.snapshot.sessions.length, 1, JSON.stringify({ days, sessions: read.snapshot.sessions }));
    const actual = read.snapshot.sessions[0]!;
    assert.deepEqual(
      {
        events: Number(actual.events),
        inputTokens: Number(actual.inputTokens),
        outputTokens: Number(actual.outputTokens),
        cacheReadTokens: Number(actual.cacheReadTokens),
        costUsd: Number(actual.costUsd),
      },
      {
        events: Number(expected.events),
        inputTokens: Number(expected.inputTokens),
        outputTokens: Number(expected.outputTokens),
        cacheReadTokens: Number(expected.cacheReadTokens),
        costUsd: Number(expected.costUsd),
      },
      JSON.stringify({ days, actual, expected }),
    );
    checked.push(days);
  }
  return checked;
}

function seedLiveLedger(databasePath: string) {
  const db = createLedger(databasePath);
  const insert = insertStatement(db);
  const oldObservedAt = new Date(NOW.getTime() - DAY_MS).toISOString();
  const liveObservedAt = new Date(NOW.getTime() - MINUTE_MS).toISOString();
  db.transaction(() => {
    for (let index = 0; index < LIVE_HISTORICAL_FACTS; index += 1) {
      insertFact(insert, `live-historical-${index}`, oldObservedAt, LIVE_SESSION);
    }
    insertFact(insert, "live-current", liveObservedAt, LIVE_SESSION);
  })();
  db.close();
}

function runLiveUpdates(databasePath: string) {
  clockMs = NOW.getTime();
  seedLiveLedger(databasePath);
  const buffer = new LocalEventBuffer(databasePath);
  try {
    settle(buffer, NOW, 140);
    const before = buffer.projection.status();
    assert.equal(before.ready, true, JSON.stringify(before));
    const mutableRow = buffer.database.prepare(
      `select rowid from buffered_events where id=?`,
    ).get("live-current") as { rowid: number };
    const countedRow = buffer.database.prepare(
      `select rowid from buffered_events where id=?`,
    ).get("live-historical-0") as { rowid: number };
    const update = buffer.database.prepare(
      `update buffered_events set input_tokens=?,output_tokens=? where rowid=?`,
    );
    let lagDuringRepair: Record<string, unknown> | null = null;
    let snapshotStatusDuringRepair: string | null = null;
    let restartRevisionAfterCountedMutation: number | null = null;
    let countedMutationApplied = false;
    let readyPublicationDuringUpdates: {
      at: string;
      snapshotStatus: string;
      snapshotLagState: string | null;
      since: string;
      expectedSince: string;
    } | null = null;
    for (let pass = 1; pass <= LIVE_UPDATE_PASSES; pass += 1) {
      const now = new Date(NOW.getTime() + pass * MINUTE_MS);
      clockMs = now.getTime();
      // One mutation deliberately lands behind the counted cursor. It must
      // restart exactly once; the continuously changing tail row must not.
      let countedMutationAppliedThisPass = false;
      if (!countedMutationApplied) {
        const activeJob = buffer.database.prepare(
          `select cursor_raw_rowid as cursorRawRowid from dashboard_session_repair_jobs
           where days=7 and session_hash=(select session_hash from dashboard_event_facts where raw_rowid=?)`,
        ).get(mutableRow.rowid) as {cursorRawRowid:number}|undefined;
        if (activeJob && activeJob.cursorRawRowid > 0) {
          update.run(5, 6, countedRow.rowid);
          countedMutationApplied = true;
          countedMutationAppliedThisPass = true;
        }
      }
      update.run(pass % 2 === 0 ? 1 : 3, pass % 2 === 0 ? 2 : 4, mutableRow.rowid);
      buffer.projection.runMaintenance(now);
      const passStatus = buffer.projection.status();
      if (!readyPublicationDuringUpdates && passStatus.lastSuccessAt !== before.lastSuccessAt) {
        const published = buffer.projection.readSnapshot(30);
        assert.equal(published.kind, "ready", JSON.stringify(published));
        if (published.kind === "ready") {
          readyPublicationDuringUpdates = {
            at: String(passStatus.lastSuccessAt),
            snapshotStatus: published.snapshot.projection.status,
            snapshotLagState: String((published.snapshot.status.projection as {
              snapshotLag?: {state?:string};
            }).snapshotLag?.state ?? ""),
            since: published.snapshot.window.since,
            expectedSince: new Date(now.getTime() - 30 * DAY_MS).toISOString(),
          };
        }
      }
      if (!lagDuringRepair) {
        const stale = buffer.projection.readSnapshot(30);
        assert.equal(stale.kind, "ready", JSON.stringify(stale));
        if (stale.kind === "ready") {
          const projection = stale.snapshot.status.projection as {
            status?: string;
            snapshotLag?: Record<string, unknown>;
          };
          if (Number(projection.snapshotLag?.activeSessionRepairs) > 0) {
            snapshotStatusDuringRepair = projection.status ?? null;
            lagDuringRepair = projection.snapshotLag ?? null;
          }
        }
      }
      if (countedMutationAppliedThisPass) {
        restartRevisionAfterCountedMutation = Number((buffer.database.prepare(
          `select restart_revision as restartRevision from dashboard_dirty_sessions
           where days=7 and session_hash=(select session_hash from dashboard_event_facts where raw_rowid=?)`,
        ).get(mutableRow.rowid) as {restartRevision:number}).restartRevision);
      }
    }
    const during = buffer.projection.status();
    const advancedWhileUpdating =
      during.lastSuccessAt !== null &&
      before.lastSuccessAt !== null &&
      Date.parse(during.lastSuccessAt) > Date.parse(before.lastSuccessAt);
    const drainAt = new Date(NOW.getTime() + LIVE_UPDATE_PASSES * MINUTE_MS);
    // Base needs many quiet passes after the live writer stops; allow it to
    // settle so this proof can report both independent non-convergence loops
    // and still compare the eventual publication with a scratch query.
    const drainPasses = settle(buffer, drainAt, 120);
    return {
      activePasses: LIVE_UPDATE_PASSES,
      drainPasses,
      advancedWhileUpdating,
      publishedDuringUpdatesAt: during.lastSuccessAt,
      beforeLastSuccessAt: before.lastSuccessAt,
      during,
      lagDuringRepair,
      snapshotStatusDuringRepair,
      readyPublicationDuringUpdates,
      restartRevisionAfterCountedMutation,
      parity: assertPublishedParity(buffer, 1825),
      sessionParityWindows: assertPublishedSessionParity(buffer),
    };
  } finally {
    buffer.close();
  }
}

function seedCutoffLedger(databasePath: string) {
  const db = createLedger(databasePath);
  const insert = insertStatement(db);
  const retainedAt = new Date(NOW.getTime() - 80 * DAY_MS).toISOString();
  const initialCutoff = NOW.getTime() - 90 * DAY_MS;
  db.transaction(() => {
    for (let session = 0; session < CUTOFF_SESSIONS; session += 1) {
      const sessionId = `cutoff-session-${session}`;
      for (let index = 0; index < CUTOFF_RETAINED_FACTS; index += 1) {
        insertFact(insert, `cutoff-retained-${session}-${index}`, retainedAt, sessionId);
      }
      for (let step = 0; step < CUTOFF_STEPS; step += 1) {
        const observedAt = new Date(initialCutoff + step * MINUTE_MS).toISOString();
        insertFact(insert, `cutoff-boundary-${session}-${step}`, observedAt, sessionId);
      }
    }
  })();
  db.close();
}

function runMovingCutoff(databasePath: string) {
  clockMs = NOW.getTime();
  seedCutoffLedger(databasePath);
  const buffer = new LocalEventBuffer(databasePath);
  try {
    settle(buffer, NOW, 180);
    const before = buffer.projection.status();
    assert.equal(before.ready, true, JSON.stringify(before));
    for (let pass = 1; pass <= CUTOFF_STEPS; pass += 1) {
      const now = new Date(NOW.getTime() + pass * MINUTE_MS);
      clockMs = now.getTime();
      buffer.projection.runMaintenance(now);
    }
    const during = buffer.projection.status();
    const advancedWhileCutoffMoved =
      during.lastSuccessAt !== null &&
      before.lastSuccessAt !== null &&
      Date.parse(during.lastSuccessAt) > Date.parse(before.lastSuccessAt);
    const finalAt = new Date(NOW.getTime() + CUTOFF_STEPS * MINUTE_MS);
    const drainPasses = settle(buffer, finalAt, 120);
    const parity = assertPublishedParity(buffer, 90);
    assert.equal(
      parity.since,
      new Date(finalAt.getTime() - 90 * DAY_MS).toISOString(),
      JSON.stringify(parity),
    );
    return {
      activePasses: CUTOFF_STEPS,
      drainPasses,
      advancedWhileCutoffMoved,
      publishedWhileCutoffMovedAt: during.lastSuccessAt,
      beforeLastSuccessAt: before.lastSuccessAt,
      during,
      parity,
    };
  } finally {
    buffer.close();
  }
}

function main() {
  const previousDateNow = Date.now;
  Date.now = () => clockMs;
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-active-convergence-")));
  try {
    const live = runLiveUpdates(path.join(root, "live.sqlite"));
    const cutoff = runMovingCutoff(path.join(root, "cutoff.sqlite"));
    const failures = [
      ...(!live.advancedWhileUpdating ? ["live_last_success_did_not_advance"] : []),
      ...(!live.lagDuringRepair ? ["live_snapshot_lag_missing"] : []),
      ...(live.snapshotStatusDuringRepair !== "stale" ? ["live_snapshot_not_stale"] : []),
      ...(live.lagDuringRepair?.state !== "behind" ? ["live_snapshot_lag_state"] : []),
      ...(live.lagDuringRepair?.lastPublishedAt !== live.beforeLastSuccessAt
        ? ["live_snapshot_lag_watermark"] : []),
      ...(Number(live.lagDuringRepair?.dirtySessions) < 1 ? ["live_snapshot_lag_dirty_sessions"] : []),
      ...(Number(live.lagDuringRepair?.activeSessionRepairs) < 1 ? ["live_snapshot_lag_active_repairs"] : []),
      ...(Number(live.lagDuringRepair?.ageMs) < MINUTE_MS ? ["live_snapshot_lag_age"] : []),
      ...(live.during.backlog.dirtySessions !== 6 ? ["live_followup_was_not_coalesced"] : []),
      ...(!live.readyPublicationDuringUpdates ? ["live_never_published_ready_while_updating"] : []),
      ...(live.readyPublicationDuringUpdates?.snapshotStatus !== "ready"
        ? ["live_publication_was_not_ready"] : []),
      ...(live.readyPublicationDuringUpdates?.snapshotLagState !== "current"
        ? ["live_publication_lag_was_not_current"] : []),
      ...(live.readyPublicationDuringUpdates?.since !== live.readyPublicationDuringUpdates?.expectedSince
        ? ["live_publication_window_was_frozen"] : []),
      ...(live.restartRevisionAfterCountedMutation !== 1
        ? ["counted_mutation_did_not_restart_exactly_once"] : []),
      ...(!cutoff.advancedWhileCutoffMoved ? ["cutoff_last_success_did_not_advance"] : []),
    ];
    assert.deepEqual(failures, [], JSON.stringify({ live, cutoff }));
    console.log(JSON.stringify({
      check: "active_projection_converges_with_exact_published_totals",
      live,
      cutoff,
    }));
  } finally {
    Date.now = previousDateNow;
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main();
