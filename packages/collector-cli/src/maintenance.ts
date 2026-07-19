import { createHash } from "node:crypto";

import type Database from "better-sqlite3";

import { MODEL_PRICING, estimateCostUsd } from "../../shared/src/index";
import type { LocalEventBuffer } from "./buffer";
import {
  runCodexReconciliationMaintenance,
  type CodexReconciliationResult,
} from "./codex-reconciliation";
import { RolloutTailer, type RolloutScanResult } from "./rollout-tailer";
import { TranscriptTailer, type TranscriptScanResult } from "./transcript-tailer";
import { captureBaselineStatus } from "./capture-baseline";
import { CaptureWorkBudget, type CaptureBudgetStatus } from "./capture-work-budget";

const PRICING_VERSION_KEY = "pricing_catalog_applied";
const PRICING_TARGET_KEY = "pricing_catalog_backfill_target";
const PRICING_CURSOR_KEY = "pricing_catalog_backfill_cursor";
const REPO_BACKFILL_CURSOR_KEY = "repo_enrichment_backfill_cursor";
const REPO_BACKFILL_COMPLETE_KEY = "repo_enrichment_backfill_complete";
const AUTOMATIC_CAPTURE_SOURCE_TURN_KEY = "automatic_capture_source_turn";
const AUTOMATIC_CAPTURE_RUNTIME_TABLE = "automatic_capture_runtime_state";

function ensureAutomaticCaptureRuntimeState(database: Database.Database) {
  database.exec(`
    create table if not exists ${AUTOMATIC_CAPTURE_RUNTIME_TABLE} (
      singleton integer primary key check (singleton = 1),
      generation integer not null,
      phase text not null check (phase in ('baseline','capture')),
      status text not null check (status in ('baseline_in_progress','deferred','complete','complete_with_errors','aborted','failed')),
      completed_at text not null,
      bytes_read integer not null,
      records_parsed integer not null,
      events_appended integer not null,
      deferred_bytes integer not null,
      deferred_generations integer not null,
      excluded_generations integer not null,
      error_count integer not null,
      yields integer not null,
      last_yield_at text,
      budget_exhausted_by text check (
        budget_exhausted_by is null or
        budget_exhausted_by in ('bytes','records','events','wall')
      )
    )
  `);
}

function recordAutomaticCaptureRuntimeState(
  database: Database.Database,
  phase: "baseline" | "capture",
  status: "baseline_in_progress" | "deferred" | "complete" | "complete_with_errors" | "aborted" | "failed",
  budget: CaptureBudgetStatus,
  rollout?: RolloutScanResult,
  transcript?: TranscriptScanResult,
) {
  ensureAutomaticCaptureRuntimeState(database);
  const results = [rollout, transcript].filter(Boolean) as Array<RolloutScanResult | TranscriptScanResult>;
  const sum = (read: (result: RolloutScanResult | TranscriptScanResult) => number) =>
    results.reduce((total, result) => total + read(result), 0);
  const lastYieldAt = results
    .map((result) => result.lastYieldAt)
    .filter((value): value is string => Boolean(value))
    .sort()
    .at(-1) ?? null;
  database.prepare(
    `insert into ${AUTOMATIC_CAPTURE_RUNTIME_TABLE} (
       singleton, generation, phase, status, completed_at, bytes_read, records_parsed,
       events_appended, deferred_bytes, deferred_generations,
       excluded_generations, error_count, yields, last_yield_at,
       budget_exhausted_by
     ) values (1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     on conflict(singleton) do update set
       generation = ${AUTOMATIC_CAPTURE_RUNTIME_TABLE}.generation + 1,
       phase = excluded.phase,
       status = excluded.status,
       completed_at = excluded.completed_at,
       bytes_read = excluded.bytes_read,
       records_parsed = excluded.records_parsed,
       events_appended = excluded.events_appended,
       deferred_bytes = excluded.deferred_bytes,
       deferred_generations = excluded.deferred_generations,
       excluded_generations = excluded.excluded_generations,
       error_count = excluded.error_count,
       yields = excluded.yields,
       last_yield_at = excluded.last_yield_at,
       budget_exhausted_by = excluded.budget_exhausted_by`,
  ).run(
    phase,
    status,
    new Date().toISOString(),
    sum((result) => result.bytesRead),
    sum((result) => result.recordsParsed),
    sum((result) => result.eventsAppended),
    sum((result) => result.bytesDeferred),
    sum((result) => result.deferredGenerations),
    sum((result) => result.excludedGenerations),
    sum((result) => result.discoveryErrors + result.statErrors + result.readErrors + result.parseErrors + result.unresolvedRecords),
    sum((result) => result.cooperativeYields),
    lastYieldAt,
    budget.exhaustedBy,
  );
}

export function automaticCaptureRuntimeStatus(database: Database.Database) {
  ensureAutomaticCaptureRuntimeState(database);
  const row = database.prepare(
    `select generation, phase, status, completed_at as completedAt,
       bytes_read as bytesRead, records_parsed as recordsParsed,
       events_appended as eventsAppended, deferred_bytes as deferredBytes,
       deferred_generations as deferredGenerations,
       excluded_generations as excludedGenerations, error_count as errorCount,
       yields, last_yield_at as lastYieldAt,
       budget_exhausted_by as budgetExhaustedBy
     from ${AUTOMATIC_CAPTURE_RUNTIME_TABLE} where singleton = 1`,
  ).get() as Record<string, unknown> | undefined;
  if (!row) return null;
  const countsValid = [
    "bytesRead",
    "recordsParsed",
    "eventsAppended",
    "deferredBytes",
    "deferredGenerations",
    "excludedGenerations",
    "errorCount",
    "yields",
  ].every((key) => Number.isSafeInteger(row[key]) && Number(row[key]) >= 0);
  const exhaustedByValid =
    row.budgetExhaustedBy === null ||
    ["bytes", "records", "events", "wall"].includes(String(row.budgetExhaustedBy));
  const timestampsValid =
    typeof row.completedAt === "string" &&
    Number.isFinite(Date.parse(row.completedAt)) &&
    (row.lastYieldAt === null ||
      (typeof row.lastYieldAt === "string" && Number.isFinite(Date.parse(row.lastYieldAt))));
  if (!countsValid || !exhaustedByValid || !timestampsValid) {
    return { status: "invalid", reason: "automatic_capture_runtime_state_invalid" };
  }
  return row;
}

function maintenanceState(database: Database.Database, key: string) {
  return (
    database
      .prepare(`select value from maintenance_state where key = ?`)
      .get(key) as { value: string } | undefined
  )?.value;
}

function setMaintenanceState(database: Database.Database, key: string, value: string) {
  database
    .prepare(
      `insert into maintenance_state (key, value, updated_at)
       values (?, ?, ?)
       on conflict(key) do update set value = excluded.value, updated_at = excluded.updated_at`,
    )
    .run(key, value, new Date().toISOString());
}

function deleteMaintenanceState(database: Database.Database, key: string) {
  database.prepare(`delete from maintenance_state where key = ?`).run(key);
}

/** Stable across object insertion order; changes when any priced-model fact changes. */
export function pricingCatalogFingerprint() {
  const canonical = Object.entries(MODEL_PRICING)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([model, price]) => [
      model,
      price.input,
      price.cachedInput,
      price.output,
      price.vendor,
      price.asOf,
    ]);
  return createHash("sha256").update(JSON.stringify(canonical)).digest("hex");
}

export type RepricingMaintenanceResult = {
  catalogFingerprint: string;
  catalogChanged: boolean;
  backfillComplete: boolean;
  legacyRowsVisited: number;
  candidateRowsVisited: number;
  rowsVisited: number;
  repriced: number;
};

/**
 * Price only durable dirty candidates. A catalog change opens one bounded,
 * resumable rowid walk so legacy null-cost rows get one reconsideration; the
 * applied fingerprint then closes that walk until pricing actually changes.
 */
export function runRepricingMaintenance(
  database: Database.Database,
  options: { backfillLimit?: number; candidateLimit?: number } = {},
): RepricingMaintenanceResult {
  const backfillLimit = Math.max(1, Math.min(options.backfillLimit ?? 5_000, 25_000));
  const candidateLimit = Math.max(1, Math.min(options.candidateLimit ?? 5_000, 25_000));
  const catalogFingerprint = pricingCatalogFingerprint();

  return database.transaction(() => {
    const applied = maintenanceState(database, PRICING_VERSION_KEY);
    let target = maintenanceState(database, PRICING_TARGET_KEY);
    const catalogChanged = applied !== catalogFingerprint;
    let legacyRowsVisited = 0;
    let backfillComplete = !catalogChanged;

    if (catalogChanged) {
      if (target !== catalogFingerprint) {
        target = catalogFingerprint;
        setMaintenanceState(database, PRICING_TARGET_KEY, target);
        setMaintenanceState(database, PRICING_CURSOR_KEY, "");
      }
      const cursor = maintenanceState(database, PRICING_CURSOR_KEY) ?? "";
      const legacyRows = database
        .prepare(
          `select id
           from buffered_events indexed by idx_events_unpriced_usage
           where id > @cursor
             and event_type in ('usage_rollout','usage_transcript')
             and cost_usd is null and model is not null
           order by id
           limit @limit`,
        )
        .all({ cursor, limit: backfillLimit }) as Array<{ id: string }>;
      legacyRowsVisited = legacyRows.length;
      const enqueue = database.prepare(
        `insert into reprice_dirty_events (event_id, queued_at)
         values (?, ?)
         on conflict(event_id) do nothing`,
      );
      const queuedAt = new Date().toISOString();
      for (const row of legacyRows) enqueue.run(row.id, queuedAt);

      if (legacyRows.length < backfillLimit) {
        setMaintenanceState(database, PRICING_VERSION_KEY, catalogFingerprint);
        deleteMaintenanceState(database, PRICING_TARGET_KEY);
        deleteMaintenanceState(database, PRICING_CURSOR_KEY);
        backfillComplete = true;
      } else {
        setMaintenanceState(
          database,
          PRICING_CURSOR_KEY,
          legacyRows[legacyRows.length - 1]!.id,
        );
      }
    }

    const candidates = database
      .prepare(
        `select q.event_id as id, e.model,
           e.input_tokens as inputTokens, e.output_tokens as outputTokens,
           e.cache_read_tokens as cacheReadTokens
         from reprice_dirty_events q
         left join buffered_events e on e.id = q.event_id
         order by q.queued_at, q.event_id
         limit ?`,
      )
      .all(candidateLimit) as Array<{
      id: string;
      model: string | null;
      inputTokens: number | null;
      outputTokens: number | null;
      cacheReadTokens: number | null;
    }>;
    const apply = database.prepare(
      `update buffered_events set
         cost_usd = @costUsd,
         payload_json = json_set(
           payload_json,
           '$.costUsd', @costUsd,
           '$.metadata.costEstimated', json('true')
         )
       where id = @id and cost_usd is null`,
    );
    const remove = database.prepare(`delete from reprice_dirty_events where event_id = ?`);
    let repriced = 0;
    for (const row of candidates) {
      if (row.model) {
        const priced = estimateCostUsd({
          model: row.model,
          inputTokens: row.inputTokens ?? 0,
          outputTokens: row.outputTokens ?? 0,
          cacheReadTokens: row.cacheReadTokens ?? 0,
        });
        if (priced) repriced += apply.run({ id: row.id, costUsd: priced.costUsd }).changes;
      }
      // Unknown models wait for a catalog-fingerprint change, not a minute
      // timer. Missing/deleted events are stale queue receipts and also leave.
      remove.run(row.id);
    }

    return {
      catalogFingerprint,
      catalogChanged,
      backfillComplete,
      legacyRowsVisited,
      candidateRowsVisited: candidates.length,
      rowsVisited: legacyRowsVisited + candidates.length,
      repriced,
    };
  })();
}

export type RepoEnrichmentMaintenanceResult = {
  backfillComplete: boolean;
  legacyRowsVisited: number;
  sessionsVisited: number;
  candidateRowsVisited: number;
  rowsVisited: number;
  backward: number;
  forward: number;
};

type RepoCandidate = {
  rowid: number;
  id: string;
  backwardRepoHash: string | null;
  backwardBranchHash: string | null;
  forwardRepoHash: string | null;
  forwardBranchHash: string | null;
};

/**
 * Stitch only sessions named by the durable dirty queue. Each session has a
 * rowid cursor: unresolved candidates are visited once, removed when drained,
 * and reconsidered only when a later linkage/token mutation resets the cursor.
 */
export function runRepoEnrichmentMaintenance(
  database: Database.Database,
  options: { legacyBackfillLimit?: number; sessionLimit?: number; eventLimit?: number } = {},
): RepoEnrichmentMaintenanceResult {
  const legacyBackfillLimit = Math.max(
    1,
    Math.min(options.legacyBackfillLimit ?? 5_000, 25_000),
  );
  const sessionLimit = Math.max(1, Math.min(options.sessionLimit ?? 50, 500));
  const eventLimit = Math.max(1, Math.min(options.eventLimit ?? 5_000, 25_000));

  return database.transaction(() => {
    let backfillComplete = maintenanceState(database, REPO_BACKFILL_COMPLETE_KEY) === "1";
    let legacyRowsVisited = 0;
    if (!backfillComplete) {
      const cursor = maintenanceState(database, REPO_BACKFILL_CURSOR_KEY) ?? "";
      const legacyRows = database
        .prepare(
          `select id, session_id as sessionId
           from buffered_events indexed by idx_events_repo_enrichment_seed
           where id > @cursor and session_id is not null and (
             repo_hash is not null or input_tokens is not null or
             output_tokens is not null or cost_usd is not null
           )
           order by id
           limit @limit`,
        )
        .all({ cursor, limit: legacyBackfillLimit }) as Array<{
        id: string;
        sessionId: string;
      }>;
      legacyRowsVisited = legacyRows.length;
      const enqueue = database.prepare(
        `insert into repo_enrichment_dirty
           (session_id, cursor_rowid, queued_at, updated_at)
         values (?, 0, ?, ?)
         on conflict(session_id) do nothing`,
      );
      const queuedAt = new Date().toISOString();
      for (const row of legacyRows) enqueue.run(row.sessionId, queuedAt, queuedAt);

      if (legacyRows.length < legacyBackfillLimit) {
        setMaintenanceState(database, REPO_BACKFILL_COMPLETE_KEY, "1");
        deleteMaintenanceState(database, REPO_BACKFILL_CURSOR_KEY);
        backfillComplete = true;
      } else {
        setMaintenanceState(
          database,
          REPO_BACKFILL_CURSOR_KEY,
          legacyRows[legacyRows.length - 1]!.id,
        );
      }
    }

    const sessions = database
      .prepare(
        `select session_id as sessionId, cursor_rowid as cursorRowid
         from repo_enrichment_dirty
         order by queued_at, session_id
         limit ?`,
      )
      .all(sessionLimit) as Array<{ sessionId: string; cursorRowid: number }>;
    const selectCandidates = database.prepare(
      `select e.rowid, e.id,
         (select r.repo_hash from buffered_events r
          where r.session_id = e.session_id and r.repo_hash is not null
            and r.observed_at <= e.observed_at
          order by r.observed_at desc, r.rowid desc limit 1) as backwardRepoHash,
         (select r.branch_hash from buffered_events r
          where r.session_id = e.session_id and r.repo_hash is not null
            and r.observed_at <= e.observed_at
          order by r.observed_at desc, r.rowid desc limit 1) as backwardBranchHash,
         (select r.repo_hash from buffered_events r
          where r.session_id = e.session_id and r.repo_hash is not null
            and r.observed_at > e.observed_at
            and (strftime('%s', r.observed_at) - strftime('%s', e.observed_at)) <= 600
          order by r.observed_at, r.rowid limit 1) as forwardRepoHash,
         (select r.branch_hash from buffered_events r
          where r.session_id = e.session_id and r.repo_hash is not null
            and r.observed_at > e.observed_at
            and (strftime('%s', r.observed_at) - strftime('%s', e.observed_at)) <= 600
          order by r.observed_at, r.rowid limit 1) as forwardBranchHash
       from buffered_events e
       where e.session_id = @sessionId and e.rowid > @cursorRowid
         and e.repo_hash is null
         and (e.input_tokens is not null or e.output_tokens is not null or e.cost_usd is not null)
       order by e.rowid
       limit @limit`,
    );
    const apply = database.prepare(
      `update buffered_events set
         repo_hash = @repoHash,
         branch_hash = @branchHash,
         payload_json = json_set(payload_json, '$.metadata.repoStitched', json('true'))
       where id = @id and repo_hash is null`,
    );
    const removeSession = database.prepare(
      `delete from repo_enrichment_dirty where session_id = ?`,
    );
    const advanceSession = database.prepare(
      `update repo_enrichment_dirty set cursor_rowid = ?, updated_at = ? where session_id = ?`,
    );

    let remainingEvents = eventLimit;
    let sessionsVisited = 0;
    let candidateRowsVisited = 0;
    let backward = 0;
    let forward = 0;
    for (const session of sessions) {
      if (remainingEvents <= 0) break;
      sessionsVisited += 1;
      const candidates = selectCandidates.all({
        sessionId: session.sessionId,
        cursorRowid: session.cursorRowid,
        limit: remainingEvents,
      }) as RepoCandidate[];
      candidateRowsVisited += candidates.length;
      remainingEvents -= candidates.length;

      for (const row of candidates) {
        const repoHash = row.backwardRepoHash ?? row.forwardRepoHash;
        if (!repoHash) continue;
        const direction = row.backwardRepoHash ? "backward" : "forward";
        const branchHash = row.backwardRepoHash
          ? row.backwardBranchHash
          : row.forwardBranchHash;
        const changed = apply.run({ id: row.id, repoHash, branchHash }).changes;
        if (direction === "backward") backward += changed;
        else forward += changed;
      }

      if (candidates.length < remainingEvents + candidates.length) {
        // The query drained this session. Unresolved candidates leave the
        // queue too; a later linkage mutation atomically re-adds the session.
        removeSession.run(session.sessionId);
      } else if (candidates.length > 0) {
        // The event budget ended exactly at this boundary. Advance past every
        // candidate, including unresolved rows, so they cannot spin forever.
        advanceSession.run(
          candidates[candidates.length - 1]!.rowid,
          new Date().toISOString(),
          session.sessionId,
        );
      } else {
        removeSession.run(session.sessionId);
      }
    }

    return {
      backfillComplete,
      legacyRowsVisited,
      sessionsVisited,
      candidateRowsVisited,
      rowsVisited: legacyRowsVisited + candidateRowsVisited,
      backward,
      forward,
    };
  })();
}

export type CollectorMaintenanceRunResult = {
  recentOnly: true;
  rollout: RolloutScanResult;
  transcript: TranscriptScanResult;
  reconciliation: CodexReconciliationResult;
  repricing: RepricingMaintenanceResult;
  enrichment: RepoEnrichmentMaintenanceResult;
  projection?: ReturnType<LocalEventBuffer["projection"]["runMaintenance"]>;
  projectionDrain?: ProjectionDrainResult;
  rawEventWrites: number;
  postCaptureDeferred?: string[];
};

export type ProjectionDrainResult = {
  slices: number;
  yields: number;
  migrationRowsVisited: number;
  activeMs: number;
  maxSlices: number;
  maxActiveMs: number;
  cadenceSeconds: number;
  remainingRowidUpperBound: number;
  estimatedMinutesUpperBound: number;
  stillMigrating: boolean;
};

const PROJECTION_DRAIN_MAX_SLICES=40;
const PROJECTION_DRAIN_MAX_ACTIVE_MS=2_000;
const PROJECTION_CADENCE_SECONDS=60;

function projectionMigrationRemaining(status:ReturnType<LocalEventBuffer["projection"]["status"]>){
  const high=status.backfill.highWater??0;
  const metricHigh=status.backfill.metricHighWater??0;
  return Math.max(0,high-status.backfill.cursor)+
    Math.max(0,high-status.backfill.parityCursor)+
    Math.max(0,metricHigh-status.backfill.metricCursor);
}

/**
 * Cooperative migration acceleration. Every synchronous transaction retains
 * the projection's 1,000-row bound; setImmediate gives capture/server work a
 * turn between slices, and the active-time cap prevents a boot-time CPU loop.
 */
export async function drainProjectionMigration(
  projection:LocalEventBuffer["projection"],
  options:{maxSlices?:number;maxActiveMs?:number;cadenceSeconds?:number;signal?:AbortSignal;budget?:CaptureWorkBudget}={},
){
  const maxSlices=Math.max(1,Math.min(options.maxSlices??PROJECTION_DRAIN_MAX_SLICES,100));
  const maxActiveMs=Math.max(1,Math.min(options.maxActiveMs??PROJECTION_DRAIN_MAX_ACTIVE_MS,5_000));
  const cadenceSeconds=Math.max(1,options.cadenceSeconds??PROJECTION_CADENCE_SECONDS);
  let slices=0,yields=0,migrationRowsVisited=0,activeMs=0;
  let receipt:ReturnType<LocalEventBuffer["projection"]["runMaintenance"]>|undefined;
  while(slices<maxSlices && !options.signal?.aborted && (options.budget?.canStart(5) ?? true)){
    if(slices>0){await new Promise<void>((resolve)=>setImmediate(resolve));yields++;}
    const started=performance.now();
    receipt=projection.runMaintenance();
    activeMs+=performance.now()-started;
    slices++;
    migrationRowsVisited+=receipt.backfillRowsVisited+receipt.parityRowsVisited+receipt.metricRowsVisited;
    const status=projection.status();
    const stillMigrating=!status.backfill.complete||!status.backfill.parityComplete||!status.backfill.metricComplete;
    if(!stillMigrating||activeMs>=maxActiveMs)break;
  }
  const status=projection.status(),remainingRowidUpperBound=projectionMigrationRemaining(status);
  const capacityPerCadence=1_000*maxSlices;
  return {receipt:receipt!,drain:{slices,yields,migrationRowsVisited,
    activeMs:Number(activeMs.toFixed(3)),maxSlices,maxActiveMs,cadenceSeconds,remainingRowidUpperBound,
    estimatedMinutesUpperBound:Math.ceil(remainingRowidUpperBound/capacityPerCadence*cadenceSeconds/60),
    stillMigrating:!status.backfill.complete||!status.backfill.parityComplete||!status.backfill.metricComplete} satisfies ProjectionDrainResult};
}

export class CollectorMaintenance {
  private current: {
    phase: "baseline" | "capture";
    source: "codex" | "claude_code";
    startedAt: string;
    budget: CaptureWorkBudget;
  } | null = null;
  private lastBudget: CaptureBudgetStatus | null = null;

  constructor(
    private readonly buffer: LocalEventBuffer,
    private readonly rolloutTailer: RolloutTailer,
    private readonly transcriptTailer: TranscriptTailer,
    private readonly signal?: AbortSignal,
  ) {
    ensureAutomaticCaptureRuntimeState(this.buffer.database);
  }

  status() {
    return {
      inFlight: this.current !== null,
      phase: this.current?.phase ?? null,
      source: this.current?.source ?? null,
      startedAt: this.current?.startedAt ?? null,
      budget: this.current?.budget.status() ?? this.lastBudget,
      baseline: captureBaselineStatus(this.buffer.database),
      lastCompleted: automaticCaptureRuntimeStatus(this.buffer.database),
    };
  }

  close() {
    this.rolloutTailer.close();
    this.transcriptTailer.close();
  }

  async runRecent(): Promise<CollectorMaintenanceRunResult> {
    const budget = new CaptureWorkBudget();
    const baselineAtStart = captureBaselineStatus(this.buffer.database);
    // Completed source snapshots stay armed while a per-generation ambiguity
    // blocks aggregate readiness. Capture classification remains globally
    // fail-closed, but it must inspect a trustworthy same-path replacement to
    // resolve that receipt; sending complete sources back through baseline is
    // a dead end because their baseline branches intentionally return early.
    const phase = baselineAtStart.sources.every((source) => source.status === "complete")
      ? "capture" as const
      : "baseline" as const;
    // Advance the turn before work starts. If this process dies in a slow
    // source, the next process begins with the other source instead of
    // manufacturing starvation across restarts.
    const firstSource = maintenanceState(
      this.buffer.database,
      AUTOMATIC_CAPTURE_SOURCE_TURN_KEY,
    ) === "claude_code" ? "claude_code" as const : "codex" as const;
    setMaintenanceState(
      this.buffer.database,
      AUTOMATIC_CAPTURE_SOURCE_TURN_KEY,
      firstSource === "codex" ? "claude_code" : "codex",
    );
    const startedAt = new Date().toISOString();
    let rollout: RolloutScanResult | undefined;
    let transcript: TranscriptScanResult | undefined;
    const runRollout = async () => {
      this.current = { phase, source: "codex", startedAt, budget };
      return this.rolloutTailer.scan({
        scope: "recent",
        now: new Date(startedAt),
        automatic: { phase, budget },
        signal: this.signal,
      });
    };
    const runTranscript = async () => {
      this.current = { phase, source: "claude_code", startedAt, budget };
      return this.transcriptTailer.scan({
        scope: "recent",
        now: new Date(startedAt),
        automatic: { phase, budget },
        signal: this.signal,
      });
    };
    try {
      if (firstSource === "claude_code") {
        transcript = await runTranscript();
        rollout = await runRollout();
      } else {
        rollout = await runRollout();
        transcript = await runTranscript();
      }
      this.lastBudget = budget.status();
      const errorCount =
        rollout.discoveryErrors + rollout.statErrors + rollout.readErrors + rollout.parseErrors + rollout.unresolvedRecords +
        transcript.discoveryErrors + transcript.statErrors + transcript.readErrors + transcript.parseErrors + transcript.unresolvedRecords;
      if (this.signal?.aborted || rollout.aborted || transcript.aborted) {
        throw new Error("automatic_maintenance_aborted");
      }
    } catch (error) {
      if (!rollout || !transcript) {
        recordAutomaticCaptureRuntimeState(this.buffer.database, phase, "failed", budget.status());
      }
      throw error;
    } finally {
      this.lastBudget = budget.status();
      this.current = null;
    }
    if (!rollout || !transcript) throw new Error("automatic_maintenance_result_missing");
    const postCaptureDeferred: string[] = [];
    let reconciliation: CodexReconciliationResult = {
      backfillComplete: false, legacyRowsVisited: 0, contextRowsVisited: 0,
      candidateRowsVisited: 0, rowsVisited: 0, rowsChanged: 0, stitched: 0,
      priced: 0, sliceDurationMs: 0, timeBudgetExhausted: true,
    };
    if (!this.signal?.aborted && budget.canStart(15)) {
      reconciliation = runCodexReconciliationMaintenance(this.buffer.database, {
        legacyRowLimit: 64,
        legacyChunkLimit: 64,
        contextWindowLimit: 2,
        contextRowLimit: 64,
        candidateLimit: 32,
        freshCandidateLimit: 16,
        timeLimitMs: Math.max(1, Math.min(25, Math.floor(budget.remainingWallMs() - 5))),
      });
    } else postCaptureDeferred.push("reconciliation");
    let repricing: RepricingMaintenanceResult = {
      catalogFingerprint: pricingCatalogFingerprint(), catalogChanged: false,
      backfillComplete: false, legacyRowsVisited: 0, candidateRowsVisited: 0,
      rowsVisited: 0, repriced: 0,
    };
    if (!this.signal?.aborted && budget.canStart(12)) {
      repricing = runRepricingMaintenance(this.buffer.database, {
        backfillLimit: 32,
        candidateLimit: 32,
      });
    } else postCaptureDeferred.push("repricing");
    let enrichment: RepoEnrichmentMaintenanceResult = {
      backfillComplete: false, legacyRowsVisited: 0, sessionsVisited: 0,
      candidateRowsVisited: 0, rowsVisited: 0, backward: 0, forward: 0,
    };
    if (!this.signal?.aborted && budget.canStart(12)) {
      enrichment = runRepoEnrichmentMaintenance(this.buffer.database, {
        legacyBackfillLimit: 32,
        sessionLimit: 4,
        eventLimit: 32,
      });
    } else postCaptureDeferred.push("enrichment");
    if (rollout.activity && !this.signal?.aborted && budget.canStart(3)) {
      this.buffer.projection.recordCaptureActivity({ source: "codex", ...rollout.activity });
    } else postCaptureDeferred.push("codex_activity");
    if (transcript.activity && !this.signal?.aborted && budget.canStart(3)) {
      this.buffer.projection.recordCaptureActivity({ source: "claude_code", ...transcript.activity });
    } else postCaptureDeferred.push("claude_activity");
    const drained = !this.signal?.aborted && budget.canStart(10)
      ? await drainProjectionMigration(this.buffer.projection, {
          maxSlices: 1,
          maxActiveMs: Math.max(1, Math.min(25, Math.floor(budget.remainingWallMs() - 5))),
          signal: this.signal,
          budget,
        })
      : null;
    if (!drained) postCaptureDeferred.push("projection");
    const errorCount =
      rollout.discoveryErrors + rollout.statErrors + rollout.readErrors + rollout.parseErrors + rollout.unresolvedRecords +
      transcript.discoveryErrors + transcript.statErrors + transcript.readErrors + transcript.parseErrors + transcript.unresolvedRecords;
    const finalBudget = budget.status();
    const baselineProgress = captureBaselineStatus(this.buffer.database).progress.state;
    const baselineIncomplete = phase === "baseline" && baselineProgress !== "complete";
    const baselineFailed = phase === "baseline" &&
      (baselineProgress === "failed" || baselineProgress === "ambiguous");
    const deferred = finalBudget.exhausted || postCaptureDeferred.length > 0 ||
      rollout.deferredGenerations > 0 || transcript.deferredGenerations > 0;
    recordAutomaticCaptureRuntimeState(
      this.buffer.database,
      phase,
      this.signal?.aborted ? "aborted" : baselineFailed ? "failed" : errorCount > 0 ? "complete_with_errors" :
        baselineIncomplete ? "baseline_in_progress" : deferred ? "deferred" : "complete",
      finalBudget,
      rollout,
      transcript,
    );
    return {
      recentOnly: true,
      rollout,
      transcript,
      reconciliation,
      repricing,
      enrichment,
      ...(drained ? { projection: drained.receipt, projectionDrain: drained.drain } : {}),
      rawEventWrites: rollout.eventsAppended + transcript.eventsAppended,
      postCaptureDeferred,
    };
  }
}

export type MaintenanceSchedulerStatus = {
  accepting: boolean;
  inFlight: boolean;
  pending: boolean;
  triggerCount: number;
  runCount: number;
  coalescedTriggerCount: number;
  overlappingJobs: number;
  maxConcurrentJobs: number;
  failedRuns: number;
  rolloutFilesRead: number;
  transcriptFilesRead: number;
  rawEventWrites: number;
  repriceRowsVisited: number;
  reconciliationRowsVisited: number;
  enrichmentRowsVisited: number;
  lastStartedAt: string | null;
  lastCompletedAt: string | null;
  lastRun: CollectorMaintenanceRunResult | null;
};

type DrainWaiter = {
  resolve: (results: CollectorMaintenanceRunResult[]) => void;
  reject: (error: unknown) => void;
};

/**
 * One daemon entrypoint for boot, interval, and internal recent-tail triggers.
 * Concurrent requests collapse into one pending follow-up. There is no full
 * mode in this scheduler; only the explicit scan-rollouts/scan-transcripts CLI
 * commands may request full history.
 */
export class CoalescingMaintenanceScheduler {
  private accepting = true;
  private running = false;
  private pending = false;
  private waiters: DrainWaiter[] = [];
  private triggerCount = 0;
  private runCount = 0;
  private coalescedTriggerCount = 0;
  private overlappingJobs = 0;
  private activeJobs = 0;
  private maxConcurrentJobs = 0;
  private failedRuns = 0;
  private rolloutFilesRead = 0;
  private transcriptFilesRead = 0;
  private rawEventWrites = 0;
  private repriceRowsVisited = 0;
  private reconciliationRowsVisited = 0;
  private enrichmentRowsVisited = 0;
  private lastStartedAt: string | null = null;
  private lastCompletedAt: string | null = null;
  private lastRun: CollectorMaintenanceRunResult | null = null;
  private idleWaiters: Array<() => void> = [];

  constructor(
    private readonly runJob: () => Promise<CollectorMaintenanceRunResult>,
  ) {}

  trigger() {
    if (!this.accepting) {
      return Promise.reject(new Error("maintenance_scheduler_stopping"));
    }
    this.triggerCount += 1;
    this.pending = true;
    if (this.running) this.coalescedTriggerCount += 1;

    const promise = new Promise<CollectorMaintenanceRunResult[]>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
    if (!this.running) {
      this.running = true;
      void this.drain();
    }
    return promise;
  }

  status(): MaintenanceSchedulerStatus {
    return {
      accepting: this.accepting,
      inFlight: this.running,
      pending: this.pending,
      triggerCount: this.triggerCount,
      runCount: this.runCount,
      coalescedTriggerCount: this.coalescedTriggerCount,
      overlappingJobs: this.overlappingJobs,
      maxConcurrentJobs: this.maxConcurrentJobs,
      failedRuns: this.failedRuns,
      rolloutFilesRead: this.rolloutFilesRead,
      transcriptFilesRead: this.transcriptFilesRead,
      rawEventWrites: this.rawEventWrites,
      repriceRowsVisited: this.repriceRowsVisited,
      reconciliationRowsVisited: this.reconciliationRowsVisited,
      enrichmentRowsVisited: this.enrichmentRowsVisited,
      lastStartedAt: this.lastStartedAt,
      lastCompletedAt: this.lastCompletedAt,
      lastRun: this.lastRun,
    };
  }

  waitForIdle() {
    if (!this.running) return Promise.resolve();
    return new Promise<void>((resolve) => this.idleWaiters.push(resolve));
  }

  stopAccepting() {
    this.accepting = false;
    this.pending = false;
  }

  private async drain() {
    const results: CollectorMaintenanceRunResult[] = [];
    let firstError: unknown;
    while (this.pending) {
      this.pending = false;
      this.runCount += 1;
      this.lastStartedAt = new Date().toISOString();
      if (this.activeJobs > 0) this.overlappingJobs += 1;
      this.activeJobs += 1;
      this.maxConcurrentJobs = Math.max(this.maxConcurrentJobs, this.activeJobs);
      try {
        const result = await this.runJob();
        this.lastRun = result;
        this.rolloutFilesRead += result.rollout.filesRead;
        this.transcriptFilesRead += result.transcript.filesRead;
        this.rawEventWrites += result.rawEventWrites;
        this.repriceRowsVisited += result.repricing.rowsVisited;
        this.reconciliationRowsVisited += result.reconciliation.rowsVisited;
        this.enrichmentRowsVisited += result.enrichment.rowsVisited;
        results.push(result);
      } catch (error) {
        this.failedRuns += 1;
        firstError ??= error;
      } finally {
        this.activeJobs -= 1;
        this.lastCompletedAt = new Date().toISOString();
      }
    }

    this.running = false;
    const idleWaiters = this.idleWaiters;
    this.idleWaiters = [];
    for (const resolve of idleWaiters) resolve();
    const waiters = this.waiters;
    this.waiters = [];
    for (const waiter of waiters) {
      if (firstError !== undefined) waiter.reject(firstError);
      else waiter.resolve(results);
    }
  }
}

/**
 * The only automatic boot/interval entrypoint. Its signature cannot express a
 * full-history request, so daemon scheduling stays recent-only by construction.
 */
export function requestAutomaticRecentMaintenance(
  scheduler: CoalescingMaintenanceScheduler,
) {
  return scheduler.trigger();
}

export const AUTOMATIC_BASELINE_STARTUP_INTERVAL_MS = 5_000;
export const AUTOMATIC_MAINTENANCE_NORMAL_INTERVAL_MS = 60_000;

export type AutomaticMaintenanceCadenceStatus = {
  accepting: boolean;
  inFlight: boolean;
  retryClass: "boot" | "startup" | "normal" | null;
  nextRetryAt: string | null;
  startupIntervalMs: number;
  normalIntervalMs: number;
  activeBudgetMs: number;
  maximumStartupDutyCycle: number;
  triggerCount: number;
  failedTriggers: number;
};

export type AutomaticMaintenanceCadenceTimer = {
  now: () => number;
  setTimeout: (callback: () => void, delayMs: number) => unknown;
  clearTimeout: (handle: unknown) => void;
};

/**
 * The daemon has one maintenance timer owner. Baseline follow-ups use the
 * fixed startup interval only after observable baseline progress. Stalled,
 * complete, failed, ambiguous and pre-start states fall back to the ordinary
 * cadence. Scheduling happens after the coalesced run drains, so callbacks do
 * not accumulate during a slow filesystem slice.
 */
export class AutomaticMaintenanceCadence {
  private accepting = true;
  private inFlight = false;
  private timer: unknown | null = null;
  private retryClass: "boot" | "startup" | "normal" | null = null;
  private nextRetryAt: string | null = null;
  private triggerCount = 0;
  private failedTriggers = 0;

  constructor(
    private readonly scheduler: CoalescingMaintenanceScheduler,
    private readonly baselineStatus: () => ReturnType<typeof captureBaselineStatus>,
    private readonly options: {
      startupIntervalMs?: number;
      normalIntervalMs?: number;
      activeBudgetMs?: number;
      onError?: (error: unknown) => void;
      timer?: AutomaticMaintenanceCadenceTimer;
    } = {},
  ) {}

  start() {
    if (!this.accepting || this.timer || this.inFlight) return;
    this.schedule("boot");
  }

  stop() {
    this.accepting = false;
    if (this.timer) this.timerApi().clearTimeout(this.timer);
    this.timer = null;
    this.retryClass = null;
    this.nextRetryAt = null;
  }

  status(): AutomaticMaintenanceCadenceStatus {
    const startupIntervalMs = this.startupIntervalMs();
    const activeBudgetMs = this.activeBudgetMs();
    return {
      accepting: this.accepting,
      inFlight: this.inFlight,
      // While a run is active the next retry has not been selected yet; null
      // is more truthful and avoids another aggregate SQLite status query on
      // every HTTP poll.
      retryClass: this.retryClass,
      nextRetryAt: this.nextRetryAt,
      startupIntervalMs,
      normalIntervalMs: this.normalIntervalMs(),
      activeBudgetMs,
      maximumStartupDutyCycle: Number((activeBudgetMs / startupIntervalMs).toFixed(4)),
      triggerCount: this.triggerCount,
      failedTriggers: this.failedTriggers,
    };
  }

  private startupIntervalMs() {
    return Math.max(1, this.options.startupIntervalMs ?? AUTOMATIC_BASELINE_STARTUP_INTERVAL_MS);
  }

  private normalIntervalMs() {
    return Math.max(1, this.options.normalIntervalMs ?? AUTOMATIC_MAINTENANCE_NORMAL_INTERVAL_MS);
  }

  private activeBudgetMs() {
    return Math.max(1, this.options.activeBudgetMs ?? 200);
  }

  private timerApi(): AutomaticMaintenanceCadenceTimer {
    return this.options.timer ?? {
      now: () => Date.now(),
      setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
      clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
    };
  }

  private classifyRetry(
    before: ReturnType<typeof captureBaselineStatus>["progress"],
    after: ReturnType<typeof captureBaselineStatus>["progress"],
    discoveryAdvanced: boolean,
  ): "startup" | "normal" {
    const advanced =
      (before.state === "not_established" && after.state === "in_progress") ||
      after.sourcesComplete > before.sourcesComplete ||
      after.filesBaselined > before.filesBaselined ||
      after.pendingMetadata !== before.pendingMetadata ||
      discoveryAdvanced;
    return after.state === "in_progress" && advanced ? "startup" : "normal";
  }

  private schedule(retryClass: "boot" | "startup" | "normal") {
    if (!this.accepting || this.timer) return;
    const delay = retryClass === "normal" ? this.normalIntervalMs() : this.startupIntervalMs();
    this.retryClass = retryClass;
    const timerApi = this.timerApi();
    this.nextRetryAt = new Date(timerApi.now() + delay).toISOString();
    this.timer = timerApi.setTimeout(() => {
      this.timer = null;
      this.retryClass = null;
      this.nextRetryAt = null;
      void this.fire();
    }, delay);
    if (
      typeof this.timer === "object" && this.timer !== null &&
      "unref" in this.timer && typeof (this.timer as { unref?: unknown }).unref === "function"
    ) (this.timer as { unref: () => void }).unref();
  }

  private async fire() {
    if (!this.accepting || this.inFlight) return;
    this.inFlight = true;
    this.triggerCount += 1;
    let failed = false;
    let discoveryAdvanced = false;
    const baselineBefore = this.baselineStatus().progress;
    try {
      const results = await requestAutomaticRecentMaintenance(this.scheduler);
      discoveryAdvanced = results.some(
        (result) =>
          result.rollout.activity.discoveryEntries > 0 ||
          result.transcript.activity.discoveryEntries > 0,
      );
    } catch (error) {
      failed = true;
      this.failedTriggers += 1;
      this.options.onError?.(error);
    } finally {
      this.inFlight = false;
      if (this.accepting) {
        const baselineAfter = this.baselineStatus().progress;
        this.schedule(
          failed
            ? "normal"
            : this.classifyRetry(baselineBefore, baselineAfter, discoveryAdvanced),
        );
      }
    }
  }
}
