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
import type { GrokUsageScanResult, GrokUsageTailer } from "./grok-usage-tailer";
import { captureBaselineStatus } from "./capture-baseline";
import {
  applyCaptureCoverage,
  beginCaptureCoverage,
  CAPTURE_COVERAGE_INTERVAL_MS,
  CAPTURE_COVERAGE_MAX_WORK_PER_TURN,
  CAPTURE_COVERAGE_TURN_MS,
  CaptureCoverageWalk,
  finishCaptureCoverage,
  type CaptureCoverageCheck,
} from "./capture-frontier";
import { CaptureWorkBudget, type CaptureBudgetStatus } from "./capture-work-budget";
import type { MaintenanceProgress } from "./maintenance-progress";
import type { MaintenanceJobProgress } from "./maintenance-protocol";
import { DEFAULT_LEARNING_FACT_MAINTENANCE_BATCH } from "./learning-facts";
import { isSessionSyncUploadLeaseError } from "./sqlite-contention";

const PRICING_VERSION_KEY = "pricing_catalog_applied";
const PRICING_TARGET_KEY = "pricing_catalog_backfill_target";
const PRICING_CURSOR_KEY = "pricing_catalog_backfill_cursor";
const REPO_BACKFILL_CURSOR_KEY = "repo_enrichment_backfill_cursor";
const REPO_BACKFILL_COMPLETE_KEY = "repo_enrichment_backfill_complete";
const AUTOMATIC_CAPTURE_SOURCE_TURN_KEY = "automatic_capture_source_turn";
const CAPTURE_COVERAGE_CHECK_KEY = "capture_coverage_checked_at";
/** Room left in a coverage turn for its last batch write and the frontier update. */
const CAPTURE_COVERAGE_TURN_MARGIN_MS = 25;
const AUTOMATIC_CAPTURE_RUNTIME_TABLE = "automatic_capture_runtime_state";
const REPAIR_SERVICE_KEY = "automatic_repair_service_v1";
const REPAIR_STAGES = [
  "projection",
  "reconciliation",
  "repricing",
  "repo_context_suppression",
  "learning_facts",
] as const;
const AUTOMATIC_CAPTURE_FAIRNESS_KEY = "automatic_capture_fairness_v1";
type CaptureSource = "codex" | "claude_code" | "grok";
type RepairStage = typeof REPAIR_STAGES[number];
type RepairService = { next: number; cycles: number; stages: Record<RepairStage, {
  attempts: number; completed: number; failures: number; rowsVisited: number; lastSuccessAt: string | null;
}> };

/**
 * Capture hand-off (eco-6hoxj.163.42). A cadence whose capture leader the
 * clock never let start (repairs or the bookkeeping before capture spent
 * the allowance) hands the next cadence to capture (`captureFirst`) and lets
 * that cadence's leader start its first unit even if the clock is spent
 * again (`leaderDenied`). `deniedCadences` counts such cadences. Path- and
 * content-free.
 */
export type AutomaticCaptureFairness = {
  version: 2;
  captureFirst: boolean;
  leaderDenied: boolean;
  deniedCadences: number;
};

/** One cadence's capture turn, for receipts and proofs. */
export type AutomaticCaptureTurn = {
  captureFirst: boolean;
  order: CaptureSource[];
  leader: CaptureSource;
  /** The leader had its turn, so the rotation moved on to the next source. */
  leaderServed: boolean;
  /** The previous cadence denied its leader, so this one's could start past a spent clock. */
  leaderOverride: boolean;
  /** Cadence clock spent before the leader's turn: repairs on repair-first cadences, and bookkeeping. */
  preCaptureMs: number;
  admitted: Partial<Record<CaptureSource, boolean>>;
  progressed: Partial<Record<CaptureSource, boolean>>;
};

export function automaticRepairServiceStatus(database: Database.Database): RepairService {
  const emptyStages = () => Object.fromEntries(REPAIR_STAGES.map(stage => [stage, {
    attempts: 0, completed: 0, failures: 0, rowsVisited: 0, lastSuccessAt: null,
  }])) as RepairService["stages"];
  const stored = maintenanceState(database, REPAIR_SERVICE_KEY);
  if (stored) {
    const parsed = JSON.parse(stored) as Partial<RepairService>;
    const stages = emptyStages();
    for (const stage of REPAIR_STAGES) {
      const prior = parsed.stages?.[stage];
      if (prior) stages[stage] = { ...stages[stage], ...prior };
    }
    const next = typeof parsed.next === "number" && Number.isSafeInteger(parsed.next) && parsed.next >= 0
      ? parsed.next
      : 0;
    const cycles = typeof parsed.cycles === "number" && Number.isSafeInteger(parsed.cycles) && parsed.cycles >= 0
      ? parsed.cycles
      : 0;
    return {
      next,
      cycles,
      stages,
    };
  }
  return { next: 0, cycles: 0, stages: emptyStages() };
}

export function automaticCaptureFairnessStatus(database: Database.Database): AutomaticCaptureFairness {
  try {
    const stored = JSON.parse(maintenanceState(database, AUTOMATIC_CAPTURE_FAIRNESS_KEY) ?? "null") as
      { version?: unknown; captureFirst?: unknown; leaderDenied?: unknown; deniedCadences?: unknown } | null;
    // Version 1 (round 2) also carried a debt ledger; only its hand-off remains.
    if (stored?.version === 1 || stored?.version === 2) {
      return {
        version: 2,
        captureFirst: stored.captureFirst === true,
        leaderDenied: stored.leaderDenied === true,
        deniedCadences: Number.isSafeInteger(stored.deniedCadences) && (stored.deniedCadences as number) >= 0
          ? stored.deniedCadences as number
          : 0,
      };
    }
  } catch {
    // An unreadable hand-off is none: the parity alternation still serves capture.
  }
  return { version: 2, captureFirst: false, leaderDenied: false, deniedCadences: 0 };
}

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
  grok?: GrokUsageScanResult,
) {
  ensureAutomaticCaptureRuntimeState(database);
  type SourceResult = RolloutScanResult | TranscriptScanResult | GrokUsageScanResult;
  const results = [rollout, transcript, grok].filter(Boolean) as SourceResult[];
  const sum = (read: (result: SourceResult) => number) =>
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
         cost_kind = 'estimated',
         payload_json = json_set(
           payload_json,
           '$.costUsd', @costUsd,
           '$.costKind', 'estimated',
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
  }).immediate();
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
  sessionId: string;
  observedAt: string;
};

type RepoNeighbor = {
  id: string;
  repoHash: string | null;
  branchHash: string | null;
  observedAt: string;
};

/**
 * Stitch only sessions named by the durable dirty queue. Each session has a
 * rowid cursor: unresolved candidates are visited once, removed when drained,
 * and reconsidered only when a later linkage/token mutation resets the cursor.
 */
export function runRepoEnrichmentMaintenance(
  database: Database.Database,
  options: {
    legacyBackfillLimit?: number;
    sessionLimit?: number;
    eventLimit?: number;
    neighborLimit?: number;
    skipLegacyBackfill?: boolean;
  } = {},
): RepoEnrichmentMaintenanceResult {
  const legacyBackfillLimit = Math.max(
    1,
    Math.min(options.legacyBackfillLimit ?? 5_000, 25_000),
  );
  const sessionLimit = Math.max(1, Math.min(options.sessionLimit ?? 50, 500));
  const eventLimit = Math.max(1, Math.min(options.eventLimit ?? 5_000, 25_000));
  const neighborLimit = Math.max(1, Math.min(options.neighborLimit ?? 50, 250));

  return database.transaction(() => {
    let backfillComplete = maintenanceState(database, REPO_BACKFILL_COMPLETE_KEY) === "1";
    let legacyRowsVisited = 0;
    if (!backfillComplete && !options.skipLegacyBackfill) {
      const cursor = maintenanceState(database, REPO_BACKFILL_CURSOR_KEY) ?? "";
      const legacyRows = database
        .prepare(
          `select id, session_id as sessionId
           from buffered_events indexed by idx_events_repo_enrichment_seed
           where id > @cursor and session_id is not null
             and not exists (
               select 1 from repo_context_event_links l
               where l.event_id = buffered_events.id
             ) and (
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
      `select e.rowid, e.id, e.session_id as sessionId, e.observed_at as observedAt
       from buffered_events e
       where e.session_id = @sessionId and e.rowid > @cursorRowid
         and e.repo_hash is null and not exists (
           select 1 from repo_context_event_links l where l.event_id = e.id
         )
         and (e.input_tokens is not null or e.output_tokens is not null or e.cost_usd is not null)
       order by e.rowid
       limit @limit`,
    );
    const selectBackwardNeighbors = database.prepare(
      `select id, repo_hash as repoHash, branch_hash as branchHash,
         observed_at as observedAt
       from buffered_events indexed by idx_events_session
       where session_id = @sessionId and observed_at <= @observedAt
       order by observed_at desc, rowid desc limit @limit`,
    );
    const selectForwardNeighbors = database.prepare(
      `select id, repo_hash as repoHash, branch_hash as branchHash,
         observed_at as observedAt
       from buffered_events indexed by idx_events_session
       where session_id = @sessionId and observed_at > @observedAt
       order by observed_at, rowid limit @limit`,
    );
    const apply = database.prepare(
      `update buffered_events set
         repo_hash = @repoHash,
         branch_hash = @branchHash,
         payload_json = json_set(payload_json, '$.metadata.repoStitched', json('true'))
       where id = @id and repo_hash is null and not exists (
         select 1 from repo_context_event_links l where l.event_id = @id
       )`,
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
        const backwardNeighbors = selectBackwardNeighbors.all({
          sessionId: row.sessionId,
          observedAt: row.observedAt,
          limit: neighborLimit,
        }) as RepoNeighbor[];
        const forwardNeighbors = selectForwardNeighbors.all({
          sessionId: row.sessionId,
          observedAt: row.observedAt,
          limit: neighborLimit,
        }) as RepoNeighbor[];
        const neighborIds = [...backwardNeighbors, ...forwardNeighbors].map((neighbor) => neighbor.id);
        const linked = neighborIds.length === 0
          ? new Set<string>()
          : new Set((database.prepare(
              `select event_id as eventId from repo_context_event_links
               where event_id in (${neighborIds.map(() => "?").join(",")})`,
            ).all(...neighborIds) as Array<{ eventId: string }>).map((link) => link.eventId));
        const backwardNeighbor = backwardNeighbors.find(
          (neighbor) => neighbor.repoHash !== null && !linked.has(neighbor.id),
        );
        const candidateTime = Date.parse(row.observedAt);
        const forwardNeighbor = forwardNeighbors.find((neighbor) => (
          neighbor.repoHash !== null && !linked.has(neighbor.id) &&
          Number.isFinite(candidateTime) &&
          Date.parse(neighbor.observedAt) - candidateTime <= 600_000
        ));
        const selected = backwardNeighbor ?? forwardNeighbor;
        const repoHash = selected?.repoHash ?? null;
        if (!repoHash) continue;
        const direction = backwardNeighbor ? "backward" : "forward";
        const branchHash = selected?.branchHash ?? null;
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
  }).immediate();
}

/**
 * Per-stage wall-clock elapsed time for one automatic recent-only cycle, in
 * whole milliseconds. Every stage reports even when its work was deferred
 * (0 ms), so a deadline kill names the real consumer instead of an inference.
 */
export type MaintenanceStageTimings = {
  codexCaptureMs: number;
  claudeCaptureMs: number;
  reconciliationMs: number;
  repricingMs: number;
  enrichmentMs: number;
  projectionDrainMs: number;
  totalMs: number;
};

export type CollectorMaintenanceRunResult = {
  recentOnly: true;
  rollout: RolloutScanResult;
  transcript: TranscriptScanResult;
  /** Present when the maintenance owns a Grok usage tailer. */
  grok?: GrokUsageScanResult;
  reconciliation: CodexReconciliationResult;
  repricing: RepricingMaintenanceResult;
  enrichment: RepoEnrichmentMaintenanceResult;
  projection?: ReturnType<LocalEventBuffer["projection"]["runMaintenance"]>;
  projectionDrain?: ProjectionDrainResult;
  rawEventWrites: number;
  /** Durable complete-record progress; attempted reads do not qualify. */
  captureAdvanced?: boolean;
  postCaptureDeferred?: string[];
  repairService?: RepairService;
  /** Capture order, admission, progress and debt of this cadence. */
  captureTurn?: AutomaticCaptureTurn;
  stageTimings?: MaintenanceStageTimings;
};

/** Path/content-free result projected across the maintenance child boundary. */
export type MaintenanceRunOutcome = {
  recentOnly: true;
  rollout: {
    filesRead: number;
    parseErrors: number;
    eventsAppended: number;
    activity: { discoveryEntries: number };
  };
  transcript: {
    filesRead: number;
    parseErrors: number;
    eventsAppended: number;
    activity: { discoveryEntries: number };
  };
  grok?: {
    filesRead: number;
    parseErrors: number;
    eventsAppended: number;
    activity: { discoveryEntries: number };
  };
  reconciliation: { rowsChanged: number; rowsVisited: number };
  repricing: { repriced: number; rowsVisited: number };
  enrichment: { backward: number; forward: number; rowsVisited: number };
  rawEventWrites: number;
  /** Durable complete-record progress; attempted reads do not qualify. */
  captureAdvanced?: boolean;
  stageTimings?: MaintenanceStageTimings;
};

export type MaintenancePartialOutcome = {
  outcome: "PARTIAL_OK";
  progress: MaintenanceJobProgress;
};

export type MaintenanceAttemptOutcome = MaintenanceRunOutcome | MaintenancePartialOutcome;

export function isMaintenancePartialOutcome(
  value: MaintenanceAttemptOutcome,
): value is MaintenancePartialOutcome {
  return "outcome" in value && value.outcome === "PARTIAL_OK";
}

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
    source: "codex" | "claude_code" | "grok";
    startedAt: string;
    budget: CaptureWorkBudget;
  } | null = null;
  private lastBudget: CaptureBudgetStatus | null = null;

  constructor(
    private readonly buffer: LocalEventBuffer,
    private readonly rolloutTailer: RolloutTailer,
    private readonly transcriptTailer: TranscriptTailer,
    private readonly signal?: AbortSignal,
    /** Grok usage files (bead eco-6hoxj.163.20); absent callers keep the two-source cadence. */
    private readonly grokTailer?: GrokUsageTailer,
    private readonly options: { captureCoverageIntervalMs?: number; captureCoverageTurnMs?: number } = {},
  ) {
    ensureAutomaticCaptureRuntimeState(this.buffer.database);
  }

  /** Coverage walks in progress, one per source; they resume on the next cadence. */
  private coverageWalks: Array<{ check: CaptureCoverageCheck; walk: CaptureCoverageWalk }> | null = null;
  private coverageSourceTurn = 0;

  /**
   * eco-6hoxj.163.18 (review r2 B1, r3 N4): the capture frontier the upload
   * claim attests moves only from a stat-only check of every tailed file
   * (capture-frontier.ts), never from a pass. Capture phase only. A check
   * starts at most once per interval (the first at once), walks each source
   * for at most CAPTURE_COVERAGE_TURN_MS per cadence and resumes where it
   * stopped; a source's frontier moves when its walk completes. Each source's
   * start time is taken before its walk. A check that fails leaves the
   * frontier unchanged. Without a Grok tailer this loop captures no Grok usage,
   * so there is none to cover.
   */
  private checkCaptureCoverage() {
    const database = this.buffer.database;
    try {
      if (!this.coverageWalks) {
        const interval = this.options.captureCoverageIntervalMs ?? CAPTURE_COVERAGE_INTERVAL_MS;
        const nowMs = Date.now();
        const lastMs = Date.parse(maintenanceState(database, CAPTURE_COVERAGE_CHECK_KEY) ?? "");
        if (Number.isFinite(lastMs) && lastMs <= nowMs && nowMs - lastMs < interval) return;
        setMaintenanceState(database, CAPTURE_COVERAGE_CHECK_KEY, new Date(nowMs).toISOString());
        const sources = [
          ["codex", () => this.rolloutTailer.coverageWalk()],
          ["claude_code", () => this.transcriptTailer.coverageWalk()],
          ["grok", () => this.grokTailer?.coverageWalk() ?? CaptureCoverageWalk.empty()],
        ] as const;
        this.coverageWalks = [];
        for (const [source, walk] of sources) {
          const check = beginCaptureCoverage(database, source, new Date().toISOString());
          if (check) this.coverageWalks.push({ check, walk: walk() });
        }
      }
      const walks = this.coverageWalks;
      const turnMs = Math.max(1, (this.options.captureCoverageTurnMs ?? CAPTURE_COVERAGE_TURN_MS) -
        CAPTURE_COVERAGE_TURN_MARGIN_MS);
      const turnDeadline = performance.now() + turnMs;
      const shareMs = turnMs / Math.max(1, walks.length);
      const first = this.coverageSourceTurn++ % Math.max(1, walks.length);
      const remaining = walks.map(() => CAPTURE_COVERAGE_MAX_WORK_PER_TURN);
      const advance = (index: number, deadline: number) => {
        const { check, walk } = walks[index]!;
        if (walk.done || remaining[index]! <= 0) return;
        const used = walk.step(deadline, (files) => applyCaptureCoverage(database, check, files),
          undefined, remaining[index]);
        remaining[index]! -= used;
        if (walk.done && walk.complete) finishCaptureCoverage(database, check);
      };
      for (let offset = 0; offset < walks.length; offset += 1) {
        if (this.signal?.aborted) break;
        advance((first + offset) % walks.length, performance.now() + shareMs);
      }
      // Every source gets its first share before an active source reuses idle
      // time. The common deadline prevents the bonus from extending the turn.
      for (let offset = 0; offset < walks.length && !this.signal?.aborted; offset += 1) {
        if (performance.now() >= turnDeadline) break;
        advance((first + offset) % walks.length, turnDeadline);
      }
      if (this.signal?.aborted) {
        walks.forEach(({ walk }) => walk.close());
        this.coverageWalks = null;
        return;
      }
      if (this.coverageWalks.every(({ walk }) => walk.done)) this.coverageWalks = null;
    } catch {
      // A failed check leaves the frontier where it was; capture goes on.
      this.coverageWalks?.forEach(({ walk }) => walk.close());
      this.coverageWalks = null;
    }
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
    this.coverageWalks?.forEach(({ walk }) => walk.close());
    this.coverageWalks = null;
    this.rolloutTailer.close();
    this.transcriptTailer.close();
    this.grokTailer?.close();
  }

  async runRecent(options: {
    quarantine?: Pick<MaintenanceProgress, "source" | "stage" | "candidateHash">;
    onProgress?: (progress: MaintenanceProgress) => boolean;
    /** Split A calls this synchronously after each bounded transaction commits. */
    onDurableCommit?: (progress: MaintenanceJobProgress) => boolean;
    clock?: () => number;
  } = {}): Promise<CollectorMaintenanceRunResult> {
    // Monotonic stage timing source. Injectable so fixture proofs can assert
    // exact per-stage durations deterministically (issue #61 first step).
    const clock = options.clock ?? (() => performance.now());
    const runStartedAtMs = clock();
    let codexCaptureMs = 0;
    let claudeCaptureMs = 0;
    const budget = new CaptureWorkBudget();
    // Alternate the order of the two consumers of the shared 200ms allowance.
    // Odd cadences run bounded repairs first (at most 75ms of admitted units);
    // even cadences are capture-first, so one synchronous repair unit that
    // overruns the allowance cannot exhaust every cadence before any tailer
    // reads. Repairs on a capture-first cadence use only what capture leaves.
    // Persist the choice before work; neither restarts nor busy sources may
    // starve either consumer on the intervening turns. The alternation is
    // not trusted alone: a cadence whose capture leader never started,
    // because repairs or the bookkeeping before capture spent the allowance,
    // hands the next cadence to capture whatever its parity, and that
    // cadence's leader starts its first unit even if the clock is spent
    // again. The hand-off is used up by that cadence.
    const repairService = automaticRepairServiceStatus(this.buffer.database);
    const firstRepair = repairService.next % REPAIR_STAGES.length;
    repairService.cycles += 1;
    const fairness = automaticCaptureFairnessStatus(this.buffer.database);
    const captureFirst = repairService.cycles % 2 === 0 || fairness.captureFirst;
    const leaderOverride = fairness.leaderDenied;
    if (fairness.captureFirst || fairness.leaderDenied) {
      setMaintenanceState(this.buffer.database, AUTOMATIC_CAPTURE_FAIRNESS_KEY,
        JSON.stringify({ ...fairness, captureFirst: false, leaderDenied: false } satisfies AutomaticCaptureFairness));
    }
    const sourceOrder = this.grokTailer
      ? ["codex", "claude_code", "grok"] as const
      : ["codex", "claude_code"] as const;
    // Capture sources lead in a fixed rotation, and the lead passes on once
    // its holder has had its turn: it started with the aggregate clock open,
    // whether it then committed, failed or was killed. Only a leader the clock
    // never let start keeps the lead. With the hand-off, the rotation moves in
    // at least one of every two cadences, so each of S sources leads within
    // 2S cadences (6 with Grok), and as the leader its first unit is admitted
    // and allowed to finish. A source that keeps failing cannot hold the
    // lead, so it cannot keep the others out (eco-6hoxj.163.42 round 3). The
    // bound does not depend on the bookkeeping before capture fitting inside
    // the allowance: a leader the clock denies starts on the next cadence
    // whatever the clock (round 4).
    const storedTurn = maintenanceState(this.buffer.database, AUTOMATIC_CAPTURE_SOURCE_TURN_KEY);
    const storedIndex = (sourceOrder as readonly string[]).indexOf(storedTurn ?? "codex");
    const firstIndex = storedIndex >= 0 ? storedIndex : 0;
    const runOrder: CaptureSource[] = sourceOrder.map((_, offset) =>
      sourceOrder[(firstIndex + offset) % sourceOrder.length]!);
    let repairTurnAdvanced = false;
    const saveRepairService = () => setMaintenanceState(this.buffer.database,
      REPAIR_SERVICE_KEY, JSON.stringify(repairService));
    saveRepairService();
    const postCaptureDeferred: string[] = ["enrichment"];
    let reconciliation: CodexReconciliationResult = {
      backfillComplete: false, legacyRowsVisited: 0, contextRowsVisited: 0,
      candidateRowsVisited: 0, rowsVisited: 0, rowsChanged: 0, stitched: 0,
      priced: 0, sliceDurationMs: 0, timeBudgetExhausted: true,
    };
    let repricing: RepricingMaintenanceResult = {
      catalogFingerprint: pricingCatalogFingerprint(), catalogChanged: false,
      backfillComplete: false, legacyRowsVisited: 0, candidateRowsVisited: 0,
      rowsVisited: 0, repriced: 0,
    };
    const enrichment: RepoEnrichmentMaintenanceResult = {
      backfillComplete: false, legacyRowsVisited: 0, sessionsVisited: 0,
      candidateRowsVisited: 0, rowsVisited: 0, backward: 0, forward: 0,
    };
    let reconciliationMs = 0, repricingMs = 0, enrichmentMs = 0, projectionDrainMs = 0;
    // Assigned inside the runRepairs closure below; the typed null initializer keeps
    // the declared union so control-flow narrowing does not reduce it to null.
    let drained = null as Awaited<ReturnType<typeof drainProjectionMigration>> | null;
    const runRepairs = async () => {
      const repairStarted = performance.now();
      for (let offset = 0; offset < REPAIR_STAGES.length; offset += 1) {
        const stage = REPAIR_STAGES[(firstRepair + offset) % REPAIR_STAGES.length];
        if (this.signal?.aborted || !budget.canStart(5) ||
            (offset > 0 && performance.now() - repairStarted >= 75)) {
          postCaptureDeferred.push(stage);
          continue;
        }
        const counter = repairService.stages[stage];
        counter.attempts += 1;
        // Rotate only admitted repair turns, avoiding a parity lock on stages.
        if (!repairTurnAdvanced) {
          repairService.next = (firstRepair + 1) % REPAIR_STAGES.length;
          repairTurnAdvanced = true;
        }
        saveRepairService();
        const stageStarted = clock();
        try {
          let rows = 0;
          switch (stage) {
            case "projection":
              drained = await drainProjectionMigration(this.buffer.projection, {
                maxSlices: 1, maxActiveMs: 25, signal: this.signal,
              });
              rows = drained.receipt.repairRowsVisited + drained.receipt.backfillRowsVisited +
                drained.receipt.parityRowsVisited + drained.receipt.metricRowsVisited +
                drained.receipt.sessionRepairRowsVisited;
              projectionDrainMs = Math.max(0, Math.round(clock() - stageStarted));
              break;
            case "reconciliation":
              reconciliation = runCodexReconciliationMaintenance(this.buffer.database, {
                legacyRowLimit: 64, legacyChunkLimit: 64, contextWindowLimit: 2,
                contextRowLimit: 64, candidateLimit: 32, freshCandidateLimit: 16,
                timeLimitMs: 25,
              });
              rows = reconciliation.rowsVisited;
              reconciliationMs = Math.max(0, Math.round(clock() - stageStarted));
              break;
            case "repricing":
              repricing = runRepricingMaintenance(this.buffer.database, { backfillLimit: 32, candidateLimit: 32 });
              rows = repricing.rowsVisited;
              repricingMs = Math.max(0, Math.round(clock() - stageStarted));
              break;
            case "repo_context_suppression":
              rows = this.buffer.drainRepoContextSuppressions().rowsVisited;
              break;
            case "learning_facts":
              rows = this.buffer.learningFacts.runMaintenance(
                DEFAULT_LEARNING_FACT_MAINTENANCE_BATCH,
              ).evicted;
              break;
          }
          counter.completed += 1;
          counter.rowsVisited += rows;
          counter.lastSuccessAt = new Date(Date.now()).toISOString();
        } catch (error) {
          // A live session send lease refuses this transaction temporarily.
          // The work remains queued, so this attempt is deferred, not failed.
          if (isSessionSyncUploadLeaseError(error)) {
            repairService.next = REPAIR_STAGES.indexOf(stage);
          } else counter.failures += 1;
          throw error;
        } finally { saveRepairService(); }
      }
    };
    if (!captureFirst) await runRepairs();
    const baselineAtStart = captureBaselineStatus(this.buffer.database);
    // Completed source snapshots stay armed while a per-generation ambiguity
    // blocks aggregate readiness. Capture classification remains globally
    // fail-closed, but it must inspect a trustworthy same-path replacement to
    // resolve that receipt; sending complete sources back through baseline is
    // a dead end because their baseline branches intentionally return early.
    const phase = baselineAtStart.sources.every((source) => source.status === "complete")
      ? "capture" as const
      : "baseline" as const;
    // Each source's turn is a scope of the one shared budget. Every aggregate
    // ceiling, the wall clock included, stays hard for admission: a source
    // whose turn comes after the allowance is spent is not admitted. The
    // remaining wall is shared among the sources still to run, so one busy
    // source cannot take a whole cadence; unused time flows to the next. An
    // admitted turn is progress, not just admission: its first bounded unit
    // is admitted on the aggregate clock even past its share, and allowed to
    // finish (`CaptureWorkBudget.unitDeadline`) instead of being abandoned
    // after its slow read was already paid for. Bytes, records and events
    // are not divided: a byte share would starve any Grok usage file larger
    // than one share. The pre-Grok two-source cadence (callers without a
    // Grok tailer) keeps its budget unchanged.
    const scopedCaptureBudget = (sourcesLeft: number, pastSpentWall = false) => this.grokTailer || pastSpentWall
      ? budget.scoped(this.grokTailer
        ? { maxWallMs: Math.floor(budget.remainingWallMs() / Math.max(1, sourcesLeft)) }
        : {}, { progressUnit: true, pastSpentWall })
      : budget;
    const admitted: Partial<Record<CaptureSource, boolean>> = {};
    const startedAt = new Date().toISOString();
    let rollout: RolloutScanResult | undefined;
    let transcript: TranscriptScanResult | undefined;
    let grok: GrokUsageScanResult | undefined;
    const runRollout = async (sourceBudget: CaptureWorkBudget) => {
      this.current = { phase, source: "codex", startedAt, budget };
      const sourceAccepted = options.onProgress?.({
        source: "codex",
        stage: "source_scan",
        candidateHash: null,
      }) !== false;
      const scanStartedAtMs = clock();
      return this.rolloutTailer.scan({
        scope: "recent",
        now: new Date(startedAt),
        automatic: { phase, budget: sourceBudget },
        signal: this.signal,
        quarantine: options.quarantine?.source === "codex" && options.quarantine.candidateHash
          ? { stage: options.quarantine.stage, candidateHash: options.quarantine.candidateHash }
          : undefined,
        onProgress: (progress) => options.onProgress?.({ source: "codex", ...progress }) ?? true,
        deferredBeforeIo: !(admitted.codex = sourceBudget.canContinue() && sourceAccepted &&
          !(options.quarantine?.source === "codex" && options.quarantine.stage === "source_scan")),
      }).finally(() => {
        codexCaptureMs = Math.max(0, Math.round(clock() - scanStartedAtMs));
      });
    };
    const runTranscript = async (sourceBudget: CaptureWorkBudget) => {
      this.current = { phase, source: "claude_code", startedAt, budget };
      const sourceAccepted = options.onProgress?.({
        source: "claude_code",
        stage: "source_scan",
        candidateHash: null,
      }) !== false;
      const scanStartedAtMs = clock();
      return this.transcriptTailer.scan({
        scope: "recent",
        now: new Date(startedAt),
        automatic: { phase, budget: sourceBudget },
        signal: this.signal,
        quarantine: options.quarantine?.source === "claude_code" && options.quarantine.candidateHash
          ? { stage: options.quarantine.stage, candidateHash: options.quarantine.candidateHash }
          : undefined,
        onProgress: (progress) => options.onProgress?.({ source: "claude_code", ...progress }) ?? true,
        deferredBeforeIo: !(admitted.claude_code = sourceBudget.canContinue() && sourceAccepted &&
          !(options.quarantine?.source === "claude_code" && options.quarantine.stage === "source_scan")),
      }).finally(() => {
        claudeCaptureMs = Math.max(0, Math.round(clock() - scanStartedAtMs));
      });
    };
    const runGrok = async (tailer: GrokUsageTailer, sourceBudget: CaptureWorkBudget) => {
      this.current = { phase, source: "grok", startedAt, budget };
      // Announce the source before any filesystem work, as the other two do,
      // so a stall inside the Grok scan is held against Grok's own stage and
      // never against the previous source's last candidate.
      const sourceAccepted = options.onProgress?.({
        source: "grok",
        stage: "source_scan",
        candidateHash: null,
      }) !== false;
      return tailer.scan({
        budget: sourceBudget,
        now: new Date(startedAt),
        signal: this.signal,
        deferredBeforeIo: !(admitted.grok = sourceBudget.canContinue() && sourceAccepted &&
          !(options.quarantine?.source === "grok" && options.quarantine.stage === "source_scan")),
      });
    };
    let leaderServed = false;
    let preCaptureMs = 0;
    try {
      for (const [index, source] of runOrder.entries()) {
        if (index === 0) preCaptureMs = Math.round(budget.elapsedWallMs());
        const sourceBudget = scopedCaptureBudget(runOrder.length - index, index === 0 && leaderOverride);
        if (index === 0 && sourceBudget.canContinue()) {
          // Pass the lead on before the leader's scan can fail, overrun or be
          // killed with its worker.
          leaderServed = true;
          setMaintenanceState(this.buffer.database, AUTOMATIC_CAPTURE_SOURCE_TURN_KEY,
            sourceOrder[(firstIndex + 1) % sourceOrder.length]!);
        }
        if (source === "codex") rollout = await runRollout(sourceBudget);
        else if (source === "claude_code") transcript = await runTranscript(sourceBudget);
        else if (this.grokTailer) grok = await runGrok(this.grokTailer, sourceBudget);
      }
      this.lastBudget = budget.status();
      if (this.signal?.aborted || rollout?.aborted || transcript?.aborted || grok?.aborted) {
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
    // The clock never let the leader start: hand the next cadence to capture
    // and let its leader start whatever the clock.
    if (!leaderServed) {
      const denied = automaticCaptureFairnessStatus(this.buffer.database);
      setMaintenanceState(this.buffer.database, AUTOMATIC_CAPTURE_FAIRNESS_KEY, JSON.stringify({
        version: 2, captureFirst: true, leaderDenied: true, deniedCadences: denied.deniedCadences + 1,
      } satisfies AutomaticCaptureFairness));
    }
    // A durable commit this cadence: records, a checkpoint, a Grok document
    // or a Grok walk step. Receipt only; the rotation does not depend on it.
    const jsonlProgressed = (result: RolloutScanResult | TranscriptScanResult) =>
      result.slicesCommitted > 0 || (result.recordsCommitted ?? 0) > 0 ||
      (result.continuationBytesAdvanced ?? 0) > 0;
    const progressed: Partial<Record<CaptureSource, boolean>> = {
      codex: jsonlProgressed(rollout),
      claude_code: jsonlProgressed(transcript),
      ...(grok ? { grok: grok.recordsCommitted > 0 || grok.filesParsed > 0 || grok.activity.discoveryEntries > 0 } : {}),
    };
    const captureTurn: AutomaticCaptureTurn = {
      captureFirst, order: runOrder, leader: runOrder[0]!, leaderServed, leaderOverride, preCaptureMs,
      admitted, progressed,
    };
    if (rollout.activity && !this.signal?.aborted) {
      this.buffer.projection.recordCaptureActivity({ source: "codex", ...rollout.activity });
    }
    if (transcript.activity && !this.signal?.aborted) {
      this.buffer.projection.recordCaptureActivity({ source: "claude_code", ...transcript.activity });
    }
    if (grok && !this.signal?.aborted) {
      this.buffer.projection.recordCaptureActivity({ source: "grok", ...grok.activity });
    }
    if (phase === "capture" && !this.signal?.aborted) this.checkCaptureCoverage();
    // Capture-first cadence: repair units take only the allowance capture left.
    if (captureFirst) {
      await runRepairs();
      this.lastBudget = budget.status();
    }
    const errorCount =
      rollout.discoveryErrors + rollout.statErrors + rollout.readErrors + rollout.parseErrors + rollout.unresolvedRecords +
      transcript.discoveryErrors + transcript.statErrors + transcript.readErrors + transcript.parseErrors + transcript.unresolvedRecords +
      (grok ? grok.discoveryErrors + grok.statErrors + grok.readErrors + grok.parseErrors + grok.unresolvedRecords : 0);
    const finalBudget = budget.status();
    const baselineProgress = captureBaselineStatus(this.buffer.database).progress.state;
    const baselineIncomplete = phase === "baseline" && baselineProgress !== "complete";
    const baselineFailed = phase === "baseline" &&
      (baselineProgress === "failed" || baselineProgress === "ambiguous");
    const deferred = finalBudget.exhausted || postCaptureDeferred.length > 0 ||
      rollout.deferredGenerations > 0 || transcript.deferredGenerations > 0 ||
      (grok?.deferredGenerations ?? 0) > 0;
    recordAutomaticCaptureRuntimeState(
      this.buffer.database,
      phase,
      this.signal?.aborted ? "aborted" : baselineFailed ? "failed" : errorCount > 0 ? "complete_with_errors" :
        baselineIncomplete ? "baseline_in_progress" : deferred ? "deferred" : "complete",
      finalBudget,
      rollout,
      transcript,
      grok,
    );
    return {
      recentOnly: true,
      rollout,
      transcript,
      ...(grok ? { grok } : {}),
      reconciliation,
      repricing,
      enrichment,
      ...(drained ? { projection: drained.receipt, projectionDrain: drained.drain } : {}),
      rawEventWrites: rollout.eventsAppended + transcript.eventsAppended + (grok?.eventsAppended ?? 0),
      captureAdvanced: (rollout.recordsCommitted ?? 0) + (transcript.recordsCommitted ?? 0) +
        (rollout.continuationBytesAdvanced ?? 0) + (transcript.continuationBytesAdvanced ?? 0) +
        (grok?.recordsCommitted ?? 0) > 0,
      postCaptureDeferred,
      repairService,
      captureTurn,
      stageTimings: {
        codexCaptureMs,
        claudeCaptureMs,
        reconciliationMs,
        repricingMs,
        enrichmentMs,
        projectionDrainMs,
        totalMs: Math.max(0, Math.round(clock() - runStartedAtMs)),
      },
    };
  }
}

export type MaintenanceSchedulerStatus<
  T extends MaintenanceAttemptOutcome = CollectorMaintenanceRunResult,
> = {
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
  lastRun: T | null;
};

type DrainWaiter<T extends MaintenanceAttemptOutcome> = {
  resolve: (results: T[]) => void;
  reject: (error: unknown) => void;
};

/**
 * One daemon entrypoint for boot, interval, and internal recent-tail triggers.
 * Concurrent requests collapse into one pending follow-up. There is no full
 * mode in this scheduler; only the explicit scan-rollouts/scan-transcripts CLI
 * commands may request full history.
 */
export class CoalescingMaintenanceScheduler<
  T extends MaintenanceAttemptOutcome = CollectorMaintenanceRunResult,
> {
  private accepting = true;
  private running = false;
  private pending = false;
  private waiters: Array<DrainWaiter<T>> = [];
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
  private lastRun: T | null = null;
  private idleWaiters: Array<() => void> = [];

  constructor(
    private readonly runJob: () => Promise<T>,
  ) {}

  trigger() {
    if (!this.accepting) {
      return Promise.reject(new Error("maintenance_scheduler_stopping"));
    }
    this.triggerCount += 1;
    this.pending = true;
    if (this.running) this.coalescedTriggerCount += 1;

    const promise = new Promise<T[]>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
    if (!this.running) {
      this.running = true;
      void this.drain();
    }
    return promise;
  }

  status(): MaintenanceSchedulerStatus<T> {
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
    const results: T[] = [];
    let firstError: unknown;
    let firstFailure: unknown;
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
        if (isMaintenancePartialOutcome(result)) {
          results.push(result);
          continue;
        }
        const completed = result as MaintenanceRunOutcome;
        this.rolloutFilesRead += completed.rollout.filesRead;
        this.transcriptFilesRead += completed.transcript.filesRead;
        this.rawEventWrites += completed.rawEventWrites;
        this.repriceRowsVisited += completed.repricing.rowsVisited;
        this.reconciliationRowsVisited += completed.reconciliation.rowsVisited;
        this.enrichmentRowsVisited += completed.enrichment.rowsVisited;
        results.push(result);
      } catch (error) {
        if (!isSessionSyncUploadLeaseError(error)) {
          this.failedRuns += 1;
          firstFailure ??= error;
        }
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
      if (firstFailure !== undefined) waiter.reject(firstFailure);
      else if (firstError !== undefined) waiter.reject(firstError);
      else waiter.resolve(results);
    }
  }
}

/**
 * The only automatic boot/interval entrypoint. Its signature cannot express a
 * full-history request, so daemon scheduling stays recent-only by construction.
 */
export function requestAutomaticRecentMaintenance<T extends MaintenanceAttemptOutcome>(
  scheduler: CoalescingMaintenanceScheduler<T>,
) {
  return scheduler.trigger();
}

export const AUTOMATIC_BASELINE_STARTUP_INTERVAL_MS = 5_000;
export const AUTOMATIC_MAINTENANCE_NORMAL_INTERVAL_MS = 60_000;
const AUTOMATIC_MAINTENANCE_STORAGE_BUSY_INITIAL_INTERVAL_MS = 1_000;
const AUTOMATIC_MAINTENANCE_STORAGE_BUSY_MAX_INTERVAL_MS = 5_000;
const AUTOMATIC_CAPTURE_FOLLOWUPS = 4;

export type AutomaticMaintenanceCadenceStatus = {
  accepting: boolean;
  inFlight: boolean;
  retryClass: "boot" | "startup" | "repair" | "capture" | "storage_busy" | "circuit" | "normal" | null;
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
 * failed, ambiguous and pre-start states fall back to the ordinary cadence.
 * After baseline, committed records renew four short capture follow-ups;
 * discovery-only and unresolved work cannot keep the fast cadence alive.
 * Scheduling happens after the coalesced run drains, so callbacks do not
 * accumulate during a slow filesystem slice.
 */
export class AutomaticMaintenanceCadence<
  T extends MaintenanceAttemptOutcome = CollectorMaintenanceRunResult,
> {
  private accepting = true;
  private inFlight = false;
  private timer: unknown | null = null;
  private retryClass: AutomaticMaintenanceCadenceStatus["retryClass"] = null;
  private nextRetryAt: string | null = null;
  private triggerCount = 0;
  private failedTriggers = 0;
  private storageBusyDeferrals = 0;
  private captureFollowups = 0;

  constructor(
    private readonly scheduler: CoalescingMaintenanceScheduler<T>,
    private readonly baselineStatus: () => ReturnType<typeof captureBaselineStatus>,
    private readonly options: {
      startupIntervalMs?: number;
      normalIntervalMs?: number;
      activeBudgetMs?: number;
      repairProgress?: () => { pending: boolean; units: number };
      retryNotBefore?: () => number | null;
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
    this.captureFollowups = 0;
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

  private schedule(retryClass: Exclude<AutomaticMaintenanceCadenceStatus["retryClass"], null>) {
    if (!this.accepting || this.timer) return;
    const now = this.timerApi().now();
    const notBefore = this.options.retryNotBefore?.() ?? null;
    const delay = notBefore !== null && notBefore > now ? notBefore - now
      : retryClass === "normal" ? this.normalIntervalMs()
        : retryClass === "storage_busy"
          ? Math.min(AUTOMATIC_MAINTENANCE_STORAGE_BUSY_MAX_INTERVAL_MS,
            AUTOMATIC_MAINTENANCE_STORAGE_BUSY_INITIAL_INTERVAL_MS *
              2 ** Math.max(0, this.storageBusyDeferrals - 1))
          : this.startupIntervalMs();
    if (notBefore !== null && notBefore > now) retryClass = "circuit";
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
    let storageBusy = false;
    let discoveryAdvanced = false;
    let baselineBefore: ReturnType<typeof captureBaselineStatus>["progress"] | null = null;
    let repairBefore: { pending: boolean; units: number } | null = null;
    let repairAdvanced = false;
    try {
      baselineBefore = this.baselineStatus().progress;
      repairBefore = this.options.repairProgress?.() ?? null;
      const results = await requestAutomaticRecentMaintenance(this.scheduler);
      this.storageBusyDeferrals = 0;
      // Preserve a short retry burst across discovery/frame deferrals. Only
      // committed records or new durable scan bytes renew it; prefix rereads,
      // refused/idle work and failed transactions cannot renew the burst.
      const captureAdvanced = results.some(result =>
        !isMaintenancePartialOutcome(result) && result.captureAdvanced === true);
      this.captureFollowups = captureAdvanced ? AUTOMATIC_CAPTURE_FOLLOWUPS
        : Math.max(0, this.captureFollowups - 1);
      const repairAfter = this.options.repairProgress?.();
      repairAdvanced = Boolean(repairAfter?.pending && repairAfter.units > (repairBefore?.units ?? 0));
      // Entries actually visited this cadence, never pending candidates the
      // capture path carried over the pending-metadata gate. A mixed turn
      // (one source still baselining, the other gated) must not keep the
      // startup retry class on a stale file count (REVIEW-78 N1).
      discoveryAdvanced = results.some(
        (result) =>
          !isMaintenancePartialOutcome(result) && (
            result.rollout.activity.discoveryEntries > 0 ||
            result.transcript.activity.discoveryEntries > 0
          ),
      );
    } catch (error) {
      if (isSessionSyncUploadLeaseError(error)) {
        storageBusy = true;
        // Cap the backoff at five seconds under consecutive session sends.
        this.storageBusyDeferrals = Math.min(4, this.storageBusyDeferrals + 1);
      } else {
        failed = true;
        this.storageBusyDeferrals = 0;
        this.captureFollowups = 0;
        this.failedTriggers += 1;
        this.options.onError?.(error);
      }
    } finally {
      this.inFlight = false;
      if (this.accepting) {
        let retry: "normal" | "repair" | "startup" | "capture" | "storage_busy" =
          storageBusy ? "storage_busy" : "normal";
        try {
          if (!storageBusy) {
            const baselineAfter = this.baselineStatus().progress;
            if (!failed && baselineBefore) retry = repairAdvanced ? "repair"
              : baselineAfter.state === "complete" && this.captureFollowups > 0 ? "capture"
                : this.classifyRetry(baselineBefore, baselineAfter, discoveryAdvanced);
          }
        } catch (error) {
          this.captureFollowups = 0;
          this.failedTriggers += 1;
          this.options.onError?.(error);
        }
        this.schedule(retry);
      }
    }
  }
}
