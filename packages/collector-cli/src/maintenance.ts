import { createHash } from "node:crypto";

import type Database from "better-sqlite3";

import { MODEL_PRICING, estimateCostUsd } from "../../shared/src/index";
import type { LocalEventBuffer } from "./buffer";
import { RolloutTailer, type RolloutScanResult } from "./rollout-tailer";
import { TranscriptTailer, type TranscriptScanResult } from "./transcript-tailer";

const PRICING_VERSION_KEY = "pricing_catalog_applied";
const PRICING_TARGET_KEY = "pricing_catalog_backfill_target";
const PRICING_CURSOR_KEY = "pricing_catalog_backfill_cursor";
const REPO_BACKFILL_CURSOR_KEY = "repo_enrichment_backfill_cursor";
const REPO_BACKFILL_COMPLETE_KEY = "repo_enrichment_backfill_complete";

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
  recentOnly: boolean;
  rollout: RolloutScanResult;
  transcript: TranscriptScanResult;
  repricing: RepricingMaintenanceResult;
  enrichment: RepoEnrichmentMaintenanceResult;
  rawEventWrites: number;
};

export class CollectorMaintenance {
  constructor(
    private readonly buffer: LocalEventBuffer,
    private readonly rolloutTailer: RolloutTailer,
    private readonly transcriptTailer: TranscriptTailer,
  ) {}

  async run(recentOnly: boolean): Promise<CollectorMaintenanceRunResult> {
    const rollout = await this.rolloutTailer.scan({ recentOnly });
    const transcript = await this.transcriptTailer.scan({ recentOnly });
    const repricing = runRepricingMaintenance(this.buffer.database);
    const enrichment = runRepoEnrichmentMaintenance(this.buffer.database);
    return {
      recentOnly,
      rollout,
      transcript,
      repricing,
      enrichment,
      rawEventWrites: rollout.eventsAppended + transcript.eventsAppended,
    };
  }
}

export type MaintenanceSchedulerStatus = {
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
 * One daemon entrypoint for boot, interval, and explicit internal triggers.
 * Concurrent requests collapse into one pending follow-up; a full-history
 * request (`recentOnly=false`) dominates any pending recent request.
 */
export class CoalescingMaintenanceScheduler {
  private running = false;
  private pendingRecentOnly: boolean | undefined;
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
  private enrichmentRowsVisited = 0;
  private lastStartedAt: string | null = null;
  private lastCompletedAt: string | null = null;
  private lastRun: CollectorMaintenanceRunResult | null = null;

  constructor(
    private readonly runJob: (recentOnly: boolean) => Promise<CollectorMaintenanceRunResult>,
  ) {}

  trigger(recentOnly: boolean) {
    this.triggerCount += 1;
    this.pendingRecentOnly =
      this.pendingRecentOnly === undefined
        ? recentOnly
        : this.pendingRecentOnly && recentOnly;
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
      inFlight: this.running,
      pending: this.pendingRecentOnly !== undefined,
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
      enrichmentRowsVisited: this.enrichmentRowsVisited,
      lastStartedAt: this.lastStartedAt,
      lastCompletedAt: this.lastCompletedAt,
      lastRun: this.lastRun,
    };
  }

  private async drain() {
    const results: CollectorMaintenanceRunResult[] = [];
    let firstError: unknown;
    while (this.pendingRecentOnly !== undefined) {
      const recentOnly = this.pendingRecentOnly;
      this.pendingRecentOnly = undefined;
      this.runCount += 1;
      this.lastStartedAt = new Date().toISOString();
      if (this.activeJobs > 0) this.overlappingJobs += 1;
      this.activeJobs += 1;
      this.maxConcurrentJobs = Math.max(this.maxConcurrentJobs, this.activeJobs);
      try {
        const result = await this.runJob(recentOnly);
        this.lastRun = result;
        this.rolloutFilesRead += result.rollout.filesRead;
        this.transcriptFilesRead += result.transcript.filesRead;
        this.rawEventWrites += result.rawEventWrites;
        this.repriceRowsVisited += result.repricing.rowsVisited;
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
    const waiters = this.waiters;
    this.waiters = [];
    for (const waiter of waiters) {
      if (firstError !== undefined) waiter.reject(firstError);
      else waiter.resolve(results);
    }
  }
}
