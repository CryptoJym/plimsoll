import type Database from "better-sqlite3";
import { runRepoEnrichmentMaintenance } from "./maintenance";
import type { RepoContextRequest, RepoContextResult } from "./repo-context";

export const MAINTENANCE_STAGES = [
  "retention_deletion",
  "wal_checkpoint",
  "pending_event_link_fill",
  "enrichment",
  "git_context",
] as const;

export type MaintenanceStage = typeof MAINTENANCE_STAGES[number];

export type MaintenanceStageCursor = {
  stage: MaintenanceStage;
  cursor: string | null;
  rowsTotal: number;
  request: RepoContextRequest | null;
  updatedAt: string;
};

export type BoundedStageResult = {
  rows: number;
  ms: number;
  remaining: number;
  batchSize: number;
};

type TimedOptions = {
  remainingMs: number;
  batchSize: number;
  now?: () => number;
};

function boundedBatchSize(value: number) {
  return Math.max(1, Math.min(Math.trunc(value) || 1, 25_000));
}

function budget(options: TimedOptions) {
  const now = options.now ?? (() => performance.now());
  const started = now();
  const allowed = Math.max(0, Math.trunc(options.remainingMs) || 0);
  return {
    now,
    started,
    allowed,
    canStart: () => allowed > 0 && now() - started < allowed,
    result: (rows: number, batchSize = boundedBatchSize(options.batchSize)): BoundedStageResult => {
      const ms = Math.max(0, Math.round(now() - started));
      return { rows, ms, remaining: Math.max(0, allowed - ms), batchSize };
    },
  };
}

export function ensureMaintenanceStageSchema(database: Database.Database) {
  database.exec(`
    create table if not exists maintenance_stage_cursors (
      stage text primary key check (stage in (
        'retention_deletion', 'wal_checkpoint', 'pending_event_link_fill',
        'enrichment', 'git_context'
      )),
      cursor text,
      rows_total integer not null default 0 check (rows_total >= 0),
      request_json text,
      updated_at text not null
    ) without rowid;
    create table if not exists maintenance_git_context_queue (
      context_id text primary key,
      source text not null check (source in ('codex', 'claude_code')),
      cwd text not null,
      queued_at text not null
    ) without rowid;
  `);
  const insert = database.prepare(
    `insert into maintenance_stage_cursors (stage, updated_at)
     values (?, ?) on conflict(stage) do nothing`,
  );
  const now = new Date().toISOString();
  database.transaction(() => {
    for (const stage of MAINTENANCE_STAGES) insert.run(stage, now);
  })();
}

function queuedGitRequest(database: Database.Database): RepoContextRequest | null {
  const row = database.prepare(
    `select context_id as contextId, source, cwd
     from maintenance_git_context_queue order by queued_at, context_id limit 1`,
  ).get() as RepoContextRequest | undefined;
  return row ?? null;
}

export function readMaintenanceStageCursor(
  database: Database.Database,
  stage: MaintenanceStage,
): MaintenanceStageCursor {
  ensureMaintenanceStageSchema(database);
  const row = database.prepare(
    `select stage, cursor, rows_total as rowsTotal, request_json as requestJson,
       updated_at as updatedAt from maintenance_stage_cursors where stage = ?`,
  ).get(stage) as {
    stage: MaintenanceStage; cursor: string | null; rowsTotal: number;
    requestJson: string | null; updatedAt: string;
  };
  let request: RepoContextRequest | null = null;
  if (stage === "git_context") request = queuedGitRequest(database);
  else if (row.requestJson) {
    try { request = JSON.parse(row.requestJson) as RepoContextRequest; } catch { request = null; }
  }
  return { stage: row.stage, cursor: row.cursor, rowsTotal: row.rowsTotal, request, updatedAt: row.updatedAt };
}

function advance(
  database: Database.Database,
  stage: MaintenanceStage,
  rows: number,
  cursor: string | null = null,
  request: RepoContextRequest | null = null,
) {
  database.prepare(
    `update maintenance_stage_cursors set cursor = ?, rows_total = rows_total + ?,
       request_json = ?, updated_at = ? where stage = ?`,
  ).run(cursor, rows, request ? JSON.stringify(request) : null, new Date().toISOString(), stage);
}

export function runRetentionDeletionStage(
  database: Database.Database,
  options: TimedOptions & { retentionDays: number; parityReady: boolean; wallNow?: () => number },
): BoundedStageResult {
  ensureMaintenanceStageSchema(database);
  const timer = budget(options);
  if (!timer.canStart()) return timer.result(0);
  const stored = readMaintenanceStageCursor(database, "retention_deletion").cursor;
  let adaptiveBatchSize = Math.min(boundedBatchSize(options.batchSize), 1_000);
  if (stored) {
    try {
      const parsed = JSON.parse(stored) as { batchSize?: number };
      if (Number.isSafeInteger(parsed.batchSize) && Number(parsed.batchSize) > 0) {
        adaptiveBatchSize = Math.min(adaptiveBatchSize, Number(parsed.batchSize));
      }
    } catch {
      // Older rowid cursor receipts are safely replaced by the observed-time scan.
    }
  }
  const wallNow = options.wallNow?.() ?? Date.now();
  const cutoff = new Date(wallNow - Math.max(0, options.retentionDays) * 86_400_000).toISOString();
  const rows = database.transaction(() => {
    let deleted = 0;
    if (options.parityReady) {
      const candidates = database.prepare(
        `select id from buffered_events indexed by idx_events_observed
         where observed_at < ? and uploaded_at is not null
         order by observed_at limit ?`,
      ).all(cutoff, adaptiveBatchSize) as Array<{ id: string }>;
      const remove = database.prepare(`delete from buffered_events where id = ?`);
      for (const candidate of candidates) deleted += remove.run(candidate.id).changes;
    }
    if (deleted < adaptiveBatchSize) {
      const candidates = database.prepare(
        `select id from metric_samples indexed by idx_metrics_observed
         where created_at < ? order by created_at limit ?`,
      ).all(cutoff, adaptiveBatchSize - deleted) as Array<{ id: string }>;
      const remove = database.prepare(`delete from metric_samples where id = ?`);
      for (const candidate of candidates) deleted += remove.run(candidate.id).changes;
    }
    return deleted;
  })();
  const result = timer.result(rows, adaptiveBatchSize);
  const nextBatchSize = result.ms <= 0 || rows <= 0
    ? adaptiveBatchSize
    : Math.max(1, Math.min(1_000, Math.floor(rows * 2_750 / result.ms)));
  advance(database, "retention_deletion", rows, JSON.stringify({ batchSize: nextBatchSize }));
  return result;
}

type CheckpointReading = { busy: number; log: number; checkpointed: number };
export type WalCheckpointStageResult = BoundedStageResult & {
  passive: CheckpointReading | null;
  truncate: CheckpointReading | null;
};

function checkpoint(database: Database.Database, mode: "PASSIVE" | "TRUNCATE") {
  const rows = database.pragma(`wal_checkpoint(${mode})`) as CheckpointReading[];
  return rows[0] ?? { busy: 1, log: 0, checkpointed: 0 };
}

export function runWalCheckpointStage(
  database: Database.Database,
  options: TimedOptions,
): WalCheckpointStageResult {
  ensureMaintenanceStageSchema(database);
  const timer = budget(options);
  if (!timer.canStart()) return { ...timer.result(0), passive: null, truncate: null };
  const passive = checkpoint(database, "PASSIVE");
  const truncate = passive.busy === 0 && timer.canStart() ? checkpoint(database, "TRUNCATE") : null;
  const pages = Math.max(0, (truncate ?? passive).checkpointed);
  advance(database, "wal_checkpoint", pages, JSON.stringify(truncate ?? passive));
  return { ...timer.result(pages), passive, truncate };
}

export function runPendingEventLinkFillStage(
  database: Database.Database,
  options: TimedOptions,
): BoundedStageResult {
  ensureMaintenanceStageSchema(database);
  const timer = budget(options);
  if (!timer.canStart()) return timer.result(0);
  const limit = boundedBatchSize(options.batchSize);
  const rows = database.transaction(() => {
    const pending = database.prepare(
      `select l.event_id as eventId, l.context_id as contextId, e.rowid,
         e.repo_hash as existingRepoHash, r.repo_hash as repoHash,
         r.branch_hash as branchHash, r.head_sha as headSha
       from repo_context_event_links l
       join buffered_events e on e.id = l.event_id
       join repo_context_results r on r.context_id = l.context_id
       where l.fill_pending = 1 and l.context_conflict = 0
         and not exists (select 1 from repo_context_suppressions s where s.context_id = l.context_id)
       order by l.event_id limit ?`,
    ).all(limit) as Array<Record<string, unknown> & { eventId: string; contextId: string; existingRepoHash: string | null; repoHash: string }>;
    const fill = database.prepare(
      `update buffered_events set repo_hash = coalesce(repo_hash, @repoHash),
       branch_hash = coalesce(branch_hash, @branchHash), head_sha = coalesce(head_sha, @headSha)
       where rowid = @rowid and id = @eventId`,
    );
    const complete = database.prepare(
      `update repo_context_event_links set fill_pending = 0,
       context_conflict = case when ? then 1 else context_conflict end
       where event_id = ? and context_id = ? and fill_pending = 1`,
    );
    for (const row of pending) {
      const conflict = Boolean(row.existingRepoHash && row.existingRepoHash !== row.repoHash);
      if (!conflict) fill.run(row);
      complete.run(conflict ? 1 : 0, row.eventId, row.contextId);
    }
    advance(database, "pending_event_link_fill", pending.length, pending.at(-1)?.eventId ?? null);
    return pending.length;
  })();
  return timer.result(rows);
}

export function runEnrichmentStage(
  database: Database.Database,
  options: TimedOptions,
): BoundedStageResult {
  ensureMaintenanceStageSchema(database);
  const timer = budget(options);
  if (!timer.canStart()) return timer.result(0);
  const limit = Math.min(boundedBatchSize(options.batchSize), 1);
  const result = database.transaction(() => {
    const slice = runRepoEnrichmentMaintenance(database, {
      legacyBackfillLimit: limit,
      sessionLimit: Math.min(limit, 500),
      eventLimit: limit,
      skipLegacyBackfill: true,
    });
    advance(database, "enrichment", slice.rowsVisited, null);
    return slice;
  })();
  return timer.result(result.rowsVisited);
}

export type EnrichmentMaintenanceJobResult = BoundedStageResult & {
  skipped: boolean;
  timedOut: boolean;
};

export function runEnrichmentMaintenanceJob(
  database: Database.Database,
  options: { remainingMs: number; now?: () => number },
): EnrichmentMaintenanceJobResult {
  if (options.remainingMs < 5_000) {
    return { rows: 0, ms: 0, remaining: Math.max(0, options.remainingMs), batchSize: 1,
      skipped: true, timedOut: false };
  }
  const result = runEnrichmentStage(database, { ...options, batchSize: 1 });
  return { ...result, skipped: false, timedOut: result.ms >= options.remainingMs };
}

export type DeadlineMaintenanceStagesResult = {
  remainingMs: number;
  stages: Array<{ stage: "wal_checkpoint" | "retention" | "fill_pending_event_links" } & BoundedStageResult>;
};

export function runDeadlineMaintenanceStages(
  database: Database.Database,
  options: {
    deadlineMs: number;
    teardownMarginMs: number;
    retentionDays: number;
    parityReady: boolean;
    now?: () => number;
    onDurableCommit?: (progress: {
      stage: "wal_checkpoint" | "retention" | "fill_pending_event_links";
      rows: number; ms: number; remaining: number;
    }) => boolean;
  },
): DeadlineMaintenanceStagesResult {
  const now = options.now ?? (() => performance.now());
  const startedAt = now();
  const workBudget = Math.max(0, Math.trunc(options.deadlineMs) - Math.max(0, Math.trunc(options.teardownMarginMs)));
  const remaining = () => Math.max(0, workBudget - Math.max(0, Math.round(now() - startedAt)));
  const stages: DeadlineMaintenanceStagesResult["stages"] = [];
  const emit = (stage: "wal_checkpoint" | "retention" | "fill_pending_event_links", result: BoundedStageResult) => {
    const progress = { stage, rows: result.rows, ms: result.ms, remaining: remaining() };
    stages.push({ ...progress, batchSize: result.batchSize });
    return options.onDurableCommit?.(progress) !== false;
  };

  const checkpointBudget = Math.min(1_000, remaining());
  const checkpoint = runWalCheckpointStage(database, { remainingMs: checkpointBudget, batchSize: 1, now });
  if (!emit("wal_checkpoint", checkpoint)) return { remainingMs: remaining(), stages };
  const retention = runRetentionDeletionStage(database, {
    remainingMs: Math.min(3_000, remaining()), batchSize: 64,
    retentionDays: options.retentionDays, parityReady: options.parityReady, now,
  });
  if (!emit("retention", retention)) return { remainingMs: remaining(), stages };
  const pending = runPendingEventLinkFillStage(database, {
    remainingMs: remaining(), batchSize: 256, now,
  });
  emit("fill_pending_event_links", pending);
  return { remainingMs: remaining(), stages };
}

export function runGitContextStage(
  database: Database.Database,
  options: TimedOptions & {
    requests?: readonly RepoContextRequest[];
    resolve: (request: RepoContextRequest) => RepoContextResult;
    commit: (result: RepoContextResult) => void;
  },
): BoundedStageResult {
  ensureMaintenanceStageSchema(database);
  const timer = budget(options);
  const queuedAt = new Date().toISOString();
  const enqueue = database.prepare(
    `insert into maintenance_git_context_queue (context_id, source, cwd, queued_at)
     values (@contextId, @source, @cwd, @queuedAt) on conflict(context_id) do nothing`,
  );
  database.transaction(() => {
    for (const request of options.requests ?? []) enqueue.run({ ...request, queuedAt });
    advance(database, "git_context", 0, null, queuedGitRequest(database));
  })();
  const limit = boundedBatchSize(options.batchSize);
  let rows = 0;
  while (rows < limit && timer.canStart()) {
    const request = queuedGitRequest(database);
    if (!request) break;
    const result = options.resolve(request);
    database.transaction(() => {
      options.commit(result);
      database.prepare(`delete from maintenance_git_context_queue where context_id = ?`).run(request.contextId);
      advance(database, "git_context", 1, request.contextId, queuedGitRequest(database));
    })();
    rows += 1;
  }
  return timer.result(rows);
}
