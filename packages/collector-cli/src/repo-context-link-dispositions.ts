import type Database from "better-sqlite3";

import type { RepoContextResult } from "./repo-context";
import type { RepoContextReplayReason } from "./repo-context-replay-state";

const DISPOSITION_REASONS = new Set<RepoContextReplayReason>([
  "source_unavailable",
  "boundary_unavailable",
  "resolution_failed",
  "worker_crash",
  "replay_exhausted",
]);

export function ensureRepoContextLinkDispositionSchema(database: Database.Database) {
  database.exec(`
    create table if not exists repo_context_link_dispositions (
      event_id text primary key,
      context_id text not null,
      reason text not null check (reason in (
        'source_unavailable', 'boundary_unavailable',
        'resolution_failed', 'worker_crash', 'replay_exhausted'
      )),
      attempt_count integer not null check (attempt_count >= 1),
      last_attempt_at text not null,
      expired_at text,
      re_resolved_at text,
      resolver_version text
    );
    create index if not exists idx_repo_context_link_dispositions_context
      on repo_context_link_dispositions (context_id, expired_at, re_resolved_at, event_id);
    create index if not exists idx_repo_context_link_dispositions_expiry
      on repo_context_link_dispositions (expired_at, re_resolved_at, last_attempt_at, event_id);
  `);
}

export function recordRepoContextFailureDispositions(
  database: Database.Database,
  input: {
    contextId: string;
    reason: RepoContextReplayReason;
    resolverVersion?: string | null;
    limit?: number;
    at?: string;
  },
) {
  ensureRepoContextLinkDispositionSchema(database);
  if (!DISPOSITION_REASONS.has(input.reason)) throw new Error("repo_context_disposition_reason_invalid");
  const limit = Math.max(1, Math.min(Math.trunc(input.limit ?? 256), 256));
  const at = input.at ?? new Date().toISOString();
  const links = database.prepare(
    `select event_id as eventId
     from repo_context_event_links
     where context_id = ? and fill_pending = 1 and context_conflict = 0
     order by event_id limit ?`,
  ).all(input.contextId, limit) as Array<{ eventId: string }>;
  const existing = database.prepare(
    `select context_id as contextId from repo_context_link_dispositions where event_id = ?`,
  );
  const upsert = database.prepare(
    `insert into repo_context_link_dispositions
       (event_id, context_id, reason, attempt_count, last_attempt_at,
        expired_at, re_resolved_at, resolver_version)
     values (@eventId, @contextId, @reason, 1, @at, null, null, @resolverVersion)
     on conflict(event_id) do update set
       reason = case when repo_context_link_dispositions.expired_at is null
         then excluded.reason else repo_context_link_dispositions.reason end,
       attempt_count = repo_context_link_dispositions.attempt_count + 1,
       last_attempt_at = excluded.last_attempt_at,
       resolver_version = excluded.resolver_version
     where repo_context_link_dispositions.context_id = excluded.context_id`,
  );
  return database.transaction(() => {
    let changed = 0;
    for (const link of links) {
      const prior = existing.get(link.eventId) as { contextId: string } | undefined;
      if (prior && prior.contextId !== input.contextId) {
        throw new Error("repo_context_disposition_identity_conflict");
      }
      changed += upsert.run({
        eventId: link.eventId,
        contextId: input.contextId,
        reason: input.reason,
        at,
        resolverVersion: input.resolverVersion ?? null,
      }).changes;
    }
    return changed;
  }).immediate();
}

export function expireRepoContextLinks(
  database: Database.Database,
  input: {
    completedPasses: number;
    expireAfterCompletePasses: number;
    limit: number;
    at?: string;
  },
) {
  ensureRepoContextLinkDispositionSchema(database);
  const threshold = Math.max(2, Math.min(Math.trunc(input.expireAfterCompletePasses), 64));
  if (input.completedPasses < threshold) return 0;
  const limit = Math.max(1, Math.min(Math.trunc(input.limit), 256));
  const at = input.at ?? new Date().toISOString();
  const rows = database.prepare(
    `select l.event_id as eventId, l.context_id as contextId,
       coalesce(d.reason, 'source_unavailable') as reason,
       coalesce(d.attempt_count, 0) as attemptCount,
       d.context_id as dispositionContextId
     from repo_context_event_links l
     left join repo_context_link_dispositions d on d.event_id = l.event_id
     where l.fill_pending = 1 and l.context_conflict = 0
       and not exists (select 1 from repo_context_results r where r.context_id = l.context_id)
       and not exists (select 1 from repo_context_suppressions s where s.context_id = l.context_id)
       and not exists (select 1 from repo_context_inflight i where i.context_id = l.context_id)
       and not exists (select 1 from repo_context_handoffs h where h.context_id = l.context_id)
       and (d.event_id is null or d.expired_at is null)
     order by l.event_id
     limit ?`,
  ).all(limit) as Array<{
    eventId: string;
    contextId: string;
    reason: RepoContextReplayReason;
    attemptCount: number;
    dispositionContextId: string | null;
  }>;
  const upsert = database.prepare(
    `insert into repo_context_link_dispositions
       (event_id, context_id, reason, attempt_count, last_attempt_at,
        expired_at, re_resolved_at, resolver_version)
     values (@eventId, @contextId, @reason, @attemptCount, @at, @at, null, null)
     on conflict(event_id) do update set
       expired_at = coalesce(repo_context_link_dispositions.expired_at, excluded.expired_at),
       last_attempt_at = excluded.last_attempt_at
     where repo_context_link_dispositions.context_id = excluded.context_id`,
  );
  const close = database.prepare(
    `update repo_context_event_links set fill_pending = 0
     where event_id = ? and context_id = ? and fill_pending = 1`,
  );
  return database.transaction(() => {
    let expired = 0;
    for (const row of rows) {
      if (row.dispositionContextId && row.dispositionContextId !== row.contextId) {
        throw new Error("repo_context_disposition_identity_conflict");
      }
      upsert.run({ ...row, attemptCount: Math.max(1, row.attemptCount), at });
      expired += close.run(row.eventId, row.contextId).changes;
    }
    return expired;
  }).immediate();
}

export function reResolveExpiredRepoContextLinks(
  database: Database.Database,
  result: RepoContextResult,
  limit = 256,
) {
  ensureRepoContextLinkDispositionSchema(database);
  if (!result.repoHash) return 0;
  const bounded = Math.max(1, Math.min(Math.trunc(limit), 256));
  const rows = database.prepare(
    `select e.rowid, e.id as eventId, e.repo_hash as repoHash
     from repo_context_link_dispositions d
     join repo_context_event_links l
       on l.event_id = d.event_id and l.context_id = d.context_id
     join buffered_events e on e.id = d.event_id
     where d.context_id = ? and d.expired_at is not null and d.re_resolved_at is null
       and l.fill_pending = 0 and l.context_conflict = 0
       and not exists (select 1 from repo_context_suppressions s where s.context_id = d.context_id)
     order by d.event_id limit ?`,
  ).all(result.contextId, bounded) as Array<{
    rowid: number;
    eventId: string;
    repoHash: string | null;
  }>;
  const fill = database.prepare(
    `update buffered_events set
       repo_hash = coalesce(repo_hash, @repoHash),
       branch_hash = coalesce(branch_hash, @branchHash),
       head_sha = coalesce(head_sha, @headSha)
     where rowid = @rowid and id = @eventId
       and not exists (
         select 1 from repo_context_suppressions where context_id = @contextId
       )`,
  );
  const reversed = database.prepare(
    `update repo_context_link_dispositions
     set re_resolved_at = ?, resolver_version = ?
     where event_id = ? and context_id = ?
       and expired_at is not null and re_resolved_at is null`,
  );
  const conflict = database.prepare(
    `update repo_context_event_links set context_conflict = 1
     where event_id = ? and context_id = ? and fill_pending = 0 and context_conflict = 0`,
  );
  const at = new Date().toISOString();
  return database.transaction(() => {
    let count = 0;
    for (const row of rows) {
      if (row.repoHash && row.repoHash !== result.repoHash) {
        conflict.run(row.eventId, result.contextId);
        continue;
      }
      fill.run({ ...row, ...result });
      count += reversed.run(at, result.resolverVersion, row.eventId, result.contextId).changes;
    }
    return count;
  }).immediate();
}

/** Rollback removes only drain evidence. Callers must disable the drain first;
 * already-filled or expired link state is intentionally not reopened. */
export function rollbackRepoContextLinkDispositionMigration(database: Database.Database) {
  database.exec(`drop table if exists repo_context_link_dispositions`);
}
