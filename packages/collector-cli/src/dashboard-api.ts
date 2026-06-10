import type Database from "better-sqlite3";

/**
 * Read-only queries backing the local dashboard. Everything here serves
 * localhost only and reads the same ledger the collector writes — numbers on
 * the dashboard are the ledger, not a copy of it. Each payload carries enough
 * lineage ("receipts") that a displayed figure can be traced to its rows.
 */

export function dashboardSummary(db: Database.Database, days = 30) {
  const since = sinceIso(days);
  const totals = db
    .prepare(
      `select
        count(*) as events,
        sum(case when input_tokens is not null or output_tokens is not null then 1 else 0 end) as tokenEvents,
        coalesce(sum(input_tokens), 0) as inputTokens,
        coalesce(sum(output_tokens), 0) as outputTokens,
        coalesce(sum(cache_read_tokens), 0) as cacheReadTokens,
        coalesce(sum(cost_usd), 0) as costUsd,
        count(distinct session_id) as sessions,
        count(distinct case when input_tokens is not null then session_id end) as sessionsWithTokens,
        min(observed_at) as oldest,
        max(observed_at) as newest
      from buffered_events where observed_at >= ?`,
    )
    .get(since) as Record<string, number | string | null>;

  const bySource = db
    .prepare(
      `select source,
        count(*) as events,
        count(distinct session_id) as sessions,
        count(distinct case when input_tokens is not null then session_id end) as sessionsWithTokens,
        coalesce(sum(input_tokens), 0) as inputTokens,
        coalesce(sum(output_tokens), 0) as outputTokens,
        coalesce(sum(cost_usd), 0) as costUsd
      from buffered_events where observed_at >= ? group by source order by costUsd desc`,
    )
    .all(since);

  const daily = db
    .prepare(
      `select substr(observed_at, 1, 10) as day,
        coalesce(sum(cost_usd), 0) as costUsd,
        coalesce(sum(input_tokens), 0) + coalesce(sum(output_tokens), 0) as tokens
      from buffered_events where observed_at >= ? group by day order by day asc`,
    )
    .all(since);

  const byModel = db
    .prepare(
      `select model,
        count(*) as calls,
        coalesce(sum(input_tokens), 0) as inputTokens,
        coalesce(sum(output_tokens), 0) as outputTokens,
        coalesce(sum(cache_read_tokens), 0) as cacheReadTokens,
        coalesce(sum(cost_usd), 0) as costUsd
      from buffered_events
      where observed_at >= ? and model is not null
        and (input_tokens is not null or output_tokens is not null or cost_usd is not null)
      group by model order by costUsd desc, inputTokens desc limit 12`,
    )
    .all(since);

  const actionMix = db
    .prepare(
      `select action_class as actionClass, count(*) as n
      from buffered_events
      where observed_at >= ? and event_type in ('tool_use','tool_result')
      group by action_class order by n desc`,
    )
    .all(since);

  return { days, since, totals, bySource, daily, byModel, actionMix };
}

export function dashboardSessions(db: Database.Database, days = 30, limit = 60) {
  const since = sinceIso(days);
  return db
    .prepare(
      `select session_id as sessionId, source,
        min(observed_at) as startedAt, max(observed_at) as endedAt,
        count(*) as events,
        coalesce(sum(input_tokens), 0) as inputTokens,
        coalesce(sum(output_tokens), 0) as outputTokens,
        coalesce(sum(cache_read_tokens), 0) as cacheReadTokens,
        coalesce(sum(cost_usd), 0) as costUsd,
        max(repo_hash) as repoHash, max(branch_hash) as branchHash,
        count(distinct repo_hash) as repoCount
      from buffered_events
      where session_id is not null and observed_at >= ?
      group by session_id, source
      order by costUsd desc, events desc
      limit ?`,
    )
    .all(since, Math.min(limit, 200));
}

export function dashboardRepos(db: Database.Database, days = 30) {
  const since = sinceIso(days);
  // Tokens ride api_request events (no repo columns); repos ride hook events.
  // Attribution is session-level: a session's repo is its dominant repo_hash.
  return db
    .prepare(
      `with sessions as (
        select session_id, max(repo_hash) as repoHash,
          count(distinct branch_hash) as branches,
          coalesce(sum(input_tokens), 0) as inputTokens,
          coalesce(sum(output_tokens), 0) as outputTokens,
          coalesce(sum(cost_usd), 0) as costUsd
        from buffered_events
        where session_id is not null and observed_at >= ?
        group by session_id
      )
      select repoHash, count(*) as sessions, sum(branches) as branchRefs,
        sum(inputTokens) as inputTokens, sum(outputTokens) as outputTokens,
        sum(costUsd) as costUsd
      from sessions
      group by repoHash
      order by costUsd desc
      limit 12`,
    )
    .all(since);
}

export function dashboardSessionDetail(db: Database.Database, sessionId: string) {
  const rollup = db
    .prepare(
      `select session_id as sessionId, source,
        min(observed_at) as startedAt, max(observed_at) as endedAt,
        count(*) as events,
        coalesce(sum(input_tokens), 0) as inputTokens,
        coalesce(sum(output_tokens), 0) as outputTokens,
        coalesce(sum(cache_read_tokens), 0) as cacheReadTokens,
        coalesce(sum(cost_usd), 0) as costUsd,
        sum(case when input_tokens is not null then 1 else 0 end) as tokenEvents
      from buffered_events where session_id = ? group by session_id, source`,
    )
    .get(sessionId);

  if (!rollup) return null;

  const receipts = {
    linkage: db
      .prepare(
        `select repo_hash as repoHash, branch_hash as branchHash, head_sha as headSha, count(*) as events
        from buffered_events
        where session_id = ? and (repo_hash is not null or branch_hash is not null or head_sha is not null)
        group by repo_hash, branch_hash, head_sha order by events desc limit 10`,
      )
      .all(sessionId),
    eventTypes: db
      .prepare(
        `select event_type as eventType, count(*) as n from buffered_events
        where session_id = ? group by event_type order by n desc`,
      )
      .all(sessionId),
    actionMix: db
      .prepare(
        `select action_class as actionClass, count(*) as n from buffered_events
        where session_id = ? and event_type in ('tool_use','tool_result')
        group by action_class order by n desc`,
      )
      .all(sessionId),
    models: db
      .prepare(
        `select model, count(*) as calls,
          coalesce(sum(input_tokens), 0) as inputTokens,
          coalesce(sum(output_tokens), 0) as outputTokens,
          coalesce(sum(cost_usd), 0) as costUsd
        from buffered_events where session_id = ? and model is not null
        group by model order by costUsd desc`,
      )
      .all(sessionId),
    suppression: db
      .prepare(
        `select count(*) as suppressedEvents from buffered_events
        where session_id = ? and suppressed_fields_json != '[]'`,
      )
      .get(sessionId),
  };

  return { rollup, receipts };
}

function sinceIso(days: number) {
  const clamped = Math.max(1, Math.min(days, 365));
  return new Date(Date.now() - clamped * 24 * 60 * 60 * 1000).toISOString();
}
