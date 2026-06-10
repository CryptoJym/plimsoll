import type Database from "better-sqlite3";

/**
 * Read-only queries backing the local dashboard. Everything here serves
 * localhost only and reads the same ledger the collector writes — numbers on
 * the dashboard are the ledger, not a copy of it. Each payload carries enough
 * lineage ("receipts") that a displayed figure can be traced to its rows.
 */

/**
 * Session-grain attribution fragments. A session can touch several repos and
 * (rarely) several account hashes — the 2026-06-10 v1→v2 collector swap split
 * one human across two hash forms mid-session. Display attribution must follow
 * the dominant weight, never `max(hash)` (a lexicographic accident):
 *  - repo: most linked events wins. Cost rides api_request rows, which carry
 *    no repo columns, so event count is the only honest session-grain weight.
 *  - account: most attributed cost wins (cost rows carry the account); event
 *    count breaks ties for cost-free sessions.
 * Both expect one `?` bind: the window's since-ISO.
 */
const dominantRepoSql = `
  select session_id, repo_hash from (
    select session_id, repo_hash,
      row_number() over (
        partition by session_id
        order by count(*) desc, count(distinct branch_hash) desc, repo_hash
      ) as rn
    from buffered_events
    where session_id is not null and repo_hash is not null and observed_at >= ?
    group by session_id, repo_hash
  ) where rn = 1`;

// Aliases (local account_aliases table, issue 0023) apply before dominance:
// once two hash forms are declared the same person, a session that straddles
// them has one candidate and merges at the root rather than in display code.
const dominantAccountSql = `
  select session_id, account_hash from (
    select e.session_id,
      coalesce(al.canonical_hash, e.account_hash) as account_hash,
      row_number() over (
        partition by e.session_id
        order by sum(coalesce(e.cost_usd, 0)) desc, count(*) desc,
          coalesce(al.canonical_hash, e.account_hash)
      ) as rn
    from buffered_events e
    left join account_aliases al on al.alias_hash = e.account_hash
    where e.session_id is not null and e.account_hash is not null and e.observed_at >= ?
    group by e.session_id, coalesce(al.canonical_hash, e.account_hash)
  ) where rn = 1`;

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

  // unpricedCalls > 0 with costUsd 0 means "no vendor rate on file", which the
  // dashboard must render as unpriced — never as $0.00 (issue 0025).
  const byModel = db
    .prepare(
      `select model,
        count(*) as calls,
        sum(case when cost_usd is null then 1 else 0 end) as unpricedCalls,
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
  // Aggregate first, then join labels from the outer row. The previous shape
  // called max() inside a correlated subquery's WHERE, which SQLite rejects at
  // run time ("misuse of aggregate function") — /api/sessions 500'd on every
  // request while proof stayed green because no check executed this query.
  return db
    .prepare(
      `with rollup as (
        select session_id as sessionId, source,
          min(observed_at) as startedAt, max(observed_at) as endedAt,
          count(*) as events,
          coalesce(sum(input_tokens), 0) as inputTokens,
          coalesce(sum(output_tokens), 0) as outputTokens,
          coalesce(sum(cache_read_tokens), 0) as cacheReadTokens,
          coalesce(sum(cost_usd), 0) as costUsd,
          max(branch_hash) as branchHash,
          count(distinct repo_hash) as repoCount
        from buffered_events
        where session_id is not null and observed_at >= ?
        group by session_id, source
      ),
      dominant as (${dominantRepoSql})
      select r.*, d.repo_hash as repoHash,
        (select label from repo_labels where repo_hash = d.repo_hash) as repoLabel
      from rollup r
      left join dominant d on d.session_id = r.sessionId
      order by r.costUsd desc, r.events desc
      limit ?`,
    )
    .all(since, since, Math.min(limit, 200));
}

export function dashboardRepos(db: Database.Database, days = 30) {
  const since = sinceIso(days);
  // Tokens ride api_request events (no repo columns); repos ride hook events.
  // Attribution is session-level: a session's repo is its dominant repo_hash
  // by linked-event count (see dominantRepoSql), never lexicographic max().
  return db
    .prepare(
      `with dominant as (${dominantRepoSql}),
      sessions as (
        select e.session_id, d.repo_hash as repoHash,
          count(distinct e.branch_hash) as branches,
          coalesce(sum(e.input_tokens), 0) as inputTokens,
          coalesce(sum(e.output_tokens), 0) as outputTokens,
          coalesce(sum(e.cost_usd), 0) as costUsd
        from buffered_events e
        left join dominant d on d.session_id = e.session_id
        where e.session_id is not null and e.observed_at >= ?
        group by e.session_id
      )
      select s.repoHash, l.label, count(*) as sessions, sum(s.branches) as branchRefs,
        sum(s.inputTokens) as inputTokens, sum(s.outputTokens) as outputTokens,
        sum(s.costUsd) as costUsd
      from sessions s left join repo_labels l on l.repo_hash = s.repoHash
      group by s.repoHash
      order by costUsd desc
      limit 12`,
    )
    .all(since, since);
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

export type SubscriptionConfig = {
  account: string;
  plan: string;
  usdPerMonth: number;
  vendor: "anthropic" | "openai" | "other";
};

export function dashboardAccounts(
  db: Database.Database,
  subscriptions: SubscriptionConfig[],
  days = 30,
) {
  const since = sinceIso(days);
  // A session's whole cost is attributed to its cost-dominant account hash
  // and event-dominant repo hash. max(hash) attributed straddle sessions to
  // whichever hash happened to sort higher — on 2026-06-10 that moved ~$486
  // of real spend onto a retired v1-era hash form.
  const rows = db
    .prepare(
      `with domaccount as (${dominantAccountSql}),
      domrepo as (${dominantRepoSql}),
      sessions as (
        select e.session_id, a.account_hash as accountHash, r.repo_hash as repoHash,
          group_concat(distinct e.machine) as machines,
          coalesce(sum(e.cost_usd), 0) as costUsd,
          coalesce(sum(e.input_tokens), 0) as inputTokens,
          coalesce(sum(e.output_tokens), 0) as outputTokens
        from buffered_events e
        left join domaccount a on a.session_id = e.session_id
        left join domrepo r on r.session_id = e.session_id
        where e.session_id is not null and e.observed_at >= ?
        group by e.session_id
      )
      select s.accountHash,
        (select label from account_labels where account_hash = s.accountHash) as label,
        (select email from account_labels where account_hash = s.accountHash) as email,
        group_concat(distinct s.machines) as machines,
        count(*) as sessions,
        sum(case when p.repo_hash is not null then s.costUsd else 0 end) as priorityUsd,
        sum(case when p.repo_hash is null and s.repoHash is not null then s.costUsd else 0 end) as otherUsd,
        sum(case when s.repoHash is null then s.costUsd else 0 end) as unlinkedUsd,
        sum(s.costUsd) as totalUsd,
        sum(s.inputTokens) as inputTokens, sum(s.outputTokens) as outputTokens
      from sessions s left join priority_repos p on p.repo_hash = s.repoHash
      group by s.accountHash
      order by totalUsd desc`,
    )
    .all(since, since, since) as Array<Record<string, unknown> & { accountHash: string | null; label: string | null; totalUsd: number }>;

  const windowMonths = days / 30.44;
  const accounts = rows.map((row) => {
    const subscription = subscriptions.find(
      (sub) => sub.account === row.accountHash || (row.label && sub.account === row.label),
    );
    const planCostWindow = subscription
      ? Number((subscription.usdPerMonth * windowMonths).toFixed(2))
      : null;
    return {
      ...row,
      machines: typeof row.machines === "string" ? [...new Set(row.machines.split(","))] : [],
      subscription: subscription
        ? {
            plan: subscription.plan,
            usdPerMonth: subscription.usdPerMonth,
            planCostWindow,
            leverage:
              planCostWindow && planCostWindow > 0
                ? Number((Number(row.totalUsd) / planCostWindow).toFixed(4))
                : null,
          }
        : null,
    };
  });

  const buckets = {
    priorityUsd: Number(rows.reduce((a, r) => a + Number(r.priorityUsd ?? 0), 0).toFixed(4)),
    otherUsd: Number(rows.reduce((a, r) => a + Number(r.otherUsd ?? 0), 0).toFixed(4)),
    unlinkedUsd: Number(rows.reduce((a, r) => a + Number(r.unlinkedUsd ?? 0), 0).toFixed(4)),
  };

  return { days, buckets, accounts, priorityRepoCount: (db.prepare(`select count(*) as n from priority_repos`).get() as {n:number}).n };
}

export function dashboardRepoDetail(db: Database.Database, repoHash: string, days = 30) {
  const since = sinceIso(days);
  const label = (db.prepare(`select label from repo_labels where repo_hash = ?`).get(repoHash) as
    | { label: string }
    | undefined)?.label;
  const sessionIds = db
    .prepare(
      `select distinct session_id as id from buffered_events
       where repo_hash = ? and session_id is not null and observed_at >= ?`,
    )
    .all(repoHash, since) as Array<{ id: string }>;
  if (sessionIds.length === 0 && !label) return null;
  const ids = sessionIds.map((row) => row.id);
  const marks = ids.map(() => "?").join(",") || "''";
  const totals = db
    .prepare(
      `select count(distinct session_id) as sessions, count(*) as events,
         coalesce(sum(input_tokens),0) as inputTokens, coalesce(sum(output_tokens),0) as outputTokens,
         coalesce(sum(cost_usd),0) as costUsd
       from buffered_events where session_id in (${marks}) and observed_at >= ?`,
    )
    .get(...ids, since);
  const daily = db
    .prepare(
      `select substr(observed_at,1,10) as day, coalesce(sum(cost_usd),0) as costUsd
       from buffered_events where session_id in (${marks}) and observed_at >= ?
       group by day order by day asc`,
    )
    .all(...ids, since);
  const branches = db
    .prepare(
      `select branch_hash as branchHash, count(*) as events, count(distinct session_id) as sessions
       from buffered_events where repo_hash = ? and branch_hash is not null and observed_at >= ?
       group by branch_hash order by events desc limit 15`,
    )
    .all(repoHash, since);
  const actionMix = db
    .prepare(
      `select action_class as actionClass, count(*) as n from buffered_events
       where session_id in (${marks}) and event_type in ('tool_use','tool_result') and observed_at >= ?
       group by action_class order by n desc`,
    )
    .all(...ids, since);
  const models = db
    .prepare(
      `select model, coalesce(sum(input_tokens),0) as inputTokens,
         coalesce(sum(output_tokens),0) as outputTokens, coalesce(sum(cost_usd),0) as costUsd
       from buffered_events where session_id in (${marks}) and model is not null and observed_at >= ?
       group by model order by costUsd desc limit 8`,
    )
    .all(...ids, since);
  return { repoHash, label: label ?? null, days, totals, daily, branches, actionMix, models };
}

function sinceIso(days: number) {
  const clamped = Math.max(1, Math.min(days, 365));
  return new Date(Date.now() - clamped * 24 * 60 * 60 * 1000).toISOString();
}
