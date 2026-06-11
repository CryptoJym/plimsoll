/**
 * Local efficiency report: joins AI sessions in the work ledger to GitHub
 * pull requests via privacy-safe linkage keys (hashed remote + hashed branch
 * + plain commit shas) and computes the token-use-to-accomplishment metrics
 * the product exists for:
 *
 *   - tokens / cost per merged PR
 *   - join rate (sessions linked to a PR / sessions with linkage keys)
 *   - Validated Delivery Yield v1 = joined PRs that merged with passing
 *     checks / joined PRs (validation evidence dimension = CI checks for now)
 *
 * Runs entirely locally against the collector ledger + GitHub REST API; no
 * cloud ingest required. Evidence lands in evidence/.
 *
 * Usage:
 *   GITHUB_TOKEN=... tsx scripts/efficiency-report.ts \
 *     --repository owner/repo [--since-days 30] [--ledger /path/to/work-ledger.sqlite]
 *
 *   # descriptive, local-only, no GitHub needed (issue 0010):
 *   tsx scripts/efficiency-report.ts --patterns [--since-days 90] [--ledger ...]
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import Database from "better-sqlite3";

import { collectorBufferPath } from "../packages/collector-cli/src/config";
import {
  branchLinkageHash,
  remoteLinkageHash,
} from "../packages/shared/src/linkage";

type SessionRow = {
  sessionId: string;
  source: string;
  startedAt: string;
  endedAt: string;
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheCreationTokens: number | null;
  costUsd: number | null;
  events: number;
  editEvents: number;
  shellEvents: number;
  repoHash: string | null;
  branchHash: string | null;
  headShas: string | null;
};

type PullSummary = {
  number: number;
  state: string;
  merged: boolean;
  branchHash?: string;
  headSha?: string;
  mergeCommitSha?: string;
  updatedAt: string;
  checks: "passed" | "failed" | "none" | "unknown";
};

function optionValue(name: string) {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

async function githubJson(url: string, token: string | undefined) {
  const response = await fetch(url, {
    headers: {
      accept: "application/vnd.github+json",
      "x-github-api-version": "2022-11-28",
      "user-agent": "Plimsoll/0.1",
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
  });
  if (!response.ok) {
    throw new Error(`GitHub ${response.status} for ${url}`);
  }
  return (await response.json()) as unknown;
}

/**
 * Descriptive pattern summary over the user's OWN ledger (issue 0010 / #10).
 * Open-tier by definition: it reports what happened — model mix, cache-read
 * ratio, action-class distribution, the costliest sessions — and offers no
 * comparison, benchmark, score, or advice. Pure SQL over promoted columns
 * (no payload_json parsing) so it stays well under the 5s budget.
 */
export function buildPatternsReport(db: Database.Database, sinceDays: number): string {
  const since = new Date(Date.now() - sinceDays * 24 * 60 * 60 * 1000).toISOString();
  const num = (n: unknown) => Number(n ?? 0).toLocaleString("en-US");
  const usd = (n: unknown) =>
    "$" + Number(n ?? 0).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const pct = (n: number) => (Number.isFinite(n) ? (n * 100).toFixed(1) + "%" : "—");

  const byModel = db
    .prepare(
      `select model,
         count(*) as calls,
         coalesce(sum(input_tokens), 0) as inputTokens,
         coalesce(sum(output_tokens), 0) as outputTokens,
         coalesce(sum(cache_read_tokens), 0) as cacheReadTokens,
         coalesce(sum(cache_creation_tokens), 0) as cacheCreationTokens,
         coalesce(sum(cost_usd), 0) as costUsd
       from buffered_events
       where observed_at >= ? and model is not null
       group by model order by costUsd desc, inputTokens desc`,
    )
    .all(since) as Array<Record<string, number | string>>;

  const actionMix = db
    .prepare(
      `select action_class as actionClass, count(*) as n
       from buffered_events
       where observed_at >= ? and event_type in ('tool_use','tool_result')
       group by action_class order by n desc`,
    )
    .all(since) as Array<{ actionClass: string | null; n: number }>;
  const actionTotal = actionMix.reduce((s, r) => s + r.n, 0) || 1;

  const topSessions = db
    .prepare(
      `select session_id as sessionId,
         coalesce(sum(cost_usd), 0) as costUsd,
         coalesce(sum(input_tokens), 0) + coalesce(sum(output_tokens), 0) as tokens
       from buffered_events
       where observed_at >= ? and session_id is not null
       group by session_id order by costUsd desc limit 10`,
    )
    .all(since) as Array<{ sessionId: string; costUsd: number; tokens: number }>;
  const sessionMix = db.prepare(
    `select action_class as actionClass, count(*) as n
     from buffered_events
     where session_id = ? and event_type in ('tool_use','tool_result')
     group by action_class order by n desc`,
  );

  const out: string[] = [];
  out.push(`# Plimsoll patterns — your own ledger, last ${sinceDays}d`);
  out.push(`Descriptive only: counts and ratios over your data. No comparisons, scores, or advice.`);
  out.push("");
  out.push(`## Tokens & cost by model`);
  out.push(`| model | calls | input | output | cache read | cache write | cost |`);
  out.push(`|---|--:|--:|--:|--:|--:|--:|`);
  for (const m of byModel) {
    out.push(
      `| ${m.model} | ${num(m.calls)} | ${num(m.inputTokens)} | ${num(m.outputTokens)} | ${num(m.cacheReadTokens)} | ${num(m.cacheCreationTokens)} | ${usd(m.costUsd)} |`,
    );
  }
  out.push("");
  out.push(`## Cache-read ratio by model`);
  out.push(`Definition: cache_read / (input + cache_read). Higher = more context served from cache.`);
  out.push(`| model | ratio |`);
  out.push(`|---|--:|`);
  for (const m of byModel) {
    const input = Number(m.inputTokens);
    const cacheRead = Number(m.cacheReadTokens);
    const denom = input + cacheRead;
    out.push(`| ${m.model} | ${denom > 0 ? pct(cacheRead / denom) : "—"} |`);
  }
  out.push("");
  out.push(`## Action-class distribution (tool events)`);
  out.push(`| action class | events | share |`);
  out.push(`|---|--:|--:|`);
  for (const a of actionMix) {
    out.push(`| ${a.actionClass ?? "—"} | ${num(a.n)} | ${pct(a.n / actionTotal)} |`);
  }
  out.push("");
  out.push(`## Top sessions by cost (with action mix)`);
  out.push(`| session | cost | tokens | action mix |`);
  out.push(`|---|--:|--:|---|`);
  for (const s of topSessions) {
    const mix = (sessionMix.all(s.sessionId) as Array<{ actionClass: string | null; n: number }>)
      .map((r) => `${r.actionClass ?? "—"} ${r.n}`)
      .join(" · ");
    out.push(`| ${s.sessionId.slice(0, 8)} | ${usd(s.costUsd)} | ${num(s.tokens)} | ${mix || "no tool events"} |`);
  }
  return out.join("\n");
}

function runPatterns(ledgerPath: string, sinceDays: number) {
  const db = new Database(ledgerPath, { readonly: true });
  const report = buildPatternsReport(db, sinceDays);
  db.close();
  console.log(report);
}

async function main() {
  if (process.argv.includes("--patterns")) {
    const sinceDays = Number(optionValue("--since-days") ?? 90);
    const ledgerPath = optionValue("--ledger") ?? collectorBufferPath();
    runPatterns(ledgerPath, sinceDays);
    return;
  }
  const repository = optionValue("--repository") ?? process.env.GITHUB_REPOSITORY;
  if (!repository?.includes("/")) {
    throw new Error("Pass --repository owner/repo or set GITHUB_REPOSITORY.");
  }
  const [owner, repo] = repository.split("/");
  const sinceDays = Number(optionValue("--since-days") ?? 30);
  const since = new Date(Date.now() - sinceDays * 24 * 60 * 60 * 1000).toISOString();
  const ledgerPath = optionValue("--ledger") ?? collectorBufferPath();
  const token = process.env.GITHUB_TOKEN ?? process.env.GH_TOKEN;
  const repoHash = remoteLinkageHash(`https://github.com/${owner}/${repo}.git`);

  const db = new Database(ledgerPath, { readonly: true });
  const sessions = db
    .prepare(
      `select session_id as sessionId, source,
        min(observed_at) as startedAt, max(observed_at) as endedAt,
        sum(input_tokens) as inputTokens, sum(output_tokens) as outputTokens,
        sum(cache_read_tokens) as cacheReadTokens,
        sum(cache_creation_tokens) as cacheCreationTokens, sum(cost_usd) as costUsd,
        count(*) as events,
        sum(case when action_class in ('edit','write') then 1 else 0 end) as editEvents,
        sum(case when action_class = 'shell' then 1 else 0 end) as shellEvents,
        max(repo_hash) as repoHash, max(branch_hash) as branchHash,
        group_concat(distinct head_sha) as headShas
      from buffered_events
      where session_id is not null and observed_at >= ?
      group by session_id, source`,
    )
    .all(since) as SessionRow[];
  db.close();

  const sessionsWithLinkage = sessions.filter((row) => row.repoHash || row.branchHash || row.headShas);
  const repoSessions = sessionsWithLinkage.filter((row) => !row.repoHash || row.repoHash === repoHash);

  const pullsRaw = (await githubJson(
    `https://api.github.com/repos/${owner}/${repo}/pulls?state=all&sort=updated&direction=desc&per_page=100`,
    token,
  )) as Array<Record<string, unknown>>;

  const pulls: PullSummary[] = pullsRaw
    .filter((pull) => String(pull.updated_at ?? "") >= since)
    .map((pull) => {
      const head = pull.head as { ref?: string; sha?: string } | undefined;
      return {
        number: Number(pull.number),
        state: String(pull.state),
        merged: Boolean(pull.merged_at),
        branchHash: branchLinkageHash(head?.ref),
        headSha: head?.sha,
        mergeCommitSha: typeof pull.merge_commit_sha === "string" ? pull.merge_commit_sha : undefined,
        updatedAt: String(pull.updated_at),
        checks: "unknown" as const,
      };
    });

  // Join sessions to PRs: branch hash match, or session head sha appears as the
  // PR head/merge sha. Time windows intentionally loose — branch hash is the
  // primary key; sha endpoints catch detached/renamed cases.
  const joins: Array<{ session: SessionRow; pull: PullSummary; via: string }> = [];
  for (const session of repoSessions) {
    const shas = new Set((session.headShas ?? "").split(",").filter(Boolean));
    for (const pull of pulls) {
      if (session.branchHash && pull.branchHash === session.branchHash) {
        joins.push({ session, pull, via: "branch_hash" });
      } else if (pull.headSha && shas.has(pull.headSha)) {
        joins.push({ session, pull, via: "head_sha" });
      } else if (pull.mergeCommitSha && shas.has(pull.mergeCommitSha)) {
        joins.push({ session, pull, via: "merge_sha" });
      }
    }
  }

  // Check runs for joined PRs only (cap GitHub calls).
  const joinedPullNumbers = [...new Set(joins.map((entry) => entry.pull.number))];
  for (const number of joinedPullNumbers.slice(0, 20)) {
    const pull = pulls.find((entry) => entry.number === number);
    if (!pull?.headSha) continue;
    try {
      const checkData = (await githubJson(
        `https://api.github.com/repos/${owner}/${repo}/commits/${pull.headSha}/check-runs?per_page=50`,
        token,
      )) as { total_count: number; check_runs: Array<{ status: string; conclusion: string | null }> };
      if (checkData.total_count === 0) {
        pull.checks = "none";
      } else {
        const conclusions = checkData.check_runs.map((run) => run.conclusion);
        pull.checks = conclusions.some((c) => c === "failure" || c === "timed_out" || c === "cancelled")
          ? "failed"
          : conclusions.every((c) => c === "success" || c === "neutral" || c === "skipped")
            ? "passed"
            : "unknown";
      }
    } catch {
      pull.checks = "unknown";
    }
  }

  const byPull = new Map<number, { pull: PullSummary; sessions: SessionRow[]; via: Set<string> }>();
  for (const entry of joins) {
    const bucket = byPull.get(entry.pull.number) ?? { pull: entry.pull, sessions: [], via: new Set<string>() };
    if (!bucket.sessions.some((row) => row.sessionId === entry.session.sessionId)) {
      bucket.sessions.push(entry.session);
    }
    bucket.via.add(entry.via);
    byPull.set(entry.pull.number, bucket);
  }

  const pullRows = [...byPull.values()].map((bucket) => ({
    pull: bucket.pull.number,
    merged: bucket.pull.merged,
    checks: bucket.pull.checks,
    joinedVia: [...bucket.via],
    sessions: bucket.sessions.length,
    inputTokens: bucket.sessions.reduce((sum, row) => sum + (row.inputTokens ?? 0), 0),
    outputTokens: bucket.sessions.reduce((sum, row) => sum + (row.outputTokens ?? 0), 0),
    cacheReadTokens: bucket.sessions.reduce((sum, row) => sum + (row.cacheReadTokens ?? 0), 0),
    cacheCreationTokens: bucket.sessions.reduce((sum, row) => sum + (row.cacheCreationTokens ?? 0), 0),
    costUsd: Number(bucket.sessions.reduce((sum, row) => sum + (row.costUsd ?? 0), 0).toFixed(4)),
  }));

  const mergedRows = pullRows.filter((row) => row.merged);
  const joinedSessionIds = new Set(joins.map((entry) => entry.session.sessionId));
  const yieldEligible = pullRows.length;
  const yieldNumerator = pullRows.filter((row) => row.merged && row.checks !== "failed").length;

  const summary = {
    proof: "plimsoll-efficiency-report",
    generatedAt: new Date().toISOString(),
    repository: `${owner}/${repo}`,
    sinceDays,
    ledgerPath,
    sessions: {
      total: sessions.length,
      withLinkage: sessionsWithLinkage.length,
      matchingRepo: repoSessions.length,
      joinedToPulls: joinedSessionIds.size,
      joinRate: sessionsWithLinkage.length
        ? Number((joinedSessionIds.size / sessionsWithLinkage.length).toFixed(3))
        : 0,
      withoutLinkage: sessions.length - sessionsWithLinkage.length,
      withoutLinkageNote:
        "sessions captured before linkage shipped (backfilled history) or outside any git repo",
    },
    pulls: {
      examined: pulls.length,
      joined: pullRows.length,
      merged: mergedRows.length,
    },
    efficiency: {
      tokensPerMergedPR: mergedRows.length
        ? Math.round(
            mergedRows.reduce((sum, row) => sum + row.inputTokens + row.outputTokens, 0) /
              mergedRows.length,
          )
        : null,
      costPerMergedPRUsd: mergedRows.length
        ? Number((mergedRows.reduce((sum, row) => sum + row.costUsd, 0) / mergedRows.length).toFixed(4))
        : null,
      validatedDeliveryYieldV1: yieldEligible
        ? Number((yieldNumerator / yieldEligible).toFixed(3))
        : null,
      yieldDefinition:
        "v1: joined PRs merged with non-failing checks / joined PRs (rework window + review friction land in P2)",
    },
    pullRows,
  };

  const stamp = summary.generatedAt.replace(/[:.]/g, "-");
  const evidenceDir = path.join(process.cwd(), "evidence");
  fs.mkdirSync(evidenceDir, { recursive: true });
  fs.writeFileSync(
    path.join(evidenceDir, `${stamp}-efficiency-report.json`),
    `${JSON.stringify(summary, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(evidenceDir, `${stamp}-efficiency-report.md`),
    [
      `# Efficiency Report — ${owner}/${repo} (last ${sinceDays}d)`,
      "",
      `Generated: ${summary.generatedAt}`,
      "",
      `- Sessions: ${summary.sessions.total} total, ${summary.sessions.withLinkage} with linkage, ${summary.sessions.joinedToPulls} joined (join rate ${summary.sessions.joinRate})`,
      `- PRs: ${summary.pulls.examined} examined, ${summary.pulls.joined} joined, ${summary.pulls.merged} merged`,
      `- Tokens per merged PR: ${summary.efficiency.tokensPerMergedPR ?? "n/a"}`,
      `- Cost per merged PR: ${summary.efficiency.costPerMergedPRUsd ?? "n/a"} USD`,
      `- Validated Delivery Yield v1: ${summary.efficiency.validatedDeliveryYieldV1 ?? "n/a"}`,
      "",
      "| PR | merged | checks | sessions | in tok | out tok | cost USD | via |",
      "|---|---|---|---|---|---|---|---|",
      ...pullRows.map(
        (row) =>
          `| #${row.pull} | ${row.merged} | ${row.checks} | ${row.sessions} | ${row.inputTokens} | ${row.outputTokens} | ${row.costUsd} | ${row.joinedVia.join("+")} |`,
      ),
      "",
    ].join("\n"),
  );

  console.log(JSON.stringify(summary, null, 2));
}

// Only run when invoked directly, not when imported (e.g. the proof imports
// buildPatternsReport). tsx sets process.argv[1] to this script's path.
const invokedDirectly =
  !!process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
if (invokedDirectly) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
