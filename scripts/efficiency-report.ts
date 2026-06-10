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
 *   GITHUB_TOKEN=... tsx scripts/capture-plimsoll-efficiency-report.ts \
 *     --repository owner/repo [--since-days 30] [--ledger /path/to/work-ledger.sqlite]
 */
import fs from "node:fs";
import path from "node:path";

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

async function main() {
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
        sum(cache_read_tokens) as cacheReadTokens, sum(cost_usd) as costUsd,
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

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
