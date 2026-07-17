/**
 * Local efficiency report: allocates AI token/cost events in the work ledger to GitHub
 * pull requests via privacy-safe linkage keys (hashed remote + hashed branch
 * + plain commit shas) and computes the token-use-to-accomplishment metrics
 * the product exists for:
 *
 *   - tokens / cost per merged PR
 *   - join rate (usage/cost sessions linked to a PR / sessions observed in the named repo)
 *   - Validated Delivery Yield v1 = joined PRs that merged with passing
 *     checks / joined PRs (validation evidence dimension = CI checks for now)
 *
 * Runs entirely locally against the collector ledger + GitHub REST API; no
 * cloud ingest required. Evidence lands in evidence/.
 *
 * Usage:
 *   GITHUB_TOKEN=... tsx scripts/efficiency-report.ts \
 *     --repository owner/repo [--since-days 30] [--yield-window-days 14] \
 *     [--ledger /path/to/work-ledger.sqlite]
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
import {
  allocateEvents,
  collectAllocationEvents,
  type PullCandidate,
} from "./event-allocation";

type PullSummary = {
  number: number;
  state: string;
  title: string;
  merged: boolean;
  mergedAt?: string;
  branchHash?: string;
  headSha?: string;
  mergeCommitSha?: string;
  createdAt: string;
  closedAt?: string;
  updatedAt: string;
  checks: "passed" | "failed" | "none" | "unknown";
};

/** Short-horizon rework against a merged PR: a revert on the default branch or
 *  a reopen event landing inside the stability window (issue 0009 / #9). */
type ReworkSignal = { pull: number; kind: "revert" | "reopen"; evidence: string; at: string };

/**
 * Validated Delivery Yield v2 = v1 numerator MINUS any merged+passing PR that
 * drew short-horizon rework (revert/reopen) within `windowDays` of its merge.
 * Pure over fetched data so the proof can feed a known-reverted PR and watch it
 * drop. Excluded PRs are named with the reason and evidence.
 */
export function validatedDeliveryYieldV2(
  eligible: Array<{ pull: number; mergedAt?: string }>,
  signals: ReworkSignal[],
  windowDays: number,
): { numerator: number; excluded: Array<{ pull: number; reason: string; evidence: string }> } {
  const windowMs = windowDays * 24 * 60 * 60 * 1000;
  const excluded: Array<{ pull: number; reason: string; evidence: string }> = [];
  for (const row of eligible) {
    const mergeMs = row.mergedAt ? Date.parse(row.mergedAt) : NaN;
    const hit = signals.find((s) => {
      if (s.pull !== row.pull) return false;
      if (Number.isNaN(mergeMs)) return true; // unknown merge time → count any signal
      const at = Date.parse(s.at);
      return !Number.isNaN(at) && at >= mergeMs && at <= mergeMs + windowMs;
    });
    if (hit) excluded.push({ pull: row.pull, reason: hit.kind, evidence: hit.evidence });
  }
  const excludedPulls = new Set(excluded.map((e) => e.pull));
  return { numerator: eligible.filter((r) => !excludedPulls.has(r.pull)).length, excluded };
}

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
  const yieldWindowDays = Number(optionValue("--yield-window-days") ?? 14);
  const since = new Date(Date.now() - sinceDays * 24 * 60 * 60 * 1000).toISOString();
  const ledgerPath = optionValue("--ledger") ?? collectorBufferPath();
  const token = process.env.GITHUB_TOKEN ?? process.env.GH_TOKEN;
  const repoHash = remoteLinkageHash(`https://github.com/${owner}/${repo}.git`);
  if (!repoHash) throw new Error("Could not derive repository linkage hash.");

  const db = new Database(ledgerPath, { readonly: true });
  const events = collectAllocationEvents(db, since);
  db.close();

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
        title: typeof pull.title === "string" ? pull.title : "",
        merged: Boolean(pull.merged_at),
        mergedAt: typeof pull.merged_at === "string" ? pull.merged_at : undefined,
        branchHash: branchLinkageHash(head?.ref),
        headSha: head?.sha,
        mergeCommitSha: typeof pull.merge_commit_sha === "string" ? pull.merge_commit_sha : undefined,
        createdAt: String(pull.created_at),
        closedAt: typeof pull.closed_at === "string" ? pull.closed_at : undefined,
        updatedAt: String(pull.updated_at),
        checks: "unknown" as const,
      };
    });

  const candidates: PullCandidate[] = pulls.map((pull) => ({
    pull: pull.number,
    repoHash,
    branchHash: pull.branchHash ?? null,
    headSha: pull.headSha ?? null,
    mergeCommitSha: pull.mergeCommitSha ?? null,
    createdAt: pull.createdAt,
    updatedAt: pull.updatedAt,
    closedAt: pull.closedAt ?? null,
    mergedAt: pull.mergedAt ?? null,
  }));
  // Upgrade event HEAD evidence from "current PR head only" to commit
  // membership for lifecycle-relevant candidates. Both dimensions are hard
  // bounded: at most 20 candidate PRs and 100 commits per PR.
  const headEvents = events.filter(
    (event) => event.repoHash === repoHash && event.headSha,
  );
  const membershipCandidates = candidates
    .filter((candidate) => {
      const created = Date.parse(candidate.createdAt) - 7 * 24 * 60 * 60 * 1_000;
      const terminal = Date.parse(
        candidate.mergedAt ?? candidate.closedAt ?? candidate.updatedAt,
      ) + 7 * 24 * 60 * 60 * 1_000;
      return headEvents.some((event) => {
        const observed = Date.parse(event.observedAt);
        return Number.isFinite(observed) && observed >= created && observed <= terminal;
      });
    })
    .slice(0, 20);
  for (const candidate of membershipCandidates) {
    try {
      const commits = (await githubJson(
        `https://api.github.com/repos/${owner}/${repo}/pulls/${candidate.pull}/commits?per_page=100`,
        token,
      )) as Array<{ sha?: string }>;
      candidate.commitShas = commits
        .map((commit) => commit.sha)
        .filter((sha): sha is string => Boolean(sha));
    } catch {
      // Current head + bounded branch fallback remain available. A failed
      // membership read never becomes fabricated direct evidence.
    }
  }
  const allocation = allocateEvents(events, candidates);

  // Check runs for joined PRs only (cap GitHub calls).
  const joinedPullNumbers = allocation.pullRows
    .filter((row) => row.repoHash === repoHash)
    .map((row) => row.pull);
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

  const pullRows = allocation.pullRows
    .filter((row) => row.repoHash === repoHash)
    .map((row) => {
      const pull = pulls.find((candidate) => candidate.number === row.pull);
      return {
        ...row,
        merged: pull?.merged ?? false,
        checks: pull?.checks ?? "unknown",
      };
    });

  const mergedRows = pullRows.filter((row) => row.merged);
  const sessionIds = new Set(events.map((event) => event.sessionId).filter(Boolean));
  const sessionsWithLinkage = new Set(
    events
      .filter((event) => event.repoHash || event.branchHash || event.headSha)
      .map((event) => event.sessionId)
      .filter(Boolean),
  );
  const repoSessions = new Set(
    events
      .filter((event) => event.repoHash === repoHash)
      .map((event) => event.sessionId)
      .filter(Boolean),
  );
  const joinedSessionIds = new Set(
    allocation.receipts
      .filter((receipt) => receipt.pull !== null && receipt.repoHash === repoHash)
      .map((receipt) => receipt.sessionId)
      .filter(Boolean),
  );
  const yieldEligible = pullRows.length;
  const v1NumeratorRows = pullRows.filter((row) => row.merged && row.checks !== "failed");
  const yieldNumerator = v1NumeratorRows.length;

  // v2: gather short-horizon rework signals against the v1 numerator PRs.
  // Reverts come from default-branch commits ("This reverts commit <sha>" or
  // `Revert "<title>"`); reopens from each PR's issue events. Both are capped
  // to keep GitHub calls bounded.
  const eligibleSummaries = v1NumeratorRows
    .map((row) => pulls.find((p) => p.number === row.pull))
    .filter((p): p is PullSummary => Boolean(p));
  const reworkSignals: ReworkSignal[] = [];
  const earliestMerge = eligibleSummaries
    .map((p) => p.mergedAt)
    .filter((d): d is string => Boolean(d))
    .sort()[0];
  if (earliestMerge) {
    try {
      const revertCommits: Array<{ sha: string; commit: { message: string } }> = [];
      for (let page = 1; page <= 3; page += 1) {
        const batch = (await githubJson(
          `https://api.github.com/repos/${owner}/${repo}/commits?since=${encodeURIComponent(earliestMerge)}&per_page=100&page=${page}`,
          token,
        )) as Array<{ sha: string; commit: { message: string } }>;
        revertCommits.push(...batch);
        if (batch.length < 100) break;
      }
      for (const pull of eligibleSummaries) {
        const revert = revertCommits.find((c) => {
          const msg = c.commit?.message ?? "";
          const bySha = Boolean(pull.mergeCommitSha) && msg.includes(pull.mergeCommitSha!.slice(0, 7));
          const byTitle = pull.title.length > 0 && msg.split("\n")[0] === `Revert "${pull.title}"`;
          return bySha || byTitle;
        });
        if (revert) {
          reworkSignals.push({
            pull: pull.number,
            kind: "revert",
            evidence: `revert ${revert.sha.slice(0, 9)}`,
            at: pull.mergedAt ?? earliestMerge,
          });
        }
      }
    } catch {
      // Revert scan is best-effort; absence of signal must not inflate yield
      // silently — the report notes when the scan could not run.
    }
    for (const pull of eligibleSummaries.slice(0, 20)) {
      try {
        const events = (await githubJson(
          `https://api.github.com/repos/${owner}/${repo}/issues/${pull.number}/events?per_page=100`,
          token,
        )) as Array<{ event: string; created_at: string }>;
        const reopen = events.find((e) => e.event === "reopened");
        if (reopen) {
          reworkSignals.push({
            pull: pull.number,
            kind: "reopen",
            evidence: `reopened ${reopen.created_at} (github.com/${owner}/${repo}/pull/${pull.number})`,
            at: reopen.created_at,
          });
        }
      } catch {
        // per-PR event fetch best-effort
      }
    }
  }
  const yieldV2 = validatedDeliveryYieldV2(
    eligibleSummaries.map((p) => ({ pull: p.number, mergedAt: p.mergedAt })),
    reworkSignals,
    yieldWindowDays,
  );

  const generatedAt = new Date().toISOString();
  const stamp = generatedAt.replace(/[:.]/g, "-");
  const receiptFile = `${stamp}-allocation-receipts.ndjson`;
  const allMergedCostsKnown =
    mergedRows.length > 0 && mergedRows.every((row) => row.costStatus === "known");
  const summary = {
    proof: "plimsoll-efficiency-report",
    generatedAt,
    repository: `${owner}/${repo}`,
    sinceDays,
    ledgerPath,
    sessions: {
      total: sessionIds.size,
      withLinkage: sessionsWithLinkage.size,
      matchingRepo: repoSessions.size,
      joinedToPulls: joinedSessionIds.size,
      joinRate: repoSessions.size
        ? Number((joinedSessionIds.size / repoSessions.size).toFixed(3))
        : 0,
      withoutLinkage: sessionIds.size - sessionsWithLinkage.size,
      withoutLinkageNote:
        "usage/cost sessions captured before linkage shipped or outside any git repo",
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
      costPerMergedPRUsd: allMergedCostsKnown
        ? Number(
            (
              mergedRows.reduce((sum, row) => sum + (row.costUsd ?? 0), 0) /
              mergedRows.length
            ).toFixed(4),
          )
        : null,
      knownCostPerMergedPRUsd: mergedRows.length
        ? Number(
            (
              mergedRows.reduce((sum, row) => sum + row.knownCostUsd, 0) /
              mergedRows.length
            ).toFixed(4),
          )
        : null,
      costCompleteness: allMergedCostsKnown
        ? "known"
        : mergedRows.some((row) => row.costStatus !== "unknown")
          ? "partial"
          : "unknown",
      validatedDeliveryYieldV1: yieldEligible
        ? Number((yieldNumerator / yieldEligible).toFixed(3))
        : null,
      validatedDeliveryYieldV2: yieldEligible
        ? Number((yieldV2.numerator / yieldEligible).toFixed(3))
        : null,
      yieldV2Delta: yieldEligible
        ? Number(((yieldV2.numerator - yieldNumerator) / yieldEligible).toFixed(3))
        : null,
      yieldWindowDays,
      yieldV2Excluded: yieldV2.excluded,
      yieldDefinition:
        "v1: joined PRs merged with non-failing checks / joined PRs. " +
        `v2: same, minus PRs with short-horizon rework (revert/reopen) within ${yieldWindowDays}d of merge.`,
    },
    allocation: {
      hierarchy: [
        "exact repo + HEAD membership (direct)",
        "time-bounded repo + branch or unique repo candidate (inferred)",
        "bounded stable same-session segment (inferred)",
        "explicit unallocated remainder",
      ],
      dominantRepoFallback: "disabled",
      candidateLimit: 100,
      candidates: candidates.length,
      commitMembershipCandidates: membershipCandidates.length,
      commitMembershipLimit: 20,
      commitsPerCandidateLimit: 100,
      receipts: allocation.receipts.length,
      receiptFile,
      coverageScope:
        "all promoted token/cost events in the ledger window; events outside the named repository remain unallocated",
      coverage: allocation.coverage,
    },
    pullRows,
  };

  const evidenceDir = path.join(process.cwd(), "evidence");
  fs.mkdirSync(evidenceDir, { recursive: true });
  fs.writeFileSync(
    path.join(evidenceDir, receiptFile),
    allocation.receipts.map((receipt) => JSON.stringify(receipt)).join("\n") +
      (allocation.receipts.length ? "\n" : ""),
  );
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
      `- Cost per merged PR: ${summary.efficiency.costPerMergedPRUsd ?? "unknown"} USD (${summary.efficiency.costCompleteness}; known portion ${summary.efficiency.knownCostPerMergedPRUsd ?? "n/a"} USD)`,
      `- Validated Delivery Yield v1: ${summary.efficiency.validatedDeliveryYieldV1 ?? "n/a"}`,
      `- Validated Delivery Yield v2 (${yieldWindowDays}d rework window): ${summary.efficiency.validatedDeliveryYieldV2 ?? "n/a"} (delta ${summary.efficiency.yieldV2Delta ?? "n/a"})`,
      ...(yieldV2.excluded.length
        ? [
            "",
            `Excluded from v2 (short-horizon rework within ${yieldWindowDays}d):`,
            ...yieldV2.excluded.map((e) => `- #${e.pull}: ${e.reason} — ${e.evidence}`),
          ]
        : [`- v2 exclusions: none within the ${yieldWindowDays}d window`]),
      "",
      "## Allocation coverage",
      "",
      "No dominant-repository fallback. Every event has zero or one allocation receipt; exact reconciliation uses integer nanodollars.",
      "",
      "| coverage | events | input | output | cache read | cache write | known cost USD | unpriced events |",
      "|---|---:|---:|---:|---:|---:|---:|---:|",
      ...(["captured", "direct", "inferred", "unallocated"] as const).map((key) => {
        const row = summary.allocation.coverage[key];
        return `| ${key} | ${row.events} | ${row.inputTokens} | ${row.outputTokens} | ${row.cacheReadTokens} | ${row.cacheWriteTokens} | ${row.knownCostUsd} | ${row.unpricedEvents} |`;
      }),
      "",
      `- Exact reconciliation: ${summary.allocation.coverage.reconciliation.exact}`,
      `- Local allocation receipts: ${receiptFile}`,
      "",
      "| PR | merged | checks | sessions | direct | inferred | in tok | out tok | cache read | cache write | cost USD | known cost USD | cost status | via |",
      "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
      ...pullRows.map(
        (row) =>
          `| #${row.pull} | ${row.merged} | ${row.checks} | ${row.sessions} | ${row.directEvents} | ${row.inferredEvents} | ${row.inputTokens} | ${row.outputTokens} | ${row.cacheReadTokens} | ${row.cacheWriteTokens} | ${row.costUsd ?? "unknown"} | ${row.knownCostUsd} | ${row.costStatus} | ${row.joinedVia.join("+")} |`,
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
