import crypto from "node:crypto";

import Database from "better-sqlite3";

import type { CollectorConfig } from "./config";
import { assertCollectorPrivacyMode, collectorBufferPath } from "./config";
import { canonicalCommitSha, canonicalLinkage } from "./outbound-envelope";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";
import { ensureUuidSessionId } from "./session-sync";
import {
  findForbiddenRawContentFields,
  githubOutcomeIngestBatchSchema,
  remoteLinkageHash,
  branchLinkageHash,
  type GitHubOutcomeIngestBatch,
} from "../../shared/src/index";

/**
 * Outcomes sync (issue 0038 / cloud Phase D2): the local efficiency report
 * (scripts/efficiency-report.ts, issues 0002/0009) already joins ledger
 * sessions to GitHub pull requests via privacy-safe linkage keys and derives
 * merge status, check results, and short-horizon rework (revert/reopen). The
 * hosted workspace has the receiving route (github-outcomes, cloud C8) and
 * the WorkArtifact/ReviewOutcome tables — but nothing fed them. This module
 * pushes the SAME locally-computed join, so hosted VDY (D3) and per-person
 * efficiency (D4) have outcome rows that join back to sessions and events
 * through the same hashes.
 *
 * Boundary rules (own-data sync, the upload-history house rules):
 * - The fetch surface is IDENTICAL to the local report: the repo's pull list,
 *   check-runs for joined PRs (≤20), a bounded default-branch revert scan
 *   (≤3 pages), and reopen events for joined PRs (≤20). D2 adds no new
 *   GitHub endpoint families and no new scopes; like the report, it runs
 *   only for a repository the owner NAMES explicitly.
 * - Naming a repo here is the same deliberate disclosure as push-repo-labels:
 *   owner/name + remoteUrlHash cross the wire. PR titles, bodies, diffs, and
 *   file paths never do. Branch names cross only as linkage hashes; commit
 *   shas are already public on the GitHub side.
 * - Idempotency comes from deterministic ids, not local state: artifact ids
 *   are `artifact:<externalId>` and outcome ids `outcome:<externalId>:<kind>`
 *   — the cloud derives a tenant-salted UUID from each and upserts, so
 *   re-running converges on the same rows (a check status that flips updates
 *   the one `:check` row in place; it never duplicates).
 * - Honest counters: sent vs accepted come from the server response
 *   (acceptedArtifacts/acceptedOutcomes/detachedSessionRefs…), never assumed.
 *
 * Deliberately NOT wired into the daemon's 5-minute cycle (unlike D1's
 * session refresh): the join needs GitHub REST reads for a NAMED repository
 * plus the owner's token from the environment — the daemon holds neither
 * (repo names are hashed in the ledger; labels are deliberate disclosures),
 * and a background GitHub poll would be new scraping cadence this phase
 * forbids. `plimsoll sync-outcomes` is the explicit, repeatable feed.
 */

const OUTCOMES_PATH = "/api/work-intelligence/github-outcomes";
const MAX_CHECKED_PULLS = 20;
const MAX_REVERT_PAGES = 3;
const MAX_REOPEN_PULLS = 20;
const MAX_LINKED_SESSION_IDS = 50;

export type LedgerSessionLink = {
  sessionId: string;
  events: number;
  repoHash: string | null;
  branchHash: string | null;
  headShas: string | null;
};

/**
 * Sessions with linkage keys from the ledger — the same grouping the local
 * report uses, scoped by created_at <= until (the upload-history watermark
 * semantics) so two runs over the same --until see the identical session set.
 */
export function collectSessionLinks(
  ledger: Database.Database,
  options: { since: string; until: string },
): LedgerSessionLink[] {
  const privacyEligible = terminalPrivacyEligibilitySql(ledger, "buffered_events");
  return ledger
    .prepare(
      `select
         session_id as sessionId,
         count(*) as events,
         max(repo_hash) as repoHash,
         max(branch_hash) as branchHash,
         group_concat(distinct head_sha) as headShas
       from buffered_events
       where session_id is not null
         and ${privacyEligible}
         and observed_at >= @since
         and created_at <= @until
       group by session_id
       order by session_id asc`,
    )
    .all({ since: options.since, until: options.until }) as LedgerSessionLink[];
}

export type PullOutcome = {
  number: number;
  state: string;
  merged: boolean;
  mergedAt?: string;
  branchHash?: string;
  headSha?: string;
  mergeCommitSha?: string;
  updatedAt: string;
  checks: "passed" | "failed" | "none" | "unknown";
  /** True only when check-runs were actually fetched for this PR — an
   * unfetched PR must not emit an unknown_check outcome. */
  checksFetched: boolean;
};

export type SessionPullJoin = {
  pull: number;
  sessionId: string;
  via: "branch_hash" | "head_sha" | "merge_sha";
  events: number;
};

/**
 * The report's join, verbatim: sessions whose repoHash matches (or is absent)
 * link to a PR by branch hash, or by a session head sha appearing as the PR
 * head/merge sha.
 */
export function joinSessionsToPulls(
  sessions: LedgerSessionLink[],
  pulls: PullOutcome[],
  repoHash: string | undefined,
): SessionPullJoin[] {
  const joins: SessionPullJoin[] = [];
  const withLinkage = sessions.filter((row) => row.repoHash || row.branchHash || row.headShas);
  const repoSessions = withLinkage.filter((row) => !row.repoHash || row.repoHash === repoHash);
  for (const session of repoSessions) {
    const shas = new Set((session.headShas ?? "").split(",").filter(Boolean));
    for (const pull of pulls) {
      if (session.branchHash && pull.branchHash === session.branchHash) {
        joins.push({ pull: pull.number, sessionId: session.sessionId, via: "branch_hash", events: session.events });
      } else if (pull.headSha && shas.has(pull.headSha)) {
        joins.push({ pull: pull.number, sessionId: session.sessionId, via: "head_sha", events: session.events });
      } else if (pull.mergeCommitSha && shas.has(pull.mergeCommitSha)) {
        joins.push({ pull: pull.number, sessionId: session.sessionId, via: "merge_sha", events: session.events });
      }
    }
  }
  return joins;
}

export type ReworkSignal = { pull: number; kind: "revert" | "reopen"; evidence: string; at: string };

/**
 * Window filter with validatedDeliveryYieldV2's exact semantics (issue 0009):
 * a signal counts when the merge time is unknown, or when it lands within
 * [mergedAt, mergedAt + windowDays]. The proof pins this parity against the
 * report's own implementation so the pushed rework flags can never drift from
 * what the local yield math excludes.
 */
export function reworkSignalsInWindow(
  pulls: Array<{ pull: number; mergedAt?: string }>,
  signals: ReworkSignal[],
  windowDays: number,
): Map<number, ReworkSignal[]> {
  const windowMs = windowDays * 24 * 60 * 60 * 1000;
  const windowed = new Map<number, ReworkSignal[]>();
  for (const row of pulls) {
    const mergeMs = row.mergedAt ? Date.parse(row.mergedAt) : NaN;
    const hits = signals.filter((signal) => {
      if (signal.pull !== row.pull) return false;
      if (Number.isNaN(mergeMs)) return true;
      const at = Date.parse(signal.at);
      return !Number.isNaN(at) && at >= mergeMs && at <= mergeMs + windowMs;
    });
    if (hits.length > 0) windowed.set(row.pull, hits);
  }
  return windowed;
}

export type OutcomeAuditRow = {
  pull: number;
  status: string;
  checks: string;
  sessions: number;
  via: string;
  outcomes: string[];
};

export type OutcomePush = {
  batch: GitHubOutcomeIngestBatch | null;
  rows: OutcomeAuditRow[];
  pullsJoined: number;
  sessionsLinked: number;
  artifacts: number;
  outcomes: number;
};

/**
 * Joined PRs → the wire batch the cloud's github-outcomes route ingests.
 * Pure and deterministic: the same inputs build a byte-identical batch
 * (artifacts ordered by PR number, outcomes by id, linked session ids
 * sorted), which is what makes the deterministic-id proof exact.
 */
export function buildOutcomePush(input: {
  tenantId: string;
  owner: string;
  repo: string;
  pulls: PullOutcome[];
  joins: SessionPullJoin[];
  signals: ReworkSignal[];
  reworkWindowDays: number;
}): OutcomePush {
  const owner = input.owner.toLowerCase();
  const repo = input.repo.toLowerCase();
  const repoSlug = `github.com/${owner}/${repo}`;
  const remoteUrlHash = remoteLinkageHash(`https://${repoSlug}.git`);

  const byPull = new Map<number, { sessions: Map<string, SessionPullJoin>; via: Set<string> }>();
  for (const join of input.joins) {
    const bucket = byPull.get(join.pull) ?? { sessions: new Map(), via: new Set<string>() };
    if (!bucket.sessions.has(join.sessionId)) bucket.sessions.set(join.sessionId, join);
    bucket.via.add(join.via);
    byPull.set(join.pull, bucket);
  }

  const joinedPulls = input.pulls
    .filter((pull) => byPull.has(pull.number))
    .sort((a, b) => a.number - b.number);
  const windowed = reworkSignalsInWindow(
    joinedPulls.filter((pull) => pull.merged).map((pull) => ({ pull: pull.number, mergedAt: pull.mergedAt })),
    input.signals,
    input.reworkWindowDays,
  );

  const artifacts: GitHubOutcomeIngestBatch["artifacts"] = [];
  const outcomes: GitHubOutcomeIngestBatch["outcomes"] = [];
  const rows: OutcomeAuditRow[] = [];
  const linkedSessionIds = new Set<string>();

  for (const pull of joinedPulls) {
    const bucket = byPull.get(pull.number)!;
    const externalId = `${repoSlug}/pull/${pull.number}`;
    const artifactId = `artifact:${externalId}`;
    const observedAt = pull.mergedAt ?? pull.updatedAt;

    // Dominant session (events desc, id asc — deterministic) becomes the
    // artifact's sessionId; every linked session id rides in metadata so the
    // cloud join is never lossy. Ids cross through the SAME deterministic
    // mapping D1's session sync used, so they hit the synced session rows.
    const linked = [...bucket.sessions.values()].sort(
      (a, b) => b.events - a.events || a.sessionId.localeCompare(b.sessionId),
    );
    const linkedIds = linked.map((join) => ensureUuidSessionId(join.sessionId).id);
    for (const id of linkedIds) linkedSessionIds.add(id);

    const reworks = windowed.get(pull.number) ?? [];
    const revert = reworks.find((signal) => signal.kind === "revert");
    const reopen = reworks.find((signal) => signal.kind === "reopen");
    const status = revert
      ? "reverted"
      : reopen
        ? "reopened"
        : pull.merged
          ? "merged"
          : pull.state === "open"
            ? "created"
            : "unknown";

    const metadata: Record<string, unknown> = {
      state: pull.state,
      joinedVia: [...bucket.via].sort(),
      linkedSessions: linked.length,
      linkedSessionIds: linkedIds.slice(0, MAX_LINKED_SESSION_IDS),
      checks: pull.checks,
      checksFetched: pull.checksFetched,
      reworkWindowDays: input.reworkWindowDays,
    };
    const branchHash = canonicalLinkage(pull.branchHash);
    const headSha = canonicalCommitSha(pull.headSha);
    const mergeCommitSha = canonicalCommitSha(pull.mergeCommitSha);
    if (branchHash) metadata.branchHash = branchHash;
    if (headSha) metadata.headSha = headSha;
    if (mergeCommitSha) metadata.mergeCommitSha = mergeCommitSha;

    artifacts.push({
      id: artifactId,
      artifactType: "pull_request",
      externalId,
      observedAt,
      status,
      ...(linkedIds.length > 0 ? { sessionId: linkedIds[0] } : {}),
      metadata,
    });

    const outcomeKinds: string[] = [];
    if (pull.merged) {
      outcomes.push({
        id: `outcome:${externalId}:merged`,
        workArtifactId: artifactId,
        outcome: "merged",
        observedAt: pull.mergedAt ?? pull.updatedAt,
        metadata: {},
      });
      outcomeKinds.push("merged");
    }
    if (pull.checksFetched) {
      const checkOutcome =
        pull.checks === "passed"
          ? "passed_check"
          : pull.checks === "failed"
            ? "failed_check"
            : pull.checks === "none"
              ? "neutral_check"
              : "unknown_check";
      outcomes.push({
        id: `outcome:${externalId}:check`,
        workArtifactId: artifactId,
        outcome: checkOutcome,
        observedAt,
        metadata: { checks: pull.checks },
      });
      outcomeKinds.push(checkOutcome);
    }
    if (revert) {
      outcomes.push({
        id: `outcome:${externalId}:reverted`,
        workArtifactId: artifactId,
        outcome: "reverted",
        observedAt: revert.at,
        metadata: { evidence: revert.evidence },
      });
      outcomeKinds.push("reverted");
    }
    if (reopen) {
      outcomes.push({
        id: `outcome:${externalId}:reopened`,
        workArtifactId: artifactId,
        outcome: "reopened",
        observedAt: reopen.at,
        metadata: { evidence: reopen.evidence },
      });
      outcomeKinds.push("reopened");
    }

    rows.push({
      pull: pull.number,
      status,
      checks: pull.checksFetched ? pull.checks : "not fetched",
      sessions: linked.length,
      via: [...bucket.via].sort().join("+"),
      outcomes: outcomeKinds,
    });
  }

  if (artifacts.length === 0) {
    return { batch: null, rows, pullsJoined: 0, sessionsLinked: 0, artifacts: 0, outcomes: 0 };
  }

  outcomes.sort((a, b) => a.id.localeCompare(b.id));

  // Defense-in-depth: the builder only writes known keys, but the same gate
  // the cloud enforces runs client-side so a future field can never leak raw
  // content silently.
  for (const item of [...artifacts, ...outcomes]) {
    const forbidden = findForbiddenRawContentFields(item.metadata ?? {});
    if (forbidden.length > 0) {
      throw new Error(`Outcome push blocked: forbidden raw-content fields ${forbidden.join(", ")}`);
    }
  }

  const batch = githubOutcomeIngestBatchSchema.parse({
    tenantId: input.tenantId,
    repository: { provider: "github", owner, name: repo, remoteUrlHash },
    artifacts,
    outcomes,
  });

  return {
    batch,
    rows,
    pullsJoined: joinedPulls.length,
    sessionsLinked: linkedSessionIds.size,
    artifacts: artifacts.length,
    outcomes: outcomes.length,
  };
}

export function renderOutcomeAudit(rows: OutcomeAuditRow[]): string {
  const header = ["pull", "status", "checks", "sessions", "via", "outcomes"];
  const table = rows.map((row) => [
    `#${row.pull}`,
    row.status,
    row.checks,
    String(row.sessions),
    row.via,
    row.outcomes.join(", ") || "none",
  ]);
  const widths = header.map((_, column) =>
    Math.max(header[column].length, ...table.map((line) => line[column].length)),
  );
  const renderRow = (line: string[]) =>
    line.map((value, column) => value.padEnd(widths[column])).join("  ").trimEnd();
  return [renderRow(header), widths.map((width) => "-".repeat(width)).join("  "), ...table.map(renderRow)].join(
    "\n",
  );
}

async function githubJson(url: string, token: string | undefined, fetchImpl: typeof fetch) {
  const response = await fetchImpl(url, {
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

/** The report's PR fetch, verbatim: one page of the 100 most recently
 * updated PRs, filtered to the window. */
export async function fetchPullOutcomes(input: {
  owner: string;
  repo: string;
  since: string;
  token?: string;
  fetchImpl: typeof fetch;
}): Promise<PullOutcome[]> {
  const pullsRaw = (await githubJson(
    `https://api.github.com/repos/${input.owner}/${input.repo}/pulls?state=all&sort=updated&direction=desc&per_page=100`,
    input.token,
    input.fetchImpl,
  )) as Array<Record<string, unknown>>;
  return pullsRaw
    .filter((pull) => String(pull.updated_at ?? "") >= input.since)
    .map((pull) => {
      const head = pull.head as { ref?: string; sha?: string } | undefined;
      return {
        number: Number(pull.number),
        state: String(pull.state),
        merged: Boolean(pull.merged_at),
        mergedAt: typeof pull.merged_at === "string" ? pull.merged_at : undefined,
        branchHash: branchLinkageHash(head?.ref),
        headSha: head?.sha,
        mergeCommitSha: typeof pull.merge_commit_sha === "string" ? pull.merge_commit_sha : undefined,
        updatedAt: String(pull.updated_at),
        checks: "unknown" as const,
        checksFetched: false,
      };
    });
}

/** Check-runs for joined PRs only, capped — the report's exact rule. Sets
 * checksFetched so unfetched PRs never emit a check outcome. */
async function fetchChecksForJoined(
  pulls: PullOutcome[],
  joinedNumbers: number[],
  input: { owner: string; repo: string; token?: string; fetchImpl: typeof fetch },
): Promise<void> {
  for (const number of joinedNumbers.slice(0, MAX_CHECKED_PULLS)) {
    const pull = pulls.find((entry) => entry.number === number);
    if (!pull?.headSha) continue;
    try {
      const checkData = (await githubJson(
        `https://api.github.com/repos/${input.owner}/${input.repo}/commits/${pull.headSha}/check-runs?per_page=50`,
        input.token,
        input.fetchImpl,
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
      pull.checksFetched = true;
    } catch {
      pull.checks = "unknown";
      // fetch failed → checksFetched stays false; no check outcome is sent.
    }
  }
}

/** Revert + reopen signals for merged joined PRs — the report's bounded scan
 * (issue 0009): default-branch commits since the earliest merge for reverts,
 * per-PR issue events for reopens. Best-effort; absence of signal is never
 * fabricated. */
async function gatherReworkSignals(
  mergedJoined: PullOutcome[],
  input: { owner: string; repo: string; token?: string; fetchImpl: typeof fetch; log: (line: string) => void },
): Promise<ReworkSignal[]> {
  const signals: ReworkSignal[] = [];
  const earliestMerge = mergedJoined
    .map((pull) => pull.mergedAt)
    .filter((value): value is string => Boolean(value))
    .sort()[0];
  if (!earliestMerge) return signals;

  try {
    const revertCommits: Array<{ sha: string; commit: { message: string } }> = [];
    for (let page = 1; page <= MAX_REVERT_PAGES; page += 1) {
      const batch = (await githubJson(
        `https://api.github.com/repos/${input.owner}/${input.repo}/commits?since=${encodeURIComponent(earliestMerge)}&per_page=100&page=${page}`,
        input.token,
        input.fetchImpl,
      )) as Array<{ sha: string; commit: { message: string } }>;
      revertCommits.push(...batch);
      if (batch.length < 100) break;
    }
    for (const pull of mergedJoined) {
      const revert = revertCommits.find((commit) => {
        const message = commit.commit?.message ?? "";
        // Sha-only matching here: the report also matches `Revert "<title>"`,
        // but titles are raw content this module never holds.
        return Boolean(pull.mergeCommitSha) && message.includes(pull.mergeCommitSha!.slice(0, 7));
      });
      if (revert) {
        signals.push({
          pull: pull.number,
          kind: "revert",
          evidence: `revert ${revert.sha.slice(0, 9)}`,
          at: pull.mergedAt ?? earliestMerge,
        });
      }
    }
  } catch (error) {
    input.log(
      JSON.stringify({
        warning: "outcomes_revert_scan_failed",
        message: error instanceof Error ? error.message : String(error),
      }),
    );
  }

  for (const pull of mergedJoined.slice(0, MAX_REOPEN_PULLS)) {
    try {
      const events = (await githubJson(
        `https://api.github.com/repos/${input.owner}/${input.repo}/issues/${pull.number}/events?per_page=100`,
        input.token,
        input.fetchImpl,
      )) as Array<{ event: string; created_at: string }>;
      const reopen = events.find((event) => event.event === "reopened");
      if (reopen) {
        signals.push({
          pull: pull.number,
          kind: "reopen",
          evidence: `reopened ${reopen.created_at} (github.com/${input.owner}/${input.repo}/pull/${pull.number})`,
          at: reopen.created_at,
        });
      }
    } catch {
      // per-PR event fetch best-effort, exactly like the report
    }
  }
  return signals;
}

export type OutcomesSyncOptions = {
  /** "owner/repo" — the explicit disclosure that scopes the whole run. */
  repository: string;
  sinceDays?: number;
  reworkWindowDays?: number;
  /** Ledger watermark (created_at <= until); GitHub data is as-of-now —
   * convergence comes from deterministic ids + upserts, not frozen reads. */
  until?: string;
  dryRun?: boolean;
  url?: string;
  appVersion?: string;
  ledgerPath?: string;
  ledgerDb?: Database.Database;
  githubToken?: string;
  fetchImpl?: typeof fetch;
  log?: (line: string) => void;
};

export type OutcomesSyncResult = {
  ok: boolean;
  reason: string | null;
  repository: string;
  until: string;
  sinceDays: number;
  reworkWindowDays: number;
  ledgerSessions: number;
  pullsExamined: number;
  pullsJoined: number;
  sessionsLinked: number;
  artifactsSent: number;
  artifactsAccepted: number | null;
  outcomesSent: number;
  outcomesAccepted: number | null;
  detachedSessionRefs: number | null;
  detachedActorRefs: number | null;
  reworkSignals: number;
  dryRun: boolean;
  durationMs: number;
  auditTable: string;
};

/**
 * The outcomes push: ledger sessions (read-only) ⋈ GitHub PR state for one
 * named repository → one signed batch to the workspace's github-outcomes
 * route. Stateless and idempotent — the cloud upserts by deterministic id,
 * so re-running converges instead of duplicating.
 */
export async function runOutcomesSync(
  config: CollectorConfig,
  options: OutcomesSyncOptions,
): Promise<OutcomesSyncResult> {
  assertCollectorPrivacyMode(config, "outcomes sync", {
    willEnableUpload: Boolean(options.url),
  });
  const log = options.log ?? ((line: string) => console.log(line));
  const fetchImpl = options.fetchImpl ?? fetch;
  const startedAt = Date.now();

  const baseUrl = options.url ?? config.uploadUrl;
  if (!baseUrl) {
    throw new Error(
      "This machine has not joined a workspace (no uploadUrl in collector.config.json). " +
        'Run: plimsoll join "<join-url>#<token>" — then retry sync-outcomes.',
    );
  }
  if ((!config.installKey || config.installKey === "local-dev") && !config.ingestKey) {
    throw new Error(
      "No workspace install credentials found (installKey is missing/local-dev and there is no ingestKey). " +
        'Run: plimsoll join "<join-url>#<token>" — then retry sync-outcomes.',
    );
  }

  const [owner, repo] = options.repository.split("/");
  if (!owner || !repo || options.repository.split("/").length !== 2) {
    throw new Error(`--repository expects owner/repo, got: ${options.repository}`);
  }

  const sinceDays = options.sinceDays ?? 30;
  const reworkWindowDays = options.reworkWindowDays ?? 14;
  const until = options.until ?? new Date().toISOString();
  if (Number.isNaN(Date.parse(until))) {
    throw new Error(`--until must be an ISO timestamp, got: ${until}`);
  }
  const since = new Date(Date.parse(until) - sinceDays * 24 * 60 * 60 * 1000).toISOString();
  const token = options.githubToken ?? process.env.GITHUB_TOKEN ?? process.env.GH_TOKEN;
  const repoHash = remoteLinkageHash(`https://github.com/${owner}/${repo}.git`);

  let ledger = options.ledgerDb ?? null;
  let ownsLedger = false;
  if (!ledger) {
    const ledgerPath = options.ledgerPath ?? collectorBufferPath();
    try {
      ledger = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      ownsLedger = true;
    } catch (error) {
      throw new Error(
        `No readable local ledger at ${ledgerPath} (${error instanceof Error ? error.message : String(error)}) — nothing to join.`,
      );
    }
  }
  let sessions: LedgerSessionLink[];
  try {
    sessions = collectSessionLinks(ledger, { since, until });
  } finally {
    if (ownsLedger) ledger.close();
  }

  log(
    JSON.stringify({
      status: "outcomes_sync_start",
      repository: `${owner}/${repo}`,
      sinceDays,
      reworkWindowDays,
      until,
      ledgerSessions: sessions.length,
      githubAuth: Boolean(token),
      dryRun: Boolean(options.dryRun),
    }),
  );

  const pulls = await fetchPullOutcomes({ owner, repo, since, token, fetchImpl });
  const joins = joinSessionsToPulls(sessions, pulls, repoHash);
  const joinedNumbers = [...new Set(joins.map((join) => join.pull))].sort((a, b) => a - b);
  await fetchChecksForJoined(pulls, joinedNumbers, { owner, repo, token, fetchImpl });
  const mergedJoined = pulls.filter((pull) => joinedNumbers.includes(pull.number) && pull.merged);
  const signals = await gatherReworkSignals(mergedJoined, { owner, repo, token, fetchImpl, log });

  const push = buildOutcomePush({
    tenantId: config.tenantId,
    owner,
    repo,
    pulls,
    joins,
    signals,
    reworkWindowDays,
  });

  const baseResult: Omit<
    OutcomesSyncResult,
    "ok" | "reason" | "artifactsAccepted" | "outcomesAccepted" | "detachedSessionRefs" | "detachedActorRefs" | "durationMs"
  > = {
    repository: `${owner}/${repo}`,
    until,
    sinceDays,
    reworkWindowDays,
    ledgerSessions: sessions.length,
    pullsExamined: pulls.length,
    pullsJoined: push.pullsJoined,
    sessionsLinked: push.sessionsLinked,
    artifactsSent: 0,
    outcomesSent: 0,
    reworkSignals: signals.length,
    dryRun: Boolean(options.dryRun),
    auditTable: renderOutcomeAudit(push.rows),
  };

  if (!push.batch) {
    log(JSON.stringify({ status: "outcomes_sync_noop", reason: "no_joined_pulls", pullsExamined: pulls.length }));
    return {
      ...baseResult,
      ok: true,
      reason: "no_joined_pulls",
      artifactsAccepted: null,
      outcomesAccepted: null,
      detachedSessionRefs: null,
      detachedActorRefs: null,
      durationMs: Date.now() - startedAt,
    };
  }

  if (options.dryRun) {
    log(
      JSON.stringify({
        status: "outcomes_sync_dry_run",
        wouldSendArtifacts: push.artifacts,
        wouldSendOutcomes: push.outcomes,
        pullsJoined: push.pullsJoined,
        sessionsLinked: push.sessionsLinked,
      }),
    );
    log("");
    log(baseResult.auditTable);
    return {
      ...baseResult,
      ok: true,
      reason: null,
      artifactsSent: push.artifacts,
      outcomesSent: push.outcomes,
      artifactsAccepted: null,
      outcomesAccepted: null,
      detachedSessionRefs: null,
      detachedActorRefs: null,
      durationMs: Date.now() - startedAt,
    };
  }

  // The outcomes route lives next to the ingest route; derive it so a custom
  // --url pointing at the ingest endpoint still lands on the right path.
  const url = new URL(baseUrl);
  url.pathname = OUTCOMES_PATH;
  const body = JSON.stringify(push.batch);
  const headers: Record<string, string> = {
    "content-type": "application/json",
    "x-plimsoll-install-key": config.installKey,
  };
  if (config.ingestKey) headers["x-plimsoll-ingest-key"] = config.ingestKey;
  if (config.uploadSigningSecret) {
    const timestamp = new Date().toISOString();
    const digest = crypto
      .createHmac("sha256", config.uploadSigningSecret)
      .update(`${timestamp}.${body}`)
      .digest("hex");
    headers["x-plimsoll-upload-timestamp"] = timestamp;
    headers["x-plimsoll-upload-signature"] = `sha256=${digest}`;
  }

  const response = await fetchImpl(url.toString(), { method: "POST", headers, body });
  const responseBody = (await response.json().catch(() => ({}))) as {
    error?: unknown;
    acceptedArtifacts?: unknown;
    acceptedOutcomes?: unknown;
    detachedSessionRefs?: unknown;
    detachedActorRefs?: unknown;
  };
  if (!response.ok) {
    // NB: error bodies can echo request fields — surface only the server's
    // error code, never the raw body.
    const errorCode = typeof responseBody.error === "string" ? responseBody.error : "unknown_error";
    const reason = `Workspace refused the outcomes batch with HTTP ${response.status} (${errorCode}). Nothing was recorded as accepted.`;
    log(JSON.stringify({ status: "outcomes_sync_failed", reason }));
    return {
      ...baseResult,
      ok: false,
      reason,
      artifactsSent: push.artifacts,
      outcomesSent: push.outcomes,
      artifactsAccepted: null,
      outcomesAccepted: null,
      detachedSessionRefs: null,
      detachedActorRefs: null,
      durationMs: Date.now() - startedAt,
    };
  }

  const counter = (value: unknown) => (typeof value === "number" ? value : null);
  const result: OutcomesSyncResult = {
    ...baseResult,
    ok: true,
    reason: null,
    artifactsSent: push.artifacts,
    outcomesSent: push.outcomes,
    artifactsAccepted: counter(responseBody.acceptedArtifacts),
    outcomesAccepted: counter(responseBody.acceptedOutcomes),
    detachedSessionRefs: counter(responseBody.detachedSessionRefs),
    detachedActorRefs: counter(responseBody.detachedActorRefs),
    durationMs: Date.now() - startedAt,
  };

  log(
    JSON.stringify({
      status: "outcomes_sync_done",
      repository: result.repository,
      pullsExamined: result.pullsExamined,
      pullsJoined: result.pullsJoined,
      sessionsLinked: result.sessionsLinked,
      artifactsSent: result.artifactsSent,
      artifactsAccepted: result.artifactsAccepted,
      outcomesSent: result.outcomesSent,
      outcomesAccepted: result.outcomesAccepted,
      detachedSessionRefs: result.detachedSessionRefs,
      detachedActorRefs: result.detachedActorRefs,
      durationMs: result.durationMs,
    }),
  );
  log("");
  log(result.auditTable);

  return result;
}
