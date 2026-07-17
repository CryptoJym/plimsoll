import crypto from "node:crypto";
import fs from "node:fs";

import {
  changedPullRefSchema,
  fullCommitShaSchema,
  githubRateReceiptSchema,
  outcomeTimelineBackfillStateSchema,
  pullTimelineFactSchema,
  type ChangedPullRef,
  type GitHubRateReceipt,
  type OutcomeTimelineBackfillState,
  type OutcomeTimelineCoverage,
  type OutcomeTimelineCoverageObservation,
  type OutcomeReworkWatch,
  type PullTimelineFact,
  type RequiredCheckPolicy,
} from "../../shared/src/index";
import { OutcomeTimelineStore } from "./outcome-timeline-store";

type CoverageObservation = OutcomeTimelineCoverageObservation;
type ChangedPullCoverage = CoverageObservation & { dimension: "changed_pulls" };

export type ProviderPage<T> = {
  items: T[];
  nextCursor: string | null;
  etag: string | null;
  rateReceipt: GitHubRateReceipt | null;
  notModified?: boolean;
};

export type CollectedPages<T> = {
  items: T[];
  pages: number;
  nextCursor: string | null;
  etag: string | null;
  rateReceipt: GitHubRateReceipt | null;
  coverage: CoverageObservation;
  retryable: boolean;
};

export class GitHubTimelineProviderError extends Error {
  constructor(
    readonly reason: "provider_failure" | "rate_exhausted",
    readonly status: number,
    readonly rateReceipt: GitHubRateReceipt | null,
    message: string,
  ) {
    super(message);
    this.name = "GitHubTimelineProviderError";
  }
}

/** Bounded pagination helper used by the real adapter and injectable proofs. */
export async function collectProviderPages<T>(input: {
  dimension: CoverageObservation["dimension"];
  maxPages: number;
  startCursor?: string | null;
  fetchPage: (cursor: string | null) => Promise<ProviderPage<T>>;
}): Promise<CollectedPages<T>> {
  const items: T[] = [];
  let cursor: string | null = input.startCursor ?? null;
  let etag: string | null = null;
  let rateReceipt: GitHubRateReceipt | null = null;
  let pages = 0;
  try {
    while (pages < input.maxPages) {
      const page = await input.fetchPage(cursor);
      pages += 1;
      items.push(...page.items);
      etag = page.etag ?? etag;
      rateReceipt = page.rateReceipt ?? rateReceipt;
      if (page.notModified || !page.nextCursor) {
        return {
          items,
          pages,
          nextCursor: null,
          etag,
          rateReceipt,
          coverage: {
            dimension: input.dimension,
            status: "complete",
            reason: page.notModified ? "not_modified" : "complete",
          },
          retryable: false,
        };
      }
      cursor = page.nextCursor;
    }
    return {
      items,
      pages,
      nextCursor: cursor,
      etag,
      rateReceipt,
      coverage: {
        dimension: input.dimension,
        status: "incomplete",
        reason: "pagination_limit",
        detail: `stopped after ${input.maxPages} provider pages`,
      },
      retryable: false,
    };
  } catch (error) {
    if (!(error instanceof GitHubTimelineProviderError)) throw error;
    return {
      items,
      pages,
      // Retry the page that failed; all earlier pages in this bounded slice
      // were already retained as immutable facts.
      nextCursor: cursor,
      etag,
      rateReceipt: error.rateReceipt ?? rateReceipt,
      coverage: {
        dimension: input.dimension,
        status: error.reason === "rate_exhausted" ? "unknown" : "incomplete",
        reason: error.reason,
        detail: `GitHub ${error.status || "request"}: ${error.message}`,
      },
      retryable: true,
    };
  }
}

export type ChangedPullPage = ProviderPage<ChangedPullRef> & {
  coverage: ChangedPullCoverage;
};

export type PullCollection = {
  facts: PullTimelineFact[];
  coverage: CoverageObservation[];
  rateReceipt: GitHubRateReceipt | null;
  retryable: boolean;
  continuationCursor?: string | null;
};

export interface GitHubOutcomeTimelineAdapter {
  listChangedPullsPage(input: {
    owner: string;
    repo: string;
    since: string;
    until: string;
    cursor: string | null;
    pageSize: number;
    ifNoneMatch: string | null;
  }): Promise<ChangedPullPage>;
  collectPull(input: {
    owner: string;
    repo: string;
    repositoryExternalId: string;
    pull: ChangedPullRef;
    until: string;
  }): Promise<PullCollection>;
  collectRework?(input: {
    owner: string;
    repo: string;
    repositoryExternalId: string;
    watch: OutcomeReworkWatch;
    until: string;
  }): Promise<PullCollection>;
}

type JsonResponse = {
  value: unknown;
  etag: string | null;
  rateReceipt: GitHubRateReceipt | null;
  notModified: boolean;
};

function rateReceiptFromHeaders(headers: Headers, now: () => string): GitHubRateReceipt {
  const integer = (name: string) => {
    const value = headers.get(name);
    if (value === null || !/^\d+$/.test(value)) return null;
    return Number(value);
  };
  const reset = integer("x-ratelimit-reset");
  return githubRateReceiptSchema.parse({
    observedAt: now(),
    limit: integer("x-ratelimit-limit"),
    remaining: integer("x-ratelimit-remaining"),
    resetAt: reset === null ? null : new Date(reset * 1000).toISOString(),
    resource: headers.get("x-ratelimit-resource"),
  });
}

function nextPageCursor(link: string | null, currentPage: number): string | null {
  if (!link || !/<[^>]+>;\s*rel="next"/.test(link)) return null;
  return String(currentPage + 1);
}

function fullSha(value: unknown): string | null {
  const parsed = fullCommitShaSchema.safeParse(value);
  return parsed.success ? parsed.data.toLowerCase() : null;
}

function timestamp(value: unknown): string | null {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value))) return null;
  return new Date(value).toISOString();
}

function conclusion(value: unknown): Extract<PullTimelineFact, { kind: "check_attempt" }>["conclusion"] {
  const known = new Set([
    "success",
    "failure",
    "cancelled",
    "timed_out",
    "action_required",
    "neutral",
    "skipped",
    "stale",
    "startup_failure",
  ]);
  return typeof value === "string" && known.has(value)
    ? (value as Extract<PullTimelineFact, { kind: "check_attempt" }>["conclusion"])
    : "unknown";
}

function reviewOutcome(value: unknown): Extract<PullTimelineFact, { kind: "review_outcome" }>["outcome"] | null {
  const normalized = typeof value === "string" ? value.toLowerCase() : "";
  if (normalized === "approved") return "approved";
  if (normalized === "changes_requested") return "changes_requested";
  if (normalized === "dismissed") return "dismissed";
  if (normalized === "commented") return "commented";
  return null;
}

/** GitHub reads are encapsulated here. The token is private constructor state
 * and is never accepted by the store, facts, state, coverage, or receipts. */
export class GitHubRestOutcomeTimelineAdapter implements GitHubOutcomeTimelineAdapter {
  private readonly token: string | undefined;
  private readonly fetchImpl: typeof fetch;
  private readonly maxPagesPerEndpoint: number;
  private readonly now: () => string;

  constructor(options: {
    token?: string;
    fetchImpl?: typeof fetch;
    maxPagesPerEndpoint?: number;
    now?: () => string;
  } = {}) {
    this.token = options.token;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.maxPagesPerEndpoint = options.maxPagesPerEndpoint ?? 10;
    this.now = options.now ?? (() => new Date().toISOString());
  }

  private async json(url: string, endpoint: string, ifNoneMatch: string | null = null): Promise<JsonResponse> {
    let response: Response;
    try {
      response = await this.fetchImpl(url, {
        headers: {
          accept: "application/vnd.github+json",
          "x-github-api-version": "2022-11-28",
          "user-agent": "Plimsoll/0.6",
          ...(this.token ? { authorization: `Bearer ${this.token}` } : {}),
          ...(ifNoneMatch ? { "if-none-match": ifNoneMatch } : {}),
        },
      });
    } catch {
      throw new GitHubTimelineProviderError("provider_failure", 0, null, `${endpoint} network request failed`);
    }
    const rateReceipt = rateReceiptFromHeaders(response.headers, this.now);
    if (response.status === 304) {
      return { value: null, etag: response.headers.get("etag"), rateReceipt, notModified: true };
    }
    if (!response.ok) {
      const exhausted = response.status === 403 && rateReceipt.remaining === 0;
      throw new GitHubTimelineProviderError(
        exhausted ? "rate_exhausted" : "provider_failure",
        response.status,
        rateReceipt,
        `${endpoint} request failed`,
      );
    }
    try {
      return {
        value: await response.json(),
        etag: response.headers.get("etag"),
        rateReceipt,
        notModified: false,
      };
    } catch {
      throw new GitHubTimelineProviderError(
        "provider_failure",
        response.status,
        rateReceipt,
        `${endpoint} returned invalid JSON`,
      );
    }
  }

  async listChangedPullsPage(input: {
    owner: string;
    repo: string;
    since: string;
    until: string;
    cursor: string | null;
    pageSize: number;
    ifNoneMatch: string | null;
  }): Promise<ChangedPullPage> {
    const page = Number(input.cursor ?? "1");
    const query = `repo:${input.owner}/${input.repo} is:pr updated:${input.since}..${input.until}`;
    const url = new URL("https://api.github.com/search/issues");
    url.searchParams.set("q", query);
    url.searchParams.set("sort", "updated");
    url.searchParams.set("order", "asc");
    url.searchParams.set("per_page", String(Math.min(100, input.pageSize)));
    url.searchParams.set("page", String(page));
    const response = await this.json(url.toString(), "changed pull search", input.ifNoneMatch);
    if (response.notModified) {
      return {
        items: [],
        nextCursor: null,
        etag: response.etag,
        rateReceipt: response.rateReceipt,
        notModified: true,
        coverage: { dimension: "changed_pulls", status: "complete", reason: "not_modified" },
      };
    }
    const body = response.value as { total_count?: number; items?: Array<Record<string, unknown>> };
    const rawItems = Array.isArray(body.items) ? body.items : [];
    const items = rawItems.map((item) =>
      changedPullRefSchema.parse({
        number: Number(item.number),
        pullExternalId: `github:pull:${String(item.node_id ?? item.id)}`,
        updatedAt: timestamp(item.updated_at),
      }),
    );
    const total = Number(body.total_count ?? items.length);
    const searchCap = 1_000;
    const consumed = page * Math.min(100, input.pageSize);
    const nextCursor = consumed < Math.min(total, searchCap) && items.length > 0 ? String(page + 1) : null;
    return {
      items,
      nextCursor,
      etag: response.etag,
      rateReceipt: response.rateReceipt,
      coverage:
        total > searchCap && !nextCursor
          ? {
              dimension: "changed_pulls",
              status: "incomplete",
              reason: "pagination_limit",
              detail: "GitHub search result cap exceeded 1000 changed pull requests",
            }
          : { dimension: "changed_pulls", status: "complete", reason: "complete" },
    };
  }

  private arrayPages(input: {
    url: (page: number) => string;
    dimension: CoverageObservation["dimension"];
    itemKey?: string;
    startCursor?: string | null;
  }): Promise<CollectedPages<Record<string, unknown>>> {
    return collectProviderPages({
      dimension: input.dimension,
      maxPages: this.maxPagesPerEndpoint,
      startCursor: input.startCursor,
      fetchPage: async (cursor) => {
        const page = Number(cursor ?? "1");
        const response = await this.json(input.url(page), input.dimension);
        const value = input.itemKey
          ? (response.value as Record<string, unknown> | null)?.[input.itemKey]
          : response.value;
        const items = Array.isArray(value) ? (value as Array<Record<string, unknown>>) : [];
        return {
          items,
          nextCursor: nextPageCursor(
            // The request helper does not retain response headers beyond safe receipts;
            // full pages are the REST fallback pagination signal.
            items.length >= 100 ? `<next>; rel="next"` : null,
            page,
          ),
          etag: response.etag,
          rateReceipt: response.rateReceipt,
        };
      },
    });
  }

  async collectPull(input: {
    owner: string;
    repo: string;
    repositoryExternalId: string;
    pull: ChangedPullRef;
    until: string;
  }): Promise<PullCollection> {
    const base = `https://api.github.com/repos/${encodeURIComponent(input.owner)}/${encodeURIComponent(input.repo)}`;
    const coverage: CoverageObservation[] = [];
    const facts: PullTimelineFact[] = [];
    let retryable = false;
    let rateReceipt: GitHubRateReceipt | null = null;
    const retain = (result: CollectedPages<Record<string, unknown>>) => {
      coverage.push(result.coverage);
      retryable ||= result.retryable;
      rateReceipt = result.rateReceipt ?? rateReceipt;
      return result.items;
    };

    let pull: Record<string, unknown> | null = null;
    try {
      const detail = await this.json(`${base}/pulls/${input.pull.number}`, "pull");
      pull = detail.value as Record<string, unknown>;
      rateReceipt = detail.rateReceipt;
      coverage.push({ dimension: "pull", status: "complete", reason: "complete" });
    } catch (error) {
      if (!(error instanceof GitHubTimelineProviderError)) throw error;
      rateReceipt = error.rateReceipt;
      retryable = true;
      coverage.push({
        dimension: "pull",
        status: error.reason === "rate_exhausted" ? "unknown" : "incomplete",
        reason: error.reason,
        detail: `GitHub ${error.status}: ${error.message}`,
      });
    }

    const pullExternalId = input.pull.pullExternalId;
    if (pull) {
      const createdAt = timestamp(pull.created_at);
      if (createdAt) {
        facts.push({
          schemaVersion: 1,
          externalId: pullExternalId,
          repositoryExternalId: input.repositoryExternalId,
          pullExternalId,
          pullNumber: input.pull.number,
          kind: "pull",
          createdAt,
        });
      }
      const mergeSha = fullSha(pull.merge_commit_sha);
      const mergedAt = timestamp(pull.merged_at);
      if (mergeSha && mergedAt) {
        facts.push({
          schemaVersion: 1,
          externalId: `${pullExternalId}:merge:${mergeSha}`,
          repositoryExternalId: input.repositoryExternalId,
          pullExternalId,
          pullNumber: input.pull.number,
          kind: "merge",
          mergeSha,
          mergedAt,
        });
      }
    }

    const commitRows = retain(
      await this.arrayPages({
        url: (page) => `${base}/pulls/${input.pull.number}/commits?per_page=100&page=${page}`,
        dimension: "revisions",
      }),
    );
    const revisions: Array<{ sha: string; committedAt: string }> = [];
    for (const row of commitRows) {
      const sha = fullSha(row.sha);
      const commit = row.commit as Record<string, unknown> | undefined;
      const committer = commit?.committer as Record<string, unknown> | undefined;
      const author = commit?.author as Record<string, unknown> | undefined;
      const committedAt = timestamp(committer?.date ?? author?.date);
      if (!sha || !committedAt) continue;
      revisions.push({ sha, committedAt });
      facts.push({
        schemaVersion: 1,
        externalId: `${pullExternalId}:revision:${sha}`,
        repositoryExternalId: input.repositoryExternalId,
        pullExternalId,
        pullNumber: input.pull.number,
        kind: "pull_revision",
        sha,
        committedAt,
      });
    }

    let checkCoverage: CoverageObservation = { dimension: "checks", status: "complete", reason: "complete" };
    let pendingChecks = false;
    for (const revision of revisions) {
      const checkRows = retain(
        await this.arrayPages({
          url: (page) =>
            `${base}/commits/${revision.sha}/check-runs?filter=all&per_page=100&page=${page}`,
          dimension: "checks",
          itemKey: "check_runs",
        }),
      );
      const last = coverage.pop();
      if (last && last.status !== "complete") checkCoverage = last;
      for (const row of checkRows) {
        const status = String(row.status ?? "");
        const completedAt = timestamp(row.completed_at);
        if (status !== "completed" || !completedAt) {
          pendingChecks = true;
          continue;
        }
        const sha = fullSha(row.head_sha) ?? revision.sha;
        const checkRunExternalId = `github:check-run:${String(row.node_id ?? row.id)}`;
        facts.push({
          schemaVersion: 1,
          externalId: checkRunExternalId,
          repositoryExternalId: input.repositoryExternalId,
          pullExternalId,
          pullNumber: input.pull.number,
          kind: "check_attempt",
          checkRunExternalId,
          sha,
          name: String(row.name ?? "unknown"),
          conclusion: conclusion(row.conclusion),
          startedAt: timestamp(row.started_at),
          completedAt,
        });
      }
    }
    coverage.push(
      pendingChecks && checkCoverage.status === "complete"
        ? { dimension: "checks", status: "unknown", reason: "pending_check_attempt" }
        : checkCoverage,
    );
    // A completed incremental window must not strand an in-flight attempt:
    // keep this PR at the deterministic recovery head until GitHub gives the
    // attempt an immutable completion outcome.
    retryable ||= pendingChecks;

    const reviewRows = retain(
      await this.arrayPages({
        url: (page) => `${base}/pulls/${input.pull.number}/reviews?per_page=100&page=${page}`,
        dimension: "reviews",
      }),
    );
    for (const row of reviewRows) {
      const outcome = reviewOutcome(row.state);
      const submittedAt = timestamp(row.submitted_at);
      if (!outcome || !submittedAt) continue;
      const reviewExternalId = `github:review:${String(row.node_id ?? row.id)}`;
      const transitionExternalId = `${reviewExternalId}:state:${outcome}:at:${submittedAt}`;
      facts.push({
        schemaVersion: 1,
        externalId: transitionExternalId,
        repositoryExternalId: input.repositoryExternalId,
        pullExternalId,
        pullNumber: input.pull.number,
        kind: "review_outcome",
        reviewExternalId,
        sha: fullSha(row.commit_id),
        outcome,
        submittedAt,
      });
    }

    const timelineRows = retain(
      await this.arrayPages({
        url: (page) => `${base}/issues/${input.pull.number}/timeline?per_page=100&page=${page}`,
        dimension: "timeline_events",
      }),
    );
    const linked = new Map<string, PullTimelineFact>();
    for (const row of timelineRows) {
      const event = String(row.event ?? "");
      const eventExternalId = `github:timeline:${String(row.node_id ?? row.id)}`;
      const createdAt = timestamp(row.created_at);
      if (event === "reopened" && createdAt) {
        facts.push({
          schemaVersion: 1,
          externalId: eventExternalId,
          repositoryExternalId: input.repositoryExternalId,
          pullExternalId,
          pullNumber: input.pull.number,
          kind: "reopen",
          reopenedAt: createdAt,
        });
      }
      if (event !== "connected" && event !== "cross-referenced") continue;
      const source = row.source as Record<string, unknown> | undefined;
      const issue = source?.issue as Record<string, unknown> | undefined;
      if (!issue || issue.pull_request) continue;
      const issueNumber = Number(issue.number);
      if (!Number.isInteger(issueNumber) || issueNumber <= 0) continue;
      const issueExternalId = `github:issue:${String(issue.node_id ?? issue.id)}`;
      const externalId = `${pullExternalId}:linked-issue:${issueExternalId}`;
      linked.set(externalId, {
        schemaVersion: 1,
        externalId,
        repositoryExternalId: input.repositoryExternalId,
        pullExternalId,
        pullNumber: input.pull.number,
        kind: "linked_issue",
        issueExternalId,
        issueNumber,
        // GitHub's cross-reference timestamp is not a durable link-created
        // timestamp, so the immutable fact records it honestly as unknown.
        linkedAt: null,
      });
    }
    facts.push(...linked.values());
    coverage.push(
      coverage.find((row) => row.dimension === "timeline_events" && row.status !== "complete")
        ? {
            dimension: "linked_issues",
            status: "incomplete",
            reason: "provider_failure",
            detail: "linked issue coverage depends on incomplete timeline events",
          }
        : { dimension: "linked_issues", status: "complete", reason: "complete" },
    );

    const merge = facts.find((fact): fact is Extract<PullTimelineFact, { kind: "merge" }> => fact.kind === "merge");
    if (merge) {
      let repository: Record<string, unknown> | null = null;
      try {
        const response = await this.json(base, "reverts");
        repository = response.value as Record<string, unknown>;
        rateReceipt = response.rateReceipt ?? rateReceipt;
      } catch (error) {
        if (!(error instanceof GitHubTimelineProviderError)) throw error;
        retryable = true;
        coverage.push({
          dimension: "reverts",
          status: error.reason === "rate_exhausted" ? "unknown" : "incomplete",
          reason: error.reason,
          detail: `GitHub ${error.status}: ${error.message}`,
        });
      }
      if (repository) {
        const branch = encodeURIComponent(String(repository.default_branch ?? "main"));
        const revertRows = retain(
          await this.arrayPages({
            url: (page) =>
              `${base}/commits?sha=${branch}&since=${encodeURIComponent(merge.mergedAt)}&until=${encodeURIComponent(input.until)}&per_page=100&page=${page}`,
            dimension: "reverts",
          }),
        );
        for (const row of revertRows) {
          const revertSha = fullSha(row.sha);
          const commit = row.commit as Record<string, unknown> | undefined;
          const message = typeof commit?.message === "string" ? commit.message : "";
          const committer = commit?.committer as Record<string, unknown> | undefined;
          const author = commit?.author as Record<string, unknown> | undefined;
          const revertedAt = timestamp(committer?.date ?? author?.date);
          const matches = [...message.matchAll(/This reverts commit ([0-9a-f]{40})\.?/gi)];
          if (!revertSha || !revertedAt) continue;
          for (const match of matches) {
            const revertedSha = fullSha(match[1]);
            if (!revertedSha || revertedSha !== merge.mergeSha) continue;
            facts.push({
              schemaVersion: 1,
              externalId: `github:commit:${revertSha}:revert:${revertedSha}`,
              repositoryExternalId: input.repositoryExternalId,
              pullExternalId,
              pullNumber: input.pull.number,
              kind: "revert",
              revertSha,
              revertedSha,
              revertedAt,
              evidence: { source: "commit_message_full_sha", matchedFullSha: revertedSha },
            });
          }
        }
      }
    } else {
      coverage.push({ dimension: "reverts", status: "complete", reason: "complete" });
    }

    const unique = new Map<string, PullTimelineFact>();
    for (const candidate of facts) {
      const fact = pullTimelineFactSchema.parse(candidate);
      unique.set(fact.externalId, fact);
    }
    return {
      facts: [...unique.values()].sort((a, b) => a.externalId.localeCompare(b.externalId)),
      coverage,
      rateReceipt,
      retryable,
    };
  }

  async collectRework(input: {
    owner: string;
    repo: string;
    repositoryExternalId: string;
    watch: OutcomeReworkWatch;
    until: string;
  }): Promise<PullCollection> {
    const base = `https://api.github.com/repos/${encodeURIComponent(input.owner)}/${encodeURIComponent(input.repo)}`;
    let rateReceipt: GitHubRateReceipt | null = null;
    let repository: Record<string, unknown>;
    try {
      const response = await this.json(base, "reverts");
      repository = response.value as Record<string, unknown>;
      rateReceipt = response.rateReceipt;
    } catch (error) {
      if (!(error instanceof GitHubTimelineProviderError)) throw error;
      return {
        facts: [],
        coverage: [
          {
            dimension: "reverts",
            status: error.reason === "rate_exhausted" ? "unknown" : "incomplete",
            reason: error.reason,
            detail: `GitHub ${error.status || "request"}: ${error.message}`,
          },
        ],
        rateReceipt: error.rateReceipt,
        retryable: true,
      };
    }

    const branch = encodeURIComponent(String(repository.default_branch ?? "main"));
    const pages = await this.arrayPages({
      url: (page) =>
        `${base}/commits?sha=${branch}&since=${encodeURIComponent(input.watch.lastCheckedThrough)}&until=${encodeURIComponent(input.until)}&per_page=100&page=${page}`,
      dimension: "reverts",
      startCursor: input.watch.paginationCursor,
    });
    const facts: PullTimelineFact[] = [];
    for (const row of pages.items) {
      const revertSha = fullSha(row.sha);
      const commit = row.commit as Record<string, unknown> | undefined;
      const message = typeof commit?.message === "string" ? commit.message : "";
      const committer = commit?.committer as Record<string, unknown> | undefined;
      const author = commit?.author as Record<string, unknown> | undefined;
      const revertedAt = timestamp(committer?.date ?? author?.date);
      if (!revertSha || !revertedAt) continue;
      for (const match of message.matchAll(/This reverts commit ([0-9a-f]{40})\.?/gi)) {
        const revertedSha = fullSha(match[1]);
        if (!revertedSha || revertedSha !== input.watch.mergeSha) continue;
        facts.push({
          schemaVersion: 1,
          externalId: `github:commit:${revertSha}:revert:${revertedSha}`,
          repositoryExternalId: input.repositoryExternalId,
          pullExternalId: input.watch.pull.pullExternalId,
          pullNumber: input.watch.pull.number,
          kind: "revert",
          revertSha,
          revertedSha,
          revertedAt,
          evidence: { source: "commit_message_full_sha", matchedFullSha: revertedSha },
        });
      }
    }
    return {
      facts,
      coverage: [pages.coverage],
      rateReceipt: pages.rateReceipt ?? rateReceipt,
      // A page cap leaves an uncovered suffix in the watched interval. Keep
      // the durable checkpoint fixed so a later run rescans the same span.
      retryable: pages.retryable || pages.coverage.status !== "complete",
      continuationCursor: pages.nextCursor,
    };
  }
}

function repositoryExternalId(owner: string, repo: string): string {
  return `github:repository:${owner.toLowerCase()}/${repo.toLowerCase()}`;
}

function statusFromCoverage(
  coverage: OutcomeTimelineCoverage[],
  windowComplete: boolean,
): "complete" | "bounded" | "incomplete" | "unknown" {
  if (coverage.some((row) => row.status === "incomplete")) return "incomplete";
  if (coverage.some((row) => row.status === "unknown")) return "unknown";
  return windowComplete ? "complete" : "bounded";
}

function combineChangedPullCoverage(
  current: ChangedPullCoverage,
  next: ChangedPullCoverage,
): ChangedPullCoverage {
  const rank = { complete: 0, unknown: 1, incomplete: 2 } as const;
  return rank[next.status] > rank[current.status] ? next : current;
}

export type OutcomeTimelineBackfillReceipt = {
  schema: "plimsoll.outcome-timeline-backfill.v1";
  runId: string;
  repositoryExternalId: string;
  status: "complete" | "bounded" | "incomplete" | "unknown";
  processedPulls: number;
  processedReworkWatches: number;
  reworkWatchRemaining: number;
  insertedFacts: number;
  duplicateFacts: number;
  coverage: { complete: number; incomplete: number; unknown: number };
  cursor: string | null;
  etag: string | null;
  completedThrough: string | null;
  activeUntil: string | null;
  rateReceipt: GitHubRateReceipt | null;
};

export async function runOutcomeTimelineBackfill(input: {
  owner: string;
  repo: string;
  since: string;
  until: string;
  maxPulls: number;
  store: OutcomeTimelineStore;
  adapter: GitHubOutcomeTimelineAdapter;
  requiredChecks?: RequiredCheckPolicy;
  reworkWindowDays?: number;
  runId?: string;
  now?: () => string;
}): Promise<OutcomeTimelineBackfillReceipt> {
  const now = input.now ?? (() => new Date().toISOString());
  const runId = input.runId ?? crypto.randomUUID();
  const repoExternalId = repositoryExternalId(input.owner, input.repo);
  if (!Number.isInteger(input.maxPulls) || input.maxPulls < 1 || input.maxPulls > 100) {
    throw new Error("maxPulls must be an integer from 1 through 100");
  }
  const reworkWindowDays = input.reworkWindowDays ?? 14;
  if (!Number.isInteger(reworkWindowDays) || reworkWindowDays < 1 || reworkWindowDays > 365) {
    throw new Error("reworkWindowDays must be an integer from 1 through 365");
  }
  if (!Number.isFinite(Date.parse(input.since)) || !Number.isFinite(Date.parse(input.until))) {
    throw new Error("since and until must be ISO timestamps");
  }

  let state: OutcomeTimelineBackfillState =
    input.store.readState(repoExternalId) ??
    outcomeTimelineBackfillStateSchema.parse({
      schemaVersion: 1,
      repositoryExternalId: repoExternalId,
      owner: input.owner,
      repo: input.repo,
      completedThrough: null,
      lastCursor: null,
      lastEtag: null,
      activeWindow: null,
      reworkWatch: [],
      lastRateReceipt: null,
    });
  if (!state.activeWindow) {
    state = {
      ...state,
      activeWindow: {
        since: state.completedThrough ?? new Date(input.since).toISOString(),
        until: new Date(input.until).toISOString(),
        cursor: null,
        nextCursor: null,
        pageLoaded: false,
        pendingPulls: [],
        etag: null,
        changedPullCoverage: {
          dimension: "changed_pulls",
          status: "complete",
          reason: "complete",
        },
      },
    };
    input.store.saveState(state, now());
  }

  const runCoverage: OutcomeTimelineCoverage[] = [];
  let processedPulls = 0;
  let processedReworkWatches = 0;
  let insertedFacts = 0;
  let duplicateFacts = 0;
  let stoppedForRetry = false;

  const coverageRow = (observation: CoverageObservation, pullExternalId?: string): OutcomeTimelineCoverage => ({
    runId,
    repositoryExternalId: repoExternalId,
    ...(pullExternalId ? { pullExternalId } : {}),
    ...observation,
  });

  const updateReworkWatch = (
    current: OutcomeReworkWatch[],
    pull: ChangedPullRef,
    facts: PullTimelineFact[],
    checkedThrough: string,
  ): OutcomeReworkWatch[] => {
    let watches = [...current];
    for (const merge of facts.filter(
      (fact): fact is Extract<PullTimelineFact, { kind: "merge" }> => fact.kind === "merge",
    )) {
      watches = watches.filter(
        (watch) => !(watch.pull.pullExternalId === pull.pullExternalId && watch.mergeSha === merge.mergeSha),
      );
      const reverted = facts.some(
        (fact) => fact.kind === "revert" && fact.revertedSha === merge.mergeSha,
      );
      const expiresAt = new Date(
        Date.parse(merge.mergedAt) + reworkWindowDays * 24 * 60 * 60 * 1000,
      ).toISOString();
      if (!reverted && checkedThrough < expiresAt) {
        watches.push({
          pull,
          mergeSha: merge.mergeSha,
          mergedAt: merge.mergedAt,
          expiresAt,
          lastCheckedThrough: checkedThrough,
          paginationCursor: null,
        });
      }
    }
    return watches.sort(
      (left, right) =>
        left.expiresAt.localeCompare(right.expiresAt) ||
        left.pull.number - right.pull.number ||
        left.mergeSha.localeCompare(right.mergeSha),
    );
  };

  const persistedSourceCoverage = state.activeWindow?.changedPullCoverage;
  if (persistedSourceCoverage && persistedSourceCoverage.status !== "complete") {
    const persisted = coverageRow(persistedSourceCoverage);
    runCoverage.push(persisted);
    input.store.recordCoverage([persisted], now());
  }

  const advanceEmptyPage = (): "none" | "advanced" | "blocked" => {
    const active = state.activeWindow;
    if (!active) return "none";
    if (!active.pageLoaded || active.pendingPulls.length > 0) return "none";
    if (active.nextCursor) {
      state = {
        ...state,
        activeWindow: {
          ...active,
          cursor: active.nextCursor,
          nextCursor: null,
          pageLoaded: false,
          etag: null,
        },
      };
    } else if (active.changedPullCoverage.status !== "complete") {
      return "blocked";
    } else {
      state = {
        ...state,
        completedThrough: active.until,
        activeWindow: null,
      };
    }
    input.store.saveState(state, now());
    return "advanced";
  };

  while (processedPulls < input.maxPulls && !stoppedForRetry) {
    const activeUntil = state.activeWindow?.until ?? new Date(input.until).toISOString();
    const watchIndex = state.reworkWatch.findIndex((watch) => {
      const target = activeUntil < watch.expiresAt ? activeUntil : watch.expiresAt;
      return watch.lastCheckedThrough < target;
    });
    if (watchIndex === -1) break;
    const watch = state.reworkWatch[watchIndex]!;
    const watchUntil = activeUntil < watch.expiresAt ? activeUntil : watch.expiresAt;
    let collection: PullCollection;
    try {
      collection = input.adapter.collectRework
        ? await input.adapter.collectRework({
            owner: input.owner,
            repo: input.repo,
            repositoryExternalId: repoExternalId,
            watch,
            until: watchUntil,
          })
        : await input.adapter.collectPull({
            owner: input.owner,
            repo: input.repo,
            repositoryExternalId: repoExternalId,
            pull: watch.pull,
            until: watchUntil,
          });
    } catch (error) {
      if (!(error instanceof GitHubTimelineProviderError)) throw error;
      collection = {
        facts: [],
        coverage: [
          {
            dimension: "reverts",
            status: error.reason === "rate_exhausted" ? "unknown" : "incomplete",
            reason: error.reason,
            detail: `GitHub ${error.status || "request"}: ${error.message}`,
          },
        ],
        rateReceipt: error.rateReceipt,
        retryable: true,
      };
    }
    const rows = collection.coverage.map((observation) =>
      coverageRow(observation, watch.pull.pullExternalId),
    );
    runCoverage.push(...rows);
    // Enforce retryability at the runner boundary too. Injected adapters and
    // future providers must not advance a durable watch checkpoint while any
    // part of the requested rework interval remains uncovered.
    const reworkRetryable =
      collection.retryable || collection.coverage.some((observation) => observation.status !== "complete");
    const revertFound = collection.facts.some(
      (fact) => fact.kind === "revert" && fact.revertedSha === watch.mergeSha,
    );
    const reworkWatch = [...state.reworkWatch];
    if (!reworkRetryable) {
      if (revertFound || watchUntil >= watch.expiresAt) {
        reworkWatch.splice(watchIndex, 1);
      } else {
        reworkWatch[watchIndex] = {
          ...watch,
          lastCheckedThrough: watchUntil,
          paginationCursor: null,
        };
      }
    } else if (collection.continuationCursor !== undefined) {
      reworkWatch[watchIndex] = {
        ...watch,
        paginationCursor: collection.continuationCursor,
      };
    }
    const nextState: OutcomeTimelineBackfillState = {
      ...state,
      reworkWatch,
      lastRateReceipt: collection.rateReceipt ?? state.lastRateReceipt,
    };
    const appended = input.store.commitPullCollection({
      facts: collection.facts,
      coverage: rows,
      state: nextState,
      now: now(),
    });
    state = nextState;
    insertedFacts += appended.inserted;
    duplicateFacts += appended.duplicates;
    if (reworkRetryable) {
      stoppedForRetry = true;
    } else {
      processedPulls += 1;
      processedReworkWatches += 1;
    }
  }

  while (processedPulls < input.maxPulls && state.activeWindow && !stoppedForRetry) {
    const advancement = advanceEmptyPage();
    if (advancement === "advanced") continue;
    if (advancement === "blocked") break;
    const active = state.activeWindow;
    if (!active.pageLoaded) {
      try {
        const page = await input.adapter.listChangedPullsPage({
          owner: input.owner,
          repo: input.repo,
          since: active.since,
          until: active.until,
          cursor: active.cursor,
          // The fetched page is cached in pendingPulls. A fixed provider page
          // size keeps cursor meaning stable even if a recovery run changes
          // its local maxPulls processing bound.
          pageSize: 100,
          ifNoneMatch: active.etag,
        });
        const row = coverageRow(page.coverage);
        runCoverage.push(row);
        input.store.recordCoverage([row], now());
        state = {
          ...state,
          lastRateReceipt: page.rateReceipt ?? state.lastRateReceipt,
          lastCursor: active.cursor,
          lastEtag: page.etag ?? state.lastEtag,
          activeWindow: {
            ...active,
            pageLoaded: true,
            pendingPulls: page.items,
            nextCursor: page.nextCursor,
            etag: page.etag,
            changedPullCoverage: combineChangedPullCoverage(
              active.changedPullCoverage,
              page.coverage,
            ),
          },
        };
        input.store.saveState(state, now());
        const afterFetch = advanceEmptyPage();
        if (afterFetch === "advanced") continue;
        if (afterFetch === "blocked") break;
      } catch (error) {
        if (!(error instanceof GitHubTimelineProviderError)) throw error;
        const row = coverageRow({
          dimension: "changed_pulls",
          status: error.reason === "rate_exhausted" ? "unknown" : "incomplete",
          reason: error.reason,
          detail: `GitHub ${error.status}: ${error.message}`,
        });
        runCoverage.push(row);
        input.store.recordCoverage([row], now());
        state = { ...state, lastRateReceipt: error.rateReceipt ?? state.lastRateReceipt };
        input.store.saveState(state, now());
        stoppedForRetry = true;
        break;
      }
    }

    const pull = state.activeWindow?.pendingPulls[0];
    if (!pull || !state.activeWindow) continue;
    let collection: PullCollection;
    try {
      collection = await input.adapter.collectPull({
        owner: input.owner,
        repo: input.repo,
        repositoryExternalId: repoExternalId,
        pull,
        until: state.activeWindow.until,
      });
    } catch (error) {
      if (!(error instanceof GitHubTimelineProviderError)) throw error;
      collection = {
        facts: [],
        coverage: [
          {
            dimension: "pull",
            status: error.reason === "rate_exhausted" ? "unknown" : "incomplete",
            reason: error.reason,
            detail: `GitHub ${error.status || "request"}: ${error.message}`,
          },
        ],
        rateReceipt: error.rateReceipt,
        retryable: true,
      };
    }
    const rows = collection.coverage.map((observation) => coverageRow(observation, pull.pullExternalId));
    if (!input.requiredChecks?.names.length) {
      rows.push(
        coverageRow(
          {
            dimension: "required_checks",
            status: "unknown",
            reason: "missing_required_check_policy",
          },
          pull.pullExternalId,
        ),
      );
    } else {
      rows.push(
        coverageRow(
          { dimension: "required_checks", status: "complete", reason: "complete" },
          pull.pullExternalId,
        ),
      );
    }
    runCoverage.push(...rows);
    const nextState: OutcomeTimelineBackfillState = {
      ...state,
      lastRateReceipt: collection.rateReceipt ?? state.lastRateReceipt,
      reworkWatch: collection.retryable
        ? state.reworkWatch
        : updateReworkWatch(
            state.reworkWatch,
            pull,
            collection.facts,
            state.activeWindow.until,
          ),
      activeWindow: {
        ...state.activeWindow,
        pendingPulls: collection.retryable
          ? state.activeWindow.pendingPulls
          : state.activeWindow.pendingPulls.slice(1),
      },
    };
    const appended = input.store.commitPullCollection({
      facts: collection.facts,
      coverage: rows,
      state: nextState,
      now: now(),
    });
    state = nextState;
    insertedFacts += appended.inserted;
    duplicateFacts += appended.duplicates;
    if (collection.retryable) {
      stoppedForRetry = true;
    } else {
      processedPulls += 1;
    }
  }
  if (!stoppedForRetry && state.activeWindow) advanceEmptyPage();

  const counts = {
    complete: runCoverage.filter((row) => row.status === "complete").length,
    incomplete: runCoverage.filter((row) => row.status === "incomplete").length,
    unknown: runCoverage.filter((row) => row.status === "unknown").length,
  };
  const receipt: OutcomeTimelineBackfillReceipt = {
    schema: "plimsoll.outcome-timeline-backfill.v1",
    runId,
    repositoryExternalId: repoExternalId,
    status: statusFromCoverage(runCoverage, state.activeWindow === null),
    processedPulls,
    processedReworkWatches,
    reworkWatchRemaining: state.reworkWatch.length,
    insertedFacts,
    duplicateFacts,
    coverage: counts,
    cursor: state.activeWindow?.cursor ?? state.lastCursor,
    etag: state.activeWindow?.etag ?? state.lastEtag,
    completedThrough: state.completedThrough,
    activeUntil: state.activeWindow?.until ?? null,
    rateReceipt: state.lastRateReceipt,
  };
  input.store.recordRun(runId, receipt, now());
  return receipt;
}

export function readRequiredCheckPolicy(filePath: string | undefined): RequiredCheckPolicy | undefined {
  if (!filePath) return undefined;
  const parsed = JSON.parse(fs.readFileSync(filePath, "utf8")) as Record<string, unknown>;
  if (!Array.isArray(parsed.requiredChecks) || parsed.requiredChecks.some((name) => typeof name !== "string")) {
    throw new Error("required-check policy must contain a requiredChecks string array");
  }
  const names = [...new Set((parsed.requiredChecks as string[]).map((name) => name.trim()).filter(Boolean))].sort();
  if (names.length === 0) throw new Error("required-check policy must name at least one check");
  return { names };
}
