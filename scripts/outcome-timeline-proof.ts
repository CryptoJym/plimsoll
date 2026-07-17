import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  GitHubRestOutcomeTimelineAdapter,
  GitHubTimelineProviderError,
  collectProviderPages,
  runOutcomeTimelineBackfill,
  type GitHubOutcomeTimelineAdapter,
  type PullCollection,
} from "../packages/collector-cli/src/github-outcome-backfill";
import {
  ImmutableTimelineConflictError,
  OutcomeTimelineStore,
} from "../packages/collector-cli/src/outcome-timeline-store";
import {
  derivePullOutcomeTimeline,
  pullTimelineFactSchema,
  type ChangedPullRef,
  type GitHubRateReceipt,
  type PullTimelineFact,
} from "../packages/shared/src/index";

type Check = { name: string; passed: boolean; detail: Record<string, unknown> };
const checks: Check[] = [];
function prove(name: string, passed: boolean, detail: Record<string, unknown>) {
  checks.push({ name, passed, detail });
  assert.equal(passed, true, `${name}: ${JSON.stringify(detail)}`);
}

const repoRoot = path.resolve(import.meta.dirname, "..");
const fixturePath = path.join(
  repoRoot,
  "packages/shared/fixtures/outcome-timeline/adversarial.json",
);
const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8")) as {
  requiredChecks: string[];
  reworkWindowDays: number;
  facts: PullTimelineFact[];
};
const facts = fixture.facts.map((fact) => pullTimelineFactSchema.parse(fact));

async function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-outcome-timeline-proof-"));
  try {
  const immutableStorePath = path.join(sandbox, "immutable.sqlite");
  const immutableStore = new OutcomeTimelineStore(immutableStorePath);
  const firstReplay = immutableStore.appendFacts([...facts].reverse(), "2026-07-30T00:00:00.000Z");
  const secondReplay = immutableStore.appendFacts(facts, "2026-07-30T01:00:00.000Z");
  prove(
    "duplicate_and_out_of_order_replay_is_idempotent",
    firstReplay.inserted === facts.length && secondReplay.duplicates === facts.length && immutableStore.facts().length === facts.length,
    { firstReplay, secondReplay, stored: immutableStore.facts().length },
  );

  let conflictClosed = false;
  try {
    const pull = facts.find((fact) => fact.kind === "pull")!;
    immutableStore.appendFacts([
      { ...pull, createdAt: "2026-07-31T00:00:00.000Z" } as PullTimelineFact,
    ]);
  } catch (error) {
    conflictClosed = error instanceof ImmutableTimelineConflictError;
  }
  prove("immutable_external_id_conflict_fails_closed", conflictClosed, { conflictClosed });

  const derived = derivePullOutcomeTimeline({
    facts: immutableStore.facts(),
    requiredChecks: { names: fixture.requiredChecks },
    reworkWindowDays: fixture.reworkWindowDays,
  });
  const pull = (number: number) => derived.find((row) => row.pullNumber === number)!;
  prove(
    "failed_sha_then_same_sha_pass_is_retry_episode",
    pull(1).retryEpisodes.length === 1 && pull(1).correctionLoops.length === 0,
    { retryEpisodes: pull(1).retryEpisodes, correctionLoops: pull(1).correctionLoops },
  );
  prove(
    "failed_sha_then_new_sha_pass_is_correction_loop",
    pull(2).retryEpisodes.length === 0 && pull(2).correctionLoops.length === 1,
    { retryEpisodes: pull(2).retryEpisodes, correctionLoops: pull(2).correctionLoops },
  );
  prove(
    "changes_requested_new_revision_approval_is_review_correction",
    pull(3).reviewCorrections.length === 1 &&
      pull(3).reviewCorrections[0]?.correctedSha === "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    { reviewCorrections: pull(3).reviewCorrections },
  );
  prove(
    "green_multi_commit_is_not_rework",
    pull(4).revisionCount === 2 &&
      pull(4).greenMultiCommitWithoutRework &&
      pull(4).correctionLoops.length === 0 &&
      pull(4).rework.length === 0,
    { pull: pull(4) },
  );
  prove(
    "revert_and_reopen_use_actual_event_timestamps_inside_window",
    pull(5).rework.length === 2 && pull(5).rework.every((signal) => signal.inWindow),
    { rework: pull(5).rework },
  );
  prove(
    "full_sha_revert_outside_window_is_not_rework",
    pull(6).rework.length === 1 &&
      pull(6).rework[0]?.at === "2026-07-25T06:00:00.000Z" &&
      pull(6).rework[0]?.inWindow === false,
    { rework: pull(6).rework },
  );
  const unknownDerivation = derivePullOutcomeTimeline({
    facts,
    reworkWindowDays: fixture.reworkWindowDays,
  });
  prove(
    "missing_required_check_policy_keeps_check_metrics_unknown",
    unknownDerivation.every((row) => row.coverage === "unknown" && row.firstPassSuccess === null),
    { rows: unknownDerivation.length },
  );

  const strictPrivacy = pullTimelineFactSchema.safeParse({
    ...facts[0],
    title: "must never become canonical",
  });
  const canonicalText = JSON.stringify(immutableStore.facts());
  prove(
    "canonical_facts_reject_pr_content_fields",
    !strictPrivacy.success &&
      !/[\"'](?:title|body|diff|path|token|authorization)[\"']\s*:/.test(canonicalText),
    { rejectedUnknownField: !strictPrivacy.success },
  );
  immutableStore.close();

  const paginated = await collectProviderPages({
    dimension: "checks",
    maxPages: 2,
    fetchPage: async (cursor) => ({
      items: [cursor ?? "page-1"],
      nextCursor: cursor === null ? "page-2" : "page-3",
      etag: `etag-${cursor ?? "1"}`,
      rateReceipt: null,
    }),
  });
  prove(
    "pagination_limit_is_explicit_incomplete_coverage",
    paginated.items.length === 2 &&
      paginated.coverage.status === "incomplete" &&
      paginated.coverage.reason === "pagination_limit",
    { coverage: paginated.coverage, pages: paginated.pages },
  );

  const providerFailure = await collectProviderPages({
    dimension: "reviews",
    maxPages: 2,
    fetchPage: async () => {
      throw new GitHubTimelineProviderError("provider_failure", 500, null, "reviews request failed");
    },
  });
  prove(
    "provider_500_is_explicit_incomplete_coverage",
    providerFailure.retryable &&
      providerFailure.coverage.status === "incomplete" &&
      providerFailure.coverage.reason === "provider_failure",
    { coverage: providerFailure.coverage },
  );

  const exhaustedReceipt: GitHubRateReceipt = {
    observedAt: "2026-07-30T00:00:00.000Z",
    limit: 5_000,
    remaining: 0,
    resetAt: "2026-07-30T01:00:00.000Z",
    resource: "core",
  };
  const rateExhausted = await collectProviderPages({
    dimension: "timeline_events",
    maxPages: 2,
    fetchPage: async () => {
      throw new GitHubTimelineProviderError("rate_exhausted", 403, exhaustedReceipt, "rate exhausted");
    },
  });
  prove(
    "rate_exhaustion_is_explicit_unknown_coverage",
    rateExhausted.retryable &&
      rateExhausted.coverage.status === "unknown" &&
      rateExhausted.coverage.reason === "rate_exhausted" &&
      rateExhausted.rateReceipt?.remaining === 0,
    { coverage: rateExhausted.coverage, rateReceipt: rateExhausted.rateReceipt },
  );

  const TOKEN_SENTINEL = "github-token-must-never-persist-SENTINEL";
  let tokenObservedOnlyInRequest = false;
  const mockFetch: typeof fetch = async (request, init) => {
    const url = String(request);
    const headers = new Headers(init?.headers);
    tokenObservedOnlyInRequest ||= headers.get("authorization") === `Bearer ${TOKEN_SENTINEL}`;
    const responseHeaders = {
      "content-type": "application/json",
      etag: '"fixture-etag"',
      "x-ratelimit-limit": "5000",
      "x-ratelimit-remaining": "4999",
      "x-ratelimit-reset": "1785373200",
      "x-ratelimit-resource": "core",
    };
    const json = (value: unknown) => new Response(JSON.stringify(value), { status: 200, headers: responseHeaders });
    if (/\/pulls\/7$/.test(url)) {
      return json({
        node_id: "PR7",
        created_at: "2026-07-01T00:00:00.000Z",
        merged_at: "2026-07-04T00:00:00.000Z",
        merge_commit_sha: "6666666666666666666666666666666666666666",
      });
    }
    if (/\/pulls\/7\/commits/.test(url)) {
      return json([
        {
          sha: "7777777777777777777777777777777777777777",
          commit: { committer: { date: "2026-07-02T00:00:00.000Z" } },
        },
      ]);
    }
    if (/\/check-runs/.test(url)) {
      return json({
        check_runs: [
          {
            id: 701,
            node_id: "CHECK701",
            head_sha: "7777777777777777777777777777777777777777",
            name: "ci",
            status: "completed",
            conclusion: "success",
            started_at: "2026-07-02T00:10:00.000Z",
            completed_at: "2026-07-02T00:20:00.000Z",
          },
          {
            id: 705,
            node_id: "CHECK705",
            head_sha: "7777777777777777777777777777777777777777",
            name: "ci",
            status: "in_progress",
            conclusion: null,
            started_at: "2026-07-02T00:30:00.000Z",
            completed_at: null,
          },
        ],
      });
    }
    if (/\/pulls\/7\/reviews/.test(url)) {
      return json([
        {
          id: 702,
          node_id: "REVIEW702",
          state: "APPROVED",
          commit_id: "7777777777777777777777777777777777777777",
          submitted_at: "2026-07-03T00:00:00.000Z",
        },
      ]);
    }
    if (/\/issues\/7\/timeline/.test(url)) {
      return json([
        { id: 703, node_id: "EVENT703", event: "reopened", created_at: "2026-07-06T00:00:00.000Z" },
        {
          id: 704,
          node_id: "EVENT704",
          event: "cross-referenced",
          created_at: "2026-07-01T00:00:00.000Z",
          source: { issue: { id: 99, node_id: "ISSUE99", number: 99 } },
        },
      ]);
    }
    if (/\/commits\?sha=main/.test(url)) {
      return json([
        {
          sha: "8888888888888888888888888888888888888888",
          commit: {
            message: "Revert fixture\n\nThis reverts commit 6666666666666666666666666666666666666666.",
            committer: { date: "2026-07-08T00:00:00.000Z" },
          },
        },
      ]);
    }
    if (/\/repos\/fixture\/repo$/.test(url)) return json({ default_branch: "main" });
    return new Response("{}", { status: 404, headers: responseHeaders });
  };
  const realAdapter = new GitHubRestOutcomeTimelineAdapter({
    token: TOKEN_SENTINEL,
    fetchImpl: mockFetch,
    now: () => "2026-07-30T00:00:00.000Z",
  });
  const actualCollection = await realAdapter.collectPull({
    owner: "fixture",
    repo: "repo",
    repositoryExternalId: "github:repository:fixture/repo",
    pull: {
      number: 7,
      pullExternalId: "github:pull:PR7",
      updatedAt: "2026-07-09T00:00:00.000Z",
    },
    until: "2026-07-30T00:00:00.000Z",
  });
  const actualKinds = new Set(actualCollection.facts.map((fact) => fact.kind));
  prove(
    "real_adapter_preserves_stable_attempt_review_lifecycle_link_and_revert_facts",
    ["pull", "pull_revision", "check_attempt", "review_outcome", "merge", "reopen", "linked_issue", "revert"].every(
      (kind) => actualKinds.has(kind as PullTimelineFact["kind"]),
    ) &&
      actualCollection.facts.find((fact) => fact.kind === "revert")?.revertedAt === "2026-07-08T00:00:00.000Z" &&
      actualCollection.retryable &&
      actualCollection.coverage.some(
        (row) => row.dimension === "checks" && row.status === "unknown" && row.reason === "pending_check_attempt",
      ),
    { kinds: [...actualKinds].sort(), factCount: actualCollection.facts.length, retryable: actualCollection.retryable },
  );

  class RecoveryAdapter implements GitHubOutcomeTimelineAdapter {
    readonly cursors: Array<string | null> = [];
    readonly pageSizes: number[] = [];
    private failEleven = true;
    // Simulates provider-only secret state; no adapter return type contains it.
    private readonly providerToken = TOKEN_SENTINEL;

    async listChangedPullsPage(input: {
      cursor: string | null;
      pageSize: number;
    }) {
      assert.ok(this.providerToken.length > 0);
      this.cursors.push(input.cursor);
      this.pageSizes.push(input.pageSize);
      const refs = (numbers: number[]): ChangedPullRef[] =>
        numbers.map((number) => ({
          number,
          pullExternalId: `github:pull:RECOVERY-${number}`,
          updatedAt: `2026-07-${number}T00:00:00.000Z`,
        }));
      return {
        items: input.cursor === null ? refs([10, 11]) : refs([12]),
        nextCursor: input.cursor === null ? "2" : null,
        etag: input.cursor === null ? '"page-1-etag"' : '"page-2-etag"',
        rateReceipt: exhaustedReceipt,
        coverage: { dimension: "changed_pulls" as const, status: "complete" as const, reason: "complete" as const },
      };
    }

    async collectPull(input: { repositoryExternalId: string; pull: ChangedPullRef }): Promise<PullCollection> {
      if (input.pull.number === 11 && this.failEleven) {
        this.failEleven = false;
        return {
          facts: [],
          coverage: [
            {
              dimension: "pull",
              status: "incomplete",
              reason: "provider_failure",
              detail: "GitHub 500: fixture failure",
            },
          ],
          rateReceipt: exhaustedReceipt,
          retryable: true,
        };
      }
      return {
        facts: [
          {
            schemaVersion: 1,
            externalId: input.pull.pullExternalId,
            repositoryExternalId: input.repositoryExternalId,
            pullExternalId: input.pull.pullExternalId,
            pullNumber: input.pull.number,
            kind: "pull",
            createdAt: input.pull.updatedAt,
          },
        ],
        coverage: [{ dimension: "pull", status: "complete", reason: "complete" }],
        rateReceipt: exhaustedReceipt,
        retryable: false,
      };
    }
  }

  const recoveryStorePath = path.join(sandbox, "recovery.sqlite");
  const recoveryStore = new OutcomeTimelineStore(recoveryStorePath);
  const recoveryAdapter = new RecoveryAdapter();
  const common = {
    owner: "fixture",
    repo: "recovery",
    since: "2026-07-01T00:00:00.000Z",
    until: "2026-07-30T00:00:00.000Z",
    maxPulls: 1,
    store: recoveryStore,
    adapter: recoveryAdapter,
    requiredChecks: { names: ["ci"] },
    now: () => "2026-07-30T00:00:00.000Z",
  };
  const run1 = await runOutcomeTimelineBackfill({ ...common, runId: "recovery-1" });
  const state1 = recoveryStore.readState("github:repository:fixture/recovery")!;
  const run2 = await runOutcomeTimelineBackfill({ ...common, runId: "recovery-2" });
  const state2 = recoveryStore.readState("github:repository:fixture/recovery")!;
  const run3 = await runOutcomeTimelineBackfill({ ...common, runId: "recovery-3" });
  const state3 = recoveryStore.readState("github:repository:fixture/recovery")!;
  const run4 = await runOutcomeTimelineBackfill({ ...common, runId: "recovery-4" });
  prove(
    "incremental_run_bounds_changed_prs_and_resumes_failed_pr_deterministically",
    [run1.processedPulls, run2.processedPulls, run3.processedPulls, run4.processedPulls].every((count) => count <= 1) &&
      state1.activeWindow?.pendingPulls[0]?.number === 11 &&
      state2.activeWindow?.pendingPulls[0]?.number === 11 &&
      state3.activeWindow?.cursor === "2" &&
      run2.status === "incomplete" &&
      run4.status === "complete" &&
      recoveryAdapter.cursors.join(",") === ",2" &&
      recoveryAdapter.pageSizes.every((size) => size === 100) &&
      run4.cursor === "2" &&
      run4.etag === '"page-2-etag"',
    {
      statuses: [run1.status, run2.status, run3.status, run4.status],
      processed: [run1.processedPulls, run2.processedPulls, run3.processedPulls, run4.processedPulls],
      cursors: recoveryAdapter.cursors,
      state1Etag: state1.activeWindow?.etag,
      finalCompletedThrough: run4.completedThrough,
      finalCursor: run4.cursor,
      finalEtag: run4.etag,
    },
  );
  prove(
    "cursor_etag_and_rate_receipt_are_persisted_without_provider_token",
    state1.activeWindow?.etag === '"page-1-etag"' &&
      state1.lastRateReceipt?.remaining === 0 &&
      !fs.readFileSync(recoveryStorePath).includes(Buffer.from(TOKEN_SENTINEL)) &&
      !JSON.stringify([run1, run2, run3, run4]).includes(TOKEN_SENTINEL),
    { etag: state1.activeWindow?.etag, rateReceipt: state1.lastRateReceipt },
  );
  recoveryStore.close();

  class MissingPolicyAdapter implements GitHubOutcomeTimelineAdapter {
    async listChangedPullsPage() {
      return {
        items: [
          {
            number: 20,
            pullExternalId: "github:pull:MISSING-POLICY",
            updatedAt: "2026-07-20T00:00:00.000Z",
          },
        ],
        nextCursor: null,
        etag: '"missing-policy"',
        rateReceipt: null,
        coverage: { dimension: "changed_pulls" as const, status: "complete" as const, reason: "complete" as const },
      };
    }
    async collectPull(input: { repositoryExternalId: string; pull: ChangedPullRef }): Promise<PullCollection> {
      return {
        facts: [
          {
            schemaVersion: 1,
            externalId: input.pull.pullExternalId,
            repositoryExternalId: input.repositoryExternalId,
            pullExternalId: input.pull.pullExternalId,
            pullNumber: input.pull.number,
            kind: "pull",
            createdAt: input.pull.updatedAt,
          },
        ],
        coverage: [{ dimension: "pull", status: "complete", reason: "complete" }],
        rateReceipt: null,
        retryable: false,
      };
    }
  }
  const missingStore = new OutcomeTimelineStore(path.join(sandbox, "missing-policy.sqlite"));
  const missing = await runOutcomeTimelineBackfill({
    owner: "fixture",
    repo: "missing-policy",
    since: "2026-07-01T00:00:00.000Z",
    until: "2026-07-30T00:00:00.000Z",
    maxPulls: 1,
    store: missingStore,
    adapter: new MissingPolicyAdapter(),
    runId: "missing-policy",
    now: () => "2026-07-30T00:00:00.000Z",
  });
  prove(
    "missing_required_check_policy_persists_unknown_coverage",
    missing.status === "unknown" &&
      missingStore.coverage("missing-policy").some(
        (row) => row.reason === "missing_required_check_policy" && row.status === "unknown",
      ),
    { receipt: missing, coverage: missingStore.coverage("missing-policy") },
  );
  missingStore.close();

  const cliSource = fs.readFileSync(path.join(repoRoot, "packages/collector-cli/src/cli.ts"), "utf8");
  const serverSource = fs.readFileSync(path.join(repoRoot, "packages/collector-cli/src/server.ts"), "utf8");
  const maintenanceSource = fs.readFileSync(path.join(repoRoot, "packages/collector-cli/src/maintenance.ts"), "utf8");
  prove(
    "backfill_command_is_explicit_and_absent_from_request_background_paths",
    cliSource.includes('command === "backfill-outcome-timeline"') &&
      !serverSource.includes("github-outcome-backfill") &&
      !maintenanceSource.includes("github-outcome-backfill") &&
      !serverSource.includes("backfill-outcome-timeline") &&
      !maintenanceSource.includes("backfill-outcome-timeline"),
    { commandWired: true, serverImport: false, maintenanceImport: false },
  );
  prove("provider_token_is_used_only_in_request_headers", tokenObservedOnlyInRequest, {
    tokenObservedOnlyInRequest,
  });

  const artifact = {
    schema: "plimsoll.outcome-timeline-proof.v1",
    generatedAt: new Date().toISOString(),
    fixture: path.relative(repoRoot, fixturePath),
    summary: { passed: checks.filter((check) => check.passed).length, failed: checks.filter((check) => !check.passed).length },
    checks,
  };
  const receiptArg = process.argv.indexOf("--receipt");
  if (receiptArg !== -1) {
    const receiptPath = path.resolve(process.argv[receiptArg + 1] ?? "evidence/outcome-timeline-proof.json");
    fs.mkdirSync(path.dirname(receiptPath), { recursive: true });
    fs.writeFileSync(receiptPath, `${JSON.stringify(artifact, null, 2)}\n`, "utf8");
  }
  console.log(JSON.stringify(artifact, null, 2));
  } finally {
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
