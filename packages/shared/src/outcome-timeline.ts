import { z } from "zod";

const timestampSchema = z.string().datetime({ offset: true });
const externalIdSchema = z.string().trim().min(1).max(512);
export const fullCommitShaSchema = z.string().regex(/^[0-9a-f]{40}$/i, "expected a full 40-character commit SHA");

const factBase = {
  schemaVersion: z.literal(1),
  externalId: externalIdSchema,
  repositoryExternalId: externalIdSchema,
  pullExternalId: externalIdSchema,
  pullNumber: z.number().int().positive(),
};

export const pullTimelineFactSchema = z.discriminatedUnion("kind", [
  z
    .object({
      ...factBase,
      kind: z.literal("pull"),
      createdAt: timestampSchema,
    })
    .strict(),
  z
    .object({
      ...factBase,
      kind: z.literal("pull_revision"),
      sha: fullCommitShaSchema,
      committedAt: timestampSchema,
    })
    .strict(),
  z
    .object({
      ...factBase,
      kind: z.literal("check_attempt"),
      checkRunExternalId: externalIdSchema,
      sha: fullCommitShaSchema,
      name: z.string().trim().min(1).max(256),
      conclusion: z.enum([
        "success",
        "failure",
        "cancelled",
        "timed_out",
        "action_required",
        "neutral",
        "skipped",
        "stale",
        "startup_failure",
        "unknown",
      ]),
      startedAt: timestampSchema.nullable(),
      completedAt: timestampSchema,
    })
    .strict(),
  z
    .object({
      ...factBase,
      kind: z.literal("review_outcome"),
      reviewExternalId: externalIdSchema,
      sha: fullCommitShaSchema.nullable(),
      outcome: z.enum(["approved", "changes_requested", "dismissed", "commented"]),
      submittedAt: timestampSchema,
    })
    .strict(),
  z
    .object({
      ...factBase,
      kind: z.literal("merge"),
      mergeSha: fullCommitShaSchema,
      mergedAt: timestampSchema,
    })
    .strict(),
  z
    .object({
      ...factBase,
      kind: z.literal("reopen"),
      reopenedAt: timestampSchema,
    })
    .strict(),
  z
    .object({
      ...factBase,
      kind: z.literal("revert"),
      revertSha: fullCommitShaSchema,
      revertedSha: fullCommitShaSchema,
      revertedAt: timestampSchema,
      evidence: z
        .object({
          source: z.literal("commit_message_full_sha"),
          matchedFullSha: fullCommitShaSchema,
        })
        .strict(),
    })
    .strict(),
  z
    .object({
      ...factBase,
      kind: z.literal("linked_issue"),
      issueExternalId: externalIdSchema,
      issueNumber: z.number().int().positive(),
      linkedAt: timestampSchema.nullable(),
    })
    .strict(),
]);
export type PullTimelineFact = z.infer<typeof pullTimelineFactSchema>;

export const timelineCoverageStatusSchema = z.enum(["complete", "incomplete", "unknown"]);
export type TimelineCoverageStatus = z.infer<typeof timelineCoverageStatusSchema>;

export const outcomeTimelineCoverageSchema = z
  .object({
    runId: externalIdSchema,
    repositoryExternalId: externalIdSchema,
    pullExternalId: externalIdSchema.optional(),
    dimension: z.enum([
      "changed_pulls",
      "pull",
      "revisions",
      "checks",
      "reviews",
      "timeline_events",
      "linked_issues",
      "reverts",
      "required_checks",
    ]),
    status: timelineCoverageStatusSchema,
    reason: z.enum([
      "complete",
      "pagination_limit",
      "provider_failure",
      "rate_exhausted",
      "missing_required_check_policy",
      "pending_check_attempt",
      "not_modified",
    ]),
    detail: z.string().trim().min(1).max(512).optional(),
  })
  .strict();
export type OutcomeTimelineCoverage = z.infer<typeof outcomeTimelineCoverageSchema>;

export const githubRateReceiptSchema = z
  .object({
    observedAt: timestampSchema,
    limit: z.number().int().nonnegative().nullable(),
    remaining: z.number().int().nonnegative().nullable(),
    resetAt: timestampSchema.nullable(),
    resource: z.string().trim().min(1).max(128).nullable(),
  })
  .strict();
export type GitHubRateReceipt = z.infer<typeof githubRateReceiptSchema>;

export const changedPullRefSchema = z
  .object({
    number: z.number().int().positive(),
    pullExternalId: externalIdSchema,
    updatedAt: timestampSchema,
  })
  .strict();
export type ChangedPullRef = z.infer<typeof changedPullRefSchema>;

const activeBackfillWindowSchema = z
  .object({
    since: timestampSchema,
    until: timestampSchema,
    cursor: z.string().trim().min(1).nullable(),
    nextCursor: z.string().trim().min(1).nullable(),
    pageLoaded: z.boolean(),
    pendingPulls: z.array(changedPullRefSchema),
    etag: z.string().trim().min(1).nullable(),
  })
  .strict();

export const outcomeTimelineBackfillStateSchema = z
  .object({
    schemaVersion: z.literal(1),
    repositoryExternalId: externalIdSchema,
    owner: z.string().trim().min(1),
    repo: z.string().trim().min(1),
    completedThrough: timestampSchema.nullable(),
    lastCursor: z.string().trim().min(1).nullable(),
    lastEtag: z.string().trim().min(1).nullable(),
    activeWindow: activeBackfillWindowSchema.nullable(),
    lastRateReceipt: githubRateReceiptSchema.nullable(),
  })
  .strict();
export type OutcomeTimelineBackfillState = z.infer<typeof outcomeTimelineBackfillStateSchema>;

export type RequiredCheckPolicy = {
  names: string[];
};

type CheckState = "failed" | "passed" | "unknown";

function checkStateForSha(
  checks: Extract<PullTimelineFact, { kind: "check_attempt" }>[],
  requiredNames: string[],
): { state: CheckState; greenAt: string | null; failedAt: string | null } {
  const required = new Set(requiredNames);
  const relevant = checks
    .filter((check) => required.has(check.name))
    .sort((a, b) => a.completedAt.localeCompare(b.completedAt) || a.externalId.localeCompare(b.externalId));
  const latestByName = new Map<string, Extract<PullTimelineFact, { kind: "check_attempt" }>>();
  let failedAt: string | null = null;
  for (const check of relevant) {
    latestByName.set(check.name, check);
    if (["failure", "cancelled", "timed_out", "action_required", "stale", "startup_failure"].includes(check.conclusion)) {
      failedAt ??= check.completedAt;
    }
    if (
      requiredNames.every((name) => latestByName.get(name)?.conclusion === "success")
    ) {
      return { state: failedAt ? "passed" : "passed", greenAt: check.completedAt, failedAt };
    }
  }
  if (failedAt) return { state: "failed", greenAt: null, failedAt };
  return { state: "unknown", greenAt: null, failedAt: null };
}

export type PullOutcomeDerivation = {
  pullExternalId: string;
  pullNumber: number;
  coverage: "complete" | "unknown";
  revisionCount: number;
  firstPassSuccess: boolean | null;
  timeToGreenMs: number | null;
  retryEpisodes: Array<{ sha: string; failedAt: string; passedAt: string }>;
  correctionLoops: Array<{ failedSha: string; correctedSha: string; failedAt: string; passedAt: string }>;
  reviewCorrections: Array<{
    changesRequestedReviewId: string;
    correctedSha: string;
    approvalReviewId: string;
  }>;
  greenMultiCommitWithoutRework: boolean;
  rework: Array<{ kind: "reopen" | "revert"; at: string; externalId: string; inWindow: boolean }>;
};

/**
 * Derive correction lineage only from immutable facts. Check-derived metrics
 * deliberately remain unknown without a named required-check policy: a green
 * optional check is not proof that a revision was green.
 */
export function derivePullOutcomeTimeline(input: {
  facts: PullTimelineFact[];
  requiredChecks?: RequiredCheckPolicy;
  reworkWindowDays: number;
}): PullOutcomeDerivation[] {
  const parsed = input.facts.map((fact) => pullTimelineFactSchema.parse(fact));
  const groups = new Map<string, PullTimelineFact[]>();
  for (const fact of parsed) {
    const facts = groups.get(fact.pullExternalId) ?? [];
    facts.push(fact);
    groups.set(fact.pullExternalId, facts);
  }

  return [...groups.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([pullExternalId, facts]) => {
      const pullNumber = facts[0]!.pullNumber;
      const pull = facts.find((fact): fact is Extract<PullTimelineFact, { kind: "pull" }> => fact.kind === "pull");
      const revisions = facts
        .filter((fact): fact is Extract<PullTimelineFact, { kind: "pull_revision" }> => fact.kind === "pull_revision")
        .sort((a, b) => a.committedAt.localeCompare(b.committedAt) || a.sha.localeCompare(b.sha));
      const checks = facts.filter(
        (fact): fact is Extract<PullTimelineFact, { kind: "check_attempt" }> => fact.kind === "check_attempt",
      );
      const policy = input.requiredChecks?.names.map((name) => name.trim()).filter(Boolean);
      const checkStates = new Map<string, ReturnType<typeof checkStateForSha>>();
      if (policy?.length) {
        for (const revision of revisions) {
          checkStates.set(
            revision.sha,
            checkStateForSha(
              checks.filter((check) => check.sha === revision.sha),
              policy,
            ),
          );
        }
      }

      const retryEpisodes: PullOutcomeDerivation["retryEpisodes"] = [];
      if (policy?.length) {
        for (const revision of revisions) {
          const revisionChecks = checks
            .filter((check) => check.sha === revision.sha && policy.includes(check.name))
            .sort((a, b) => a.completedAt.localeCompare(b.completedAt) || a.externalId.localeCompare(b.externalId));
          const latestByName = new Map<string, Extract<PullTimelineFact, { kind: "check_attempt" }>>();
          let failedAt: string | null = null;
          for (const check of revisionChecks) {
            latestByName.set(check.name, check);
            if (["failure", "cancelled", "timed_out", "action_required", "stale", "startup_failure"].includes(check.conclusion)) {
              failedAt ??= check.completedAt;
            }
            if (failedAt && policy.every((name) => latestByName.get(name)?.conclusion === "success")) {
              retryEpisodes.push({ sha: revision.sha, failedAt, passedAt: check.completedAt });
              break;
            }
          }
        }
      }

      const correctionLoops: PullOutcomeDerivation["correctionLoops"] = [];
      if (policy?.length) {
        for (let index = 0; index < revisions.length - 1; index += 1) {
          const failed = revisions[index]!;
          const failedState = checkStates.get(failed.sha);
          if (!failedState?.failedAt) continue;
          const nextRevision = revisions
            .slice(index + 1)
            .find((revision) => revision.committedAt > failedState.failedAt!);
          if (!nextRevision || (failedState.greenAt && failedState.greenAt < nextRevision.committedAt)) continue;
          const corrected = revisions
            .slice(index + 1)
            .find(
              (revision) =>
                revision.committedAt >= nextRevision.committedAt && checkStates.get(revision.sha)?.greenAt,
            );
          if (!corrected) continue;
          const correctedState = checkStates.get(corrected.sha)!;
          correctionLoops.push({
            failedSha: failed.sha,
            correctedSha: corrected.sha,
            failedAt: failedState.failedAt,
            passedAt: correctedState.greenAt!,
          });
        }
      }

      const reviews = facts
        .filter((fact): fact is Extract<PullTimelineFact, { kind: "review_outcome" }> => fact.kind === "review_outcome")
        .sort((a, b) => a.submittedAt.localeCompare(b.submittedAt) || a.externalId.localeCompare(b.externalId));
      const reviewCorrections: PullOutcomeDerivation["reviewCorrections"] = [];
      for (const requested of reviews.filter((review) => review.outcome === "changes_requested")) {
        const corrected = revisions.find((revision) => revision.committedAt > requested.submittedAt);
        if (!corrected) continue;
        const approval = reviews.find(
          (review) => review.outcome === "approved" && review.submittedAt > corrected.committedAt,
        );
        if (approval) {
          reviewCorrections.push({
            changesRequestedReviewId: requested.reviewExternalId,
            correctedSha: corrected.sha,
            approvalReviewId: approval.reviewExternalId,
          });
        }
      }

      const merge = facts
        .filter((fact): fact is Extract<PullTimelineFact, { kind: "merge" }> => fact.kind === "merge")
        .sort((a, b) => a.mergedAt.localeCompare(b.mergedAt))[0];
      const windowMs = input.reworkWindowDays * 24 * 60 * 60 * 1000;
      const mergeMs = merge ? Date.parse(merge.mergedAt) : NaN;
      const rework = facts
        .filter(
          (fact): fact is Extract<PullTimelineFact, { kind: "reopen" | "revert" }> =>
            fact.kind === "reopen" || fact.kind === "revert",
        )
        .map((fact) => {
          const at = fact.kind === "reopen" ? fact.reopenedAt : fact.revertedAt;
          const atMs = Date.parse(at);
          return {
            kind: fact.kind,
            at,
            externalId: fact.externalId,
            inWindow: Number.isFinite(mergeMs) && atMs >= mergeMs && atMs <= mergeMs + windowMs,
          };
        })
        .sort((a, b) => a.at.localeCompare(b.at) || a.externalId.localeCompare(b.externalId));

      const firstState = revisions[0] ? checkStates.get(revisions[0].sha) : undefined;
      const firstGreenAt = revisions
        .map((revision) => checkStates.get(revision.sha)?.greenAt)
        .filter((at): at is string => Boolean(at))
        .sort()[0];
      const timeToGreenMs = pull && firstGreenAt ? Date.parse(firstGreenAt) - Date.parse(pull.createdAt) : null;
      const allKnownGreen = Boolean(policy?.length) && revisions.length > 1 && revisions.every(
        (revision) => checkStates.get(revision.sha)?.greenAt && !checkStates.get(revision.sha)?.failedAt,
      );

      return {
        pullExternalId,
        pullNumber,
        coverage: policy?.length ? "complete" : "unknown",
        revisionCount: revisions.length,
        firstPassSuccess: policy?.length ? Boolean(firstState?.greenAt && !firstState.failedAt) : null,
        timeToGreenMs: policy?.length ? timeToGreenMs : null,
        retryEpisodes,
        correctionLoops,
        reviewCorrections,
        greenMultiCommitWithoutRework:
          allKnownGreen && correctionLoops.length === 0 && rework.every((signal) => !signal.inWindow),
        rework,
      };
    });
}
