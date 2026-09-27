import type Database from "better-sqlite3";
import { terminalPrivacyEligibilitySql } from "../packages/collector-cli/src/privacy-disposition";
import { remoteLinkageHash } from "../packages/shared/src/linkage";

export type AllocationAmounts = {
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  knownCostNanos: number;
};

export type AllocationEvent = {
  eventId: string;
  sessionId: string | null;
  observedAt: string;
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheWriteTokens: number | null;
  costUsd: number | null;
  repoHash: string | null;
  branchHash: string | null;
  headSha: string | null;
  workItemId?: string | null;
  runId?: string | null;
  workEvidenceRef?: string | null;
  workAttributionState?: string | null;
};

export type WorkArtifactLink = {
  workItemId: string;
  artifactRef: string;
  evidenceRef: string;
};

export type PullCandidate = {
  pull: number;
  repoHash: string;
  branchHash: string | null;
  headSha: string | null;
  mergeCommitSha?: string | null;
  commitShas?: string[];
  createdAt: string;
  updatedAt: string;
  closedAt?: string | null;
  mergedAt?: string | null;
};

export type AllocationConfidence = "direct" | "inferred" | "unallocated";

export type AllocationReceipt = {
  eventId: string;
  sessionId: string | null;
  observedAt: string;
  repoHash: string | null;
  pull: number | null;
  confidence: AllocationConfidence;
  reason: string;
  weight: 0 | 1;
  amounts: AllocationAmounts;
  costKnown: boolean;
  workItemId: string | null;
  runId: string | null;
  workAllocation: "BOUND" | "UNLINKED";
  via: "work_id" | "inferred_git" | "UNLINKED";
};

export type AllocationTotals = AllocationAmounts & {
  events: number;
  pricedEvents: number;
  unpricedEvents: number;
  knownCostUsd: number;
  costStatus: "known" | "partial" | "unknown";
};

export type PullAllocation = AllocationTotals & {
  pull: number;
  repoHash: string;
  sessions: number;
  directEvents: number;
  inferredEvents: number;
  joinedVia: string[];
  /** Null if any allocated event has no price. knownCostUsd remains visible. */
  costUsd: number | null;
};

export type AllocationResult = {
  receipts: AllocationReceipt[];
  pullRows: PullAllocation[];
  workRows: Array<AllocationTotals & { workItemId: string | null; runs: string[]; pulls: number[] }>;
  coverage: {
    captured: AllocationTotals;
    direct: AllocationTotals;
    inferred: AllocationTotals;
    unallocated: AllocationTotals;
    reconciliation: {
      inputTokens: boolean;
      outputTokens: boolean;
      cacheReadTokens: boolean;
      cacheWriteTokens: boolean;
      knownCostNanos: boolean;
      knownCostUsd: boolean;
      pricedEvents: boolean;
      unpricedEvents: boolean;
      exact: boolean;
    };
  };
};

export type AllocationOptions = {
  /** Candidate lifecycle padding for work just before PR open / after close. */
  fallbackWindowMs?: number;
  /** Maximum distance from an unlinked event to stable same-session context. */
  segmentWindowMs?: number;
  /** Fail closed instead of allowing an unbounded GitHub candidate set. */
  maxCandidates?: number;
  /** Accepted local work-to-PR evidence for the named repository. */
  workArtifacts?: WorkArtifactLink[];
  repository?: string;
  /** Rollback keeps source metadata and the original git inference. */
  explicitJoinEnabled?: boolean;
};

const DEFAULT_FALLBACK_WINDOW_MS = 7 * 24 * 60 * 60 * 1_000;
const DEFAULT_SEGMENT_WINDOW_MS = 30 * 60 * 1_000;
const DEFAULT_MAX_CANDIDATES = 100;
const BEADS_WORK = /^beads:eco-[a-z0-9]+(?:\.[1-9][0-9]*)*$/;
const RUN_UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const ZERO_AMOUNTS: AllocationAmounts = {
  inputTokens: 0,
  outputTokens: 0,
  cacheReadTokens: 0,
  cacheWriteTokens: 0,
  knownCostNanos: 0,
};

type NormalizedEvent = Omit<AllocationEvent, "repoHash" | "branchHash" | "headSha"> & {
  repoHash: string | null;
  branchHash: string | null;
  headSha: string | null;
  observedMs: number;
  amounts: AllocationAmounts;
  costKnown: boolean;
};

type NormalizedCandidate = PullCandidate & {
  key: string;
  repoHash: string;
  branchHash: string | null;
  headSha: string | null;
  mergeCommitSha: string | null;
  commitShas: string[];
};

type CandidateResolution =
  | { candidate: NormalizedCandidate; reason: string }
  | { candidate: null; reason: string };

function linkage(value: string | null | undefined): string | null {
  const normalized = value?.trim().toLowerCase();
  return normalized ? normalized : null;
}

function commitSha(value: string | null | undefined): string | null {
  const normalized = value?.trim().toLowerCase();
  return normalized && /^[0-9a-f]{40,64}$/.test(normalized) ? normalized : null;
}

function tokenCount(value: number | null, field: string, eventId: string): number {
  if (value === null) return 0;
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Invalid ${field} for allocation event ${eventId}`);
  }
  return value;
}

function costNanos(value: number | null, eventId: string): number {
  if (value === null) return 0;
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid costUsd for allocation event ${eventId}`);
  }
  const nanos = Math.round(value * 1_000_000_000);
  if (!Number.isSafeInteger(nanos)) {
    throw new Error(`costUsd exceeds exact allocation range for event ${eventId}`);
  }
  return nanos;
}

function normalizeEvent(event: AllocationEvent): NormalizedEvent {
  const observedMs = Date.parse(event.observedAt);
  return {
    ...event,
    repoHash: linkage(event.repoHash),
    branchHash: linkage(event.branchHash),
    headSha: commitSha(event.headSha),
    observedMs,
    amounts: {
      inputTokens: tokenCount(event.inputTokens, "inputTokens", event.eventId),
      outputTokens: tokenCount(event.outputTokens, "outputTokens", event.eventId),
      cacheReadTokens: tokenCount(event.cacheReadTokens, "cacheReadTokens", event.eventId),
      cacheWriteTokens: tokenCount(event.cacheWriteTokens, "cacheWriteTokens", event.eventId),
      knownCostNanos: costNanos(event.costUsd, event.eventId),
    },
    costKnown: event.costUsd !== null,
  };
}

function normalizeCandidate(candidate: PullCandidate): NormalizedCandidate {
  const repoHash = linkage(candidate.repoHash);
  if (!repoHash) throw new Error(`Pull #${candidate.pull} is missing repoHash`);
  return {
    ...candidate,
    key: `${repoHash}#${candidate.pull}`,
    repoHash,
    branchHash: linkage(candidate.branchHash),
    headSha: commitSha(candidate.headSha),
    mergeCommitSha: commitSha(candidate.mergeCommitSha),
    commitShas: [...new Set((candidate.commitShas ?? []).map(commitSha).filter((sha): sha is string => Boolean(sha)))].sort(),
  };
}

function lifecycleContains(
  candidate: NormalizedCandidate,
  observedMs: number,
  fallbackWindowMs: number,
): boolean {
  if (!Number.isFinite(observedMs)) return false;
  const createdMs = Date.parse(candidate.createdAt);
  const updatedMs = Date.parse(candidate.updatedAt);
  if (!Number.isFinite(createdMs) || !Number.isFinite(updatedMs)) return false;
  const terminalValue = candidate.mergedAt ?? candidate.closedAt;
  const terminalMs = terminalValue ? Date.parse(terminalValue) : updatedMs;
  if (!Number.isFinite(terminalMs)) return false;
  return observedMs >= createdMs - fallbackWindowMs && observedMs <= terminalMs + fallbackWindowMs;
}

function exactMatch(candidate: NormalizedCandidate, sha: string): { rank: number; reason: string } | null {
  if (candidate.headSha === sha) return { rank: 0, reason: "head_sha" };
  if (candidate.commitShas.includes(sha)) return { rank: 1, reason: "commit_sha" };
  if (candidate.mergeCommitSha === sha) return { rank: 2, reason: "merge_sha" };
  return null;
}

function resolveCandidate(
  event: Pick<NormalizedEvent, "repoHash" | "branchHash" | "headSha" | "observedMs">,
  candidates: NormalizedCandidate[],
  fallbackWindowMs: number,
): CandidateResolution {
  if (!event.repoHash) return { candidate: null, reason: "missing_repo_evidence" };
  const repoCandidates = candidates.filter((candidate) => candidate.repoHash === event.repoHash);
  if (repoCandidates.length === 0) return { candidate: null, reason: "no_repo_candidate" };

  if (event.headSha) {
    const exact = repoCandidates
      .map((candidate) => ({ candidate, match: exactMatch(candidate, event.headSha!) }))
      .filter((entry): entry is { candidate: NormalizedCandidate; match: { rank: number; reason: string } } => Boolean(entry.match));
    if (exact.length > 0) {
      const bestRank = Math.min(...exact.map((entry) => entry.match.rank));
      let narrowed = exact.filter((entry) => entry.match.rank === bestRank);
      if (event.branchHash) {
        const branchMatches = narrowed.filter((entry) => entry.candidate.branchHash === event.branchHash);
        if (branchMatches.length > 0) narrowed = branchMatches;
      }
      if (narrowed.length > 1) {
        const active = narrowed.filter((entry) =>
          lifecycleContains(entry.candidate, event.observedMs, fallbackWindowMs),
        );
        if (active.length > 0) narrowed = active;
      }
      if (narrowed.length === 1) {
        return { candidate: narrowed[0].candidate, reason: narrowed[0].match.reason };
      }
      return { candidate: null, reason: "ambiguous_exact_head" };
    }
  }

  if (!Number.isFinite(event.observedMs)) {
    return { candidate: null, reason: "invalid_observed_at" };
  }
  const active = repoCandidates.filter((candidate) =>
    lifecycleContains(candidate, event.observedMs, fallbackWindowMs),
  );
  if (event.branchHash) {
    const branchMatches = active.filter((candidate) => candidate.branchHash === event.branchHash);
    if (branchMatches.length === 1) {
      return { candidate: branchMatches[0], reason: "branch_time" };
    }
    if (branchMatches.length > 1) return { candidate: null, reason: "ambiguous_branch_time" };
    const anyBranch = repoCandidates.some((candidate) => candidate.branchHash === event.branchHash);
    return { candidate: null, reason: anyBranch ? "branch_outside_time_window" : "no_branch_candidate" };
  }
  if (active.length === 1) return { candidate: active[0], reason: "repo_time" };
  if (active.length > 1) return { candidate: null, reason: "ambiguous_repo_time" };
  return { candidate: null, reason: "repo_outside_time_window" };
}

const MAX_SAFE_INTEGER_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);

function addExactAmount(
  field: keyof AllocationAmounts,
  left: number,
  right: number,
): number {
  // Inputs are already per-event safe integers. Compare the exact BigInt sum
  // before converting back to number so IEEE-754 rounding can never turn an
  // overflowing aggregate into a plausible reconciled value.
  const sum = BigInt(left) + BigInt(right);
  if (sum > MAX_SAFE_INTEGER_BIGINT) {
    throw new RangeError(
      `Allocation aggregate exceeds Number.MAX_SAFE_INTEGER for ${field}`,
    );
  }
  return Number(sum);
}

function addAmounts(left: AllocationAmounts, right: AllocationAmounts): AllocationAmounts {
  return {
    inputTokens: addExactAmount("inputTokens", left.inputTokens, right.inputTokens),
    outputTokens: addExactAmount("outputTokens", left.outputTokens, right.outputTokens),
    cacheReadTokens: addExactAmount(
      "cacheReadTokens",
      left.cacheReadTokens,
      right.cacheReadTokens,
    ),
    cacheWriteTokens: addExactAmount(
      "cacheWriteTokens",
      left.cacheWriteTokens,
      right.cacheWriteTokens,
    ),
    knownCostNanos: addExactAmount(
      "knownCostNanos",
      left.knownCostNanos,
      right.knownCostNanos,
    ),
  };
}

function summarize(receipts: AllocationReceipt[]): AllocationTotals {
  const amounts = receipts.reduce(
    (total, receipt) => addAmounts(total, receipt.amounts),
    ZERO_AMOUNTS,
  );
  const pricedEvents = receipts.filter((receipt) => receipt.costKnown).length;
  const unpricedEvents = receipts.length - pricedEvents;
  return {
    ...amounts,
    events: receipts.length,
    pricedEvents,
    unpricedEvents,
    knownCostUsd: amounts.knownCostNanos / 1_000_000_000,
    costStatus: unpricedEvents === 0 ? "known" : pricedEvents === 0 ? "unknown" : "partial",
  };
}

function receipt(
  event: NormalizedEvent,
  confidence: AllocationConfidence,
  reason: string,
  candidate: NormalizedCandidate | null,
  workItemId: string | null = null,
  runId: string | null = null,
): AllocationReceipt {
  return {
    eventId: event.eventId,
    sessionId: event.sessionId,
    observedAt: event.observedAt,
    repoHash: candidate?.repoHash ?? event.repoHash,
    pull: candidate?.pull ?? null,
    confidence,
    reason,
    weight: candidate ? 1 : 0,
    amounts: event.amounts,
    costKnown: event.costKnown,
    workItemId,
    runId,
    workAllocation: workItemId ? "BOUND" : "UNLINKED",
    via: workItemId && candidate ? "work_id" : candidate ? "inferred_git" : "UNLINKED",
  };
}

function stableNeighborContext(
  event: NormalizedEvent,
  beforeInput: NormalizedEvent | undefined,
  afterInput: NormalizedEvent | undefined,
  segmentWindowMs: number,
): { repoHash: string; branchHash: string | null } | null {
  let before = beforeInput;
  let after = afterInput;
  if (before && (!Number.isFinite(event.observedMs) || event.observedMs - before.observedMs > segmentWindowMs)) {
    before = undefined;
  }
  if (after && (!Number.isFinite(event.observedMs) || after.observedMs - event.observedMs > segmentWindowMs)) {
    after = undefined;
  }
  if (!before && !after) return null;
  if (before && after && before.repoHash !== after.repoHash) return null;
  const repoHash = before?.repoHash ?? after?.repoHash;
  if (!repoHash) return null;
  if (
    before?.branchHash &&
    after?.branchHash &&
    before.branchHash !== after.branchHash
  ) {
    return null;
  }
  return { repoHash, branchHash: before?.branchHash ?? after?.branchHash ?? null };
}

/**
 * Allocate each captured event to zero or one pull. No token class or known
 * cost is divided or copied: an allocated receipt always has weight 1 and an
 * unallocated receipt weight 0. The returned coverage buckets are asserted to
 * reconcile exactly before the result leaves this function.
 */
export function allocateEvents(
  inputEvents: AllocationEvent[],
  inputCandidates: PullCandidate[],
  options: AllocationOptions = {},
): AllocationResult {
  const fallbackWindowMs = options.fallbackWindowMs ?? DEFAULT_FALLBACK_WINDOW_MS;
  const segmentWindowMs = options.segmentWindowMs ?? DEFAULT_SEGMENT_WINDOW_MS;
  const maxCandidates = options.maxCandidates ?? DEFAULT_MAX_CANDIDATES;
  if (inputCandidates.length > maxCandidates) {
    throw new Error(`Allocation candidate limit exceeded: ${inputCandidates.length} > ${maxCandidates}`);
  }
  const candidates = inputCandidates.map(normalizeCandidate).sort((a, b) =>
    a.key.localeCompare(b.key),
  );
  if (new Set(candidates.map((candidate) => candidate.key)).size !== candidates.length) {
    throw new Error("Duplicate pull candidate key");
  }
  const events = inputEvents.map(normalizeEvent).sort((a, b) =>
    a.observedAt.localeCompare(b.observedAt) || a.eventId.localeCompare(b.eventId),
  );
  if (new Set(events.map((event) => event.eventId)).size !== events.length) {
    throw new Error("Duplicate allocation event id");
  }
  const artifactPrefix = options.repository ? `github:${options.repository.toLowerCase()}/pull/` : null;
  const artifactRepoHash = options.repository
    ? remoteLinkageHash(`https://github.com/${options.repository.toLowerCase()}.git`) : null;
  const artifactPulls = new Map<string, Set<number>>();
  for (const artifact of options.workArtifacts ?? []) {
    if (!BEADS_WORK.test(artifact.workItemId) || !artifact.evidenceRef || !artifactPrefix ||
        !artifact.artifactRef.startsWith(artifactPrefix)) continue;
    const number = Number(artifact.artifactRef.slice(artifactPrefix.length));
    if (!Number.isSafeInteger(number) || number < 1 ||
        artifact.artifactRef !== `${artifactPrefix}${number}` ||
        !candidates.some((candidate) => candidate.pull === number && candidate.repoHash === artifactRepoHash)) continue;
    const pulls = artifactPulls.get(artifact.workItemId) ?? new Set<number>();
    pulls.add(number);
    artifactPulls.set(artifact.workItemId, pulls);
  }

  const bySession = new Map<string, NormalizedEvent[]>();
  for (const event of events) {
    if (!event.sessionId) continue;
    const rows = bySession.get(event.sessionId) ?? [];
    rows.push(event);
    bySession.set(event.sessionId, rows);
  }
  const neighbors = new Map<
    string,
    { before: NormalizedEvent | undefined; after: NormalizedEvent | undefined }
  >();
  for (const rows of bySession.values()) {
    let before: NormalizedEvent | undefined;
    for (const row of rows) {
      neighbors.set(row.eventId, { before, after: undefined });
      if (row.repoHash) before = row;
    }
    let after: NormalizedEvent | undefined;
    for (let index = rows.length - 1; index >= 0; index -= 1) {
      const row = rows[index];
      neighbors.get(row.eventId)!.after = after;
      if (row.repoHash) after = row;
    }
  }

  const receipts: AllocationReceipt[] = [];
  for (const event of events) {
    const bound = options.explicitJoinEnabled !== false &&
      typeof event.workItemId === "string" && BEADS_WORK.test(event.workItemId) &&
      event.workItemId.length - "beads:".length <= 128 &&
      typeof event.runId === "string" && RUN_UUID_V4.test(event.runId) &&
      typeof event.workEvidenceRef === "string" && event.workEvidenceRef.length > 0 &&
      event.workAttributionState !== "conflict";
    if (bound) {
      const matches = artifactPulls.get(event.workItemId!);
      const number = matches?.size === 1 ? [...matches][0] : null;
      const candidate = number === null ? null : candidates.find((row) =>
        row.pull === number && row.repoHash === artifactRepoHash) ?? null;
      receipts.push(receipt(event, candidate ? "direct" : "unallocated",
        candidate ? "work_id" : "work_without_unique_artifact_evidence",
        candidate, event.workItemId!, event.runId!));
      continue;
    }
    let resolution = resolveCandidate(event, candidates, fallbackWindowMs);
    if (event.repoHash) {
      receipts.push(
        resolution.candidate
          ? receipt(
              event,
              ["head_sha", "commit_sha", "merge_sha"].includes(resolution.reason)
                ? "direct"
                : "inferred",
              resolution.reason,
              resolution.candidate,
            )
          : receipt(event, "unallocated", resolution.reason, null),
      );
      continue;
    }

    const neighbor = neighbors.get(event.eventId);
    const context = neighbor
      ? stableNeighborContext(event, neighbor.before, neighbor.after, segmentWindowMs)
      : null;
    if (!context) {
      receipts.push(receipt(event, "unallocated", "no_stable_session_segment", null));
      continue;
    }
    resolution = resolveCandidate(
      {
        repoHash: context.repoHash,
        branchHash: event.branchHash ?? context.branchHash,
        headSha: event.headSha,
        observedMs: event.observedMs,
      },
      candidates,
      fallbackWindowMs,
    );
    if (resolution.candidate) {
      const contextualEvent = { ...event, repoHash: context.repoHash };
      receipts.push(
        receipt(
          contextualEvent,
          "inferred",
          `session_segment_${resolution.reason}`,
          resolution.candidate,
        ),
      );
    } else {
      receipts.push(receipt(event, "unallocated", `session_segment_${resolution.reason}`, null));
    }
  }

  const captured = summarize(receipts);
  const direct = summarize(receipts.filter((row) => row.confidence === "direct"));
  const inferred = summarize(receipts.filter((row) => row.confidence === "inferred"));
  const unallocated = summarize(receipts.filter((row) => row.confidence === "unallocated"));
  const reconciled = addAmounts(addAmounts(direct, inferred), unallocated);
  const reconciliation = {
    inputTokens: captured.inputTokens === reconciled.inputTokens,
    outputTokens: captured.outputTokens === reconciled.outputTokens,
    cacheReadTokens: captured.cacheReadTokens === reconciled.cacheReadTokens,
    cacheWriteTokens: captured.cacheWriteTokens === reconciled.cacheWriteTokens,
    knownCostNanos: captured.knownCostNanos === reconciled.knownCostNanos,
    // USD is a display conversion; nanodollars are the exact reconciliation
    // surface so IEEE-754 addition cannot create a false mismatch.
    knownCostUsd: captured.knownCostNanos === reconciled.knownCostNanos,
    pricedEvents:
      captured.pricedEvents === direct.pricedEvents + inferred.pricedEvents + unallocated.pricedEvents,
    unpricedEvents:
      captured.unpricedEvents === direct.unpricedEvents + inferred.unpricedEvents + unallocated.unpricedEvents,
    exact: false,
  };
  reconciliation.exact = Object.entries(reconciliation)
    .filter(([key]) => key !== "exact")
    .every(([, value]) => value);
  if (!reconciliation.exact) throw new Error("Allocation coverage failed exact reconciliation");

  const pullBuckets = new Map<string, AllocationReceipt[]>();
  for (const row of receipts) {
    if (row.pull === null || !row.repoHash) continue;
    const key = `${row.repoHash}#${row.pull}`;
    const rows = pullBuckets.get(key) ?? [];
    rows.push(row);
    pullBuckets.set(key, rows);
  }
  const pullRows = [...pullBuckets.entries()]
    .map(([key, rows]) => {
      const totals = summarize(rows);
      const [repoHash, pullText] = key.split("#");
      return {
        ...totals,
        pull: Number(pullText),
        repoHash,
        sessions: new Set(rows.map((row) => row.sessionId).filter(Boolean)).size,
        directEvents: rows.filter((row) => row.confidence === "direct").length,
        inferredEvents: rows.filter((row) => row.confidence === "inferred").length,
        joinedVia: [...new Set(rows.map((row) => row.reason))].sort(),
        costUsd: totals.costStatus === "known" ? totals.knownCostUsd : null,
      } satisfies PullAllocation;
    })
    .sort((a, b) => a.repoHash.localeCompare(b.repoHash) || a.pull - b.pull);

  const workBuckets = new Map<string | null, AllocationReceipt[]>();
  for (const row of receipts) {
    const bucket = workBuckets.get(row.workItemId) ?? [];
    bucket.push(row);
    workBuckets.set(row.workItemId, bucket);
  }
  const workRows = [...workBuckets.entries()].map(([workItemId, rows]) => ({
    ...summarize(rows), workItemId,
    runs: [...new Set(rows.map((row) => row.runId).filter((id): id is string => Boolean(id)))].sort(),
    pulls: [...new Set(rows.map((row) => row.pull).filter((id): id is number => id !== null))].sort((a, b) => a - b),
  })).sort((a, b) => (a.workItemId ?? "").localeCompare(b.workItemId ?? ""));
  const workAmounts = workRows.reduce((total, row) => addAmounts(total, row), ZERO_AMOUNTS);
  if (workRows.reduce((total, row) => total + row.events, 0) !== captured.events ||
      Object.keys(ZERO_AMOUNTS).some((field) =>
        workAmounts[field as keyof AllocationAmounts] !== captured[field as keyof AllocationAmounts])) {
    throw new Error("Work allocation failed exact reconciliation");
  }

  return { receipts, pullRows, workRows, coverage: { captured, direct, inferred, unallocated, reconciliation } };
}

/**
 * The ledger read is time-bounded, applies the shared terminal privacy gate,
 * and extracts only the four text-free binding fields from payload_json.
 * Raw payloads never leave SQLite or enter the report.
 */
export function collectAllocationEvents(
  db: Database.Database,
  since: string,
): AllocationEvent[] {
  const columns = new Set((db.pragma("table_info(buffered_events)") as Array<{ name: string }>).map((column) => column.name));
  const workField = (path: string) => columns.has("payload_json")
    ? `case when json_valid(payload_json) then json_extract(payload_json, '${path}') else null end`
    : "null";
  const privacyEligible = terminalPrivacyEligibilitySql(db, "buffered_events");
  return db
    .prepare(
      `select id as eventId, session_id as sessionId, observed_at as observedAt,
         input_tokens as inputTokens, output_tokens as outputTokens,
         cache_read_tokens as cacheReadTokens,
         cache_creation_tokens as cacheWriteTokens, cost_usd as costUsd,
         repo_hash as repoHash, branch_hash as branchHash, head_sha as headSha,
         ${workField("$.metadata.workItemId")} as workItemId,
         ${workField("$.metadata.attemptId")} as runId,
         ${workField("$.metadata.workEvidenceRef")} as workEvidenceRef,
         ${workField("$.metadata.workAttributionState")} as workAttributionState
       from buffered_events
       where observed_at >= ?
         and ${privacyEligible}
         and (input_tokens is not null or output_tokens is not null
           or cache_read_tokens is not null or cache_creation_tokens is not null
           or cost_usd is not null)
       order by observed_at asc, id asc`,
    )
    .all(since) as AllocationEvent[];
}
