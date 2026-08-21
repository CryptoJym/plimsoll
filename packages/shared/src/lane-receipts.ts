/**
 * Deterministic build/audit lane workflow instrumentation (issue #176).
 *
 * Read-only workflow controls: liveness preflight, bounded phased fan-out,
 * worktree census, deterministic handoff receipts, and immutable attempt
 * history. These functions never mutate inputs, touch the filesystem, spawn
 * processes, or grant merge/install/deploy authority. Receipts are
 * privacy-scoped: prompts, transcripts, credentials, environment values,
 * provider data, home paths, and personal productivity judgments are
 * structurally excluded and mechanically rejected.
 */

import crypto from "node:crypto";

export const LIVENESS_STATES = ["ACTIVE", "COMPLETE", "BLOCKED", "UNKNOWN"] as const;
export type LaneLiveness = (typeof LIVENESS_STATES)[number];

export const OWNER_ROLES = ["builder", "reviewer", "integrator", "release"] as const;
export type OwnerRole = (typeof OWNER_ROLES)[number];

export const CLEAN_STATES = ["CLEAN", "DIRTY", "UNKNOWN"] as const;
export type CleanState = (typeof CLEAN_STATES)[number];

export const RECEIPT_SCHEMA_VERSION = 1;
export const STALE_LIVENESS_BUDGET_MS = 30 * 60 * 1000;
export const HANDOFF_STALENESS_BUDGET_MS = 24 * 60 * 60 * 1000;
const SHA_PATTERN = /^[0-9a-f]{40}$/;
const SAFE_ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;
const MAX_LIST_ITEMS = 512;
const MAX_ITEM_LENGTH = 4096;

function isBoundedStringList(value: unknown): value is readonly string[] {
  return (
    Array.isArray(value) &&
    value.length <= MAX_LIST_ITEMS &&
    value.every(
      (item) => typeof item === "string" && item.length >= 1 && item.length <= MAX_ITEM_LENGTH,
    )
  );
}

function isSha(value: unknown): value is string {
  return typeof value === "string" && SHA_PATTERN.test(value);
}

/**
 * Parse a raw liveness preflight record. Fails closed on missing fields,
 * out-of-vocabulary enums, malformed SHAs, or non-finite timestamps.
 */
export function parsePreflight(raw: unknown):
  | { ok: true; value: PreflightRecord }
  | { ok: false; issues: string[] } {
  const issues: string[] = [];
  if (typeof raw !== "object" || raw === null) {
    return { ok: false, issues: ["preflight_not_an_object"] };
  }
  const record = raw as Record<string, unknown>;

  const laneId = record.laneId;
  if (typeof laneId !== "string" || !SAFE_ID_PATTERN.test(laneId)) {
    issues.push("missing_or_invalid_field:laneId");
  }
  const ownerRole = record.ownerRole;
  if (typeof ownerRole !== "string" || !(OWNER_ROLES as readonly string[]).includes(ownerRole)) {
    issues.push("missing_or_invalid_field:ownerRole");
  }
  const branch = record.branch;
  if (typeof branch !== "string" || branch.length < 1 || branch.length > 256 || /\s/.test(branch)) {
    issues.push("missing_or_invalid_field:branch");
  }
  const baseSha = record.baseSha;
  if (!isSha(baseSha)) issues.push("missing_or_invalid_field:baseSha");
  const headSha = record.headSha;
  if (!isSha(headSha)) issues.push("missing_or_invalid_field:headSha");
  const cleanState = record.cleanState;
  if (
    typeof cleanState !== "string" ||
    !(CLEAN_STATES as readonly string[]).includes(cleanState)
  ) {
    issues.push("missing_or_invalid_field:cleanState");
  }
  const lastActivityAtMs = record.lastActivityAtMs;
  if (
    typeof lastActivityAtMs !== "number" ||
    !Number.isFinite(lastActivityAtMs) ||
    lastActivityAtMs < 0
  ) {
    issues.push("missing_or_invalid_field:lastActivityAtMs");
  }
  const liveness = record.liveness;
  if (typeof liveness !== "string" || !(LIVENESS_STATES as readonly string[]).includes(liveness)) {
    issues.push("missing_or_invalid_field:liveness");
  }

  if (issues.length > 0) return { ok: false, issues };

  const blockedReason =
    typeof record.blockedReason === "string" && record.blockedReason.length <= MAX_ITEM_LENGTH
      ? record.blockedReason
      : undefined;

  return {
    ok: true,
    value: {
      laneId: laneId as string,
      ownerRole: ownerRole as OwnerRole,
      branch: branch as string,
      baseSha: baseSha as string,
      headSha: headSha as string,
      cleanState: cleanState as CleanState,
      lastActivityAtMs: lastActivityAtMs as number,
      liveness: liveness as LaneLiveness,
      blockedReason,
    },
  };
}

export interface PreflightRecord {
  laneId: string;
  ownerRole: OwnerRole;
  branch: string;
  baseSha: string;
  headSha: string;
  cleanState: CleanState;
  lastActivityAtMs: number;
  liveness: LaneLiveness;
  blockedReason?: string | undefined;
}

/** Detect self-contradictory preflight states. Pure observation; never mutates. */
export function preflightContradictions(record: PreflightRecord): string[] {
  const contradictions: string[] = [];
  if (record.liveness === "COMPLETE" && record.cleanState === "DIRTY") {
    contradictions.push("complete_but_dirty");
  }
  if (record.liveness === "BLOCKED" && !record.blockedReason) {
    contradictions.push("blocked_without_reason");
  }
  if (record.liveness === "ACTIVE" && record.headSha === record.baseSha && false) {
    contradictions.push("unreachable");
  }
  return contradictions;
}

/**
 * Literal staleness check: an ACTIVE lane whose last activity is older than
 * the budget may not be trusted as live. Never changes state — callers only.
 */
export function isStaleLiveness(
  record: PreflightRecord,
  nowMs: number,
  budgetMs: number = STALE_LIVENESS_BUDGET_MS,
): boolean {
  return record.liveness === "ACTIVE" && nowMs - record.lastActivityAtMs > budgetMs;
}

// ---------------------------------------------------------------------------
// Worktree census
// ---------------------------------------------------------------------------

export interface WorktreeCensusEntry {
  name: string;
  laneId?: string | undefined;
  owner?: string | undefined;
  branch?: string | undefined;
  headSha?: string | undefined;
  baseSha?: string | undefined;
  clean?: boolean | undefined;
  handoffHeadSha?: string | undefined;
  handoffUpdatedAtMs?: number | undefined;
}

export interface CensusFinding {
  kind:
    | "duplicate_head"
    | "dirty_unowned"
    | "duplicate_lane_ownership"
    | "base_drift"
    | "missing_worktree"
    | "stale_handoff";
  subject: string;
  detail: string;
}

export interface CensusOptions {
  expectedLanes?: readonly string[];
  canonicalBaseSha?: string | undefined;
  nowMs?: number | undefined;
  handoffStalenessBudgetMs?: number | undefined;
}

/**
 * Read-only census over worktree snapshots. Detects duplicate heads, dirty
 * lanes without owners, duplicate lane ownership, base drift against a
 * canonical base, missing worktrees for expected lanes, and stale handoffs.
 * Returns findings only; never repairs, prunes, or stops anything.
 */
export function runWorktreeCensus(
  entries: readonly WorktreeCensusEntry[],
  options: CensusOptions = {},
): CensusFinding[] {
  const findings: CensusFinding[] = [];

  const headsBySha = new Map<string, string[]>();
  for (const entry of entries) {
    if (typeof entry.headSha !== "string" || !isSha(entry.headSha)) continue;
    const bucket = headsBySha.get(entry.headSha);
    if (bucket) bucket.push(entry.name);
    else headsBySha.set(entry.headSha, [entry.name]);
  }
  for (const [sha, names] of [...headsBySha.entries()].sort(([a], [b]) => (a < b ? -1 : 1))) {
    if (names.length > 1) {
      findings.push({
        kind: "duplicate_head",
        subject: `${sha.slice(0, 12)}*`,
        detail: `heads_shared_by:${names.slice().sort().join(",")}`,
      });
    }
  }

  const lanesByLaneId = new Map<string, string[]>();
  for (const entry of entries) {
    if (typeof entry.laneId !== "string" || entry.laneId.length === 0) continue;
    const bucket = lanesByLaneId.get(entry.laneId);
    if (bucket) bucket.push(entry.name);
    else lanesByLaneId.set(entry.laneId, [entry.name]);
  }
  for (const [laneId, names] of [...lanesByLaneId.entries()].sort(([a], [b]) => (a < b ? -1 : 1))) {
    if (names.length > 1) {
      findings.push({
        kind: "duplicate_lane_ownership",
        subject: laneId,
        detail: `claimed_by:${names.slice().sort().join(",")}`,
      });
    }
  }

  for (const entry of entries) {
    if (entry.clean === false && !entry.owner) {
      findings.push({
        kind: "dirty_unowned",
        subject: entry.name,
        detail: "dirty_worktree_without_owner",
      });
    }
    if (
      options.canonicalBaseSha &&
      typeof entry.baseSha === "string" &&
      entry.baseSha !== options.canonicalBaseSha
    ) {
      findings.push({
        kind: "base_drift",
        subject: entry.name,
        detail: "base_differs_from_canonical",
      });
    }
    if (
      typeof entry.handoffHeadSha === "string" &&
      typeof entry.headSha === "string" &&
      entry.handoffHeadSha !== entry.headSha
    ) {
      findings.push({
        kind: "stale_handoff",
        subject: entry.name,
        detail: "handoff_head_differs_from_worktree_head",
      });
    } else if (
      typeof entry.handoffUpdatedAtMs === "number" &&
      Number.isFinite(entry.handoffUpdatedAtMs) &&
      typeof options.nowMs === "number" &&
      options.nowMs - entry.handoffUpdatedAtMs >
        (options.handoffStalenessBudgetMs ?? HANDOFF_STALENESS_BUDGET_MS)
    ) {
      findings.push({
        kind: "stale_handoff",
        subject: entry.name,
        detail: "handoff_older_than_budget",
      });
    }
  }

  const coveredLanes = new Set(lanesByLaneId.keys());
  for (const expected of options.expectedLanes ?? []) {
    if (!coveredLanes.has(expected)) {
      findings.push({ kind: "missing_worktree", subject: expected, detail: "no_entry_for_lane" });
    }
  }

  return findings.sort((a, b) =>
    a.kind === b.kind ? (a.subject < b.subject ? -1 : 1) : a.kind < b.kind ? -1 : 1,
  );
}

// ---------------------------------------------------------------------------
// Phased fan-out declaration
// ---------------------------------------------------------------------------

export const PHASE_KINDS = ["build", "review", "integration", "release-gate"] as const;
export type PhaseKind = (typeof PHASE_KINDS)[number];
export const GATED_PHASE_KINDS: readonly PhaseKind[] = ["review", "integration", "release-gate"];

export interface PhaseDeclaration {
  kind: PhaseKind;
  lanes: readonly string[];
}

export interface FanOutDeclaration {
  laneBudget: number;
  phases: readonly PhaseDeclaration[];
}

/**
 * Declare the lane budget and phases before fan-out. Builders may overlap
 * across build phases; review, integration, and release-gate phases must each
 * appear exactly once, own exactly one distinct lane, and share no lane with
 * any other phase. Total declared lanes must fit the budget. Fails closed on
 * violations without starting anything.
 */
export function declareFanOut(raw: unknown):
  | { ok: true; value: FanOutDeclaration }
  | { ok: false; issues: string[] } {
  const issues: string[] = [];
  if (typeof raw !== "object" || raw === null) {
    return { ok: false, issues: ["declaration_not_an_object"] };
  }
  const candidate = raw as { laneBudget?: unknown; phases?: unknown };
  const laneBudget = candidate.laneBudget;
  if (typeof laneBudget !== "number" || !Number.isInteger(laneBudget) || laneBudget < 1) {
    issues.push("invalid_field:laneBudget");
  }
  if (!Array.isArray(candidate.phases)) {
    return { ok: false, issues: [...issues, "invalid_field:phases"] };
  }
  const phases: PhaseDeclaration[] = [];
  candidate.phases.forEach((phaseRaw, index) => {
    if (typeof phaseRaw !== "object" || phaseRaw === null) {
      issues.push(`invalid_phase:${index}`);
      return;
    }
    const phase = phaseRaw as { kind?: unknown; lanes?: unknown };
    if (
      typeof phase.kind !== "string" ||
      !(PHASE_KINDS as readonly string[]).includes(phase.kind)
    ) {
      issues.push(`invalid_phase_kind:${index}`);
      return;
    }
    if (
      !Array.isArray(phase.lanes) ||
      phase.lanes.length === 0 ||
      !phase.lanes.every(
        (lane) => typeof lane === "string" && lane.length >= 1 && lane.length <= 128,
      )
    ) {
      issues.push(`invalid_phase_lanes:${index}:${phase.kind ?? "?"}`);
      return;
    }
    phases.push({ kind: phase.kind as PhaseKind, lanes: phase.lanes as readonly string[] });
  });

  for (const gated of GATED_PHASE_KINDS) {
    const matching = phases.filter((phase) => phase.kind === gated);
    if (matching.length !== 1) {
      issues.push(`phase_count_must_be_exactly_one:${gated}:${matching.length}`);
      continue;
    }
    const lanes = matching[0]!.lanes;
    if (lanes.length !== 1) {
      issues.push(`gated_phase_requires_single_lane:${gated}`);
    }
    for (const other of phases) {
      if (other === matching[0]) continue;
      const overlap = lanes.filter((lane) => other.lanes.includes(lane));
      if (overlap.length > 0) {
        issues.push(`lane_shared_across_phases:${gated}:${other.kind}:${overlap.join(",")}`);
      }
    }
  }

  if (typeof laneBudget === "number" && Number.isInteger(laneBudget)) {
    const allLanes = new Set<string>();
    for (const phase of phases) for (const lane of phase.lanes) allLanes.add(lane);
    if (allLanes.size > laneBudget) {
      issues.push(`declared_lanes_exceed_budget:${allLanes.size}>${laneBudget}`);
    }
  }

  if (issues.length > 0) return { ok: false, issues };
  return {
    ok: true,
    value: Object.freeze({
      laneBudget: laneBudget as number,
      phases: Object.freeze(phases.map((phase) => Object.freeze({ ...phase }))),
    }),
  };
}

// ---------------------------------------------------------------------------
// Deterministic ops receipts and handoffs
// ---------------------------------------------------------------------------

export interface OpsReceiptInput {
  laneId: string;
  attemptId: string;
  branch: string;
  baseSha: string;
  headSha: string;
  changedFiles: readonly string[];
  exactTests: readonly string[];
  failures: readonly string[];
  blockers: readonly string[];
  resumeCommand: string;
}

export interface OpsReceipt {
  schemaVersion: number;
  laneId: string;
  attemptId: string;
  branch: string;
  baseSha: string;
  headSha: string;
  changedFiles: string[];
  exactTests: string[];
  failures: string[];
  blockers: string[];
  resumeCommand: string;
  contentHash: string;
}

function sortedUnique(values: readonly string[]): string[] {
  return [...new Set(values)].sort();
}

/**
 * Build a receipt deterministically: fixed key order, sorted lists, no
 * timestamps, no environment-derived values. The same input always yields
 * byte-identical JSON and the same content hash.
 */
export function buildOpsReceipt(input: OpsReceiptInput): OpsReceipt {
  const core = {
    schemaVersion: RECEIPT_SCHEMA_VERSION,
    laneId: input.laneId,
    attemptId: input.attemptId,
    branch: input.branch,
    baseSha: input.baseSha,
    headSha: input.headSha,
    changedFiles: sortedUnique(input.changedFiles),
    exactTests: sortedUnique(input.exactTests),
    failures: sortedUnique(input.failures),
    blockers: sortedUnique(input.blockers),
    resumeCommand: input.resumeCommand,
  };
  const canonical = JSON.stringify(core);
  return { ...core, contentHash: crypto.createHash("sha256").update(canonical).digest("hex") };
}

/** Canonical serialization: identical receipts serialize to identical bytes. */
export function serializeOpsReceipt(receipt: OpsReceipt): string {
  const ordered = {
    schemaVersion: receipt.schemaVersion,
    laneId: receipt.laneId,
    attemptId: receipt.attemptId,
    branch: receipt.branch,
    baseSha: receipt.baseSha,
    headSha: receipt.headSha,
    changedFiles: receipt.changedFiles,
    exactTests: receipt.exactTests,
    failures: receipt.failures,
    blockers: receipt.blockers,
    resumeCommand: receipt.resumeCommand,
    contentHash: receipt.contentHash,
  };
  return JSON.stringify(ordered);
}

function renderList(lines: string[], heading: string, values: readonly string[]) {
  lines.push(`## ${heading}`, "");
  if (values.length === 0) {
    lines.push("(none)");
  } else {
    for (const value of values) lines.push(`- ${value}`);
  }
  lines.push("");
}

/** Deterministic markdown handoff derived only from the receipt fields. */
export function renderHandoffMarkdown(receipt: OpsReceipt): string {
  const lines: string[] = [
    `# HANDOFF ${receipt.laneId}`,
    "",
    `attempt: ${receipt.attemptId}`,
    `branch: ${receipt.branch}`,
    `base: ${receipt.baseSha}`,
    `head: ${receipt.headSha}`,
    "",
  ];
  renderList(lines, "Changed files", receipt.changedFiles);
  renderList(lines, "Exact tests", receipt.exactTests);
  renderList(lines, "Failures", receipt.failures);
  renderList(lines, "Blockers", receipt.blockers);
  lines.push("## Resume", "", "```sh", receipt.resumeCommand, "```", "");
  lines.push(`receipt-content-hash: ${receipt.contentHash}`, "");
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Privacy guard
// ---------------------------------------------------------------------------

const SECRET_PATTERNS: ReadonlyArray<{ label: string; pattern: RegExp }> = [
  { label: "api_key_assignment", pattern: /\b(api[_-]?key|secret|password|token)\b\s*[:=]\s*\S+/i },
  { label: "openai_style_key", pattern: /\bsk-[A-Za-z0-9_-]{16,}\b/ },
  { label: "github_token", pattern: /\b(ghp_|gho_|github_pat_)[A-Za-z0-9_]{10,}\b/ },
  { label: "aws_access_key", pattern: /\bAKIA[0-9A-Z]{12,}\b/ },
  { label: "slack_token", pattern: /\bxox[abprs]-[A-Za-z0-9-]{8,}\b/ },
  { label: "private_key_block", pattern: /-----BEGIN [A-Z ]*PRIVATE KEY-----/ },
  { label: "bearer_token", pattern: /\bBearer\s+[A-Za-z0-9._-]{20,}\b/ },
];

const FORBIDDEN_FIELD_CONCEPTS = [
  "prompt",
  "transcript",
  "message",
  "output",
  "command_output",
  "environment",
  "env_value",
  "credential",
  "provider_payload",
  "productivity_score",
  "performance_rating",
  "personal_judgment",
];

const HOME_PATH_PATTERN = /(?:\/Users\/[^/\s"]+|\/home\/[^/\s"]+|C:\\Users\\[^\\\s"]+)/;

const ALLOWED_RECEIPT_FIELDS: ReadonlySet<string> = new Set([
  "schemaVersion",
  "laneId",
  "attemptId",
  "branch",
  "baseSha",
  "headSha",
  "changedFiles",
  "exactTests",
  "failures",
  "blockers",
  "resumeCommand",
  "contentHash",
]);

export interface PrivacyViolation {
  reason: string;
  excerptLabel?: string | undefined;
}

function scanStringForViolations(value: string): PrivacyViolation[] {
  const violations: PrivacyViolation[] = [];
  for (const secret of SECRET_PATTERNS) {
    if (secret.pattern.test(value)) violations.push({ reason: `secret_like_string:${secret.label}` });
  }
  if (HOME_PATH_PATTERN.test(value)) violations.push({ reason: "home_path_present" });
  return violations;
}

/**
 * Reject receipts that carry forbidden top-level fields or secret-like /
 * home-path strings anywhere in their serialized form. Structural allowlist
 * first, then pattern scan; both must pass.
 */
export function findReceiptPrivacyViolations(receipt: unknown): PrivacyViolation[] {
  const violations: PrivacyViolation[] = [];
  if (typeof receipt !== "object" || receipt === null) {
    return [{ reason: "receipt_not_an_object" }];
  }
  const record = receipt as Record<string, unknown>;
  for (const key of Object.keys(record)) {
    if (!ALLOWED_RECEIPT_FIELDS.has(key)) violations.push({ reason: `forbidden_field:${key}` });
    const lower = key.toLowerCase();
    if (FORBIDDEN_FIELD_CONCEPTS.some((concept) => lower.includes(concept))) {
      violations.push({ reason: `forbidden_field_concept:${key}` });
    }
  }
  const serialized = JSON.stringify(record);
  violations.push(...scanStringForViolations(serialized));
  return violations;
}

/** Convenience gate used by proofs and integrators: empty array means clean. */
export function assertReceiptPrivacy(receipt: unknown): void {
  const violations = findReceiptPrivacyViolations(receipt);
  if (violations.length > 0) {
    throw new Error(`receipt_privacy_violation:${violations.map((v) => v.reason).join(",")}`);
  }
}

// ---------------------------------------------------------------------------
// Immutable attempt history
// ---------------------------------------------------------------------------

export interface AttemptObservation {
  result: "pass" | "fail";
  sourceHead: string;
  failureSummary?: string | undefined;
}

export interface AttemptEntry {
  attemptId: string;
  sourceHead: string;
  observations: AttemptObservation[];
}

export type AttemptHistory = readonly AttemptEntry[];

/**
 * Append an observation to attempt history. Retries reuse the original
 * attempt ID and source head; prior observations are preserved verbatim so a
 * later pass can never overwrite an earlier failure. Mutating or removing
 * prior entries fails closed.
 */
export function appendAttemptObservation(
  history: AttemptHistory,
  next: { attemptId: string; sourceHead: string; result: "pass" | "fail"; failureSummary?: string },
): { ok: true; history: AttemptHistory } | { ok: false; issue: string } {
  const existingIndex = history.findIndex((entry) => entry.attemptId === next.attemptId);
  if (existingIndex === -1) {
    const entry: AttemptEntry = {
      attemptId: next.attemptId,
      sourceHead: next.sourceHead,
      observations: [{ result: next.result, sourceHead: next.sourceHead, failureSummary: next.failureSummary }],
    };
    return { ok: true, history: [...history, Object.freeze({ ...entry, observations: [...entry.observations] })] };
  }
  const existing = history[existingIndex]!;
  if (existing.sourceHead !== next.sourceHead) {
    return { ok: false, issue: `source_head_changed_for_attempt:${next.attemptId}` };
  }
  const updated = [...history];
  updated[existingIndex] = Object.freeze({
    ...existing,
    observations: [
      ...existing.observations,
      { result: next.result, sourceHead: next.sourceHead, failureSummary: next.failureSummary },
    ],
  });
  return { ok: true, history: updated };
}

/**
 * Tamper detector: compares two history snapshots and reports whether any
 * prior observation was removed, altered, or re-ordered into erasure.
 */
export function detectRetryOverwrite(
  before: AttemptHistory,
  after: AttemptHistory,
): { tampered: boolean; reasons: string[] } {
  const reasons: string[] = [];
  for (const prior of before) {
    const later = after.find((entry) => entry.attemptId === prior.attemptId);
    if (!later) {
      reasons.push(`attempt_removed:${prior.attemptId}`);
      continue;
    }
    if (later.sourceHead !== prior.sourceHead) {
      reasons.push(`attempt_source_head_rewritten:${prior.attemptId}`);
    }
    for (let index = 0; index < prior.observations.length; index += 1) {
      const priorObs = prior.observations[index];
      const laterObs = later.observations[index];
      if (!laterObs) {
        reasons.push(`observation_erased:${prior.attemptId}:${index}`);
        continue;
      }
      if (
        priorObs.result !== laterObs.result ||
        priorObs.sourceHead !== laterObs.sourceHead ||
        priorObs.failureSummary !== laterObs.failureSummary
      ) {
        reasons.push(`observation_rewritten:${prior.attemptId}:${index}`);
      }
    }
  }
  return { tampered: reasons.length > 0, reasons };
}

// ---------------------------------------------------------------------------
// Integration gate
// ---------------------------------------------------------------------------

export type GateName = "typescript-exact-head" | "privacy" | "tamper" | "adversarial-review";

export interface GateEvidence {
  gate: GateName;
  passed: boolean;
  headSha: string;
  reviewer: string;
}

export interface IntegrationDecision {
  allowed: boolean;
  reasons: string[];
}

const REQUIRED_GATES: readonly GateName[] = [
  "typescript-exact-head",
  "privacy",
  "tamper",
  "adversarial-review",
];

/**
 * Decide whether a lane may integrate: all four gates must have passed at the
 * exact head being integrated, and the adversarial reviewer must be
 * independent of the builder. Read-only decision support; grants no authority
 * by itself.
 */
export function evaluateIntegrationGate(
  evidence: readonly GateEvidence[],
  context: { headSha: string; builderOwner: string },
): IntegrationDecision {
  const reasons: string[] = [];
  for (const gate of REQUIRED_GATES) {
    const match = evidence.filter((item) => item.gate === gate);
    const passing = match.filter(
      (item) => item.passed && item.headSha === context.headSha,
    );
    if (match.length === 0) reasons.push(`gate_missing_evidence:${gate}`);
    else if (passing.length === 0) reasons.push(`gate_not_passed_at_head:${gate}`);
  }
  const reviewers = new Set(
    evidence.filter((item) => item.gate === "adversarial-review").map((item) => item.reviewer),
  );
  if (reviewers.size > 0 && reviewers.has(context.builderOwner)) {
    reasons.push("adversarial_reviewer_matches_builder_owner");
  }
  return { allowed: reasons.length === 0, reasons };
}
