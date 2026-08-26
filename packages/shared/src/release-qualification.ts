/**
 * Exact-artifact release qualification and canary receipts (issue #171).
 *
 * Pure workflow instrumentation for the manually produced, signed first
 * release: builder handoff → independent adversarial review → integration
 * rerun → owner approval of one exact digest → single rollout. Any change to
 * code (head), configuration, dependencies, schema, or the artifact digest
 * resets approval. Automatic fleet update is NOT represented here and stays
 * disabled; packaging/auto-update/stable-fleet promotion remain gated by the
 * stage-gate issues recorded in GATED_FLEET_STAGE_ISSUES.
 *
 * Canary side: isolated-home read-only compatibility receipts for Studio0 and
 * the authorized MacBook (no install, no service activation), and a Studio 3
 * preflight that stays BLOCKED_MISSING_SSH_MAPPING until an unprivileged
 * key-based account exists.
 *
 * Doctrine enforced structurally: SSH state, machine reachability, quota
 * readings, event volume, and working hours are operational context ONLY.
 * Records here are closed schemas — productivity scores, performance
 * ratings, verdicts, rankings, coaching/compensation/discipline fields are
 * not representable, and findPerformanceEvidenceViolations rejects them in
 * foreign records. Machine identity is pseudonymous by construction (only a
 * SHA-256 key hash is stored). Nothing here touches the filesystem, spawns
 * processes, reaches the network, or grants merge/install/deploy authority;
 * a gate result is decision support, never authority by itself.
 */

import crypto from "node:crypto";

// ---------------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------------

export const RELEASE_QUALIFICATION_SCHEMA = "plimsoll.release-qualification.v1" as const;
export const RELEASE_QUALIFICATION_SCHEMA_VERSION = 1 as const;

const SHA_PATTERN = /^[0-9a-f]{40}$/;
const HEX64_PATTERN = /^[0-9a-f]{64}$/;
const DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/;
const SAFE_ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;
const MAX_LIST_ITEMS = 512;
const MAX_ITEM_LENGTH = 4096;

function isSha(value: unknown): value is string {
  return typeof value === "string" && SHA_PATTERN.test(value);
}

function isDigest(value: unknown): value is string {
  return typeof value === "string" && DIGEST_PATTERN.test(value);
}

function isHex64(value: unknown): value is string {
  return typeof value === "string" && HEX64_PATTERN.test(value);
}

function isBoundedStringList(value: unknown): value is readonly string[] {
  return (
    Array.isArray(value) &&
    value.length <= MAX_LIST_ITEMS &&
    value.every(
      (item) => typeof item === "string" && item.length >= 1 && item.length <= MAX_ITEM_LENGTH,
    )
  );
}

function sortedUnique(values: readonly string[]): string[] {
  return [...new Set(values)].sort();
}

/**
 * Canonical serialization: recursively key-sorted JSON. Deliberately local —
 * this module must not consume capacity-module symbols (capacity doctrine
 * gate, scripts/capacity-dependency-reachability.ts).
 */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
    a.localeCompare(b),
  );
  return `{${entries.map(([key, val]) => `${JSON.stringify(key)}:${canonicalJson(val)}`).join(",")}}`;
}

function digestLinkage(material: string): string {
  return `sha256:${crypto.createHash("sha256").update(material).digest("hex")}`;
}

// ---------------------------------------------------------------------------
// Stage 1 — Builder handoff
// ---------------------------------------------------------------------------

export interface TestReceipt {
  suite: string;
  passed: boolean;
}

export interface BuilderHandoffInput {
  laneId: string;
  builderActor: string;
  baseSha: string;
  headSha: string;
  changedFiles: readonly string[];
  testReceipt: TestReceipt;
  fixtureFingerprint: string;
  unresolvedFindings: readonly string[];
  artifactDigest: string;
  /** Pinned at build time when present; any drift at release time resets approval. */
  configFingerprint?: string | undefined;
  dependencyFingerprint?: string | undefined;
  schemaFingerprint?: string | undefined;
}

export interface BuilderHandoff extends BuilderHandoffInput {
  schemaVersion: number;
  changedFiles: string[];
  unresolvedFindings: string[];
  handoffDigest: string;
}

const HANDOFF_ALLOWED_KEYS: ReadonlySet<string> = new Set([
  "laneId",
  "builderActor",
  "baseSha",
  "headSha",
  "changedFiles",
  "testReceipt",
  "fixtureFingerprint",
  "unresolvedFindings",
  "artifactDigest",
  "configFingerprint",
  "dependencyFingerprint",
  "schemaFingerprint",
]);

/**
 * Validate and freeze the builder handoff. Fails closed on malformed SHAs or
 * digests, out-of-bounds lists, unknown fields, or a test receipt without a
 * suite name. The handoff digest is deterministic over the exact handed-off
 * facts, so any later edit of the record itself is detectable.
 */
export function parseBuilderHandoff(
  raw: unknown,
): { ok: true; value: BuilderHandoff } | { ok: false; issues: string[] } {
  const issues: string[] = [];
  if (typeof raw !== "object" || raw === null) {
    return { ok: false, issues: ["handoff_not_an_object"] };
  }
  const input = raw as Record<string, unknown>;
  for (const key of Object.keys(input)) {
    if (!HANDOFF_ALLOWED_KEYS.has(key)) issues.push(`unknown_field:${key}`);
  }

  const laneId = input.laneId;
  if (typeof laneId !== "string" || !SAFE_ID_PATTERN.test(laneId)) {
    issues.push("missing_or_invalid_field:laneId");
  }
  const builderActor = input.builderActor;
  if (typeof builderActor !== "string" || !SAFE_ID_PATTERN.test(builderActor)) {
    issues.push("missing_or_invalid_field:builderActor");
  }
  if (!isSha(input.baseSha)) issues.push("missing_or_invalid_field:baseSha");
  if (!isSha(input.headSha)) issues.push("missing_or_invalid_field:headSha");
  if (!isBoundedStringList(input.changedFiles)) {
    issues.push("missing_or_invalid_field:changedFiles");
  }

  const testReceiptRaw = input.testReceipt;
  let testReceipt: TestReceipt | undefined;
  if (
    typeof testReceiptRaw !== "object" ||
    testReceiptRaw === null ||
    typeof (testReceiptRaw as Record<string, unknown>).suite !== "string" ||
    !SAFE_ID_PATTERN.test((testReceiptRaw as Record<string, unknown>).suite as string) ||
    typeof (testReceiptRaw as Record<string, unknown>).passed !== "boolean"
  ) {
    issues.push("missing_or_invalid_field:testReceipt");
  } else {
    testReceipt = {
      suite: (testReceiptRaw as Record<string, unknown>).suite as string,
      passed: (testReceiptRaw as Record<string, unknown>).passed as boolean,
    };
  }

  if (!isDigest(input.fixtureFingerprint)) {
    issues.push("missing_or_invalid_field:fixtureFingerprint");
  }
  if (!isBoundedStringList(input.unresolvedFindings)) {
    issues.push("missing_or_invalid_field:unresolvedFindings");
  }
  if (!isDigest(input.artifactDigest)) {
    issues.push("missing_or_invalid_field:artifactDigest");
  }
  for (const field of ["configFingerprint", "dependencyFingerprint", "schemaFingerprint"] as const) {
    const value = input[field];
    if (value !== undefined && !isDigest(value)) {
      issues.push(`invalid_field:${field}`);
    }
  }

  if (issues.length > 0) return { ok: false, issues };

  const core = {
    laneId: laneId as string,
    builderActor: builderActor as string,
    baseSha: input.baseSha as string,
    headSha: input.headSha as string,
    changedFiles: sortedUnique(input.changedFiles as readonly string[]),
    testReceipt: testReceipt as TestReceipt,
    fixtureFingerprint: input.fixtureFingerprint as string,
    unresolvedFindings: sortedUnique(input.unresolvedFindings as readonly string[]),
    artifactDigest: input.artifactDigest as string,
    configFingerprint: input.configFingerprint as string | undefined,
    dependencyFingerprint: input.dependencyFingerprint as string | undefined,
    schemaFingerprint: input.schemaFingerprint as string | undefined,
  };
  // Undefined fingerprints must not participate in the digest material.
  const digestMaterial: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(core)) {
    if (value !== undefined) digestMaterial[key] = value;
  }
  return {
    ok: true,
    value: {
      ...core,
      schemaVersion: RELEASE_QUALIFICATION_SCHEMA_VERSION,
      handoffDigest: digestLinkage(canonicalJson(digestMaterial)),
    },
  };
}

// ---------------------------------------------------------------------------
// Stages 2–4 — review, integration rerun, owner approval
// ---------------------------------------------------------------------------

export type ReviewDisposition = "approve" | "reject";

export interface ReviewApproval {
  reviewerActor: string;
  headSha: string;
  artifactDigest: string;
  verdict: ReviewDisposition;
}

const REVIEW_ALLOWED_KEYS: ReadonlySet<string> = new Set([
  "reviewerActor",
  "headSha",
  "artifactDigest",
  "verdict",
]);

export function parseReviewApproval(
  raw: unknown,
): { ok: true; value: ReviewApproval } | { ok: false; issues: string[] } {
  const issues: string[] = [];
  if (typeof raw !== "object" || raw === null) return { ok: false, issues: ["review_not_an_object"] };
  const input = raw as Record<string, unknown>;
  for (const key of Object.keys(input)) {
    if (!REVIEW_ALLOWED_KEYS.has(key)) issues.push(`unknown_field:${key}`);
  }
  if (typeof input.reviewerActor !== "string" || !SAFE_ID_PATTERN.test(input.reviewerActor)) {
    issues.push("missing_or_invalid_field:reviewerActor");
  }
  if (!isSha(input.headSha)) issues.push("missing_or_invalid_field:headSha");
  if (!isDigest(input.artifactDigest)) issues.push("missing_or_invalid_field:artifactDigest");
  if (
    typeof input.verdict !== "string" ||
    !(["approve", "reject"] as const).includes(input.verdict as ReviewDisposition)
  ) {
    issues.push("missing_or_invalid_field:verdict");
  }
  if (issues.length > 0) return { ok: false, issues };
  return {
    ok: true,
    value: {
      reviewerActor: input.reviewerActor as string,
      headSha: input.headSha as string,
      artifactDigest: input.artifactDigest as string,
      verdict: input.verdict as ReviewDisposition,
    },
  };
}

export interface IntegrationRerun {
  integratorActor: string;
  headSha: string;
  suite: string;
  passed: boolean;
}

const INTEGRATION_ALLOWED_KEYS: ReadonlySet<string> = new Set([
  "integratorActor",
  "headSha",
  "suite",
  "passed",
]);

export function parseIntegrationRerun(
  raw: unknown,
): { ok: true; value: IntegrationRerun } | { ok: false; issues: string[] } {
  const issues: string[] = [];
  if (typeof raw !== "object" || raw === null) {
    return { ok: false, issues: ["integration_rerun_not_an_object"] };
  }
  const input = raw as Record<string, unknown>;
  for (const key of Object.keys(input)) {
    if (!INTEGRATION_ALLOWED_KEYS.has(key)) issues.push(`unknown_field:${key}`);
  }
  if (typeof input.integratorActor !== "string" || !SAFE_ID_PATTERN.test(input.integratorActor)) {
    issues.push("missing_or_invalid_field:integratorActor");
  }
  if (!isSha(input.headSha)) issues.push("missing_or_invalid_field:headSha");
  if (typeof input.suite !== "string" || !SAFE_ID_PATTERN.test(input.suite)) {
    issues.push("missing_or_invalid_field:suite");
  }
  if (typeof input.passed !== "boolean") issues.push("missing_or_invalid_field:passed");
  if (issues.length > 0) return { ok: false, issues };
  return {
    ok: true,
    value: {
      integratorActor: input.integratorActor as string,
      headSha: input.headSha as string,
      suite: input.suite as string,
      passed: input.passed as boolean,
    },
  };
}

export interface OwnerApprovalState {
  ownerActor: string;
  artifactDigest: string;
  /** Exactly one rollout is granted; consumption is irreversible. */
  rolloutsGranted: number;
  rolloutsConsumed: number;
}

// ---------------------------------------------------------------------------
// Release chain — pure transitions with attach-time binding guards
// ---------------------------------------------------------------------------

export interface ReleaseChain {
  handoff: BuilderHandoff;
  review: ReviewApproval | null;
  integration: IntegrationRerun | null;
  ownerApproval: OwnerApprovalState | null;
}

export type AttachResult =
  | { ok: true; chain: ReleaseChain }
  | { ok: false; issue: string };

export function startReleaseChain(handoff: BuilderHandoff): ReleaseChain {
  return Object.freeze({ handoff, review: null, integration: null, ownerApproval: null });
}

/**
 * Attach the independent adversarial reviewer's verdict. Step 2 requires the
 * SAME source head and digest the builder handed off, and the reviewer must
 * be independent of the builder actor. Violations refuse the attachment.
 */
export function attachReview(chain: ReleaseChain, review: ReviewApproval): AttachResult {
  if (review.headSha !== chain.handoff.headSha) {
    return { ok: false, issue: `review_head_mismatch:${review.headSha}` };
  }
  if (review.artifactDigest !== chain.handoff.artifactDigest) {
    return { ok: false, issue: `review_digest_mismatch:${review.artifactDigest}` };
  }
  if (review.reviewerActor === chain.handoff.builderActor) {
    return { ok: false, issue: "reviewer_not_independent" };
  }
  return { ok: true, chain: Object.freeze({ ...chain, review }) };
}

/** Attach the integration lead's rerun of the complete exact-head suite. */
export function attachIntegrationRerun(chain: ReleaseChain, rerun: IntegrationRerun): AttachResult {
  if (rerun.headSha !== chain.handoff.headSha) {
    return { ok: false, issue: `integration_head_mismatch:${rerun.headSha}` };
  }
  return { ok: true, chain: Object.freeze({ ...chain, integration: rerun }) };
}

/**
 * James approves that exact digest for ONE rollout. A digest other than the
 * handed-off artifact is refused — approval cannot be minted for a different
 * artifact than the one qualified.
 */
export function grantOwnerApproval(
  chain: ReleaseChain,
  input: { ownerActor: string; artifactDigest: string },
): AttachResult {
  if (!SAFE_ID_PATTERN.test(input.ownerActor)) {
    return { ok: false, issue: "invalid_owner_actor" };
  }
  if (!isDigest(input.artifactDigest)) {
    return { ok: false, issue: "invalid_owner_approval_digest" };
  }
  if (input.artifactDigest !== chain.handoff.artifactDigest) {
    return { ok: false, issue: `owner_approval_digest_mismatch:${input.artifactDigest}` };
  }
  return {
    ok: true,
    chain: Object.freeze({
      ...chain,
      ownerApproval: {
        ownerActor: input.ownerActor,
        artifactDigest: input.artifactDigest,
        rolloutsGranted: 1,
        rolloutsConsumed: 0,
      },
    }),
  };
}

/**
 * Consume the single owner-approved rollout. A second consumption fails —
 * one digest, one rollout, then approval is spent.
 */
export function consumeOwnerRollout(chain: ReleaseChain): AttachResult {
  const approval = chain.ownerApproval;
  if (!approval) return { ok: false, issue: "owner_rollout_without_approval" };
  if (approval.rolloutsConsumed >= approval.rolloutsGranted) {
    return { ok: false, issue: "owner_rollout_already_used" };
  }
  return {
    ok: true,
    chain: Object.freeze({
      ...chain,
      ownerApproval: { ...approval, rolloutsConsumed: approval.rolloutsConsumed + 1 },
    }),
  };
}

// ---------------------------------------------------------------------------
// Gate evaluation — exact-match bindings, fail-closed on any drift
// ---------------------------------------------------------------------------

export interface CurrentReleaseState {
  currentHeadSha: string;
  currentArtifactDigest: string;
  currentConfigFingerprint?: string | undefined;
  currentDependencyFingerprint?: string | undefined;
  currentSchemaFingerprint?: string | undefined;
}

export interface ReleaseGateDecision {
  allowed: boolean;
  reasons: string[];
}

const FINGERPRINT_FIELDS = [
  "configFingerprint",
  "dependencyFingerprint",
  "schemaFingerprint",
] as const;

type FingerprintField = (typeof FINGERPRINT_FIELDS)[number];

/**
 * Decide whether the qualified artifact may proceed. Every stage must be
 * present, bound to the handoff's exact head/digest, approving/passing, the
 * owner approval unspent, and every pinned fingerprint must match the
 * current state exactly. Any code, configuration, dependency, schema, or
 * digest change produces a distinct reset reason — approval does not carry.
 */
export function evaluateArtifactReleaseGate(
  chain: ReleaseChain,
  current: CurrentReleaseState,
): ReleaseGateDecision {
  const reasons: string[] = [];
  const handoff = chain.handoff;

  if (current.currentHeadSha !== handoff.headSha) {
    reasons.push("head_changed_since_handoff");
  }
  if (current.currentArtifactDigest !== handoff.artifactDigest) {
    reasons.push("artifact_digest_changed_since_handoff");
  }

  for (const field of FINGERPRINT_FIELDS) {
    const pinned = handoff[field];
    const now = current[`current${field.charAt(0).toUpperCase()}${field.slice(1)}` as keyof CurrentReleaseState] as
      | string
      | undefined;
    if (pinned !== undefined && now === undefined) {
      reasons.push(`current_state_missing:${field}`);
    } else if (pinned === undefined && now !== undefined) {
      reasons.push(`unpinned_current_${field}`);
    } else if (pinned !== undefined && now !== undefined && pinned !== now) {
      reasons.push(`${field}_drift_resets_approval`);
    }
  }

  if (!chain.review) {
    reasons.push("review_missing");
  } else {
    if (chain.review.headSha !== handoff.headSha) reasons.push("review_head_mismatch");
    if (chain.review.artifactDigest !== handoff.artifactDigest) {
      reasons.push("review_digest_mismatch");
    }
    if (chain.review.verdict !== "approve") reasons.push("review_verdict_reject");
    if (chain.review.reviewerActor === handoff.builderActor) {
      reasons.push("reviewer_not_independent");
    }
  }

  if (!chain.integration) {
    reasons.push("integration_missing");
  } else {
    if (chain.integration.headSha !== handoff.headSha) reasons.push("integration_head_mismatch");
    if (!chain.integration.passed) reasons.push("integration_suite_failed");
  }

  if (!chain.ownerApproval) {
    reasons.push("owner_approval_missing");
  } else {
    if (chain.ownerApproval.artifactDigest !== handoff.artifactDigest) {
      reasons.push("owner_approval_digest_mismatch");
    }
    if (chain.ownerApproval.rolloutsConsumed >= chain.ownerApproval.rolloutsGranted) {
      reasons.push("owner_rollout_already_used");
    }
  }

  return { allowed: reasons.length === 0, reasons };
}

// ---------------------------------------------------------------------------
// Canary sequence — isolated-home read-only compatibility receipts
// ---------------------------------------------------------------------------

export const CANARY_COMPATIBILITY_RECEIPT_SCHEMA =
  "plimsoll.canary-compatibility-receipt.v1" as const;

/** The four attestations every compatibility receipt must assert literally. */
export const CANARY_ATTESTATIONS = [
  "isolatedHome",
  "readOnly",
  "noInstall",
  "noServiceActivation",
] as const;
export type CanaryAttestation = (typeof CANARY_ATTESTATIONS)[number];

export const CANARY_RESULTS = ["compatible", "incompatible"] as const;
export type CanaryResult = (typeof CANARY_RESULTS)[number];

const BOUNDED_LABEL_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9 ._/:-]{0,95}$/;
const MAX_PROVIDER_VERSIONS = 16;
const HOME_PATH_PATTERN = /(?:\/Users\/[^/\s"]+|\/home\/[^/\s"]+|C:\\Users\\[^\\\s"]+)/;

const CREDENTIAL_VALUE_PATTERNS: ReadonlyArray<{ kind: string; pattern: RegExp }> = [
  { kind: "jwt", pattern: /^eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{5,}$/ },
  { kind: "auth_header_value", pattern: /\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]{16,}/i },
  { kind: "provider_api_key", pattern: /\bsk-[A-Za-z0-9_-]{20,}\b/ },
  { kind: "aws_access_key", pattern: /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/ },
  { kind: "github_token", pattern: /\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b/ },
  { kind: "slack_token", pattern: /\bxox[bparsa]-[A-Za-z0-9-]{10,}\b/ },
];

export interface CompatibilityReceipt {
  schema: typeof CANARY_COMPATIBILITY_RECEIPT_SCHEMA;
  machineRole: string;
  /** SHA-256 of the pseudonymous machine key — the raw key is never stored. */
  machineKeyHash: string;
  osPlatform: string;
  plimsollVersion: string;
  providerVersions: Record<string, string>;
  attestations: Record<CanaryAttestation, true>;
  result: CanaryResult;
}

function scanPrivacyMaterial(value: unknown, findings: string[]): void {
  if (Array.isArray(value)) {
    for (const item of value) scanPrivacyMaterial(item, findings);
    return;
  }
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value as Record<string, unknown>)) {
      scanPrivacyMaterial(child, findings);
    }
    return;
  }
  if (typeof value !== "string") return;
  if (HOME_PATH_PATTERN.test(value)) findings.push("home_path_present");
  for (const { kind, pattern } of CREDENTIAL_VALUE_PATTERNS) {
    if (pattern.test(value)) {
      findings.push(`credential_value_shape:${kind}`);
      return;
    }
  }
}

const COMPATIBILITY_ALLOWED_KEYS: ReadonlySet<string> = new Set([
  "schema",
  "machineRole",
  "machineKeyHash",
  "osPlatform",
  "plimsollVersion",
  "providerVersions",
  "attestations",
  "result",
]);

/**
 * Parse an isolated-home read-only compatibility receipt (Studio0 or the
 * authorized MacBook). Closed vocabulary: unknown fields refuse the receipt.
 * All four attestations must be literal true — a receipt that was not
 * isolated/read-only/no-install/no-activation is not producible in this
 * vocabulary, so it cannot be laundered into one. The machine identity must
 * arrive already pseudonymized (hex key hash); home paths and credential-
 * shaped strings anywhere are rejected whole, redacted.
 */
export function parseCompatibilityReceipt(
  raw: unknown,
): { ok: true; value: CompatibilityReceipt } | { ok: false; issues: string[] } {
  if (typeof raw !== "object" || raw === null) {
    return { ok: false, issues: ["receipt_not_an_object"] };
  }
  const input = raw as Record<string, unknown>;
  const issues: string[] = [];
  for (const key of Object.keys(input)) {
    if (!COMPATIBILITY_ALLOWED_KEYS.has(key)) issues.push(`unknown_field:${key}`);
  }
  const privacyFindings: string[] = [];
  scanPrivacyMaterial(input, privacyFindings);
  if (privacyFindings.length > 0) {
    return { ok: false, issues: [`privacy_material_rejected_redacted:${privacyFindings[0]}`] };
  }

  if (input.schema !== CANARY_COMPATIBILITY_RECEIPT_SCHEMA) {
    issues.push("missing_or_invalid_field:schema");
  }
  if (typeof input.machineRole !== "string" || !SAFE_ID_PATTERN.test(input.machineRole)) {
    issues.push("missing_or_invalid_field:machineRole");
  }
  if (!isHex64(input.machineKeyHash)) {
    issues.push("machine_key_must_be_pseudonymous_sha256_hex");
  }
  if (typeof input.osPlatform !== "string" || !BOUNDED_LABEL_PATTERN.test(input.osPlatform)) {
    issues.push("missing_or_invalid_field:osPlatform");
  }
  if (
    typeof input.plimsollVersion !== "string" ||
    !SAFE_ID_PATTERN.test(input.plimsollVersion)
  ) {
    issues.push("missing_or_invalid_field:plimsollVersion");
  }
  const providerVersions = input.providerVersions;
  if (
    typeof providerVersions !== "object" ||
    providerVersions === null ||
    Array.isArray(providerVersions)
  ) {
    issues.push("missing_or_invalid_field:providerVersions");
  } else {
    const entries = Object.entries(providerVersions as Record<string, unknown>);
    if (
      entries.length > MAX_PROVIDER_VERSIONS ||
      !entries.every(
        ([key, value]) =>
          SAFE_ID_PATTERN.test(key) && typeof value === "string" && SAFE_ID_PATTERN.test(value),
      )
    ) {
      issues.push("invalid_provider_version_entries");
    }
  }
  const attestations = input.attestations;
  if (typeof attestations !== "object" || attestations === null) {
    issues.push("missing_or_invalid_field:attestations");
  } else {
    const record = attestations as Record<string, unknown>;
    for (const attestation of CANARY_ATTESTATIONS) {
      if (record[attestation] !== true) {
        issues.push(`attestation_not_true:${attestation}`);
      }
    }
    if (Object.keys(record).length !== CANARY_ATTESTATIONS.length) {
      issues.push("attestations_closed_vocabulary");
    }
  }
  if (
    typeof input.result !== "string" ||
    !(CANARY_RESULTS as readonly string[]).includes(input.result)
  ) {
    issues.push("missing_or_invalid_field:result");
  }

  if (issues.length > 0) return { ok: false, issues };
  return {
    ok: true,
    value: {
      schema: CANARY_COMPATIBILITY_RECEIPT_SCHEMA,
      machineRole: input.machineRole as string,
      machineKeyHash: input.machineKeyHash as string,
      osPlatform: input.osPlatform as string,
      plimsollVersion: input.plimsollVersion as string,
      providerVersions: providerVersions as Record<string, string>,
      attestations: Object.fromEntries(
        CANARY_ATTESTATIONS.map((attestation) => [attestation, true]),
      ) as Record<CanaryAttestation, true>,
      result: input.result as CanaryResult,
    },
  };
}

// ---------------------------------------------------------------------------
// Canary sequence — Studio 3 preflight (blocked until SSH mapping exists)
// ---------------------------------------------------------------------------

export const STUDIO3_PREFLIGHT_SCHEMA = "plimsoll.studio3-preflight.v1" as const;
export const STUDIO3_SSH_MAPPING_STATES = ["present", "missing"] as const;
export type Studio3SshMapping = (typeof STUDIO3_SSH_MAPPING_STATES)[number];

export const STUDIO3_PREFLIGHT_STATUSES = [
  "PREFLIGHT_RECORDED",
  "BLOCKED_MISSING_SSH_MAPPING",
] as const;
export type Studio3PreflightStatus = (typeof STUDIO3_PREFLIGHT_STATUSES)[number];

export interface Studio3Preflight {
  schema: typeof STUDIO3_PREFLIGHT_SCHEMA;
  machineKeyHash: string;
  osPlatform: string;
  plimsollVersion: string;
  providerVersions: Record<string, string>;
  sshMapping: Studio3SshMapping;
  status: Studio3PreflightStatus;
}

export function studio3PreflightStatus(sshMapping: Studio3SshMapping): Studio3PreflightStatus {
  return sshMapping === "missing" ? "BLOCKED_MISSING_SSH_MAPPING" : "PREFLIGHT_RECORDED";
}

const STUDIO3_ALLOWED_KEYS: ReadonlySet<string> = new Set([
  "schema",
  "machineKeyHash",
  "osPlatform",
  "plimsollVersion",
  "providerVersions",
  "sshMapping",
]);

/**
 * Parse a Studio 3 preflight record: pseudonymous machine key hash, OS,
 * provider versions, Plimsoll version, and the SSH mapping state. Closed
 * vocabulary. The status is DERIVED from sshMapping — a missing mapping is
 * BLOCKED, never silently ready. Credential-shaped values are rejected whole
 * and redacted.
 */
export function parseStudio3Preflight(
  raw: unknown,
): { ok: true; value: Studio3Preflight } | { ok: false; issues: string[] } {
  if (typeof raw !== "object" || raw === null) {
    return { ok: false, issues: ["preflight_not_an_object"] };
  }
  const input = raw as Record<string, unknown>;
  const issues: string[] = [];
  for (const key of Object.keys(input)) {
    if (!STUDIO3_ALLOWED_KEYS.has(key)) issues.push(`unknown_field:${key}`);
  }
  const privacyFindings: string[] = [];
  scanPrivacyMaterial(input, privacyFindings);
  if (privacyFindings.length > 0) {
    return { ok: false, issues: [`privacy_material_rejected_redacted:${privacyFindings[0]}`] };
  }

  if (input.schema !== STUDIO3_PREFLIGHT_SCHEMA) issues.push("missing_or_invalid_field:schema");
  if (!isHex64(input.machineKeyHash)) {
    issues.push("machine_key_must_be_pseudonymous_sha256_hex");
  }
  if (typeof input.osPlatform !== "string" || !BOUNDED_LABEL_PATTERN.test(input.osPlatform)) {
    issues.push("missing_or_invalid_field:osPlatform");
  }
  if (typeof input.plimsollVersion !== "string" || !SAFE_ID_PATTERN.test(input.plimsollVersion)) {
    issues.push("missing_or_invalid_field:plimsollVersion");
  }
  const providerVersions = input.providerVersions;
  if (
    typeof providerVersions !== "object" ||
    providerVersions === null ||
    Array.isArray(providerVersions)
  ) {
    issues.push("missing_or_invalid_field:providerVersions");
  } else {
    const entries = Object.entries(providerVersions as Record<string, unknown>);
    if (
      entries.length > MAX_PROVIDER_VERSIONS ||
      !entries.every(
        ([key, value]) =>
          SAFE_ID_PATTERN.test(key) && typeof value === "string" && SAFE_ID_PATTERN.test(value),
      )
    ) {
      issues.push("invalid_provider_version_entries");
    }
  }
  if (
    typeof input.sshMapping !== "string" ||
    !(STUDIO3_SSH_MAPPING_STATES as readonly string[]).includes(input.sshMapping)
  ) {
    issues.push("missing_or_invalid_field:sshMapping");
  }

  if (issues.length > 0) return { ok: false, issues };
  const value: Studio3Preflight = {
    schema: STUDIO3_PREFLIGHT_SCHEMA,
    machineKeyHash: input.machineKeyHash as string,
    osPlatform: input.osPlatform as string,
    plimsollVersion: input.plimsollVersion as string,
    providerVersions: providerVersions as Record<string, string>,
    sshMapping: input.sshMapping as Studio3SshMapping,
    status: studio3PreflightStatus(input.sshMapping as Studio3SshMapping),
  };
  return { ok: true, value };
}

/**
 * Whether the canary may progress past the Studio 3 preflight. While the SSH
 * mapping is missing the answer is no — recorded as blocked_missing_ssh_mapping,
 * which names the missing infrastructure, never a person or a performance fact.
 */
export function canaryProgressAllowed(preflight: Studio3Preflight): ReleaseGateDecision {
  return preflight.status === "BLOCKED_MISSING_SSH_MAPPING"
    ? { allowed: false, reasons: ["blocked_missing_ssh_mapping"] }
    : { allowed: true, reasons: [] };
}

// ---------------------------------------------------------------------------
// Scope gate — canary scopes vs stage-gated scopes
// ---------------------------------------------------------------------------

export const CANARY_SCOPES = [
  "isolated_home_compatibility_receipt",
  "studio3_preflight_record",
] as const;
export type CanaryScope = (typeof CANARY_SCOPES)[number];

/**
 * Stage-gate issue set recorded verbatim from #171: packaging, automatic
 * update, and stable-fleet promotion remain gated by these issues "as each
 * stage requires". The per-stage mapping is deliberately NOT invented here.
 */
export const GATED_FLEET_STAGE_ISSUES = [
  "#103",
  "#105",
  "#128",
  "#131",
  "#133",
  "#135",
  "#148",
  "#154",
  "#155",
  "#158",
  "#159",
  "#162",
] as const;

export type ScopeClaimDecision =
  | { allowed: true; scope: CanaryScope }
  | { allowed: false; reason: "scope_gated_by_stage_issues"; gatedBy: readonly string[] };

/** Only the two canary scopes are open; everything else stays behind the gates. */
export function evaluateScopeClaim(scope: string): ScopeClaimDecision {
  if ((CANARY_SCOPES as readonly string[]).includes(scope)) {
    return { allowed: true, scope: scope as CanaryScope };
  }
  return { allowed: false, reason: "scope_gated_by_stage_issues", gatedBy: GATED_FLEET_STAGE_ISSUES };
}

// ---------------------------------------------------------------------------
// Performance-evidence doctrine guard
// ---------------------------------------------------------------------------

/**
 * Key concepts that would turn operational context (SSH state, reachability,
 * quota readings, event volume, working hours) into employee-performance
 * evidence. Matched against normalized object KEYS at every depth.
 */
const FORBIDDEN_PERFORMANCE_KEY_CONCEPTS = [
  "productivity",
  "performancerating",
  "employeerating",
  "employeescore",
  "performanceverdict",
  "verdict",
  "ranking",
  "coach",
  "disciplin",
  "compensat",
  "intervent",
  "workinghoursverdict",
] as const;

export interface PerformanceEvidenceViolation {
  reason: string;
  path: string;
}

/**
 * Scan a record (any shape) for performance-evidence fields. Our own closed
 * schemas make such fields unrepresentable; this scanner covers records that
 * arrive from elsewhere before anything stores or forwards them.
 */
export function findPerformanceEvidenceViolations(record: unknown): PerformanceEvidenceViolation[] {
  const violations: PerformanceEvidenceViolation[] = [];
  const visit = (value: unknown, path: string): void => {
    if (Array.isArray(value)) {
      value.forEach((item, index) => visit(item, `${path}[${index}]`));
      return;
    }
    if (value === null || typeof value !== "object") return;
    for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
      const normalized = key.replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
      const matched = FORBIDDEN_PERFORMANCE_KEY_CONCEPTS.find((concept) =>
        normalized.includes(concept),
      );
      const qualified = path.length > 0 ? `${path}.${key}` : key;
      if (matched) violations.push({ reason: `performance_evidence_field:${matched}`, path: qualified });
      visit(child, qualified);
    }
  };
  visit(record, "");
  return violations;
}

/** Convenience gate: throws naming the first violation, path included. */
export function assertNoPerformanceEvidence(record: unknown): void {
  const violations = findPerformanceEvidenceViolations(record);
  if (violations.length > 0) {
    throw new Error(
      `performance_evidence_rejected:${violations[0]!.reason}:${violations[0]!.path}`,
    );
  }
}
