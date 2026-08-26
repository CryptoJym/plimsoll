/**
 * Fleet operations: desired state and sanitized device receipts (issue #154).
 *
 * Pure, deterministic coordination data for managing Plimsoll across
 * registered Macs. Everything here is data-in/data-out: no filesystem,
 * process, service-manager, network, or credential access. Hosted fleet
 * state carries signed desired policy and sanitized receipts only — local
 * paths, usernames, prompts, outputs, commands, environment values, config
 * bodies, ledger rows, and reusable credentials are mechanically rejected.
 *
 * Reconciliation is a single bounded step computed on demand; callers invoke
 * it from an existing bounded sync opportunity, startup, or an explicit
 * operator command. Nothing here schedules, loops, or runs continuously.
 */

import crypto from "node:crypto";

// ---------------------------------------------------------------------------
// Observed-state vocabulary (issue #154: observed stays distinct from requested)
// ---------------------------------------------------------------------------

export const FLEET_OBSERVED_STATES = [
  "desired",
  "downloaded",
  "staged",
  "switched",
  "service_ready",
  "signal_verified",
  "rollback_started",
  "rolled_back",
  "revoked",
  "offline",
] as const;
export type FleetObservedState = (typeof FLEET_OBSERVED_STATES)[number];

/**
 * Allowed observed-state transitions. The forward arm never skips a stage;
 * rollback may start from any applied-but-not-rolled-back state; `revoked`
 * is terminal (a revoked device can never be revived by transition);
 * `offline` is a contact condition reachable from any live state, and a
 * returning device may report whatever it truthfully became while offline.
 */
const ALLOWED_TRANSITIONS_TABLE: Record<FleetObservedState, readonly FleetObservedState[]> = {
  desired: ["downloaded", "offline", "revoked"],
  downloaded: ["staged", "rollback_started", "offline", "revoked"],
  staged: ["switched", "rollback_started", "offline", "revoked"],
  switched: ["service_ready", "rollback_started", "offline", "revoked"],
  service_ready: ["signal_verified", "rollback_started", "offline", "revoked"],
  signal_verified: ["desired", "rollback_started", "offline", "revoked"],
  rollback_started: ["rolled_back", "offline", "revoked"],
  rolled_back: ["desired", "offline", "revoked"],
  revoked: [],
  offline: FLEET_OBSERVED_STATES.filter((state) => state !== "offline"),
};

export const ALLOWED_FLEET_TRANSITIONS: Readonly<
  Record<FleetObservedState, readonly FleetObservedState[]>
> = Object.freeze(ALLOWED_TRANSITIONS_TABLE);

const FORWARD_ROLLOUT_STATES: readonly FleetObservedState[] = [
  "desired",
  "downloaded",
  "staged",
  "switched",
  "service_ready",
  "signal_verified",
];
const ROLLBACK_STATES: readonly FleetObservedState[] = ["rollback_started", "rolled_back"];

export function isFleetTransitionAllowed(from: FleetObservedState, to: FleetObservedState): boolean {
  if (from === to) return false;
  return ALLOWED_FLEET_TRANSITIONS[from].includes(to);
}

export function advanceFleetState(
  from: FleetObservedState,
  to: FleetObservedState,
): { ok: true; state: FleetObservedState } | { ok: false; reason: string } {
  if (from === to) return { ok: false, reason: `transition_no_op:${from}` };
  if (!ALLOWED_FLEET_TRANSITIONS[from].includes(to)) {
    return { ok: false, reason: `transition_forbidden:${from}->${to}` };
  }
  return { ok: true, state: to };
}

// ---------------------------------------------------------------------------
// Signed desired state
// ---------------------------------------------------------------------------

export const DESIRED_STATE_SCHEMA_VERSION = 1 as const;

const DEVICE_ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$/;
const SHA256_PATTERN = /^[a-f0-9]{64}$/;
const ARTIFACT_VERSION_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/;
const MAX_REJECTED_DIGESTS = 64;

export type DesiredScope =
  | { kind: "device"; deviceId: string }
  | { kind: "cohort"; cohortId: string };

export interface FleetArtifactRef {
  version: string;
  sha256: string;
}

/** The unsigned core of a desired-state document. Canonical JSON is fixed-order. */
export interface DesiredStateCore {
  schemaVersion: typeof DESIRED_STATE_SCHEMA_VERSION;
  /** Monotonic per scope: strictly greater than every version previously seen for this scope. */
  desiredVersion: number;
  scope: DesiredScope;
  artifact: FleetArtifactRef;
  issuedAtMs: number;
}

export interface SignedDesiredState extends DesiredStateCore {
  /** Base64 Ed25519 signature over the canonical core serialization. */
  signature: string;
}

function boundedMs(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function parseScope(raw: unknown): DesiredScope | null {
  if (typeof raw !== "object" || raw === null) return null;
  const record = raw as Record<string, unknown>;
  if (record.kind === "device") {
    return typeof record.deviceId === "string" && DEVICE_ID_PATTERN.test(record.deviceId)
      ? { kind: "device", deviceId: record.deviceId }
      : null;
  }
  if (record.kind === "cohort") {
    return typeof record.cohortId === "string" && DEVICE_ID_PATTERN.test(record.cohortId)
      ? { kind: "cohort", cohortId: record.cohortId }
      : null;
  }
  return null;
}

/** Fixed-key-order canonical serialization; the signature covers exactly these bytes. */
export function serializeDesiredStateCore(core: DesiredStateCore): string {
  const scope =
    core.scope.kind === "device"
      ? { kind: "device", deviceId: core.scope.deviceId }
      : { kind: "cohort", cohortId: core.scope.cohortId };
  return JSON.stringify({
    schemaVersion: core.schemaVersion,
    desiredVersion: core.desiredVersion,
    scope,
    artifact: { version: core.artifact.version, sha256: core.artifact.sha256 },
    issuedAtMs: core.issuedAtMs,
  });
}

/** Parse and fully validate the core fields of a candidate document. Fails closed. */
export function parseDesiredStateCore(raw: unknown):
  | { ok: true; value: DesiredStateCore }
  | { ok: false; reason: string } {
  if (typeof raw !== "object" || raw === null) return { ok: false, reason: "core_not_an_object" };
  const record = raw as Record<string, unknown>;
  if (record.schemaVersion !== DESIRED_STATE_SCHEMA_VERSION) {
    return { ok: false, reason: "unsupported_schema_version" };
  }
  if (
    typeof record.desiredVersion !== "number" ||
    !Number.isSafeInteger(record.desiredVersion) ||
    record.desiredVersion < 1
  ) {
    return { ok: false, reason: "invalid_desired_version" };
  }
  const scope = parseScope(record.scope);
  if (!scope) return { ok: false, reason: "invalid_scope" };
  if (typeof record.artifact !== "object" || record.artifact === null) {
    return { ok: false, reason: "invalid_artifact" };
  }
  const artifact = record.artifact as Record<string, unknown>;
  if (
    typeof artifact.version !== "string" ||
    !ARTIFACT_VERSION_PATTERN.test(artifact.version) ||
    artifact.version.includes("..")
  ) {
    return { ok: false, reason: "invalid_artifact_version" };
  }
  if (typeof artifact.sha256 !== "string" || !SHA256_PATTERN.test(artifact.sha256)) {
    return { ok: false, reason: "invalid_artifact_digest" };
  }
  if (!boundedMs(record.issuedAtMs)) return { ok: false, reason: "invalid_issued_at" };
  return {
    ok: true,
    value: {
      schemaVersion: DESIRED_STATE_SCHEMA_VERSION,
      desiredVersion: record.desiredVersion,
      scope,
      artifact: { version: artifact.version, sha256: artifact.sha256 },
      issuedAtMs: record.issuedAtMs,
    },
  };
}

export function signDesiredState(core: DesiredStateCore, privateKey: crypto.KeyObject): SignedDesiredState {
  const signature = crypto
    .sign(null, Buffer.from(serializeDesiredStateCore(core), "utf8"), privateKey)
    .toString("base64");
  return { ...core, scope: { ...core.scope }, artifact: { ...core.artifact }, signature };
}

export type DesiredStateVerification =
  | { ok: true; value: SignedDesiredState }
  | { ok: false; reason: string };

/**
 * Verify a signed desired document against the fleet public key: schema
 * validity first, then signature over the canonical core bytes. A tampered
 * field of any kind fails here — never downstream.
 */
export function verifySignedDesiredState(raw: unknown, publicKey: crypto.KeyObject): DesiredStateVerification {
  if (typeof raw !== "object" || raw === null) return { ok: false, reason: "document_not_an_object" };
  const record = raw as Record<string, unknown>;
  const parsed = parseDesiredStateCore(record);
  if (!parsed.ok) return parsed;
  if (typeof record.signature !== "string" || record.signature.length === 0 || record.signature.length > 2048) {
    return { ok: false, reason: "invalid_signature_encoding" };
  }
  let valid = false;
  try {
    valid = crypto.verify(
      null,
      Buffer.from(serializeDesiredStateCore(parsed.value), "utf8"),
      publicKey,
      Buffer.from(record.signature, "base64"),
    );
  } catch {
    valid = false;
  }
  if (!valid) return { ok: false, reason: "signature_invalid" };
  return {
    ok: true,
    value: { ...parsed.value, scope: { ...parsed.value.scope }, artifact: { ...parsed.value.artifact }, signature: record.signature },
  };
}

/**
 * Scope resolution. A device-scoped document applies only to that exact
 * device; a cohort-scoped document applies only through explicitly declared
 * cohort membership. An undeclared cohort applies to nobody (fail closed).
 */
export function desiredAppliesTo(
  doc: SignedDesiredState,
  deviceId: string,
  cohortMembership: Readonly<Record<string, readonly string[]>>,
): boolean {
  if (doc.scope.kind === "device") return doc.scope.deviceId === deviceId;
  const members = cohortMembership[doc.scope.cohortId];
  return Array.isArray(members) && members.includes(deviceId);
}

/**
 * Replay/rollback guard: a document is acceptable only when its version is
 * strictly greater than the highest version already processed for its scope.
 * Equal and lower versions are refused even when validly signed.
 */
export function isDesiredVersionMonotonic(doc: SignedDesiredState, highestSeenVersion: number): boolean {
  return Number.isSafeInteger(highestSeenVersion) && doc.desiredVersion > highestSeenVersion;
}

// ---------------------------------------------------------------------------
// Sanitized device receipts
// ---------------------------------------------------------------------------

export const DEVICE_RECEIPT_SCHEMA_VERSION = 1 as const;

export interface DeviceReceiptInput {
  deviceId: string;
  cohortId?: string | undefined;
  observedState: FleetObservedState;
  /** Digest actually applied locally; null before `switched`. */
  appliedArtifact: FleetArtifactRef | null;
  /** Highest desired version this device has processed, monotonic memory. */
  highestDesiredVersionSeen: number;
  /** Digests this device has rolled back and must never auto-reapply. */
  rejectedDigests?: readonly string[];
  lastSignalAtMs: number;
  generatedAtMs: number;
}

export interface DeviceReceipt {
  schemaVersion: typeof DEVICE_RECEIPT_SCHEMA_VERSION;
  deviceId: string;
  cohortId: string | null;
  observedState: FleetObservedState;
  appliedArtifact: FleetArtifactRef | null;
  highestDesiredVersionSeen: number;
  rejectedDigests: readonly string[];
  lastSignalAtMs: number;
  generatedAtMs: number;
  contentHash: string;
}

function canonicalReceiptJson(receipt: Omit<DeviceReceipt, "contentHash">): string {
  return JSON.stringify({
    schemaVersion: receipt.schemaVersion,
    deviceId: receipt.deviceId,
    cohortId: receipt.cohortId,
    observedState: receipt.observedState,
    appliedArtifact:
      receipt.appliedArtifact === null
        ? null
        : { version: receipt.appliedArtifact.version, sha256: receipt.appliedArtifact.sha256 },
    highestDesiredVersionSeen: receipt.highestDesiredVersionSeen,
    rejectedDigests: receipt.rejectedDigests,
    lastSignalAtMs: receipt.lastSignalAtMs,
    generatedAtMs: receipt.generatedAtMs,
  });
}

/**
 * Build a sanitized receipt from observed facts. Every field is validated;
 * rejected digests are deduplicated, sorted, and capped. The same input
 * always yields byte-identical output and content hash. Anything that does
 * not fit the sanitized shape is refused, not coerced.
 */
export function buildDeviceReceipt(input: DeviceReceiptInput): DeviceReceipt {
  if (typeof input.deviceId !== "string" || !DEVICE_ID_PATTERN.test(input.deviceId)) {
    throw new Error("receipt_invalid_device_id");
  }
  if (input.cohortId !== undefined) {
    if (typeof input.cohortId !== "string" || !DEVICE_ID_PATTERN.test(input.cohortId)) {
      throw new Error("receipt_invalid_cohort_id");
    }
  }
  if (!(FLEET_OBSERVED_STATES as readonly string[]).includes(input.observedState)) {
    throw new Error("receipt_invalid_observed_state");
  }
  if (input.appliedArtifact !== null) {
    if (
      typeof input.appliedArtifact !== "object" ||
      !ARTIFACT_VERSION_PATTERN.test(input.appliedArtifact.version ?? "") ||
      typeof input.appliedArtifact.sha256 !== "string" ||
      !SHA256_PATTERN.test(input.appliedArtifact.sha256)
    ) {
      throw new Error("receipt_invalid_applied_artifact");
    }
  }
  if (typeof input.highestDesiredVersionSeen !== "number" || !Number.isSafeInteger(input.highestDesiredVersionSeen) || input.highestDesiredVersionSeen < 0) {
    throw new Error("receipt_invalid_highest_desired_version");
  }
  if (!boundedMs(input.lastSignalAtMs)) throw new Error("receipt_invalid_last_signal_at");
  if (!boundedMs(input.generatedAtMs)) throw new Error("receipt_invalid_generated_at");

  const rawRejected = input.rejectedDigests ?? [];
  if (rawRejected.length > MAX_REJECTED_DIGESTS) throw new Error("receipt_rejected_digests_out_of_bounds");
  const rejectedDigests = [...new Set(rawRejected)].sort();
  for (const digest of rejectedDigests) {
    if (typeof digest !== "string" || !SHA256_PATTERN.test(digest)) {
      throw new Error("receipt_invalid_rejected_digest");
    }
  }

  const withoutHash = {
    schemaVersion: DEVICE_RECEIPT_SCHEMA_VERSION,
    deviceId: input.deviceId,
    cohortId: input.cohortId ?? null,
    observedState: input.observedState,
    appliedArtifact:
      input.appliedArtifact === null
        ? null
        : { version: input.appliedArtifact.version, sha256: input.appliedArtifact.sha256 },
    highestDesiredVersionSeen: input.highestDesiredVersionSeen,
    rejectedDigests,
    lastSignalAtMs: input.lastSignalAtMs,
    generatedAtMs: input.generatedAtMs,
  };
  const contentHash = crypto.createHash("sha256").update(canonicalReceiptJson(withoutHash)).digest("hex");
  return { ...withoutHash, rejectedDigests: Object.freeze(rejectedDigests), contentHash };
}

/** Canonical serialization: identical receipts serialize to identical bytes. */
export function serializeDeviceReceipt(receipt: DeviceReceipt): string {
  return `${canonicalReceiptJson(receipt)}|${receipt.contentHash}`;
}

// --- Privacy guard ----------------------------------------------------------

/**
 * The complete set of keys a sanitized receipt may carry at any depth.
 * Structured payloads under any other key are forbidden-field violations —
 * fleet hosted state holds desired policy and sanitized receipts only.
 */
const ALLOWED_RECEIPT_KEYS: ReadonlySet<string> = new Set([
  "schemaVersion",
  "deviceId",
  "cohortId",
  "observedState",
  "appliedArtifact",
  "highestDesiredVersionSeen",
  "rejectedDigests",
  "lastSignalAtMs",
  "generatedAtMs",
  "contentHash",
  "version",
  "sha256",
]);

const FORBIDDEN_CONCEPTS: readonly string[] = [
  "path",
  "home",
  "user",
  "username",
  "prompt",
  "output",
  "command",
  "argument",
  "env",
  "environment",
  "config",
  "credential",
  "secret",
  "token",
  "cookie",
  "ledger",
  "row",
  "transcript",
  "message",
];

const SECRET_PATTERNS: ReadonlyArray<{ label: string; pattern: RegExp }> = [
  { label: "openai_style_key", pattern: /\bsk-[A-Za-z0-9_-]{16,}\b/ },
  { label: "github_token", pattern: /\b(ghp_|gho_|github_pat_)[A-Za-z0-9_]{10,}\b/ },
  { label: "aws_access_key", pattern: /\bAKIA[0-9A-Z]{12,}\b/ },
  { label: "slack_token", pattern: /\bxox[abprs]-[A-Za-z0-9-]{8,}\b/ },
  { label: "private_key_block", pattern: /-----BEGIN [A-Z ]*PRIVATE KEY-----/ },
  { label: "bearer_token", pattern: /\bBearer\s+[A-Za-z0-9._-]{20,}\b/ },
];

const HOME_PATH_PATTERN = /(?:\/Users\/[^/\s"]+|\/home\/[^/\s"]+|C:\\Users\\[^\\\s"]+)/;

export interface ReceiptPrivacyViolation {
  reason: string;
}

/**
 * Reject anything shaped like a receipt that smuggles unsanitized material:
 * non-allowlisted keys at any depth, forbidden concept substrings in key
 * names, secret-like strings or home paths anywhere in the serialized form.
 */
export function findFleetReceiptPrivacyViolations(candidate: unknown): ReceiptPrivacyViolation[] {
  const violations: ReceiptPrivacyViolation[] = [];
  if (typeof candidate !== "object" || candidate === null) {
    return [{ reason: "receipt_not_an_object" }];
  }
  const visit = (value: unknown, pathKey: string): void => {
    if (Array.isArray(value)) {
      value.forEach((item, index) => visit(item, `${pathKey}[${index}]`));
      return;
    }
    if (typeof value !== "object" || value === null) return;
    for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
      const qualified = pathKey.length > 0 ? `${pathKey}.${key}` : key;
      if (!ALLOWED_RECEIPT_KEYS.has(key)) violations.push({ reason: `forbidden_field:${qualified}` });
      const lower = key.toLowerCase();
      if (FORBIDDEN_CONCEPTS.some((concept) => lower.includes(concept))) {
        violations.push({ reason: `forbidden_field_concept:${qualified}` });
      }
      visit(child, qualified);
    }
  };
  visit(candidate, "");
  const serialized = JSON.stringify(candidate);
  for (const secret of SECRET_PATTERNS) {
    if (secret.pattern.test(serialized)) violations.push({ reason: `secret_like_string:${secret.label}` });
  }
  if (HOME_PATH_PATTERN.test(serialized)) violations.push({ reason: "home_path_present" });
  return violations;
}

/** Convenience gate used by proofs and sync paths: empty array means clean. */
export function assertFleetReceiptPrivacy(candidate: unknown): void {
  const violations = findFleetReceiptPrivacyViolations(candidate);
  if (violations.length > 0) {
    throw new Error(`fleet_receipt_privacy_violation:${violations.map((v) => v.reason).join(",")}`);
  }
}

// ---------------------------------------------------------------------------
// Bounded reconciliation planning
// ---------------------------------------------------------------------------

/** Reconciliation rides an existing opportunity; nothing else may trigger it. */
export const RECONCILE_TRIGGERS = ["sync", "startup", "operator"] as const;
export type ReconcileTrigger = (typeof RECONCILE_TRIGGERS)[number];

export type ReconcileAction =
  | { action: "no_op"; reason: "already_current" | "revoked" }
  | { action: "await_signal"; reason: "offline" | "signal_stale" }
  | { action: "hold_for_operator"; reason: "version_not_monotonic" | "digest_rolled_back" | "cohort_not_declared" }
  | { action: "start_rollback"; reason: "applied_digest_drift" }
  | { action: "finish_rollback"; reason: "rollback_in_progress" }
  | {
      action: "apply_step";
      reason:
        | "new_cycle"
        | "download"
        | "stage"
        | "switch"
        | "confirm_service_ready"
        | "confirm_signal_verified";
    };

export interface ReconcilePlan {
  plan: ReconcileAction;
  /** The one-step observed-state transition the device should report next. */
  nextState: FleetObservedState | null;
}

export const DEFAULT_SIGNAL_STALENESS_MS = 30 * 60 * 1000;

/**
 * Compute exactly one bounded next step for a device, or refuse honestly.
 * Never re-applies a rolled-back digest, never advances a revoked device,
 * never acts on a stale-silent device, and refuses unbounded triggers.
 * Pure decision support: executing the step belongs to the caller's existing
 * sync/startup/operator path.
 */
export function planReconciliation(input: {
  trigger: string;
  desired: SignedDesiredState;
  verified: boolean;
  applies: boolean;
  receipt: DeviceReceipt;
  nowMs: number;
  signalStalenessBudgetMs?: number | undefined;
}): { ok: true; plan: ReconcilePlan } | { ok: false; reason: string } {
  if (!(RECONCILE_TRIGGERS as readonly string[]).includes(input.trigger)) {
    return { ok: false, reason: `trigger_not_bounded:${input.trigger}` };
  }
  if (!input.verified) return { ok: false, reason: "desired_signature_invalid" };

  const { receipt, desired } = input;

  // Revocation is terminal and wins over everything, including offline.
  if (receipt.observedState === "revoked") {
    return { ok: true, plan: { plan: { action: "no_op", reason: "revoked" }, nextState: null } };
  }

  // A silent device is never commanded; wait for it to report.
  const budget = input.signalStalenessBudgetMs ?? DEFAULT_SIGNAL_STALENESS_MS;
  if (receipt.observedState === "offline" || input.nowMs - receipt.lastSignalAtMs > budget) {
    return {
      ok: true,
      plan: {
        plan: {
          action: "await_signal",
          reason: receipt.observedState === "offline" ? "offline" : "signal_stale",
        },
        nextState: null,
      },
    };
  }

  if (!isDesiredVersionMonotonic(desired, receipt.highestDesiredVersionSeen)) {
    return {
      ok: true,
      plan: {
        plan: { action: "hold_for_operator", reason: "version_not_monotonic" },
        nextState: null,
      },
    };
  }

  // Cohort documents apply only through declared membership (fail closed).
  if (!input.applies) {
    return {
      ok: true,
      plan: {
        plan: { action: "hold_for_operator", reason: "cohort_not_declared" },
        nextState: null,
      },
    };
  }

  if (receipt.rejectedDigests.includes(desired.artifact.sha256)) {
    return {
      ok: true,
      plan: {
        plan: { action: "hold_for_operator", reason: "digest_rolled_back" },
        nextState: null,
      },
    };
  }

  if (receipt.observedState === "rollback_started") {
    return { ok: true, plan: { plan: { action: "finish_rollback", reason: "rollback_in_progress" }, nextState: "rolled_back" } };
  }

  // Drift: something is applied that the approved artifact did not approve.
  if (
    receipt.appliedArtifact !== null &&
    receipt.appliedArtifact.sha256 !== desired.artifact.sha256 &&
    (receipt.observedState === "switched" ||
      receipt.observedState === "service_ready" ||
      receipt.observedState === "signal_verified")
  ) {
    return { ok: true, plan: { plan: { action: "start_rollback", reason: "applied_digest_drift" }, nextState: "rollback_started" } };
  }

  switch (receipt.observedState) {
    case "desired":
      return { ok: true, plan: { plan: { action: "apply_step", reason: "download" }, nextState: "downloaded" } };
    case "downloaded":
      return { ok: true, plan: { plan: { action: "apply_step", reason: "stage" }, nextState: "staged" } };
    case "staged":
      return { ok: true, plan: { plan: { action: "apply_step", reason: "switch" }, nextState: "switched" } };
    case "switched":
      return { ok: true, plan: { plan: { action: "apply_step", reason: "confirm_service_ready" }, nextState: "service_ready" } };
    case "service_ready":
      return { ok: true, plan: { plan: { action: "apply_step", reason: "confirm_signal_verified" }, nextState: "signal_verified" } };
    case "signal_verified":
      return { ok: true, plan: { plan: { action: "no_op", reason: "already_current" }, nextState: null } };
    case "rolled_back":
      return { ok: true, plan: { plan: { action: "apply_step", reason: "new_cycle" }, nextState: "desired" } };
    default:
      return { ok: false, reason: `unplannable_state:${receipt.observedState}` };
  }
}

// ---------------------------------------------------------------------------
// One truthful fleet view (derived from registry + sanitized receipts only)
// ---------------------------------------------------------------------------

export const FLEET_UNKNOWN = "UNKNOWN" as const;

export type EnrolledStatus = "pending" | "active" | "suspended" | "revoked";

export interface DeviceRegistryEntry {
  deviceId: string;
  cohortId?: string | undefined;
  enrolledStatus: EnrolledStatus;
}

export type SignalFreshness = "fresh" | "stale" | typeof FLEET_UNKNOWN;

export interface FleetDeviceView {
  deviceId: string;
  enrolledStatus: EnrolledStatus;
  approvedVersion: string | typeof FLEET_UNKNOWN;
  installedVersion: string | typeof FLEET_UNKNOWN;
  readiness: FleetObservedState | typeof FLEET_UNKNOWN;
  signalFreshness: SignalFreshness;
  rolloutState: FleetObservedState | typeof FLEET_UNKNOWN;
  rollbackState: FleetObservedState | typeof FLEET_UNKNOWN;
  revoked: boolean;
}

/**
 * Aggregate one row per registered device from verified desired documents and
 * sanitized receipts. Missing evidence renders as the literal UNKNOWN — a
 * device without a receipt is reported as unknown, never fabricated.
 */
export function renderFleetView(input: {
  registry: readonly DeviceRegistryEntry[];
  receiptsByDevice: Readonly<Record<string, DeviceReceipt>>;
  verifiedDesiredStates: readonly SignedDesiredState[];
  cohortMembership: Readonly<Record<string, readonly string[]>>;
  nowMs: number;
  signalStalenessBudgetMs?: number | undefined;
}): FleetDeviceView[] {
  const budget = input.signalStalenessBudgetMs ?? DEFAULT_SIGNAL_STALENESS_MS;
  return input.registry.map((entry) => {
    const receipt = input.receiptsByDevice[entry.deviceId];

    // Newest applicable monotonic document decides the approved artifact.
    let approved: SignedDesiredState | null = null;
    for (const doc of input.verifiedDesiredStates) {
      if (!desiredAppliesTo(doc, entry.deviceId, input.cohortMembership)) continue;
      if (approved === null || doc.desiredVersion > approved.desiredVersion) approved = doc;
    }

    const revoked =
      entry.enrolledStatus === "revoked" || receipt?.observedState === "revoked";

    let signalFreshness: SignalFreshness = FLEET_UNKNOWN;
    if (receipt) {
      if (receipt.observedState === "offline") signalFreshness = "stale";
      else if (input.nowMs - receipt.lastSignalAtMs > budget) signalFreshness = "stale";
      else signalFreshness = "fresh";
    }

    const observedState = receipt?.observedState;
    return {
      deviceId: entry.deviceId,
      enrolledStatus: entry.enrolledStatus,
      approvedVersion: approved ? approved.artifact.version : FLEET_UNKNOWN,
      installedVersion:
        receipt?.appliedArtifact ? receipt.appliedArtifact.version : FLEET_UNKNOWN,
      readiness: observedState ?? FLEET_UNKNOWN,
      signalFreshness,
      rolloutState:
        observedState && FORWARD_ROLLOUT_STATES.includes(observedState)
          ? observedState
          : FLEET_UNKNOWN,
      rollbackState:
        observedState && ROLLBACK_STATES.includes(observedState) ? observedState : FLEET_UNKNOWN,
      revoked,
    };
  });
}
