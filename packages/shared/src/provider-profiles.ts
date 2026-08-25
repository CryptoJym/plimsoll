/**
 * Provider profile registry + optional signed capacity-sync client
 * (issue #172, parent #167).
 *
 * Outcome: independent provider config homes (`CLAUDE_CONFIG_DIR`,
 * `CODEX_HOME`) registered explicitly per device, plus an OPTIONAL,
 * default-off, owner-gated upload lane for sanitized capacity facts.
 *
 * Doctrine enforced here:
 * - Provider credentials stay in provider-owned homes. Nothing in this
 *   module reads, copies, switches, or expands into account/auth APIs.
 * - Profile keys are device-local opaque random values, never derived from
 *   labels, emails, paths, or account data.
 * - The same label never merges distinct profiles: a label collision across
 *   different provider homes is rejected, never reconciled silently.
 * - A changed provider home or account is NEVER inferred as a switch. The
 *   only path that rebinds a home or rotates identity context is an explicit
 *   owner-confirmed rotation (exact confirmation string) that issues a new
 *   opaque key and bumps the profile epoch; pre-rotation queue items are
 *   refused at delivery time.
 * - Codex exposes no stable account identity through allowed methods, so its
 *   identity status is the literal UNDETECTABLE_WITH_ALLOWED_METHODS and
 *   cross-machine merges involving it require participant approval.
 * - The cloud body is allowlisted at construction AND re-validated by a
 *   structural scanner before signing: no person, tenant, device id, email,
 *   label, filesystem path, billing figure, credential, provider account
 *   id, or raw provider-report value can appear in it.
 * - Sync defaults to OFF. preview / enable / pause / revoke / export /
 *   delete are explicit commands; revocation stops delivery permanently
 *   while local self-view keeps working.
 * - The capacity queue is separate from any telemetry outbox (its own state
 *   object, own schema) and carries a monotonic device sequence that never
 *   resets or reuses numbers — including after pause/resume or deletion.
 * - Event-triggered debounce is always available; the 15-minute fallback
 *   timer stays unauthorized until a probe-cost proof measures real samples
 *   and shows p95 within budget. Authorization RE-EVALUATES the samples;
 *   a claimed-pass receipt alone is never trusted.
 *
 * This module is deliberately self-contained: it imports nothing from other
 * repository modules so it cannot couple capacity planning into decision
 * surfaces (see scripts/capacity-dependency-reachability.ts doctrine). All
 * randomness and HMAC come from injected functions whose defaults use
 * node:crypto; proofs inject deterministic equivalents.
 */

import crypto from "node:crypto";

export const PROVIDER_PROFILES_SCHEMA_VERSION = 1 as const;
export const PROVIDER_PROFILE_REGISTRY_SCHEMA =
  "plimsoll.provider-profiles.v1" as const;
export const CAPACITY_SYNC_BODY_SCHEMA = "plimsoll.capacity-sync.v1" as const;

/** Literal required by issue #172: the allowed Codex method exposes no
 * stable account identity, so external account changes are undetectable. */
export const CODEX_IDENTITY_UNDETECTABLE =
  "UNDETECTABLE_WITH_ALLOWED_METHODS" as const;
export const IDENTITY_STATUS_STABLE = "STABLE_PROVIDER_IDENTITY" as const;

export type IdentityStatus =
  | typeof IDENTITY_STATUS_STABLE
  | typeof CODEX_IDENTITY_UNDETECTABLE;

export type SyncLifecycleState = "off" | "enabled" | "paused" | "revoked";

/** Explicit config-home binding. At least one side must be set by the owner;
 * null means "no home of this kind bound", not "default location". */
export type ProviderHomeBinding = {
  claudeConfigDir: string | null;
  codexHome: string | null;
};

export const DEFAULT_EVENT_DEBOUNCE_MS = 2_000 as const;
export const FALLBACK_INTERVAL_MS = 15 * 60 * 1_000 as const;
export const PROBE_COST_MIN_SAMPLES = 5 as const;
export const PROBE_COST_BUDGET_P95_MS = 50 as const;

const PROFILE_KEY_PATTERN = /^[0-9a-f]{32}$/;
const LABEL_PATTERN = /^[A-Za-z0-9][A-Za-z0-9 ._-]{0,127}$/;
const DIMENSION_PATTERN = /^[a-z0-9_]{1,64}$/;
const SYNC_UNITS = ["tokens", "requests", "usd", "percent"] as const;
export type CapacitySyncUnit = (typeof SYNC_UNITS)[number];
export type CapacitySyncFreshness = "fresh" | "STALE" | "UNKNOWN";
export type CapacitySyncSource = "local_telemetry" | "provider_report";
export const ROTATION_REASONS = [
  "account_change",
  "home_change",
  "manual",
] as const;
export type RotationReason = (typeof ROTATION_REASONS)[number];

export type RotationReceipt = {
  kind: "rotation";
  label: string;
  reason: RotationReason;
  epochBefore: number;
  epochAfter: number;
  keyFingerprintBefore: string;
  keyFingerprintAfter: string;
  homeRebound: boolean;
  confirmedAt: string;
};

export type DeletionReceipt = {
  kind: "deletion";
  label: string;
  removedQueueItems: number;
  confirmedAt: string;
};

export type ProviderProfileRecord = {
  profileKey: string;
  label: string;
  provider: "claude_code" | "codex";
  home: ProviderHomeBinding;
  epoch: number;
  identityStatus: IdentityStatus;
  createdAt: string;
  rotatedAt: string | null;
  revokedAt: string | null;
};

export type SanitizedCapacityItem = {
  /** Short fingerprint of the opaque profile key — still opaque off-device. */
  k: string;
  /** Profile epoch at enqueue time; pre-rotation items are refused on drain. */
  e: number;
  d: string;
  u: CapacitySyncUnit;
  f: CapacitySyncFreshness;
  limit: number | null;
  used: number | null;
};

export type QueuedSyncItem = {
  deviceSeq: number;
  profileKey: string;
  item: SanitizedCapacityItem;
  queuedAt: string;
};

export type CapacitySyncSettings = {
  state: SyncLifecycleState;
  enabledAt: string | null;
  pausedAt: string | null;
  revokedAt: string | null;
  fallbackTimerAuthorized: boolean;
  fallbackAuthorizedAt: string | null;
  lastDeliveredDeviceSeq: number;
};

export type ProfileRegistryState = {
  schema: typeof PROVIDER_PROFILE_REGISTRY_SCHEMA;
  schemaVersion: typeof PROVIDER_PROFILES_SCHEMA_VERSION;
  profiles: ProviderProfileRecord[];
  receipts: Array<RotationReceipt | DeletionReceipt>;
  queue: QueuedSyncItem[];
  sequence: { lastDeviceSeq: number };
  sync: CapacitySyncSettings;
  createdAt: string;
};

export type RandomHex = (byteCount: number) => string;
export type HmacFn = (
  secretHex: string,
  canonicalJson: string,
) => string;

export type RegistryDeps = {
  now?: () => string;
  /** 32 hex chars of randomness per call for keys/signing secrets. */
  randomHex?: RandomHex;
  /** Stable short fingerprint for receipts/audit lines. */
  fingerprint?: (value: string) => string;
  hmac?: HmacFn;
};

function isoNow(): string {
  return new Date().toISOString();
}

function defaultRandomHex(byteCount: number): string {
  return crypto.randomBytes(byteCount).toString("hex");
}

function defaultFingerprint(value: string): string {
  return `kfp_${crypto
    .createHash("sha256")
    .update(value)
    .digest("hex")
    .slice(0, 16)}`;
}

function defaultHmac(secretHex: string, canonicalJson: string): string {
  return crypto
    .createHmac("sha256", Buffer.from(secretHex, "hex"))
    .update(canonicalJson)
    .digest("hex");
}

function requireIsoTimestamp(field: string, value: unknown): string {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value))) {
    throw new Error(`invalid ${field}`);
  }
  return value;
}

function requireLabel(value: unknown): string {
  if (typeof value !== "string" || !LABEL_PATTERN.test(value)) {
    throw new Error("invalid profile label");
  }
  return value;
}

function requireHomeDir(field: string, value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new Error(`invalid ${field}`);
  const trimmed = value.trim();
  if (trimmed === "") return null;
  if (trimmed.length > 1024 || /[\u0000-\u001f]/.test(trimmed)) {
    throw new Error(`invalid ${field}`);
  }
  return trimmed;
}

function requireHomeBinding(home: unknown): ProviderHomeBinding {
  if (!home || typeof home !== "object") {
    throw new Error("invalid provider home binding");
  }
  const candidate = home as Partial<ProviderHomeBinding>;
  const claudeConfigDir = requireHomeDir("claudeConfigDir", candidate.claudeConfigDir);
  const codexHome = requireHomeDir("codexHome", candidate.codexHome);
  if (claudeConfigDir === null && codexHome === null) {
    throw new Error(
      "profile requires an explicit CLAUDE_CONFIG_DIR or CODEX_HOME binding",
    );
  }
  return { claudeConfigDir, codexHome };
}

function sameHome(a: ProviderHomeBinding, b: ProviderHomeBinding): boolean {
  return (
    a.claudeConfigDir === b.claudeConfigDir && a.codexHome === b.codexHome
  );
}

function homeToString(home: ProviderHomeBinding): string {
  return `${home.claudeConfigDir ?? "-"}|${home.codexHome ?? "-"}`;
}

function requireConfirmation(expected: string, actual: unknown): void {
  if (actual !== expected) {
    throw new Error(
      `owner confirmation required: expected exact token "${expected}"`,
    );
  }
}

function finiteNonNegativeOrNull(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < 0 ||
    value > Number.MAX_SAFE_INTEGER
  ) {
    throw new Error("invalid numeric capacity fact");
  }
  return value;
}

/**
 * Deterministic canonical JSON (sorted object keys) used for signing.
 * Arrays keep order; numbers serialize via JSON semantics.
 */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((entry) => canonicalJson(entry)).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const keys = Object.keys(value as Record<string, unknown>).sort();
    return `{${keys
      .map(
        (key) =>
          `${JSON.stringify(key)}:${canonicalJson(
            (value as Record<string, unknown>)[key],
          )}`,
      )
      .join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}

/** Create registry state. Sync is OFF and the fallback timer is UNAUTHORIZED
 * until a measured probe-cost proof passes. */
export function createProfileRegistryState(deps: RegistryDeps = {}): ProfileRegistryState {
  const now = deps.now ?? isoNow;
  const createdAt = requireIsoTimestamp("now()", now());
  return {
    schema: PROVIDER_PROFILE_REGISTRY_SCHEMA,
    schemaVersion: PROVIDER_PROFILES_SCHEMA_VERSION,
    profiles: [],
    receipts: [],
    queue: [],
    sequence: { lastDeviceSeq: 0 },
    sync: {
      state: "off",
      enabledAt: null,
      pausedAt: null,
      revokedAt: null,
      fallbackTimerAuthorized: false,
      fallbackAuthorizedAt: null,
      lastDeliveredDeviceSeq: 0,
    },
    createdAt,
  };
}

function freshKey(deps: RegistryDeps): string {
  const key = (deps.randomHex ?? defaultRandomHex)(16).toLowerCase();
  if (!PROFILE_KEY_PATTERN.test(key)) {
    throw new Error("generated profile key is malformed");
  }
  return key;
}

function findProfile(
  state: ProfileRegistryState,
  label: string,
): ProviderProfileRecord | undefined {
  return state.profiles.find((record) => record.label === label);
}

/**
 * Register a provider profile pinned to explicit config homes. Idempotent
 * for the identical (label, provider, home) triple; anything else reusing a
 * live label is rejected so one label can never stand for two profiles.
 */
export function registerProviderProfile(
  state: ProfileRegistryState,
  input: {
    label: string;
    provider: "claude_code" | "codex";
    home: ProviderHomeBinding;
  },
  deps: RegistryDeps = {},
): { state: ProfileRegistryState; record: ProviderProfileRecord } {
  const label = requireLabel(input.label);
  if (input.provider !== "claude_code" && input.provider !== "codex") {
    throw new Error("invalid provider");
  }
  const home = requireHomeBinding(input.home);
  const existing = findProfile(state, label);
  if (existing) {
    if (existing.provider === input.provider && sameHome(existing.home, home)) {
      return { state, record: existing };
    }
    throw new Error("label_conflict_distinct_profile");
  }
  const homeOwner = state.profiles.find((record) => sameHome(record.home, home));
  if (homeOwner) {
    throw new Error(
      `home_already_registered_to_label:${homeOwner.label}`,
    );
  }
  const record: ProviderProfileRecord = {
    profileKey: freshKey(deps),
    label,
    provider: input.provider,
    home,
    epoch: 1,
    identityStatus:
      input.provider === "codex"
        ? CODEX_IDENTITY_UNDETECTABLE
        : IDENTITY_STATUS_STABLE,
    createdAt: requireIsoTimestamp("(deps.now)", (deps.now ?? isoNow)()),
    rotatedAt: null,
    revokedAt: null,
  };
  return {
    state: { ...state, profiles: [...state.profiles, record] },
    record,
  };
}

/** Read-only drift check. Reports that the observed home differs from the
 * registration; Plimsoll does NOT infer a switch and does NOT mutate. */
export function checkHomeDrift(
  state: ProfileRegistryState,
  input: { label: string; observedHome: ProviderHomeBinding },
): { known: boolean; drift: boolean; rotationRequired: boolean } {
  const record = findProfile(state, requireLabel(input.label));
  if (!record) {
    return { known: false, drift: false, rotationRequired: false };
  }
  const drift = !sameHome(record.home, requireHomeBinding(input.observedHome));
  return { known: true, drift, rotationRequired: drift };
}

/**
 * Owner-confirmed rotation: the ONLY path that rebinds a config home or
 * rotates identity context. Requires the exact confirmation token
 * `rotate:<label>`, issues a brand-new opaque key, bumps the epoch, records
 * a receipt, and leaves prior queue items in place (they are refused at
 * drain time because their epoch is stale).
 */
export function rotateProviderProfileKey(
  state: ProfileRegistryState,
  input: {
    label: string;
    confirmation: string;
    reason: RotationReason;
    newHome?: ProviderHomeBinding;
  },
  deps: RegistryDeps = {},
): {
  state: ProfileRegistryState;
  record: ProviderProfileRecord;
  receipt: RotationReceipt;
} {
  const fingerprint = deps.fingerprint ?? defaultFingerprint;
  const label = requireLabel(input.label);
  const existing = findProfile(state, label);
  if (!existing) throw new Error("unknown_profile");
  if (existing.revokedAt !== null) {
    throw new Error("revoked_profiles_cannot_rotate");
  }
  requireConfirmation(`rotate:${label}`, input.confirmation);
  if (!ROTATION_REASONS.includes(input.reason)) {
    throw new Error("invalid rotation reason");
  }
  let home = existing.home;
  let homeRebound = false;
  if (input.newHome !== undefined) {
    home = requireHomeBinding(input.newHome);
    homeRebound = !sameHome(existing.home, home);
    const collision = state.profiles.find(
      (record) =>
        record.label !== label && record.revokedAt === null && sameHome(record.home, home),
    );
    if (collision) {
      throw new Error(`home_already_registered_to_label:${collision.label}`);
    }
  }
  const fingerprintBefore = fingerprint(existing.profileKey);
  const updated: ProviderProfileRecord = {
    ...existing,
    profileKey: freshKey(deps),
    home,
    epoch: existing.epoch + 1,
    rotatedAt: requireIsoTimestamp("(deps.now)", (deps.now ?? isoNow)()),
  };
  const receipt: RotationReceipt = {
    kind: "rotation",
    label,
    reason: input.reason,
    epochBefore: existing.epoch,
    epochAfter: updated.epoch,
    keyFingerprintBefore: fingerprintBefore,
    keyFingerprintAfter: fingerprint(updated.profileKey),
    homeRebound,
    confirmedAt: updated.rotatedAt!,
  };
  return {
    state: {
      ...state,
      profiles: state.profiles.map((record) =>
        record.label === label ? updated : record,
      ),
      receipts: [...state.receipts, receipt],
    },
    record: updated,
    receipt,
  };
}

export type CapacityFactInput = {
  dimension: string;
  unit: CapacitySyncUnit;
  freshness: CapacitySyncFreshness;
  source: CapacitySyncSource;
  limit: number | null;
  used: number | null;
};

/**
 * Whitelist sanitizer. Values sourced from `provider_report` are raw
 * provider data and are replaced with null (UNKNOWN), never uploaded.
 * Local telemetry values must be finite non-negative numbers or null.
 */
export function sanitizeCapacityFact(
  fact: CapacityFactInput,
  input: { profileKey: string; epoch: number },
): SanitizedCapacityItem {
  const dimension = typeof fact.dimension === "string" ? fact.dimension : "";
  if (!DIMENSION_PATTERN.test(dimension)) {
    throw new Error("invalid_capacity_dimension");
  }
  if (!SYNC_UNITS.includes(fact.unit)) {
    throw new Error("invalid_capacity_unit");
  }
  if (!["fresh", "STALE", "UNKNOWN"].includes(fact.freshness)) {
    throw new Error("invalid_capacity_freshness");
  }
  if (!["local_telemetry", "provider_report"].includes(fact.source)) {
    throw new Error("invalid_capacity_source");
  }
  if (!PROFILE_KEY_PATTERN.test(input.profileKey)) {
    throw new Error("invalid profile key");
  }
  if (!Number.isInteger(input.epoch) || input.epoch < 1) {
    throw new Error("invalid profile epoch");
  }
  const localOnly = fact.source === "local_telemetry";
  return {
    k: defaultFingerprint(input.profileKey),
    e: input.epoch,
    d: dimension,
    u: fact.unit,
    f: fact.freshness,
    limit: localOnly ? finiteNonNegativeOrNull(fact.limit) : null,
    used: localOnly ? finiteNonNegativeOrNull(fact.used) : null,
  };
}

const CLOUD_STRING_SHAPES: Array<{ name: string; test: (value: string) => boolean }> = [
  { name: "schema", test: (v) => v === CAPACITY_SYNC_BODY_SCHEMA },
  { name: "iso_timestamp", test: (v) => !Number.isNaN(Date.parse(v)) && /^\d{4}-\d{2}-\d{2}T/.test(v) },
  { name: "key_fingerprint", test: (v) => /^kfp_[0-9a-f]{16}$/.test(v) },
  { name: "dimension", test: (v) => DIMENSION_PATTERN.test(v) },
  { name: "unit", test: (v) => (SYNC_UNITS as readonly string[]).includes(v) },
  { name: "freshness", test: (v) => ["fresh", "STALE", "UNKNOWN"].includes(v) },
];

/**
 * Structural allowlist scan over a fully built cloud body. Every string in
 * every key position and value position must match a whitelisted shape;
 * anything else (emails, labels, paths, billing text, free prose smuggled
 * via any field) is a violation. Fail closed.
 */
export function scanCloudBodyForForbiddenContent(
  body: unknown,
  path = "$",
): string[] {
  const violations: string[] = [];
  if (typeof body === "number") {
    if (!Number.isFinite(body)) violations.push(`${path}:non_finite_number`);
    return violations;
  }
  if (body === null) return violations;
  if (typeof body === "string") {
    if (!CLOUD_STRING_SHAPES.some((shape) => shape.test(body))) {
      violations.push(`${path}:unallowed_string_shape`);
    }
    return violations;
  }
  if (Array.isArray(body)) {
    body.forEach((entry, index) => {
      violations.push(
        ...scanCloudBodyForForbiddenContent(entry, `${path}[${index}]`),
      );
    });
    return violations;
  }
  if (typeof body === "object") {
    for (const [key, value] of Object.entries(body as Record<string, unknown>)) {
      if (!/^[A-Za-z][A-Za-z0-9_]{0,31}$/.test(key)) {
        violations.push(`${path}.${key}:unallowed_key_shape`);
      }
      violations.push(...scanCloudBodyForForbiddenContent(value, `${path}.${key}`));
    }
    return violations;
  }
  violations.push(`${path}:unallowed_value_type`);
  return violations;
}

export type CloudBody = {
  schema: typeof CAPACITY_SYNC_BODY_SCHEMA;
  v: typeof PROVIDER_PROFILES_SCHEMA_VERSION;
  items: Array<{
    k: string;
    e: number;
    d: string;
    u: CapacitySyncUnit;
    f: CapacitySyncFreshness;
    limit: number | null;
    used: number | null;
    seq: number;
    queuedAt: string;
  }>;
  watermark: { lastDeviceSeq: number; lastDeliveredDeviceSeq: number };
};

/** Build the wire body from the current queue and fail closed if any part
 * of it violates the cloud allowlist. Contains no labels/homes/identity. */
export function buildCloudBody(state: ProfileRegistryState): CloudBody {
  const body: CloudBody = {
    schema: CAPACITY_SYNC_BODY_SCHEMA,
    v: PROVIDER_PROFILES_SCHEMA_VERSION,
    items: state.queue.map((queued) => ({
      k: queued.item.k,
      e: queued.item.e,
      d: queued.item.d,
      u: queued.item.u,
      f: queued.item.f,
      limit: queued.item.limit,
      used: queued.item.used,
      seq: queued.deviceSeq,
      queuedAt: queued.queuedAt,
    })),
    watermark: {
      lastDeviceSeq: state.sequence.lastDeviceSeq,
      lastDeliveredDeviceSeq: state.sync.lastDeliveredDeviceSeq,
    },
  };
  const violations = scanCloudBodyForForbiddenContent(body);
  if (violations.length > 0) {
    throw new Error(`cloud_body_privacy_violation:${violations.join(";")}`);
  }
  return body;
}

export function signCapacitySyncBody(
  body: CloudBody,
  secretHex: string,
  deps: RegistryDeps = {},
): string {
  if (!/^[0-9a-f]{32,128}$/.test(secretHex)) {
    throw new Error("invalid signing secret");
  }
  return (deps.hmac ?? defaultHmac)(secretHex, canonicalJson(body));
}

export function verifyCapacitySyncBodySignature(
  body: CloudBody,
  signature: string,
  secretHex: string,
  deps: RegistryDeps = {},
): boolean {
  try {
    const expected = signCapacitySyncBody(body, secretHex, deps);
    if (expected.length !== signature.length) return false;
    let mismatch = 0;
    for (let index = 0; index < expected.length; index++) {
      mismatch |= expected.charCodeAt(index) ^ signature.charCodeAt(index);
    }
    return mismatch === 0;
  } catch {
    return false;
  }
}

export type EnqueueResult =
  | { ok: true; deviceSeq: number; state: ProfileRegistryState }
  | { ok: false; reason: string; state: ProfileRegistryState };

/**
 * Enqueue one sanitized snapshot for a profile. Refuses (without mutating)
 * unless sync is enabled and the observed home still matches registration.
 */
export function enqueueCapacitySnapshot(
  state: ProfileRegistryState,
  input: {
    label: string;
    observedHome?: ProviderHomeBinding;
    facts: CapacityFactInput[];
  },
  deps: RegistryDeps = {},
): EnqueueResult {
  const now = requireIsoTimestamp("(deps.now)", (deps.now ?? isoNow)());
  if (state.sync.state !== "enabled") {
    return { ok: false, reason: `sync_${state.sync.state}`, state };
  }
  const record = findProfile(state, requireLabel(input.label));
  if (!record) throw new Error("unknown_profile");
  if (record.revokedAt !== null) {
    return { ok: false, reason: "profile_revoked", state };
  }
  if (input.observedHome !== undefined) {
    const drift = checkHomeDrift(state, {
      label: input.label,
      observedHome: input.observedHome,
    });
    if (drift.rotationRequired) {
      // No inference, no auto-rebinding: the owner must rotate explicitly.
      return { ok: false, reason: "rotation_required", state };
    }
  }
  const sanitized = input.facts.map((fact) =>
    sanitizeCapacityFact(fact, { profileKey: record.profileKey, epoch: record.epoch }),
  );
  let seq = state.sequence.lastDeviceSeq;
  const queued = sanitized.map((item) => {
    seq += 1;
    return { deviceSeq: seq, profileKey: record.profileKey, item, queuedAt: now };
  });
  return {
    ok: true,
    deviceSeq: seq,
    state: {
      ...state,
      queue: [...state.queue, ...queued],
      sequence: { lastDeviceSeq: seq },
    },
  };
}

export function previewCapacitySync(state: ProfileRegistryState): {
  syncState: SyncLifecycleState;
  pendingItems: number;
  nextBody: CloudBody;
} {
  return {
    syncState: state.sync.state,
    pendingItems: state.queue.length,
    nextBody: buildCloudBody(state),
  };
}

export function enableCapacitySync(
  state: ProfileRegistryState,
  input: { confirmation: string },
  deps: RegistryDeps = {},
): ProfileRegistryState {
  if (state.sync.state === "revoked") {
    throw new Error("revoked_sync_requires_delete_and_reregistration");
  }
  if (state.sync.state === "enabled") {
    throw new Error("sync_already_enabled");
  }
  requireConfirmation("enable-capacity-sync", input.confirmation);
  return {
    ...state,
    sync: {
      ...state.sync,
      state: "enabled",
      enabledAt: requireIsoTimestamp("(deps.now)", (deps.now ?? isoNow)()),
      pausedAt: null,
    },
  };
}

export function pauseCapacitySync(
  state: ProfileRegistryState,
  deps: RegistryDeps = {},
): ProfileRegistryState {
  if (state.sync.state !== "enabled") {
    throw new Error("pause_requires_enabled_sync");
  }
  return {
    ...state,
    sync: {
      ...state.sync,
      state: "paused",
      pausedAt: requireIsoTimestamp("(deps.now)", (deps.now ?? isoNow)()),
    },
  };
}

export function revokeCapacitySync(
  state: ProfileRegistryState,
  input: { confirmation: string },
  deps: RegistryDeps = {},
): ProfileRegistryState {
  if (state.sync.state === "revoked") {
    throw new Error("sync_already_revoked");
  }
  requireConfirmation("revoke-capacity-sync", input.confirmation);
  return {
    ...state,
    sync: {
      ...state.sync,
      state: "revoked",
      revokedAt: requireIsoTimestamp("(deps.now)", (deps.now ?? isoNow)()),
    },
  };
}

export function deleteProviderProfile(
  state: ProfileRegistryState,
  input: { label: string; confirmation: string },
  deps: RegistryDeps = {},
): { state: ProfileRegistryState; removedQueueItems: number } {
  const label = requireLabel(input.label);
  const existing = findProfile(state, label);
  if (!existing) throw new Error("unknown_profile");
  requireConfirmation(`delete:${label}`, input.confirmation);
  const keptQueue = state.queue.filter((queued) => {
    return queued.profileKey !== existing.profileKey;
  });
  const receipt: DeletionReceipt = {
    kind: "deletion",
    label,
    removedQueueItems: state.queue.length - keptQueue.length,
    confirmedAt: requireIsoTimestamp("(deps.now)", (deps.now ?? isoNow)()),
  };
  return {
    state: {
      ...state,
      profiles: state.profiles.filter((record) => record.label !== label),
      queue: keptQueue,
      receipts: [...state.receipts, receipt],
    },
    removedQueueItems: receipt.removedQueueItems,
  };
}

/** Device-local full export (labels, homes, receipts, queue). This artifact
 * never goes to the cloud body path. */
export function exportLocalState(
  state: ProfileRegistryState,
): ProfileRegistryState {
  return JSON.parse(JSON.stringify(state)) as ProfileRegistryState;
}

export type DrainResult = {
  delivered: number;
  blockedBy: string | null;
  staleEpochDropped: number;
  failureClass: string | null;
};

/**
 * Attempt delivery of the whole pending batch. Blocked entirely while sync
 * is off/paused/revoked. Items whose profile epoch has since rotated are
 * dropped as stale (never delivered under a superseded key). Acknowledged
 * batches clear the queue and advance the delivered watermark.
 */
export function drainCapacityQueue(
  state: ProfileRegistryState,
  input: {
    transport: (body: CloudBody, signature: string) => "acknowledged" | string;
    signingSecretHex: string;
  },
  deps: RegistryDeps = {},
): { result: DrainResult; state: ProfileRegistryState } {
  if (state.sync.state !== "enabled") {
    return {
      result: { delivered: 0, blockedBy: `sync_${state.sync.state}`, staleEpochDropped: 0, failureClass: null },
      state,
    };
  }
  const epochByKey = new Map<string, number>();
  for (const record of state.profiles) epochByKey.set(record.profileKey, record.epoch);
  const current = state.queue.filter((queued) => {
    const epoch = epochByKey.get(queued.profileKey);
    return epoch !== undefined && epoch === queued.item.e;
  });
  const staleEpochDropped = state.queue.length - current.length;
  if (current.length === 0) {
    return {
      result: { delivered: 0, blockedBy: null, staleEpochDropped, failureClass: null },
      state: { ...state, queue: [] },
    };
  }
  const batchState: ProfileRegistryState = { ...state, queue: current };
  const body = buildCloudBody(batchState);
  const signature = signCapacitySyncBody(body, input.signingSecretHex, deps);
  const outcome = input.transport(body, signature);
  if (outcome !== "acknowledged") {
    return {
      result: { delivered: 0, blockedBy: null, staleEpochDropped, failureClass: outcome },
      state,
    };
  }
  const maxSeq = current.reduce((acc, queued) => Math.max(acc, queued.deviceSeq), 0);
  return {
    result: { delivered: current.length, blockedBy: null, staleEpochDropped, failureClass: null },
    state: {
      ...batchState,
      queue: [],
      sync: {
        ...batchState.sync,
        lastDeliveredDeviceSeq: Math.max(batchState.sync.lastDeliveredDeviceSeq, maxSeq),
      },
    },
  };
}

/** Local self-view works regardless of sync state — including after revoke. */
export function localSelfView(state: ProfileRegistryState): {
  syncState: SyncLifecycleState;
  profiles: Array<{
    label: string;
    provider: string;
    home: ProviderHomeBinding;
    epoch: number;
    identityStatus: IdentityStatus;
    revoked: boolean;
  }>;
  queueDepth: number;
  lastDeviceSeq: number;
} {
  return {
    syncState: state.sync.state,
    profiles: state.profiles.map((record) => ({
      label: record.label,
      provider: record.provider,
      home: { ...record.home },
      epoch: record.epoch,
      identityStatus: record.identityStatus,
      revoked: record.revokedAt !== null,
    })),
    queueDepth: state.queue.length,
    lastDeviceSeq: state.sequence.lastDeviceSeq,
  };
}

export type ProbeCostProofInput = {
  samplesMs: number[];
  budgetP95Ms?: number;
};

export type ProbeCostProofReceipt = {
  authorized: boolean;
  sampleCount: number;
  p95Ms: number | null;
  budgetP95Ms: number;
  reason: string;
};

/**
 * Measure-based gate for the 15-minute fallback timer. Authorized ONLY when
 * at least PROBE_COST_MIN_SAMPLES real measurements exist and their p95 is
 * within budget. Fabricated verdicts carry no weight — callers pass samples,
 * never conclusions.
 */
export function evaluateProbeCostProof(
  input: ProbeCostProofInput,
): ProbeCostProofReceipt {
  const budgetP95Ms = input.budgetP95Ms ?? PROBE_COST_BUDGET_P95_MS;
  const samples = input.samplesMs;
  const invalid = samples.some(
    (sample) => typeof sample !== "number" || !Number.isFinite(sample) || sample < 0,
  );
  if (invalid) {
    return { authorized: false, sampleCount: samples.length, p95Ms: null, budgetP95Ms, reason: "invalid_samples" };
  }
  if (samples.length < PROBE_COST_MIN_SAMPLES) {
    return {
      authorized: false,
      sampleCount: samples.length,
      p95Ms: null,
      budgetP95Ms,
      reason: `insufficient_samples:${samples.length}/${PROBE_COST_MIN_SAMPLES}`,
    };
  }
  const sorted = [...samples].sort((a, b) => a - b);
  const p95 = sorted[Math.ceil(0.95 * sorted.length) - 1]!;
  if (p95 > budgetP95Ms) {
    return {
      authorized: false,
      sampleCount: samples.length,
      p95Ms: p95,
      budgetP95Ms,
      reason: `p95_over_budget:${p95}>${budgetP95Ms}`,
    };
  }
  return { authorized: true, sampleCount: samples.length, p95Ms: p95, budgetP95Ms, reason: "p95_within_budget" };
}

/**
 * Authorize the 15-minute fallback timer from probe samples. The receipt's
 * conclusion is IGNORED — samples are re-evaluated locally so a forged
 * `authorized:true` receipt cannot flip the flag.
 */
export function authorizeFallbackTimerFromProof(
  state: ProfileRegistryState,
  proof: ProbeCostProofInput & { authorized?: boolean },
  deps: RegistryDeps = {},
): { state: ProfileRegistryState; receipt: ProbeCostProofReceipt } {
  const receipt = evaluateProbeCostProof({
    samplesMs: proof.samplesMs,
    budgetP95Ms: proof.budgetP95Ms,
  });
  if (!receipt.authorized) {
    return { state, receipt };
  }
  return {
    state: {
      ...state,
      sync: {
        ...state.sync,
        fallbackTimerAuthorized: true,
        fallbackAuthorizedAt: requireIsoTimestamp("(deps.now)", (deps.now ?? isoNow)()),
      },
    },
    receipt,
  };
}

export type SchedulePolicy = {
  eventDebounceMs: number;
  fallbackEnabled: boolean;
  fallbackIntervalMs: number;
  basis: string;
};

export function schedulePolicy(state: ProfileRegistryState): SchedulePolicy {
  const fallbackEnabled = state.sync.fallbackTimerAuthorized && state.sync.state === "enabled";
  return {
    eventDebounceMs: DEFAULT_EVENT_DEBOUNCE_MS,
    fallbackEnabled,
    fallbackIntervalMs: FALLBACK_INTERVAL_MS,
    basis: fallbackEnabled
      ? "probe_cost_proof_measured_p95_within_budget"
      : "fallback_blocked_until_probe_cost_proof_passes",
  };
}

export type MergeSideIdentity = {
  identityStatus: IdentityStatus;
  /** Only meaningful when identityStatus is STABLE_PROVIDER_IDENTITY. */
  identityHash: string | null;
};

/**
 * Cross-machine merge gate: allowed only with matching stable provider
 * identity on both sides OR an explicit participant-approved merge. Because
 * Codex identity is UNDETECTABLE_WITH_ALLOWED_METHODS, any merge involving a
 * Codex side requires participant approval — it can never qualify via
 * identity alone.
 */
export function assertCrossMachineMergeAllowed(input: {
  left: MergeSideIdentity;
  right: MergeSideIdentity;
  approval?: { approved: boolean; approvedBy: string; approvedAt: string };
}): void {
  const identityMatched =
    input.left.identityStatus === IDENTITY_STATUS_STABLE &&
    input.right.identityStatus === IDENTITY_STATUS_STABLE &&
    input.left.identityHash !== null &&
    input.left.identityHash === input.right.identityHash;
  if (identityMatched) return;
  const approval = input.approval;
  if (
    approval &&
    approval.approved === true &&
    typeof approval.approvedBy === "string" &&
    approval.approvedBy.length > 0 &&
    typeof approval.approvedAt === "string" &&
    Number.isFinite(Date.parse(approval.approvedAt))
  ) {
    return;
  }
  throw new Error(
    "cross-machine merge requires matching stable provider identity or participant-approved merge",
  );
}
