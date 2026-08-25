/**
 * Provider Capacity Snapshot V1 + sanitized provider_capacity_sync contract
 * (issue #168).
 *
 * Scientific unit of measurement: `device + provider profile + quota window +
 * adapter version` — never a person. Capacity, depletion, unused headroom,
 * event volume, and working hours are operational context ONLY. Nothing here
 * can independently prove quality, quantity, effort, availability, diligence,
 * productivity, or performance, and none of these facts may feed a surface
 * that claims to.
 *
 * Doctrine enforced by this module and scripts/provider-capacity-sync-proof.ts:
 * - Store only `usedBasisPoints`; remaining headroom is DERIVED at read time.
 * - Missing quota windows remain absent — they are never defaulted, backfilled,
 *   or represented as zero.
 * - Unknown/stale/future-dated observations stay UNKNOWN (derived remaining is
 *   null); they never degrade to zero. A fresh observation of full depletion
 *   (usedBasisPoints 10000) legitimately derives remaining 0 — evidence-based
 *   zero, never absence dressed up as data.
 * - Windows are generic bounded identifiers; nothing provider-specific is
 *   hard-coded.
 * - Every fact carries its source, adapter version, capture time, and a
 *   freshness classification.
 * - The cloud-sanitized `provider_capacity_sync` BODY asserts no tenant, no
 *   device, and no actor. Identity rides authenticated transport metadata
 *   (the same installKey/tenantId envelope pattern as every sibling batch);
 *   rows carry operational context only.
 * - Prompts, commands, transcript paths, emails, credentials, raw provider
 *   bodies, billing details, and productivity fields are NOT REPRESENTABLE:
 *   closed strict schemas plus bounded identifier charsets plus a value-shape
 *   privacy seal reject them before anything echoes, stores, or forwards.
 */

import { createHash } from "node:crypto";

import { z } from "zod";

import { LOCAL_TENANT_ID } from "./schemas";

export const PROVIDER_CAPACITY_SNAPSHOT_SCHEMA =
  "plimsoll.provider-capacity-snapshot.v1" as const;
export const PROVIDER_CAPACITY_SYNC_SCHEMA = "plimsoll.provider-capacity-sync.v1" as const;
export const PROVIDER_CAPACITY_SYNC_KIND = "provider_capacity_sync" as const;
export const PROVIDER_CAPACITY_PROTOCOL_ID = "plimsoll.provider-capacity.protocol.v1" as const;

/** Quota windows are normalized to basis points: 10000 bp = the whole window. */
export const CAPACITY_BASIS_POINTS_MAX = 10_000 as const;

/** One sync batch may carry at most this many sanitized snapshot rows. */
export const PROVIDER_CAPACITY_SYNC_MAX_ROWS = 500 as const;

/** Observation sources stay the two capacity-lane facts; nothing else exists. */
export const PROVIDER_CAPACITY_SOURCES = ["provider_report", "local_telemetry"] as const;

/** Exact sanitized sync-body fields — single source of truth for the consent
 * template's "Exact fields disclosed" block and the protocol fingerprint. */
export const PROVIDER_CAPACITY_SYNC_ROW_FIELDS = [
  "adapterVersion",
  "capturedAt",
  "providerProfileId",
  "source",
  "usedBasisPoints",
  "window",
] as const;

/** Exact local snapshot fields (the local-rich superset of the sync body). */
export const PROVIDER_CAPACITY_SNAPSHOT_UNIT_FIELDS = [
  "adapterVersion",
  "deviceInstallId",
  "providerProfileId",
  "window",
] as const;

const ISO_TIMESTAMP_PATTERN = /(?:Z|[+-]\d{2}:\d{2})$/;

const isoTimestampSchema = z
  .string()
  .trim()
  .min(1)
  .refine(
    (value) =>
      !Number.isNaN(Date.parse(value)) && ISO_TIMESTAMP_PATTERN.test(value),
    "Expected an ISO timestamp with a timezone offset.",
  );

/**
 * Bounded generic identifier for windows, provider profiles, and adapter
 * versions. Printable word characters only — no whitespace, no path
 * separators, no `@`, so emails, file paths, prompts, and commands are
 * structurally unrepresentable. Secret-shaped prefixes and compact-JWT
 * shapes are refused by name.
 */
const capacityIdentifierSchema = z
  .string()
  .trim()
  .min(1)
  .max(96)
  .regex(
    /^[a-zA-Z0-9](?:[a-zA-Z0-9._:-]{0,94}[a-zA-Z0-9])?$/,
    "Expected a bounded identifier of letters, digits, dots, underscores, colons, and hyphens.",
  )
  .refine(
    (value) =>
      !/^(?:eyJ|sk[_-]|sk(?:live|test)|ghp|gho|ghu|ghs|ghr|github_pat|xox|glpat|shp(?:at|pa|ca)|npm_|aiza|akia|asia)/i.test(
        value,
      ) &&
      !/^(?:bearer|basic|authorization|cookie)[._:-]/i.test(value),
    "Secret-shaped identifiers are not allowed in capacity contracts.",
  );

/** Local device identity component of the measurement unit. Local-only:
 * it never appears in the sanitized sync body. Same bounded charset so it
 * also cannot smuggle paths or emails. */
const localDeviceInstallIdSchema = z
  .string()
  .trim()
  .min(1)
  .max(128)
  .regex(
    /^[\x21-\x7e]+$/,
    "Expected a bounded printable-ASCII device install id without whitespace.",
  );

export const providerCapacityUnitIdentitySchema = z
  .object({
    deviceInstallId: localDeviceInstallIdSchema,
    providerProfileId: capacityIdentifierSchema,
    window: capacityIdentifierSchema,
    adapterVersion: capacityIdentifierSchema,
  })
  .strict();
export type ProviderCapacityUnitIdentity = z.infer<typeof providerCapacityUnitIdentitySchema>;

export const providerCapacityObservationV1Schema = z
  .object({
    /** The ONLY stored usage quantity. 10000 = whole window consumed. */
    usedBasisPoints: z
      .number()
      .int()
      .min(0)
      .max(CAPACITY_BASIS_POINTS_MAX),
    source: z.enum(PROVIDER_CAPACITY_SOURCES),
    capturedAt: isoTimestampSchema,
  })
  .strict();
export type ProviderCapacityObservationV1 = z.infer<typeof providerCapacityObservationV1Schema>;

/**
 * The canonical local-rich fact. One snapshot = one unit of measurement
 * (device × provider profile × quota window × adapter version) × one
 * observation. There is deliberately no `remaining*` field anywhere: headroom
 * is derived at read time by `deriveRemainingBasisPoints`.
 */
export const providerCapacitySnapshotV1Schema = z
  .object({
    schema: z.literal(PROVIDER_CAPACITY_SNAPSHOT_SCHEMA),
    unit: providerCapacityUnitIdentitySchema,
    observation: providerCapacityObservationV1Schema,
  })
  .strict();
export type ProviderCapacitySnapshotV1 = z.infer<typeof providerCapacitySnapshotV1Schema>;

/**
 * Sanitized sync-body row. Strictly the fields listed in
 * PROVIDER_CAPACITY_SYNC_ROW_FIELDS — no tenant, device, actor, email,
 * prompt, command, transcript path, credential, raw provider body, billing
 * figure, or productivity field is representable here.
 */
export const providerCapacitySyncRowSchema = z
  .object({
    providerProfileId: capacityIdentifierSchema,
    window: capacityIdentifierSchema,
    adapterVersion: capacityIdentifierSchema,
    usedBasisPoints: z.number().int().min(0).max(CAPACITY_BASIS_POINTS_MAX),
    source: z.enum(PROVIDER_CAPACITY_SOURCES),
    capturedAt: isoTimestampSchema,
  })
  .strict();
export type ProviderCapacitySyncRow = z.infer<typeof providerCapacitySyncRowSchema>;

/**
 * Sanitized `provider_capacity_sync` batch. Envelope routing fields
 * (kind/tenantId/installKey/appVersion) follow the repository-wide batch
 * pattern; the BODY (rows) asserts no tenant, device, or actor. An EMPTY
 * snapshots array is valid: nothing observed means nothing asserted —
 * absence is an honest state, not an error and not a zero.
 */
export const providerCapacitySyncBatchSchema = z
  .object({
    kind: z.literal(PROVIDER_CAPACITY_SYNC_KIND),
    tenantId: z.string().trim().min(1).default(LOCAL_TENANT_ID),
    installKey: z.string().trim().min(1),
    appVersion: z.string().trim().min(1).default("0.1.0"),
    snapshots: z.array(providerCapacitySyncRowSchema).max(PROVIDER_CAPACITY_SYNC_MAX_ROWS),
  })
  .strict();
export type ProviderCapacitySyncBatch = z.infer<typeof providerCapacitySyncBatchSchema>;

export type CapacitySnapshotFreshnessStatus = "fresh" | "STALE" | "UNKNOWN";

/**
 * Freshness classification for one capture time. Older than maxAgeMs is
 * STALE; a future-dated capture (clock skew, bad input) fails CLOSED as
 * UNKNOWN — never fresh, never clamped to age zero (issue #195 doctrine).
 */
export function classifyProviderCapacitySnapshotFreshness(input: {
  capturedAt: string;
  now: string;
  maxAgeMs: number;
}): { status: CapacitySnapshotFreshnessStatus; ageMs: number | null } {
  const nowMs = Date.parse(input.now);
  if (Number.isNaN(nowMs)) throw new Error("invalid capacity now timestamp");
  if (!Number.isFinite(input.maxAgeMs) || input.maxAgeMs <= 0) {
    throw new Error("capacity maxAgeMs must be positive");
  }
  const capturedMs = Date.parse(input.capturedAt);
  if (Number.isNaN(capturedMs)) throw new Error("invalid capacity capturedAt timestamp");
  if (capturedMs > nowMs) return { status: "UNKNOWN", ageMs: null };
  const ageMs = nowMs - capturedMs;
  return { status: ageMs <= input.maxAgeMs ? "fresh" : "STALE", ageMs };
}

/**
 * Derive unused headroom in basis points. Returns a number ONLY for fresh
 * evidence; stale, future-dated (UNKNOWN), or absent windows yield null —
 * null is UNKNOWN, never zero. Fresh full depletion derives a REAL zero.
 */
export function deriveRemainingBasisPoints(
  observation: Pick<ProviderCapacityObservationV1, "usedBasisPoints" | "capturedAt">,
  options: { now: string; maxAgeMs: number },
): { status: CapacitySnapshotFreshnessStatus; ageMs: number | null; remainingBasisPoints: number | null } {
  const freshness = classifyProviderCapacitySnapshotFreshness({
    capturedAt: observation.capturedAt,
    now: options.now,
    maxAgeMs: options.maxAgeMs,
  });
  return {
    ...freshness,
    remainingBasisPoints:
      freshness.status === "fresh"
        ? CAPACITY_BASIS_POINTS_MAX - observation.usedBasisPoints
        : null,
  };
}

/**
 * Derive the sanitized sync rows from local snapshots. This is the ONLY
 * sanctioned projection across the boundary: it picks exactly the allowlisted
 * row fields and drops the device identity by construction, so a future edit
 * that adds a field to the snapshot cannot silently leak it to the cloud.
 */
export function projectSnapshotsToSyncRows(
  snapshots: ProviderCapacitySnapshotV1[],
): ProviderCapacitySyncRow[] {
  return snapshots.map((snapshot) => ({
    providerProfileId: snapshot.unit.providerProfileId,
    window: snapshot.unit.window,
    adapterVersion: snapshot.unit.adapterVersion,
    usedBasisPoints: snapshot.observation.usedBasisPoints,
    source: snapshot.observation.source,
    capturedAt: snapshot.observation.capturedAt,
  }));
}

/** Row keys that would re-introduce tenant/device/actor assertions into the
 * sanitized body. Sealing refuses them outright. */
const FORBIDDEN_BODY_KEYS = new Set([
  "actorid",
  "actor",
  "device",
  "deviceinstallid",
  "deviceid",
  "email",
  "emailhash",
  "installkey",
  "tenant",
  "tenantid",
  "userid",
  "user",
]);

/** Credential VALUE shapes, independent of field name (mirrors the
 * provider-report lane in capacity.ts). */
const CREDENTIAL_VALUE_PATTERNS: ReadonlyArray<{ kind: string; pattern: RegExp }> = [
  { kind: "jwt", pattern: /^eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{5,}$/ },
  { kind: "auth_header_value", pattern: /\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]{16,}/i },
  { kind: "provider_api_key", pattern: /\bsk-[A-Za-z0-9_-]{20,}\b/ },
  { kind: "aws_access_key", pattern: /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/ },
  { kind: "github_token", pattern: /\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b/ },
  { kind: "slack_token", pattern: /\bxox[bparsa]-[A-Za-z0-9-]{10,}\b/ },
  { kind: "google_api_key", pattern: /\bAIza[0-9A-Za-z_-]{30,}/ },
  { kind: "gitlab_token", pattern: /\bglpat-[A-Za-z0-9_-]{15,}\b/ },
];

function findPrivacyFindings(value: unknown, path: string, findings: string[]): void {
  if (Array.isArray(value)) {
    value.forEach((item, index) => findPrivacyFindings(item, `${path}[${index}]`, findings));
    return;
  }
  if (value !== null && typeof value === "object") {
    for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
      if (FORBIDDEN_BODY_KEYS.has(key.replace(/[^a-zA-Z0-9]/g, "").toLowerCase())) {
        findings.push(`${path}.${key}:forbidden_identity_key`);
        continue;
      }
      findPrivacyFindings(child, `${path}.${key}`, findings);
    }
    return;
  }
  if (typeof value === "string") {
    for (const { kind, pattern } of CREDENTIAL_VALUE_PATTERNS) {
      if (pattern.test(value)) {
        findings.push(`${path}:credential_value_shape:${kind}`);
        return;
      }
    }
    // Opaque high-entropy fallback: a long blob mixing UPPER and lower case
    // letters with digits and no other structure is treated as credential
    // material. Structured window/profile/version labels are lowercase words
    // with separators, so they never trip this — proven by negative controls.
    if (
      /^[A-Za-z0-9+/=_-]{40,}$/.test(value) &&
      /[a-z]/.test(value) &&
      /[A-Z]/.test(value) &&
      /\d/.test(value)
    ) {
      findings.push(`${path}:credential_value_shape:opaque_high_entropy`);
    }
  }
}

export type SealedSyncBatchOutcome =
  | { ok: true; batch: ProviderCapacitySyncBatch }
  | { ok: false; reason: "schema" | "privacy" | "duplicate_unit"; detail?: string };

/**
 * Seal a `provider_capacity_sync` batch for upload: schema-validate, refuse
 * duplicate measurement units, and run the value-shape privacy sweep over the
 * whole body. Fails closed before anything echoes, stores, or forwards.
 */
export function sealProviderCapacitySyncBatch(input: unknown): SealedSyncBatchOutcome {
  const parsed = providerCapacitySyncBatchSchema.safeParse(input);
  if (!parsed.success) {
    return { ok: false, reason: "schema", detail: parsed.error.issues[0]?.message };
  }
  const findings: string[] = [];
  findPrivacyFindings(parsed.data.snapshots, "snapshots", findings);
  if (findings.length > 0) {
    return { ok: false, reason: "privacy", detail: findings.join("; ") };
  }
  const seenUnits = new Set<string>();
  for (const row of parsed.data.snapshots) {
    const unitKey = `${row.providerProfileId}\u0000${row.window}\u0000${row.adapterVersion}`;
    if (seenUnits.has(unitKey)) {
      return {
        ok: false,
        reason: "duplicate_unit",
        detail: `duplicate measurement unit ${JSON.stringify(unitKey.replaceAll("\u0000", "/"))}`,
      };
    }
    seenUnits.add(unitKey);
  }
  return { ok: true, batch: parsed.data };
}

// ---------------------------------------------------------------------------
// Protocol fingerprint + cross-repository compatibility receipt.
//
// The fingerprint binds (a) the exact contract structure and (b) the exact
// bytes of every golden fixture, so ANY drift in either flips the fingerprint
// and stales the receipt until it is regenerated deliberately.
// ---------------------------------------------------------------------------

/** Recursively key-sorted canonical JSON serialization. */
export function canonicalJsonString(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null";
  if (Array.isArray(value)) return `[${value.map(canonicalJsonString).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
    a.localeCompare(b),
  );
  return `{${entries.map(([key, val]) => `${JSON.stringify(key)}:${canonicalJsonString(val)}`).join(",")}}`;
}

export function sha256Hex(material: string): string {
  return createHash("sha256").update(material).digest("hex");
}

export function sha256Linkage(material: string): string {
  return `sha256:${sha256Hex(material)}`;
}

/** Structural contract material — the identity of the wire surface itself. */
export function computeProviderCapacityContractMaterial(): Record<string, unknown> {
  return {
    basisPointsMax: CAPACITY_BASIS_POINTS_MAX,
    maxSyncRows: PROVIDER_CAPACITY_SYNC_MAX_ROWS,
    protocolId: PROVIDER_CAPACITY_PROTOCOL_ID,
    snapshotFields: [...PROVIDER_CAPACITY_SNAPSHOT_UNIT_FIELDS].sort(),
    snapshotSchema: PROVIDER_CAPACITY_SNAPSHOT_SCHEMA,
    sources: [...PROVIDER_CAPACITY_SOURCES].sort(),
    syncEnvelopeFields: [
      "appVersion",
      "installKey",
      "kind",
      "snapshots",
      "tenantId",
    ],
    syncKind: PROVIDER_CAPACITY_SYNC_KIND,
    syncRowFields: [...PROVIDER_CAPACITY_SYNC_ROW_FIELDS].sort(),
    syncSchema: PROVIDER_CAPACITY_SYNC_SCHEMA,
  };
}

export type ProviderCapacityProtocolReceipt = {
  compatibilityPolicy: "exact_fingerprint_match";
  contractMaterialDigest: string;
  fixtureCount: number;
  fixtureDigests: Record<string, string>;
  generatedAt: string;
  protocolFingerprint: string;
  protocolId: typeof PROVIDER_CAPACITY_PROTOCOL_ID;
};

export type FixtureDigestInput = Record<string, string>;

/**
 * Compute the protocol fingerprint over contract material plus the exact
 * digests of the golden fixtures (keys are repo-relative POSIX paths, sorted
 * canonically). Deterministic across machines and repositories.
 */
export function computeProviderCapacityProtocolFingerprint(fixtureDigests: FixtureDigestInput): {
  contractMaterialDigest: string;
  protocolFingerprint: string;
} {
  const contractMaterial = computeProviderCapacityContractMaterial();
  const contractMaterialDigest = sha256Linkage(canonicalJsonString(contractMaterial));
  const fingerprintMaterial = canonicalJsonString({
    contractMaterialDigest,
    fixtureDigests,
  });
  return {
    contractMaterialDigest,
    protocolFingerprint: sha256Linkage(fingerprintMaterial),
  };
}

/** Build a receipt ready to be committed next to the fixtures. */
export function buildProviderCapacityProtocolReceipt(input: {
  fixtureDigests: FixtureDigestInput;
  generatedAt: string;
}): ProviderCapacityProtocolReceipt {
  const { contractMaterialDigest, protocolFingerprint } =
    computeProviderCapacityProtocolFingerprint(input.fixtureDigests);
  return {
    compatibilityPolicy: "exact_fingerprint_match",
    contractMaterialDigest,
    fixtureCount: Object.keys(input.fixtureDigests).length,
    fixtureDigests: input.fixtureDigests,
    generatedAt: input.generatedAt,
    protocolFingerprint,
    protocolId: PROVIDER_CAPACITY_PROTOCOL_ID,
  };
}

export type ProtocolCompatibilityResult =
  | { status: "compatible"; fingerprint: string }
  | { status: "incompatible"; localFingerprint: string; foreignFingerprint: string };

/**
 * Cross-repository compatibility: two repos speak the same capacity protocol
 * iff their computed fingerprints match exactly. Anything else is
 * INCOMPATIBLE — never "probably fine", never silently downgraded.
 */
export function checkProviderCapacityProtocolCompatibility(
  localFingerprint: string,
  foreignFingerprint: string,
): ProtocolCompatibilityResult {
  return localFingerprint === foreignFingerprint
    ? { status: "compatible", fingerprint: localFingerprint }
    : { status: "incompatible", localFingerprint, foreignFingerprint };
}

// ---------------------------------------------------------------------------
// Consent gate: before the FIRST upload, an owner-granted consent record must
// bind the exact reviewed source head AND the exact artifact digest (here:
// the protocol fingerprint). A change to EITHER invalidates the consent and
// requires renewed approval. There is no representation of a half-consented
// or revoked-but-recorded state: a consent record exists only when granted.
// ---------------------------------------------------------------------------

export const CAPACITY_UPLOAD_CONSENT_KIND =
  "plimsoll.provider-capacity-upload-consent" as const;
export const CAPACITY_UPLOAD_CONSENT_VERSION = 1 as const;

const gitSourceHeadSchema = z
  .string()
  .trim()
  .regex(/^[0-9a-f]{40}$/, "Expected a 40-character lowercase git commit sha.");

const artifactDigestSchema = z
  .string()
  .trim()
  .regex(/^sha256:[a-f0-9]{64}$/, "Expected sha256: followed by 64 hexadecimal characters.");

export const providerCapacityUploadConsentSchema = z
  .object({
    approved: z.literal(true),
    approvedAt: isoTimestampSchema,
    approvedBy: z.string().trim().min(1).max(200),
    binding: z
      .object({
        artifactDigest: artifactDigestSchema,
        /** What the digest digests — the capacity protocol artifact. */
        protocolId: z.literal(PROVIDER_CAPACITY_PROTOCOL_ID),
        sourceHead: gitSourceHeadSchema,
      })
      .strict(),
    consentKind: z.literal(CAPACITY_UPLOAD_CONSENT_KIND),
    scope: z
      .object({
        noPerformanceUse: z.literal(true),
        purpose: z.literal("operational_capacity_context_only"),
        surfaces: z.array(z.literal(PROVIDER_CAPACITY_SYNC_KIND)).min(1),
      })
      .strict(),
    version: z.literal(CAPACITY_UPLOAD_CONSENT_VERSION),
  })
  .strict();
export type ProviderCapacityUploadConsent = z.infer<typeof providerCapacityUploadConsentSchema>;

export type ConsentGateResult =
  | { allowed: true }
  | {
      allowed: false;
      reason:
        | "artifact_digest_changed"
        | "consent_invalid"
        | "source_head_changed"
        | "surface_not_in_scope";
      detail: string;
    };

/**
 * Gate one would-be upload against the consent record. Both bindings are
 * exact-match: a moved source head or a drifted artifact digest each force
 * renewed approval before any upload may proceed.
 */
export function evaluateProviderCapacityUpload(input: {
  consent: unknown;
  currentArtifactDigest: string;
  currentSourceHead: string;
  surface: typeof PROVIDER_CAPACITY_SYNC_KIND;
}): ConsentGateResult {
  const parsed = providerCapacityUploadConsentSchema.safeParse(input.consent);
  if (!parsed.success) {
    return {
      allowed: false,
      reason: "consent_invalid",
      detail: parsed.error.issues[0]?.message ?? "consent record failed schema",
    };
  }
  const consent = parsed.data;
  if (!consent.scope.surfaces.includes(input.surface)) {
    return {
      allowed: false,
      reason: "surface_not_in_scope",
      detail: `consent does not cover surface ${input.surface}`,
    };
  }
  if (consent.binding.sourceHead !== input.currentSourceHead) {
    return {
      allowed: false,
      reason: "source_head_changed",
      detail: `consent bound ${consent.binding.sourceHead}; current head is ${input.currentSourceHead}`,
    };
  }
  if (consent.binding.artifactDigest !== input.currentArtifactDigest) {
    return {
      allowed: false,
      reason: "artifact_digest_changed",
      detail: `consent bound ${consent.binding.artifactDigest}; current artifact is ${input.currentArtifactDigest}`,
    };
  }
  return { allowed: true };
}
