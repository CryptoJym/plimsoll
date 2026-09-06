#!/usr/bin/env node

/**
 * Issue #168 proof: frozen Capacity Rail contract.
 *
 * Proves, against golden fixtures and adversarial cases:
 *  - ProviderCapacitySnapshotV1 + provider_capacity_sync parse exactly their
 *    documented shapes; the sanitized body asserts no tenant/device/actor.
 *  - Missing quota windows remain absent (never defaulted, never zero rows).
 *  - Unknown/stale/future-dated evidence never becomes zero; only fresh
 *    evidence derives headroom, and fresh full depletion earns a REAL zero.
 *  - Prompts, commands, transcript paths, emails, credentials, raw provider
 *    bodies, billing details, and productivity fields are unrepresentable.
 *  - The protocol fingerprint is deterministic, binds every fixture byte
 *    (drift-proof), and cross-repo compatibility is exact-match fail-closed.
 *  - The consent template stays bound to the code's exact field list, and the
 *    consent gate blocks uploads when the source head OR artifact digest has
 *    moved.
 *
 * Run: pnpm proof:capacity-contract
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import { guardProofCompletion } from "./lib/proof-completion";
import {
  CAPACITY_BASIS_POINTS_MAX,
  CAPACITY_UPLOAD_CONSENT_KIND,
  PROVIDER_CAPACITY_SYNC_ROW_FIELDS,
  buildProviderCapacityProtocolReceipt,
  canonicalJsonString,
  checkProviderCapacityProtocolCompatibility,
  classifyProviderCapacitySnapshotFreshness,
  computeProviderCapacityContractMaterial,
  computeProviderCapacityProtocolFingerprint,
  deriveRemainingBasisPoints,
  evaluateProviderCapacityUpload,
  projectSnapshotsToSyncRows,
  providerCapacitySnapshotV1Schema,
  providerCapacitySyncBatchSchema,
  sealProviderCapacitySyncBatch,
} from "../packages/shared/src/index";

const root = process.cwd();
const snapshotDir = path.join(
  root,
  "packages/shared/fixtures/capacity/provider-capacity-snapshot",
);
const consentTemplatePath = path.join(
  root,
  "docs/consent/provider-capacity-upload-consent-template-v1.md",
);

type Check = { name: string; detail: Record<string, unknown> };
const checks: Check[] = [];

function prove(name: string, condition: unknown, detail: Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

// Refuses the two silent-green modes — an early event-loop drain and a hang
// that never exits. See scripts/lib/proof-completion.ts for why each is needed.
const guard = guardProofCompletion({
  countChecks: () => checks.length,
});

function sha256File(fullPath: string): string {
  return `sha256:${createHash("sha256").update(fs.readFileSync(fullPath)).digest("hex")}`;
}

const NOW = "2026-08-21T00:00:00.000Z";
const MAX_AGE_MS = 6 * 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// A. Golden valid fixtures — exact shapes, exact sanitized row fields.
// ---------------------------------------------------------------------------
{
  const snapshotFixture = JSON.parse(
    fs.readFileSync(path.join(snapshotDir, "valid/snapshot-fresh.json"), "utf8"),
  );
  const parsedSnapshot = providerCapacitySnapshotV1Schema.safeParse(snapshotFixture);
  prove(
    "golden_snapshot_fixture_parses",
    parsedSnapshot.success && parsedSnapshot.data.observation.usedBasisPoints === 8200,
    { success: parsedSnapshot.success },
  );

  const batchFixture = JSON.parse(
    fs.readFileSync(path.join(snapshotDir, "valid/sync-batch.json"), "utf8"),
  );
  const parsedBatch = providerCapacitySyncBatchSchema.safeParse(batchFixture);
  assert.ok(parsedBatch.success);
  prove(
    "golden_sync_batch_fixture_parses_with_defaulted_envelope",
    parsedBatch.data.tenantId === "00000000-0000-4000-8000-000000000001" &&
      parsedBatch.data.appVersion === "0.1.0" &&
      parsedBatch.data.snapshots.length === 2,
    { tenantId: parsedBatch.data.tenantId },
  );

  // The sanitized body is EXACTLY the allowlisted fields — no more, ever.
  const rowKeySets = parsedBatch.data.snapshots.map((row) =>
    Object.keys(row).sort().join(","),
  );
  prove(
    "sync_body_rows_carry_exactly_the_documented_fields",
    rowKeySets.every(
      (keys) => keys === [...PROVIDER_CAPACITY_SYNC_ROW_FIELDS].sort().join(","),
    ),
    { rowKeySets },
  );

  const sealed = sealProviderCapacitySyncBatch(batchFixture);
  prove("golden_sync_batch_seals_clean", sealed.ok, { sealed });

  // Projection drops device identity by construction.
  const projected = projectSnapshotsToSyncRows([
    {
      schema: "plimsoll.provider-capacity-snapshot.v1",
      unit: {
        deviceInstallId: "secret-local-device-label",
        providerProfileId: "anthropic.max.primary",
        window: "five_hour",
        adapterVersion: "1.4.2",
      },
      observation: {
        usedBasisPoints: 8200,
        source: "provider_report",
        capturedAt: NOW,
      },
    },
  ]);
  prove(
    "projection_drops_device_identity_by_construction",
    projected.length === 1 &&
      !Object.keys(projected[0]!).includes("deviceInstallId") &&
      !JSON.stringify(projected).includes("secret-local-device-label"),
    { projected },
  );

  // Envelope top-level keys stay the repository-wide batch pattern.
  const envelopeKeys = Object.keys(parsedBatch.data).sort();
  prove(
    "sync_envelope_keys_match_repo_batch_pattern",
    JSON.stringify(envelopeKeys) ===
      JSON.stringify(["appVersion", "installKey", "kind", "snapshots", "tenantId"]),
    { envelopeKeys },
  );
}

// ---------------------------------------------------------------------------
// B. Missing windows remain absent.
// ---------------------------------------------------------------------------
{
  // An empty sync batch is valid and honest: nothing observed, nothing sent.
  const emptyBatch = providerCapacitySyncBatchSchema.safeParse({
    kind: "provider_capacity_sync",
    installKey: "ik-local-x",
    snapshots: [],
  });
  prove(
    "empty_snapshots_batch_is_valid_honest_absence",
    emptyBatch.success && emptyBatch.data.snapshots.length === 0,
    { success: emptyBatch.success },
  );

  // Only observed windows exist anywhere in derived views; querying an absent
  // window finds nothing rather than finding zero usage.
  const batch = providerCapacitySyncBatchSchema.parse({
    kind: "provider_capacity_sync",
    installKey: "ik-local-x",
    snapshots: [
      {
        providerProfileId: "p.one",
        window: "five_hour",
        adapterVersion: "1.4.2",
        usedBasisPoints: 8200,
        source: "provider_report",
        capturedAt: NOW,
      },
    ],
  });
  const seenWindows = new Set(batch.snapshots.map((row) => row.window));
  const absentLookup = batch.snapshots.find(
    (row) => row.providerProfileId === "p.one" && row.window === "weekly_all_models",
  );
  prove(
    "absent_window_stays_absent_never_zero_row",
    seenWindows.size === 1 && seenWindows.has("five_hour") && absentLookup === undefined,
    { seenWindows: [...seenWindows], absentLookup },
  );

  // Distinct adapter versions are DISTINCT measurement units of the same
  // window — both coexist instead of one overwriting or blending the other.
  const twoAdapterVersions = sealProviderCapacitySyncBatch({
    kind: "provider_capacity_sync",
    installKey: "ik-local-x",
    snapshots: [
      {
        providerProfileId: "p.one",
        window: "five_hour",
        adapterVersion: "1.4.2",
        usedBasisPoints: 1000,
        source: "local_telemetry",
        capturedAt: NOW,
      },
      {
        providerProfileId: "p.one",
        window: "five_hour",
        adapterVersion: "1.5.0",
        usedBasisPoints: 2000,
        source: "local_telemetry",
        capturedAt: NOW,
      },
    ],
  });
  prove(
    "same_window_across_adapter_versions_are_distinct_units_not_blended",
    twoAdapterVersions.ok && twoAdapterVersions.batch.snapshots.length === 2,
    { ok: twoAdapterVersions.ok },
  );
}

// ---------------------------------------------------------------------------
// C. Unknown/stale never become zero.
// ---------------------------------------------------------------------------
{
  const observation = (capturedAt: string, usedBasisPoints = 5000) => ({
    usedBasisPoints,
    capturedAt,
  });

  // Stale capture: remaining is null (UNKNOWN), never 10000-used.
  const stale = deriveRemainingBasisPoints(observation("2026-08-20T10:00:00.000Z"), {
    now: NOW,
    maxAgeMs: MAX_AGE_MS,
  });
  prove(
    "stale_capture_derives_null_remaining_never_zero",
    stale.status === "STALE" && stale.remainingBasisPoints === null && stale.ageMs !== null,
    { stale },
  );

  // Future-dated capture (clock skew): fails closed to UNKNOWN, never fresh.
  const future = deriveRemainingBasisPoints(observation("2026-08-21T06:00:00.000Z"), {
    now: NOW,
    maxAgeMs: MAX_AGE_MS,
  });
  prove(
    "future_dated_capture_fails_closed_unknown_never_clamped",
    future.status === "UNKNOWN" && future.remainingBasisPoints === null && future.ageMs === null,
    { future },
  );

  // Fresh full depletion earns a REAL zero; fresh zero usage earns FULL room.
  // These are evidence-based numbers, categorically different from UNKNOWN.
  const depleted = deriveRemainingBasisPoints(observation(NOW, CAPACITY_BASIS_POINTS_MAX), {
    now: NOW,
    maxAgeMs: MAX_AGE_MS,
  });
  const untouched = deriveRemainingBasisPoints(observation(NOW, 0), {
    now: NOW,
    maxAgeMs: MAX_AGE_MS,
  });
  prove(
    "fresh_full_depletion_derives_real_zero_and_fresh_empty_derives_full_room",
    depleted.status === "fresh" &&
      depleted.remainingBasisPoints === 0 &&
      untouched.status === "fresh" &&
      untouched.remainingBasisPoints === CAPACITY_BASIS_POINTS_MAX,
    { depleted, untouched },
  );

  // Freshness boundary is honest: age == maxAgeMs is fresh, one ms more is not.
  const edgeFresh = classifyProviderCapacitySnapshotFreshness({
    capturedAt: new Date(Date.parse(NOW) - MAX_AGE_MS).toISOString(),
    now: NOW,
    maxAgeMs: MAX_AGE_MS,
  });
  const edgeStale = classifyProviderCapacitySnapshotFreshness({
    capturedAt: new Date(Date.parse(NOW) - MAX_AGE_MS - 1).toISOString(),
    now: NOW,
    maxAgeMs: MAX_AGE_MS,
  });
  prove(
    "freshness_boundary_exact_max_age_fresh_one_ms_more_stale",
    edgeFresh.status === "fresh" && edgeStale.status === "STALE",
    { edgeFresh, edgeStale },
  );
}

// ---------------------------------------------------------------------------
// D. Unrepresentability — golden invalid fixtures plus layered adversarial
//    cases that try to smuggle forbidden content through each layer.
// ---------------------------------------------------------------------------
{
  type Wrapper = { mustReject: boolean; target: string; because: string; payload: unknown };
  const invalidDir = path.join(snapshotDir, "invalid");
  const rejected: Array<{ file: string; because: string }> = [];
  const acceptedWhenMustReject: string[] = [];

  for (const entry of fs.readdirSync(invalidDir).sort()) {
    if (!entry.endsWith(".json")) continue;
    const wrapper = JSON.parse(
      fs.readFileSync(path.join(invalidDir, entry), "utf8"),
    ) as Wrapper;
    assert.equal(wrapper.mustReject, true, entry);
    let refused: boolean;
    if (wrapper.target === "providerCapacitySnapshotV1") {
      refused = !providerCapacitySnapshotV1Schema.safeParse(wrapper.payload).success;
    } else if (wrapper.target === "providerCapacitySyncBatch") {
      refused = !providerCapacitySyncBatchSchema.safeParse(wrapper.payload).success;
    } else if (wrapper.target === "sealedSyncBatch") {
      refused = !sealProviderCapacitySyncBatch(wrapper.payload).ok;
    } else {
      throw new Error(`unknown fixture target ${wrapper.target} in ${entry}`);
    }
    if (refused) rejected.push({ file: entry, because: wrapper.because });
    else acceptedWhenMustReject.push(entry);
  }
  prove(
    "all_invalid_fixtures_rejected_by_their_named_layer",
    acceptedWhenMustReject.length === 0 && rejected.length >= 15,
    { rejectedCount: rejected.length, acceptedWhenMustReject },
  );

  const byFile = new Map(rejected.map((r) => [r.file, r.because]));
  const mandatory = [
    "snapshot-productivity-field.json",
    "snapshot-stored-remaining.json",
    "window-email-shape.json",
    "window-path-shape.json",
    "profile-prompt-text.json",
    "adapter-secret-shape.json",
    "sync-row-deviceid-in-body.json",
    "sync-row-tenantid-in-body.json",
    "sync-row-actor-email-in-body.json",
    "sync-row-prompt-field.json",
    "sync-row-billing-field.json",
    "sync-duplicate-unit.json",
  ];
  const missingRejections = mandatory.filter((file) => !byFile.has(file));
  prove(
    "identity_assertions_prompts_paths_emails_billing_and_productivity_all_refused",
    missingRejections.length === 0,
    { missingRejections, rejectedFiles: [...byFile.keys()] },
  );

  // Layered defense probe 1: classic secret-prefixed identifiers die at the
  // SCHEMA layer itself, with the refusal naming the rule (not the secret).
  const prefixedSmuggle = providerCapacitySyncBatchSchema.safeParse({
    kind: "provider_capacity_sync",
    installKey: "ik-local-x",
    snapshots: [
      {
        providerProfileId: "p.one",
        window: "ghp_abcdefghijklmnopqrstuvwxyz012345",
        adapterVersion: "1.4.2",
        usedBasisPoints: 1000,
        source: "local_telemetry",
        capturedAt: NOW,
      },
    ],
  });
  prove(
    "schema_layer_refuses_secret_prefixed_identifier_by_name",
    !prefixedSmuggle.success &&
      JSON.stringify(prefixedSmuggle.error.issues[0]?.message ?? "").includes("Secret-shaped"),
    {},
  );

  // Layered defense probe 2: a credential-SHAPED VALUE embedded mid-string
  // passes the schema charset AND prefix rules, so the seal's value sweep
  // must be the layer that catches it.
  const valueSmuggle = sealProviderCapacitySyncBatch({
    kind: "provider_capacity_sync",
    installKey: "ik-local-x",
    snapshots: [
      {
        providerProfileId: "p.one",
        window: "acme-sk-abcdefghijklmnopqrstuvwx",
        adapterVersion: "1.4.2",
        usedBasisPoints: 1000,
        source: "local_telemetry",
        capturedAt: NOW,
      },
    ],
  });
  prove(
    "seal_layer_rejects_secret_shaped_value_that_slips_charset",
    !valueSmuggle.ok && valueSmuggle.reason === "privacy",
    { valueSmuggle },
  );

  // Layer-2b: opaque high-entropy blob hidden in a window label passes the
  // charset and prefix rules but fails the entropy sweep.
  const opaqueBlob = "A1b2C3d4E5".repeat(5); // 50 chars mixed case+digits
  const opaqueSmuggle = sealProviderCapacitySyncBatch({
    kind: "provider_capacity_sync",
    installKey: "ik-local-x",
    snapshots: [
      {
        providerProfileId: "p.one",
        window: opaqueBlob,
        adapterVersion: "1.4.2",
        usedBasisPoints: 1000,
        source: "local_telemetry",
        capturedAt: NOW,
      },
    ],
  });
  prove(
    "seal_layer_rejects_opaque_high_entropy_blob_in_identifier_slot",
    !opaqueSmuggle.ok && opaqueSmuggle.reason === "privacy",
    { opaqueSmuggle },
  );

  // Negative controls: realistic structured labels must NOT trip the seal
  // (no false-positive lockout of legitimate windows).
  const realisticLabels = [
    "weekly_all_models",
    "five_hour_window_with_long_descriptive_suffix_v2",
    "anthropic.max.workspace-primary-2026",
  ];
  const negativeControl = sealProviderCapacitySyncBatch({
    kind: "provider_capacity_sync",
    installKey: "ik-local-x",
    snapshots: realisticLabels.map((window, index) => ({
      providerProfileId: `p.${index}`,
      window,
      adapterVersion: "1.4.2",
      usedBasisPoints: index * 100,
      source: "local_telemetry" as const,
      capturedAt: NOW,
    })),
  });
  prove("negative_control_realistic_labels_seal_clean", negativeControl.ok, {
    negativeControl,
  });

  // Nested-object smuggling under an extra key dies at strict parse.
  const nestedSmuggling = providerCapacitySyncBatchSchema.safeParse({
    kind: "provider_capacity_sync",
    installKey: "ik-local-x",
    snapshots: [
      {
        providerProfileId: "p.one",
        window: "five_hour",
        adapterVersion: "1.4.2",
        usedBasisPoints: 1000,
        source: "local_telemetry",
        capturedAt: NOW,
        raw_provider_response: { httpBody: "entire provider payload" },
      },
    ],
  });
  prove(
    "raw_provider_body_has_no_representable_slot_strict_parse_refuses",
    !nestedSmuggling.success,
    {},
  );
}

// ---------------------------------------------------------------------------
// E. Protocol fingerprint — determinism, drift-proof fixtures, receipts,
//    cross-repository compatibility.
// ---------------------------------------------------------------------------
{
  // Digest every golden fixture byte (receipt excludes protocol-receipt.json).
  const fixtureDigests: Record<string, string> = {};
  const walk = (dir: string): void => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true }).sort((a, b) =>
      a.name.localeCompare(b.name),
    )) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (
        entry.isFile() &&
        entry.name.endsWith(".json") &&
        entry.name !== "protocol-receipt.json"
      ) {
        const rel = path.relative(snapshotDir, full).split(path.sep).join("/");
        fixtureDigests[rel] = sha256File(full);
      }
    }
  };
  walk(snapshotDir);

  // Determinism: same inputs, byte-identical fingerprint, repeatedly.
  const first = computeProviderCapacityProtocolFingerprint(fixtureDigests);
  const second = computeProviderCapacityProtocolFingerprint(fixtureDigests);
  prove(
    "protocol_fingerprint_deterministic_byte_identical",
    first.protocolFingerprint === second.protocolFingerprint &&
      /^sha256:[a-f0-9]{64}$/.test(first.protocolFingerprint) &&
      canonicalJsonString(computeProviderCapacityContractMaterial()) ===
        canonicalJsonString(computeProviderCapacityContractMaterial()),
    { fingerprint: first.protocolFingerprint },
  );

  // Committed receipt matches reality (fingerprint-bearing fields exactly).
  const receiptPath = path.join(snapshotDir, "protocol-receipt.json");
  const committed = JSON.parse(fs.readFileSync(receiptPath, "utf8")) as Record<string, unknown>;
  const expected = buildProviderCapacityProtocolReceipt({
    fixtureDigests,
    generatedAt: typeof committed.generatedAt === "string" ? committed.generatedAt : NOW,
  });
  prove(
    "committed_receipt_matches_recomputed_fingerprint_and_fixture_digests",
    committed.protocolFingerprint === expected.protocolFingerprint &&
      committed.contractMaterialDigest === expected.contractMaterialDigest &&
      committed.fixtureCount === expected.fixtureCount &&
      canonicalJsonString(committed.fixtureDigests) ===
        canonicalJsonString(expected.fixtureDigests) &&
      committed.compatibilityPolicy === "exact_fingerprint_match",
    {
      committed: committed.protocolFingerprint,
      recomputed: expected.protocolFingerprint,
      fixtures: Object.keys(fixtureDigests).length,
    },
  );

  // DRIFT PROOF: flipping ONE hex char of ONE fixture digest must flip the
  // fingerprint and break compatibility against the committed receipt.
  const tamperedDigests = { ...fixtureDigests };
  const victimKey = Object.keys(tamperedDigests)[0]!;
  const victimHex = tamperedDigests[victimKey]!;
  tamperedDigests[victimKey] =
    victimHex.slice(0, -1) + (victimHex.endsWith("0") ? "1" : "0");
  const drifted = computeProviderCapacityProtocolFingerprint(tamperedDigests);
  const driftedVerdict = checkProviderCapacityProtocolCompatibility(
    committed.protocolFingerprint as string,
    drifted.protocolFingerprint,
  );
  prove(
    "single_fixture_byte_drift_flips_fingerprint_and_breaks_compatibility",
    drifted.protocolFingerprint !== committed.protocolFingerprint &&
      driftedVerdict.status === "incompatible" &&
      checkProviderCapacityProtocolCompatibility(
        committed.protocolFingerprint as string,
        first.protocolFingerprint,
      ).status === "compatible",
    {
      victim: victimKey,
      drifted: drifted.protocolFingerprint,
      verdict: driftedVerdict.status,
    },
  );

  // Contract-material sensitivity: removing one structural field changes the
  // material digest even with identical fixtures.
  const mutated: Record<string, unknown> = {
    ...computeProviderCapacityContractMaterial(),
  };
  delete mutated.basisPointsMax;
  const mutatedDigest = `sha256:${createHash("sha256")
    .update(canonicalJsonString(mutated))
    .digest("hex")}`;
  prove(
    "contract_material_change_changes_digest_independent_of_fixtures",
    mutatedDigest !== expected.contractMaterialDigest,
    { mutatedDigest, original: expected.contractMaterialDigest },
  );

  // Cross-repo compatibility is exact-match, never fuzzy.
  const foreignB = "sha256:" + "f".repeat(64);
  const compatSame = checkProviderCapacityProtocolCompatibility(
    first.protocolFingerprint,
    first.protocolFingerprint,
  );
  const compatDiff = checkProviderCapacityProtocolCompatibility(
    first.protocolFingerprint,
    foreignB,
  );
  prove(
    "cross_repo_compatibility_exact_match_or_fail_closed",
    compatSame.status === "compatible" &&
      compatDiff.status === "incompatible" &&
      compatDiff.localFingerprint === first.protocolFingerprint &&
      compatDiff.foreignFingerprint === foreignB,
    { compatSame, compatDiff },
  );
}

// ---------------------------------------------------------------------------
// F. Consent template binding + upload gate.
// ---------------------------------------------------------------------------
{
  const template = fs.readFileSync(consentTemplatePath, "utf8");

  const requiredSections = [
    "## What is being consented to",
    "## Exact fields disclosed",
    "## Purpose",
    "## Who can view",
    "## Retention",
    "## Pausing",
    "## Deletion",
    "## No-performance-use rule",
    "## Consent binding (renewal triggers)",
    "## Renewal record",
  ];
  const missingSections = requiredSections.filter((section) => !template.includes(section));
  prove("consent_template_lists_all_required_sections", missingSections.length === 0, {
    missingSections,
  });

  // Template's exact-field block must equal the code's field list — either
  // side drifting turns the proof red.
  const blockMatch = /## Exact fields disclosed[\s\S]*?```json\s*([\s\S]*?)```/.exec(template);
  assert.ok(blockMatch, "consent template exact-fields block not found");
  const listedFields = JSON.parse(blockMatch[1]!) as string[];
  prove(
    "consent_template_field_list_matches_code_exactly",
    canonicalJsonString(listedFields.slice().sort()) ===
      canonicalJsonString([...PROVIDER_CAPACITY_SYNC_ROW_FIELDS].sort()),
    { listedFields, code: [...PROVIDER_CAPACITY_SYNC_ROW_FIELDS].sort() },
  );

  const noPerformanceSection = template.split("## No-performance-use rule")[1] ?? "";
  prove(
    "consent_template_states_no_performance_use_rule",
    noPerformanceSection.includes("ONLY.") &&
      noPerformanceSection.toLowerCase().includes("never be used"),
    { excerpt: noPerformanceSection.slice(0, 140) },
  );

  // Undecided owner policy stays an explicit placeholder, not invented data.
  prove(
    "consent_template_keeps_owner_set_placeholders_explicit",
    template.includes("<OWNER-SET: retention_days>"),
    {},
  );

  const SOURCE_HEAD = "a".repeat(40);
  const fingerprintArtifact = computeProviderCapacityProtocolFingerprint({});
  const consentRecord = {
    approved: true,
    approvedAt: "2026-08-21T00:00:00.000Z",
    approvedBy: "owner",
    binding: {
      artifactDigest: fingerprintArtifact.protocolFingerprint,
      protocolId: "plimsoll.provider-capacity.protocol.v1",
      sourceHead: SOURCE_HEAD,
    },
    consentKind: CAPACITY_UPLOAD_CONSENT_KIND,
    scope: {
      noPerformanceUse: true,
      purpose: "operational_capacity_context_only",
      surfaces: ["provider_capacity_sync"],
    },
    version: 1,
  };

  const allowed = evaluateProviderCapacityUpload({
    consent: consentRecord,
    currentArtifactDigest: fingerprintArtifact.protocolFingerprint,
    currentSourceHead: SOURCE_HEAD,
    surface: "provider_capacity_sync",
  });
  prove("consent_gate_allows_exact_binding_match", allowed.allowed === true, { allowed });

  const movedHead = evaluateProviderCapacityUpload({
    consent: consentRecord,
    currentArtifactDigest: fingerprintArtifact.protocolFingerprint,
    currentSourceHead: "b".repeat(40),
    surface: "provider_capacity_sync",
  });
  prove(
    "moved_source_head_blocks_upload_requires_renewal",
    movedHead.allowed === false && movedHead.reason === "source_head_changed",
    { movedHead },
  );

  const driftedArtifact = evaluateProviderCapacityUpload({
    consent: consentRecord,
    currentArtifactDigest: "sha256:" + "9".repeat(64),
    currentSourceHead: SOURCE_HEAD,
    surface: "provider_capacity_sync",
  });
  prove(
    "drifted_artifact_digest_blocks_upload_requires_renewal",
    driftedArtifact.allowed === false && driftedArtifact.reason === "artifact_digest_changed",
    { driftedArtifact },
  );

  // Tampering with the no-performance-use clause makes the record invalid —
  // there is no representable consent without that literal.
  const tampered: Record<string, unknown> = { ...consentRecord };
  tampered.scope = {
    ...(consentRecord.scope as Record<string, unknown>),
    noPerformanceUse: false,
  };
  const tamperedResult = evaluateProviderCapacityUpload({
    consent: tampered,
    currentArtifactDigest: fingerprintArtifact.protocolFingerprint,
    currentSourceHead: SOURCE_HEAD,
    surface: "provider_capacity_sync",
  });
  prove(
    "tampered_no_performance_use_clause_invalidates_consent_record",
    tamperedResult.allowed === false && tamperedResult.reason === "consent_invalid",
    { tamperedResult },
  );

  // A consent whose scope does not cover this surface blocks the upload. With
  // today's single-surface literal the schema itself refuses such a record
  // (fail-closed either way); surface_not_in_scope stays available for the
  // day a second surface literal exists.
  const otherSurfaceConsent: Record<string, unknown> = {
    ...consentRecord,
    scope: { ...(consentRecord.scope as Record<string, unknown>), surfaces: [] },
  };
  const outOfScope = evaluateProviderCapacityUpload({
    consent: otherSurfaceConsent,
    currentArtifactDigest: fingerprintArtifact.protocolFingerprint,
    currentSourceHead: SOURCE_HEAD,
    surface: "provider_capacity_sync",
  });
  prove(
    "surface_not_covered_by_consent_blocks_upload_fail_closed",
    outOfScope.allowed === false &&
      (outOfScope.reason === "surface_not_in_scope" || outOfScope.reason === "consent_invalid"),
    { outOfScope },
  );

  const garbage = evaluateProviderCapacityUpload({
    consent: { note: "trust me" },
    currentArtifactDigest: fingerprintArtifact.protocolFingerprint,
    currentSourceHead: SOURCE_HEAD,
    surface: "provider_capacity_sync",
  });
  prove(
    "malformed_consent_record_fails_closed",
    garbage.allowed === false && garbage.reason === "consent_invalid",
    { garbage },
  );
}

// ---------------------------------------------------------------------------
// G. Doctrine statics on the new module itself.
// ---------------------------------------------------------------------------
{
  const moduleSource = fs.readFileSync(
    path.join(root, "packages/shared/src/provider-capacity-snapshot.ts"),
    "utf8",
  );
  const exportedNames = [
    ...moduleSource.matchAll(/export (?:async )?(?:function|const|type) ([A-Za-z0-9]+)/g),
  ].map((match) => match[1]!);
  const decisionVerbExports = exportedNames.filter((name) =>
    /(?:route|coach|rank|score|verdict|intervent|disciplin|compensat)/i.test(name),
  );
  prove(
    "module_exports_no_routing_coaching_ranking_or_verdict_verbs",
    decisionVerbExports.length === 0,
    { exportedNames, decisionVerbExports },
  );
  const importsForbiddenSurface =
    /from\s+"[^"]*(?:performance-layer|metric-registry|learning-evidence|learning-facts)"/.test(
      moduleSource,
    );
  prove("module_imports_no_scoring_or_performance_surface", !importsForbiddenSurface, {});

  // The wire schemas represent NO storable remaining field anywhere: parse a
  // snapshot carrying remainingBasisPoints and confirm both the key refusal
  // here and that derivation is the only producer of that name.
  const withStoredRemaining = providerCapacitySnapshotV1Schema.safeParse({
    schema: "plimsoll.provider-capacity-snapshot.v1",
    unit: {
      deviceInstallId: "dev-1",
      providerProfileId: "p.one",
      window: "w",
      adapterVersion: "1.0.0",
    },
    observation: {
      usedBasisPoints: 5000,
      remainingBasisPoints: 5000,
      source: "provider_report",
      capturedAt: NOW,
    },
  });
  prove(
    "derived_headroom_has_no_storable_slot_in_wire_schemas",
    !withStoredRemaining.success,
    {},
  );
}

console.log(JSON.stringify({ issue: "168", passed: checks.length, checks }, null, 2));
guard.complete();
