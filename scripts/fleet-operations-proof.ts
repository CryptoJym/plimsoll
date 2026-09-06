#!/usr/bin/env node

/**
 * Issue #154 proof: fleet operations — desired state and sanitized receipts.
 *
 * Pure data-in/data-out proof against packages/shared/src/fleet-operations.ts.
 * Every adversarial check here encodes a way a naive implementation fails:
 * replayed versions, tampered documents, skipped rollout stages, revived
 * revoked devices, rolled-back digest reapplication, undeclared cohorts,
 * unbounded triggers, and privacy smuggling into hosted fleet state.
 * Fixtures live in scripts/fixtures/fleet-operations/hostile; no filesystem
 * outside temp, no network, no processes are touched.
 */

import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { guardProofCompletion } from "./lib/proof-completion";
import {
  ALLOWED_FLEET_TRANSITIONS,
  DEFAULT_SIGNAL_STALENESS_MS,
  DESIRED_STATE_SCHEMA_VERSION,
  DEVICE_RECEIPT_SCHEMA_VERSION,
  FLEET_OBSERVED_STATES,
  type DeviceReceipt,
  advanceFleetState,
  assertFleetReceiptPrivacy,
  buildDeviceReceipt,
  desiredAppliesTo,
  findFleetReceiptPrivacyViolations,
  isDesiredVersionMonotonic,
  isFleetTransitionAllowed,
  parseDesiredStateCore,
  planReconciliation,
  renderFleetView,
  serializeDesiredStateCore,
  serializeDeviceReceipt,
  signDesiredState,
  verifySignedDesiredState,
} from "../packages/shared/src/fleet-operations";

const scriptPath = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(scriptPath), "..");
const hostileDir = path.join(repoRoot, "scripts", "fixtures", "fleet-operations", "hostile");

type Check = { name: string; adversarial: boolean; detail: Record<string, unknown> };
const checks: Check[] = [];

function check(
  name: string,
  adversarial: boolean,
  condition: unknown,
  detail: Record<string, unknown> = {},
): void {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, adversarial, detail });
}

// Refuses the two silent-green modes — an early event-loop drain and a hang
// that never exits. See scripts/lib/proof-completion.ts for why each is needed.
const guard = guardProofCompletion({
  countChecks: () => checks.length,
});

function readFixture(name: string): Record<string, unknown> {
  return JSON.parse(fs.readFileSync(path.join(hostileDir, name), "utf8")) as Record<string, unknown>;
}

// One fleet signing keypair for the whole proof; only the public half ever
// reaches verification, mirroring the deployment shape (devices cannot forge).
const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");

const DIGEST_A = crypto.createHash("sha256").update("plimsoll-runtime-a").digest("hex");
const DIGEST_B = crypto.createHash("sha256").update("plimsoll-runtime-b").digest("hex");
const NOW = 1_756_160_000_000; // fixed epoch for determinism

function coreFor(
  overrides: Partial<{
    schemaVersion: number;
    desiredVersion: number;
    scope: { kind: "device"; deviceId: string } | { kind: "cohort"; cohortId: string };
    artifactVersion: string;
    artifactSha: string;
    issuedAtMs: number;
  }> = {},
) {
  const parsed = parseDesiredStateCore({
    schemaVersion: overrides.schemaVersion ?? DESIRED_STATE_SCHEMA_VERSION,
    desiredVersion: overrides.desiredVersion ?? 7,
    scope: overrides.scope ?? { kind: "device", deviceId: "mac-ops-001" },
    artifact: {
      version: overrides.artifactVersion ?? "0.8.2",
      sha256: overrides.artifactSha ?? DIGEST_A,
    },
    issuedAtMs: overrides.issuedAtMs ?? NOW,
  });
  assert.ok(parsed.ok, "test core must parse");
  return parsed.value;
}

function receiptFor(
  overrides: Partial<DeviceReceipt> & { deviceId?: string } = {},
): DeviceReceipt {
  const full = buildDeviceReceipt({
    deviceId: overrides.deviceId ?? "mac-ops-001",
    observedState: overrides.observedState ?? "desired",
    appliedArtifact: overrides.appliedArtifact ?? null,
    highestDesiredVersionSeen: overrides.highestDesiredVersionSeen ?? 6,
    rejectedDigests: overrides.rejectedDigests,
    lastSignalAtMs: overrides.lastSignalAtMs ?? NOW - 1000,
    generatedAtMs: overrides.generatedAtMs ?? NOW - 500,
  });
  return full;
}

// ---------------------------------------------------------------------------
// Desired state: sign, verify, scope, monotonic versioning
// ---------------------------------------------------------------------------

function proveDesiredState() {
  const doc = signDesiredState(coreFor(), privateKey);

  const verified = verifySignedDesiredState(doc, publicKey);
  check("valid_signed_document_verifies", false, verified.ok);
  if (verified.ok) {
    check("roundtrip_preserves_core", false, verified.value.desiredVersion === 7 &&
      verified.value.artifact.sha256 === DIGEST_A);
  }

  // Canonical serialization is stable byte-for-byte.
  const again = signDesiredState(coreFor(), privateKey);
  check("canonical_serialization_deterministic", false,
    serializeDesiredStateCore(coreFor()) === serializeDesiredStateCore(coreFor()) &&
    again.signature === doc.signature);

  // Adversarial: flip one byte of the payload after signing.
  const tampered = { ...doc, desiredVersion: 8 };
  const tamperedResult = verifySignedDesiredState(tampered, publicKey);
  check("tampered_payload_fails_signature", true, !tamperedResult.ok && tamperedResult.reason === "signature_invalid");

  // Adversarial: validly signed document for a DIFFERENT core (signature swap).
  const otherDoc = signDesiredState(coreFor({ desiredVersion: 9 }), privateKey);
  const swapped = { ...coreFor({ desiredVersion: 7 }), signature: otherDoc.signature };
  const swappedResult = verifySignedDesiredState(swapped, publicKey);
  check("cross_document_signature_swap_fails", true, !swappedResult.ok && swappedResult.reason === "signature_invalid");

  // Adversarial: wrong public key (device trying to verify under its own key).
  const rogue = crypto.generateKeyPairSync("ed25519");
  const wrongKey = verifySignedDesiredState(doc, rogue.publicKey);
  check("wrong_public_key_fails", true, !wrongKey.ok && wrongKey.reason === "signature_invalid");

  // Monotonicity: strictly greater required, even when validly signed.
  const v7 = signDesiredState(coreFor({ desiredVersion: 7 }), privateKey);
  check("equal_version_is_replay", true, !isDesiredVersionMonotonic(v7, 7));
  check("lower_version_is_regression", true, !isDesiredVersionMonotonic(v7, 9));
  check("higher_version_accepted", false, isDesiredVersionMonotonic(v7, 6));

  // Scope resolution: exact device match; explicit cohort membership only.
  const cohortDoc = signDesiredState(
    coreFor({ scope: { kind: "cohort", cohortId: "wave-2026-34" } }),
    privateKey,
  );
  const membership = { "wave-2026-34": ["mac-ops-001", "mac-ops-002"] };
  check("cohort_applies_to_declared_member", false,
    desiredAppliesTo(cohortDoc, "mac-ops-002", membership));
  check("cohort_ignores_undeclared_device", true,
    !desiredAppliesTo(cohortDoc, "mac-rogue-099", membership));
  check("undeclared_cohort_applies_to_nobody", true,
    !desiredAppliesTo(cohortDoc, "mac-ops-001", {}));
}

// ---------------------------------------------------------------------------
// Observed-state machine: literal states, no stage skipping, revoked terminal
// ---------------------------------------------------------------------------

function proveStateMachine() {
  const forward: Array<[string, string]> = [
    ["desired", "downloaded"],
    ["downloaded", "staged"],
    ["staged", "switched"],
    ["switched", "service_ready"],
    ["service_ready", "signal_verified"],
  ];
  for (const [from, to] of forward) {
    check(`forward_transition_${from}_to_${to}`, false, isFleetTransitionAllowed(from as never, to as never));
  }

  // Adversarial: skipping stages must fail.
  const skips: Array<[string, string]> = [
    ["desired", "switched"],
    ["desired", "signal_verified"],
    ["downloaded", "service_ready"],
    ["staged", "signal_verified"],
  ];
  for (const [from, to] of skips) {
    const result = advanceFleetState(from as never, to as never);
    check(`stage_skip_rejected_${from}_to_${to}`, true,
      !result.ok && result.reason === `transition_forbidden:${from}->${to}`);
  }

  // Rollback arm exists from every applied state.
  for (const state of ["downloaded", "staged", "switched", "service_ready", "signal_verified"]) {
    check(`rollback_startable_from_${state}`, false,
      isFleetTransitionAllowed(state as never, "rollback_started"));
  }
  check("rollback_completes", false,
    isFleetTransitionAllowed("rollback_started", "rolled_back"));
  check("no_rollback_from_pristine_desired", true,
    !isFleetTransitionAllowed("desired", "rollback_started"));

  // Adversarial: revoked is terminal — no revival path exists.
  for (const target of FLEET_OBSERVED_STATES.filter((s) => s !== "revoked")) {
    const result = advanceFleetState("revoked", target);
    check(`revoked_cannot_become_${target}`, true, !result.ok);
  }
  check("transition_table_terminal_revoked", false, ALLOWED_FLEET_TRANSITIONS.revoked.length === 0);

  // Offline is reachable from live states and reports truth on return.
  check("offline_reachable_from_switched", false,
    isFleetTransitionAllowed("switched", "offline"));
  check("offline_returns_to_truthful_state", false,
    isFleetTransitionAllowed("offline", "rolled_back"));
  check("offline_no_op_rejected", true, !advanceFleetState("offline", "offline").ok);
}

// ---------------------------------------------------------------------------
// Sanitized receipts: deterministic build + mechanical privacy rejection
// ---------------------------------------------------------------------------

function proveReceipts() {
  const receipt = buildDeviceReceipt({
    deviceId: "mac-ops-001",
    cohortId: "wave-2026-34",
    observedState: "switched",
    appliedArtifact: { version: "0.8.2", sha256: DIGEST_A },
    highestDesiredVersionSeen: 7,
    rejectedDigests: [DIGEST_B, DIGEST_B, DIGEST_A].sort().reverse(),
    lastSignalAtMs: NOW - 1000,
    generatedAtMs: NOW,
  });

  check("receipt_schema_version", false, receipt.schemaVersion === DEVICE_RECEIPT_SCHEMA_VERSION);
  check("rejected_digests_sorted_deduped", false,
    JSON.stringify(receipt.rejectedDigests) === JSON.stringify([DIGEST_A, DIGEST_B].sort()));

  const rebuilt = buildDeviceReceipt({
    deviceId: "mac-ops-001",
    cohortId: "wave-2026-34",
    observedState: "switched",
    appliedArtifact: { version: "0.8.2", sha256: DIGEST_A },
    highestDesiredVersionSeen: 7,
    rejectedDigests: [DIGEST_B, DIGEST_A],
    lastSignalAtMs: NOW - 1000,
    generatedAtMs: NOW,
  });
  check("receipt_build_deterministic_bytes", false,
    serializeDeviceReceipt(receipt) === serializeDeviceReceipt(rebuilt));
  check("receipt_content_hash_stable", false, receipt.contentHash === rebuilt.contentHash);

  // Clean receipt passes the privacy guard.
  check("clean_receipt_passes_privacy", false,
    findFleetReceiptPrivacyViolations(receipt).length === 0);

  // Adversarial: hostile fixtures must each be mechanically rejected.
  const smuggle = readFixture("nested-config-body-smuggle.json");
  const smuggleViolations = findFleetReceiptPrivacyViolations(smuggle);
  check("fixture_nested_config_body_smuggled_rejected", true,
    smuggleViolations.some((v) => v.reason.startsWith("forbidden_field_concept:")) ||
      smuggleViolations.some((v) => v.reason.startsWith("forbidden_field:")));

  const paths = readFixture("home-path-and-secret-leak.json");
  const pathViolations = findFleetReceiptPrivacyViolations(paths);
  check("fixture_home_path_leak_rejected", true,
    pathViolations.some((v) => v.reason === "home_path_present"));
  check("fixture_secret_like_string_rejected", true,
    pathViolations.some((v) => v.reason.startsWith("secret_like_string:")));

  // Adversarial: direct throws on out-of-shape builds (fail closed, not coerced).
  assert.throws(() => buildDeviceReceipt({
    deviceId: "/Users/alice/laptop",
    observedState: "desired",
    appliedArtifact: null,
    highestDesiredVersionSeen: 0,
    lastSignalAtMs: NOW,
    generatedAtMs: NOW,
  }), /receipt_invalid_device_id/);
  check("device_id_may_not_be_a_path", true, true);

  assert.throws(() => buildDeviceReceipt({
    deviceId: "mac-ops-001",
    observedState: "prompt_capture_enabled" as never,
    appliedArtifact: null,
    highestDesiredVersionSeen: 0,
    lastSignalAtMs: NOW,
    generatedAtMs: NOW,
  }), /receipt_invalid_observed_state/);
  check("off_vocabulary_state_refused", true, true);

  assert.throws(() => buildDeviceReceipt({
    deviceId: "mac-ops-001",
    observedState: "desired",
    appliedArtifact: null,
    highestDesiredVersionSeen: 0,
    rejectedDigests: Array.from({ length: 65 }, () => DIGEST_A),
    lastSignalAtMs: NOW,
    generatedAtMs: NOW,
  }), /receipt_rejected_digests_out_of_bounds/);
  check("unbounded_rejected_digest_list_refused", true, true);

  assertFleetReceiptPrivacy(receipt);
  check("assert_gate_passes_clean_receipt", false, true);
  assert.throws(() => assertFleetReceiptPrivacy(readFixture("home-path-and-secret-leak.json")),
    /fleet_receipt_privacy_violation/);
  check("assert_gate_throws_on_hostile_fixture", true, true);
}

// ---------------------------------------------------------------------------
// Reconciliation: bounded single steps, honest holds, no auto-revival
// ---------------------------------------------------------------------------

function reconcile(input: {
  trigger?: string;
  desiredVersion?: number;
  verified?: boolean;
  applies?: boolean;
  receipt: DeviceReceipt;
  nowMs?: number;
}) {
  return planReconciliation({
    trigger: input.trigger ?? "sync",
    desired: signDesiredState(
      coreFor({ desiredVersion: input.desiredVersion ?? 7 }),
      privateKey,
    ),
    verified: input.verified ?? true,
    applies: input.applies ?? true,
    receipt: input.receipt,
    nowMs: input.nowMs ?? NOW,
  });
}

function proveReconciliation() {
  // Happy path walks one bounded step at a time.
  const step1 = reconcile({ receipt: receiptFor({ observedState: "desired" }) });
  check("step_download", false,
    step1.ok && step1.plan.plan.action === "apply_step" && step1.plan.plan.reason === "download" &&
    step1.plan.nextState === "downloaded");

  const step5 = reconcile({ receipt: receiptFor({ observedState: "service_ready" }) });
  check("step_confirm_signal", false,
    step5.ok && step5.plan.nextState === "signal_verified");

  const done = reconcile({ receipt: receiptFor({ observedState: "signal_verified" }) });
  check("current_device_no_op", false,
    done.ok && done.plan.plan.action === "no_op" && done.plan.plan.reason === "already_current");

  // Adversarial: unbounded triggers are refused outright.
  for (const trigger of ["daemon_tick", "continuous_loop", "", "SYNC"]) {
    const refused = reconcile({ trigger, receipt: receiptFor() });
    check(`trigger_refused:${trigger || "<empty>"}`, true,
      !refused.ok && refused.reason === `trigger_not_bounded:${trigger}`);
  }

  // Adversarial: replayed version holds for an operator instead of acting.
  const replay = reconcile({ desiredVersion: 6, receipt: receiptFor({ highestDesiredVersionSeen: 6 }) });
  check("replayed_version_holds", true,
    replay.ok && replay.plan.plan.action === "hold_for_operator" &&
    replay.plan.plan.reason === "version_not_monotonic");

  // Adversarial: rolled-back digest is never auto-reapplied.
  const reapply = reconcile({
    receipt: receiptFor({ observedState: "rolled_back", rejectedDigests: [DIGEST_A] }),
  });
  check("rolled_back_digest_never_reapplied", true,
    reapply.ok && reapply.plan.plan.action === "hold_for_operator" &&
    reapply.plan.plan.reason === "digest_rolled_back");

  // A rolled-back device may start a NEW cycle for a different digest.
  const freshCycle = reconcile({
    receipt: receiptFor({ observedState: "rolled_back", rejectedDigests: [DIGEST_B] }),
  });
  check("new_cycle_after_unrelated_rollback", false,
    freshCycle.ok && freshCycle.plan.plan.action === "apply_step" &&
    freshCycle.plan.plan.reason === "new_cycle" && freshCycle.plan.nextState === "desired");

  // Adversarial: revoked device is never commanded, even offline-fresh docs.
  const revoked = reconcile({ receipt: receiptFor({ observedState: "revoked" }) });
  check("revoked_device_no_op", true,
    revoked.ok && revoked.plan.plan.action === "no_op" && revoked.plan.plan.reason === "revoked" &&
    revoked.plan.nextState === null);

  // Adversarial: silent or offline devices are awaited, never commanded.
  const stale = reconcile({
    nowMs: NOW + DEFAULT_SIGNAL_STALENESS_MS + 1,
    receipt: receiptFor(),
  });
  check("stale_signal_awaits", true,
    stale.ok && stale.plan.plan.action === "await_signal" && stale.plan.plan.reason === "signal_stale");
  const offline = reconcile({ receipt: receiptFor({ observedState: "offline" }) });
  check("offline_device_awaits", true,
    offline.ok && offline.plan.plan.action === "await_signal");

  // Adversarial: undeclared cohort membership fails closed to an operator hold.
  const undeclared = reconcile({ applies: false, receipt: receiptFor() });
  check("undeclared_cohort_holds", true,
    undeclared.ok && undeclared.plan.plan.action === "hold_for_operator" &&
    undeclared.plan.plan.reason === "cohort_not_declared");

  // Adversarial: unsigned/tampered document refuses regardless of state.
  const unsigned = reconcile({ verified: false, receipt: receiptFor() });
  check("unverified_desired_refused", true, !unsigned.ok && unsigned.reason === "desired_signature_invalid");

  // Drift detection: foreign applied digest after switch forces rollback.
  const drift = reconcile({
    receipt: receiptFor({
      observedState: "signal_verified",
      appliedArtifact: { version: "0.9.0-rogue", sha256: crypto.createHash("sha256").update("rogue").digest("hex") },
    }),
  });
  check("digest_drift_starts_rollback", true,
    drift.ok && drift.plan.plan.action === "start_rollback" &&
    drift.plan.plan.reason === "applied_digest_drift" && drift.plan.nextState === "rollback_started");

  const finishing = reconcile({ receipt: receiptFor({ observedState: "rollback_started" }) });
  check("rollback_finishes", false,
    finishing.ok && finishing.plan.plan.action === "finish_rollback" &&
    finishing.plan.nextState === "rolled_back");
}

// ---------------------------------------------------------------------------
// Fleet view: one truthful row per registered device, UNKNOWN when absent
// ---------------------------------------------------------------------------

function proveFleetView() {
  const registry = [
    { deviceId: "mac-ops-001", cohortId: "wave-2026-34", enrolledStatus: "active" as const },
    { deviceId: "mac-ops-002", cohortId: "wave-2026-34", enrolledStatus: "active" as const },
    { deviceId: "mac-ghost-003", enrolledStatus: "pending" as const },
    { deviceId: "mac-exit-004", enrolledStatus: "revoked" as const },
  ];
  const receiptsByDevice: Record<string, DeviceReceipt> = {
    "mac-ops-001": receiptFor({ observedState: "signal_verified" }),
    "mac-ops-002": receiptFor({
      deviceId: "mac-ops-002",
      observedState: "rolled_back",
      rejectedDigests: [DIGEST_A],
    }),
    // mac-ghost-003 has never reported: everything stays UNKNOWN.
  };
  const desiredDocs = [
    signDesiredState(coreFor({ desiredVersion: 7 }), privateKey),
    signDesiredState(
      coreFor({ desiredVersion: 6, scope: { kind: "cohort", cohortId: "wave-2026-34" } }),
      privateKey,
    ),
  ];

  const view = renderFleetView({
    registry,
    receiptsByDevice,
    verifiedDesiredStates: desiredDocs,
    cohortMembership: { "wave-2026-34": ["mac-ops-001", "mac-ops-002"] },
    nowMs: NOW,
  });

  check("one_row_per_registered_device", false, view.length === registry.length);

  const first = view[0]!;
  check("approved_from_newest_applicable_doc", false,
    first.approvedVersion === "0.8.2");
  // The fixture receipt for mac-ops-001 carries no applied artifact, so the
  // installed column stays honestly UNKNOWN even though readiness is known.
  check("installed_and_readiness_truthful", false,
    first.installedVersion === "UNKNOWN" &&
    first.readiness === "signal_verified" &&
    first.rolloutState === "signal_verified" && first.rollbackState === "UNKNOWN");

  const second = view[1]!;
  check("rollback_state_visible_per_device", false,
    second.rollbackState === "rolled_back" && second.rolloutState === "UNKNOWN");

  const ghost = view[2]!;
  check("missing_receipt_is_unknown_everywhere", true,
    ghost.approvedVersion === "UNKNOWN" && ghost.installedVersion === "UNKNOWN" &&
    ghost.readiness === "UNKNOWN" && ghost.signalFreshness === "UNKNOWN" &&
    ghost.rolloutState === "UNKNOWN" && ghost.rollbackState === "UNKNOWN" &&
    ghost.revoked === false);

  const exited = view[3]!;
  check("registry_revocation_surfaces", true,
    exited.revoked === true);

  // Adversarial: a revoked receipt overrides a healthy registry status.
  const lyingRegistry = renderFleetView({
    registry: [{ deviceId: "mac-ops-001", enrolledStatus: "active" as const }],
    receiptsByDevice: {
      "mac-ops-001": receiptFor({ observedState: "revoked" }),
    },
    verifiedDesiredStates: desiredDocs,
    cohortMembership: {},
    nowMs: NOW,
  });
  check("receipt_revocation_overrides_registry_optimism", true,
    lyingRegistry[0]!.revoked === true);
}

// ---------------------------------------------------------------------------

proveDesiredState();
proveStateMachine();
proveReceipts();
proveReconciliation();
proveFleetView();

const adversarial = checks.filter((c) => c.adversarial).length;
console.log(
  JSON.stringify(
    {
      proof: "fleet-operations",
      status: "passed",
      checks: checks.length,
      adversarialChecks: adversarial,
    },
    null,
    2,
  ),
);
guard.complete();
