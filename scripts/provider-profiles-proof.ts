/**
 * Proof for issue #172: provider profile registry + optional signed
 * capacity-sync client.
 *
 * Covers every deliverable and acceptance line with adversarial cases:
 * label-collision non-merge, explicit owner-confirmed rotation, sanitized
 * cloud body (allowlist scan), default-off sync lifecycle, revocation with
 * usable local self-view, separate queue + monotonic device sequence,
 * probe-cost-gated 15-minute fallback, and the Codex
 * UNDETECTABLE_WITH_ALLOWED_METHODS honesty rule.
 *
 * Uses only injected randomness/HMAC (deterministic) and in-memory state:
 *   pnpm exec tsx scripts/provider-profiles-proof.ts
 */
import assert from "node:assert/strict";

import {
  CAPACITY_SYNC_BODY_SCHEMA,
  CODEX_IDENTITY_UNDETECTABLE,
  DEFAULT_EVENT_DEBOUNCE_MS,
  FALLBACK_INTERVAL_MS,
  PROVIDER_PROFILE_REGISTRY_SCHEMA,
  assertCrossMachineMergeAllowed,
  authorizeFallbackTimerFromProof,
  buildCloudBody,
  canonicalJson,
  checkHomeDrift,
  createProfileRegistryState,
  deleteProviderProfile,
  drainCapacityQueue,
  enableCapacitySync,
  enqueueCapacitySnapshot,
  evaluateProbeCostProof,
  exportLocalState,
  localSelfView,
  pauseCapacitySync,
  previewCapacitySync,
  registerProviderProfile,
  revokeCapacitySync,
  rotateProviderProfileKey,
  sanitizeCapacityFact,
  schedulePolicy,
  scanCloudBodyForForbiddenContent,
  signCapacitySyncBody,
  verifyCapacitySyncBodySignature,
  type ProfileRegistryState,
} from "../packages/shared/src/index";

type Check = { name: string; detail: Record<string, unknown> };
const checks: Check[] = [];

function prove(name: string, condition: unknown, detail: Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function throws(reason: string, run: () => unknown): boolean {
  try {
    run();
    return false;
  } catch (error) {
    return error instanceof Error && error.message.includes(reason);
  }
}

/** Deterministic key stream so runs are reproducible. */
function deterministicRandomHex(seedBase: number) {
  let counter = 0;
  return (_byteCount: number): string => {
    counter += 1;
    const raw = (seedBase * 1_000_003 + counter * 7_919) >>> 0;
    const hex = raw.toString(16).padStart(8, "0").repeat(4);
    return hex.slice(0, 32);
  };
}

const fixedHmac = (secretHex: string, canonical: string): string => {
  // Deterministic stand-in for tests only; real deployments use node:crypto.
  let acc = 0x811c9dc5;
  const material = `${secretHex}|${canonical}`;
  for (let index = 0; index < material.length; index++) {
    acc ^= material.charCodeAt(index);
    acc = Math.imul(acc, 0x01000193) >>> 0;
  }
  return acc.toString(16).padStart(8, "0") + "0".repeat(56);
};

const depsFor = (seed: number, now = "2026-08-24T12:00:00.000Z") => ({
  now: () => now,
  randomHex: deterministicRandomHex(seed),
  hmac: fixedHmac,
});

const CLAUDE_HOME = { claudeConfigDir: "/Users/dev/.claude-work", codexHome: null };
const CODEX_HOME_A = { claudeConfigDir: null, codexHome: "/Users/dev/.codex-a" };

function baseRegistry(): { state: ProfileRegistryState; deps: ReturnType<typeof depsFor> } {
  const deps = depsFor(1);
  let state = createProfileRegistryState(deps);
  state = registerProviderProfile(
    state,
    { label: "work-claude", provider: "claude_code", home: CLAUDE_HOME },
    deps,
  ).state;
  state = registerProviderProfile(
    state,
    { label: "work-codex", provider: "codex", home: CODEX_HOME_A },
    deps,
  ).state;
  return { state, deps };
}

// ---------------------------------------------------------------------------
// 1. Registry basics — explicit homes, opaque keys, distinct profiles.
// ---------------------------------------------------------------------------
{
  const { state } = baseRegistry();
  prove("registry_schema_is_versioned", state.schema === PROVIDER_PROFILE_REGISTRY_SCHEMA, {
    schema: state.schema,
  });
  const [claude, codex] = [state.profiles[0]!, state.profiles[1]!];
  prove("profiles_pin_explicit_homes", homeMatches(claude.home, CLAUDE_HOME) && homeMatches(codex.home, CODEX_HOME_A), {});
  prove("profile_keys_opaque_and_distinct", claude.profileKey !== codex.profileKey && /^[0-9a-f]{32}$/.test(claude.profileKey), {});
  prove("keys_not_derived_from_label_or_path", !claude.profileKey.includes("work") && !claude.profileKey.includes("claude"), {});

  // Same metadata registered in a fresh registry yields a DIFFERENT key:
  // keys are random per device, not a function of inputs.
  const fresh = createProfileRegistryState(depsFor(2));
  const again = registerProviderProfile(
    fresh,
    { label: "work-claude", provider: "claude_code", home: CLAUDE_HOME },
    depsFor(2),
  ).record;
  prove("key_randomness_not_input_derived", again.profileKey !== claude.profileKey, {});

  // Codex identity honesty: literal status, never a fabricated account id.
  prove("codex_identity_undetectable_literal", codex.identityStatus === CODEX_IDENTITY_UNDETECTABLE, {
    identityStatus: codex.identityStatus,
  });
}

function homeMatches(home: { claudeConfigDir: string | null; codexHome: string | null }, expected: typeof CLAUDE_HOME) {
  return home.claudeConfigDir === expected.claudeConfigDir && home.codexHome === expected.codexHome;
}

// ---------------------------------------------------------------------------
// 2. ADVERSARIAL: same label must never merge distinct profiles.
// ---------------------------------------------------------------------------
{
  const { state, deps } = baseRegistry();
  prove(
    "adversarial_same_label_other_home_rejected",
    throws("label_conflict_distinct_profile", () =>
      registerProviderProfile(
        state,
        { label: "work-claude", provider: "claude_code", home: { claudeConfigDir: "/Users/dev/.claude-personal", codexHome: null } },
        deps,
      ),
    ),
    {},
  );
  prove(
    "adversarial_same_label_other_provider_rejected",
    throws("label_conflict_distinct_profile", () =>
      registerProviderProfile(state, { label: "work-claude", provider: "codex", home: CLAUDE_HOME }, deps),
    ),
    {},
  );
  // Identical triple re-registration is idempotent (no second profile).
  const rerun = registerProviderProfile(state, { label: "work-claude", provider: "claude_code", home: CLAUDE_HOME }, deps);
  prove("identical_reregistration_idempotent", rerun.state.profiles.length === 2 && rerun.record.profileKey === state.profiles[0]!.profileKey, {});
  // Two labels on one home would be a silent merge vector — rejected too.
  prove(
    "adversarial_second_label_on_same_home_rejected",
    throws("home_already_registered_to_label", () =>
      registerProviderProfile(state, { label: "alias-claude", provider: "claude_code", home: CLAUDE_HOME }, deps),
    ),
    {},
  );
  // A home binding with nothing explicit is invalid.
  prove(
    "adversarial_home_without_explicit_dirs_rejected",
    throws("explicit CLAUDE_CONFIG_DIR or CODEX_HOME", () =>
      registerProviderProfile(state, { label: "empty-home", provider: "codex", home: { claudeConfigDir: null, codexHome: null } }, deps),
    ),
    {},
  );
}

// ---------------------------------------------------------------------------
// 3. Home drift is reported, never inferred; rotation is the only rebind path.
// ---------------------------------------------------------------------------
{
  const { state, deps } = baseRegistry();
  const drift = checkHomeDrift(state, {
    label: "work-claude",
    observedHome: { claudeConfigDir: "/Users/dev/.claude-personal", codexHome: null },
  });
  prove("drift_reported_rotation_required", drift.known && drift.drift && drift.rotationRequired, { drift });

  // Enqueue while drifted is refused without mutation or inference.
  const enabled = enableCapacitySync(state, { confirmation: "enable-capacity-sync" }, deps);
  const refused = enqueueCapacitySnapshot(
    enabled,
    {
      label: "work-claude",
      observedHome: { claudeConfigDir: "/Users/dev/.claude-personal", codexHome: null },
      facts: [{ dimension: "five_hour_window", unit: "tokens", freshness: "fresh", source: "local_telemetry", limit: 1000, used: 10 }],
    },
    deps,
  );
  prove("enqueue_while_drifted_refused_rotation_required", !refused.ok && refused.reason === "rotation_required", { reason: refused.ok ? null : refused.reason });
  if (!refused.ok) prove("refusal_does_not_mutate_state", refused.state.queue.length === 0 && refused.state.profiles[0]!.home.claudeConfigDir === CLAUDE_HOME.claudeConfigDir, {});

  // Rotation without/wrong confirmation is rejected.
  prove("rotation_without_confirmation_rejected", throws("owner confirmation required", () =>
    rotateProviderProfileKey(state, { label: "work-claude", confirmation: "", reason: "home_change" }, deps)), {});
  prove(
    "rotation_wrong_confirmation_rejected",
    throws("owner confirmation required", () =>
      rotateProviderProfileKey(state, { label: "work-claude", confirmation: "rotate:other-profile", reason: "home_change" }, deps)),
    {},
  );

  // Confirmed rotation rebinds home, rotates key, bumps epoch, records receipt.
  const rotated = rotateProviderProfileKey(
    state,
    {
      label: "work-claude",
      confirmation: "rotate:work-claude",
      reason: "home_change",
      newHome: { claudeConfigDir: "/Users/dev/.claude-personal", codexHome: null },
    },
    deps,
  );
  const record = rotated.record;
  prove(
    "confirmed_rotation_rotates_key_epoch_and_home",
    record.epoch === 2 &&
      record.profileKey !== state.profiles[0]!.profileKey &&
      record.home.claudeConfigDir === "/Users/dev/.claude-personal",
    { epoch: record.epoch },
  );
  const rotatedReceipt = rotated.state.receipts[0]!;
  prove("rotation_receipt_recorded", rotatedReceipt.kind === "rotation" && rotatedReceipt.homeRebound === true, {});
  prove(
    "receipt_carries_fingerprints_not_keys",
    rotatedReceipt.kind === "rotation" &&
      /^kfp_[0-9a-f]{16}$/.test(rotatedReceipt.keyFingerprintBefore) &&
      /^kfp_[0-9a-f]{16}$/.test(rotatedReceipt.keyFingerprintAfter),
    {},
  );

  // After rotation, the new home registers as matching (no more drift).
  const afterDrift = checkHomeDrift(rotated.state, {
    label: "work-claude",
    observedHome: { claudeConfigDir: "/Users/dev/.claude-personal", codexHome: null },
  });
  prove("post_rotation_drift_cleared", afterDrift.known && !afterDrift.drift, {});

  // Stale pre-rotation queue items are dropped at drain, never delivered.
  const seeded = enableCapacitySync(rotated.state, { confirmation: "enable-capacity-sync" }, deps);
  const staleQueued = seedStaleItem(seeded, state.profiles[0]!.profileKey, deps);
  const drained = drainCapacityQueue(staleQueued, {
    transport: () => "acknowledged",
    signingSecretHex: "a".repeat(64),
  }, deps);
  prove("stale_pre_rotation_items_dropped_never_delivered", drained.result.staleEpochDropped === 1 && drained.result.delivered === 0, drained.result);
}

/** Inject one queue item carrying a superseded epoch (simulates an item that
 * was queued before a rotation). Test-only helper using exported sanitizer. */
function seedStaleItem(state: ProfileRegistryState, oldKey: string, deps: ReturnType<typeof depsFor>): ProfileRegistryState {
  const item = sanitizeCapacityFact(
    { dimension: "five_hour_window", unit: "tokens", freshness: "fresh", source: "local_telemetry", limit: 100, used: 5 },
    { profileKey: oldKey, epoch: 1 },
  );
  return {
    ...state,
    queue: [...state.queue, { deviceSeq: state.sequence.lastDeviceSeq + 1, profileKey: oldKey, item, queuedAt: deps.now() }],
    sequence: { lastDeviceSeq: state.sequence.lastDeviceSeq + 1 },
  };
}

// ---------------------------------------------------------------------------
// 4. Sync lifecycle: default off → enable/pause/revoke/export/delete.
// ---------------------------------------------------------------------------
{
  const fresh = createProfileRegistryState(depsFor(3));
  prove("sync_defaults_off_and_fallback_unauthorized", fresh.sync.state === "off" && fresh.sync.fallbackTimerAuthorized === false, {});

  const { state, deps } = baseRegistry();
  const blocked = enqueueCapacitySnapshot(
    state,
    { label: "work-codex", facts: [{ dimension: "weekly_window", unit: "requests", freshness: "UNKNOWN", source: "local_telemetry", limit: null, used: null }] },
    deps,
  );
  prove("enqueue_while_off_refused_no_queue_growth", !blocked.ok && blocked.reason === "sync_off" && blocked.state.queue.length === 0, {});

  prove("enable_requires_confirmation", throws("owner confirmation required", () => enableCapacitySync(state, { confirmation: "yes" }, deps)), {});
  const enabled = enableCapacitySync(state, { confirmation: "enable-capacity-sync" }, deps);
  prove("enable_with_confirmation_sets_enabled", enabled.sync.state === "enabled", {});
  prove("double_enable_rejected", throws("sync_already_enabled", () => enableCapacitySync(enabled, { confirmation: "enable-capacity-sync" }, deps)), {});

  // Preview works in any state and shows exactly what would be sent.
  const queuedResult = enqueueCapacitySnapshot(
    enabled,
    { label: "work-codex", facts: [{ dimension: "weekly_window", unit: "requests", freshness: "UNKNOWN", source: "local_telemetry", limit: null, used: null }] },
    deps,
  );
  if (!queuedResult.ok) throw new Error("expected enqueue to succeed");
  const previewOff = previewCapacitySync(blocked.state);
  const previewOn = previewCapacitySync(queuedResult.state);
  prove("preview_works_while_off_and_enabled", previewOff.pendingItems === 0 && previewOn.pendingItems === 1 && previewOn.nextBody.items.length === 1, {});
  prove("preview_body_is_sanitized_schema", previewOn.nextBody.schema === CAPACITY_SYNC_BODY_SCHEMA && previewOn.nextBody.items[0]!.limit === null && previewOn.nextBody.items[0]!.used === null, {});

  const paused = pauseCapacitySync(queuedResult.state, deps);
  prove("pause_stops_delivery_but_keeps_queue", paused.sync.state === "paused" && paused.queue.length === 1, {});
  const pausedDrain = drainCapacityQueue(paused, { transport: () => "acknowledged", signingSecretHex: "b".repeat(64) }, deps);
  prove("drain_blocked_while_paused", pausedDrain.result.delivered === 0 && pausedDrain.result.blockedBy === "sync_paused", pausedDrain.result);
  prove("pause_requires_enabled_sync", throws("pause_requires_enabled_sync", () => pauseCapacitySync(createProfileRegistryState(deps), deps)), {});

  // Revocation stops delivery permanently; requires its own confirmation.
  prove("revoke_requires_confirmation", throws("owner confirmation required", () => revokeCapacitySync(paused, { confirmation: "stop" }, deps)), {});
  const revoked = revokeCapacitySync(paused, { confirmation: "revoke-capacity-sync" }, deps);
  const revokedEnqueue = enqueueCapacitySnapshot(revoked, { label: "work-codex", facts: [{ dimension: "weekly_window", unit: "requests", freshness: "UNKNOWN", source: "local_telemetry", limit: null, used: null }] }, deps);
  const revokedDrain = drainCapacityQueue(revoked, { transport: () => "acknowledged", signingSecretHex: "b".repeat(64) }, deps);
  prove("revocation_stops_enqueue_and_delivery_forever", revoked.sync.state === "revoked" && !revokedEnqueue.ok && revokedDrain.result.blockedBy === "sync_revoked", {});
  prove("re_enable_after_revocation_rejected", throws("revoked_sync_requires_delete_and_reregistration", () => enableCapacitySync(revoked, { confirmation: "enable-capacity-sync" }, deps)), {});

  // Local self-view remains fully usable after revocation.
  const view = localSelfView(revoked);
  prove("local_self_view_usable_after_revocation", view.syncState === "revoked" && view.profiles.length === 2 && view.profiles.every((p) => p.label.length > 0), { queueDepth: view.queueDepth });

  // Export is device-local full fidelity (labels/homes present by design).
  const exported = exportLocalState(revoked);
  prove("export_local_state_keeps_labels_and_homes_locally", exported.profiles[0]!.label === "work-claude" && exported.profiles[0]!.home.claudeConfigDir !== null && exported.receipts.length === revoked.receipts.length, {});

  // Delete removes profile + its queue items, keeps receipts, keeps sequence.
  prove("delete_requires_confirmation", throws("owner confirmation required", () => deleteProviderProfile(exported, { label: "work-codex", confirmation: "remove" }, deps)), {});
  const deleted = deleteProviderProfile(exported, { label: "work-codex", confirmation: "delete:work-codex" }, deps);
  prove(
    "delete_removes_profile_and_receipt_records_deletion",
    deleted.state.profiles.length === 1 &&
      deleted.removedQueueItems === 1 &&
      deleted.state.receipts.some((r) => r.kind === "deletion" && r.label === "work-codex"),
    { removed: deleted.removedQueueItems },
  );
  prove("deletion_does_not_reset_monotonic_sequence", deleted.state.sequence.lastDeviceSeq === exported.sequence.lastDeviceSeq && deleted.state.sequence.lastDeviceSeq > 0, { seq: deleted.state.sequence.lastDeviceSeq });
}

// ---------------------------------------------------------------------------
// 5. Sanitized cloud body: allowlist holds under adversarial smuggling.
// ---------------------------------------------------------------------------
{
  const { state, deps } = baseRegistry();
  const enabled = enableCapacitySync(state, { confirmation: "enable-capacity-sync" }, deps);
  const enqueued = enqueueCapacitySnapshot(
    enabled,
    {
      label: "work-codex",
      facts: [
        { dimension: "five_hour_window", unit: "tokens", freshness: "fresh", source: "local_telemetry", limit: 500_000, used: 123_456 },
        { dimension: "provider_quota_snapshot", unit: "tokens", freshness: "fresh", source: "provider_report", limit: 999_999, used: 42 },
      ],
    },
    deps,
  );
  if (!enqueued.ok) throw new Error("expected enqueue");
  const body = buildCloudBody(enqueued.state);
  const serialized = JSON.stringify(body);
  prove(
    "cloud_body_contains_no_label_email_path_account_or_raw_provider_data",
    !serialized.includes("work-codex") &&
      !serialized.includes("@") &&
      !serialized.includes("/Users") &&
      !serialized.includes(".codex") &&
      !serialized.includes("999999") &&
      body.items[1]!.limit === null &&
      body.items[1]!.used === null &&
      body.items[0]!.limit === 500000,
    { items: body.items.length },
  );

  // Structural allowlist rejects smuggled shapes before anything leaves.
  const smuggling: Array<Record<string, unknown>> = [
    { schema: CAPACITY_SYNC_BODY_SCHEMA, v: 1, items: [], watermark: {}, note: "owner@example.com" },
    { schema: CAPACITY_SYNC_BODY_SCHEMA, v: 1, items: [], watermark: {}, note: "/Users/dev/secret" },
    { schema: CAPACITY_SYNC_BODY_SCHEMA, v: 1, items: [], watermark: { lastDeviceSeq: "tenant-xyz" } },
    { schema: CAPACITY_SYNC_BODY_SCHEMA, v: 1, items: [], watermark: {}, "bad key!": 1 },
    { schema: CAPACITY_SYNC_BODY_SCHEMA, v: 1, items: [{ k: "work-codex", e: 1, d: "x", u: "tokens", f: "fresh", limit: 1, used: 1, seq: 1, queuedAt: "2026-08-24T12:00:00.000Z" }], watermark: {} },
  ];
  const allFlagged = smuggling.every((candidate) => scanCloudBodyForForbiddenContent(candidate).length > 0);
  prove("adversarial_smuggled_content_fail_closed", allFlagged, { cases: smuggling.length });
  prove("clean_body_scans_clean", scanCloudBodyForForbiddenContent(body).length === 0, {});

  // Sanitizer rejects hostile fact inputs outright (nothing enqueued).
  prove("dimension_smuggling_rejected", throws("invalid_capacity_dimension", () =>
    sanitizeCapacityFact({ dimension: "win@evil.com", unit: "tokens", freshness: "fresh", source: "local_telemetry", limit: 1, used: 1 }, { profileKey: state.profiles[1]!.profileKey, epoch: 1 })), {});
  prove("path_in_dimension_rejected", throws("invalid_capacity_dimension", () =>
    sanitizeCapacityFact({ dimension: "../../etc/passwd", unit: "tokens", freshness: "fresh", source: "local_telemetry", limit: 1, used: 1 }, { profileKey: state.profiles[1]!.profileKey, epoch: 1 })), {});
  prove("negative_number_rejected", throws("invalid numeric capacity fact", () =>
    sanitizeCapacityFact({ dimension: "ok_dim", unit: "usd", freshness: "fresh", source: "local_telemetry", limit: -5, used: 1 }, { profileKey: state.profiles[1]!.profileKey, epoch: 1 })), {});
  prove("nan_used_becomes_rejected_not_zero", throws("invalid numeric capacity fact", () =>
    sanitizeCapacityFact({ dimension: "ok_dim", unit: "usd", freshness: "fresh", source: "local_telemetry", limit: 1, used: Number.NaN }, { profileKey: state.profiles[1]!.profileKey, epoch: 1 })), {});
  prove("unknown_stays_null_not_zero", sanitizeCapacityFact({ dimension: "ok_dim", unit: "usd", freshness: "STALE", source: "local_telemetry", limit: null, used: null }, { profileKey: state.profiles[1]!.profileKey, epoch: 1 }).limit === null, {});
}

// ---------------------------------------------------------------------------
// 6. Signing + tamper detection.
// ---------------------------------------------------------------------------
{
  const { state, deps } = baseRegistry();
  const enabled = enableCapacitySync(state, { confirmation: "enable-capacity-sync" }, deps);
  const enqueued = enqueueCapacitySnapshot(enabled, { label: "work-claude", facts: [{ dimension: "daily_window", unit: "percent", freshness: "fresh", source: "local_telemetry", limit: 100, used: 25 }] }, deps);
  if (!enqueued.ok) throw new Error("expected enqueue");
  const secret = "c".repeat(64);
  const body = buildCloudBody(enqueued.state);
  const signature = signCapacitySyncBody(body, secret, deps);
  prove("signature_verifies", verifyCapacitySyncBodySignature(body, signature, secret, deps), {});
  const tampered = { ...body, watermark: { ...body.watermark, lastDeviceSeq: body.watermark.lastDeviceSeq + 1 } };
  prove("tampered_body_fails_verification", !verifyCapacitySyncBodySignature(tampered, signature, secret, deps), {});
  prove("wrong_secret_fails_verification", !verifyCapacitySyncBodySignature(body, signature, "d".repeat(64), deps), {});
  prove("canonical_json_is_key_order_stable", canonicalJson({ b: 1, a: 2 }) === '{"a":2,"b":1}', {});
  prove("invalid_secret_rejected", throws("invalid signing secret", () => signCapacitySyncBody(body, "zzz", deps)), {});
}

// ---------------------------------------------------------------------------
// 7. Monotonic device sequence across pause/resume/delete cycles.
// ---------------------------------------------------------------------------
{
  const { state, deps } = baseRegistry();
  const enabled = enableCapacitySync(state, { confirmation: "enable-capacity-sync" }, deps);
  const fact = { dimension: "daily_window", unit: "tokens" as const, freshness: "fresh" as const, source: "local_telemetry" as const, limit: 10, used: 1 };
  const first = enqueueCapacitySnapshot(enabled, { label: "work-claude", facts: [fact] }, deps);
  const paused = pauseCapacitySync(first.ok ? first.state : enabled, deps);
  const resumed = enableCapacitySync(paused, { confirmation: "enable-capacity-sync" }, deps);
  const second = enqueueCapacitySnapshot(resumed, { label: "work-claude", facts: [fact] }, deps);
  prove(
    "sequence_strictly_monotonic_across_pause_resume",
    second.ok && first.ok && second.deviceSeq === first.deviceSeq + 1,
    { first: first.ok ? first.deviceSeq : -1, second: second.ok ? second.deviceSeq : -1 },
  );
  const withDelete = deleteProviderProfile(second.ok ? second.state : resumed, { label: "work-codex", confirmation: "delete:work-codex" }, deps);
  prove("delete_keeps_enabled_state_and_sequence", withDelete.state.sync.state === "enabled" && withDelete.state.sequence.lastDeviceSeq === (second.ok ? second.deviceSeq : 0), {});
  const reregistered = registerProviderProfile(withDelete.state, { label: "work-codex", provider: "codex", home: CODEX_HOME_A }, deps);
  const third = enqueueCapacitySnapshot(reregistered.state, { label: "work-codex", facts: [fact] }, deps);
  prove("sequence_never_reused_after_delete_reregister", third.ok && third.deviceSeq > (second.ok ? second.deviceSeq : 0), { third: third.ok ? third.deviceSeq : -1 });
  const currentState = third.ok ? third.state : reregistered.state;
  const transient = drainCapacityQueue(currentState, { transport: () => "remote_transient", signingSecretHex: "e".repeat(64) }, deps);
  prove("failed_transport_keeps_queue_and_reports_class", transient.result.failureClass === "remote_transient" && transient.result.delivered === 0 && transient.state.queue.length === 3, transient.result);
  const drained = drainCapacityQueue(transient.state, { transport: () => "acknowledged", signingSecretHex: "e".repeat(64) }, deps);
  prove(
    "delivered_watermark_advances_only_on_ack",
    drained.result.delivered === 3 &&
      drained.result.staleEpochDropped === 0 &&
      drained.state.queue.length === 0 &&
      drained.state.sync.lastDeliveredDeviceSeq === drained.state.sequence.lastDeviceSeq,
    drained.result,
  );
}

// ---------------------------------------------------------------------------
// 8. Probe-cost gate: 15-minute fallback only after measured proof.
// ---------------------------------------------------------------------------
{
  const { state } = baseRegistry();
  prove("fallback_interval_is_15_minutes", FALLBACK_INTERVAL_MS === 15 * 60 * 1000, { ms: FALLBACK_INTERVAL_MS });
  const policyBefore = schedulePolicy(state);
  prove("fallback_blocked_before_proof", policyBefore.fallbackEnabled === false && policyBefore.eventDebounceMs === DEFAULT_EVENT_DEBOUNCE_MS && policyBefore.basis.includes("blocked"), policyBefore);

  // Forged pass verdict with insufficient samples must NOT authorize.
  const forged = authorizeFallbackTimerFromProof(state, { samplesMs: [1, 2, 3], authorized: true }, depsFor(9));
  prove("adversarial_forged_proof_rejected_samples_recounted", forged.state.sync.fallbackTimerAuthorized === false && forged.receipt.reason.startsWith("insufficient_samples"), forged.receipt);

  const slow = evaluateProbeCostProof({ samplesMs: [10, 12, 11, 13, 900] });
  prove("p95_over_budget_denies_authorization", !slow.authorized && slow.p95Ms === 900, slow);

  const good = authorizeFallbackTimerFromProof(state, { samplesMs: [8, 9, 10, 11, 12] }, depsFor(10));
  prove("measured_p95_within_budget_authorizes_fallback", good.state.sync.fallbackTimerAuthorized === true && good.receipt.authorized === true && good.receipt.p95Ms === 12, good.receipt);

  const negativeSample = evaluateProbeCostProof({ samplesMs: [8, 9, 10, 11, -1] });
  prove("invalid_samples_fail_closed", !negativeSample.authorized && negativeSample.reason === "invalid_samples", negativeSample);

  const enabledGood = enableCapacitySync(good.state, { confirmation: "enable-capacity-sync" }, depsFor(11));
  prove("schedule_policy_enables_fallback_after_proof_and_enable", schedulePolicy(enabledGood).fallbackEnabled === true && schedulePolicy(enabledGood).fallbackIntervalMs === FALLBACK_INTERVAL_MS, {});
}

// ---------------------------------------------------------------------------
// 9. Cross-machine merge gate incl. Codex UNDETECTABLE rule.
// ---------------------------------------------------------------------------
{
  let rejected = false;
  try {
    assertCrossMachineMergeAllowed({
      left: { identityStatus: "UNDETECTABLE_WITH_ALLOWED_METHODS", identityHash: null },
      right: { identityStatus: "UNDETECTABLE_WITH_ALLOWED_METHODS", identityHash: null },
    });
  } catch {
    rejected = true;
  }
  prove("codex_codex_merge_without_approval_rejected", rejected, {});
  let rejectedToo = false;
  try {
    assertCrossMachineMergeAllowed({
      left: { identityStatus: "UNDETECTABLE_WITH_ALLOWED_METHODS", identityHash: null },
      right: { identityStatus: "UNDETECTABLE_WITH_ALLOWED_METHODS", identityHash: null },
      approval: { approved: false, approvedBy: "owner", approvedAt: "2026-08-24T12:00:00.000Z" },
    });
  } catch {
    rejectedToo = true;
  }
  prove("non_approved_flag_insufficient", rejectedToo, {});
  let approvedOk = false;
  try {
    assertCrossMachineMergeAllowed({
      left: { identityStatus: "UNDETECTABLE_WITH_ALLOWED_METHODS", identityHash: null },
      right: { identityStatus: "UNDETECTABLE_WITH_ALLOWED_METHODS", identityHash: null },
      approval: { approved: true, approvedBy: "participant-owner", approvedAt: "2026-08-24T12:00:00.000Z" },
    });
    approvedOk = true;
  } catch {
    approvedOk = false;
  }
  prove("participant_approved_merge_allowed", approvedOk, {});
  let stableMatched = false;
  try {
    assertCrossMachineMergeAllowed({
      left: { identityStatus: "STABLE_PROVIDER_IDENTITY", identityHash: "sha256:abc" },
      right: { identityStatus: "STABLE_PROVIDER_IDENTITY", identityHash: "sha256:abc" },
    });
    stableMatched = true;
  } catch {
    stableMatched = false;
  }
  prove("matching_stable_identity_allows_merge", stableMatched, {});
  let stableMismatchRejected = false;
  try {
    assertCrossMachineMergeAllowed({
      left: { identityStatus: "STABLE_PROVIDER_IDENTITY", identityHash: "sha256:abc" },
      right: { identityStatus: "STABLE_PROVIDER_IDENTITY", identityHash: "sha256:def" },
    });
  } catch {
    stableMismatchRejected = true;
  }
  prove("mismatched_identity_requires_approval", stableMismatchRejected, {});
}

console.log(JSON.stringify({ schema: "plimsoll.provider-profiles-proof.v1", passed: checks.length, checks }, null, 2));
