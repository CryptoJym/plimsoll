#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema, collectorDeviceIdentityPath } from "../packages/collector-cli/src/config";
import {
  loadOrCreateDeviceIdentity,
  readDeviceIdentity,
  recordDeviceSeen,
  recordDeviceUpload,
  setDeviceKey,
  setDeviceStatus,
} from "../packages/collector-cli/src/device-identity";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";

import {
  DEVICE_REGISTRY_SCHEMA_VERSION,
  activateDevice,
  createDeviceRegistry,
  enrollDevice,
  leaveDevice,
  previewDeviceReassignment,
  reassignDevice,
  reinstallDevice,
  renderDeviceRegistry,
  revokeDevice,
  rotateDeviceKey,
  tenantScopedActorPseudonym,
} from "../packages/shared/src/device-registry";

delete process.env.PLIMSOLL_HOME;

const createdAt = "2026-08-26T12:00:00.000Z";
const seenAt = "2026-08-26T12:05:00.000Z";
const uploadedAt = "2026-08-26T12:06:00.000Z";
const TENANT_A = "tenant-a";
const TENANT_B = "tenant-b";
const DEVICE = "dev_01HZX4M7B5Y8Q9R2W3K6P1N0A7";
const REPLACEMENT_DEVICE = "dev_01HZX4M7B5Y8Q9R2W3K6P1N0A8";

const checks: string[] = [];
function check(name: string, condition: unknown) {
  assert.ok(condition, name);
  checks.push(name);
}

let registry = createDeviceRegistry();
const enrolled = enrollDevice(registry, {
  deviceId: DEVICE,
  tenantId: TENANT_A,
  keyId: "key_initial",
  appVersion: "0.6.0",
  policyVersion: "2026-05-17.metadata-v1",
  createdAt,
});
assert.equal(enrolled.ok, true);
registry = enrolled.registry;
check("enrollment_is_pending", enrolled.entry.status === "pending");
check("registry_schema_version_is_stable", enrolled.entry.schemaVersion === DEVICE_REGISTRY_SCHEMA_VERSION);

const activated = activateDevice(registry, DEVICE, seenAt);
assert.equal(activated.ok, true);
registry = activated.registry;
check("handshake_activates_pending_device", activated.entry.status === "active");
check("activation_is_idempotent", activateDevice(registry, DEVICE, seenAt).ok);

const rotated = rotateDeviceKey(registry, DEVICE, "key_rotated", seenAt);
assert.equal(rotated.ok, true);
registry = rotated.registry;
check("rotation_replaces_key_and_requires_handshake", rotated.entry.keyId === "key_rotated" && rotated.entry.status === "pending");
const repeatedRotation = rotateDeviceKey(registry, DEVICE, "key_rotated", seenAt);
check("rotation_is_idempotent", repeatedRotation.ok && repeatedRotation.changed === false);

const preview = previewDeviceReassignment(registry, DEVICE, TENANT_B);
check("reassignment_preview_has_no_mutation", preview.ok && preview.fromTenantId === TENANT_A && preview.toTenantId === TENANT_B && registry.devices[0]?.tenantId === TENANT_A);
const unconfirmedReassignment = reassignDevice(registry, DEVICE, TENANT_B, { at: seenAt });
check("reassignment_refuses_without_confirmation", !unconfirmedReassignment.ok && registry.devices[0]?.tenantId === TENANT_A);
const reassigned = reassignDevice(registry, DEVICE, TENANT_B, { confirmed: true, at: seenAt });
assert.equal(reassigned.ok, true);
registry = reassigned.registry;
check("reassignment_requires_explicit_confirmation", reassigned.entry.tenantId === TENANT_B && reassigned.entry.status === "pending");
const repeatedReassignment = reassignDevice(registry, DEVICE, TENANT_B, { confirmed: true, at: seenAt });
check("reassignment_is_idempotent", repeatedReassignment.ok && repeatedReassignment.changed === false);

const left = leaveDevice(registry, DEVICE, uploadedAt);
assert.equal(left.ok, true);
registry = left.registry;
check("leave_suspends_device", left.entry.status === "suspended");
const repeatedLeave = leaveDevice(registry, DEVICE, uploadedAt);
check("leave_is_idempotent", repeatedLeave.ok && repeatedLeave.changed === false);

const reinstalled = reinstallDevice(registry, DEVICE, {
  deviceId: REPLACEMENT_DEVICE,
  tenantId: TENANT_B,
  keyId: "key_reinstall",
  appVersion: "0.6.0",
  policyVersion: "2026-05-17.metadata-v1",
  createdAt: uploadedAt,
}, uploadedAt);
assert.equal(reinstalled.ok, true);
registry = reinstalled.registry;
check(
  "reinstall_revokes_old_identity_and_enrolls_new_pending_identity",
  registry.devices.find((entry) => entry.deviceId === DEVICE)?.status === "revoked" &&
    reinstalled.entry.deviceId === REPLACEMENT_DEVICE &&
    reinstalled.entry.status === "pending",
);
const repeatedReinstall = reinstallDevice(registry, DEVICE, {
  deviceId: REPLACEMENT_DEVICE,
  tenantId: TENANT_B,
  keyId: "key_reinstall",
  appVersion: "0.6.0",
  policyVersion: "2026-05-17.metadata-v1",
  createdAt: uploadedAt,
}, uploadedAt);
check("reinstall_is_idempotent", repeatedReinstall.ok && repeatedReinstall.changed === false);

const revoked = revokeDevice(registry, REPLACEMENT_DEVICE, uploadedAt);
assert.equal(revoked.ok, true);
registry = revoked.registry;
check("revocation_is_terminal", revoked.entry.status === "revoked");
const repeatedRevocation = revokeDevice(registry, REPLACEMENT_DEVICE, uploadedAt);
check("revocation_is_idempotent", repeatedRevocation.ok && repeatedRevocation.changed === false);
check("revoked_device_cannot_activate", !activateDevice(registry, REPLACEMENT_DEVICE, uploadedAt).ok);
check("revoked_device_cannot_rotate", !rotateDeviceKey(registry, REPLACEMENT_DEVICE, "key_again", uploadedAt).ok);
check("revoked_device_cannot_reassign", !reassignDevice(registry, REPLACEMENT_DEVICE, TENANT_A, { confirmed: true, at: uploadedAt }).ok);
check("reinstalled_identity_cannot_revive_old_revoked_identity", !activateDevice(registry, DEVICE, uploadedAt).ok && !rotateDeviceKey(registry, DEVICE, "key_again", uploadedAt).ok);

const sameWorkspaceActor = tenantScopedActorPseudonym("actor@example.invalid", TENANT_A);
const sameWorkspaceActorAgain = tenantScopedActorPseudonym("actor@example.invalid", TENANT_A);
const otherWorkspaceActor = tenantScopedActorPseudonym("actor@example.invalid", TENANT_B);
check("actor_pseudonym_converges_within_tenant", sameWorkspaceActor === sameWorkspaceActorAgain);
check("actor_pseudonym_is_unlinkable_across_tenants", sameWorkspaceActor !== otherWorkspaceActor);

const view = renderDeviceRegistry({
  registry,
  now: uploadedAt,
  telemetry: {
    [REPLACEMENT_DEVICE]: { lastSeenAt: seenAt, lastUploadAt: uploadedAt, queueAgeSeconds: 17 },
  },
});
const replacementView = view.find((entry) => entry.deviceId === REPLACEMENT_DEVICE);
check("registry_view_reports_versions_timestamps_queue_and_status", replacementView?.appVersion === "0.6.0" && replacementView?.policyVersion === "2026-05-17.metadata-v1" && replacementView?.lastSeenAt === seenAt && replacementView?.lastUploadAt === uploadedAt && replacementView?.queueAgeSeconds === 17 && replacementView?.status === "revoked");

const identityHome = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-device-identity-proof-"));
try {
  const initialIdentity = loadOrCreateDeviceIdentity(identityHome, { now: new Date(createdAt) });
  const stableIdentity = loadOrCreateDeviceIdentity(identityHome, { now: new Date(uploadedAt) });
  recordDeviceSeen(identityHome, new Date(seenAt));
  recordDeviceUpload(identityHome, new Date(uploadedAt));
  setDeviceStatus(identityHome, "active", new Date(uploadedAt));
  const persistedIdentity = readDeviceIdentity(identityHome);
  const identityDocument = JSON.parse(
    fs.readFileSync(collectorDeviceIdentityPath(identityHome), "utf8"),
  ) as Record<string, unknown>;
  const identityMode = fs.statSync(collectorDeviceIdentityPath(identityHome)).mode & 0o777;
  check(
    "local_identity_is_stable_atomic_and_statusful",
    initialIdentity.deviceId === stableIdentity.deviceId &&
      initialIdentity.keyId === stableIdentity.keyId &&
      persistedIdentity?.status === "active" &&
      persistedIdentity.lastSeenAt === uploadedAt &&
      persistedIdentity.lastUploadAt === uploadedAt &&
      !Object.hasOwn(identityDocument, "installKey") &&
      !Object.hasOwn(identityDocument, "ingestKey") &&
      !Object.hasOwn(identityDocument, "uploadSigningSecret") &&
      fs.readdirSync(identityHome).every((name) => name === "device.identity.json") &&
      identityMode === 0o600,
  );
  setDeviceStatus(identityHome, "revoked", new Date(uploadedAt));
  assert.throws(
    () => setDeviceKey(identityHome, "key_after_revoke", new Date(uploadedAt)),
    /device_identity_revoked_is_terminal/,
  );
  check(
    "local_revocation_is_terminal",
    readDeviceIdentity(identityHome)?.status === "revoked",
  );
} finally {
  fs.rmSync(identityHome, { recursive: true, force: true });
}

const ledgerDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-device-registry-proof-"));
const ledgerPath = path.join(ledgerDirectory, "ledger.sqlite");
const deviceConfig = collectorConfigSchema.parse({
  managed: true,
  tenantId: TENANT_A,
  installKey: "device-registry-proof-install",
  uploadUrl: "https://tenant-a.example/api/work-intelligence/ingest",
});
const deviceBuffer = new LocalEventBuffer(ledgerPath, {
  workspaceId: TENANT_A,
  deviceId: DEVICE,
  delivery: { enabled: true },
});
try {
  const history = appendForwardedHook(
    { id: "device-registry-a-history", source: "claude_code", event_type: "UserPromptSubmit" },
    { config: deviceConfig, buffer: deviceBuffer, source: "claude_code" },
  );
  const boundRows = deviceBuffer.database
    .prepare(
      `select workspace_id as workspaceId, device_id as deviceId
       from buffered_events where id = ?
       union all
       select workspace_id as workspaceId, device_id as deviceId
       from upload_outbox where raw_id = ?`,
    )
    .all(history.event.id, history.event.id) as Array<{ workspaceId: string; deviceId: string }>;
  check(
    "ledger_and_outbox_rows_carry_workspace_and_device_binding",
    boundRows.length === 2 && boundRows.every((row) => row.workspaceId === TENANT_A && row.deviceId === DEVICE),
  );

  deviceBuffer.transitionWorkspace(TENANT_A, TENANT_B, DEVICE);
  const beforeNewCapture = deviceBuffer.listUnuploaded().map((row) => row.id);
  const beforeNewLease = deviceBuffer.delivery.lease({ now: new Date() }).items.map((item) => item.envelope.event.id);
  const workspaceBConfig = collectorConfigSchema.parse({
    managed: true,
    tenantId: TENANT_B,
    installKey: "device-registry-proof-b-install",
    uploadUrl: "https://tenant-b.example/api/work-intelligence/ingest",
  });
  const newCapture = appendForwardedHook(
    { id: "device-registry-b-event", source: "claude_code", event_type: "UserPromptSubmit" },
    { config: workspaceBConfig, buffer: deviceBuffer, source: "claude_code" },
  );
  const visibleAfterReassignment = deviceBuffer.listUnuploaded().map((row) => row.id);
  const leasedAfterReassignment = deviceBuffer.delivery.lease({ now: new Date() });
  check(
    "reassignment_leases_only_new_workspace_device_rows",
    beforeNewCapture.length === 0 &&
      beforeNewLease.length === 0 &&
      visibleAfterReassignment.length === 1 &&
      visibleAfterReassignment[0] === newCapture.event.id &&
      leasedAfterReassignment.items.length === 1 &&
      leasedAfterReassignment.items[0]?.deviceId === DEVICE,
  );
} finally {
  deviceBuffer.close();
  fs.rmSync(ledgerDirectory, { recursive: true, force: true });
}

console.log(JSON.stringify({ ok: true, proof: "device-registry", checks }, null, 2));
