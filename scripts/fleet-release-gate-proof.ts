#!/usr/bin/env node

/**
 * Source-only client-side release gate for issue #105.
 *
 * The hosted registry is represented by an injected transport. This proof
 * exercises the real local ledger, outbox, device identity, and registry
 * contracts without making a provider or hosted request.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  loadOrCreateDeviceIdentity,
  readDeviceIdentity,
  setDeviceKey,
  setDeviceStatus,
} from "../packages/collector-cli/src/device-identity";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { DeliveryUploadError, uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import {
  activateDevice,
  createDeviceRegistry,
  enrollDevice,
  reinstallDevice,
  revokeDevice,
  rotateDeviceKey,
  tenantScopedActorPseudonym,
} from "../packages/shared/src/device-registry";

const WORKSPACE = "workspace-release-gate";
const DEVICE_A = "dev_fleet_machine_a_01";
const DEVICE_B = "dev_fleet_machine_b_01";
const DEVICE_REINSTALL = "dev_fleet_machine_a_02";
const KEY_A_OLD = "key_fleet_a_old";
const KEY_A_ROTATED = "key_fleet_a_new";
const KEY_B = "key_fleet_b";
const KEY_REINSTALL = "key_fleet_reinstall";
const INSTALL_A_OLD = "fixture-install-a-revoked";
const INSTALL_A_ROTATED = "fixture-install-a-rotated";
const INSTALL_B = "fixture-install-b";
const EVENT_A_1 = "00000000-0000-4000-8000-000000001051";
const EVENT_A_2 = "00000000-0000-4000-8000-000000001052";
const EVENT_B_1 = "00000000-0000-4000-8000-000000001053";
const EVENT_REINSTALL = "00000000-0000-4000-8000-000000001054";

type UploadBody = {
  tenantId: string;
  installKey: string;
  events: Array<{ event?: { id?: string } }>;
};

type RequestWitness = {
  tenantId: string;
  installKey: string;
  eventIds: string[];
};

const checks: string[] = [];

function check(name: string, condition: unknown, detail?: unknown) {
  assert.ok(condition, detail === undefined ? name : `${name}: ${JSON.stringify(detail)}`);
  checks.push(name);
}

function response(status: number, body: Record<string, unknown>) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function fixtureTransport(revokedInstallKeys: ReadonlySet<string>, requests: RequestWitness[]) {
  return (async (_input: Parameters<typeof fetch>[0], init?: RequestInit) => {
    const headers = new Headers(init?.headers);
    const body = JSON.parse(String(init?.body ?? "{}")) as UploadBody;
    const eventIds = body.events.map((entry) => entry.event?.id ?? "");
    requests.push({
      tenantId: body.tenantId,
      installKey: body.installKey,
      eventIds,
    });
    assert.equal(headers.get("x-plimsoll-install-key"), body.installKey);
    return revokedInstallKeys.has(body.installKey)
      ? response(401, { accepted: false })
      : response(200, { accepted: eventIds.length });
  }) as typeof fetch;
}

function config(input: { deviceId: string; keyId: string; installKey: string }) {
  return collectorConfigSchema.parse({
    managed: true,
    tenantId: WORKSPACE,
    deviceId: input.deviceId,
    keyId: input.keyId,
    installKey: input.installKey,
    uploadUrl: "https://workspace.example/api/work-intelligence/ingest",
    delivery: {
      maxBackoffSeconds: 30,
      requestTimeoutSeconds: 1,
      maxProbesPerCycle: 3,
      leaseSeconds: 60,
    },
  });
}

function captureWithConfig(buffer: LocalEventBuffer, eventId: string, cfg: ReturnType<typeof config>) {
  return appendForwardedHook(
    {
      id: eventId,
      source: "claude_code",
      event_type: "UserPromptSubmit",
    },
    { config: cfg, buffer, source: "claude_code" },
  );
}

function activeCount(buffer: LocalEventBuffer) {
  return buffer.delivery.status().remainingDelivery;
}

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-fleet-release-gate-"));
const homeA = path.join(root, "machine-a-home");
const homeB = path.join(root, "machine-b-home");
const homeReinstall = path.join(root, "reinstall-home");
const ledgerA = path.join(root, "machine-a.sqlite");
const ledgerB = path.join(root, "machine-b.sqlite");
const ledgerReinstall = path.join(root, "reinstall.sqlite");
for (const directory of [homeA, homeB, homeReinstall]) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });

let nowMs = Date.parse("2026-08-26T12:00:00.000Z");
const now = () => new Date(nowMs);
const advance = (seconds: number) => { nowMs += seconds * 1_000; };
const cfgAOld = config({ deviceId: DEVICE_A, keyId: KEY_A_OLD, installKey: INSTALL_A_OLD });
const cfgARotated = config({ deviceId: DEVICE_A, keyId: KEY_A_ROTATED, installKey: INSTALL_A_ROTATED });
const cfgB = config({ deviceId: DEVICE_B, keyId: KEY_B, installKey: INSTALL_B });
const cfgReinstallOldCredential = config({ deviceId: DEVICE_REINSTALL, keyId: KEY_REINSTALL, installKey: INSTALL_A_OLD });
const requests: RequestWitness[] = [];
const fetchImpl = fixtureTransport(new Set([INSTALL_A_OLD]), requests);

let bufferA: LocalEventBuffer | undefined;
let bufferB: LocalEventBuffer | undefined;
let bufferReinstall: LocalEventBuffer | undefined;

try {
  const identityA = loadOrCreateDeviceIdentity(homeA, {
    seed: { deviceId: DEVICE_A, keyId: KEY_A_OLD },
    now: now(),
  });
  const identityB = loadOrCreateDeviceIdentity(homeB, {
    seed: { deviceId: DEVICE_B, keyId: KEY_B },
    now: now(),
  });
  check("two_machine_identities_are_distinct", identityA.deviceId !== identityB.deviceId && identityA.keyId !== identityB.keyId);
  const identityDocuments = [homeA, homeB].map((home) =>
    JSON.parse(fs.readFileSync(path.join(home, ".plimsoll", "device.identity.json"), "utf8")) as Record<string, unknown>,
  );
  check(
    "device_identity_files_contain_metadata_only",
    identityDocuments.every((document) =>
      !Object.hasOwn(document, "installKey") &&
      !Object.hasOwn(document, "ingestKey") &&
      !Object.hasOwn(document, "uploadSigningSecret"),
    ),
  );

  const memberA = tenantScopedActorPseudonym("member-a", WORKSPACE);
  const memberB = tenantScopedActorPseudonym("member-b", WORKSPACE);
  const memberAOnSecondDevice = tenantScopedActorPseudonym("member-a", WORKSPACE);
  const memberAOtherWorkspace = tenantScopedActorPseudonym("member-a", "other-workspace");
  check("two_members_are_distinct_within_one_workspace", memberA !== memberB);
  check("actor_maps_across_own_devices_only_through_workspace_identity", memberA === memberAOnSecondDevice && memberA !== memberAOtherWorkspace);

  let registry = createDeviceRegistry();
  const enrollmentA = enrollDevice(registry, {
    deviceId: DEVICE_A,
    tenantId: WORKSPACE,
    keyId: KEY_A_OLD,
    appVersion: "0.1.0",
    policyVersion: "metadata-v1",
    createdAt: now().toISOString(),
  });
  assert.equal(enrollmentA.ok, true);
  registry = enrollmentA.registry;
  const enrollmentB = enrollDevice(registry, {
    deviceId: DEVICE_B,
    tenantId: WORKSPACE,
    keyId: KEY_B,
    appVersion: "0.1.0",
    policyVersion: "metadata-v1",
    createdAt: now().toISOString(),
  });
  assert.equal(enrollmentB.ok, true);
  registry = enrollmentB.registry;
  const activatedA = activateDevice(registry, DEVICE_A, now().toISOString());
  assert.equal(activatedA.ok, true);
  registry = activatedA.registry;
  const activatedB = activateDevice(registry, DEVICE_B, now().toISOString());
  assert.equal(activatedB.ok, true);
  registry = activatedB.registry;
  check("two_members_two_devices_are_registered_in_one_workspace", registry.devices.length === 2 && registry.devices.every((entry) => entry.tenantId === WORKSPACE && entry.status === "active"));

  const activeA = new LocalEventBuffer(ledgerA, {
    workspaceId: WORKSPACE,
    deviceId: DEVICE_A,
    delivery: { enabled: true, limits: cfgAOld.delivery, now },
  });
  const activeB = new LocalEventBuffer(ledgerB, {
    workspaceId: WORKSPACE,
    deviceId: DEVICE_B,
    delivery: { enabled: true, limits: cfgB.delivery, now },
  });
  bufferA = activeA;
  bufferB = activeB;
  captureWithConfig(activeA, EVENT_A_1, cfgAOld);
  captureWithConfig(activeB, EVENT_B_1, cfgB);
  check("captured_rows_are_bound_to_their_own_workspace_and_device", [activeA, activeB].every((buffer) => {
    const row = buffer.database.prepare("select workspace_id as workspaceId, device_id as deviceId from buffered_events limit 1").get() as { workspaceId: string; deviceId: string };
    return row.workspaceId === WORKSPACE && row.deviceId === buffer.currentDeviceId;
  }));

  let revokedFailure: unknown;
  try {
    await uploadBufferedEvents(cfgAOld, activeA, { fetchImpl, now });
  } catch (error) {
    revokedFailure = error;
  }
  check("revocation_returns_remote_auth_failure", revokedFailure instanceof DeliveryUploadError && revokedFailure.failureClass === "remote_auth");
  check("revocation_opens_auth_circuit_without_dead_letter", activeA.delivery.status(now).circuit.kind === "auth_blocked" && activeA.delivery.status(now).receipts.dead === 0);

  const bUpload = await uploadBufferedEvents(cfgB, activeB, { fetchImpl, now });
  check("second_machine_uploads_only_its_own_install_key", bUpload.uploadedEvents === 1 && requests[1]?.installKey === INSTALL_B && requests[1]?.eventIds.includes(EVENT_B_1));

  captureWithConfig(activeA, EVENT_A_2, cfgAOld);
  const beforeCircuitRequests = requests.length;
  const blocked = await uploadBufferedEvents(cfgAOld, activeA, { fetchImpl, now });
  check("next_upload_is_blocked_without_a_network_request", blocked.uploadedEvents === 0 && requests.length === beforeCircuitRequests && blocked.response && typeof blocked.response === "object" && (blocked.response as { status?: string }).status === "circuit_open");

  const rotatedRegistry = rotateDeviceKey(registry, DEVICE_A, KEY_A_ROTATED, now().toISOString());
  assert.equal(rotatedRegistry.ok, true);
  registry = rotatedRegistry.registry;
  const reactivatedA = activateDevice(registry, DEVICE_A, now().toISOString());
  assert.equal(reactivatedA.ok, true);
  registry = reactivatedA.registry;
  setDeviceKey(homeA, KEY_A_ROTATED, now());
  setDeviceStatus(homeA, "active", now());
  advance(31);
  const rotatedUpload = await uploadBufferedEvents(cfgARotated, activeA, { fetchImpl, now });
  check("credential_rotation_restores_only_the_intended_device", rotatedUpload.uploadedEvents === 2 && requests[requests.length - 1]?.installKey === INSTALL_A_ROTATED && registry.devices.find((entry) => entry.deviceId === DEVICE_A)?.keyId === KEY_A_ROTATED && registry.devices.find((entry) => entry.deviceId === DEVICE_A)?.status === "active");
  const activeAfterRotation = activeA.delivery.status(now);
  check("rotated_device_drains_each_queued_event_exactly_once", activeAfterRotation.remainingDelivery === 0 && activeAfterRotation.receipts.acknowledged === 2 && activeAfterRotation.receipts.dead === 0);
  const rotatedIdentity = readDeviceIdentity(homeA);
  check("local_rotation_persists_the_new_non_secret_key_id", rotatedIdentity?.deviceId === DEVICE_A && rotatedIdentity.keyId === KEY_A_ROTATED);

  let wrongDeviceError: unknown;
  try {
    await uploadBufferedEvents(cfgARotated, activeB, { fetchImpl, now });
  } catch (error) {
    wrongDeviceError = error;
  }
  check("rotated_credential_cannot_claim_the_other_device_ledger", wrongDeviceError instanceof Error && wrongDeviceError.message.includes("Ledger device binding mismatch") && requests.length === 3);

  const revokedRegistry = revokeDevice(registry, DEVICE_A, now().toISOString());
  assert.equal(revokedRegistry.ok, true);
  registry = revokedRegistry.registry;
  const reinstalled = reinstallDevice(registry, DEVICE_A, {
    deviceId: DEVICE_REINSTALL,
    tenantId: WORKSPACE,
    keyId: KEY_REINSTALL,
    appVersion: "0.1.0",
    policyVersion: "metadata-v1",
    createdAt: now().toISOString(),
  }, now().toISOString());
  assert.equal(reinstalled.ok, true);
  registry = reinstalled.registry;
  check("reinstall_creates_new_pending_identity_without_reviving_revoked_device", registry.devices.find((entry) => entry.deviceId === DEVICE_A)?.status === "revoked" && reinstalled.entry.deviceId === DEVICE_REINSTALL && reinstalled.entry.status === "pending");
  check("revoked_device_cannot_be_reactivated", !activateDevice(registry, DEVICE_A, now().toISOString()).ok);

  const reinstalledIdentity = loadOrCreateDeviceIdentity(homeReinstall, {
    seed: { deviceId: DEVICE_REINSTALL, keyId: KEY_REINSTALL },
    now: now(),
  });
  check("reinstall_gets_a_new_local_identity", reinstalledIdentity.deviceId === DEVICE_REINSTALL && reinstalledIdentity.keyId === KEY_REINSTALL && reinstalledIdentity.deviceId !== DEVICE_A);

  const reinstallBuffer = new LocalEventBuffer(ledgerReinstall, {
    workspaceId: WORKSPACE,
    deviceId: DEVICE_REINSTALL,
    delivery: { enabled: true, limits: cfgReinstallOldCredential.delivery, now },
  });
  bufferReinstall = reinstallBuffer;
  captureWithConfig(reinstallBuffer, EVENT_REINSTALL, cfgReinstallOldCredential);
  let reinstallFailure: unknown;
  try {
    await uploadBufferedEvents(cfgReinstallOldCredential, reinstallBuffer, { fetchImpl, now });
  } catch (error) {
    reinstallFailure = error;
  }
  check("fresh_reinstall_presenting_revoked_credential_is_refused", reinstallFailure instanceof DeliveryUploadError && reinstallFailure.failureClass === "remote_auth" && reinstallBuffer.delivery.status(now).receipts.acknowledged === 0);
  advance(31);
  let repeatedReinstallFailure: unknown;
  try {
    await uploadBufferedEvents(cfgReinstallOldCredential, reinstallBuffer, { fetchImpl, now });
  } catch (error) {
    repeatedReinstallFailure = error;
  }
  check("revoked_credential_remains_refused_after_circuit_expiry", repeatedReinstallFailure instanceof DeliveryUploadError && repeatedReinstallFailure.failureClass === "remote_auth" && requests.filter((request) => request.installKey === INSTALL_A_OLD).length === 3);

  setDeviceStatus(homeA, "active", now());
  setDeviceStatus(homeA, "revoked", now());
  check("local_revocation_is_terminal_after_rotation", readDeviceIdentity(homeA)?.status === "revoked");
  check("no_extra_device_rows_are_eligible_after_terminal_revocation", activeCount(activeA) === 0);
} finally {
  bufferReinstall?.close();
  bufferB?.close();
  bufferA?.close();
  fs.rmSync(root, { recursive: true, force: true });
}

console.log(JSON.stringify({
  proof: "fleet-release-gate",
  status: "passed",
  checks: checks.length,
  requestCount: requests.length,
  liveStateTouched: false,
  providerCalls: 0,
}, null, 2));
