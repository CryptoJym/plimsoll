/**
 * B2a (collector): the binding-version stamp as the PAIR (install, version) on the identity row and on the wire, the echo on
 * every request, the null stamp before the first response, and the install scope across a re-join (docs/lean/CONTRACTS.md C1,
 * C4; ARCHITECTURE.md §5.1-5.2). Round 2 of B0 (review-r1 blocker 1): the persisted pair is scoped to the install the version was
 * issued to and cleared by a join, re-join or workspace transition; rows stamped before a re-join keep their old pair. Round 3 of
 * B0 (review-r2 blocker 1): the ledger records the install it is JOINED with at join activation (`recordJoinedInstall`, the
 * grant's DeviceInstall id and the handshake's version), and a versioned response is accepted only from that install; the old
 * install's late answer after a re-join (an in-flight request, or a daemon that loaded its config before the join, cli.ts:2479)
 * is ignored and counted, never a replacement of the pair. Round 4 of B0 (round 10; the read of C1/C4 after round 9): the handshake's
 * version is recorded only when the handshake response names the grant's install (test 8, driven through join.ts activation with a
 * fake cloud); the join request proves possession of the previous install's key so the cloud can link the new install to the
 * ledger's lineage (test 8); a pre-B2a ledger is seeded from the config's cloudDeviceId or reports joined_install_unknown (test 9);
 * a join that changed the ledger but not the config is reported as join_incomplete, and every batch names the install its echo is
 * for (test 10); a summary batch refused as stamp_not_issued_flood parks only the refused segments (test 11). Pending until B2a lands.
 */
import assert from "node:assert/strict";
import { createHmac, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { event, fn, loadSurface, openTempBuffer, pending } from "./_pending";
import { acknowledgingFetch } from "../../../scripts/fixtures/delivery-ack-fixture";

// Today's product modules are loaded at run time too (review-r1 low): a rename stays a pending failure, never a red CI step.
type Parse<T> = { parse(v: unknown): T; safeParse(v: unknown): { success: boolean } };
async function wire() {
  const [config, upload, shared, envelope] = await Promise.all([
    loadSurface("../../../packages/collector-cli/src/config.ts"), loadSurface("../../../packages/collector-cli/src/upload.ts"),
    loadSurface("../../../packages/shared/src/index.ts"), loadSurface("../../../packages/collector-cli/src/outbound-envelope.ts"),
  ]);
  return {
    collectorConfigSchema: config.collectorConfigSchema as Parse<unknown>,
    buildIngestBatch: fn(upload, "buildIngestBatch") as (config: unknown, buffer: unknown) => { batch?: { events: Array<{ event: { id: string; metadata: unknown } }> } },
    aiWorkIngestBatchSchema: shared.aiWorkIngestBatchSchema as Parse<unknown>,
    metadataKeyDisposition: fn(shared, "metadataKeyDisposition") as (key: string) => { valueKind?: string } | undefined,
    sealOutboundEnvelope: fn(envelope, "sealOutboundEnvelope") as (input: unknown) => { ok: boolean; envelope: { event: { metadata: unknown } } },
  };
}

type Binding = { actorBindingVersion?: number | null; actorBindingInstall?: string | null; joinedInstall?: string | null; ignoredResponses?: number } | null;
type Stamped = {
  /** Join activation (join.ts:605-621, in the same step as useWorkspace/transitionWorkspace): the grant's install and the handshake's version, if the response carried one AND named that install (round 10). */
  recordJoinedInstall(installId: string, version: number | null): void;
  /** A versioned response: accepted only from the joined install and only upward; "ignored" for any other install (counted), "kept" for a lower version of the joined install. */
  recordActorBindingVersion(version: number, installId: string): "accepted" | "kept" | "ignored";
  /** B2a's first start on a ledger joined before B2a: seeds joined_install from the config's cloudDeviceId ("seeded"), keeps a recorded one ("kept"), or reports the unknown state ("unknown"). */
  seedJoinedInstall(cloudDeviceId: string | undefined): "seeded" | "kept" | "unknown";
  workspaceBinding(): Binding;
};
const HOME_ROOT = () => fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-lean-join-"));
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
const urlOf = (input: Parameters<typeof fetch>[0]) => (typeof input === "string" ? new URL(input) : input instanceof URL ? input : new URL(input.url));
/**
 * A joined collector home (config for the OLD install X with its install key and cloudDeviceId) and a fake cloud whose join route
 * grants install Z and whose handshake upload answers with `handshake` (deviceId and actorBindingVersion are what B2a reads).
 * `performJoin` is today's join.ts path: the token is redeemed, the one-event handshake runs on a temporary ledger, and activation
 * runs useWorkspace/transitionWorkspace on the active ledger before the config is written (join.ts:605-648).
 */
async function joinedThroughJoinTs(handshake: Record<string, unknown>, options: { previousInstallKey?: string } = {}) {
  const [join, config] = await Promise.all([loadSurface("../../../packages/collector-cli/src/join.ts"), loadSurface("../../../packages/collector-cli/src/config.ts")]);
  const performJoin = fn(join, "performJoin") as (o: Record<string, unknown>) => Promise<{ joined: boolean; deviceId: string }>;
  const configSchema = config.collectorConfigSchema as { parse(v: unknown): Record<string, unknown> };
  const collectorConfigPath = fn(config, "collectorConfigPath") as (home: string) => string;
  const collectorBufferPath = fn(config, "collectorBufferPath") as (home: string) => string;
  const homeDir = HOME_ROOT();
  const previousKey = options.previousInstallKey ?? "pli_previous_install_key_x";
  const old = configSchema.parse({ tenantId: "tenant-lean-contract", installKey: previousKey, cloudDeviceId: INSTALL_X, uploadUrl: "https://cloud.example/api/work-intelligence/ingest", managed: true, port: 49123 });
  const configPath = collectorConfigPath(homeDir);
  fs.mkdirSync(path.dirname(configPath), { recursive: true, mode: 0o700 });
  fs.writeFileSync(configPath, `${JSON.stringify(old, null, 2)}\n`, { mode: 0o600 });
  const requests: Array<{ url: string; body: Record<string, unknown> }> = [];
  const cloud = acknowledgingFetch((async (input, init) => {
    const url = urlOf(input);
    const body = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
    requests.push({ url: url.href, body });
    if (url.pathname.endsWith("/join")) return json({ ok: true, tenantId: "tenant-lean-contract", deviceId: INSTALL_Z, installKey: "pli_new_install_key_z", uploadUrl: "https://cloud.example/api/work-intelligence/ingest" }, 201);
    return json({ ok: true, accepted: 1, ...handshake }, 200);
  }) as typeof fetch);
  const token = "pljt_lean-contract-token";
  const result = await performJoin({ target: `https://cloud.example#${token}`, homeDir, reassign: true, fetchImpl: cloud, temporaryRoot: path.join(homeDir, "handshake-tmp") });
  const ledgerPath = collectorBufferPath(homeDir);
  return { result, requests, token, previousKey, homeDir, ledgerPath, configPath, close: () => fs.rmSync(homeDir, { recursive: true, force: true }) };
}
const INSTALL_X = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa001";
const INSTALL_Z = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa002";
const stampOf = (buffer: ReturnType<typeof openTempBuffer>["buffer"], id: string) =>
  buffer.database.prepare("select actor_binding_version as version, actor_binding_install as install from summary_members where event_id = ?").get(id) as { version: number | null; install: string | null } | undefined;
const pairOf = (buffer: ReturnType<typeof openTempBuffer>["buffer"]) => {
  const b = buffer.workspaceBinding() as Binding;
  return [b?.actorBindingVersion, b?.actorBindingInstall];
};

test("B2a C4: the persisted binding version is null before the first response, and rows admitted before it carry a null stamp", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const before = event();
    buffer.append(before);
    const binding = buffer.workspaceBinding() as Binding;
    assert.ok(binding && "actorBindingVersion" in binding, "workspaceBinding() reports actorBindingVersion");
    assert.equal(binding.actorBindingVersion, null);
    assert.equal(binding.actorBindingInstall, null);
    assert.equal(binding.joinedInstall, null, "a ledger that was never joined (and has no config to seed from) knows no install");
    const stamp = stampOf(buffer, before.id);
    assert.ok(stamp, "an identity row exists for the admitted row");
    assert.deepEqual(stamp, { version: null, install: null });
  } finally { close(); }
});

test("B2a C1: after a response from the joined install supplies a version the collector persists the (install, version) pair and stamps every later row with it; a lower version from the same install never lowers it", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped;
    stamped.recordJoinedInstall(INSTALL_X, null);                 // the ledger is joined with X (a join whose handshake carried no version)
    assert.equal(stamped.recordActorBindingVersion(1, INSTALL_X), "accepted");
    assert.deepEqual(pairOf(buffer), [1, INSTALL_X]);
    const after = event();
    buffer.append(after);
    assert.deepEqual(stampOf(buffer, after.id), { version: 1, install: INSTALL_X });
    assert.equal(stamped.recordActorBindingVersion(0, INSTALL_X), "kept"); // a stale response of the SAME install never lowers the persisted version
    assert.equal((buffer.workspaceBinding() as Binding)?.actorBindingVersion, 1);
  } finally { close(); }
});

test("B2a C1: every upload batch echoes actorBindingVersionHeard (>= every stamp it carries for the current install) and carries the pair on the wire; the shared schema accepts it", pending("B2a"), async () => {
  const { collectorConfigSchema, buildIngestBatch, aiWorkIngestBatchSchema } = await wire();
  const { buffer, close } = openTempBuffer({ workspaceId: "lean-contract", deviceId: "lean-device", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped;
    stamped.recordJoinedInstall(INSTALL_X, null);
    stamped.recordActorBindingVersion(2, INSTALL_X);
    buffer.append(event());
    const config = collectorConfigSchema.parse({ tenantId: "lean-contract", installKey: "lean-contract-install", deviceId: "lean-device" });
    const { batch } = buildIngestBatch(config, buffer);
    assert.ok(batch, "a batch was built");
    assert.equal((batch as unknown as { actorBindingVersionHeard?: number | null }).actorBindingVersionHeard, 2);
    assert.equal((batch as unknown as { actorBindingInstallHeard?: string | null }).actorBindingInstallHeard, INSTALL_X, "the echo names the install it is for (round 10: the cloud attributes it to that install, never to whoever authenticated)");
    const metadata = batch!.events[0].event.metadata as { actorBindingVersion?: number; actorBindingInstall?: string };
    assert.equal(metadata.actorBindingVersion, 2, "the version travels on the wire");
    assert.equal(metadata.actorBindingInstall, INSTALL_X, "the install the version was issued to travels with it");
    assert.equal(aiWorkIngestBatchSchema.safeParse({ ...batch, actorBindingVersionHeard: 2, actorBindingInstallHeard: INSTALL_X }).success, true);
  } finally { close(); }
});

test("B2a C1: metadata.actorBindingVersion and metadata.actorBindingInstall are allowlisted identifier keys that survive the outbound seal", pending("B2a"), async () => {
  const { metadataKeyDisposition, sealOutboundEnvelope } = await wire();
  for (const key of ["actorBindingVersion", "actorBindingInstall"]) {
    assert.ok(metadataKeyDisposition(key), `${key}: disposition exists`);
    assert.equal(metadataKeyDisposition(key)?.valueKind, metadataKeyDisposition("workItemId")?.valueKind, key);
  }
  const sealed = sealOutboundEnvelope({ event: event({ metadata: { actorBindingVersion: 3, actorBindingInstall: INSTALL_X } }), suppressedFields: [] });
  assert.ok(sealed.ok, "sealed");
  assert.deepEqual([(sealed.envelope.event.metadata as { actorBindingVersion?: number }).actorBindingVersion, (sealed.envelope.event.metadata as { actorBindingInstall?: string }).actorBindingInstall], [3, INSTALL_X]);
});

test("B2a C4 (round 2-3): the persisted pair is scoped to the joined install: a re-join (new installation epoch) clears the pair and the joined install, rows before the new install's first versioned response are null-stamped, the new install's version 0 is accepted, and a workspace transition clears it too", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped;
    stamped.recordJoinedInstall(INSTALL_X, null);
    stamped.recordActorBindingVersion(2, INSTALL_X);
    // the Mac re-joins: join.ts activates the grant with a NEW installation epoch on the same workspace (useWorkspace(tenant, device, epoch))
    buffer.useWorkspace("tenant-lean-contract", undefined, randomUUID());
    assert.deepEqual([...pairOf(buffer), (buffer.workspaceBinding() as Binding)?.joinedInstall], [null, null, null], "the re-join clears the pair and the joined install");
    stamped.recordJoinedInstall(INSTALL_Z, null);                 // join activation whose handshake response carried no version (C4: the disclosed window)
    const beforeFirstResponse = event();
    buffer.append(beforeFirstResponse);
    assert.deepEqual(stampOf(buffer, beforeFirstResponse.id), { version: null, install: null }, "null until the new install's first versioned response (C4)");
    assert.equal(stamped.recordActorBindingVersion(0, INSTALL_Z), "accepted", "the NEW install's version 0: accepted, not refused as lower than X's 2");
    assert.deepEqual(pairOf(buffer), [0, INSTALL_Z]);
    const underZ = event();
    buffer.append(underZ);
    assert.deepEqual(stampOf(buffer, underZ.id), { version: 0, install: INSTALL_Z });
    // a workspace transition (issue 0089 flow) clears the pair and the joined install as well
    buffer.transitionWorkspace("tenant-lean-contract", "tenant-lean-contract-2");
    assert.deepEqual([...pairOf(buffer), (buffer.workspaceBinding() as Binding)?.joinedInstall], [null, null, null]);
  } finally { close(); }
});

test("B2a C1/C4 (round 2-3): join activation records the new install with the handshake's version 0; rows stamped before the re-join keep their old pair on the wire while the batch echoes the current install's version", pending("B2a"), async () => {
  const { collectorConfigSchema, buildIngestBatch } = await wire();
  const { buffer, close } = openTempBuffer({ workspaceId: "lean-contract", deviceId: "lean-device", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped;
    stamped.recordJoinedInstall(INSTALL_X, null);
    stamped.recordActorBindingVersion(2, INSTALL_X);
    const preJoin = event();
    buffer.append(preJoin);                                    // captured under X's v2, still in the outbox at the re-join
    buffer.useWorkspace("lean-contract", undefined, randomUUID());
    stamped.recordJoinedInstall(INSTALL_Z, 0);                // join activation through the join path: the grant's install Z and the handshake's version 0
    assert.deepEqual(pairOf(buffer), [0, INSTALL_Z]);
    const postJoin = event();
    buffer.append(postJoin);
    assert.deepEqual(stampOf(buffer, preJoin.id), { version: 2, install: INSTALL_X }, "the pre-join row keeps its pair");
    assert.deepEqual(stampOf(buffer, postJoin.id), { version: 0, install: INSTALL_Z });
    const config = collectorConfigSchema.parse({ tenantId: "lean-contract", installKey: "lean-contract-install", deviceId: "lean-device" });
    const { batch } = buildIngestBatch(config, buffer);
    assert.ok(batch, "a batch was built");
    assert.equal((batch as unknown as { actorBindingVersionHeard?: number | null }).actorBindingVersionHeard, 0, "the echo is the CURRENT install's version; an earlier install's stamps do not raise it");
    const byId = new Map(batch!.events.map((row) => [row.event.id, row.event.metadata as { actorBindingVersion?: number; actorBindingInstall?: string }]));
    assert.deepEqual([byId.get(preJoin.id)?.actorBindingVersion, byId.get(preJoin.id)?.actorBindingInstall], [2, INSTALL_X], "the old pair travels unchanged");
    assert.deepEqual([byId.get(postJoin.id)?.actorBindingVersion, byId.get(postJoin.id)?.actorBindingInstall], [0, INSTALL_Z]);
  } finally { close(); }
});

test("B2a C4 (round 3): after a re-join a response from the OLD install is ignored and counted, never accepted: the pair stays the joined install's, rows captured after it are stamped with the joined install's pair, and the echo is the joined install's version", pending("B2a"), async () => {
  const { collectorConfigSchema, buildIngestBatch } = await wire();
  const { buffer, close } = openTempBuffer({ workspaceId: "lean-contract", deviceId: "lean-device", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped;
    stamped.recordJoinedInstall(INSTALL_X, null);
    stamped.recordActorBindingVersion(2, INSTALL_X);           // X's collector reached v2 before the re-join
    buffer.useWorkspace("lean-contract", undefined, randomUUID());
    stamped.recordJoinedInstall(INSTALL_Z, 0);                 // re-join: the ledger is now joined with Z; the handshake supplied Z's 0
    // the old install's answer to an in-flight request (or from a daemon that loaded its config before the join) arrives after the join
    assert.equal(stamped.recordActorBindingVersion(2, INSTALL_X), "ignored", "a response from an install other than the joined one is ignored (round 2 replaced the pair with it)");
    assert.deepEqual(pairOf(buffer), [0, INSTALL_Z], "the pair is untouched");
    assert.equal((buffer.workspaceBinding() as Binding)?.ignoredResponses, 1, "the ignored response is counted for /status");
    const underD = event();
    buffer.append(underD);
    assert.deepEqual(stampOf(buffer, underD.id), { version: 0, install: INSTALL_Z }, "rows captured under the new install's actor carry the new install's pair, never (X, 2)");
    assert.equal(stamped.recordActorBindingVersion(1, INSTALL_Z), "accepted", "the joined install's own later version is accepted");
    assert.equal(stamped.recordActorBindingVersion(3, INSTALL_X), "ignored", "a later stale answer from X is ignored too");
    assert.deepEqual([...pairOf(buffer), (buffer.workspaceBinding() as Binding)?.ignoredResponses], [1, INSTALL_Z, 2]);
    const config = collectorConfigSchema.parse({ tenantId: "lean-contract", installKey: "lean-contract-install", deviceId: "lean-device" });
    const { batch } = buildIngestBatch(config, buffer);
    assert.ok(batch, "a batch was built");
    assert.equal((batch as unknown as { actorBindingVersionHeard?: number | null }).actorBindingVersionHeard, 1, "the echo is the joined install's version, never the old install's 2 or 3");
  } finally { close(); }
});

test("B2a C4 (round 4, through join.ts): join activation records the grant's install as joined_install with the handshake's version when the handshake response names that install; a response naming another install leaves the pair null and is counted; the join request proves possession of the previous install's key", pending("B2a"), async () => {
  const { LocalEventBuffer } = await loadSurface("../../../packages/collector-cli/src/buffer.ts") as unknown as { LocalEventBuffer: new (p: string, o?: unknown) => { workspaceBinding(): Binding; close(): void } };
  // the honest cloud: the handshake response carries the grant's install and its version 0
  const honest = await joinedThroughJoinTs({ deviceId: INSTALL_Z, actorBindingVersion: 0 });
  try {
    assert.equal(honest.result.joined, true);
    const joinRequest = honest.requests.find((r) => r.url.endsWith("/join"))!.body as { previousInstall?: { deviceId?: string; proof?: string } };
    assert.equal(joinRequest.previousInstall?.deviceId, INSTALL_X, "the join request names the previous install so the cloud can link the ledger's lineage");
    assert.equal(joinRequest.previousInstall?.proof, createHmac("sha256", honest.previousKey).update(honest.token).digest("hex"), "with proof of possession of its install key (HMAC-SHA256 over the token): the cloud links only on a verified proof");
    const ledger = new LocalEventBuffer(honest.ledgerPath);
    try {
      const binding = ledger.workspaceBinding();
      assert.equal(binding?.joinedInstall, INSTALL_Z, "the grant's DeviceInstall id, from the join path itself");
      assert.deepEqual([binding?.actorBindingVersion, binding?.actorBindingInstall], [0, INSTALL_Z], "the handshake's version becomes the pair");
    } finally { ledger.close(); }
  } finally { honest.close(); }
  // a cloud whose handshake response names ANOTHER install (a misrouted or stale answer): joined_install is still the grant's, the pair stays null, the answer is counted
  const other = await joinedThroughJoinTs({ deviceId: INSTALL_X, actorBindingVersion: 3 });
  try {
    assert.equal(other.result.joined, true, "the join itself is never blocked by a versionless or misrouted handshake");
    const ledger = new LocalEventBuffer(other.ledgerPath);
    try {
      const binding = ledger.workspaceBinding();
      assert.equal(binding?.joinedInstall, INSTALL_Z);
      assert.deepEqual([binding?.actorBindingVersion, binding?.actorBindingInstall, binding?.ignoredResponses], [null, null, 1], "the handshake's version is recorded only when its deviceId equals the grant's install (round 10)");
    } finally { ledger.close(); }
  } finally { other.close(); }
});

test("B2a C4 (round 4): a ledger joined before B2a is seeded from the config's cloudDeviceId at B2a's first start; one with neither reports joined_install_unknown and stamps null; a recorded joined install is kept", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped;
    assert.equal(stamped.seedJoinedInstall(undefined), "unknown", "a pre-B2a ledger whose config carries no cloudDeviceId (stageGrant writes it only when the grant carried one, join.ts:225)");
    assert.equal((buffer.workspaceBinding() as Binding)?.joinedInstall, null);
    assert.equal(stamped.recordActorBindingVersion(1, INSTALL_X), "ignored", "every response is ignored and counted until the next join");
    const nullStamped = event();
    buffer.append(nullStamped);
    assert.deepEqual(stampOf(buffer, nullStamped.id), { version: null, install: null });
    assert.equal(stamped.seedJoinedInstall(INSTALL_X), "seeded", "the config's cloudDeviceId (config and ledger are written together at a join, so at start they agree)");
    assert.equal((buffer.workspaceBinding() as Binding)?.joinedInstall, INSTALL_X);
    assert.equal(stamped.recordActorBindingVersion(1, INSTALL_X), "accepted");
    assert.equal(stamped.seedJoinedInstall(INSTALL_Z), "kept", "a recorded joined install is never overwritten by a seed");
    assert.equal((buffer.workspaceBinding() as Binding)?.joinedInstall, INSTALL_X);
  } finally { close(); }
});

test("B2a C4 (round 4): a join that changed the ledger but not the config (join.ts:605-648 fails after the ledger step) is reported as join_incomplete until join --resume completes it; every batch names the install its echo is for, so the cloud never attributes the ledger's echo to the config's install", pending("B2a"), async () => {
  const { collectorConfigSchema, buildIngestBatch } = await wire();
  const status = fn(await loadSurface("../../../packages/collector-cli/src/lean/actor-binding-status.ts"), "actorBindingStatus") as (i: { binding: Binding; config: { cloudDeviceId?: string } }) => { state: "ok" | "joined_install_unknown" | "join_incomplete"; joinedInstall: string | null; configInstall: string | null };
  const { buffer, close } = openTempBuffer({ workspaceId: "lean-contract", deviceId: "lean-device", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped;
    stamped.recordJoinedInstall(INSTALL_Z, 0);                                          // the ledger step of the join ran (joined_install = Z) ...
    const staleConfig = collectorConfigSchema.parse({ tenantId: "lean-contract", installKey: "lean-contract-install", deviceId: "lean-device", cloudDeviceId: INSTALL_X }) as { cloudDeviceId?: string };
    assert.deepEqual(status({ binding: buffer.workspaceBinding() as Binding, config: staleConfig }), { state: "join_incomplete", joinedInstall: INSTALL_Z, configInstall: INSTALL_X }, "... but the config still names X: /status says so (join --resume completes the activation)");
    buffer.append(event());
    const { batch } = buildIngestBatch(staleConfig, buffer);
    assert.ok(batch, "a batch was built");
    const echo = batch as unknown as { actorBindingVersionHeard?: number | null; actorBindingInstallHeard?: string | null };
    assert.deepEqual([echo.actorBindingVersionHeard, echo.actorBindingInstallHeard], [0, INSTALL_Z], "the echo is the ledger's joined install's and says so; the cloud ignores and counts an echo whose install is not the one that authenticated (echo_from_other_install), so no fact and no heard_at can arise for X");
    assert.deepEqual(status({ binding: buffer.workspaceBinding() as Binding, config: { cloudDeviceId: INSTALL_Z } }), { state: "ok", joinedInstall: INSTALL_Z, configInstall: INSTALL_Z });
    assert.equal(status({ binding: { actorBindingVersion: null, actorBindingInstall: null, joinedInstall: null, ignoredResponses: 0 }, config: {} }).state, "joined_install_unknown");
  } finally { close(); }
});

test("B2a C1 (round 4, low): a summary batch refused as stamp_not_issued_flood (400) is split: the segments whose parts name a refused pair are parked and listed, the rest is resubmitted, so one faulty segment never stalls the claim and receipt lane", pending("B2a"), async () => {
  const partition = fn(await loadSurface("../../../packages/collector-cli/src/lean/summary-upload.ts"), "partitionFloodRefusal") as (items: unknown[], refusal: { pairs: Array<{ install: string; version: number }> }) => { resubmit: unknown[]; parked: Array<{ itemKey: string; segmentSeq: number; pairs: string[] }> };
  const item = (key: string, segments: Array<[number, string[]]>) => ({ key, segments: segments.map(([seq, pairs]) => ({ seq, partsByPair: Object.fromEntries(pairs.map((p) => [p, { memberCount: 1 }])) })) });
  const faulty = item("session:1", [[1, [`${INSTALL_X}:2`]], [2, [`${INSTALL_X}:9`, `${INSTALL_X}:2`]]]);
  const honest = item("session:2", [[1, [`${INSTALL_X}:2`]]]);
  const claim = { key: "claim", segments: [] };
  const { resubmit, parked } = partition([faulty, honest, claim], { pairs: [{ install: INSTALL_X, version: 9 }] });
  assert.deepEqual(parked, [{ itemKey: "session:1", segmentSeq: 2, pairs: [`${INSTALL_X}:9`] }], "only the segment whose part names the refused pair is parked (retried after the next rebind is heard, or released by an admin)");
  assert.deepEqual(resubmit.map((i) => (i as { key: string }).key), ["session:1", "session:2", "claim"], "every item travels again ...");
  assert.deepEqual((resubmit[0] as { segments: Array<{ seq: number }> }).segments.map((s) => s.seq), [1], "... the faulty item without its parked segment; the claim and the receipts of the other segments are unaffected");
});
