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
 * for (test 10); a summary batch refused as stamp_not_issued_flood parks only the refused segments (test 11). Round 5 of B0 (round 11;
 * the read of C1 after round 10): a batch refused as stamp_from_other_ledger (a pair naming an install outside the ledger: after a
 * re-join that could not prove the previous install's key) is split the same way, the refused rows parked in the outbox, undelivered
 * and never judged, until a response reports a ledger other than the one they were parked under (round 12: the response names the
 * ledger beside its lineage, so a member of a chain an admin merged, whose lineage was already linked, retries too) or an admin
 * releases them (test 12). Round 7 of B0 (round 13; the read of C1 after round 12, blocking 2): the 400 names the ledger it judged
 * (`ledgerInstallId`, the uploader's ledger the request was authorized with), `partitionRefusedRows` hands that wire value on, and
 * `parkOutboxRows` parks under it, never under a value the collector supplied or learned itself (test 12, changed); the race in which
 * the chain was merged between the request's authorization and the sighting's lock is refused naming the stale view U, parked under
 * U from the wire, released by the next response naming X and retried, where parking under X would have kept the row parked on
 * every later X response (test 13). Pending until B2a lands.
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

/** the 400 body as the cloud sends it (round 13: it names the ledger the refusal was judged against, the uploader's ledger the request was authorized with) */
type StampRefusal = { reason: "stamp_from_other_ledger"; pairs: Array<{ install: string; version: number }>; ledgerInstallId: string };
type PartitionRefusedRows = (events: Array<{ event: { id: string; metadata: Record<string, unknown> } }>, refusal: StampRefusal) => { resubmit: string[]; parked: Array<{ id: string; pair: string }>; ledgerInstallId: string };
type Parking = {
  /** the outbox rows the cloud refused: kept undelivered, never leased for upload while parked, under the ledger the 400 named (round 13: the value partitionRefusedRows handed on from the wire, never one the collector supplied or learned itself); returns the number parked */
  parkOutboxRows(ids: string[], reason: "stamp_from_other_ledger", ledgerInstallId: string): number;
  parkedRows(): Array<{ id: string; reason: string; ledgerInstallId: string }>;
  /** lineage_linked: the rows parked under a ledger other than `ledgerInstallId` (the one the latest response named) go back to the outbox and are resubmitted, the others stay parked (round 12: a response for the same ledger changes nothing); admin_release: acknowledged as undeliverable_unlinked_ledger under the admin's receipt, never delivered, never judged, listed on the certify */
  releaseParkedRows(input: { reason: "lineage_linked"; ledgerInstallId: string } | { reason: "admin_release"; receipt: string }): { retried: number; acknowledged: number };
};
const refusal = (pairs: Array<{ install: string; version: number }>, ledgerInstallId: string): StampRefusal => ({ reason: "stamp_from_other_ledger", pairs, ledgerInstallId });

test("B2a C1 (round 11, read c1 r10 blockers 1-2; rounds 12-13): a batch refused as stamp_from_other_ledger (400, the pairs listed and the ledger the refusal was judged against named) is split: the rows whose pair names an install outside the ledger are parked in the outbox (undelivered, retained, never judged) under the ledger the 400 named, and the rest is resubmitted at once; /status lists them with the lineage; a parked row is retried when a response names a ledger other than the one it was parked under (the ledger was linked, or the chain it was in was merged), not on a response for the same ledger, and an admin's release acknowledges it as undeliverable without a judgment", pending("B2a"), async () => {
  const park = await loadSurface("../../../packages/collector-cli/src/lean/upload-park.ts");
  const partitionRefusedRows = fn(park, "partitionRefusedRows") as PartitionRefusedRows;
  const ev = (id: string, install: string | null, version: number | null) => ({ event: { id, metadata: install === null ? {} : { actorBindingInstall: install, actorBindingVersion: version } } });
  // after an UNLINKED re-join (the config was lost, or the key rotated): r1 and r4 are pre-re-join outbox rows stamped with X's pair, r2 is the new install's, r3 is null-stamped;
  // the refusing response was judged against Z's own (unlinked) ledger and says so
  const wire = refusal([{ install: INSTALL_X, version: 2 }, { install: INSTALL_X, version: 1 }], INSTALL_Z);
  const { resubmit, parked, ledgerInstallId } = partitionRefusedRows([ev("r1", INSTALL_X, 2), ev("r2", INSTALL_Z, 0), ev("r3", null, null), ev("r4", INSTALL_X, 1)], wire);
  assert.deepEqual(parked, [{ id: "r1", pair: `${INSTALL_X}:2` }, { id: "r4", pair: `${INSTALL_X}:1` }], "only the rows whose pair the cloud refused are parked");
  assert.deepEqual(resubmit, ["r2", "r3"], "the new install's rows and the null-stamped rows travel again at once");
  assert.equal(ledgerInstallId, wire.ledgerInstallId, "the ledger to park under is the one the 400 named (round 13): it comes out of the partition, from the wire");
  assert.throws(() => partitionRefusedRows([ev("r1", INSTALL_X, 2)], { reason: "stamp_from_other_ledger", pairs: [{ install: INSTALL_X, version: 2 }] } as unknown as StampRefusal), /ledgerInstallId/, "a 400 that names no ledger is not a refusal the collector can park on: nothing is parked under a guessed ledger");
  const status = fn(await loadSurface("../../../packages/collector-cli/src/lean/actor-binding-status.ts"), "actorBindingStatus") as (i: { binding: Binding; config: { cloudDeviceId?: string }; lineage?: "linked" | "unlinked"; parkedRows?: number }) => { state: string; lineage: "linked" | "unlinked" | null; parkedRows: number };
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", deviceId: "lean-device", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped & Parking;
    stamped.recordJoinedInstall(INSTALL_X, 2);
    const old = event(); buffer.append(old);                                            // captured under (X, 2) before the re-join
    stamped.recordJoinedInstall(INSTALL_Z, 0);                                          // the re-join (unlinked): the pair cleared, Z's 0 recorded
    const fresh = event(); buffer.append(fresh);
    assert.deepEqual([stampOf(buffer, old.id), stampOf(buffer, fresh.id)], [{ version: 2, install: INSTALL_X }, { version: 0, install: INSTALL_Z }]);
    const refused = partitionRefusedRows([ev(old.id, INSTALL_X, 2), ev(fresh.id, INSTALL_Z, 0)], refusal([{ install: INSTALL_X, version: 2 }], INSTALL_Z));   // the refusing response was judged against Z's own (unlinked) ledger
    assert.equal(stamped.parkOutboxRows(refused.parked.map((row) => row.id), "stamp_from_other_ledger", refused.ledgerInstallId), 1, "parked under the ledger the 400 named, as the partition handed it on from the wire");
    assert.deepEqual(stamped.parkedRows(), [{ id: old.id, reason: "stamp_from_other_ledger", ledgerInstallId: INSTALL_Z }]);
    assert.deepEqual(status({ binding: buffer.workspaceBinding() as Binding, config: { cloudDeviceId: INSTALL_Z }, lineage: "unlinked", parkedRows: stamped.parkedRows().length }), { state: "ok", lineage: "unlinked", parkedRows: 1 }, "/status shows the unlinked lineage and the parked count per host");
    assert.deepEqual(stamped.releaseParkedRows({ reason: "lineage_linked", ledgerInstallId: INSTALL_Z }), { retried: 0, acknowledged: 0 }, "a response naming the same ledger changes nothing: the row stays parked");
    assert.deepEqual(stamped.releaseParkedRows({ reason: "lineage_linked", ledgerInstallId: INSTALL_X }), { retried: 1, acknowledged: 0 }, "a response naming another ledger (the admin linked Z's ledger into X's, or merged the chain Z was in) sends the row again, to be judged in that ledger; a member of a merged chain whose lineage was already linked is retried the same way");
    assert.deepEqual(stamped.parkedRows(), []);
    assert.equal(stamped.parkOutboxRows([old.id], "stamp_from_other_ledger", INSTALL_Z), 1);   // refused again (still unlinked in this variant)
    assert.deepEqual(stamped.releaseParkedRows({ reason: "admin_release", receipt: "admin-release-lean-contract" }), { retried: 0, acknowledged: 1 }, "an admin's release acknowledges the row as undeliverable_unlinked_ledger: never delivered, never judged, listed on the certify; the old install's summary answer stands alone");
    assert.deepEqual([stamped.parkedRows(), stampOf(buffer, old.id)], [[], { version: 2, install: INSTALL_X }], "the identity row keeps its pair for the certify");
  } finally { close(); }
});

test("B2a C1 (round 13, read c1 r12 blocking 2): the wire-to-parking race: V's delivery was authorized with V's ledger U, the admin merged U's chain into X before the named install was locked, and the cloud refused against U, naming U on the 400; the collector parks the row under that wire value, the next response names X and releases it, and the row is retried and delivered again; parked under the current ledger X it would have stayed parked on every later X response", pending("B2a"), async () => {
  const partitionRefusedRows = fn(await loadSurface("../../../packages/collector-cli/src/lean/upload-park.ts"), "partitionRefusedRows") as PartitionRefusedRows;
  const INSTALL_U = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa0a3", INSTALL_V = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa0a5";
  const ev = (id: string, install: string, version: number) => ({ event: { id, metadata: { actorBindingInstall: install, actorBindingVersion: version } } });
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", deviceId: "lean-device", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped & Parking;
    stamped.recordJoinedInstall(INSTALL_V, 0);                                          // the ledger is joined with V (the proof-linked child of the unlinked root U)
    const row = event(); buffer.append(row);                                            // captured under (V, 0)
    assert.deepEqual(stampOf(buffer, row.id), { version: 0, install: INSTALL_V });
    // 700: the delivery is authorized with V's ledger read as U; 701: the admin merges U into X; 702: the sighting locks V, reads X, compares it with U and refuses, naming U
    const wire = refusal([{ install: INSTALL_V, version: 0 }], INSTALL_U);
    const refused = partitionRefusedRows([ev(row.id, INSTALL_V, 0)], wire);
    assert.deepEqual([refused.parked, refused.resubmit, refused.ledgerInstallId], [[{ id: row.id, pair: `${INSTALL_V}:0` }], [], INSTALL_U]);
    assert.equal(stamped.parkOutboxRows(refused.parked.map((r) => r.id), "stamp_from_other_ledger", refused.ledgerInstallId), 1);
    assert.deepEqual(stamped.parkedRows(), [{ id: row.id, reason: "stamp_from_other_ledger", ledgerInstallId: INSTALL_U }], "parked under the ledger the 400 named (U), not under the ledger current when it was parked (X)");
    assert.deepEqual(stamped.releaseParkedRows({ reason: "lineage_linked", ledgerInstallId: INSTALL_U }), { retried: 0, acknowledged: 0 }, "a response naming the ledger the refusal named changes nothing");
    // 720: the next acknowledged response names V's ledger, X (round 12: every response names the ledger): released, because U differs from X
    assert.deepEqual(stamped.releaseParkedRows({ reason: "lineage_linked", ledgerInstallId: INSTALL_X }), { retried: 1, acknowledged: 0 }, "released by the first response after the merge; resubmitted for X it is judged there (the cloud's side of the race is actor-binding-stamp test 17 and the Postgres proof)");
    assert.deepEqual(stamped.parkedRows(), []);
    assert.deepEqual(stampOf(buffer, row.id), { version: 0, install: INSTALL_V }, "the row travels again with its pair unchanged");
    // the contrast: parked under the current ledger X (the only value round 12's text let a collector find), the same row is not released by any X response
    assert.equal(stamped.parkOutboxRows([row.id], "stamp_from_other_ledger", INSTALL_X), 1);
    assert.deepEqual(stamped.releaseParkedRows({ reason: "lineage_linked", ledgerInstallId: INSTALL_X }), { retried: 0, acknowledged: 0 }, "parked under X, a response naming X leaves it parked: the row would wait for a ledger change that never comes");
    assert.deepEqual(stamped.releaseParkedRows({ reason: "admin_release", receipt: "admin-release-lean-contract-13" }), { retried: 0, acknowledged: 1 });
  } finally { close(); }
});
