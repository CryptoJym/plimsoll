/**
 * B2a (collector): the binding-version stamp as the PAIR (install, version) on the identity row and on the wire, the echo on
 * every request, the null stamp before the first response, and the install scope across a re-join (docs/lean/CONTRACTS.md C1,
 * C4; ARCHITECTURE.md §5.1-5.2). Round 2 of B0 (review-r1 blocker 1): the persisted pair is scoped to the install the version was
 * issued to and cleared by a join, re-join or workspace transition; rows stamped before a re-join keep their old pair. Round 3 of
 * B0 (review-r2 blocker 1): the ledger records the install it is JOINED with at join activation (`recordJoinedInstall`, the
 * grant's DeviceInstall id and the handshake's version), and a versioned response is accepted only from that install; the old
 * install's late answer after a re-join (an in-flight request, or a daemon that loaded its config before the join, cli.ts:2479)
 * is ignored and counted, never a replacement of the pair. Pending until B2a lands.
 */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";

import { event, fn, loadSurface, openTempBuffer, pending } from "./_pending";

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
  /** Join activation (join.ts:605-621, in the same step as useWorkspace/transitionWorkspace): the grant's install and the handshake's version, if the response carried one. */
  recordJoinedInstall(installId: string, version: number | null): void;
  /** A versioned response: accepted only from the joined install and only upward; "ignored" for any other install (counted), "kept" for a lower version of the joined install. */
  recordActorBindingVersion(version: number, installId: string): "accepted" | "kept" | "ignored";
  workspaceBinding(): Binding;
};
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
    const metadata = batch!.events[0].event.metadata as { actorBindingVersion?: number; actorBindingInstall?: string };
    assert.equal(metadata.actorBindingVersion, 2, "the version travels on the wire");
    assert.equal(metadata.actorBindingInstall, INSTALL_X, "the install the version was issued to travels with it");
    assert.equal(aiWorkIngestBatchSchema.safeParse({ ...batch, actorBindingVersionHeard: 2 }).success, true);
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
