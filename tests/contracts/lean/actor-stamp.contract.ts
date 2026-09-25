/**
 * B2a (collector): the binding-version stamp as the PAIR (install, version) on the identity row and on the wire, the echo on
 * every request, the null stamp before the first response, and the install scope across a re-join (docs/lean/CONTRACTS.md C1,
 * C4; ARCHITECTURE.md §5.1-5.2). Round 2 of B0 (review-r1 blocker 1): the persisted pair is scoped to the install the version was
 * issued to and cleared by a join, re-join or workspace transition, so an honest collector never carries one install's version
 * into another install's rows; rows stamped before a re-join keep their old pair. Pending until B2a lands.
 */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";

import { collectorConfigSchema } from "../../../packages/collector-cli/src/config";
import { sealOutboundEnvelope } from "../../../packages/collector-cli/src/outbound-envelope";
import { buildIngestBatch } from "../../../packages/collector-cli/src/upload";
import { aiWorkIngestBatchSchema, metadataKeyDisposition } from "../../../packages/shared/src/index";
import { event, openTempBuffer, pending } from "./_pending";

type Binding = { actorBindingVersion?: number | null; actorBindingInstall?: string | null } | null;
type Stamped = { recordActorBindingVersion(version: number, installId: string): void; workspaceBinding(): Binding };
const INSTALL_X = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa001";
const INSTALL_Z = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa002";
const stampOf = (buffer: ReturnType<typeof openTempBuffer>["buffer"], id: string) =>
  buffer.database.prepare("select actor_binding_version as version, actor_binding_install as install from summary_members where event_id = ?").get(id) as { version: number | null; install: string | null } | undefined;

test("B2a C4: the persisted binding version is null before the first response, and rows admitted before it carry a null stamp", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const before = event();
    buffer.append(before);
    const binding = buffer.workspaceBinding() as Binding;
    assert.ok(binding && "actorBindingVersion" in binding, "workspaceBinding() reports actorBindingVersion");
    assert.equal(binding.actorBindingVersion, null);
    assert.equal(binding.actorBindingInstall, null);
    const stamp = stampOf(buffer, before.id);
    assert.ok(stamp, "an identity row exists for the admitted row");
    assert.deepEqual(stamp, { version: null, install: null });
  } finally { close(); }
});

test("B2a C1: after a response supplies a version the collector persists the (install, version) pair and stamps every later row with it; a lower version from the same install never lowers it", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    (buffer as unknown as Stamped).recordActorBindingVersion(1, INSTALL_X);
    assert.deepEqual([buffer.workspaceBinding()?.actorBindingVersion, (buffer.workspaceBinding() as Binding)?.actorBindingInstall], [1, INSTALL_X]);
    const after = event();
    buffer.append(after);
    assert.deepEqual(stampOf(buffer, after.id), { version: 1, install: INSTALL_X });
    (buffer as unknown as Stamped).recordActorBindingVersion(0, INSTALL_X); // a stale response of the SAME install never lowers the persisted version
    assert.equal((buffer.workspaceBinding() as Binding)?.actorBindingVersion, 1);
  } finally { close(); }
});

test("B2a C1: every upload batch echoes actorBindingVersionHeard (>= every stamp it carries for the current install) and carries the pair on the wire; the shared schema accepts it", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "lean-contract", deviceId: "lean-device", lean: { write: true } });
  try {
    (buffer as unknown as Stamped).recordActorBindingVersion(2, INSTALL_X);
    buffer.append(event());
    const config = collectorConfigSchema.parse({ tenantId: "lean-contract", installKey: "lean-contract-install", deviceId: "lean-device" });
    const { batch } = buildIngestBatch(config, buffer);
    assert.ok(batch, "a batch was built");
    assert.equal((batch as unknown as { actorBindingVersionHeard?: number | null }).actorBindingVersionHeard, 2);
    const metadata = batch.events[0].event.metadata as { actorBindingVersion?: number; actorBindingInstall?: string };
    assert.equal(metadata.actorBindingVersion, 2, "the version travels on the wire");
    assert.equal(metadata.actorBindingInstall, INSTALL_X, "the install the version was issued to travels with it");
    assert.equal(aiWorkIngestBatchSchema.safeParse({ ...batch, actorBindingVersionHeard: 2 }).success, true);
  } finally { close(); }
});

test("B2a C1: metadata.actorBindingVersion and metadata.actorBindingInstall are allowlisted identifier keys that survive the outbound seal", pending("B2a"), () => {
  for (const key of ["actorBindingVersion", "actorBindingInstall"]) {
    assert.ok(metadataKeyDisposition(key), `${key}: disposition exists`);
    assert.equal(metadataKeyDisposition(key)?.valueKind, metadataKeyDisposition("workItemId")?.valueKind, key);
  }
  const sealed = sealOutboundEnvelope({ event: event({ metadata: { actorBindingVersion: 3, actorBindingInstall: INSTALL_X } }), suppressedFields: [] });
  assert.ok(sealed.ok, "sealed");
  assert.deepEqual([(sealed.envelope.event.metadata as { actorBindingVersion?: number }).actorBindingVersion, (sealed.envelope.event.metadata as { actorBindingInstall?: string }).actorBindingInstall], [3, INSTALL_X]);
});

test("B2a C4 (round 2): the persisted pair is scoped to the install: a re-join (new installation epoch) clears it, rows before the new install's first versioned response are null-stamped, the new install's version 0 is accepted, and a workspace transition clears it too", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped;
    stamped.recordActorBindingVersion(2, INSTALL_X);
    // the Mac re-joins: join.ts activates the grant with a NEW installation epoch on the same workspace (useWorkspace(tenant, device, epoch))
    buffer.useWorkspace("tenant-lean-contract", undefined, randomUUID());
    assert.deepEqual([buffer.workspaceBinding()?.actorBindingVersion, (buffer.workspaceBinding() as Binding)?.actorBindingInstall], [null, null], "the re-join clears the pair");
    const beforeFirstResponse = event();
    buffer.append(beforeFirstResponse);
    assert.deepEqual(stampOf(buffer, beforeFirstResponse.id), { version: null, install: null }, "null until the new install's first versioned response (C4)");
    stamped.recordActorBindingVersion(0, INSTALL_Z); // the join or first response supplies the NEW install's version 0: accepted, not refused as lower
    assert.deepEqual([buffer.workspaceBinding()?.actorBindingVersion, (buffer.workspaceBinding() as Binding)?.actorBindingInstall], [0, INSTALL_Z]);
    const underZ = event();
    buffer.append(underZ);
    assert.deepEqual(stampOf(buffer, underZ.id), { version: 0, install: INSTALL_Z });
    // a workspace transition (issue 0089 flow) clears the pair as well
    buffer.transitionWorkspace("tenant-lean-contract", "tenant-lean-contract-2");
    assert.deepEqual([buffer.workspaceBinding()?.actorBindingVersion, (buffer.workspaceBinding() as Binding)?.actorBindingInstall], [null, null]);
  } finally { close(); }
});

test("B2a C1 (round 2): a response from a different install replaces the pair (no cross-install comparison), and rows stamped before a re-join keep their old pair on the wire while the batch echoes the current install's version", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "lean-contract", deviceId: "lean-device", lean: { write: true } });
  try {
    const stamped = buffer as unknown as Stamped;
    stamped.recordActorBindingVersion(2, INSTALL_X);
    const preJoin = event();
    buffer.append(preJoin);                                    // captured under X's v2, still in the outbox at the re-join
    stamped.recordActorBindingVersion(0, INSTALL_Z);          // the new install's first response: a different install id starts a new scope
    assert.deepEqual([buffer.workspaceBinding()?.actorBindingVersion, (buffer.workspaceBinding() as Binding)?.actorBindingInstall], [0, INSTALL_Z]);
    const postJoin = event();
    buffer.append(postJoin);
    assert.deepEqual(stampOf(buffer, preJoin.id), { version: 2, install: INSTALL_X }, "the pre-join row keeps its pair");
    assert.deepEqual(stampOf(buffer, postJoin.id), { version: 0, install: INSTALL_Z });
    const config = collectorConfigSchema.parse({ tenantId: "lean-contract", installKey: "lean-contract-install", deviceId: "lean-device" });
    const { batch } = buildIngestBatch(config, buffer);
    assert.ok(batch, "a batch was built");
    assert.equal((batch as unknown as { actorBindingVersionHeard?: number | null }).actorBindingVersionHeard, 0, "the echo is the CURRENT install's version; an earlier install's stamps do not raise it");
    const byId = new Map(batch.events.map((row) => [row.event.id, row.event.metadata as { actorBindingVersion?: number; actorBindingInstall?: string }]));
    assert.deepEqual([byId.get(preJoin.id)?.actorBindingVersion, byId.get(preJoin.id)?.actorBindingInstall], [2, INSTALL_X], "the old pair travels unchanged");
    assert.deepEqual([byId.get(postJoin.id)?.actorBindingVersion, byId.get(postJoin.id)?.actorBindingInstall], [0, INSTALL_Z]);
  } finally { close(); }
});
