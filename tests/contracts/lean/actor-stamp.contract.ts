/**
 * B2a (collector): the binding-version stamp on the identity row and on the wire, the echo on every request, and the null
 * stamp before the first response (docs/lean/CONTRACTS.md C1, C4; ARCHITECTURE.md §5.1-5.2). Pending until B2a lands.
 */
import assert from "node:assert/strict";
import test from "node:test";

import { collectorConfigSchema } from "../../../packages/collector-cli/src/config";
import { sealOutboundEnvelope } from "../../../packages/collector-cli/src/outbound-envelope";
import { buildIngestBatch } from "../../../packages/collector-cli/src/upload";
import { aiWorkIngestBatchSchema, metadataKeyDisposition } from "../../../packages/shared/src/index";
import { event, openTempBuffer, pending } from "./_pending";

type Stamped = { recordActorBindingVersion(version: number): void; workspaceBinding(): { actorBindingVersion?: number | null } | null };

test("B2a C4: the persisted binding version is null before the first response, and rows admitted before it carry a null stamp", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const before = event();
    buffer.append(before);
    const binding = buffer.workspaceBinding() as { actorBindingVersion?: number | null } | null;
    assert.ok(binding && "actorBindingVersion" in binding, "workspaceBinding() reports actorBindingVersion");
    assert.equal(binding.actorBindingVersion, null);
    const stamp = buffer.database.prepare("select actor_binding_version as v from summary_members where event_id = ?").get(before.id) as { v: number | null } | undefined;
    assert.ok(stamp, "an identity row exists for the admitted row");
    assert.equal(stamp.v, null);
  } finally { close(); }
});

test("B2a C1: after a response supplies a version the collector persists it and stamps every later row with it", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    (buffer as unknown as Stamped).recordActorBindingVersion(1);
    assert.equal((buffer.workspaceBinding() as { actorBindingVersion?: number | null }).actorBindingVersion, 1);
    const after = event();
    buffer.append(after);
    const stamp = buffer.database.prepare("select actor_binding_version as v from summary_members where event_id = ?").get(after.id) as { v: number | null };
    assert.equal(stamp.v, 1);
    (buffer as unknown as Stamped).recordActorBindingVersion(0); // a lower version from a stale response never lowers the persisted one
    assert.equal((buffer.workspaceBinding() as { actorBindingVersion?: number | null }).actorBindingVersion, 1);
  } finally { close(); }
});

test("B2a C1: every upload batch echoes actorBindingVersionHeard (>= every stamp it carries) and the shared schema accepts it", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "lean-contract", deviceId: "lean-device", lean: { write: true } });
  try {
    (buffer as unknown as Stamped).recordActorBindingVersion(2);
    buffer.append(event());
    const config = collectorConfigSchema.parse({ tenantId: "lean-contract", installKey: "lean-contract-install", deviceId: "lean-device" });
    const { batch } = buildIngestBatch(config, buffer);
    assert.ok(batch, "a batch was built");
    assert.equal((batch as unknown as { actorBindingVersionHeard?: number | null }).actorBindingVersionHeard, 2);
    assert.equal((batch.events[0].event.metadata as { actorBindingVersion?: number }).actorBindingVersion, 2, "the stamp travels on the wire");
    assert.equal(aiWorkIngestBatchSchema.safeParse({ ...batch, actorBindingVersionHeard: 2 }).success, true);
  } finally { close(); }
});

test("B2a C1: metadata.actorBindingVersion is an allowlisted identifier key that survives the outbound seal", pending("B2a"), () => {
  assert.ok(metadataKeyDisposition("actorBindingVersion"), "disposition exists");
  assert.equal(metadataKeyDisposition("actorBindingVersion")?.valueKind, metadataKeyDisposition("workItemId")?.valueKind);
  const sealed = sealOutboundEnvelope({ event: event({ metadata: { actorBindingVersion: 3 } }), suppressedFields: [] });
  assert.ok(sealed.ok, "sealed");
  assert.equal((sealed.envelope.event.metadata as { actorBindingVersion?: number }).actorBindingVersion, 3);
});
