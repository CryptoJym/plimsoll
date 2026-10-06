import { nativeCodexFixture } from "../scripts/lib/native-codex-fixture";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-lease-lineage-aba-"));
const at = new Date("2026-09-29T12:00:00.000Z");
const event = (id: string) => aiInteractionEventSchema.parse({
  id, sessionId: id, source: "codex", model: "gpt-6-sol", eventType: "assistant_response",
  dataMode: "metadata", observedAt: at.toISOString(), actionClass: "other",
  inputTokens: 1, outputTokens: 1, metadata: { ...nativeCodexFixture(id).metadata, proof: "lineage-aba" },
});

try {
  for (const field of ["rawId", "rawCreatedAt", "rawGeneration"] as const) {
    const file = path.join(root, `${field}.sqlite`);
    const buffer = new LocalEventBuffer(file, { workspaceId: "00000000-0000-4000-8000-000000000001", deviceId: "aba-fixture-device", delivery: { enabled: true } });
    try {
      const id = "11711711-1111-4111-8111-111111111140";
      assert.equal(buffer.append(event(id)), true);
      const db = buffer.database;
      const original = db.prepare(`select rowid as rowid,id,created_at as createdAt,
        privacy_generation as generation from buffered_events where id=?`).get(id) as {
        rowid: number; id: string; createdAt: string; generation: string;
      };
      const lease = buffer.delivery.lease({ now: new Date(Date.now() + 2_000) });
      assert.equal(lease.items.length, 1);
      const cached = lease.items[0]!;
      assert.equal(cached.rawRowid, original.rowid);

      const next = {
        id: field === "rawId" ? "11711711-1111-4111-8111-111111111141" : id,
        createdAt: field === "rawCreatedAt"
          ? new Date(Date.parse(original.createdAt) + 1_000).toISOString()
          : original.createdAt,
        generation: field === "rawGeneration"
          ? "new-incarnation-generation" : original.generation,
      };
      db.prepare("delete from upload_outbox where delivery_id=?").run(cached.deliveryId);
      db.prepare("delete from buffered_events where rowid=?").run(original.rowid);
      db.prepare(`insert into buffered_events
        (rowid,id,source,event_type,data_mode,observed_at,payload_json,
         suppressed_fields_json,created_at,privacy_generation)
        values (?,?,'codex','assistant_response','metadata',?,?,'[]',?,?)`).run(
        original.rowid, next.id, at.toISOString(), JSON.stringify(event(next.id)),
        next.createdAt, next.generation);
      db.prepare(`insert into upload_outbox
        (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,
         base_envelope_json,base_bytes,state,attempt_count,next_attempt_at,
         lease_id,lease_expires_at,created_at,updated_at)
        values (?,?,?,?,?,?,?,'in_flight',1,?,?,?,?,?)`).run(
        cached.deliveryId, original.rowid, next.id, next.createdAt,
        next.generation, cached.envelopeJson, Buffer.byteLength(cached.envelopeJson),
        at.toISOString(), lease.leaseId, new Date(Date.now() + 60_000).toISOString(),
        next.createdAt, next.createdAt);
      const revalidated = buffer.delivery.revalidateLeaseItems(lease.leaseId,
        [cached], new Date(Date.now() + 3_000));
      assert.deepEqual(revalidated, { items: [], locallyDead: 0 }, field);
      const newRaw = db.prepare(`select privacy_disposition as disposition
        from buffered_events where rowid=?`).get(original.rowid) as {
        disposition: string | null;
      };
      assert.equal(newRaw.disposition, null, field);
      assert.equal((db.prepare(`select state from upload_outbox where delivery_id=?`)
        .get(cached.deliveryId) as { state: string }).state, "in_flight", field);
      console.log(JSON.stringify({ field, staleLeaseWithheld: true,
        newIncarnationPrivacyDisposition: newRaw.disposition }));
    } finally {
      buffer.close();
    }
  }
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
