import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-missing-raw-delivery-"));
  const ledger = path.join(root, "ledger.sqlite");
  const id = "00000000-0000-4000-8000-000000004191";
  const workspaceId = "missing-raw-proof";
  const deviceId = "missing-raw-device";
  const now = new Date();
  const received: string[] = [];
  let server: http.Server | undefined;
  try {
    const buffer = new LocalEventBuffer(ledger, { workspaceId, deviceId,
      delivery: { enabled: false },
      enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000) });
    try {
      const event = aiInteractionEventSchema.parse({ id, sessionId: id,
        source: "codex", eventType: "assistant_response", dataMode: "metadata",
        observedAt: now.toISOString(), actionClass: "other",
        inputTokens: 1, outputTokens: 1 });
      assert.equal(buffer.append(event), true);
      buffer.delivery.configure({ enabled: true });
      assert.equal(buffer.delivery.repairRawById(id).enqueued, 1);
      // Exercise the unrecorded-loss branch itself. Product retention always
      // records an expiry; an external delete supplies no privacy decision.
      assert.equal(buffer.database.prepare("delete from buffered_events where id=?")
        .run(id).changes, 1);
      assert.equal((buffer.database.prepare("select count(*) as n from raw_retention_receipts")
        .get() as { n: number }).n, 0);
      server = http.createServer(async (request, response) => {
        try {
          let body = "";
          for await (const chunk of request) body += String(chunk);
          const payload = JSON.parse(body) as { events: Array<{ event: { id: string } }> };
          received.push(...payload.events.map((row) => row.event.id));
          const expected = deliveryExpectation(body, "proof-install");
          response.writeHead(200, { "content-type": "application/json" });
          response.end(JSON.stringify({ accepted: expected.itemIds.length,
            ack: deliveryAcknowledgement(expected, expected.itemIds) }));
        } catch (error) {
          response.writeHead(500, { "content-type": "application/json" });
          response.end(JSON.stringify({ error: String(error) }));
        }
      });
      await new Promise<void>((resolve, reject) => {
        server!.once("error", reject);
        server!.listen(0, "127.0.0.1", resolve);
      });
      const address = server.address();
      assert.ok(address && typeof address !== "string");
      const config = collectorConfigSchema.parse({ tenantId: workspaceId, deviceId,
        installKey: "proof-install", retentionDays: 30,
        uploadUrl: `http://127.0.0.1:${address.port}/ingest`,
        delivery: { maxOldestAgeDays: 3650 } });
      const uploaded = await uploadBufferedEvents(config, buffer, {
        now: () => new Date(now.getTime() + 3_600_000),
      });
      console.log(JSON.stringify({ phase: "missing_raw_result", uploaded, received,
        receipt: buffer.database.prepare(`select terminal_state,reason from upload_receipts
          where delivery_id=?`).get(id),
        remaining: buffer.delivery.status(now).remainingDelivery }));
      assert.equal(uploaded.uploadedEvents, 1,
        "a missing raw without an exact privacy decision must not be dead-lettered");
      assert.deepEqual(received, [id]);
      assert.deepEqual(buffer.database.prepare(`select terminal_state as state,
        reason from upload_receipts where delivery_id=?`).get(id),
        { state: "acknowledged", reason: "remote_acknowledged" });
      assert.equal(buffer.delivery.status(now).remainingDelivery, 0);
      console.log(JSON.stringify({ phase: "missing_raw_without_privacy", uploaded: 1,
        acknowledged: id, remaining: 0 }));
    } finally { buffer.close(); }
  } finally {
    if (server?.listening) await new Promise<void>((resolve) => server!.close(() => resolve()));
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
