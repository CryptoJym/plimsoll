import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

async function main() {
  const old39Root = process.env.PR417_0739_WORKTREE;
  assert.ok(old39Root, "exact 0.7.39 checkout required");
  const Old39Buffer = require(path.join(old39Root,
    "packages/collector-cli/src/buffer.ts")).LocalEventBuffer as typeof LocalEventBuffer;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-0739-delivery-"));
  const ledger = path.join(root, "ledger.sqlite");
  const now = new Date();
  const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
  const id = "00000000-0000-4000-8000-000000007390";
  const workspaceId = "legacy-0739-delivery";
  const deviceId = "legacy-0739-device";
  const options = { workspaceId, deviceId,
    delivery: { enabled: false },
    enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000) };
  const received: string[] = [];
  let ingestServer: http.Server | undefined;
  let statusServer: http.Server | undefined;
  const listen = async (server: http.Server) => {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    return address.port;
  };
  const close = async (server?: http.Server) => {
    if (server?.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
  };
  try {
    const old = new Old39Buffer(ledger, options);
    try {
      const event = aiInteractionEventSchema.parse({ id, sessionId: id,
        source: "codex", eventType: "assistant_response", dataMode: "metadata",
        observedAt: oldAt, actionClass: "other", inputTokens: 1, outputTokens: 1 });
      assert.equal(old.append(event), true);
      old.database.prepare("update buffered_events set created_at=? where id=?")
        .run(oldAt, id);
      old.delivery.configure({ enabled: true });
      assert.equal(old.delivery.repairRawById(id).enqueued, 1);
      assert.equal(old.prune(30, { maxRows: 10, now }).events, 1);
      assert.equal(old.database.prepare("select 1 from buffered_events where id=?").get(id), undefined);
      assert.ok(old.database.prepare("select 1 from raw_retention_receipts where event_id=?")
        .get(id));
      console.log(JSON.stringify({ phase: "exact_0739", removedRaw: id,
        queuedCopies: 1, expiryReceipts: 1 }));
    } finally { old.close(); }

    ingestServer = http.createServer(async (request, response) => {
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
    const ingestPort = await listen(ingestServer);
    const config = collectorConfigSchema.parse({ tenantId: workspaceId, deviceId,
      installKey: "proof-install", retentionDays: 30,
      uploadUrl: `http://127.0.0.1:${ingestPort}/ingest`,
      delivery: { maxOldestAgeDays: 3650 } });
    const upgraded = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: true } });
    try {
      assert.equal(upgraded.delivery.status(now).remainingDelivery, 1);
      assert.equal(upgraded.retentionStatus(30, now).states.heldForUpload, 0);
      const uploaded = await uploadBufferedEvents(config, upgraded, {
        now: () => new Date(now.getTime() + 3_600_000),
      });
      assert.equal(uploaded.uploadedEvents, 1);
      assert.deepEqual(received, [id]);
      assert.deepEqual(upgraded.database.prepare(`select terminal_state as state,
        reason from upload_receipts where delivery_id=?`).get(id),
        { state: "acknowledged", reason: "remote_acknowledged" });
      assert.equal((upgraded.database.prepare("select count(*) as n from upload_outbox")
        .get() as { n: number }).n, 0);
      statusServer = createCollectorServer(config, upgraded);
      const statusPort = await listen(statusServer);
      const response = await fetch(`http://127.0.0.1:${statusPort}/status`);
      assert.equal(response.status, 200);
      const status = await response.json() as {
        retention?: { states?: { heldForUpload?: number } };
        delivery?: { remainingDelivery?: number };
      };
      assert.equal(status.retention?.states?.heldForUpload, 0);
      assert.equal(status.delivery?.remainingDelivery, 0);
      console.log(JSON.stringify({ phase: "direct_0739_upgrade", uploaded: 1,
        acknowledged: id, outbox: 0,
        held: status.retention?.states?.heldForUpload,
        remaining: status.delivery?.remainingDelivery }));
    } finally { await close(statusServer); upgraded.close(); }
  } finally {
    await close(ingestServer);
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
