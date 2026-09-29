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

const baseRoot = process.env.PR417_BASE_WORKTREE;
assert.ok(baseRoot, "set PR417_BASE_WORKTREE to exact 0.7.44");
const OldBuffer = require(path.join(baseRoot,
  "packages/collector-cli/src/buffer.ts")).LocalEventBuffer as typeof LocalEventBuffer;

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-repeated-reuse-"));
  const ledger = path.join(root, "ledger.sqlite");
  const id = "00000000-0000-4000-8000-000000004198";
  const now = new Date();
  const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
  const options = { workspaceId: "repeated-reuse", deviceId: "device",
    delivery: { enabled: false }, enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000) };
  const event = aiInteractionEventSchema.parse({ id, sessionId: id, source: "codex",
    eventType: "assistant_response", dataMode: "metadata", observedAt: oldAt,
    actionClass: "other", inputTokens: 1, outputTokens: 1 });
  const queued = new Map<string, string>();
  const repairOutcomes: number[] = [];
  let server: http.Server | undefined;
  try {
    for (let incarnation = 0; incarnation < 3; incarnation++) {
      const BufferClass = incarnation === 0 ? OldBuffer : LocalEventBuffer;
      const upgraded = new BufferClass(ledger, options);
      try {
        assert.equal(upgraded.append(event), true);
        upgraded.database.prepare("update buffered_events set created_at=? where id=?")
          .run(oldAt, id);
        if (incarnation > 0) {
          upgraded.delivery.configure({ enabled: true });
          const repaired = upgraded.delivery.repairRawById(id).enqueued;
          repairOutcomes.push(repaired);
          const copy = upgraded.database.prepare(`select delivery_id as id,
            raw_generation as generation from upload_outbox where raw_id=?
              and raw_generation=(select privacy_generation from buffered_events where id=?)`)
            .get(id, id) as { id: string; generation: string } | undefined;
          if (copy) queued.set(copy.id, copy.generation);
          console.log(JSON.stringify({ phase: "new_queue", incarnation,
            repaired, matchingCopy: copy?.id ?? null }));
        }
      } finally { upgraded.close(); }
      const old = new OldBuffer(ledger, { ...options,
        delivery: { enabled: incarnation > 0 } });
      try {
        assert.equal(old.prune(30, { now }).events, 1);
        const receipts = old.database.prepare(`select raw_generation as generation
          from raw_retention_receipts where event_id=?`).all(id) as Array<{ generation: string }>;
        assert.equal(receipts.length, incarnation + 1,
          "the exact old pruner records each expired incarnation");
        assert.equal(new Set(receipts.map((row) => row.generation)).size, incarnation + 1);
        assert.equal(old.database.prepare("select 1 from buffered_events where id=?").get(id), undefined);
        console.log(JSON.stringify({ phase: "old_prune", incarnation,
          expiries: receipts.length, queued: queued.size }));
      } finally { old.close(); }
    }
    assert.deepEqual(repairOutcomes, [1, 1],
      "each reincarnation gains its own durable queued copy");
    assert.equal(queued.size, 2);
    const received: string[] = [];
    server = http.createServer(async (request, response) => {
      try {
        let body = "";
        for await (const chunk of request) body += String(chunk);
        const payload = JSON.parse(body) as { events: Array<{ event: { id: string } }> };
        received.push(...payload.events.map((item) => item.event.id));
        const expected = deliveryExpectation(body, "reuse-install");
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
    const config = collectorConfigSchema.parse({ tenantId: options.workspaceId,
      deviceId: options.deviceId, installKey: "reuse-install", retentionDays: 30,
      uploadUrl: `http://127.0.0.1:${address.port}/ingest`,
      delivery: { maxOldestAgeDays: 3650 } });
    const final = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: true } });
    try {
      const uploadAt = new Date(now.getTime() + 3_600_000);
      assert.equal(final.delivery.status(uploadAt).remainingDelivery, 2);
      const uploaded = await uploadBufferedEvents(config, final, { now: () => uploadAt });
      assert.equal(uploaded.uploadedEvents, 2);
      assert.deepEqual(received.sort(), [...queued.keys()].sort());
      for (const [deliveryId, generation] of queued) {
        assert.deepEqual(final.database.prepare(`select terminal_state as state,
          reason,raw_generation as generation from upload_receipts where delivery_id=?`)
          .get(deliveryId),
        { state: "acknowledged", reason: "remote_acknowledged", generation });
      }
      assert.equal(final.database.prepare("select count(*) as n from upload_outbox").get()?.n, 0);
      assert.equal(final.delivery.status(uploadAt).remainingDelivery, 0);
      assert.equal(final.retentionStatus(30, uploadAt).states.heldForUpload, 0);
      console.log(JSON.stringify({ phase: "reupgrade", uploaded: received.length,
        acknowledged: queued.size, held: 0, remaining: 0, repairOutcomes }));
    } finally { final.close(); }
  } finally {
    if (server) await new Promise<void>((resolve) => server!.close(() => resolve()));
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
