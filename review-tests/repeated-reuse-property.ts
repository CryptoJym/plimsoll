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

async function listen(server: http.Server) {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  return address.port;
}
async function close(server: http.Server) {
  await new Promise<void>((resolve, reject) => server.close((error) =>
    error ? reject(error) : resolve()));
}

async function main() {
  const received: string[] = [];
  const server = http.createServer(async (request, response) => {
    try {
      let body = "";
      for await (const chunk of request) body += String(chunk);
      const payload = JSON.parse(body) as { events: Array<{ event: { id: string } }> };
      received.push(...payload.events.map((item) => item.event.id));
      const expected = deliveryExpectation(body, "property-install");
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ accepted: expected.itemIds.length,
        ack: deliveryAcknowledgement(expected, expected.itemIds) }));
    } catch (error) {
      response.writeHead(500, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: String(error) }));
    }
  });
  const port = await listen(server);
  try {
    for (const count of [2, 3, 4, 5]) {
      for (const violationAt of new Set([0, Math.floor(count / 2), count - 1])) {
      for (const privacySource of ["old", "new"] as const) {
        const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-reuse-property-"));
        const ledger = path.join(root, "ledger.sqlite");
        const id = `00000000-0000-4000-8000-${String(4200 + count * 20 + violationAt * 2 +
          (privacySource === "new" ? 1 : 0)).padStart(12, "0")}`;
        const realNow = new Date();
        const oldAt = new Date(realNow.getTime() - 45 * 86_400_000).toISOString();
        const pruneAt = new Date(realNow.getTime() + 60 * 86_400_000);
        const uploadAt = new Date(pruneAt.getTime() + 3_600_000);
        const workspaceId = `property-${count}-${violationAt}-${privacySource}`;
        const deviceId = "property-device";
        const options = { workspaceId, deviceId,
          enrollmentNow: () => new Date(realNow.getTime() - 60 * 86_400_000) };
        const event = aiInteractionEventSchema.parse({ id, sessionId: id,
          source: "codex", eventType: "assistant_response", dataMode: "metadata",
          observedAt: oldAt, actionClass: "other", inputTokens: 1, outputTokens: 1 });
        const expected = new Map<string, string>();
        const privateGenerations: string[] = [];
        received.length = 0;
        try {
          for (let incarnation = 0; incarnation < count; incarnation++) {
            const violating = incarnation === violationAt;
            const BufferClass = violating && privacySource === "old"
              ? OldBuffer : LocalEventBuffer;
            const buffer = new BufferClass(ledger, { ...options,
              delivery: { enabled: violating } });
            try {
              const supplied = violating ? aiInteractionEventSchema.parse({ ...event,
                metadata: { serviceName: "sk_live_private_12345678901234567890" } }) : event;
              assert.equal(buffer.append(supplied), true);
              if (!violating) buffer.database.prepare(
                "update buffered_events set created_at=? where id=?").run(oldAt, id);
              const raw = buffer.database.prepare(`select rowid as rowid,
                created_at as createdAt,privacy_generation as generation,
                privacy_disposition as disposition from buffered_events where id=?`)
                .get(id) as { rowid: number; createdAt: string;
                  generation: string; disposition: string | null };
              assert.ok(raw.generation);
              if (violating) {
                assert.equal(raw.disposition, "local_privacy_violation");
                privateGenerations.push(raw.generation);
              } else {
                buffer.delivery.configure({ enabled: true });
                buffer.delivery.repairRawById(id);
                const copy = buffer.database.prepare(`select delivery_id as id
                  from upload_outbox where raw_id=? and raw_created_at=?
                    and raw_generation=?`).get(id, raw.createdAt, raw.generation) as
                    { id: string } | undefined;
                assert.ok(copy, `incarnation ${incarnation} has its own queued copy`);
                assert.ok(!expected.has(copy.id), "delivery ID belongs to one incarnation");
                expected.set(copy.id, raw.generation);
              }
            } finally { buffer.close(); }
            const old = new OldBuffer(ledger, { ...options,
              delivery: { enabled: !violating } });
            try {
              for (let pass = 0; pass < 3 && old.database.prepare(
                "select 1 from buffered_events where id=?").get(id); pass++) {
                const pruned = old.prune(30, { maxRows: 100, now: pruneAt });
                console.log(JSON.stringify({ phase: "old_prune", count, violationAt,
                  privacySource,
                  incarnation, pass, pruned,
                  raw: old.database.prepare(`select rowid as rowid,created_at as createdAt,
                    privacy_disposition as disposition from buffered_events where id=?`).get(id) }));
              }
              assert.equal(old.database.prepare(
                "select 1 from buffered_events where id=?").get(id), undefined,
                `exact 0.7.44 pruned incarnation ${incarnation}`);
            } finally { old.close(); }
          }
          const config = collectorConfigSchema.parse({ tenantId: workspaceId,
            deviceId, installKey: "property-install", retentionDays: 30,
            uploadUrl: `http://127.0.0.1:${port}/ingest`,
            delivery: { maxOldestAgeDays: 3650 } });
          const upgraded = new LocalEventBuffer(ledger, { ...options,
            delivery: { enabled: true } });
          try {
            assert.equal(upgraded.delivery.status(uploadAt).remainingDelivery,
              expected.size, "every allowed incarnation remains queued");
            const uploaded = await uploadBufferedEvents(config, upgraded,
              { now: () => uploadAt });
            assert.equal(uploaded.uploadedEvents, expected.size);
            assert.deepEqual(received.sort(), [...expected.keys()].sort(),
              "the loopback server saw every allowed copy exactly once");
            for (const [deliveryId, generation] of expected) {
              assert.deepEqual(upgraded.database.prepare(`select terminal_state as state,
                reason,raw_generation as generation from upload_receipts
                where delivery_id=?`).get(deliveryId),
              { state: "acknowledged", reason: "remote_acknowledged", generation });
            }
            for (const generation of privateGenerations) {
              assert.ok(upgraded.database.prepare(`select 1 from upload_receipts
                where raw_id=? and raw_generation=? and terminal_state='dead'
                  and reason='local_privacy_violation'`).get(id, generation),
              `private incarnation ${generation} retains its terminal decision`);
            }
            assert.equal(upgraded.database.prepare(
              "select count(*) as n from upload_outbox").get()?.n, 0);
            assert.equal(upgraded.delivery.status(uploadAt).remainingDelivery, 0);
            assert.equal(upgraded.retentionStatus(30, uploadAt).states.heldForUpload, 0);
            console.log(JSON.stringify({ count, violationAt, privacySource,
              uploaded: received.length,
              blocked: privateGenerations.length, held: 0, remaining: 0 }));
          } finally { upgraded.close(); }
        } finally { fs.rmSync(root, { recursive: true, force: true }); }
      }
      }
    }
  } finally { await close(server); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
