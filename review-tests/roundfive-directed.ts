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

const base = process.env.PR417_BASE_WORKTREE;
assert.ok(base, "exact 0.7.44 worktree required");
const OldBuffer = require(path.join(base,
  "packages/collector-cli/src/buffer.ts")).LocalEventBuffer as typeof LocalEventBuffer;
const DAY = 86_400_000;
const now = new Date();
const oldAt = new Date(now.getTime() - 45 * DAY).toISOString();
const pruneAt = new Date(now.getTime() + 60 * DAY);
const uploadAt = new Date(pruneAt.getTime() + 3_600_000);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-r5-directed-"));

function event(id: string, privacy = false) {
  return aiInteractionEventSchema.parse({
    id, sessionId: id, source: "codex", eventType: "assistant_response",
    dataMode: "metadata", observedAt: oldAt, actionClass: "other",
    inputTokens: 1, outputTokens: 1,
    ...(privacy ? { metadata: { serviceName: "sk_live_private_12345678901234567890" } } : {}),
  });
}

async function reuseCase(privacyAt: number, port: number, received: string[]) {
  const id = `00000000-0000-4000-8000-${String(5700 + privacyAt).padStart(12, "0")}`;
  const ledger = path.join(root, `reuse-${privacyAt}.sqlite`);
  const workspaceId = `r5-reuse-${privacyAt}`;
  const options = { workspaceId, deviceId: "r5-device",
    enrollmentNow: () => new Date(now.getTime() - 60 * DAY) };
  const expected = new Map<string, string>();
  let privateGeneration = "";
  received.length = 0;
  for (let i = 0; i < 6; i++) {
    const privacy = i === privacyAt;
    const BufferClass = privacy ? OldBuffer : LocalEventBuffer;
    const buffer = new BufferClass(ledger, { ...options, delivery: { enabled: privacy } });
    try {
      assert.equal(buffer.append(event(id, privacy)), true);
      if (!privacy) buffer.database.prepare("update buffered_events set created_at=? where id=?")
        .run(oldAt, id);
      const raw = buffer.database.prepare(`select created_at as at,
        privacy_generation as generation, privacy_disposition as disposition
        from buffered_events where id=?`).get(id) as {
          at: string; generation: string; disposition: string | null;
        };
      if (privacy) {
        assert.equal(raw.disposition, "local_privacy_violation");
        privateGeneration = raw.generation;
      } else {
        buffer.delivery.configure({ enabled: true });
        buffer.delivery.repairRawById(id);
        const copy = buffer.database.prepare(`select delivery_id as id
          from upload_outbox where raw_id=? and raw_created_at=?
            and raw_generation=?`).get(id, raw.at, raw.generation) as { id: string } | undefined;
        assert.ok(copy, `valid incarnation ${i} must have a copy`);
        assert.ok(!expected.has(copy.id), "delivery ID must be unique by incarnation");
        expected.set(copy.id, raw.generation);
      }
    } finally { buffer.close(); }
    const old = new OldBuffer(ledger, { ...options, delivery: { enabled: !privacy } });
    try {
      for (let turn = 0; turn < 3 && old.database.prepare(
        "select 1 from buffered_events where id=?").get(id); turn++) {
        old.prune(30, { maxRows: 100, now: pruneAt });
      }
      assert.equal(old.database.prepare("select 1 from buffered_events where id=?").get(id),
        undefined, `exact 0.7.44 pruned incarnation ${i}`);
    } finally { old.close(); }
  }
  assert.equal(expected.size, 5);
  const final = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: true } });
  try {
    const config = collectorConfigSchema.parse({ tenantId: workspaceId,
      deviceId: options.deviceId, installKey: "r5-install", retentionDays: 30,
      uploadUrl: `http://127.0.0.1:${port}/ingest`,
      delivery: { maxOldestAgeDays: 3650 } });
    const result = await uploadBufferedEvents(config, final, { now: () => uploadAt });
    assert.equal(result.uploadedEvents, 5);
    assert.deepEqual(received.sort(), [...expected.keys()].sort());
    for (const [deliveryId, generation] of expected) {
      assert.deepEqual(final.database.prepare(`select terminal_state as state,
        reason,raw_generation as generation from upload_receipts where delivery_id=?`)
        .get(deliveryId),
      { state: "acknowledged", reason: "remote_acknowledged", generation });
    }
    assert.ok(final.database.prepare(`select 1 from upload_receipts where raw_id=?
      and raw_generation=? and reason='local_privacy_violation'`).get(id, privateGeneration));
    assert.equal(final.database.prepare("select count(*) as n from upload_outbox").get()?.n, 0);
    assert.equal(final.retentionStatus(30, uploadAt).states.heldForUpload, 0);
    console.log(JSON.stringify({ case: "six_reuses", privacyAt, uploaded: 5,
      blocked: 1, acknowledged: 5, remaining: 0, held: 0 }));
  } finally { final.close(); }
}

function ambiguousLegacyReceipt() {
  const id = "00000000-0000-4000-8000-000000005710";
  const ledger = path.join(root, "legacy-ambiguous.sqlite");
  const buffer = new LocalEventBuffer(ledger, { delivery: { enabled: true } });
  try {
    assert.equal(buffer.append(event(id)), true);
    const db = buffer.database;
    const before = db.prepare(`select raw_id as id,raw_created_at as at,
      raw_generation as generation from upload_outbox where delivery_id=?`).get(id) as {
        id: string; at: string; generation: string;
      };
    assert.ok(before);
    db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead','local_privacy_violation','local',0,?,?)`).run(
      id, "2020-01-01T00:00:00.000Z", now.toISOString());
    const lease = buffer.delivery.lease({ now: uploadAt });
    assert.equal(lease.items.length, 0);
    assert.deepEqual(db.prepare(`select raw_id as id,raw_created_at as at,
      raw_generation as generation from upload_outbox where delivery_id=?`).get(id), before,
    "unbound receipt must not delete an unrelated queued copy");
    assert.equal(db.prepare(`select privacy_disposition as disposition
      from buffered_events where id=?`).get(id)?.disposition, null);
    console.log(JSON.stringify({ case: "unbound_legacy_receipt",
      preservedCopy: true, preservedRaw: true, deliverable: false }));
  } finally { buffer.close(); }
}

function receiptBeforeCopyDeletion() {
  const id = "00000000-0000-4000-8000-000000005711";
  const ledger = path.join(root, "receipt-crash.sqlite");
  const options = { workspaceId: "r5-crash", deviceId: "r5-device",
    delivery: { enabled: true }, enrollmentNow: () => new Date(now.getTime() - 60 * DAY) };
  const first = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: false } });
  try {
    assert.equal(first.append(event(id)), true);
    first.database.prepare("update buffered_events set created_at=? where id=?").run(oldAt, id);
    first.delivery.configure({ enabled: true });
    first.delivery.repairRawById(id);
  } finally { first.close(); }
  const old = new OldBuffer(ledger, options);
  try {
    assert.equal(old.prune(30, { now: pruneAt }).events, 1);
  } finally { old.close(); }
  const interrupted = new LocalEventBuffer(ledger, options);
  try {
    const copy = interrupted.database.prepare(`select raw_rowid as rowid,
      raw_id as id,raw_created_at as at,raw_generation as generation,
      attempt_count as attempts,created_at as createdAt from upload_outbox
      where delivery_id=?`).get(id) as {
        rowid: number; id: string; at: string; generation: string;
        attempts: number; createdAt: string;
      };
    assert.ok(copy);
    interrupted.database.prepare(`insert into upload_receipts
      (delivery_id,raw_rowid,raw_id,raw_created_at,raw_generation,
       terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,?,?,?,?,'acknowledged','remote_acknowledged','remote_2xx',?,?,?)`).run(
      id, copy.rowid, copy.id, copy.at, copy.generation, copy.attempts,
      copy.createdAt, uploadAt.toISOString());
    assert.ok(interrupted.database.prepare("select 1 from upload_outbox where delivery_id=?")
      .get(id), "the interrupted copy is still present");
  } finally { interrupted.close(); }
  const restarted = new LocalEventBuffer(ledger, options);
  try {
    const lease = restarted.delivery.lease({ now: uploadAt });
    assert.equal(lease.items.length, 0);
    assert.equal(restarted.database.prepare("select 1 from upload_outbox where delivery_id=?")
      .get(id), undefined);
    assert.equal(restarted.database.prepare(`select terminal_state as state from upload_receipts
      where delivery_id=?`).get(id)?.state, "acknowledged");
    assert.equal(restarted.retentionStatus(30, uploadAt).states.heldForUpload, 0);
    console.log(JSON.stringify({ case: "receipt_written_copy_not_deleted",
      copyCleaned: true, acknowledgementPreserved: true, held: 0 }));
  } finally { restarted.close(); }
}

async function main() {
  const received: string[] = [];
  const server = http.createServer(async (request, response) => {
    try {
      let body = "";
      for await (const chunk of request) body += String(chunk);
      const expected = deliveryExpectation(body, "r5-install");
      const payload = JSON.parse(body) as { events: Array<{ event: { id: string } }> };
      received.push(...payload.events.map((item) => item.event.id));
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ accepted: expected.itemIds.length,
        ack: deliveryAcknowledgement(expected, expected.itemIds) }));
    } catch (error) {
      response.writeHead(500, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: String(error) }));
    }
  });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    for (const position of [0, 3, 5]) await reuseCase(position, address.port, received);
    ambiguousLegacyReceipt();
    receiptBeforeCopyDeletion();
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
