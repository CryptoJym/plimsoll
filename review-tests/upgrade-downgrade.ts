import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { refreshUnsentRawDelivery } from "../packages/collector-cli/src/outbox";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const rollbackStatement = "Rolling back to 0.7.44 restores its retention: expired rows may be pruned locally before upload; their queued copies still upload.";

async function main() {
  const baseRoot = process.env.PR417_BASE_WORKTREE;
  assert.ok(baseRoot, "set PR417_BASE_WORKTREE to a local exact 1ae7bbc8 checkout");
  const baseModule = await import(pathToFileURL(path.join(baseRoot,
    "packages/collector-cli/src/buffer.ts")).href);
  const OldBuffer = baseModule.LocalEventBuffer as typeof LocalEventBuffer;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-upgrade-downgrade-"));
  const ledger = path.join(root, "ledger.sqlite");
  const now = new Date();
  const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
  const enrollmentAt = new Date(now.getTime() - 60 * 86_400_000);
  const ids = {
    unsealed: "00000000-0000-4000-8000-000000004170",
    sealed: "00000000-0000-4000-8000-000000004171",
    reseal: "00000000-0000-4000-8000-000000004172",
    privacy: "00000000-0000-4000-8000-000000004173",
    unqueued: "00000000-0000-4000-8000-000000004174",
  };
  const queuedIds = [ids.unsealed, ids.sealed, ids.reseal];
  const allIds = [...queuedIds, ids.unqueued];
  const seededIds = [...allIds, ids.privacy];
  const workspaceId = "upgrade-downgrade";
  const deviceId = "upgrade-device";
  const options = { workspaceId, deviceId,
    delivery: { enabled: false }, enrollmentNow: () => enrollmentAt };
  const exists = (buffer: LocalEventBuffer, rawId: string) => Boolean(buffer.database.prepare(
    "select 1 from buffered_events where id=?").get(rawId));
  const count = (buffer: LocalEventBuffer, table: string) =>
    (buffer.database.prepare(`select count(*) as n from ${table}`).get() as { n: number }).n;
  let ingestServer: http.Server | undefined;
  let statusServer: http.Server | undefined;
  const received: string[] = [];
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
    const old = new OldBuffer(ledger, options);
    try {
      for (const rawId of seededIds) {
        const event = aiInteractionEventSchema.parse({ id: rawId, sessionId: rawId,
          source: "codex", eventType: "assistant_response", dataMode: "metadata",
          observedAt: oldAt, actionClass: "other", inputTokens: 1, outputTokens: 1 });
        assert.equal(old.append(event), true);
        old.database.prepare("update buffered_events set created_at=? where id=?")
          .run(oldAt, rawId);
      }
      old.delivery.configure({ enabled: true });
      for (const id of [...queuedIds, ids.privacy])
        assert.equal(old.delivery.repairRawById(id).enqueued, 1);
      assert.equal(count(old, "upload_outbox"), queuedIds.length + 1);
      assert.equal((old.database.prepare("select count(*) as n from upload_outbox where raw_id=?")
        .get(ids.unqueued) as { n: number }).n, 0);
    } finally { old.close(); }

    const upgraded = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: true } });
    try {
      // A stale queued copy that acquires a terminal privacy decision must be
      // retired while the raw row is still present, before old retention runs.
      upgraded.database.prepare(`update buffered_events set
        privacy_disposition='local_privacy_violation',privacy_disposed_at=?
        where id=?`).run(now.toISOString(), ids.privacy);
      const first = upgraded.prune(30, { maxRows: 10, now });
      assert.equal(first.events, 1);
      for (const id of allIds) assert.equal(exists(upgraded, id), true);
      assert.equal(exists(upgraded, ids.privacy), false);
      assert.equal(upgraded.retentionStatus(30, now).states.heldForUpload, allIds.length);
      assert.equal((upgraded.database.prepare(`select reason from upload_receipts
        where delivery_id=?`).get(ids.privacy) as { reason: string }).reason,
        "local_privacy_violation");
      // Persist cached-sealed and pending re-seal states before old retention.
      // Every outbox row already owns a complete base envelope.
      const db = upgraded.database;
      db.prepare(`update upload_outbox set sealed_envelope_json=base_envelope_json,
        sealed_bytes=base_bytes where raw_id in (?,?)`).run(ids.sealed, ids.reseal);
      assert.equal(refreshUnsentRawDelivery(db, ids.reseal), true);
      const states = db.prepare(`select raw_id as id,
        sealed_envelope_json is not null as sealed,
        length(base_envelope_json) > 0 as basePresent,
        workspace_id as workspaceId,device_id as deviceId
        from upload_outbox order by raw_id`).all() as Array<{
          id: string; sealed: number; basePresent: number;
          workspaceId: string; deviceId: string;
        }>;
      assert.deepEqual(states.map((row) => row.sealed), [0, 1, 0]);
      assert.ok(states.every((row) => row.basePresent === 1 &&
        row.workspaceId === workspaceId && row.deviceId === deviceId));
      console.log(JSON.stringify({ phase: "upgrade", first, held: allIds.length, states }));
    } finally { upgraded.close(); }

    // The exact 0.7.44 source operates on the upgraded ledger.
    const downgraded = new OldBuffer(ledger, { ...options, delivery: { enabled: true } });
    try {
      const pass = downgraded.prune(30, { maxRows: 10, now });
      assert.equal(pass.events, queuedIds.length);
      for (const id of queuedIds) assert.equal(exists(downgraded, id), false);
      assert.equal(exists(downgraded, ids.unqueued), true);
      assert.equal(count(downgraded, "upload_outbox"), queuedIds.length);
      assert.equal(count(downgraded, "raw_retention_receipts"), queuedIds.length + 1);
      console.log(JSON.stringify({ phase: "downgrade", pass,
        rawRemoved: queuedIds, outbox: count(downgraded, "upload_outbox") }));
    } finally { downgraded.close(); }

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
    const config = collectorConfigSchema.parse({
      tenantId: workspaceId, deviceId, installKey: "proof-install", retentionDays: 30,
      uploadUrl: `http://127.0.0.1:${ingestPort}/ingest`,
      delivery: { maxOldestAgeDays: 3650 },
    });
    const reupgraded = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: true } });
    try {
      assert.equal(reupgraded.retentionStatus(30, now).states.heldForUpload, 1);
      assert.equal(reupgraded.delivery.status(now).remainingDelivery, queuedIds.length);
      const expiry = reupgraded.database.prepare(`select event_id as id, raw_rowid as rowid,
        raw_generation as generation from raw_retention_receipts`).all() as Array<{
          id: string; rowid: number; generation: string | null;
        }>;
      assert.deepEqual(expiry.map((row) => row.id).sort(), [...queuedIds, ids.privacy].sort());
      const uploaded = await uploadBufferedEvents(config, reupgraded, {
        now: () => new Date(now.getTime() + 3_600_000),
      });
      assert.equal(uploaded.uploadedEvents, allIds.length);
      assert.deepEqual(received.sort(), [...allIds].sort());
      assert.equal(reupgraded.delivery.status(now).remainingDelivery, 0);
      assert.equal(count(reupgraded, "upload_outbox"), 0);
      for (const id of allIds) {
        const receipt = reupgraded.database.prepare(`select terminal_state as state,
          reason from upload_receipts where delivery_id=?`).get(id) as
          { state: string; reason: string } | undefined;
        assert.deepEqual(receipt, { state: "acknowledged", reason: "remote_acknowledged" });
      }
      assert.equal(received.includes(ids.privacy), false);
      const after = reupgraded.prune(30, { maxRows: 10, now: new Date(now.getTime() + 3_600_000) });
      assert.equal(after.events, 1, "the formerly unqueued raw expires after acknowledgement");
      assert.equal(reupgraded.retentionStatus(30, now).states.heldForUpload, 0);
      statusServer = createCollectorServer(config, reupgraded);
      const statusPort = await listen(statusServer);
      const statusResponse = await fetch(`http://127.0.0.1:${statusPort}/status`);
      assert.equal(statusResponse.status, 200);
      const status = await statusResponse.json() as {
        retention?: { states?: { heldForUpload?: number } };
        delivery?: { remainingDelivery?: number };
      };
      assert.equal(status.retention?.states?.heldForUpload, 0);
      assert.equal(status.delivery?.remainingDelivery, 0);
      console.log(JSON.stringify({ phase: "reupgrade", removedRawUploaded: queuedIds,
        received, uploaded: uploaded.uploadedEvents, remaining: 0,
        receipts: count(reupgraded, "upload_receipts"),
        held: status.retention?.states?.heldForUpload, status: statusResponse.status }));
    } finally { await close(statusServer); reupgraded.close(); }
    assert.ok(fs.readFileSync(path.join(__dirname, "../docs/runbooks/raw-retention-rollback.md"), "utf8")
      .includes(rollbackStatement), "the runbook must state 0.7.44 rollback behavior");
  } finally {
    await close(ingestServer);
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
