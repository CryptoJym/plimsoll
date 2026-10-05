import { nativeCodexFixture } from "../scripts/lib/native-codex-fixture";
import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { isCaptureGap } from "../packages/collector-cli/src/codex-model-capture";
import { aiInteractionEventSchema, type AiInteractionEvent } from "../packages/shared/src/index";

const baseRoot = process.env.PR417_BASE_WORKTREE;
assert.ok(baseRoot, "set PR417_BASE_WORKTREE to exact 0.7.44");
const OldBuffer = require(path.join(baseRoot,
  "packages/collector-cli/src/buffer.ts")).LocalEventBuffer as typeof LocalEventBuffer;

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-reuse-privacy-collision-"));
  const ledger = path.join(root, "ledger.sqlite");
  const id = "00000000-0000-4000-8000-000000004199";
  const now = new Date();
  const oldAt = new Date(now.getTime() - 45 * 86_400_000).toISOString();
  const options = { workspaceId: "review-reuse-privacy", deviceId: "review-device",
    delivery: { enabled: false }, enrollmentNow: () => new Date(now.getTime() - 60 * 86_400_000) };
  const event = aiInteractionEventSchema.parse({ id, sessionId: id, source: "codex", ...nativeCodexFixture(id),
    eventType: "assistant_response", dataMode: "metadata", observedAt: oldAt,
    actionClass: "other", inputTokens: 1, outputTokens: 1 });
  let firstGeneration = "";
  let privateGeneration = "";
  let oldFrozenBytes = "";
  let server: http.Server | undefined;
  try {
    const first = new OldBuffer(ledger, options);
    try {
      assert.equal(first.append(event), true);
      first.database.prepare("update buffered_events set created_at=? where id=?").run(oldAt, id);
      first.delivery.configure({ enabled: true });
      assert.equal(first.delivery.repairRawById(id).enqueued, 1);
      // Exercise an already-frozen old request. The newer collector must
      // retain those bytes' identity while replacing unsafe usage separately.
      first.database.prepare(`update upload_outbox set sealed_envelope_json=base_envelope_json,
        sealed_bytes=base_bytes,state='retry',attempt_count=1 where delivery_id=?`).run(id);
      oldFrozenBytes=(first.database.prepare(`select sealed_envelope_json as bytes
        from upload_outbox where delivery_id=?`).get(id) as {bytes:string}).bytes;
      firstGeneration = (first.database.prepare(`select raw_generation as generation
        from upload_outbox where delivery_id=?`).get(id) as { generation: string }).generation;
      assert.equal(first.prune(30, { now }).events, 1);
      assert.equal(first.database.prepare("select count(*) as n from upload_outbox").get()?.n, 1);
      assert.equal((first.database.prepare(`select raw_generation as generation
        from raw_retention_receipts where event_id=?`).get(id) as { generation: string }).generation,
        firstGeneration);
      // Exact 0.7.44 rejects a later incarnation while the first envelope remains queued.
      const privateEvent = aiInteractionEventSchema.parse({ ...event,
        metadata: { ...event.metadata, serviceName: "sk_live_private_12345678901234567890" } });
      assert.equal(first.append(privateEvent), true);
      const raw = first.database.prepare(`select rowid as rowid,privacy_generation as generation,
        privacy_disposition as disposition from buffered_events where id=?`).get(id) as
        { rowid: number; generation: string; disposition: string };
      privateGeneration = raw.generation;
      const receipt = first.database.prepare(`select reason,created_at as createdAt
        from upload_receipts where delivery_id=?`).get(id) as { reason: string; createdAt: string };
      console.log(JSON.stringify({ phase: "old_private_reuse", raw, receipt,
        outbox: first.database.prepare("select count(*) as n from upload_outbox").get() }));
      assert.equal(raw.disposition, "local_privacy_violation");
      assert.equal(receipt.reason, "local_privacy_violation");
      assert.notEqual(privateGeneration, firstGeneration);
      assert.notEqual(receipt.createdAt, oldAt);
      if (process.env.PR417_DROP_SECOND_RECEIPT === "1") {
        // Isolate the independent recycled-rowid privacy lookup fault.
        first.database.prepare("delete from upload_receipts where delivery_id=?").run(id);
      }
    } finally { first.close(); }

    const received: AiInteractionEvent[] = [];
    server = http.createServer(async (request, response) => {
      try {
        let body = "";
        for await (const chunk of request) body += String(chunk);
        const payload = JSON.parse(body) as { events: Array<{ event: AiInteractionEvent }> };
        received.push(...payload.events.map((item) => item.event));
        const expected = deliveryExpectation(body, "privacy-collision-install");
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
      deviceId: options.deviceId, installKey: "privacy-collision-install",
      retentionDays: 30, uploadUrl: `http://127.0.0.1:${address.port}/ingest`,
      delivery: { maxOldestAgeDays: 3650 } });
    const upgraded = new LocalEventBuffer(ledger, { ...options, delivery: { enabled: true } });
    try {
      const before = upgraded.database.prepare(`select delivery_id as id,raw_rowid as rowid,
        raw_generation as generation,length(base_envelope_json) as bytes from upload_outbox`).all();
      assert.equal(before.length, 1);
      assert.equal(before[0]?.generation, firstGeneration);
      assert.ok(before[0]?.bytes > 0);
      assert.equal((upgraded.database.prepare(`select sealed_envelope_json as bytes
        from upload_outbox where delivery_id=?`).get(id) as {bytes:string}).bytes,oldFrozenBytes);
      upgraded.database.exec(`create temp table frozen_byte_rewrites (delivery_id text);
        create temp trigger prove_old_frozen_bytes before update of base_envelope_json,sealed_envelope_json
          on upload_outbox when old.delivery_id='${id}' and
          (new.base_envelope_json is not old.base_envelope_json or
            new.sealed_envelope_json is not old.sealed_envelope_json)
          begin insert into frozen_byte_rewrites values(old.delivery_id); end;`);
      const uploadAt = new Date(now.getTime() + 3_600_000);
      const retired = await uploadBufferedEvents(config, upgraded, { now: () => uploadAt });
      assert.equal(retired.uploadedEvents, 0,
        "the older sealed copy lacks retained native capture provenance");
      assert.deepEqual(received, [], "unsafe frozen usage never reaches the wire");
      const replacement = upgraded.database.prepare(`select delivery_id as id,base_envelope_json as bytes,
        raw_generation as generation from upload_outbox`).get() as {id:string;bytes:string;generation:string};
      assert.ok(replacement);
      assert.notEqual(replacement.id,id,"the frozen ID must never be rewritten as a gap");
      assert.equal(replacement.generation,firstGeneration,
        "the old incarnation's gap must not inherit the later privacy rejection");
      const result = await uploadBufferedEvents(config, upgraded, { now: () => uploadAt });
      assert.equal(result.uploadedEvents, 1);
      assert.equal(received.length,1);
      const gap=received[0]!;
      assert.equal(gap.id,replacement.id);
      assert.equal(isCaptureGap(gap),true);
      assert.equal(gap.metadata.usageSource,"capture_gap");
      assert.equal(gap.model,undefined);
      for(const field of ["inputTokens","outputTokens","cacheReadTokens","cacheCreationTokens","costUsd"] as const)
        assert.equal(gap[field],undefined);
      // Local capture diagnostics are deliberately omitted from the wire so
      // exact 0.7.47/.48 readers re-seal it byte-for-byte. The old frozen copy
      // still supplies its original counters as diagnostic evidence.
      assert.equal(gap.metadata.modelGapInputTokens,undefined);
      assert.equal(gap.metadata.modelGapOutputTokens,undefined);
      const oldDiagnostic=JSON.parse(oldFrozenBytes).event;
      assert.equal(oldDiagnostic.inputTokens,1);
      assert.equal(oldDiagnostic.outputTokens,1);
      assert.equal((upgraded.database.prepare("select count(*) as n from frozen_byte_rewrites").get() as {n:number}).n,0);
      const decisions=upgraded.database.prepare(`select raw_generation as generation
        from codex_capture_decisions where raw_id=?`).all(id) as {generation:string}[];
      assert.ok(decisions.some(row=>row.generation===firstGeneration));
      assert.equal(decisions.some(row=>row.generation===privateGeneration),false,
        "an old gap cannot decide the later incarnation's capture state");
      assert.equal(JSON.stringify(gap).includes("sk_live_private_"),false);
      assert.equal(upgraded.database.prepare("select count(*) as n from upload_outbox").get()?.n, 0);
      assert.equal(upgraded.delivery.status(uploadAt).remainingDelivery, 0);
      assert.deepEqual(upgraded.database.prepare(`select terminal_state as state,reason,
        raw_generation as generation from upload_receipts where delivery_id=?`).get(replacement.id),
      { state: "acknowledged", reason: "remote_acknowledged", generation: firstGeneration });
      assert.deepEqual(upgraded.database.prepare(`select terminal_state as state,reason,
        raw_generation as generation from upload_receipts where delivery_id=?`).get(id),
        {state:"dead",reason:"local_model_capture_gap",generation:firstGeneration});
      if (process.env.PR417_DROP_SECOND_RECEIPT !== "1") {
        assert.ok(upgraded.database.prepare(`select 1 from upload_receipts
          where raw_id=? and raw_generation=? and terminal_state='dead'
            and reason='local_privacy_violation'`).get(id, privateGeneration),
        "the later incarnation keeps its own terminal privacy receipt");
      }
      assert.equal((upgraded.database.prepare(`select privacy_disposition as disposition
        from buffered_events where id=?`).get(id) as { disposition: string }).disposition,
        "local_privacy_violation");
      assert.equal(upgraded.retentionStatus(30, uploadAt).states.heldForUpload, 0);
      console.log(JSON.stringify({ phase: "reupgrade", uploaded: result.uploadedEvents,
        received, acknowledged: 1, privateBlocked: true, held: 0, remaining: 0 }));
    } finally { upgraded.close(); }
  } finally {
    if (server) await new Promise<void>((resolve) => server!.close(() => resolve()));
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
