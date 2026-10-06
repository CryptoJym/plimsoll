import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";
import { withReader } from "./lib/legacy-reader";

const completion = createProofCompletion("codex-replay-restamp-base-origin", 1);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "codex-replay-restamp-base-"));
const at = Date.now() - 300000;
let now = new Date(at + 2000);
const id = "00000000-0000-4000-8000-000000000853";
const opts = {
  workspaceId: "11111111-1111-4111-8111-111111111111",
  deviceId: "review-device",
  enrollmentNow: () => new Date(at - 1000),
  delivery: { enabled: true, now: () => now },
};
async function main() {
  try {
    await withReader("34d58bcd90865679e09fcbd1ee1703de5effda97", async ({ Buffer }) => {
    const buffer = new Buffer(path.join(root, "ledger.sqlite"), opts);
    try {
      const raw = aiInteractionEventSchema.parse({
        id, sessionId: "22222222-2222-4222-8222-222222222222", source: "codex",
        dataMode: "metadata", eventType: "assistant_response",
        observedAt: new Date(at).toISOString(), model: "gpt-6.1-sol",
        inputTokens: 19, outputTokens: 2,
        metadata: { traceId: "b".repeat(32), otelEventName: "handle_responses",
          "gen_ai.request.model": "gpt-6.1-sol" },
      });
      buffer.append(raw); now = new Date(at + 63000);
      const first = buffer.delivery.lease({ now });
      const frozen = first.items.find((item: any) => item.rawId === id);
      assert.ok(frozen); now = new Date(at + 184000);
      const expired = buffer.delivery.lease({ now });
      const retry = expired.items.find((item: any) => item.rawId === id);
      assert.ok(retry); assert.equal(retry.envelopeJson, frozen.envelopeJson);
      assert.equal(buffer.delivery.deadLetterRemote(expired.leaseId, [retry.deliveryId], now), 1);
      assert.equal(buffer.delivery.replayDeadLetters({ reason: "remote_validation_rejected", now }).requeued, 1);
      const payload = JSON.parse((buffer.database.prepare(
        "select payload_json as p from buffered_events where id=?",
      ).get(id) as { p: string }).p);
      const restamped = buffer.delivery.restampUnsentRaw(id, JSON.stringify({
        ...payload, metadata: { ...payload.metadata, workItemId: "44444444-4444-4444-8444-444444444444" },
      }));
      now = new Date(at + 245000);
      const after = buffer.delivery.lease({ now }).items.find((item: any) => item.rawId === id);
      assert.ok(after); assert.equal(restamped, true);
      assert.notEqual(after.envelopeJson, frozen.envelopeJson);
      console.log(JSON.stringify({ proof: "codex-replay-restamp-base-origin",
        readerCommit: "34d58bcd90865679e09fcbd1ee1703de5effda97",
        baseDefectObserved: true, restamped, frozenBytesSame: false }, null, 2));
    } finally { buffer.close(); }
  });
    completion.check("base-0.7.48-reproducer-retains-the-original-defect");
    completion.complete();
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });

