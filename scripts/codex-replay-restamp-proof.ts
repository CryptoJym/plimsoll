import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("codex-replay-restamp", 1);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "codex-replay-restamp-"));
const at = Date.now() - 300000;
let now = new Date(at + 2000);
const id = "00000000-0000-4000-8000-000000000853";
const opts = {
  workspaceId: "11111111-1111-4111-8111-111111111111",
  deviceId: "review-device",
  enrollmentNow: () => new Date(at - 1000),
  delivery: { enabled: true, now: () => now },
};
try {
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), opts);
  try {
    const raw = aiInteractionEventSchema.parse({
      id, sessionId: "22222222-2222-4222-8222-222222222222", source: "codex",
      dataMode: "metadata", eventType: "assistant_response",
      observedAt: new Date(at).toISOString(), model: "gpt-6.1-sol",
      inputTokens: 19, outputTokens: 2,
      metadata: { traceId: "b".repeat(32), otelEventName: "handle_responses",
        "gen_ai.request.model": "gpt-6.1-sol" },
    });
    buffer.append(raw);
    now = new Date(at + 63000);
    const first = buffer.delivery.lease({ now });
    const frozen = first.items.find((item) => item.rawId === id);
    assert.ok(frozen);
    now = new Date(at + 184000);
    const expired = buffer.delivery.lease({ now });
    const retry = expired.items.find((item) => item.rawId === id);
    assert.ok(retry);
    assert.equal(retry.envelopeJson, frozen.envelopeJson);
    assert.equal(buffer.delivery.deadLetterRemote(expired.leaseId, [retry.deliveryId], now), 1);
    assert.equal(buffer.delivery.replayDeadLetters({ reason: "remote_validation_rejected", now }).requeued, 1);
    const payload = JSON.parse((buffer.database.prepare(
      "select payload_json as p from buffered_events where id=?",
    ).get(id) as { p: string }).p);
    const restamped = buffer.delivery.restampUnsentRaw(id, JSON.stringify({
      ...payload, metadata: { ...payload.metadata, workItemId: "44444444-4444-4444-8444-444444444444" },
    }));
    assert.equal(restamped, false);
    now = new Date(at + 245000);
    const after = buffer.delivery.lease({ now }).items.find((item) => item.rawId === id);
    assert.ok(after);
    assert.equal(after.deliveryId, frozen.deliveryId);
    assert.equal(after.envelopeJson, frozen.envelopeJson);
    const lineage = buffer.database.prepare(
      "select frozen_envelope_json as frozen, frozen_attempt_count as attempts from upload_replays where delivery_id=?",
    ).get(frozen.deliveryId) as { frozen: string; attempts: number };
    assert.equal(lineage.frozen, frozen.envelopeJson);
    assert.ok(lineage.attempts >= 1);
    console.log(JSON.stringify({ proof: "codex-replay-restamp", passed: true,
      restamped, sameId: true, frozenBytesSame: true, durableLineage: true }, null, 2));
  } finally { buffer.close(); }
  completion.check("head-replay-preserves-frozen-named-delivery");
  completion.complete();
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}

