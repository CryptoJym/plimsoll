import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { createProofCompletion } from "./lib/proof-completion";
import { aiInteractionEventSchema, type AiInteractionEvent } from "../packages/shared/src/index";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { withLegacyReader, proofTempRoot } from "./lib/legacy-reader";

const completion = createProofCompletion("codex-capture-review-r4", 7);
const workspace = "11111111-1111-4111-8111-111111111111";
const device = "review-device";
const session = "22222222-2222-4222-8222-222222222222";
const at = Date.now() - 300_000;
const opts = (now: () => Date) => ({
  workspaceId: workspace, deviceId: device,
  enrollmentNow: () => new Date(at - 1_000),
  delivery: { enabled: true, now },
});
const event = (value: Partial<AiInteractionEvent>): AiInteractionEvent =>
  aiInteractionEventSchema.parse({
    id: value.id ?? crypto.randomUUID(), sessionId: session, source: "codex",
    dataMode: "metadata", eventType: "assistant_response",
    observedAt: new Date(at).toISOString(), inputTokens: 19, outputTokens: 2,
    metadata: {}, ...value,
  });

async function legacySse() {
  const root = proofTempRoot("legacy-sse");
  const file = path.join(root, "ledger.sqlite");
  let now = new Date(at + 2_000);
  await withLegacyReader(async ({ Buffer, reconciliation }) => {
    const old = new Buffer(file, opts(() => now));
    try {
      old.append(event({ id: "00000000-0000-4000-8000-000000000402", sessionId: "33333333-3333-4333-8333-333333333333",
        eventType: "otel_span", model: "gpt-6-astra", inputTokens: undefined, outputTokens: undefined,
        metadata: { otelEventName: "thread/read", traceId: "b".repeat(32) } }));
      old.append(event({ id: "00000000-0000-4000-8000-000000000401", metadata: { otelEventName: "codex.sse_event", traceId: "a".repeat(32) } }));
      reconciliation.runCodexReconciliationMaintenance(old.database, { legacyRowLimit: 100, legacyChunkLimit: 100,
        contextWindowLimit: 100, contextRowLimit: 100, candidateLimit: 100, freshCandidateLimit: 100, timeLimitMs: 1_000 });
    } finally { old.close(); }
    now = new Date(at + 123_000);
    const head = new LocalEventBuffer(file, opts(() => now));
    try {
      const lease = head.delivery.lease({ now });
      const sent = lease.items.find((item) => item.rawId === "00000000-0000-4000-8000-000000000401");
      assert.ok(sent);
      assert.equal(sent.envelope.event.model, undefined);
      assert.equal(sent.envelope.event.inputTokens, undefined);
    } finally { head.close(); }
  });
  fs.rmSync(root, { recursive: true, force: true });
}

async function legacySealed() {
  const root = proofTempRoot("legacy-sealed");
  const file = path.join(root, "ledger.sqlite");
  let now = new Date(at + 2_000);
  await withLegacyReader(async ({ Buffer, reconciliation }) => {
    const old = new Buffer(file, { ...opts(() => now), delivery: { enabled: false, now: () => now } });
    try {
      old.append(event({ id: "00000000-0000-4000-8000-000000000412", sessionId: "33333333-3333-4333-8333-333333333333",
        eventType: "otel_span", model: "gpt-6-astra", inputTokens: undefined, outputTokens: undefined,
        metadata: { otelEventName: "thread/read", traceId: "b".repeat(32) } }));
      old.append(event({ id: "00000000-0000-4000-8000-000000000411", metadata: { otelEventName: "handle_responses", traceId: "a".repeat(32) } }));
      reconciliation.runCodexReconciliationMaintenance(old.database, { legacyRowLimit: 100, legacyChunkLimit: 100,
        contextWindowLimit: 100, contextRowLimit: 100, candidateLimit: 100, freshCandidateLimit: 100, timeLimitMs: 1_000 });
    } finally { old.close(); }
    now = new Date(at + 123_000);
    const oldRetry = new Buffer(file, opts(() => now));
    try {
      oldRetry.delivery.migrateLegacy({ maxRows: 100, now });
      assert.equal(oldRetry.delivery.lease({ now: new Date(at + 2_000) }).items.length, 0);
      const frozen = oldRetry.delivery.lease({ now: new Date(at + 184_000) });
      assert.equal(frozen.items.find((item: { rawId: string | null; envelope: { event: AiInteractionEvent } }) => item.rawId === "00000000-0000-4000-8000-000000000411")?.envelope.event.model, "gpt-6-astra");
    } finally { oldRetry.close(); }
    const head = new LocalEventBuffer(file, opts(() => new Date(at + 305_000)));
    try {
      const lease = head.delivery.lease({ now: new Date(at + 305_000) });
      assert.equal(lease.locallyDead, 1);
      assert.equal(lease.items.some((item) => item.rawId === "00000000-0000-4000-8000-000000000411"), false);
      const sent = head.delivery.lease({ now: new Date(at + 305_000) }).items
        .find((item: { rawId: string | null }) => item.rawId === "00000000-0000-4000-8000-000000000411");
      assert.ok(sent);
      assert.equal(sent.envelope.event.model, undefined);
      assert.equal(sent.envelope.event.inputTokens, undefined);
    } finally { head.close(); }
  });
  fs.rmSync(root, { recursive: true, force: true });
}

async function remoteReplay() {
  const root = proofTempRoot("gap-replay");
  let now = new Date(at + 2_000);
  const b = new LocalEventBuffer(path.join(root, "ledger.sqlite"), opts(() => now));
  try {
    const target = event({ id: "00000000-0000-4000-8000-000000000701", metadata: { traceId: "d".repeat(32), otelEventName: "handle_responses" } });
    b.append(target);
    now = new Date(at + 63_000);
    const first = b.delivery.lease({ now });
    assert.equal(first.items[0]?.envelope.event.metadata.usageSource, "capture_gap");
    assert.equal(b.delivery.deadLetterRemote(first.leaseId, [target.id], now), 1);
    b.append(event({ id: "00000000-0000-4000-8000-000000000702", eventType: "otel_span", model: "gpt-6.1-sol",
      metadata: { traceId: "d".repeat(32), otelEventName: "codex.sse_event", "gen_ai.request.model": "gpt-6.1-sol" } }));
    assert.equal(b.delivery.replayDeadLetters({ reason: "remote_validation_rejected", now }).requeued, 1);
    now = new Date(at + 124_000);
    const second = b.delivery.lease({ now });
    const replayed = second.items.find((item) => item.rawId === target.id);
    assert.equal(replayed?.envelope.event.metadata.usageSource, "capture_gap");
    assert.equal(replayed?.envelope.event.inputTokens, undefined);
    assert.equal((b.database.prepare("select count(*) as n from codex_capture_decisions where decision='gap'").get() as { n: number }).n, 1);
  } finally { b.close(); fs.rmSync(root, { recursive: true, force: true }); }
}

async function main() {
  await legacySse(); completion.check("legacy-sse-directed");
  await legacySealed(); completion.check("legacy-sealed-directed"); completion.check("legacy-sealed-expired-directed");
  await remoteReplay(); completion.check("gap-replay-directed"); completion.check("gap-replay-expired-directed");
  completion.check("remote-replay-persists-lineage-decision");
  completion.check("reviewer-paths-share-durable-lineage-guard");
  completion.complete();
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
