import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { withRoundThreeReader } from "./lib/legacy-reader";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "review-upgrade-gap-"));
const file = path.join(root, "ledger.sqlite");
const at = Date.now() - 300_000;
let now = new Date(at + 2_000);
const id = "00000000-0000-4000-8000-000000000831";
const session = "22222222-2222-4222-8222-222222222222";
const trace = "c".repeat(32);
const opts = {
  workspaceId: "11111111-1111-4111-8111-111111111111", deviceId: "review-device",
  enrollmentNow: () => new Date(at - 1_000), delivery: { enabled: true, now: () => now },
};
const event = (value: object) => aiInteractionEventSchema.parse({
  id, source: "codex", dataMode: "metadata", eventType: "assistant_response",
  sessionId: session, observedAt: new Date(at).toISOString(), inputTokens: 19, outputTokens: 2,
  metadata: { otelEventName: "handle_responses", traceId: trace }, ...value,
});

async function main() {
  let head: InstanceType<typeof LocalEventBuffer> | undefined;
  try {
    await withRoundThreeReader(async ({ Buffer: R3Buffer }) => {
      const old = new R3Buffer(file, opts);
      try {
        old.append(event({}));
        now = new Date(at + 63_000);
        const first = old.delivery.lease({ now });
        const initial = first.items.find((item: any) => item.rawId === id);
        assert.ok(initial);
        assert.equal(initial.envelope.event.metadata.usageSource, "capture_gap");
        assert.equal(initial.envelope.event.inputTokens, undefined);
      } finally { old.close(); }
    });
    now = new Date(at + 184_000);
    head = new LocalEventBuffer(file, opts);
    const retainedLease = head.delivery.lease({ now });
    const retained = retainedLease.items.find((item) => item.rawId === id);
    assert.ok(retained);
    assert.equal(retained.envelope.event.metadata.usageSource, "capture_gap");
    assert.equal(retained.envelope.event.inputTokens, undefined);
    const decisionBefore = head.database.prepare(
      "select count(*) as n from codex_capture_decisions where decision='gap'",
    ).get() as { n: number };
    assert.equal(decisionBefore.n, 1);
    assert.equal(head.delivery.deadLetterRemote(retainedLease.leaseId, [id], now), 1);
    head.append(aiInteractionEventSchema.parse({
      id: "00000000-0000-4000-8000-000000000832", source: "codex", dataMode: "metadata",
      eventType: "otel_span", sessionId: session, observedAt: new Date(at + 1_000).toISOString(),
      model: "gpt-6.1-sol", metadata: { otelEventName: "codex.sse_event", trace, "gen_ai.request.model": "gpt-6.1-sol" },
    }));
    const replay = head.delivery.replayDeadLetters({ reason: "remote_validation_rejected", now });
    assert.equal(replay.requeued, 1);
    now = new Date(at + 245_000);
    const after = head.delivery.lease({ now }).items.find((item) => item.rawId === id);
    assert.ok(after);
    const passed = after.envelope.event.metadata.usageSource === "capture_gap" && after.envelope.event.inputTokens === undefined;
    console.log(JSON.stringify({ case: "r3-sealed-gap-survives-upgrade-remote-replay", passed, decisionBefore, replay,
      afterReplay: after.envelope, expected: "an older sealed gap stays tokenless after terminal replay" }, null, 2));
    if (!passed) process.exitCode = 1;
  } finally { head?.close(); fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
