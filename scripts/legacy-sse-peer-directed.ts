import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { withLegacyReader } from "./lib/legacy-reader";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "review-legacy-sse-peer-"));
const file = path.join(root, "ledger.sqlite");
const at = Date.now() - 300_000;
let now = new Date(at + 2_000);
const session = "22222222-2222-4222-8222-222222222222";
const trace = "e".repeat(32);
const opts = {
  workspaceId: "11111111-1111-4111-8111-111111111111",
  deviceId: "review-device",
  enrollmentNow: () => new Date(at - 1_000),
  delivery: { enabled: true, now: () => now },
};
const event = (id: string, extra: object) => aiInteractionEventSchema.parse({
  id, source: "codex", dataMode: "metadata", eventType: "assistant_response",
  sessionId: session, observedAt: new Date(at).toISOString(), metadata: {}, ...extra,
});

async function main() {
  let head: InstanceType<typeof LocalEventBuffer> | undefined;
  try {
    await withLegacyReader(async ({ Buffer: OldBuffer, reconciliation }) => {
      const old = new OldBuffer(file, { ...opts, delivery: { enabled: false, now: () => now } });
      try {
        const target = event("00000000-0000-4000-8000-000000000821", {
          inputTokens: 19, outputTokens: 2,
          metadata: { otelEventName: "handle_responses", traceId: trace },
        });
        const peer = event("00000000-0000-4000-8000-000000000822", {
          inputTokens: 17, outputTokens: 3,
          metadata: { otelEventName: "codex.sse_event", traceId: trace },
        });
        old.append(event("00000000-0000-4000-8000-000000000823", {
          sessionId: "33333333-3333-4333-8333-333333333333", eventType: "otel_span",
          model: "gpt-6-astra", metadata: { otelEventName: "thread/read", traceId: "b".repeat(32) },
        }));
        old.append(peer); old.append(target);
        reconciliation.runCodexReconciliationMaintenance(old.database, {
          legacyRowLimit: 100, legacyChunkLimit: 100, contextWindowLimit: 100,
          contextRowLimit: 100, candidateLimit: 100, freshCandidateLimit: 100, timeLimitMs: 1_000,
        });
        const guessedPeer = JSON.parse((old.database.prepare(
          "select payload_json as p from buffered_events where id=?",
        ).get(peer.id) as { p: string }).p);
        assert.equal(guessedPeer.model, "gpt-6-astra");
        assert.equal(guessedPeer.metadata.model, undefined);
      } finally { old.close(); }
    });
    now = new Date(at + 123_000);
    head = new LocalEventBuffer(file, opts);
    head.delivery.migrateLegacy({ maxRows: 100, now });
    now = new Date(at + 184_000);
    const lease = head.delivery.lease({ now });
    const sent = lease.items.find((item) => item.rawId === "00000000-0000-4000-8000-000000000821");
    assert.ok(sent);
    const passed = sent.envelope.event.model === undefined && sent.envelope.event.inputTokens === undefined;
    console.log(JSON.stringify({
      case: "legacy-maintenance-sse-peer-cannot-supply-model", passed,
      headDelivery: sent.envelope,
      expected: "a bare guessed SSE peer is not native trace evidence; target is tokenless",
    }, null, 2));
    if (!passed) process.exitCode = 1;
  } finally {
    head?.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
