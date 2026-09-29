/** A 409 refusal shares the HTTP response byte cap and deadline. */
import assert from "node:assert/strict";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { MAX_RESPONSE_BYTES } from "../packages/collector-cli/src/http-transport";
import { runSessionSync } from "../packages/collector-cli/src/session-sync";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-sync-refusal-bound", 4);
const root = process.env.PLIMSOLL_PROOF_ROOT!;
const tenantId = "00000000-0000-4000-8000-000000000719";
const installKey = "refusal-bound-proof";
const config = collectorConfigSchema.parse({
  port: 48317, uploadUrl: "http://127.0.0.1:48316/ingest",
  tenantId, installKey, uploadSigningSecret: "refusal-bound-proof-secret",
  delivery: { requestTimeoutSeconds: 5 },
});

function addRow(buffer: LocalEventBuffer, sessionId: string, id: string) {
  assert.equal(buffer.append(aiInteractionEventSchema.parse({
    id, sessionId, source: "codex", eventType: "assistant_response",
    observedAt: new Date().toISOString(), inputTokens: 1, outputTokens: 1,
  })), true);
}

async function main() {
  const buffer = new LocalEventBuffer(path.join(root, "refusal-bound.sqlite"), { workspaceId: tenantId });
  const oversizedId = "11111111-1111-4111-8111-111111111719";
  const ordinaryId = "22222222-2222-4222-8222-222222222719";
  addRow(buffer, oversizedId, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa719");
  try {
    const chunkBytes = 16 * 1024;
    let producedBytes = 0;
    const oversized = await runSessionSync(config, {
      ledgerDb: buffer.database, sessionIds: [oversizedId], incremental: true,
      until: new Date(Date.now() + 60_000).toISOString(),
      maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
      fetchImpl: (async () => new Response(new ReadableStream<Uint8Array>({
        pull(controller) {
          if (producedBytes >= MAX_RESPONSE_BYTES * 4) { controller.close(); return; }
          controller.enqueue(new Uint8Array(chunkBytes));
          producedBytes += chunkBytes;
        },
      }), { status: 409, headers: { "content-type": "application/json" } })) as typeof fetch,
    });
    console.log(JSON.stringify({ case: "headerless_oversize_409", producedBytes,
      capBytes: MAX_RESPONSE_BYTES, reason: oversized.reason }));
    assert.ok(producedBytes <= MAX_RESPONSE_BYTES + chunkBytes * 2,
      "the 409 body must stop streaming at the existing byte cap");
    completion.check("oversized_409_stops_at_transport_byte_cap");
    assert.equal(oversized.ok, false);
    assert.match(oversized.reason ?? "", /response_too_large/);
    completion.check("oversized_409_is_response_too_large");
    assert.equal(oversized.settlements.length, 0);
    assert.ok(buffer.database.prepare("select 1 from session_sync_upload_leases where session_id = ?")
      .get(oversizedId), "an unrecognized oversized response leaves the finite lease");
    completion.check("oversized_uncertain_response_keeps_lease");

    addRow(buffer, ordinaryId, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbb719");
    const ordinary = await runSessionSync(config, {
      ledgerDb: buffer.database, sessionIds: [ordinaryId], incremental: true,
      until: new Date(Date.now() + 60_000).toISOString(),
      maxAttemptsPerBatch: 1, delayMs: 0, log: () => undefined,
      fetchImpl: (async () => new Response(JSON.stringify({
        error: "session_sync_clock_skew", serverTime: new Date().toISOString(),
      }), { status: 409, headers: { "content-type": "application/json" } })) as typeof fetch,
    });
    assert.equal(ordinary.settlements[0]?.status, "clock_skew");
    assert.equal(buffer.database.prepare("select 1 from session_sync_upload_leases where session_id = ?")
      .get(ordinaryId), undefined);
    completion.check("bounded_normal_409_settles_and_releases");
    completion.complete();
  } finally {
    buffer.close();
  }
}

void main().catch((error) => { console.error(error); process.exitCode = 1; });
