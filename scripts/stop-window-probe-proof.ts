import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import type { AiInteractionEvent } from "../packages/shared/src/index";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";
import { markStopWindowProbe } from "../packages/collector-cli/src/stop-window-probe";

const event: AiInteractionEvent = {
  id: randomUUID(), source: "codex", dataMode: "metadata", eventType: "assistant_response",
  observedAt: new Date().toISOString(), intent: "unknown", actionClass: "other",
  model: "gpt-5-codex", inputTokens: 11, outputTokens: 7, costUsd: 0.01,
  costKind: "reported", metadata: {},
};
const regular = sealOutboundEnvelope({ event, suppressedFields: [] });
assert.equal(regular.ok, true, "ordinary usage still uploads");
const probe = markStopWindowProbe(event);
assert.equal(probe.metadata.stopWindowProbe, true);
for (const field of ["model", "inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens", "costUsd", "costKind"] as const) {
  assert.equal(probe[field], undefined, `${field} must carry no spend input`);
}
const sealed = sealOutboundEnvelope({ event: probe, suppressedFields: [] });
assert.deepEqual(sealed, { ok: false, reason: "privacy" }, "probe must never upload");
console.log(JSON.stringify({ proof: "stop-window-probe", checks: 10, passed: 10, failed: 0 }));
