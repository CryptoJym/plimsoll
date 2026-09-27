import assert from "node:assert/strict";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";

const workId = "beads:eco-6hoxj.163.104";
const runId = "84e0e0a8-44b2-4d53-a92b-b975ea0fef35";
const event = {
  id: "e0b748d1-34e7-47db-8256-d17618073670",
  sessionId: "d9c4ee55-4caa-4fb5-83b7-4b80ca148321",
  source: "codex" as const,
  dataMode: "metadata" as const,
  eventType: "assistant_response" as const,
  observedAt: "2026-09-26T17:00:00.000Z",
  inputTokens: 10,
  outputTokens: 2,
  metadata: { workItemId: workId, attemptId: runId, workEvidenceRef: "dispatch:one", prompt: "private" },
};

const sealed = sealOutboundEnvelope({ event, suppressedFields: ["tool_input"] });
assert.equal(sealed.ok, true);
if (!sealed.ok) throw new Error("missing sealed event");
assert.deepEqual(sealed.envelope.event.metadata.work_ref, {
  schema: "work-ref/v1", work_id: "eco-6hoxj.163.104", run_id: runId,
});
assert.equal("prompt" in sealed.envelope.event.metadata, false);
assert.ok(sealed.envelope.suppressedFields.includes("prompt"));

const unbound = sealOutboundEnvelope({ event: { ...event, metadata: { attemptId: runId } }, suppressedFields: [] });
assert.equal(unbound.ok, true);
if (!unbound.ok) throw new Error("missing unbound event");
assert.equal(unbound.envelope.event.metadata.work_ref, undefined);

const evidence = sealOutboundEnvelope({ event: { ...event, dataMode: "evidence" }, suppressedFields: [] });
assert.deepEqual(evidence, { ok: false, reason: "privacy" });
const conflict = sealOutboundEnvelope({ event: { ...event, metadata: {
  ...event.metadata, workAttributionState: "conflict",
} }, suppressedFields: [] });
assert.equal(conflict.ok, true);
if (!conflict.ok) throw new Error("conflicted event failed sealing");
assert.equal(conflict.envelope.event.metadata.work_ref, undefined);
console.log("work-ref proof: 5 passed");
