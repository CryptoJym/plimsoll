import { fixtureEpochId } from "./lib/fixture-epoch-id";
/** Reviewer regression: a later Claude root must not inherit another root's work. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { currentDispatchCaptureRoots, dispatchBindingSchema, rootEventMetadata } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { restampDispatch } from "../packages/collector-cli/src/dispatch-command";
import { normalizeForwardedHook } from "../packages/collector-cli/src/forwarder";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";

const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
const sessionId = "22222222-2222-4222-8222-222222222222";
const attemptId = "11111111-1111-4111-8111-111111111111";
const observedAt = new Date(Date.now() - 60_000).toISOString();
const binding = dispatchBindingSchema.parse({
  sessionId, workItemId: "beads:eco-6hoxj.165.97", projectKey: `sha256:${"a".repeat(64)}`,
  companyRef: null, attemptId, parentAttemptId: null, acceptedOutcomeId: null,
  validFrom: new Date(Date.now() - 120_000).toISOString(),
  validUntil: new Date(Date.now() + 120_000).toISOString(), evidenceRef: "dispatch:synthetic-review",
});
const roots = [
  { rootId: "claude-a", profileId: "claude-a", installationEpochId: fixtureEpochId("epoch-a"),
    source: "claude_code" as const, directory: path.join(home, ".claude", "projects"), dispatch: [binding] },
  { rootId: "claude-b", profileId: "claude-b", installationEpochId: fixtureEpochId("epoch-b"),
    source: "claude_code" as const, directory: path.join(home, ".claude-seats", "seat-b", "projects") },
];
for (const root of roots) fs.mkdirSync(root.directory, { recursive: true, mode: 0o700 });
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
const config = collectorConfigSchema.parse({ deviceId: "dev_pr429-review", uploadUrl: "http://127.0.0.1:1/unused",
  captureRoots: roots });
fs.writeFileSync(path.join(plimsoll, "collector.config.json"), JSON.stringify(config) + "\n", { mode: 0o600 });

const transcriptMetadata = rootEventMetadata(roots[1], "transcript-from-b", observedAt, sessionId);
// Hook and OTLP intake have only a source-wide credential, no authenticated
// root identity. They cannot tell B's same-ID session from A's bind.
const hook = normalizeForwardedHook({ id: crypto.randomUUID(), session_id: sessionId,
  hook_event_name: "AssistantResponse", timestamp: observedAt },
  { config, source: "claude_code", now: () => Date.now() }).event;
const attr = (key: string, value: string) => ({ key, value: { stringValue: value } });
const otlp = explodeOtlpPayload({ resourceLogs: [{ resource: { attributes: [attr("service.name", "claude-code")] },
  scopeLogs: [{ logRecords: [{ timeUnixNano: String(Date.parse(observedAt) * 1_000_000),
    attributes: [attr("event.name", "claude_code.api_request"), attr("session.id", sessionId),
      { key: "input_token_count", value: { intValue: "1" } }] }] }] }] },
  { source: "claude_code" }).events[0]?.event;
assert.ok(otlp);
const buffer = new LocalEventBuffer(path.join(plimsoll, "review-ledger.sqlite"), {
  workspaceId: config.tenantId, deviceId: config.deviceId,
  enrollmentNow: () => new Date(Date.now() - 3_600_000), delivery: { enabled: true },
});
const rawId = crypto.randomUUID();
try {
  assert.equal(buffer.append({ id: rawId, source: "claude_code", eventType: "assistant_response",
    dataMode: "metadata", observedAt, sessionId, actionClass: "other", intent: "unknown",
    inputTokens: 1, outputTokens: 1, metadata: { captureRootId: "claude-b" } }, []), true);
  const restamp = restampDispatch(["--attempt-id", attemptId], buffer, currentDispatchCaptureRoots());
  const raw = buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
    .get(rawId) as { payload: string };
  const outbox = buffer.database.prepare("select base_envelope_json as envelope from upload_outbox where raw_id=?")
    .get(rawId) as { envelope: string };
  const restampedEvent = JSON.parse(raw.payload);
  const wireEvent = JSON.parse(outbox.envelope).event;
  console.log(JSON.stringify({
    scenario: "new Claude root B added after A was bound; B independently has the same session ID",
    transcript: { captureRootId: transcriptMetadata.captureRootId,
      workItemId: transcriptMetadata.workItemId ?? null },
    hook: { workItemId: hook.metadata.workItemId ?? null },
    otlp: { workItemId: otlp.metadata.workItemId ?? null },
    restamp: { count: restamp.restamped, captureRootId: restampedEvent.metadata.captureRootId,
      workItemId: restampedEvent.metadata.workItemId ?? null,
      wireWorkRef: wireEvent.metadata.work_ref ?? null },
  }));
  assert.equal(transcriptMetadata.workItemId, undefined, "root B transcript inherited A's work");
  assert.equal(hook.metadata.workItemId, undefined, "root B hook inherited A's work");
  assert.equal(otlp.metadata.workItemId, undefined, "root B OTLP inherited A's work");
  assert.equal(restamp.restamped, 0, "restamp changed a row proven to belong to root B");
  assert.equal(restampedEvent.metadata.workItemId, undefined);
  assert.equal(wireEvent.metadata.work_ref, undefined);
} finally {
  buffer.close();
}
