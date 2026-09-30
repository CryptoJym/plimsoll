/** Reviewer privacy challenge for bound Claude hook and OTLP upload envelopes. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { dispatchBindingSchema } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { normalizeForwardedHook } from "../packages/collector-cli/src/forwarder";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";

const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
const now = Date.now();
const observedAt = new Date(now - 60_000).toISOString();
const sessionId = "22222222-2222-4222-8222-222222222222";
const binding = dispatchBindingSchema.parse({ sessionId,
  workItemId: "beads:eco-6hoxj.165.97", projectKey: `sha256:${"a".repeat(64)}`,
  companyRef: null, attemptId: "11111111-1111-4111-8111-111111111111",
  parentAttemptId: null, acceptedOutcomeId: null,
  validFrom: new Date(now - 120_000).toISOString(), validUntil: null,
  evidenceRef: "dispatch:synthetic-privacy" });
const root = { rootId: "claude-a", profileId: "claude-a", installationEpochId: "epoch-a",
  source: "claude_code" as const, directory: path.join(home, ".claude", "projects"), dispatch: [binding] };
fs.mkdirSync(root.directory, { recursive: true, mode: 0o700 });
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
const config = collectorConfigSchema.parse({ deviceId: "dev_pr429-privacy",
  uploadUrl: "http://127.0.0.1:1/unused", captureRoots: [root] });
fs.writeFileSync(path.join(plimsoll, "collector.config.json"), JSON.stringify(config) + "\n");
const buffer = new LocalEventBuffer(path.join(plimsoll, "review-ledger.sqlite"), {
  workspaceId: config.tenantId, deviceId: config.deviceId,
  enrollmentNow: () => new Date(now - 3_600_000), delivery: { enabled: true },
});
const privateValues = ["REVIEW_SECRET_PROMPT", "REVIEW_SECRET_TITLE",
  "REVIEW_SECRET_PATH", "REVIEW_SECRET_BRANCH"];
const attr = (key: string, value: string) => ({ key, value: { stringValue: value } });
const payload = { resourceLogs: [{ resource: { attributes: [attr("service.name", "claude-code")] },
  scopeLogs: [{ logRecords: [{ timeUnixNano: String(Date.parse(observedAt) * 1_000_000),
    attributes: [attr("event.name", "claude_code.api_request"), attr("session.id", sessionId),
      { key: "input_token_count", value: { intValue: "1" } },
      attr("prompt", privateValues[0]), attr("title", privateValues[1]),
      attr("path", `/tmp/${privateValues[2]}`), attr("branch", privateValues[3])] }] }] }],
  resourceSpans: [{ resource: { attributes: [attr("service.name", "claude-code")] },
    scopeSpans: [{ spans: [{ name: "claude_code.api_request",
      startTimeUnixNano: String(Date.parse(observedAt) * 1_000_000),
      endTimeUnixNano: String((Date.parse(observedAt) + 1_000) * 1_000_000),
      attributes: [attr("session.id", sessionId),
        { key: "gen_ai.usage.input_tokens", value: { intValue: "1" } },
        attr("prompt", privateValues[0]), attr("title", privateValues[1]),
        attr("path", `/tmp/${privateValues[2]}`), attr("branch", privateValues[3])] }] }] }] };
try {
  const hook = normalizeForwardedHook({ id: crypto.randomUUID(), session_id: sessionId,
    hook_event_name: "UserPromptSubmit", timestamp: observedAt,
    prompt: privateValues[0], title: privateValues[1],
    transcript_path: `/tmp/${privateValues[2]}/session.jsonl`, branch: privateValues[3] },
  { config, source: "claude_code", now: () => now });
  assert.equal(buffer.append(hook.event, hook.suppressedFields), true);
  const otlp = explodeOtlpPayload(payload, { source: "claude_code" });
  assert.equal(otlp.events.length, 2);
  for (const entry of otlp.events) assert.equal(buffer.append(entry.event, entry.suppressedFields), true);
  for (const id of [hook.event.id, ...otlp.events.map(entry => entry.event.id)]) {
    const raw = buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
      .get(id) as { payload: string };
    const queued = buffer.database.prepare("select base_envelope_json as envelope from upload_outbox where raw_id=?")
      .get(id) as { envelope: string };
    for (const value of privateValues) {
      assert.equal(raw.payload.includes(value), false, `raw leak ${value}`);
      assert.equal(queued.envelope.includes(value), false, `outbox leak ${value}`);
    }
    assert.deepEqual(JSON.parse(queued.envelope).event.metadata.work_ref, {
      schema: "work-ref/v1", work_id: "eco-6hoxj.165.97", run_id: binding.attemptId });
  }
  console.log(JSON.stringify({ tested: ["Claude hook", "Claude OTLP log", "Claude OTLP span"],
    localAndQueuedPrivateValues: "absent", workRef: "present" }));
} finally {
  buffer.close();
}
