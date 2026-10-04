import { fixtureEpochId } from "./lib/fixture-epoch-id";
/** A different held validity window must veto anonymous Claude attribution. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import { claudeDispatchSkipStatus, currentDispatchBindingSnapshot, dispatchBindingSchema } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { normalizeForwardedHook } from "../packages/collector-cli/src/forwarder";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { createProofCompletion } from "./lib/proof-completion";

const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
const now = Date.now();
const at = (seconds: number) => new Date(now + seconds * 1000).toISOString();
const session = "r2-window-divergence";
const binding = dispatchBindingSchema.parse({
  sessionId: session, workItemId: "beads:eco-6hoxj.165.97",
  projectKey: `sha256:${"a".repeat(64)}`, companyRef: null,
  attemptId: "11111111-1111-4111-8111-111111111111", parentAttemptId: null,
  acceptedOutcomeId: null, validFrom: at(-120), validUntil: at(120),
  evidenceRef: "dispatch:window-divergence-review",
});
const roots = [
  { rootId: "claude-a", profileId: "claude-a", installationEpochId: fixtureEpochId("epoch-a"),
    source: "claude_code" as const, directory: path.join(home, ".claude-a", "projects"), dispatch: [binding] },
  { rootId: "claude-b", profileId: "claude-b", installationEpochId: fixtureEpochId("epoch-b"),
    source: "claude_code" as const, directory: path.join(home, ".claude-b", "projects"),
    dispatch: [{ ...binding, validUntil: at(-30) }] },
  { rootId: "claude-c", profileId: "claude-c", installationEpochId: fixtureEpochId("epoch-c"),
    source: "claude_code" as const, directory: path.join(home, ".claude-c", "projects"), dispatch: [binding] },
];
for (const root of roots) fs.mkdirSync(root.directory, { recursive: true, mode: 0o700 });
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
const config = collectorConfigSchema.parse({ deviceId: "dev_pr429-window-divergence",
  uploadUrl: "http://127.0.0.1:1/unused", captureRoots: roots });
fs.writeFileSync(path.join(plimsoll, "collector.config.json"), `${JSON.stringify(config)}\n`, { mode: 0o600 });
const snapshot = currentDispatchBindingSnapshot();
const before = claudeDispatchSkipStatus();
const timestamp = at(0);
const hook = normalizeForwardedHook({ id: "window-hook", hook_event_name: "AssistantResponse",
  session_id: session, timestamp }, { config, source: "claude_code", now: () => now,
    dispatchSnapshot: snapshot }).event;
const attr = (key: string, value: string) => ({ key, value: { stringValue: value } });
const nano = String(BigInt(now) * 1_000_000n);
const logs = explodeOtlpPayload({ resourceLogs: [{ resource: {
  attributes: [attr("service.name", "claude-code")] }, scopeLogs: [{ logRecords: [{
    timeUnixNano: nano, attributes: [attr("event.name", "claude_code.api_request"),
      attr("session.id", session), { key: "input_token_count", value: { intValue: "1" } }],
  }] }] }] }, { source: "claude_code" });
const spans = explodeOtlpPayload({ resourceSpans: [{ resource: {
  attributes: [attr("service.name", "claude-code")] }, scopeSpans: [{ spans: [{
    name: "claude_code.api_request", startTimeUnixNano: nano,
    endTimeUnixNano: String(BigInt(now + 1000) * 1_000_000n),
    attributes: [attr("session.id", session),
      { key: "gen_ai.usage.input_tokens", value: { intValue: "1" } }],
  }] }] }] }, { source: "claude_code" });
assert.equal(logs.events.length, 1);
assert.equal(spans.events.length, 1);
const actual = { hook: hook.metadata.workItemId ?? null,
  log: logs.events[0].event.metadata.workItemId ?? null,
  span: spans.events[0].event.metadata.workItemId ?? null,
  skipDelta: claudeDispatchSkipStatus().total - before.total };
console.log(JSON.stringify({ scenario: "three roots; B holds same work/attempt with an earlier validUntil",
  expected: { hook: null, log: null, span: null, skipDelta: 3 }, actual }));
assert.deepEqual(actual, { hook: null, log: null, span: null, skipDelta: 3 });
const proof = createProofCompletion("pr429-r2-window-divergence", 1);
proof.check("stale_copy_vetoes_hook_log_and_span");
proof.complete();
