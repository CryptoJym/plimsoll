import { fixtureEpochId } from "./lib/fixture-epoch-id";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { appendRootObservation, claudeDispatchSkipStatus, currentDispatchBindingSnapshot, dispatchBindingSchema,
  rootEventMetadata } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema, rollbackCollectorDispatchHistory } from "../packages/collector-cli/src/config";
import { historicalDispatchBindings } from "../packages/collector-cli/src/dispatch-binding-index";
import { bindDispatch, restampDispatch } from "../packages/collector-cli/src/dispatch-command";
import { normalizeForwardedHook } from "../packages/collector-cli/src/forwarder";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";
import { createProofCompletion } from "./lib/proof-completion";

const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
const now = Date.now();
const iso = (offset: number) => new Date(now + offset).toISOString();
const sessionId = "r3-sequential-cli-bind";
const oldWork = "beads:eco-6hoxj.165.96";
const newWork = "beads:eco-6hoxj.165.97";
const roots = Array.from({ length: 3 }, (_, i) => ({ rootId: `claude-${i}`,
  profileId: `profile-${i}`, installationEpochId: fixtureEpochId(`epoch-${i}`), source: "claude_code" as const,
  directory: path.join(home, `.claude-${i}`, "projects") }));
for (const root of roots) fs.mkdirSync(root.directory, { recursive: true });
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
const config = collectorConfigSchema.parse({ deviceId: "dev_pr429-sequential-cli",
  uploadUrl: "http://127.0.0.1:1/unused", captureRoots: roots });
fs.writeFileSync(path.join(plimsoll, "collector.config.json"), `${JSON.stringify(config)}\n`, { mode: 0o600 });
function bind(workItemId: string, attemptId: string, from: string, until: string | null) {
  return bindDispatch(["--session-id", sessionId, "--work-item-id", workItemId,
    "--project-key", `sha256:${"a".repeat(64)}`, "--attempt-id", attemptId,
    "--valid-from", from, ...(until ? ["--valid-until", until] : [])], new Date(now));
}
const old = bind(oldWork, "11111111-1111-4111-8111-111111111111", iso(-120_000), iso(-60_000));
// An active binding remains open; a future window end is no longer accepted.
const next = bind(newWork, "22222222-2222-4222-8222-222222222222", iso(-60_000), null);
const snapshot = currentDispatchBindingSnapshot();
const at = iso(-30_000);
const before = claudeDispatchSkipStatus().conflictingBindings;
const transcript = rootEventMetadata(snapshot.roots[0], "seq-transcript", at, sessionId,
  true, snapshot).workItemId ?? null;
const hook = normalizeForwardedHook({ id: "seq-hook", hook_event_name: "AssistantResponse",
  session_id: sessionId, timestamp: at },
  { config, source: "claude_code", now: () => now, dispatchSnapshot: snapshot }).event.metadata.workItemId ?? null;
const skipDelta = claudeDispatchSkipStatus().conflictingBindings - before;
const buffer = new LocalEventBuffer(path.join(plimsoll, "sequential.sqlite"), {
  workspaceId: config.tenantId, deviceId: config.deviceId,
  enrollmentNow: () => new Date(now - 3_600_000), delivery: { enabled: true },
});
let restamp;
try {
  const id = crypto.randomUUID();
  const root = snapshot.roots[0];
  const event = aiInteractionEventSchema.parse({ id, source: "claude_code",
    eventType: "assistant_response", dataMode: "metadata", observedAt: at, sessionId,
    actionClass: "other", intent: "unknown", inputTokens: 1, outputTokens: 1,
    metadata: { captureRootId: root.rootId, captureProfileId: root.profileId,
      installationEpochId: root.installationEpochId } });
  assert.equal(appendRootObservation(buffer, event, root), true);
  restamp = restampDispatch(["--attempt-id", "22222222-2222-4222-8222-222222222222"],
    buffer, snapshot.roots);
} finally { buffer.close(); }
const archived = historicalDispatchBindings(snapshot.roots, { source: "claude_code", sessionId }, dispatchBindingSchema.parse);
const copies = snapshot.roots.map(root => [...(root.dispatch ?? []),
  ...archived.filter(row => row.root.rootId === root.rootId).map(row => row.binding)]
  .sort((a, b) => a.validFrom.localeCompare(b.validFrom)).map(binding => ({
  workItemId: binding.workItemId, attemptId: binding.attemptId,
  validFrom: binding.validFrom, validUntil: binding.validUntil })));
const actual = { oldBindRoots: old.roots, newBindRoots: next.roots, transcript, hook,
  restamped: restamp.restamped, restampSkipped: restamp.skipped, skipDelta, copies };
console.log(JSON.stringify(actual));
assert.equal(old.roots, 3);
assert.equal(next.roots, 3);
assert.equal(copies.every(copy => copy.length === 2), true);
assert.equal(copies.every(copy => JSON.stringify(copy) === JSON.stringify(copies[0])), true);
assert.equal(transcript, newWork, "known-root transcript lost the only active binding");
assert.equal(hook, newWork, "rootless hook lost the only active binding");
assert.equal(restamp.restamped, 1, "known-root row lost the only active binding during restamp");
// Seed the older overlap fixture through the production lossless rollback.
// Immutable history is never dropped or rewritten to create a conflict.
rollbackCollectorDispatchHistory();
const overlapRoots = currentDispatchBindingSnapshot().roots.map(root => ({ ...root, dispatch: root.dispatch?.map(binding =>
  binding.workItemId === oldWork ? { ...binding, validUntil: iso(-59_000) } : binding) }));
const overlapConfig = collectorConfigSchema.parse({ ...config, captureRoots: overlapRoots });
fs.writeFileSync(path.join(plimsoll, "collector.config.json"), `${JSON.stringify(overlapConfig)}\n`, { mode: 0o600 });
const overlapSnapshot = currentDispatchBindingSnapshot();
const beforeOverlap = claudeDispatchSkipStatus().conflictingBindings;
const overlapHook = normalizeForwardedHook({ id: "seq-overlap", hook_event_name: "AssistantResponse",
  session_id: sessionId, timestamp: at },
  { config: overlapConfig, source: "claude_code", now: () => now,
    dispatchSnapshot: overlapSnapshot }).event;
const overlap = { workItemId: overlapHook.metadata.workItemId ?? null,
  conflictingBindingsDelta: claudeDispatchSkipStatus().conflictingBindings - beforeOverlap };
console.log(JSON.stringify({ overlap }));
assert.deepEqual(overlap, { workItemId: null, conflictingBindingsDelta: 1 });
const proof = createProofCompletion("pr429-r3-sequential-bind-cli", 2);
proof.check("sequential_cli_binds_transcript_hook_and_restamp");
proof.check("overlapping_prior_attempt_vetoes_and_counts_even_after_it_ends");
proof.complete();
