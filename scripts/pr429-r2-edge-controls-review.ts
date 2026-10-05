import { fixtureEpochId } from "./lib/fixture-epoch-id";
/** Focused F1' controls for missing copies, sightings, torn config and bind races. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import { claudeBindingForUnrootedEvent, claudeDispatchSkipStatus,
  currentDispatchBindingSnapshot, dispatchBindingSchema, observeClaudeRootSession } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { bindDispatch } from "../packages/collector-cli/src/dispatch-command";
import { normalizeForwardedHook } from "../packages/collector-cli/src/forwarder";
import { createProofCompletion } from "./lib/proof-completion";

const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
const file = path.join(plimsoll, "collector.config.json");
const now = Date.now();
const at = (seconds: number) => new Date(now + seconds * 1000).toISOString();
const work = "beads:eco-6hoxj.165.97";
const attempt = "11111111-1111-4111-8111-111111111111";
const binding = (sessionId: string) => dispatchBindingSchema.parse({ sessionId,
  workItemId: work, projectKey: `sha256:${"a".repeat(64)}`, companyRef: null,
  attemptId: attempt, parentAttemptId: null, acceptedOutcomeId: null,
  validFrom: at(-120), validUntil: at(120), evidenceRef: "dispatch:edge-controls-review" });
const bareRoots = ["a", "b", "c"].map(letter => ({
  rootId: `claude-${letter}`, profileId: `claude-${letter}`,
  installationEpochId: fixtureEpochId(`epoch-${letter}`), source: "claude_code" as const,
  directory: path.join(home, `.claude-${letter}`, "projects"),
}));
for (const root of bareRoots) fs.mkdirSync(root.directory, { recursive: true, mode: 0o700 });
const publish = (roots: Array<typeof bareRoots[number] & { dispatch?: ReturnType<typeof binding>[] }>) => {
  const config = collectorConfigSchema.parse({ deviceId: "dev_pr429-edge-controls",
    uploadUrl: "http://127.0.0.1:1/unused", captureRoots: roots });
  const next = `${file}.next`;
  fs.writeFileSync(next, `${JSON.stringify(config)}\n`, { mode: 0o600 });
  fs.renameSync(next, file);
  return config;
};
const resolve = (session: string, snapshot = currentDispatchBindingSnapshot()) =>
  claudeBindingForUnrootedEvent(session, at(0), snapshot)?.workItemId ?? null;
const results: Record<string, unknown> = {};

const fanout = "r2-edge-fanout";
publish(bareRoots.map(root => ({ ...root, dispatch: [binding(fanout)] })));
assert.equal(resolve(fanout), work);
results.identicalFanout = "stamped";

const changedWindow = "r2-edge-active-window";
publish(bareRoots.map((root, index) => ({ ...root, dispatch: [{ ...binding(changedWindow),
  ...(index === 1 ? { validFrom: at(-180) } : {}) }] })));
const conflictsBefore = claudeDispatchSkipStatus().conflictingBindings;
assert.equal(resolve(changedWindow), null);
assert.equal(claudeDispatchSkipStatus().conflictingBindings, conflictsBefore + 1);
results.differentActiveWindows = "vetoed_and_counted";

const missing = "r2-edge-missing";
publish(bareRoots.map((root, index) => ({ ...root,
  dispatch: index === 1 ? [] : [binding(missing)] })));
assert.equal(resolve(missing), work);
observeClaudeRootSession(bareRoots[1], missing);
const sightingsBefore = claudeDispatchSkipStatus().otherRootSeen;
assert.equal(resolve(missing), null);
assert.equal(claudeDispatchSkipStatus().otherRootSeen, sightingsBefore + 1);
results.missingCopy = "allowed_until_other_root_sighting_then_vetoed_and_counted";

const stale = "r2-edge-stale-seen";
publish(bareRoots.map((root, index) => ({ ...root,
  dispatch: [{ ...binding(stale), ...(index === 1 ? { validUntil: at(-30) } : {}) }] })));
observeClaudeRootSession(bareRoots[1], stale);
assert.equal(resolve(stale), null);
results.staleCopyWithSighting = "vetoed";

const torn = "r2-edge-torn";
publish(bareRoots.map(root => ({ ...root, dispatch: [binding(torn)] })));
assert.equal(resolve(torn), work);
fs.writeFileSync(file, '{"captureRoots":[');
assert.equal(currentDispatchBindingSnapshot().roots.length, 0);
assert.equal(resolve(torn), null);
publish(bareRoots.map(root => ({ ...root, dispatch: [binding(torn)] })));
assert.equal(resolve(torn), work);
results.tornConfig = "fail_closed_then_recovered";

const raced = "r2-edge-bind-race";
const oldConfig = publish(bareRoots.map(root => ({ ...root, dispatch: [] })));
const oldSnapshot = currentDispatchBindingSnapshot();
const hookPayload = { id: "r2-edge-before-bind", hook_event_name: "AssistantResponse",
  session_id: raced, timestamp: at(0) };
const hook = (snapshot: typeof oldSnapshot) => normalizeForwardedHook(hookPayload,
  { config: oldConfig, source: "claude_code", now: () => now,
    dispatchSnapshot: snapshot }).event.metadata.workItemId ?? null;
assert.equal(hook(oldSnapshot), null);
bindDispatch(["--session-id", raced, "--work-item-id", work,
  "--project-key", `sha256:${"a".repeat(64)}`, "--attempt-id", attempt,
  "--valid-from", at(-60)], new Date(now));
const newSnapshot = currentDispatchBindingSnapshot();
assert.notStrictEqual(newSnapshot, oldSnapshot);
assert.equal(hook(oldSnapshot), null);
assert.equal(hook(newSnapshot), work);
results.bindBetweenBatches = "old_batch_unstamped_new_batch_stamped";
console.log(JSON.stringify(results));
const proof = createProofCompletion("pr429-r2-edge-controls", 1);
proof.check("fanout_sightings_config_refresh_and_bind_race");
proof.complete();
