import { fixtureEpochId } from "./lib/fixture-epoch-id";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { appendRootObservation, claudeDispatchSkipStatus, dispatchBindingSchema,
  durableClaudeRootSessionSightings, observeClaudeRootSession } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { normalizeForwardedHook } from "../packages/collector-cli/src/forwarder";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";
import { createProofCompletion } from "./lib/proof-completion";

const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
const now = Date.now();
const observedAt = new Date(now - 60_000).toISOString();
const makeBinding = (sessionId: string) => dispatchBindingSchema.parse({ sessionId,
  workItemId: "beads:eco-6hoxj.165.97", projectKey: `sha256:${"a".repeat(64)}`,
  companyRef: null, attemptId: "11111111-1111-4111-8111-111111111111",
  parentAttemptId: null, acceptedOutcomeId: null,
  validFrom: new Date(now - 120_000).toISOString(), validUntil: null,
  evidenceRef: "dispatch:r3-atomicity" });
const rootA = { rootId: "claude-a", profileId: "profile-a", installationEpochId: fixtureEpochId("epoch-a"),
  source: "claude_code" as const, directory: path.join(home, ".claude-a", "projects"), dispatch: [] as ReturnType<typeof makeBinding>[] };
const rootB = { rootId: "claude-b", profileId: "profile-b", installationEpochId: fixtureEpochId("epoch-b"),
  source: "claude_code" as const, directory: path.join(home, ".claude-b", "projects") };
for (const root of [rootA, rootB]) fs.mkdirSync(root.directory, { recursive: true, mode: 0o700 });
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
const baseConfig = collectorConfigSchema.parse({ deviceId: "dev_pr429-r3-atomicity",
  uploadUrl: "http://127.0.0.1:1/unused", captureRoots: [rootA, rootB] });
const options = { workspaceId: baseConfig.tenantId, deviceId: baseConfig.deviceId,
  enrollmentNow: () => new Date(now - 3_600_000), delivery: { enabled: true } };
const newEvent = (sessionId: string, id: string) => aiInteractionEventSchema.parse({
  id, source: "claude_code", eventType: "session_start", dataMode: "metadata",
  observedAt, sessionId, metadata: { captureRootId: rootB.rootId, captureProfileId: rootB.profileId },
});
const counts = (buffer: LocalEventBuffer, id: string, sessionId: string) => ({
  raw: Boolean(buffer.database.prepare("select 1 from buffered_events where id=?").get(id)),
  observations: (buffer.database.prepare("select count(*) as n from capture_root_observations where event_id=?")
    .get(id) as { n: number }).n,
  sightings: durableClaudeRootSessionSightings(buffer.database, sessionId).size,
});
function hook(buffer: LocalEventBuffer, sessionId: string) {
  const config = collectorConfigSchema.parse({ deviceId: options.deviceId,
    uploadUrl: "http://127.0.0.1:1/unused", captureRoots: [{ ...rootA, dispatch: [makeBinding(sessionId)] }, rootB] });
  const configFile = path.join(plimsoll, "collector.config.json");
  fs.writeFileSync(`${configFile}.next`, `${JSON.stringify(config)}\n`, { mode: 0o600 });
  fs.renameSync(`${configFile}.next`, configFile);
  const before = claudeDispatchSkipStatus().otherRootSeen;
  const event = normalizeForwardedHook({ id: crypto.randomUUID(), hook_event_name: "AssistantResponse",
    session_id: sessionId, timestamp: observedAt },
    { config, buffer, source: "claude_code", now: () => now }).event;
  return { workItemId: event.metadata.workItemId ?? null,
    otherRootSeenDelta: claudeDispatchSkipStatus().otherRootSeen - before };
}
function caughtFailure(label: string, failOn: string) {
  const sessionId = `r3-${label}`;
  const id = crypto.randomUUID();
  const file = path.join(plimsoll, `${label}.sqlite`);
  const buffer = new LocalEventBuffer(file, options);
  const database = buffer.database;
  // Prime the local sighting cache, then force a single failure inside the
  // transaction. The fallback write must invalidate the cache for a reopen.
  assert.equal(durableClaudeRootSessionSightings(database, sessionId).size, 0);
  const originalPrepare = database.prepare.bind(database);
  let injected = false;
  database.prepare = ((sql: string) => {
    if (!injected && sql.trimStart().startsWith(failOn)) {
      injected = true;
      throw new Error(`injected_${label}`);
    }
    return originalPrepare(sql);
  }) as typeof database.prepare;
  try { assert.throws(() => appendRootObservation(buffer, newEvent(sessionId, id), rootB),
    new RegExp(`injected_${label}`)); }
  finally { database.prepare = originalPrepare; buffer.close(); }
  assert.equal(injected, true);
  const reopened = new LocalEventBuffer(file, options);
  try {
    const actual = { ...counts(reopened, id, sessionId), ...hook(reopened, sessionId) };
    console.log(JSON.stringify({ label, actual }));
    assert.deepEqual(actual, { raw: false, observations: 0, sightings: 1,
      workItemId: null, otherRootSeenDelta: 1 });
  } finally { reopened.close(); }
}

if (process.argv[2] === "crash-child") {
  const file = process.env.PR429_CRASH_FILE!;
  const sessionId = process.env.PR429_CRASH_SESSION!;
  const id = process.env.PR429_CRASH_ID!;
  const buffer = new LocalEventBuffer(file, options);
  const database = buffer.database;
  // The real transcript tailer records this in memory before calling append.
  // SIGKILL then removes the process-local sighting.
  observeClaudeRootSession(rootB, sessionId);
  const originalPrepare = database.prepare.bind(database);
  database.prepare = ((sql: string) => {
    if (sql.startsWith("insert into capture_root_observations values")) {
      process.stdout.write("SIGKILL_AFTER_RAW_BEFORE_RECEIPT\n");
      process.kill(process.pid, "SIGKILL");
    }
    return originalPrepare(sql);
  }) as typeof database.prepare;
  appendRootObservation(buffer, newEvent(sessionId, id), rootB);
  process.exit(98);
} else {
  caughtFailure("receipt-prepare", "insert into capture_root_observations values");
  caughtFailure("sighting-prepare", "insert into capture_root_session_sightings");

  const sessionId = "r3-hard-crash";
  const id = crypto.randomUUID();
  const file = path.join(plimsoll, "hard-crash.sqlite");
  new LocalEventBuffer(file, options).close();
  const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
  const child = spawnSync(process.execPath, ["--import", loader, path.resolve(process.argv[1]), "crash-child"], {
    cwd: process.cwd(), encoding: "utf8", timeout: 30_000,
    env: { ...process.env, PR429_CRASH_FILE: file, PR429_CRASH_SESSION: sessionId, PR429_CRASH_ID: id },
  });
  assert.equal(child.signal, "SIGKILL", `child exited unexpectedly: ${child.status}, ${child.stderr}`);
  assert.match(child.stdout, /SIGKILL_AFTER_RAW_BEFORE_RECEIPT/);
  const reopened = new LocalEventBuffer(file, options);
  try {
    const actual = { ...counts(reopened, id, sessionId), ...hook(reopened, sessionId) };
    const config = collectorConfigSchema.parse({ deviceId: options.deviceId,
      uploadUrl: "http://127.0.0.1:1/unused",
      captureRoots: [{ ...rootA, dispatch: [makeBinding(sessionId)] }, rootB] });
    const hookId = crypto.randomUUID();
    const persistedHook = normalizeForwardedHook({ id: hookId, hook_event_name: "AssistantResponse",
      session_id: sessionId, timestamp: observedAt },
      { config, buffer: reopened, source: "claude_code", now: () => now }).event;
    assert.equal(reopened.append(persistedHook, []), true);
    assert.equal(appendRootObservation(reopened, newEvent(sessionId, id), rootB), true);
    const saved = reopened.database.prepare("select payload_json as payload from buffered_events where id=?")
      .get(hookId) as { payload: string };
    const postReplayHookWork = JSON.parse(saved.payload).metadata.workItemId ?? null;
    console.log(JSON.stringify({ label: "hard_crash_between_raw_and_receipt", actual,
      childSignal: child.signal, postReplayHookWork }));
    assert.equal(actual.raw, false, "a raw row escaped the interrupted transaction");
    assert.equal(actual.observations, 0);
    assert.equal(actual.workItemId, null, "B was seen before SIGKILL; A's work was stamped before replay");
    assert.equal(actual.otherRootSeenDelta, 1, "status omitted the lost B sighting");
    assert.equal(postReplayHookWork, null, "replay cannot correct an already committed work item");
  } finally { reopened.close(); }
  const proof = createProofCompletion("pr429-r3-atomicity-crash", 3);
  proof.check("caught_receipt_failure_vetoes_after_reopen");
  proof.check("caught_sighting_failure_vetoes_after_reopen");
  proof.check("sigkill_vetoes_before_and_after_replay");
  proof.complete();
}
