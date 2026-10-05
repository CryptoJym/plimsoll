import { fixtureEpochId } from "./lib/fixture-epoch-id";
/** A committed B event must remain a rootless veto after a receipt write fails. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { appendRootObservation, claudeDispatchSkipStatus, dispatchBindingSchema,
  durableClaudeRootSessionSightings } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { normalizeForwardedHook } from "../packages/collector-cli/src/forwarder";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";
import { createProofCompletion } from "./lib/proof-completion";

const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
const now = Date.now();
const observedAt = new Date(now - 60_000).toISOString();
const sessionId = "r2-sighting-atomicity";
const binding = dispatchBindingSchema.parse({ sessionId,
  workItemId: "beads:eco-6hoxj.165.97", projectKey: `sha256:${"a".repeat(64)}`,
  companyRef: null, attemptId: "11111111-1111-4111-8111-111111111111",
  parentAttemptId: null, acceptedOutcomeId: null,
  validFrom: new Date(now - 120_000).toISOString(), validUntil: null,
  evidenceRef: "dispatch:sighting-atomicity-review" });
const roots = [
  { rootId: "claude-a", profileId: "claude-a", installationEpochId: fixtureEpochId("epoch-a"),
    source: "claude_code" as const, directory: path.join(home, ".claude-a", "projects"), dispatch: [binding] },
  { rootId: "claude-b", profileId: "claude-b", installationEpochId: fixtureEpochId("epoch-b"),
    source: "claude_code" as const, directory: path.join(home, ".claude-b", "projects") },
];
for (const root of roots) fs.mkdirSync(root.directory, { recursive: true, mode: 0o700 });
const config = collectorConfigSchema.parse({ deviceId: "dev_pr429-sighting-atomicity",
  uploadUrl: "http://127.0.0.1:1/unused", captureRoots: roots });
fs.writeFileSync(path.join(plimsoll, "collector.config.json"), `${JSON.stringify(config)}\n`, { mode: 0o600 });
const file = path.join(plimsoll, "sighting-atomicity.sqlite");
const options = { workspaceId: config.tenantId, deviceId: config.deviceId,
  enrollmentNow: () => new Date(now - 3_600_000), delivery: { enabled: true } };
const eventId = crypto.randomUUID();
const buffer = new LocalEventBuffer(file, options);
const event = aiInteractionEventSchema.parse({ id: eventId, source: "claude_code",
  eventType: "session_start", dataMode: "metadata", observedAt, sessionId,
  metadata: { captureRootId: roots[1].rootId, captureProfileId: roots[1].profileId } });
const database = buffer.database;
const originalPrepare = database.prepare.bind(database);
let injected = false;
database.prepare = ((sql: string) => {
  if (sql.startsWith("insert into capture_root_observations values")) {
    injected = true;
    throw new Error("synthetic_crash_after_raw_append");
  }
  return originalPrepare(sql);
}) as typeof database.prepare;
try {
  assert.throws(() => appendRootObservation(buffer, event, roots[1]),
    /synthetic_crash_after_raw_append/);
} finally {
  database.prepare = originalPrepare;
  buffer.close();
}
assert.equal(injected, true);
const reopened = new LocalEventBuffer(file, options);
try {
  const raw = reopened.database.prepare("select id from buffered_events where id=?").get(eventId);
  assert.equal(Boolean(raw), false, "raw B event must roll back with its receipts");
  const observations = reopened.database.prepare(
    "select count(*) as n from capture_root_observations where event_id=?"
  ).get(eventId) as { n: number };
  const durable = durableClaudeRootSessionSightings(reopened.database, sessionId);
  const before = claudeDispatchSkipStatus().otherRootSeen;
  const hook = normalizeForwardedHook({ id: crypto.randomUUID(),
    hook_event_name: "AssistantResponse", session_id: sessionId, timestamp: observedAt },
    { config, source: "claude_code", now: () => now, buffer: reopened }).event;
  const actual = { rawExists: Boolean(raw), observationCount: observations.n,
    durableSightings: durable.size, hookWorkItemId: hook.metadata.workItemId ?? null,
    otherRootSeenDelta: claudeDispatchSkipStatus().otherRootSeen - before };
  console.log(JSON.stringify({ scenario: "crash after B raw append before root receipts",
    expected: "B raw rolls back, or subsequent rootless intake refuses A and counts the veto", actual }));
  assert.equal(actual.observationCount, 0);
  assert.equal(actual.durableSightings, 1, "failed B intake must leave a durable veto");
  assert.equal(actual.hookWorkItemId, null, "failed B intake lost its attribution veto");
  assert.ok(actual.otherRootSeenDelta >= 1, "status omitted the attribution veto");
} finally {
  reopened.close();
}
const proof = createProofCompletion("pr429-r2-sighting-atomicity", 1);
proof.check("raw_receipt_transaction_and_restart_veto");
proof.complete();
