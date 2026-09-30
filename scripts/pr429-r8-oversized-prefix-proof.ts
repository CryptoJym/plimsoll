import { fixtureEpochId } from "./lib/fixture-epoch-id";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { startClaudeReplayBarrier } from "../packages/collector-cli/src/claude-replay-barrier";
import { dispatchBindingSchema, durableClaudeRootSessionSightings, rootCursorKey } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { createProofCompletion } from "./lib/proof-completion";

const scenario = process.argv[2] ?? "unchanged";
assert.ok(["unchanged", "last-64k", "two-appends"].includes(scenario));
const now = Date.now();
const sessionA = crypto.randomUUID();
const sessionC = crypto.randomUUID();
const A = {rootId: "claude-a", profileId: "profile-a", installationEpochId: fixtureEpochId("epoch-a"),
  source: "claude_code" as const, directory: path.join(process.env.HOME!, "claude-a/projects")};
const B = {rootId: "claude-b", profileId: "profile-b", installationEpochId: fixtureEpochId("epoch-b"),
  source: "claude_code" as const, directory: path.join(process.env.HOME!, "claude-b/projects")};
const file = path.join(B.directory, "project/startup.jsonl");
const record = (sessionId: string, id: string, padding = "") => JSON.stringify({
  type: "assistant", sessionId, timestamp: new Date(now - 1_000).toISOString(),
  message: {id, model: "claude-sonnet-4-20250514", content: [],
    usage: {input_tokens: 1, output_tokens: 1, cache_read_input_tokens: 0,
      cache_creation_input_tokens: 0}}, padding,
}) + "\n";
const edge = JSON.stringify({type: "progress", padding: "x".repeat(70_000)}) + "\n";
const original = scenario === "last-64k"
  ? edge + edge + record(sessionC, "message-old")
  : edge + record(sessionC, "message-old") + edge;
const replacement = scenario === "unchanged" ? original :
  original.replace(sessionC, sessionA);
const append = (id: string) => record(sessionC, id, "z".repeat(150_000));
assert.equal(Buffer.byteLength(replacement), Buffer.byteLength(original));
assert.equal(replacement.slice(0, 64 * 1024), original.slice(0, 64 * 1024));
if (scenario === "two-appends") {
  assert.equal(replacement.slice(-64 * 1024), original.slice(-64 * 1024));
} else if (scenario === "last-64k") {
  assert.notEqual(replacement.slice(-64 * 1024), original.slice(-64 * 1024));
}
fs.mkdirSync(A.directory, {recursive: true});
fs.mkdirSync(path.dirname(file), {recursive: true});
fs.mkdirSync(process.env.PLIMSOLL_HOME!, {recursive: true});
fs.writeFileSync(file, original);
const binding = dispatchBindingSchema.parse({sessionId: sessionA,
  workItemId: "beads:eco-6hoxj.165.97", projectKey: `sha256:${"a".repeat(64)}`,
  companyRef: null, attemptId: crypto.randomUUID(), parentAttemptId: null,
  acceptedOutcomeId: null, validFrom: new Date(now - 60_000).toISOString(),
  validUntil: new Date(now + 60_000).toISOString(),
  evidenceRef: "proof:r8-oversized-prefix"});
const config = collectorConfigSchema.parse({deviceId: `dev-r8-${scenario}`,
  uploadUrl: "http://127.0.0.1:1/unused", captureRoots: [{...A, dispatch: [binding]}, B]});
fs.writeFileSync(path.join(process.env.PLIMSOLL_HOME!, "collector.config.json"),
  JSON.stringify(config) + "\n");
const dbPath = path.join(process.env.PLIMSOLL_HOME!, "r8-oversized.sqlite");
const options = {workspaceId: config.tenantId, deviceId: config.deviceId,
  enrollmentNow: () => new Date(now - 3_600_000), delivery: {enabled: true},
  databaseBusyTimeoutMs: 0};
let buffer = new LocalEventBuffer(dbPath, options);
const key = jsonlScanStateKey(rootCursorKey(config.captureRoots ?? [], file));
const cursor = () => buffer.database.prepare(`select committed_offset as offset,
  committed_prefix_hash as digest from rollout_scan_state where file=?`).get(key) as
  {offset: number | null; digest: string | null} | undefined;
const scan = async (steps: number) => {
  const tailer = new TranscriptTailer(buffer, A.directory, undefined, config.captureRoots ?? []);
  const scans: unknown[] = [];
  try {
    for (let i = 0; i < steps; i++) {
      const result = await tailer.scan({scope: "full"});
      scans.push({records: result.recordsCommitted, slices: result.slicesCommitted,
        offset: cursor()?.offset, errors: result.parseErrors});
      assert.equal(result.parseErrors, 0);
      if (result.slicesCommitted === 0) break;
    }
  } finally { tailer.close(); }
  return scans;
};

async function main() {
  try {
    await scan(4);
    const oldCursor = cursor();
    assert.equal(oldCursor?.offset, Buffer.byteLength(original));
    assert.equal(oldCursor.digest, crypto.createHash("sha256").update(original).digest("hex"));
    assert.equal(durableClaudeRootSessionSightings(buffer.database, sessionC).size, 1);
    buffer.close();
    const oldStat = fs.statSync(file, {bigint: true});
    fs.writeFileSync(file, replacement);
    fs.appendFileSync(file, append("message-new-1"));
    const newStat = fs.statSync(file, {bigint: true});
    assert.equal(newStat.ino, oldStat.ino);
    assert.equal(newStat.birthtimeNs, oldStat.birthtimeNs);
    buffer = new LocalEventBuffer(dbPath, options);
    const firstScans = await scan(12);
    const firstOffset = cursor()?.offset;
    let secondScans: unknown[] = [];
    if (scenario === "two-appends") {
      assert.ok(firstOffset === null || firstOffset === undefined ||
        firstOffset <= oldCursor.offset!);
      buffer.close();
      fs.appendFileSync(file, append("message-new-2"));
      buffer = new LocalEventBuffer(dbPath, options);
      secondScans = await scan(12);
    }
    const finalCursor = cursor();
    const barrier = startClaudeReplayBarrier(buffer, config.captureRoots ?? [], {timeoutMs: 800});
    const hook = appendForwardedHook({id: crypto.randomUUID(),
      hook_event_name: "AssistantResponse", session_id: sessionA,
      timestamp: new Date(now - 1_000).toISOString()},
      {config, buffer, source: "claude_code", now: () => now}).event;
    const receipt = await barrier.done;
    const row = buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
      .get(hook.id) as {payload: string};
    const savedWork = JSON.parse(row.payload).metadata.workItemId ?? null;
    const sightingA = durableClaudeRootSessionSightings(buffer.database, sessionA).size;
    console.log(JSON.stringify({scenario, oldCursor, firstOffset, finalCursor, firstScans,
      secondScans, finalSize: fs.statSync(file).size, sightingA, receipt, savedWork}));
    if (scenario === "unchanged") {
      assert.equal(finalCursor?.offset, fs.statSync(file).size);
      assert.equal(finalCursor.digest,
        crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex"));
      assert.equal(receipt.state, "ready");
      assert.equal(sightingA, 0);
      assert.equal(savedWork, "beads:eco-6hoxj.165.97");
    } else {
      assert.ok(finalCursor?.offset === null || finalCursor?.offset === undefined ||
        finalCursor.offset < fs.statSync(file).size);
      assert.equal(receipt.state, "timed_out");
      assert.equal(savedWork, null);
    }
    const proof = createProofCompletion(`pr429-r8-oversized-prefix-${scenario}`, 1);
    proof.check(scenario);
    proof.complete();
  } finally { buffer.close(); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
