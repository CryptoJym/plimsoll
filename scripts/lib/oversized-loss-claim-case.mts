/** Independent review's real maintenance fixture, with the fixed claim as its
 * success condition. The two oversized usage records are never parsed. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const root = path.resolve(import.meta.dirname, "../..");
const load = (relative: string) => import(pathToFileURL(path.join(root, relative)).href);
const [{ LocalEventBuffer }, { CollectorMaintenance }, { RolloutTailer }, { TranscriptTailer },
  { captureBaselineStatus }, { captureFrontier }, { DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey },
  { rootCursorKey }, { classifySkippedRecord, recordCaptureRecordLoss }] = await Promise.all([
  load("packages/collector-cli/src/buffer.ts"),
  load("packages/collector-cli/src/maintenance.ts"),
  load("packages/collector-cli/src/rollout-tailer.ts"),
  load("packages/collector-cli/src/transcript-tailer.ts"),
  load("packages/collector-cli/src/capture-baseline.ts"),
  load("packages/collector-cli/src/capture-frontier.ts"),
  load("packages/collector-cli/src/jsonl-byte-tailer.ts"),
  load("packages/collector-cli/src/capture-root-inventory.ts"),
  load("packages/collector-cli/src/capture-record-loss.ts"),
]);

if (process.env.PLIMSOLL_SKIP_CLASSIFIER_CASES !== "1") {
const codexChallenge = {header:{type:"session_meta"},type:"event_msg",payload:{type:"token_count"}};
const claudeChallenge = {header:{type:"user"},type:"assistant"};
assert.deepEqual(classifySkippedRecord("codex",Buffer.from(JSON.stringify(codexChallenge))),
  {kind:"codex_token_count",usagePossible:true},"nested Codex type must remain a usage loss");
assert.deepEqual(classifySkippedRecord("claude",Buffer.from(JSON.stringify(claudeChallenge))),
  {kind:"claude_assistant",usagePossible:true},"nested Claude type must remain a usage loss");
assert.deepEqual(classifySkippedRecord("codex",Buffer.from(JSON.stringify({
  header:{type:"session_meta",padding:"x".repeat(3000)},type:"event_msg",payload:{type:"token_count"},
})).subarray(0,2048)),{kind:"unknown",usagePossible:true},"ambiguous Codex prefix must remain a possible loss");
assert.deepEqual(classifySkippedRecord("claude",Buffer.from(JSON.stringify({
  header:{type:"user",padding:"x".repeat(3000)},type:"assistant",
})).subarray(0,2048)),{kind:"unknown",usagePossible:true},"ambiguous Claude prefix must remain a possible loss");
assert.deepEqual(classifySkippedRecord("claude",Buffer.from('{"header":{"type":"assistant"},"type":"user"}')),
  {kind:"claude_non_usage",usagePossible:false});
assert.deepEqual(classifySkippedRecord("codex",Buffer.from('{"type":"event_msg","payload":{"type":"user_message"}}')),
  {kind:"codex_non_usage",usagePossible:false});
assert.deepEqual(classifySkippedRecord("claude",Buffer.from('{"\\u0074ype":"assistant"}')),
  {kind:"claude_assistant",usagePossible:true});
}

const home = fs.mkdtempSync(path.join(fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir()), "oversized-loss-claim-"));
const codexHome = path.join(home, "codex"), claudeHome = path.join(home, "claude");
fs.mkdirSync(codexHome, { recursive: true });
fs.mkdirSync(claudeHome, { recursive: true });
const options = { workspaceId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", delivery: { enabled: true } };
let buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"), options);
const epoch = buffer.workspaceBinding().currentInstallationEpochId;
const roots = [
  { source: "codex", directory: codexHome, rootId: "codex", profileId: "codex", installationEpochId: epoch },
  { source: "claude_code", directory: claudeHome, rootId: "claude", profileId: "claude", installationEpochId: epoch },
];
const makeMaintenance = () => new CollectorMaintenance(buffer,
  new RolloutTailer(buffer, undefined, () => [], DEFAULT_JSONL_TAILER_IO, roots.slice(0, 1)),
  new TranscriptTailer(buffer, undefined, DEFAULT_JSONL_TAILER_IO, roots.slice(1)),
  undefined, undefined, { captureCoverageIntervalMs: 0, captureCoverageTurnMs: 250 });
let maintenance = makeMaintenance();
const restart = () => {maintenance.close();buffer.close();
  buffer = new LocalEventBuffer(path.join(home,"ledger.sqlite"),options);maintenance = makeMaintenance();};
const codexId = "22222222-2222-4222-8222-222222222222";
const claudeId = "33333333-3333-4333-8333-333333333333";
const ambiguousClaudeId = "66666666-6666-4666-8666-666666666666";
const old = new Date(Date.now() - 15 * 86_400_000).toISOString().slice(0, 10).split("-");
const codexFile = path.join(codexHome, ...old, `rollout-${codexId}.jsonl`);
const claudeFile = path.join(claudeHome, "project", `${claudeId}.jsonl`);
const ambiguousClaudeFile = path.join(claudeHome, "project", `${ambiguousClaudeId}.jsonl`);
const write = (name: string, rows: unknown[]) => { fs.mkdirSync(path.dirname(name), { recursive: true });
  fs.writeFileSync(name, rows.map(r => JSON.stringify(r)).join("\n") + "\n"); };
const cursor = (name: string) => buffer.database.prepare("select committed_offset as offset from rollout_scan_state where file=?")
  .get(jsonlScanStateKey(rootCursorKey(roots, name))) as {offset:number}|undefined;
try {
  for (let i = 0; i < 40 && captureBaselineStatus(buffer.database).status !== "complete"; i++)
    await maintenance.runRecent();
  if (captureBaselineStatus(buffer.database).status !== "complete") throw new Error("baseline did not complete");
  await new Promise(resolve => setTimeout(resolve, 10));
  const stamp = new Date().toISOString();
  const oversized = "x".repeat(17 * 1024 * 1024);
  write(codexFile, [
    { type: "session_meta", timestamp: stamp, payload: { id: codexId } },
    { type: "turn_context", timestamp: stamp, payload: { model: "gpt-6-sol" } },
    { type: "event_msg", timestamp: stamp, payload: { type: "token_count", info: {
      total_token_usage: { input_tokens: 0, cached_input_tokens: 0, output_tokens: 0, reasoning_output_tokens: 0 },
    } } },
    { header: { type: "session_meta" }, type: "event_msg", timestamp: stamp, payload: { type: "token_count",
      rate_limits: { plan_type: "q".repeat(5000) }, info: {
      total_token_usage: { input_tokens: 2, cached_input_tokens: 0, output_tokens: 0, reasoning_output_tokens: 0 },
    } }, padding: oversized },
  ]);
  write(claudeFile, [{ header: { type: "user" }, type: "assistant", sessionId: claudeId, timestamp: stamp,
    message: { id: "usage-1", model: "q".repeat(5000), usage: { input_tokens: 11, output_tokens: 1 } },
    padding: oversized }]);
  // The actual type is beyond the bounded prefix. It must be reported as an
  // unknown possible loss, not confidently classified from its nested header.
  write(ambiguousClaudeFile, [{header:{type:"user",padding:"q".repeat(3000)},type:"assistant",
    sessionId:ambiguousClaudeId,timestamp:stamp,message:{id:"ambiguous-usage",model:"q".repeat(5000),
      usage:{input_tokens:13,output_tokens:1}},
    padding:oversized}]);
  let turns = 0;
  let legacyReclassified = false;
  const codexFileKey = crypto.createHash("sha256").update(codexFile).digest("hex");
  for (; turns < 200 && (cursor(codexFile)?.offset !== fs.statSync(codexFile).size ||
    cursor(claudeFile)?.offset !== fs.statSync(claudeFile).size ||
    cursor(ambiguousClaudeFile)?.offset !== fs.statSync(ambiguousClaudeFile).size); turns++) {
    await maintenance.runRecent();
    if (!legacyReclassified) {
      const row = buffer.database.prepare("select envelope_json as value from jsonl_continuations where provider='codex' and file_key=?")
        .get(codexFileKey) as {value:string}|undefined;
      if (row) {
        const {sha256: _seal, ...body} = JSON.parse(row.value);
        if (body.skip?.classified && body.skip.fingerprint) {
          // Emulate a prior-round sealed checkpoint with the lexical false
          // non-usage classification. The restarted reader must repair it.
          delete body.skip.fingerprint;
          body.skip.kind = "codex_non_usage";
          body.skip.usagePossible = false;
          const sha256 = crypto.createHash("sha256").update(JSON.stringify(body)).digest("hex");
          buffer.database.prepare("update jsonl_continuations set envelope_json=? where provider='codex' and file_key=?")
            .run(JSON.stringify({...body,sha256}),codexFileKey);
          legacyReclassified = true;
          restart();
        }
      }
    }
  }
  await maintenance.runRecent();
  const hasReceipts = Boolean(buffer.database.prepare("select 1 from sqlite_master where type='table' and name='capture_record_losses'").get());
  const receipts = hasReceipts ? buffer.database.prepare(`select source,kind,usage_possible as usagePossible,skipped_bytes as bytes
    from capture_record_losses order by source`).all() : [];
  const events = buffer.database.prepare(`select session_id as sessionId,count(*) as count,
    coalesce(sum(input_tokens),0) as tokens from buffered_events where session_id in (?,?,?) group by session_id`)
    .all(codexId, claudeId, ambiguousClaudeId);
  const smallCodexId = "44444444-4444-4444-8444-444444444444";
  const smallClaudeId = "55555555-5555-4555-8555-555555555555";
  const smallCodexFile = path.join(codexHome, ...old, `rollout-${smallCodexId}.jsonl`);
  const smallClaudeFile = path.join(claudeHome, "project", `${smallClaudeId}.jsonl`);
  write(smallCodexFile, [
    { type: "session_meta", timestamp: stamp, payload: { id: smallCodexId } },
    { type: "turn_context", timestamp: stamp, payload: { model: "gpt-6-sol" } },
    { type: "event_msg", timestamp: stamp, payload: { type: "token_count", info: {
      total_token_usage: { input_tokens: 0, cached_input_tokens: 0, output_tokens: 0, reasoning_output_tokens: 0 },
    } } },
    { header: { type: "session_meta" }, type: "event_msg", timestamp: stamp, payload: { type: "token_count", info: {
      total_token_usage: { input_tokens: 2, cached_input_tokens: 0, output_tokens: 0, reasoning_output_tokens: 0 },
    } } },
  ]);
  write(smallClaudeFile, [{ header: { type: "user" }, type: "assistant", sessionId: smallClaudeId, timestamp: stamp,
    message: { id: "usage-2", model: "claude-opus-5", usage: { input_tokens: 11, output_tokens: 1 } } }]);
  for (let i = 0; i < 20 && (cursor(smallCodexFile)?.offset !== fs.statSync(smallCodexFile).size ||
    cursor(smallClaudeFile)?.offset !== fs.statSync(smallClaudeFile).size); i++) await maintenance.runRecent();
  const controls = buffer.database.prepare(`select session_id as sessionId,count(*) as count,
    coalesce(sum(input_tokens),0) as tokens from buffered_events where session_id in (?,?) group by session_id order by session_id`)
    .all(smallCodexId, smallClaudeId) as Array<{sessionId:string;count:number;tokens:number}>;
  const frontier = captureFrontier(buffer.database);
  const eof = cursor(codexFile)?.offset === fs.statSync(codexFile).size &&
    cursor(claudeFile)?.offset === fs.statSync(claudeFile).size &&
    cursor(ambiguousClaudeFile)?.offset === fs.statSync(ambiguousClaudeFile).size;
  const passed = eof && legacyReclassified && receipts.length === 3 &&
    receipts.some((r: any) => r.source === "codex" && r.kind === "codex_token_count" && r.usagePossible === 1) &&
    receipts.some((r: any) => r.source === "claude_code" && r.kind === "claude_assistant" && r.usagePossible === 1) &&
    receipts.some((r: any) => r.source === "claude_code" && r.kind === "unknown" && r.usagePossible === 1) &&
    events.length === 0 && controls.length === 2 && controls[0]?.tokens === 2 && controls[1]?.tokens === 11 &&
    (frontier?.gaps.length ?? 0) > 0;
  // A same-offset, same-size replacement must create a second receipt; an
  // identical replay must remain idempotent. The fingerprint is content-blind.
  const synthetic = {source:"codex" as const,fileKey:"synthetic-fingerprint-key",record:{
    offset:17,bytes:17*1024*1024,reason:"record_exceeds_byte_budget",kind:"unknown" as const,
    usagePossible:true,fingerprint:crypto.createHash("sha256").update("first").digest("hex"),
  }};
  const receiptCount = () => (buffer.database.prepare("select count(*) as n from capture_record_losses").get() as {n:number}).n;
  const beforeIdentity = receiptCount();
  recordCaptureRecordLoss(buffer.database,synthetic);
  recordCaptureRecordLoss(buffer.database,synthetic);
  recordCaptureRecordLoss(buffer.database,{...synthetic,record:{...synthetic.record,
    fingerprint:crypto.createHash("sha256").update("second").digest("hex")}});
  const fingerprintPassed = receiptCount() === beforeIdentity + 2;
  const continuations = buffer.database.prepare("select provider, envelope_json as envelope from jsonl_continuations").all()
    .map((row: any) => ({provider:row.provider,prefixEnd:JSON.parse(row.envelope).prefix?.end,
      reason:JSON.parse(row.envelope).reason,skip:JSON.parse(row.envelope).skip}));
  console.log(JSON.stringify({ schema: "plimsoll.oversized-loss-claim-proof.v1", turns, eof, legacyReclassified,
    codexCursor:cursor(codexFile),claudeCursor:cursor(claudeFile),ambiguousClaudeCursor:cursor(ambiguousClaudeFile),
    continuations,receipts,events,controls,
    gaps: frontier?.gaps, fingerprintPassed, passed:passed && fingerprintPassed }, null, 2));
  if (!passed || !fingerprintPassed) process.exitCode = 1;
} finally {
  maintenance.close(); buffer.close(); fs.rmSync(home, { recursive: true, force: true });
}
