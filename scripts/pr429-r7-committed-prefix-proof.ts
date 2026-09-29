import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { startClaudeReplayBarrier } from "../packages/collector-cli/src/claude-replay-barrier";
import { claudeDispatchSkipStatus, dispatchBindingSchema,
  durableClaudeRootSessionSightings, rootCursorKey } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { createProofCompletion } from "./lib/proof-completion";

const scenario = process.argv[2] ?? "offset-eof";
assert.ok(["offset-eof", "preserve-last-64k", "identical-ctime",
  "legacy-no-digest", "middle-appended", "legacy-resume-append",
  "changed-resume-append"].includes(scenario));
const now = Date.now();
const sessionA = crypto.randomUUID();
const sessionC = crypto.randomUUID();
const A = {rootId:"claude-a",profileId:"profile-a",installationEpochId:"epoch-a",
  source:"claude_code" as const,directory:path.join(process.env.HOME!,"claude-a","projects")};
const B = {rootId:"claude-b",profileId:"profile-b",installationEpochId:"epoch-b",
  source:"claude_code" as const,directory:path.join(process.env.HOME!,"claude-b","projects")};
const file = path.join(B.directory,"project","startup.jsonl");
const usage = (sessionId:string) => JSON.stringify({type:"assistant",sessionId,
  timestamp:new Date(now-1_000).toISOString(),message:{id:"message-b",
    model:"claude-sonnet-4-20250514",content:[],usage:{input_tokens:1,output_tokens:1,
      cache_read_input_tokens:0,cache_creation_input_tokens:0}}})+"\n";
const edge = JSON.stringify({type:"progress",padding:"x".repeat(70_000)})+"\n";
const resumeEdge = (JSON.stringify({type:"progress",padding:"x".repeat(512)})+"\n").repeat(140);
const partial = '{"type":"progress","padding":"xxxxxxxxxx';
const resumeAppend = scenario === "legacy-resume-append" || scenario === "changed-resume-append";
const original = scenario === "identical-ctime" ? usage(sessionA) :
  resumeAppend ? resumeEdge+usage(sessionC)+resumeEdge :
  scenario === "preserve-last-64k" ? usage(sessionC)+edge :
  scenario === "middle-appended" ? usage(sessionC)+partial : usage(sessionC);
const replacement = scenario === "identical-ctime" ? original :
  resumeAppend ? resumeEdge+usage(sessionA)+resumeEdge+JSON.stringify({type:"progress",padding:"tail"})+"\n" :
  scenario === "preserve-last-64k" ? usage(sessionA)+edge :
  scenario === "middle-appended" ? usage(sessionA)+partial+'more"}\n' : usage(sessionA);
const binding = dispatchBindingSchema.parse({sessionId:sessionA,
  workItemId:"beads:eco-6hoxj.165.97",projectKey:`sha256:${"a".repeat(64)}`,
  companyRef:null,attemptId:crypto.randomUUID(),parentAttemptId:null,
  acceptedOutcomeId:null,validFrom:new Date(now-60_000).toISOString(),
  validUntil:new Date(now+60_000).toISOString(),evidenceRef:"proof:r7-committed-prefix"});
const config = collectorConfigSchema.parse({deviceId:`dev-r7-${scenario}`,
  uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[{...A,dispatch:[binding]},B]});
fs.mkdirSync(A.directory,{recursive:true});
fs.mkdirSync(path.dirname(file),{recursive:true});
fs.mkdirSync(process.env.PLIMSOLL_HOME!,{recursive:true});
fs.writeFileSync(file,original);
fs.writeFileSync(path.join(process.env.PLIMSOLL_HOME!,"collector.config.json"),
  JSON.stringify(config)+"\n");
const dbPath = path.join(process.env.PLIMSOLL_HOME!,"committed-prefix.sqlite");
const options = {workspaceId:config.tenantId,deviceId:config.deviceId,
  enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:true},databaseBusyTimeoutMs:0};
let buffer = new LocalEventBuffer(dbPath,options);
const key = jsonlScanStateKey(rootCursorKey(config.captureRoots??[],file));
const cursor = () => buffer.database.prepare(`select committed_offset as offset,
  committed_prefix_hash as digest from rollout_scan_state where file=?`).get(key) as
  {offset:number|null;digest:string|null}|undefined;

async function main() {
  const tailer = new TranscriptTailer(buffer,A.directory,undefined,config.captureRoots??[]);
  try {
    const scan = await tailer.scan({scope:"full"});
    assert.equal(scan.parseErrors,0);
  } finally { tailer.close(); }
  const beforeCursor = cursor();
  const committed = scenario === "middle-appended" ? usage(sessionC).length : original.length;
  assert.equal(beforeCursor?.offset,committed);
  assert.equal(beforeCursor.digest,
    crypto.createHash("sha256").update(original.slice(0,committed)).digest("hex"));
  assert.equal(durableClaudeRootSessionSightings(buffer.database,sessionA).size,
    scenario === "identical-ctime" ? 1 : 0);
  if (scenario === "legacy-no-digest" || scenario === "legacy-resume-append") {
    buffer.database.prepare("update rollout_scan_state set committed_prefix_hash=null where file=?")
      .run(key);
  }
  const initialStat = fs.statSync(file,{bigint:true});
  buffer.close();
  const fd = fs.openSync(file,"r+");
  try { fs.ftruncateSync(fd,0); fs.writeSync(fd,replacement,0,"utf8"); }
  finally { fs.closeSync(fd); }
  if (scenario === "identical-ctime" &&
      fs.statSync(file,{bigint:true}).ctimeNs === initialStat.ctimeNs) {
    fs.utimesSync(file,new Date(now-10_000),new Date(now+5_000));
  }
  const finalStat = fs.statSync(file,{bigint:true});
  assert.equal(finalStat.ino,initialStat.ino);
  if (scenario === "identical-ctime") {
    assert.notEqual(finalStat.ctimeNs,initialStat.ctimeNs);
    assert.equal(replacement,original);
  } else assert.notEqual(replacement.slice(0,committed),original.slice(0,committed));
  if (scenario === "preserve-last-64k") {
    assert.equal(replacement.length,original.length);
    assert.equal(replacement.slice(committed-64*1024,committed),
      original.slice(committed-64*1024,committed));
  }
  if (scenario === "middle-appended" || resumeAppend) {
    if (resumeAppend) assert.equal(committed,original.length);
    else assert.ok(committed<original.length);
    assert.ok(replacement.length>original.length);
  } else assert.equal(replacement.length,original.length);
  buffer = new LocalEventBuffer(dbPath,options);
  try {
    if (resumeAppend) {
      assert.equal(replacement.slice(0,512),original.slice(0,512));
      assert.equal(replacement.slice(committed-512,committed),
        original.slice(committed-512,committed));
      const resumed = new TranscriptTailer(buffer,A.directory,undefined,config.captureRoots??[]);
      const resumeScans: unknown[] = [];
      try {
        for (let attempt=0;attempt<3;attempt++) {
          const scan = await resumed.scan({scope:"full"});
          resumeScans.push({parseErrors:scan.parseErrors,slicesCommitted:scan.slicesCommitted,
            recordsCommitted:scan.recordsCommitted,unresolvedRecords:scan.unresolvedRecords,
            cursor:cursor()?.offset});
          if (cursor()?.offset === replacement.length) break;
        }
      }
      finally { resumed.close(); }
      console.log(JSON.stringify({scenario,resumeScans,afterCursor:cursor(),
        rootBSawA:durableClaudeRootSessionSightings(buffer.database,sessionA).size}));
      if (scenario === "legacy-resume-append") {
        assert.equal(cursor()?.offset,replacement.length);
        assert.equal(durableClaudeRootSessionSightings(buffer.database,sessionA).size,1);
      } else {
        assert.equal(cursor()?.offset,committed);
        assert.equal(durableClaudeRootSessionSightings(buffer.database,sessionA).size,0);
      }
    }
    const skippedBefore = claudeDispatchSkipStatus().replayBytesUnvouched??0;
    const barrier = startClaudeReplayBarrier(buffer,config.captureRoots??[],{timeoutMs:800});
    const hook = appendForwardedHook({id:crypto.randomUUID(),
      hook_event_name:"AssistantResponse",session_id:sessionA,
      timestamp:new Date(now-1_000).toISOString()},
      {config,buffer,source:"claude_code",now:()=>now}).event;
    assert.equal(hook.metadata.workItemId??null,null);
    const receipt = await barrier.done;
    const row = buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
      .get(hook.id) as {payload:string};
    const savedWork = JSON.parse(row.payload).metadata.workItemId??null;
    const unvouched = (claudeDispatchSkipStatus().replayBytesUnvouched??0)-skippedBefore;
    console.log(JSON.stringify({scenario,committed,originalSize:original.length,
      finalSize:replacement.length,cursorDigest:beforeCursor.digest,
      ctimeChanged:finalStat.ctimeNs!==initialStat.ctimeNs,receipt,savedWork,unvouched}));
    assert.equal(savedWork,null,"an unvouched prefix must never stamp A");
    if (scenario === "identical-ctime" || scenario === "legacy-resume-append") {
      assert.equal(receipt.state,"ready");
      assert.equal(receipt.attributed,0);
      assert.equal(unvouched,0);
      assert.equal(durableClaudeRootSessionSightings(buffer.database,sessionA).size,1);
    } else {
      assert.equal(receipt.state,"timed_out");
      assert.equal(receipt.timedOutHooks,1);
      assert.equal(unvouched,1);
    }
    const proof = createProofCompletion(`pr429-r7-committed-prefix-${scenario}`,1);
    proof.check(scenario);
    proof.complete();
  } finally { buffer.close(); }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
