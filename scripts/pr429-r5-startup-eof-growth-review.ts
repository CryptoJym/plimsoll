import { fixtureEpochId } from "./lib/fixture-epoch-id";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { startClaudeReplayBarrier } from "../packages/collector-cli/src/claude-replay-barrier";
import { jsonlCoverageCheck } from "../packages/collector-cli/src/capture-frontier";
import { jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { dispatchBindingSchema,rootCursorKey } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";

async function main() {
  const home=process.env.HOME!,plimsoll=process.env.PLIMSOLL_HOME!;
  const now=Date.now(),sessionA=crypto.randomUUID(),sessionB=crypto.randomUUID();
  const A={rootId:"claude-a",profileId:"profile-a",installationEpochId:fixtureEpochId("epoch-a"),
    source:"claude_code" as const,directory:path.join(home,"claude-a","projects")};
  const B={rootId:"claude-b",profileId:"profile-b",installationEpochId:fixtureEpochId("epoch-b"),
    source:"claude_code" as const,directory:path.join(home,"claude-b","projects")};
  const project=path.join(B.directory,"-synthetic-project");
  fs.mkdirSync(A.directory,{recursive:true,mode:0o700});
  fs.mkdirSync(project,{recursive:true,mode:0o700});
  fs.mkdirSync(plimsoll,{recursive:true,mode:0o700});
  const file=path.join(project,`${sessionB}.jsonl`);
  fs.writeFileSync(file,JSON.stringify({type:"assistant",sessionId:sessionB,
    timestamp:new Date(now-1_000).toISOString(),message:{id:"message-b",
      model:"claude-sonnet-4-20250514",content:[],usage:{input_tokens:1,output_tokens:1,
        cache_read_input_tokens:0,cache_creation_input_tokens:0}}})+"\n");
  const startupSize=fs.statSync(file).size;
  const binding=dispatchBindingSchema.parse({sessionId:sessionA,
    workItemId:"beads:eco-6hoxj.165.97",projectKey:`sha256:${"a".repeat(64)}`,
    companyRef:null,attemptId:crypto.randomUUID(),parentAttemptId:null,
    acceptedOutcomeId:null,validFrom:new Date(now-60_000).toISOString(),
    validUntil:new Date(now+60_000).toISOString(),evidenceRef:"dispatch:r5-startup-eof"});
  const config=collectorConfigSchema.parse({deviceId:"dev-r5-startup-eof",
    uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[{...A,dispatch:[binding]},B]});
  fs.writeFileSync(path.join(plimsoll,"collector.config.json"),JSON.stringify(config)+"\n",{mode:0o600});
  const buffer=new LocalEventBuffer(path.join(plimsoll,"startup-eof.sqlite"),{
    workspaceId:config.tenantId,deviceId:config.deviceId,
    enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:true},databaseBusyTimeoutMs:0});
  try {
    const barrier=startClaudeReplayBarrier(buffer,config.captureRoots??[],{timeoutMs:600});
    // Claude appends an incomplete next record after startup captured the EOF.
    fs.appendFileSync(file,'{"type":"assistant"');
    const hook=appendForwardedHook({id:crypto.randomUUID(),
      hook_event_name:"AssistantResponse",session_id:sessionA,
      timestamp:new Date(now-1_000).toISOString()},
      {config,buffer,source:"claude_code",now:()=>now}).event;
    const receipt=await barrier.done;
    const stat=fs.statSync(file);
    const coverage=jsonlCoverageCheck(buffer.database)(rootCursorKey(config.captureRoots??[],file),stat);
    const cursor=buffer.database.prepare(`select size,committed_offset as committedOffset,
      deferred_bytes as deferredBytes,work_remaining as workRemaining,
      unresolved_kind as unresolvedKind from rollout_scan_state where file=?`)
      .get(jsonlScanStateKey(rootCursorKey(config.captureRoots??[],file)));
    const saved=buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
      .get(hook.id) as {payload:string};
    const work=JSON.parse(saved.payload).metadata.workItemId??null;
    console.log(JSON.stringify({scenario:"append_partial_record_after_startup_eof",
      startupSize,currentSize:stat.size,coverage,cursor,receipt,savedWork:work}));
    assert.ok((coverage?.progress??-1)>=startupSize,
      "the tailer must have reached the startup EOF before testing the barrier");
    assert.equal(receipt.state,"ready",
      "new bytes after startup EOF kept an already-covered root behind the barrier");
    assert.equal(work,"beads:eco-6hoxj.165.97");
  } finally { buffer.close(); }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
