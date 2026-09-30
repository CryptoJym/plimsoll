import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { startClaudeReplayBarrier } from "../packages/collector-cli/src/claude-replay-barrier";
import { claudeDispatchSkipStatus, dispatchBindingSchema } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import { createProofCompletion } from "./lib/proof-completion";

const scenario=process.argv[2]??"same-size";
assert.ok(["same-size","larger","between-reads","identical"].includes(scenario));
const now=Date.now(),sessionA=crypto.randomUUID(),sessionC=crypto.randomUUID();
const A={rootId:"claude-a",profileId:"profile-a",installationEpochId:"epoch-a",
  source:"claude_code" as const,directory:path.join(process.env.HOME!,"claude-a","projects")};
const B={rootId:"claude-b",profileId:"profile-b",installationEpochId:"epoch-b",
  source:"claude_code" as const,directory:path.join(process.env.HOME!,"claude-b","projects")};
// A non-UUID basename makes the session identity come from the transcript.
const file=path.join(B.directory,"project","startup.jsonl");
const filler=Array.from({length:64},(_,i)=>JSON.stringify({type:"progress",sessionId:sessionA,
  messageId:`filler-${i}`} )+"\n").join("");
const usage=(sessionId:string)=>JSON.stringify({type:"assistant",sessionId,
  timestamp:new Date(now-1_000).toISOString(),message:{id:"message-b",
    model:"claude-sonnet-4-20250514",content:[],usage:{input_tokens:1,output_tokens:1,
      cache_read_input_tokens:0,cache_creation_input_tokens:0}}})+"\n";
const prefix=scenario==="between-reads"?filler:"";
const original=prefix+usage(sessionA);
const replacement=prefix+usage(sessionC)+(scenario==="larger"?'{"type":"partial"':'');
const bound=dispatchBindingSchema.parse({sessionId:sessionA,
  workItemId:"beads:eco-6hoxj.165.97",projectKey:`sha256:${"a".repeat(64)}`,
  companyRef:null,attemptId:crypto.randomUUID(),parentAttemptId:null,
  acceptedOutcomeId:null,validFrom:new Date(now-60_000).toISOString(),
  validUntil:new Date(now+60_000).toISOString(),evidenceRef:"dispatch:r6-startup-eof"});
const config=collectorConfigSchema.parse({deviceId:`dev-r6-${scenario}`,
  uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[{...A,dispatch:[bound]},B]});
fs.mkdirSync(A.directory,{recursive:true});
fs.mkdirSync(path.dirname(file),{recursive:true});
fs.mkdirSync(process.env.PLIMSOLL_HOME!,{recursive:true});
fs.writeFileSync(file,original);
fs.writeFileSync(path.join(process.env.PLIMSOLL_HOME!,"collector.config.json"),
  JSON.stringify(config)+"\n");
const initial=fs.statSync(file);
const buffer=new LocalEventBuffer(path.join(process.env.PLIMSOLL_HOME!,`r6-${scenario}.sqlite`),{
  workspaceId:config.tenantId,deviceId:config.deviceId,
  enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:true},databaseBusyTimeoutMs:0});
const rewrite=(bytes:string)=>{
  const fd=fs.openSync(file,"r+");
  try { fs.ftruncateSync(fd,0);fs.writeSync(fd,bytes,0,"utf8"); }
  finally { fs.closeSync(fd); }
};
async function main() {
  const before=claudeDispatchSkipStatus().replayBytesUnvouched??0;
  const barrier=startClaudeReplayBarrier(buffer,config.captureRoots??[],{timeoutMs:2_000});
  let rewrites=0;
  const rewriteOnce=()=>{ rewrite(scenario==="identical"?original:replacement);rewrites++; };
  const originalRead=fs.readSync;
  if(scenario==="between-reads") {
    let scheduled=false;
    fs.readSync=((fd:number,...args:unknown[])=>{
      const n=(originalRead as (...values:unknown[])=>number)(fd,...args);
      if(!scheduled&&typeof args[3]==="number"&&args[3]===0&&n>1_000) {
        const stat=fs.fstatSync(fd);
        if(stat.ino===initial.ino&&stat.dev===initial.dev) {
          scheduled=true;
          setImmediate(rewriteOnce);
        }
      }
      return n;
    }) as typeof fs.readSync;
  } else rewriteOnce();
  const hook=appendForwardedHook({id:crypto.randomUUID(),hook_event_name:"AssistantResponse",
    session_id:sessionA,timestamp:new Date(now-1_000).toISOString()},
    {config,buffer,source:"claude_code",now:()=>now}).event;
  try {
    assert.equal(hook.metadata.workItemId??null,null,"hook must be held before replay");
    const receipt=await barrier.done;
    const saved=buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
      .get(hook.id) as {payload:string};
    const savedWork=JSON.parse(saved.payload).metadata.workItemId??null;
    const final=fs.statSync(file);
    const counted=(claudeDispatchSkipStatus().replayBytesUnvouched??0)-before;
    console.log(JSON.stringify({scenario,startupSize:initial.size,finalSize:final.size,
      inodeSame:initial.ino===final.ino,rewrites,receipt,savedWork,counted}));
    assert.equal(rewrites,1,"fixture must rewrite exactly once");
    assert.equal(initial.ino,final.ino);
    if(scenario==="same-size"||scenario==="between-reads")
      assert.equal(final.size,initial.size);
    if(scenario==="larger") assert.ok(final.size>initial.size);
    assert.equal(savedWork,null,"a changed startup region must never stamp A");
    if(scenario==="identical") {
      assert.equal(receipt.state,"ready","identical replay bytes are vouched");
      assert.equal(counted,0);
    } else {
      assert.equal(receipt.state,"timed_out","changed startup bytes are unvouched");
      assert.equal(receipt.timedOutHooks,1);
      assert.equal(counted,1);
    }
    const proof=createProofCompletion(`pr429-r6-startup-eof-${scenario}`,1);
    proof.check(scenario);
    proof.complete();
  } finally { fs.readSync=originalRead;buffer.close(); }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
