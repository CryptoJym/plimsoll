import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { startClaudeReplayBarrier } from "../packages/collector-cli/src/claude-replay-barrier";
import { dispatchBindingSchema } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";

async function main() {
  const home=process.env.HOME!,plimsoll=process.env.PLIMSOLL_HOME!;
  const realStart=Date.now(),frozenNow=realStart,sessionId=crypto.randomUUID();
  const A={rootId:"claude-a",profileId:"profile-a",installationEpochId:"epoch-a",
    source:"claude_code" as const,directory:path.join(home,"claude-a","projects")};
  const B={rootId:"claude-b",profileId:"profile-b",installationEpochId:"epoch-b",
    source:"claude_code" as const,directory:path.join(home,"claude-b","projects")};
  fs.mkdirSync(A.directory,{recursive:true});
  fs.mkdirSync(B.directory,{recursive:true});
  fs.mkdirSync(plimsoll,{recursive:true});
  const binding=dispatchBindingSchema.parse({sessionId,workItemId:"beads:eco-6hoxj.165.97",
    projectKey:`sha256:${"a".repeat(64)}`,companyRef:null,attemptId:crypto.randomUUID(),
    parentAttemptId:null,acceptedOutcomeId:null,
    validFrom:new Date(realStart-60_000).toISOString(),
    validUntil:new Date(realStart+60_000).toISOString(),evidenceRef:"dispatch:r5-clock-skew"});
  const config=collectorConfigSchema.parse({deviceId:"dev-r5-clock-skew",
    uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[{...A,dispatch:[binding]},B]});
  fs.writeFileSync(path.join(plimsoll,"collector.config.json"),JSON.stringify(config)+"\n");
  const file=path.join(plimsoll,"clock-skew.sqlite");
  const buffer=new LocalEventBuffer(file,{workspaceId:config.tenantId,
    deviceId:config.deviceId,enrollmentNow:()=>new Date(realStart-3_600_000),
    delivery:{enabled:true},databaseBusyTimeoutMs:0});
  let resolved=false;
  const barrier=startClaudeReplayBarrier(buffer,config.captureRoots??[],
    {timeoutMs:50,now:()=>frozenNow});
  barrier.done.then(()=>{resolved=true;});
  const hook=appendForwardedHook({id:crypto.randomUUID(),hook_event_name:"AssistantResponse",
    session_id:sessionId,timestamp:new Date(realStart-1_000).toISOString()},
    {config,buffer,source:"claude_code",now:()=>realStart}).event;
  const locker=new Database(file,{timeout:0});
  locker.exec("BEGIN IMMEDIATE");
  await new Promise<void>(resolve=>setTimeout(resolve,800));
  const held=buffer.database.prepare("select status from claude_replay_hooks where event_id=?")
    .get(hook.id) as {status:string};
  locker.exec("ROLLBACK");locker.close();
  const receipt=await Promise.race([barrier.done,
    new Promise<null>(resolve=>setTimeout(()=>resolve(null),1_000))]);
  const actual={scenario:"wall_clock_stalls_during_reconcile_lock",
    timeoutMs:50,elapsedMs:Date.now()-realStart,barrierDone:resolved,
    heldStatus:held.status,receipt,barrierState:buffer.claudeReplayBarrierState()};
  console.log(JSON.stringify(actual));
  if(!receipt) process.exit(1);
  const after=buffer.database.prepare("select status from claude_replay_hooks where event_id=?")
    .get(hook.id) as {status:string};
  buffer.close();
  assert.equal(receipt.state,"timed_out","a frozen wall clock must not turn a timed-out hold into a ready replay");
  assert.equal(after.status,"timed_out");
}
main().catch(error=>{console.error(error);process.exitCode=1;});
