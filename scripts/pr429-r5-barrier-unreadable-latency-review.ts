import { fixtureEpochId } from "./lib/fixture-epoch-id";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { startClaudeReplayBarrier } from "../packages/collector-cli/src/claude-replay-barrier";
import { dispatchBindingSchema } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";

async function main() {
  const home=process.env.HOME!,plimsoll=process.env.PLIMSOLL_HOME!;
  const now=Date.now(),sessionId=crypto.randomUUID();
  const A={rootId:"claude-a",profileId:"profile-a",installationEpochId:fixtureEpochId("epoch-a"),
    source:"claude_code" as const,directory:path.join(home,"claude-a","projects")};
  const B={rootId:"claude-b",profileId:"profile-b",installationEpochId:fixtureEpochId("epoch-b"),
    source:"claude_code" as const,directory:path.join(home,"claude-b","projects")};
  fs.mkdirSync(A.directory,{recursive:true});
  fs.mkdirSync(B.directory,{recursive:true});
  fs.mkdirSync(plimsoll,{recursive:true});
  const unreadable=path.join(B.directory,`${crypto.randomUUID()}.jsonl`);
  fs.writeFileSync(unreadable,'{"type":"assistant"');
  fs.chmodSync(unreadable,0o000);
  const binding=dispatchBindingSchema.parse({sessionId,
    workItemId:"beads:eco-6hoxj.165.97",projectKey:`sha256:${"a".repeat(64)}`,
    companyRef:null,attemptId:crypto.randomUUID(),parentAttemptId:null,
    acceptedOutcomeId:null,validFrom:new Date(now-60_000).toISOString(),
    validUntil:new Date(now+60_000).toISOString(),evidenceRef:"dispatch:r5-unreadable"});
  const config=collectorConfigSchema.parse({deviceId:"dev-r5-unreadable",
    uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[{...A,dispatch:[binding]},B]});
  fs.writeFileSync(path.join(plimsoll,"collector.config.json"),JSON.stringify(config)+"\n");
  const buffer=new LocalEventBuffer(path.join(plimsoll,"unreadable.sqlite"),{
    workspaceId:config.tenantId,deviceId:config.deviceId,
    enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:true},databaseBusyTimeoutMs:0});
  try {
    const barrier=startClaudeReplayBarrier(buffer,config.captureRoots??[],{timeoutMs:1_800});
    const start=performance.now();
    const hook=appendForwardedHook({id:crypto.randomUUID(),hook_event_name:"AssistantResponse",
      session_id:sessionId,timestamp:new Date(now-1_000).toISOString()},
      {config,buffer,source:"claude_code",now:()=>now}).event;
    const hookMs=performance.now()-start;
    const beforeLease=buffer.delivery.lease({now:new Date(now+2_000)}).items.length;
    await new Promise<void>(resolve=>setTimeout(resolve,500));
    const waitingCpuStart=process.cpuUsage();
    await new Promise<void>(resolve=>setTimeout(resolve,400));
    const waitingCpu=process.cpuUsage(waitingCpuStart);
    const waitingCpuMs=(waitingCpu.user+waitingCpu.system)/1_000;
    const midStatus=(buffer.database.prepare("select status from claude_replay_hooks where event_id=?")
      .get(hook.id) as {status:string}).status;
    const receipt=await barrier.done;
    const afterLease=buffer.delivery.lease({now:new Date(now+2_000)}).items
      .filter(item=>item.envelope.event.id===hook.id).length;
    const held=buffer.database.prepare("select status from claude_replay_hooks where event_id=?")
      .get(hook.id) as {status:string};
    const cpuStart=process.cpuUsage();
    await new Promise<void>(resolve=>setTimeout(resolve,400));
    const cpu=process.cpuUsage(cpuStart);
    const idleCpuMs=(cpu.user+cpu.system)/1_000;
    console.log(JSON.stringify({scenario:"unreadable_root_and_prompt_hook",
      hookMs,beforeLease,receipt,heldStatus:held.status,afterLease,
      midStatus,waitingCpuMsOver400Ms:waitingCpuMs,idleCpuMsOver400Ms:idleCpuMs}));
    assert.equal(hook.metadata.workItemId??null,null);
    assert.equal(beforeLease,0);
    assert.equal(midStatus,"pending");
    assert.equal(receipt.state,"timed_out");
    assert.equal(held.status,"timed_out");
    assert.equal(afterLease,1);
  } finally {
    fs.chmodSync(unreadable,0o600);
    buffer.close();
  }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
