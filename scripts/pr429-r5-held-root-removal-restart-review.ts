import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { startClaudeReplayBarrier } from "../packages/collector-cli/src/claude-replay-barrier";
import { dispatchBindingSchema } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";

async function main() {
  const home=process.env.HOME!,plimsoll=process.env.PLIMSOLL_HOME!;
  const now=Number(process.env.PR429_NOW??Date.now());
  const observedAt=new Date(now-1_000).toISOString();
  const sessionId="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  const A={rootId:"claude-a",profileId:"profile-a",installationEpochId:"epoch-a",
    source:"claude_code" as const,directory:path.join(home,"claude-a","projects")};
  const B={rootId:"claude-b",profileId:"profile-b",installationEpochId:"epoch-b",
    source:"claude_code" as const,directory:path.join(home,"claude-b","projects")};
  const binding=dispatchBindingSchema.parse({sessionId,workItemId:"beads:eco-6hoxj.165.97",
    projectKey:`sha256:${"a".repeat(64)}`,companyRef:null,
    attemptId:"11111111-1111-4111-8111-111111111111",parentAttemptId:null,
    acceptedOutcomeId:null,validFrom:new Date(now-60_000).toISOString(),
    validUntil:new Date(now+60_000).toISOString(),evidenceRef:"dispatch:r5-held-root-removal"});
  const initial=collectorConfigSchema.parse({deviceId:"dev-r5-root-removal",
    uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[{...A,dispatch:[binding]},B]});
  const afterRemoval=collectorConfigSchema.parse({...initial,
    captureRoots:[{...A,dispatch:[binding]}]});
  const keepB=process.argv[2]==="keep-b";
  const file=path.join(plimsoll,"held-root-removal.sqlite");
  const configFile=path.join(plimsoll,"collector.config.json");
  const options={workspaceId:initial.tenantId,deviceId:initial.deviceId,
    enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:true},databaseBusyTimeoutMs:0};
  if(process.argv[2]==="child") {
    const buffer=new LocalEventBuffer(file,options);
    startClaudeReplayBarrier(buffer,initial.captureRoots??[]);
    const hook=appendForwardedHook({id:crypto.randomUUID(),
      hook_event_name:"AssistantResponse",session_id:sessionId,timestamp:observedAt},
      {config:initial,buffer,source:"claude_code",now:()=>now}).event;
    const status=buffer.database.prepare(`select status,root_set_json as rootSetJson
      from claude_replay_hooks where event_id=?`).get(hook.id) as {status:string;rootSetJson:string};
    assert.equal(status.status,"pending");
    assert.equal(JSON.parse(status.rootSetJson).length,2);
    process.stdout.write(`HELD_HOOK_ID=${hook.id}\n`);
    process.kill(process.pid,"SIGKILL");
  }
  fs.mkdirSync(plimsoll,{recursive:true});
  fs.mkdirSync(A.directory,{recursive:true});
  fs.mkdirSync(B.directory,{recursive:true});
  const project=path.join(B.directory,"-synthetic-project");
  fs.mkdirSync(project,{recursive:true});
  fs.writeFileSync(path.join(project,`${sessionId}.jsonl`),JSON.stringify({type:"assistant",
    sessionId,timestamp:observedAt,message:{id:"message-b",model:"claude-sonnet-4-20250514",
      content:[],usage:{input_tokens:1,output_tokens:1,
        cache_read_input_tokens:0,cache_creation_input_tokens:0}}})+"\n");
  fs.writeFileSync(configFile,JSON.stringify(initial)+"\n");
  new LocalEventBuffer(file,options).close();
  const loader=path.resolve("node_modules/tsx/dist/loader.mjs");
  const child=spawnSync(process.execPath,["--import",loader,path.resolve(process.argv[1]),
    "child"],{cwd:process.cwd(),encoding:"utf8",timeout:30_000,
    env:{...process.env,PR429_NOW:String(now)}});
  assert.equal(child.signal,"SIGKILL",`${child.status} ${child.stderr}`);
  const id=child.stdout.match(/HELD_HOOK_ID=([0-9a-f-]+)/)?.[1];
  assert.ok(id,child.stdout);
  const activeConfig=keepB?initial:afterRemoval;
  fs.writeFileSync(`${configFile}.next`,JSON.stringify(activeConfig)+"\n");
  fs.renameSync(`${configFile}.next`,configFile);
  const reopened=new LocalEventBuffer(file,options);
  try {
    const barrier=startClaudeReplayBarrier(reopened,activeConfig.captureRoots??[]);
    const receipt=await barrier.done;
    const row=reopened.database.prepare("select payload_json as payload from buffered_events where id=?")
      .get(id) as {payload:string};
    const savedWork=JSON.parse(row.payload).metadata.workItemId??null;
    const held=reopened.database.prepare("select status from claude_replay_hooks where event_id=?")
      .get(id) as {status:string}|undefined;
    const outboxBefore=reopened.database.prepare(`select state,raw_id as rawId,workspace_id as workspaceId,
      device_id as deviceId,next_attempt_at as nextAttemptAt from upload_outbox`).all();
    const leaseReceipt=reopened.delivery.lease({now:new Date(now+10_000)});
    const leaseItems=leaseReceipt.items;
    const leased=leaseItems.length;
    const hookLeaseCount=leaseItems.filter(item=>item.envelope.event.id===id).length;
    const leasedAgain=reopened.delivery.lease({now:new Date(now+10_000)}).items.length;
    console.log(JSON.stringify({scenario:keepB?"same_roots_after_held_hook_restart":
      "root_removed_between_held_hook_and_restart",
      childSignal:child.signal,receipt,savedWork,heldStatus:held?.status??null,
      deliverable:leased,hookLeaseCount,secondLease:leasedAgain,oldBFileStillExists:true,
      outboxBefore,leaseReceipt:{blockedBy:leaseReceipt.blockedBy,locallyDead:leaseReceipt.locallyDead},
      outboxAfter:reopened.database.prepare("select state, last_failure_class as failure from upload_outbox").all()}));
    assert.equal(savedWork,null,"restarted barrier stamped A after the previously configured B root was removed");
    assert.equal(hookLeaseCount,1);
    assert.equal(leasedAgain,0);
  } finally { reopened.close(); }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
