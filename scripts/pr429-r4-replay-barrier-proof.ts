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

const home=process.env.HOME!,plimsoll=process.env.PLIMSOLL_HOME!;
const now=Date.now(),observedAt=new Date(now-1_000).toISOString();
fs.mkdirSync(plimsoll,{recursive:true});
function setup(label:string,missingRoot=false) {
  const sessionId=crypto.randomUUID();
  const A={rootId:`a-${label}`,profileId:`profile-a-${label}`,
    installationEpochId:`epoch-a-${label}`,source:"claude_code" as const,
    directory:path.join(home,`a-${label}`,"projects")};
  const B={rootId:`b-${label}`,profileId:`profile-b-${label}`,
    installationEpochId:`epoch-b-${label}`,source:"claude_code" as const,
    directory:path.join(home,`b-${label}`,"projects")};
  fs.mkdirSync(A.directory,{recursive:true});
  if(!missingRoot) fs.mkdirSync(B.directory,{recursive:true});
  const binding=dispatchBindingSchema.parse({sessionId,workItemId:"beads:eco-6hoxj.165.97",
    projectKey:`sha256:${"a".repeat(64)}`,companyRef:null,
    attemptId:crypto.randomUUID(),parentAttemptId:null,acceptedOutcomeId:null,
    validFrom:new Date(now-60_000).toISOString(),validUntil:new Date(now+60_000).toISOString(),
    evidenceRef:`dispatch:${label}`});
  const config=collectorConfigSchema.parse({deviceId:`dev-${label}`,
    uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[{...A,dispatch:[binding]},B]});
  const configFile=path.join(plimsoll,"collector.config.json");
  fs.writeFileSync(`${configFile}.next`,JSON.stringify(config)+"\n");
  fs.renameSync(`${configFile}.next`,configFile);
  const file=path.join(plimsoll,`${label}.sqlite`);
  const options={
    workspaceId:config.tenantId,deviceId:config.deviceId,
    enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:true},
  };
  const buffer=new LocalEventBuffer(file,options);
  const hook=()=>appendForwardedHook({id:crypto.randomUUID(),
    hook_event_name:"AssistantResponse",session_id:sessionId,timestamp:observedAt},
    {config,buffer,source:"claude_code",now:()=>now}).event;
  const saved=(id:string)=>{
    const row=buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
      .get(id) as {payload:string};
    return JSON.parse(row.payload).metadata.workItemId??null;
  };
  return {buffer,config,hook,saved,file,options,B};
}

async function main() {
  const proof=createProofCompletion("pr429-r4-replay-barrier",2);
  const clear=setup("clear");
  try {
    const barrier=startClaudeReplayBarrier(clear.buffer,clear.config.captureRoots??[]);
    const early=clear.hook();
    assert.equal(early.metadata.workItemId??null,null);
    assert.equal(clear.buffer.delivery.lease({now:new Date(now+2_000)}).items.length,0,
      "an early hook cannot enter delivery while its attribution is pending");
    const receipt=await barrier.done;
    assert.equal(receipt.state,"ready");
    assert.equal(receipt.attributed,1);
    assert.equal(clear.saved(early.id),"beads:eco-6hoxj.165.97");
    assert.equal(clear.buffer.delivery.lease({now:new Date(now+2_000)}).items.length,1);
    console.log(JSON.stringify({scenario:"pending_hook_attributed_after_empty_root_replay",receipt}));
    proof.check("pending_hook_is_held_then_attributed_and_deliverable");
  } finally { clear.buffer.close(); }

  const blocked=setup("missing",true);
  let timedOutId="";
  try {
    const before=claudeDispatchSkipStatus().replayTimeout;
    const barrier=startClaudeReplayBarrier(blocked.buffer,blocked.config.captureRoots??[],
      {timeoutMs:50});
    const early=blocked.hook();
    timedOutId=early.id;
    assert.equal(early.metadata.workItemId??null,null);
    assert.equal(blocked.buffer.delivery.lease({now:new Date(now+2_000)}).items.length,0);
    const receipt=await barrier.done;
    assert.equal(receipt.state,"timed_out");
    assert.equal(receipt.timedOutHooks,1);
    assert.equal(blocked.saved(early.id),null);
    const status=blocked.buffer.database.prepare("select status from claude_replay_hooks where event_id=?")
      .get(early.id) as {status:string};
    assert.equal(status.status,"timed_out");
    const later=blocked.hook();
    assert.equal(later.metadata.workItemId??null,null);
    assert.equal(claudeDispatchSkipStatus().replayTimeout-before,2);
    console.log(JSON.stringify({scenario:"missing_root_times_out_unbound",receipt,
      countedTimeouts:claudeDispatchSkipStatus().replayTimeout-before}));
  } finally { blocked.buffer.close(); }
  fs.mkdirSync(blocked.B.directory,{recursive:true});
  const restarted=new LocalEventBuffer(blocked.file,blocked.options);
  try {
    const second=startClaudeReplayBarrier(restarted,blocked.config.captureRoots??[]);
    assert.equal((await second.done).state,"ready");
    const raw=restarted.database.prepare("select payload_json as payload from buffered_events where id=?")
      .get(timedOutId) as {payload:string};
    assert.equal(JSON.parse(raw.payload).metadata.workItemId??null,null,
      "a timed-out hook stays unbound after a later successful restart");
    proof.check("unreadable_root_times_out_with_counted_unbound_hooks");
  } finally { restarted.close(); }
  proof.complete();
}
main().catch(error=>{console.error(error);process.exitCode=1;});
