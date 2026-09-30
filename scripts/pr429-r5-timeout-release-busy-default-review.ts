import { fixtureEpochId } from "./lib/fixture-epoch-id";
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
  const now=Date.now(),sessionId=crypto.randomUUID();
  const A={rootId:"claude-a",profileId:"profile-a",installationEpochId:fixtureEpochId("epoch-a"),
    source:"claude_code" as const,directory:path.join(home,"claude-a","projects")};
  const B={rootId:"claude-b",profileId:"profile-b",installationEpochId:fixtureEpochId("epoch-b"),
    source:"claude_code" as const,directory:path.join(home,"missing-b","projects")};
  fs.mkdirSync(A.directory,{recursive:true});
  fs.mkdirSync(plimsoll,{recursive:true});
  const binding=dispatchBindingSchema.parse({sessionId,
    workItemId:"beads:eco-6hoxj.165.97",projectKey:`sha256:${"a".repeat(64)}`,
    companyRef:null,attemptId:crypto.randomUUID(),parentAttemptId:null,
    acceptedOutcomeId:null,validFrom:new Date(now-60_000).toISOString(),
    validUntil:new Date(now+60_000).toISOString(),evidenceRef:"dispatch:r5-timeout-busy"});
  const config=collectorConfigSchema.parse({deviceId:"dev-r5-timeout-busy",
    uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[{...A,dispatch:[binding]},B]});
  fs.writeFileSync(path.join(plimsoll,"collector.config.json"),JSON.stringify(config)+"\n");
  const file=path.join(plimsoll,"timeout-busy.sqlite");
  const buffer=new LocalEventBuffer(file,{workspaceId:config.tenantId,
    deviceId:config.deviceId,enrollmentNow:()=>new Date(now-3_600_000),
    delivery:{enabled:true}});
  let locker:Database.Database|undefined;
  try {
    const barrier=startClaudeReplayBarrier(buffer,config.captureRoots??[],{timeoutMs:80});
    const hook=appendForwardedHook({id:crypto.randomUUID(),hook_event_name:"AssistantResponse",
      session_id:sessionId,timestamp:new Date(now-1_000).toISOString()},
      {config,buffer,source:"claude_code",now:()=>now}).event;
    const before=buffer.database.prepare("select status from claude_replay_hooks where event_id=?")
      .get(hook.id) as {status:string};
    assert.equal(before.status,"pending");
    locker=new Database(file,{timeout:0});
    locker.exec("BEGIN IMMEDIATE");
    // The first release attempt hits the collector's five-second busy timeout.
    // Once it yields, this timer drops the transient writer lock so a retry can commit.
    const unlocked=new Promise<void>(resolve=>setTimeout(()=>{
      locker?.exec("COMMIT");
      locker?.close(); locker=undefined;
      resolve();
    },500));
    const receipt=await barrier.done;
    await unlocked;
    const after=buffer.database.prepare("select status from claude_replay_hooks where event_id=?")
      .get(hook.id) as {status:string};
    const leased=buffer.delivery.lease({now:new Date(now+2_000)}).items.length;
    await new Promise<void>(resolve=>setTimeout(resolve,350));
    const later=buffer.database.prepare("select status from claude_replay_hooks where event_id=?")
      .get(hook.id) as {status:string};
    const leasedLater=buffer.delivery.lease({now:new Date(now+3_000)}).items.length;
    const raw=buffer.database.prepare("select 1 from buffered_events where id=?").get(hook.id);
    console.log(JSON.stringify({scenario:"one_write_lock_during_timeout_release_default_5s_busy_timeout",
      receipt,barrierState:buffer.claudeReplayBarrierState(),heldStatus:after.status,
      rawExists:Boolean(raw),leasedAfterLockReleased:leased,
      heldStatusLater:later.status,leasedAfterRetryWindow:leasedLater}));
    assert.equal(receipt.state,"timed_out");
    assert.equal(after.status,"timed_out",
      "a transient SQLite writer lock left a hook held after the timeout finished");
    assert.equal(leased,1);
  } finally {
    try { locker?.exec("ROLLBACK"); } catch { /* already released */ }
    locker?.close();
    buffer.close();
  }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
