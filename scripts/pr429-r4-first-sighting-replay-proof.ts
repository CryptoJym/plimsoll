import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { claudeDispatchSkipStatus, dispatchBindingSchema,
  durableClaudeRootSessionSightings } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { normalizeForwardedHook } from "../packages/collector-cli/src/forwarder";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { startClaudeReplayBarrier } from "../packages/collector-cli/src/claude-replay-barrier";
import { createProofCompletion } from "./lib/proof-completion";

const home=process.env.HOME!,plimsoll=process.env.PLIMSOLL_HOME!;
const now=Date.now(),observedAt=new Date(now-60_000).toISOString();
const sessionId="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const A={rootId:"claude-a",profileId:"profile-a",installationEpochId:"epoch-a",
  source:"claude_code" as const,directory:path.join(home,".claude-a","projects")};
const B={rootId:"claude-b",profileId:"profile-b",installationEpochId:"epoch-b",
  source:"claude_code" as const,directory:path.join(home,".claude-b","projects")};
const binding=dispatchBindingSchema.parse({sessionId,workItemId:"beads:eco-6hoxj.165.97",
  projectKey:`sha256:${"a".repeat(64)}`,companyRef:null,
  attemptId:"11111111-1111-4111-8111-111111111111",parentAttemptId:null,
  acceptedOutcomeId:null,validFrom:new Date(now-120_000).toISOString(),
  validUntil:new Date(now+120_000).toISOString(),evidenceRef:"dispatch:r4-first-sighting-kill"});
const config=collectorConfigSchema.parse({deviceId:"dev_pr429-r4-first-sighting",
  uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[{...A,dispatch:[binding]},B]});
const options={workspaceId:config.tenantId,deviceId:config.deviceId,
  enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:false}};
function fixture(phase:string) {
  const project=path.join(B.directory,`-${phase}`),file=path.join(project,`${sessionId}.jsonl`);
  fs.mkdirSync(project,{recursive:true});
  const line={type:"assistant",sessionId,timestamp:observedAt,
    message:{id:`message-${phase}`,model:"claude-sonnet-4-20250514",content:[],
      usage:{input_tokens:1,output_tokens:1,cache_read_input_tokens:0,
        cache_creation_input_tokens:0}}};
  fs.writeFileSync(file,JSON.stringify(line)+"\n");
  return path.join(plimsoll,`${phase}.sqlite`);
}
function killAtFirstSighting(buffer:LocalEventBuffer,phase:string) {
  const database=buffer.database,original=database.prepare.bind(database);
  database.prepare=((sql:string)=>{
    if(!sql.includes("insert into capture_root_session_sightings")) return original(sql);
    if(phase==="before-insert") {
      process.stdout.write(`SIGKILL_FIRST_SIGHTING=${phase}\n`);
      process.kill(process.pid,"SIGKILL");
    }
    const statement=original(sql);
    return new Proxy(statement,{get(target,key) {
      if(key==="run") return (...args:unknown[])=>{
        const value=(target.run as (...values:unknown[])=>unknown).apply(target,args);
        process.stdout.write(`SIGKILL_FIRST_SIGHTING=${phase}\n`);
        process.kill(process.pid,"SIGKILL");
        return value;
      };
      const value=Reflect.get(target,key);
      return typeof value==="function"?value.bind(target):value;
    }});
  }) as typeof database.prepare;
}
function probe(buffer:LocalEventBuffer,phase:string) {
  const before=claudeDispatchSkipStatus().otherRootSeen;
  const event=normalizeForwardedHook({id:crypto.randomUUID(),
    hook_event_name:"AssistantResponse",session_id:sessionId,timestamp:observedAt},
    {config,buffer,source:"claude_code",now:()=>now}).event;
  assert.equal(buffer.append(event,[]),true);
  return {phase,work:event.metadata.workItemId??null,
    otherRootSeenDelta:claudeDispatchSkipStatus().otherRootSeen-before,
    sightings:durableClaudeRootSessionSightings(buffer.database,sessionId).size,id:event.id};
}

async function main() {
if(process.argv[2]==="child") {
  const phase=process.argv[3]!,file=process.env.PR429_FIRST_SIGHTING_FILE!;
  const buffer=new LocalEventBuffer(file,options);
  killAtFirstSighting(buffer,phase);
  const tailer=new TranscriptTailer(buffer,B.directory,undefined,config.captureRoots);
  await tailer.scan({scope:"full"});
  process.exit(98);
} else {
  fs.mkdirSync(plimsoll,{recursive:true});
  fs.mkdirSync(A.directory,{recursive:true});
  fs.writeFileSync(path.join(plimsoll,"collector.config.json"),JSON.stringify(config)+"\n");
  const outcomes=[];
  assert.match(process.argv[2]??"",/^phase-(before-insert|after-insert-before-commit)$/);
  const phases=[process.argv[2]!.slice("phase-".length)];
  for(const phase of phases) {
    const file=fixture(phase);
    new LocalEventBuffer(file,options).close();
    const loader=path.resolve("node_modules/tsx/dist/loader.mjs");
    const child=spawnSync(process.execPath,["--import",loader,path.resolve(process.argv[1]),
      "child",phase],{cwd:process.cwd(),encoding:"utf8",timeout:30_000,
      env:{...process.env,PR429_FIRST_SIGHTING_FILE:file}});
    assert.equal(child.signal,"SIGKILL",`${phase}: ${child.status} ${child.stderr}`);
    assert.match(child.stdout,new RegExp(`SIGKILL_FIRST_SIGHTING=${phase}`));
    const reopened=new LocalEventBuffer(file,options);
    try {
      const barrier=startClaudeReplayBarrier(reopened,config.captureRoots??[]);
      const before=probe(reopened,phase);
      const replay=await barrier.done;
      assert.equal(replay.state,"ready");
      assert.equal(replay.targets,1);
      const after=probe(reopened,`${phase}-after-replay`);
      const saved=reopened.database.prepare("select payload_json as payload from buffered_events where id=?")
        .get(before.id) as {payload:string};
      const savedWork=JSON.parse(saved.payload).metadata.workItemId??null;
      outcomes.push({before,after,savedWork});
      console.log(JSON.stringify({before,after,savedWork,replay,childSignal:child.signal}));
      assert.equal(after.work,null);
      assert.equal(after.sightings,1);
    } finally { reopened.close(); }
  }
  assert.equal(outcomes.every(x=>x.before.work===null),true,
    "B's first-sighting write was interrupted; A's work stamped before B replay");
  const proof=createProofCompletion("pr429-r4-first-sighting-replay",1);
  proof.check(`interrupted_${phases[0]}_holds_hook_until_replay`);
  proof.complete();
}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
