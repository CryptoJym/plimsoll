import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";

const completion=createProofCompletion("projection-window-upgrade",7);
const root=fs.mkdtempSync(path.join(os.tmpdir(),"plimsoll-window-upgrade-"));
const start=new Date("2026-11-01T12:00:00Z"), now=new Date("2026-11-03T12:00:00Z");
const originalNow=Date.now;
Date.now=()=>start.getTime();
function settle(buffer:LocalEventBuffer, at:Date){
  for(let i=0;i<80;i++){
    buffer.projection.runMaintenance(at);
    const s=buffer.projection.status();
    if(s.parityReady&&!s.dirty&&Object.values(s.backlog).every(n=>n===0))return;
  }
  assert.fail(`projection did not settle: ${JSON.stringify(buffer.projection.status())}`);
}
let sequence=0;
function event(at:string, model="gpt-6.1-sol", source:"codex"|"claude_code"="codex"){
  return aiInteractionEventSchema.parse({id:`00000000-0000-4000-8000-${String(++sequence).padStart(12,"0")}`,
    tenantId:"local",source,dataMode:"metadata",eventType:"usage_rollout",observedAt:at,
    sessionId:"fork-own-session",model,inputTokens:11,outputTokens:3,cacheReadTokens:7});
}
const tables=["dashboard_window_totals","dashboard_source_window","dashboard_daily_window",
  "dashboard_model_window","dashboard_action_window","dashboard_parity_window",
  "dashboard_post_highwater_window"];
function aggregates(buffer:LocalEventBuffer){
  return tables.map(table=>buffer.database.prepare(`select * from ${table} order by 1,2`).all());
}
async function main(){try{
  const oldRoot=path.join(root,"old");fs.mkdirSync(oldRoot);
  const archive=spawnSync("git",["archive","375f277b85f7d4ede7db77bf4359c371c0e8a4aa","packages"],{maxBuffer:32*1024*1024});
  assert.equal(archive.status,0,archive.stderr.toString());
  assert.equal(spawnSync("tar",["-xf","-","-C",oldRoot],{input:archive.stdout}).status,0);
  fs.symlinkSync(path.join(process.cwd(),"node_modules"),path.join(oldRoot,"node_modules"),"dir");
  const {LocalEventBuffer:OldBuffer}=await import(pathToFileURL(path.join(oldRoot,
    "packages/collector-cli/src/buffer.ts")).href) as {LocalEventBuffer:typeof LocalEventBuffer};
  const old=new OldBuffer(path.join(root,"upgrade.sqlite"));
  // One expiry batch leaves a second timestamp ahead of its durable cursor.
  for(let i=0;i<251;i++)assert.equal(old.append(event(i<250?
    "2026-10-26T12:00:00.000Z":"2026-10-26T12:00:00.001Z")),true);
  assert.equal(old.append(event(start.toISOString(),"claude-haiku-4-5-20251001","claude_code")),true);
  const compact=aiInteractionEventSchema.parse({...event(start.toISOString()),
    sessionId:undefined,eventType:"tool_use",model:undefined,inputTokens:undefined,
    outputTokens:undefined,cacheReadTokens:undefined,actionClass:"other"});
  assert.equal(old.append(compact),true);
  settle(old,start);Date.now=()=>now.getTime();
  old.projection.runMaintenance(now);
  const partial=old.database.prepare(`select target_cutoff_at from dashboard_window_control where days=7`)
    .get() as {target_cutoff_at:string|null};
  assert.notEqual(partial.target_cutoff_at,null);
  assert.equal(old.append(event("2026-10-26T18:00:00Z")),true);
  settle(old,now);
  const broken=old.database.prepare(`select events,input_tokens from dashboard_window_totals where days=7`)
    .get() as {events:number;input_tokens:number};
  assert.equal(broken.input_tokens,0,"one retained 11-token event minus one never-added 11-token event");
  const brokenModel=old.database.prepare(`select calls,input_tokens from dashboard_model_window
    where days=7 and model='gpt-6.1-sol'`).get() as {calls:number;input_tokens:number};
  assert.equal(brokenModel.calls,-1);
  old.database.exec(`update dashboard_model_window set input_tokens=input_tokens+101 where days=90`);
  console.log(JSON.stringify({oldVersion:"0.7.44",broken,brokenModel}));
  old.close();
  const upgraded=new LocalEventBuffer(path.join(root,"upgrade.sqlite"));
  settle(upgraded,now);
  const rebuilt=new LocalEventBuffer(path.join(root,"from-scratch.sqlite"));
  for(const row of upgraded.database.prepare(`select payload_json from buffered_events order by rowid`)
    .all() as Array<{payload_json:string}>)assert.equal(rebuilt.append(aiInteractionEventSchema.parse(JSON.parse(row.payload_json))),true);
  settle(rebuilt,now);
  assert.deepEqual(aggregates(upgraded),aggregates(rebuilt));
  assert.equal(upgraded.projection.status().degradedReason,null);
  assert.equal(upgraded.projection.readSnapshot(30).kind,"ready");
  // Later corruption is repaired without another upgrade; invalid source still refuses.
  upgraded.database.exec(`update dashboard_model_window set input_tokens=-1 where days=30;
    update dashboard_projection_control set dirty=1 where singleton=1`);
  upgraded.projection.runMaintenance(now);
  assert.equal(upgraded.projection.status().degradedReason,"projection_negative_total");
  upgraded.projection.runMaintenance(now);
  assert.equal(upgraded.projection.status().degradedReason,null,"next refresh repairs and publishes");
  settle(upgraded,now);assert.deepEqual(aggregates(upgraded),aggregates(rebuilt));
  // The current writer admits late arrivals and model changes under exactly
  // the same cutoff/cursor predicate as expiry, including the 7-day window.
  for(let i=0;i<40;i++)assert.equal(upgraded.append(event(start.toISOString())),true);
  settle(upgraded,now);
  const at=new Date("2026-11-09T12:00:00Z");
  let expiryRows=0;
  upgraded.projection.runMaintenance(at,{maxActiveMs:1,clock:()=>expiryRows,
    onWorkRowForProof:phase=>{if(phase==="expiry")expiryRows++;}});
  assert.notEqual((upgraded.database.prepare(`select target_cutoff_at from dashboard_window_control
    where days=7`).get() as {target_cutoff_at:string|null}).target_cutoff_at,null);
  Date.now=()=>at.getTime();
  const late=event("2026-11-02T00:00:00Z","gpt-6-astra");
  assert.equal(upgraded.append(late),true);
  upgraded.database.prepare(`update buffered_events set model='gpt-5.6-terra',input_tokens=19
    where id=?`).run(late.id);
  settle(upgraded,at);
  const fresh=new LocalEventBuffer(path.join(root,"after-expiry.sqlite"));
  for(const row of upgraded.database.prepare(`select payload_json,model,input_tokens from buffered_events order by rowid`)
    .all() as Array<{payload_json:string;model:string|null;input_tokens:number|null}>){
    assert.equal(fresh.append(aiInteractionEventSchema.parse({...JSON.parse(row.payload_json),
      model:row.model??undefined,inputTokens:row.input_tokens??undefined})),true);
  }
  settle(fresh,at);assert.deepEqual(aggregates(upgraded),aggregates(fresh));fresh.close();
  // Negative source evidence cannot be made consistent by an aggregate repair.
  upgraded.database.prepare(`update dashboard_event_facts set input_tokens=-100000 where projection_id=
    (select projection_id from dashboard_event_facts order by raw_rowid desc limit 1)`).run();
  upgraded.database.exec(`update dashboard_window_totals set input_tokens=-1 where days=30;
    update dashboard_projection_control set dirty=1 where singleton=1`);
  for(let i=0;i<8;i++)upgraded.projection.runMaintenance(at);
  assert.equal(upgraded.projection.status().parityReady,false);
  assert.equal(upgraded.projection.status().degradedReason,"projection_negative_total");
  const checks=["old expiry reproduces missing contribution","upgrade equals full rebuild",
      "snapshot publishes","negative model repaired on next refresh","late arrival and re-key equal full rebuild",
      "compacted history retained","invalid source still refuses"];
  console.log(JSON.stringify({status:"pass",windows:[7,30,90,182,365,1825],checks}));
  upgraded.close();rebuilt.close();
  for(const check of checks)completion.check(check);completion.complete();
}finally{Date.now=originalNow;fs.rmSync(root,{recursive:true,force:true});}}
void main();
