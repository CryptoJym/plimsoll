import { fixtureEpochId } from "./lib/fixture-epoch-id";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer as HeadBuffer } from "../packages/collector-cli/src/buffer";
import { claudeDispatchSkipStatus, dispatchBindingSchema,durableClaudeRootSessionSightings,
  recordClaudeRootSessionSighting, captureRootDigest } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { normalizeForwardedHook } from "../packages/collector-cli/src/forwarder";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";
import { createProofCompletion } from "./lib/proof-completion";

const home=process.env.HOME!,plimsoll=process.env.PLIMSOLL_HOME!;
const now=Date.now(),observedAt=new Date(now-60_000).toISOString();
const A={rootId:"claude-a",profileId:"profile-a",installationEpochId:fixtureEpochId("epoch-a"),
  source:"claude_code" as const,directory:path.join(home,".claude-a","projects")};
const B={rootId:"claude-b",profileId:"profile-b",installationEpochId:fixtureEpochId("epoch-b"),
  source:"claude_code" as const,directory:path.join(home,".claude-b","projects")};
for(const root of [A,B]) fs.mkdirSync(root.directory,{recursive:true});
fs.mkdirSync(plimsoll,{recursive:true});
const base=collectorConfigSchema.parse({deviceId:"dev_pr429-r4-legacy",
  uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[A,B]});
const options={workspaceId:base.tenantId,deviceId:base.deviceId,
  enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:false}};
const binding=(sessionId:string)=>dispatchBindingSchema.parse({sessionId,
  workItemId:"beads:eco-6hoxj.165.97",projectKey:`sha256:${"a".repeat(64)}`,
  companyRef:null,attemptId:"11111111-1111-4111-8111-111111111111",
  parentAttemptId:null,acceptedOutcomeId:null,
  validFrom:new Date(now-120_000).toISOString(),validUntil:null,
  evidenceRef:"dispatch:r4-legacy-promotion"});
function seed(label:string) {
  const file=path.join(plimsoll,`${label}.sqlite`),sessionId=crypto.randomUUID(),
    eventId=crypto.randomUUID();
  const old=new HeadBuffer(file,options);
  try {
    const event=aiInteractionEventSchema.parse({id:eventId,source:"claude_code",
      eventType:"session_start",dataMode:"metadata",observedAt,sessionId,
      metadata:{captureRootId:B.rootId,captureProfileId:B.profileId}});
    assert.equal(old.append(event,[]),true);
    // Exact 0.7.44 observation shape: raw session ID plus root digest, with
    // no compact session sighting. The cross-version gate opens a real older
    // binary separately; this fixture keeps the named proof self-contained.
    old.database.exec(`create table if not exists capture_root_observations (
      root_digest text not null,event_id text not null,payload_digest text not null,
      observed_at text not null,state text not null check(state in ('admitted','duplicate','conflict')),
      primary key(root_digest,event_id));
      create index if not exists idx_capture_root_event on capture_root_observations(event_id)`);
    old.database.prepare("insert into capture_root_observations values(?,?,?,?,?)")
      .run(captureRootDigest(B),eventId,"legacy-digest",observedAt,"admitted");
    assert.equal(Boolean(old.database.prepare("select 1 from capture_root_observations where event_id=?")
      .get(eventId)),true);
    assert.equal(Boolean(old.database.prepare("select 1 from sqlite_master where name='capture_root_session_sightings'")
      .get()),false);
  } finally { old.close(); }
  return {file,sessionId,eventId};
}
function compact(buffer:HeadBuffer,sessionId:string) {
  const table=Boolean(buffer.database.prepare("select 1 from sqlite_master where name='capture_root_session_sightings'").get());
  return table ? (buffer.database.prepare(`select count(*) as n from capture_root_session_sightings
    where source='claude_code' and session_id=?`).get(sessionId) as {n:number}).n : 0;
}
function raw(buffer:HeadBuffer,id:string) {
  return Boolean(buffer.database.prepare("select 1 from buffered_events where id=?").get(id));
}
function hook(buffer:HeadBuffer,sessionId:string,save=false) {
  const config=collectorConfigSchema.parse({...base,captureRoots:[{...A,dispatch:[binding(sessionId)]},B]});
  const configFile=path.join(plimsoll,"collector.config.json");
  fs.writeFileSync(`${configFile}.next`,JSON.stringify(config)+"\n");
  fs.renameSync(`${configFile}.next`,configFile);
  const before=claudeDispatchSkipStatus().otherRootSeen;
  const event=normalizeForwardedHook({id:crypto.randomUUID(),hook_event_name:"AssistantResponse",
    session_id:sessionId,timestamp:observedAt},{config,buffer,source:"claude_code",now:()=>now}).event;
  if(save) assert.equal(buffer.append(event,[]),true);
  return {work:event.metadata.workItemId??null,
    otherRootSeenDelta:claudeDispatchSkipStatus().otherRootSeen-before,
    id:event.id};
}
function savedWork(buffer:HeadBuffer,id:string) {
  const row=buffer.database.prepare("select payload_json as payload from buffered_events where id=?")
    .get(id) as {payload:string};
  return JSON.parse(row.payload).metadata.workItemId??null;
}

if(process.argv[2]==="promotion-child") {
  const file=process.env.PR429_LEGACY_FILE!,sessionId=process.env.PR429_LEGACY_SESSION!;
  const buffer=new HeadBuffer(file,options);
  const database=buffer.database,original=database.prepare.bind(database);
  database.prepare=((sql:string)=>{
    const statement=original(sql);
    if(!sql.includes("insert into capture_root_session_sightings")) return statement;
    return new Proxy(statement,{get(target,key){
      if(key==="run") return (...args:unknown[])=>{
        const value=(target.run as (...values:unknown[])=>unknown).apply(target,args);
        process.stdout.write("SIGKILL_DURING_LEGACY_PROMOTION\n");
        process.kill(process.pid,"SIGKILL");
        return value;
      };
      const value=Reflect.get(target,key);
      return typeof value==="function"?value.bind(target):value;
    }});
  }) as typeof database.prepare;
  // Interrupt the actual upgrade page, after its insert but before its
  // cursor/insert transaction commits.
  buffer.prune(0,{now:new Date(now+86_400_000),maxRows:10});
  process.exit(98);
} else {
  const proof=createProofCompletion("pr429-r4-legacy-promotion",5);
  const normal=seed("normal");
  const opened=new HeadBuffer(normal.file,options);
  try {
    assert.equal(raw(opened,normal.eventId),true);
    assert.equal(compact(opened,normal.sessionId),0);
    const before=hook(opened,normal.sessionId);
    const seen=durableClaudeRootSessionSightings(opened.database,normal.sessionId);
    assert.equal(seen.has(captureRootDigest(B)),true);
    assert.equal(before.work,null);
    assert.equal(before.otherRootSeenDelta,1);
    assert.equal(recordClaudeRootSessionSighting(opened,B,normal.sessionId,observedAt),true);
    assert.equal(compact(opened,normal.sessionId),1);
    const pruned=opened.prune(0,{now:new Date(now+86_400_000),maxRows:10});
    assert.equal(raw(opened,normal.eventId),false);
    console.log(JSON.stringify({scenario:"explicit_promotion_then_retention",before,
      after:hook(opened,normal.sessionId),compact:compact(opened,normal.sessionId),pruned}));
    proof.check("explicit_promotion_survives_retention");
  } finally { opened.close(); }

  const live=seed("live-race");
  const promoter=new HeadBuffer(live.file,options);
  const observer=new HeadBuffer(live.file,options);
  try {
    const original=promoter.database.prepare.bind(promoter.database);
    let racing:ReturnType<typeof hook>|undefined;
    promoter.database.prepare=((sql:string)=>{
      const statement=original(sql);
      if(!sql.includes("insert into capture_root_session_sightings")) return statement;
      return new Proxy(statement,{get(target,key) {
        if(key==="run") return (...args:unknown[])=>{
          const value=(target.run as (...values:unknown[])=>unknown).apply(target,args);
          // A second connection reads while the promoter's insert is uncommitted.
          racing=hook(observer,live.sessionId);
          return value;
        };
        const value=Reflect.get(target,key);
        return typeof value==="function"?value.bind(target):value;
      }});
    }) as typeof promoter.database.prepare;
    assert.equal(recordClaudeRootSessionSighting(promoter,B,live.sessionId,observedAt),true);
    const after=hook(observer,live.sessionId);
    console.log(JSON.stringify({scenario:"live_hook_during_legacy_promotion",
      racing,after,compact:compact(observer,live.sessionId)}));
    assert.equal(racing?.work,null);
    assert.equal(racing?.otherRootSeenDelta,1);
    assert.equal(after.work,null);
    assert.equal(compact(observer,live.sessionId),1);
    proof.check("hook_racing_uncommitted_promotion_stays_unbound");
  } finally { observer.close(); promoter.close(); }

  const quiet=seed("quiet");
  const first=new HeadBuffer(quiet.file,options);
  try {
    const before=hook(first,quiet.sessionId);
    assert.equal(before.work,null);
    assert.equal(compact(first,quiet.sessionId),0,
      "opening a quiet 0.7.44 ledger should reveal whether migration occurred");
    const pruned=first.prune(0,{now:new Date(now+86_400_000),maxRows:10});
    assert.equal(raw(first,quiet.eventId),false);
    console.log(JSON.stringify({scenario:"quiet_legacy_retention_before_promotion",
      before,compactBefore:compact(first,quiet.sessionId),pruned}));
  } finally { first.close(); }
  const restarted=new HeadBuffer(quiet.file,options);
  try {
    const after=hook(restarted,quiet.sessionId,true);
    const legacySeen=durableClaudeRootSessionSightings(restarted.database,quiet.sessionId).size;
    const promoted=recordClaudeRootSessionSighting(restarted,B,quiet.sessionId,observedAt);
    const afterPromotion=hook(restarted,quiet.sessionId);
    const persistedWork=savedWork(restarted,after.id);
    console.log(JSON.stringify({scenario:"hook_wins_retention_promotion_race",
      after,legacySeen,promoted,compact:compact(restarted,quiet.sessionId),
      afterPromotion,persistedWork}));
    assert.equal(after.work,null,"quiet legacy B observation was erased by retention before promotion");
    assert.equal(persistedWork,null);
    assert.equal(legacySeen,1);
    assert.equal(promoted,false);
    assert.equal(afterPromotion.work,null);
    proof.check("quiet_legacy_observation_promoted_before_prune");
  } finally { restarted.close(); }

  const interrupted=seed("interrupted");
  const loader=path.resolve("node_modules/tsx/dist/loader.mjs");
  const child=spawnSync(process.execPath,["--import",loader,path.resolve(process.argv[1]),
    "promotion-child"],{cwd:process.cwd(),encoding:"utf8",timeout:30_000,
      env:{...process.env,PR429_LEGACY_FILE:interrupted.file,
        PR429_LEGACY_SESSION:interrupted.sessionId}});
  assert.equal(child.signal,"SIGKILL",`${child.status} ${child.stderr}`);
  assert.match(child.stdout,/SIGKILL_DURING_LEGACY_PROMOTION/);
  const afterKill=new HeadBuffer(interrupted.file,options);
  try {
    const before=hook(afterKill,interrupted.sessionId);
    console.log(JSON.stringify({scenario:"sigkill_during_promotion_before_retention",
      compact:compact(afterKill,interrupted.sessionId),raw:raw(afterKill,interrupted.eventId),
      before}));
    assert.equal(compact(afterKill,interrupted.sessionId),0);
    assert.equal(raw(afterKill,interrupted.eventId),true);
    assert.equal(before.work,null);
    afterKill.prune(0,{now:new Date(now+86_400_000),maxRows:10});
  } finally { afterKill.close(); }
  const afterPrune=new HeadBuffer(interrupted.file,options);
  try {
    const after=hook(afterPrune,interrupted.sessionId);
    console.log(JSON.stringify({scenario:"sigkill_promotion_then_retention_then_hook",
      compact:compact(afterPrune,interrupted.sessionId),raw:raw(afterPrune,interrupted.eventId),after}));
    assert.equal(after.work,null);
    assert.equal(compact(afterPrune,interrupted.sessionId),1);
    assert.equal(raw(afterPrune,interrupted.eventId),false);
    proof.check("interrupted_upgrade_retains_raw_then_promotes_before_prune");
  } finally { afterPrune.close(); }

  const bounded=seed("bounded");
  const sessions=[bounded.sessionId];
  const writer=new HeadBuffer(bounded.file,options);
  try {
    for(let i=0;i<2;i++) {
      const id=crypto.randomUUID(),sessionId=crypto.randomUUID();
      sessions.push(sessionId);
      const event=aiInteractionEventSchema.parse({id,source:"claude_code",
        eventType:"session_start",dataMode:"metadata",observedAt,sessionId,
        metadata:{captureRootId:B.rootId,captureProfileId:B.profileId}});
      assert.equal(writer.append(event,[]),true);
      writer.database.prepare("insert into capture_root_observations values(?,?,?,?,?)")
        .run(captureRootDigest(B),id,`legacy-${i}`,observedAt,"admitted");
    }
  } finally { writer.close(); }
  for(let page=0;page<3;page++) {
    const resumed=new HeadBuffer(bounded.file,options);
    try {
      const pass=resumed.prune(0,{now:new Date(now+86_400_000),maxRows:1});
      assert.equal(pass.events,0,"retention must wait for the bounded upgrade");
      assert.equal(pass.hasMore,true);
      assert.equal((resumed.database.prepare("select count(*) as n from buffered_events")
        .get() as {n:number}).n,3);
    } finally { resumed.close(); }
  }
  const complete=new HeadBuffer(bounded.file,options);
  try {
    const pass=complete.prune(0,{now:new Date(now+86_400_000),maxRows:1});
    assert.equal(pass.events,1);
    for(const sessionId of sessions)
      assert.equal(durableClaudeRootSessionSightings(complete.database,sessionId).size,1);
    console.log(JSON.stringify({scenario:"bounded_upgrade_resumes_across_reopens",
      pages:3,firstRetentionEvents:pass.events,compactSessions:sessions.length}));
  } finally { complete.close(); }
  proof.check("bounded_upgrade_resumes_before_first_retention_pass");
  proof.complete();
}
