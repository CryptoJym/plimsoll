import { fixtureEpochId } from "./lib/fixture-epoch-id";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureRootDigest,durableClaudeRootSessionSightings } from
  "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";

const home=process.env.HOME!,plimsoll=process.env.PLIMSOLL_HOME!;
const now=Date.now(),sessionId=crypto.randomUUID(),id=crypto.randomUUID();
const B={rootId:"claude-b",profileId:"profile-b",installationEpochId:fixtureEpochId("epoch-b"),
  source:"claude_code" as const,directory:path.join(home,"missing-claude-b","projects")};
const config=collectorConfigSchema.parse({deviceId:"dev-r5-corrupt-cursor",
  uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[B]});
fs.mkdirSync(plimsoll,{recursive:true});
const file=path.join(plimsoll,"corrupt-cursor.sqlite");
const options={workspaceId:config.tenantId,deviceId:config.deviceId,
  enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:false}};
function raw(buffer:LocalEventBuffer) {
  return Boolean(buffer.database.prepare("select 1 from buffered_events where id=?").get(id));
}
function count(buffer:LocalEventBuffer) {
  return (buffer.database.prepare("select count(*) as n from buffered_events")
    .get() as {n:number}).n;
}
const seeded=new LocalEventBuffer(file,options);
try {
  const event=aiInteractionEventSchema.parse({id,source:"claude_code",
    eventType:"session_start",dataMode:"metadata",
    observedAt:new Date(now-60_000).toISOString(),sessionId,
    metadata:{captureRootId:B.rootId,captureProfileId:B.profileId}});
  assert.equal(seeded.append(event,[]),true);
  seeded.database.exec(`create table if not exists capture_root_observations (
    root_digest text not null,event_id text not null,payload_digest text not null,
    observed_at text not null,state text not null check(state in ('admitted','duplicate','conflict')),
    primary key(root_digest,event_id));
    create index if not exists idx_capture_root_event on capture_root_observations(event_id)`);
  seeded.database.prepare("insert into capture_root_observations values(?,?,?,?,?)")
    .run(captureRootDigest(B),id,"legacy-digest",new Date(now-60_000).toISOString(),"admitted");
  seeded.database.prepare("insert into capture_root_observations values(?,?,?,?,?)")
    .run("not-a-root-digest",crypto.randomUUID(),"orphaned-payload","not-a-date","admitted");
  // A damaged durable page cursor is recoverable in principle: promotion
  // inserts are idempotent, so replaying from rowid zero would be safe.
  seeded.database.prepare("insert into maintenance_state(key,value,updated_at) values(?,?,?)")
    .run("claude_legacy_sighting_promotion_v1","not-a-rowid",new Date().toISOString());
} finally { seeded.close(); }
const errors:string[]=[];
const rawAfter:boolean[]=[];
const compactAfter:number[]=[];
const resetsAfter:number[]=[];
for(let pass=0;pass<2;pass++) {
  const reopened=new LocalEventBuffer(file,options);
  try {
    try { reopened.prune(0,{now:new Date(now+86_400_000),maxRows:10}); }
    catch(error) { errors.push((error as Error).message); }
    rawAfter.push(raw(reopened));
    compactAfter.push(durableClaudeRootSessionSightings(reopened.database,sessionId).size);
    resetsAfter.push(Number((reopened.database.prepare("select value from maintenance_state where key=?")
      .get("claude_legacy_sighting_cursor_reset_count_v1") as {value:string}|undefined)?.value??0));
  } finally { reopened.close(); }
}
console.log(JSON.stringify({scenario:"corrupt_promotion_cursor_across_reopens",
  rootFileExists:fs.existsSync(B.directory),orphanObservationPresent:true,
  errors,rawAfter,compactAfter,resetsAfter}));
assert.deepEqual(errors,[],"one invalid page cursor blocks every retention pass until manual repair");
assert.deepEqual(rawAfter,[false,false]);
assert.deepEqual(compactAfter,[1,1]);
assert.deepEqual(resetsAfter,[1,1]);
