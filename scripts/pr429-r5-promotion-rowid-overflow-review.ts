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
const config=collectorConfigSchema.parse({deviceId:"dev-r5-overflow-rowid",
  uploadUrl:"http://127.0.0.1:1/unused",captureRoots:[B]});
fs.mkdirSync(plimsoll,{recursive:true});
const file=path.join(plimsoll,"overflow-rowid.sqlite");
const options={workspaceId:config.tenantId,deviceId:config.deviceId,
  enrollmentNow:()=>new Date(now-3_600_000),delivery:{enabled:false}};
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
  // SQLite accepts this rowid, but JavaScript cannot represent it as a safe
  // pagination cursor. Its payload is deliberately orphaned and malformed.
  seeded.database.prepare(`insert into capture_root_observations
    (rowid,root_digest,event_id,payload_digest,observed_at,state) values(?,?,?,?,?,?)`)
    .run(9_007_199_254_740_992n,"malformed-root",crypto.randomUUID(),"orphan","not-a-date","admitted");
} finally { seeded.close(); }
const passes:Array<{receipt:unknown;error:string|null;cursor:string|null;raw:boolean;compact:number;skipped:number}>=[];
for(let i=0;i<4;i++) {
  const reopened=new LocalEventBuffer(file,options);
  try {
    let receipt:unknown=null,error:string|null=null;
    try { receipt=reopened.prune(0,{now:new Date(now+86_400_000),maxRows:1}); }
    catch(caught) { error=(caught as Error).message; }
    const cursor=(reopened.database.prepare("select value from maintenance_state where key=?")
      .get("claude_legacy_sighting_promotion_v1") as {value:string}|undefined)?.value??null;
    const raw=Boolean(reopened.database.prepare("select 1 from buffered_events where id=?").get(id));
    const compact=durableClaudeRootSessionSightings(reopened.database,sessionId).size;
    const skipped=Number((reopened.database.prepare("select value from maintenance_state where key=?")
      .get("claude_legacy_sighting_row_skipped_count_v1") as {value:string}|undefined)?.value??0);
    passes.push({receipt,error,cursor,raw,compact,skipped});
  } finally { reopened.close(); }
}
console.log(JSON.stringify({scenario:"orphan_legacy_observation_unsafe_rowid",passes,
  rootFileExists:fs.existsSync(B.directory)}));
assert.deepEqual(passes.map(pass=>pass.error),[null,null,null,null],
  "an orphan observation with a legal SQLite rowid blocked retention after cursor persistence");
assert.equal(passes.at(-1)?.cursor,"9007199254740992");
assert.equal(passes.at(-1)?.skipped,1);
