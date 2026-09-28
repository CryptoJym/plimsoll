/** A locally durable crash fault withdraws the old claim before new events. */
import assert from "node:assert/strict";
import {createHash} from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { advanceCaptureFrontier, CAPTURE_WRITE_LAG_MS } from "../packages/collector-cli/src/capture-frontier";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { declareUnresolvedFileGap, rolloutGapScope } from "../packages/collector-cli/src/lean/capture-gaps";
import { readActivitySummaryAdvertisement } from "../packages/collector-cli/src/lean/activity-summary-capability";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir(),"b22-v2-"));
const tenantId="00000000-0000-4000-8000-0000000000a1";
const cloudDeviceId="00000000-0000-4000-8000-0000000000a2";
const installKey="pli_b22_v2_fixture_install";
const config=collectorConfigSchema.parse({tenantId,installKey,cloudDeviceId,
  uploadUrl:"http://127.0.0.1:49772/api/work-intelligence/ingest",
  delivery:{maxOldestAgeDays:3650}});
const requests:Array<{body:Record<string,unknown>;raw:string;claim:Record<string,unknown>}> = [];
let advertisement=true;
let badGapDigest=false;
let v2Unavailable=false;
const fetchImpl:typeof fetch=async (_input,init)=>{
  const raw=String(init?.body??"");
  const body=JSON.parse(raw) as Record<string,unknown>;
  const claim=JSON.parse(new Headers(init?.headers).get("x-plimsoll-capture")??"null") as Record<string,unknown>;
  requests.push({body,raw,claim});
  if(body.kind==="activity_summary_v2") {
    if(v2Unavailable) return new Response(JSON.stringify({error:"activity_summary_v2_unavailable"}),
      {status:503,headers:{"content-type":"application/json","retry-after":"60"}});
    const items=body.items as Array<{kind:string;gapId:string;revision:number}>;
    const canonical=(value:unknown):unknown=>Array.isArray(value)?value.map(canonical)
      :value&&typeof value==="object"?Object.fromEntries(Object.entries(value)
        .sort(([a],[b])=>a.localeCompare(b)).map(([key,item])=>[key,canonical(item)])):value;
    return new Response(JSON.stringify({ok:true,receipts:items.map(item=>({
      kind:item.kind,key:item.gapId,revision:item.revision,
      digest:badGapDigest?"f".repeat(32):createHash("sha256")
        .update(JSON.stringify(canonical(item))).digest("hex").slice(0,32),
      status:"held",coverage:"unknown",segments:[],
    })),capture:{status:"advanced",cursor:claim.cursor,state:"pending",acceptedThrough:null},
      actorBindingVersion:3,deviceId:cloudDeviceId,
      ...(advertisement?{activitySummaryContractVersion:2}:{})}),
      {status:200,headers:{"content-type":"application/json"}});
  }
  const expected=deliveryExpectation(raw,installKey);
  return new Response(JSON.stringify({ok:true,accepted:expected.itemIds.length,
    ack:deliveryAcknowledgement(expected,expected.itemIds),actorBindingVersion:3,
    deviceId:cloudDeviceId,
    ...(advertisement?{activitySummaryContractVersion:2}:{})}),
    {status:200,headers:{"content-type":"application/json"}});
};
const file=path.join(root,"ledger.sqlite");
const spoolHome=path.join(root,"spools");
let sequence=0;
const makeBuffer=()=>new LocalEventBuffer(file,{workspaceId:tenantId,
  delivery:{enabled:true,limits:{maxOldestAgeDays:3650}},
  enrollmentNow:()=>new Date(Date.now()-3*60*60*1000)});
const append=(buffer:LocalEventBuffer)=>{
  sequence++;
  buffer.append(aiInteractionEventSchema.parse({
    id:`00000000-0000-4000-8000-${String(sequence).padStart(12,"0")}`,
    sessionId:`00000000-0000-4000-8000-${String(sequence+100).padStart(12,"0")}`,
    source:"codex",dataMode:"metadata",eventType:"assistant_response",
    observedAt:new Date(Date.now()-2*60*60*1000).toISOString(),
    actionClass:"other",inputTokens:1,outputTokens:1,
  }));
};
async function main(){
let buffer=makeBuffer();
try{
  append(buffer);
  const frontierAt=new Date(Date.now()+CAPTURE_WRITE_LAG_MS).toISOString();
  for(const source of ["codex","claude_code","grok"] as const)
    advanceCaptureFrontier(buffer.database,source,{complete:true,files:[]},frontierAt);
  buffer.captureDurability.markFreshWalkComplete();
  await uploadBufferedEvents(config,buffer,{fetchImpl,spoolHome});
  assert.equal(requests[0]?.body.kind,undefined);
  assert.equal(readActivitySummaryAdvertisement(buffer.database,installKey).enabled,true);
  const scope=rolloutGapScope(buffer.database);
  buffer.database.exec(`create trigger b22_v2_fail before insert on capture_gaps
    begin select raise(abort,'injected'); end`);
  assert.throws(()=>buffer.transactionWithRepoContextHandoffs(()=>declareUnresolvedFileGap(buffer.database,{
    ...scope,source:"codex",fileKeyDigest:"c".repeat(64),reason:"tailer_unread",
    lastWriteAtMs:Date.now(),unreadBytes:1,generationIdentity:"v2-proof",
  })),/gap_record_unavailable/);
  buffer.database.exec("drop trigger b22_v2_fail");
  const faultId=buffer.captureDurability.status().faults[0]!.faultId;
  buffer.close();
  buffer=makeBuffer();
  assert.equal(buffer.captureDurability.status().restartUnverified,true);
  append(buffer);
  requests.length=0;
  await uploadBufferedEvents(config,buffer,{fetchImpl,spoolHome});
  assert.equal(requests[0]?.body.kind,"activity_summary_v2");
  assert.deepEqual(requests[0]?.body.items,[]);
  assert.equal(requests[0]?.claim.v,2);
  assert.equal(requests[0]?.claim.through,null);
  assert.equal(requests[0]?.claim.unattested,"restart_unverified");
  assert.ok((requests[0]?.claim.faults as Array<{faultId:string}>).some(f=>f.faultId===faultId));
  assert.ok(requests.findIndex(request=>request.body.kind===undefined)>0,
    "event follows the withdrawal and any gap items");
  console.log("PASS crash_fault_zero_item_v2_withdrawal_precedes_event");

  buffer.transactionWithRepoContextHandoffs(()=>declareUnresolvedFileGap(buffer.database,{
    ...scope,source:"codex",fileKeyDigest:"c".repeat(64),reason:"tailer_unread",
    lastWriteAtMs:Date.now(),unreadBytes:1,generationIdentity:"v2-proof",
  }));
  requests.length=0;
  await uploadBufferedEvents(config,buffer,{fetchImpl,spoolHome});
  assert.equal(requests[0]?.body.kind,"activity_summary_v2");
  assert.ok(requests.some(request=>Array.isArray(request.body.items) &&
    (request.body.items as unknown[]).length>0),"pending gap gets a summary receipt");
  assert.equal((buffer.database.prepare("select count(*) as n from capture_gaps where upload_state='pending'")
    .get() as {n:number}).n,0);
  console.log("PASS pending_gap_receipt_acks_local_gap_revision");

  buffer.transactionWithRepoContextHandoffs(()=>declareUnresolvedFileGap(buffer.database,{
    ...scope,source:"codex",fileKeyDigest:"c".repeat(64),reason:"tailer_unread",
    lastWriteAtMs:Date.now(),unreadBytes:2,generationIdentity:"v2-proof",
  }));
  badGapDigest=true;
  await assert.rejects(()=>uploadBufferedEvents(config,buffer,{fetchImpl,spoolHome}),
    /activity_summary_v2_gap_receipt_incomplete/);
  assert.ok((buffer.database.prepare("select count(*) as n from capture_gaps where upload_state='pending'")
    .get() as {n:number}).n>0);
  badGapDigest=false;
  await uploadBufferedEvents(config,buffer,{fetchImpl,spoolHome});
  assert.equal((buffer.database.prepare("select count(*) as n from capture_gaps where upload_state='pending'")
    .get() as {n:number}).n,0);
  console.log("PASS mismatched_digest_cannot_clear_gap_and_retry_can");

  advertisement=false;
  append(buffer);
  requests.length=0;
  await uploadBufferedEvents(config,buffer,{fetchImpl,spoolHome});
  assert.equal(readActivitySummaryAdvertisement(buffer.database,installKey).enabled,false);
  append(buffer);
  requests.length=0;
  await uploadBufferedEvents(config,buffer,{fetchImpl,spoolHome});
  assert.ok(requests.every(request=>request.body.kind===undefined));
  assert.equal(requests[0]?.claim.v,1);
  assert.equal(requests[0]?.claim.through,null);
  console.log("PASS advertisement_withdrawal_falls_back_to_v1");

  // The receiver's server-side switch can be rolled back while our last
  // advertisement still says 2. Its explicit 503 also withdraws capability.
  advertisement=true;
  const {observeActivitySummaryAdvertisement}=await import("../packages/collector-cli/src/lean/activity-summary-capability");
  observeActivitySummaryAdvertisement(buffer.database,installKey,
    {activitySummaryContractVersion:2,actorBindingVersion:3,deviceId:cloudDeviceId});
  v2Unavailable=true;
  advertisement=false;
  append(buffer);
  requests.length=0;
  await uploadBufferedEvents(config,buffer,{fetchImpl,spoolHome});
  assert.equal(requests[0]?.body.kind,"activity_summary_v2");
  assert.equal(requests[1]?.body.kind,undefined);
  assert.equal(readActivitySummaryAdvertisement(buffer.database,installKey).enabled,false);
  console.log("PASS disabled_receiver_503_reverts_to_v1");
}finally{try{buffer.close();}catch{}fs.rmSync(root,{recursive:true,force:true});}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
