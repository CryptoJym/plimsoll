import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { withReader, proofTempRoot } from "./lib/legacy-reader";
import { createProofCompletion } from "./lib/proof-completion";
const completion=createProofCompletion("codex-legacy-acked-fields",8);
const root=proofTempRoot("legacy-acked-fields"),AT=Date.now()-900000;
const fields=["inputTokens","outputTokens","cacheReadTokens","cacheCreationTokens","costUsd"] as const;
const attr=(key:string,value:string|number)=>({key,value:typeof value==="string"?{stringValue:value}:Number.isInteger(value)?{intValue:String(value)}:{doubleValue:value}});
const keys={inputTokens:"input_token_count",outputTokens:"output_token_count",cacheReadTokens:"cached_token_count",cacheCreationTokens:"gen_ai.usage.cache_creation_input_tokens",costUsd:"cost_usd"};
function event(at:number,amounts:Partial<Record<typeof fields[number],number>>) {
  return explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attr("service.name","codex-app-server")]},scopeLogs:[{logRecords:[{
    timeUnixNano:String(BigInt(AT+at)*1000000n),traceId:"a".repeat(32),attributes:[attr("event.name","codex.sse_event"),
      attr("conversation.id","22222222-2222-4222-8222-222222222222"),attr("request_id","old-acked-response"),attr("model","gpt-6.1-sol"),
      ...Object.entries(amounts).map(([k,v])=>attr(keys[k as keyof typeof keys],v))]}]}]}]},
    {source:"codex",transportPath:"/v1/logs"}).events[0]!.event;
}
async function fixture(Reader:any,version:string,reverse:boolean,allFields:boolean) {
  const file=path.join(root,`${version}-${reverse}-${allFields}.sqlite`);
  let now=new Date(AT+100000),b:any;
  const options={workspaceId:"11111111-1111-4111-8111-111111111111",deviceId:"legacy-fields-fixture",enrollmentNow:()=>new Date(AT-1000000),delivery:{enabled:true,now:()=>now}};
  const first=event(6000,{inputTokens:19,...(allFields?{cacheReadTokens:3}:{})});
  const second=event(11000,{outputTokens:2,...(allFields?{cacheCreationTokens:5,costUsd:.125}:{})});
  const saved=new Map<string,any>();
  const drain=()=>{for(let n=0;n<8;n++) {
    now=new Date(now.getTime()+123000);const lease=b.delivery.lease({now});assert.equal(lease.locallyDead,0);
    for(const item of lease.items) {const prior=saved.get(item.deliveryId);if(prior)assert.equal(item.envelopeJson,prior.envelopeJson);else saved.set(item.deliveryId,item);}
    b.delivery.acknowledge(lease.leaseId,lease.items.map((i:any)=>i.deliveryId),now);if(!lease.items.length)break;
  }};
  const raw=()=>b.database.prepare("select id,uploaded_at,input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,cost_usd,usage_duplicate_reason,payload_json from buffered_events where id in (?,?) order by id").all(first.id,second.id);
  const totals=()=>Object.fromEntries(fields.filter(k=>[...saved.values()].some(i=>i.envelope.event[k]!==undefined))
    .map(k=>[k,[...saved.values()].reduce((n,i)=>n+(i.envelope.event[k]??0),0)]));
  const expected={inputTokens:19,outputTokens:2,...(allFields?{cacheReadTokens:3,cacheCreationTokens:5,costUsd:.125}:{})};
  try {
    // The ledger first exists under the ACTUAL released schema and writer.
    b=new Reader(file,options);
    for(const e of reverse?[second,first]:[first,second]){assert.equal(b.append(e),true);drain();}
    assert.deepEqual(totals(),expected);const original=JSON.stringify(raw());assert.ok(raw().every((r:any)=>r.uploaded_at));
    b.close();b=new LocalEventBuffer(file,options);
    assert.equal(b.append(event(5000,expected)),true);drain();
    for(let n=0;n<40;n++){b.projection.runMaintenance(now);const status=b.projection.status();if(status.parityReady&&!status.dirty)break;}
    assert.deepEqual(totals(),expected,"new named IDs must not repeat old ACKed fields");
    assert.equal(JSON.stringify(raw()),original,"ACKed raw finance and diagnostics stay byte-for-byte unchanged");
    assert.equal(b.database.prepare("select count(*) as n from codex_named_captures where raw_id in (?,?)").get(first.id,second.id).n,0,
      "missing released envelope bytes must not become invented named witnesses");
    const projection=b.database.prepare("select sum(input_tokens) as inputTokens,sum(output_tokens) as outputTokens,sum(cache_read_tokens) as cacheReadTokens,sum(cache_creation_tokens) as cacheCreationTokens,sum(cost_nanos)/1e9 as costUsd from dashboard_event_facts where source='codex'").get();
    for(const [k,v] of Object.entries(expected))assert.ok(Math.abs(projection[k]-v)<1e-12,"projection retains each old ACK field "+k);
    completion.check(`${version}-${reverse?"output-first":"input-first"}-${allFields?"five-fields":"tokens"}`);
  } finally {b?.close();}
}
async function main(){
  for(const [version,commit] of [["0.7.47","a60590559403cace3db7cbbda49812c9e3dbfe62"],["0.7.48","34d58bcd90865679e09fcbd1ee1703de5effda97"]])
    await withReader(commit,async({Buffer:Reader})=>{for(const reverse of [false,true])for(const allFields of [false,true])await fixture(Reader,version,reverse,allFields);});
  completion.complete();
}
main().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>fs.rmSync(root,{recursive:true,force:true}));
