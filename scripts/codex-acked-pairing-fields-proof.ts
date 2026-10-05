import assert from "node:assert/strict";
import fs from "node:fs";import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { withReader,proofTempRoot } from "./lib/legacy-reader";
import { createProofCompletion } from "./lib/proof-completion";
const root=proofTempRoot("acked-pairing-fields"),completion=createProofCompletion("codex-acked-pairing-fields",3),AT=Date.now()-900000;
const attr=(key:string,value:string|number)=>({key,value:typeof value==="string"?{stringValue:value}:Number.isInteger(value)?{intValue:String(value)}:{doubleValue:value}});
const session="22222222-2222-4222-8222-222222222222",trace="a".repeat(32);
function log(){return explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attr("service.name","codex-app-server")]},scopeLogs:[{logRecords:[{
 timeUnixNano:String(BigInt(AT+6000)*1000000n),traceId:trace,attributes:[attr("event.name","codex.sse_event"),attr("conversation.id",session),attr("request_id","acked-pair-response"),attr("model","gpt-6.1-sol"),attr("input_token_count",19),attr("output_token_count",2)]}]}]}]},
 {source:"codex",transportPath:"/v1/logs"}).events[0]!.event;}
function span(){return explodeOtlpPayload({resourceSpans:[{resource:{attributes:[attr("service.name","codex-app-server")]},scopeSpans:[{spans:[{
 name:"handle_responses",traceId:trace,spanId:"1".repeat(16),startTimeUnixNano:String(BigInt(AT+5000)*1000000n),endTimeUnixNano:String(BigInt(AT+6000)*1000000n),
 attributes:[attr("conversation.id",session),attr("request_id","acked-pair-response"),attr("gen_ai.usage.input_tokens",19),attr("gen_ai.usage.output_tokens",2),
 attr("gen_ai.usage.cache_read_tokens",3),attr("gen_ai.usage.cache_creation_input_tokens",5),attr("cost_usd",.125)]}]}]}]},
 {source:"codex",transportPath:"/v1/traces"}).events[0]!.event;}
async function fixture(Reader:any,version:string){let b:any,now=new Date(AT+100000);const file=path.join(root,version+".sqlite");
 const opts={workspaceId:"11111111-1111-4111-8111-111111111111",deviceId:"acked-pair-fixture",enrollmentNow:()=>new Date(AT-1000000),delivery:{enabled:true,now:()=>now}};
 const saved=new Map<string,any>();
 const drain=()=>{for(let n=0;n<8;n++){now=new Date(now.getTime()+123000);const lease=b.delivery.lease({now});assert.equal(lease.locallyDead,0);
 for(const item of lease.items){const prior=saved.get(item.deliveryId);if(prior)assert.equal(item.envelopeJson,prior.envelopeJson);else saved.set(item.deliveryId,item);}b.delivery.acknowledge(lease.leaseId,lease.items.map((i:any)=>i.deliveryId),now);if(!lease.items.length)break;}};
 const old=log(),raw=()=>JSON.stringify(b.database.prepare("select payload_json,input_tokens,output_tokens,cache_read_tokens,cache_creation_tokens,cost_usd,usage_duplicate_reason from buffered_events where id=?").get(old.id));
 try {b=new Reader(file,opts);assert.equal(b.append(old),true);drain();const before=raw();b.close();b=new LocalEventBuffer(file,opts);
 assert.equal(b.append(span()),true);drain();assert.equal(raw(),before,"pairing must not rewrite accepted raw financial fields");
 const totals=Object.fromEntries(["inputTokens","outputTokens","cacheReadTokens","cacheCreationTokens","costUsd"].map(k=>[k,[...saved.values()].reduce((n,i)=>n+(i.envelope.event[k]??0),0)]));
 assert.deepEqual(totals,{inputTokens:19,outputTokens:2,cacheReadTokens:3,cacheCreationTokens:5,costUsd:.125},"new fields get their own named remainder");
 completion.check(version);
 }finally{b?.close();}
}
async function main(){await fixture(LocalEventBuffer,"head");for(const [version,commit] of [["0.7.47","a60590559403cace3db7cbbda49812c9e3dbfe62"],["0.7.48","34d58bcd90865679e09fcbd1ee1703de5effda97"]])await withReader(commit,({Buffer:Reader})=>fixture(Reader,version));completion.complete();}
main().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>fs.rmSync(root,{recursive:true,force:true}));
