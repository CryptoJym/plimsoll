import { ensureSessionSummarySchema, updateSessionSummary } from "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { RolloutTailer } from '../packages/collector-cli/src/rollout-tailer';
import { explodeOtlpPayload } from '../packages/collector-cli/src/otlp';
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline, sealCaptureBaselineGenerations } from '../packages/collector-cli/src/capture-baseline';
import { deriveCaptureRootIdentity } from '../packages/collector-cli/src/capture-root-inventory';
import { planCaptureHistory, applyCaptureHistory } from '../packages/collector-cli/src/capture-history-import';

const base=false;
const Reader=LocalEventBuffer, Tailer=RolloutTailer;
const completion=createProofCompletion('codex-response-coverage',22);
const root=fs.mkdtempSync(path.join(os.tmpdir(),'r8-own-'));
const AT=Date.now()-600000, SESSION='22222222-2222-4222-8222-222222222222';
const SOL='gpt-6.1-sol', ASTRA='gpt-6-astra';
let index=0;
const outcomes:any[]=[];
const attr=(key:string,value:string|number)=>({key,value:typeof value==='number'?{intValue:String(value)}:{stringValue:value}});
function sse(){return explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attr('service.name','codex-app-server')]},scopeLogs:[{logRecords:[{
 timeUnixNano:String(BigInt(AT+6000)*1000000n),traceId:'a'.repeat(32),attributes:[attr('event.name','codex.sse_event'),attr('conversation.id',SESSION),attr('model',SOL),attr('input_token_count',19),attr('output_token_count',2)]
}]}]}]},{source:'codex',transportPath:'/v1/logs'}).events[0]!.event;}
class Fixture {
 dir=path.join(root,String(++index)); file=path.join(this.dir,'ledger.sqlite'); sessions=path.join(this.dir,'sessions'); b:any; now=new Date(AT+2000); native='';
 constructor(){fs.mkdirSync(this.dir);this.b=new Reader(this.file,{workspaceId:'11111111-1111-4111-8111-111111111111',deviceId:'r8-independent',enrollmentNow:()=>new Date(AT-1000000),delivery:{enabled:true,now:()=>this.now}});}
 write(records:Array<[number,string,any]>){const day=path.join(this.sessions,...new Date(AT).toISOString().slice(0,10).split('-'));fs.mkdirSync(day,{recursive:true});this.native=path.join(day,`rollout-review-${SESSION}.jsonl`);fs.writeFileSync(this.native,records.map(([offset,type,payload])=>JSON.stringify({timestamp:new Date(AT+offset).toISOString(),type,payload})).join('\n')+'\n');}
 lease(offset:number){this.now=new Date(AT+offset);return this.b.delivery.lease({now:this.now});}
 async history(){const db=this.b.database;const rootIdentity={...deriveCaptureRootIdentity('r8-independent','codex',this.sessions),source:'codex' as const,directory:this.sessions,installationEpochId:this.b.workspaceBinding().currentInstallationEpochId};
 const start=beginAutomaticCaptureBaseline(db,'codex',{startedAt:new Date(AT-2000).toISOString(),filesDiscovered:0});completeAutomaticCaptureBaseline(db,'codex',{runId:start.latestRun!.runId,completedAt:new Date(AT-1000).toISOString()});const st=fs.statSync(this.native,{bigint:true});sealCaptureBaselineGenerations(db,'codex',[{path:this.native,device:st.dev,inode:st.ino,size:st.size,birthtimeNs:st.birthtimeNs}],new Date(AT+64000).toISOString());
 const plan=await planCaptureHistory(db,rootIdentity);const receipt=await applyCaptureHistory(this.b,rootIdentity);return {plan,receipt};}
 close(){this.b.close();}
}
const tokens=(input:number,output:number,cache=0)=>({type:'token_count',info:{total_token_usage:{input_tokens:input,output_tokens:output,cached_input_tokens:cache}}});
const named=(items:any[])=>items.filter(i=>i.envelope.event.model&&i.envelope.event.metadata.usageSource!=='capture_gap'&&(i.envelope.event.inputTokens!==undefined||i.envelope.event.outputTokens!==undefined));
async function check(name:string,body:()=>Promise<any>){try{outcomes.push({name,...await body()});}catch(error){outcomes.push({name,passed:false,error:String(error)});}}
async function main(){try{
 for(const mode of ['tailer','history'])await check('two-native-responses-after-one-ACKed-SSE-'+mode,async()=>{
  const f=new Fixture();try{
   const e=sse();assert.equal(f.b.append(e),true);const first=f.lease(64000);const firstNamed=named(first.items);assert.equal(firstNamed.length,1);assert.equal(firstNamed[0].envelope.event.inputTokens,19);assert.equal(firstNamed[0].envelope.event.outputTokens,2);f.b.delivery.acknowledge(first.leaseId,first.items.map((i:any)=>i.deliveryId),f.now);
   f.write([[5000,'session_meta',{id:SESSION}],[5000,'turn_context',{turn_id:'first-response',model:SOL}],[5000,'event_msg',tokens(0,0)],[6000,'event_msg',tokens(19,2)],
    [10000,'turn_context',{turn_id:'second-response',model:SOL}],[11000,'event_msg',tokens(32,5)]]);
   let execution:any;
   if(mode==='tailer'){const t=new Tailer(f.b,f.sessions,()=>[]);try{execution=await t.scan({scope:'full',now:new Date(AT+65000)});}finally{t.close();}}
   else execution=await f.history();
   const later=f.lease(200000),secondNamed=named(later.items);const input=19+secondNamed.reduce((s:number,i:any)=>s+(i.envelope.event.inputTokens??0),0),output=2+secondNamed.reduce((s:number,i:any)=>s+(i.envelope.event.outputTokens??0),0);
   return {passed:input===32&&output===5,reader:base?'0.7.48':'head',firstAcknowledged:firstNamed[0].envelope,execution,laterDeliveries:later.items.map((i:any)=>i.envelope),expectedTotal:{input:32,output:5},observedTotal:{input,output},missingKnownResponse:{turn:'second-response',model:SOL,input:13,output:3},raw:f.b.database.prepare('select id,event_type,input_tokens,output_tokens,payload_json from buffered_events order by rowid').all()};
  }finally{f.close();}
 });
 if(!base){
 await check('public-history-same-turn-conflicting-models',async()=>{
  const f=new Fixture();try{f.write([[5000,'session_meta',{id:SESSION}],[5000,'turn_context',{turn_id:'one-turn',model:SOL}],[5000,'event_msg',tokens(0,0)],[5500,'turn_context',{turn_id:'one-turn',model:ASTRA}],[6000,'event_msg',tokens(19,2)]]);const execution=await f.history(),lease=f.lease(200000);const gaps=lease.items.filter((i:any)=>i.envelope.event.metadata.usageSource==='capture_gap');return {passed:named(lease.items).length===0&&gaps.length===1&&gaps[0].envelope.event.inputTokens===undefined&&gaps[0].envelope.event.model===undefined,execution,deliveries:lease.items.map((i:any)=>i.envelope),facts:f.b.database.prepare('select * from codex_turn_model_evidence order by model').all()};}finally{f.close();}
 });
 await check('public-history-next-turn-missing-model',async()=>{
  const f=new Fixture();try{f.write([[5000,'session_meta',{id:SESSION}],[5000,'turn_context',{turn_id:'first-response',model:SOL}],[5000,'event_msg',tokens(0,0)],[6000,'event_msg',tokens(19,2)],[10000,'turn_context',{turn_id:'second-response'}],[11000,'event_msg',tokens(32,5)]]);const execution=await f.history(),lease=f.lease(200000),good=named(lease.items),gaps=lease.items.filter((i:any)=>i.envelope.event.metadata.usageSource==='capture_gap');return {passed:good.length===1&&good[0].envelope.event.inputTokens===19&&good[0].envelope.event.outputTokens===2&&gaps.length===1&&gaps[0].envelope.event.inputTokens===undefined&&gaps[0].envelope.event.model===undefined,execution,deliveries:lease.items.map((i:any)=>i.envelope),raw:f.b.database.prepare('select payload_json from buffered_events order by rowid').all()};}finally{f.close();}
 });
 }

 for(const mode of ['tailer','history']) for(const shape of ['held','ACKed','partial','native-first','native-frozen','cached','neighbour'])
 await check('response-coverage-'+mode+'-'+shape,async()=>{
  const f=new Fixture();try{
   const firstCache=shape==='cached'?5:0,secondCache=shape==='cached'?7:0;
   f.write([[5000,'session_meta',{id:SESSION}],[5000,'turn_context',{turn_id:'first-response',model:SOL}],
    [5000,'event_msg',tokens(0,0)],[6000,'event_msg',tokens(19,2,firstCache)],
    [10000,'turn_context',{turn_id:'second-response',model:SOL}],[11000,'event_msg',tokens(32,5,secondCache)]]);
   const retained=new Map<string,any>();
   const observe=(lease:any)=>{for(const item of lease.items){const prior=retained.get(item.deliveryId);
    if(prior)assert.equal(item.envelopeJson,prior.envelopeJson);else retained.set(item.deliveryId,item);}};
   const ingest=async()=>{if(mode==='history')return f.history();const t=new Tailer(f.b,f.sessions,()=>[]);
    try{return await t.scan({scope:'full',now:new Date(AT+65000)});}finally{t.close();}};
   const e=sse();if(shape==='neighbour')e.observedAt=new Date(AT+21000).toISOString();if(shape==='partial'){delete e.outputTokens;delete e.metadata.output_token_count;}
   if(shape==='cached'){e.cacheReadTokens=5;e.costUsd=.1;e.costKind='reported';
    Object.assign(e.metadata,{cached_token_count:5,cost_usd:.1});}
   if(shape.startsWith('native-')){await ingest();if(shape==='native-frozen'){const first=f.lease(200000);observe(first);
    f.b.delivery.acknowledge(first.leaseId,first.items.map((i:any)=>i.deliveryId),f.now);}}
   assert.equal(f.b.append(e),true);
   if(!shape.startsWith('native-')&&shape!=='held'){const first=f.lease(64000);observe(first);
    f.b.delivery.acknowledge(first.leaseId,first.items.map((i:any)=>i.deliveryId),f.now);}
   await ingest();const last=f.lease(320000);observe(last);
   const good=named([...retained.values()]);
   const sum=(k:string)=>good.reduce((n:number,i:any)=>n+(i.envelope.event[k]??0),0);
   const expectedInput=shape==='neighbour'?51:32,expectedOutput=shape==='neighbour'?7:5;
   assert.equal(sum('inputTokens'),expectedInput);assert.equal(sum('outputTokens'),expectedOutput);
   assert.equal(sum('cacheReadTokens'),secondCache);assert.equal(sum('costUsd'),shape==='cached'?.1:0);
   for(let i=0;i<40;i++){f.b.projection.runMaintenance(new Date(AT+320000));
    if(f.b.projection.status().parityReady&&!f.b.projection.status().dirty)break;}
   const totals=f.b.database.prepare(`select sum(input_tokens) as input,sum(output_tokens) as output,
    sum(cache_read_tokens) as cache from dashboard_event_facts where source='codex'`).get();
   assert.deepEqual(totals,{input:expectedInput,output:expectedOutput,cache:secondCache});
   ensureSessionSummarySchema(f.b.database);
   const read=async (queries:any[])=>queries.flatMap(q=>f.b.database.prepare(q.sql).all(q.params));
   const until=new Date(Math.max(Date.now(),AT+320000)+1000).toISOString();
   let summary=await updateSessionSummary(f.b.database,SESSION,until,{read});
   for(let i=0;i<40&&!summary.complete;i++)summary=await updateSessionSummary(f.b.database,SESSION,
    until,{read});
   assert.equal(summary.complete,true,JSON.stringify(summary));assert.equal(summary.snapshot!.inputTokens,expectedInput);
   assert.equal(summary.snapshot!.outputTokens,expectedOutput);assert.equal(summary.snapshot!.cacheReadTokens,secondCache);

   if(shape==='partial')assert.ok(good.some((i:any)=>i.envelope.event.inputTokens===0&&i.envelope.event.outputTokens===2));
   for(const item of good){const ev=item.envelope.event;
    if(ev.metadata.otelEventName==='codex.sse_event'&&ev.inputTokens===0&&ev.metadata.input_token_count!==undefined)
     assert.equal(ev.metadata.input_token_count,0,'wire aliases retain only the unpaid input portion');}

   return {passed:true,totals,retainedDeliveries:good.map((i:any)=>i.envelope.event),shape};
  }finally{f.close();}
 });
 for(const mode of ['tailer','history'])await check('two-known-native-responses-without-SSE-'+mode,async()=>{
  const f=new Fixture();try{
   f.write([[5000,'session_meta',{id:SESSION}],[5000,'turn_context',{turn_id:'first-response',model:SOL}],
    [5000,'event_msg',tokens(0,0)],[6000,'event_msg',tokens(19,2)],
    [10000,'turn_context',{turn_id:'second-response',model:SOL}],[11000,'event_msg',tokens(32,5)]]);
   if(mode==='history')await f.history();else{const t=new Tailer(f.b,f.sessions,()=>[]);
    try{await t.scan({scope:'full',now:new Date(AT+65000)});}finally{t.close();}}
   const good=named(f.lease(200000).items);assert.equal(good.length,2);
   assert.equal(good.reduce((n:number,i:any)=>n+(i.envelope.event.inputTokens??0),0),32);
   assert.equal(good.reduce((n:number,i:any)=>n+(i.envelope.event.outputTokens??0),0),5);return {passed:true};
  }finally{f.close();}
 });
 for(const producer of ['native','SSE'])await check('unkeyed-frozen-span-zero-complement-'+producer,async()=>{
  const f=new Fixture();try{
   const span=explodeOtlpPayload({resourceSpans:[{resource:{attributes:[attr('service.name','codex-app-server')]},scopeSpans:[{spans:[{
    name:'handle_responses',traceId:'a'.repeat(32),spanId:'1'.repeat(16),
    startTimeUnixNano:String(BigInt(AT+5000)*1000000n),endTimeUnixNano:String(BigInt(AT+6000)*1000000n),
    attributes:[attr('gen_ai.request.model',SOL),attr('gen_ai.usage.input_tokens',19),attr('gen_ai.usage.output_tokens',2)]
   }]}]}]},{source:'codex',transportPath:'/v1/traces'}).events[0]!.event;
   assert.equal(span.sessionId,undefined);assert.equal(f.b.append(span),true);
   const first=f.lease(64000),owner=named(first.items).find((i:any)=>i.rawId===span.id);assert.ok(owner);
   f.b.delivery.acknowledge(first.leaseId,first.items.map((i:any)=>i.deliveryId),f.now);
   if(producer==='SSE'){const e=sse();e.cacheReadTokens=0;e.metadata.cached_input_token_count=0;assert.equal(f.b.append(e),true);}
   else {f.write([[5000,'session_meta',{id:SESSION}],[5000,'turn_context',{turn_id:'first-response',model:SOL}],
    [5000,'event_msg',tokens(0,0)],[6000,'event_msg',tokens(19,2)]]);
    const t=new Tailer(f.b,f.sessions,()=>[]);try{await t.scan({scope:'full',now:new Date(AT+65000)});}finally{t.close();}}
   const later=f.lease(200000),good=named([...first.items,...later.items]);
   assert.equal(good.reduce((n:number,i:any)=>n+(i.envelope.event.inputTokens??0),0),19);
   assert.equal(good.reduce((n:number,i:any)=>n+(i.envelope.event.outputTokens??0),0),2);
   assert.ok(good.some((i:any)=>i.envelope.event.cacheReadTokens===0),'native reported zero is retained');
   const stored=f.b.database.prepare('select envelope_json as bytes from codex_named_captures where delivery_id=?').get(owner.deliveryId);
   assert.equal(stored.bytes,owner.envelopeJson,'first frozen span bytes stay identical');
   return {passed:true,producer,deliveries:good.map((i:any)=>i.envelope)};
  }finally{f.close();}
 });
 console.log(JSON.stringify({case:'r8-independent-generator-omissions',reader:base?'0.7.48':'head',outcomes},null,2));for(const outcome of outcomes){assert.equal(outcome.passed,true,JSON.stringify(outcome));completion.check(outcome.name);}completion.complete();
}finally{fs.rmSync(root,{recursive:true,force:true});}}
main().catch(error=>{console.error(error);process.exitCode=1;});
