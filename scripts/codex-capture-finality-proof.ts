import assert from 'node:assert/strict';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { withLegacyReader } from './lib/legacy-reader';
import { createProofCompletion } from './lib/proof-completion';
let MainBuffer: any;
import { RolloutTailer } from '../packages/collector-cli/src/rollout-tailer';
import { explodeOtlpPayload } from '../packages/collector-cli/src/otlp';
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline, sealCaptureBaselineGenerations } from '../packages/collector-cli/src/capture-baseline';
import { deriveCaptureRootIdentity } from '../packages/collector-cli/src/capture-root-inventory';
import { planCaptureHistory, applyCaptureHistory } from '../packages/collector-cli/src/capture-history-import';
const root=fs.mkdtempSync(path.join(os.tmpdir(),'r7-directed-'));
const AT=Date.now()-300000, SESSION='22222222-2222-4222-8222-222222222222', MODEL='gpt-6.1-sol';
const attr=(key:string,value:string|number)=>({key,value:typeof value==='number'?{intValue:String(value)}:{stringValue:value}});
const options=(now:()=>Date)=>({workspaceId:'11111111-1111-4111-8111-111111111111',deviceId:'r7-review-device',enrollmentNow:()=>new Date(AT-10_000_000),delivery:{enabled:true,now}});
const outputs:any[]=[];let sequence=0;
const controls=false;
const completion=createProofCompletion('codex-capture-finality',6);
const Reader=LocalEventBuffer;
class Fixture {
 dir=path.join(root,String(++sequence)); file=path.join(this.dir,'ledger.sqlite'); now=new Date(AT+2000); b:any;
 constructor(){fs.mkdirSync(this.dir);this.b=new Reader(this.file,options(()=>this.now));}
 close(){this.b?.close();}
 lease(offset:number){this.now=new Date(AT+offset);return this.b.delivery.lease({now:this.now});}
}
function sse(trace:string,model:string,input?:number,output?:number,at=AT+1000,conflict=false){
 return explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attr('service.name','codex-app-server')]},scopeLogs:[{logRecords:[{timeUnixNano:String(BigInt(at)*1000000n),traceId:trace,attributes:[attr('event.name','codex.sse_event'),attr('conversation.id',SESSION),attr('model',model),...(input===undefined?[]:[attr('input_token_count',input)]),...(output===undefined?[]:[attr('output_token_count',output)]),...(conflict?[attr('gen_ai.request.model','gpt-6-astra')]:[])]}]}]}]},{source:'codex',transportPath:'/v1/logs'}).events[0]!.event;
}
function usage(items:any[]){return items.filter(i=>i.envelope.event.metadata.usageSource!=='capture_gap' && (i.envelope.event.inputTokens!==undefined||i.envelope.event.outputTokens!==undefined));}
function nativeFile(f:Fixture){
 const sessions=path.join(f.dir,'sessions'),dir=path.join(sessions,...new Date(AT).toISOString().slice(0,10).split('-'));fs.mkdirSync(dir,{recursive:true});
 const file=path.join(dir,`rollout-review-${SESSION}.jsonl`);
 const row=(offset:number,type:string,payload:any)=>JSON.stringify({timestamp:new Date(AT+offset).toISOString(),type,payload});
 fs.writeFileSync(file,[row(10000,'session_meta',{id:SESSION}),row(10000,'turn_context',{turn_id:'review-native-turn',model:MODEL}),row(10000,'event_msg',{type:'token_count',info:{total_token_usage:{input_tokens:0,output_tokens:0,cached_input_tokens:0}}}),row(11000,'event_msg',{type:'token_count',info:{total_token_usage:{input_tokens:19,output_tokens:2,cached_input_tokens:0}}})].join('\n')+'\n');
 return {sessions,file};
}
function historyRoot(f:Fixture){
 const {sessions,file}=nativeFile(f); const db=f.b.database;
 const r={...deriveCaptureRootIdentity('review','codex',sessions),directory:sessions,source:'codex' as const,installationEpochId:f.b.workspaceBinding().currentInstallationEpochId};
 const baseline=beginAutomaticCaptureBaseline(db,'codex',{startedAt:new Date(AT-2000).toISOString(),filesDiscovered:0});
 completeAutomaticCaptureBaseline(db,'codex',{runId:baseline.latestRun!.runId,completedAt:new Date(AT-1000).toISOString()});
 const stat=fs.statSync(file,{bigint:true});sealCaptureBaselineGenerations(db,'codex',[{path:file,device:stat.dev,inode:stat.ino,size:stat.size,birthtimeNs:stat.birthtimeNs}],new Date(AT+64000).toISOString());
 return r;
}
async function check(name:string,body:()=>Promise<any>|any){try{outputs.push({name,...await body()});}catch(error){outputs.push({name,passed:false,error:String(error)});}}
async function main(){try{
 if(!controls){
 await check('authority-witness-on-second-128-row-page',()=>{
  const f=new Fixture();try{
   const good=sse('1'.repeat(32),MODEL,19,2,AT-2_000_000); assert.equal(f.b.append(good),true);
   const ids:string[]=[];
   for(let i=0;i<129;i++){const bad=sse((i+1000).toString(16).padStart(32,'0'),MODEL,17,3,AT+i,true);assert.equal(f.b.append(bad),true);ids.push(bad.id);}
   const ordered=f.b.database.prepare("select id from buffered_events where source='codex' and session_id=? order by observed_at desc,id desc").all(SESSION);
   assert.equal(ordered[129].id,good.id);assert.equal(ordered.slice(0,128).some((r:any)=>r.id===good.id),false);
   const rawBefore=f.b.database.prepare('select payload_json from buffered_events order by rowid').all();
   const authority=f.b.sessionUsageAuthority('codex',SESSION);assert.equal(authority,'live');
   assert.deepEqual(f.b.database.prepare('select payload_json from buffered_events order by rowid').all(),rawBefore);
   const hasDecisions=Boolean(f.b.database.prepare("select 1 from sqlite_master where name='codex_capture_decisions'").get());
   const premature=hasDecisions?f.b.database.prepare('select count(*) as n from codex_capture_decisions').get().n>0:false;assert.equal(premature,false);
   return {passed:true,candidateRows:130,witnessRank:130,authority,rawUnchanged:true,prematureGapDecision:false};
  }finally{f.close();}
 });
 for(const unkeyed of [false,true]) await check('public-history-native-model-after-'+(unkeyed?'unkeyed':'keyed')+'-gap',async()=>{
  const f=new Fixture();try{
   const peer=sse((unkeyed?'3':'2').repeat(32),MODEL,17,3,AT+11000,true);
   if(unkeyed){delete peer.sessionId;delete peer.metadata['conversation.id'];}
   assert.equal(f.b.append(peer),true);const first=f.lease(63000);const item=first.items.find((i:any)=>i.rawId===peer.id);assert.equal(item.envelope.event.metadata.usageSource,'capture_gap');
   f.b.delivery.acknowledge(first.leaseId,first.items.map((i:any)=>i.deliveryId),f.now);
   const r=historyRoot(f),plan=await planCaptureHistory(f.b.database,r);assert.equal(plan.missingRows,1);assert.equal(plan.skippedLiveSessions,0);
   const receipt=await applyCaptureHistory(f.b,r);assert.equal(receipt.importedRows,1);
   const raw=f.b.database.prepare("select payload_json as p from buffered_events where event_type='usage_rollout'").all().map((r:any)=>JSON.parse(r.p));
   const second=f.lease(126000);const named=usage(second.items);
   return {passed:named.length===1&&named[0].envelope.event.model===MODEL&&named[0].envelope.event.inputTokens===19&&named[0].envelope.event.outputTokens===2,plan,importedRows:receipt.importedRows,nativeFileTurnId:'review-native-turn',raw,deliveries:second.items.map((i:any)=>i.envelope),expected:'the explicit native turn/model in the imported file must deliver one named 19/2 row, just as the ordinary native tailer does'};
  }finally{f.close();}
 });
 await check('gap-sealed-between-history-preflight-and-writer-entry',async()=>{
  const f=new Fixture();let old:any;try{
   const r=historyRoot(f);const plan=await planCaptureHistory(f.b.database,r);assert.equal(plan.missingRows,1);
   const original=f.b.transactionWithRepoContextHandoffs.bind(f.b);let crossed=false;let seeded:string|undefined;
   f.b.transactionWithRepoContextHandoffs=(work:any)=>{
    if(!crossed){crossed=true;old=new MainBuffer(f.file,options(()=>f.now));const peer=sse('4'.repeat(32),MODEL,17,3,AT+11000,true);seeded=peer.id;
     assert.equal(old.append(peer),true);old.close();old=undefined;
     const first=f.lease(63000);const gap=first.items.find((i:any)=>i.rawId===peer.id);assert.ok(gap);assert.equal(gap.envelope.event.metadata.usageSource,'capture_gap');
     f.b.delivery.acknowledge(first.leaseId,first.items.map((i:any)=>i.deliveryId),f.now);
    }
    return original(work);
   };
   const receipt=await applyCaptureHistory(f.b,r);assert.equal(crossed,true);assert.equal(receipt.importedRows,1);
   const state=f.b.database.prepare("select state_json as state from capture_history_session_counters where session_id=?").get(SESSION);
   const byteReceipt=f.b.database.prepare("select imported_length as length from capture_history_session_bytes where session_id=?").get(SESSION);
   assert.equal(JSON.parse(state.state).previous.input,19);assert.ok(byteReceipt.length>0);
   const {sessions}=nativeFile(f);const tailer=new RolloutTailer(f.b,sessions,()=>[]);let scan;try{scan=await tailer.scan({scope:'full',now:new Date(AT+127000)});}finally{tailer.close();}
   assert.equal(scan.eventsAppended,0);
   return {passed:true,crossedActualWriterBoundary:true,legacyWriter:'34d58bcd',seededGap:seeded,importedRows:receipt.importedRows,handoffCounters:JSON.parse(state.state).previous,tailerAppended:scan.eventsAppended,scope:'writer admission and handoff conservation; financial envelope checked separately'};
  }finally{old?.close();f.close();}
 });
 }
 for(const acknowledged of [false,true]) await check('native-named-'+(acknowledged?'ACK':'retry')+'-with-late-trace-model-conflict',async()=>{
  const f=new Fixture();let tailer:any;try{
   const trace=(acknowledged?'6':'5').repeat(32),good=sse(trace,MODEL,19,2);assert.equal(f.b.append(good),true);
   const first=f.lease(63000),frozen=usage(first.items);assert.equal(frozen.length,1);assert.equal(frozen[0].envelope.event.model,MODEL);const original=frozen[0];
   if(acknowledged)assert.equal(f.b.delivery.acknowledge(first.leaseId,first.items.map((i:any)=>i.deliveryId),f.now).acknowledged,first.items.length);
   const contrary=sse(trace,'gpt-6-astra',undefined,undefined,AT+100000);assert.equal(f.b.append(contrary),true);
   const authority=f.b.sessionUsageAuthority('codex',SESSION);
   let scan:any=null;
   if(acknowledged){const {sessions}=nativeFile(f);tailer=new RolloutTailer(f.b,sessions,()=>[]);scan=await tailer.scan({scope:'full',now:new Date(AT+184000)});tailer.close();tailer=undefined;}
   const later=f.lease(185000),counted=usage(later.items);
   const sameId=counted.length===1&&counted[0].deliveryId===original.deliveryId,sameBytes=counted.length===1&&counted[0].envelopeJson===original.envelopeJson;
   const passed=acknowledged?counted.length===0:counted.length===1&&sameId&&sameBytes&&later.locallyDead===0;
   return {passed,firstNativeRow:good,firstFrozen:original.envelope,firstDeliveryId:original.deliveryId,acknowledged,contraryNativeRow:contrary,authority,scan,laterLocallyDead:later.locallyDead,laterDeliveries:later.items.map((i:any)=>i.envelope),sameId,sameBytes,financialInputIfAccepted:(acknowledged?19:0)+counted.reduce((s:number,i:any)=>s+(i.envelope.event.inputTokens??0),0),financialOutputIfAccepted:(acknowledged?2:0)+counted.reduce((s:number,i:any)=>s+(i.envelope.event.outputTokens??0),0),expected:acknowledged?'accepted native SSE counts remain covered; identical native rollout must not count the same response twice':'a once-captured native named row must retain its frozen ID and bytes on retry even if later trace facts become ambiguous'};
  }finally{tailer?.close();f.close();}
 });
 console.log(JSON.stringify({case:'r7-new-code-directed-breaks',reader:'head',outcomes:outputs},null,2));for (const outcome of outputs) completion.check(outcome.name,outcome.passed);
completion.complete();
}finally{fs.rmSync(root,{recursive:true,force:true});}}
withLegacyReader(async ({ Buffer }) => { MainBuffer=Buffer; await main(); }).catch(error=>{console.error(error);process.exitCode=1;});
