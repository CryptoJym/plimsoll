import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {LocalEventBuffer} from '../packages/collector-cli/src/buffer';
import {aiInteractionEventSchema} from '../packages/shared/src/index';
import {createProofCompletion} from './lib/proof-completion';
import {proofTempRoot,withReader} from './lib/legacy-reader';

// The independent review's eight comparisons, using the actual released
// reader as well as head. Each cell appends, leases and ACKs both requests.
const completion=createProofCompletion('codex-account-boundary',16);
const root=fs.realpathSync(proofTempRoot('account-boundary'));
const AT=Date.now()-300_000,WORKSPACE='11111111-1111-4111-8111-111111111111';
const SESSION='22222222-2222-4222-8222-222222222222';let serial=0;
const results:unknown[]=[];
async function compare(BufferClass:any,label:string){
 for(const traced of [false,true])for(const shared of [false,true]){
  const b=new BufferClass(path.join(root,++serial+'.sqlite'),{workspaceId:WORKSPACE,deviceId:'account-boundary',
   enrollmentNow:()=>new Date(AT-7_200_000),delivery:{enabled:true}});
  try{
   const wires:any[]=[],frozen:Array<{id:string;bytes:string}>=[];
   for(let n=0;n<2;n++){
    const e=aiInteractionEventSchema.parse({id:`00000000-0000-4000-8000-${String(n+1).padStart(12,'0')}`,
     source:'codex',sessionId:SESSION,actorId:n?'sha256:fedcba9876543210':'sha256:0123456789abcdef',
     eventType:'assistant_response',observedAt:new Date(AT+n*1000).toISOString(),model:'gpt-6-sol',inputTokens:19,outputTokens:2,
     metadata:{otelEventName:'codex.sse_event',model:'gpt-6-sol',request_id:'distinct-request-'+n,
      call_id:shared?'equal-call-text':'distinct-call-'+n,...(traced?{traceId:String(n+1).padStart(32,'a')}:{})}});
    assert.equal(b.append(e),true);b.database.prepare('update upload_outbox set next_attempt_at=?').run(new Date(AT+61_000).toISOString());
    const now=new Date(AT+63_000+n*123_000),lease=b.delivery.lease({now}),item=lease.items.find((i:any)=>i.rawId===e.id);
    assert.ok(item);assert.equal(lease.locallyDead,0);wires.push(item.envelope.event);frozen.push({id:item.deliveryId,bytes:item.envelopeJson});
    assert.equal(item.envelope.event.metadata.usageSource==='capture_gap',false);
    assert.equal(item.envelope.event.model,'gpt-6-sol');assert.equal(item.envelope.event.inputTokens,19);assert.equal(item.envelope.event.outputTokens,2);
    const ack=b.delivery.acknowledge(lease.leaseId,lease.items.map((i:any)=>i.deliveryId),now);assert.equal(ack.locallyDead,0);
    assert.equal((b.database.prepare('select terminal_state as state from upload_receipts where delivery_id=?').get(item.deliveryId) as {state:string}).state,'acknowledged');
   }
   const input=wires.reduce((sum,e)=>sum+(e.inputTokens??0),0),output=wires.reduce((sum,e)=>sum+(e.outputTokens??0),0);
   assert.equal(input,38);assert.equal(output,4);
   if(label==='head')for(const f of frozen)assert.equal((b.database.prepare('select envelope_json as bytes from codex_named_captures where delivery_id=?').get(f.id) as {bytes:string}).bytes,f.bytes);
   const name=`${label}/${traced?'traced':'trace-free'}/${shared?'shared-call':'different-call'}`;
   results.push({name,input,output,requests:'distinct',accounts:'known-disjoint',append:true,lease:true,ack:true,rawMutation:false});completion.check(name);
  }finally{b.close();}
 }
}
async function compareLocal(BufferClass:any,label:string){
 for(const traced of [false,true])for(const usageSource of ['rollout','codex_local_turn'] as const){
  const b=new BufferClass(path.join(root,++serial+'.sqlite'),{workspaceId:WORKSPACE,deviceId:'account-boundary',
   enrollmentNow:()=>new Date(AT-7_200_000),delivery:{enabled:true}});
  try{
   const fact=aiInteractionEventSchema.parse({id:'00000000-0000-4000-8000-000000000003',source:'codex',
    sessionId:SESSION,actorId:'sha256:0123456789abcdef',eventType:'tool_result',model:'gpt-6-sol',
    observedAt:new Date(AT).toISOString(),metadata:{usageSource,codexTurnId:'T',request_id:'R1',model:'gpt-6-sol',
     ...(traced?{traceId:'a'.repeat(32),'conversation.id':SESSION}:{})}});
   assert.equal(b.append(fact),true);
   b.database.prepare('update upload_outbox set next_attempt_at=?').run(new Date(AT+61_000).toISOString());
   const leaseOne=b.delivery.lease({now:new Date(AT+62_000)}),first=leaseOne.items.find((i:any)=>i.rawId===fact.id);
   assert.ok(first);assert.equal(first.envelope.event.inputTokens,undefined);assert.equal(first.envelope.event.outputTokens,undefined);
   b.delivery.acknowledge(leaseOne.leaseId,leaseOne.items.map((i:any)=>i.deliveryId),new Date(AT+62_000));
   assert.equal((b.database.prepare('select terminal_state as state from upload_receipts where delivery_id=?').get(first.deliveryId) as {state:string}).state,'acknowledged');
   const e=aiInteractionEventSchema.parse({id:'00000000-0000-4000-8000-000000000004',source:'codex',
    sessionId:SESSION,actorId:'sha256:fedcba9876543210',eventType:'assistant_response',model:'gpt-6-sol',
    observedAt:new Date(AT+1000).toISOString(),inputTokens:19,outputTokens:2,
    metadata:{otelEventName:'codex.sse_event',codexTurnId:'T',request_id:'R2',model:'gpt-6-sol',
     ...(traced?{traceId:'b'.repeat(32),'conversation.id':SESSION}:{})}});
   assert.equal(b.append(e),true);b.database.prepare('update upload_outbox set next_attempt_at=?').run(new Date(AT+61_000).toISOString());
   const now=new Date(AT+185_000),lease=b.delivery.lease({now}),item=lease.items.find((i:any)=>i.rawId===e.id);
   assert.ok(item);assert.equal(lease.locallyDead,0);assert.equal(item.envelope.event.model,'gpt-6-sol');
   assert.equal(item.envelope.event.inputTokens,19);assert.equal(item.envelope.event.outputTokens,2);
   assert.equal(item.envelope.event.metadata.usageSource==='capture_gap',false);
   const ack=b.delivery.acknowledge(lease.leaseId,lease.items.map((i:any)=>i.deliveryId),now);assert.equal(ack.locallyDead,0);
   assert.equal((b.database.prepare('select terminal_state as state from upload_receipts where delivery_id=?').get(item.deliveryId) as {state:string}).state,'acknowledged');
   const retained=b.database.prepare('select sealed_envelope_json as bytes from upload_outbox where delivery_id=?').get(first.deliveryId) as {bytes:string}|undefined;
   if(retained)assert.equal(retained.bytes,first.envelopeJson);
   if(label==='head'){
    const frozen=b.database.prepare('select envelope_json as bytes from codex_named_captures where delivery_id=?').get(item.deliveryId) as {bytes:string};assert.equal(frozen.bytes,item.envelopeJson);
   }
   const name=`${label}/local-turn/${usageSource}/${traced?'traced':'trace-free'}`;
   results.push({name,input:19,output:2,requests:'distinct',accounts:'known-disjoint',factCounters:'not reported',
    append:true,lease:true,ack:true,factAcknowledged:true,rawMutation:false});completion.check(name);
  }finally{b.close();}
 }
}
async function main(){try{
 await compare(LocalEventBuffer,'head');await compareLocal(LocalEventBuffer,'head');
 await withReader('121b55437555c3a3c34bafe5889f4d6d8870509f',async({Buffer})=>{await compare(Buffer,'released-0.7.50');await compareLocal(Buffer,'released-0.7.50');});
 console.log(JSON.stringify({proof:'codex-account-boundary',results}));completion.complete();
}finally{fs.rmSync(root,{recursive:true,force:true});}}
main().catch(e=>{console.error(e);process.exitCode=1});
