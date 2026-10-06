import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {LocalEventBuffer} from '../packages/collector-cli/src/buffer';
import {captureCodexModel,isCaptureGap,recordCodexTurnModel,CODEX_NATIVE_LINKED_SCOPE_SQL} from '../packages/collector-cli/src/codex-model-capture';
import {aiInteractionEventSchema,type AiInteractionEvent} from '../packages/shared/src/index';
import {createProofCompletion} from './lib/proof-completion';

const completion=createProofCompletion('codex-capture-veto',340);
const root=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),'codex-capture-veto-')));
const AT=Date.now()-300_000, A='sha256:0123456789abcdef',B='sha256:fedcba9876543210';
const MODEL='gpt-6-sol',OTHER='gpt-5.5',SESSION='22222222-2222-4222-8222-222222222222';
const uuid=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const make=(id:number,metadata:Record<string,unknown>,extra:Partial<AiInteractionEvent>={})=>
 aiInteractionEventSchema.parse({id:uuid(id),source:'codex',eventType:'assistant_response',sessionId:SESSION,
 actorId:A,observedAt:new Date(AT).toISOString(),inputTokens:19,outputTokens:2,metadata,...extra});
const failures:Array<{name:string;error:string}>=[];let serial=0,executed=0;
class Fixture {
 readonly b:LocalEventBuffer; readonly trace:string;
 constructor(readonly name:string,noise=0){
  this.trace=(++serial).toString(16).padStart(32,'0');
  this.b=new LocalEventBuffer(path.join(root,serial+'.sqlite'),{workspaceId:uuid(998),deviceId:'veto-proof',
   enrollmentNow:()=>new Date(AT-7_200_000),delivery:{enabled:false}});
  for(let n=0;n<noise;n++)this.append(make(1000+n,{}, {eventType:'tool_result',inputTokens:undefined,outputTokens:undefined}));
 }
 append(e:AiInteractionEvent){assert.equal(this.b.append(e),true);return e;}
 raw(e:AiInteractionEvent):AiInteractionEvent{return JSON.parse((this.b.database.prepare(
  'select payload_json as p from buffered_events where id=?').get(e.id) as {p:string}).p);}
 capture(e:AiInteractionEvent){const raw=this.raw(e);return captureCodexModel(this.b.database,raw,raw.id,false,false);}
 deliver(e:AiInteractionEvent,gap:boolean,paired=false){
  const c=this.capture(e);
  if(gap){assert.equal(isCaptureGap(c),true);assert.equal(c.model,undefined);assert.equal(c.inputTokens,undefined);
   assert.equal(c.outputTokens,undefined);assert.equal(c.metadata.modelGapInputTokens,19);assert.equal(c.metadata.modelGapOutputTokens,2);}
  else {assert.equal(isCaptureGap(c),false);assert.equal(c.model,MODEL);
   assert.equal(c.inputTokens,paired?undefined:19);assert.equal(c.outputTokens,paired?undefined:2);
   if(paired){assert.equal(c.metadata.modelCaptureInputTokens,19);assert.equal(c.metadata.modelCaptureOutputTokens,2);}}
  this.b.delivery.configure({enabled:true});this.b.delivery.repairRawById(e.id);
  this.b.database.prepare('update upload_outbox set next_attempt_at=?').run(new Date(AT+61_000).toISOString());
  const lease=this.b.delivery.lease({now:new Date(AT+62_000)});
  const item=lease.items.find(i=>i.envelope.event.id===e.id);
  assert.ok(item,'target must execute the real wire path');const wire=item.envelope.event;
  assert.equal(isCaptureGap(wire),gap);assert.equal(wire.model,gap?undefined:MODEL);
  assert.equal(wire.inputTokens,gap||paired?undefined:19);assert.equal(wire.outputTokens,gap||paired?undefined:2);
  if(gap)assert.equal(wire.metadata.modelGapInputTokens,undefined,'diagnostic counters stay local');
  this.b.delivery.acknowledge(lease.leaseId,lease.items.map(i=>i.deliveryId),new Date(AT+62_000));
  const custody=this.b.database.prepare('select terminal_state as state from upload_receipts where delivery_id=?').get(item.deliveryId) as {state:string};
  assert.equal(custody.state,'acknowledged');
 }
 freezeFact(e:AiInteractionEvent,ack:boolean){
  this.b.delivery.configure({enabled:true});this.b.delivery.repairRawById(e.id);
  this.b.database.prepare('update upload_outbox set next_attempt_at=?').run(new Date(AT+61_000).toISOString());
  const lease=this.b.delivery.lease({now:new Date(AT+62_000)});const item=lease.items.find(i=>i.envelope.event.id===e.id);
  assert.ok(item,'contradiction fact must actually be frozen');
  if(ack)this.b.delivery.acknowledge(lease.leaseId,lease.items.map(i=>i.deliveryId),new Date(AT+62_000));
  this.b.delivery.configure({enabled:false});return {id:item.deliveryId,bytes:item.envelopeJson,event:item.envelope.event};
 }
 frozenUnchanged(f:{id:string;bytes:string}){assert.equal((this.b.database.prepare(
  `select sealed_envelope_json as bytes from upload_outbox where delivery_id=?
   union all select envelope_json as bytes from codex_named_captures where delivery_id=?`).get(f.id,f.id) as {bytes:string}).bytes,f.bytes);}
 close(){this.b.close();}
}
function cell(name:string,noise:number,run:(f:Fixture)=>void){const f=new Fixture(name,noise);executed++;
 try{run(f);completion.check(name);console.log('PASS '+name);}catch(e){failures.push({name,error:String(e)});completion.check(name,false);console.error('FAIL '+name+' '+e);}
 finally{f.close();}}
try{
 cell('linked-native-scope-is-indexed',0,f=>{
  const binding=f.b.workspaceBinding()!;
  const plans=f.b.database.prepare(`explain query plan select id from buffered_events where
   source='codex' and workspace_id is ? and device_id is ? and installation_epoch_id is ?
   and ${CODEX_NATIVE_LINKED_SCOPE_SQL}=0`).all(binding.currentWorkspaceId,binding.currentDeviceId,
    binding.currentInstallationEpochId) as Array<{detail:string}>;
  assert.ok(plans.some(p=>p.detail.includes('idx_codex_capture_linked_native_scope_v2')),
   'uncertain facts use the indexed bucket, not a cross-conversation scan');
  f.deliver(f.append(make(1,{traceId:f.trace,'conversation.id':SESSION,request_id:'own',model:MODEL,
   otelEventName:'codex.sse_event'},{model:MODEL})),false);
 });
 // All twelve reviewer wire cells, expanded to session and retained-state contradictions.
 for(const noise of [0,129])for(const kind of ['generic','span'] as const)
  for(const conflict of ['model','account','session','none'] as const)
   for(const state of (conflict==='none'?['counterless'] as const:['counterless','distant','gap','acked'] as const)){
    cell(`wire/${kind}/${conflict}/${state}/${noise}`,noise,f=>{
     let frozen:ReturnType<Fixture['freezeFact']>|undefined;
     if(conflict!=='none'){
      const model=conflict==='model'?OTHER:MODEL;
      const meta:Record<string,unknown>={traceId:f.trace,model};
      // Make a real gap from a conflicting trace fact that will AGREE with
      // the later target. Only the retained gap's single native attributes
      // can then veto the fresh pair; no internal-model conflict masks it.
      if(state==='gap')f.append(make(4,{traceId:f.trace,model:MODEL},
       {eventType:'tool_result',model:MODEL,inputTokens:undefined,outputTokens:undefined}));
      const fact=f.append(make(3,meta,{eventType:state==='counterless'||state==='distant'?'tool_result':'assistant_response',
       model,actorId:conflict==='account'?B:A,sessionId:conflict==='session'?uuid(997):SESSION,
       ...(state==='counterless'||state==='distant'?{inputTokens:undefined,outputTokens:undefined}:{inputTokens:7,outputTokens:1}),
       ...(state==='distant'?{observedAt:new Date(AT-3_600_000).toISOString()}:{} )}));
      if(state==='gap'||state==='acked'){
       frozen=f.freezeFact(fact,state==='acked');assert.equal(isCaptureGap(frozen.event),state==='gap');
      }
     }
     f.append(make(2,{traceId:f.trace,otelEventName:'codex.sse_event',model:MODEL},{model:MODEL}));
     const target=f.append(make(1,{traceId:f.trace,...(kind==='span'?{otelEventName:'handle_responses'}:{model:MODEL})},
      kind==='generic'?{model:MODEL}:{}));
     f.deliver(target,conflict!=='none',kind==='span'&&conflict==='none');if(frozen)f.frozenUnchanged(frozen);
    });
   }
 for(const noise of [0,129])for(const disagree of [false,true])cell(`native-rollout/${disagree}/${noise}`,noise,f=>{
  const target=f.append(make(1,{usageSource:'rollout',codexTurnId:'T',model:disagree?OTHER:MODEL},
   {eventType:'usage_rollout',model:disagree?OTHER:MODEL}));
  f.append(make(2,{otelEventName:'codex.sse_event',codexTurnId:'T',model:MODEL},{model:MODEL}));
  f.deliver(target,disagree);
 });
 // A target need not carry its exact SSE witness's trace or typed identity.
 // Fresh pair finance still depends on ALL of that witness's native facts.
 for(const noise of [0,129])for(const kind of ['generic','span'] as const)
  for(const conflict of ['model','account','session','none'] as const)
   cell(`pair-source-trace/${kind}/${conflict}/${noise}`,noise,f=>{
    if(conflict!=='none')f.append(make(3,{traceId:f.trace,model:conflict==='model'?OTHER:MODEL},
     {eventType:'tool_result',model:conflict==='model'?OTHER:MODEL,inputTokens:undefined,outputTokens:undefined,
      actorId:conflict==='account'?B:A,sessionId:conflict==='session'?uuid(997):SESSION,
      observedAt:new Date(AT-3_600_000).toISOString()}));
    f.append(make(2,{traceId:f.trace,model:MODEL,otelEventName:'codex.sse_event'},{model:MODEL}));
    const target=f.append(make(1,kind==='span'?{otelEventName:'handle_responses'}:{}));
    f.deliver(target,conflict!=='none',kind==='span'&&conflict==='none');
   });
 for(const noise of [0,129])for(const identity of ['request','call','turn','native-turn-table'] as const)
  for(const conflict of ['model','account'] as const)
   cell(`pair-source-identity/${identity}/${conflict}/${noise}`,noise,f=>{
    const key=identity==='request'?'request_id':identity==='call'?'call_id':'codexTurnId';
    if(identity==='native-turn-table')recordCodexTurnModel(f.b.database,SESSION,'source',conflict==='model'?OTHER:MODEL,conflict==='account'?B:A);
    else f.append(make(3,{[key]:'source',model:conflict==='model'?OTHER:MODEL},
     {eventType:'tool_result',model:conflict==='model'?OTHER:MODEL,actorId:conflict==='account'?B:A,
      inputTokens:undefined,outputTokens:undefined}));
    f.append(make(2,{[key]:'source',model:MODEL,otelEventName:'codex.sse_event'},{model:MODEL}));
    f.deliver(f.append(make(1,{})),true);
   });
 // The independent r15 review's exact 24-cell shape: a selected SSE has a
 // typed identity which the trace-free target lacks. The hour-old, counterless
 // contradiction is outside its trace. Trace and typed links must BOTH veto.
 for(const traced of [false,true])for(const identity of ['request_id','call_id','codexTurnId'] as const)
  for(const conflict of ['model','account'] as const)for(const noise of [0,129])
   cell(`source-${traced?'traced':'trace-free'}/${identity}/${conflict}/${noise}`,noise,f=>{
    const model=conflict==='model'?OTHER:MODEL;
    f.append(make(3,{[identity]:'physical-source',model},{model,eventType:'tool_result',
     actorId:conflict==='account'?B:A,inputTokens:undefined,outputTokens:undefined,
     observedAt:new Date(AT-3_600_000).toISOString()}));
    f.append(make(2,{[identity]:'physical-source',model:MODEL,otelEventName:'codex.sse_event',
     ...(traced?{traceId:f.trace}:{})},{model:MODEL}));
    f.deliver(f.append(make(1,{})),true);
   });
 // Apply the same cumulative rule when the traced SSE is the target itself.
 for(const identity of ['request_id','call_id','codexTurnId'] as const)
  for(const conflict of ['model','account'] as const)for(const noise of [0,129])
   cell(`traced-native-target/${identity}/${conflict}/${noise}`,noise,f=>{
    const model=conflict==='model'?OTHER:MODEL;
    f.append(make(3,{[identity]:'physical-target',model},{model,eventType:'tool_result',
     actorId:conflict==='account'?B:A,inputTokens:undefined,outputTokens:undefined,
     observedAt:new Date(AT-3_600_000).toISOString()}));
    f.deliver(f.append(make(1,{traceId:f.trace,[identity]:'physical-target',model:MODEL,
     otelEventName:'codex.sse_event'},{model:MODEL})),true);
   });
 for(const identity of ['request_id','call_id','codexTurnId'] as const)for(const noise of [0,129])
  cell(`traced-native-target-clean/${identity}/${noise}`,noise,f=>{
   f.append(make(3,{[identity]:'physical-target',model:MODEL},{model:MODEL,eventType:'tool_result',
    inputTokens:undefined,outputTokens:undefined,observedAt:new Date(AT-3_600_000).toISOString()}));
   f.deliver(f.append(make(1,{traceId:f.trace,[identity]:'physical-target',model:MODEL,
    otelEventName:'codex.sse_event'},{model:MODEL})),false);
  });
 // A saved span's request/call and native-turn context remain provenance
 // dependencies even when its mutable rollout owner lacks those aliases.
 for(const identity of ['request_id','call_id'] as const)for(const conflict of ['model','account'] as const)
  for(const noise of [0,129])cell(`saved-span-linked/${identity}/${conflict}/${noise}`,noise,f=>{
   recordCodexTurnModel(f.b.database,SESSION,'T',MODEL,A);
   const span=f.append(make(1,{traceId:f.trace,codexTurnId:'T',[identity]:'saved-source',otelEventName:'handle_responses'}));
   const rollout=f.append(make(2,{usageSource:'rollout',codexTurnId:'T'},{eventType:'usage_rollout',model:MODEL}));
   assert.ok(f.b.database.prepare('select 1 from codex_span_rollout_pairs where span_id=?').get(span.id));
   const model=conflict==='model'?OTHER:MODEL;
   f.append(make(3,{[identity]:'saved-source',model},{model,eventType:'tool_result',actorId:conflict==='account'?B:A,
    inputTokens:undefined,outputTokens:undefined,observedAt:new Date(AT-3_600_000).toISOString()}));
   const captured=f.capture(span);assert.equal(isCaptureGap(captured),true);assert.equal(captured.model,undefined);
   f.deliver(rollout,true);
  });
 for(const noise of [0,129])for(const conflict of ['model','account'] as const)
  cell(`traced-source-native-turn-table/${conflict}/${noise}`,noise,f=>{
   recordCodexTurnModel(f.b.database,SESSION,'source',conflict==='model'?OTHER:MODEL,conflict==='account'?B:A);
   f.append(make(2,{traceId:f.trace,codexTurnId:'source',model:MODEL,otelEventName:'codex.sse_event'},{model:MODEL}));
   f.deliver(f.append(make(1,{})),true);
  });
 // Reused request/call text across separately reported native conversations
 // is not a physical-response link. This is also the released-floor boundary.
 for(const identity of ['request_id','call_id'] as const)
  cell(`independent-native-conversation/${identity}`,0,f=>{
   const otherSession=uuid(997);
   const prior=f.append(make(3,{traceId:'e'.repeat(32),'conversation.id':otherSession,
    [identity]:'reused',model:OTHER,otelEventName:'codex.sse_event'},{sessionId:otherSession,model:OTHER}));
   const frozen=f.freezeFact(prior,true);assert.equal(isCaptureGap(frozen.event),false);
   f.deliver(f.append(make(1,{traceId:f.trace,'conversation.id':SESSION,[identity]:'reused',model:MODEL,
    otelEventName:'codex.sse_event'},{model:MODEL})),false);f.frozenUnchanged(frozen);
  });
 for(const noise of [0,129])for(const conflict of ['model','account'] as const)
  cell(`same-native-conversation-request-contradiction/${conflict}/${noise}`,noise,f=>{
   const model=conflict==='model'?OTHER:MODEL;
   f.append(make(3,{traceId:'e'.repeat(32),'conversation.id':SESSION,request_id:'physical-source',model},
    {model,eventType:'tool_result',actorId:conflict==='account'?B:A,inputTokens:undefined,outputTokens:undefined,
     observedAt:new Date(AT-3_600_000).toISOString()}));
   f.append(make(2,{traceId:f.trace,'conversation.id':SESSION,request_id:'physical-source',model:MODEL,
    otelEventName:'codex.sse_event'},{model:MODEL}));
   f.deliver(f.append(make(1,{})),true);
  });
 // The SQL independent-conversation prefilter must retain every uncertain
 // representation and same-trace fact; only the exact independent native
 // boundary may avoid decoding. Exercise the optimized path, not just aliases.
 for(const noise of [0,129])for(const boundary of ['missing-producer','mismatched-producer',
   'stitched-session','blank-trace','nested-independent','same-trace'] as const)
  cell(`linked-query-boundary/${boundary}/${noise}`,noise,f=>{
   const otherSession=uuid(997);
   const metadata:Record<string,unknown>={traceId:boundary==='same-trace'?f.trace:
     boundary==='blank-trace'?' \t ':'e'.repeat(32),request_id:'reused',model:OTHER};
   if(boundary==='nested-independent')metadata.otelAttributes={'conversation.id':otherSession};
   else if(boundary!=='missing-producer')metadata['conversation.id']=
     boundary==='mismatched-producer'?uuid(996):otherSession;
   if(boundary==='stitched-session')metadata.stitched='time_window';
   f.append(make(3,metadata,{sessionId:otherSession,model:OTHER,eventType:'tool_result',
    inputTokens:undefined,outputTokens:undefined,observedAt:new Date(AT-3_600_000).toISOString()}));
   f.deliver(f.append(make(1,{traceId:f.trace,'conversation.id':SESSION,request_id:'reused',model:MODEL,
   otelEventName:'codex.sse_event'},{model:MODEL})),boundary!=='nested-independent');
  });
 // A JSON array/object is unknown native evidence even when SQLite renders it
 // as text. It must stay in the uncertain bucket and veto a genuinely linked
 // target or selected SSE source. Invalid producer metadata cannot partition.
 for(const noise of [0,129])for(const targetKind of ['native-target','selected-source'] as const)
  for(const identity of ['request_id','call_id'] as const)for(const conflict of ['model','account'] as const)
   for(const representation of ['trace-array','trace-object','producer-array','producer-object'] as const)
    cell(`linked-json-unknown/${targetKind}/${identity}/${conflict}/${representation}/${noise}`,noise,f=>{
     const otherSession=uuid(997),model=conflict==='model'?OTHER:MODEL;
     const metadata:Record<string,unknown>={traceId:'e'.repeat(32),'conversation.id':otherSession,[identity]:'reused',model};
     if(representation==='trace-array')metadata.traceId=['e'.repeat(32)];
     if(representation==='trace-object')metadata.traceId={value:'e'.repeat(32)};
     if(representation==='producer-array')metadata['conversation.id']=[otherSession];
     if(representation==='producer-object')metadata['conversation.id']={value:otherSession};
     const fact=f.append(make(3,metadata,{sessionId:otherSession,model,eventType:'tool_result',
      actorId:conflict==='account'?B:A,inputTokens:undefined,outputTokens:undefined,
      observedAt:new Date(AT-3_600_000).toISOString()}));
     assert.equal((f.b.database.prepare(`select ${CODEX_NATIVE_LINKED_SCOPE_SQL} as bucket
      from buffered_events where id=?`).get(fact.id) as {bucket:number}).bucket,0);
     const native={[identity]:'reused',traceId:f.trace,'conversation.id':SESSION,model:MODEL,otelEventName:'codex.sse_event'};
     if(targetKind==='native-target')f.deliver(f.append(make(1,native,{model:MODEL})),true);
     else{f.append(make(2,native,{model:MODEL}));f.deliver(f.append(make(1,{})),true);}
    });
 // SQL-rendered containers must not invent an alias to a literal native text
 // id either. Keep exact JSON string matching in complete-trace and typed
 // request/call/turn selectors as well as the independent-conversation index.
 for(const noise of [0,129])for(const targetKind of ['native-target','selected-source'] as const)
  for(const identity of ['traceId','request_id','call_id','codexTurnId'] as const)
   for(const representation of ['array','object'] as const)
    cell(`linked-json-unlinked/${targetKind}/${identity}/${representation}/${noise}`,noise,f=>{
     const malformed=representation==='array'?['literal']:{value:'literal'},literal=JSON.stringify(malformed);
     f.append(make(3,{[identity]:malformed,model:OTHER},{model:OTHER,eventType:'tool_result',
      inputTokens:undefined,outputTokens:undefined,observedAt:new Date(AT-3_600_000).toISOString()}));
     const native={traceId:f.trace,'conversation.id':SESSION,[identity]:literal,model:MODEL,otelEventName:'codex.sse_event'};
     if(targetKind==='native-target')f.deliver(f.append(make(1,native,{model:MODEL})),false);
     else{f.append(make(2,native,{model:MODEL}));f.deliver(f.append(make(1,{})),false);}
    });
 // The exact persisted-legacy A/B/A counterexample: real lineage first, original
 // producer bytes restored together, no paid capture/coverage witness injected.
 for(const noise of [0,129])cell(`linked-legacy-request-A-B-A/${noise}`,noise,f=>{
  const events=[MODEL,OTHER,MODEL].map((model,n)=>make(n+1,{otelEventName:'codex.sse_event',model,request_id:'one-physical-request'},{model}));
  for(const e of events){f.append(make(Number(e.id.slice(-12)),{}, {eventType:e.eventType,inputTokens:undefined,outputTokens:undefined}));
   const raw={...e,metadata:{...e.metadata,installationEpochId:f.b.workspaceBinding()!.currentInstallationEpochId}};
   f.b.database.prepare('update buffered_events set payload_json=?,model=?,input_tokens=?,output_tokens=? where id=?')
    .run(JSON.stringify(raw),e.model,19,2,e.id);}
  f.deliver(events[0]!,true);
 });
 for(const noise of [0,129])for(const identity of ['request','call','turn'] as const)
  for(const conflict of ['model','account','session'] as const)for(const state of ['counterless','distant','gap','acked'] as const){
   // A turn belongs to a conversation; the different-conversation control below
   // deliberately proves that equal turn text cannot cross that boundary.
   if(identity==='turn'&&conflict==='session')continue;
   cell(`linked/${identity}/${conflict}/${state}/${noise}`,noise,f=>{
    const key=identity==='request'?'request_id':identity==='call'?'call_id':'codexTurnId';
    const model=conflict==='model'?OTHER:MODEL,meta:Record<string,unknown>={[key]:'physical',model};
    if(state==='gap')f.append(make(4,{[key]:'physical',model:MODEL},
     {eventType:'tool_result',model:MODEL,inputTokens:undefined,outputTokens:undefined}));
    const fact=f.append(make(3,meta,{eventType:state==='acked'||state==='gap'?'assistant_response':'tool_result',
     model,actorId:conflict==='account'?B:A,sessionId:conflict==='session'?uuid(997):SESSION,
     ...(state==='acked'||state==='gap'?{inputTokens:7,outputTokens:1}:{inputTokens:undefined,outputTokens:undefined}),
     ...(state==='acked'?{metadata:{...meta,otelEventName:'codex.sse_event'}}:{}),
     ...(state==='distant'?{observedAt:new Date(AT-3_600_000).toISOString()}:{} )}));
    let frozen:ReturnType<Fixture['freezeFact']>|undefined;
    if(state==='gap'||state==='acked'){frozen=f.freezeFact(fact,state==='acked');assert.equal(isCaptureGap(frozen.event),state==='gap');}
    const target=f.append(make(1,{[key]:'physical',model:MODEL,otelEventName:'codex.sse_event'},{model:MODEL}));
    f.deliver(target,true);if(frozen)f.frozenUnchanged(frozen);
   });
  }
 for(const noise of [0,129])cell(`linked-transitive-request-call/${noise}`,noise,f=>{
  f.append(make(3,{request_id:'R',call_id:'C'},{eventType:'tool_result',inputTokens:undefined,outputTokens:undefined}));
  f.append(make(4,{call_id:'C',model:OTHER},{model:OTHER,eventType:'tool_result',inputTokens:undefined,outputTokens:undefined}));
  f.deliver(f.append(make(1,{request_id:'R',model:MODEL,otelEventName:'codex.sse_event'},{model:MODEL})),true);
 });
 for(const noise of [0,129])for(const scope of ['different-request','different-typed-namespace','different-conversation-turn'] as const)
  cell(`linked-positive/${scope}/${noise}`,noise,f=>{
   const targetMeta=scope==='different-conversation-turn'?{codexTurnId:'same'}:{request_id:'same'};
   const factMeta=scope==='different-request'?{request_id:'different'}:scope==='different-typed-namespace'?{call_id:'same'}:{codexTurnId:'same'};
   f.append(make(3,{...factMeta,model:OTHER},{model:OTHER,eventType:'tool_result',inputTokens:undefined,outputTokens:undefined,
    ...(scope==='different-conversation-turn'?{sessionId:uuid(997)}:{})}));
   f.deliver(f.append(make(1,{...targetMeta,model:MODEL,otelEventName:'codex.sse_event'},{model:MODEL})),false);
  });
 cell('direct-trace-free-SSE-survives-500-unrelated-requests',0,f=>{
  for(let n=0;n<500;n++)f.append(make(1000+n,{request_id:'other-'+n,model:OTHER,otelEventName:'codex.sse_event'},{model:OTHER}));
  f.deliver(f.append(make(1,{request_id:'target',model:MODEL,otelEventName:'codex.sse_event'},{model:MODEL})),false);
 });
 for(const noise of [0,129])for(const contradiction of [false,true])cell(`saved-span-rollout/${contradiction}/${noise}`,noise,f=>{
  recordCodexTurnModel(f.b.database,SESSION,'T',MODEL,A);
  const span=f.append(make(1,{traceId:f.trace,codexTurnId:'T',otelEventName:'handle_responses'}));
  // Same-trace counterless tools do not justify a false gap in a clean pair.
  for(let n=0;n<noise;n++)f.append(make(2000+n,{traceId:f.trace},{eventType:'tool_result',inputTokens:undefined,outputTokens:undefined}));
  f.append(make(2,{usageSource:'rollout',codexTurnId:'T'},{eventType:'usage_rollout',model:MODEL}));
  assert.ok(f.b.database.prepare('select 1 from codex_span_rollout_pairs where span_id=?').get(span.id),'saved pair must exist');
  if(contradiction)f.append(make(3,{traceId:f.trace,model:OTHER},{eventType:'tool_result',model:OTHER,inputTokens:undefined,outputTokens:undefined}));
  if(contradiction){const gap=f.capture(span);assert.equal(isCaptureGap(gap),true);
   assert.equal(gap.model,undefined);assert.equal(gap.inputTokens,undefined);assert.equal(gap.metadata.modelGapInputTokens,19);
   const rollout=make(2,{usageSource:'rollout',codexTurnId:'T'},{eventType:'usage_rollout',model:MODEL});f.deliver(rollout,true);}
  else {const observation=f.capture(span);assert.equal(isCaptureGap(observation),false);assert.equal(observation.model,MODEL);
   assert.equal(observation.inputTokens,undefined);assert.equal(observation.metadata.modelCaptureInputTokens,19);
   const rollout=make(2,{usageSource:'rollout',codexTurnId:'T'},{eventType:'usage_rollout',model:MODEL});f.deliver(rollout,false);}

 });
 for(const noise of [0,129])cell(`native-turn-table-veto-before-exact-pair/${noise}`,noise,f=>{
  recordCodexTurnModel(f.b.database,SESSION,'T',OTHER,A);
  f.append(make(2,{otelEventName:'codex.sse_event',codexTurnId:'T',model:MODEL},{model:MODEL}));
  const target=f.append(make(1,{usageSource:'rollout',codexTurnId:'T'},{eventType:'usage_rollout',model:MODEL}));
  f.deliver(target,true);
 });
 for(const noise of [0,129])cell(`frozen-named-custody-is-immutable/${noise}`,noise,f=>{
  const target=f.append(make(1,{traceId:f.trace,otelEventName:'codex.sse_event',model:MODEL},{model:MODEL}));
  const frozen=f.freezeFact(target,true);assert.equal(isCaptureGap(frozen.event),false);
  f.append(make(3,{traceId:f.trace,model:OTHER},{eventType:'tool_result',model:OTHER,inputTokens:undefined,outputTokens:undefined}));
  const c=f.capture(target);assert.equal(c.model,MODEL);assert.equal(c.inputTokens,19);assert.equal(c.outputTokens,2);f.frozenUnchanged(frozen);
 });
 assert.equal(executed,340,'every declared matrix cell executes');
 console.log(JSON.stringify({executed,failed:failures.length,failures}));completion.complete();
}finally{fs.rmSync(root,{recursive:true,force:true});}
