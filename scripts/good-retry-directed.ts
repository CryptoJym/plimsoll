import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { captureCodexModel, recordCodexTurnModel } from '../packages/collector-cli/src/codex-model-capture';
import { explodeOtlpPayload } from '../packages/collector-cli/src/otlp';
import { aiInteractionEventSchema } from '../packages/shared/src/index';
const root=fs.mkdtempSync(path.join(os.tmpdir(),'review-good-retry-'));
const session='22222222-2222-4222-8222-222222222222';
const workspace='11111111-1111-4111-8111-111111111111';
const at=Date.now()-300000;
const attr=(key:string,value:string|number)=>({key,value:typeof value==='number'?{intValue:String(value)}:{stringValue:value}});
const outcomes=[];
try {
 for(const tier of ['native-trace-peer','native-local-turn']) {
  let now=new Date(at+2000);
  const opts={workspaceId:workspace,deviceId:'review-device',enrollmentNow:()=>new Date(at-1000),delivery:{enabled:true,now:()=>now}};
  const file=path.join(root,tier+'.sqlite');let b=new LocalEventBuffer(file,opts);
  try {
   const target=tier==='native-trace-peer'?explodeOtlpPayload({resourceSpans:[{resource:{attributes:[attr('service.name','codex-app-server')]},scopeSpans:[{spans:[{name:'handle_responses',traceId:'f'.repeat(32),spanId:'1'.repeat(16),startTimeUnixNano:String(BigInt(at)*1000000n),endTimeUnixNano:String(BigInt(at+1000)*1000000n),attributes:[attr('gen_ai.usage.input_tokens',19),attr('gen_ai.usage.output_tokens',2),attr('conversation.id',session)]}]}]}]},{source:'codex'}).events[0]!.event:aiInteractionEventSchema.parse({id:'00000000-0000-4000-8000-000000000801',source:'codex',dataMode:'metadata',eventType:'usage_rollout',sessionId:session,observedAt:new Date(at).toISOString(),inputTokens:19,outputTokens:2,metadata:{usageSource:'rollout',codexTurnId:'native-turn'}});
   if(tier==='native-trace-peer') {
    const evidence=explodeOtlpPayload({resourceLogs:[{resource:{attributes:[attr('service.name','codex-app-server')]},scopeLogs:[{logRecords:[{timeUnixNano:String(BigInt(at+1000)*1000000n),traceId:'f'.repeat(32),attributes:[attr('event.name','codex.sse_event'),attr('model','gpt-6.1-sol'),attr('conversation.id',session)]}]}]}]},{source:'codex'}).events[0]!.event;
    b.append(evidence);const context=b.delivery.lease({now});b.delivery.acknowledge(context.leaseId,context.items.map(i=>i.deliveryId),now);
   } else recordCodexTurnModel(b.database,session,'native-turn','gpt-6.1-sol');
   b.append(target);now=new Date(at+63000);
   const first=b.delivery.lease({now});const initial=first.items.find(i=>i.rawId===target.id)!;
   assert.ok(initial);assert.equal(initial.envelope.event.model,'gpt-6.1-sol');assert.equal(initial.envelope.event.inputTokens,19);assert.equal(initial.envelope.event.outputTokens,2);
   const frozen=JSON.stringify(initial.envelope);b.close();now=new Date(at+184000);b=new LocalEventBuffer(file,opts);
   const raw=JSON.parse((b.database.prepare('select payload_json as payload from buffered_events where id=?').get(target.id) as any).payload);
   const validated=captureCodexModel(b.database,raw,target.id);assert.equal(validated.model,'gpt-6.1-sol');assert.equal(validated.inputTokens,19);
   const second=b.delivery.lease({now});const retry=second.items.find(i=>i.deliveryId===initial.deliveryId);
   const replacement=b.delivery.lease({now}).items.find(i=>i.rawId===target.id);
   const receipt=b.database.prepare('select reason,terminal_state from upload_receipts where delivery_id=?').get(initial.deliveryId);
   const passed=second.locallyDead===0&&!!retry&&JSON.stringify(retry.envelope)===frozen;
   outcomes.push({tier,passed,first:initial.envelope,rawValidation:validated,locallyDead:second.locallyDead,retry:retry?.envelope,replacement:replacement?.envelope,receipt,expected:'valid native captured usage retries with identical ID, model and counters; no ACK has been received'});
  } finally { b.close(); }
 }
 console.log(JSON.stringify({case:'genuine-native-usage-survives-head-retry',outcomes},null,2));
 if(outcomes.some(x=>!x.passed))process.exitCode=1;
} finally {fs.rmSync(root,{recursive:true,force:true});}
