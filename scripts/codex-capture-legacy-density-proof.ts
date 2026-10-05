import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { captureCodexModel, isCaptureGap } from '../packages/collector-cli/src/codex-model-capture';
import { codexResponseCoverage } from '../packages/collector-cli/src/codex-response-coverage';
import { rowHasAdmittedUsage } from '../packages/collector-cli/src/usage-authority';
import { aiInteractionEventSchema, type AiInteractionEvent } from '../packages/shared/src/index';
import { nativeCodexFixture } from './lib/native-codex-fixture';
import { createProofCompletion } from './lib/proof-completion';
const completion = createProofCompletion('codex-capture-legacy-density',36);
const root=fs.mkdtempSync(path.join(os.tmpdir(),'codex-capture-legacy-density-'));
const at=new Date().toISOString();
const event=(n:number)=>aiInteractionEventSchema.parse({id:`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,
 source:'codex',eventType:'assistant_response',observedAt:at,inputTokens:0,outputTokens:7});
function open(name:string){return new LocalEventBuffer(path.join(root,name+'.sqlite'),{
 workspaceId:'00000000-0000-4000-8000-000000000001',deviceId:'fixture-device',delivery:{enabled:true}});}
try {
 for(const [name,metadata] of [['missing',undefined],['null',null],['array',[]],['number',5],['string','legacy'],['empty',{}]] as const){
  const b=open(name);
  try {
   const raw={...event(1),metadata} as AiInteractionEvent;b.append(event(1));
   b.database.prepare('update buffered_events set payload_json=? where id=?').run(JSON.stringify(raw),raw.id);
   completion.check(name+'_cannot_cover_response',codexResponseCoverage(b.database,raw)===undefined);
   completion.check(name+'_cannot_own_usage',rowHasAdmittedUsage(b.database,raw.id)===false);
   const gap=captureCodexModel(b.database,raw);
   completion.check(name+'_diagnostics_keep_reported_zero',isCaptureGap(gap)&&gap.inputTokens===undefined&&gap.outputTokens===undefined&&
    gap.model===undefined&&gap.metadata.modelGapInputTokens===0&&gap.metadata.modelGapOutputTokens===7);
  }finally{b.close();}
 }
 const b=open('unrelated-traces');
 try {
  let last!:AiInteractionEvent;
  for(let n=1;n<=500;n++){last={...event(n),...nativeCodexFixture(String(n))};assert.equal(b.append(last),true);}
  const captured=captureCodexModel(b.database,last);
  completion.check('unrelated_native_traces_do_not_overflow_evidence',!isCaptureGap(captured)&&captured.model===last.model&&captured.inputTokens===0&&captured.outputTokens===7);
 }finally{b.close();}
 const conflicting=open('same-trace');
 try {
  const target={...event(1),...nativeCodexFixture('same')};conflicting.append(target);
  for(let n=2;n<=131;n++) conflicting.append({...event(n),...nativeCodexFixture('same'),inputTokens:undefined,outputTokens:undefined,eventType:'otel_span'});
  const gap=captureCodexModel(conflicting.database,target);
  completion.check('same_native_trace_overflow_still_fails_closed',isCaptureGap(gap)&&gap.inputTokens===undefined&&gap.outputTokens===undefined);
 }finally{conflicting.close();}
 const legacy=open('sql-source-provenance');
 try {
  const payloads=[{}, {source:'claude_code',metadata:{}}, {source:'grok',metadata:{}}, {source:'unknown',metadata:{}}];
  for (let index=0;index<payloads.length;index++) {
   const raw=event(index+1);legacy.append(raw);
   legacy.database.prepare('update buffered_events set payload_json=? where id=?').run(JSON.stringify(payloads[index]),raw.id);
   const stored=legacy.database.prepare('select input_tokens as input,output_tokens as output from buffered_events where id=?')
     .get(raw.id) as {input:number;output:number};
   completion.check('legacy_sql_codex_source_'+index+'_cannot_bypass_admission',
    rowHasAdmittedUsage(legacy.database,raw.id)===false&&stored.input===0&&stored.output===7);
  }
  const genuine={...event(10),source:'claude_code' as const};legacy.append(genuine);
  legacy.database.prepare('update buffered_events set payload_json=? where id=?').run('{}',genuine.id);
  completion.check('genuine_legacy_claude_retains_authority',rowHasAdmittedUsage(legacy.database,genuine.id)===true);
  const missingCounters={...event(11),inputTokens:undefined,outputTokens:undefined};legacy.append(missingCounters);
  legacy.database.prepare('update buffered_events set payload_json=? where id=?').run('{}',missingCounters.id);
  completion.check('unknown_legacy_codex_is_not_reported_as_zero',rowHasAdmittedUsage(legacy.database,missingCounters.id)===false&&
   (legacy.database.prepare('select input_tokens as input,output_tokens as output from buffered_events where id=?')
    .get(missingCounters.id) as {input:null;output:null}).input===null);
  const native={...event(12),...nativeCodexFixture('provenance-native')};legacy.append(native);
  completion.check('valid_native_codex_still_admitted',rowHasAdmittedUsage(legacy.database,native.id)===true);
  const partial={...event(13),...nativeCodexFixture('provenance-partial'),outputTokens:undefined};legacy.append(partial);
  completion.check('partial_native_preserves_zero_and_unknown',rowHasAdmittedUsage(legacy.database,partial.id)===true&&
   captureCodexModel(legacy.database,partial).inputTokens===0&&captureCodexModel(legacy.database,partial).outputTokens===undefined);
 }finally{legacy.close();}
 const trace=open('native-producer-trace');
 try {
  const span={...event(1),metadata:{traceId:'1234567890abcdef1234567890abcdef',otelEventName:'handle_responses'}};
  trace.append(span);
  const producer={...event(2),model:'gpt-6-sol',inputTokens:undefined,outputTokens:undefined,
   eventType:'tool_result' as const,metadata:{traceId:span.metadata.traceId,model:'gpt-6-sol'}};
  trace.append(producer);
  const captured=captureCodexModel(trace.database,span);
  completion.check('one_native_tool_model_in_exact_trace_is_captured',!isCaptureGap(captured)&&captured.model==='gpt-6-sol'&&captured.inputTokens===0&&captured.outputTokens===7);
  const withoutTyped={...event(3),metadata:{...nativeCodexFixture('missing-typed').metadata}};
  trace.append(withoutTyped);
  const normalized=captureCodexModel(trace.database,withoutTyped);
  completion.check('missing_normalized_model_uses_its_native_trace_attribute',normalized.model==='gpt-6-sol'&&normalized.outputTokens===7);
  const other={...producer,id:event(4).id,model:'gpt-5.5',metadata:{...producer.metadata,model:'gpt-5.5'}};trace.append(other);
  completion.check('two_native_tool_models_in_trace_still_gap',isCaptureGap(captureCodexModel(trace.database,span)));
  const foreign={...event(5),metadata:{traceId:'fedcba0987654321fedcba0987654321',otelEventName:'handle_responses'}};trace.append(foreign);
  completion.check('neighbouring_tool_model_does_not_name_another_trace',isCaptureGap(captureCodexModel(trace.database,foreign)));
  const hook={...event(6),eventType:'session_end' as const,model:'gpt-6-sol',metadata:{input_tokens:0,output_tokens:0},inputTokens:0,outputTokens:0};trace.append(hook);
  completion.check('explicit_zero_hook_with_no_native_model_is_gap',isCaptureGap(captureCodexModel(trace.database,hook)));
  const resourceOnly={...event(7),model:'gpt-6-sol',metadata:{traceId:'aabbccddeeffaabbccddeeffaabbccddee',otelResourceAttributes:{model:'gpt-6-sol'}}};trace.append(resourceOnly);
  completion.check('resource_model_is_not_request_model',isCaptureGap(captureCodexModel(trace.database,resourceOnly)));
 }finally{trace.close();}
 const sse=open('independent-trace-free-sse');
 try {
  let last!:AiInteractionEvent;
  for(let n=1;n<=200;n++) {
   const model=n%2?'gpt-5.5':'gpt-6-sol';
   last={...event(n),model,metadata:{model,otelEventName:'codex.sse_event'}};
   assert.equal(sse.append(last),true);
  }
  const captured=captureCodexModel(sse.database,last);
  completion.check('trace_free_sse_keeps_its_own_native_model_amid_other_requests',
   !isCaptureGap(captured)&&captured.model===last.model&&captured.inputTokens===0&&captured.outputTokens===7);
  const guessed={...event(201),model:'gpt-6-sol',metadata:{otelEventName:'codex.sse_event'}};
  sse.append(guessed);
  completion.check('bare_trace_free_sse_still_cannot_guess_a_model',isCaptureGap(captureCodexModel(sse.database,guessed)));
 }finally{sse.close();}
 completion.complete();
}finally{fs.rmSync(root,{recursive:true,force:true});}
