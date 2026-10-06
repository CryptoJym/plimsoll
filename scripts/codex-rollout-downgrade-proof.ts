import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {LocalEventBuffer} from '../packages/collector-cli/src/buffer';
import {RolloutTailer} from '../packages/collector-cli/src/rollout-tailer';
import {captureCodexModel,isCaptureGap} from '../packages/collector-cli/src/codex-model-capture';
import {CaptureWorkBudget} from '../packages/collector-cli/src/capture-work-budget';
import {beginAutomaticCaptureBaseline,completeAutomaticCaptureBaseline,captureBaselineStatus,sealCaptureBaselineGenerations} from '../packages/collector-cli/src/capture-baseline';
import {jsonlScanStateKey} from '../packages/collector-cli/src/jsonl-byte-tailer';
import {jsonlCursorDigest} from '../packages/collector-cli/src/jsonl-continuation';
import {createProofCompletion} from './lib/proof-completion';
import {proofTempRoot,withReader} from './lib/legacy-reader';

const completion=createProofCompletion('codex-rollout-downgrade',44);
const root=fs.realpathSync(proofTempRoot('rollout-downgrade'));
const SESSION='22222222-2222-4222-8222-222222222222',MODEL='gpt-6-sol',OTHER='gpt-5.5';
const releases=[['0.7.51','71d6ff27f0d39aa31d188c9bcc31d37bf188c384'],
 ['0.7.50','121b55437555c3a3c34bafe5889f4d6d8870509f']] as const;
const head={Buffer:LocalEventBuffer,Tailer:RolloutTailer};
const reports:unknown[]=[];
function check(name:string,fn:()=>void){fn();completion.check(name);}

async function main(){
 try{for(const [version,commit] of releases)await withReader(commit,async old=>{
  const dir=path.join(root,version),sessions=path.join(dir,'sessions');
  const day=path.join(sessions,...new Date().toISOString().slice(0,10).split('-'));fs.mkdirSync(day,{recursive:true});
  const file=path.join(day,`rollout-fixture-${SESSION}.jsonl`),ledger=path.join(dir,'ledger.sqlite');
  const options={workspaceId:'11111111-1111-4111-8111-111111111111',deviceId:'rollout-downgrade',
   enrollmentNow:()=>new Date(Date.now()-7_200_000),delivery:{enabled:false}};
  const context=(turn:string,model=MODEL)=>({timestamp:new Date().toISOString(),type:'turn_context',payload:{turn_id:turn,model}});
  const usage=(input:number,output:number)=>({timestamp:new Date().toISOString(),type:'event_msg',payload:{type:'token_count',
   info:{total_token_usage:{input_tokens:input,output_tokens:output,cached_input_tokens:0,reasoning_output_tokens:0}}}});
  const append=(rows:unknown[])=>fs.appendFileSync(file,rows.map(r=>JSON.stringify(r)).join('\n')+'\n');
  append([{timestamp:new Date().toISOString(),type:'session_meta',payload:{id:SESSION}},context('old-turn'),usage(0,0)]);
  let b=new old.Buffer(ledger,options),tail=new old.Tailer(b,sessions,()=>[]);
  try{
   await tail.scan({scope:'full'});
   const cutoff=new Date().toISOString();
   for(const source of ['codex','claude_code'] as const){
    const start=beginAutomaticCaptureBaseline(b.database,source,{startedAt:cutoff,filesDiscovered:0});
    completeAutomaticCaptureBaseline(b.database,source,{runId:start.latestRun!.runId,completedAt:cutoff});
   }
   assert.equal(captureBaselineStatus(b.database).status,'complete');
   const stat=fs.statSync(file,{bigint:true});sealCaptureBaselineGenerations(b.database,'codex',
    [{path:file,device:stat.dev,inode:stat.ino,size:stat.size,birthtimeNs:stat.birthtimeNs}],cutoff);
  }finally{tail.close();b.close();}

  const scan=async(label:string,reader:{Buffer:any;Tailer:any},rows:unknown[],expected:{input:number;output:number})=>{
   append(rows);b=new reader.Buffer(ledger,options);tail=new reader.Tailer(b,sessions,()=>[]);
   try{
    const amounts=()=>b.database.prepare("select count(*) as rows,coalesce(sum(input_tokens),0) as input,coalesce(sum(output_tokens),0) as output from buffered_events where event_type='usage_rollout'").get();
    const before=amounts(),cadences=[];
    for(let cadence=0;cadence<3;cadence++){
     const r=await tail.scan({scope:'recent',automatic:{phase:'capture',budget:new CaptureWorkBudget()}});
     assert.equal(r.parseErrors,0);assert.equal(r.readErrors,0);assert.equal(r.statErrors,0);
     cadences.push({cadence,eventsAppended:r.eventsAppended,tokensAppended:r.tokensAppended,
      excludedGenerations:r.excludedGenerations,checkpointRebuilds:r.checkpointRebuilds,recordsCommitted:r.recordsCommitted});
    }
    const after=amounts(),delta={rows:after.rows-before.rows,input:after.input-before.input,output:after.output-before.output};
    const cursor=b.database.prepare('select parser_state_json as state,committed_offset as offset from rollout_scan_state where file=?').get(jsonlScanStateKey(file));
    const raw=JSON.parse(b.database.prepare("select payload_json as payload from buffered_events where event_type='usage_rollout' order by rowid desc limit 1").get().payload);
    reports.push({release:version,commit,stage:label,expected,delta,cadences,parserKeys:Object.keys(JSON.parse(cursor.state)),offset:cursor.offset,size:fs.statSync(file).size});
    return {delta,cursor,raw};
   }finally{tail.close();b.close();}
  };
  const stages=[
   {name:'release-capture',reader:old,rows:[usage(19,2)],input:19,output:2},
   {name:'head-context',reader:head,rows:[context('head-turn'),usage(48,9)],input:29,output:7},
   {name:'head-restart',reader:head,rows:[usage(67,11)],input:19,output:2},
   {name:'downgrade',reader:old,rows:[usage(86,13)],input:19,output:2},
   {name:'downgrade-next-cadence',reader:old,rows:[usage(105,15)],input:19,output:2},
  ];
  for(const s of stages){
   const r=await scan(s.name,s.reader,s.rows,s);
   check(`${version}/${s.name}/capture`,()=>assert.deepEqual(r.delta,{rows:1,input:s.input,output:s.output}));
   check(`${version}/${s.name}/released-parser-shape`,()=>{
    assert.equal(Object.hasOwn(JSON.parse(r.cursor.state),'turnId'),false);
    assert.equal(r.cursor.offset,fs.statSync(file).size);
   });
   if(s.name==='head-restart')check(`${version}/restart-keeps-exact-native-turn`,()=>assert.equal(r.raw.metadata.codexTurnId,'head-turn'));
  }
  b=new LocalEventBuffer(ledger,options);
  try{check(`${version}/released-writer-invalidates-turn-provenance`,()=>{
   const p=JSON.parse((b.database.prepare('select value from maintenance_state where key=?')
    .get(`codex_rollout_turn_v1:${jsonlScanStateKey(file)}`) as {value:string}).value);
   assert.equal(p.turnId,'head-turn');assert.notEqual(p.cursor,jsonlCursorDigest(b.database,file));
  });}finally{b.close();}
  const unknown=await scan('upgrade-after-released-writer',head,[usage(124,17)],{input:19,output:2});
  check(`${version}/unknown-turn-retains-raw-counters`,()=>assert.deepEqual(unknown.delta,{rows:1,input:19,output:2}));
  check(`${version}/unknown-turn-is-not-restored`,()=>assert.equal(unknown.raw.metadata.codexTurnId,undefined));
  b=new LocalEventBuffer(ledger,options);
  try{check(`${version}/unknown-turn-gaps-at-capture`,()=>{
   const c=captureCodexModel(b.database,unknown.raw,unknown.raw.id,false,false);
   assert.equal(isCaptureGap(c),true);assert.equal(c.model,undefined);assert.equal(c.inputTokens,undefined);
   assert.equal(c.metadata.modelGapInputTokens,19);assert.equal(c.metadata.modelGapOutputTokens,2);
  });}finally{b.close();}
  const fresh=await scan('new-native-turn',head,[context('fresh-turn',OTHER),usage(143,19)],{input:19,output:2});
  check(`${version}/fresh-turn-counters`,()=>assert.deepEqual(fresh.delta,{rows:1,input:19,output:2}));
  b=new LocalEventBuffer(ledger,options);
  try{
   check(`${version}/fresh-turn-model`,()=>{
    assert.equal(fresh.raw.metadata.codexTurnId,'fresh-turn');
    const c=captureCodexModel(b.database,fresh.raw,fresh.raw.id,false,false);assert.equal(isCaptureGap(c),false);
    assert.equal(c.model,OTHER);assert.equal(c.inputTokens,19);assert.equal(c.outputTokens,2);
   });
   check(`${version}/fresh-turn-real-wire-and-ack`,()=>{
    b.delivery.configure({enabled:true});b.delivery.repairRawById(fresh.raw.id);
    const now=new Date(Date.now()+62_000);b.database.prepare('update upload_outbox set next_attempt_at=?').run(now.toISOString());
    const lease=b.delivery.lease({now});const item=lease.items.find(i=>i.envelope.event.id===fresh.raw.id);assert.ok(item);
    assert.equal(item.envelope.event.model,OTHER);assert.equal(item.envelope.event.inputTokens,19);assert.equal(item.envelope.event.outputTokens,2);
    const ack=b.delivery.acknowledge(lease.leaseId,lease.items.map(i=>i.deliveryId),now);assert.equal(ack.locallyDead,0);
    assert.equal((b.database.prepare('select terminal_state as state from upload_receipts where delivery_id=?').get(item.deliveryId) as {state:string}).state,'acknowledged');
   });
   // Deliberately restore only the known r15 checkpoint key to exercise its
   // upgrade migration. Producer rows and financial witnesses are untouched.
   const cursor=b.database.prepare('select parser_state_json as state from rollout_scan_state where file=?').get(jsonlScanStateKey(file)) as {state:string};
   b.database.prepare('update rollout_scan_state set parser_state_json=? where file=?')
    .run(JSON.stringify({...JSON.parse(cursor.state),turnId:'fresh-turn'}),jsonlScanStateKey(file));
  }finally{b.close();}
  const migrated=await scan('legacy-pr-checkpoint-migration',head,[usage(162,21)],{input:19,output:2});
  check(`${version}/legacy-turn-migrates-without-loss`,()=>{assert.deepEqual(migrated.delta,{rows:1,input:19,output:2});assert.equal(migrated.raw.metadata.codexTurnId,'fresh-turn');});
  check(`${version}/legacy-turn-removed-from-parser`,()=>assert.equal(Object.hasOwn(JSON.parse(migrated.cursor.state),'turnId'),false));
  const returned=await scan('released-reader-after-migration',old,[usage(181,23)],{input:19,output:2});
  check(`${version}/released-capture-after-migration`,()=>assert.deepEqual(returned.delta,{rows:1,input:19,output:2}));
  check(`${version}/released-cursor-after-migration`,()=>assert.equal(returned.cursor.offset,fs.statSync(file).size));
 });
 console.log(JSON.stringify({proof:'codex-rollout-downgrade',reports}));completion.complete();
 }finally{fs.rmSync(root,{recursive:true,force:true});}
}
main().catch(e=>{console.error(e);process.exitCode=1});
