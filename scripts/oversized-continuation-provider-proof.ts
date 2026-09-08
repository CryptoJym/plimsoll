import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {LocalEventBuffer} from '../packages/collector-cli/src/buffer';
import {RolloutTailer} from '../packages/collector-cli/src/rollout-tailer';
import {TranscriptTailer} from '../packages/collector-cli/src/transcript-tailer';
import {jsonlScanStateKey} from '../packages/collector-cli/src/jsonl-byte-tailer';
async function main(){
const root=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),'oversized-provider-')));
const id='aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
const now=new Date().toISOString();
const count=(n:number,pad=0)=>JSON.stringify({type:'event_msg',timestamp:now,payload:{type:'token_count',discard:'X'.repeat(pad),info:{total_token_usage:{input_tokens:n,cached_input_tokens:0,output_tokens:n/10,reasoning_output_tokens:0}}}})+'\n';
const assistant=(n:number,pad=0)=>JSON.stringify({type:'assistant',sessionId:id,timestamp:now,message:{id:'m'+n,model:'claude-sonnet-4-20250514',content:'X'.repeat(pad),usage:{input_tokens:n,output_tokens:n/10,cache_creation_input_tokens:0,cache_read_input_tokens:0}}})+'\n';
const receipts=[];
for(const provider of ['codex','claude'] as const){
 const dir=path.join(root,provider),leaf=provider==='codex'?path.join(dir,...now.slice(0,10).split('-')):path.join(dir,'project');fs.mkdirSync(leaf,{recursive:true});
 const file=path.join(leaf,provider==='codex'?`rollout-${now.slice(0,10)}T00-00-00-${id}.jsonl`:`${id}.jsonl`);
 const db=new LocalEventBuffer(path.join(root,provider+'.sqlite'));
 const tailer=provider==='codex'?new RolloutTailer(db,dir,()=>[]):new TranscriptTailer(db,dir);
 fs.writeFileSync(file,provider==='codex'?JSON.stringify({type:'session_meta',timestamp:now,payload:{id}})+'\n'+count(0):assistant(10));
 const first=await tailer.scan({scope:'full'});assert.equal(first.readErrors,0);
 const row=()=>db.database.prepare('select * from rollout_scan_state where file=?').get(jsonlScanStateKey(file));
 const before=row();assert((before as any).committed_offset>0);
 fs.appendFileSync(file,provider==='codex'?count(100,700*1024):assistant(100,700*1024));
 const result=await tailer.scan({scope:'full'});
 console.log(JSON.stringify({provider,result,envelope:db.database.prepare('select envelope_json from jsonl_continuations').all().map((r:any)=>{const e=JSON.parse(r.envelope_json);return {reason:e.reason,offset:e.prefix.end}})}));
 assert.equal(result.readErrors,0);
 assert.equal((row() as any).committed_offset,fs.statSync(file).size);
 assert.equal((db.database.prepare('select count(*) as n from jsonl_continuations').get() as {n:number}).n,0);
 const again=await tailer.scan({scope:'full'});assert.equal(again.eventsAppended,0);assert.equal(again.bytesRead,0);
 receipts.push({provider,bytes:result.bytesRead,committed:result.recordsCommitted,events:result.eventsAppended,scan:result.continuationBytesAdvanced});
 tailer.close();db.close();
}
console.log(JSON.stringify({status:'PASS',receipts,fixture:root}));

}
main().catch(error=>{console.error(error);process.exitCode=1;});
