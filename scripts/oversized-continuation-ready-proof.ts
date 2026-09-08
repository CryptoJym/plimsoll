import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {LocalEventBuffer} from '../packages/collector-cli/src/buffer';
import {ensureJsonlScanState,DEFAULT_JSONL_TAILER_IO} from '../packages/collector-cli/src/jsonl-byte-tailer';
import {ensureJsonlContinuationStore,readJsonlContinuation} from '../packages/collector-cli/src/jsonl-continuation';
import {restore} from '../packages/collector-cli/src/oversized-extractor.mjs';
async function main(){
 const dir=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),'oversized-ready-'))),receipts=[];
 for(const provider of ['codex','claude'] as const){
   const db=new LocalEventBuffer(path.join(dir,provider+'.sqlite'),{workspaceId:'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'}),database=db.database;
   ensureJsonlScanState(database);ensureJsonlContinuationStore(database);
   const file=path.join(dir,provider+'.jsonl'),v='s'.repeat(3000);
   const record=provider==='codex'?{padding:'X'.repeat(90000),type:'fixture_ignored',timestamp:v,payload:{id:v,model:v,originator:v}}:{padding:'X'.repeat(90000),type:'fixture_ignored',timestamp:v,sessionId:v,cwd:v,message:{id:v}};
   fs.writeFileSync(file,JSON.stringify(record)+'\n');
   const read=(n=65536)=>readJsonlContinuation(file,fs.statSync(file),undefined,{maxBytes:n,maxRecords:64},DEFAULT_JSONL_TAILER_IO,{database,provider,cursorKey:file,directory:dir,deadline:performance.now()+200,eligible:()=>true})!;
   const raw=()=> (database.prepare('select envelope_json from jsonl_continuations').get() as any).envelope_json;
   const save=(r:ReturnType<typeof read>)=>{try{r.assertStableForCommit();db.transactionWithRepoContextHandoffs(()=>r.continuation!.applyCheckpoint());}finally{r.close();}};
   save(read());save(read());
   const e=JSON.parse(raw()),oldPartial=e.prefix.end-e.prefix.start-e.prefix.fullBytes;
   save(read(fs.statSync(file).size-e.prefix.end+oldPartial));assert.equal(restore(JSON.parse(raw()).parser).status,'ready');
   const frozen=raw();
   for(const limit of [0,1,2048]){const r=read(limit);assert.equal(r.bytesRead,0);assert.equal(r.continuation?.action,'park');assert.equal(r.continuation?.reason,'insufficient_budget');assert(r.continuation!.requiredMinimumBytes!>12000);assert.equal(raw(),frozen);r.close();}
   const ready=read();assert.equal(ready.continuation?.action,'complete');assert.equal(ready.lines.length,1);assert.equal(ready.continuation?.scanBytesAdvanced,0);assert(ready.bytesRead>12000&&ready.bytesRead<=17408);ready.close();
   // A source can be stable while enrollment changes: reject the proposal in
   // the write transaction, and fence subsequent reads before source I/O.
   const pending=read();const epoch=(database.prepare('select current_installation_epoch_id from collector_workspace_binding').get() as any).current_installation_epoch_id;
   database.prepare('update collector_workspace_binding set current_installation_epoch_id=?').run('ffffffff-eeee-4ddd-8ccc-bbbbbbbbbbbb');
   assert.throws(()=>db.transactionWithRepoContextHandoffs(()=>pending.continuation!.remove()),/stale_continuation/);pending.close();
   const mismatch=read();assert.equal(mismatch.bytesRead,0);assert.equal(mismatch.continuation?.reason,'binding_mismatch');mismatch.close();assert.equal(raw(),frozen);
   database.prepare('update collector_workspace_binding set current_installation_epoch_id=?').run(epoch);
   receipts.push({provider,readyBytes:ready.bytesRead,requiredMinimumBytes:ready.bytesRead,checkpointSize:Buffer.byteLength(frozen),checks:['READY persists without scalar materialization','zero read/mutation for undersized READY','bounded scalar/probe reads','actual enrollment mismatch and stale transaction rejected']});db.close();
 }
 console.log(JSON.stringify({status:'PASS',receipts,fixture:dir}));
}
main().catch(error=>{console.error(error);process.exitCode=1;});
