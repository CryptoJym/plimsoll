import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {LocalEventBuffer} from '../packages/collector-cli/src/buffer';
import {RolloutTailer} from '../packages/collector-cli/src/rollout-tailer';
import {TranscriptTailer} from '../packages/collector-cli/src/transcript-tailer';
import {DEFAULT_JSONL_TAILER_IO,loadJsonlScanCursor,jsonlScanStateKey} from '../packages/collector-cli/src/jsonl-byte-tailer';
import {readJsonlContinuation} from '../packages/collector-cli/src/jsonl-continuation';
async function main(){
 const home=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),'oversized-boundary-'))),receipts=[];
 for(const provider of ['codex','claude'] as const){
   const dir=path.join(home,provider),leaf=path.join(dir,...(provider==='codex'?new Date().toISOString().slice(0,10).split('-'):['project']));fs.mkdirSync(leaf,{recursive:true});
   const ledger=path.join(dir,'test.sqlite');let db=new LocalEventBuffer(ledger);
   const make=()=>provider==='codex'?new RolloutTailer(db,dir,()=>[]):new TranscriptTailer(db,dir);let tailer=make();
   let index=0;
   for(const content of [' '.repeat(90*1024)+'null\n',' '.repeat(90*1024)+'{}\n','"'+'X'.repeat(90*1024)+'"\n','['+' '.repeat(90*1024)+'{"type":"assistant","message":{"usage":{"input_tokens":999}}}]\n']){
     const file=path.join(leaf,`rollout-ignored-${index++}.jsonl`);fs.writeFileSync(file,content);
     const r=await tailer.scan({scope:'full'});assert.equal(r.readErrors,0);assert.equal(r.eventsAppended,0);assert.equal((db.database.prepare('select committed_offset from rollout_scan_state where file=?').get(jsonlScanStateKey(file)) as any).committed_offset,Buffer.byteLength(content));
   }
   const bad=path.join(leaf,'rollout-malformed.jsonl');fs.writeFileSync(bad,'{"padding":"'+'X'.repeat(90*1024)+'"!}\n');
   const malformed=await tailer.scan({scope:'full'});assert.equal(malformed.continuationReasons?.malformed_json,1);assert.equal(db.database.prepare('select * from rollout_scan_state where file=?').get(jsonlScanStateKey(bad)),undefined);
   const unfinished=path.join(leaf,'rollout-incomplete.jsonl');fs.writeFileSync(unfinished,'{"padding":"'+'X'.repeat(90*1024));
   const incomplete=await tailer.scan({scope:'full'});assert.equal(incomplete.continuationReasons?.incomplete_record,1);
   const repeated=await tailer.scan({scope:'full'});assert.equal(repeated.bytesRead,0);assert.equal(repeated.continuationBytesAdvanced??0,0);
   // Close actual SQLite and recreate both database and provider instances.
   tailer.close();db.close();db=new LocalEventBuffer(ledger);tailer=make();fs.appendFileSync(unfinished,'"}\n');
   const resumed=await tailer.scan({scope:'full'});assert.equal(resumed.readErrors,0);assert.equal(resumed.eventsAppended,0);assert.equal((db.database.prepare('select committed_offset from rollout_scan_state where file=?').get(jsonlScanStateKey(unfinished)) as any).committed_offset,fs.statSync(unfinished).size);
   const unsafe=path.join(leaf,'rollout-unsafe.jsonl');fs.writeFileSync(unsafe,'{"padding":"'+'X'.repeat(150*1024)+'"}\n');let validations=0;
   await tailer.scan({scope:'full',onProgress:p=>p.stage!=='jsonl_validation'||++validations<3});
   const cursor=loadJsonlScanCursor(db.database,unsafe,provider==='codex'?'codex-rollout':'claude-transcript',1,x=>x as any);
   const options={database:db.database,provider,cursorKey:unsafe,directory:dir,deadline:performance.now()+200,eligible:()=>true};
   const otherProvider=provider==='codex'?'claude':'codex';
   const mismatch=readJsonlContinuation(unsafe,fs.statSync(unsafe),cursor,{maxBytes:65536},DEFAULT_JSONL_TAILER_IO,{...options,provider:otherProvider});assert.equal(mismatch?.bytesRead,0);assert.equal(mismatch?.continuation?.reason,'binding_mismatch');mismatch?.close();
   const excluded=readJsonlContinuation(unsafe,fs.statSync(unsafe),cursor,{maxBytes:65536},DEFAULT_JSONL_TAILER_IO,{...options,eligible:()=>false});assert.equal(excluded?.bytesRead,0);excluded?.close();
   const moved=leaf+'-moved';fs.renameSync(leaf,moved);fs.symlinkSync(moved,leaf);
   const alias=readJsonlContinuation(unsafe,fs.statSync(unsafe),cursor,{maxBytes:65536},DEFAULT_JSONL_TAILER_IO,options);assert.equal(alias?.bytesRead,0);assert.equal(alias?.continuation?.reason,'source_changed');alias?.close();fs.unlinkSync(leaf);fs.renameSync(moved,leaf);
   const proposal=readJsonlContinuation(unsafe,fs.statSync(unsafe),cursor,{maxBytes:65536},DEFAULT_JSONL_TAILER_IO,{...options,deadline:performance.now()+200})!;
   const before=db.database.prepare('select envelope_json from jsonl_continuations order by file_key').all();
   fs.appendFileSync(unsafe,'null\n');assert.throws(()=>proposal.assertStableForCommit(),/source_changed/);proposal.close();assert.deepEqual(db.database.prepare('select envelope_json from jsonl_continuations order by file_key').all(),before);
   receipts.push({provider,validIgnoredRoots:4,malformedReason:'malformed_json',incompleteReason:'incomplete_record',reopenedDatabase:true,security:['cross-provider binding fenced','excluded zero reads','ancestor alias zero reads','changed final source rejects before transaction']});tailer.close();db.close();
 }
 console.log(JSON.stringify({status:'PASS',receipts,fixture:home}));
}
main().catch(error=>{console.error(error);process.exitCode=1;});
