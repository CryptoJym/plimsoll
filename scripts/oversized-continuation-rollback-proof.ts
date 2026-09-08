import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import {buildSync} from 'esbuild';
import {LocalEventBuffer} from '../packages/collector-cli/src/buffer';
import {RolloutTailer} from '../packages/collector-cli/src/rollout-tailer';
import {TranscriptTailer} from '../packages/collector-cli/src/transcript-tailer';
import {DEFAULT_JSONL_TAILER_IO,jsonlScanStateKey} from '../packages/collector-cli/src/jsonl-byte-tailer';
import {rootCursorKey,type CaptureRoot} from '../packages/collector-cli/src/capture-root-inventory';
import {retireJsonlContinuations,ContinuationAdmission} from '../packages/collector-cli/src/jsonl-continuation';
async function main(){
 const baseline=process.env.PLIMSOLL_OVERSIZED_LEGACY_SOURCE;assert(baseline,'supply the sealed 411-file 0.7.4 source input');
 const input=JSON.parse(fs.readFileSync(process.env.PLIMSOLL_OVERSIZED_INPUT_MANIFEST!,'utf8'));
 const entries=Object.entries(input.files).map(([path,sha256])=>({path,sha256}));let verified=0;
 for(const row of entries){const actual:string=crypto.createHash('sha256').update(fs.readFileSync(path.join(baseline,row.path))).digest('hex');assert.equal(actual,row.sha256,row.path);verified++;}assert.equal(verified,411);
 const generated=fs.mkdtempSync(path.join(__dirname,'.oversized-rollback-')),bundle=path.join(generated,'old.cjs');
 buildSync({stdin:{contents:`export {RolloutTailer} from ${JSON.stringify(path.join(baseline,'packages/collector-cli/src/rollout-tailer.ts'))}; export {TranscriptTailer} from ${JSON.stringify(path.join(baseline,'packages/collector-cli/src/transcript-tailer.ts'))};`,resolveDir:process.cwd(),loader:'ts'},platform:'node',format:'cjs',bundle:true,outfile:bundle,external:['better-sqlite3'],nodePaths:[path.join(process.cwd(),'node_modules')],logLevel:'silent'});
 const old=require(bundle),dir=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),'oversized-rollback-'))),receipts=[];
 for(const provider of ['codex','claude_code'] as const){
   const ledger=path.join(dir,provider+'.sqlite'),buffer=new LocalEventBuffer(ledger,{workspaceId:"bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"}),root:CaptureRoot={source:provider,rootId:provider,profileId:'fixture',installationEpochId:buffer.workspaceBinding()!.currentInstallationEpochId!,directory:path.join(dir,provider)};
   const leaf=path.join(root.directory,...(provider==='codex'?new Date().toISOString().slice(0,10).split('-'):['project']));fs.mkdirSync(leaf,{recursive:true});
   const file=path.join(leaf,'rollout-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.jsonl');
   fs.writeFileSync(file,JSON.stringify({type:'fixture_ignored'})+'\n');
   let tailer=provider==='codex'?new RolloutTailer(buffer,undefined,()=>[],DEFAULT_JSONL_TAILER_IO,[root]):new TranscriptTailer(buffer,undefined,DEFAULT_JSONL_TAILER_IO,[root]);
   await tailer.scan({scope:'full'});
   const key=jsonlScanStateKey(rootCursorKey([root],file)),row=()=>buffer.database.prepare('select * from rollout_scan_state where file=?').get(key) as any,frozen=JSON.stringify(row());assert(row().committed_offset>0);
   fs.appendFileSync(file,JSON.stringify({padding:'X'.repeat(850*1024)})+'\n');let validations=0;
   await tailer.scan({scope:'full',onProgress:p=>p.stage!=='jsonl_validation'||++validations<=2});
   assert.equal(JSON.stringify(row()),frozen);assert(buffer.database.prepare('select 1 from jsonl_continuations').get());
   const bytes=fs.readFileSync(file);bytes[10]=89;fs.writeFileSync(file,bytes);
   const refused=await tailer.scan({scope:'full'});assert.equal(refused.continuationReasons?.rewrite_ambiguous,1);assert.equal(JSON.stringify(row()),frozen);tailer.close();
   // Negative control: old code with the root enabled corrupts the cursor truth.
   const unsafeLedger=path.join(dir,provider+'-unsafe.sqlite');await buffer.database.backup(unsafeLedger);
   const unsafeBuffer=new LocalEventBuffer(unsafeLedger,{workspaceId:"bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"});
   const unsafe=provider==='codex'?new old.RolloutTailer(unsafeBuffer,undefined,()=>[],DEFAULT_JSONL_TAILER_IO,[root]):new old.TranscriptTailer(unsafeBuffer,undefined,DEFAULT_JSONL_TAILER_IO,[root]);
   await unsafe.scan({scope:'full'});const unsafeRow=unsafeBuffer.database.prepare('select * from rollout_scan_state where file=?').get(key) as any;
   assert.notEqual(JSON.stringify(unsafeRow),frozen);assert.equal(unsafeRow.committed_offset,0);unsafe.close();unsafeBuffer.close();
   // Existing configured empty inventory is respected by BOTH versions. This
   // is a synthetic owner-controlled downgrade fence, not a live config write.
   let bodyReads=0;const io={...DEFAULT_JSONL_TAILER_IO,readTail:(...args:Parameters<typeof DEFAULT_JSONL_TAILER_IO.readTail>)=>{bodyReads++;return DEFAULT_JSONL_TAILER_IO.readTail(...args);}};
   const oldDisabled=provider==='codex'?new old.RolloutTailer(buffer,root.directory,()=>[],io,[]):new old.TranscriptTailer(buffer,root.directory,io,[]);
   await oldDisabled.scan({scope:'full'});oldDisabled.close();assert.equal(bodyReads,0);assert.equal(JSON.stringify(row()),frozen);
   const newDisabled=provider==='codex'?new RolloutTailer(buffer,root.directory,()=>[],io,[]):new TranscriptTailer(buffer,root.directory,io,[]);
   await newDisabled.scan({scope:'full'});newDisabled.close();assert.equal(bodyReads,0);assert.equal(JSON.stringify(row()),frozen);
   const tombstone=(buffer.database.prepare('select envelope_json from jsonl_continuations').get() as any).envelope_json;assert.equal(JSON.parse(tombstone).reason,'retired_binding');assert(Buffer.byteLength(tombstone)<512);
   tailer=provider==='codex'?new RolloutTailer(buffer,undefined,()=>[],io,[root]):new TranscriptTailer(buffer,undefined,io,[root]);
   const reenabled=await tailer.scan({scope:'full'});assert.equal(bodyReads,0);assert.equal(reenabled.continuationReasons?.retired_binding,1);assert.equal(JSON.stringify(row()),frozen);tailer.close();
   // Retention compaction traverses at most 16 rows per maintenance call and
   // never deletes the compatibility fence or changes any legacy cursor.
   for(let i=0;i<40;i++)buffer.database.prepare('insert into jsonl_continuations(provider,file_key,envelope_json) values (?,?,?)').run(provider==='codex'?'codex':'claude',crypto.createHash('sha256').update('retirement'+i).digest('hex'),tombstone);
   let visits=0;for(let i=0;i<4;i++){const n=retireJsonlContinuations(buffer.database,provider==='codex'?'codex':'claude',[],performance.now()+200);assert(n<=16);visits+=n;}assert(visits>=41);assert.equal(JSON.stringify(row()),frozen);
   receipts.push({provider,oldInputFiles:verified,unsafeOldOffset:unsafeRow.committed_offset,preservedOffset:row().committed_offset,disabledBodyReads:bodyReads,retirementVisits:visits,tombstoneBytes:Buffer.byteLength(tombstone)});buffer.close();
 }
 const admission=new ContinuationAdmission();admission.beginCadence();for(let i=0;i<65;i++)admission.park(String(i));assert.equal((admission as any).held.size,64);assert(!admission.allows('64'));for(let i=0;i<4;i++)admission.beginCadence();assert(admission.allows('64'));
 console.log(JSON.stringify({status:'PASS',checks:['unmodified 0.7.4 negative rewrite counterexample','disabled configured inventory fences both readers','old cursor bytes unchanged','root reenable remains fenced after retirement','retirement <=16 per call','admission metadata <=64 and backoff <=4 cadences'],receipts,fixture:dir}));
 fs.rmSync(generated,{recursive:true,force:true});
}
main().catch(error=>{console.error(error);process.exitCode=1;});
