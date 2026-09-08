import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import {LocalEventBuffer} from '../packages/collector-cli/src/buffer';
import {RolloutTailer} from '../packages/collector-cli/src/rollout-tailer';
import {DEFAULT_JSONL_TAILER_IO,loadJsonlScanCursor,jsonlScanStateKey, type JsonlTailRead} from '../packages/collector-cli/src/jsonl-byte-tailer';
import {readJsonlContinuation,jsonlCursorDigest,MAX_ENVELOPE_BYTES} from '../packages/collector-cli/src/jsonl-continuation';
import {checkpoint,restore} from '../packages/collector-cli/src/oversized-extractor.mjs';
async function main(){
 const dir=fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(),'oversized-state-'))),leaf=path.join(dir,...new Date().toISOString().slice(0,10).split('-'));
 fs.mkdirSync(leaf,{recursive:true}); const file=path.join(leaf,'rollout-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.jsonl');
 const db=new LocalEventBuffer(path.join(dir,'test.sqlite')),database=db.database;
 const prior=JSON.stringify({type:'session_meta',timestamp:new Date().toISOString(),payload:{id:'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee',originator:'state-proof'}})+'\n';
 fs.writeFileSync(file,prior); const tailer=new RolloutTailer(db,dir,()=>[]); await tailer.scan({scope:'full'});
 const key=jsonlScanStateKey(file),row=()=>database.prepare('select * from rollout_scan_state where file=?').get(key) as any;
 const original=row();
 database.prepare(`update rollout_scan_state set parser_state_json=?,scanned_at=?,work_remaining=0,size=committed_offset+37,deferred_bytes=37,
 unresolved_kind='record_exceeds_byte_budget',unresolved_offset=?,unresolved_observed_bytes=37,unresolved_available_bytes=37,unresolved_byte_budget=2048 where file=?`).run(JSON.stringify(JSON.parse(original.parser_state_json),null,2),'2026-09-08T00:00:01.001Z',original.committed_offset,key);
 const frozen=JSON.stringify(row()),frozenDigest=jsonlCursorDigest(database,file); assert(row().committed_offset>0);assert.equal(Object.keys(row()).length,21);
 fs.appendFileSync(file,JSON.stringify({ignored:'X'.repeat(900*1024)})+'\n');
 let reads=0;const native=fs.readSync;fs.readSync=((...args:any[])=>{reads++;return (native as any)(...args);}) as typeof native;
 const cursor=()=>{const r=row();return loadJsonlScanCursor(database,file,r.parser_kind,r.checkpoint_version,x=>x as any);};
 const options=()=>({database,provider:'codex' as const,cursorKey:file,directory:dir,deadline:performance.now()+200,eligible:()=>true});
 const read=(n=65536)=>readJsonlContinuation(file,fs.statSync(file),cursor(),{maxBytes:n,maxRecords:64},DEFAULT_JSONL_TAILER_IO,options())!;
 const raw=()=> (database.prepare('select envelope_json from jsonl_continuations').get() as any)?.envelope_json as string|undefined;
 const setRaw=(text:string)=>database.prepare('update jsonl_continuations set envelope_json=?').run(text);
 const commit=(r:JsonlTailRead)=>{try{r.assertStableForCommit();database.transaction(()=>r.continuation!.applyCheckpoint())();}finally{r.close();}assert.equal(JSON.stringify(row()),frozen);};
 const checks:string[]=[]; const ok=(s:string)=>checks.push(s);
 try{
   assert.equal(cursor()?.checkpointStatus,"valid");const seed=read(); assert.equal(seed.continuation?.action,'checkpoint');commit(seed);ok('initial partial preserves all 21 cursor columns and exact JSON');
   const a=read(4095),b=read(4095);commit(a);assert.throws(()=>database.transaction(()=>b.continuation!.applyCheckpoint())(),/stale_continuation/);b.close();ok('concurrent checkpoint CAS rejects stale proposal');
   const saved=raw()!;assert(Buffer.byteLength(saved)<=MAX_ENVELOPE_BYTES);assert(!saved.includes(dir));
   const e=JSON.parse(saved);assert.equal(e.prefix.end-e.prefix.start,4095);
   for(const budget of [0,1,2048,4095]){const n=reads,r=read(budget);assert.equal(r.bytesRead,0);assert.equal(reads,n);assert.equal(r.continuation?.reason,'insufficient_budget');assert.equal(r.continuation?.requiredMinimumBytes,4096);r.close();assert.equal(raw(),saved);}ok('oldPartial4095 with budgets0/1/2048/4095 performs zero reads and mutation');
   const exact=read(4096);assert.equal(exact.continuation?.scanBytesAdvanced,1);assert.equal(exact.bytesRead,4096);commit(exact);ok('4096 exact admission reads old partial plus one new byte');
   setRaw(saved);
   const mutations:Array<[string,(e:any)=>void]> = Object.keys(e.binding).map(k=>['binding '+k,(m:any)=>{m.binding[k]=k==='provider'?'claude':'f'.repeat(64);}]);
   mutations.push(['version',m=>m.version=2],['rollback version',m=>m.rollbackVersion=2],['prior cursor digest',m=>m.priorCursor='f'.repeat(64)],['extra field',m=>m.path=dir],['unsafe offset',m=>m.prefix.end=Number.MAX_SAFE_INTEGER+1],['fingerprint mismatch',m=>m.prefix.end--],['parser checksum',m=>m.parser=m.parser.replace('scanning','ready')],['record start',m=>{const p=restore(m.parser);p.recordStart=0;m.parser=checkpoint(p);m.prefix.start=0;}],['verification corrupt',m=>m.verification={snapshot:m.snapshot,prefix:{}}]);
   for(const [name,mutate] of mutations){const m=JSON.parse(saved);mutate(m);delete m.sha256;m.sha256=crypto.createHash("sha256").update(JSON.stringify(m)).digest("hex");setRaw(JSON.stringify(m));const n=reads;const r=read();assert.equal(r.bytesRead,0,name);assert.equal(reads,n,name);assert.equal(r.continuation?.action,'park',name);assert.equal(JSON.stringify(row()),frozen);r.close();}
   for(const text of ['{','null','[]','"bad"',saved.replace('scanning','ready')]){setRaw(text);const r=read();assert.equal(r.continuation?.reason,'invalid_envelope');assert.equal(r.bytesRead,0);r.close();}setRaw(saved);ok(`all ${mutations.length+5} binding/version/corruption fences preserve cursor`);
   const fail=read();database.exec("create trigger fail_checkpoint before update on jsonl_continuations begin select raise(abort,'injected_checkpoint_failure'); end");
   assert.throws(()=>database.transaction(()=>fail.continuation!.applyCheckpoint())(),/injected_checkpoint/);fail.close();database.exec('drop trigger fail_checkpoint');assert.equal(raw(),saved);assert.equal(JSON.stringify(row()),frozen);ok('failed checkpoint leaves envelope and cursor exact');
   const stale=read();database.prepare('update rollout_scan_state set scanned_at=? where file=?').run('changed',key);assert.throws(()=>database.transaction(()=>stale.continuation!.applyCheckpoint())(),/stale_continuation/);stale.close();database.prepare('update rollout_scan_state set scanned_at=? where file=?').run(JSON.parse(frozen).scanned_at,key);assert.equal(jsonlCursorDigest(database,file),frozenDigest);ok('prior stored cursor rechecked inside transaction');
   // Genuine reopen: same private DB and source, no new initial parser state.
   const resumed=read();assert((resumed.continuation?.scanBytesAdvanced??0)>0);commit(resumed);ok('restart resumes nonzero scan without cursor mutation');
   const beforeGrowth=JSON.parse(raw()!);fs.appendFileSync(file,'null\n');let verificationReads=0;
   for(let i=0;i<20;i++){const r=read();verificationReads+=r.continuation?.prefixBytesRead??0;if(r.continuation?.action==='complete'){r.close();break;}commit(r);}
   assert(verificationReads>=beforeGrowth.prefix.end-beforeGrowth.prefix.start);ok('finite append verifies entire saved prefix before advancing');
   // Same identity rewrite after saved progress fences before body reads.
   const rewrite=Buffer.from(fs.readFileSync(file));rewrite[prior.length+20]=89;fs.writeFileSync(file,rewrite);const n=reads;
   const refused=read();assert.equal(refused.bytesRead,0);assert.equal(refused.continuation?.reason,'rewrite_ambiguous');commit(refused);assert.equal(reads,n);const fenced=read();assert.equal(fenced.bytesRead,0);assert.equal(fenced.continuation?.action,'park');fenced.close();ok('rewrite then refusal and restart preserve exact nonzero cursor');
   console.log(JSON.stringify({status:'PASS',checks,reads,fixture:dir,columns:Object.keys(row()),priorOffset:row().committed_offset}));
 }finally{fs.readSync=native;tailer.close();db.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
