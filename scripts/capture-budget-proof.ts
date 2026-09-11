/** Repository proof for capture progress under a deliberately slow repair backlog.
 * No provider data, network or installed collectors. Delay is deliberate fault injection,
 * not a claim that this small ledger reproduces a production ledger's physical query timings.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { CollectorMaintenance, automaticRepairServiceStatus } from '../packages/collector-cli/src/maintenance';
import { RolloutTailer } from '../packages/collector-cli/src/rollout-tailer';
import { TranscriptTailer } from '../packages/collector-cli/src/transcript-tailer';
import { captureBaselineStatus } from '../packages/collector-cli/src/capture-baseline';
import { runMaintenanceWorkerService } from '../packages/collector-cli/src/maintenance-worker';
import { MAINTENANCE_PROTOCOL_SCHEMA, parseMaintenanceWorkerReceipt } from '../packages/collector-cli/src/maintenance-protocol';
import { DEFAULT_JSONL_TAILER_IO, readJsonlTail } from '../packages/collector-cli/src/jsonl-byte-tailer';
import type { CaptureRoot } from '../packages/collector-cli/src/capture-root-inventory';

const require = createRequire(path.resolve('package.json'));
const Database = require('better-sqlite3');
const base = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()),'plimsoll-capture-budget-proof-'));
const mode = process.argv[2] ?? 'fixed';
const checks: Array<{name:string;passed:boolean;detail?:unknown}> = [];
function check(name:string, passed:boolean, detail?:unknown) { checks.push({name,passed,...(detail===undefined?{}:{detail})}); }
const at = () => new Date().toISOString();
const sleep = (ms:number) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,ms);

async function main() {
  const buffer = new LocalEventBuffer(path.join(base,'ledger.sqlite'), {workspaceId:'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',databaseBusyTimeoutMs:900});
  const db = buffer.database;
  const roots:CaptureRoot[] = (['codex','claude_code'] as const).map(source=>({source,rootId:source,profileId:'fixture',directory:path.join(base,source),installationEpochId:buffer.workspaceBinding()!.currentInstallationEpochId!}));
  const leaf = (r:CaptureRoot) => path.join(r.directory,...(r.source==='codex'?at().slice(0,10).split('-'):['project']));
  roots.forEach(r=>fs.mkdirSync(leaf(r),{recursive:true}));
  const oldDir = path.join(roots[0].directory,'2025','01','01');
  fs.mkdirSync(oldDir,{recursive:true});
  // Total Codex files becomes exactly 16,940 after adding 26 post-enrollment files.
  for(let i=0;i<16906;i++) fs.writeFileSync(path.join(oldDir,`rollout-old-${i}.jsonl`),'PRIVATE_SYNTHETIC_HISTORY\n');
  const history=new Set<string>();
  for(const r of roots) for(let i=0;i<(r.source==='codex'?8:2);i++) {
    const f=path.join(leaf(r),`rollout-history-${i}.jsonl`);fs.writeFileSync(f,'PRIVATE_SYNTHETIC_HISTORY\n');history.add(f);
  }
  let privateReads=0,maxSliceRecords=0,maxSliceBytes=0,oldDirectoriesOpened=0;
  const originalOpen=fs.opendirSync;
  fs.opendirSync=((dir:any,...args:any[])=>{if(String(dir).includes(oldDir))oldDirectoriesOpened++;return (originalOpen as any)(dir,...args);}) as any;
  const io={...DEFAULT_JSONL_TAILER_IO,readTail:(...args:Parameters<typeof readJsonlTail>)=>{
    if(history.has(args[0])||args[0].startsWith(oldDir))privateReads++;
    maxSliceRecords=Math.max(maxSliceRecords,args[3]?.maxRecords??0);maxSliceBytes=Math.max(maxSliceBytes,args[3]?.maxBytes??0);
    return readJsonlTail(...args);
  }};
  let tailers:any[]=[];let maintenance:CollectorMaintenance;let full:any;
  const resetTailers=()=>{tailers.forEach(t=>t.close());tailers=[new RolloutTailer(buffer,undefined,()=>[],io,[roots[0]]),new TranscriptTailer(buffer,undefined,io,[roots[1]])];
    maintenance=new CollectorMaintenance(buffer,tailers[0],tailers[1]);const original=maintenance.runRecent.bind(maintenance);
    maintenance.runRecent=async opts=>{full=await original(opts);return full;};};
  resetTailers();
  let baselineRuns=0;
  while(captureBaselineStatus(db).status!=='complete'&&baselineRuns++<30) await maintenance!.runRecent();
  assert.equal(captureBaselineStatus(db).status,'complete','real pre-enrollment baseline completes');
  sleep(10);
  const codexContents=(id:string,records=0)=>[
    {type:'session_meta',timestamp:at(),payload:{id}},
    {type:'turn_context',timestamp:at(),payload:{model:'gpt-5.5'}},
    ...Array.from({length:records},()=>({type:'fixture_ignored',padding:'x'.repeat(600)})),
    {type:'event_msg',timestamp:at(),payload:{type:'token_count',info:{total_token_usage:{input_tokens:10,output_tokens:2}}}}
  ].map(x=>JSON.stringify(x)+'\n').join('');
  const claudeContents=(id:string,records=0)=>[
    ...Array.from({length:records},()=>({type:'fixture_ignored',padding:'x'.repeat(600)})),
    {type:'assistant',timestamp:at(),sessionId:id,message:{id:`${id}-message`,model:'claude-opus-5',usage:{input_tokens:11,output_tokens:3}}}
  ].map(x=>JSON.stringify(x)+'\n').join('');
  for(let i=0;i<26;i++){const id=`bbbbbbbb-bbbb-4bbb-8bbb-${String(i).padStart(12,'0')}`;fs.writeFileSync(path.join(leaf(roots[0]),`rollout-${id}.jsonl`),codexContents(id));}
  for(let i=0;i<7;i++){const id=`cccccccc-cccc-4ccc-8ccc-${String(i).padStart(12,'0')}`;fs.writeFileSync(path.join(leaf(roots[1]),`rollout-${id}.jsonl`),claudeContents(id));}
  // Current observed dirty-session queue cardinality. Rows have no private content.
  db.transaction(()=>{const s=db.prepare("insert into dashboard_dirty_sessions(days,session_hash,reason,queued_at) values(7,?,'fixture',?)");
    for(let i=0;i<31421;i++)s.run(i.toString(16).padStart(64,'0'),at());})();
  db.exec('create table fixture_lock_probe(n integer);insert into fixture_lock_probe values(0)');
  const competitor=new Database(path.join(base,'ledger.sqlite'),{timeout:0});
  let delayCalls=0,competingBusy=0;
  db.function('fixture_projection_delay',()=>{delayCalls++;try{competitor.prepare('update fixture_lock_probe set n=n+1').run();}catch(e:any){if(e.code==='SQLITE_BUSY')competingBusy++;else throw e;}sleep(305);return 0;});
  db.exec('create trigger fixture_delay before update of repair_facts on dashboard_projection_control begin select fixture_projection_delay();end');
  db.prepare("delete from maintenance_state where key in ('automatic_repair_service_v1','automatic_capture_source_turn')").run();

  let resolveJob:((value:any)=>void)|null=null,rejectJob:((error:Error)=>void)|null=null;
  let frames:any[]=[],generation=0;
  class Transport extends EventEmitter {
    send(raw:unknown,callback?:()=>void){const f=parseMaintenanceWorkerReceipt(raw);assert(f,'actual protocol parser');
      if(f.type!=='ready'&&f.type!=='closed')frames.push(f);
      queueMicrotask(()=>{callback?.();if('sequence'in f)this.emit('message',{schema:MAINTENANCE_PROTOCOL_SCHEMA,type:'ack',generation:f.generation,nonce:f.nonce,sequence:f.sequence});
        if(f.type==='result')resolveJob?.(f.result);if(f.type==='error')rejectJob?.(new Error(JSON.stringify(f)));});return true;}
    disconnect(){}
  }
  const transport=new Transport();
  // Proxy permits a persistent-worker restart simulation of tailer memory while DB markers survive.
  const proxy={runRecent:(opts:any)=>maintenance!.runRecent(opts),close:()=>maintenance!.close()} as CollectorMaintenance;
  runMaintenanceWorkerService({maintenance:proxy,buffer,spawnNonce:randomUUID(),transport});
  const jobs:any[]=[];
  const run=async(label:string)=>{frames=[];const started=performance.now();const result=await new Promise<any>((resolve,reject)=>{
    resolveJob=resolve;rejectJob=reject;transport.emit('message',{schema:MAINTENANCE_PROTOCOL_SCHEMA,type:'run',generation:++generation,nonce:randomUUID(),deadlineMs:30000,quarantine:null,repoContexts:[]});});
    const b=maintenance!.status().budget!;
    const row={label,cadence:generation,normalCadenceSeconds:60,wallMs:performance.now()-started,budget:b,frames:frames.length,result,
      full:{codex:{recordsCommitted:full.rollout.recordsCommitted,filesSeen:full.rollout.filesSeen},claude:{recordsCommitted:full.transcript.recordsCommitted,filesSeen:full.transcript.filesSeen}},
      repair:automaticRepairServiceStatus(db)};jobs.push(row);
    check(`bounds cadence ${generation}`,b.maxWallMs===200&&b.bytesRead<=524288&&b.recordsParsed<=512&&b.eventsAppended<=512&&frames.length<=128&&tailers.every(t=>(t.captureAttempt?.pendingFiles.length??0)<=64));
    check(`deadline stages cadence ${generation}`,['wal_checkpoint','retention','fill_pending_event_links'].every(s=>frames.some(f=>f.type==='maintenance_job_progress'&&f.stage===s)));
    return result;};
  try {
    await run('first-two');await run('first-two');
    const firstTwo=jobs.slice();
    const advanced=firstTwo.some(j=>j.result.captureAdvanced);
    check('capture advances within two cadences',advanced);
    if(mode==='fixed'){
      // Restore only in-memory tailers, then make both sources permanently busy.
      // A source-turn advanced during exhausted repair-only runs would phase-lock one provider.
      resetTailers();
      for(const r of roots){const id=randomUUID();fs.writeFileSync(path.join(leaf(r),`rollout-${id}.jsonl`),r.source==='codex'?codexContents(id,3000):claudeContents(id,3000));}
      for(let i=0;i<12;i++)await run('fairness');
      check('both providers commit within four cadences under the protocol frame cap',jobs.slice(0,4).some(j=>j.full.codex.recordsCommitted>0)&&jobs.slice(0,4).some(j=>j.full.claude.recordsCommitted>0));
      const fairness=jobs.slice(2);
      check('both busy providers keep committing',fairness.some(j=>j.full.codex.recordsCommitted>0)&&fairness.some(j=>j.full.claude.recordsCommitted>0));
      const repairs=automaticRepairServiceStatus(db);
      check('every repair stage retains service',Object.values(repairs.stages).every(s=>s.completed>0));
      const sentinel=randomUUID();fs.writeFileSync(path.join(leaf(roots[0]),`rollout-${sentinel}.jsonl`),codexContents(sentinel));
      for(let i=0;i<24;i++){await run('new-file-discovery');if(db.prepare('select 1 from buffered_events where session_id=? limit 1').get(sentinel))break;}
      check('new file discovered and captured amid existing backlog',Boolean(db.prepare('select 1 from buffered_events where session_id=? limit 1').get(sentinel)));
    }
    check('no pre-enrollment content read',privateReads===0);
    check('no old-day enumeration / full rescan',oldDirectoriesOpened===0);
    check('64-record and byte slice limits',maxSliceRecords<=64&&maxSliceBytes<=524288);
    check('actual projection transaction excludes competing writer',delayCalls>0&&competingBusy===delayCalls);
    const dbBytes=fs.statSync(path.join(base,'ledger.sqlite')).size;
    const summary={mode,passed:checks.every(c=>c.passed),expectedBaselineFailure:mode==='baseline'&&!advanced,
      fixture:{codexFilesAtStart:16940,oldDayFiles:16906,preEnrollmentRecentFiles:history.size,newCodexFiles:26,newClaudeFiles:7,dirtySessionsSeeded:31421,ledgerBytes:dbBytes,projectionDelayMs:305,delayCalls,competingBusy,privateReads,oldDirectoriesOpened,maxSliceRecords,maxSliceBytes},
      limits:'Real 200 ms cooperative clock and actual worker/tailers/SQLite; 60 s idle waits elided; synchronous injected repair may exceed 200 ms exactly as production permits. No physical 29.4 GB ledger or process deadline-kill recreation.',baselineRuns,checks,jobs};
    console.log(JSON.stringify(summary,null,2));
    process.exitCode=summary.passed?0:1;
  } finally {competitor.close();fs.opendirSync=originalOpen;transport.emit('message',{schema:MAINTENANCE_PROTOCOL_SCHEMA,type:'shutdown',nonce:randomUUID()});}
}
main().catch(e=>{console.error(e);process.exitCode=2;}).finally(()=>{fs.rmSync(base,{recursive:true,force:true});});
