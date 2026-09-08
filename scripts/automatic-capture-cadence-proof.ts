// Bounded continuation of the accepted 20-root fixture. Actual worker service
// owns retention, progress admission, IPC projection and capture. Only timer
// waiting is virtual; filesystem/SQLite work and the 200ms clock remain real.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { randomUUID } from 'node:crypto';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { CollectorMaintenance, AutomaticMaintenanceCadence, CoalescingMaintenanceScheduler, automaticRepairServiceStatus } from '../packages/collector-cli/src/maintenance';
import { RolloutTailer } from '../packages/collector-cli/src/rollout-tailer';
import { TranscriptTailer } from '../packages/collector-cli/src/transcript-tailer';
import { captureBaselineStatus } from '../packages/collector-cli/src/capture-baseline';
import { runMaintenanceWorkerService } from '../packages/collector-cli/src/maintenance-worker';
import { MAINTENANCE_PROTOCOL_SCHEMA, parseMaintenanceWorkerReceipt, type MaintenanceWorkerReceipt } from '../packages/collector-cli/src/maintenance-protocol';
import { DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey, readJsonlTail } from '../packages/collector-cli/src/jsonl-byte-tailer';
import { rootCursorKey, type CaptureRoot } from '../packages/collector-cli/src/capture-root-inventory';

async function prove(provider: CaptureRoot['source']) {
  const base=fs.mkdtempSync(path.join(process.env.PLIMSOLL_PROOF_HOME!,'runtime-'));
  const historical=new Set<string>(), roots:CaptureRoot[]=[];
  const dates=new Date().toISOString().slice(0,10).split('-');
  const buffer=new LocalEventBuffer(path.join(base,'ledger.sqlite'),{workspaceId:'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',delivery:{enabled:true}});
  const leaf=(r:CaptureRoot)=>path.join(r.directory,...(r.source==='codex'?dates:['project']));
  for(const [source,count,files] of [['codex',13,173],['claude_code',7,75]] as const) {
    for(let i=0;i<count;i++) {
      const r:CaptureRoot={source,rootId:`${source}-${i}`,profileId:`profile-${source}-${i}`,directory:path.join(base,source,String(i)),installationEpochId:buffer.workspaceBinding()!.currentInstallationEpochId!};
      roots.push(r);fs.mkdirSync(leaf(r),{recursive:true});
      for(let j=i;j<files;j+=count) {const file=path.join(leaf(r),`rollout-legacy-${j}.jsonl`);fs.writeFileSync(file,'SYNTHETIC_HISTORY_NOT_READ\n');historical.add(file);}
    }
  }
  let privateReads=0,maxReadRecords=0,full:any;
  const io={...DEFAULT_JSONL_TAILER_IO,readTail:(...args:Parameters<typeof readJsonlTail>)=>{
    if(historical.has(args[0]))privateReads++;
    assert(!historical.has(args[0]));maxReadRecords=Math.max(maxReadRecords,args[3]?.maxRecords??0);
    assert((args[3]?.maxRecords??0)<=64);return readJsonlTail(...args);
  }};
  const tailers=[new RolloutTailer(buffer,undefined,()=>[],io,roots.filter(r=>r.source==='codex')),
    new TranscriptTailer(buffer,undefined,io,roots.filter(r=>r.source==='claude_code'))] as const;
  const maintenance=new CollectorMaintenance(buffer,...tailers);
  const original=maintenance.runRecent.bind(maintenance);
  maintenance.runRecent=async options=>{full=await original(options);return full;};
  let pendingResolve:((result:any)=>void)|null=null,pendingReject:((error:Error)=>void)|null=null;
  let frames:MaintenanceWorkerReceipt[]=[],generation=0;
  class Transport extends EventEmitter {
    send(raw:unknown,callback?:()=>void) {
      const receipt=parseMaintenanceWorkerReceipt(raw);assert(receipt,'actual IPC parser accepts every worker receipt');
      if(receipt.type!=='ready'&&receipt.type!=='closed')frames.push(receipt);
      queueMicrotask(()=>{callback?.();
        if('sequence' in receipt)this.emit('message',{schema:MAINTENANCE_PROTOCOL_SCHEMA,type:'ack',generation:receipt.generation,nonce:receipt.nonce,sequence:receipt.sequence});
        if(receipt.type==='result')pendingResolve?.(receipt.result);
        if(receipt.type==='error')pendingReject?.(new Error(JSON.stringify(receipt)));
      });return true;
    }
    disconnect() {}
  }
  const transport=new Transport();runMaintenanceWorkerService({maintenance,buffer,spawnNonce:randomUUID(),transport});
  const run=()=>new Promise<any>((resolve,reject)=>{frames=[];pendingResolve=resolve;pendingReject=reject;
    transport.emit('message',{schema:MAINTENANCE_PROTOCOL_SCHEMA,type:'run',generation:++generation,nonce:randomUUID(),deadlineMs:30000,quarantine:null,repoContexts:[]});});
  let timerId=0,tick=Date.now(),task:{id:number;callback:()=>void;delay:number}|null=null;
  const scheduler=new CoalescingMaintenanceScheduler(run);
  const cadence=new AutomaticMaintenanceCadence(scheduler,()=>captureBaselineStatus(buffer.database),{
    repairProgress:()=>{const p=buffer.projection.status(),r=automaticRepairServiceStatus(buffer.database);return {
      pending:Object.values(p.backlog).some(n=>n>0)||!p.backfill.complete||!p.backfill.parityComplete||!p.backfill.metricComplete,
      units:Object.values(r.stages).reduce((s,v)=>s+v.rowsVisited,0)+p.counters.snapshotBuilds+p.counters.expiryFacts+p.counters.compactGcItemsVisited};},
    timer:{now:()=>tick,setTimeout:(callback,delay)=>{assert(!task);task={id:++timerId,callback,delay};return timerId;},clearTimeout:()=>{task=null;}},
  });
  const cursor=(file:string)=>buffer.database.prepare('select committed_offset,deferred_bytes,unresolved_kind,unresolved_byte_budget from rollout_scan_state where file=?').get(jsonlScanStateKey(rootCursorKey(roots,file))) as any;
  let target='',sentinel='',targetVisits:any[]=[],turn=0;
  const jobs:any[]=[];
  const advance=async(phase:string)=>{
    assert(task);const current=task;task=null;tick+=current.delay;const began=performance.now(),cpuBefore=process.cpuUsage();current.callback();
    const until=performance.now()+35000;
    while(!task) {assert(performance.now()<until,'worker result/cadence completion deadline');await new Promise(r=>setImmediate(r));}
    const cpu=process.cpuUsage(cpuBefore),jobWallMs=performance.now()-began;
    const b=maintenance.status().budget!;assert(b.bytesRead<=524288&&b.recordsParsed<=512&&b.eventsAppended<=512&&b.maxWallMs===200);
    assert(frames.length<=128);
    const pending=tailers.map(t=>(t as any).captureAttempt?.pendingFiles??[]);assert(pending.every(p=>p.length<=64));
    const stageCounts:Record<string,number>={};for(const f of frames)if('stage'in f)stageCounts[f.stage]=(stageCounts[f.stage]??0)+1;
    const c=target?cursor(target):null;
    const currentSource=provider==='codex'?full.rollout:full.transcript;
    jobs.push({phase,turn:turn++,jobWallMs,cpuMs:(cpu.user+cpu.system)/1000,waitMs:current.delay,nextRetry:cadence.status().retryClass,nextDelayMs:(task as any).delay,
      frames:frames.length,stageCounts,budget:b,source:{recordsCommitted:currentSource.recordsCommitted??0,slicesCommitted:currentSource.slicesCommitted,recordsParsed:currentSource.recordsParsed,filesSeen:currentSource.filesSeen,filesRead:currentSource.filesRead,bytesDeferred:currentSource.bytesDeferred,deferredGenerations:currentSource.deferredGenerations,discoveryEntries:currentSource.activity.discoveryEntries,errors:currentSource.readErrors+currentSource.statErrors},
      pending:pending.map(p=>({count:p.length,serviced:p.filter((f:any)=>f.servicedCadences>0).length})),target:c,
      stageTimings:full.stageTimings});
    if(c?.committed_offset>(targetVisits.at(-1)?.offset??0))targetVisits.push({turn:turn-1,offset:c.committed_offset,deferred:c.deferred_bytes});
  };
  try {
    cadence.start();for(let i=0;i<100&&captureBaselineStatus(buffer.database).status!=='complete';i++)await advance('baseline');
    assert.equal(captureBaselineStatus(buffer.database).status,'complete');assert.equal(privateReads,0);
    await new Promise(r=>setTimeout(r,5));
    const ownRoots=roots.filter(r=>r.source===provider),at=new Date().toISOString();
    const large=(size:number)=>JSON.stringify({type:'fixture_ignored',padding:'x'.repeat(size)})+'\n';
    const contents=(id:string,parts:number,bytes=350*1024)=>{
      const prefix=provider==='codex'?[{type:'session_meta',timestamp:at,payload:{id}},
        {type:'turn_context',timestamp:at,payload:{model:'gpt-5.5'}},
        {type:'event_msg',timestamp:at,payload:{type:'token_count',info:{total_token_usage:{input_tokens:0,output_tokens:0}}}}]:[];
      const usage=provider==='codex'?{type:'event_msg',timestamp:at,payload:{type:'token_count',info:{total_token_usage:{input_tokens:1,output_tokens:0}}}}:
        {type:'assistant',timestamp:at,sessionId:id,message:{id:`${id}-1`,model:'claude-opus-5',usage:{input_tokens:1,output_tokens:0}}};
      return prefix.map(x=>JSON.stringify(x)+'\n').join('')+large(bytes).repeat(parts)+JSON.stringify(usage)+'\n';
    };
    for(let i=0;i<33;i++) {
      const id=`bbbbbbbb-bbbb-4bbb-8bbb-${String(i).padStart(12,'0')}`;
      const r=i===32?ownRoots.at(-1)!:ownRoots[i%ownRoots.length]!;
      const file=path.join(leaf(r),`rollout-${id}.jsonl`);
      fs.writeFileSync(file,contents(id,i===32?16:2));if(i===32)target=file;
    }
    for(let i=0;i<160;i++) {
      await advance('capture');
      if(targetVisits.length>0&&!sentinel){const id='cccccccc-cccc-4ccc-8ccc-cccccccccccc';sentinel=path.join(leaf(ownRoots.at(-1)!),`rollout-${id}.jsonl`);fs.writeFileSync(sentinel,contents(id,0));}
      if(cursor(target)?.deferred_bytes===0&&cursor(sentinel)?.deferred_bytes===0)break;
    }
    const capture=jobs.filter(j=>j.phase==='capture');
    const positive=capture.filter(j=>j.budget.bytesRead>0),zeroFrames=capture.filter(j=>j.budget.bytesRead===0&&!j.budget.exhausted&&j.frames>=112);
    const committed=capture.filter(j=>j.source.slicesCommitted>0&&j.source.recordsParsed>0);
    const checks=[{name:'committed capture receives the existing five-second follow-up',passed:committed.length>0&&committed.every(j=>j.nextDelayMs===5000)},
      {name:'real worker preserves retention, history privacy and bounded queue fairness',passed:privateReads===0&&cursor(sentinel)?.deferred_bytes===0&&targetVisits.length>=3&&jobs.every(j=>j.stageCounts.retention===1&&j.stageCounts.wal_checkpoint===1&&j.stageCounts.fill_pending_event_links===1)}];
    const targetBytes=fs.statSync(target).size,targetResult=cursor(target),sentinelCaptured=cursor(sentinel)?.deferred_bytes===0;
    const normalPolicyWaitMs=capture.reduce((total,j,index)=>total+j.waitMs+(index>0&&capture[index-1].nextRetry==='capture'?55000:0),0);
    const activeWallMs=capture.reduce((total,j)=>total+j.jobWallMs,0),cpuMs=capture.reduce((total,j)=>total+j.cpuMs,0);
    // Retire only this fixture's generated future files, then isolate the
    // above-cap refusal without other useful work renewing the burst.
    for(const r of roots) for(const name of fs.readdirSync(leaf(r))) {const file=path.join(leaf(r),name);if(!historical.has(file))fs.unlinkSync(file);}
    tailers.forEach(t=>t.close());
    const giant=path.join(leaf(ownRoots[0]!), 'rollout-giant.jsonl');fs.writeFileSync(giant,large(600*1024));
    for(let i=0;i<80&&cursor(giant)?.unresolved_byte_budget!==524288;i++)await advance('giant');
    const giantResult=cursor(giant);
    for(let i=0;i<6;i++)await advance('no-progress');
    const noProgress=jobs.filter(j=>j.phase==='no-progress');
    checks.push({name:'above-ceiling record stays unresolved and cannot renew fast cadence',passed:giantResult?.committed_offset===0&&giantResult?.unresolved_kind==='record_exceeds_byte_budget'&&giantResult?.unresolved_byte_budget===524288&&noProgress.every(j=>j.source.recordsCommitted===0)&&noProgress.at(-1)?.nextRetry==='normal'});
    const result={checks,passed:checks.every(c=>c.passed),provider,roots:roots.length,historicalFiles:historical.size,eligibleFiles:34,privateReads,maxReadRecords,
      targetBytes,target:targetResult,targetVisits,sentinelCaptured,giant:giantResult,normalPolicyWaitMs,activeWallMs,cpuMs,
      captureJobs:capture.length,positiveJobs:positive.length,zeroByteFrameBoundJobs:zeroFrames.length,
      scheduledWaitMs:capture.reduce((s,j)=>s+j.waitMs,0),normalAfterPositive:positive.filter(j=>j.nextRetry==='normal').length,
      retentionEveryJob:jobs.every(j=>j.stageCounts.retention===1&&j.stageCounts.wal_checkpoint===1&&j.stageCounts.fill_pending_event_links===1),jobs};
    return result;
  } finally {cadence.stop();transport.emit('message',{schema:MAINTENANCE_PROTOCOL_SCHEMA,type:'shutdown',nonce:randomUUID()});fs.rmSync(base,{recursive:true,force:true});}
}
async function main(){const results=[];for(const source of ['codex','claude_code'] as const){const r=await prove(source);results.push(r);console.error(JSON.stringify({...r,jobs:undefined,targetVisits:undefined}));}console.log(JSON.stringify({schema:'eco-6hoxj.13.runtime-diagnosis.v1',actualWorker:true,virtualTimerWaitOnly:true,results,passed:results.every(r=>r.passed)},null,2));if(!results.every(r=>r.passed))process.exitCode=1;}
main().catch(error=>{console.error(error);process.exitCode=1;});
