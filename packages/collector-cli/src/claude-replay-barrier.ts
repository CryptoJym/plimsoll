import fs from "node:fs";

import type { LocalEventBuffer } from "./buffer";
import { aiInteractionEventSchema } from "../../shared/src/schemas";
import { captureRootBaselineFiles, claudeBindingForUnrootedEvent,
  countClaudeReplayTimeout, currentDispatchBindingSnapshot,
  dispatchBindingMetadata, durableClaudeRootSessionSightings, inspectCaptureRoots,
  rootCursorKey, type CaptureRoot } from "./capture-root-inventory";
import { jsonlCoverageCheck } from "./capture-frontier";
import { TranscriptTailer } from "./transcript-tailer";

const MAX_REPLAY_WAIT_MS=5*60_000;
type Target={file:string;size:number;dev:number;ino:number;birthtimeMs:number};
export type ClaudeReplayBarrierReceipt={
  state:"ready"|"timed_out";waitMs:number;targets:number;scans:number;
  attributed:number;unbound:number;timedOutHooks:number;
};

/** Freeze each configured root's file EOF when the listener opens. A file
 * removed, replaced or left unread cannot satisfy the barrier. Paths stay in
 * this process only; the durable ledger holds pending event IDs, never paths. */
function startupTargets(roots:readonly CaptureRoot[]) {
  const targets:Target[]=[];
  let blocked=false;
  for(const root of roots) {
    if(inspectCaptureRoots([root])[0]?.state!=="ready") { blocked=true;continue; }
    const scan=captureRootBaselineFiles("claude_code",root.directory);
    if(scan.errors) blocked=true;
    for(const file of scan.files) {
      try {
        const stat=fs.lstatSync(file);
        if(!stat.isFile()||stat.isSymbolicLink()) { blocked=true;continue; }
        targets.push({file,size:stat.size,dev:stat.dev,ino:stat.ino,birthtimeMs:stat.birthtimeMs});
      } catch { blocked=true; }
    }
  }
  return {targets,blocked};
}

/** The daemon calls this before it accepts hooks. The scan runs on later event
 * loop turns; until it reaches every startup EOF, rootless hooks are committed
 * with a pending marker that excludes them from upload. */
export function startClaudeReplayBarrier(buffer:LocalEventBuffer,captureRoots:readonly CaptureRoot[],
  options:{timeoutMs?:number;now?:()=>number}={}) {
  const roots=captureRoots.filter(root=>root.source==="claude_code");
  const started=(options.now??Date.now)();
  const timeoutMs=Math.max(1,Math.min(options.timeoutMs??MAX_REPLAY_WAIT_MS,MAX_REPLAY_WAIT_MS));
  const deadline=started+timeoutMs;
  const snapshot=startupTargets(roots);
  let tailer:TranscriptTailer|undefined;
  if(roots.length && !snapshot.blocked) {
    try { tailer=new TranscriptTailer(buffer,roots[0]!.directory,undefined,roots); }
    catch { snapshot.blocked=true; }
  }
  const covered=tailer ? jsonlCoverageCheck(buffer.database) : null;
  const controller=new AbortController();
  buffer.beginClaudeReplayBarrier();
  let scans=0,attributed=0,unbound=0;
  const rootsUnchanged=() => {
    const current=currentDispatchBindingSnapshot().roots.filter(root=>root.source==="claude_code");
    return current.length===roots.length&&current.every((root,index)=>
      JSON.stringify([root.rootId,root.profileId,root.installationEpochId,root.directory])===
      JSON.stringify([roots[index]?.rootId,roots[index]?.profileId,
        roots[index]?.installationEpochId,roots[index]?.directory]));
  };
  const allCovered=() => !snapshot.blocked && snapshot.targets.every(target=>{
    let stat:fs.Stats;
    try { stat=fs.lstatSync(target.file); } catch { return false; }
    if(!stat.isFile()||stat.isSymbolicLink()||stat.dev!==target.dev||
       stat.ino!==target.ino||stat.birthtimeMs!==target.birthtimeMs||stat.size<target.size)
      return false;
    const receipt=covered?.(rootCursorKey(roots,target.file),stat);
    return Boolean(receipt?.fullyRead && receipt.progress>=target.size);
  });
  const reconcile=async() => {
    while((options.now??Date.now)()<deadline) {
      if(!rootsUnchanged()) return false;
      const rows=buffer.database.prepare(`select held.event_id as id,raw.payload_json as payloadJson
        from claude_replay_hooks held left join buffered_events raw on raw.id=held.event_id
        where held.status='pending' order by held.event_id limit 128`).all() as Array<{
          id:string;payloadJson:string|null;
        }>;
      if(!rows.length) return true;
      try { const page=buffer.database.transaction(()=>{
        const remove=buffer.database.prepare("delete from claude_replay_hooks where event_id=? and status='pending'");
        let pageAttributed=0,pageUnbound=0;
        for(const row of rows) {
          let stamped=false;
          if(row.payloadJson) {
            let event:ReturnType<typeof aiInteractionEventSchema.parse>|undefined;
            try {
              event=aiInteractionEventSchema.parse(JSON.parse(row.payloadJson));
            } catch { /* A malformed row stays unbound. */ }
            if(event?.source==="claude_code"&&event.sessionId) {
              const binding=claudeBindingForUnrootedEvent(event.sessionId,event.observedAt,
                currentDispatchBindingSnapshot(),
                durableClaudeRootSessionSightings(buffer.database,event.sessionId));
              if(binding) {
                const corrected=aiInteractionEventSchema.parse({...event,
                  metadata:{...event.metadata,...dispatchBindingMetadata(binding)}});
                stamped=buffer.delivery.restampUnsentRaw(row.id,JSON.stringify(corrected));
              }
            }
          }
          if(stamped) pageAttributed++; else pageUnbound++;
          remove.run(row.id);
        }
        return {pageAttributed,pageUnbound};
      }).immediate();
      attributed+=page.pageAttributed;unbound+=page.pageUnbound; }
      catch { await new Promise<void>(resolve=>setTimeout(resolve,250));continue; }
      await new Promise<void>(resolve=>setImmediate(resolve));
    }
    return false;
  };
  const done=new Promise<ClaudeReplayBarrierReceipt>(resolve=>setImmediate(async()=>{
    const timer=setTimeout(()=>controller.abort(),Math.max(1,deadline-(options.now??Date.now)()));
    let reached=false;
    try {
      while((options.now??Date.now)()<deadline && !controller.signal.aborted) {
        if(allCovered()) { reached=true;break; }
        if(tailer) {
          scans++;
          try { await tailer.scan({scope:"full",signal:controller.signal}); }
          catch { /* A missing or busy root remains untrusted until a retry. */ }
        }
        if(allCovered()) { reached=true;break; }
        await new Promise<void>(next=>setTimeout(next,250));
      }
      if(reached) reached=rootsUnchanged();
      if(reached) {
        reached=await reconcile();
        if(reached) buffer.finishClaudeReplayBarrier(true);
      }
    } catch {
      reached=false;
    } finally {
      clearTimeout(timer);
      tailer?.close();
      if(!reached) buffer.finishClaudeReplayBarrier(false);
      let timedOutHooks=0;
      if(!reached) try { timedOutHooks=buffer.database.prepare(`update claude_replay_hooks
        set status='timed_out' where status='pending'`).run().changes; }
      catch { /* A closed ledger stays unbound on its next open. */ }
      if(timedOutHooks) countClaudeReplayTimeout(timedOutHooks);
      resolve({state:reached?"ready":"timed_out",waitMs:(options.now??Date.now)()-started,
        targets:snapshot.targets.length,scans,attributed,unbound,timedOutHooks});
    }
  }));
  return {done,abort:()=>controller.abort()};
}
