import crypto from "node:crypto";
import fs from "node:fs";
import { performance } from "node:perf_hooks";

import type { LocalEventBuffer } from "./buffer";
import { aiInteractionEventSchema } from "../../shared/src/schemas";
import { captureRootBaselineFiles, captureRootDigest, claudeBindingForUnrootedEvent,
  countClaudeReplayBytesUnvouched, countClaudeReplayRootUnavailable, countClaudeReplayTimeout,
  currentDispatchBindingSnapshot,
  dispatchBindingMetadata, durableClaudeRootSessionSightings, inspectCaptureRoots,
  rootCursorKey, type CaptureRoot } from "./capture-root-inventory";
import { TranscriptTailer } from "./transcript-tailer";
import { ensureJsonlScanState, jsonlScanStateKey } from "./jsonl-byte-tailer";

const MAX_REPLAY_WAIT_MS=5*60_000;
const pause=(ms:number)=>new Promise<void>(resolve=>setTimeout(resolve,ms));
const isWriterLock=(error:unknown)=>{
  const code=(error as {code?:unknown})?.code;
  return typeof code==="string"&&(code.startsWith("SQLITE_BUSY")||code.startsWith("SQLITE_LOCKED"));
};
type Target={file:string;size:number;start:number;digest:string;prefixDigest:string;
  dev:bigint;ino:bigint;birthtimeNs:bigint;lastVerifiedCtimeNs:bigint;
  nextOffset:number;scanHash:crypto.Hash;verified:boolean;unvouched:boolean};
export type ClaudeReplayBarrierReceipt={
  state:"ready"|"timed_out";waitMs:number;targets:number;scans:number;
  attributed:number;unbound:number;timedOutHooks:number;
};

/** Freeze each configured root's file EOF when the listener opens. A file
 * removed, replaced or left unread cannot satisfy the barrier. The durable
 * hook holds keep only root digests, never transcript paths. */
function startupTargets(buffer:LocalEventBuffer,roots:readonly CaptureRoot[]) {
  const targets:Target[]=[];
  let blocked=false,bytesUnvouched=false;
  ensureJsonlScanState(buffer.database);
  const committedCursor=buffer.database.prepare(`select committed_offset as committedOffset,
    committed_prefix_hash as committedPrefixHash
    from rollout_scan_state where file=?`);
  for(const root of roots) {
    if(inspectCaptureRoots([root])[0]?.state!=="ready") { blocked=true;continue; }
    const scan=captureRootBaselineFiles("claude_code",root.directory);
    if(scan.errors) blocked=true;
    for(const file of scan.files) {
      let captured=false;
      for(let attempt=0;attempt<3&&!captured;attempt++) {
        let fd:number|undefined;
        try {
          fd=fs.openSync(file,fs.constants.O_RDONLY|(fs.constants.O_NOFOLLOW??0));
          const before=fs.fstatSync(fd,{bigint:true});
          const pathBefore=fs.lstatSync(file,{bigint:true});
          const size=Number(before.size);
          if(!before.isFile()||pathBefore.isSymbolicLink()||!pathBefore.isFile()||
             before.dev!==pathBefore.dev||before.ino!==pathBefore.ino||
             before.birthtimeNs!==pathBefore.birthtimeNs||
             !Number.isSafeInteger(size)||size<0) throw new Error("startup_file_changed");
          const receipt=committedCursor.get(jsonlScanStateKey(rootCursorKey(roots,file))) as
            {committedOffset:number|null;committedPrefixHash:string|null}|undefined;
          const start=receipt?.committedOffset??0;
          if(!Number.isSafeInteger(start)||start<0||start>size)
            throw new Error("startup_cursor_outside_file");
          // A cursor at EOF is useful only when its previously committed bytes
          // are still the bytes that produced its session sightings. Older
          // ledgers have no digest and must fail closed until replayed.
          if(start>0&&!/^[0-9a-f]{64}$/.test(receipt?.committedPrefixHash??""))
            throw new Error("startup_cursor_without_prefix_digest");
          const prefixHash=crypto.createHash("sha256");
          const hash=crypto.createHash("sha256");
          const chunk=Buffer.allocUnsafe(64*1024);
          for(let offset=0;offset<start;) {
            const read=fs.readSync(fd,chunk,0,Math.min(chunk.length,start-offset),offset);
            if(read<=0) throw new Error("startup_prefix_shrank");
            prefixHash.update(chunk.subarray(0,read));
            offset+=read;
          }
          const prefixDigest=prefixHash.digest("hex");
          if(start>0&&prefixDigest!==receipt?.committedPrefixHash)
            throw new Error("startup_cursor_prefix_changed");
          for(let offset=start;offset<size;) {
            const read=fs.readSync(fd,chunk,0,Math.min(chunk.length,size-offset),offset);
            if(read<=0) throw new Error("startup_file_shrank");
            hash.update(chunk.subarray(0,read));
            offset+=read;
          }
          const after=fs.fstatSync(fd,{bigint:true});
          const pathAfter=fs.lstatSync(file,{bigint:true});
          if(after.dev!==before.dev||after.ino!==before.ino||
             after.birthtimeNs!==before.birthtimeNs||after.ctimeNs!==before.ctimeNs||
             after.mtimeNs!==before.mtimeNs||after.size!==before.size||
             pathAfter.dev!==before.dev||pathAfter.ino!==before.ino||
             pathAfter.birthtimeNs!==before.birthtimeNs||pathAfter.ctimeNs!==before.ctimeNs||
             pathAfter.size!==before.size) continue;
          targets.push({file,size,start,digest:hash.digest("hex"),prefixDigest,dev:before.dev,
            ino:before.ino,birthtimeNs:before.birthtimeNs,
            lastVerifiedCtimeNs:before.ctimeNs,nextOffset:start,
            scanHash:crypto.createHash("sha256"),verified:start===size,unvouched:false});
          captured=true;
        } catch { /* A moving file gets another bounded capture attempt. */ }
        finally { if(fd!==undefined) fs.closeSync(fd); }
      }
      if(!captured) { blocked=true;bytesUnvouched=true; }
    }
  }
  return {targets,blocked,bytesUnvouched};
}

/** The daemon calls this before it accepts hooks. The scan runs on later event
 * loop turns; until it reaches every startup EOF, rootless hooks are committed
 * with a pending marker that excludes them from upload. */
export function startClaudeReplayBarrier(buffer:LocalEventBuffer,captureRoots:readonly CaptureRoot[],
  options:{timeoutMs?:number;now?:()=>number}={}) {
  const roots=captureRoots.filter(root=>root.source==="claude_code");
  const rootDigests=roots.map(captureRootDigest).sort();
  const rootSet=new Set(rootDigests);
  const started=performance.now();
  const timeoutMs=Math.max(1,Math.min(options.timeoutMs??MAX_REPLAY_WAIT_MS,MAX_REPLAY_WAIT_MS));
  const deadline=started+timeoutMs;
  const snapshot=startupTargets(buffer,roots);
  const targetsByFile=new Map(snapshot.targets.map(target=>[target.file,target]));
  let tailer:TranscriptTailer|undefined;
  if(roots.length && !snapshot.blocked) {
    try { tailer=new TranscriptTailer(buffer,roots[0]!.directory,undefined,roots); }
    catch { snapshot.blocked=true; }
  }
  const committedCursor=tailer ? buffer.database.prepare(`select committed_offset as committedOffset
    from rollout_scan_state where file=?`) : null;
  const controller=new AbortController();
  buffer.beginClaudeReplayBarrier(rootDigests);
  let scans=0,attributed=0,unbound=0;
  const scanSourceSpan=(file:string,span:{offset:number;bytes:Buffer}|null,
    committedOffset:number) => {
    const target=targetsByFile.get(file);
    if(!target||target.unvouched||target.verified) return;
    if(!span) {
      if(committedOffset>target.nextOffset) target.unvouched=true;
      return;
    }
    const from=Math.max(span.offset,target.start);
    const until=Math.min(span.offset+span.bytes.length,target.size);
    if(until<=from) return;
    if(from!==target.nextOffset) { target.unvouched=true;return; }
    target.scanHash.update(span.bytes.subarray(from-span.offset,until-span.offset));
    target.nextOffset=until;
    if(until===target.size) {
      target.verified=target.scanHash.digest("hex")===target.digest;
      target.unvouched=!target.verified;
    }
  };
  const bytesUnvouched=()=>snapshot.bytesUnvouched||snapshot.targets.some(target=>target.unvouched);
  // A rewrite can interrupt the tailer before it commits the next slice. In
  // that case there is no second byte span to hash, so confirm the changed
  // generation against the captured region before considering any cursor.
  const currentRegionStillMatches=(target:Target,ctimeNs:bigint) => {
    let fd:number|undefined;
    try {
      fd=fs.openSync(target.file,fs.constants.O_RDONLY|(fs.constants.O_NOFOLLOW??0));
      const before=fs.fstatSync(fd,{bigint:true});
      if(!before.isFile()||before.dev!==target.dev||before.ino!==target.ino||
         before.birthtimeNs!==target.birthtimeNs||before.ctimeNs!==ctimeNs||
         before.size<BigInt(target.size)) return false;
      const prefixHash=crypto.createHash("sha256");
      const hash=crypto.createHash("sha256");
      const chunk=Buffer.allocUnsafe(64*1024);
      for(let offset=0;offset<target.size;) {
        const read=fs.readSync(fd,chunk,0,Math.min(chunk.length,target.size-offset),offset);
        if(read<=0) return false;
        if(offset<target.start) {
          const prefixBytes=Math.min(read,target.start-offset);
          prefixHash.update(chunk.subarray(0,prefixBytes));
          if(prefixBytes<read) hash.update(chunk.subarray(prefixBytes,read));
        } else hash.update(chunk.subarray(0,read));
        offset+=read;
      }
      const after=fs.fstatSync(fd,{bigint:true});
      const pathAfter=fs.lstatSync(target.file,{bigint:true});
      if(after.ctimeNs!==before.ctimeNs||after.size!==before.size||
         pathAfter.ctimeNs!==before.ctimeNs||pathAfter.size!==before.size||
         pathAfter.dev!==before.dev||pathAfter.ino!==before.ino) return false;
      if(prefixHash.digest("hex")!==target.prefixDigest||hash.digest("hex")!==target.digest)
        return false;
      target.lastVerifiedCtimeNs=ctimeNs;
      return true;
    } catch { return false; }
    finally { if(fd!==undefined) fs.closeSync(fd); }
  };
  const rootsUnchanged=() => {
    const current=currentDispatchBindingSnapshot().roots.filter(root=>root.source==="claude_code");
    return current.length===roots.length&&current.every((root,index)=>
      JSON.stringify([root.rootId,root.profileId,root.installationEpochId,root.directory])===
      JSON.stringify([roots[index]?.rootId,roots[index]?.profileId,
        roots[index]?.installationEpochId,roots[index]?.directory]));
  };
  const allCovered=() => !snapshot.blocked && snapshot.targets.every(target=>{
    let stat:fs.BigIntStats;
    try { stat=fs.lstatSync(target.file,{bigint:true}); }
    catch { target.unvouched=true;return false; }
    if(!stat.isFile()||stat.isSymbolicLink()||stat.dev!==target.dev||
       stat.ino!==target.ino||stat.birthtimeNs!==target.birthtimeNs||
       stat.size<BigInt(target.size)||stat.size<BigInt(target.start)) {
      target.unvouched=true;return false;
    }
    if(stat.ctimeNs!==target.lastVerifiedCtimeNs&&
       !currentRegionStillMatches(target,stat.ctimeNs)) {
      target.unvouched=true;return false;
    }
    const receipt=committedCursor?.get(jsonlScanStateKey(rootCursorKey(roots,target.file))) as
      {committedOffset:number|null}|undefined;
    // A live writer may grow this file after startup. Only the captured EOF
    // is part of this barrier. The committed offset excludes partial records;
    // a legacy size-only cursor must be replayed before it can satisfy us.
    return target.verified && receipt?.committedOffset!==null &&
      receipt?.committedOffset!==undefined && receipt.committedOffset>=target.size;
  });
  // A scan that could not advance an unchanged startup file need not repeat
  // on each short poll. A size, mode, or timestamp change wakes it sooner.
  const targetFingerprint=()=>JSON.stringify(snapshot.targets.map(target=>{
    try {
      const stat=fs.lstatSync(target.file);
      return [stat.dev,stat.ino,stat.size,stat.mode,stat.mtimeMs,stat.ctimeMs];
    } catch { return null; }
  }));
  const rootsForHoldAvailable=(json:string|null) => {
    if(json===null) return false; // A pre-upgrade hold has no trusted inventory.
    try {
      const held=JSON.parse(json) as unknown;
      return Array.isArray(held)&&held.every(digest=>
        typeof digest==="string"&&/^[0-9a-f]{64}$/.test(digest)&&rootSet.has(digest));
    } catch { return false; }
  };
  const reconcile=async() => {
    while(performance.now()<deadline && !controller.signal.aborted) {
      if(!rootsUnchanged()) return false;
      const rows=buffer.database.prepare(`select held.event_id as id,held.root_set_json as rootSetJson,
        raw.payload_json as payloadJson
        from claude_replay_hooks held left join buffered_events raw on raw.id=held.event_id
        where held.status='pending' order by held.event_id limit 128`).all() as Array<{
          id:string;rootSetJson:string|null;payloadJson:string|null;
        }>;
      if(!rows.length) return true;
      try { const page=buffer.database.transaction(()=>{
        const remove=buffer.database.prepare("delete from claude_replay_hooks where event_id=? and status='pending'");
        const releaseUnavailable=buffer.database.prepare(`update claude_replay_hooks
          set status='timed_out' where event_id=? and status='pending'`);
        let pageAttributed=0,pageUnbound=0,pageUnavailable=0;
        for(const row of rows) {
          if(!rootsForHoldAvailable(row.rootSetJson)) {
            pageUnavailable+=releaseUnavailable.run(row.id).changes;
            pageUnbound++;
            continue;
          }
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
        return {pageAttributed,pageUnbound,pageUnavailable};
      }).immediate();
      attributed+=page.pageAttributed;unbound+=page.pageUnbound;
      if(page.pageUnavailable) countClaudeReplayRootUnavailable(page.pageUnavailable); }
      catch { await pause(Math.min(250,Math.max(1,deadline-performance.now())));continue; }
      await new Promise<void>(resolve=>setImmediate(resolve));
    }
    return false;
  };
  const releasePending=async() => {
    let delay=25;
    for(;;) {
      try {
        return buffer.database.prepare(`update claude_replay_hooks
          set status='timed_out' where status='pending'`).run().changes;
      } catch(error) {
        if(!isWriterLock(error)) throw error;
        await pause(delay);
        delay=Math.min(delay*2,250);
      }
    }
  };
  const done=new Promise<ClaudeReplayBarrierReceipt>((resolve,reject)=>setImmediate(()=>{
    void (async()=>{
    const timer=setTimeout(()=>controller.abort(),Math.max(1,deadline-performance.now()));
    let reached=false;
    try {
      let scanDelay=250;
      let lastScanFingerprint:string|null=null;
      let lastFullScanAt=-Infinity;
      while(performance.now()<deadline && !controller.signal.aborted) {
        if(allCovered()) { reached=true;break; }
        if(bytesUnvouched()) break;
        if(tailer) {
          const fingerprint=targetFingerprint();
          if(fingerprint!==lastScanFingerprint || performance.now()-lastFullScanAt>=5_000) {
            scans++;
            try { await tailer.scan({scope:"full",signal:controller.signal,
              onCommittedSourceSpan:scanSourceSpan}); }
            catch { /* A missing or busy root remains untrusted until a retry. */ }
            lastScanFingerprint=fingerprint;
            lastFullScanAt=performance.now();
          }
        }
        if(allCovered()) { reached=true;break; }
        if(bytesUnvouched()) break;
        await pause(Math.min(scanDelay,Math.max(1,deadline-performance.now())));
        if(tailer) scanDelay=Math.min(scanDelay*4,5_000);
      }
      if(reached) reached=rootsUnchanged();
      if(reached) {
        reached=await reconcile();
      }
    } catch {
      reached=false;
    } finally {
      clearTimeout(timer);
      tailer?.close();
    }
    const timedOutHooks=reached ? 0 : await releasePending();
    // Do not publish a terminal state while an earlier held row is still
    // pending. Hooks appended during lock retries join the release UPDATE.
    buffer.finishClaudeReplayBarrier(reached);
    if(timedOutHooks) countClaudeReplayTimeout(timedOutHooks);
    if(timedOutHooks&&bytesUnvouched()) countClaudeReplayBytesUnvouched(timedOutHooks);
    return {state:reached?"ready":"timed_out",waitMs:performance.now()-started,
      targets:snapshot.targets.length,scans,attributed,unbound,timedOutHooks} as ClaudeReplayBarrierReceipt;
    })().then(resolve,reject);
  }));
  return {done,abort:()=>controller.abort()};
}
