import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import Database from "better-sqlite3";
import { collectorConfigSchema, collectorConfigPath, mutateCollectorConfigTransactionally,
  readCollectorConfig, rollbackCollectorDispatchHistory, writeCollectorConfigTransactionally } from "../packages/collector-cli/src/config";
import { bindDispatch, closeDispatch, restampDispatch } from "../packages/collector-cli/src/dispatch-command";
import { captureRootSchema, captureRootDigest, currentDispatchBindingSnapshot, currentDispatchCaptureRoots,
  currentDispatchRoot, currentDispatchInventoryStatus, dispatchBindingForSession, dispatchBindingSchema,
  dispatchBindingMetadata, rootEventMetadata, validateCaptureRoots, appendRootObservation,
  type CaptureRoot, type DispatchBinding } from "../packages/collector-cli/src/capture-root-inventory";
import { DISPATCH_HISTORY_LIMITS, dispatchHistoryPressure, historicalDispatchBindings,
  materializeDispatchHistoryForRollback } from "../packages/collector-cli/src/dispatch-binding-index";
import { dispatchBindingProofDigest, type DispatchTerminalAuthority } from "../packages/collector-cli/src/dispatch-binding-lifecycle";
import { MAX_COLLECTOR_PROFILE_BYTES, readPrivateStateFile } from "../packages/collector-cli/src/collector-state-io";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";
import { createProofCompletion, requireIsolatedProofEnvironment } from "./lib/proof-completion";

const NOW = new Date("2026-10-03T12:00:00.000Z"), DAY = 86400000;
const PROJECT = `sha256:${"a".repeat(64)}`, EPOCH = "00000000-0000-4000-8000-000000000001";
const hash = (value: string | Buffer) => crypto.createHash("sha256").update(value).digest("hex");
const iso = (ms: number) => new Date(ms).toISOString();
const attempt = (i: number) => `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`;
function binding(i: number, extra: Partial<DispatchBinding> = {}): DispatchBinding {
  const sessionId = `session-${i}`, attemptId = attempt(i), workItemId = `beads:eco-fixture.${i + 1}`;
  const result = { sessionId,attemptId,workItemId,projectKey: PROJECT,companyRef: null,parentAttemptId: null,
    acceptedOutcomeId: null,validFrom: "2026-09-01T00:00:00.000Z",validUntil: null,
    role: "author",workClass: "operations",complexityBand: "unknown",...extra };
  return dispatchBindingSchema.parse({ ...result,evidenceRef: extra.evidenceRef ??
    `dispatch:${hash(JSON.stringify([result.sessionId,result.attemptId,result.workItemId]))}` });
}
const closed = (i: number) => binding(i,{validUntil: "2026-10-02T00:00:00.000Z"});
const mixed = () => Array.from({length:1000},(_,i)=>i<945?binding(i):closed(i));
const open = () => Array.from({length:1000},(_,i)=>binding(i));
function args(b: DispatchBinding) {
  const flags = ["--strict","--session-id",b.sessionId,"--attempt-id",b.attemptId,"--work-item-id",b.workItemId,
    "--project-key",b.projectKey,"--valid-from",b.validFrom,"--role",b.role??"author",
    "--work-class",b.workClass??"other","--complexity-band",b.complexityBand??"unknown"];
  if(b.validUntil)flags.push("--valid-until",b.validUntil);
  if(b.parentAttemptId)flags.push("--parent-attempt-id",b.parentAttemptId);
  for(const [field,flag] of [["techniqueId","--technique-id"],["techniqueVersion","--technique-version"],
    ["assignmentId","--assignment-id"],["arm","--arm"],["launchedBy","--launched-by"]] as const)
    if(b[field])flags.push(flag,b[field]!);
  return flags;
}
let nextCase = 0;
function fixture(bindings: DispatchBinding[], count = 1, source: "codex"|"claude_code" = "codex") {
  const home = path.join(process.env.PLIMSOLL_PROOF_ROOT!,`history-case-${++nextCase}`);
  fs.mkdirSync(home,{mode:0o700}); process.env.PLIMSOLL_HOME=home;
  const roots: CaptureRoot[] = Array.from({length:count},(_,i)=>({rootId:`root-${i}`,profileId:`profile-${i}`,
    installationEpochId:EPOCH,source,directory:path.join(home,`capture-${i}`),dispatch:structuredClone(bindings)}));
  const config = collectorConfigSchema.parse({captureRoots:roots});
  const file = collectorConfigPath();
  fs.writeFileSync(file,JSON.stringify({...config,unrelatedFleetMarker:{status:"UNKNOWN",reservation:"keep"}}),{mode:0o600});
  return {home,file,roots,config};
}
const bytes = () => fs.readFileSync(collectorConfigPath());
const roots = () => currentDispatchCaptureRoots();
const fresh = () => bindDispatch(args(binding(20000)),NOW);
function proofFor(b: DispatchBinding): DispatchTerminalAuthority {
  return {proof:{schema:"dispatch-terminal-proof/v1",proofId:"fixture-authoritative-proof",
    authority:"authenticated-dispatch-lifecycle/v1",authorityEvidenceSha256:"b".repeat(64),
    terminalScope:"native_thread_terminal",continuationAllowed:false,
    sessionId:b.sessionId,attemptId:b.attemptId,workItemId:b.workItemId,
    terminalAt:NOW.toISOString(),issuedAt:NOW.toISOString(),expectedProfileSha256:hash(bytes()),
    bindings:roots().map(root=>({rootDigest:captureRootDigest(root),bindingSha256:dispatchBindingProofDigest(b)}))},
    verify:()=>true};
}
function unchanged(action:()=>unknown,pattern:RegExp) {
  const before=bytes(); assert.throws(action,pattern); assert.deepEqual(bytes(),before);
}
function editStored(change:(value:any)=>void) {
  const stored=JSON.parse(bytes().toString());change(stored);fs.writeFileSync(collectorConfigPath(),JSON.stringify(stored),{mode:0o600});
}
function archiveFile() { return path.join(process.env.PLIMSOLL_HOME!,"dispatch-binding-history",`${roots()[0].dispatchHistory!.sha256}.sqlite`); }
function spin(ms:number) { const end=performance.now()+ms;while(performance.now()<end){} }
function patch<T extends object>(target:T,key:keyof T,replacement:any,action:()=>unknown) {
  const original=target[key];target[key]=replacement;try{return action();}finally{target[key]=original;}
}

async function worker() {
  requireIsolatedProofEnvironment();
  const [mode,idText]=process.argv.slice(3),id=Number(idText??20001);
  if(mode==="crash-before-profile"||mode==="crash-after-profile") {
    const original=fs.renameSync;
    fs.renameSync=((from:any,to:any)=>{
      if(String(to)===collectorConfigPath()) {
        if(mode==="crash-before-profile")process.exit(77);
        original(from,to);process.exit(78);
      }
      return original(from,to);
    }) as typeof original;
    fresh();throw new Error("crash hook not reached");
  }
  if(mode==="observe") {
    let reads=0,failures=0;const generations=new Set<string>();
    process.send?.({ready:true});
    await new Promise<void>(resolve=>process.once("message",()=>resolve()));
    // The parent owns the existing 10s child deadline. Observe a baseline
    // before writers start, then keep concurrent reads through an acknowledged
    // post-writer read. A 700ms sampling horizon can end before a valid commit.
    let writersComplete=false,baselineSent=false;
    const completed=(message:unknown)=>{if(message==="writers-complete")writersComplete=true;};
    process.on("message",completed);
    while(true) {
      const current=roots();reads++;
      if(current.length!==2)failures++;
      const a=current[0]?.dispatchHistory,b=current[1]?.dispatchHistory;
      if(Boolean(a)!==Boolean(b)||a?.generation!==b?.generation)failures++;
      if(dispatchBindingForSession("codex","session-945","2026-10-01T00:00:00.000Z",current)?.attemptId!==attempt(945))failures++;
      generations.add(a?.generation??"legacy");
      if(!baselineSent){baselineSent=true;process.send?.({baseline:true});}
      if(writersComplete)break;
      await new Promise(resolve=>setTimeout(resolve,2));
    }
    process.off("message",completed);
    process.send?.({reads,failures,generations:[...generations]});return;
  }
  process.send?.({ready:true});
  await new Promise<void>(resolve=>process.once("message",()=>resolve()));
  const result=bindDispatch(args(binding(id)),NOW);process.send?.({bound:result.attemptId});
}
function child(mode:string,id?:number) {
  const processChild=spawn(process.execPath,["--import",path.resolve("node_modules/tsx/dist/loader.mjs"),
    fileURLToPath(import.meta.url),"worker",mode,String(id??20001)],{env:{...process.env},stdio:["ignore","pipe","pipe","ipc"]});
  let diagnostics="";const messages:any[]=[];
  processChild.stderr!.on("data",chunk=>{diagnostics+=chunk;});
  const ready=new Promise<void>(resolve=>processChild.on("message",message=>{messages.push(message);if((message as any).ready)resolve();}));
  const baseline=new Promise<void>(resolve=>processChild.on("message",message=>{if((message as any).baseline)resolve();}));
  const done=new Promise<{code:number|null;messages:any[]}>( (resolve,reject)=>{
    const timer=setTimeout(()=>{processChild.kill("SIGKILL");reject(new Error("fixture child deadline"));},10000);
    processChild.on("error",reject);processChild.on("exit",code=>{clearTimeout(timer);
      if(code!==0&&!mode.startsWith("crash"))reject(new Error(diagnostics||`child exited ${code}`));else resolve({code,messages});});
  });
  return {processChild,ready,baseline,done};
}

async function main() {
  const completion=createProofCompletion("dispatch-history");
  const receipts:Array<{name:string;elapsedMs:number}>=[];
  const check=async(name:string,run:()=>unknown|Promise<unknown>)=>{const start=performance.now();await run();
    completion.check(name);receipts.push({name,elapsedMs:performance.now()-start});};
  await check("mixed-cap-admits-fresh-and-preserves-all-1000-exact-bindings",()=>{
    fixture(mixed());const result=fresh();assert.equal(result.archived,55);assert.equal(result.pruned,0);
    const current=roots();assert.equal(current[0].dispatch!.length,946);assert.equal(current[0].dispatchHistory!.rootRows,55);
    for(const b of mixed())assert.deepEqual(dispatchBindingForSession("codex",b.sessionId,"2026-10-01T00:00:00.000Z",current),b);
    assert.equal(result.pressure.state,"known");assert.equal(JSON.parse(bytes().toString()).unrelatedFleetMarker.reservation,"keep");
  });
  await check("mixed-fanout-26-roots-preserves-original-945-open-55-history-per-root",()=>{
    fixture(mixed(),26);fresh();const current=roots();assert.equal(current.length,26);
    for(const root of current){assert.equal(root.dispatch!.length,946);assert.equal(root.dispatchHistory!.rootRows,55);}
    for(const b of mixed())assert.deepEqual(dispatchBindingForSession("codex",b.sessionId,"2026-10-01T00:00:00.000Z",current),b);
  });
  await check("all-open-unknown-cap-refuses-without-proof",()=>{fixture(open());unchanged(fresh,/dispatch_binding_capacity_exceeded/);});
  await check("open-unknown-does-not-expire-by-source-age",()=>{fixture(open());unchanged(()=>bindDispatch(args(binding(20000)),new Date("2099-01-01T00:00:00.000Z")),/capacity/);});
  await check("same-attempt-replacement-at-open-cap",()=>{fixture(open());bindDispatch(args(binding(1)),NOW);assert.equal(roots()[0].dispatch!.length,1000);});
  await check("hot-metadata-replacement-veto-preserves-original-role-and-evidence",()=>{fixture(open());const b=binding(1,{role:"reviewer"});
    unchanged(()=>bindDispatch(args(b),NOW),/replacement_conflict/);assert.deepEqual(dispatchBindingForSession("codex",b.sessionId,NOW.toISOString(),roots()),binding(1));});
  await check("same-session-conflicting-overlap-veto",()=>{fixture([binding(1)]);unchanged(()=>bindDispatch(args(binding(2,{sessionId:"session-1"})),NOW),/dispatch_session_window_conflict/);});
  await check("late-root-cap-failure-publishes-neither-profile-nor-history",()=>{
    const f=fixture(mixed(),2);editStored(value=>{value.captureRoots[1].dispatch=open();});unchanged(fresh,/capacity/);
    assert.equal(fs.existsSync(path.join(f.home,"dispatch-binding-history")),false);
  });
  await check("late-root-conflict-failure-publishes-neither-profile-nor-history",()=>{
    const f=fixture(mixed(),2);editStored(value=>{value.captureRoots[1].dispatch=[binding(2,{sessionId:"session-20000"})];});unchanged(fresh,/window_conflict/);
    assert.equal(fs.existsSync(path.join(f.home,"dispatch-binding-history")),false);
  });
  await check("invalid-bind-clock-fails-before-history-or-profile-publication",()=>{fixture([binding(1),closed(2)]);unchanged(()=>bindDispatch(args(binding(20000)),new Date("invalid")),/dispatch_clock_invalid/);});
  await check("invalid-close-clock-fails-without-transition",()=>{fixture([binding(1)]);unchanged(()=>closeDispatch(["--attempt-id",attempt(1)],new Date("invalid")),/dispatch_clock_invalid/);});
  await check("invalid-codex-event-clock-has-no-attribution",()=>{fixture([binding(1)]);assert.equal(dispatchBindingForSession("codex","session-1","invalid",roots()),null);});
  await check("invalid-claude-event-clock-repaired",()=>{fixture([binding(1)],1,"claude_code");assert.equal(dispatchBindingForSession("claude_code","session-1","invalid",roots()),null);});
  for(const [name,extra] of [["future-start",{validFrom:iso(NOW.getTime()+1)}],["future-end",{validUntil:iso(NOW.getTime()+1)}],
    ["empty-window",{validFrom:NOW.toISOString(),validUntil:NOW.toISOString()}]] as const)
    await check(`new-${name}-refuses`,()=>{fixture([]);unchanged(()=>bindDispatch(args(binding(1,extra)),NOW),/future_window|window_invalid/);});
  await check("malformed-window-rejected-by-real-zod",()=>{assert.equal(dispatchBindingSchema.safeParse({...binding(1),validFrom:"invalid"}).success,false);});
  await check("existing-future-window-is-not-archived-or-closed",()=>{
    fixture([binding(1,{validUntil:iso(NOW.getTime()+DAY)})]);fresh();assert.equal(roots()[0].dispatch!.length,2);assert.equal(roots()[0].dispatchHistory,undefined);
  });
  await check("old-closed-history-is-preserved-without-ttl",()=>{const b=binding(1,{validUntil:"2026-09-02T00:00:00.000Z"});fixture([b]);fresh();
    assert.deepEqual(dispatchBindingForSession("codex",b.sessionId,"2026-09-01T12:00:00.000Z",roots()),b);});
  await check("delayed-claude-history-exact-attribution-and-provenance",()=>{
    const f=fixture(mixed(),2,"claude_code");fresh();const current=roots(),b=closed(945),at="2026-10-01T00:00:00.000Z";
    assert.deepEqual(dispatchBindingForSession("claude_code",b.sessionId,at,current),b);
    const metadata=rootEventMetadata(f.roots[0],"delayed-source-id",at,b.sessionId,true,currentDispatchBindingSnapshot());
    for(const [key,value]of Object.entries(dispatchBindingMetadata(b)))assert.equal(metadata[key],value);
    assert.equal(metadata.captureRootId,"root-0");assert.equal(metadata.sourceIdentityEvidenceRef,"native_runtime_event_v1");
  });
  await check("half-open-history-start-includes-and-end-excludes",()=>{const b=closed(1);fixture([b]);fresh();
    assert.deepEqual(dispatchBindingForSession("codex",b.sessionId,b.validFrom,roots()),b);
    assert.equal(dispatchBindingForSession("codex",b.sessionId,b.validUntil!,roots()),null);});
  await check("same-attempt-disjoint-old-and-current-continuations-preserved",()=>{
    const old=closed(1);fixture([old],2,"claude_code");fresh();const next=binding(1,{validFrom:old.validUntil!});bindDispatch(args(next),NOW);
    assert.deepEqual(dispatchBindingForSession("claude_code",old.sessionId,"2026-10-01T00:00:00.000Z",roots()),old);
    assert.deepEqual(dispatchBindingForSession("claude_code",next.sessionId,NOW.toISOString(),roots()),next);
  });
  await check("different-attempt-adjacent-continuations-preserved",()=>{const old=closed(1);fixture([old]);fresh();const next=binding(2,{sessionId:old.sessionId,validFrom:old.validUntil!});
    bindDispatch(args(next),NOW);assert.deepEqual(dispatchBindingForSession("codex",old.sessionId,old.validUntil!,roots()),next);});
  await check("archived-exact-same-attempt-idempotent-replacement",()=>{fixture([closed(1)]);fresh();const ref=roots()[0].dispatchHistory;bindDispatch(args(closed(1)),NOW);assert.deepEqual(roots()[0].dispatchHistory,ref);});
  await check("archived-metadata-rewrite-veto",()=>{fixture([closed(1)]);fresh();unchanged(()=>bindDispatch(args({...closed(1),role:"reviewer"}),NOW),/replacement_conflict/);});
  await check("bind-finite-end-cannot-bypass-terminal-proof-on-an-open-identity",()=>{fixture([binding(1)]);unchanged(()=>bindDispatch(args(closed(1)),NOW),/replacement_conflict/);});
  await check("open-identity-window-cannot-be-restamped-by-rebinding",()=>{fixture([binding(1)]);unchanged(()=>bindDispatch(args(binding(1,{validFrom:NOW.toISOString()})),NOW),/replacement_conflict/);});
  for(const [field,value] of [["rootId","different-root"],["source","claude_code"],["profileId","different-profile"],
    ["installationEpochId","00000000-0000-4000-8000-000000000002"],["directory","/different-directory"]] as const)
    await check(`history-${field}-custody-mismatch-veto`,()=>{fixture([closed(1)]);fresh();const modified=structuredClone(roots());(modified[0] as any)[field]=value;
      assert.equal(dispatchBindingForSession(modified[0].source,"session-1","2026-10-01T00:00:00.000Z",modified),null);});
  await check("current-root-mismatch-clears-attribution",()=>{fixture([binding(1)]);const r={...roots()[0],profileId:"old-profile"};
    assert.equal(currentDispatchRoot(r).dispatch,undefined);assert.equal(rootEventMetadata(r,"source",NOW.toISOString(),"session-1").workItemId,undefined);});
  await check("snapshot-generation-mismatch-fails-closed-including-open",()=>{fixture(mixed());fresh();const r=structuredClone(roots());r[0].dispatchHistory!.generation=EPOCH;
    assert.equal(dispatchBindingForSession("codex","session-1",NOW.toISOString(),r),null);});
  await check("partial-history-root-membership-veto",()=>{fixture(mixed(),2);fresh();const r=structuredClone(roots());delete r[1].dispatchHistory;
    assert.equal(dispatchBindingForSession("codex","session-1",NOW.toISOString(),r),null);});
  await check("stale-mutable-array-cache-repaired",()=>{const f=fixture([binding(1)]);const r=f.roots;
    assert.equal(dispatchBindingForSession("codex","session-1",NOW.toISOString(),r)?.role,"author");r[0].dispatch![0].role="lead";
    assert.equal(dispatchBindingForSession("codex","session-1",NOW.toISOString(),r)?.role,"lead");});
  await check("old-immutable-snapshot-and-new-generation-remain-coherent",()=>{
    fixture([closed(1),binding(2)]);fresh();const old=currentDispatchBindingSnapshot();const b=binding(2);
    closeDispatch(["--attempt-id",b.attemptId],NOW,proofFor(b));const current=currentDispatchBindingSnapshot();
    assert.notEqual(old.roots[0].dispatchHistory!.generation,current.roots[0].dispatchHistory!.generation);
    assert.deepEqual(dispatchBindingForSession("codex",b.sessionId,NOW.toISOString(),old.roots),b);
    assert.equal(dispatchBindingForSession("codex",b.sessionId,NOW.toISOString(),current.roots),null);
    assert.deepEqual(dispatchBindingForSession("codex","session-1","2026-10-01T00:00:00.000Z",old.roots),closed(1));
  });
  await check("missing-history-fails-closed-with-unknown-pressure",()=>{fixture(mixed());fresh();const r=roots();fs.unlinkSync(archiveFile());
    assert.equal(dispatchBindingForSession("codex","session-1",NOW.toISOString(),r),null);const pressure=dispatchHistoryPressure(r,dispatchBindingSchema.parse);
    assert.equal(pressure.state,"unknown");if(pressure.state==="unknown")assert.equal(pressure.availableHot,null);});
  await check("tampered-history-invalidates-cache",()=>{fixture(mixed());fresh();const file=archiveFile(),r=roots();const image=fs.readFileSync(file);image[200]^=1;fs.writeFileSync(file,image);
    assert.equal(dispatchBindingForSession("codex","session-1",NOW.toISOString(),r),null);});
  await check("history-row-count-mismatch-veto",()=>{fixture(mixed());fresh();const r=structuredClone(roots());r[0].dispatchHistory!.rootRows++;
    assert.equal(dispatchBindingForSession("codex","session-945","2026-10-01T00:00:00.000Z",r),null);});
  await check("hot-history-collision-veto",()=>{fixture([closed(1)]);fresh();editStored(value=>value.captureRoots[0].dispatch.push(closed(1)));unchanged(()=>bindDispatch(args(binding(20001)),NOW),/history_collision/);});
  await check("history-query-row-bound-fails-closed",()=>{
    const rows=Array.from({length:1000},(_,i)=>binding(i,{sessionId:"one-session",validFrom:iso(NOW.getTime()-DAY+i*1000),validUntil:iso(NOW.getTime()-DAY+(i+1)*1000)}));
    fixture(rows,2);fresh();assert.equal(dispatchBindingForSession("codex","one-session",rows[1].validFrom,roots()),null);
    assert.throws(()=>historicalDispatchBindings(roots(),{source:"codex",sessionId:"one-session"},dispatchBindingSchema.parse),/query_bound/);
  });
  await check("history-query-noncooperating-late-result-veto",()=>{fixture([closed(1)]);fresh();const prepare=Database.prototype.prepare;
    patch(Database.prototype,"prepare",function(this:Database.Database,sql:string){const stmt=(prepare as any).call(this,sql);
      if(!sql.includes("indexed by"))return stmt;return new Proxy(stmt,{get(target,key){if(key==="all")return(...a:any[])=>{spin(130);return target.all(...a);};return Reflect.get(target,key);}});},()=>{
      assert.equal(dispatchBindingForSession("codex","session-1","2026-10-01T00:00:00.000Z",roots()),null);
    });});
  await check("profile-byte-bound-before-allocation",()=>{fixture([]);const fd=fs.openSync(collectorConfigPath(),"r+");fs.ftruncateSync(fd,MAX_COLLECTOR_PROFILE_BYTES+1);fs.closeSync(fd);
    assert.throws(()=>readPrivateStateFile(collectorConfigPath()),/byte_bound/);assert.equal(readCollectorConfig().status,"invalid");assert.equal(currentDispatchInventoryStatus().state,"unknown");});
  await check("history-file-byte-bound-before-sqlite",()=>{fixture([closed(1)]);fresh();const file=archiveFile(),r=roots();const fd=fs.openSync(file,"r+");fs.ftruncateSync(fd,DISPATCH_HISTORY_LIMITS.fileBytes+1);fs.closeSync(fd);
    assert.equal(dispatchBindingForSession("codex","session-1","2026-10-01T00:00:00.000Z",r),null);});
  await check("history-root-row-pressure-does-not-prune",()=>{
    fixture([]);let counter=0;
    for(let batch=0;batch<4;batch++){editStored(value=>{value.captureRoots[0].dispatch=Array.from({length:1000},()=>closed(counter++));});bindDispatch(args(binding(20000+batch)),NOW);}
    editStored(value=>{value.captureRoots[0].dispatch=Array.from({length:100},()=>closed(counter++));});unchanged(()=>bindDispatch(args(binding(21000)),NOW),/history_row_bound/);
  });
  await check("history-storage-pressure-refuses-before-publication",()=>{fixture([closed(1)]);fresh();const directory=path.dirname(archiveFile());
    for(let i=1;i<DISPATCH_HISTORY_LIMITS.files;i++)fs.writeFileSync(path.join(directory,`${String(i).padStart(64,"0")}.sqlite`),"orphan",{mode:0o600});
    editStored(value=>value.captureRoots[0].dispatch.push(closed(2)));unchanged(()=>bindDispatch(args(binding(20001)),NOW),/history_storage_pressure/);});
  await check("history-aggregate-raw-byte-pressure-refuses-without-pruning",()=>{fixture([]);let counter=0;
    editStored(value=>value.captureRoots[0].directory="/"+"x".repeat(7000));
    for(let batch=0;batch<2;batch++){editStored(value=>value.captureRoots[0].dispatch=Array.from({length:1000},()=>closed(counter++)));bindDispatch(args(binding(21000+batch)),NOW);}
    editStored(value=>value.captureRoots[0].dispatch=Array.from({length:1000},()=>closed(counter++)));unchanged(()=>bindDispatch(args(binding(22000)),NOW),/history_byte_bound/);});
  for(const kind of ["profile","history","lock"] as const)await check(`${kind}-symlink-veto`,()=>{
    const f=fixture(kind==="history"?[closed(1)]:[]);if(kind==="history")fresh();
    const file=kind==="history"?archiveFile():kind==="lock"?path.join(f.home,".collector.config.json.mutation.lock.sqlite"):f.file;
    if(kind==="lock")fs.writeFileSync(file,"",{mode:0o600});const pre=fs.readFileSync(file);const target=path.join(f.home,"symlink-target");fs.renameSync(file,target);fs.symlinkSync(target,file);
    if(kind==="history")assert.equal(dispatchBindingForSession("codex","session-1","2026-10-01T00:00:00.000Z",roots()),null);
    else if(kind==="profile")assert.equal(readCollectorConfig().status,"invalid");else assert.throws(fresh,/ELOOP|unsafe/);
    assert.deepEqual(fs.readFileSync(target),pre);
  });
  await check("profile-hardlink-veto",()=>{const f=fixture([]);fs.linkSync(f.file,path.join(f.home,"duplicate"));assert.equal(readCollectorConfig().status,"invalid");});
  await check("profile-public-file-mode-veto",()=>{const f=fixture([]);fs.chmodSync(f.file,0o644);assert.equal(readCollectorConfig().status,"invalid");});
  await check("history-public-directory-mode-veto",()=>{fixture([closed(1)]);fresh();const file=archiveFile(),r=roots();fs.chmodSync(path.dirname(file),0o755);
    assert.equal(dispatchBindingForSession("codex","session-1","2026-10-01T00:00:00.000Z",r),null);});
  await check("profile-read-noncooperating-late-result-veto",()=>{fixture([binding(1)]);const read=fs.readSync;let delayed=false;
    patch(fs,"readSync",function(...a:any[]){if(!delayed){delayed=true;spin(2100);}return (read as any)(...a);},()=>assert.throws(()=>readPrivateStateFile(collectorConfigPath()),/deadline/));});
  await check("ordinary-writer-cannot-drop-historical-membership",()=>{fixture([closed(1)]);fresh();unchanged(()=>mutateCollectorConfigTransactionally(config=>({
    ...config,captureRoots:config.captureRoots!.map(({dispatchHistory:ignored,...root})=>root)})),/history_partial_snapshot|generation_loss/);});
  await check("lossless-rollback-materializes-exact-history-within-legacy-cap",()=>{fixture([closed(1),binding(2)]);fresh();const legacy=materializeDispatchHistoryForRollback(roots(),dispatchBindingSchema.parse);
    assert.equal(legacy[0].dispatchHistory,undefined);assert.equal(legacy[0].dispatch!.length,3);
    assert.deepEqual(dispatchBindingForSession("codex","session-1","2026-10-01T00:00:00.000Z",legacy),closed(1));});
  await check("mixed-1001-total-rollback-refuses-losslessly",()=>{fixture(mixed());fresh();const before=bytes();assert.throws(()=>materializeDispatchHistoryForRollback(roots(),dispatchBindingSchema.parse),/rollback_capacity/);assert.deepEqual(bytes(),before);});
  await check("legacy-strict-root-schema-rejects-history-instead-of-ignoring-it",()=>{fixture([closed(1)]);fresh();assert.equal(
    collectorConfigSchema.shape.captureRoots.safeParse(roots()).success,true);const legacy=captureRootLegacySchema();assert.equal(legacy.safeParse(roots()[0]).success,false);});
  await check("close-without-authoritative-proof-refuses",()=>{fixture([binding(1)]);unchanged(()=>closeDispatch(["--attempt-id",attempt(1)],NOW),/terminal_proof_required/);});
  for(const [name,change]of [["completed-turn",{terminalScope:"turn_completed"}],["active-continuation",{continuationAllowed:true}],
    ["future-proof-clock",{issuedAt:iso(NOW.getTime()+1)}],["stale-profile",{expectedProfileSha256:"c".repeat(64)}],
    ["wrong-attempt",{attemptId:attempt(2)}],["wrong-work",{workItemId:"beads:eco-wrong.1"}]] as const)
    await check(`terminal-${name}-veto`,()=>{fixture([binding(1)]);const proof=proofFor(binding(1));proof.proof={...proof.proof as any,...change};
      unchanged(()=>closeDispatch(["--attempt-id",attempt(1)],NOW,proof),/terminal_proof|Zod|Invalid/);});
  await check("unverified-authority-refuses",()=>{fixture([binding(1)]);const proof=proofFor(binding(1));proof.verify=()=>false;unchanged(()=>closeDispatch(["--attempt-id",attempt(1)],NOW,proof),/not_authoritative/);});
  await check("late-noncooperating-terminal-verifier-refuses-before-publication",()=>{fixture([binding(1)]);const proof=proofFor(binding(1));proof.verify=()=>{spin(130);return true;};
    unchanged(()=>closeDispatch(["--attempt-id",attempt(1)],NOW,proof),/deadline/);});
  await check("terminal-proof-root-custody-veto",()=>{fixture([binding(1)]);const proof=proofFor(binding(1));(proof.proof as any).bindings[0].rootDigest="c".repeat(64);
    unchanged(()=>closeDispatch(["--attempt-id",attempt(1)],NOW,proof),/binding_mismatch/);});
  await check("terminal-proof-exact-binding-signature-veto",()=>{fixture([binding(1)]);const proof=proofFor(binding(1));(proof.proof as any).bindings[0].bindingSha256="c".repeat(64);
    unchanged(()=>closeDispatch(["--attempt-id",attempt(1)],NOW,proof),/binding_mismatch/);});
  await check("authoritative-terminal-proof-reclaims-one-open-slot-with-exact-delayed-history",()=>{
    fixture(open(),2);const b=binding(1);const result=closeDispatch(["--attempt-id",b.attemptId],NOW,proofFor(b));assert.equal(result.closed,2);fresh();
    for(const root of roots()){assert.equal(root.dispatch!.length,1000);assert.equal(root.dispatchHistory!.rootRows,1);}
    assert.deepEqual(dispatchBindingForSession("codex",b.sessionId,"2026-10-01T00:00:00.000Z",roots()),{...b,validUntil:NOW.toISOString()});
  });
  await check("terminal-proof-instant-or-future-start-never-deletes-a-binding",()=>{const b=binding(1,{validFrom:NOW.toISOString()});fixture([b]);unchanged(()=>closeDispatch(["--attempt-id",b.attemptId],NOW,proofFor(b)),/binding_mismatch/);});
  await check("authoritative-terminal-markers-veto-same-thread-continuation",()=>{const b=binding(1);fixture([b]);closeDispatch(["--attempt-id",b.attemptId],NOW,proofFor(b));
    unchanged(()=>bindDispatch(args(binding(2,{sessionId:b.sessionId,validFrom:NOW.toISOString()})),NOW),/terminal_identity_cannot_continue/);});
  await check("attempt-terminal-scope-allows-only-a-different-attempt-continuation",()=>{const b=binding(1);fixture([b]);const proof=proofFor(b);(proof.proof as any).terminalScope="attempt_irrevocably_retired";
    closeDispatch(["--attempt-id",b.attemptId],NOW,proof);unchanged(()=>bindDispatch(args({...b,validFrom:NOW.toISOString()}),NOW),/terminal_identity_cannot_continue/);
    bindDispatch(args(binding(2,{sessionId:b.sessionId,validFrom:NOW.toISOString()})),NOW);});
  await check("legacy-rollback-refuses-to-lose-authoritative-terminal-markers",()=>{const b=binding(1);fixture([b]);closeDispatch(["--attempt-id",b.attemptId],NOW,proofFor(b));
    unchanged(()=>rollbackCollectorDispatchHistory(),/terminal_markers_unsupported/);});
  await check("lossless-rollback-publisher-is-atomic-and-preserves-unknown-fields",()=>{fixture([closed(1),binding(2)]);fresh();rollbackCollectorDispatchHistory();
    assert.equal(roots()[0].dispatchHistory,undefined);assert.equal(roots()[0].dispatch!.length,3);assert.equal(JSON.parse(bytes().toString()).unrelatedFleetMarker.reservation,"keep");});
  await check("rollback-publisher-refuses-over-cap-with-no-publication",()=>{fixture(mixed());fresh();unchanged(()=>rollbackCollectorDispatchHistory(),/rollback_capacity/);});
  for(const stage of ["archive-fsync","archive-create","before-rename","after-rename","before-lock-commit","after-lock-commit"] as const)
    await check(`publication-fault-${stage}-has-coherent-generation`,()=>{
      const f=fixture(mixed());const before=bytes();const fsync=fs.fsyncSync,open=fs.openSync,rename=fs.renameSync,exec=Database.prototype.exec;
      let fired=false;const post=stage==="after-rename"||stage.includes("lock-commit");
      const failure=new Error(`injected-${stage}`);
      const replacements: Array<[any,string,any]> = stage==="archive-fsync"?[[fs,"fsyncSync",(fd:number)=>{const stat=fs.fstatSync(fd);if(!fired&&stat.isFile()&&stat.size>1000){fired=true;throw failure;}return fsync(fd);}]]:
        stage==="archive-create"?[[fs,"openSync",(file:any,flags:any,...rest:any[])=>{if(String(file).includes("/dispatch-binding-history/")&&String(file).endsWith(".sqlite")){
          fired=true;throw failure;}return (open as any)(file,flags,...rest);}]]:
        stage==="before-rename"?[[fs,"renameSync",(from:any,to:any)=>{if(String(to)===f.file){fired=true;throw failure;}return rename(from,to);}]]:
        stage==="after-rename"?[[fs,"fsyncSync",(fd:number)=>{if(fs.fstatSync(fd).isDirectory()&&fs.realpathSync(f.home)===f.home){
          const current=JSON.parse(fs.readFileSync(f.file,"utf8"));if(current.captureRoots[0].dispatchHistory){fired=true;throw failure;}}return fsync(fd);}]]:
        [[Database.prototype,"exec",function(this:Database.Database,sql:string){if(sql==="COMMIT"&&!fired){fired=true;if(stage==="after-lock-commit")exec.call(this,sql);throw failure;}return exec.call(this,sql);}]];
      const [target,key,replacement]=replacements[0];patch(target,key,replacement,()=>assert.throws(fresh,post?/uncertain_reread/:/injected/));
      assert.equal(fired,true);if(!post)assert.deepEqual(bytes(),before);else assert.notDeepEqual(bytes(),before);
      const current=roots();for(const b of mixed())assert.deepEqual(dispatchBindingForSession("codex",b.sessionId,"2026-10-01T00:00:00.000Z",current),b);
      assert.equal(dispatchBindingForSession("codex","session-20000",NOW.toISOString(),current)!==null,post);
    });
  for(const mode of ["crash-before-profile","crash-after-profile"])
    await check(`${mode}-recovery-uses-profile-generation-only`,async()=>{fixture(mixed());const before=bytes();const crashed=child(mode);const result=await crashed.done;
      assert.equal(result.code,mode==="crash-before-profile"?77:78);if(mode==="crash-before-profile")assert.deepEqual(bytes(),before);else assert.notDeepEqual(bytes(),before);
      for(const b of mixed())assert.deepEqual(dispatchBindingForSession("codex",b.sessionId,"2026-10-01T00:00:00.000Z",roots()),b);
      bindDispatch(args(binding(20001)),NOW);assert.ok(dispatchBindingForSession("codex","session-20001",NOW.toISOString(),roots()));});
  await check("concurrent-real-sqlite-writers-and-reader-have-no-partial-generation-or-lost-binding",async()=>{
    fixture(mixed(),2);const observer=child("observe"),writers=[20001,20002,20003,20004].map(id=>child("bind",id));
    await Promise.all([observer.ready,...writers.map(w=>w.ready)]);observer.processChild.send("go");
    await observer.baseline;for(const writer of writers)writer.processChild.send("go");
    await Promise.all(writers.map(w=>w.done));observer.processChild.send("writers-complete");
    const result=await observer.done;const observation=result.messages.find(m=>m.reads);
    assert.ok(observation.reads>1);assert.equal(observation.failures,0);assert.ok(observation.generations.length>=2);
    for(const root of roots())assert.equal(root.dispatch!.length,949);
    for(const id of [20001,20002,20003,20004])assert.equal(dispatchBindingForSession("codex",`session-${id}`,NOW.toISOString(),roots())?.attemptId,attempt(id));
    console.log(JSON.stringify({concurrency:observation}));
  });
  await check("real-restamp-from-archived-codex-window-preserves-outbox-incarnation",()=>restampFixture("codex"));
  await check("real-restamp-from-archived-claude-window-requires-admitted-full-root-digest",()=>restampFixture("claude_code"));
  for(const mode of ["peer-race","deadline","oversized","protected","custody","row-bound"] as const)
    await check(`real-restamp-${mode}-guard`,()=>restampFixture("codex",mode));
  await check("direct-hot-lookup-invalid-window-fails-closed",()=>{const f=fixture([binding(1)]);f.roots[0].dispatch![0].validFrom="invalid";
    assert.equal(dispatchBindingForSession("claude_code","session-1",NOW.toISOString(),f.roots),null);});
  console.log(JSON.stringify({proof:"dispatch-history",checks:receipts,limits:DISPATCH_HISTORY_LIMITS}));
  completion.complete();
}

function captureRootLegacySchema() {
  // The actual new schema minus the additive history member is byte-for-byte
  // the legacy root field set. Independent old-source execution follows in packet rollback tests.
  return captureRootSchema.omit({dispatchHistory:true});
}
function restampFixture(source:"codex"|"claude_code",mode="normal") {
  const f=fixture([]);const buffer=new LocalEventBuffer(path.join(f.home,"ledger.sqlite"),{
    workspaceId:f.config.tenantId,deviceId:"fixture-device",enrollmentNow:()=>new Date("2026-09-01T00:00:00.000Z"),delivery:{enabled:true}});
  try {
    const root={...f.roots[0],source,installationEpochId:buffer.workspaceBinding()!.currentInstallationEpochId!};
    const b=closed(945),observedAt="2026-10-01T00:00:00.000Z";
    const make=(i:number)=>aiInteractionEventSchema.parse({id:attempt(80000+i),source,sessionId:b.sessionId,eventType:"assistant_response",dataMode:"metadata",observedAt,
      inputTokens:3,outputTokens:1,metadata:{captureRootId:root.rootId,captureProfileId:root.profileId,installationEpochId:root.installationEpochId,preserve:"metadata"}});
    const raw=make(1);assert.equal(appendRootObservation(buffer,raw,root,true),true);
    const sealed=make(2);assert.equal(appendRootObservation(buffer,sealed,root,true),true);
    buffer.database.prepare("update upload_outbox set attempt_count=1 where raw_id=?").run(sealed.id);
    const beforeSealed=buffer.database.prepare("select payload_json from buffered_events where id=?").get(sealed.id);
    if(mode==="oversized")buffer.database.prepare("update buffered_events set payload_json=? where id=?").run(JSON.stringify({...raw,metadata:{...raw.metadata,synthetic:"x".repeat(100000)}}),raw.id);
    if(mode==="protected")buffer.database.prepare("update buffered_events set uploaded_at=? where id=?").run(NOW.toISOString(),raw.id);
    if(mode==="custody")buffer.database.prepare("update buffered_events set payload_json=? where id=?").run(JSON.stringify({...raw,metadata:{...raw.metadata,captureProfileId:"wrong-profile"}}),raw.id);
    if(mode==="row-bound") {
      const saved=buffer.database.prepare("select * from buffered_events where id=?").get(raw.id) as Record<string,unknown>;
      const columns=Object.keys(saved),insert=buffer.database.prepare(`insert into buffered_events (${columns.join(",")}) values (${columns.map(()=>"?").join(",")})`);
      buffer.database.transaction(()=>{for(let i=0;i<5001;i++) {const id=attempt(90000+i),event={...raw,id,metadata:{preserve:"metadata"}};
        const copy:Record<string,unknown>={...saved,id,payload_json:JSON.stringify(event)};insert.run(...columns.map(name=>copy[name]));}})();
    }
    writeCollectorConfigTransactionally(collectorConfigSchema.parse({...f.config,captureRoots:[{...root,dispatch:mixed()}]}),f.file);
    fresh();const original=buffer.delivery.restampUnsentRaw.bind(buffer.delivery);let peer:Database.Database|undefined,blocked=false;
    if(mode==="peer-race") {peer=new Database(path.join(f.home,"ledger.sqlite"),{timeout:1});buffer.delivery.restampUnsentRaw=(id,payload)=>{
      assert.throws(()=>peer!.prepare("update buffered_events set created_at=? where id=?").run(NOW.toISOString(),id),(error:any)=>error.code==="SQLITE_BUSY");blocked=true;return original(id,payload);};}
    if(mode==="deadline")buffer.delivery.restampUnsentRaw=(id,payload)=>{spin(2100);return original(id,payload);};
    let result;try{result=restampDispatch(["--attempt-id",b.attemptId],buffer,roots());}finally{peer?.close();}
    if(mode==="row-bound") {assert.ok(result.scanned<=5000);assert.ok(result.queries<=128);assert.ok(result.readBytes<=16*1024*1024);assert.equal(result.truncated,true);return;}
    if(["deadline","oversized","protected","custody"].includes(mode)){assert.equal(result.restamped,0);
      if(mode==="deadline")assert.equal(result.truncated,true);
      assert.equal(JSON.parse((buffer.database.prepare("select payload_json as payload from buffered_events where id=?").get(raw.id) as any).payload).metadata.workItemId,undefined);return;}
    assert.equal(result.restamped,1);if(mode==="peer-race")assert.equal(blocked,true);
    const payload=JSON.parse((buffer.database.prepare("select payload_json as payload from buffered_events where id=?").get(raw.id) as any).payload);
    for(const[key,value]of Object.entries(dispatchBindingMetadata(b)))assert.equal(payload.metadata[key],value);
    assert.equal(payload.metadata.preserve,"metadata");assert.deepEqual(buffer.database.prepare("select payload_json from buffered_events where id=?").get(sealed.id),beforeSealed);
    const queued=buffer.database.prepare("select base_envelope_json as envelope from upload_outbox where raw_id=?").get(raw.id) as any;
    assert.equal(JSON.parse(queued.envelope).event.metadata.workItemId,b.workItemId);
    assert.equal(buffer.database.prepare("pragma integrity_check").get() instanceof Object,true);
  }finally{buffer.close();}
}
if(process.argv[2]==="worker")worker().catch(error=>{console.error(error);process.exitCode=1;});
else main().catch(error=>{console.error(error);process.exitCode=1;});
