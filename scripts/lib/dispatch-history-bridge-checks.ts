import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { performance } from "node:perf_hooks";
import { collectorConfigSchema,collectorConfigPath,writeCollectorConfigTransactionally,
  mutateCollectorConfigTransactionally,reconcileCloudDeviceIdFromIngest,readCollectorConfig } from "../../packages/collector-cli/src/config";
import { bindDispatch } from "../../packages/collector-cli/src/dispatch-command";
import { dispatchBindingSchema,currentDispatchCaptureRoots,dispatchBindingForSession,captureRootDigest } from "../../packages/collector-cli/src/capture-root-inventory";
import { DISPATCH_HISTORY_BUILD_PAIR } from "../../packages/collector-cli/src/dispatch-history-build-pair";
import { DISPATCH_HISTORY_ROLLBACK_READER,withDispatchHistoryAdoption,withDispatchHistoryPublication,
  dispatchHistoryPublicationQualified } from "../../packages/collector-cli/src/dispatch-history-adoption";
import { dispatchHistoryAdoptionFixture } from "./dispatch-history-adoption-fixture";
import { createProofCompletion,requireIsolatedProofEnvironment } from "./proof-completion";
export function runDispatchHistoryBridgeChecks() {
requireIsolatedProofEnvironment();
const completion=createProofCompletion("dispatch-history-bridge"),now=new Date("2026-10-03T12:00:00.000Z");
const hash=(bytes:string|Buffer)=>crypto.createHash("sha256").update(bytes).digest("hex");
const binding=(i:number,finite=i>=945)=>dispatchBindingSchema.parse({sessionId:"session-"+i,
  attemptId:`00000000-0000-4000-8000-${String(i).padStart(12,"0")}`,workItemId:"beads:eco-fixture."+(i+1),
  projectKey:"sha256:"+"a".repeat(64),companyRef:null,parentAttemptId:null,acceptedOutcomeId:null,
  validFrom:i>=20000?now.toISOString():"2026-09-01T00:00:00.000Z",validUntil:finite?"2026-10-02T00:00:00.000Z":null,
  evidenceRef:"dispatch:"+hash(JSON.stringify(["session-"+i,`00000000-0000-4000-8000-${String(i).padStart(12,"0")}`,"beads:eco-fixture."+(i+1)])),
  role:"author",workClass:"operations",complexityBand:"unknown"});
const mixed=()=>Array.from({length:1000},(_,i)=>binding(i));
const args=(b:ReturnType<typeof binding>)=>["--strict","--session-id",b.sessionId,"--attempt-id",b.attemptId,"--work-item-id",b.workItemId,
  "--project-key",b.projectKey,"--valid-from",b.validFrom,"--role","author","--work-class","operations","--complexity-band","unknown"];
const fresh=()=>bindDispatch(args(binding(20000,false)),now);
let count=0;
function fixture(rows=mixed(),roots=25) {
  const home=path.join(process.env.PLIMSOLL_PROOF_ROOT!,"bridge-"+(++count));fs.mkdirSync(home,{mode:0o700});process.env.PLIMSOLL_HOME=home;
  const captureRoots=Array.from({length:roots},(_,i)=>({source:"codex" as const,rootId:"root-"+i,profileId:"profile-"+i,
    installationEpochId:"00000000-0000-4000-8000-000000000001",directory:path.join(home,"capture-"+i),dispatch:rows}));
  const file=collectorConfigPath(),bytes=Buffer.from(JSON.stringify({...collectorConfigSchema.parse({captureRoots}),ownerUnknown:{continuation:"UNKNOWN"}}));
  fs.writeFileSync(file,bytes,{mode:0o600});return {home,file,bytes,captureRoots};
}
function unchanged(f:ReturnType<typeof fixture>,run:()=>unknown,pattern=/adoption_required/) {
  assert.deepEqual(fs.readFileSync(f.file),f.bytes);
  const before=fs.statSync(f.file,{bigint:true});let renames=0;const original=fs.renameSync;
  fs.renameSync=((...args:any[])=>{renames++;return (original as any)(...args);}) as typeof original;
  try {assert.throws(run,pattern);} finally {fs.renameSync=original;}
  assert.equal(renames,0);assert.deepEqual(fs.readFileSync(f.file),f.bytes);assert.deepEqual(fs.statSync(f.file,{bigint:true}),before);
  assert.equal(fs.existsSync(path.join(f.home,"dispatch-binding-history")),false);
}
const releasedManifest=JSON.parse(fs.readFileSync("scripts/fixtures/dispatch-history-released-source-manifest.json","utf8"));
const releasedFile=path.resolve("scripts/fixtures/dispatch-history-released-reader-34d58bcd.cjs");
assert.equal(hash(fs.readFileSync(releasedFile)),releasedManifest.artifactSha256);
const released=createRequire(import.meta.url)(releasedFile);
const observations:Array<{name:string;elapsedMs:number}>=[];
function check(name:string,run:()=>void){const start=performance.now();run();observations.push({name,elapsedMs:performance.now()-start});completion.check(name);}
check("bridge-fixed-pair-is-exact-actual-released-0.7.48-and-adoption-off",()=>{
  assert.deepEqual(DISPATCH_HISTORY_BUILD_PAIR,{mode:"rollback-bridge",previousSourceCommit:"34d58bcd90865679e09fcbd1ee1703de5effda97",
    previousCollectorVersion:"0.7.48",previousReadsHistory:false,qualificationScope:"installed-pair-required"});
  assert.notEqual(DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit,DISPATCH_HISTORY_BUILD_PAIR.previousSourceCommit);
});
check("explicit-complete-source-probe-cannot-replace-actual-installed-pair-25000-bytes-retained",()=>{
  const f=fixture();const prior=released.readReleasedSnapshot(f.bytes,process.env.TMPDIR!);assert.equal(prior.status,"valid");assert.equal(prior.roots.length,25);
  let supplied=false;unchanged(f,()=>withDispatchHistoryAdoption(request=>{supplied=true;return dispatchHistoryAdoptionFixture(request);},fresh));
  assert.equal(supplied,false);const roots=currentDispatchCaptureRoots();assert.equal(roots.length,25);
  for(const root of roots){assert.deepEqual(root.dispatch,mixed());assert.equal(root.dispatchHistory,undefined);assert.equal(captureRootDigest(root),captureRootDigest(f.captureRoots.find(r=>r.rootId===root.rootId)!));}
  const after=released.readReleasedSnapshot(fs.readFileSync(f.file),process.env.TMPDIR!);assert.deepEqual(after.roots,prior.roots);assert.equal(after.profileSha256,hash(f.bytes));
  for(const b of mixed())assert.deepEqual(dispatchBindingForSession("codex",b.sessionId,"2026-10-01T00:00:00.000Z",roots),b);
  try{fresh();}catch(error:any){assert.equal(error.pressure.state,"known");assert.equal(error.pressure.roots.length,25);for(const p of error.pressure.roots){assert.equal(p.retainedBindings,1000);assert.equal(p.availableLegacy,0);}}
});
for(const name of ["missing","unknown","tampered","stale","wrong-source","wrong-reader","partial","pretend-installed"])check(name+"-source-contract-cannot-enable-bridge",()=>{
  const f=fixture();unchanged(f,()=>withDispatchHistoryAdoption(()=>({contract:{label:name,installed:true,qualifiedRootsSha256:"a".repeat(64)} } as any),fresh));
});
check("1000-open-or-unknown-identities-still-refuse-with-no-inferred-terminal-proof",()=>{const f=fixture(Array.from({length:1000},(_,i)=>binding(i,false)),1);unchanged(f,()=>withDispatchHistoryAdoption(dispatchHistoryAdoptionFixture,fresh),/capacity_exceeded/);});
check("ordinary-legacy-bind-retains-every-window-and-needs-no-adoption",()=>{fixture(mixed().slice(1),1);const result=fresh();assert.equal(result.archived,0);const roots=currentDispatchCaptureRoots();assert.equal(roots[0].dispatch!.length,1000);assert.equal(roots[0].dispatchHistory,undefined);});
check("public-context-hashes-and-getters-cannot-mint-or-reuse-qualification",()=>{
  const f=fixture([binding(1)],1),roots=currentDispatchCaptureRoots(),profile=f.bytes;
  const supplied={sourceProfileSha256:hash(profile),sourcePath:f.file,started:performance.now(),normalizeRoots:(rows:readonly unknown[])=>[...rows],profileForRoots:()=>profile,
    qualifiedRootsSha256:hash(JSON.stringify(roots)),qualifiedProfileSha256:hash(profile)};
  withDispatchHistoryPublication(supplied,()=>assert.equal(dispatchHistoryPublicationQualified(roots,profile),false));
  Object.defineProperty(supplied,"qualifiedRootsSha256",{get(){throw new Error("public permission getter consumed");}});
  withDispatchHistoryPublication(supplied,()=>assert.equal(dispatchHistoryPublicationQualified(roots,profile),false));
});
console.log(JSON.stringify({proof:"dispatch-history-bridge",sourceFixtureOnly:true,installedQualified:false,actualReleasedTarget:DISPATCH_HISTORY_BUILD_PAIR,
  futureBridgeProbe:DISPATCH_HISTORY_ROLLBACK_READER,releasedReaderSha256:releasedManifest.artifactSha256,observations}));
completion.complete();

return JSON.parse(fs.readFileSync(process.env.PLIMSOLL_PROOF_RECEIPT!,"utf8"));
}
