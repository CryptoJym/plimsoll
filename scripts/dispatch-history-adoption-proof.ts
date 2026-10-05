import { runDispatchHistoryBridgeChecks } from "./lib/dispatch-history-bridge-checks";
import { DISPATCH_HISTORY_BUILD_PAIR } from "../packages/collector-cli/src/dispatch-history-build-pair";
import { runFutureDispatchHistorySourcePair } from "./lib/dispatch-history-future-pair-fixture";
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import childProcess, { spawnSync } from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';
import { collectorConfigSchema,collectorConfigPath,readCollectorConfig,
  mutateCollectorConfigTransactionally,writeCollectorConfigTransactionally,rollbackCollectorDispatchHistory,
  reconcileCloudDeviceIdFromIngest } from '../packages/collector-cli/src/config';
import { bindDispatch } from '../packages/collector-cli/src/dispatch-command';
import { currentDispatchCaptureRoots,captureRootDigest,dispatchBindingSchema,dispatchBindingForSession,
  type DispatchBinding } from '../packages/collector-cli/src/capture-root-inventory';
import { DISPATCH_HISTORY_ROLLBACK_READER,withDispatchHistoryAdoption,dispatchHistoryRuntimeIdentity,
  type DispatchHistoryAdoptionInput,type DispatchHistoryAdoptionRequest } from '../packages/collector-cli/src/dispatch-history-adoption';
import { updateDispatchHistory } from '../packages/collector-cli/src/dispatch-binding-index';
import { dispatchHistoryAdoptionFixture } from './lib/dispatch-history-adoption-fixture';
import { createProofCompletion,requireIsolatedProofEnvironment } from './lib/proof-completion';

requireIsolatedProofEnvironment();
let completion: ReturnType<typeof createProofCompletion>;
const NOW=new Date('2026-10-03T12:00:00.000Z');
const sha=(x:string|Buffer)=>crypto.createHash('sha256').update(x).digest('hex');
const epoch='00000000-0000-4000-8000-000000000001',projectKey='sha256:'+'a'.repeat(64);
function binding(i:number,finite=false):DispatchBinding {
  const sessionId='session-'+i,attemptId=`00000000-0000-4000-8000-${String(i).padStart(12,'0')}`,workItemId='beads:eco-fixture.'+(i+1);
  return dispatchBindingSchema.parse({sessionId,attemptId,workItemId,projectKey,companyRef:null,parentAttemptId:null,acceptedOutcomeId:null,
    validFrom:i>=20000?NOW.toISOString():'2026-09-01T00:00:00.000Z',validUntil:finite?'2026-10-02T00:00:00.000Z':null,
    evidenceRef:'dispatch:'+sha(JSON.stringify([sessionId,attemptId,workItemId])),role:'author',workClass:'operations',complexityBand:'unknown'});
}
const mixed=()=>Array.from({length:1000},(_,i)=>binding(i,i>=945));
const flags=(b:DispatchBinding)=>['--strict','--session-id',b.sessionId,'--attempt-id',b.attemptId,'--work-item-id',b.workItemId,
  '--project-key',b.projectKey,'--valid-from',b.validFrom,'--role','author','--work-class','operations','--complexity-band','unknown',
  ...(b.validUntil?['--valid-until',b.validUntil]:[])];
let cases=0;
function fixture(rows=mixed(),count=1) {
  const home=path.join(process.env.PLIMSOLL_PROOF_ROOT!,'adoption-'+(++cases));fs.mkdirSync(home,{mode:0o700});process.env.PLIMSOLL_HOME=home;
  const roots=Array.from({length:count},(_,i)=>({source:'codex' as const,rootId:'root-'+i,profileId:'profile-'+i,installationEpochId:epoch,
    directory:path.join(home,'capture-'+i),dispatch:rows}));
  const file=collectorConfigPath();fs.writeFileSync(file,JSON.stringify({...collectorConfigSchema.parse({captureRoots:roots}),ownerUnknown:{reservation:'UNKNOWN'}}),{mode:0o600});
  return {home,file,roots};
}
const bytes=()=>fs.readFileSync(collectorConfigPath());
function archiveState(home:string){const dir=path.join(home,'dispatch-binding-history');return fs.existsSync(dir)?fs.readdirSync(dir).sort().map(name=>({name,sha256:sha(fs.readFileSync(path.join(dir,name)))})):[];}
function refusal(home:string,action:()=>unknown,pattern:RegExp){const before=bytes(),files=archiveState(home);assert.throws(action,pattern);assert.deepEqual(bytes(),before);assert.deepEqual(archiveState(home),files);}
const records:Array<{name:string;elapsedMs:number}>=[];
async function check(name:string,run:()=>unknown|Promise<unknown>){const start=performance.now();await run();records.push({name,elapsedMs:performance.now()-start});completion.check(name);}
const qualify=<T>(run:()=>T)=>withDispatchHistoryAdoption(dispatchHistoryAdoptionFixture,run);
const fresh=()=>bindDispatch(flags(binding(20000)),NOW);
const readerDriver=`const fs=require('node:fs'),path=require('node:path'),Module=require('node:module');const body=fs.readFileSync(0);let at=20;const chunks=[0,1,2,3].map(i=>{const n=body.readUInt32BE(4+i*4),v=body.subarray(at,at+n);at+=n;return v;});const meta=JSON.parse(chunks[0].toString());const filename=path.join(process.cwd(),'bridge-reader.cjs');const mod=new Module(filename);mod.filename=filename;mod.paths=Module._nodeModulePaths(process.cwd());mod._compile(chunks[1].toString('utf8'),filename);process.stdout.write(JSON.stringify(mod.exports.readRollbackSnapshot({profile:chunks[2],image:chunks[3],scratchRoot:meta.scratchRoot,events:meta.events})));`;
function reopen(profile:Buffer,image:Buffer,events?:any[]){
  const artifact=fs.readFileSync(path.resolve('scripts/fixtures/dispatch-history-bridge-reader-7f53dd8e.cjs'));
  assert.equal(sha(artifact),DISPATCH_HISTORY_ROLLBACK_READER.artifactSha256);
  const chunks=[Buffer.from(JSON.stringify({scratchRoot:process.env.TMPDIR!,events})),artifact,profile,image],header=Buffer.alloc(20);header.write('DHB2');chunks.forEach((bytes,i)=>header.writeUInt32BE(bytes.length,4+i*4));
  const started=performance.now(),child=spawnSync(process.execPath,['--eval',readerDriver],{cwd:path.resolve('packages/collector-cli'),
    input:Buffer.concat([header,...chunks]),
    encoding:'utf8',timeout:5000,maxBuffer:16*1024*1024,env:{HOME:process.env.TMPDIR,TMPDIR:process.env.TMPDIR,LANG:'en_US.UTF-8',TZ:'UTC'}});
  assert.equal(child.status,0,child.stderr);assert.equal(child.error,undefined);
  return {result:JSON.parse(child.stdout),elapsedMs:performance.now()-started};
}

async function main(){
  completion=createProofCompletion('dispatch-history-adoption');
  // Inert dependency trees exercise the actual runtime digest reader. These
  // fixtures only measure reads; their fake native bytes grant no qualification.
  const cacheBase=path.join(process.env.PLIMSOLL_PROOF_ROOT!,"runtime-digest-cache");
  let cacheCase=0;
  function dependencyFixture(){
    const root=path.join(cacheBase,String(++cacheCase)),directory=path.join(root,"packages/collector-cli"),pkg=path.join(directory,"node_modules/better-sqlite3"),native=path.join(pkg,"build/Release/better_sqlite3.node"),lock=path.join(root,"pnpm-lock.yaml");
    fs.mkdirSync(path.dirname(native),{recursive:true,mode:0o700});
    fs.writeFileSync(path.join(pkg,"package.json"),JSON.stringify({name:"better-sqlite3",version:"12.10.0"}),{mode:0o600});
    fs.writeFileSync(native,"cache-native-A",{mode:0o600});fs.writeFileSync(lock,"cache-lock-A",{mode:0o600});
    return {directory,native,lock};
  }
  const d=dependencyFixture();
  function countedRead(action:()=>unknown,onRead?:()=>void){
    const old=fs.readSync,stat=fs.statSync(d.native,{bigint:true});let reads=0;
    fs.readSync=((...args:any[])=>{const opened=fs.fstatSync(args[0],{bigint:true});const value=(old as any)(...args);
      if(opened.dev===stat.dev&&opened.ino===stat.ino){reads++;onRead?.();}return value;}) as typeof old;
    try{return {value:action(),reads};}finally{fs.readSync=old;}
  }
  await check('private-runtime-cache-mints-after-real-read-and-reuses-only-unchanged-inode',()=>{
    const cold=countedRead(()=>dispatchHistoryRuntimeIdentity(d.directory));
    assert.ok(cold.reads>0);assert.equal((cold.value as any).nativeDependencySha256,sha(fs.readFileSync(d.native)));
    const warm=countedRead(()=>dispatchHistoryRuntimeIdentity(d.directory));assert.equal(warm.reads,0);assert.deepEqual(warm.value,cold.value);
  });
  await check('runtime-cache-rehashes-same-inode-edit-with-restored-mtime-and-mode-change',()=>{
    const before=fs.statSync(d.native);fs.writeFileSync(d.native,"cache-native-B");fs.utimesSync(d.native,before.atime,before.mtime);
    const changed=countedRead(()=>dispatchHistoryRuntimeIdentity(d.directory));assert.ok(changed.reads>0);assert.equal((changed.value as any).nativeDependencySha256,sha(fs.readFileSync(d.native)));
    fs.chmodSync(d.native,0o400);try{assert.ok(countedRead(()=>dispatchHistoryRuntimeIdentity(d.directory)).reads>0);}finally{fs.chmodSync(d.native,0o600);}
  });
  await check('runtime-cache-rehashes-atomic-inode-replacement-at-same-path',()=>{
    const before=fs.statSync(d.native);const replacement=d.native+".replacement";fs.writeFileSync(replacement,"cache-native-C",{mode:0o600});fs.utimesSync(replacement,before.atime,before.mtime);fs.renameSync(replacement,d.native);
    const changed=countedRead(()=>dispatchHistoryRuntimeIdentity(d.directory));assert.ok(changed.reads>0);assert.equal((changed.value as any).nativeDependencySha256,sha(fs.readFileSync(d.native)));
  });
  await check('runtime-cache-does-not-reuse-a-changed-frozen-lock-digest',()=>{
    const before=dispatchHistoryRuntimeIdentity(d.directory);fs.writeFileSync(d.lock,"cache-lock-B");const after=dispatchHistoryRuntimeIdentity(d.directory);
    assert.notEqual(after.frozenLockSha256,before.frozenLockSha256);assert.equal(after.frozenLockSha256,sha(fs.readFileSync(d.lock)));
  });
  await check('runtime-cache-remains-finite-and-rehashes-an-evicted-private-entry',()=>{
    for(let i=0;i<9;i++)dispatchHistoryRuntimeIdentity(dependencyFixture().directory);
    assert.ok(countedRead(()=>dispatchHistoryRuntimeIdentity(d.directory)).reads>0);
  });
  await check('runtime-cache-refuses-mutation-during-read-before-minting-digest',()=>{
    fs.writeFileSync(d.native,"cache-native-D");let changed=false;
    assert.throws(()=>countedRead(()=>dispatchHistoryRuntimeIdentity(d.directory),()=>{
      if(!changed){changed=true;fs.writeFileSync(d.native,"cache-native-E");}
    }),/dispatch_history_runtime_changed/);assert.equal(changed,true);
    const actual=dispatchHistoryRuntimeIdentity(d.directory);assert.equal(actual.nativeDependencySha256,sha(fs.readFileSync(d.native)));
  });
  await check('unqualified-25x1000-default-preserves-exact-old-bytes-and-all-windows',()=>{
    const f=fixture(mixed(),25),before=bytes(),stamp=fs.statSync(f.file,{bigint:true});let renames=0;
    const rename=fs.renameSync;fs.renameSync=((...args:any[])=>{renames++;return (rename as any)(...args);}) as typeof rename;
    try{refusal(f.home,fresh,/dispatch_history_adoption_required/);}finally{fs.renameSync=rename;}
    assert.equal(renames,0);assert.equal(fs.existsSync(path.join(f.home,'dispatch-binding-history')),false);
    assert.deepEqual(fs.statSync(f.file,{bigint:true}),stamp);assert.deepEqual(bytes(),before);
    const current=currentDispatchCaptureRoots();assert.equal(current.length,25);for(const root of current){assert.deepEqual(root.dispatch,mixed());assert.equal(root.dispatchHistory,undefined);}
    for(const b of mixed())assert.deepEqual(dispatchBindingForSession('codex',b.sessionId,'2026-10-01T00:00:00.000Z',current),b);
    try{fresh();}catch(error:any){assert.equal(error.pressure.state,'known');assert.equal(error.pressure.roots.length,25);for(const r of error.pressure.roots){assert.equal(r.retainedBindings,1000);assert.equal(r.availableLegacy,0);}}
  });
  await check('unqualified-normal-legacy-bind-retains-finite-windows-without-adoption',()=>{fixture(mixed().slice(1));const result=fresh();assert.equal(result.archived,0);const r=currentDispatchCaptureRoots()[0];assert.equal(r.dispatch!.length,1000);assert.equal(r.dispatchHistory,undefined);assert.deepEqual(r.dispatch!.filter(b=>b.sessionId!=='session-20000'),mixed().slice(1));});
  await check('normal-legacy-bind-does-not-impose-an-unused-history-custody-byte-bound',()=>{fixture([binding(945,true)]);const stored=JSON.parse(bytes().toString());stored.captureRoots[0].directory='/'+ 'x'.repeat(9000);fs.writeFileSync(collectorConfigPath(),JSON.stringify(stored),{mode:0o600});const result=fresh();assert.equal(result.archived,0);assert.equal(currentDispatchCaptureRoots()[0].dispatch!.length,2);assert.equal(currentDispatchCaptureRoots()[0].dispatchHistory,undefined);});
  await check('unqualified-exact-idempotent-bind-at-mixed-cap-needs-no-adoption',()=>{fixture(mixed());bindDispatch(flags(binding(1)),NOW);const r=currentDispatchCaptureRoots()[0];assert.equal(r.dispatch!.length,1000);assert.equal(r.dispatchHistory,undefined);for(const b of mixed())assert.ok(r.dispatch!.some(v=>JSON.stringify(v)===JSON.stringify(b)));});
  await check('1000-all-open-unknown-still-refuses-with-qualified-reader',()=>{const f=fixture(Array.from({length:1000},(_,i)=>binding(i)));refusal(f.home,()=>qualify(fresh),/dispatch_binding_capacity_exceeded/);});
  let retainedProfile:Buffer,retainedImage:Buffer,qualifiedContract:unknown;
  await check('future-source-pair-with-real-frozen-bridge-reader-admits-new-binding-under-5s',()=>{
    fixture(mixed(),25);const started=performance.now();
    withDispatchHistoryAdoption(request=>{const input=dispatchHistoryAdoptionFixture(request);qualifiedContract=input.contract;return input;},fresh);
    const elapsed=performance.now()-started;assert.ok(elapsed<5000,`writer ${elapsed}ms`);
    const roots=currentDispatchCaptureRoots();for(const root of roots){assert.equal(root.dispatch!.length,946);assert.equal(root.dispatchHistory!.rootRows,55);assert.equal(captureRootDigest(root),root.dispatchHistory!.rootDigest);}
    retainedProfile=bytes();const ref=roots[0].dispatchHistory!;retainedImage=fs.readFileSync(path.join(process.env.PLIMSOLL_HOME!,'dispatch-binding-history',ref.sha256+'.sqlite'));
    const directory=path.resolve('evidence/dispatch-history-adoption');fs.mkdirSync(directory,{recursive:true});
    fs.writeFileSync(path.join(directory,'overcap-profile.snapshot'),retainedProfile,{mode:0o600});fs.writeFileSync(path.join(directory,'overcap-index.sqlite'),retainedImage,{mode:0o600});
    fs.writeFileSync(path.join(directory,'qualified-contract.json'),JSON.stringify(qualifiedContract,null,2)+'\n',{mode:0o600});
    console.log(JSON.stringify({qualifiedWriterElapsedMs:elapsed,profileSha256:sha(retainedProfile),imageSha256:sha(retainedImage),reader:DISPATCH_HISTORY_ROLLBACK_READER}));
  });
  await check('cold-complete-25-root-24056516-byte-qualification-retains-2s-reader-5s-writer',()=>{
    const raw=JSON.parse(bytes().toString());raw.coldReaderPadding='';
    const target=24_056_516,empty=Buffer.byteLength(JSON.stringify(raw,null,2)+'\n');
    assert.ok(empty<target);raw.coldReaderPadding='x'.repeat(target-empty);
    const parsed=collectorConfigSchema.parse(raw),started=performance.now();
    withDispatchHistoryAdoption(request=>{const input=dispatchHistoryAdoptionFixture(request);qualifiedContract=input.contract;return input;},
      ()=>writeCollectorConfigTransactionally(parsed,collectorConfigPath(),{preserveUnknownFields:raw}));
    const elapsed=performance.now()-started;assert.ok(elapsed<5000,`cold writer ${elapsed}ms`);
    retainedProfile=bytes();assert.equal(retainedProfile.length,target);assert.equal(currentDispatchCaptureRoots().length,25);
    const directory=path.resolve('evidence/dispatch-history-adoption');fs.writeFileSync(path.join(directory,'overcap-profile.snapshot'),retainedProfile,{mode:0o600});
    fs.writeFileSync(path.join(directory,'qualified-contract.json'),JSON.stringify(qualifiedContract,null,2)+'\n',{mode:0o600});
    console.log(JSON.stringify({coldCompleteQualification:{profileBytes:target,roots:25,bindingsPerRoot:1001,writerElapsedMs:elapsed,readerDeadlineMs:2000,writerDeadlineMs:5000,sourceFixtureOnly:true}}));
  });
  await check('frozen-bridge-reader-reopens-later-build-profile-every-1001-binding-and-all-25-root-custodies',()=>{
    const bindings=[...mixed(),binding(20000)],events=bindings.map(b=>({source:'codex',sessionId:b.sessionId,observedAt:b.sessionId==='session-20000'?NOW.toISOString():'2026-10-01T00:00:00.000Z',rootId:'root-0'}));
    events.push({source:'codex',sessionId:'session-945',observedAt:'2026-09-01T00:00:00.000Z',rootId:'root-0'},
      {source:'codex',sessionId:'session-945',observedAt:'2026-10-02T00:00:00.000Z',rootId:'root-0'},
      {source:'claude_code',sessionId:'session-945',observedAt:'2026-10-01T00:00:00.000Z',rootId:'root-0'});
    const {result,elapsedMs}=reopen(retainedProfile,retainedImage,events);assert.equal(result.roots.length,25);
    const parsed=collectorConfigSchema.parse(JSON.parse(retainedProfile.toString()));
    for(const root of parsed.captureRoots!){const entry=result.roots.find((r:any)=>r.rootDigest===captureRootDigest(root));assert.equal(entry.total,1001);assert.equal(entry.hot,946);assert.equal(entry.historical,55);assert.equal(entry.inventorySha256,sha(JSON.stringify(bindings.map(b=>JSON.stringify(b)).sort())));}
    for(let i=0;i<bindings.length;i++){assert.deepEqual(result.events[i].binding,bindings[i]);assert.equal(result.events[i].metadata.workItemId,bindings[i].workItemId);assert.equal(result.events[i].metadata.captureProfileId,'profile-0');assert.equal(result.events[i].metadata.installationEpochId,epoch);}
    assert.deepEqual(result.events[1001].binding,binding(945,true));assert.equal(result.events[1002].binding,null);assert.equal(result.events[1003].binding,null);
    const receipt={protocol:result.protocol,sourceCommit:result.sourceCommit,profileSha256:sha(retainedProfile),imageSha256:sha(retainedImage),readerArtifactSha256:DISPATCH_HISTORY_ROLLBACK_READER.artifactSha256,
      elapsedMs,roots:result.roots,exactBindings:1001,exactRoots:25,delayedBindings:55,halfOpenAndWrongSourceChecked:true,inventorySha256:result.inventorySha256};
    fs.writeFileSync(path.resolve('evidence/dispatch-history-adoption/rollback-reader-proof.json'),JSON.stringify(receipt,null,2)+'\n',{mode:0o600});console.log(JSON.stringify({rollbackReaderProof:receipt}));
  });
  await check('vanilla-materialization-still-refuses-1001-without-dropping-new-or-old',()=>{refusal(process.env.PLIMSOLL_HOME!,()=>rollbackCollectorDispatchHistory(),/rollback_capacity_exceeded/);});
  await check('ordinary-followup-bind-cannot-use-history-without-scoped-qualification',()=>{refusal(process.env.PLIMSOLL_HOME!,()=>bindDispatch(flags(binding(20001)),NOW),/adoption_required/);});
  await check('generic-config-write-cannot-bypass-history-adoption',()=>{const current=readCollectorConfig();assert.equal(current.status,'valid');if(current.status==='valid')refusal(process.env.PLIMSOLL_HOME!,()=>writeCollectorConfigTransactionally({...current.config,port:48272}),/adoption_required/);});
  await check('generic-config-mutation-cannot-bypass-history-adoption',()=>{refusal(process.env.PLIMSOLL_HOME!,()=>mutateCollectorConfigTransactionally(c=>({...c,port:48272})),/adoption_required/);});
  await check('internal-config-publication-cannot-bypass-history-adoption',()=>{const current=readCollectorConfig();if(current.status!=='valid')throw new Error('profile invalid');refusal(process.env.PLIMSOLL_HOME!,()=>reconcileCloudDeviceIdFromIngest(current.config,crypto.randomUUID()),/adoption_required/);});
  await check('dropping-an-entire-archived-root-refuses-partial-custody',()=>{const f=fixture(mixed(),2);qualify(fresh);const c=readCollectorConfig();if(c.status!=='valid')throw new Error('profile invalid');refusal(f.home,()=>qualify(()=>writeCollectorConfigTransactionally({...c.config,captureRoots:c.config.captureRoots!.slice(0,1)})),/partial_snapshot/);});
  await check('real-late-reader-result-refuses-before-publication',()=>{const f=fixture([binding(945,true)]),original=childProcess.spawnSync;
    childProcess.spawnSync=((...args:any[])=>{const end=performance.now()+2100;while(performance.now()<end){}return (original as any)(...args);}) as typeof spawnSync;syncBuiltinESMExports();
    try{refusal(f.home,()=>qualify(fresh),/reader_unqualified/);}finally{childProcess.spawnSync=original;syncBuiltinESMExports();}
  });
  await check('nonreturning-reader-child-is-killed-and-refuses-before-publication',()=>{const f=fixture([binding(945,true)]),original=childProcess.spawnSync;let killed=false;
    childProcess.spawnSync=((command:any,args:any[],options:any)=>{const result=(original as any)(command,['--eval','while(true){}'],options);killed=result.signal==='SIGKILL'&&result.error?.code==='ETIMEDOUT';return result;}) as typeof spawnSync;syncBuiltinESMExports();
    try{refusal(f.home,()=>qualify(fresh),/reader_unqualified/);assert.equal(killed,true);}finally{childProcess.spawnSync=original;syncBuiltinESMExports();}
  });
  const variants:Array<[string,(input:DispatchHistoryAdoptionInput,request:DispatchHistoryAdoptionRequest)=>void,RegExp]>=[
    ['missing-contract',i=>{i.contract=undefined;},/contract_invalid/],['unknown-contract',i=>{i.contract={schema:'unknown'};},/contract_invalid/],
    ['wrong-reader-protocol',i=>{(i.contract as any).reader={...DISPATCH_HISTORY_ROLLBACK_READER,protocol:'vanilla'};},/contract_invalid/],
    ['wrong-reader-source',i=>{(i.contract as any).reader={...DISPATCH_HISTORY_ROLLBACK_READER,sourceCommit:'0'.repeat(40)};},/contract_invalid/],
    ['released-0.7.48-as-previous-target',i=>{(i.contract as any).reader={...DISPATCH_HISTORY_ROLLBACK_READER,sourceCommit:'34d58bcd90865679e09fcbd1ee1703de5effda97'};},/contract_invalid/],
    ['e7-probe-substituted-as-actual-previous',i=>{(i.contract as any).reader={...DISPATCH_HISTORY_ROLLBACK_READER,sourceCommit:'e7e937faa4960fa0a8cb5d5263361fc02894b7f6'};},/contract_invalid/],
    ['missing-runtime',i=>{delete (i.contract as any).runtime;},/contract_invalid/],
    ['wrong-node-binary',i=>{(i.contract as any).runtime.nodeSha256='0'.repeat(64);},/runtime_mismatch/],
    ['wrong-node-abi',i=>{(i.contract as any).runtime.abi='115';},/contract_invalid/],
    ['wrong-native-dependency',i=>{(i.contract as any).runtime.nativeDependencySha256='0'.repeat(64);},/runtime_mismatch/],
    ['wrong-frozen-lock',i=>{(i.contract as any).runtime.frozenLockSha256='0'.repeat(64);},/contract_invalid/],
    ['tampered-complete-runtime',i=>{const file=path.join(process.env.TMPDIR!,'wrong-complete-runtime.mjs');fs.writeFileSync(file,'wrong',{mode:0o600});i.runtimeArtifactPath=file;},/runtime_digest_mismatch/],
    ['missing-complete-runtime',i=>{i.runtimeArtifactPath=undefined as any;},/complete_pair_missing/],
    ['missing-named-usage-witness',i=>{delete (i.contract as any).namedUsage;},/contract_invalid/],
    ['tampered-named-usage-ledger',i=>{(i.contract as any).namedUsage.ledgerSha256='0'.repeat(64);},/named_usage_mismatch/],
    ['tampered-named-usage-sealed-bytes',i=>{(i.contract as any).namedUsage.sealedSha256='0'.repeat(64);},/named_usage_mismatch/],
    ['public-qualified-hashes-are-not-contract-permission',i=>{(i.contract as any).qualifiedRootsSha256='a'.repeat(64);(i.contract as any).qualifiedProfileSha256='b'.repeat(64);},/contract_invalid/],
    ['vanilla-reader-falsely-declared-compatible',i=>{(i.contract as any).reader={...DISPATCH_HISTORY_ROLLBACK_READER,vanillaCompatible:true};},/contract_invalid/],
    ['false-rollout-permission',i=>{(i.contract as any).rolloutAuthorized=true;},/contract_invalid/],
    ['partial-root-contract',i=>{(i.contract as any).roots=(i.contract as any).roots.slice(1);},/root_scope_mismatch/],
    ['wrong-root-custody',i=>{(i.contract as any).roots=(i.contract as any).roots.map((r:any,n:number)=>n===0?{...r,rootDigest:'0'.repeat(64)}:r);},/root_scope_mismatch/],
    ['wrong-root-count',i=>{(i.contract as any).roots=(i.contract as any).roots.map((r:any,n:number)=>n===0?{...r,total:r.total-1}:r);},/root_scope_mismatch/],
    ['stale-source-profile',i=>{(i.contract as any).sourceProfileSha256='0'.repeat(64);},/stale_or_mismatched/],
    ['tampered-next-profile',i=>{(i.contract as any).nextProfileSha256='0'.repeat(64);},/stale_or_mismatched/],
    ['wrong-generation',i=>{(i.contract as any).generation=crypto.randomUUID();},/stale_or_mismatched/],
    ['tampered-image',i=>{(i.contract as any).imageSha256='0'.repeat(64);},/stale_or_mismatched/],
    ['tampered-inventory',i=>{(i.contract as any).inventorySha256='0'.repeat(64);},/stale_or_mismatched/],
    ['tampered-terminal-proof-coverage',i=>{(i.contract as any).terminalSha256='0'.repeat(64);},/stale_or_mismatched/],
    ['invalid-clock',i=>{(i.contract as any).issuedAt='invalid';},/contract_invalid/],
    ['future-issue-clock',i=>{(i.contract as any).issuedAt=new Date(NOW.getTime()+1).toISOString();},/clock_invalid/],
    ['expired-half-open-window',i=>{(i.contract as any).validUntil=NOW.toISOString();},/clock_invalid/],
    ['excessive-validity-window',i=>{(i.contract as any).validUntil=new Date(NOW.getTime()+60001).toISOString();},/clock_invalid/],
    ['missing-held-writer-requirement',i=>{delete (i.contract as any).operatingRequirements.holdAllWritersDuringRollback;},/contract_invalid/],
    ['contract-byte-pressure',i=>{(i.contract as any).untrusted='x'.repeat(65536);},/contract_byte_bound/],
  ];
  for(const [name,change,pattern]of variants)await check(name+'-refuses-before-any-image-profile-or-generation-publication',()=>{
    const f=fixture([binding(1),binding(945,true)],2);refusal(f.home,()=>withDispatchHistoryAdoption(request=>{const input=dispatchHistoryAdoptionFixture(request);input.contract=structuredClone(input.contract);change(input,request);return input;},fresh),pattern);
    assert.equal(fs.existsSync(path.join(f.home,'dispatch-binding-history')),false);
  });
  await check('tampered-reader-bytes-refuse-before-publication',()=>{const f=fixture([binding(945,true)]);refusal(f.home,()=>withDispatchHistoryAdoption(request=>{const input=dispatchHistoryAdoptionFixture(request);const file=path.join(f.home,'tampered-reader.cjs');fs.writeFileSync(file,'module.exports={};',{mode:0o600});return {...input,readerArtifactPath:file};},fresh),/reader_digest_mismatch/);});
  await check('reader-byte-pressure-refuses-before-publication',()=>{const f=fixture([binding(945,true)]);refusal(f.home,()=>withDispatchHistoryAdoption(request=>{const input=dispatchHistoryAdoptionFixture(request);const file=path.join(f.home,'oversized-reader.cjs');fs.writeFileSync(file,'',{mode:0o600});fs.truncateSync(file,2*1024*1024+1);return {...input,readerArtifactPath:file};},fresh),/byte_bound/);});
  await check('reader-symlink-refuses-before-publication',()=>{const f=fixture([binding(945,true)]);refusal(f.home,()=>withDispatchHistoryAdoption(request=>{const input=dispatchHistoryAdoptionFixture(request);const file=path.join(f.home,'symlink-reader.cjs');fs.symlinkSync(input.readerArtifactPath,file);return {...input,readerArtifactPath:file};},fresh),/unsafe|ELOOP/);});
  await check('reader-hardlink-refuses-before-publication',()=>{const f=fixture([binding(945,true)]),file=path.join(f.home,'linked-reader.cjs');try{refusal(f.home,()=>withDispatchHistoryAdoption(request=>{const input=dispatchHistoryAdoptionFixture(request);fs.linkSync(input.readerArtifactPath,file);return {...input,readerArtifactPath:file};},fresh),/unsafe/);}finally{if(fs.existsSync(file))fs.unlinkSync(file);}});
  for(const [name,field,pattern]of [
    ['reader','readerArtifactPath',/reader_digest_mismatch/],
    ['complete-runtime','runtimeArtifactPath',/runtime_digest_mismatch/],
    ['named-usage','namedUsage',/named_usage_mismatch/],
  ] as const)await check('successful-real-reader-then-'+name+'-tamper-refuses-before-any-publication',()=>{
    const f=fixture([binding(945,true)]),original=childProcess.spawnSync;let file='',saved:Buffer;
    childProcess.spawnSync=((...args:any[])=>{const result=(original as any)(...args);if(String(args[1]?.[1]).includes('DHB2')&&result.status===0){saved=fs.readFileSync(file);fs.writeFileSync(file,'tampered',{mode:0o600});}return result;}) as typeof spawnSync;syncBuiltinESMExports();
    try{refusal(f.home,()=>withDispatchHistoryAdoption(request=>{const input=dispatchHistoryAdoptionFixture(request);file=field==='namedUsage'?input.namedUsage.ledgerArtifactPath:input[field];return input;},fresh),pattern);}
    finally{childProcess.spawnSync=original;syncBuiltinESMExports();if(saved!)fs.writeFileSync(file,saved,{mode:0o600});}
    assert.equal(fs.existsSync(path.join(f.home,'dispatch-binding-history')),false);
  });
  await check('late-noncooperative-contract-provider-refuses-with-no-publication',()=>{const f=fixture([binding(945,true)]);refusal(f.home,()=>withDispatchHistoryAdoption(request=>{const input=dispatchHistoryAdoptionFixture(request),end=performance.now()+5100;while(performance.now()<end){}return input;},fresh),/writer_deadline_exceeded/);});
  await check('noncooperating-source-change-during-qualification-is-preserved-and-refuses',()=>{
    const f=fixture([binding(945,true)]),before=bytes();let changed:Buffer|undefined;
    assert.throws(()=>withDispatchHistoryAdoption(request=>{const input=dispatchHistoryAdoptionFixture(request);const value=JSON.parse(before.toString());value.concurrentWriter='retain';changed=Buffer.from(JSON.stringify(value));fs.writeFileSync(f.file,changed,{mode:0o600});return input;},fresh),/source_changed_reread_required/);
    assert.deepEqual(bytes(),changed);assert.equal(fs.existsSync(path.join(f.home,'dispatch-binding-history')),false);
  });
  await check('stale-contract-cannot-publish-next-generation',()=>{const f=fixture([binding(945,true)]);let cached:DispatchHistoryAdoptionInput;
    withDispatchHistoryAdoption(request=>{cached=dispatchHistoryAdoptionFixture(request);return cached;},fresh);
    refusal(f.home,()=>withDispatchHistoryAdoption(()=>cached,()=>bindDispatch(flags(binding(20001)),NOW)),/stale_or_mismatched/);
  });
  await check('archive-update-utility-cannot-publish-outside-a-locked-source-context',()=>{const f=fixture([binding(945,true)]);refusal(f.home,()=>qualify(()=>updateDispatchHistory(currentDispatchCaptureRoots(),NOW,(_root,bindings)=>[...bindings,binding(20000)],dispatchBindingSchema.parse)),/adoption_required/);assert.equal(fs.existsSync(path.join(f.home,'dispatch-binding-history')),false);});
  console.log(JSON.stringify({proof:'dispatch-history-adoption',records,artifact:DISPATCH_HISTORY_ROLLBACK_READER}));completion.complete();
}
Promise.resolve().then(()=>DISPATCH_HISTORY_BUILD_PAIR.mode==="rollback-bridge" ? runFutureDispatchHistorySourcePair("scripts/dispatch-history-adoption-proof.ts",runDispatchHistoryBridgeChecks()) : main()).catch(error=>{console.error(error);process.exitCode=1;});
