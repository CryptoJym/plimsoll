import crypto from 'node:crypto';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { LocalEventBuffer } from '../../packages/collector-cli/src/buffer';
import { aiInteractionEventSchema } from '../../packages/shared/src/schemas';
import childProcess from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';
import { performance } from 'node:perf_hooks';
import { DISPATCH_HISTORY_ROLLBACK_READER, type DispatchHistoryAdoptionRequest,
  type DispatchHistoryAdoptionInput, dispatchHistoryRuntimeIdentity } from '../../packages/collector-cli/src/dispatch-history-adoption';
import { requireIsolatedProofEnvironment } from './proof-completion';

/** Technical fixture qualification only. It neither authenticates an owner nor
 * grants installation/rollback approval. The real pinned reader executes for
 * every intended generation; a Boolean receipt cannot substitute for it. */
export function dispatchHistoryAdoptionFixture(request: DispatchHistoryAdoptionRequest): DispatchHistoryAdoptionInput {
  requireIsolatedProofEnvironment();
  installReaderExecutionObservation();
  const readerArtifactPath = path.join(process.env.TMPDIR!, 'dispatch-history-bridge-reader-7f53dd8e.cjs');
  if (!fs.existsSync(readerArtifactPath)) {
    fs.copyFileSync(path.resolve('scripts/fixtures/dispatch-history-bridge-reader-7f53dd8e.cjs'),
      readerArtifactPath, fs.constants.COPYFILE_EXCL);
    fs.chmodSync(readerArtifactPath, 0o600);
  }
  const runtimeArtifactPath=path.join(process.env.TMPDIR!, 'dispatch-history-bridge-runtime-7f53dd8e.mjs');
  if(!fs.existsSync(runtimeArtifactPath))fs.writeFileSync(runtimeArtifactPath,Buffer.from(fs.readFileSync(path.resolve('scripts/fixtures/dispatch-history-bridge-runtime-7f53dd8e.base64'),'utf8'),'base64'),{mode:0o600,flag:'wx'});
  const namedUsage=prepareNamedUsageRollbackFixture();
  const runtime=dispatchHistoryRuntimeIdentity(path.resolve('packages/collector-cli'));
  return {
    contract: { schema: 'dispatch-history-source-adoption/v2', qualificationId: crypto.randomUUID(),
      sourceQualificationOnly: true, rolloutAuthorized: false,
      writerBaseCommit: DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit, reader: DISPATCH_HISTORY_ROLLBACK_READER,
      runtime, namedUsage:{ledgerSha256:crypto.createHash('sha256').update(fs.readFileSync(namedUsage.ledgerArtifactPath)).digest('hex'),
        sealedSha256:namedUsage.sealedSha256,now:namedUsage.now,workspaceId:namedUsage.workspaceId,deviceId:namedUsage.deviceId},
      issuedAt: request.now, validUntil: new Date(Date.parse(request.now)+30_000).toISOString(),
      sourceProfileSha256: request.sourceProfileSha256, nextProfileSha256: request.nextProfileSha256,
      imageSha256: request.imageSha256, generation: request.generation, inventorySha256: request.inventorySha256,
      terminalSha256: request.terminalSha256, roots: request.roots,
      operatingRequirements: { holdOlderReadersBeforeAdoption: true, holdAllWritersDuringRollback: true,
        originalProfileAndImagesRetained: true, exactReaderAndFrozenDependenciesRetained: true, vanillaDowngradeForbidden: true },
    },
    readerArtifactPath,runtimeArtifactPath,namedUsage,
    dependencyDirectory: path.resolve('packages/collector-cli'), scratchDirectory: process.env.TMPDIR!,
  };
}

let namedUsageFixture: DispatchHistoryAdoptionInput['namedUsage'] | undefined;
/** Durable named-usage source witness; PR450 model-capture/claim qualification
 * and the actual installed pair remain separate release-owner requirements. */
function prepareNamedUsageRollbackFixture() {
  if(namedUsageFixture)return namedUsageFixture;
  const ledgerArtifactPath=path.join(process.env.TMPDIR!,`dispatch-bridge-named-usage-${process.pid}.sqlite`);
  const base=new Date('2026-10-03T00:00:00.000Z'),workspaceId='00000000-0000-4000-8000-000000000001',deviceId='bridge-source-fixture';
  const buffer=new LocalEventBuffer(ledgerArtifactPath,{workspaceId,deviceId,enrollmentNow:()=>base,
    delivery:{enabled:true,now:()=>new Date(base.getTime()+61000)}});
  let sealedSha256:string;
  try {
    for(const [id,named]of [['00000000-0000-4000-8000-000000000901',true],['00000000-0000-4000-8000-000000000902',false]] as const)
      buffer.append(aiInteractionEventSchema.parse({id,sessionId:'00000000-0000-4000-8000-000000000911',source:'codex',dataMode:'metadata',
        eventType:'assistant_response',observedAt:base.toISOString(),...(named?{model:'gpt-6.1-sol',inputTokens:19,outputTokens:2}:{}),metadata:named?{}:{usageSource:'capture_gap'}}));
    const lease=buffer.delivery.lease({now:new Date(base.getTime()+61000)});
    if(lease.items.length!==2||lease.locallyDead!==0)throw new Error('bridge named usage fixture failed to seal');
    const sealed=buffer.database.prepare('select delivery_id as id,sealed_envelope_json as sealed from upload_outbox order by delivery_id').all();
    sealedSha256=crypto.createHash('sha256').update(JSON.stringify(sealed)).digest('hex');
  } finally {buffer.close();}
  fs.chmodSync(ledgerArtifactPath,0o600);
  if(path.basename(process.argv[1]??'')==='dispatch-history-adoption-proof.ts') {
    const directory=path.resolve('evidence/dispatch-history-adoption');fs.mkdirSync(directory,{recursive:true,mode:0o700});
    fs.copyFileSync(ledgerArtifactPath,path.join(directory,'named-usage-witness.sqlite'));
    fs.writeFileSync(path.join(directory,'named-usage-witness.json'),JSON.stringify({sourceFixtureOnly:true,installedQualified:false,
      producerBaseCommit:DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit,previousReader:DISPATCH_HISTORY_ROLLBACK_READER,
      workspaceId,deviceId,sealedSha256:sealedSha256!,now:new Date(base.getTime()+182000).toISOString(),
      ledgerSha256:crypto.createHash('sha256').update(fs.readFileSync(ledgerArtifactPath)).digest('hex')},null,2)+'\n',{mode:0o600});
  }
  namedUsageFixture={ledgerArtifactPath,workspaceId,deviceId,sealedSha256:sealedSha256!,now:new Date(base.getTime()+182000).toISOString()};
  return namedUsageFixture;
}

let timingInstalled=false;
/** Observes actual mutation-lock SQL; it never changes statements or returns. */
export function installDispatchHistoryWriterTimingFixture() {
  if(timingInstalled)return;timingInstalled=true;
  const exec=Database.prototype.exec,started=new WeakMap<object,number>();
  const observations:Array<{elapsedMs:number;boundary:string}>=[];
  Database.prototype.exec=function(this:Database.Database,sql:string){
    const tracked=String(this.name).endsWith('.mutation.lock.sqlite');
    const result=exec.call(this,sql);
    if(tracked&&sql==='BEGIN IMMEDIATE')started.set(this,performance.now());
    if(tracked&&(sql==='COMMIT'||sql==='ROLLBACK')) {
      const from=started.get(this);if(from!==undefined){observations.push({elapsedMs:performance.now()-from,boundary:sql});started.delete(this);}
    }
    return result;
  };
  process.on('exit',()=>console.log(JSON.stringify({proof:'dispatch-history-writer-duration-observations',observations})));
}

let readerObservationInstalled=false;
function installReaderExecutionObservation() {
  if(readerObservationInstalled)return;readerObservationInstalled=true;
  const original=childProcess.spawnSync;
  childProcess.spawnSync=((...args:any[])=>{
    const start=performance.now(),result=(original as any)(...args);
    if(String(args[1]?.[1]).includes('DHB2'))console.log(JSON.stringify({readerExecution:{elapsedMs:performance.now()-start,
      status:result.status,signal:result.signal,error:result.error?.code??null,
      profileBytes:Buffer.isBuffer(args[2]?.input)?args[2].input.readUInt32BE(12):null,inputBytes:Buffer.isBuffer(args[2]?.input)?args[2].input.length:null,
      ...(result.status!==0?{diagnostic:String(result.stderr).slice(-4000)}:{})}}));
    return result;
  }) as typeof childProcess.spawnSync;
  syncBuiltinESMExports();
}
