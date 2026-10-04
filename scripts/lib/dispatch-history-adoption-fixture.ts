import crypto from 'node:crypto';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { performance } from 'node:perf_hooks';
import { DISPATCH_HISTORY_ROLLBACK_READER, type DispatchHistoryAdoptionRequest,
  type DispatchHistoryAdoptionInput } from '../../packages/collector-cli/src/dispatch-history-adoption';
import { requireIsolatedProofEnvironment } from './proof-completion';

/** Technical fixture qualification only. It neither authenticates an owner nor
 * grants installation/rollback approval. The real pinned reader executes for
 * every intended generation; a Boolean receipt cannot substitute for it. */
export function dispatchHistoryAdoptionFixture(request: DispatchHistoryAdoptionRequest): DispatchHistoryAdoptionInput {
  requireIsolatedProofEnvironment();
  const readerArtifactPath = path.join(process.env.TMPDIR!, 'dispatch-history-rollback-reader-e7e937fa.cjs');
  if (!fs.existsSync(readerArtifactPath)) {
    fs.copyFileSync(path.resolve('scripts/fixtures/dispatch-history-rollback-reader-e7e937fa.cjs'),
      readerArtifactPath, fs.constants.COPYFILE_EXCL);
    fs.chmodSync(readerArtifactPath, 0o600);
  }
  return {
    contract: { schema: 'dispatch-history-source-adoption/v1', qualificationId: crypto.randomUUID(),
      sourceQualificationOnly: true, rolloutAuthorized: false,
      writerBaseCommit: DISPATCH_HISTORY_ROLLBACK_READER.sourceCommit, reader: DISPATCH_HISTORY_ROLLBACK_READER,
      issuedAt: request.now, validUntil: new Date(Date.parse(request.now)+30_000).toISOString(),
      sourceProfileSha256: request.sourceProfileSha256, nextProfileSha256: request.nextProfileSha256,
      imageSha256: request.imageSha256, generation: request.generation, inventorySha256: request.inventorySha256,
      terminalSha256: request.terminalSha256, roots: request.roots,
      operatingRequirements: { holdOlderReadersBeforeAdoption: true, holdAllWritersDuringRollback: true,
        originalProfileAndImagesRetained: true, exactReaderAndFrozenDependenciesRetained: true, vanillaDowngradeForbidden: true },
    },
    readerArtifactPath,
    dependencyDirectory: path.resolve('packages/collector-cli'), scratchDirectory: process.env.TMPDIR!,
  };
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
