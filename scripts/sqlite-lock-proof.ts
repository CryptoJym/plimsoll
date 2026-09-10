/** Exact pending-event-link stage, real WAL connections, deterministic interleaving. */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import assert from 'node:assert/strict';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { runPendingEventLinkFillStage } from '../packages/collector-cli/src/maintenance-stage-primitives';
const require=createRequire(path.resolve('package.json'));
const Database=require('better-sqlite3');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'plimsoll-sqlite-lock-proof-'));
const buffer=new LocalEventBuffer(path.join(dir,'ledger.sqlite'),{databaseBusyTimeoutMs:900});
const db=buffer.database;
const writer=new Database(path.join(dir,'ledger.sqlite'),{timeout:0});
try {
  db.exec('create table fixture_other_writer(n integer); insert into fixture_other_writer values(0)');
  const prepare=db.prepare.bind(db);
  let injected=false;
  (db as any).prepare=(sql:string)=>{
    const stmt=prepare(sql);
    if(sql.includes('from repo_context_event_links l')&&sql.includes('order by l.event_id limit')) {
      const all=stmt.all.bind(stmt);
      (stmt as any).all=(...args:any[])=>{
        const rows=all(...args);assert(db.inTransaction);
        // This commits and releases the write lock before the stale reader's write.
        writer.prepare('update fixture_other_writer set n=n+1').run();injected=true;return rows;
      };
    }
    return stmt;
  };
  let code:string|null=null;
  try {runPendingEventLinkFillStage(db,{remainingMs:30000,batchSize:256});}catch(e:any){code=e.code;}
  (db as any).prepare=prepare;
  assert(injected);assert.equal(code,'SQLITE_BUSY_SNAPSHOT');assert.equal(writer.inTransaction,false);assert.equal(db.inTransaction,false);
  const retry=runPendingEventLinkFillStage(db,{remainingMs:30000,batchSize:256});
  const checks={snapshotPromotionReproduced:code==='SQLITE_BUSY_SNAPSHOT',otherWriterAlreadyCommitted:!writer.inTransaction,failedTransactionRolledBack:!db.inTransaction,nextFreshTransactionSucceeds:retry.rows===0};
  console.log(JSON.stringify({passed:true,errorCode:code,checks,retry,scope:'Synthetic ledger only. Proves exact-stage read-to-write promotion failure; does not identify Studio0 writer.'},null,2));
} finally {writer.close();buffer.close();fs.rmSync(dir,{recursive:true,force:true});}
