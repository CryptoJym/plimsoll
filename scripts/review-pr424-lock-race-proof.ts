/** Regression: a completed cleanup may not unlink a successor's rebuild lock. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { rebuildLedger, observeRebuildConnectionOwnership,
  connectionOwnershipClosed } from "../packages/collector-cli/src/maintenance-rebuild";
import { currentRebuildWriterIdentity } from "../packages/collector-cli/src/rebuild-writer-identity";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr424-lock-race-")));
const ledger = path.join(root, "ledger.sqlite");
const lock = `${ledger}.maintenance-rebuild.lock`;

async function main() {
  try {
    const db = new Database(ledger);
    db.exec(`create table maintenance_state(key text primary key,value text not null,updated_at text not null);
      create table buffered_events(id text primary key,payload_json text not null);
      create table upload_outbox(delivery_id text primary key,state text not null);
      create table dashboard_snapshots(days integer primary key,payload_json text not null);
      create table finance_publication_control(singleton integer primary key,revision integer not null);
      insert into buffered_events values ('before','{}');
      insert into finance_publication_control values (1,7);
      insert into dashboard_snapshots values (30,'{}'),(90,'{}'),(182,'{}'),(365,'{}'),(1825,'{}');`);
    db.close();
    let successorPublished = false;
    await assert.rejects(() => rebuildLedger({
      ledgerPath: ledger, stage: "S10", walHighWaterBytes: 0, copyDrill: true,
      quiesce: async () => {
        const after = observeRebuildConnectionOwnership(ledger);
        return { before: after, after, connectionsClosed: connectionOwnershipClosed(after) };
      },
      checkpoint: () => [{ busy: 1, log: 1, checkpointed: 0 }],
      resume: async () => {
        assert.equal(fs.existsSync(lock), false, "first owner has released its lock");
        fs.writeFileSync(lock, `${JSON.stringify(currentRebuildWriterIdentity())}\n`, { flag: "wx", mode: 0o600 });
        successorPublished = true;
      },
    }), /wal_not_empty_after_checkpoint/);
    assert.equal(successorPublished, true);
    const survives = fs.existsSync(lock);
    console.log(JSON.stringify({ check: "successor_lock_survives_first_owner_finally", successorPublished, survives }));
    assert.equal(survives, true, "a second rebuild owner's lock must survive first owner's finally");
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
