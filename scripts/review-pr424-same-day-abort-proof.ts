/** Regression: an ABORT rebuild can follow S10 on the same calendar day. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { rebuildLedger, observeRebuildConnectionOwnership,
  connectionOwnershipClosed, recoverInterruptedRebuild } from "../packages/collector-cli/src/maintenance-rebuild";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr424-same-day-abort-")));
const ledger = path.join(root, "ledger.sqlite");

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
    const quiesce = async () => {
      const after = observeRebuildConnectionOwnership(ledger);
      return { before: after, after, connectionsClosed: connectionOwnershipClosed(after) };
    };
    const common = { ledgerPath: ledger, walHighWaterBytes: 0, copyDrill: true,
      quiesce, resume: async () => undefined };
    const s10 = await rebuildLedger({ ...common, stage: "S10" });
    assert.equal(s10.status, "rebuilt");
    let abortStatus: string;
    let abortBackup: string | undefined;
    try {
      const abort = await rebuildLedger({ ...common, stage: "ABORT" });
      abortStatus = abort.status;
      abortBackup = abort.backupPath;
    } catch (error) {
      abortStatus = (error as Error).message;
    }
    console.log(JSON.stringify({ check: "same_day_abort_after_s10", first: s10.status,
      backup: path.basename(s10.backupPath), second: abortStatus, abortBackup: abortBackup && path.basename(abortBackup) }));
    assert.equal(abortStatus, "rebuilt", "same-day ABORT must not collide with the retained S10 backup");
    assert.notEqual(abortBackup, s10.backupPath);
    assert.equal(fs.existsSync(s10.backupPath), true, "the S10 backup remains as forensic evidence");
    assert.equal(fs.existsSync(abortBackup!), true);
    const stateFile = `${ledger}.maintenance-rebuild.json`;
    const state = JSON.parse(fs.readFileSync(stateFile, "utf8")) as { backupPath: string };
    fs.writeFileSync(stateFile, `${JSON.stringify({ ...state, backupPath: s10.backupPath })}\n`);
    fs.writeFileSync(`${ledger}.maintenance-rebuild.lock`, "2147483647\n");
    assert.throws(() => recoverInterruptedRebuild(ledger), /rebuild_state_path_invalid/,
      "a recovery state cannot borrow another rebuild's retained backup");
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
