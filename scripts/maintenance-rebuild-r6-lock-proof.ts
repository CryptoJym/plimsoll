import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { connectionOwnershipClosed, observeRebuildConnectionOwnership, rebuildLedger } from
  "../packages/collector-cli/src/maintenance-rebuild";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "b13-r6-lock-")));
const ledger = path.join(root, "ledger.sqlite");
const lock = `${ledger}.maintenance-rebuild.lock`;
const otherOwner = "other-rebuild-owner\n";

function fixture() {
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
}

async function main() {
  fixture();
  let launchAgentLoaded = true;
  let daemonReady = true;
  let resumeCalls = 0;
  let raced = false;
  const originalOpen = fs.openSync;
  (fs as typeof fs & { openSync: typeof fs.openSync }).openSync = ((file: fs.PathLike, flags: string | number,
    mode?: number) => {
    if (String(file) === lock && flags === "wx" && !raced) {
      raced = true;
      const other = originalOpen(file, "wx", 0o600);
      try { fs.writeFileSync(other, otherOwner); fs.fsyncSync(other); }
      finally { fs.closeSync(other); }
    }
    return originalOpen(file, flags, mode);
  }) as typeof fs.openSync;
  try {
    await assert.rejects(() => rebuildLedger({ ledgerPath: ledger, stage: "S10", walHighWaterBytes: 0,
      copyDrill: true,
      quiesce: async () => {
        launchAgentLoaded = false;
        daemonReady = false;
        const after = observeRebuildConnectionOwnership(ledger);
        return { before: after, after, connectionsClosed: connectionOwnershipClosed(after) };
      },
      resume: async () => {
        assert.equal(fs.readFileSync(lock, "utf8"), otherOwner,
          "resume cannot remove a lock owned by another rebuild");
        resumeCalls += 1;
        launchAgentLoaded = true;
        daemonReady = true;
      },
    }), /EEXIST/);
  } finally { (fs as typeof fs & { openSync: typeof fs.openSync }).openSync = originalOpen; }
  assert.equal(raced, true, "the other rebuild must acquire the lock after final preflight");
  assert.equal(resumeCalls, 1, "a pre-lock failure resumes the collector once");
  assert.equal(launchAgentLoaded, true, "the launch agent is loaded after failure");
  assert.equal(daemonReady, true, "the daemon is ready after failure");
  assert.equal(fs.readFileSync(lock, "utf8"), otherOwner, "the competing lock remains intact");
  assert.equal(fs.existsSync(`${ledger}.maintenance-rebuild.json`), false);
  fs.unlinkSync(lock);
  let secondPreflightResumes = 0;
  await assert.rejects(() => rebuildLedger({ ledgerPath: ledger, stage: "S10", walHighWaterBytes: 0,
    copyDrill: true,
    quiesce: async () => {
      fs.writeFileSync(lock, otherOwner, { mode: 0o600 });
      const after = observeRebuildConnectionOwnership(ledger);
      return { before: after, after, connectionsClosed: connectionOwnershipClosed(after) };
    },
    resume: async () => { secondPreflightResumes += 1; },
  }), /rebuild_lease_held/);
  assert.equal(secondPreflightResumes, 1, "a lock seen by the final preflight also resumes");
  assert.equal(fs.readFileSync(lock, "utf8"), otherOwner);
  console.log(JSON.stringify({ check: "r6_lock_race_resumes_without_stealing_lock",
    resumeCalls, secondPreflightResumes, launchAgentLoaded, daemonReady, competingLockIntact: true }));
}

main().finally(() => fs.rmSync(root, { recursive: true, force: true })).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
