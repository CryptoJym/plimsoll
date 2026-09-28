/** Regression: recovery must tolerate a SIGKILL during a durable state write. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { recoverInterruptedRebuild, preflightMaintenanceRebuild } from
  "../packages/collector-cli/src/maintenance-rebuild";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr424-crash-temp-")));
const ledger = path.join(root, "ledger.sqlite");

function main() {
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
    const nonce = randomUUID();
    const stateFile = `${ledger}.maintenance-rebuild.json`;
    const state = { version: 1, nonce, phase: "paused", stage: "S10",
      backupPath: `${ledger}.pre-lean-2026-09-28`, targetPath: `${ledger}.rebuild`,
      startedAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
    fs.writeFileSync(stateFile, `${JSON.stringify(state)}\n`);
    fs.writeFileSync(`${ledger}.maintenance-rebuild.lock`, "2147483647\n");
    // The owner died after openSync(..., "wx") in its next writeState call,
    // leaving the nonce-specific temporary while the prior state is durable.
    fs.writeFileSync(`${stateFile}.${nonce}.tmp`, "{partial", { flag: "wx", mode: 0o600 });
    const ownershipProbe = spawnSync("/usr/sbin/lsof", ["-n", "-P", "-t", "--",
      ledger, `${ledger}-wal`, `${ledger}-shm`], { encoding: "utf8", timeout: 15_000 });
    console.log(JSON.stringify({ check: "fixture_lsof", status: ownershipProbe.status,
      signal: ownershipProbe.signal, error: ownershipProbe.error?.message }));
    // Interrupt recovery after its durable terminal state but before it can
    // retire the fence. A fresh call must finish the same recovery.
    const originalUnlink = fs.unlinkSync;
    (fs as typeof fs & { unlinkSync: typeof fs.unlinkSync }).unlinkSync = ((file: fs.PathLike) => {
      if (String(file) === `${ledger}.maintenance-rebuild.lock`) throw new Error("simulated_recovery_unlink_crash");
      return originalUnlink(file);
    }) as typeof fs.unlinkSync;
    let recovered: string;
    try { recovered = recoverInterruptedRebuild(ledger).status; }
    catch (error) { recovered = (error as NodeJS.ErrnoException).code ?? (error as Error).message; }
    finally { (fs as typeof fs & { unlinkSync: typeof fs.unlinkSync }).unlinkSync = originalUnlink; }
    const durablePhase = JSON.parse(fs.readFileSync(stateFile, "utf8")).phase as string;
    const heldLock = fs.existsSync(`${ledger}.maintenance-rebuild.lock`);
    let retry: string;
    try { retry = recoverInterruptedRebuild(ledger).status; }
    catch (error) { retry = (error as Error).message; }
    let nextRebuild: string;
    try { preflightMaintenanceRebuild({ ledgerPath: ledger, stage: "S10", walHighWaterBytes: 0,
      copyDrill: true }); nextRebuild = "ready"; }
    catch (error) { nextRebuild = (error as Error).message; }
    console.log(JSON.stringify({ check: "recover_crash_in_state_write", recovered, retry, nextRebuild,
      durablePhase, heldLock, lockExists: fs.existsSync(`${ledger}.maintenance-rebuild.lock`) }));
    assert.equal(recovered, "simulated_recovery_unlink_crash",
      "the first recovery must reach the restartable lock-retirement seam");
    assert.equal(durablePhase, "recovered");
    assert.equal(heldLock, true);
    assert.equal(retry, "recovered_untouched_source");
    assert.equal(nextRebuild, "ready");
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}
try { main(); } catch (error) { console.error(error); process.exitCode = 1; }
