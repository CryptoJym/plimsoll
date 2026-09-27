import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import Database from "better-sqlite3";
import { connectionOwnershipClosed, observeRebuildConnectionOwnership, rebuildLedger,
  recoverInterruptedRebuild, canonicalRecoveryLedgerPath, REQUIRED_REBUILD_WRITERS } from "../packages/collector-cli/src/maintenance-rebuild";
import { openRebuildFencedDatabase } from "../packages/collector-cli/src/rebuild-open-gate";
import { WalCheckpointWorker } from "../packages/collector-cli/src/wal-checkpoint-worker";

const mutant = process.argv.includes("--skip-fence-mutation");
const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "plimsoll-b13-r2-")));
const home = path.join(root, "home");
fs.mkdirSync(home, { mode: 0o700 });
const cli = path.resolve("packages/collector-cli/src/cli.ts");
const childEnv = { ...process.env, HOME: root, USERPROFILE: root, PLIMSOLL_HOME: home,
  CODEX_HOME: path.join(root, ".codex"), CLAUDE_CONFIG_DIR: path.join(root, ".claude"),
  TMPDIR: root };

function fixture(file: string) {
  const db = new Database(file);
  db.pragma("journal_mode = WAL");
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
function rows(file: string) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try { return db.prepare("select id from buffered_events order by id").all(); }
  finally { db.close(); }
}
function runCli(args: string[], extraEnv: Record<string, string> = {}) {
  return spawnSync(process.execPath, ["--import", "tsx", cli, ...args], {
    cwd: process.cwd(), env: { ...childEnv, ...extraEnv }, encoding: "utf8", timeout: 60_000,
  });
}
async function main() {
  const separateStores = new Set(["outcome-timeline-store", "learning-materializer"]);
  assert.equal(REQUIRED_REBUILD_WRITERS.length, 31);
  for (const name of REQUIRED_REBUILD_WRITERS) {
    const source = fs.readFileSync(path.resolve(`packages/collector-cli/src/${name}.ts`), "utf8");
    if (name === "buffer" || separateStores.has(name)) continue;
    // Fail if any inventory module adds a write-capable independent open;
    // it must be added to the fenced opener inventory first.
    for (const match of source.matchAll(/new Database\([^\n]+/g)) {
      assert.match(match[0], /readonly:\s*true/, `${name} has an unfenced write-capable open`);
    }
  }
  console.log(JSON.stringify({ check: "writer_inventory_routes", inventoried: REQUIRED_REBUILD_WRITERS.length,
    sharedLedgerConnection: REQUIRED_REBUILD_WRITERS.length - separateStores.size,
    separateStateDatabases: [...separateStores] }));
  const ledger = path.join(home, "work-ledger.sqlite");
  fixture(ledger);
  const lock = `${ledger}.maintenance-rebuild.lock`;
  const before = observeRebuildConnectionOwnership(ledger);
  assert.equal(connectionOwnershipClosed(before), true);
  const input = { ledgerPath: ledger, stage: "S10" as const, walHighWaterBytes: 0, copyDrill: true };
  let delayedRefused = false;
  let mutantCommitted = false;
  let fencedResume = false;
  const result = await rebuildLedger({ ...input,
    quiesce: async () => {
      const after = observeRebuildConnectionOwnership(ledger);
      return { before, after, connectionsClosed: connectionOwnershipClosed(after) };
    },
    beforeSwap: () => {
      assert.throws(() => openRebuildFencedDatabase(ledger), /maintenance_rebuild_paused/);
      const previous = process.argv[2];
      process.argv[2] = "start";
      try { assert.throws(() => openRebuildFencedDatabase(ledger), /maintenance_rebuild_paused/); }
      finally { process.argv[2] = previous; }
      assert.throws(() => {
        const raw = new Database(ledger, { timeout: 0 });
        try { raw.prepare("insert into buffered_events values (?,?)").run("during_swap", "{}"); }
        finally { raw.close(); }
      }, /database is locked/, "the exclusive source connection blocks the reviewer's raw opener");
      const direct = runCli(["maintenance", "--disable-account-assertion", "codex", "--yes"]);
      assert.notEqual(direct.status, 0);
      assert.match(direct.stderr, /maintenance_rebuild_paused/);
      delayedRefused = true;
    },
    afterFirstRename: () => {
      if (!mutant) {
        assert.throws(() => openRebuildFencedDatabase(ledger), /maintenance_rebuild_paused/);
        return;
      }
      // Mutation: a call site omits the common gate in the missing-main
      // window. SQLite cannot lock a file that has not yet been renamed in.
      const writer = new Database(ledger, { timeout: 0 });
      try {
        writer.exec("create table buffered_events(id text primary key,payload_json text not null)");
        writer.prepare("insert into buffered_events values (?,?)").run("during_gap", "{}");
        mutantCommitted = true;
      } finally { writer.close(); }
    },
    resume: async () => {
      assert.equal(fs.existsSync(lock), true, "the fence remains through daemon resume");
      assert.throws(() => openRebuildFencedDatabase(ledger), /maintenance_rebuild_paused/);
      const previous = process.argv[2];
      process.argv[2] = "start";
      try { openRebuildFencedDatabase(ledger).close(); fencedResume = true; }
      finally { process.argv[2] = previous; }
    },
  });
  assert.equal(fencedResume, true);
  const active = rows(ledger);
  const backup = rows(result.backupPath);
  assert.deepEqual(active, backup, "a delayed independent writer cannot be lost across the swap");
  if (mutantCommitted) assert.ok(active.some((row) => (row as { id: string }).id === "during_gap"),
    "a committed delayed row survives the swap");
  assert.equal(delayedRefused, true);
  assert.equal(connectionOwnershipClosed(result.quiesce.fencedOwnership), true);
  console.log(JSON.stringify({ check: "late_independent_open_fenced", active, backup,
    quiesce: result.quiesce }));

  const alias = path.join(root, "alias.sqlite");
  fs.symlinkSync(ledger, alias);
  fs.writeFileSync(lock, `${process.pid}\n`);
  try {
    assert.throws(() => openRebuildFencedDatabase(alias), /maintenance_rebuild_paused/);
    const pairing = runCli(["lifecycle", "pairing-indexes", "--apply"]);
    assert.notEqual(pairing.status, 0);
    assert.match(pairing.stderr, /maintenance_rebuild_paused/);
    const workerDatabase = new Database(ledger, { fileMustExist: true });
    try {
      const checkpointWorker = new WalCheckpointWorker(workerDatabase);
      assert.throws(() => (checkpointWorker as unknown as { spawn: () => unknown }).spawn(),
        /maintenance_rebuild_paused/);
    } finally { workerDatabase.close(); }
  } finally { fs.unlinkSync(lock); }
  console.log(JSON.stringify({ check: "independent_openers_fenced",
    accountAssertion: true, lifecyclePairing: true, walCheckpointWorker: true, canonicalAlias: true }));

  for (const mode of ["--recover", "--rename-back"]) {
    const file = path.join(root, `${mode.slice(2)}.sqlite`);
    fixture(file);
    const killed = runCli(["maintenance", "rebuild", "--ledger", file, "--copy-drill", "--copy-root", root,
      "--stage", "S10", "--wal-high-water-bytes", "0"],
    { PLIMSOLL_REBUILD_COPY_KILL_AFTER_FIRST_RENAME: "1" });
    assert.equal(killed.signal, "SIGKILL", killed.stderr || killed.stdout);
    assert.equal(fs.existsSync(file), false, "SIGKILL landed between the two renames");
    const restored = runCli(["maintenance", "rebuild", "--ledger", file, "--copy-drill", "--copy-root", root, mode]);
    assert.equal(restored.status, 0, restored.stderr || restored.stdout);
    assert.deepEqual(rows(file), [{ id: "before" }]);
    console.log(JSON.stringify({ check: `first_rename_sigkill_${mode.slice(2)}`, signal: killed.signal,
      restored: JSON.parse(restored.stdout) }));
  }

  const stale = path.join(root, "stale-lock.sqlite");
  fixture(stale);
  fs.writeFileSync(`${stale}.maintenance-rebuild.lock`, `${process.pid}\n`);
  assert.deepEqual(recoverInterruptedRebuild(stale), { status: "recovered_stale_lock" });
  assert.equal(fs.existsSync(`${stale}.maintenance-rebuild.lock`), false);
  assert.deepEqual(rows(stale), [{ id: "before" }]);
  console.log(JSON.stringify({ check: "lock_before_state_recovery", recovered: true }));
  const parentAlias = path.join(root, "parent-alias");
  fs.symlinkSync(root, parentAlias);
  assert.throws(() => canonicalRecoveryLedgerPath(path.join(parentAlias, "stale-lock.sqlite")),
    /ledger_path_not_canonical/);
  const forged = path.join(root, "forged.sqlite");
  fixture(forged);
  fs.writeFileSync(`${forged}.maintenance-rebuild.json`, JSON.stringify({ version: 1,
    nonce: "353aaf50-5a41-4099-a8d9-8ff9ae9c41fb", phase: "verified", stage: "S10",
    backupPath: path.join(root, "unrelated.sqlite"), targetPath: `${forged}.rebuild`,
    startedAt: new Date().toISOString(), updatedAt: new Date().toISOString() }));
  assert.throws(() => recoverInterruptedRebuild(forged), /rebuild_state_path_invalid/);
  console.log(JSON.stringify({ check: "recovery_path_validation", parentAliasRefused: true,
    forgedBackupRefused: true }));

  const benchmark = path.join(root, "gate-cost.sqlite");
  fixture(benchmark);
  const samples = 40;
  const measure = (open: () => Database.Database) => {
    const start = performance.now();
    for (let n = 0; n < samples; n += 1) open().close();
    return (performance.now() - start) / samples;
  };
  const rawMs = measure(() => new Database(benchmark, { fileMustExist: true }));
  const gatedMs = measure(() => openRebuildFencedDatabase(benchmark, { fileMustExist: true }));
  console.log(JSON.stringify({ check: "default_open_gate_cost", samples, rawMs, gatedMs,
    addedMs: gatedMs - rawMs }));
}
main().finally(() => fs.rmSync(root, { recursive: true, force: true })).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
