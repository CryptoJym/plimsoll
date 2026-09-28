import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { LearningMaterializationStateStore } from "../packages/collector-cli/src/learning-materializer";
import { OutcomeTimelineStore } from "../packages/collector-cli/src/outcome-timeline-store";
import { connectionOwnershipClosed, observeRebuildConnectionOwnership, rebuildLedger } from
  "../packages/collector-cli/src/maintenance-rebuild";
import { claimRebuildResumePermit, openRebuildFencedDatabase } from
  "../packages/collector-cli/src/rebuild-open-gate";
import { createCollectorRuntimeIdentity } from "../packages/collector-cli/src/runtime-ownership";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "b13-r3-")));
const cli = path.resolve("packages/collector-cli/src/cli.ts");
const childEnv = { ...process.env, HOME: root, USERPROFILE: root, PLIMSOLL_HOME: path.join(root, "home"),
  CODEX_HOME: path.join(root, ".codex"), CLAUDE_CONFIG_DIR: path.join(root, ".claude"), TMPDIR: root };

function rows(file: string) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try { return db.prepare("select id from buffered_events order by id").all(); }
  finally { db.close(); }
}
function fixture(file: string) {
  const db = new OutcomeTimelineStore(file);
  db.database.exec(`
    create table maintenance_state(key text primary key,value text not null,updated_at text not null);
    create table buffered_events(id text primary key,payload_json text not null);
    create table upload_outbox(delivery_id text primary key,state text not null);
    create table dashboard_snapshots(days integer primary key,payload_json text not null);
    create table finance_publication_control(singleton integer primary key,revision integer not null);
    insert into buffered_events values ('before','{}');
    insert into finance_publication_control values (1,7);
    insert into dashboard_snapshots values (30,'{}'),(90,'{}'),(182,'{}'),(365,'{}'),(1825,'{}');
  `);
  db.close();
}
async function aliasStoreGap() {
  const ledger = path.join(root, "alias-ledger.sqlite");
  const aliasDir = path.join(root, "alias-parent");
  fs.symlinkSync(root, aliasDir);
  fixture(ledger);
  const before = observeRebuildConnectionOwnership(ledger);
  let outcomeRefused = false;
  let learningRefused = false;
  const rebuilt = await rebuildLedger({ ledgerPath: ledger, stage: "S10", walHighWaterBytes: 0,
    copyDrill: true,
    quiesce: async () => {
      const after = observeRebuildConnectionOwnership(ledger);
      return { before, after, connectionsClosed: connectionOwnershipClosed(after) };
    },
    afterFirstRename: () => {
      const alias = path.join(aliasDir, path.basename(ledger));
      try { new OutcomeTimelineStore(alias).close(); }
      catch (error) { assert.match(String(error), /maintenance_rebuild_paused/); outcomeRefused = true; }
      try { new LearningMaterializationStateStore(alias).close(); }
      catch (error) { assert.match(String(error), /maintenance_rebuild_paused/); learningRefused = true; }
      for (const [label, args] of [
        ["--store", ["backfill-outcome-performance", "--store", alias]],
        ["--state", ["materialize-learning-evidence", "--ledger", ledger,
          "--store", path.join(root, "auxiliary.sqlite"), "--state", alias]],
      ] as const) {
        const run = spawnSync(process.execPath, ["--import", "tsx", cli, ...args],
          { cwd: process.cwd(), env: childEnv, encoding: "utf8", timeout: 30_000 });
        assert.notEqual(run.status, 0, `${label} unexpectedly accepted an aliased target`);
        assert.match(run.stderr, /maintenance_rebuild_paused/, `${label}: ${run.stderr || run.stdout}`);
      }
      assert.equal(fs.existsSync(ledger), false, "a refused alias must not recreate the missing main");
    },
    resume: async () => undefined,
  });
  assert.equal(outcomeRefused, true, "reviewer --store path alias must be fenced");
  assert.equal(learningRefused, true, "reviewer --state path alias must be fenced");
  assert.deepEqual(rows(ledger), [{ id: "before" }]);
  assert.deepEqual(rows(rebuilt.backupPath), [{ id: "before" }]);
  console.log(JSON.stringify({ check: "r2_1_alias_store_first_rename", outcomeRefused, learningRefused,
    activeRows: rows(ledger), backupRows: rows(rebuilt.backupPath) }));
}
function recoveryFixture(file: string) {
  const db = new Database(file);
  db.exec("create table buffered_events(id text primary key); insert into buffered_events values ('before')");
  db.close();
  const backup = `${file}.pre-lean-${new Date().toISOString().slice(0, 10)}`;
  const now = new Date().toISOString();
  fs.writeFileSync(`${file}.maintenance-rebuild.json`, `${JSON.stringify({ version: 1, nonce: randomUUID(),
    phase: "verified", stage: "S10", backupPath: backup, targetPath: `${file}.rebuild`,
    startedAt: now, updatedAt: now })}\n`);
  fs.writeFileSync(`${file}.maintenance-rebuild.lock`, "2147483647\n");
  fs.renameSync(file, backup);
  return backup;
}
function activeMissingMainRecovery() {
  fs.mkdirSync(childEnv.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });
  const ledger = path.join(childEnv.PLIMSOLL_HOME, "work-ledger.sqlite");
  for (const mode of ["--recover", "--rename-back"]) {
    const backup = recoveryFixture(ledger);
    assert.equal(fs.existsSync(ledger), false);
    const run = spawnSync(process.execPath, ["--import", "tsx", cli, "maintenance", "rebuild",
      "--ledger", ledger, mode], { cwd: process.cwd(), env: childEnv, encoding: "utf8", timeout: 30_000 });
    // The disposable home has no launch-agent manifest. Reaching this later
    // refusal proves recovery completed without touching a live agent.
    assert.match(run.stderr, /launch_agent_manifest_invalid/, `${mode}: ${run.stderr || run.stdout}`);
    assert.doesNotMatch(run.stderr, /ENOENT/);
    assert.deepEqual(rows(ledger), [{ id: "before" }]);
    assert.equal(fs.existsSync(backup), false);
    console.log(JSON.stringify({ check: "r2_2_active_missing_main_recovery", mode,
      restoredRows: rows(ledger), expectedFixtureRefusal: "launch_agent_manifest_invalid" }));
    fs.rmSync(ledger);
  }
}
function secondStartRefused() {
  const ledger = path.join(root, "resume-ledger.sqlite");
  const db = new Database(ledger);
  db.exec("create table buffered_events(id text primary key); insert into buffered_events values ('before')");
  db.close();
  fs.writeFileSync(`${ledger}.maintenance-rebuild.lock`, "fixture\n");
  const nonce = randomUUID();
  fs.writeFileSync(`${ledger}.maintenance-rebuild.json`, JSON.stringify({ phase: "resume_started", nonce }));
  const prior = process.argv[2];
  process.argv[2] = "start";
  try { assert.throws(() => openRebuildFencedDatabase(ledger), /maintenance_rebuild_paused/); }
  finally { process.argv[2] = prior; }
  const identity = createCollectorRuntimeIdentity();
  const startLock = path.join(root, "collector.pid.start.lock");
  fs.writeFileSync(startLock, "null", { mode: 0o600 });
  assert.throws(() => claimRebuildResumePermit(ledger, startLock, identity), /maintenance_rebuild_paused/,
    "an invalid collector start lock cannot claim a permit");
  fs.writeFileSync(startLock, JSON.stringify({ ...identity, version: 3, label: "com.plimsoll.collector" }),
    { mode: 0o600 });
  assert.equal(claimRebuildResumePermit(ledger, startLock, identity), true);
  assert.throws(() => claimRebuildResumePermit(ledger, startLock, identity), /maintenance_rebuild_paused/,
    "the claim is one-use");
  fs.writeFileSync(`${ledger}.maintenance-rebuild.json`, JSON.stringify({ phase: "resume_started", nonce: randomUUID() }));
  assert.throws(() => openRebuildFencedDatabase(ledger), /maintenance_rebuild_paused/,
    "a different rebuild nonce invalidates the claim");
  fs.writeFileSync(`${ledger}.maintenance-rebuild.json`, JSON.stringify({ phase: "resume_started", nonce }));
  const resumed = openRebuildFencedDatabase(ledger);
  resumed.close();
  const second = spawnSync(process.execPath, ["--import", "tsx",
    path.resolve("scripts/maintenance-rebuild-resume-child.ts"), ledger],
    { cwd: process.cwd(), env: childEnv, encoding: "utf8", timeout: 30_000 });
  assert.equal(second.status, 0, second.stderr || second.stdout);
  assert.match(second.stdout, /second_start_refused/);
  assert.deepEqual(rows(ledger), [{ id: "before" }]);
  console.log(JSON.stringify({ check: "r2_3_one_use_resume_permit", rows: rows(ledger),
    firstProcessPermitted: true, secondStartRefused: true }));
}

function leaseDirectoryRetiresAfterClose() {
  const ledger = path.join(root, "closed-writer.sqlite");
  const directory = `${ledger}.rebuild-open-leases`;
  const db = openRebuildFencedDatabase(ledger);
  try {
    db.exec("create table proof_row(id integer primary key)");
    assert.equal(fs.existsSync(directory), true);
  } finally { db.close(); }
  assert.equal(fs.existsSync(directory), false, "the last writer must remove its empty lease directory");
  console.log(JSON.stringify({ check: "default_off_lease_directory_cleanup", removed: true }));
}

async function main() {
  const requested = process.argv[2] ?? "all";
  try {
    if (requested === "all" || requested === "aliases") await aliasStoreGap();
    if (requested === "all" || requested === "recovery") activeMissingMainRecovery();
    if (requested === "all" || requested === "resume") secondStartRefused();
    if (requested === "all" || requested === "leases") leaseDirectoryRetiresAfterClose();
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
