/** Rebuild lock ownership and post-complete SIGKILL drills on disposable SQLite files. */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { rebuildLedger, type ConnectionOwnership } from
  "../packages/collector-cli/src/maintenance-rebuild";
import { openRebuildFencedDatabase } from "../packages/collector-cli/src/rebuild-open-gate";
import { currentRebuildWriterIdentity } from "../packages/collector-cli/src/rebuild-writer-identity";

const childMode = process.argv[2] === "gap-child" || process.argv[2] === "complete-child";
const root = childMode ? path.dirname(process.argv[3]!) :
  fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "b13-r7-recovery-")));
const script = path.resolve("scripts/maintenance-rebuild-r7-recovery-proof.ts");
const cli = path.resolve("packages/collector-cli/src/cli.ts");

function fixture(ledger: string) {
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
function rows(ledger: string) {
  const db = new Database(ledger, { readonly: true, fileMustExist: true });
  try { return db.prepare("select id from buffered_events order by id").all(); }
  finally { db.close(); }
}
function recoverCli(ledger: string) {
  return spawnSync(process.execPath, ["--import", "tsx", cli, "maintenance", "rebuild",
    "--ledger", ledger, "--copy-drill", "--copy-root", root, "--recover"],
  { cwd: process.cwd(), env: process.env, encoding: "utf8", timeout: 60_000 });
}
function waitFor(file: string, child: ReturnType<typeof spawn>, output: () => string) {
  return new Promise<void>((resolve, reject) => {
    const deadline = Date.now() + 30_000;
    const poll = () => {
      if (fs.existsSync(file)) return resolve();
      if (child.exitCode !== null || child.signalCode !== null) return reject(new Error(
        `child_exited_before_seam:${child.exitCode ?? child.signalCode}\n${output()}`));
      if (Date.now() >= deadline) return reject(new Error("child_seam_timeout"));
      setTimeout(poll, 20);
    };
    poll();
  });
}
function startChild(mode: "gap-child" | "complete-child", ledger: string) {
  const child = spawn(process.execPath, ["--import", "tsx", script, mode, ledger],
    { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  child.stdout.on("data", (data: Buffer) => { output += data.toString(); });
  child.stderr.on("data", (data: Buffer) => { output += data.toString(); });
  const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) =>
    child.once("exit", (code, signal) => resolve({ code, signal })));
  return { child, exit, output: () => output };
}
async function childRebuild(mode: "gap-child" | "complete-child", ledger: string) {
  const lock = `${ledger}.maintenance-rebuild.lock`;
  let afterLockPublished: (() => void) | undefined;
  if (mode === "gap-child") {
    const ready = `${ledger}.r7-ready`;
    const release = `${ledger}.r7-release`;
    const pause = () => {
      fs.writeFileSync(ready, "lock published, SQLite not opened\n");
      const deadline = Date.now() + 30_000;
      const sleep = new Int32Array(new SharedArrayBuffer(4));
      while (!fs.existsSync(release) && Date.now() < deadline) Atomics.wait(sleep, 0, 0, 20);
      if (!fs.existsSync(release)) throw new Error("gap_release_timeout");
    };
    afterLockPublished = pause;
  } else {
    const originalUnlink = fs.unlinkSync;
    (fs as typeof fs & { unlinkSync: typeof fs.unlinkSync }).unlinkSync = ((file: fs.PathLike) => {
      if (String(file) === lock) process.kill(process.pid, "SIGKILL");
      return originalUnlink(file);
    }) as typeof fs.unlinkSync;
  }
  // This drill targets the published-lock seam. The disposable fixture was
  // closed before spawn, so supply its known empty quiesce receipt instead of
  // making reaching the seam depend on a second, host-sensitive lsof run.
  // rebuildLedger still performs its real lsof check under the fence; the
  // writer-route proof separately exercises live connection ownership.
  const empty: ConnectionOwnership = { openTokens: [], sqlitePids: [], writerLeases: [] };
  const quiesce = async () => ({ before: empty, after: empty, connectionsClosed: true });
  const rebuilt = await rebuildLedger({ ledgerPath: ledger, stage: "S10", walHighWaterBytes: 0,
    copyDrill: true, quiesce, resume: async () => undefined, afterLockPublished });
  console.log(JSON.stringify({ check: "owner_swap_completed", pauseMs: rebuilt.pauseMs }));
}
async function liveGap() {
  const ledger = path.join(root, "live-owner.sqlite");
  fixture(ledger);
  const { child, exit, output } = startChild("gap-child", ledger);
  const lock = `${ledger}.maintenance-rebuild.lock`;
  let recovery: ReturnType<typeof recoverCli> | undefined;
  try {
    await waitFor(`${ledger}.r7-ready`, child, output);
    assert.equal(fs.existsSync(lock), true, "owner has published its fence");
    assert.equal(fs.existsSync(`${ledger}.maintenance-rebuild.json`), false,
      "owner is paused before SQLite and the first state write");
    recovery = recoverCli(ledger);
    assert.notEqual(recovery.status, 0, "recovery must refuse a live rebuild owner");
    assert.match(recovery.stderr, /rebuild_owner_active/);
    assert.equal(fs.existsSync(lock), true, "recovery cannot remove a live owner's lock");
  } finally { fs.writeFileSync(`${ledger}.r7-release`, "continue\n"); }
  const owner = await exit;
  assert.equal(owner.code, 0, output());
  assert.deepEqual(rows(ledger), [{ id: "before" }]);
  assert.equal(fs.existsSync(lock), false);
  console.log(JSON.stringify({ check: "r7_recover_refuses_live_owner_before_sqlite",
    recoverExit: recovery?.status, ownerExit: owner.code, lockHeld: true, swapComplete: true }));
}
async function completedCrash() {
  const ledger = path.join(root, "completed-owner.sqlite");
  fixture(ledger);
  const { exit, output } = startChild("complete-child", ledger);
  const owner = await exit;
  assert.equal(owner.signal, "SIGKILL", output());
  const state = JSON.parse(fs.readFileSync(`${ledger}.maintenance-rebuild.json`, "utf8")) as { phase: string };
  assert.equal(state.phase, "complete", "owner was killed after the durable complete state");
  const lock = `${ledger}.maintenance-rebuild.lock`;
  assert.equal(fs.existsSync(lock), true);
  const deadOwner = fs.readFileSync(lock, "utf8");
  fs.writeFileSync(lock, `${JSON.stringify(currentRebuildWriterIdentity())}\n`);
  const liveOwner = recoverCli(ledger);
  assert.notEqual(liveOwner.status, 0, "a complete state cannot retire a live owner's fence");
  assert.match(liveOwner.stderr, /rebuild_owner_active/);
  assert.equal(fs.existsSync(lock), true);
  fs.writeFileSync(lock, deadOwner);
  // Simulate the already resumed daemon's open SQLite connection. This
  // connection is safe to keep: phase complete has no future rename.
  const runningDaemon = new Database(ledger, { fileMustExist: true });
  let recovered: ReturnType<typeof recoverCli>;
  try {
    assert.deepEqual(runningDaemon.prepare("select id from buffered_events").all(), [{ id: "before" }]);
    recovered = recoverCli(ledger);
  } finally { runningDaemon.close(); }
  assert.equal(recovered.status, 0, recovered.stderr || recovered.stdout);
  assert.equal(JSON.parse(recovered.stdout).status, "recovered_completed_rebuild");
  assert.equal(fs.existsSync(lock), false);
  assert.deepEqual(rows(ledger), [{ id: "before" }]);
  const resumed = openRebuildFencedDatabase(ledger, { fileMustExist: true });
  try { assert.deepEqual(resumed.prepare("select id from buffered_events order by id").all(),
    [{ id: "before" }]); }
  finally { resumed.close(); }
  console.log(JSON.stringify({ check: "r7_complete_sigkill_recover_and_reopen",
    signal: owner.signal, liveOwnerRefused: true, recoverExit: recovered.status, ledgerOpened: true }));
}
function unverifiableLock() {
  const ledger = path.join(root, "unverifiable-owner.sqlite");
  fixture(ledger);
  const lock = `${ledger}.maintenance-rebuild.lock`;
  fs.writeFileSync(lock, "", { mode: 0o600 });
  const refused = recoverCli(ledger);
  assert.notEqual(refused.status, 0);
  assert.match(refused.stderr, /rebuild_owner_unverified/);
  assert.equal(fs.existsSync(lock), true);
  assert.deepEqual(rows(ledger), [{ id: "before" }]);
  console.log(JSON.stringify({ check: "r7_empty_owner_identity_fails_closed", recoverExit: refused.status }));
}

async function main() {
  const mode = process.argv[2] ?? "all";
  try {
    if (mode === "gap-child" || mode === "complete-child") return await childRebuild(mode, process.argv[3]!);
    if (mode === "gap" || mode === "all") await liveGap();
    if (mode === "complete" || mode === "all") await completedCrash();
    if (mode === "all") unverifiableLock();
  } finally { if (mode !== "gap-child" && mode !== "complete-child") fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
