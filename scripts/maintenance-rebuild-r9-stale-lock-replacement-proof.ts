/** The reviewer's stale-unlink interleaving, with the successor following the
 * same SQLite publication protocol as a real rebuild. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import { recoverInterruptedRebuild } from "../packages/collector-cli/src/maintenance-rebuild";
import { currentRebuildWriterIdentity } from "../packages/collector-cli/src/rebuild-writer-identity";

const childCode = String.raw`
const fs = require('node:fs');
const Database = require('better-sqlite3');
const [ledger, identity] = process.argv.slice(1);
const lock = ledger + '.maintenance-rebuild.lock';
const coord = ledger + '.maintenance-rebuild-coordination.sqlite';
fs.writeFileSync(ledger + '.child-ready', 'ready');
const nap = new Int32Array(new SharedArrayBuffer(4));
while (!fs.existsSync(ledger + '.trigger')) Atomics.wait(nap, 0, 0, 10);
const db = new Database(coord, { timeout: 15000 });
try {
  db.pragma('journal_mode = DELETE');
  db.exec('BEGIN EXCLUSIVE');
  const staging = lock + '.successor';
  fs.writeFileSync(staging, identity, { flag: 'wx', mode: 0o600 });
  fs.linkSync(staging, lock);
  fs.unlinkSync(staging);
  fs.writeFileSync(ledger + '.published', 'published');
  db.exec('COMMIT');
} catch (error) {
  try { db.exec('ROLLBACK'); } catch {}
  throw error;
} finally { db.close(); }
`;

async function until(predicate: () => boolean, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(predicate(), true, "fixture child reached its checkpoint");
}
function untilSync(predicate: () => boolean, timeoutMs = 1_500) {
  const deadline = Date.now() + timeoutMs;
  const nap = new Int32Array(new SharedArrayBuffer(4));
  while (!predicate() && Date.now() < deadline) Atomics.wait(nap, 0, 0, 10);
  return predicate();
}

async function main() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r9-stale-lock-")));
  const ledger = path.join(root, "ledger.sqlite");
  const lock = `${ledger}.maintenance-rebuild.lock`;
  const originalUnlink = fs.unlinkSync;
  let child: ReturnType<typeof spawn> | null = null;
  try {
    const db = new Database(ledger);
    db.exec("create table fixture_row (id integer primary key); insert into fixture_row values (1)");
    db.close();
    const nonce = randomUUID();
    const at = new Date().toISOString();
    fs.writeFileSync(`${ledger}.maintenance-rebuild.json`, JSON.stringify({ version: 1, nonce,
      phase: "complete", stage: "S10", backupPath: `${ledger}.pre-lean-${at.slice(0, 10)}`,
      targetPath: `${ledger}.rebuild`, startedAt: at, updatedAt: at }) + "\n");
    fs.writeFileSync(lock, "2147483647\n");
    child = spawn(process.execPath, ["-e", childCode, ledger,
      `${JSON.stringify(currentRebuildWriterIdentity())}\n`],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
    let childError = "";
    child.stderr?.on("data", (chunk) => { childError += String(chunk); });
    await until(() => fs.existsSync(`${ledger}.child-ready`) || child?.exitCode !== null);
    if (!fs.existsSync(`${ledger}.child-ready`)) throw new Error(`publisher_start_failed:${childError}`);
    let atUnlink = false;
    let publishedBeforeUnlink = false;
    (fs as typeof fs & { unlinkSync: typeof fs.unlinkSync }).unlinkSync = ((file: fs.PathLike) => {
      if (String(file) === lock && !atUnlink) {
        atUnlink = true;
        // Recoverer A has removed the stale inode. Recoverer B is paused in
        // its own unlink call while the successor attempts publication.
        originalUnlink(file);
        fs.writeFileSync(`${ledger}.trigger`, "go");
        publishedBeforeUnlink = untilSync(() => fs.existsSync(`${ledger}.published`));
        if (!publishedBeforeUnlink) return;
      }
      return originalUnlink(file);
    }) as typeof fs.unlinkSync;
    const recovered = recoverInterruptedRebuild(ledger);
    (fs as typeof fs & { unlinkSync: typeof fs.unlinkSync }).unlinkSync = originalUnlink;
    await until(() => fs.existsSync(`${ledger}.published`));
    await new Promise<void>((resolve, reject) => {
      if (child!.exitCode !== null) return child!.exitCode === 0 ? resolve() : reject(new Error(childError));
      child!.once("exit", (code) => code === 0 ? resolve() : reject(new Error(childError)));
    });
    const survives = fs.existsSync(lock);
    console.log(JSON.stringify({ check: "stale_recovery_successor_lock", recovered,
      publishedBeforeUnlink, survives }));
    assert.equal(survives, true, "a coordinated successor's live fence survives stale recovery");
  } finally {
    (fs as typeof fs & { unlinkSync: typeof fs.unlinkSync }).unlinkSync = originalUnlink;
    if (child && child.exitCode === null) child.kill("SIGTERM");
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
