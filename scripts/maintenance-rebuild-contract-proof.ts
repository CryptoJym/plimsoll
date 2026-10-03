/** Contract cases can also run unchanged on the review's 15f4f149 checkout. */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { writeHookSpoolFile } from "../packages/collector-cli/src/hook-spool";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { connectionOwnershipClosed, observeRebuildConnectionOwnership, readActiveRebuildWriterLeases,
  recoverInterruptedRebuild, rebuildLedger, REQUIRED_REBUILD_WRITERS,
  type QuiesceReceipt } from "../packages/collector-cli/src/maintenance-rebuild";
import { releaseStopWindowListener, runStopWindowListener } from "../packages/collector-cli/src/stop-window-listener";

const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "plimsoll-b13-contract-")));
const selected = process.argv[2] ?? "all";
function fixture(file: string, full = false) {
  const db = new Database(file);
  db.pragma("journal_mode = WAL");
  if (full) db.exec(`create table maintenance_state(key text primary key,value text not null,updated_at text not null);
    create table buffered_events(id text primary key,payload_json text not null);
    create table upload_outbox(delivery_id text primary key,state text not null);
    create table dashboard_snapshots(days integer primary key,payload_json text not null);
    create table finance_publication_control(singleton integer primary key,revision integer not null);
    insert into buffered_events values ('before','{}');
    insert into finance_publication_control values (1,7);
    insert into dashboard_snapshots values (30,'{}'),(90,'{}'),(182,'{}'),(365,'{}'),(1825,'{}');`);
  else db.exec("create table retained(id text primary key); insert into retained values ('before')");
  db.close();
}
function ids(file: string) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try { return db.prepare("select id from buffered_events order by id").all(); }
  finally { db.close(); }
}
async function lateWriter() {
  const file = path.join(root, "late.sqlite");
  fixture(file, true);
  const originalRename = fs.renameSync;
  let writerBlocked = false;
  (fs as typeof fs & { renameSync: typeof fs.renameSync }).renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
    if (String(from) === file && String(to).includes(".pre-lean-")) {
      try {
        const writer = new Database(file, { timeout: 0 });
        try { writer.prepare("insert into buffered_events values (?,?)").run("during_swap", "{}"); }
        finally { writer.close(); }
      } catch (error) {
        if ((error as { code?: string }).code !== "SQLITE_BUSY") throw error;
        writerBlocked = true;
      }
    }
    return originalRename(from, to);
  }) as typeof fs.renameSync;
  try {
    const quiesce = async () => {
      if (typeof observeRebuildConnectionOwnership === "function") {
        const after = observeRebuildConnectionOwnership(file);
        return { before: after, after, connectionsClosed: connectionOwnershipClosed(after) };
      }
      return { modules: [...REQUIRED_REBUILD_WRITERS], connectionsClosed: true } as unknown as
        QuiesceReceipt;
    };
    const result = await rebuildLedger({ ledgerPath: file, stage: "S10", walHighWaterBytes: 0,
      copyDrill: true, quiesce, resume: async () => undefined });
    assert.equal(writerBlocked, true, "the raw independent SQLite opener is blocked");
    assert.deepEqual(ids(file), ids(result.backupPath), "no committed late row is lost");
    console.log(JSON.stringify({ check: "F1_late_raw_writer", writerBlocked }));
  } finally {
    (fs as typeof fs & { renameSync: typeof fs.renameSync }).renameSync = originalRename;
  }
}
function missingMainCli() {
  const file = path.join(root, "missing.sqlite");
  fixture(file);
  const backupPath = `${file}.pre-lean-2026-09-27`;
  const now = new Date().toISOString();
  fs.writeFileSync(`${file}.maintenance-rebuild.json`, JSON.stringify({ version: 1, nonce: randomUUID(),
    phase: "verified", stage: "S10", backupPath, targetPath: `${file}.rebuild`,
    startedAt: now, updatedAt: now }));
  fs.writeFileSync(`${file}.maintenance-rebuild.lock`, "2147483647\n");
  fs.renameSync(file, backupPath);
  const cli = path.resolve("packages/collector-cli/src/cli.ts");
  const result = spawnSync(process.execPath, ["--import", "tsx", cli, "maintenance", "rebuild",
    "--ledger", file, "--copy-drill", "--copy-root", root, "--recover"], {
      cwd: process.cwd(), env: { ...process.env, HOME: root, USERPROFILE: root,
        PLIMSOLL_HOME: path.join(root, "home"), CODEX_HOME: path.join(root, ".codex"),
        CLAUDE_CONFIG_DIR: path.join(root, ".claude"), TMPDIR: root },
      encoding: "utf8", timeout: 30_000,
    });
  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.equal(fs.existsSync(file), true);
  console.log(JSON.stringify({ check: "F3_missing_main_cli_recovery", status: result.status }));
}
function observedLease() {
  const file = path.join(root, "owner.sqlite");
  const buffer = new LocalEventBuffer(file);
  try {
    const owners = readActiveRebuildWriterLeases(buffer.database);
    assert.deepEqual(owners, [{ pid: process.pid, owner: "local_event_buffer" }],
      "the receipt names the actual connection rather than 31 static modules");
    console.log(JSON.stringify({ check: "F4_observed_connection_lease", owners }));
  } finally { buffer.close(); }
}
function lockWithoutState() {
  const file = path.join(root, "stale.sqlite");
  fixture(file);
  fs.writeFileSync(`${file}.maintenance-rebuild.lock`, "2147483647\n");
  assert.deepEqual(recoverInterruptedRebuild(file), { status: "recovered_stale_lock" });
  console.log(JSON.stringify({ check: "LOW_lock_without_state_recovery" }));
}
async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port === 48271 ? freePort() : port;
}
async function markerRetired() {
  const home = path.join(root, "marker-home");
  fs.mkdirSync(home, { mode: 0o700 });
  loadOrCreateLocalIngestAuth(home);
  const port = await freePort();
  const listener = runStopWindowListener(collectorConfigSchema.parse({ port }), home,
    { mode: "maintenance_rebuild" });
  try {
    for (let n = 0; n < 100; n += 1) {
      try { if ((await fetch(`http://127.0.0.1:${port}/healthz`)).status === 200) break; }
      catch { /* binding */ }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    await releaseStopWindowListener(port, home);
    await listener;
    assert.equal(captureSpoolState(home).pendingFiles, 0);
    await new Promise((resolve) => setTimeout(resolve, 3));
    const unrelated = writeHookSpoolFile({ home, source: "claude_code", body: "{}" });
    assert.ok(unrelated);
    assert.equal(captureSpoolState(home).maintenanceRebuildPending, false,
      "a later unrelated backlog is not marked as rebuild pending");
    const legacyHome = path.join(root, "legacy-marker-home");
    fs.mkdirSync(legacyHome, { mode: 0o700 });
    fs.writeFileSync(path.join(legacyHome, "maintenance-rebuild-pause.json"),
      JSON.stringify({ version: 1, at: new Date(Date.now() - 1000).toISOString() }));
    assert.equal(captureSpoolState(legacyHome).maintenanceRebuildPending, false);
    assert.equal(fs.existsSync(path.join(legacyHome, "maintenance-rebuild-pause.json")), false,
      "an undrained legacy marker with no arrivals is retired");
    console.log(JSON.stringify({ check: "LOW_pause_marker_retired" }));
  } finally {
    await releaseStopWindowListener(port, home).catch(() => false);
    await listener;
  }
}
async function main() {
  const cases: Record<string, () => void | Promise<void>> = {
    f1: lateWriter, f3: missingMainCli, f4: observedLease, lock: lockWithoutState, marker: markerRetired,
  };
  assert.ok(selected === "all" || selected in cases);
  for (const [name, run] of Object.entries(cases)) if (selected === "all" || selected === name) await run();
}
main().finally(() => fs.rmSync(root, { recursive: true, force: true })).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
