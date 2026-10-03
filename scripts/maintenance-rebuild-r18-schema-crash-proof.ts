/** SIGKILL after side-table DDL must roll back the entire schema transition. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

import { withProof0744Source } from "./proof-0744-source";
let oldRoot = process.env.PR424_0744_CHECKOUT
  ? path.join(process.env.PR424_0744_CHECKOUT, "packages/collector-cli/src") : "";
async function dispatch() {
if (process.argv[2] === "--crash" || process.argv[2] === "--install") {
  const [ledger, ready, release] = process.argv.slice(3);
  const prototype = Database.prototype as typeof Database.prototype & { exec(sql: string): Database.Database };
  const original = prototype.exec;
  prototype.exec = function(sql: string) {
    const result = original.call(this, sql);
    if (sql.includes("create table if not exists maintenance_rebuild_event_order")) {
      fs.writeFileSync(ready!, "after table statement\n");
      const sleep = new Int32Array(new SharedArrayBuffer(4));
      const deadline = Date.now() + 45_000;
      while (Date.now() < deadline) {
        if (release && fs.existsSync(release)) return result;
        Atomics.wait(sleep, 0, 0, 10);
      }
      throw new Error("installer_not_killed_at_seam");
    }
    return result;
  };
  new LocalEventBuffer(ledger!);
} else if (process.argv[2] === "--old-writer") {
  const oldBufferModule = await import(pathToFileURL(path.join(oldRoot, "buffer.ts")).href);
  const oldForwarder = await import(pathToFileURL(path.join(oldRoot, "forwarder.ts")).href);
  const oldConfig = await import(pathToFileURL(path.join(oldRoot, "config.ts")).href);
  if (process.argv[5]) fs.writeFileSync(process.argv[5]!, "old writer starting\n");
  const old = new oldBufferModule.LocalEventBuffer(process.argv[3]!);
  try { oldForwarder.appendForwardedHook(JSON.parse(process.argv[4]!), {
    config: oldConfig.collectorConfigSchema.parse({}), source: "claude_code", buffer: old,
  }); }
  finally { old.close(); }
} else {
  await withProof0744Source(async (checkout) => {
    oldRoot = path.join(checkout, "packages/collector-cli/src");
    process.env.PR424_0744_CHECKOUT = checkout;
    await main();
  });
}
}
void dispatch().catch((error) => { console.error(error); process.exitCode = 1; });

async function main() {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r9-schema-crash-")));
  const ledger = path.join(home, "ledger.sqlite");
  const ready = path.join(home, "crash-ready");
  const release = path.join(home, "installer-release");
  const oldReady = path.join(home, "old-writer-ready");
  const commitMode = process.argv[2] === "--commit";
  let child: ReturnType<typeof spawn> | null = null;
  let oldWriter: ReturnType<typeof spawn> | null = null;
  try {
    const oldBufferModule = await import(pathToFileURL(path.join(oldRoot, "buffer.ts")).href);
    const seed = new oldBufferModule.LocalEventBuffer(ledger);
    seed.close();
    child = spawn(process.execPath, ["--import", "tsx", __filename,
      commitMode ? "--install" : "--crash", ledger, ready, ...(commitMode ? [release] : [])],
      { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    child.stdout!.on("data", (data: Buffer) => { output += data.toString(); });
    child.stderr!.on("data", (data: Buffer) => { output += data.toString(); });
    const deadline = Date.now() + 20_000;
    while (!fs.existsSync(ready)) {
      assert.equal(child.exitCode, null, output);
      assert.ok(Date.now() < deadline, "installer did not reach table statement");
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    const wire = JSON.stringify({ id: randomUUID(), session_id: randomUUID(),
      hook_event_name: "UserPromptSubmit", timestamp: new Date().toISOString(), input_tokens: 9 });
    oldWriter = spawn(process.execPath, ["--import", "tsx", __filename,
      "--old-writer", ledger, wire, oldReady],
    { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"] });
    let oldOutput = "";
    oldWriter.stdout!.on("data", (data: Buffer) => { oldOutput += data.toString(); });
    oldWriter.stderr!.on("data", (data: Buffer) => { oldOutput += data.toString(); });
    const oldDeadline = Date.now() + 20_000;
    while (!fs.existsSync(oldReady)) {
      assert.equal(oldWriter.exitCode, null, oldOutput);
      assert.ok(Date.now() < oldDeadline, "old writer did not start");
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    await new Promise((resolve) => setTimeout(resolve, 150));
    assert.equal(oldWriter.exitCode, null, "0.7.44 writer must wait for the installer transaction");
    const blocked = new Database(ledger, { readonly: true, fileMustExist: true });
    assert.equal((blocked.prepare("select count(*) as n from buffered_events").get() as { n: number }).n, 0);
    blocked.close();
    assert.ok(child.pid);
    if (commitMode) fs.writeFileSync(release, "commit\n");
    else process.kill(child.pid, "SIGKILL");
    const exited = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) =>
      child!.once("exit", (code, signal) => resolve({ code, signal })));
    console.log(JSON.stringify({ check: "installer_child_exit", ...exited, output }));
    if (commitMode) assert.equal(exited.code, 0, output);
    else assert.ok(exited.signal === "SIGKILL" || exited.code === 137,
      `installer must die from SIGKILL: ${JSON.stringify(exited)} ${output}`);
    assert.equal(fs.existsSync(ready), true);
    const observer = new Database(ledger, { readonly: true, fileMustExist: true });
    const sideTable = !!observer.prepare(`select 1 from sqlite_master where type='table'
      and name='maintenance_rebuild_event_order'`).get();
    const triggers = (observer.prepare(`select count(*) as n from sqlite_master where type='trigger'
      and name like 'trg_maintenance_rebuild_event_order_%'`).get() as { n: number }).n;
    observer.close();
    const oldExit = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
      if (oldWriter!.exitCode !== null || oldWriter!.signalCode !== null)
        return resolve({ code: oldWriter!.exitCode, signal: oldWriter!.signalCode });
      oldWriter!.once("exit", (code, signal) => resolve({ code, signal }));
    });
    assert.equal(oldExit.code, 0, oldOutput);
    if (commitMode) {
      const committed = new Database(ledger, { readonly: true, fileMustExist: true });
      const eventId = (JSON.parse(wire) as { id: string }).id;
      const sequence = (committed.prepare(`select o.seq from buffered_events e
        left join maintenance_rebuild_event_order o on o.event_id=e.id where e.id=?`)
        .get(eventId) as { seq: number | null }).seq;
      committed.close();
      console.log(JSON.stringify({ check: "old_writer_started_during_install_committed_after",
        sideTable, triggers, sequence }));
      assert.equal(sideTable, true);
      assert.equal(triggers, 3);
      assert.ok(sequence !== null && sequence > 0);
      return;
    }
    const upgraded = new LocalEventBuffer(ledger);
    upgraded.close();
    const saved = writeHookSpoolEnvelope({ home, source: "claude_code", body: wire,
      cause: "maintenance_rebuild" });
    assert.equal(saved.ok, true);
    markMaintenanceRebuildPause(home);
    recordMaintenanceRebuildRefusal(home, "hook", "claude_code", wire,
      { spoolName: path.basename(saved.path) });
    finishMaintenanceRebuildPause(home);
    fs.unlinkSync(saved.path);
    const receiptDir = path.join(home, "maintenance-rebuild-refusals");
    const receipt = JSON.parse(fs.readFileSync(path.join(receiptDir,
      fs.readdirSync(receiptDir)[0]!), "utf8")) as { at: string };
    const state = reconcileMaintenanceRebuildRefusals(home, ledger,
      Date.parse(receipt.at) + MISSING_HOOK_RETRY_MS + 1);
    console.log(JSON.stringify({ check: "crash_after_table_statement", sideTable,
      triggers, oldWriterStartedDuringTransaction: true, oldWriterSucceeded: true, state }));
    assert.equal(sideTable, false, "the table DDL must roll back on crash");
    assert.equal(triggers, 0);
    assert.equal(state.count, 0);
    assert.equal(state.lost.length, 1, "an unsequenced pre-refusal row is visible loss");
  } finally {
    if (child?.pid && child.exitCode === null && child.signalCode === null) {
      try { process.kill(child.pid, "SIGKILL"); } catch { /* Already exited. */ }
    }
    if (oldWriter?.pid && oldWriter.exitCode === null && oldWriter.signalCode === null) {
      try { process.kill(oldWriter.pid, "SIGKILL"); } catch { /* Already exited. */ }
    }
    fs.rmSync(home, { recursive: true, force: true });
  }
}
