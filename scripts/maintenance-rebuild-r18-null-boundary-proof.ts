/** A busy writer can force a null refusal boundary before a 0.7.44 retry. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { execFileSync, spawn } from "node:child_process";
import { pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

import http from "node:http";
import { captureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { releaseStopWindowListener, runStopWindowListener } from "../packages/collector-cli/src/stop-window-listener";
import { withProof0744Source } from "./proof-0744-source";
const oldRoot = process.env.PR424_0744_CHECKOUT
  ? path.join(process.env.PR424_0744_CHECKOUT, "packages/collector-cli/src") : "";
const mode = process.argv[2];

async function dispatch() {
if (mode === "--holder") {
  const [ledger, ready, release] = process.argv.slice(3);
  const db = new Database(ledger!);
  db.exec("BEGIN IMMEDIATE");
  fs.writeFileSync(ready!, "writer lock held\n");
  const sleep = new Int32Array(new SharedArrayBuffer(4));
  const deadline = Date.now() + 150_000;
  while (!fs.existsSync(release!) && Date.now() < deadline) Atomics.wait(sleep, 0, 0, 10);
  if (!fs.existsSync(release!)) throw new Error("holder_release_timeout");
  db.exec("COMMIT");
  db.close();
} else if (mode === "--old-writer") {
  const oldBufferModule = await import(pathToFileURL(path.join(oldRoot, "buffer.ts")).href);
  const oldServer = await import(pathToFileURL(path.join(oldRoot, "server.ts")).href);
  const oldConfig = await import(pathToFileURL(path.join(oldRoot, "config.ts")).href);
  const old = new oldBufferModule.LocalEventBuffer(process.argv[3]!);
  try {
    const drain = oldServer.createHookSpoolDrain(oldConfig.collectorConfigSchema.parse({}), old,
      { home: path.dirname(process.argv[3]!) });
    assert.equal((await drain.tick()).recovered, 1);
  } finally { old.close(); }
} else {
  await withProof0744Source(async (checkout) => {
    process.env.PR424_0744_CHECKOUT = checkout;
    await main();
  });
}
}
void dispatch().catch((error) => { console.error(error); process.exitCode = 1; });

async function main() {
  console.log(JSON.stringify({ check: "measured_process_priority", pid: process.pid, load: os.loadavg(),
    priority: execFileSync("ps", ["-o", "pid,ppid,pri,nice,command", "-p", String(process.pid)], { encoding: "utf8" }) }));
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r9-null-boundary-")));
  const ledger = path.join(home, "ledger.sqlite");
  const ready = path.join(home, "holder-ready");
  const release = path.join(home, "holder-release");
  let holder: ReturnType<typeof spawn> | null = null;
  let listener: Promise<void> | null = null;
  let port = 0;
  const auth = loadOrCreateLocalIngestAuth(home);
  try {
    const initial = new LocalEventBuffer(ledger);
    initial.close();
    markMaintenanceRebuildPause(home);
    if (mode === "--http") {
      for (let candidate = 49600; candidate <= 49699; candidate += 1) {
        const probe = http.createServer();
        const bound = await new Promise<boolean>((resolve) => {
          probe.once("error", () => resolve(false));
          probe.listen(candidate, "127.0.0.1", () => resolve(true));
        });
        if (bound) { await new Promise<void>((resolve) => probe.close(() => resolve())); port = candidate; break; }
      }
      assert.ok(port, "fixture port range has an available port");
      listener = runStopWindowListener(collectorConfigSchema.parse({ port }), home, { mode: "maintenance_rebuild" });
      for (let attempt = 0; ; attempt += 1) {
        try { if ((await fetch(`http://127.0.0.1:${port}/healthz`)).ok) break; } catch { /* Binding. */ }
        assert.ok(attempt < 100, "listener did not bind");
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      const schema = new Database(ledger);
      schema.exec("drop trigger trg_maintenance_rebuild_event_order_insert");
      schema.close();
    }
    const wire = JSON.stringify({ id: randomUUID(), session_id: randomUUID(),
      hook_event_name: "UserPromptSubmit", timestamp: new Date().toISOString(), input_tokens: 9 });
    holder = spawn(process.execPath, ["--import", "tsx", __filename, "--holder", ledger, ready, release],
      { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"] });
    let holderOutput = "";
    holder.stdout!.on("data", (data: Buffer) => { holderOutput += data; });
    holder.stderr!.on("data", (data: Buffer) => { holderOutput += data; });
    const deadline = Date.now() + 20_000;
    while (!fs.existsSync(ready)) {
      assert.equal(holder.exitCode, null, holderOutput);
      assert.ok(Date.now() < deadline, "holder did not acquire the writer lock");
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    const marker = JSON.parse(fs.readFileSync(path.join(home, "maintenance-rebuild-pause.json"), "utf8"));
    const probe = new Database(ledger, { timeout: 0 });
    let busyError: Error | null = null;
    try { probe.exec("BEGIN IMMEDIATE"); probe.exec("ROLLBACK"); }
    catch (error) { busyError = error as Error; }
    probe.close();
    console.log(JSON.stringify({ check: "holder_control", marker,
      busyError: busyError?.message, busyCode: (busyError as { code?: string } | null)?.code,
      holderExited: holder.exitCode !== null,
      fenceExists: fs.existsSync(`${ledger}.maintenance-rebuild.lock`),
      homeEntries: fs.readdirSync(home) }));
    assert.equal((busyError as { code?: string } | null)?.code, "SQLITE_BUSY",
      "fixture holder must hold the ledger writer lock");
    const started = Date.now();
    const originalExec = Database.prototype.exec;
    let saved!: ReturnType<typeof writeHookSpoolEnvelope>;
    Database.prototype.exec = function(sql: string) {
      // Replay the same real SQLITE_BUSY at the 30-second timeout boundary.
      if (sql === "BEGIN IMMEDIATE" && !(["--real", "--http"].includes(mode ?? ""))) throw busyError!;
      return originalExec.call(this, sql);
    };
    try {
      if (mode === "--http") {
        const response = await fetch(`http://127.0.0.1:${port}/hooks/claude-code`, {
          method: "POST", headers: { "content-type": "application/json", "x-plimsoll-token": auth.claudeCodeProducer },
          body: wire });
        assert.equal(response.status, 503);
        assert.equal(response.headers.get("retry-after"), "1");
        const directory = path.join(home, "maintenance-rebuild-refusals");
        assert.equal(fs.readdirSync(directory).filter((name) => name.endsWith(".receipt")).length, 1,
          "the refusal is durable before the 503 reaches the client");
        assert.equal(fs.existsSync(path.join(home, "hook-spool")), false, "the server owns no retry spool");
        console.log(JSON.stringify({ check: "busy_http_503", elapsedMs: Date.now() - started,
          status: response.status, receiptBefore503: true }));
      }
      saved = writeHookSpoolEnvelope({ home, source: "claude_code", body: wire,
      cause: "maintenance_rebuild" }); }
    finally { Database.prototype.exec = originalExec; }
    assert.equal(saved.ok, true);
    const waitMs = Date.now() - started;
    const directory = path.join(home, "maintenance-rebuild-refusals");
    const receiptPath = path.join(directory, fs.readdirSync(directory)[0]!);
    const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8")) as
      { at: string; eventId: string; ledgerAdmissionSequence: number | null };
    fs.writeFileSync(release, "release\n");
    await new Promise<void>((resolve, reject) => {
      if (holder!.exitCode !== null) return holder!.exitCode === 0 ? resolve() :
        reject(new Error(`holder exit ${holder!.exitCode}: ${holderOutput}`));
      holder!.once("exit", (code) => code === 0 ? resolve() :
        reject(new Error(`holder exit ${code}: ${holderOutput}`)));
    });
    // Finish the intentionally incomplete schema after releasing the fixture
    // writer. The unknown refusal boundary must never be retroactively guessed.
    if (mode === "--http") new LocalEventBuffer(ledger).close();
    execFileSync(process.execPath, ["--import", "tsx", __filename, "--old-writer", ledger, wire],
      { cwd: process.cwd(), env: process.env, stdio: "pipe", timeout: 60_000 });
    assert.equal(fs.existsSync(saved.path), false, "the real 0.7.44 drain consumed its spool");
    finishMaintenanceRebuildPause(home);
    const db = new Database(ledger, { readonly: true });
    const row = db.prepare(`select o.seq from buffered_events e
      left join maintenance_rebuild_event_order o on o.event_id=e.id where e.id=?`)
      .get(receipt.eventId) as { seq: number | null };
    db.close();
    const state = reconcileMaintenanceRebuildRefusals(home, ledger,
      Date.parse(receipt.at) + MISSING_HOOK_RETRY_MS + 365 * 24 * 60 * 60 * 1000);
    const stateAgain = reconcileMaintenanceRebuildRefusals(home, ledger,
      Date.parse(receipt.at) + MISSING_HOOK_RETRY_MS + 366 * 24 * 60 * 60 * 1000);
    console.log(JSON.stringify({ check: "busy_writer_null_boundary_old_binary_retry",
      writerWaitMs: waitMs, boundary: receipt.ledgerAdmissionSequence, rowSequence: row.seq,
      state, stateAgain, receiptExists: fs.existsSync(receiptPath) }));
    assert.ok(waitMs < 3_000, "a busy refusal must complete within three seconds");
    assert.equal(receipt.ledgerAdmissionSequence, null);
    assert.ok(row.seq !== null);
    assert.equal(state.count, 0, "a consumed retry must settle visibly after its horizon");
    assert.equal(state.lost.length, 1, "unknown ordering must be visible capture loss");
    assert.equal(state.unverifiedHookRetries, 0, "null ordering cannot establish an unverified retry");
    assert.equal(fs.existsSync(receiptPath), false);
    assert.deepEqual(stateAgain.lost, state.lost, "the loss survives restart");
    const terminal = fs.readFileSync(path.join(home, "maintenance-rebuild-terminal.jsonl"), "utf8");
    assert.match(terminal, /unknown_admission_order/);
    const capture = captureSpoolState(home);
    assert.equal(capture.maintenanceRebuildPending, false);
    assert.equal(capture.losses.length, 1, "the claim input exposes the durable loss");
    assert.equal(stateAgain.count, 0, "the hold must not survive a later restart/reconciliation");
  } finally {
    if (holder && holder.exitCode === null && holder.signalCode === null) {
      fs.writeFileSync(release, "release\n");
      const pid = holder.pid;
      if (pid) { try { process.kill(pid, "SIGTERM"); } catch { /* Already exited. */ } }
    }
    if (listener) { await releaseStopWindowListener(port, home); await listener; }
    fs.rmSync(home, { recursive: true, force: true });
  }
}
