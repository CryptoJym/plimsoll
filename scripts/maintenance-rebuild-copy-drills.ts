/** Operator-only drills. Invoke only with a cp -c clone of the kept ledger. */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  preflightMaintenanceRebuild, rebuildLedger, recoverInterruptedRebuild,
  renameBackBeforeResume, REQUIRED_REBUILD_WRITERS, readActiveRebuildWriterLeases,
  observeRebuildConnectionOwnership, connectionOwnershipClosed,
} from "../packages/collector-cli/src/maintenance-rebuild";

const cloneRoot = fs.realpathSync(process.argv[2] ?? "");
const mode = process.argv[3];
const ledger = path.join(cloneRoot, "work-ledger.sqlite");
assert.ok(cloneRoot.includes("/eco-6hoxj.164.7/copy-"), "drill_requires_lane_clone");
assert.ok(fs.statSync(ledger).isFile());
const originalHash = "4a99680dc7af70d5458f29c4ff6a63a55177588a498eb5c33a8b4bfd69c7b1c4";
function sha256(file: string) {
  const result = spawnSync("shasum", ["-a", "256", file], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.split(" ")[0];
}
function snapshot() {
  const db = new Database(ledger, { readonly: true, fileMustExist: true });
  try {
    assert.equal(db.pragma("integrity_check", { simple: true }), "ok");
    return {
      events: (db.prepare("select count(*) as n from buffered_events").get() as { n: number }).n,
      outbox: (db.prepare("select count(*) as n from upload_outbox").get() as { n: number }).n,
      snapshots: (db.prepare("select count(*) as n from dashboard_snapshots").get() as { n: number }).n,
    };
  } finally { db.close(); }
}
const input = { ledgerPath: ledger, stage: "S10" as const,
  walHighWaterBytes: 0, copyDrill: true };
const quiesce = async () => {
  const before = observeRebuildConnectionOwnership(ledger);
  const after = observeRebuildConnectionOwnership(ledger);
  return { before, after, connectionsClosed: connectionOwnershipClosed(after) };
};

async function main() {
  assert.equal(sha256(ledger), originalHash);
  console.log(JSON.stringify({ check: "clone_hash", sha256: originalHash, mode }));
  const before = snapshot();
  if (mode === "main") {
    assert.throws(() => preflightMaintenanceRebuild({ ...input, freeBytes: 1 }), /insufficient_headroom/);
    assert.throws(() => preflightMaintenanceRebuild({ ...input, copyDrill: false }), /stage_not_ready/);
    const preflight = preflightMaintenanceRebuild(input);
    console.log(JSON.stringify({ check: "precondition_refusals", requiredBytes: preflight.requiredBytes,
      shortfallAtOneByte: preflight.requiredBytes - 1, outboxPending: preflight.outboxPending }));

    const writer = new LocalEventBuffer(ledger);
    try {
      const held = readActiveRebuildWriterLeases(writer.database);
      assert.deepEqual(held, [{ pid: process.pid, owner: "local_event_buffer" }]);
      await assert.rejects(() => rebuildLedger({ ...input, quiesce,
        resume: async () => undefined }), /writer_not_quiesced/);
      console.log(JSON.stringify({ check: "writer_refusal", observedLeasesHeld: held.length }));
    } finally { writer.close(); }
    const check = new Database(ledger, { readonly: true });
    try { assert.equal(readActiveRebuildWriterLeases(check).length, 0); }
    finally { check.close(); }
    console.log(JSON.stringify({ check: "writer_leases_released", observedLeasesRemaining: 0 }));

    let resumedAfterIncomplete = false;
    await assert.rejects(() => rebuildLedger({ ...input,
      quiesce: async () => ({ ...await quiesce(), connectionsClosed: false }),
      resume: async () => { resumedAfterIncomplete = true; },
    }), /writer_not_quiesced/);
    assert.equal(resumedAfterIncomplete, true);
    console.log(JSON.stringify({ check: "failed_connection_receipt_refusal", resumed: true }));

    const beforeReopenFailure = sha256(ledger);
    let reopenedFailureResumed = false;
    await assert.rejects(() => rebuildLedger({ ...input, quiesce,
      reopen: () => { throw new Error("forced_reopen_failure"); },
      resume: async () => { reopenedFailureResumed = true; },
    }), /forced_reopen_failure/);
    assert.equal(reopenedFailureResumed, true);
    assert.equal(sha256(ledger), beforeReopenFailure);
    assert.deepEqual(snapshot(), before);
    console.log(JSON.stringify({ check: "rename_back_before_resume", restoredSha256: beforeReopenFailure }));

    let resumedAfterSwap = false;
    const result = await rebuildLedger({ ...input, quiesce,
      resume: async () => { resumedAfterSwap = true; },
    });
    assert.equal(resumedAfterSwap, true);
    assert.deepEqual(snapshot(), before);
    assert.throws(() => renameBackBeforeResume(ledger), /forward_repair_only/);
    assert.throws(() => recoverInterruptedRebuild(ledger), /forward_repair_only/);
    console.log(JSON.stringify({ check: "copy_verify_swap_and_forward_refusal",
      pauseMs: result.pauseMs, oldBytes: fs.statSync(result.backupPath).size,
      newBytes: fs.statSync(ledger).size, backupPath: result.backupPath,
      inventory: before }));
    return;
  }
  if (mode === "first-rename-kill" || mode === "active-recover" || mode === "active-rename-back") {
    const killed = spawnSync(process.execPath, ["--import", "tsx", "packages/collector-cli/src/cli.ts",
      "maintenance", "rebuild", "--ledger", ledger, "--copy-drill", "--copy-root", cloneRoot,
      "--stage", "S10", "--wal-high-water-bytes", "0"], {
      cwd: process.cwd(), env: { ...process.env, PLIMSOLL_REBUILD_COPY_KILL_AFTER_FIRST_RENAME: "1" },
      encoding: "utf8", timeout: 180_000,
    });
    assert.equal(killed.signal, "SIGKILL", killed.stderr || killed.stdout);
    assert.equal(fs.existsSync(ledger), false);
    if (mode !== "first-rename-kill") {
      // Active-ledger validation is exercised against this disposable clone.
      // The fixture has no producer auth or LaunchAgent manifest, so the CLI
      // restores the file and then refuses before any service operation.
      const portServer = http.createServer();
      await new Promise<void>((resolve) => portServer.listen(0, "127.0.0.1", resolve));
      const port = (portServer.address() as { port: number }).port;
      await new Promise<void>((resolve) => portServer.close(() => resolve()));
      assert.notEqual(port, 48271);
      fs.writeFileSync(path.join(cloneRoot, "collector.config.json"),
        `${JSON.stringify(collectorConfigSchema.parse({ port }))}\n`, { mode: 0o600 });
      const flag = mode === "active-recover" ? "--recover" : "--rename-back";
      const active = spawnSync(process.execPath, ["--import", "tsx", "packages/collector-cli/src/cli.ts",
        "maintenance", "rebuild", "--ledger", ledger, flag], {
        cwd: process.cwd(), env: { ...process.env, PLIMSOLL_HOME: cloneRoot },
        encoding: "utf8", timeout: 120_000,
      });
      assert.notEqual(active.status, 0);
      assert.match(active.stderr, /launch_agent_manifest_invalid/, active.stderr || active.stdout);
      assert.doesNotMatch(active.stderr, /ENOENT/);
      assert.deepEqual(snapshot(), before);
      console.log(JSON.stringify({ check: "studio5_copy_active_ledger_first_rename_recovery",
        mode, flag, signal: killed.signal, rows: before.events,
        expectedFixtureRefusal: "launch_agent_manifest_invalid", port }));
      return;
    }
    const recovered = spawnSync(process.execPath, ["--import", "tsx", "packages/collector-cli/src/cli.ts",
      "maintenance", "rebuild", "--ledger", ledger, "--copy-drill", "--copy-root", cloneRoot,
      "--recover"], { cwd: process.cwd(), env: process.env, encoding: "utf8", timeout: 120_000 });
    assert.equal(recovered.status, 0, recovered.stderr || recovered.stdout);
    assert.deepEqual(snapshot(), before);
    console.log(JSON.stringify({ check: "studio5_copy_first_rename_sigkill_cli_recovery",
      signal: killed.signal, recovered: JSON.parse(recovered.stdout), inventory: before }));
    return;
  }
  if (mode !== "kill") throw new Error("mode_must_be_main_kill_or_first_rename_kill");
  const child = spawn(process.execPath, ["--import", "tsx", "packages/collector-cli/src/cli.ts",
    "maintenance", "rebuild", "--ledger", ledger, "--copy-drill", "--copy-root", cloneRoot,
    "--stage", "S10", "--wal-high-water-bytes", "0"],
  { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"] });
  let childOutput = "";
  child.stdout.on("data", (data: Buffer) => { childOutput += data.toString(); });
  child.stderr.on("data", (data: Buffer) => { childOutput += data.toString(); });
  const exitPromise = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) =>
    child.once("exit", (code, signal) => resolve({ code, signal })));
  let killedAtBytes = 0;
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline && child.exitCode === null && child.signalCode === null) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    try {
      const state = JSON.parse(fs.readFileSync(`${ledger}.maintenance-rebuild.json`, "utf8")) as { phase: string };
      const target = fs.statSync(`${ledger}.rebuild`, { throwIfNoEntry: false });
      if (state.phase === "vacuum" && target && target.size > 0) {
        killedAtBytes = target.size;
        child.kill("SIGKILL");
        break;
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT" && !(error instanceof SyntaxError)) throw error;
    }
  }
  if (!killedAtBytes) child.kill("SIGKILL");
  const exit = await exitPromise;
  assert.ok(killedAtBytes > 0, `vacuum_not_observed:${childOutput}`);
  assert.equal(exit.signal, "SIGKILL");
  const recovery = recoverInterruptedRebuild(ledger);
  assert.deepEqual(snapshot(), before);
  assert.equal(fs.existsSync(`${ledger}.rebuild`), false);
  const reopened = new LocalEventBuffer(ledger);
  try { assert.equal((reopened.database.prepare("select 1 as ok").get() as { ok: number }).ok, 1); }
  finally { reopened.close(); }
  console.log(JSON.stringify({ check: "sigkill_during_vacuum_recover_capture",
    killedAtBytes, signal: exit.signal, recovery, inventory: before,
    captureReopened: true }));
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
