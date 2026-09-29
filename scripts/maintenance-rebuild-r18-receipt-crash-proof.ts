/** Reviewer R9-3: SIGKILL at receipt publication, plus a live-writer control. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, reconcileMaintenanceRebuildRefusals } from
  "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const mode = process.argv[2];
if (mode === "--child") {
  const [home, wire, seam, ready] = process.argv.slice(3);
  const pause = (file: string) => {
    fs.writeFileSync(ready!, file);
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30_000);
    throw new Error("child_not_killed_at_publication_seam");
  };
  const open = fs.openSync;
  fs.openSync = ((file, flags, permissions) => {
    const descriptor = open(file, flags, permissions);
    if (seam === "empty" && flags === "wx" && String(file).includes("maintenance-rebuild-refusals/") &&
      /\.receipt(?:\.[0-9a-f-]{36}\.tmp)?$/.test(String(file))) pause(String(file));
    return descriptor;
  }) as typeof fs.openSync;
  const rename = fs.renameSync;
  fs.renameSync = ((from, to) => {
    if (seam === "fsynced" && String(to).endsWith(".receipt")) pause(String(from));
    return rename(from, to);
  }) as typeof fs.renameSync;
  const result = writeHookSpoolEnvelope({ home: home!, source: "claude_code", body: wire!,
    cause: "maintenance_rebuild" });
  assert.equal(result.ok, true);
} else if (mode === "--legacy-writer") {
  const [file, ready] = process.argv.slice(3);
  fs.openSync(file!, "w", 0o600);
  fs.writeFileSync(ready!, file!);
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30_000);
  throw new Error("legacy_writer_not_killed");
} else {
  void main().catch((error) => { console.error(error); process.exitCode = 1; });
}

async function stop(child: ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const done = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  if (child.pid) process.kill(child.pid, "SIGKILL");
  await done;
}
async function main() {
  for (const seam of ["empty", "fsynced", "legacy"] as const) {
    const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r18-receipt-")));
    const ledger = path.join(home, "ledger.sqlite");
    const ready = path.join(home, "ready");
    let child: ChildProcess | null = null;
    let writer: Database.Database | null = null;
    try {
      new LocalEventBuffer(ledger).close();
      markMaintenanceRebuildPause(home);
      const wire = JSON.stringify({ id: randomUUID(), session_id: randomUUID(),
        hook_event_name: "UserPromptSubmit", timestamp: new Date().toISOString(), input_tokens: 9 });
      let args = ["--child", home, wire, seam, ready];
      const directory = path.join(home, "maintenance-rebuild-refusals");
      if (seam === "legacy") {
        const seeded = writeHookSpoolEnvelope({ home, source: "claude_code", body: wire,
          cause: "maintenance_rebuild" });
        assert.equal(seeded.ok, true);
        const file = path.join(directory, fs.readdirSync(directory).find((name) => name.endsWith(".receipt"))!);
        args = ["--legacy-writer", file, ready];
      } else {
        // Hold the real ledger writer as in the reviewer's reproducer. On the
        // fixed path this times out before a private temp is created; it can
        // never expose an empty final receipt while waiting for the ledger.
        writer = new Database(ledger);
        writer.exec("BEGIN IMMEDIATE");
      }
      child = spawn(process.execPath, ["--import", "tsx", __filename, ...args],
        { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"] });
      let output = "";
      for (const stream of [child.stdout!, child.stderr!]) stream.on("data", (part) => { output += part; });
      const deadline = Date.now() + 20_000;
      while (!fs.existsSync(ready)) {
        assert.equal(child.exitCode, null, output);
        assert.ok(Date.now() < deadline, `publication seam not reached: ${output}`);
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      const artifact = fs.readFileSync(ready, "utf8");
      const size = fs.statSync(artifact).size;
      const live = reconcileMaintenanceRebuildRefusals(home, ledger);
      console.log(JSON.stringify({ check: "live_receipt_writer_held", seam, size, live }));
      assert.equal(fs.existsSync(artifact), true, "reconciliation never reaps a live writer");
      assert.equal(live.lost.length, 0);
      await stop(child);
      assert.equal(child.signalCode, "SIGKILL");
      writer?.exec("COMMIT"); writer?.close(); writer = null;
      finishMaintenanceRebuildPause(home);
      const immediate = reconcileMaintenanceRebuildRefusals(home, ledger);
      const afterHorizon = reconcileMaintenanceRebuildRefusals(home, ledger,
        Date.now() + MISSING_HOOK_RETRY_MS + 365 * 24 * 60 * 60 * 1000);
      console.log(JSON.stringify({ check: "sigkill_receipt_publication", seam, size, immediate, afterHorizon }));
      assert.equal(afterHorizon.count, 0, "an abandoned receipt cannot hold attestation forever");
      assert.equal(afterHorizon.lost.length, 1, "abandoned evidence is visible capture loss");
      assert.equal(afterHorizon.unverifiedHookRetries, 0);
      assert.deepEqual(immediate, afterHorizon, "loss is durable across reconciliation");
      assert.equal(fs.existsSync(artifact), false);
      const retry = writeHookSpoolEnvelope({ home, source: "claude_code", body: wire,
        cause: "maintenance_rebuild" });
      assert.equal(retry.ok, true, "retry repairs the identity without a JSON parse error");
      const final = fs.readdirSync(directory).filter((name) => name.endsWith(".receipt"));
      assert.equal(final.length, 1);
      assert.equal(JSON.parse(fs.readFileSync(path.join(directory, final[0]!), "utf8")).version, 7);
      if (seam !== "legacy") assert.match(artifact, /\.tmp$/, "private temp only, never an empty final filename");
      assert.equal(reconcileMaintenanceRebuildRefusals(home, ledger).count, 1,
        "a prior damaged instance must not retire the new retry receipt");
      if (seam === "legacy") {
        for (const malformed of ["{", "null"]) {
          fs.writeFileSync(path.join(directory, final[0]!), malformed);
          const directRetry = writeHookSpoolEnvelope({ home, source: "claude_code", body: wire,
            cause: "maintenance_rebuild" });
          assert.equal(directRetry.ok, true, "retry itself repairs malformed evidence without a prior reconciliation");
          assert.equal(reconcileMaintenanceRebuildRefusals(home, ledger).count, 1);
        }
      }
    } finally {
      if (child) await stop(child);
      try { writer?.exec("ROLLBACK"); } catch { /* Already ended. */ }
      writer?.close();
      fs.rmSync(home, { recursive: true, force: true });
    }
  }
}
