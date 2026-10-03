/** The paused listener waits until sequence schema publication has committed. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { DEFAULT_POLICY } from "../packages/shared/src/index";
import { writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";
import { finishMaintenanceRebuildPause, markMaintenanceRebuildPause,
  MISSING_HOOK_RETRY_MS, recordMaintenanceRebuildRefusal,
  reconcileMaintenanceRebuildRefusals } from "../packages/collector-cli/src/maintenance-rebuild-pause-state";

const script = path.resolve("scripts/maintenance-rebuild-r16-listener-wait-proof.ts");
const mode = process.argv[2];
if (mode === "upgrade-child") {
  const [ledger, ready, release] = process.argv.slice(3);
  const prototype = Database.prototype as typeof Database.prototype & { exec(sql: string): Database.Database };
  const original = prototype.exec;
  let paused = false;
  prototype.exec = function(sql: string) {
    const result = original.call(this, sql);
    if (!paused && sql.includes("create table if not exists maintenance_rebuild_event_order")) {
      paused = true;
      fs.writeFileSync(ready!, "table statement returned\n");
      const deadline = Date.now() + 30_000;
      const sleep = new Int32Array(new SharedArrayBuffer(4));
      while (!fs.existsSync(release!) && Date.now() < deadline) Atomics.wait(sleep, 0, 0, 10);
      if (!fs.existsSync(release!)) throw new Error("schema_release_timeout");
    }
    return result;
  };
  const buffer = new LocalEventBuffer(ledger!);
  buffer.close();
  console.log(JSON.stringify({ check: "schema_child_committed", paused }));
} else if (mode === "listener-child") {
  const [home, wire, spoolName, started, done] = process.argv.slice(3);
  fs.writeFileSync(started!, "listener entering pause mark\n");
  markMaintenanceRebuildPause(home!);
  recordMaintenanceRebuildRefusal(home!, "hook", "claude_code", wire!, { spoolName });
  fs.writeFileSync(done!, "receipt durable\n");
  console.log(JSON.stringify({ check: "listener_boundary_durable" }));
} else {
  void runParent().catch((error) => { console.error(error); process.exitCode = 1; });
}

async function runParent() {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r16-listener-wait-")));
  const ledger = path.join(home, "ledger.sqlite");
  const ready = path.join(home, "schema-ready");
  const release = path.join(home, "schema-release");
  const started = path.join(home, "listener-started");
  const done = path.join(home, "listener-done");
  const body = { id: randomUUID(), session_id: randomUUID(), hook_event_name: "UserPromptSubmit",
    timestamp: new Date().toISOString(), input_tokens: 9 };
  const wire = JSON.stringify(body);
  type Child = ReturnType<typeof spawn>;
  const children: Child[] = [];
  const outputs = new Map<Child, string>();
  const start = (args: string[]) => {
    const child = spawn(process.execPath, ["--import", "tsx", script, ...args],
      { cwd: process.cwd(), env: process.env, stdio: ["ignore", "pipe", "pipe"] });
    children.push(child);
    outputs.set(child, "");
    for (const stream of [child.stdout, child.stderr]) stream.on("data", (data: Buffer) =>
      outputs.set(child, (outputs.get(child) ?? "") + data.toString()));
    return child;
  };
  const waitFile = async (file: string, child: Child) => {
    const deadline = Date.now() + 30_000;
    while (!fs.existsSync(file)) {
      if (child.exitCode !== null || child.signalCode !== null)
        throw new Error(`child_exited_before_${path.basename(file)}: ${outputs.get(child)}`);
      if (Date.now() >= deadline) throw new Error(`timeout_waiting_for_${path.basename(file)}`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  };
  const exit = (child: Child) => new Promise<void>((resolve, reject) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      return child.exitCode === 0 ? resolve() : reject(new Error(outputs.get(child)));
    }
    child.once("exit", (code, signal) => code === 0 ? resolve() :
      reject(new Error(`child_exit_${code ?? signal}: ${outputs.get(child)}`)));
  });
  try {
    const initial = new LocalEventBuffer(ledger);
    initial.database.exec(`drop trigger trg_maintenance_rebuild_event_order_insert;
      drop trigger trg_maintenance_rebuild_event_order_delete;
      drop trigger trg_maintenance_rebuild_event_order_rekey;
      drop table maintenance_rebuild_event_order`);
    initial.close();
    const saved = writeHookSpoolEnvelope({ home, source: "claude_code", body: wire,
      cause: "maintenance_rebuild" });
    assert.equal(saved.ok, true);
    const upgrade = start(["upgrade-child", ledger, ready, release]);
    await waitFile(ready, upgrade);
    const observer = new Database(ledger, { readonly: true, fileMustExist: true });
    const partialVisible = !!observer.prepare(`select 1 from sqlite_master
      where type='table' and name='maintenance_rebuild_event_order'`).get();
    observer.close();
    assert.equal(partialVisible, false, "a listener cannot see a partial schema commit");
    const listener = start(["listener-child", home, wire, path.basename(saved.path), started, done]);
    await waitFile(started, listener);
    // The installer is deliberately held at the old DDL gap. Its transaction
    // must prevent the listener from publishing a receipt there.
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(fs.existsSync(done), false, "receipt must await schema commit");
    fs.writeFileSync(release, "commit schema\n");
    await Promise.all([exit(upgrade), exit(listener)]);
    const receipt = path.join(home, "maintenance-rebuild-refusals",
      fs.readdirSync(path.join(home, "maintenance-rebuild-refusals"))[0]!);
    const boundary = JSON.parse(fs.readFileSync(receipt, "utf8")) as
      { at: string; ledgerAdmissionSequence: number; sessionId: string };
    assert.equal(boundary.ledgerAdmissionSequence, 0);
    const older = new Database(ledger);
    older.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,session_id)
      values (?,'claude_code','user_prompt_submit','safe',?,?,?,?)`)
      .run(body.id, body.timestamp, JSON.stringify({ tenantId: DEFAULT_POLICY.tenantId }),
        body.timestamp, body.session_id);
    const row = older.prepare(`select seq from maintenance_rebuild_event_order where event_id=?`)
      .get(body.id) as { seq: number };
    older.close();
    fs.unlinkSync(saved.path);
    finishMaintenanceRebuildPause(home);
    const state = reconcileMaintenanceRebuildRefusals(home, ledger,
      Date.parse(boundary.at) + MISSING_HOOK_RETRY_MS + 1);
    console.log(JSON.stringify({ check: "listener_waits_for_atomic_schema", partialVisible,
      receiptBeforeCommit: false, boundary: boundary.ledgerAdmissionSequence,
      oldWriterSequence: row.seq, state }));
    assert.ok(row.seq > boundary.ledgerAdmissionSequence);
    assert.equal(state.count, 0);
    assert.equal(state.unverifiedHookRetries, 1);
  } finally {
    for (const child of children) if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    await Promise.all(children.map((child) => new Promise<void>((resolve) => {
      if (child.exitCode !== null || child.signalCode !== null) return resolve();
      const timer = setTimeout(() => child.kill("SIGKILL"), 1_000);
      child.once("exit", () => { clearTimeout(timer); resolve(); });
    })));
    fs.rmSync(home, { recursive: true, force: true });
  }
}
