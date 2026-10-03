import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { DASHBOARD_WINDOWS, DashboardProjectionStore } from
  "../packages/collector-cli/src/dashboard-projection";
import { connectionOwnershipClosed, observeRebuildConnectionOwnership,
  readActiveRebuildWriterLeases, rebuildLedger } from "../packages/collector-cli/src/maintenance-rebuild";

function fixture(ledger: string) {
  const buffer = new LocalEventBuffer(ledger);
  try {
    const now = new Date();
    for (const days of DASHBOARD_WINDOWS) {
      const snapshot = DashboardProjectionStore.regenerateForRebuildVerification(
        buffer.database, days, 1, now) as { window: { since: string } };
      buffer.database.prepare(`insert into dashboard_snapshots
        (days,schema_version,generation,since_at,payload_json,created_at) values (?,?,?,?,?,?)
        on conflict(days) do update set schema_version=excluded.schema_version,
        generation=excluded.generation,since_at=excluded.since_at,
        payload_json=excluded.payload_json,created_at=excluded.created_at`)
        .run(days, 2, 1, snapshot.window.since, JSON.stringify(snapshot), now.toISOString());
    }
  } finally { buffer.close(); }
}

function startWriter(ledger: string) {
  const script = path.resolve("scripts/maintenance-rebuild-r4-dead-writer-child.ts");
  const child = spawn(process.execPath, ["--import", "tsx", script, ledger], {
    cwd: process.cwd(), env: process.env, stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  let stderr = "";
  child.stderr?.on("data", (chunk) => { stderr += String(chunk); });
  const ready = new Promise<number>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`writer_child_ready_timeout:${stderr}`)), 30_000);
    child.once("message", (message: unknown) => {
      clearTimeout(timer);
      if (message && typeof message === "object" && (message as { ready?: boolean }).ready === true)
        resolve((message as { pid: number }).pid);
      else reject(new Error(`writer_child_unexpected_message:${JSON.stringify(message)}`));
    });
    child.once("exit", (code) => { clearTimeout(timer); reject(new Error(`writer_child_exited:${code}:${stderr}`)); });
  });
  return { child, ready };
}

async function killWriter(child: ChildProcess) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  child.kill("SIGKILL");
  await exited;
}

async function main() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "b13-r4-lease-")));
  const ledger = path.join(root, "ledger.sqlite");
  let child: ChildProcess | null = null;
  try {
    fixture(ledger);
    const quiesce = async () => {
      const before = observeRebuildConnectionOwnership(ledger);
      const after = observeRebuildConnectionOwnership(ledger);
      return { before, after, connectionsClosed: connectionOwnershipClosed(after) };
    };
    const options = { ledgerPath: ledger, stage: "S10" as const, walHighWaterBytes: 0,
      copyDrill: true, quiesce, resume: async () => undefined };
    const live = new LocalEventBuffer(ledger);
    try {
      await assert.rejects(rebuildLedger(options), /writer_not_quiesced/,
        "a live writer is never retired or bypassed");
      assert.equal(readActiveRebuildWriterLeases(live.database).length, 1);
      assert.equal(observeRebuildConnectionOwnership(ledger).openTokens.length, 1);
      console.log(JSON.stringify({ check: "r4_live_writer_control_refused", leaseRetained: true,
        tokenRetained: true }));
    } finally { live.close(); }
    const started = startWriter(ledger);
    child = started.child;
    const pid = await started.ready;
    await killWriter(child);
    assert.equal(observeRebuildConnectionOwnership(ledger).openTokens.length, 1,
      "SIGKILL leaves a durable opener token");
    const read = new Database(ledger, { readonly: true, fileMustExist: true });
    let stale: ReturnType<typeof readActiveRebuildWriterLeases>;
    let durableIdentity: { pid: number; processStartFingerprint: string;
      processStartFingerprintAlgorithm: string };
    try {
      stale = readActiveRebuildWriterLeases(read);
      const row = read.prepare(`select value from maintenance_state where key like 'rebuild_writer_lease:%'`)
        .get() as { value: string };
      durableIdentity = JSON.parse(row.value);
    }
    finally { read.close(); }
    assert.equal(stale.length, 1, "SIGKILL leaves a durable database writer lease");
    assert.equal(durableIdentity.pid, pid);
    assert.match(durableIdentity.processStartFingerprint, /^sha256:[0-9a-f]{64}$/);
    assert.equal(durableIdentity.processStartFingerprintAlgorithm, "plimsoll-ps-lstart-utc-v2");
    const tokenName = observeRebuildConnectionOwnership(ledger).openTokens[0]!.token;
    const tokenIdentity = JSON.parse(fs.readFileSync(path.join(`${ledger}.rebuild-open-leases`, tokenName),
      "utf8")) as { pid: number; processStartFingerprint: string };
    assert.equal(tokenIdentity.pid, pid);
    assert.equal(tokenIdentity.processStartFingerprint, durableIdentity.processStartFingerprint);
    const rebuilt = await rebuildLedger(options);
    assert.equal(rebuilt.status, "rebuilt");
    assert.equal(connectionOwnershipClosed(rebuilt.quiesce.fencedOwnership), true);
    assert.equal(connectionOwnershipClosed(observeRebuildConnectionOwnership(ledger)), true);
    console.log(JSON.stringify({ check: "r4_dead_writer_lease_reaped_then_rebuilt",
      killedPid: pid, staleLeaseCount: stale.length, result: rebuilt.status,
      fencedOwnership: rebuilt.quiesce.fencedOwnership }));
  } finally {
    if (child) await killWriter(child);
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
