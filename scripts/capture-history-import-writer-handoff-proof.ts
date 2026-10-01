/** A waiting SQLite intake writer gets a turn while history import is slicing. */
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import type Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { deriveCaptureRootIdentity, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { applyCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { useFixtureRoot } from "./lib/fixture-root";

const ROWS_PER_FILE = 640;
const FILES = 2;
const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-history-handoff-")));
const fixture = useFixtureRoot(root, { home: path.join(root, "home"),
  plimsollHome: path.join(root, "plimsoll-home") });
fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });

async function main() {
  const directory = path.join(fixture.home, "profile", "sessions");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const files: string[] = [];
  for (let fileIndex = 0; fileIndex < FILES; fileIndex += 1) {
    const session = `019d0000-0000-7000-8000-${String(fileIndex + 1).padStart(12, "0")}`;
    const file = path.join(directory, `rollout-2026-01-02T00-00-00-${session}.jsonl`);
    const start = Date.parse("2026-01-02T00:00:00.000Z");
    const lines = [JSON.stringify({ type: "session_meta", payload: { id: session } }),
      JSON.stringify({ type: "event_msg", timestamp: new Date(start).toISOString(),
        payload: { type: "token_count", info: { total_token_usage: { input_tokens: 0, output_tokens: 0 } } } })];
    for (let index = 1; index <= ROWS_PER_FILE; index += 1) lines.push(JSON.stringify({
      type: "event_msg", timestamp: new Date(start + index * 60_000).toISOString(),
      payload: { type: "token_count", info: { total_token_usage: {
        input_tokens: index, output_tokens: index,
      } } },
    }));
    fs.writeFileSync(file, `${lines.join("\n")}\n`, { mode: 0o600 });
    files.push(file);
  }
  const ledger = path.join(fixture.env.PLIMSOLL_HOME, "ledger.sqlite");
  const buffer = new LocalEventBuffer(ledger, {
    workspaceId: "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10",
    deviceId: "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d",
  });
  let probe: ChildProcess | null = null;
  const slicePhases: Array<{
    slice: number; requestedAtMs: number; beginAcquiredAtMs?: number;
    rowLoopStartedAtMs?: number; rowLoopEndedAtMs?: number;
    commitStartedAtMs?: number; commitEndedAtMs?: number;
    handoffStartedAtMs?: number; handoffEndedAtMs?: number;
    walSampledAtMs?: number; walBytes?: number;
  }> = [];
  const traceStarted = performance.now();
  const at = () => performance.now() - traceStarted;
  const originalTransaction = buffer.database.transaction;
  const originalPrepare = buffer.database.prepare;
  const originalHandoffs = buffer.transactionWithRepoContextHandoffs;
  let activeSlice: typeof slicePhases[number] | null = null;
  // Observe the existing transaction boundaries without moving any work or
  // changing SQLite settings. The callback begins after BEGIN IMMEDIATE;
  // transaction return is after COMMIT; handoff return includes finalization.
  buffer.database.transaction = function <T extends (...args: any[]) => any>(this: Database.Database, work: T) {
    const transaction = originalTransaction.call(this, work) as Database.Transaction<T>;
    if (!activeSlice) return transaction;
    const immediate = transaction.immediate;
    const observedImmediate = function (this: unknown, ...args: Parameters<typeof immediate>): ReturnType<T> {
      const result = Reflect.apply(immediate, this, args) as ReturnType<T>;
      if (activeSlice) {
        activeSlice.commitEndedAtMs = at();
        activeSlice.handoffStartedAtMs = activeSlice.commitEndedAtMs;
      }
      return result;
    };
    // The ledger opener preserves the native read-only transaction variants.
    // Copy their descriptors onto a new callable; never mutate the original.
    const observed = function (this: unknown, ...args: Parameters<typeof transaction>): ReturnType<T> {
      return Reflect.apply(transaction, this, args) as ReturnType<T>;
    };
    Object.defineProperties(observed, {
      ...Object.getOwnPropertyDescriptors(transaction),
      immediate: { ...Object.getOwnPropertyDescriptor(transaction, "immediate"), value: observedImmediate },
    });
    return observed as Database.Transaction<T>;
  } as typeof originalTransaction;
  buffer.database.prepare = function (this: Database.Database, sql: string) {
    // The authority read is the first statement in each row iteration.
    if (activeSlice && sql.includes("select authority from session_usage_authority where source=? and session_id=?"))
      activeSlice.rowLoopStartedAtMs ??= at();
    // The progress receipt is written immediately after the row loop.
    if (activeSlice && sql.includes("update capture_history_import_runs set imported_rows="))
      activeSlice.rowLoopEndedAtMs = at();
    return originalPrepare.call(this, sql);
  } as typeof originalPrepare;
  buffer.transactionWithRepoContextHandoffs = function <T>(work: () => T): T {
    const phase: typeof slicePhases[number] = { slice: slicePhases.length + 1, requestedAtMs: at() };
    slicePhases.push(phase);
    activeSlice = phase;
    try {
      const result = originalHandoffs.call(this, () => {
        phase.beginAcquiredAtMs = at();
        const value = work();
        phase.commitStartedAtMs = at();
        return value;
      }) as T;
      phase.handoffEndedAtMs = at();
      // flush() measures elapsed immediately after this method returns.
      // Defer stat until its next await so filesystem timing stays outside
      // that writer measurement, before another import slice can start.
      queueMicrotask(() => {
        phase.walSampledAtMs = at();
        phase.walBytes = fs.statSync(`${ledger}-wal`).size;
      });
      return result;
    } finally {
      activeSlice = null;
    }
  };
  try {
    const epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
    const captureRoot: CaptureRoot = { ...deriveCaptureRootIdentity("fixture", "codex", directory),
      directory, source: "codex", installationEpochId: epoch };
    const baseline = beginAutomaticCaptureBaseline(buffer.database, "codex", {
      startedAt: "2026-01-01T00:00:00.000Z", filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(buffer.database, "codex", {
      runId: baseline.latestRun!.runId, completedAt: "2026-01-01T00:00:00.000Z" });
    sealCaptureBaselineGenerations(buffer.database, "codex", files.map(file => {
      const stat = fs.statSync(file, { bigint: true });
      return { path: file, device: stat.dev, inode: stat.ino, size: stat.size,
        birthtimeNs: stat.birthtimeNs };
    }), "2026-01-03T00:00:00.000Z");
    buffer.database.exec("create table history_handoff_probe (at text not null)");
    let delayedRows = 0;
    buffer.database.function("history_handoff_delay", () => {
      // The first slice starts with four rows. Three 44 ms rows cross the
      // 120 ms deadline while a fourth remains, independent of host speed.
      // Later rows retain the steady load used to probe writer handoff.
      delayedRows += 1;
      const until = performance.now() + (delayedRows <= 3 ? 44 : 16);
      while (performance.now() < until) { /* deterministic one-row writer cost */ }
    });
    buffer.database.exec(`create trigger history_handoff_pressure before insert on buffered_events
      when new.event_type='usage_rollout' begin select history_handoff_delay(); end`);
    const childCode = `const DB=require('better-sqlite3');const {performance}=require('node:perf_hooks');
      const db=new DB(process.env.HANDOFF_LEDGER,{timeout:2000});db.pragma('wal_autocheckpoint=0');
      const stmt=db.prepare('insert into history_handoff_probe(at) values (?)');
      let count=0,errors=0,maxBeginMs=0;const samples=[];
      const timer=setInterval(()=>{const started=performance.now();try{
        db.exec('BEGIN IMMEDIATE');const beginMs=performance.now()-started;
        stmt.run(new Date().toISOString());db.exec('COMMIT');count++;
        maxBeginMs=Math.max(maxBeginMs,beginMs);samples.push(beginMs);
      }catch(e){errors++;if(db.inTransaction)db.exec('ROLLBACK');}},25);
      console.log('READY');process.on('SIGTERM',()=>{clearInterval(timer);db.close();
        samples.sort((a,b)=>a-b);console.log(JSON.stringify({count,errors,maxBeginMs,
          p99BeginMs:samples[Math.ceil(samples.length*.99)-1]??0}));process.exit(0)});`;
    probe = spawn(process.execPath, ["-e", childCode], { cwd: path.resolve(import.meta.dirname, ".."),
      env: { ...process.env, ...fixture.env, HANDOFF_LEDGER: ledger },
      stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    probe.stdout!.on("data", chunk => output += chunk.toString());
    probe.stderr!.on("data", chunk => output += chunk.toString());
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("handoff_probe_start_timeout")), 5000);
      const ready = (_chunk: Buffer) => {
        if (!output.includes("READY\n")) return;
        clearTimeout(timer); probe!.stdout!.off("data", ready); resolve();
      };
      probe!.stdout!.on("data", ready);
      probe!.once("error", reject);
    });
    const receipt = await applyCaptureHistory(buffer, captureRoot);
    probe.kill("SIGTERM");
    await new Promise<void>(resolve => probe!.once("close", () => resolve()));
    probe = null;
    const result = JSON.parse(output.trim().split("\n").at(-1)!) as {
      count: number; errors: number; maxBeginMs: number; p99BeginMs: number;
    };
    const rows = (buffer.database.prepare(`select count(*) as n from buffered_events
      where event_type='usage_rollout'`).get() as { n: number }).n;
    const projected = (buffer.database.prepare(`select count(*) as n from dashboard_event_facts
      where event_type='usage_rollout'`).get() as { n: number }).n;
    const walBytes = fs.statSync(`${ledger}-wal`).size;
    console.log(JSON.stringify({ proof: "history_writer_handoff", importedRows: receipt.importedRows,
      writerSlices: receipt.writerSlices, maxWriterSliceMs: receipt.maxWriterSliceMs,
      intake: result, walBytes }));
    assert.equal(receipt.importedRows, ROWS_PER_FILE * FILES);
    assert.equal(rows, ROWS_PER_FILE * FILES);
    assert.equal(projected, ROWS_PER_FILE * FILES);
    assert.ok(receipt.timeBudgetStops > 0);
    assert.ok(result.count >= 20, `intake probe made only ${result.count} writes`);
    assert.equal(result.errors, 0, "intake writer timed out");
    assert.ok(receipt.maxWriterSliceMs < 250, `writer slice ${receipt.maxWriterSliceMs} ms`);
    assert.ok(result.maxBeginMs < 750, `intake waited ${result.maxBeginMs} ms for writer`);
    console.log(JSON.stringify({ checks: 8, importedRows: receipt.importedRows,
      writerSlices: receipt.writerSlices, maxWriterSliceMs: receipt.maxWriterSliceMs,
      intake: result }));
  } catch (error) {
    console.error(JSON.stringify({ proof: "history_writer_handoff", phaseClock: "ms_since_trace_start",
      slicePhases }));
    throw error;
  } finally {
    if (probe && probe.exitCode === null && probe.signalCode === null) {
      probe.kill("SIGTERM");
      await new Promise<void>(resolve => probe!.once("close", () => resolve()));
    }
    buffer.database.transaction = originalTransaction;
    buffer.database.prepare = originalPrepare;
    buffer.transactionWithRepoContextHandoffs = originalHandoffs;
    buffer.close(); fixture.restore(); fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
