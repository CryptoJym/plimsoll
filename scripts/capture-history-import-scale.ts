/** Synthetic busy-host scale receipt. Run only under the lane's isolated CI home. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { deriveCaptureRootIdentity, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { applyCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { useFixtureRoot } from "./lib/fixture-root";

const REPO = path.resolve(import.meta.dirname, "..");
const ROOTS = 23;
const USAGE_PER_ROOT = 4348;
const PADDING_PER_USAGE = 9;
const USAGE_ROWS = ROOTS * USAGE_PER_ROOT;
const HISTORY_ROWS = ROOTS * (2 + USAGE_PER_ROOT * (1 + PADDING_PER_USAGE));
const FENCE = "2026-01-03T00:00:00.000Z";
const START = Date.parse("2025-01-01T00:00:00.000Z");
const FILL = "x".repeat(2250);
const fixtureDir = fs.realpathSync(fs.mkdtempSync(path.join(REPO, "..", "scale-fixture-")));
const fixture = useFixtureRoot(fixtureDir, {
  home: path.join(fixtureDir, "home"), plimsollHome: path.join(fixtureDir, "plimsoll-home"),
});
fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });

async function writeRoot(file: string, session: string, rows: number) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const stream = fs.createWriteStream(file, { mode: 0o600 });
  const write = async (line: string) => {
    if (!stream.write(`${line}\n`)) await new Promise<void>(resolve => stream.once("drain", resolve));
  };
  await write(JSON.stringify({ type: "session_meta", timestamp: new Date(START).toISOString(),
    payload: { id: session } }));
  await write(JSON.stringify({ type: "event_msg", timestamp: new Date(START + 1000).toISOString(),
    payload: { type: "token_count", info: { total_token_usage: { input_tokens: 0, output_tokens: 0 } } } }));
  for (let index = 0; index < rows; index += 1) {
    const at = new Date(START + (index + 2) * 60_000).toISOString();
    await write(JSON.stringify({ type: "event_msg", timestamp: at,
      payload: { type: "token_count", info: { total_token_usage: {
        input_tokens: index + 1, output_tokens: index + 1,
      } } } }));
    for (let filler = 0; filler < PADDING_PER_USAGE; filler += 1)
      await write(JSON.stringify({ type: "message", timestamp: at, payload: { content: FILL } }));
  }
  await new Promise<void>((resolve, reject) => {
    stream.once("error", reject);
    stream.end(resolve);
  });
}

async function main() {
  const ledger = path.join(fixture.env.PLIMSOLL_HOME, "work-ledger.sqlite");
  const buffer = new LocalEventBuffer(ledger, { workspaceId: "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10",
    deviceId: "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d" });
  const started = performance.now();
  const roots: CaptureRoot[] = [];
  const files: string[] = [];
  let sourceBytes = 0;
  let totalRows = 0;
  let maxWriterSliceMs = 0;
  let maxWriterWorkMs = 0;
  let maxWriterRowMs = 0;
  let overBudgetSlices = 0;
  let writerSlices = 0;
  let timeBudgetStops = 0;
  let injectedDeadlineRows = 0;
  const writerSliceHistogram: Record<string, number> = {};
  let peakRss = process.memoryUsage().rss;
  let probe: ReturnType<typeof spawn> | null = null;
  try {
    const epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
    for (let index = 0; index < ROOTS; index += 1) {
      const rows = USAGE_PER_ROOT;
      const session = `019d0000-0000-7000-8000-${String(index + 1).padStart(12, "0")}`;
      const directory = path.join(fixture.home, `profile-${index + 1}`, "sessions");
      const file = path.join(directory, `rollout-2025-01-01T00-00-00-${session}.jsonl`);
      await writeRoot(file, session, rows);
      sourceBytes += fs.statSync(file).size;
      totalRows += rows;
      files.push(file);
      roots.push({ ...deriveCaptureRootIdentity("fixture", "codex", directory),
        directory, source: "codex", installationEpochId: epoch });
      peakRss = Math.max(peakRss, process.memoryUsage().rss);
    }
    assert.equal(totalRows, USAGE_ROWS);
    assert.ok(HISTORY_ROWS >= 1_000_000);
    assert.ok(sourceBytes >= 2_000_000_000, `history bytes ${sourceBytes}`);
    fs.writeFileSync(path.join(fixture.env.PLIMSOLL_HOME, "collector.config.json"),
      `${JSON.stringify(collectorConfigSchema.parse({ tenantId: "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10",
        deviceId: "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d", installKey: "fixture-scale",
        captureRoots: roots }))}\n`, { mode: 0o600 });
    const begun = beginAutomaticCaptureBaseline(buffer.database, "codex", {
      startedAt: "2024-12-31T00:00:00.000Z", filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(buffer.database, "codex", {
      runId: begun.latestRun!.runId, completedAt: "2024-12-31T00:00:00.000Z" });
    sealCaptureBaselineGenerations(buffer.database, "codex", files.map(file => {
      const stat = fs.statSync(file, { bigint: true });
      return { path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs };
    }), FENCE);
    // Fixture-only pressure makes the first four-row slice cross 120 ms
    // after three rows. The gate then proves the deadline path even on a
    // fast, normally scheduled host without weakening the 250 ms limit.
    buffer.database.function("history_scale_deadline_delay", () => {
      if (injectedDeadlineRows >= 3) return;
      injectedDeadlineRows += 1;
      const until = performance.now() + 44;
      while (performance.now() < until) { /* deterministic fixture row cost */ }
    });
    buffer.database.exec(`create trigger history_scale_deadline_pressure
      before insert on buffered_events when new.event_type='usage_rollout'
      begin select history_scale_deadline_delay(); end`);
    buffer.database.exec(`create table if not exists history_import_probe (at text not null)`);
    const code = `const DB=require('better-sqlite3');const {performance}=require('node:perf_hooks');
      const db=new DB(process.env.PROBE_LEDGER,{timeout:2000});db.pragma('wal_autocheckpoint=0');
      const stmt=db.prepare('insert into history_import_probe(at) values (?)');
      let count=0,max=0,errors=0;const times=[];const timer=setInterval(()=>{const t=performance.now();
        try{stmt.run(new Date().toISOString());count++;const ms=performance.now()-t;times.push(ms);max=Math.max(max,ms)}
        catch{errors++}},50);process.on('SIGTERM',()=>{clearInterval(timer);db.close();
        times.sort((a,b)=>a-b);console.log(JSON.stringify({count,maxMs:max,p99Ms:times[Math.ceil(times.length*.99)-1]??0,errors}));process.exit(0)});`;
    probe = spawn(process.execPath, ["-e", code], {
      cwd: REPO, env: { ...process.env, ...fixture.env, PROBE_LEDGER: ledger },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let probeOutput = "";
    probe.stdout!.on("data", (chunk: Buffer) => { probeOutput += chunk.toString(); });
    probe.stderr!.on("data", (chunk: Buffer) => { probeOutput += chunk.toString(); });
    const importStarted = performance.now();
    let imported = 0;
    for (let index = 0; index < roots.length; index += 1) {
      const receipt = await applyCaptureHistory(buffer, roots[index]!);
      if (index === 0) buffer.database.exec("drop trigger history_scale_deadline_pressure");
      imported += receipt.importedRows;
      maxWriterSliceMs = Math.max(maxWriterSliceMs, receipt.maxWriterSliceMs);
      maxWriterWorkMs = Math.max(maxWriterWorkMs, receipt.maxWriterWorkMs);
      maxWriterRowMs = Math.max(maxWriterRowMs, receipt.maxWriterRowMs);
      overBudgetSlices += receipt.overBudgetSlices;
      writerSlices += receipt.writerSlices;
      timeBudgetStops += receipt.timeBudgetStops;
      for (const [bucket, count] of Object.entries(receipt.writerSliceHistogram))
        writerSliceHistogram[bucket] = (writerSliceHistogram[bucket] ?? 0) + count;
      peakRss = Math.max(peakRss, process.memoryUsage().rss);
      console.error(JSON.stringify({ root: index + 1, importedRows: receipt.importedRows,
        elapsedSeconds: Math.round((performance.now() - importStarted) / 1000),
        maxWriterSliceMs: receipt.maxWriterSliceMs, maxWriterWorkMs: receipt.maxWriterWorkMs,
        maxWriterRowMs: receipt.maxWriterRowMs, overBudgetSlices: receipt.overBudgetSlices,
        writerSlices: receipt.writerSlices, timeBudgetStops: receipt.timeBudgetStops }));
    }
    const importSeconds = (performance.now() - importStarted) / 1000;
    probe.kill("SIGTERM");
    await new Promise<void>(resolve => probe!.once("close", () => resolve()));
    probe = null;
    const probeReceipt = JSON.parse(probeOutput.trim()) as { count: number; maxMs: number; p99Ms: number; errors: number };
    const ledgerRows = (buffer.database.prepare(`select count(*) as n from buffered_events
      where event_type='usage_rollout'`).get() as { n: number }).n;
    const projected = buffer.database.prepare(`select count(*) as rows,sum(input_tokens) as input
      from dashboard_event_facts where event_type='usage_rollout'`).get() as
      { rows: number; input: number };
    const probeRows = (buffer.database.prepare(`select count(*) as n from history_import_probe`).get() as { n: number }).n;
    console.log(JSON.stringify({ schema: "capture_history_scale_v1", roots: ROOTS,
      historyRows: HISTORY_ROWS, usageRows: USAGE_ROWS,
      sourceBytes, importSeconds, totalSeconds: (performance.now() - started) / 1000,
      peakRssBytes: Math.max(peakRss, process.resourceUsage().maxRSS * 1024),
      maxWriterSliceMs, maxWriterWorkMs, maxWriterRowMs, overBudgetSlices,
      writerSlices, timeBudgetStops, injectedDeadlineRows, writerSliceHistogram,
      intake: probeReceipt, ledgerRows, projected }, null, 2));
    assert.equal(imported, USAGE_ROWS);
    assert.equal(ledgerRows, USAGE_ROWS);
    assert.deepEqual(projected, { rows: USAGE_ROWS, input: USAGE_ROWS });
    assert.equal(probeRows, probeReceipt.count);
    assert.equal(Object.values(writerSliceHistogram).reduce((sum, count) => sum + count, 0), writerSlices);
    assert.equal(injectedDeadlineRows, 3);
    // A million-row host must exercise the deadline, not merely the row cap.
    // Removing the deadline makes this scale proof fail even on a fast host.
    assert.ok(timeBudgetStops > 0, "writer deadline was not exercised");
    assert.equal(probeReceipt.errors, 0);
    assert.ok(maxWriterSliceMs < 750 && overBudgetSlices === 0);
    assert.ok(probeReceipt.maxMs < 750);
  } finally {
    if (probe) { probe.kill("SIGTERM"); await new Promise<void>(resolve => probe!.once("close", () => resolve())); }
    buffer.close();
    fixture.restore();
    fs.rmSync(fixtureDir, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
