/** Synthetic WAL checkpoint stress: compare a free WAL with a pinned reader. */
import assert from "node:assert/strict";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { applyCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { deriveCaptureRootIdentity, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { useFixtureRoot } from "./lib/fixture-root";

const repo = path.resolve(import.meta.dirname, "..");
const require = createRequire(path.join(repo, "package.json"));
const Database = require("better-sqlite3") as typeof import("better-sqlite3");
const SESSION = "019d0000-0000-7000-8000-000000000202";

async function run(pinned: boolean, gate = false) {
  const scratch = fs.realpathSync(fs.mkdtempSync(path.join(path.resolve(process.cwd(), ".."), "pr431-wal-")));
  const fixture = useFixtureRoot(scratch, { home: path.join(scratch, "home"),
    plimsollHome: path.join(scratch, "plimsoll-home") });
  fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
  fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });
  const directory = path.join(fixture.home, "profile", "sessions");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = path.join(directory, `rollout-2026-01-02T00-00-00-${SESSION}.jsonl`);
  const lines = [JSON.stringify({ type: "session_meta", payload: { id: SESSION } }),
    JSON.stringify({ type: "event_msg", timestamp: "2026-01-02T00:00:00.000Z", payload: {
      type: "token_count", info: { total_token_usage: { input_tokens: 0, output_tokens: 0 } } } })];
  for (let i = 1; i <= 640; i++) lines.push(JSON.stringify({ type: "event_msg",
    timestamp: new Date(Date.parse("2026-01-02T00:00:00.000Z") + i * 60_000).toISOString(),
    payload: { type: "token_count", info: { total_token_usage: {
      input_tokens: i, output_tokens: i } } },
  }));
  fs.writeFileSync(file, `${lines.join("\n")}\n`, { mode: 0o600 });
  const ledger = path.join(fixture.env.PLIMSOLL_HOME, "ledger.sqlite");
  const buffer = new LocalEventBuffer(ledger, { workspaceId: "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10",
    deviceId: "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d" });
  let reader: InstanceType<typeof Database> | null = null;
  try {
    const root: CaptureRoot = { ...deriveCaptureRootIdentity("review", "codex", directory),
      source: "codex", directory, installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! };
    const baseline = beginAutomaticCaptureBaseline(buffer.database, "codex", {
      startedAt: "2026-01-01T00:00:00.000Z", filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(buffer.database, "codex", {
      runId: baseline.latestRun!.runId, completedAt: "2026-01-01T00:00:00.000Z" });
    const stat = fs.statSync(file, { bigint: true });
    sealCaptureBaselineGenerations(buffer.database, "codex", [{ path: file,
      device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs }],
    "2026-01-03T00:00:00.000Z");
    buffer.database.function("review_slow_insert", () => {
      const until = performance.now() + 5;
      while (performance.now() < until) { /* synthetic writer work */ }
    });
    buffer.database.exec(`create trigger review_slow_insert before insert on buffered_events
      when new.event_type='usage_rollout' begin select review_slow_insert(); end`);
    if (pinned) {
      reader = new Database(ledger, { readonly: true });
      reader.exec("BEGIN");
      reader.prepare("select count(*) from buffered_events").get();
    }
    let refusal = "none";
    let receipt: Awaited<ReturnType<typeof applyCaptureHistory>> | null = null;
    try { receipt = await applyCaptureHistory(buffer, root,
      gate ? { walLimitBytes: 8 * 1024 * 1024, walStallMs: 500 } : {}); }
    catch (error) { refusal = String(error); }
    const rowsBeforeResume = (buffer.database.prepare(`select count(*) as n from buffered_events
      where event_type='usage_rollout'`).get() as { n: number }).n;
    if (gate && pinned) {
      assert.ok(refusal.includes("wal_checkpoint_stalled"),
        "a reader pin must stop an over-limit WAL resumably");
      reader!.exec("ROLLBACK"); reader!.close(); reader = null;
      receipt = await applyCaptureHistory(buffer, root,
        { walLimitBytes: 8 * 1024 * 1024, walStallMs: 500 });
    }
    if (!receipt) throw new Error(refusal);
    const walBytes = fs.statSync(`${ledger}-wal`).size;
    const checkpoint = buffer.database.pragma("wal_checkpoint(PASSIVE)");
    const totalRows = (buffer.database.prepare(`select count(*) as n from buffered_events
      where event_type='usage_rollout'`).get() as { n: number }).n;
    return { pinned, gate, importedRows: receipt.importedRows, slices: receipt.writerSlices,
      rowsBeforeResume, totalRows, refusal, walBytes, checkpoint };
  } finally {
    if (reader) { reader.exec("ROLLBACK"); reader.close(); }
    buffer.close(); fixture.restore(); fs.rmSync(scratch, { recursive: true, force: true });
  }
}

async function main() {
  const gate = process.argv[2] === "gate";
  const unpinned = await run(false, gate);
  const pinned = await run(true, gate);
  console.log(JSON.stringify({ case: "passive_checkpoint_pinned_reader", unpinned, pinned,
    walRatio: Number((pinned.walBytes / Math.max(unpinned.walBytes, 1)).toFixed(2)) }));
  assert.equal(unpinned.totalRows, 640);
  assert.equal(pinned.totalRows, 640);
  if (gate) {
    assert.ok(pinned.rowsBeforeResume > 0 && pinned.rowsBeforeResume < 640);
    assert.ok(pinned.refusal.includes("wal_checkpoint_stalled"));
    assert.ok(pinned.walBytes <= 8 * 1024 * 1024);
  } else {
    assert.ok(unpinned.slices >= 8 && pinned.slices >= 8);
    assert.ok(pinned.walBytes > unpinned.walBytes,
      "a reader pin should expose WAL retention despite PASSIVE checkpoints");
  }
}

void main();
