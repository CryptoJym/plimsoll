/** Synthetic writer-deadline and durable-cursor proof for history import. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { deriveCaptureRootIdentity, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { applyCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { useFixtureRoot } from "./lib/fixture-root";

const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-history-slices-")));
const fixture = useFixtureRoot(root, { home: path.join(root, "home"),
  plimsollHome: path.join(root, "plimsoll-home") });
fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });

async function main() {
  const session = "019d0000-0000-7000-8000-000000000081";
  const directory = path.join(fixture.home, "profile", "sessions");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = path.join(directory, `rollout-2026-01-02T00-00-00-${session}.jsonl`);
  const start = Date.parse("2026-01-02T00:00:00.000Z");
  const lines = [JSON.stringify({ type: "session_meta", payload: { id: session } }),
    JSON.stringify({ type: "event_msg", timestamp: new Date(start).toISOString(),
      payload: { type: "token_count", info: { total_token_usage: { input_tokens: 0, output_tokens: 0 } } } })];
  for (let index = 1; index <= 512; index += 1) lines.push(JSON.stringify({
    type: "event_msg", timestamp: new Date(start + index * 60_000).toISOString(),
    payload: { type: "token_count", info: { total_token_usage: {
      input_tokens: index, output_tokens: index,
    } } },
  }));
  fs.writeFileSync(file, `${lines.join("\n")}\n`, { mode: 0o600 });
  const buffer = new LocalEventBuffer(path.join(fixture.env.PLIMSOLL_HOME, "ledger.sqlite"), {
    workspaceId: "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10",
    deviceId: "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d",
  });
  try {
    const epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
    const captureRoot: CaptureRoot = { ...deriveCaptureRootIdentity("fixture", "codex", directory),
      directory, source: "codex", installationEpochId: epoch };
    const baseline = beginAutomaticCaptureBaseline(buffer.database, "codex", {
      startedAt: "2026-01-01T00:00:00.000Z", filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(buffer.database, "codex", {
      runId: baseline.latestRun!.runId, completedAt: "2026-01-01T00:00:00.000Z" });
    const stat = fs.statSync(file, { bigint: true });
    sealCaptureBaselineGenerations(buffer.database, "codex", [{ path: file, device: stat.dev,
      inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs }], "2026-01-03T00:00:00.000Z");
    let admitted = 0;
    buffer.database.function("import_slice_delay", () => {
      admitted += 1;
      if (admitted <= 128) return;
      const until = performance.now() + 20;
      while (performance.now() < until) { /* deterministic fixture pressure */ }
    });
    buffer.database.exec(`create trigger history_slice_pressure before insert on buffered_events
      when new.event_type='usage_rollout' begin select import_slice_delay(); end`);
    await assert.rejects(applyCaptureHistory(buffer, captureRoot, { stopAfterSlices: 1 }),
      /capture_history_injected_crash/);
    const cursor = buffer.database.prepare(`select resume_candidate_index as position,
      resume_candidate_digest as digest from capture_history_import_runs where root_id=?`)
      .get(captureRoot.rootId) as { position: number; digest: string };
    assert.equal(cursor.position, 8);
    assert.match(cursor.digest, /^[0-9a-f]{64}$/);
    // The simulated process died after commit. Preserve the committed row and
    // make the lock stale exactly as a new process would observe it.
    buffer.database.prepare(`update capture_history_import_lock set owner_pid=999999 where singleton=1`).run();
    buffer.database.prepare(`update capture_history_import_runs set resume_candidate_digest=? where root_id=?`)
      .run("0".repeat(64), captureRoot.rootId);
    await assert.rejects(applyCaptureHistory(buffer, captureRoot), /resume_cursor_digest_changed/);
    buffer.database.prepare(`update capture_history_import_runs set resume_candidate_digest=? where root_id=?`)
      .run(cursor.digest, captureRoot.rootId);
    admitted = 0;
    const receipt = await applyCaptureHistory(buffer, captureRoot);
    assert.equal(receipt.importedRows, 504);
    assert.ok(receipt.timeBudgetStops > 0, "writer deadline was exercised");
    assert.ok(receipt.maxWriterSliceMs < 250, "writer slice exceeded 250 ms");
    assert.ok(receipt.writerSlices > 1);
    assert.equal(Object.values(receipt.writerSliceHistogram).reduce((a, b) => a + b, 0), receipt.writerSlices);
    const totals = buffer.database.prepare(`select count(*) as rows,sum(input_tokens) as input,
      sum(output_tokens) as output from buffered_events where session_id=?`).get(session) as
      { rows: number; input: number; output: number };
    assert.deepEqual(totals, { rows: 512, input: 512, output: 512 });
    const rerun = await applyCaptureHistory(buffer, captureRoot);
    assert.equal(rerun.importedRows, 0);
    console.log(JSON.stringify({ checks: 11, cursor: cursor.position,
      maxWriterSliceMs: receipt.maxWriterSliceMs, timeBudgetStops: receipt.timeBudgetStops,
      writerSliceHistogram: receipt.writerSliceHistogram, rows: totals.rows }));
  } finally {
    buffer.close();
    fixture.restore();
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
