/** Many zero-token usage records must not become one large writer transaction. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { applyCaptureHistory, planCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { deriveCaptureRootIdentity, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { useFixtureRoot } from "./lib/fixture-root";

const SESSION = "019d0000-0000-7000-8000-000000000703";
const START = "2026-01-01T00:00:00.000Z", FENCE = "2026-01-03T00:00:00.000Z";
const source = process.argv[2] === "codex" ? "codex" : process.argv[2] === "claude" ? "claude_code" : undefined;
if (!source) throw new Error("choose codex or claude");
const line = (row: unknown) => JSON.stringify(row) + "\n";
const sha = (bytes: Buffer) => crypto.createHash("sha256").update(bytes).digest("hex");

async function main() {
  const scratch = fs.realpathSync(fs.mkdtempSync(path.join(path.resolve(process.cwd(), ".."), "history-record-gaps-")));
  const fixture = useFixtureRoot(scratch, { home: path.join(scratch, "home"), plimsollHome: path.join(scratch, "ledger") });
  fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
  fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });
  const buffer = new LocalEventBuffer(path.join(fixture.env.PLIMSOLL_HOME, "work-ledger.sqlite"), {
    workspaceId: "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10",
    deviceId: "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d", enrollmentNow: () => new Date(START) });
  try {
    const roots: CaptureRoot[] = ["a", "b"].map(name => {
      const directory = path.join(fixture.home, name, source === "codex" ? "sessions" : "projects");
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      return { ...deriveCaptureRootIdentity("record-gaps-proof", source!, directory), source: source!, directory,
        installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! };
    });
    const head = line(source === "codex" ? { type: "session_meta", payload: { id: SESSION } }
      : { type: "message", sessionId: SESSION });
    const records = Array.from({ length: 513 }, (_, index) => {
      const value = index === 512 ? 100 : 0;
      const timestamp = new Date(Date.parse("2026-01-02T00:00:00.000Z") + index * 1000).toISOString();
      return line(source === "codex" ? { type: "event_msg", timestamp,
        payload: { type: "token_count", info: { total_token_usage: { input_tokens: value, output_tokens: 0 } } } }
        : { type: "assistant", sessionId: SESSION, timestamp, message: { id: "same-message",
          usage: { input_tokens: value, output_tokens: 0 } } });
    });
    const original = Buffer.from(head + records.join(""));
    const short = Buffer.from(head + records.slice(0, 256).join(""));
    const files = roots.map((root, index) => {
      const file = path.join(root.directory, `${source === "codex" ? "rollout-2026-01-02-" : ""}${SESSION}.jsonl`);
      fs.writeFileSync(file, index ? short : original, { mode: 0o600 }); return file;
    });
    const db = buffer.database;
    const baseline = beginAutomaticCaptureBaseline(db, source!, { startedAt: START, filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(db, source!, { runId: baseline.latestRun!.runId, completedAt: START });
    sealCaptureBaselineGenerations(db, source!, files.map(file => {
      const stat = fs.statSync(file, { bigint: true });
      return { path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs };
    }), FENCE);
    db.exec(`create table capture_history_record_bytes (
      source text not null,session_id text not null,record_index integer not null,
      byte_offset integer not null,prefix_digest text not null,
      primary key(source,session_id,record_index,byte_offset))`);
    const frames: Array<{ records: number; started: number }> = [];
    let maxRecordRows = 0, maxRecordWriteMs = 0;
    const transaction = db.transaction.bind(db) as (callback: (...args: any[]) => any) => any;
    (db as any).transaction = (callback: (...args: any[]) => any) => transaction((...args: any[]) => {
      const frame = { records: 0, started: performance.now() }; frames.push(frame);
      try { return callback(...args); }
      finally {
        frames.pop();
        if (frame.records) {
          maxRecordRows = Math.max(maxRecordRows, frame.records);
          maxRecordWriteMs = Math.max(maxRecordWriteMs, performance.now() - frame.started);
        }
      }
    });
    let pressure = true, failAt = 0, pendingWrites = 0;
    db.function("record_gap_pressure", () => {
      for (const frame of frames) frame.records += 1;
      if (failAt && ++pendingWrites === failAt) throw new Error("record_gap_injected_before_row");
      const until = performance.now() + (pressure ? 3 : 0);
      while (performance.now() < until) { /* deterministic cost for saving one fingerprint */ }
    });
    db.exec(`create trigger record_gap_delay before insert on capture_history_record_bytes
      begin select record_gap_pressure(); end`);
    await assert.rejects(applyCaptureHistory(buffer, roots[0]!, { stopAfterSlices: 1 }), /capture_history_injected_crash/);
    const count = db.prepare(`select count(*) as n from capture_history_record_bytes where source=? and session_id=?`)
      .get(source, SESSION) as { n: number };
    const saved = db.prepare(`select byte_offset as offset,prefix_digest as digest from capture_history_record_bytes
      where source=? and session_id=? and record_index=255`).get(source, SESSION) as { offset: number; digest: string };
    assert.equal(count.n, 513, "all usage fingerprints are saved before the counted row commits");
    assert.equal(saved.offset, short.length); assert.equal(saved.digest, sha(short));
    console.log(JSON.stringify({ source, savedRecords: count.n, maxRecordRows, maxRecordWriteMs }));
    assert.ok(maxRecordRows <= 40, "more than 40 usage fingerprints were saved in one transaction");
    assert.ok(maxRecordWriteMs < 250, "record fingerprint writes exceeded the writer limit");
    db.prepare("update capture_history_import_lock set owner_pid=999999 where singleton=1").run();
    const plan = await planCaptureHistory(db, roots[1]!);
    assert.equal(plan.missingRows, 0); assert.equal(plan.refusals.length, 0);
    assert.equal((await applyCaptureHistory(buffer, roots[1]!)).importedRows, 0);
    assert.equal((await applyCaptureHistory(buffer, roots[0]!)).importedRows, 0);
    const total = db.prepare("select sum(input_tokens) as n from buffered_events where session_id=?").get(SESSION) as { n: number };
    assert.equal(total.n, 100);
    console.log(JSON.stringify({ proof: "record_gaps", source, savedRecords: count.n,
      maxRecordRows, maxRecordWriteMs, shortRows: 0, replayRows: 0, total: total.n }));

    // A failed file may save fingerprints before it publishes any counted
    // row. A permitted retry of that unpublished file must replace those
    // pending fingerprints when its bytes change.
    pressure = false;
    const retrySession = SESSION.replace(/703$/, "704");
    const retryHead = line(source === "codex" ? { type: "session_meta", payload: { id: retrySession, marker: "retry-a" } }
      : { type: "message", sessionId: retrySession, marker: "retry-a" });
    const retryRecords = records.map(row => row.replaceAll(SESSION, retrySession));
    const retryRoots: CaptureRoot[] = ["c", "d"].map(name => {
      const directory = path.join(fixture.home, name, source === "codex" ? "sessions" : "projects");
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      return { ...deriveCaptureRootIdentity("record-gaps-proof", source!, directory), source: source!, directory,
        installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! };
    });
    const retryBytes = Buffer.from(retryHead + retryRecords.join(""));
    const changedRetryBytes = Buffer.from(retryBytes.toString().replace("retry-a", "retry-b"));
    const changedShortBytes = Buffer.from(retryHead.replace("retry-a", "retry-b") + retryRecords.slice(0, 40).join(""));
    const retryFiles = retryRoots.map((root, index) => {
      const file = path.join(root.directory, `${source === "codex" ? "rollout-2026-01-02-" : ""}${retrySession}.jsonl`);
      fs.writeFileSync(file, index ? changedShortBytes : retryBytes, { mode: 0o600 }); return file;
    });
    sealCaptureBaselineGenerations(db, source!, retryFiles.map(file => {
      const stat = fs.statSync(file, { bigint: true });
      return { path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs };
    }), FENCE);
    failAt = 90;
    await assert.rejects(applyCaptureHistory(buffer, retryRoots[0]!), /record_gap_injected_before_row/);
    const pending = db.prepare("select count(*) as n from capture_history_record_bytes where source=? and session_id=?")
      .get(source, retrySession) as { n: number };
    assert.ok(pending.n > 0 && pending.n < failAt);
    assert.equal((db.prepare("select count(*) as n from buffered_events where session_id=?").get(retrySession) as { n: number }).n, 0);
    failAt = 0;
    fs.writeFileSync(retryFiles[0]!, changedRetryBytes);
    assert.equal((await applyCaptureHistory(buffer, retryRoots[0]!)).importedRows, 1);
    const retryPlan = await planCaptureHistory(db, retryRoots[1]!);
    assert.equal(retryPlan.missingRows, 0); assert.equal(retryPlan.refusals.length, 0);
    assert.equal((await applyCaptureHistory(buffer, retryRoots[1]!)).importedRows, 0);
    assert.equal((await applyCaptureHistory(buffer, retryRoots[0]!)).importedRows, 0);
    const retrySaved = db.prepare(`select prefix_digest as digest from capture_history_record_bytes
      where source=? and session_id=? and record_index=39 and byte_offset=?`)
      .get(source, retrySession, changedShortBytes.length) as { digest: string };
    assert.equal(retrySaved.digest, sha(changedShortBytes));
    assert.ok(maxRecordRows <= 40);
    console.log(JSON.stringify({ proof: "pending_records_retry", source, pendingRecords: pending.n,
      changedBytes: 1, importedRows: 1, shortRows: 0, replayRows: 0, maxRecordRows }));
  } finally { buffer.close(); fixture.restore(); fs.rmSync(scratch, { recursive: true, force: true }); }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
