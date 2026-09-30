/** Imported files keep main's durable Claude sighting and cursor byte evidence. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { applyCaptureHistory, planCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { captureRootDigest, deriveCaptureRootIdentity, rootCursorKey,
  type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { useFixtureRoot } from "./lib/fixture-root";

const SESSION = "019d0000-0000-7000-8000-000000000708";
const START = "2026-01-01T00:00:00.000Z", FENCE = "2026-01-03T00:00:00.000Z";
const source = process.argv[2] === "codex" ? "codex" : process.argv[2] === "claude" ? "claude_code" : undefined;
if (!source) throw new Error("choose codex or claude");
const line = (value: unknown) => JSON.stringify(value) + "\n";

async function main() {
  const scratch = fs.realpathSync(fs.mkdtempSync(path.join(path.resolve(process.cwd(), ".."), "history-main-handoff-")));
  const fixture = useFixtureRoot(scratch, { home: path.join(scratch, "home"), plimsollHome: path.join(scratch, "ledger") });
  fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
  fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });
  const ledger = path.join(fixture.env.PLIMSOLL_HOME, "work-ledger.sqlite");
  const buffer = new LocalEventBuffer(ledger, { workspaceId: "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10",
    deviceId: "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d", enrollmentNow: () => new Date(START) });
  const observer = new Database(ledger, { readonly: true });
  try {
    const roots: CaptureRoot[] = ["a", "b"].map(name => {
      const directory = path.join(fixture.home, name, source === "codex" ? "sessions" : "projects");
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      return { ...deriveCaptureRootIdentity("history-main-handoff", source!, directory), source: source!, directory,
        installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! };
    });
    const head = line(source === "codex" ? { type: "session_meta", payload: { id: SESSION, marker: "first-a" } }
      : { type: "message", sessionId: SESSION, marker: "first-a" });
    const values = source === "codex" ? [0, 100] : [100];
    const bytes = head + values.map((value, index) => line(source === "codex" ? {
      type: "event_msg", timestamp: `2026-01-02T00:00:0${index + 1}.000Z`,
      payload: { type: "token_count", info: { total_token_usage: { input_tokens: value, output_tokens: 0 } } },
    } : { type: "assistant", sessionId: SESSION, timestamp: "2026-01-02T00:00:01.000Z",
      message: { id: "message-a", usage: { input_tokens: value, output_tokens: 0 } } })).join("");
    const files = roots.map((root, index) => {
      const file = path.join(root.directory, `${source === "codex" ? "rollout-2026-01-02-" : ""}${SESSION}.jsonl`);
      fs.writeFileSync(file, index ? bytes.replace("first-a", "first-b") : bytes, { mode: 0o600 }); return file;
    });
    const db = buffer.database;
    const baseline = beginAutomaticCaptureBaseline(db, source!, { startedAt: START, filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(db, source!, { runId: baseline.latestRun!.runId, completedAt: START });
    sealCaptureBaselineGenerations(db, source!, files.map(file => {
      const stat = fs.statSync(file, { bigint: true });
      return { path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs };
    }), FENCE);
    let observedBeforeRows = 0;
    if (source === "claude_code") {
      db.function("history_sighting_visible", () => {
        assert.ok(observer.prepare(`select 1 from capture_root_session_sightings
          where source='claude_code' and session_id=? and root_digest=?`)
          .get(SESSION, captureRootDigest(roots[0]!)), "another connection sees the Claude folder before counted rows");
        observedBeforeRows++;
      });
      db.exec(`create trigger verify_history_sighting before insert on buffered_events
        when new.source='claude_code' begin select history_sighting_visible(); end`);
    }
    const receipt = await applyCaptureHistory(buffer, roots[0]!);
    assert.ok(receipt.importedRows > 0);
    assert.equal((db.prepare("select sum(input_tokens) as n from buffered_events where session_id=?")
      .get(SESSION) as { n: number }).n, 100);
    if (source === "claude_code") assert.equal(observedBeforeRows, receipt.importedRows);
    const cursor = observer.prepare(`select committed_offset as offset,committed_prefix_hash as digest
      from rollout_scan_state where file=?`).get(jsonlScanStateKey(rootCursorKey([roots[0]!], files[0]!))) as
      { offset: number; digest: string };
    assert.equal(cursor.offset, Buffer.byteLength(bytes));
    assert.equal(cursor.digest, crypto.createHash("sha256").update(bytes).digest("hex"));
    assert.equal((await applyCaptureHistory(buffer, roots[0]!)).importedRows, 0);
    const plan = await planCaptureHistory(db, roots[1]!);
    assert.equal(plan.missingRows, 0); assert.equal(plan.refusals.length, 1);
    await assert.rejects(applyCaptureHistory(buffer, roots[1]!), /copied_prefix_bytes_differ/);
    if (source === "claude_code") assert.equal(observer.prepare(`select 1 from capture_root_session_sightings
      where source='claude_code' and session_id=? and root_digest=?`).get(SESSION, captureRootDigest(roots[1]!)), undefined,
    "refused bytes cannot establish a folder sighting");
    console.log(JSON.stringify({ proof: "history_main_handoff", source, importedRows: receipt.importedRows,
      total: 100, observedBeforeRows, cursor, replayRows: 0, refusedCopyRows: 0 }));
  } finally { observer.close(); buffer.close(); fixture.restore(); fs.rmSync(scratch, { recursive: true, force: true }); }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
