import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus, rememberCaptureSweepResume } from "../packages/collector-cli/src/capture-baseline";
import { rememberCaptureRotation } from "../packages/collector-cli/src/capture-fairness";
import { AUTOMATIC_CAPTURE_LIMITS, CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import {
  DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey, prepareJsonlCommittedPrefixHash,
  readJsonlTail, rememberJsonlScanCursor,
} from "../packages/collector-cli/src/jsonl-byte-tailer";
import { AUTOMATIC_MAINTENANCE_NORMAL_INTERVAL_MS, CollectorMaintenance, automaticRepairServiceStatus } from "../packages/collector-cli/src/maintenance";
import { maintenanceCandidateHash } from "../packages/collector-cli/src/maintenance-progress";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { createProofCompletion } from "./lib/proof-completion";
import { installVirtualClock, restoreRealClock } from "./lib/virtual-clock";

const completion = createProofCompletion("codex-scan-backlog", 4);
const results: Array<{ name: string; passed: boolean; detail?: unknown }> = [];
const line = (type: string, payload: object, timestamp = new Date().toISOString()) =>
  JSON.stringify({ type, payload, timestamp }) + "\n";
const count = (input: number) => line("event_msg", {
  type: "token_count", info: { total_token_usage: { input_tokens: input, output_tokens: input / 10 } },
});
const ignored = JSON.stringify({ type: "fixture_ignored", padding: "x".repeat(1000) }) + "\n";

async function fixture(body: (f: {
  buffer: LocalEventBuffer; tailer: RolloutTailer; maintenance: CollectorMaintenance;
  sessions: string; claude: string; reads: string[];
  file: (day: string, records?: number) => { file: string; session: string };
  seed: (file: string, session: string) => void;
}) => Promise<void>) {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "codex-backlog-"));
  const sessions = path.join(base, "codex/sessions");
  const claude = path.join(base, "claude");
  fs.mkdirSync(sessions, { recursive: true });
  fs.mkdirSync(claude, { recursive: true });
  const buffer = new LocalEventBuffer(path.join(base, "ledger.sqlite"));
  const reads: string[] = [];
  const io = { ...DEFAULT_JSONL_TAILER_IO, readTail: (...args: Parameters<typeof readJsonlTail>) => {
    reads.push(args[0]);
    return readJsonlTail(...args);
  } };
  const tailer = new RolloutTailer(buffer, sessions, () => [], io);
  const transcript = new TranscriptTailer(buffer, claude, io);
  const maintenance = new CollectorMaintenance(buffer, tailer, transcript);
  try {
    installVirtualClock();
    for (let n = 0; n < 8 && captureBaselineStatus(buffer.database).status !== "complete"; n++) {
      await maintenance.runRecent();
    }
    assert.equal(captureBaselineStatus(buffer.database).status, "complete");
    // All subsequent generations are post-enrollment, even when their day
    // directory and mtime are old. No pre-enrollment history is admitted.
    await new Promise(resolve => setTimeout(resolve, 5));
    const file = (day: string, records = 0) => {
      const session = randomUUID();
      const directory = path.join(sessions, ...day.split("-"));
      fs.mkdirSync(directory, { recursive: true });
      const file = path.join(directory, `rollout-${day}T12-00-00-${session}.jsonl`);
      fs.writeFileSync(file, line("session_meta", { id: session }) +
        line("turn_context", { model: "gpt-5.4" }) + count(0) + count(100) + ignored.repeat(records));
      if (day !== new Date().toISOString().slice(0, 10)) {
        const mtime = new Date(`${day}T12:00:00Z`);
        fs.utimesSync(file, mtime, mtime);
      }
      return { file, session };
    };
    const seed = (file: string, session: string) => {
      const read = readJsonlTail(file, fs.statSync(file), undefined, { maxBytes: 4096, maxRecords: 3 })!;
      try {
        prepareJsonlCommittedPrefixHash(buffer.database, file, file, undefined, read);
        // Only metadata and the observed zero token counter precede this cursor.
        rememberJsonlScanCursor(buffer.database, file, "codex-rollout-v2", 2, read, {
          parserKind: "codex-rollout-v2", checkpointVersion: 2, conversationId: session,
          model: "gpt-5.4", previous: { input: 0, cachedInput: 0, output: 0, reasoningOutput: 0 },
          tokenCountIndex: 0, contextOccurrenceIndex: 1,
        });
      } finally { read.close(); }
    };
    reads.length = 0;
    await body({ buffer, tailer, maintenance, sessions, claude, reads, file, seed });
  } finally {
    restoreRealClock();
    maintenance.close();
    buffer.close();
    fs.rmSync(base, { recursive: true, force: true });
  }
}

const today = () => new Date().toISOString().slice(0, 10);
const yesterday = () => new Date(Date.now() - 86_400_000).toISOString().slice(0, 10);
const offset = (buffer: LocalEventBuffer, file: string) =>
  (buffer.database.prepare("select committed_offset as n from rollout_scan_state where file=?")
    .get(jsonlScanStateKey(file)) as { n: number } | undefined)?.n ?? 0;
const hasUsage = (buffer: LocalEventBuffer, session: string) => Boolean(buffer.database.prepare(
  "select 1 from buffered_events where session_id=? and event_type='usage_rollout' and input_tokens>0",
).get(session));

const cases = [
  { name: "today's fresh file precedes a persisted backlog rotation", run: () => fixture(async f => {
    for (let n = 0; n < 3; n++) f.file(yesterday(), 300);
    const current = f.file(today());
    rememberCaptureRotation(f.buffer.database, "codex", maintenanceCandidateHash(current.file));
    await f.tailer.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget({
      ...AUTOMATIC_CAPTURE_LIMITS, maxBytes: 64 * 1024, maxRecords: 64,
    }) } });
    assert.equal(f.reads[0], current.file, "old rotation must not demote the current file");
    assert.ok(hasUsage(f.buffer, current.session));
  }) },
  { name: "a large Codex file takes at most four slices and leaves service for Claude and repairs", run: () => fixture(async f => {
    const large = f.file(today(), 3000);
    const session = randomUUID();
    fs.writeFileSync(path.join(f.claude, "session.jsonl"), JSON.stringify({ type: "assistant", sessionId: session,
      timestamp: new Date().toISOString(), message: { id: randomUUID(), model: "claude-sonnet-4",
        usage: { input_tokens: 10, output_tokens: 2 } } }) + "\n");
    f.buffer.database.prepare("delete from maintenance_state where key='automatic_capture_source_turn'").run();
    const before = automaticRepairServiceStatus(f.buffer.database);
    const result = await f.maintenance.runRecent();
    assert.equal(f.reads.filter(file => file === large.file).length, 4, "at most four quanta per file per cadence");
    assert.ok(result.transcript.eventsAppended > 0, "the next source retains byte/record allowance");
    const after = automaticRepairServiceStatus(f.buffer.database);
    assert.ok(after.cycles > before.cycles && Object.values(after.stages).every((stage, index) =>
      stage.completed > Object.values(before.stages)[index]!.completed), "every repair stage still advances");
    const budget = f.maintenance.status().budget!;
    assert.ok(budget.bytesRead <= AUTOMATIC_CAPTURE_LIMITS.maxBytes);
    assert.ok(budget.recordsParsed <= AUTOMATIC_CAPTURE_LIMITS.maxRecords);
  }) },
  { name: "a deferred generation gets a slice alongside a busy current-day queue", run: () => fixture(async f => {
    const old = f.file(yesterday(), 3000);
    for (let n = 0; n < 10; n++) f.file(today(), 3000);
    await f.tailer.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
    assert.ok(offset(f.buffer, old.file) > 0, "fresh generations cannot consume every backlog turn");
    assert.equal(f.reads[1], old.file, "one background slice follows the freshest slice");
  }) },
  { name: "1,000 real files with over 3 GB deferred deliver today's event in one maintenance interval", run: () => fixture(async f => {
    const oldFiles: string[] = [];
    for (let n = 0; n < 1000; n++) {
      const old = f.file(yesterday(), 3072);
      f.seed(old.file, old.session);
      oldFiles.push(old.file);
    }
    const backlog = f.buffer.database.prepare("select count(*) as files, sum(deferred_bytes) as bytes from rollout_scan_state where work_remaining=1")
      .get() as { files: number; bytes: number };
    assert.equal(backlog.files, 1000);
    assert.ok(backlog.bytes > 3_000_000_000, "multi-GB backlog is real JSONL, not inflated metadata or sparse holes");
    rememberCaptureSweepResume(f.buffer.database, "codex", { rootIndex: 1, observedEntries: 1000 });
    // A progress gate leaves yesterday's metadata batch unserviced. This is
    // the production gate path, not an injected private tailer queue.
    await f.maintenance.runRecent({ onProgress: progress => progress.stage !== "jsonl_open" });
    const current = f.file(today());
    const started = Date.now();
    await f.maintenance.runRecent();
    const elapsedMs = Date.now() - started;
    assert.ok(hasUsage(f.buffer, current.session), "today must not wait for the carried metadata batch");
    assert.ok(elapsedMs < AUTOMATIC_MAINTENANCE_NORMAL_INTERVAL_MS);
    assert.ok(oldFiles.some(file => offset(f.buffer, file) > 512), "background drain continues");
    const budget = f.maintenance.status().budget!;
    assert.ok(budget.bytesRead <= 524288 && budget.recordsParsed <= 512 && budget.maxWallMs === 200);
    console.log(JSON.stringify({ syntheticBacklog: backlog, currentDayMaintenanceTurns: 1, elapsedMs,
      schedulingClock: "deterministic admission; real filesystem, ledger and elapsed duration", budget }));
  }) },
];

async function main() {
  for (const item of cases) {
    try {
      await item.run();
      results.push({ name: item.name, passed: true });
      completion.check(item.name);
    } catch (error) {
      results.push({ name: item.name, passed: false, detail: error instanceof Error ? error.message : String(error) });
      completion.check(item.name, false);
    }
  }
  console.log(JSON.stringify({ checks: results }, null, 2));
  completion.complete();
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
