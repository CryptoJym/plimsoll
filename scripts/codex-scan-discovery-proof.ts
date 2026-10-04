import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { AUTOMATIC_CAPTURE_LIMITS, CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { createProofCompletion } from "./lib/proof-completion";
import { installVirtualClock, restoreRealClock } from "./lib/virtual-clock";

const completion = createProofCompletion("codex-scan-discovery", 4);
const record = (type: string, payload: object) =>
  JSON.stringify({ type, payload, timestamp: new Date().toISOString() }) + "\n";
const usage = (input: number) => record("event_msg", {
  type: "token_count", info: { total_token_usage: { input_tokens: input, output_tokens: input / 10 } },
});
const prefix = (session: string) => record("session_meta", { id: session }) +
  record("turn_context", { model: "gpt-5.4" }) + usage(0);

async function fixture(body: (f: {
  buffer: LocalEventBuffer; tailer: RolloutTailer; partition: string;
  scan: (budget?: CaptureWorkBudget) => ReturnType<RolloutTailer["scan"]>;
}) => Promise<void>) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "codex-discovery-"));
  const sessions = path.join(root, "sessions");
  const partition = path.join(sessions, ...new Date().toISOString().slice(0, 10).split("-"));
  const claude = path.join(root, "claude");
  fs.mkdirSync(partition, { recursive: true });
  fs.mkdirSync(claude);
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
  const tailer = new RolloutTailer(buffer, sessions, () => []);
  const maintenance = new CollectorMaintenance(buffer, tailer, new TranscriptTailer(buffer, claude));
  try {
    installVirtualClock();
    for (let n = 0; n < 8 && captureBaselineStatus(buffer.database).status !== "complete"; n++) {
      await maintenance.runRecent();
    }
    assert.equal(captureBaselineStatus(buffer.database).status, "complete");
    await new Promise(resolve => setTimeout(resolve, 5));
    await body({ buffer, tailer, partition, scan: (budget = new CaptureWorkBudget()) =>
      tailer.scan({ scope: "recent", automatic: { phase: "capture", budget } }) });
  } finally {
    restoreRealClock();
    maintenance.close();
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

const cases = [
  { name: "progress-gated current-day candidates resume without more discovery", run: () => fixture(async f => {
    for (let n = 0; n < 24; n++) fs.writeFileSync(path.join(f.partition, `rollout-${n}.jsonl`), "{}\n");
    let metadata = 0;
    const signal = new AbortController();
    const first = await f.tailer.scan({ scope: "recent", signal: signal.signal,
      automatic: { phase: "capture", budget: new CaptureWorkBudget() },
      onProgress: progress => {
        if (progress.stage === "candidate_metadata" && ++metadata === 4) {
          signal.abort();
          return false;
        }
        return true;
      },
    });
    const next = await f.scan();
    assert.ok(first.activity.discoveryEntries > 0, "publish every visited entry, including foreground discovery");
    assert.equal(next.activity.discoveryEntries, 0, "resume unserviced metadata before another discovery quantum");
    assert.ok(next.filesSeen > 0);
  }) },
  { name: "an unchanged sweep enumerates the current-day directory once and reads no content", run: () => fixture(async f => {
    const session = randomUUID();
    fs.writeFileSync(path.join(f.partition, `rollout-${session}.jsonl`), prefix(session) + usage(100));
    await f.scan();
    f.tailer.close();
    const original = fs.opendirSync;
    let opened = 0;
    fs.opendirSync = ((...args: Parameters<typeof fs.opendirSync>) => {
      if (path.resolve(String(args[0])) === f.partition) opened++;
      return original(...args);
    }) as typeof fs.opendirSync;
    try {
      const unchanged = await f.scan();
      assert.equal(unchanged.filesRead, 0);
      assert.equal(unchanged.bytesRead, 0);
      assert.equal(unchanged.eventsAppended, 0);
      assert.equal(opened, 1, "a stable sweep must not start a duplicate foreground walk");
    } finally { fs.opendirSync = original; }
  }) },
  { name: "a small ready tail precedes a newer oversized file within the shared budget", run: () => fixture(async f => {
    const session = randomUUID();
    fs.writeFileSync(path.join(f.partition, `rollout-${session}.jsonl`), prefix(session) + usage(100));
    await new Promise(resolve => setTimeout(resolve, 5));
    fs.writeFileSync(path.join(f.partition, "rollout-newest-large.jsonl"),
      JSON.stringify({ type: "fixture_ignored", padding: "x".repeat(600 * 1024) }) + "\n");
    const result = await f.scan(new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxBytes: 64 * 1024 }));
    assert.ok(f.buffer.database.prepare(
      "select 1 from buffered_events where session_id=? and event_type='usage_rollout' and input_tokens=100",
    ).get(session), "the oversized candidate must not spend the small ready tail's allowance");
    assert.ok(result.bytesRead <= 64 * 1024);
  }) },
  { name: "byte slices preserve UTF-8 record boundaries and wait for a final newline", run: () => fixture(async f => {
    const session = randomUUID();
    const file = path.join(f.partition, `rollout-${session}.jsonl`);
    const complete = prefix(session) + usage(100) +
      JSON.stringify({ type: "fixture_ignored", padding: "é".repeat(70 * 1024) }) + "\n" + usage(200);
    fs.writeFileSync(file, complete + usage(300).trimEnd());
    const cursor = () => f.buffer.database.prepare(
      "select committed_offset as n from rollout_scan_state where file=?",
    ).get(jsonlScanStateKey(file)) as { n: number } | undefined;
    for (let n = 0; n < 20 && (cursor()?.n ?? 0) < Buffer.byteLength(complete); n++) {
      const result = await f.scan(new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxBytes: 64 * 1024 }));
      assert.equal(result.parseErrors, 0);
      assert.equal(result.readErrors, 0);
      assert.ok((cursor()?.n ?? 0) <= Buffer.byteLength(complete), "incomplete final record cannot commit");
    }
    assert.equal(cursor()?.n, Buffer.byteLength(complete));
    const totals = () => (f.buffer.database.prepare(
      "select sum(input_tokens) as n from buffered_events where session_id=? and event_type='usage_rollout'",
    ).get(session) as { n: number }).n;
    assert.equal(totals(), 200);
    fs.appendFileSync(file, "\n");
    for (let n = 0; n < 20 && cursor()?.n !== fs.statSync(file).size; n++) {
      const result = await f.scan(new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxBytes: 64 * 1024 }));
      assert.equal(result.parseErrors, 0);
      assert.equal(result.readErrors, 0);
    }
    assert.equal(cursor()?.n, fs.statSync(file).size);
    assert.equal(totals(), 300, "every complete usage delta arrives exactly once");
    const unchanged = await f.scan();
    assert.equal(unchanged.filesRead, 0);
    assert.equal(unchanged.bytesRead, 0);
    assert.equal(unchanged.eventsAppended, 0);
  }) },
];

async function main() {
  let failed = false;
  for (const item of cases) {
    try {
      await item.run();
      completion.check(item.name);
      console.log(JSON.stringify({ name: item.name, passed: true }));
    } catch (error) {
      failed = true;
      completion.check(item.name, false);
      console.log(JSON.stringify({ name: item.name, passed: false,
        error: error instanceof Error ? error.message : String(error) }));
    }
  }
  completion.complete();
  if (failed) process.exitCode = 1;
}
main().catch(error => { console.error(error); process.exitCode = 1; });
