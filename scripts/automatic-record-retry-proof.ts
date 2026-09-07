import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { createProfileCapture } from "../packages/collector-cli/src/profile-capture";
import { CaptureWorkBudget, AUTOMATIC_CAPTURE_LIMITS } from "../packages/collector-cli/src/capture-work-budget";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { rootCursorKey, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";

// Synthetic files and ledger only. Real profile paths are never used.
const home = fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir());
const fixture = fs.mkdtempSync(path.join(home, "automatic-record-retry-"));
const checks: Array<{ name: string; passed: boolean; detail?: unknown }> = [];
function check(name: string, passed: boolean, detail?: unknown) {
  checks.push({ name, passed, ...(detail === undefined ? {} : { detail }) });
}
const sentinel = "PRIVATE_SYNTHETIC_RECORD_";
const session = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const baselineAt = new Date(Date.now() - 2_000);
const cutoff = new Date(Date.now() - 60_000).toISOString();
const old = new Date(Date.parse(cutoff) - 1_000).toISOString();
const future = new Date(Date.now() + 1_000).toISOString();

async function prove(source: CaptureRoot["source"], largeBytes: number) {
  const name = `${source}-${largeBytes}`;
  const base = path.join(fixture, name);
  fs.mkdirSync(base);
  const buffer = new LocalEventBuffer(path.join(base, "ledger.sqlite"), {
    workspaceId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    enrollmentNow: () => new Date(cutoff),
  });
  const directory = path.join(base, source === "codex" ? "sessions" : "projects");
  fs.mkdirSync(directory);
  const root: CaptureRoot = { source, directory, rootId: name, profileId: name,
    installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! };
  const config = { captureRoots: [root] };
  let capture = createProfileCapture(buffer, config);
  const budgets: ReturnType<CaptureWorkBudget["status"]>[] = [];
  try {
    for (let turn = 0; turn < 4; turn++) {
      for (const tailer of [capture.rollout, capture.transcript]) {
        await tailer.scan({ scope: "recent", now: baselineAt,
          automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
      }
    }
    assert.equal(captureBaselineStatus(buffer.database).status, "complete");
    const file = source === "codex"
      ? path.join(directory, ...new Date().toISOString().slice(0, 10).split("-"), `rollout-${session}.jsonl`)
      : path.join(directory, "fixture-project", `${session}.jsonl`);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const usage = (timestamp: string, tokens: number, id: string) => source === "codex"
      ? { type: "event_msg", timestamp, payload: { type: "token_count", info: {
        total_token_usage: { input_tokens: tokens, cached_input_tokens: 0, output_tokens: 0, reasoning_output_tokens: 0 },
      } } }
      : { type: "assistant", sessionId: session, timestamp, message: {
        id, model: "claude-opus-5", usage: { input_tokens: tokens, output_tokens: 0 },
      } };
    const prefix = source === "codex" ? [
      { type: "session_meta", timestamp: future, payload: { id: session } },
      { type: "turn_context", timestamp: future, payload: { model: "gpt-5.5" } },
    ] : [];
    const large = { type: "event_msg", timestamp: future, payload: {
      type: "user_message", message: sentinel + "x".repeat(largeBytes),
    } };
    fs.writeFileSync(file, [...prefix, usage(old, 100, "old"), large,
      usage(future, source === "codex" ? 107 : 7, "future-1"),
      usage(future, source === "codex" ? 109 : 2, "future-2"),
    ].map(line => JSON.stringify(line)).join("\n") + "\n");
    const cursor = () => buffer.database.prepare(`select committed_offset, deferred_bytes,
      unresolved_kind, unresolved_byte_budget from rollout_scan_state where file = ?`)
      .get(jsonlScanStateKey(rootCursorKey([root], file))) as Record<string, unknown> | undefined;
    const scan = async () => {
      const budget = new CaptureWorkBudget();
      const result = await (source === "codex" ? capture.rollout : capture.transcript).scan({
        scope: "recent", automatic: { phase: "capture", budget },
      });
      budgets.push(budget.status());
      return result;
    };
    await scan();
    const firstCursor = cursor();
    check(`${name}: initial fixed quantum records unresolved offset`,
      firstCursor?.unresolved_kind === "record_exceeds_byte_budget" &&
      firstCursor.unresolved_byte_budget === AUTOMATIC_CAPTURE_LIMITS.sliceBytes, firstCursor);
    capture.close(); capture = createProfileCapture(buffer, config);
    for (let turn = 0; turn < 6; turn++) await scan();
    const total = buffer.database.prepare(`select count(*) as n, coalesce(sum(input_tokens),0) as tokens
      from buffered_events`).get() as { n: number; tokens: number };
    const finalCursor = cursor();
    if (largeBytes < AUTOMATIC_CAPTURE_LIMITS.maxBytes - 2_048) {
      check(`${name}: unchanged future file resumes after restart`, total.n === 2 && total.tokens === 9,
        { total, cursor: finalCursor, budgets });
      check(`${name}: cursor consumes suffix and clears unresolved state`,
        finalCursor?.deferred_bytes === 0 && finalCursor.unresolved_kind === null, finalCursor);
    } else {
      check(`${name}: above cadence cap remains explicitly unresolved`, total.n === 0 &&
        finalCursor?.unresolved_kind === "record_exceeds_byte_budget", finalCursor);
      const idle = await scan();
      check(`${name}: unchanged above-cap record does not spin`, idle.bytesRead === 0, idle.bytesRead);
    }
    check(`${name}: pre-cutoff events stay excluded`,
      (buffer.database.prepare("select count(*) as n from buffered_events where observed_at < ?").get(cutoff) as {n:number}).n === 0);
    check(`${name}: retries keep all existing cadence limits`, budgets.every(b =>
      b.bytesRead <= b.maxBytes && b.recordsParsed <= b.maxRecords && b.eventsAppended <= b.maxEvents), budgets);
    // Reading synthetic payloads here verifies that discarded source bodies never persist.
    const persisted = JSON.stringify(buffer.database.prepare("select payload_json from buffered_events").all()) +
      JSON.stringify(buffer.database.prepare("select parser_state_json from rollout_scan_state").all());
    check(`${name}: synthetic private record is absent from ledger`, !persisted.includes(sentinel));
  } finally { capture.close(); buffer.close(); }
}

async function main() {
  try {
    for (const source of ["codex", "claude_code"] as const) {
      await prove(source, 350 * 1024);
      await prove(source, 600 * 1024);
    }
    const receipt = { schema: "plimsoll.automatic-record-retry-proof.v1", observedAt: new Date().toISOString(),
      node: process.version, syntheticOnly: true, checks, passed: checks.every(check => check.passed) };
    console.log(JSON.stringify(receipt, null, 2));
    if (!receipt.passed) process.exitCode = 1;
  } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
