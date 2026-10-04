import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../../packages/collector-cli/src/rollout-tailer";
import { PlanLimitEmitter } from "../../packages/collector-cli/src/plan-limit-observation";
import { jsonlScanStateKey } from "../../packages/collector-cli/src/jsonl-byte-tailer";
import { providerAccountKey } from "../../packages/shared/src/policy";

const session = "11111111-1111-4111-8111-111111111111";
const stamp = "2026-09-29T10:00:00.000Z";
const window = { window: "five_hour", minutes: 300, usedPercent: 12.5,
  resetsAt: "2026-09-29T15:00:00.000Z" };
const line = (ordinal: number, type: string, payload: object) =>
  JSON.stringify({ ordinal, timestamp: stamp, type, payload }) + "\n";
const count = (ordinal: number, input: number, cached: number, output: number) => {
  const usage = { input_tokens: input, cached_input_tokens: cached, cache_write_input_tokens: 0,
    output_tokens: output, reasoning_output_tokens: 0, total_tokens: input + output };
  return line(ordinal, "event_msg", { type: "token_count", info: {
    total_token_usage: usage, last_token_usage: usage, model_context_window: 200000,
  }, rate_limits: { limit_id: "synthetic", limit_name: null,
    primary: { used_percent: 12.5, window_minutes: 300, resets_at: 1790694000 },
    secondary: null, credits: { has_credits: false, unlimited: false, balance: "0" },
    individual_limit: null, spend_control_reached: null, plan_type: "pro",
    rate_limit_reached_type: null } });
};

async function fixture(run: (buffer: LocalEventBuffer, tailer: RolloutTailer, file: string) => Promise<void>) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-token-count-"));
  const sessions = path.join(root, ".codex/sessions");
  const day = path.join(sessions, "2026/09/29");
  fs.mkdirSync(day, { recursive: true });
  const file = path.join(day, `rollout-2026-09-29T10-00-00-${session}.jsonl`);
  // Values are invented; only the Codex 0.159.0 keys and types are modeled.
  fs.writeFileSync(file, line(0, "session_meta", { id: session, cli_version: "0.159.0" }) +
    line(1, "turn_context", { model: "gpt-5.4", cwd: root }) +
    count(2, 20, 5, 3) + count(3, 40, 10, 7));
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
  const tailer = new RolloutTailer(buffer, sessions, () => []);
  try { await run(buffer, tailer, file); }
  finally { tailer.close(); buffer.close(); fs.rmSync(root, { recursive: true, force: true }); }
}

export const rolloutTokenCountCases: Array<{ name: string; run: () => Promise<void> }> = [
  { name: "synthetic Codex 0.159.0 shape preserves token lineage, pricing and dedupe", run: () => fixture(async (buffer, tailer, file) => {
    const scan = await tailer.scan({ scope: "full" });
    assert.equal(scan.readErrors, 0);
    assert.equal(scan.parseErrors, 0);
    assert.deepEqual(scan.tokensAppended, { input: 20, cachedInput: 5, output: 4 });
    assert.deepEqual(scan.tokensUnvalidated, { input: 20, cachedInput: 5, output: 3 });
    const rows = buffer.database.prepare("select payload_json as payload from buffered_events where event_type='usage_rollout' order by rowid")
      .all() as Array<{ payload: string }>;
    assert.equal(rows.length, 2);
    const [first, second] = rows.map(row => JSON.parse(row.payload));
    assert.equal(first.metadata.counterLineage, "unknown_nonzero_first");
    assert.equal(first.costUsd, undefined);
    assert.ok(second.costUsd > 0);
    assert.equal(second.costUsd, 0.000099);
    assert.equal(second.costKind, "estimated");
    const cursor = buffer.database.prepare("select committed_offset as offset from rollout_scan_state where file=?")
      .get(jsonlScanStateKey(file)) as { offset: number };
    assert.equal(cursor.offset, fs.statSync(file).size);
    assert.equal((await tailer.scan({ scope: "full" })).eventsAppended, 0);
  }) },
  { name: "missing account skips plan readings locally while usage commits", run: () => fixture(async (buffer, tailer) => {
    const scan = await tailer.scan({ scope: "full" });
    assert.equal((buffer.database.prepare("select count(*) as n from buffered_events where event_type='plan_limit_observation'")
      .get() as { n: number }).n, 0);
    assert.equal((scan as unknown as { planLimitReadingsSkippedNoAccount: number }).planLimitReadingsSkippedNoAccount, 2);
    assert.equal(scan.eventsAppended, 2);
    assert.equal((buffer.database.prepare("select count(*) as n from sqlite_master where name='plan_limit_emission_state'")
      .get() as { n: number }).n, 0);
  }) },
  { name: "emitter refuses missing accounts before creating throttle state", run: () => fixture(async (buffer) => {
    const emitter = new PlanLimitEmitter(buffer);
    const input = { source: "codex" as const, observedAt: stamp, window, planLimitSource: "codex_rollout" as const };
    assert.equal(emitter.observe(input), false);
    assert.equal((buffer.database.prepare("select count(*) as n from sqlite_master where name='plan_limit_emission_state'")
      .get() as { n: number }).n, 0);
    assert.equal(emitter.observe({ ...input, accountKey: providerAccountKey("synthetic-account") }), true);
    assert.equal(emitter.observe({ ...input, accountKey: providerAccountKey("synthetic-account") }), false);
  }) },
  { name: "native commit exception logs private diagnostics and rolls back cursor and counters", run: () => fixture(async (buffer, tailer, file) => {
    const sentinel = "SYNTHETIC_PRIVATE_CONTENT_MUST_NOT_BE_LOGGED";
    buffer.database.exec(`create trigger synthetic_fail before insert on buffered_events begin
      select raise(abort, '${sentinel}'); end`);
    const logs: string[] = [];
    const original = console.error;
    console.error = (...args) => { logs.push(args.join(" ")); };
    let scan;
    try { scan = await tailer.scan({ scope: "full" }); }
    finally { console.error = original; }
    assert.equal(scan.readErrors, 1);
    assert.equal(scan.eventsAppended, 0);
    assert.deepEqual(scan.tokensAppended, { input: 0, cachedInput: 0, output: 0 });
    assert.equal((scan as unknown as { planLimitReadingsSkippedNoAccount: number }).planLimitReadingsSkippedNoAccount, 0,
      "failed slice must restore skip accounting");
    assert.equal((buffer.database.prepare("select count(*) as n from rollout_scan_state").get() as { n: number }).n, 0);
    assert.equal(logs.length, 1);
    const diagnostic = JSON.parse(logs[0]);
    assert.equal(diagnostic.status, "rollout_commit_error");
    assert.equal(diagnostic.errorClass, "SqliteError");
    assert.deepEqual(Object.keys(diagnostic).sort(),
      ["errorClass", "fileHandleHash", "message", "messageHash", "offset", "status"]);
    assert.equal(diagnostic.message, "[redacted error message]");
    assert.equal(diagnostic.messageHash, "sha256:" + crypto.createHash("sha256")
      .update("plimsoll-maintenance-candidate-v1\0" + sentinel).digest("hex"));
    assert.equal(diagnostic.offset, 0);
    assert.equal(diagnostic.fileHandleHash, "sha256:" + crypto.createHash("sha256")
      .update("plimsoll-maintenance-candidate-v1\0" + file).digest("hex"));
    assert.ok(typeof diagnostic.message === "string" && diagnostic.message.length > 0);
    assert.ok(!logs[0].includes(sentinel) && !logs[0].includes(file));
    buffer.database.exec("drop trigger synthetic_fail");
    assert.equal((await tailer.scan({ scope: "full" })).eventsAppended, 2);
  }) },
  { name: "parse commit exception logs its class and safe message", run: () => fixture(async (buffer, tailer, file) => {
    fs.appendFileSync(file, '{"type":"event_msg","payload":{"type":"token_count"}\n');
    const logs: string[] = [];
    const original = console.error;
    console.error = (...args) => { logs.push(args.join(" ")); };
    let scan;
    try { scan = await tailer.scan({ scope: "full" }); }
    finally { console.error = original; }
    assert.equal(scan.parseErrors, 1);
    assert.equal(scan.eventsAppended, 0);
    assert.equal(logs.length, 1);
    assert.equal(JSON.parse(logs[0]).message, "rollout_slice_parse_failed");
    assert.equal((buffer.database.prepare("select count(*) as n from rollout_scan_state").get() as { n: number }).n, 0);
  }) },
];

export async function proveRolloutTokenCountCases() {
  for (const item of rolloutTokenCountCases) await item.run();
  return { checks: rolloutTokenCountCases.length, passed: rolloutTokenCountCases.length, failed: 0 };
}
