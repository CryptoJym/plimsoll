import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline } from "../packages/collector-cli/src/capture-baseline";
import { AUTOMATIC_CAPTURE_LIMITS, CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { createProofCompletion } from "./lib/proof-completion";
import { installVirtualClock, restoreRealClock } from "./lib/virtual-clock";

const completion = createProofCompletion("codex-bounded-response-accounting", 4);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "bounded-accounting-"));
const at = Date.now() - 600_000;
const session = "22222222-2222-4222-8222-222222222222";
const model = "gpt-6.1-sol";
const fields = ["inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens", "costUsd"] as const;
const outcomes: unknown[] = [];

const attribute = (key: string, value: string | number) => ({ key,
  value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value } });
function sse(complete: boolean) {
  const event = explodeOtlpPayload({ resourceLogs: [{ resource: { attributes: [attribute("service.name", "codex-app-server")] },
    scopeLogs: [{ logRecords: [{ timeUnixNano: String(BigInt(at + (complete ? 5500 : 6000)) * 1_000_000n),
      traceId: "a".repeat(32), attributes: [attribute("event.name", "codex.sse_event"),
        attribute("conversation.id", session), attribute("model", model), attribute("turn.id", "first-response"),
        attribute("request_id", "bounded-request"), attribute("input_token_count", complete ? 25 : 19),
        ...(complete ? [attribute("output_token_count", 4)] : [])] }] }] }] },
  { source: "codex", transportPath: "/v1/logs" }).events[0]!.event;
  event.cacheReadTokens = complete ? 8 : 0;
  event.metadata.cached_token_count = event.cacheReadTokens;
  if (complete) {
    event.cacheCreationTokens = 5;
    event.costUsd = .125;
    event.costKind = "reported";
    Object.assign(event.metadata, { "gen_ai.usage.cache_creation_input_tokens": 5, cost_usd: .125 });
  }
  assert.equal(event.outputTokens, complete ? 4 : undefined, "absence differs from a reported zero");
  return event;
}
const record = (type: string, payload: object, offset = 6000) =>
  JSON.stringify({ type, payload, timestamp: new Date(at + offset).toISOString() }) + "\n";
const tokens = (input: number, output: number, cache: number, offset = 6000) => record("event_msg", {
  type: "token_count", info: { total_token_usage: { input_tokens: input,
    output_tokens: output, cached_input_tokens: cache } },
}, offset);

async function fixture(shape: "held-SSE" | "ACKed-SSE" | "native-first" | "restart") {
  const directory = path.join(root, shape);
  const sessions = path.join(directory, "sessions");
  const day = path.join(sessions, ...new Date(at).toISOString().slice(0, 10).split("-"));
  fs.mkdirSync(day, { recursive: true });
  const ledger = path.join(directory, "ledger.sqlite");
  let now = new Date(at + 2000);
  const options = { workspaceId: "11111111-1111-4111-8111-111111111111", deviceId: "bounded-accounting",
    enrollmentNow: () => new Date(at - 1_000_000), delivery: { enabled: true, now: () => now } };
  let buffer = new LocalEventBuffer(ledger, options);
  let tailer = new RolloutTailer(buffer, sessions, () => []);
  const file = path.join(day, `rollout-bounded-${session}.jsonl`);
  const frozen = new Map<string, { bytes: string; event: any }>();
  const scans: unknown[] = [];
  function freeze() {
    now = new Date(now.getTime() + 123_000);
    const lease = buffer.delivery.lease({ now });
    assert.equal(lease.locallyDead, 0);
    for (const item of lease.items) {
      const previous = frozen.get(item.deliveryId);
      if (previous) assert.equal(item.envelopeJson, previous.bytes);
      else frozen.set(item.deliveryId, { bytes: item.envelopeJson, event: item.envelope.event });
    }
    assert.equal(buffer.delivery.acknowledge(lease.leaseId, lease.items.map(item => item.deliveryId), now).locallyDead, 0);
  }
  function amounts() {
    return Object.fromEntries(fields.map(field => [field, [...frozen.values()]
      .reduce((sum, item) => sum + (item.event[field] ?? 0), 0)]));
  }
  async function scan() {
    const budget = new CaptureWorkBudget();
    const result = await tailer.scan({ scope: "recent", now: new Date(at + 65_000),
      automatic: { phase: "capture", budget } });
    assert.equal(result.parseErrors, 0);
    assert.equal(result.readErrors, 0);
    assert.equal(result.skippedRecords ?? 0, 0, "ordinary deferred records are never losses");
    assert.ok(result.slicesCommitted <= 4, "main's per-file cadence bound survives the merge");
    assert.ok(result.bytesRead <= AUTOMATIC_CAPTURE_LIMITS.maxBytes);
    assert.ok(result.recordsParsed <= AUTOMATIC_CAPTURE_LIMITS.maxRecords);
    assert.ok(result.eventsAppended <= AUTOMATIC_CAPTURE_LIMITS.maxEvents);
    scans.push({ committed: offset(), deferred: result.bytesDeferred, slices: result.slicesCommitted,
      records: result.recordsParsed, bytes: result.bytesRead, budget: budget.status() });
    return result;
  }
  function offset() {
    return (buffer.database.prepare("select committed_offset as n from rollout_scan_state where file=?")
      .get(jsonlScanStateKey(file)) as { n: number } | undefined)?.n ?? 0;
  }
  try {
    installVirtualClock();
    const baseline = beginAutomaticCaptureBaseline(buffer.database, "codex", {
      startedAt: new Date(at - 2000).toISOString(), filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(buffer.database, "codex", {
      runId: baseline.latestRun!.runId, completedAt: new Date(at - 1000).toISOString() });
    // The later native update and a distinct response sit past four real slices.
    // Scheduling uses the existing virtual clock; filesystem, parsing and
    // event/cursor/outbox commits are real, with all production byte/record caps.
    fs.writeFileSync(file, record("session_meta", { id: session }) +
      record("turn_context", { turn_id: "first-response", model }) + tokens(0, 0, 0, 5000) + tokens(19, 2, 3) +
      record("fixture_ignored", { padding: "x".repeat(1000) }).repeat(300) + tokens(23, 3, 7, 6500) +
      record("turn_context", { turn_id: "second-response", model }, 10_000) + tokens(36, 6, 9, 11_000));
    if (shape !== "native-first") {
      assert.equal(buffer.append(sse(false)), true);
      if (shape === "ACKed-SSE" || shape === "restart") {
        freeze();
        assert.deepEqual(amounts(), { inputTokens: 19, outputTokens: 0, cacheReadTokens: 0,
          cacheCreationTokens: 0, costUsd: 0 });
        const partial = [...frozen.values()][0]!.event;
        assert.equal(partial.outputTokens, undefined, "an unknown output is never fabricated as zero");
        assert.equal(partial.cacheReadTokens, 0, "an explicit cache zero remains known");
      }
    }
    const first = await scan();
    assert.equal(first.slicesCommitted, 4);
    assert.ok(offset() > 0 && offset() < fs.statSync(file).size);
    assert.ok(first.bytesDeferred > 0 && !first.exhaustive, "a cap reports unfinished capture");
    freeze();
    assert.deepEqual(amounts(), { inputTokens: 19, outputTokens: 2, cacheReadTokens: 3,
      cacheCreationTokens: 0, costUsd: 0 }, "only the observed response prefix has been counted");
    if (shape === "native-first") assert.equal(buffer.append(sse(false)), true);
    if (shape === "restart") {
      const committed = offset();
      tailer.close(); buffer.close();
      buffer = new LocalEventBuffer(ledger, options);
      tailer = new RolloutTailer(buffer, sessions, () => []);
      assert.equal(offset(), committed, "the committed prefix survives re-opening");
    }
    assert.equal(buffer.append(sse(true)), true);
    freeze();
    assert.deepEqual(amounts(), { inputTokens: 25, outputTokens: 4, cacheReadTokens: 8,
      cacheCreationTokens: 5, costUsd: .125 }, "late, earlier-timestamped fields take response maxima");
    for (let cadence = 0; cadence < 12 && offset() < fs.statSync(file).size; cadence++) {
      await scan(); freeze();
    }
    assert.equal(offset(), fs.statSync(file).size, "deferred observations eventually commit");
    // Independent producer arithmetic: max({19,2,3},{23,3,7},{25,4,8,5,.125})
    // plus the distinct native response {13,3,2}. No accounting helper is the oracle.
    assert.deepEqual(amounts(), { inputTokens: 38, outputTokens: 7, cacheReadTokens: 10,
      cacheCreationTokens: 5, costUsd: .125 });
    for (const [id, item] of frozen) {
      assert.equal(item.event.metadata.usageSource === "capture_gap", false);
      const stored = buffer.database.prepare("select envelope_json as bytes from codex_named_captures where delivery_id=?")
        .get(id) as { bytes: string } | undefined;
      assert.equal(stored?.bytes, item.bytes, "later cadences preserve every frozen owner");
    }
    const unchanged = await scan();
    assert.equal(unchanged.bytesRead, 0);
    assert.equal(unchanged.eventsAppended, 0);
    outcomes.push({ shape, passed: true, totals: amounts(), frozenDeliveries: frozen.size, scans });
    completion.check(shape);
  } finally {
    restoreRealClock(); tailer.close(); buffer.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
}

async function main() {
  try {
    for (const shape of ["held-SSE", "ACKed-SSE", "native-first", "restart"] as const) await fixture(shape);
    console.log(JSON.stringify({ proof: "codex-bounded-response-accounting", outcomes }, null, 2));
    completion.complete();
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
