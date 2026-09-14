/**
 * Adversarial proof for issue 0075 (#144): aggregate repeated local admission
 * rejections.
 *
 * Uses only a temporary Plimsoll home, a fixture SQLite ledger, an ephemeral
 * loopback port, and an injected fake clock. It never reads or writes
 * installed tool config, the live ledger, launchd, or any running process.
 *
 * Run: pnpm proof:rejection-aggregation
 */
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { LOCAL_HTTP_LIMITS } from "../packages/collector-cli/src/http-boundary";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import type {
  CollectorServer,
  OtlpRecordRejectionDiagnostic,
  RejectionDiagnosticsCounters,
  RejectionSummaryLine,
} from "../packages/collector-cli/src/rejection-diagnostics";

type Check = { name: string; passed: boolean; detail: unknown };
type BurstResult = { status: number; bodyText: string };

const checks: Check[] = [];

function check(name: string, passed: boolean, detail: unknown) {
  checks.push({ name, passed, detail });
}

const SENTINEL_SOURCE = "SOURCE_HEADER_SENTINEL_9F3A";
const SENTINEL_BODY = "PAYLOAD_BODY_SENTINEL_B42C";
const SENTINEL_ORIGIN = "https://origin-sentinel-e77a.example";

const INTERVAL_MS = 60_000;
const T0 = 1_760_000_000_000;

const clock = { value: T0 };

function totalChanges(buffer: LocalEventBuffer) {
  return Number(
    (buffer.database.prepare("select total_changes() as n").get() as { n: number }).n,
  );
}

function parseRejection(bodyText: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(bodyText) as unknown;
    return parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

function isLiteralRejection(result: BurstResult, reason: string, status: number) {
  const parsed = parseRejection(result.bodyText);
  return (
    result.status === status &&
    parsed !== null &&
    parsed.error === "collector_request_rejected" &&
    parsed.reason === reason &&
    Object.keys(parsed).length === 2 &&
    Buffer.byteLength(result.bodyText) <= 128
  );
}

function conservation(c: RejectionDiagnosticsCounters) {
  const perReason = c.reasons.every(
    (row) =>
      row.rejected === row.emittedFirst + row.suppressed &&
      row.rejected === row.summarized + (row.openWindow?.count ?? 0),
  );
  const totals =
    c.totals.rejectedTotal === c.totals.emittedFirstTotal + c.totals.suppressedTotal &&
    c.totals.rejectedTotal ===
      c.totals.summarizedTotal +
        c.reasons.reduce((sum, row) => sum + (row.openWindow?.count ?? 0), 0);
  return { perReason, totals, ok: perReason && totals };
}

// ---------------------------------------------------------------------------
// Live HTTP client with keep-alive for burst fixtures
// ---------------------------------------------------------------------------

function makeAgent(concurrency: number) {
  return new http.Agent({
    keepAlive: true,
    maxSockets: concurrency,
    maxFreeSockets: concurrency,
  });
}

function oneRequest(
  agent: http.Agent,
  port: number,
  spec: {
    route: string;
    method?: string;
    headers?: Record<string, string>;
    body?: Buffer | string;
    /** Declared content-length override (fixture for oversize rejection). */
    declaredContentLength?: number;
  },
  tag = "",
): Promise<BurstResult> {
  const bodyBuffer = spec.body === undefined ? undefined : Buffer.isBuffer(spec.body) ? spec.body : Buffer.from(spec.body);
  const headers: Record<string, string> = { ...spec.headers };
  if (spec.declaredContentLength !== undefined) {
    headers["content-length"] = String(spec.declaredContentLength);
  } else if (bodyBuffer !== undefined) {
    headers["content-length"] = String(bodyBuffer.length);
  }
  return new Promise<BurstResult>((resolve, reject) => {
    const client = http.request(
      {
        host: "127.0.0.1",
        port,
        path: spec.route,
        method: spec.method ?? "POST",
        agent,
        headers,
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        response.on("end", () => {
          resolve({
            status: response.statusCode ?? 0,
            bodyText: Buffer.concat(chunks).toString("utf8"),
          });
        });
      },
    );
    client.setTimeout(45_000, () => client.destroy(new Error(`ProofBurstTimeout:${spec.route}${tag ? `:${tag}` : ""}`)));
    client.on("error", reject);
    if (bodyBuffer !== undefined) client.write(bodyBuffer);
    client.end();
  });
}

async function firePool(
  tasks: Array<() => Promise<void>>,
  concurrency: number,
) {
  let index = 0;
  const workers = Array.from({ length: Math.min(concurrency, tasks.length) }, async () => {
    while (index < tasks.length) {
      const task = tasks[index++]!;
      await task();
    }
  });
  await Promise.all(workers);
}

type Tally = Map<string, number>;

async function fireCategory(
  port: number,
  count: number,
  spec: Parameters<typeof oneRequest>[2] | (() => Parameters<typeof oneRequest>[2]),
  tally: Tally,
  expect: (result: BurstResult) => boolean,
  mismatches: Array<{ index: number; result: BurstResult }>,
  offsetRef: { value: number },
  concurrency = 96,
) {
  const phaseStarted = performance.now();
  console.error(`[phase] start count=${count}`);
  const agent = makeAgent(concurrency);
  try {
    await firePool(
      Array.from({ length: count }, () => () => {
        const index = offsetRef.value++;
        const resolved = typeof spec === "function" ? spec() : spec;
        return oneRequest(agent, port, resolved, `idx${index}`).then((result) => {
          const parsed = parseRejection(result.bodyText);
          const key =
            parsed?.error === "collector_request_rejected"
              ? `reject:${String(parsed.reason)}:${result.status}`
              : `${parsed === null ? "raw" : String(parsed.error)}:${result.status}`;
          tally.set(key, (tally.get(key) ?? 0) + 1);
          if (!expect(result) && mismatches.length < 5) mismatches.push({ index, result });
        });
      }),
      concurrency,
    );
  } finally {
    agent.destroy();
    console.error(`[phase] done in ${Math.round(performance.now() - phaseStarted)}ms`);
  }
}

function validControlEnvelope(sequence: number) {
  return JSON.stringify({
    resourceLogs: [
      {
        scopeLogs: [
          {
            logRecords: [
              {
                timeUnixNano: String(1_760_000_000_000_000_000n + BigInt(sequence)),
                severityText: "INFO",
                attributes: [
                  { key: "cwd", value: { stringValue: "/PROOF_TMP/control" } },
                  { key: "gen_ai.usage.input_tokens", value: { intValue: "3" } },
                  { key: "gen_ai.usage.output_tokens", value: { intValue: "7" } },
                ],
              },
            ],
          },
        ],
      },
    ],
  });
}

// ---------------------------------------------------------------------------
// Unit-level checks against the diagnostics module (fake clock, no HTTP)
// ---------------------------------------------------------------------------

type DiagnosticsModule = typeof import("../packages/collector-cli/src/rejection-diagnostics");
let rejectionDiagnostics: DiagnosticsModule | null = null;

async function unitChecks() {
  let moduleMissing = false;
  try {
    rejectionDiagnostics = await import("../packages/collector-cli/src/rejection-diagnostics");
  } catch {
    moduleMissing = true;
  }

  const intervalMsLoaded = rejectionDiagnostics?.REJECTION_SUMMARY_INTERVAL_MS;
  const capLoaded = rejectionDiagnostics?.REJECTION_COUNTER_CAP;
  check(
    "rejection_diagnostics_module_exists_with_fixed_interval",
    rejectionDiagnostics !== null &&
      intervalMsLoaded === INTERVAL_MS &&
      capLoaded === Number.MAX_SAFE_INTEGER,
    moduleMissing
      ? "module_missing: packages/collector-cli/src/rejection-diagnostics.ts"
      : {
          intervalMs: intervalMsLoaded ?? null,
          cap: capLoaded ?? null,
        },
  );
  if (!rejectionDiagnostics) return;

  const mod = rejectionDiagnostics;

  const clientClasses = [
    mod.classifyRejectionClient({ headers: { "x-plimsoll-source": "codex" }, socket: { remotePort: 55123 } }),
    mod.classifyRejectionClient({ headers: { "x-plimsoll-source": "claude_code" }, socket: { remotePort: 55124 } }),
    mod.classifyRejectionClient({ headers: { "user-agent": "OTel-OTLP-Exporter-Java/1.2" }, socket: { remotePort: 55125 } }),
    mod.classifyRejectionClient({ headers: { "user-agent": SENTINEL_SOURCE }, socket: { remotePort: 55126 } }),
  ];
  check(
    "rejection_client_identity_is_closed_content_free_and_fixed_cardinality",
    JSON.stringify(clientClasses) === JSON.stringify(["codex", "claude_code", "otlp_exporter", "unknown"]) &&
      !JSON.stringify(clientClasses).includes(SENTINEL_SOURCE),
    { clientClasses },
  );

  let identityNow = T0;
  const identityAgg = mod.createRejectionDiagnostics({ nowMs: () => identityNow });
  const identityFirst = identityAgg.observeRejection("compressed_body_too_large", "codex");
  identityNow += INTERVAL_MS;
  const identityBoundary = identityAgg.observeRejection("compressed_body_too_large", "claude_code");
  check(
    "compressed_body_summary_records_bounded_client_class_without_raw_headers",
    identityFirst.first && identityBoundary.summaries.length === 1 &&
      identityBoundary.summaries[0]?.clientClass === "codex" &&
      !JSON.stringify(identityBoundary).includes(SENTINEL_SOURCE),
    { identityFirst, identityBoundary },
  );

  const recordShapeAgg = mod.createRejectionDiagnostics({ nowMs: () => identityNow });
  const firstRecordShape = recordShapeAgg.observeRejection(
    "otlp_record_limit_exceeded",
    "codex",
    {
      recordCount: 640,
      recordArrays: { logRecords: 512, spans: 120, events: 8 },
      decodedBytes: 701_234,
    },
  );
  const suppressedRecordShape = recordShapeAgg.observeRejection(
    "otlp_record_limit_exceeded",
    "codex",
    {
      recordCount: 2_048,
      recordArrays: { logRecords: 1_024, spans: 1_000, events: 24 },
      decodedBytes: 1_901_111,
    },
  );
  const lastRecordShape = recordShapeAgg.observeRejection(
    "otlp_record_limit_exceeded",
    "codex",
    {
      recordCount: 1_024,
      recordArrays: { logRecords: 900, spans: 100, events: 24 },
      decodedBytes: 1_201_111,
    },
  );
  const recordShapeSummary = recordShapeAgg.flush()[0];
  check(
    "record_limit_summary_keeps_value_free_last_and_max_shape_by_closed_array_key",
    firstRecordShape.first === true &&
      suppressedRecordShape.first === false &&
      lastRecordShape.first === false &&
      recordShapeSummary?.reason === "otlp_record_limit_exceeded" &&
      recordShapeSummary.clientClass === "codex" &&
      recordShapeSummary.recordCountLast === 1_024 &&
      recordShapeSummary.recordCountMax === 2_048 &&
      recordShapeSummary.decodedBytesLast === 1_201_111 &&
      recordShapeSummary.decodedBytesMax === 1_901_111 &&
      JSON.stringify(recordShapeSummary.recordArraysLast) ===
        JSON.stringify({ logRecords: 900, spans: 100, events: 24 }) &&
      JSON.stringify(recordShapeSummary.recordArraysMax) ===
        JSON.stringify({ logRecords: 1_024, spans: 1_000, events: 24 }) &&
      !JSON.stringify(recordShapeSummary).includes(SENTINEL_SOURCE) &&
      Buffer.byteLength(JSON.stringify(recordShapeSummary)) <=
        mod.REJECTION_SUMMARY_LINE_MAX_BYTES,
    { firstRecordShape, suppressedRecordShape, lastRecordShape, recordShapeSummary },
  );

  const allRecordArrays = {
    logRecords: 100_000,
    spans: 100_000,
    metrics: 100_000,
    dataPoints: 100_000,
    events: 100_000,
    links: 100_000,
    exemplars: 100_000,
  };
  const fullKeyAgg = mod.createRejectionDiagnostics({ nowMs: () => identityNow });
  fullKeyAgg.observeRejection("otlp_record_limit_exceeded", "otlp_exporter", {
    recordCount: 100_000,
    recordArrays: allRecordArrays,
    decodedBytes: 2_097_152,
  });
  const fullKeySummary = fullKeyAgg.flush()[0];
  const worstCounterFullKeySummary = {
    ...fullKeySummary,
    count: Number.MAX_SAFE_INTEGER,
    suppressed: Number.MAX_SAFE_INTEGER,
  };
  check(
    "record_limit_summary_all_closed_keys_stay_inside_explicit_byte_ceiling",
    mod.REJECTION_SUMMARY_LINE_MAX_BYTES === 640 &&
      JSON.stringify(fullKeySummary?.recordArraysLast) === JSON.stringify(allRecordArrays) &&
      JSON.stringify(fullKeySummary?.recordArraysMax) === JSON.stringify(allRecordArrays) &&
      Buffer.byteLength(JSON.stringify(worstCounterFullKeySummary)) <=
        mod.REJECTION_SUMMARY_LINE_MAX_BYTES,
    {
      fixedByteCeiling: mod.REJECTION_SUMMARY_LINE_MAX_BYTES,
      worstLineBytes: Buffer.byteLength(JSON.stringify(worstCounterFullKeySummary)),
    },
  );

  // Every bounded reason must have a compile-time symbolic next action.
  const reasonEnumValues = Object.values(mod.HTTP_BOUNDARY_REASONS);
  const actionMap = mod.HTTP_REJECTION_NEXT_ACTIONS as Record<string, string>;
  const missingActions = reasonEnumValues.filter((reason) => !actionMap[reason]);
  const extraActions = Object.keys(actionMap).filter(
    (reason) => !reasonEnumValues.includes(reason as never),
  );
  check(
    "every_bounded_reason_has_exactly_one_symbolic_next_action",
    reasonEnumValues.length >= 20 &&
      missingActions.length === 0 &&
      extraActions.length === 0,
    { reasons: reasonEnumValues.length, missingActions, extraActions },
  );

  // Interval edges under a fake clock, no sleeps.
  let now = T0;
  const agg = mod.createRejectionDiagnostics({ nowMs: () => now });
  const first = agg.observeRejection("source_required");
  now = T0 + 1;
  const suppressedEarly = [agg.observeRejection("source_required"), agg.observeRejection("source_required")];
  now = T0 + INTERVAL_MS - 1;
  const justBeforeBoundary = agg.observeRejection("source_required");
  now = T0 + INTERVAL_MS;
  const atBoundary = agg.observeRejection("source_required");
  const boundarySummary = atBoundary.summaries[0];
  check(
    "fake_clock_interval_edges_emit_first_then_suppress_then_boundary_summary_and_first",
    first.first === true &&
      first.summaries.length === 0 &&
      suppressedEarly.every((entry) => entry.first === false && entry.summaries.length === 0) &&
      justBeforeBoundary.first === false &&
      justBeforeBoundary.summaries.length === 0 &&
      atBoundary.first === true &&
      atBoundary.summaries.length === 1 &&
      boundarySummary !== undefined &&
      boundarySummary.error === "collector_request_rejected_summary" &&
      boundarySummary.reason === "source_required" &&
      boundarySummary.count === 4 &&
      boundarySummary.suppressed === 3 &&
      boundarySummary.intervalMs === INTERVAL_MS &&
      actionMap[boundarySummary.reason] === boundarySummary.action &&
      Buffer.byteLength(JSON.stringify(boundarySummary)) <= 256,
    { first, suppressedEarly, justBeforeBoundary, atBoundary, boundarySummary },
  );

  // Concurrency: interleaved reasons across window boundaries conserve exactly.
  now = T0 + 10 * INTERVAL_MS;
  const mixed = mod.createRejectionDiagnostics({ nowMs: () => now });
  let conservedThroughout = true;
  for (let step = 0; step < 4_000; step += 1) {
    mixed.observeRejection(step % 3 === 0 ? "invalid_json" : step % 3 === 1 ? "host_not_allowed" : "source_not_allowed");
    if (step > 0 && step % 700 === 0) now += INTERVAL_MS;
    if (step % 97 === 0 && !conservation(mixed.counters()).ok) conservedThroughout = false;
  }
  const mixedCounters = mixed.counters();
  check(
    "interleaved_reasons_and_clock_jumps_keep_counter_conservation_exact",
    conservedThroughout && conservation(mixedCounters).ok,
    { counters: mixedCounters, conservedThroughout },
  );

  // Monotonicity: counters never decrease across observations.
  const beforeSnap = mixed.counters();
  mixed.observeRejection("storage_busy_retry");
  const afterSnap = mixed.counters();
  const monotonic =
    afterSnap.totals.rejectedTotal >= beforeSnap.totals.rejectedTotal &&
    afterSnap.totals.suppressedTotal >= beforeSnap.totals.suppressedTotal;
  check(
    "counters_are_monotonic_without_content_or_identifiers",
    monotonic &&
      !JSON.stringify(afterSnap).includes(SENTINEL_SOURCE) &&
      afterSnap.reasons.every((row) => typeof row.reason === "string"),
    { before: beforeSnap.totals, after: afterSnap.totals },
  );

  // Shutdown flush: at most one bounded summary per active reason.
  now = T0 + 100 * INTERVAL_MS;
  const flushAgg = mod.createRejectionDiagnostics({ nowMs: () => now });
  flushAgg.observeRejection("source_mismatch");
  for (let i = 0; i < 9; i += 1) flushAgg.observeRejection("source_mismatch");
  flushAgg.observeRejection("browser_origin_not_allowed");
  for (let i = 0; i < 4; i += 1) flushAgg.observeRejection("browser_origin_not_allowed");
  const flushed = flushAgg.flush();
  const flushedAgain = flushAgg.flush();
  const postFlush = flushAgg.counters();
  check(
    "shutdown_flush_emits_at_most_one_summary_per_active_reason_then_nothing",
    flushed.length === 2 &&
      new Set(flushed.map((line) => line.reason)).size === 2 &&
      flushed.every((line) => line.error === "collector_request_rejected_summary") &&
      flushedAgain.length === 0 &&
      conservation(postFlush).ok &&
      postFlush.reasons.every((row) => row.openWindow === null) &&
      postFlush.reasons.every((row) => row.rejected === row.summarized),
    { flushed, flushedAgain, totals: postFlush.totals },
  );

  // Counter overflow bounds: saturation without wraparound or conservation loss.
  now = T0 + 200 * INTERVAL_MS;
  const cap = Number.MAX_SAFE_INTEGER;
  const saturated = mod.createRejectionDiagnostics({
    nowMs: () => now,
    initialByReason: {
      source_required: {
        rejected: cap,
        suppressed: cap - 1,
        emittedFirst: 1,
        summarized: 0,
        openWindow: { count: cap, suppressed: cap - 1 },
      },
    },
  });
  const satObserved = saturated.observeRejection("source_required");
  const satFlush = saturated.flush();
  const satCounters = saturated.counters().reasons.find((row) => row.reason === "source_required");
  check(
    "counter_saturation_at_safe_integer_bound_preserves_conservation_and_line_budget",
    satObserved.first === false &&
      satObserved.summaries.length === 0 &&
      satFlush.length === 1 &&
      satFlush[0]?.count === cap &&
      satFlush[0]?.suppressed === cap - 1 &&
      satCounters?.rejected === cap &&
      satCounters?.emittedFirst === 1 &&
      satCounters?.suppressed === cap - 1 &&
      satCounters?.summarized === cap &&
      satCounters?.openWindow === null &&
      conservation(saturated.counters()).ok &&
      Buffer.byteLength(JSON.stringify(satFlush[0])) <= 256,
    { observed: satObserved, flushLine: satFlush[0], row: satCounters },
  );

  // Restart loses only ephemeral suppression state.
  now = T0 + 300 * INTERVAL_MS;
  const preRestart = mod.createRejectionDiagnostics({ nowMs: () => now });
  preRestart.observeRejection("decoded_body_too_large");
  preRestart.observeRejection("decoded_body_too_large");
  const restarted = mod.createRejectionDiagnostics({ nowMs: () => now });
  const restartCounters = restarted.counters();
  const restartObserve = restarted.observeRejection("decoded_body_too_large");
  check(
    "restart_resets_ephemeral_counters_and_suppression_without_losing_decisions",
    restartCounters.totals.rejectedTotal === 0 &&
      restartCounters.reasons.length === 0 &&
      restartObserve.first === true &&
      restartObserve.summaries.length === 0,
    { restartCounters, restartObserve },
  );

  // Accepted counters are keyed by the bounded producer-source enum only.
  const acceptAgg = mod.createRejectionDiagnostics({ nowMs: () => now });
  acceptAgg.recordAccepted("codex");
  acceptAgg.recordAccepted("claude_code");
  acceptAgg.recordAccepted("codex");
  const acceptedSnapshot = acceptAgg.counters();
  check(
    "accepted_counters_are_keyed_by_bounded_source_enum",
    acceptedSnapshot.acceptedBySource.codex === 2 &&
      acceptedSnapshot.acceptedBySource.claude_code === 1 &&
      acceptedSnapshot.totals.acceptedTotal === 3,
    acceptedSnapshot.acceptedBySource,
  );
}

// ---------------------------------------------------------------------------
// Integration proof
// ---------------------------------------------------------------------------

async function integrationChecks() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-rejection-agg-"));
  process.env.PLIMSOLL_HOME = tempDir;
  const ledgerPath = path.join(tempDir, "proof-ledger.sqlite");
  const buffer = new LocalEventBuffer(ledgerPath);
  const serverOptions = { diagnosticsNowMs: () => clock.value } as Parameters<
    typeof createCollectorServer
  >[2];
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, serverOptions) as CollectorServer;

  const warnings: string[] = [];
  const originalWarn = console.warn;
  console.warn = (...values: unknown[]) => warnings.push(values.map(String).join(" "));
  const statusBodies: string[] = [];

  const expectedTally: Record<string, number> = {};
  const bumpExpected = (key: string, count: number) =>
    (expectedTally[key] = (expectedTally[key] ?? 0) + count);

  try {
    await unitChecks();

    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;

    // Byte-for-byte boundary equivalence: one representative request per
    // category gets the exact existing literal response.
    const literalAgent = makeAgent(8);
    const literals: Array<{ name: string; spec: Parameters<typeof oneRequest>[2]; reason: string; status: number }> = [
      {
        name: "missing_source",
        spec: { route: "/v1/logs", body: "{}" },
        reason: "source_required",
        status: 401,
      },
      {
        name: "wrong_source",
        spec: { route: "/v1/traces", body: "{}", headers: { "x-plimsoll-source": SENTINEL_SOURCE } },
        reason: "source_not_allowed",
        status: 401,
      },
      {
        name: "swapped_hook_source",
        spec: { route: "/hooks/claude-code", body: "{}", headers: { "x-plimsoll-source": "codex" } },
        reason: "source_mismatch",
        status: 401,
      },
      {
        name: "browser_origin",
        spec: {
          route: "/v1/logs",
          body: "{}",
          headers: { origin: SENTINEL_ORIGIN, "x-plimsoll-source": "codex" },
        },
        reason: "browser_origin_not_allowed",
        status: 403,
      },
      {
        name: "malformed_json",
        spec: {
          route: "/v1/logs",
          body: `{nope ${SENTINEL_BODY}`,
          headers: { "x-plimsoll-source": "codex" },
        },
        reason: "invalid_json",
        status: 400,
      },
      {
        name: "oversized_declared",
        spec: {
          route: "/v1/logs",
          headers: { "x-plimsoll-source": "codex" },
          declaredContentLength: LOCAL_HTTP_LIMITS.compressedBodyBytes + 1,
        },
        reason: "compressed_body_too_large",
        status: 413,
      },
    ];
    clock.value = T0;
    let literalAllExact = true;
    for (const entry of literals) {
      const result = await oneRequest(literalAgent, port, entry.spec);
      const exact =
        isLiteralRejection(result, entry.reason, entry.status) &&
        result.bodyText ===
          JSON.stringify({ error: "collector_request_rejected", reason: entry.reason });
      if (!exact) {
        literalAllExact = false;
        check(`literal_boundary_${entry.name}_byte_for_byte`, false, result);
      }
    }
    literalAgent.destroy();
    check(
      "existing_literal_rejections_byte_for_byte_at_http_boundary_before_aggregation",
      literalAllExact,
      { categories: literals.map((entry) => entry.name) },
    );

    const changesAtBaseline = totalChanges(buffer);
    const gcIfAvailable = () => {
      const gc = (globalThis as { gc?: () => void }).gc;
      if (typeof gc === "function") {
        gc();
        gc();
      }
    };
    // Retained heap (not raw RSS) is the quantity that can prove per-request
    // retention or unbounded buffer growth. Raw RSS in this single-process
    // fixture is dominated by transient allocator high-water from 108k
    // client+server requests and grows linearly on Node >= 22 even when the
    // collector retains nothing (isolated two-wave child measurement:
    // wave-1 RSS +188 MB vs wave-2 +10 MB, retained heap ~0). GC is forced
    // via --expose-gc when the runner provides it.
    gcIfAvailable();
    const heapUsedBefore = process.memoryUsage().heapUsed;
    const rssBefore = process.memoryUsage().rss;
    const cpuBefore = process.cpuUsage();

    // CI runners cannot absorb the full 108k-request storm (hosted macos-14
    // measured 29s for the first 30k phase then loopback ETIMEDOUT). The
    // properties under proof are scale-invariant; REJECTION_PROOF_SCALE < 1
    // runs the identical category matrix smaller and stamps the scale into the
    // burst receipt. Full scale (1) remains the local/audit bar.
    const STORM_SCALE = Math.min(1, Math.max(0.01, Number(process.env.REJECTION_PROOF_SCALE ?? "1") || 1));
    const scaled = (n: number) => Math.max(200, Math.round(n * STORM_SCALE));
    const N_BUSY = scaled(3_000);
    const N_CONTROLS = scaled(2_000);

    // ---- Storm phase: frozen fake clock => exactly one log line per reason.
    clock.value = T0;
    const tally: Tally = new Map();
    const mismatches: Array<{ index: number; result: BurstResult }> = [];
    const offsetRef = { value: 0 };

    bumpExpected("reject:source_required:401", scaled(30_000));
    await fireCategory(
      port,
      scaled(30_000),
      { route: "/v1/logs", body: "{}" },
      tally,
      (result) => isLiteralRejection(result, "source_required", 401),
      mismatches,
      offsetRef,
    );

    // /status responsiveness probes while the storm continues.
    bumpExpected("reject:source_not_allowed:401", scaled(25_000));
    const statusProbePromise = (async () => {
      const probeAgent = makeAgent(4);
      const probes: Array<{ elapsedMs: number; ok: boolean; hasAdmission: boolean; conserved: boolean }> = [];
      for (let i = 0; i < 5; i += 1) {
        const started = performance.now();
        const result = await oneRequest(probeAgent, port, { route: "/status", method: "GET" });
        const elapsedMs = performance.now() - started;
        const parsed = parseRejection(result.bodyText) as Record<string, unknown>;
        const admission = parsed?.httpAdmission as RejectionDiagnosticsCounters | undefined;
        const conserved = admission ? conservation(admission).ok : false;
        probes.push({ elapsedMs, ok: result.status === 200, hasAdmission: Boolean(admission), conserved });
        statusBodies.push(result.bodyText);
      }
      probeAgent.destroy();
      return probes;
    })();

    await fireCategory(
      port,
      scaled(25_000),
      { route: "/v1/traces", body: "{}", headers: { "x-plimsoll-source": SENTINEL_SOURCE } },
      tally,
      (result) => isLiteralRejection(result, "source_not_allowed", 401),
      mismatches,
      offsetRef,
    );
    const statusProbes = await statusProbePromise;

    bumpExpected("reject:source_mismatch:401", scaled(15_000));
    await fireCategory(
      port,
      scaled(15_000),
      { route: "/hooks/claude-code", body: "{}", headers: { "x-plimsoll-source": "codex" } },
      tally,
      (result) => isLiteralRejection(result, "source_mismatch", 401),
      mismatches,
      offsetRef,
    );

    bumpExpected("reject:browser_origin_not_allowed:403", scaled(12_000));
    await fireCategory(
      port,
      scaled(12_000),
      {
        route: "/v1/logs",
        body: "{}",
        headers: { origin: SENTINEL_ORIGIN, "x-plimsoll-source": "codex" },
      },
      tally,
      (result) => isLiteralRejection(result, "browser_origin_not_allowed", 403),
      mismatches,
      offsetRef,
    );

    bumpExpected("reject:invalid_json:400", scaled(12_000));
    await fireCategory(
      port,
      scaled(12_000),
      {
        route: "/v1/logs",
        body: `{nope ${SENTINEL_BODY}`,
        headers: { "x-plimsoll-source": "codex" },
      },
      tally,
      (result) => isLiteralRejection(result, "invalid_json", 400),
      mismatches,
      offsetRef,
    );

    bumpExpected("reject:compressed_body_too_large:413", scaled(6_000));
    await fireCategory(
      port,
      scaled(6_000),
      {
        route: "/v1/logs",
        headers: { "x-plimsoll-source": "codex" },
        declaredContentLength: LOCAL_HTTP_LIMITS.compressedBodyBytes + 1,
      },
      tally,
      (result) => isLiteralRejection(result, "compressed_body_too_large", 413),
      mismatches,
      offsetRef,
    );

    const changesAfterStorm = totalChanges(buffer);

    // ---- Interval-edge integration: advance the fake clock across the
    // boundary; the next rejection closes the old window (summary) and opens
    // a new one (first).
    clock.value = T0 + INTERVAL_MS;
    const edgeAgent = makeAgent(8);
    for (let i = 0; i < 100; i += 1) {
      const result = await oneRequest(edgeAgent, port, { route: "/v1/logs", body: "{}" });
      if (!isLiteralRejection(result, "source_required", 401) && mismatches.length < 5) {
        mismatches.push({ index: -1, result });
      }
      bumpExpected("reject:source_required:401", 1);
      tally.set("reject:source_required:401", (tally.get("reject:source_required:401") ?? 0) + 1);
    }
    edgeAgent.destroy();
    const changesAfterEdge = totalChanges(buffer);

    // ---- Rate-limited class via genuine SQLITE_BUSY contention.
    const priorBusyTimeout = Number(
      buffer.database.pragma("busy_timeout", { simple: true }) as number,
    );
    // The busy handler blocks the collector's event loop for the timeout on
    // every contended write, so the fixture keeps the wait at the 1 ms floor:
    // contention is genuine, the phase stays fast.
    buffer.database.pragma("busy_timeout = 1");
    const blocker = new Database(ledgerPath);
    blocker.exec("BEGIN EXCLUSIVE");
    bumpExpected("reject:storage_busy_retry:503", N_BUSY);
    await fireCategory(
      port,
      N_BUSY,
      { route: "/v1/logs", body: "{}", headers: { "x-plimsoll-source": "codex" } },
      tally,
      (result) => isLiteralRejection(result, "storage_busy_retry", 503),
      mismatches,
      offsetRef,
      32,
    );
    blocker.exec("ROLLBACK");
    blocker.close();
    buffer.database.pragma(`busy_timeout = ${priorBusyTimeout}`);
    const changesAfterBusy = totalChanges(buffer);

    // ---- Valid controls remain accepted and are the only ledger mutation.
    const controlAgent = makeAgent(64);
    let controlSequence = 0;
    let controlsAccepted = 0;
    await firePool(
      Array.from({ length: N_CONTROLS }, () => async () => {
        const result = await oneRequest(controlAgent, port, {
          route: "/v1/logs",
          body: validControlEnvelope(controlSequence++),
          headers: { "x-plimsoll-source": "codex" },
        });
        const parsed = parseRejection(result.bodyText) as Record<string, unknown>;
        if (result.status === 202 && parsed?.accepted === true) controlsAccepted += 1;
      }),
      64,
    );
    controlAgent.destroy();
    const changesAfterControls = totalChanges(buffer);
    const eventsInLedger = Number(
      (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n,
    );

    const cpuUsed = process.cpuUsage(cpuBefore);
    const cpuUsedMs = (cpuUsed.user + cpuUsed.system) / 1_000;
    gcIfAvailable();
    const heapUsedAfter = process.memoryUsage().heapUsed;
    const heapGrowthBytes = Math.max(0, heapUsedAfter - heapUsedBefore);
    const rssGrowthBytes = Math.max(0, process.memoryUsage().rss - rssBefore);
    const totalRequests = offsetRef.value + 100 + N_BUSY + N_CONTROLS;

    // ---- Assertions over the whole run.
    const tallyMatches =
      tally.size === Object.keys(expectedTally).length &&
      [...tally.entries()].every(([key, count]) => expectedTally[key] === count);

    check(
      "burst_full_category_matrix_every_invalid_request_kept_existing_literal_rejection",
      totalRequests >= scaled(100_000) &&
        tallyMatches &&
        mismatches.length === 0,
      { totalRequests, stormScale: STORM_SCALE, tally: Object.fromEntries(tally), expected: expectedTally, mismatches },
    );

    check(
      "invalid_categories_cause_zero_ledger_mutation_and_valid_controls_are_sole_writes",
      changesAfterStorm === changesAtBaseline &&
        changesAfterEdge === changesAtBaseline &&
        changesAfterBusy === changesAtBaseline &&
        changesAfterControls > changesAtBaseline &&
        controlsAccepted === N_CONTROLS &&
        eventsInLedger === N_CONTROLS,
      {
        baseline: changesAtBaseline,
        afterStorm: changesAfterStorm,
        afterEdge: changesAfterEdge,
        afterBusy: changesAfterBusy,
        afterControls: changesAfterControls,
        controlsAccepted,
        eventsInLedger,
      },
    );

    // ---- Log writes bounded by reason classes plus interval summaries.
    const parsedWarnings = warnings.map((line) => {
      try {
        const parsed = JSON.parse(line) as Record<string, unknown>;
        return {
          kind: parsed.error,
          reason: parsed.reason,
          count: typeof parsed.count === "number" ? parsed.count : null,
        };
      } catch {
        return { kind: "unparseable", reason: line.slice(0, 24), count: null };
      }
    });
    const firstLines = parsedWarnings.filter((entry) => entry.kind === "collector_request_rejected");
    const summaryLines = parsedWarnings.filter((entry) => entry.kind === "collector_request_rejected_summary");
    const distinctRejectedReasons = new Set([
      ...[...tally.keys()].map((key) => key.replace(/^reject:/, "").replace(/:\d+$/, "")),
    ]);
    const rejectedPerReason = new Map<string, number>();
    for (const [key, count] of tally) {
      const reason = key.replace(/^reject:/, "").replace(/:\d+$/, "");
      rejectedPerReason.set(reason, (rejectedPerReason.get(reason) ?? 0) + count);
    }
    const firstsPerReason = new Map<string, number>();
    for (const entry of firstLines) {
      firstsPerReason.set(String(entry.reason), (firstsPerReason.get(String(entry.reason)) ?? 0) + 1);
    }
    // source_required crosses one interval edge (summary + new first); every
    // other reason stays inside its single frozen-clock window.
    const expectedFirsts = new Map<string, number>(
      [...distinctRejectedReasons].map((reason) => [reason, reason === "source_required" ? 2 : 1]),
    );
    // Window expiry is lazy: the single observation past the boundary closes
    // every window aged past it. All storm windows opened at T0, so exactly
    // one summary per aged reason is emitted there; the storage_busy_retry
    // window opened later (frozen clock) and only reports at shutdown flush.
    // source_required's aged window closed at the edge BEFORE its 100
    // post-edge rejections, which stay open until the shutdown flush — its
    // closed-window count is literal(1) + storm(30000), not its lifetime
    // rejected total.
    const agedReasons = [...distinctRejectedReasons].filter((reason) => reason !== "storage_busy_retry");
    const expectedClosedWindowCount = (reason: string) =>
      reason === "source_required"
        ? (rejectedPerReason.get(reason) ?? 0) - 100 + 1
        : (rejectedPerReason.get(reason) ?? 0) + 1;
    check(
      "log_writes_bounded_by_reason_classes_plus_interval_summaries_independent_of_request_count",
      firstsPerReason.size === distinctRejectedReasons.size &&
        [...expectedFirsts].every(([reason, count]) => firstsPerReason.get(reason) === count) &&
        firstLines.length === distinctRejectedReasons.size + 1 &&
        summaryLines.length === agedReasons.length &&
        agedReasons.every((reason) => {
          const line = summaryLines.find((entry) => entry.reason === reason);
          return (
            line !== undefined &&
            line.count === expectedClosedWindowCount(reason) &&
            line.kind === "collector_request_rejected_summary"
          );
        }) &&
        warnings.length === firstLines.length + summaryLines.length,
      {
        requests: totalRequests,
        warningCount: warnings.length,
        firstLines: firstLines.length,
        summaryLines: summaryLines.map((entry) => ({ reason: entry.reason, count: entry.count })),
        firstsPerReason: Object.fromEntries(firstsPerReason),
        rejectedPerReason: Object.fromEntries(rejectedPerReason),
        agedReasons: agedReasons.sort(),
      },
    );

    check(
      "log_lines_stay_inside_fixed_byte_ceiling_and_value_free",
      warnings.every((line) => Buffer.byteLength(line) <= 256) &&
        SENTINELS_ABSENT(warnings.join("\n")) &&
        SENTINELS_ABSENT(statusBodies.join("\n")),
      {
        maxLineBytes: Math.max(...warnings.map((line) => Buffer.byteLength(line))),
        statusProbes: statusBodies.length,
      },
    );

    check(
      "status_endpoint_stays_fast_and_carries_conserved_in_memory_counters_during_storm",
      statusProbes.every(
        (probe) =>
          probe.ok && probe.hasAdmission && probe.conserved && probe.elapsedMs <= 250,
      ),
      statusProbes,
    );

    // The 108k-request fixture shares one process for client and server, so
    // the CPU ceiling bounds total fixture work and the retained-heap ceiling
    // fails loudly on per-request retention or unbounded buffer growth while
    // ignoring allocator churn. Raw RSS growth is reported but not asserted.
    check(
      "cpu_retained_heap_log_byte_ceilings_hold_across_full_burst",
      cpuUsedMs <= 90_000 && heapGrowthBytes <= 160 * 1024 * 1024,
      { cpuUsedMs: Math.round(cpuUsedMs), heapGrowthBytes, rssGrowthBytes, totalRequests },
    );

    // ---- Counter conservation at the live boundary (before shutdown).
    const diagnosticsApi = server.plimsollHttpDiagnostics ?? null;
    if (!diagnosticsApi) {
      check("live_counter_conservation_holds_across_interval_edge_before_shutdown_flush", false,
        "plimsollHttpDiagnostics_missing_from_server");
      check("server_shutdown_style_flush_emits_one_summary_per_active_reason_exactly_once", false,
        "plimsollHttpDiagnostics_missing_from_server");
      return;
    }
    const liveCounters = diagnosticsApi.counters();
    check(
      "live_counter_conservation_holds_across_interval_edge_before_shutdown_flush",
      conservation(liveCounters).ok &&
        liveCounters.reasons.find((row) => row.reason === "source_required")?.rejected ===
          scaled(30_000) + 1 + 100 &&
        liveCounters.acceptedBySource.codex === N_CONTROLS &&
        liveCounters.totals.acceptedTotal === N_CONTROLS,
      {
        totals: liveCounters.totals,
        sourceRequired: liveCounters.reasons.find((row) => row.reason === "source_required"),
        acceptedBySource: liveCounters.acceptedBySource,
      },
    );

    // ---- Shutdown flush: at most one bounded summary per active reason.
    const openReasons = liveCounters.reasons.filter((row) => row.openWindow !== null).length;
    const flushed = diagnosticsApi.flush();
    const flushedSecondTime = diagnosticsApi.flush();
    const postFlushCounters = diagnosticsApi.counters();
    check(
      "server_shutdown_style_flush_emits_one_summary_per_active_reason_exactly_once",
      flushed.length === openReasons &&
        new Set(flushed.map((line) => line.reason)).size === flushed.length &&
        flushed.every((line) => line.error === "collector_request_rejected_summary") &&
        flushedSecondTime.length === 0 &&
        conservation(postFlushCounters).ok &&
        postFlushCounters.reasons.every((row) => row.openWindow === null) &&
        postFlushCounters.totals.rejectedTotal === postFlushCounters.totals.summarizedTotal,
      {
        openReasons,
        flushedCount: flushed.length,
        secondFlushCount: flushedSecondTime.length,
        totals: postFlushCounters.totals,
        sample: flushed.find((line) => line.reason === "source_required") ?? null,
      },
    );
  } finally {
    console.warn = originalWarn;
    if (server.listening) {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    buffer.close();
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}


// ---------------------------------------------------------------------------
// Route classification of a measured window (bead eco-6hoxj.74.1, review F3)
//
// Its own home, ledger, port and fake clock so it cannot perturb the fixture
// above. Everything runs through the real server code path: real loopback
// HTTP, genuine SQLITE_BUSY contention from an exclusive writer, the real
// emitter, and the real `plimsollHttpDiagnostics` surface. The hook route
// answers 503 rather than spooling because this server's env sets the spool
// kill switch, which is the shipped configuration in which an operator sees
// busy 503s on a hook route at all.
// ---------------------------------------------------------------------------

const ROUTE_T0 = T0 + 900 * INTERVAL_MS;

async function routeClassificationChecks() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-rejection-route-"));
  const ledgerPath = path.join(tempDir, "route-ledger.sqlite");
  process.env.PLIMSOLL_HOME = tempDir;
  const buffer = new LocalEventBuffer(ledgerPath);
  const routeClock = { value: ROUTE_T0 };
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, {
    diagnosticsNowMs: () => routeClock.value,
    env: { ...process.env, PLIMSOLL_HOME: tempDir, PLIMSOLL_HOOK_SPOOL: "off" },
  } as Parameters<typeof createCollectorServer>[2]);

  const warnings: string[] = [];
  const originalWarn = console.warn;
  console.warn = (...values: unknown[]) => warnings.push(values.map(String).join(" "));

  const parsedWarnings = () =>
    warnings.map((line) => {
      try {
        return JSON.parse(line) as Record<string, unknown>;
      } catch {
        return { error: "unparseable", line } as Record<string, unknown>;
      }
    });
  const firstLines = () =>
    parsedWarnings().filter((entry) => entry.error === "collector_request_rejected");
  const summaryLines = () =>
    parsedWarnings().filter((entry) => entry.error === "collector_request_rejected_summary");

  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;

    // Genuine contention: an exclusive writer holds the ledger for the phase,
    // and the collector's busy handler waits the 1 ms floor per contended
    // write so the phase stays fast.
    const priorBusyTimeout = Number(
      buffer.database.pragma("busy_timeout", { simple: true }) as number,
    );
    buffer.database.pragma("busy_timeout = 1");
    const blocker = new Database(ledgerPath);
    blocker.exec("BEGIN EXCLUSIVE");

    const agent = makeAgent(4);
    const busyPost = (route: string) =>
      oneRequest(agent, port, {
        route,
        body: "{}",
        headers: { "x-plimsoll-source": "claude_code" },
      });

    // One 60 s window, one source, three busy 503s across two routes: the
    // exact shape the reviewer measured (F3) — hooks x2 then OTLP x1.
    const windowResults = [
      await busyPost("/hooks/claude-code"),
      await busyPost("/hooks/claude-code"),
      await busyPost("/v1/logs"),
    ];
    const allBusy = windowResults.every((result) =>
      isLiteralRejection(result, "storage_busy_retry", 503),
    );
    const windowFirsts = firstLines();

    // The observation past the boundary closes the window: its summary is
    // printed by the real emitter, ahead of the new window's first line.
    routeClock.value = ROUTE_T0 + INTERVAL_MS;
    const nextWindowResult = await busyPost("/v1/logs");
    const closed = summaryLines()[0] as unknown as RejectionSummaryLine | undefined;
    const closedRoutes = (closed?.routes ?? {}) as Record<string, number>;
    const routeSum = Object.values(closedRoutes).reduce((sum, value) => sum + value, 0);

    check(
      "busy_window_classifies_every_503_by_route_with_exact_per_route_counts",
      allBusy &&
        isLiteralRejection(nextWindowResult, "storage_busy_retry", 503) &&
        closed !== undefined &&
        closed.error === "collector_request_rejected_summary" &&
        closed.reason === "storage_busy_retry" &&
        closed.clientClass === "claude_code" &&
        closed.count === 3 &&
        closed.suppressed === 2 &&
        closedRoutes["/hooks/claude-code"] === 2 &&
        closedRoutes.otlp === 1 &&
        Object.keys(closedRoutes).length === 2 &&
        routeSum === closed.count,
      { closed, routeSum, statuses: windowResults.map((result) => result.status) },
    );

    // The defect itself: one first line for the window, naming one route. The
    // summary above is what classifies the other two 503s.
    check(
      "busy_window_prints_one_first_line_naming_one_route_and_the_summary_covers_the_rest",
      windowFirsts.length === 1 &&
        windowFirsts[0]?.route === "/hooks/claude-code" &&
        windowFirsts[0]?.reason === "storage_busy_retry" &&
        closed?.count === 3 &&
        (closed?.routes?.otlp ?? 0) === 1,
      { firstLines: windowFirsts, summaryRoutes: closed?.routes ?? null },
    );

    // The OTLP busy 503 is classified like the others rather than vanishing:
    // its own single-route window keeps the pre-change key order exactly and
    // appends `routes`.
    routeClock.value = ROUTE_T0 + 2 * INTERVAL_MS;
    const singleRoute = server.plimsollHttpDiagnostics.flush()[0];
    const { routes: singleRouteBreakdown, ...singleRouteWithoutRoutes } = singleRoute ?? ({} as RejectionSummaryLine);
    const preChangeLine = JSON.stringify(singleRouteWithoutRoutes);
    const emittedLine = JSON.stringify(singleRoute);
    check(
      "single_route_busy_window_keeps_the_pre_change_line_and_appends_only_its_route_split",
      singleRoute !== undefined &&
        singleRoute.reason === "storage_busy_retry" &&
        singleRoute.clientClass === "claude_code" &&
        singleRoute.count === 1 &&
        singleRoute.suppressed === 0 &&
        JSON.stringify(singleRouteBreakdown) === JSON.stringify({ otlp: 1 }) &&
        emittedLine === `${preChangeLine.slice(0, -1)},"routes":{"otlp":1}}` &&
        JSON.stringify(Object.keys(singleRouteWithoutRoutes)) ===
          JSON.stringify([
            "error",
            "reason",
            "clientClass",
            "count",
            "suppressed",
            "intervalMs",
            "action",
          ]),
      { emittedLine, preChangeLine },
    );

    blocker.exec("ROLLBACK");
    blocker.close();
    buffer.database.pragma(`busy_timeout = ${priorBusyTimeout}`);
    agent.destroy();

    // Bounds and vocabulary, off the live server: a route is only ever one of
    // the closed five, only the route-classified reasons carry a breakdown,
    // and a saturated full-vocabulary breakdown still fits the fixed ceiling.
    const mod = rejectionDiagnostics;
    if (!mod) {
      check("route_breakdown_is_bounded_by_the_closed_vocabulary_and_the_line_ceiling", false,
        "rejection_diagnostics_module_missing");
    } else {
      let routeNow = ROUTE_T0;
      // Read defensively so an aggregation that never exported the vocabulary
      // FAILS this check instead of throwing and taking the run with it (the
      // negative control runs exactly that aggregation).
      const vocabulary = Array.isArray(mod.REJECTION_ROUTES) ? mod.REJECTION_ROUTES : [];
      const vocabAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      for (const route of vocabulary) {
        vocabAgg.observeRejection("storage_busy_retry", "claude_code", undefined, route);
      }
      const vocabSummary = vocabAgg.flush()[0];
      const saturatedBreakdown = Object.fromEntries(
        vocabulary.map((route) => [route, Number.MAX_SAFE_INTEGER]),
      );
      const worstRouteLine = {
        ...vocabSummary,
        reason: "otlp_attribute_limit_exceeded",
        action: "reduce_envelope_cardinality",
        count: Number.MAX_SAFE_INTEGER,
        suppressed: Number.MAX_SAFE_INTEGER,
        routes: saturatedBreakdown,
      };
      // A reason outside ROUTE_CLASSIFIED_REASONS never carries a breakdown,
      // so the record-array maps and the route map are never on one line.
      const nonBusyAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      nonBusyAgg.observeRejection("otlp_record_limit_exceeded", "otlp_exporter", {
        recordCount: 100_000,
        recordArrays: { logRecords: 100_000 },
        decodedBytes: 2_097_152,
      }, "otlp");
      const nonBusySummary = nonBusyAgg.flush()[0];
      // Reverse direction: even an incorrect caller cannot attach record maps
      // to a busy summary. The emitter, not caller convention, owns the bound.
      //
      // The saturated diagnostic and the client class it is measured under are
      // read off the exported vocabularies, not pinned to today's members, so
      // an eighth record-array key or a longer client class grows the subject
      // here instead of growing the real worst line unmeasured. Read
      // defensively, and an empty read is carried into the predicate below, so
      // an aggregation that stopped exporting one of them FAILS this check
      // with the missing export named, instead of either throwing and taking
      // the run with it or passing on a collapsed subject.
      const recordArrayKeys = Array.isArray(mod.OTLP_RECORD_ARRAY_KEYS)
        ? (mod.OTLP_RECORD_ARRAY_KEYS as readonly string[])
        : [];
      const clientClassVocabulary = Array.isArray(mod.REJECTION_CLIENT_CLASSES)
        ? (mod.REJECTION_CLIENT_CLASSES as readonly string[])
        : [];
      const missingVocabularyExports = [
        ...(recordArrayKeys.length > 0 ? [] : ["OTLP_RECORD_ARRAY_KEYS"]),
        ...(clientClassVocabulary.length > 0 ? [] : ["REJECTION_CLIENT_CLASSES"]),
      ];
      // Longest first; equal lengths break on ascending lexical order, so the
      // subject is one deterministic class however the vocabulary is written.
      const worstClientClass = [...clientClassVocabulary].sort(
        (left, right) => right.length - left.length || (left < right ? -1 : 1),
      )[0] as never;
      const fullRecordDiagnostic: OtlpRecordRejectionDiagnostic = {
        recordCount: 100_000,
        recordArrays: Object.fromEntries(recordArrayKeys.map((key) => [key, 100_000])),
        decodedBytes: 2_097_152,
      };
      const recordFields = ["recordCountLast", "recordCountMax", "recordArraysLast",
        "recordArraysMax", "decodedBytesLast", "decodedBytesMax"] as const;
      const hasNoRecordFields = (line: RejectionSummaryLine | undefined) =>
        line !== undefined && recordFields.every((key) => !Object.hasOwn(line, key));
      const hasAllRecordFields = (line: RejectionSummaryLine | undefined) =>
        line !== undefined && recordFields.every((key) => Object.hasOwn(line, key));
      const mixedAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      for (const route of vocabulary) {
        mixedAgg.observeRejection("storage_busy_retry", "otlp_exporter", fullRecordDiagnostic, route);
      }
      const mixedSummary = mixedAgg.flush()[0];
      // Every counter on the busy branch's worst line is saturated, the
      // discard counter included: it is the only key on that line whose width
      // a future change can move without moving any other check's subject.
      const saturatedMixedLine = {
        ...mixedSummary,
        count: Number.MAX_SAFE_INTEGER,
        suppressed: Number.MAX_SAFE_INTEGER,
        recordDiagnosticsDiscarded: Number.MAX_SAFE_INTEGER,
        routes: saturatedBreakdown,
      };
      check(
        "route_breakdown_is_bounded_by_the_closed_vocabulary_and_the_line_ceiling",
        JSON.stringify(vocabulary) ===
          JSON.stringify(["/hooks/claude-code", "/hooks/codex", "/hooks/grok", "otlp", "other"]) &&
          JSON.stringify(vocabSummary?.routes) ===
            JSON.stringify(Object.fromEntries(vocabulary.map((route) => [route, 1]))) &&
          JSON.stringify(mod.ROUTE_CLASSIFIED_REASONS) === JSON.stringify(["storage_busy_retry"]) &&
          nonBusySummary?.routes === undefined &&
          nonBusySummary?.recordCountLast === 100_000 &&
          hasNoRecordFields(mixedSummary) &&
          Buffer.byteLength(JSON.stringify(worstRouteLine)) <=
            mod.REJECTION_SUMMARY_LINE_MAX_BYTES &&
          Buffer.byteLength(JSON.stringify(saturatedMixedLine)) <=
            mod.REJECTION_SUMMARY_LINE_MAX_BYTES,
        {
          routes: vocabulary,
          worstRouteLineBytes: Buffer.byteLength(JSON.stringify(worstRouteLine)),
          mixedLineBytes: Buffer.byteLength(JSON.stringify(saturatedMixedLine)),
          ceiling: mod.REJECTION_SUMMARY_LINE_MAX_BYTES,
        },
      );
      check(
        "busy_summary_rejects_record_maps_even_when_the_caller_supplies_both",
        hasNoRecordFields(mixedSummary) && mixedSummary?.count === vocabulary.length &&
          JSON.stringify(mixedSummary.routes) === JSON.stringify(Object.fromEntries(vocabulary.map((route) => [route, 1]))),
        { summary: mixedSummary },
      );
      // The ceiling has to bound the worst line the emitter can really produce.
      // Record-array maps and a route map are mutually exclusive by
      // construction — ingest builds no record statistics for a
      // route-classified reason and `closeWindow` strips them again — so the
      // worst mixture of diagnostic payload and line strings is the longest
      // reason/action pair that CAN carry record statistics, saturated across
      // every closed array key. Derived from the exported vocabularies, not
      // pinned to today's longest reason, so a longer one added later is
      // measured here instead of silently breaching 640 B in production.
      const nextActions = mod.HTTP_REJECTION_NEXT_ACTIONS as Record<string, string>;
      const routeClassifiedReasons = mod.ROUTE_CLASSIFIED_REASONS as readonly string[];
      const allReasons = mod.HTTP_BOUNDARY_REASONS as readonly string[];
      const worstRecordReason = allReasons
        .filter((reason) => !routeClassifiedReasons.includes(reason))
        .sort(
          (left, right) =>
            right.length + (nextActions[right]?.length ?? 0) -
              (left.length + (nextActions[left]?.length ?? 0)) || (left < right ? -1 : 1),
        )[0] as never;
      const worstRecordAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      // A route is offered too: a reason outside the route-classified set must
      // ignore it, so this one line proves both directions of the exclusivity.
      worstRecordAgg.observeRejection(worstRecordReason, worstClientClass, fullRecordDiagnostic, "otlp");
      const worstRecordSummary = worstRecordAgg.flush()[0];
      const saturatedRecordCarryingLine = {
        ...worstRecordSummary,
        count: Number.MAX_SAFE_INTEGER,
        suppressed: Number.MAX_SAFE_INTEGER,
      };
      const saturatedRecordCarryingBytes = Buffer.byteLength(JSON.stringify(saturatedRecordCarryingLine));
      // …and the exclusivity itself, swept over the whole reason vocabulary:
      // record statistics are stripped for every route-classified reason and
      // for no other, so no reason can carry both maps on one line.
      const exclusivityByReason = allReasons.map((reason) => {
        const agg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
        agg.observeRejection(reason as never, worstClientClass, fullRecordDiagnostic, "otlp");
        agg.observeRejection(reason as never, worstClientClass, fullRecordDiagnostic, "otlp");
        const line = agg.flush()[0];
        const routeClassified = routeClassifiedReasons.includes(reason);
        return {
          reason,
          routeClassified,
          recordStatsStripped: hasNoRecordFields(line),
          carriesRoutes: line?.routes !== undefined,
          lineBytes: Buffer.byteLength(JSON.stringify(line)),
        };
      });
      const exclusivityHolds = exclusivityByReason.every(
        (entry) =>
          entry.recordStatsStripped === entry.routeClassified &&
          entry.carriesRoutes === entry.routeClassified &&
          entry.lineBytes <= mod.REJECTION_SUMMARY_LINE_MAX_BYTES,
      );
      check(
        "mixed_diagnostic_saturated_summary_preserves_the_fixed_byte_ceiling",
        missingVocabularyExports.length === 0 &&
          mod.REJECTION_SUMMARY_LINE_MAX_BYTES === 640 &&
          // the subject really is a record-carrying line, not an empty one
          hasAllRecordFields(worstRecordSummary) &&
          worstRecordSummary?.recordCountMax === 100_000 &&
          worstRecordSummary.decodedBytesMax === 2_097_152 &&
          worstRecordSummary.routes === undefined &&
          saturatedRecordCarryingBytes <= mod.REJECTION_SUMMARY_LINE_MAX_BYTES &&
          exclusivityHolds &&
          exclusivityByReason.some((entry) => entry.routeClassified) &&
          exclusivityByReason.some((entry) => !entry.routeClassified),
        {
          missingVocabularyExports,
          worstRecordReason,
          worstClientClass: (worstClientClass as string | undefined) ?? null,
          recordArrayKeys,
          clientClassVocabulary,
          lineBytes: saturatedRecordCarryingBytes,
          headroom: mod.REJECTION_SUMMARY_LINE_MAX_BYTES - saturatedRecordCarryingBytes,
          ceiling: mod.REJECTION_SUMMARY_LINE_MAX_BYTES,
          strippedReasons: exclusivityByReason.filter((entry) => entry.recordStatsStripped).map((entry) => entry.reason),
          routeClassifiedReasons,
          worstExclusivityLineBytes: Math.max(...exclusivityByReason.map((entry) => entry.lineBytes)),
        },
      );

      // R2: a route-classified window can never emit record statistics, so it
      // must not build them either. The probe diagnostic counts every property
      // read `updateRecordStats`/`copyRecordDiagnostic` would make, so this
      // asserts the work is gone, not merely that the line comes out clean —
      // and the busy line is the no-diagnostic line plus one discard counter,
      // so ten thousand dropped diagnostics are stated instead of silent.
      const makeProbeDiagnostic = () => {
        const probe = { reads: 0 };
        const diagnostic = {
          get recordCount() { probe.reads += 1; return 100_000; },
          get recordArrays() {
            probe.reads += 1;
            return {
              logRecords: 100_000, spans: 100_000, metrics: 100_000,
              dataPoints: 100_000, events: 100_000, links: 100_000, exemplars: 100_000,
            };
          },
          get decodedBytes() { probe.reads += 1; return 2_097_152; },
        };
        return { probe, diagnostic };
      };
      const BUSY_DIAGNOSTIC_FEED = 10_000;
      const busyProbe = makeProbeDiagnostic();
      const busyWorkAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      for (let index = 0; index < BUSY_DIAGNOSTIC_FEED; index += 1) {
        busyWorkAgg.observeRejection("storage_busy_retry", "claude_code", busyProbe.diagnostic, "otlp");
      }
      const busyFedDiagnostics = busyWorkAgg.flush()[0];
      const busyQuietAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      for (let index = 0; index < BUSY_DIAGNOSTIC_FEED; index += 1) {
        busyQuietAgg.observeRejection("storage_busy_retry", "claude_code", undefined, "otlp");
      }
      const busyFedNone = busyQuietAgg.flush()[0];
      // Control: the same probe on a reason that CAN emit record statistics is
      // read on every observation, so zero reads above is the ingest gate and
      // not a probe blind to the work.
      const controlProbe = makeProbeDiagnostic();
      const controlAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      const CONTROL_FEED = 100;
      for (let index = 0; index < CONTROL_FEED; index += 1) {
        controlAgg.observeRejection("otlp_record_limit_exceeded", "otlp_exporter", controlProbe.diagnostic);
      }
      const controlSummary = controlAgg.flush()[0];
      const { recordDiagnosticsDiscarded, ...busyLineWithoutDiscardCounter } =
        busyFedDiagnostics ?? ({} as RejectionSummaryLine);
      check(
        "busy_window_does_no_record_statistics_work_however_many_diagnostics_arrive",
        busyProbe.probe.reads === 0 &&
          controlProbe.probe.reads >= CONTROL_FEED &&
          hasNoRecordFields(busyFedDiagnostics) &&
          hasAllRecordFields(controlSummary) &&
          busyFedDiagnostics?.count === BUSY_DIAGNOSTIC_FEED &&
          recordDiagnosticsDiscarded === BUSY_DIAGNOSTIC_FEED &&
          busyFedNone?.recordDiagnosticsDiscarded === undefined &&
          // the only difference the discarded diagnostics make to the line
          JSON.stringify(busyLineWithoutDiscardCounter) === JSON.stringify(busyFedNone) &&
          Buffer.byteLength(JSON.stringify(busyFedDiagnostics)) <= mod.REJECTION_SUMMARY_LINE_MAX_BYTES &&
          conservation(busyWorkAgg.counters()).ok,
        {
          fed: BUSY_DIAGNOSTIC_FEED,
          busyDiagnosticReads: busyProbe.probe.reads,
          controlDiagnosticReads: controlProbe.probe.reads,
          readsPerControlObservation: controlProbe.probe.reads / CONTROL_FEED,
          discarded: recordDiagnosticsDiscarded ?? null,
          busyLineFedNone: JSON.stringify(busyFedNone),
          busyLineFedDiagnostics: JSON.stringify(busyFedDiagnostics),
        },
      );
      const edgeAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      edgeAgg.observeRejection("storage_busy_retry", "codex", undefined, "otlp");
      edgeAgg.observeRejection("storage_busy_retry", "codex", fullRecordDiagnostic, "/hooks/codex");
      routeNow += INTERVAL_MS;
      const expiry = edgeAgg.observeRejection("storage_busy_retry", "codex", fullRecordDiagnostic, "/hooks/grok");
      const final = edgeAgg.flush()[0];
      check(
        "busy_diagnostic_exclusivity_covers_suppressed_expiry_and_shutdown_paths",
        expiry.summaries.length === 1 && hasNoRecordFields(expiry.summaries[0]) &&
          expiry.summaries[0]?.count === 2 && expiry.summaries[0]?.suppressed === 1 &&
          JSON.stringify(expiry.summaries[0]?.routes) === JSON.stringify({ "/hooks/codex": 1, otlp: 1 }) &&
          hasNoRecordFields(final) && final?.count === 1 && final.routes?.["/hooks/grok"] === 1 &&
          edgeAgg.flush().length === 0 && conservation(edgeAgg.counters()).ok,
        { expiry: expiry.summaries, final },
      );
      const seeded = mod.createRejectionDiagnostics({ nowMs: () => routeNow,
        initialByReason: { storage_busy_retry: { rejected: 3, suppressed: 2, emittedFirst: 1,
          summarized: 0, openWindow: { count: 3, suppressed: 2 } } },
      });
      seeded.observeRejection("storage_busy_retry", "unknown", fullRecordDiagnostic, "otlp");
      const seededSummary = seeded.flush()[0];
      check(
        "seeded_busy_counts_do_not_invent_routes_for_unattributed_history",
        hasNoRecordFields(seededSummary) && seededSummary?.count === 4 &&
          seededSummary.suppressed === 3 && JSON.stringify(seededSummary.routes) === JSON.stringify({ otlp: 1 }) &&
          conservation(seeded.counters()).ok,
        { summary: seededSummary, attribution: "three seeded observations have no route" },
      );

      // F3 (REVIEW-85): the exclusivity invariant's single source of truth is
      // frozen, not merely `readonly` in the type. Both the ingest gate and
      // the `closeWindow` guard read it live, so a runtime push between a
      // window's open and its close used to make the two disagree and strip
      // six record fields from that window with no counter and no log.
      const vocabularyBeforeMutation = JSON.stringify(mod.ROUTE_CLASSIFIED_REASONS);
      const mutableVocabulary = mod.ROUTE_CLASSIFIED_REASONS as unknown as string[];
      let freezeMutationTook = false;
      let freezeMutationError = "none";
      try {
        mutableVocabulary.push("otlp_record_limit_exceeded");
        freezeMutationTook = true;
      } catch (error) {
        freezeMutationError = error instanceof TypeError ? "TypeError" : String(error);
      }
      // If the array was not frozen the push landed. Undo it, so this check is
      // the only one that fails and every later check still measures the
      // module it meant to rather than a vocabulary this check widened.
      if (freezeMutationTook) mutableVocabulary.pop();
      check(
        "route_classified_reason_vocabulary_is_frozen_against_runtime_mutation",
        Object.isFrozen(mod.ROUTE_CLASSIFIED_REASONS) &&
          !freezeMutationTook &&
          freezeMutationError === "TypeError" &&
          JSON.stringify(mod.ROUTE_CLASSIFIED_REASONS) === vocabularyBeforeMutation &&
          JSON.stringify(mod.ROUTE_CLASSIFIED_REASONS) === JSON.stringify(["storage_busy_retry"]),
        {
          isFrozen: Object.isFrozen(mod.ROUTE_CLASSIFIED_REASONS),
          mutationTook: freezeMutationTook,
          mutationError: freezeMutationError,
          vocabulary: mod.ROUTE_CLASSIFIED_REASONS,
        },
      );

      // eco-6hoxj.111 (REVIEW-99 §3, residual 11): the ingest gate normalises
      // the reason, so the gate, the window key and the reason the window
      // state stores are one value and cannot disagree.
      //
      // A window is identified by `${reason}:${clientClass}`, a template
      // literal, which coerces. The gate used to classify the RAW argument
      // with `Array.prototype.includes` (SameValueZero), which does not. A
      // caller passing a `String` object therefore landed on the already-open
      // route-classified window while the gate read its reason as
      // unclassified: ingest built record statistics that window could never
      // emit, and `closeWindow`'s guard silently dropped them at the end.
      // `observeRejection` now coerces once with `String()` and the gate, the
      // key and the stored reason all read that one value, so the boxed
      // caller takes the route-classified branch: no statistics are built and
      // the discarded diagnostics are COUNTED. The guard that used to catch
      // this is unreachable and is gone; this check replaces the pin on it.
      //
      // TypeScript forbids this caller — which is the point. The emitter, not
      // caller convention, owns the bound, so the check constructs it anyway.
      const boxedReason = (reason: string) => new String(reason) as unknown as never;
      const BOXED_FEED = 3;
      const boxedAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      boxedAgg.observeRejection("storage_busy_retry", "otlp_exporter", undefined, "otlp");
      for (let index = 0; index < BOXED_FEED; index += 1) {
        boxedAgg.observeRejection(boxedReason("storage_busy_retry"), "otlp_exporter", fullRecordDiagnostic);
      }
      const boxedCounters = boxedAgg.counters();
      // The window state stores the NORMALISED reason: the counters row is the
      // only place outside the summary that exposes it, and it must be the
      // string primitive the window is keyed by, not the object handed in.
      const boxedRow = boxedCounters.reasons.find(
        (row) => String(row.reason) === "storage_busy_retry",
      );
      const boxedStoredReasonIsPrimitive =
        boxedRow !== undefined &&
        typeof boxedRow.reason === "string" &&
        String(boxedRow.reason) === "storage_busy_retry" &&
        boxedCounters.reasons.length === 1;
      const boxedSummary = boxedAgg.flush()[0];
      // Same feed, primitive reason: behaviour must be unchanged, which here
      // means BYTE-IDENTICAL to the boxed line. That is the whole claim — the
      // gate no longer distinguishes the two shapes.
      const primitiveAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      primitiveAgg.observeRejection("storage_busy_retry", "otlp_exporter", undefined, "otlp");
      for (let index = 0; index < BOXED_FEED; index += 1) {
        primitiveAgg.observeRejection("storage_busy_retry", "otlp_exporter", fullRecordDiagnostic);
      }
      const primitiveSummary = primitiveAgg.flush()[0];
      // Second arm: the boxed reason OPENS the window. `stateFor` is handed
      // the coerced value, so the key and the reason the state stores are the
      // same primitive. `counters()` is the only surface that exposes the
      // stored reason unserialised — `JSON.stringify` renders a `String`
      // object and a string primitive identically, `typeof` does not, so this
      // is the assertion that catches a state that kept the raw argument.
      const boxedFirstAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      for (let index = 0; index < BOXED_FEED; index += 1) {
        boxedFirstAgg.observeRejection(boxedReason("storage_busy_retry"), "otlp_exporter", fullRecordDiagnostic, "otlp");
      }
      const boxedFirstCounters = boxedFirstAgg.counters();
      const boxedFirstRow = boxedFirstCounters.reasons[0];
      const boxedFirstStoredReasonIsPrimitive =
        boxedFirstRow !== undefined &&
        typeof boxedFirstRow.reason === "string" &&
        String(boxedFirstRow.reason) === "storage_busy_retry" &&
        boxedFirstCounters.reasons.length === 1 &&
        conservation(boxedFirstCounters).ok;
      const boxedFirstSummary = boxedFirstAgg.flush()[0];
      const primitiveFirstAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      for (let index = 0; index < BOXED_FEED; index += 1) {
        primitiveFirstAgg.observeRejection("storage_busy_retry", "otlp_exporter", fullRecordDiagnostic, "otlp");
      }
      const primitiveFirstSummary = primitiveFirstAgg.flush()[0];
      // Third arm (REVIEW-111 F1): the two arms above cannot see WHICH value
      // the window is keyed by. `${new String("storage_busy_retry")}` and
      // `${"storage_busy_retry"}` are the same key text, so a `stateFor` that
      // re-derived the key from the RAW argument — gate and stored reason
      // still normalised — passed both arms 34/34 while reintroducing exactly
      // the gate/key disagreement this bead closed. Pin the key site with a
      // reason whose coercion is NOT stable: it answers "storage_busy_retry"
      // for as many coercions as the normalising gate spends — one per
      // observation, which is what "coerced once, not three times" means — and
      // a fresh string after that. One window opens iff every site that
      // decides a window read that one normalised value; a second coercion
      // anywhere lands on a key nothing else agrees with and opens another.
      //
      // TypeScript forbids this caller too; the emitter owns the bound, so the
      // check constructs it anyway.
      let varyingCoercions = 0;
      const varyingReason = {
        toString() {
          varyingCoercions += 1;
          return varyingCoercions <= BOXED_FEED
            ? "storage_busy_retry"
            : `unstable_coercion_${varyingCoercions}`;
        },
      } as unknown as never;
      const varyingAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      for (let index = 0; index < BOXED_FEED; index += 1) {
        varyingAgg.observeRejection(varyingReason, "otlp_exporter", fullRecordDiagnostic, "otlp");
      }
      const varyingCounters = varyingAgg.counters();
      const varyingRow = varyingCounters.reasons[0];
      const varyingLines = varyingAgg.flush();
      // Which clause of this arm pins which mutant, measured by the .120
      // reviewer and carried here because the .120 report argued it the other
      // way (REVIEW-120 F-2). The BOXED_FEED budget only reaches a second
      // coercion site that coerces on EVERY call: that one spends 6 coercions,
      // crosses the budget, and opens 3 windows, so the window count and the
      // stored reasons below both move. A second site that coerces once for
      // the whole run — a memoised key cache keyed on the raw object, which is
      // what a "don't re-coerce this" optimisation actually writes — spends 4
      // and opens ONE, so varyingWindowsOpened, varyingCounters.reasons and
      // varyingRow.reason are all still exactly the clean values and the
      // budget is blind to it. The clause that catches it is the last one,
      // byte-identity of the flushed line against the primitive's: the
      // memoised key lands on a window whose line the primitive's does not
      // match. Removing only that clause leaves the memoised mutant green at
      // 34/34 while the per-call one still reds, so the line clause, not the
      // budget, is the load-bearing pin on a single-coercion second site.
      //
      // That last clause is also why this arm is not silent on the older raw
      // mutants: with the gate reading the raw argument the flushed line
      // differs from the primitive's as well, so this flag is false and the
      // arm fires there too. It is one more assertion about the same window,
      // not a test that only the key site can ever trip.
      const varyingKeyIsTheNormalisedReason =
        varyingCounters.reasons.length === 1 &&
        varyingRow !== undefined &&
        typeof varyingRow.reason === "string" &&
        varyingRow.reason === "storage_busy_retry" &&
        varyingLines.length === 1 &&
        JSON.stringify(varyingLines[0]) === JSON.stringify(primitiveFirstSummary);
      // Control: the same boxed construction on a reason that is NOT
      // route-classified still carries all six record fields, so a clean busy
      // line is the gate classifying the boxed reason and not an injection
      // that quietly built no record statistics at all.
      const boxedControlAgg = mod.createRejectionDiagnostics({ nowMs: () => routeNow });
      boxedControlAgg.observeRejection("otlp_record_limit_exceeded", "otlp_exporter", undefined);
      boxedControlAgg.observeRejection(boxedReason("otlp_record_limit_exceeded"), "otlp_exporter", fullRecordDiagnostic);
      const boxedControlSummary = boxedControlAgg.flush()[0];
      check(
        "ingest_gate_normalises_the_reason_so_a_boxed_reason_is_classified_and_its_diagnostics_counted",
        // classified: no record statistics were built for the busy window …
        hasNoRecordFields(boxedSummary) &&
          // … and every dropped diagnostic is counted, not silently discarded
          boxedSummary?.recordDiagnosticsDiscarded === BOXED_FEED &&
          boxedSummary.count === BOXED_FEED + 1 &&
          boxedSummary.suppressed === BOXED_FEED &&
          JSON.stringify(boxedSummary.routes) === JSON.stringify({ otlp: 1 }) &&
          // the boxed observations landed on the ONE window the primitive
          // opened, and that window stores the normalised reason
          boxedStoredReasonIsPrimitive &&
          // …and a window the boxed reason OPENED stores it normalised too,
          // so the key and the stored reason agree whoever opened the window
          boxedFirstStoredReasonIsPrimitive &&
          // …and the window KEY is that same normalised value and not the raw
          // argument: an unstably coercing reason opens exactly one window and
          // emits the line the primitive emits (REVIEW-111 F1)
          varyingKeyIsTheNormalisedReason &&
          hasNoRecordFields(boxedFirstSummary) &&
          boxedFirstSummary?.recordDiagnosticsDiscarded === BOXED_FEED &&
          JSON.stringify(boxedFirstSummary) === JSON.stringify(primitiveFirstSummary) &&
          // primitive reason: unchanged, and byte-identical to the boxed line
          JSON.stringify(primitiveSummary) === JSON.stringify(boxedSummary) &&
          // an unclassified reason is untouched by the normalisation
          hasAllRecordFields(boxedControlSummary) &&
          boxedControlSummary?.recordCountMax === 100_000 &&
          boxedControlSummary.decodedBytesMax === 2_097_152 &&
          boxedControlSummary.routes === undefined &&
          boxedControlSummary.recordDiagnosticsDiscarded === undefined &&
          Buffer.byteLength(JSON.stringify(boxedSummary)) <=
            mod.REJECTION_SUMMARY_LINE_MAX_BYTES &&
          conservation(boxedCounters).ok &&
          conservation(boxedAgg.counters()).ok,
        {
          fed: BOXED_FEED,
          boxedLine: JSON.stringify(boxedSummary),
          boxedLineBytes: Buffer.byteLength(JSON.stringify(boxedSummary)),
          primitiveLine: JSON.stringify(primitiveSummary),
          linesIdentical: JSON.stringify(primitiveSummary) === JSON.stringify(boxedSummary),
          discarded: boxedSummary?.recordDiagnosticsDiscarded ?? null,
          storedReasonType: boxedRow === undefined ? "missing" : typeof boxedRow.reason,
          windowsOpened: boxedCounters.reasons.length,
          boxedOpenedStoredReasonType:
            boxedFirstRow === undefined ? "missing" : typeof boxedFirstRow.reason,
          boxedOpenedLine: JSON.stringify(boxedFirstSummary),
          primitiveOpenedLine: JSON.stringify(primitiveFirstSummary),
          varyingCoercions,
          varyingWindowsOpened: varyingCounters.reasons.length,
          varyingStoredReasons: varyingCounters.reasons.map((row) => String(row.reason)),
          varyingLines: varyingLines.map((line) => JSON.stringify(line)),
          controlLine: JSON.stringify(boxedControlSummary),
          controlLineBytes: Buffer.byteLength(JSON.stringify(boxedControlSummary)),
        },
      );
    }

    // Every line this phase printed stays inside the emitted-line ceiling the
    // storm phase asserts, and carries no request content.
    check(
      "route_classified_lines_stay_inside_the_emitted_line_ceiling_and_value_free",
      warnings.length > 0 &&
        warnings.every((line) => Buffer.byteLength(line) <= 256) &&
        SENTINELS_ABSENT(warnings.join("\n")) &&
        summaryLines().length >= 1,
      {
        lines: warnings.length,
        maxLineBytes: Math.max(...warnings.map((line) => Buffer.byteLength(line))),
        sample: warnings.find((line) => line.includes("\"routes\"")) ?? null,
      },
    );
  } finally {
    console.warn = originalWarn;
    if (server.listening) {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    buffer.close();
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

function SENTINELS_ABSENT(text: string) {
  return ![SENTINEL_SOURCE, SENTINEL_BODY, SENTINEL_ORIGIN].some((sentinel) =>
    text.includes(sentinel),
  );
}

async function main() {
  await integrationChecks();
  await routeClassificationChecks();

  for (const result of checks) {
    console.log(`${result.passed ? "PASS" : "FAIL"} ${result.name} ${JSON.stringify(result.detail)}`);
  }
  const failed = checks.filter((result) => !result.passed);
  console.log(
    JSON.stringify({
      checks: checks.length,
      passed: checks.length - failed.length,
      failed: failed.length,
      intervalMs: INTERVAL_MS,
      // Content identity, not a count tag: the ordered check names and their
      // verdicts. A renamed, removed, reordered or added check moves it, and
      // so does any flipped verdict — a red run can no longer print the same
      // digest as the green one. Scale-invariant, so a CI run under
      // REJECTION_PROOF_SCALE prints the same digest as a full local run.
      digest: crypto
        .createHash("sha256")
        .update(checks.map((result) => `${result.passed ? "PASS" : "FAIL"} ${result.name}`).join("\n"))
        .digest("hex")
        .slice(0, 8),
    }),
  );
  if (failed.length > 0) process.exitCode = 1;
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ error: "rejection_aggregation_proof_failed", message: message.slice(0, 200) }));
  process.exitCode = 1;
});
