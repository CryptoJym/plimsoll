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
  RejectionDiagnosticsCounters,
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
    // these ceilings bound the collector's diagnostic workload with generous
    // fixture headroom while still failing loudly on per-request logging or
    // unbounded buffer growth.
    check(
      "cpu_rss_log_byte_ceilings_hold_across_full_burst",
      cpuUsedMs <= 90_000 && rssGrowthBytes <= 160 * 1024 * 1024,
      { cpuUsedMs: Math.round(cpuUsedMs), rssGrowthBytes, totalRequests },
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

function SENTINELS_ABSENT(text: string) {
  return ![SENTINEL_SOURCE, SENTINEL_BODY, SENTINEL_ORIGIN].some((sentinel) =>
    text.includes(sentinel),
  );
}

async function main() {
  await integrationChecks();

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
      digest: crypto.createHash("sha256").update(String(checks.length)).digest("hex").slice(0, 8),
    }),
  );
  if (failed.length > 0) process.exitCode = 1;
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ error: "rejection_aggregation_proof_failed", message: message.slice(0, 200) }));
  process.exitCode = 1;
});
