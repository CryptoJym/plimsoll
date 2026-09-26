/**
 * Bead eco-6hoxj.163.17: an OTLP export the ledger cannot take in time is kept
 * in a bounded, private, metadata-only spool and replayed exactly once.
 *
 * Every stage drives the real HTTP route, the real `LocalEventBuffer` and, in
 * the last stage, the real daemon (`cli.ts start`) in an isolated home. The
 * spool module is loaded dynamically so this same file runs against a tree
 * without it and fails there on the behaviour, not on an import.
 */
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import { Worker } from "node:worker_threads";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import type * as SpoolModuleType from "../packages/collector-cli/src/otlp-spool";

type SpoolModule = typeof SpoolModuleType;
type Spool = InstanceType<SpoolModule["OtlpIntakeSpool"]>;

const repoRoot = path.resolve(__dirname, "..");
const cliSource = path.join(repoRoot, "packages/collector-cli/src/cli.ts");
// The local tenant, as the other intake proofs use: enrollment gating (events
// observed before a non-local workspace's epoch are refused) is orthogonal to
// the spool, and a fixed clock keeps every id deterministic across processes.
const workspace = "00000000-0000-4000-8000-000000000001";
const config = collectorConfigSchema.parse({});
const T0_NANOS = BigInt(Date.parse("2026-09-23T12:00:00.000Z")) * 1_000_000n;

let spoolModule: SpoolModule | null = null;
const checks: Array<{ name: string; passed: boolean; detail: unknown }> = [];
const measurements: Record<string, unknown> = {};

function check(name: string, passed: boolean, detail: unknown = {}) {
  checks.push({ name, passed, detail });
  console.log(JSON.stringify({ check: name, passed, ...(passed ? {} : { detail }) }));
}

function privateTempDir(label: string) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-otlp-spool-${label}-`));
  fs.chmodSync(dir, 0o700);
  return dir;
}

function nanos(offset: number) {
  return String(T0_NANOS + BigInt(offset) * 1_000_000n);
}

/** Codex-shaped usage log records: one event each, deterministic ids. */
function usageLogs(tag: string, count: number, extra: Array<Record<string, unknown>> = []) {
  return {
    resourceLogs: [{
      resource: { attributes: [{ key: "service.name", value: { stringValue: "codex_cli_rs" } }] },
      scopeLogs: [{
        logRecords: Array.from({ length: count }, (_, index) => ({
          timeUnixNano: nanos(index),
          attributes: [
            { key: "event.name", value: { stringValue: "codex.sse_event" } },
            { key: "conversation.id", value: { stringValue: `conv-${tag}` } },
            { key: "event.sequence", value: { intValue: String(index) } },
            { key: "input_token_count", value: { intValue: String(100 + index) } },
            { key: "output_token_count", value: { intValue: "7" } },
            { key: "model", value: { stringValue: "gpt-5-codex" } },
            ...extra,
          ],
        })),
      }],
    }],
  };
}

/** Admitted usage spans plus `dropped` zero-value app-server spans (admission drops). */
function usageSpans(tag: string, admitted: number, dropped: number) {
  const span = (index: number, service: "usage" | "drop") => ({
    name: service === "usage" ? "codex.turn" : "realtime_conversation.running_state",
    traceId: `${tag.length.toString(16)}${index.toString(16)}`.padStart(32, "0"),
    spanId: (index + 1).toString(16).padStart(16, "0"),
    startTimeUnixNano: nanos(index),
    endTimeUnixNano: nanos(index + 1),
    attributes: service === "usage"
      ? [
          { key: "conversation.id", value: { stringValue: `span-${tag}` } },
          { key: "gen_ai.usage.input_tokens", value: { intValue: String(10 + index) } },
        ]
      : [],
  });
  return {
    resourceSpans: [
      {
        resource: { attributes: [{ key: "service.name", value: { stringValue: "codex_cli_rs" } }] },
        scopeSpans: [{ spans: Array.from({ length: admitted }, (_, index) => span(index, "usage")) }],
      },
      {
        resource: { attributes: [{ key: "service.name", value: { stringValue: "codex-app-server" } }] },
        scopeSpans: [{ spans: Array.from({ length: dropped }, (_, index) => span(1_000 + index, "drop")) }],
      },
    ],
  };
}

function metricsBody(tag: string, points: number) {
  return {
    resourceMetrics: [{
      resource: { attributes: [{ key: "service.name", value: { stringValue: "claude-code" } }] },
      scopeMetrics: [{
        metrics: [{
          name: "claude_code.token.usage",
          sum: {
            dataPoints: Array.from({ length: points }, (_, index) => ({
              timeUnixNano: nanos(index),
              asInt: String(1_000 + index),
              attributes: [
                { key: "session.id", value: { stringValue: `metric-${tag}` } },
                { key: "type", value: { stringValue: index % 2 ? "input" : "output" } },
                { key: "model", value: { stringValue: "claude-opus" } },
              ],
            })),
          },
        }],
      }],
    }],
  };
}

type Answer = {
  status: number;
  retryAfter: string | null;
  body: Record<string, unknown>;
  error?: string;
};

function post(
  port: number,
  route: string,
  payload: unknown,
  headers: Record<string, string> = {},
): Promise<Answer> {
  const text = JSON.stringify(payload);
  return new Promise((resolve) => {
    const request = http.request(
      {
        host: "127.0.0.1",
        port,
        path: route,
        method: "POST",
        headers: {
          connection: "close",
          "content-type": "application/json",
          "content-length": Buffer.byteLength(text),
          "x-plimsoll-source": route === "/v1/metrics" ? "claude_code" : "codex",
          ...headers,
        },
      },
      (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          body += chunk;
        });
        response.on("end", () => {
          let parsed: Record<string, unknown> = {};
          try {
            parsed = JSON.parse(body) as Record<string, unknown>;
          } catch {
            parsed = { unparsed: body.slice(0, 200) };
          }
          resolve({
            status: response.statusCode ?? 0,
            retryAfter: (response.headers["retry-after"] as string | undefined) ?? null,
            body: parsed,
          });
        });
      },
    );
    request.setTimeout(20_000, () => request.destroy(new Error("proof_client_timeout")));
    request.on("error", (error) => resolve({ status: 0, retryAfter: null, body: {}, error: error.message }));
    request.end(text);
  });
}

type Fixture = {
  root: string;
  ledger: string;
  buffer: LocalEventBuffer;
  spool: Spool | undefined;
  port: number;
  server: ReturnType<typeof createCollectorServer>;
  close: () => Promise<void>;
};

async function openFixture(options: {
  root?: string;
  limits?: Record<string, number>;
  env?: NodeJS.ProcessEnv;
  nowMs?: () => number;
  withSpool?: boolean;
} = {}): Promise<Fixture> {
  const root = options.root ?? privateTempDir("fixture");
  const ledger = path.join(root, "work-ledger.sqlite");
  const buffer = new LocalEventBuffer(ledger, { databaseBusyTimeoutMs: 0, workspaceId: workspace });
  const spool = spoolModule && options.withSpool !== false
    ? new spoolModule.OtlpIntakeSpool({
        home: root,
        env: options.env ?? {},
        limits: options.limits,
        nowMs: options.nowMs,
        onWarning: () => undefined,
      })
    : undefined;
  const server = createCollectorServer(config, buffer, spool ? { otlpSpool: spool } : {});
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as AddressInfo).port;
  return {
    root,
    ledger,
    buffer,
    spool,
    port,
    server,
    async close() {
      spool?.stopDrain();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      buffer.close();
    },
  };
}

/** Hold the ledger's writer lock from a second connection, as a maintenance writer would. */
function holdWriter(ledger: string) {
  const lock = new Database(ledger, { timeout: 0 });
  lock.exec("BEGIN IMMEDIATE");
  return () => {
    lock.exec("ROLLBACK");
    lock.close();
  };
}

function count(db: Database.Database, sql: string) {
  return (db.prepare(sql).get() as { n: number }).n;
}

function eventRows(buffer: LocalEventBuffer) {
  return count(buffer.database, "select count(*) as n from buffered_events");
}

function metricRows(buffer: LocalEventBuffer) {
  return count(buffer.database, "select count(*) as n from metric_samples");
}

function droppedTotal(buffer: LocalEventBuffer) {
  return buffer.otlpAdmissionCounters().reduce((total, row) => total + row.droppedCount, 0);
}

function cursorRows(buffer: LocalEventBuffer) {
  return buffer.database
    .prepare(`select key, value from maintenance_state where key like 'otlp_intake_spool:%' order by key`)
    .all() as Array<{ key: string; value: string }>;
}

/** Pending spool files in drain order: receive time, pid, sequence. */
function spoolFiles(root: string) {
  const key = (name: string) => name.split("-").slice(0, 3).map(Number);
  try {
    return fs.readdirSync(path.join(root, "otlp-spool"))
      .filter((name) => /^\d{13,}-\d+-\d+-[0-9a-f]{8}\.json$/.test(name))
      .sort((left, right) => {
        const [a, b] = [key(left), key(right)];
        return a[0]! - b[0]! || a[1]! - b[1]! || a[2]! - b[2]!;
      });
  } catch {
    return [];
  }
}

async function drainAll(fixture: Fixture, maxPasses = 50) {
  let passes = 0;
  const totals = { replayed: 0, events: 0, deferred: 0, rejected: 0 };
  while (fixture.spool && passes < maxPasses) {
    const pass = await fixture.spool.drain(fixture.buffer);
    passes += 1;
    totals.replayed += pass.replayed;
    totals.events += pass.replayedEvents;
    totals.rejected += pass.rejected;
    if (pass.deferred) totals.deferred += 1;
    if (fixture.spool.status().pendingFiles === 0) break;
  }
  return { passes, ...totals };
}

// ---------------------------------------------------------------------------
// A. A busy ledger: every signal is spooled, answered 202, replayed once.
// ---------------------------------------------------------------------------
async function stageBusySpoolAndReplay() {
  const fixture = await openFixture();
  try {
    const bodies = {
      logs: usageLogs("a", 40),
      traces: usageSpans("a", 20, 5),
      metrics: metricsBody("a", 18),
    };
    const release = holdWriter(fixture.ledger);
    let answers: Answer[];
    try {
      answers = [
        await post(fixture.port, "/v1/logs", bodies.logs),
        await post(fixture.port, "/v1/traces", bodies.traces),
        await post(fixture.port, "/v1/metrics", bodies.metrics),
      ];
    } finally {
      release();
    }
    check(
      "a_busy_ledger_otlp_is_spooled_and_answered_202_after_the_durable_write",
      answers.every((answer) => answer.status === 202 && answer.body.status === "otlp_spooled" && answer.retryAfter === null) &&
        spoolFiles(fixture.root).length === 3,
      { answers: answers.map((answer) => ({ status: answer.status, body: answer.body })), files: spoolFiles(fixture.root).length },
    );
    check(
      "a_nothing_reached_the_ledger_while_it_was_busy",
      eventRows(fixture.buffer) === 0 && metricRows(fixture.buffer) === 0 && droppedTotal(fixture.buffer) === 0,
      { events: eventRows(fixture.buffer), metrics: metricRows(fixture.buffer) },
    );
    const status = fixture.spool?.status();
    check(
      "a_status_reports_pending_files_and_oldest_age",
      status?.pendingFiles === 3 && typeof status.oldestPendingAgeSeconds === "number" && status.spooled.requests === 3 &&
        status.spooled.events === 60 && status.spooled.metricSamples === 18,
      status ?? null,
    );
    const drained = await drainAll(fixture);
    const events = eventRows(fixture.buffer);
    const metrics = metricRows(fixture.buffer);
    const dropped = droppedTotal(fixture.buffer);
    check(
      "a_replay_commits_every_spooled_row_exactly_once",
      events === 60 && metrics === 18 && dropped === 5 && spoolFiles(fixture.root).length === 0,
      { drained, events, metrics, dropped, files: spoolFiles(fixture.root) },
    );
    const again = fixture.spool ? await fixture.spool.drain(fixture.buffer) : null;
    check(
      "a_a_second_drain_changes_nothing",
      again !== null && again.replayed === 0 && eventRows(fixture.buffer) === 60 && metricRows(fixture.buffer) === 18 &&
        droppedTotal(fixture.buffer) === 5 && cursorRows(fixture.buffer).length === 0,
      { again, cursors: cursorRows(fixture.buffer) },
    );
    // An exporter that re-sent the same batch anyway lands on the same ids.
    const resend = await post(fixture.port, "/v1/logs", bodies.logs);
    check(
      "a_replayed_rows_are_byte_identical_to_live_rows",
      resend.status === 202 && resend.body.deduplicated === 40 && resend.body.collisionQuarantined === undefined &&
        eventRows(fixture.buffer) === 60,
      { resend: resend.body, events: eventRows(fixture.buffer) },
    );
    const statusResponse = await new Promise<Record<string, unknown>>((resolve) => {
      http.get(`http://127.0.0.1:${fixture.port}/status`, (response) => {
        let text = "";
        response.on("data", (chunk) => {
          text += chunk;
        });
        response.on("end", () => resolve(JSON.parse(text) as Record<string, unknown>));
      });
    });
    const otlpSpool = statusResponse.otlpSpool as Record<string, any> | null | undefined;
    check(
      "a_http_status_carries_spooled_replayed_dropped_by_cap_and_oldest_age",
      Boolean(otlpSpool) && otlpSpool!.spooled.requests === 3 && otlpSpool!.replayed.requests === 3 &&
        otlpSpool!.replayed.events === 60 && otlpSpool!.pendingFiles === 0 && otlpSpool!.oldestPendingAgeSeconds === null &&
        otlpSpool!.droppedByCap.fileCap === 0 && otlpSpool!.droppedByCap.ageCap === 0 && otlpSpool!.enabled === true,
      otlpSpool ?? null,
    );
  } finally {
    await fixture.close();
    fs.rmSync(fixture.root, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// B. Chunk 0 commits live, chunk 1 meets a busy ledger: only the rest is kept.
// ---------------------------------------------------------------------------
async function stagePartialCommit() {
  const fixture = await openFixture();
  let release: (() => void) | null = null;
  const original = fixture.buffer.appendMany.bind(fixture.buffer);
  let calls = 0;
  fixture.buffer.appendMany = ((...args: Parameters<LocalEventBuffer["appendMany"]>) => {
    const result = original(...args);
    calls += 1;
    if (calls === 1) release = holdWriter(fixture.ledger);
    return result;
  }) as LocalEventBuffer["appendMany"];
  try {
    const answer = await post(fixture.port, "/v1/traces", usageSpans("b", 40, 6));
    (release as (() => void) | null)?.();
    release = null;
    fixture.buffer.appendMany = original;
    const liveRows = eventRows(fixture.buffer);
    check(
      "b_a_partial_commit_spools_only_the_uncommitted_chunks",
      answer.status === 202 && answer.body.status === "otlp_spooled" && answer.body.events === 24 &&
        liveRows === 16 && droppedTotal(fixture.buffer) === 6,
      { answer: answer.body, liveRows, dropped: droppedTotal(fixture.buffer) },
    );
    await drainAll(fixture);
    check(
      "b_replay_completes_the_request_and_admission_drops_count_once",
      eventRows(fixture.buffer) === 40 && droppedTotal(fixture.buffer) === 6 && spoolFiles(fixture.root).length === 0,
      { events: eventRows(fixture.buffer), dropped: droppedTotal(fixture.buffer) },
    );
  } finally {
    (release as (() => void) | null)?.();
    fixture.buffer.appendMany = original;
    await fixture.close();
    fs.rmSync(fixture.root, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// C. Deadline and event-loop pressure.
// ---------------------------------------------------------------------------
function busyWait(ms: number) {
  const until = performance.now() + ms;
  while (performance.now() < until) {
    /* hold the event loop, as a synchronous ledger read once did */
  }
}

/**
 * The client lives on a worker thread so it can deliver the body while the
 * collector's own loop is blocked: headers first, the body 150 ms later.
 */
function postFromWorker(port: number, route: string, payload: unknown, headers: Record<string, string>) {
  const body = JSON.stringify(payload);
  const worker = new Worker(
    `
      const http = require("node:http");
      const { parentPort, workerData } = require("node:worker_threads");
      const request = http.request({
        host: "127.0.0.1", port: workerData.port, path: workerData.route, method: "POST",
        headers: { ...workerData.headers, connection: "close", "content-type": "application/json",
          "content-length": Buffer.byteLength(workerData.body) },
      }, (response) => {
        let text = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => { text += chunk; });
        response.on("end", () => parentPort.postMessage({
          status: response.statusCode, retryAfter: response.headers["retry-after"] ?? null, text }));
      });
      request.on("error", (error) => parentPort.postMessage({ status: 0, error: error.message }));
      request.flushHeaders();
      setTimeout(() => request.end(workerData.body), 150);
    `,
    { eval: true, workerData: { port, route, headers, body } },
  );
  return new Promise<Answer>((resolve) => {
    worker.once("message", (message: { status: number; retryAfter?: string | null; text?: string; error?: string }) => {
      let parsed: Record<string, unknown> = {};
      try {
        parsed = JSON.parse(message.text ?? "{}") as Record<string, unknown>;
      } catch {
        parsed = {};
      }
      void worker.terminate();
      resolve({ status: message.status, retryAfter: message.retryAfter ?? null, body: parsed, error: message.error });
    });
  });
}

async function stageDeadlineAndLoopPressure() {
  const fixture = await openFixture();
  try {
    // C1: the loop is blocked while the body arrives, past the 1.5 s deadline.
    fixture.server.on("request", (request) => {
      const blockMs = Number(request.headers["x-proof-block-ms"] ?? 0);
      if (blockMs > 0) setImmediate(() => busyWait(blockMs));
    });
    const blocked = await postFromWorker(fixture.port, "/v1/logs", usageLogs("c1", 20), {
      "x-plimsoll-source": "codex",
      "x-proof-block-ms": "1700",
    });
    check(
      "c_a_body_that_arrived_while_the_loop_was_blocked_is_spooled_not_408",
      blocked.status === 202 && blocked.body.status === "otlp_spooled" && blocked.body.events === 20,
      blocked,
    );
    // C2: the deadline is spent inside the commit (a slow writer turn).
    const original = fixture.buffer.appendMany.bind(fixture.buffer);
    let calls = 0;
    fixture.buffer.appendMany = ((...args: Parameters<LocalEventBuffer["appendMany"]>) => {
      const result = original(...args);
      calls += 1;
      if (calls === 1) busyWait(1_600);
      return result;
    }) as LocalEventBuffer["appendMany"];
    let slow: Answer;
    try {
      slow = await post(fixture.port, "/v1/logs", usageLogs("c2", 40));
    } finally {
      fixture.buffer.appendMany = original;
    }
    check(
      "c_a_deadline_spent_during_commit_spools_the_remainder",
      slow.status === 202 && slow.body.status === "otlp_spooled" && slow.body.events === 24 && eventRows(fixture.buffer) === 16,
      { slow, events: eventRows(fixture.buffer) },
    );
    await drainAll(fixture);
    check(
      "c_both_deadline_refusals_replay_exactly_once",
      eventRows(fixture.buffer) === 60 && spoolFiles(fixture.root).length === 0,
      { events: eventRows(fixture.buffer) },
    );
    // A stalled body is not a whole request: still 408, nothing written.
    const stalled = await new Promise<Answer>((resolve) => {
      const request = http.request(
        {
          host: "127.0.0.1",
          port: fixture.port,
          path: "/v1/logs",
          method: "POST",
          headers: { connection: "close", "content-type": "application/json", "x-plimsoll-source": "codex" },
        },
        (response) => {
          let text = "";
          response.on("data", (chunk) => {
            text += chunk;
          });
          response.on("end", () => resolve({
            status: response.statusCode ?? 0,
            retryAfter: (response.headers["retry-after"] as string | undefined) ?? null,
            body: JSON.parse(text) as Record<string, unknown>,
          }));
        },
      );
      request.on("error", (error) => resolve({ status: 0, retryAfter: null, body: {}, error: error.message }));
      request.write("{");
    });
    check(
      "c_a_stalled_incomplete_body_is_still_refused_408_and_not_spooled",
      stalled.status === 408 && stalled.body.reason === "request_deadline_exceeded" && spoolFiles(fixture.root).length === 0,
      stalled,
    );
  } finally {
    await fixture.close();
    fs.rmSync(fixture.root, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// D. Spool full or disabled: 503 + Retry-After (never a silent 202).
// ---------------------------------------------------------------------------
async function stageSpoolFullAndKillSwitch() {
  const originalWarn = console.warn;
  const lines: string[] = [];
  console.warn = (...args: unknown[]) => {
    lines.push(args.map(String).join(" "));
  };
  const fileCap = await openFixture({ limits: { maxFiles: 1 } });
  try {
    const release = holdWriter(fileCap.ledger);
    let first: Answer;
    let second: Answer;
    try {
      first = await post(fileCap.port, "/v1/logs", usageLogs("d1", 3));
      second = await post(fileCap.port, "/v1/logs", usageLogs("d2", 3));
    } finally {
      release();
    }
    const rejection = lines
      .map((line) => {
        try {
          return JSON.parse(line) as Record<string, unknown>;
        } catch {
          return null;
        }
      })
      .find((line) => line?.error === "collector_request_rejected" && line.reason === "storage_busy_retry");
    check(
      "d_file_cap_answers_503_with_retry_after_and_keeps_what_it_acknowledged",
      first.status === 202 && second.status === 503 && second.body.reason === "storage_busy_retry" &&
        second.retryAfter === "1" && fileCap.spool?.status().droppedByCap.fileCap === 1 &&
        spoolFiles(fileCap.root).length === 1,
      { first: first.status, second, status: fileCap.spool?.status().droppedByCap },
    );
    check(
      "d_the_refusal_line_names_the_route_and_the_spool_bound",
      rejection?.route === "otlp" && rejection.spoolAttempted === true && rejection.spoolRefused === true &&
        rejection.spoolRefusedReason === "spool_bounds" && rejection.spoolBound === "files",
      rejection ?? null,
    );
  } finally {
    await fileCap.close();
    fs.rmSync(fileCap.root, { recursive: true, force: true });
  }

  const byteCap = await openFixture({ limits: { maxBytes: 2_048 } });
  try {
    const release = holdWriter(byteCap.ledger);
    let answer: Answer;
    try {
      answer = await post(byteCap.port, "/v1/logs", usageLogs("d3", 20));
    } finally {
      release();
    }
    check(
      "d_byte_cap_answers_503_with_retry_after",
      answer.status === 503 && answer.retryAfter === "1" && byteCap.spool?.status().droppedByCap.byteCap === 1 &&
        spoolFiles(byteCap.root).length === 0,
      { answer, status: byteCap.spool?.status().droppedByCap },
    );
    // A deadline refusal the full spool cannot hold becomes a retryable 503, not 408.
    const original = byteCap.buffer.appendMany.bind(byteCap.buffer);
    byteCap.buffer.appendMany = ((...args: Parameters<LocalEventBuffer["appendMany"]>) => {
      const result = original(...args);
      busyWait(1_600);
      return result;
    }) as LocalEventBuffer["appendMany"];
    let deadline: Answer;
    try {
      deadline = await post(byteCap.port, "/v1/logs", usageLogs("d4", 40));
    } finally {
      byteCap.buffer.appendMany = original;
    }
    check(
      "d_a_deadline_refusal_the_spool_cannot_hold_is_503_retry_after_not_408",
      deadline.status === 503 && deadline.body.reason === "request_deadline_exceeded" && deadline.retryAfter === "1",
      deadline,
    );
  } finally {
    await byteCap.close();
    fs.rmSync(byteCap.root, { recursive: true, force: true });
  }

  const off = await openFixture({ env: { PLIMSOLL_OTLP_SPOOL: "off" } });
  try {
    const release = holdWriter(off.ledger);
    let busy: Answer;
    try {
      busy = await post(off.port, "/v1/logs", usageLogs("d5", 3));
    } finally {
      release();
    }
    const original = off.buffer.appendMany.bind(off.buffer);
    off.buffer.appendMany = ((...args: Parameters<LocalEventBuffer["appendMany"]>) => {
      const result = original(...args);
      busyWait(1_600);
      return result;
    }) as LocalEventBuffer["appendMany"];
    let deadline: Answer;
    try {
      deadline = await post(off.port, "/v1/logs", usageLogs("d6", 40));
    } finally {
      off.buffer.appendMany = original;
    }
    check(
      "d_the_kill_switch_restores_the_previous_answers_exactly",
      off.spool?.enabled === false && busy.status === 503 && busy.body.reason === "storage_busy_retry" &&
        busy.retryAfter === "1" && deadline.status === 408 && deadline.body.reason === "request_deadline_exceeded" &&
        !fs.existsSync(path.join(off.root, "otlp-spool")),
      { busy, deadline, enabled: off.spool?.enabled ?? null },
    );
  } finally {
    await off.close();
    fs.rmSync(off.root, { recursive: true, force: true });
    console.warn = originalWarn;
  }
}

// ---------------------------------------------------------------------------
// E/F. Crashes, in real child processes killed with SIGKILL at the named point.
// ---------------------------------------------------------------------------
async function runChild(mode: string, root: string, extra: string[] = []) {
  const child = spawn(process.execPath, ["--import", "tsx", __filename, "child", mode, root, ...extra], {
    cwd: repoRoot,
    env: process.env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (text: string) => {
    stdout += text;
  });
  child.stderr.on("data", (text: string) => {
    stderr += text;
  });
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) =>
    child.once("exit", (code, signal) => resolve({ code, signal })));
  return { child, exited, output: () => ({ stdout, stderr }) };
}

async function waitFor(predicate: () => boolean, timeoutMs: number) {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (predicate()) return true;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return predicate();
}

async function childMain(mode: string, root: string) {
  if (!spoolModule) {
    console.log("NO_SPOOL_MODULE");
    return;
  }
  const ledger = path.join(root, "work-ledger.sqlite");
  const buffer = new LocalEventBuffer(ledger, { databaseBusyTimeoutMs: 0, workspaceId: workspace });
  const kill = () => process.kill(process.pid, "SIGKILL");
  if (mode === "crash-after-write") {
    const spool = new spoolModule.OtlpIntakeSpool({
      home: root,
      env: {},
      hooks: { afterDurableWrite: kill },
      onWarning: () => undefined,
    });
    const server = createCollectorServer(config, buffer, { otlpSpool: spool });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    console.log(`READY ${(server.address() as AddressInfo).port}`);
    return;
  }
  const spool = new spoolModule.OtlpIntakeSpool({
    home: root,
    env: {},
    hooks: mode === "crash-mid-replay"
      ? { afterChunkCommitted: (_id, chunk) => { if (chunk === 0) kill(); } }
      : { beforeRemove: kill },
    onWarning: () => undefined,
  });
  await spool.drain(buffer);
  console.log("CHILD_DRAIN_RETURNED_WITHOUT_CRASH");
  buffer.close();
}

async function stageCrashBetweenWriteAndAck() {
  const root = privateTempDir("crash-ack");
  const body = usageLogs("e", 40);
  // The child owns the listener; this process plays the exporter and the lock.
  const ledger = path.join(root, "work-ledger.sqlite");
  new LocalEventBuffer(ledger, { databaseBusyTimeoutMs: 0, workspaceId: workspace }).close();
  const run = await runChild("crash-after-write", root);
  try {
    const ready = await waitFor(() => /READY \d+/.test(run.output().stdout) || run.output().stdout.includes("NO_SPOOL"), 30_000);
    const port = Number(/READY (\d+)/.exec(run.output().stdout)?.[1] ?? 0);
    let answer: Answer = { status: -1, retryAfter: null, body: {} };
    if (ready && port > 0) {
      const release = holdWriter(ledger);
      try {
        answer = await post(port, "/v1/logs", body);
      } finally {
        release();
      }
    }
    const exit = await Promise.race([run.exited, new Promise<null>((resolve) => setTimeout(() => resolve(null), 10_000))]);
    check(
      "e_the_collector_died_after_the_durable_write_before_answering",
      answer.status === 0 && exit?.signal === "SIGKILL" && spoolFiles(root).length === 1,
      { answer, exit, files: spoolFiles(root), child: run.output().stdout.slice(0, 200) },
    );
    // The restarted collector finds the file; the exporter, having no answer,
    // re-sends. Both paths must end with each row exactly once.
    const fixture = await openFixture({ root });
    try {
      const resent = await post(fixture.port, "/v1/logs", body);
      const drained = await drainAll(fixture);
      check(
        "e_restart_replays_the_unacknowledged_file_and_the_resend_dedupes",
        resent.status === 202 && eventRows(fixture.buffer) === 40 && drained.replayed === 1 &&
          fixture.spool?.status().replayed.deduplicated === 40 && fixture.spool?.status().replayed.collisions === 0,
        { resent: resent.body, events: eventRows(fixture.buffer), drained, status: fixture.spool?.status().replayed },
      );
      // A temporary left by a crash inside the write is never replayed and is reaped.
      const orphan = path.join(root, "otlp-spool", `${Date.now()}-1-1-0000abcd.json.tmp`);
      fs.writeFileSync(orphan, "{\"torn\":", { mode: 0o600 });
      await fixture.close();
      const later = await openFixture({ root, nowMs: () => Date.now() + 120_000 });
      try {
        check(
          "e_a_torn_temporary_is_not_pending_and_is_reaped_at_start_up",
          later.spool?.status().pendingFiles === 0 && !fs.existsSync(orphan),
          { pending: later.spool?.status().pendingFiles, orphanExists: fs.existsSync(orphan) },
        );
      } finally {
        await later.close();
      }
    } finally {
      await fixture.close().catch(() => undefined);
    }
  } finally {
    run.child.kill("SIGKILL");
    fs.rmSync(root, { recursive: true, force: true });
  }
}

async function spoolOneRequest(root: string, route: string, payload: unknown) {
  const fixture = await openFixture({ root });
  const release = holdWriter(fixture.ledger);
  let answer: Answer;
  try {
    answer = await post(fixture.port, route, payload);
  } finally {
    release();
  }
  await fixture.close();
  return answer;
}

async function stageCrashMidReplay() {
  const root = privateTempDir("crash-replay");
  try {
    // 40 admitted spans (3 chunks) and 6 admission drops riding chunk 0.
    const spooled = await spoolOneRequest(root, "/v1/traces", usageSpans("f", 40, 6));
    const run = await runChild("crash-mid-replay", root);
    const exit = await run.exited;
    const buffer = new LocalEventBuffer(path.join(root, "work-ledger.sqlite"), { databaseBusyTimeoutMs: 0, workspaceId: workspace });
    const midRows = eventRows(buffer);
    const midDropped = droppedTotal(buffer);
    const midCursor = cursorRows(buffer);
    buffer.close();
    check(
      "f_a_crash_after_chunk_0_leaves_chunk_0_and_its_cursor_committed_together",
      spooled.status === 202 && exit.signal === "SIGKILL" && midRows === 16 && midDropped === 6 &&
        midCursor.length === 1 && JSON.parse(midCursor[0]!.value).nextChunk === 1 && spoolFiles(root).length === 1,
      { spooled: spooled.status, exit, midRows, midDropped, midCursor, child: run.output() },
    );
    const fixture = await openFixture({ root });
    try {
      const drained = await drainAll(fixture);
      check(
        "f_restart_resumes_at_chunk_1_so_every_row_and_drop_counts_once",
        eventRows(fixture.buffer) === 40 && droppedTotal(fixture.buffer) === 6 && drained.replayed === 1 &&
          fixture.spool?.status().replayed.deduplicated === 0 && cursorRows(fixture.buffer).length === 0 &&
          spoolFiles(root).length === 0,
        { events: eventRows(fixture.buffer), dropped: droppedTotal(fixture.buffer), drained, cursors: cursorRows(fixture.buffer) },
      );
    } finally {
      await fixture.close();
    }
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }

  const second = privateTempDir("crash-remove");
  try {
    await spoolOneRequest(second, "/v1/traces", usageSpans("g", 40, 6));
    const run = await runChild("crash-before-remove", second);
    const exit = await run.exited;
    const fixture = await openFixture({ root: second });
    try {
      const committedRows = eventRows(fixture.buffer);
      const cursor = cursorRows(fixture.buffer);
      check(
        "f_a_crash_after_the_last_commit_before_removal_keeps_file_and_complete_cursor",
        exit.signal === "SIGKILL" && committedRows === 40 && droppedTotal(fixture.buffer) === 6 &&
          cursor.length === 1 && JSON.parse(cursor[0]!.value).nextChunk === 3 && spoolFiles(second).length === 1,
        { exit, committedRows, cursor, files: spoolFiles(second) },
      );
      const pass = fixture.spool ? await fixture.spool.drain(fixture.buffer) : null;
      check(
        "f_restart_removes_it_without_committing_anything_again",
        pass !== null && pass.replayed === 1 && pass.chunks === 0 && eventRows(fixture.buffer) === 40 &&
          droppedTotal(fixture.buffer) === 6 && spoolFiles(second).length === 0 && cursorRows(fixture.buffer).length === 0,
        { pass, events: eventRows(fixture.buffer), dropped: droppedTotal(fixture.buffer) },
      );
    } finally {
      await fixture.close();
    }
  } finally {
    fs.rmSync(second, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// G. Privacy: planted content never reaches the spool file.
// ---------------------------------------------------------------------------
const PLANTED = [
  "PLANTED_LOG_BODY",
  "PLANTED_PROMPT_ATTRIBUTE",
  "PLANTED_INPUT_MESSAGES",
  "PLANTED_OUTPUT_MESSAGES",
  "PLANTED_TOOL_ARGUMENTS",
  "PLANTED_COMMAND",
  "PLANTED_CWD_SEGMENT",
  "PLANTED_FILE_PATH",
  "PLANTED_EMAIL",
  "PLANTED_UNKNOWN_ATTRIBUTE",
  "PLANTED_RESOURCE_COMMAND_LINE",
  "PLANTED_SCOPE_ATTRIBUTE",
  "PLANTED_SPAN_PATH",
  "PLANTED_SPAN_EVENT",
  "PLANTED_METRIC_PROMPT",
];

function plantedBodies() {
  const secret = (marker: string) => ({ stringValue: `${marker} private text` });
  const plantedAttributes = [
    { key: "prompt", value: secret("PLANTED_PROMPT_ATTRIBUTE") },
    { key: "gen_ai.input.messages", value: secret("PLANTED_INPUT_MESSAGES") },
    { key: "gen_ai.output.messages", value: secret("PLANTED_OUTPUT_MESSAGES") },
    { key: "arguments", value: { stringValue: JSON.stringify({ workdir: "/Users/x/PLANTED_CWD_SEGMENT", cmd: "PLANTED_TOOL_ARGUMENTS" }) } },
    { key: "command", value: secret("PLANTED_COMMAND") },
    { key: "cwd", value: { stringValue: "/Users/x/PLANTED_CWD_SEGMENT/repo" } },
    { key: "file_path", value: { stringValue: "/Users/x/PLANTED_FILE_PATH.ts" } },
    { key: "user.email", value: { stringValue: "PLANTED_EMAIL@example.com" } },
    { key: "custom.free_text_note", value: secret("PLANTED_UNKNOWN_ATTRIBUTE") },
    // Positive control: the ledger keeps this value, so the spool must too.
    { key: "conversation.id", value: { stringValue: "KEPT_CONTROL_conversation" } },
  ];
  const resource = {
    attributes: [
      { key: "service.name", value: { stringValue: "codex_cli_rs" } },
      { key: "process.command_line", value: secret("PLANTED_RESOURCE_COMMAND_LINE") },
    ],
  };
  const scope = { name: "codex", attributes: [{ key: "prompt", value: secret("PLANTED_SCOPE_ATTRIBUTE") }] };
  return {
    logs: {
      resourceLogs: [{
        resource,
        scopeLogs: [{
          scope,
          logRecords: Array.from({ length: 3 }, (_, index) => ({
            timeUnixNano: nanos(index),
            body: secret("PLANTED_LOG_BODY"),
            attributes: [
              { key: "event.name", value: { stringValue: "codex.sse_event" } },
              { key: "input_token_count", value: { intValue: "12" } },
              { key: "event.sequence", value: { intValue: String(index) } },
              ...plantedAttributes,
            ],
          })),
        }],
      }],
    },
    traces: {
      resourceSpans: [{
        resource,
        scopeSpans: [{
          scope,
          spans: [{
            name: "/Users/x/PLANTED_SPAN_PATH/tool",
            traceId: "0".repeat(31) + "7",
            spanId: "0".repeat(15) + "7",
            startTimeUnixNano: nanos(1),
            endTimeUnixNano: nanos(2),
            attributes: [{ key: "gen_ai.usage.input_tokens", value: { intValue: "9" } }, ...plantedAttributes],
            events: [{
              name: "exception",
              attributes: [{ key: "exception.message", value: secret("PLANTED_SPAN_EVENT") }],
            }],
          }],
        }],
      }],
    },
    metrics: {
      resourceMetrics: [{
        resource: { attributes: [
          { key: "service.name", value: { stringValue: "claude-code" } },
          { key: "process.command_line", value: secret("PLANTED_RESOURCE_COMMAND_LINE") },
        ] },
        scopeMetrics: [{
          scope,
          metrics: [{
            name: "claude_code.token.usage",
            sum: {
              dataPoints: [{
                timeUnixNano: nanos(3),
                asInt: "321",
                attributes: [
                  { key: "session.id", value: { stringValue: "KEPT_CONTROL_session" } },
                  { key: "type", value: { stringValue: "input" } },
                  { key: "prompt", value: secret("PLANTED_METRIC_PROMPT") },
                  { key: "file_path", value: { stringValue: "/Users/x/PLANTED_FILE_PATH.ts" } },
                ],
              }],
            },
          }],
        }],
      }],
    },
  };
}

async function stagePrivacy() {
  const fixture = await openFixture();
  try {
    const bodies = plantedBodies();
    const release = holdWriter(fixture.ledger);
    let answers: Answer[];
    try {
      answers = [
        await post(fixture.port, "/v1/logs", bodies.logs),
        await post(fixture.port, "/v1/traces", bodies.traces),
        await post(fixture.port, "/v1/metrics", bodies.metrics),
      ];
    } finally {
      release();
    }
    const directory = path.join(fixture.root, "otlp-spool");
    const files = spoolFiles(fixture.root);
    const texts = files.map((name) => fs.readFileSync(path.join(directory, name), "utf8"));
    const joined = texts.join("\n");

    const leaked = PLANTED.filter((marker) => joined.includes(marker));
    check(
      "g_no_planted_content_reaches_the_spool",
      answers.every((answer) => answer.status === 202) && files.length === 3 && leaked.length === 0,
      { statuses: answers.map((answer) => answer.status), files: files.length, leaked },
    );
    check(
      "g_positive_control_the_values_the_ledger_keeps_are_present",
      joined.includes("KEPT_CONTROL_conversation") && joined.includes("KEPT_CONTROL_session"),
      { conversation: joined.includes("KEPT_CONTROL_conversation"), session: joined.includes("KEPT_CONTROL_session") },
    );
    const modes = files.map((name) => fs.statSync(path.join(directory, name)).mode & 0o777);
    check(
      "g_the_spool_is_private_0700_and_0600",
      modes.every((mode) => mode === 0o600) && (fs.statSync(directory).mode & 0o777) === 0o700,
      { modes: modes.map((mode) => mode.toString(8)), directory: (fs.statSync(directory).mode & 0o777).toString(8) },
    );
    const spooledEvents = texts.flatMap((text) =>
      (JSON.parse(text) as { batch: { events: Array<{ event: { id: string } }> } }).batch.events.map((entry) => entry.event));
    const spooledSamples = texts.flatMap((text) =>
      (JSON.parse(text) as { batch: { metricSamples: Array<{ id: string; attrs: unknown }> } }).batch.metricSamples);
    await drainAll(fixture);
    const stored = new Map(
      (fixture.buffer.database.prepare("select id, payload_json as payloadJson from buffered_events").all() as Array<{ id: string; payloadJson: string }>)
        .map((row) => [row.id, row.payloadJson]),
    );
    const storedAttrs = new Map(
      (fixture.buffer.database.prepare("select id, attrs_json as attrsJson from metric_samples").all() as Array<{ id: string; attrsJson: string }>)
        .map((row) => [row.id, row.attrsJson]),
    );
    const eventMismatches = spooledEvents.filter((event) => stored.get(event.id) !== JSON.stringify(event)).length;
    const sampleMismatches = spooledSamples.filter((sample) => storedAttrs.get(sample.id) !== JSON.stringify(sample.attrs)).length;
    check(
      "g_each_spooled_row_is_exactly_the_row_the_ledger_stores",
      spooledEvents.length === 4 && spooledSamples.length === 1 && eventMismatches === 0 && sampleMismatches === 0,
      { events: spooledEvents.length, samples: spooledSamples.length, eventMismatches, sampleMismatches },
    );
    check(
      "g_events_that_carried_a_raw_working_directory_are_counted_not_written",
      (fixture.spool?.status().repoContextDropped ?? 0) >= 3,
      { repoContextDropped: fixture.spool?.status().repoContextDropped ?? null },
    );
  } finally {
    await fixture.close();
    fs.rmSync(fixture.root, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// H. Bounds, quarantine, pass ceilings and durable counters.
// ---------------------------------------------------------------------------
async function stageBounds() {
  let clock = Date.now();
  const root = privateTempDir("bounds");
  const fixture = await openFixture({ root, nowMs: () => clock, limits: { maxFilesPerPass: 2 } });
  try {
    const release = holdWriter(fixture.ledger);
    try {
      for (let index = 0; index < 5; index += 1) await post(fixture.port, "/v1/logs", usageLogs(`h${index}`, 2));
    } finally {
      release();
    }
    const firstPass = fixture.spool ? await fixture.spool.drain(fixture.buffer) : null;
    check(
      "h_a_pass_stops_at_its_file_ceiling_and_the_next_one_continues",
      firstPass?.replayed === 2 && firstPass.budgetExhausted === true && fixture.spool?.status().pendingFiles === 3,
      { firstPass, pending: fixture.spool?.status().pendingFiles },
    );
    // A busy ledger defers a pass without touching the files.
    const busyRelease = holdWriter(fixture.ledger);
    let busyPass;
    try {
      busyPass = fixture.spool ? await fixture.spool.drain(fixture.buffer) : null;
    } finally {
      busyRelease();
    }
    check(
      "h_a_busy_ledger_defers_the_pass_and_keeps_every_file",
      busyPass?.deferred === true && busyPass.replayed === 0 && fixture.spool?.status().pendingFiles === 3 &&
        (fixture.spool?.status().deferredPasses ?? 0) >= 1,
      { busyPass },
    );
    // A file that is not this uid's private 0600 file is quarantined, never replayed.
    const directory = path.join(root, "otlp-spool");
    const [victim, tampered] = spoolFiles(root).slice(0, 2);
    fs.chmodSync(path.join(directory, victim!), 0o644);
    const text = fs.readFileSync(path.join(directory, tampered!), "utf8");
    fs.writeFileSync(path.join(directory, tampered!), text.replace("\"gpt-5-codex\"", "\"gpt-5-tampered\""), { mode: 0o600 });
    const before = eventRows(fixture.buffer);
    const quarantinePass = fixture.spool ? await fixture.spool.drain(fixture.buffer) : null;
    const rejected = fs.existsSync(path.join(directory, "rejected")) ? fs.readdirSync(path.join(directory, "rejected")) : [];
    const afterQuarantine = eventRows(fixture.buffer);
    const lastPass = fixture.spool ? await fixture.spool.drain(fixture.buffer) : null;
    check(
      "h_untrusted_or_edited_files_are_quarantined_not_replayed",
      quarantinePass?.rejected === 2 && rejected.some((name) => name.includes(".spool_untrusted.")) &&
        rejected.some((name) => name.includes(".spool_digest_mismatch.")) && afterQuarantine === before &&
        lastPass?.replayed === 1 && eventRows(fixture.buffer) === before + 2 &&
        fixture.spool?.status().rejectedOnReplay === 2 && fixture.spool?.status().pendingFiles === 0,
      { quarantinePass, lastPass, rejected, afterQuarantine, events: eventRows(fixture.buffer), before },
    );
    // Age cap: an acknowledged file past the limit is deleted and counted as lost.
    const ageRelease = holdWriter(fixture.ledger);
    try {
      await post(fixture.port, "/v1/logs", usageLogs("h-old", 4));
    } finally {
      ageRelease();
    }
    clock += 8 * 24 * 60 * 60 * 1000;
    const agePass = fixture.spool ? await fixture.spool.drain(fixture.buffer) : null;
    const status = fixture.spool?.status();
    check(
      "h_age_cap_deletes_and_counts_files_older_than_seven_days",
      agePass?.expired === 1 && status?.droppedByCap.ageCap === 1 && status.droppedByCap.ageCapEvents === 4 &&
        status.pendingFiles === 0,
      { agePass, droppedByCap: status?.droppedByCap },
    );
    await fixture.close();
    const reopened = await openFixture({ root });
    try {
      const again = reopened.spool?.status();
      check(
        "h_counters_survive_a_restart",
        again?.spooled.requests === 6 && again.replayed.requests === 3 && again.droppedByCap.ageCap === 1 &&
          again.rejectedOnReplay === 2,
        again ?? null,
      );
    } finally {
      await reopened.close();
    }
  } finally {
    await fixture.close().catch(() => undefined);
    fs.rmSync(root, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// J. Replay failures: storage faults wait, a poison file is quarantined, and
//    cursor rows left behind by a crash are collected.
// ---------------------------------------------------------------------------
async function stageReplayFailures() {
  const fixture = await openFixture({ limits: { housekeepingEveryTicks: 1 } });
  const original = fixture.buffer.appendMany.bind(fixture.buffer);
  try {
    const release = holdWriter(fixture.ledger);
    try {
      await post(fixture.port, "/v1/logs", usageLogs("j1", 3));
      await post(fixture.port, "/v1/logs", usageLogs("j2", 3));
    } finally {
      release();
    }
    fixture.buffer.appendMany = (() => {
      throw Object.assign(new Error("disk full"), { code: "SQLITE_FULL" });
    }) as LocalEventBuffer["appendMany"];
    const faultPasses = [];
    for (let pass = 0; pass < 7; pass += 1) faultPasses.push(await fixture.spool!.drain(fixture.buffer));
    check(
      "j_a_storage_fault_defers_replay_and_never_quarantines_acknowledged_data",
      faultPasses.every((pass) => pass.deferred && pass.rejected === 0) && fixture.spool!.status().pendingFiles === 2 &&
        fixture.spool!.status().rejectedOnReplay === 0,
      { faultPasses: faultPasses.map((pass) => ({ deferred: pass.deferred, rejected: pass.rejected })) },
    );
    let calls = 0;
    fixture.buffer.appendMany = ((...args: Parameters<LocalEventBuffer["appendMany"]>) => {
      calls += 1;
      if (args[0].some((entry) => entry.event.sessionId === "conv-j1")) throw new TypeError("poison row");
      return original(...args);
    }) as LocalEventBuffer["appendMany"];
    const poisonPasses = [];
    for (let pass = 0; pass < 5; pass += 1) poisonPasses.push(await fixture.spool!.drain(fixture.buffer));
    const last = poisonPasses[poisonPasses.length - 1]!;
    check(
      "j_a_file_that_keeps_failing_is_quarantined_after_five_tries_and_stops_blocking",
      poisonPasses.slice(0, 4).every((pass) => pass.failed === 1 && pass.replayed === 0) && last.rejected === 1 &&
        last.replayed === 1 && eventRows(fixture.buffer) === 3 && fixture.spool!.status().pendingFiles === 0 &&
        fs.readdirSync(path.join(fixture.root, "otlp-spool", "rejected")).some((name) => name.includes(".replay_failed.")),
      { poisonPasses, calls, events: eventRows(fixture.buffer) },
    );
    fixture.buffer.appendMany = original;
    // A file removed behind the drain's back is dropped from the index, not quarantined.
    const vanishRelease = holdWriter(fixture.ledger);
    try {
      await post(fixture.port, "/v1/logs", usageLogs("j3", 2));
    } finally {
      vanishRelease();
    }
    const [vanishing] = spoolFiles(fixture.root);
    fs.unlinkSync(path.join(fixture.root, "otlp-spool", vanishing!));
    const vanishPass = await fixture.spool!.drain(fixture.buffer);
    check(
      "j_a_file_removed_outside_the_drain_is_forgotten_not_quarantined",
      vanishPass.rejected === 0 && vanishPass.replayed === 0 && fixture.spool!.status().pendingFiles === 0 &&
        fixture.spool!.status().rejectedOnReplay === 1,
      { vanishPass, status: fixture.spool!.status().pendingFiles },
    );
    fixture.buffer.database
      .prepare(`insert into maintenance_state (key, value, updated_at) values (?, ?, ?)`)
      .run("otlp_intake_spool:v1:1790000000000-1-1-deadbeef", JSON.stringify({ nextChunk: 1, chunks: 2, digest: "sha256:x" }), new Date().toISOString());
    await fixture.spool!.drain(fixture.buffer);
    check(
      "j_cursor_rows_whose_file_is_gone_are_collected",
      cursorRows(fixture.buffer).length === 0,
      { cursors: cursorRows(fixture.buffer) },
    );
  } finally {
    fixture.buffer.appendMany = original;
    await fixture.close();
    fs.rmSync(fixture.root, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// I. The real daemon arms the spool and its drain (isolated home and port).
// ---------------------------------------------------------------------------
function reserveLoopbackPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = http.createServer();
    server.once("listening", () => {
      const port = (server.address() as AddressInfo).port;
      server.close(() => resolve(port));
    });
    server.once("error", reject);
    server.listen(0, "127.0.0.1");
  });
}

function fetchStatus(port: number, token: string) {
  return new Promise<Record<string, unknown> | null>((resolve) => {
    const request = http.get(
      `http://127.0.0.1:${port}/status`,
      { timeout: 5_000, headers: { "x-plimsoll-token": token } },
      (response) => {
        let body = "";
        response.on("data", (chunk) => {
          body += chunk;
        });
        response.on("end", () => {
          try {
            resolve(JSON.parse(body) as Record<string, unknown>);
          } catch {
            resolve(null);
          }
        });
      },
    );
    request.on("error", () => resolve(null));
    request.on("timeout", () => {
      request.destroy();
      resolve(null);
    });
  });
}

async function stageDaemon() {
  const home = privateTempDir("daemon");
  const port = await reserveLoopbackPort();
  fs.writeFileSync(path.join(home, "collector.config.json"), `${JSON.stringify({ port }, null, 2)}\n`, { mode: 0o600 });
  const env = { ...process.env, PLIMSOLL_HOME: home };
  delete (env as Record<string, string | undefined>).PLIMSOLL_OTLP_SPOOL;
  const daemon = spawn(process.execPath, ["--import", "tsx", cliSource, "start"], {
    cwd: repoRoot,
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  daemon.stdout.setEncoding("utf8");
  daemon.stderr.setEncoding("utf8");
  daemon.stdout.on("data", (text: string) => {
    stdout += text;
  });
  daemon.stderr.on("data", (text: string) => {
    stderr += text;
  });
  try {
    const active = await waitFor(() => stdout.includes('"active"'), 90_000);
    const auth = readLocalIngestAuth(home);
    check("i_daemon_starts_in_an_isolated_home", active && auth !== null, { active, stdoutHead: stdout.slice(0, 300) });
    if (!active || !auth) return;
    const ledger = path.join(home, "work-ledger.sqlite");
    let answer: Answer = { status: -1, retryAfter: null, body: {} };
    // Take the writer lock once the daemon's own start-up writes have finished.
    const lockDeadline = Date.now() + 30_000;
    let release: (() => void) | null = null;
    while (!release && Date.now() < lockDeadline) {
      try {
        release = holdWriter(ledger);
      } catch {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
    try {
      answer = await post(port, "/v1/logs", usageLogs("i", 20), { "x-plimsoll-token": auth.codexProducer });
    } finally {
      release?.();
    }
    check(
      "i_the_daemon_spools_a_busy_otlp_request_and_answers_202",
      answer.status === 202 && answer.body.status === "otlp_spooled" && spoolFiles(home).length === 1,
      { answer: { status: answer.status, body: answer.body, error: answer.error }, files: spoolFiles(home).length },
    );
    let status: Record<string, unknown> | null = null;
    const replayed = await (async () => {
      const until = Date.now() + 30_000;
      while (Date.now() < until) {
        status = await fetchStatus(port, auth.managementRead);
        const spool = status?.otlpSpool as Record<string, any> | undefined;
        if (spool?.replayed?.requests === 1 && spool.pendingFiles === 0) return true;
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
      return false;
    })();
    const reader = new Database(ledger, { readonly: true, fileMustExist: true });
    const rows = count(reader, "select count(*) as n from buffered_events where source = 'codex' and event_type = 'assistant_response'");
    reader.close();
    check(
      "i_the_daemon_drain_replays_it_into_the_ledger_and_status_says_so",
      replayed && rows === 20 && spoolFiles(home).length === 0,
      { replayed, rows, otlpSpool: (status as Record<string, unknown> | null)?.otlpSpool ?? null, stderrTail: stderr.slice(-400) },
    );
  } finally {
    daemon.kill("SIGTERM");
    await waitFor(() => daemon.exitCode !== null || daemon.signalCode !== null, 20_000);
    if (daemon.exitCode === null && daemon.signalCode === null) daemon.kill("SIGKILL");
    fs.rmSync(home, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// Measurements for the REPORT: file size per event and drain throughput.
// ---------------------------------------------------------------------------
async function measure() {
  const fixture = await openFixture({ limits: { maxBytes: 512 * 1024 * 1024 } });
  try {
    const extra = [
      { key: "user.account_id", value: { stringValue: "acct-measure" } },
      { key: "auth_mode", value: { stringValue: "chatgpt" } },
      { key: "slug", value: { stringValue: "gpt-5-codex" } },
      { key: "event.kind", value: { stringValue: "response.completed" } },
      { key: "cached_token_count", value: { intValue: "4096" } },
      { key: "reasoning_token_count", value: { intValue: "512" } },
      { key: "tool_token_count", value: { intValue: "0" } },
    ];
    const release = holdWriter(fixture.ledger);
    const writeMs: number[] = [];
    try {
      for (let index = 0; index < 8; index += 1) {
        const at = performance.now();
        await post(fixture.port, "/v1/logs", usageLogs(`m${index}`, 512, extra));
        writeMs.push(performance.now() - at);
      }
    } finally {
      release();
    }
    const status = fixture.spool?.status();
    const bytesPerEvent = status ? status.pendingBytes / Math.max(1, status.spooled.events) : null;
    const started = performance.now();
    const drained = await drainAll(fixture, 1_000);
    const drainMs = performance.now() - started;
    Object.assign(measurements, {
      files: status?.pendingFiles ?? null,
      spooledEvents: status?.spooled.events ?? null,
      pendingBytes: status?.pendingBytes ?? null,
      bytesPerEvent,
      busyAnswerMsMax: writeMs.length ? Math.max(...writeMs) : null,
      drainPasses: drained.passes,
      drainActiveMs: Math.round(drainMs),
      replayedEvents: drained.events,
      eventsPerActiveSecond: drainMs > 0 ? Math.round(drained.events / (drainMs / 1_000)) : null,
    });
    check(
      "m_measurement_fixture_replayed_everything",
      eventRows(fixture.buffer) === 4_096,
      { events: eventRows(fixture.buffer) },
    );
  } finally {
    await fixture.close();
    fs.rmSync(fixture.root, { recursive: true, force: true });
  }
}

/**
 * `require`, not `import()`: under tsx a dynamic import loads a second module
 * graph, and the repository sidecar the spool counts lives in a module-level
 * WeakMap that must be the one `otlp.ts` attached it in.
 */
function loadSpoolModule(): SpoolModule | null {
  try {
    return require("../packages/collector-cli/src/otlp-spool") as SpoolModule;
  } catch {
    return null;
  }
}

async function main() {
  spoolModule = loadSpoolModule();
  if (process.argv[2] === "child") {
    await childMain(process.argv[3]!, process.argv[4]!);
    return;
  }
  const startedAt = Date.now();
  const only = process.env.OTLP_SPOOL_PROOF_ONLY?.split(",");
  const stages: Array<[string, () => Promise<void>]> = [
    ["busy", stageBusySpoolAndReplay],
    ["partial", stagePartialCommit],
    ["deadline", stageDeadlineAndLoopPressure],
    ["full", stageSpoolFullAndKillSwitch],
    ["crash-ack", stageCrashBetweenWriteAndAck],
    ["crash-replay", stageCrashMidReplay],
    ["privacy", stagePrivacy],
    ["bounds", stageBounds],
    ["failures", stageReplayFailures],
    ["daemon", stageDaemon],
    ["measure", measure],
  ];
  for (const [name, stage] of stages) {
    if (only && !only.includes(name)) continue;
    const at = Date.now();
    try {
      await stage();
    } catch (error) {
      check(`${name}_stage_completed`, false, { error: error instanceof Error ? error.stack : String(error) });
    }
    console.log(JSON.stringify({ stage: name, ms: Date.now() - at }));
  }
  const failures = checks.filter((entry) => !entry.passed).map((entry) => entry.name);
  console.log(JSON.stringify({
    proof: "otlp-intake-spool",
    bead: "eco-6hoxj.163.17",
    spoolModulePresent: spoolModule !== null,
    passed: failures.length === 0,
    checks: checks.length,
    failures,
    measurements,
    durationMs: Date.now() - startedAt,
  }));
  if (failures.length > 0) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack : String(error));
  process.exitCode = 1;
});
