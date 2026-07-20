/**
 * Focused proof for GitHub #108's bounded local HTTP ingress slice.
 *
 * Uses only a temporary Plimsoll home, SQLite ledger, and ephemeral loopback
 * port. It never reads or writes installed tool config, the live ledger, or
 * the live collector.
 *
 * Run: pnpm proof:http-boundary
 */
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import { Worker } from "node:worker_threads";
import zlib from "node:zlib";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  LOCAL_HTTP_LIMITS,
  isAllowedLocalHostValue,
} from "../packages/collector-cli/src/http-boundary";
import { createCollectorServer } from "../packages/collector-cli/src/server";

type Receipt = { error?: unknown; reason?: unknown; [key: string]: unknown };
type HttpResult = {
  status: number;
  body: Receipt;
  bodyBytes: number;
  elapsedMs: number;
  headers: http.IncomingHttpHeaders;
};
type Check = { name: string; passed: boolean; detail: unknown };

const checks: Check[] = [];
const SENTINELS = [
  "HOST_VALUE_MUST_NOT_LEAK.example",
  "ORIGIN_VALUE_MUST_NOT_LEAK.example",
  "SOURCE_VALUE_MUST_NOT_LEAK",
  "BODY_VALUE_MUST_NOT_LEAK",
];

function check(name: string, passed: boolean, detail: unknown) {
  checks.push({ name, passed, detail });
}

function request(
  port: number,
  route: string,
  body: Buffer | string,
  headers: Record<string, string> = {},
  method = "POST",
) {
  const startedAt = performance.now();
  const bodyBuffer = Buffer.isBuffer(body) ? body : Buffer.from(body);
  return new Promise<HttpResult>((resolve, reject) => {
    const client = http.request(
      {
        host: "127.0.0.1",
        port,
        path: route,
        method,
        headers: {
          connection: "close",
          "content-type": "application/json",
          "content-length": String(bodyBuffer.length),
          ...headers,
        },
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        response.on("end", () => {
          const responseBody = Buffer.concat(chunks);
          let parsed: Receipt = {};
          try {
            parsed = JSON.parse(responseBody.toString("utf8")) as Receipt;
          } catch {
            parsed = {};
          }
          resolve({
            status: response.statusCode ?? 0,
            body: parsed,
            bodyBytes: responseBody.length,
            elapsedMs: performance.now() - startedAt,
            headers: response.headers,
          });
        });
      },
    );
    client.setTimeout(5_000, () => client.destroy(new Error("ProofClientTimeout")));
    client.on("error", reject);
    client.end(bodyBuffer);
  });
}

function hostMultiplicityRequest(port: number, hosts: string[]) {
  const startedAt = performance.now();
  return new Promise<HttpResult>((resolve, reject) => {
    const rawHeaders = [
      "Connection",
      "close",
      ...hosts.flatMap((host) => ["Host", host]),
    ];
    const client = http.request(
      {
        host: "127.0.0.1",
        port,
        path: "/status",
        method: "GET",
        setHost: false,
        headers: rawHeaders,
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        response.on("end", () => {
          const responseBody = Buffer.concat(chunks);
          let parsed: Receipt = {};
          try {
            parsed = JSON.parse(responseBody.toString("utf8")) as Receipt;
          } catch {
            parsed = {};
          }
          resolve({
            status: response.statusCode ?? 0,
            body: parsed,
            bodyBytes: responseBody.length,
            elapsedMs: performance.now() - startedAt,
            headers: response.headers,
          });
        });
      },
    );
    client.setTimeout(5_000, () => client.destroy(new Error("ProofClientTimeout")));
    client.on("error", reject);
    client.end();
  });
}

function slowRequest(port: number) {
  const startedAt = performance.now();
  return new Promise<HttpResult>((resolve, reject) => {
    const client = http.request(
      {
        host: "127.0.0.1",
        port,
        path: "/v1/logs",
        method: "POST",
        headers: {
          connection: "close",
          "content-type": "application/json",
          "x-plimsoll-source": "codex",
        },
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        response.on("end", () => {
          const responseBody = Buffer.concat(chunks);
          resolve({
            status: response.statusCode ?? 0,
            body: JSON.parse(responseBody.toString("utf8")) as Receipt,
            bodyBytes: responseBody.length,
            elapsedMs: performance.now() - startedAt,
            headers: response.headers,
          });
        });
      },
    );
    client.setTimeout(5_000, () => client.destroy(new Error("ProofClientTimeout")));
    client.on("error", reject);
    client.write("{");
  });
}

async function prepareStatusProbeDuringAppend(port: number, signal: SharedArrayBuffer) {
  const worker = new Worker(
    `
      const http = require("node:http");
      const { parentPort, workerData } = require("node:worker_threads");
      const state = new Int32Array(workerData.signal);
      const wait = Atomics.wait(state, 0, 0, 5_000);
      if (wait === "timed-out") {
        parentPort.postMessage({ error: "AppendSignalTimeout" });
      } else {
        const startedAtMs = Date.now();
        const started = performance.now();
        const request = http.request({
          host: "127.0.0.1",
          port: workerData.port,
          path: "/status",
          method: "GET",
          headers: { connection: "close" },
        }, (response) => {
          response.resume();
          response.on("end", () => parentPort.postMessage({
            status: response.statusCode || 0,
            elapsedMs: performance.now() - started,
            startedAtMs,
          }));
        });
        request.setTimeout(5_000, () => request.destroy(new Error("StatusProbeTimeout")));
        request.on("error", () => parentPort.postMessage({ error: "StatusProbeFailed" }));
        request.end();
      }
    `,
    { eval: true, workerData: { port, signal } },
  );
  await new Promise<void>((resolve, reject) => {
    worker.once("online", resolve);
    worker.once("error", reject);
  });
  const result = new Promise<{ status: number; elapsedMs: number; startedAtMs: number }>(
    (resolve, reject) => {
      worker.once("message", (message: {
        error?: string;
        status?: number;
        elapsedMs?: number;
        startedAtMs?: number;
      }) => {
        if (message.error) {
          reject(new Error(message.error));
          return;
        }
        resolve({
          status: message.status ?? 0,
          elapsedMs: message.elapsedMs ?? Number.POSITIVE_INFINITY,
          startedAtMs: message.startedAtMs ?? Number.POSITIVE_INFINITY,
        });
      });
      worker.once("error", reject);
    },
  ).finally(() => worker.terminate());
  return { result };
}

function totalChanges(buffer: LocalEventBuffer) {
  return Number(
    (buffer.database.prepare("select total_changes() as n").get() as { n: number }).n,
  );
}

function stableRejection(result: HttpResult, reason: string, status: number) {
  return (
    result.status === status &&
    result.body.error === "collector_request_rejected" &&
    result.body.reason === reason &&
    Object.keys(result.body).length === 2 &&
    result.bodyBytes <= 128 &&
    SENTINELS.every((sentinel) => !JSON.stringify(result.body).includes(sentinel))
  );
}

function maxRecordBody(cwdSentinel: string) {
  return JSON.stringify({
    resourceLogs: [{
      scopeLogs: [{
        logRecords: Array.from({ length: LOCAL_HTTP_LIMITS.otlpRecords }, (_, index) => ({
          timeUnixNano: String(1_760_000_000_000_000_000n + BigInt(index)),
          attributes: [
            { key: "cwd", value: { stringValue: `${cwdSentinel}/${index}` } },
            { key: "gen_ai.usage.input_tokens", value: { intValue: "1" } },
            { key: "gen_ai.usage.output_tokens", value: { intValue: "2" } },
          ],
        })),
      }],
    }],
  });
}

async function isolatedMaxRecordRun(index: number) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-http-max128-${index}-`));
  const previousHome = process.env.PLIMSOLL_HOME;
  process.env.PLIMSOLL_HOME = home;
  const buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer);
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;
    const cwdSentinel = `/HTTP_MAX128_PRIVATE_CWD_${index}`;
    const body = maxRecordBody(cwdSentinel);
    const signal = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
    const signalView = new Int32Array(signal);
    buffer.database.function("proof_isolated_max_append_notify", () => {
      Atomics.store(signalView, 0, 1);
      Atomics.notify(signalView, 0);
      return 1;
    });
    buffer.database.exec(`
      create temp table proof_isolated_max_append_gate (
        singleton integer primary key check (singleton = 1)
      );
      create temp trigger proof_isolated_max_append_started
      after insert on main.buffered_events
      when not exists (select 1 from proof_isolated_max_append_gate)
      begin
        insert into proof_isolated_max_append_gate values (1);
        select proof_isolated_max_append_notify();
      end;
    `);
    const { result: statusPromise } = await prepareStatusProbeDuringAppend(port, signal);
    const acceptedPromise = request(port, "/v1/logs", body, {
      "x-plimsoll-source": "codex",
    });
    const [accepted, status] = await Promise.all([acceptedPromise, statusPromise]);
    const facts = buffer.database.prepare(
      `select count(*) as events,
         coalesce(sum(input_tokens), 0) as inputTokens,
         coalesce(sum(output_tokens), 0) as outputTokens,
         coalesce(sum(instr(payload_json, ?)), 0) as rawCwdMatches
       from buffered_events`,
    ).get(cwdSentinel) as {
      events: number;
      inputTokens: number;
      outputTokens: number;
      rawCwdMatches: number;
    };
    const contexts = Number((buffer.database.prepare(
      `select count(*) as count from repo_context_event_links`,
    ).get() as { count: number }).count);
    const pending = Number((buffer.database.prepare(
      `select count(*) as count from repo_context_event_links where fill_pending = 1`,
    ).get() as { count: number }).count);
    const handoffs = Number((buffer.database.prepare(
      `select count(*) as count from repo_context_handoffs`,
    ).get() as { count: number }).count);
    const overflow = buffer.repoContextUnknownCounters()
      .find((row) => row.reason === "queue_overflow")?.droppedCount ?? 0;
    return {
      index,
      acceptedStatus: accepted.status,
      accepted: accepted.body.accepted,
      acceptedEvents: accepted.body.events,
      acceptedRecordCount: accepted.body.recordCount,
      acceptedMs: Number(accepted.elapsedMs.toFixed(2)),
      statusCode: status.status,
      statusMs: Number(status.elapsedMs.toFixed(2)),
      facts,
      contexts,
      pending,
      handoffs,
      overflow,
      signal: Atomics.load(signalView, 0),
    };
  } finally {
    if (server.listening) {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    buffer.close();
    fs.rmSync(home, { recursive: true, force: true });
    if (previousHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = previousHome;
  }
}

async function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-http-boundary-"));
  process.env.PLIMSOLL_HOME = tempDir;
  const buffer = new LocalEventBuffer(path.join(tempDir, "proof-ledger.sqlite"));
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer);
  const warnings: string[] = [];
  const originalWarn = console.warn;
  console.warn = (...values: unknown[]) => warnings.push(values.map(String).join(" "));

  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;
    const changesBefore = totalChanges(buffer);
    const rssBefore = process.memoryUsage().rss;
    const cpuBefore = process.cpuUsage();

    const validHostValues = [
      "localhost",
      "LOCALHOST",
      "LoCaLhOsT:1",
      "127.0.0.1",
      "127.0.0.1:65535",
      "[::1]",
      "[::1]:48271",
    ];
    const invalidHostValues = [
      SENTINELS[0],
      "localhost/path",
      "localhost?x=1",
      "localhost#frag",
      "127.0.0.1/path",
      "[::1]/path",
      "localhost:",
      "127.0.0.1:",
      "[::1]:",
      "2130706433",
      "0177.0.0.1",
      "127.1",
      "0x7f000001",
      "017700000001",
      "user@localhost",
      "localhost.",
      "local host",
      "localhost :48271",
      "localhost: 48271",
      "localhost:0",
      "localhost:00",
      "localhost:01",
      "localhost:65536",
      "localhost:99999",
      "localhost:+80",
      "localhost:-1",
      "localhost:80.0",
      "localhost:abc",
      "[0:0:0:0:0:0:0:1]",
    ];
    const directInvalidWhitespaceValues = [
      " localhost",
      "localhost ",
      "\tlocalhost",
      "localhost\t",
      "localhost:\t48271",
    ];
    check(
      "host_value_grammar_accepts_only_exact_case_insensitive_loopback_forms",
      validHostValues.every(isAllowedLocalHostValue) &&
        [...invalidHostValues, ...directInvalidWhitespaceValues].every(
          (host) => !isAllowedLocalHostValue(host),
        ),
      {
        accepted: validHostValues,
        rejected: invalidHostValues.length + directInvalidWhitespaceValues.length,
      },
    );

    const validHostResponses = await Promise.all(
      validHostValues.map((host) => request(port, "/not-found", "", { host }, "GET")),
    );
    check(
      "actual_requests_accept_only_the_canonical_loopback_host_matrix",
      validHostResponses.every((result) => result.status === 404),
      validHostResponses.map((result, index) => ({
        host: validHostValues[index],
        status: result.status,
      })),
    );

    const invalidHostResponses = await Promise.all(
      invalidHostValues.map((host) => request(port, "/status", "", { host }, "GET")),
    );
    check(
      "actual_status_requests_reject_normalization_alias_path_query_fragment_userinfo_whitespace_and_port_edges",
      invalidHostResponses.every(
        (result) => stableRejection(result, "host_not_allowed", 421),
      ) &&
        invalidHostResponses.every((result, index) =>
          !JSON.stringify(result.body).includes(invalidHostValues[index]!),
        ),
      invalidHostResponses.map((result, index) => ({
        host: invalidHostValues[index],
        status: result.status,
        reason: result.body.reason,
      })),
    );

    const duplicateSameHost = await hostMultiplicityRequest(port, ["localhost", "localhost"]);
    const duplicateDifferentHost = await hostMultiplicityRequest(port, [
      "localhost",
      "127.0.0.1",
    ]);
    const hostMultiplicityResponses = [duplicateSameHost, duplicateDifferentHost];
    check(
      "duplicate_host_headers_reject_before_route_work",
      hostMultiplicityResponses.every((result) =>
        stableRejection(result, "host_not_allowed", 421),
      ),
      hostMultiplicityResponses,
    );

    const hookOrigin = await request(port, "/hooks/codex", "{}", {
      origin: `https://${SENTINELS[1]}`,
    });
    const otlpOrigin = await request(port, "/v1/logs", "{}", {
      origin: "http://127.0.0.1:48271",
      "x-plimsoll-source": "codex",
    });
    check(
      "all_browser_origins_rejected_without_cors",
      stableRejection(hookOrigin, "browser_origin_not_allowed", 403) &&
        stableRejection(otlpOrigin, "browser_origin_not_allowed", 403) &&
        hookOrigin.headers["access-control-allow-origin"] === undefined &&
        otlpOrigin.headers["access-control-allow-origin"] === undefined,
      { hookOrigin, otlpOrigin },
    );

    const missingSource = await request(port, "/v1/traces", "{}");
    const hostileSource = await request(port, "/v1/traces", "{}", {
      "x-plimsoll-source": SENTINELS[2],
    });
    const swappedHookSource = await request(port, "/hooks/claude-code", "{}", {
      "x-plimsoll-source": "codex",
    });
    check(
      "source_spoof_missing_unknown_and_hook_path_swapped_claims_rejected",
      stableRejection(missingSource, "source_required", 401) &&
        stableRejection(hostileSource, "source_not_allowed", 401) &&
        stableRejection(swappedHookSource, "source_mismatch", 401),
      { missingSource, hostileSource, swappedHookSource },
    );

    const oversizedBody = Buffer.alloc(LOCAL_HTTP_LIMITS.compressedBodyBytes + 1, "x");
    const oversized = await request(port, "/v1/logs", oversizedBody, {
      "x-plimsoll-source": "codex",
    });
    check(
      "compressed_transport_bytes_have_fixed_ceiling",
      stableRejection(oversized, "compressed_body_too_large", 413),
      { status: oversized.status, reason: oversized.body.reason, elapsedMs: oversized.elapsedMs },
    );

    const bomb = zlib.gzipSync(
      Buffer.from(
        JSON.stringify({
          resourceLogs: [],
          padding: "x".repeat(LOCAL_HTTP_LIMITS.decodedBodyBytes + 1),
        }),
      ),
    );
    const bombResult = await request(port, "/v1/logs", bomb, {
      "content-encoding": "gzip",
      "x-plimsoll-source": "codex",
    });
    check(
      "compression_bomb_stops_at_decoded_ceiling",
      stableRejection(bombResult, "decoded_body_too_large", 413) &&
        bombResult.elapsedMs < 1_000,
      { compressedBytes: bomb.length, elapsedMs: bombResult.elapsedMs, receipt: bombResult.body },
    );

    const highRatioBody = Buffer.from(
      JSON.stringify({ resourceLogs: [], padding: "y".repeat(128 * 1024) }),
    );
    const highRatio = await request(port, "/v1/logs", zlib.gzipSync(highRatioBody), {
      "content-encoding": "gzip",
      "x-plimsoll-source": "codex",
    });
    check(
      "compression_ratio_has_fixed_ceiling",
      stableRejection(highRatio, "compression_ratio_too_large", 413),
      highRatio,
    );

    const deepBody = `${"[".repeat(LOCAL_HTTP_LIMITS.jsonDepth + 1)}${JSON.stringify(SENTINELS[3])}${"]".repeat(LOCAL_HTTP_LIMITS.jsonDepth + 1)}`;
    const deep = await request(port, "/v1/logs", deepBody, {
      "x-plimsoll-source": "codex",
    });
    check(
      "deep_json_rejected_before_parse_or_write",
      stableRejection(deep, "json_depth_exceeded", 413) && deep.elapsedMs < 500,
      deep,
    );

    const highNodeBody = JSON.stringify(
      Array.from({ length: LOCAL_HTTP_LIMITS.jsonNodes + 1 }, () => 0),
    );
    const highNodes = await request(port, "/hooks/codex", highNodeBody, {
      "x-plimsoll-source": "codex",
    });
    check(
      "flat_json_node_cardinality_rejected_before_write",
      stableRejection(highNodes, "json_node_limit_exceeded", 413) &&
        highNodes.elapsedMs < 500,
      { bodyBytes: Buffer.byteLength(highNodeBody), elapsedMs: highNodes.elapsedMs },
    );

    const changesBeforeHighRecords = totalChanges(buffer);
    const highRecordEnvelope = {
      resourceLogs: [
        {
          scopeLogs: [
            { logRecords: Array.from({ length: LOCAL_HTTP_LIMITS.otlpRecords + 1 }, () => ({})) },
          ],
        },
      ],
    };
    const highRecords = await request(port, "/v1/logs", JSON.stringify(highRecordEnvelope), {
      "x-plimsoll-source": "codex",
    });
    const changesAfterHighRecords = totalChanges(buffer);
    check(
      "otlp_record_cardinality_rejected_before_write",
      stableRejection(highRecords, "otlp_record_limit_exceeded", 413) &&
        highRecords.elapsedMs < 50 &&
        changesAfterHighRecords === changesBeforeHighRecords &&
        buffer.repoContextQueueStatus().queued === 0,
      {
        status: highRecords.status,
        reason: highRecords.body.reason,
        elapsedMs: highRecords.elapsedMs,
        changesBefore: changesBeforeHighRecords,
        changesAfter: changesAfterHighRecords,
      },
    );

    const highAttributeEnvelope = {
      resourceSpans: [
        {
          scopeSpans: [
            {
              spans: [
                {
                  attributes: Array.from(
                    { length: LOCAL_HTTP_LIMITS.otlpAttributesPerContainer + 1 },
                    (_, index) => ({ key: `attr-${index}`, value: { intValue: "1" } }),
                  ),
                },
              ],
            },
          ],
        },
      ],
    };
    const highAttributes = await request(
      port,
      "/v1/traces",
      JSON.stringify(highAttributeEnvelope),
      { "x-plimsoll-source": "codex" },
    );
    check(
      "otlp_attribute_cardinality_rejected_before_write",
      stableRejection(highAttributes, "otlp_attribute_limit_exceeded", 413) &&
        highAttributes.elapsedMs < 500,
      highAttributes,
    );

    const deadline = await slowRequest(port);
    check(
      "request_deadline_rejects_stalled_body",
      stableRejection(deadline, "request_deadline_exceeded", 408) &&
        deadline.elapsedMs >= LOCAL_HTTP_LIMITS.requestDeadlineMs - 100 &&
        deadline.elapsedMs <= LOCAL_HTTP_LIMITS.requestDeadlineMs + 750,
      { deadlineMs: LOCAL_HTTP_LIMITS.requestDeadlineMs, elapsedMs: deadline.elapsedMs },
    );

    const changesAfterRejections = totalChanges(buffer);
    const rssGrowthBytes = Math.max(0, process.memoryUsage().rss - rssBefore);
    const cpuUsed = process.cpuUsage(cpuBefore);
    const cpuUsedMs = (cpuUsed.user + cpuUsed.system) / 1_000;
    check(
      "rejections_have_zero_ledger_mutation_and_bounded_cpu_rss",
      changesAfterRejections === changesBefore &&
        cpuUsedMs <= 1_000 &&
        rssGrowthBytes <= 96 * 1024 * 1024,
      { changesBefore, changesAfterRejections, cpuUsedMs, rssGrowthBytes },
    );

    const warningText = warnings.join("\n");
    const expectedWarningCount = invalidHostValues.length + hostMultiplicityResponses.length + 13;
    check(
      "all_rejection_receipts_are_bounded_and_value_free",
      warnings.length === expectedWarningCount &&
        warnings.every((warning) => Buffer.byteLength(warning) <= 128) &&
        SENTINELS.every((sentinel) => !warningText.includes(sentinel)),
      {
        warningCount: warnings.length,
        expectedWarningCount,
        maxWarningBytes: Math.max(...warnings.map((warning) => Buffer.byteLength(warning))),
      },
    );

    const maxCwdSentinel = "/HTTP_MAX_PRIVATE_CWD";
    const maxBody = maxRecordBody(maxCwdSentinel);
    const appendSignal = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
    const appendSignalView = new Int32Array(appendSignal);
    buffer.database.function("proof_max_append_notify", () => {
      Atomics.store(appendSignalView, 0, 1);
      Atomics.notify(appendSignalView, 0);
      return 1;
    });
    buffer.database.exec(`
      create temp table proof_max_append_gate (
        singleton integer primary key check (singleton = 1)
      );
      create temp trigger proof_max_append_started
      after insert on main.buffered_events
      when not exists (select 1 from proof_max_append_gate)
      begin
        insert into proof_max_append_gate values (1);
        select proof_max_append_notify();
      end;
    `);
    const { result: statusProbe } = await prepareStatusProbeDuringAppend(port, appendSignal);
    let maxSettledAtMs = Number.POSITIVE_INFINITY;
    const maxAcceptedPromise = request(port, "/v1/logs", maxBody, {
      "x-plimsoll-source": "codex",
    }).finally(() => {
      maxSettledAtMs = Date.now();
    });
    const [maxAccepted, statusDuringMax] = await Promise.all([
      maxAcceptedPromise,
      statusProbe,
    ]);
    buffer.database.exec(`
      drop trigger proof_max_append_started;
      drop table proof_max_append_gate;
    `);
    const maxFacts = buffer.database
      .prepare(
        `select count(*) as count,
           (select count(*) from repo_context_event_links) as contexts,
           coalesce(sum(input_tokens), 0) as inputTokens,
           coalesce(sum(output_tokens), 0) as outputTokens,
           coalesce(sum(instr(payload_json, ?)), 0) as rawCwdMatches
         from buffered_events`,
      )
      .get(maxCwdSentinel) as {
        count: number;
        contexts: number;
        inputTokens: number;
        outputTokens: number;
        rawCwdMatches: number;
      };
    const maxHandoffs = Number((buffer.database
      .prepare(`select count(*) as count from repo_context_handoffs`)
      .get() as { count: number }).count);
    const maxPendingContexts = Number((buffer.database
      .prepare(
        `select count(*) as count from repo_context_event_links where fill_pending = 1`,
      )
      .get() as { count: number }).count);
    const maxOverflow = buffer.repoContextUnknownCounters()
      .find((row) => row.reason === "queue_overflow")?.droppedCount;
    check(
      "maximum_128_record_http_path_is_exact_durable_and_concurrently_available",
      Buffer.byteLength(maxBody) <= LOCAL_HTTP_LIMITS.compressedBodyBytes &&
        maxAccepted.status === 202 && maxAccepted.body.accepted === true &&
        maxAccepted.body.events === LOCAL_HTTP_LIMITS.otlpRecords &&
        maxAccepted.body.recordCount === LOCAL_HTTP_LIMITS.otlpRecords &&
        maxAccepted.elapsedMs <= 500 &&
        maxFacts.count === LOCAL_HTTP_LIMITS.otlpRecords &&
        maxFacts.contexts === LOCAL_HTTP_LIMITS.otlpRecords &&
        maxFacts.inputTokens === LOCAL_HTTP_LIMITS.otlpRecords &&
        maxFacts.outputTokens === LOCAL_HTTP_LIMITS.otlpRecords * 2 &&
        maxFacts.rawCwdMatches === 0 &&
        buffer.repoContextQueueStatus().queued === 128 &&
        maxHandoffs === 128 &&
        maxPendingContexts === 128 &&
        (maxOverflow ?? 0) === 0 &&
        Atomics.load(appendSignalView, 0) === 1 &&
        statusDuringMax.startedAtMs <= maxSettledAtMs &&
        statusDuringMax.status === 200 && statusDuringMax.elapsedMs <= 250,
      {
        acceptedStatus: maxAccepted.status,
        acceptedElapsedMs: Math.round(maxAccepted.elapsedMs * 100) / 100,
        bodyBytes: Buffer.byteLength(maxBody),
        events: maxFacts.count,
        inputTokens: maxFacts.inputTokens,
        outputTokens: maxFacts.outputTokens,
        queued: buffer.repoContextQueueStatus().queued,
        handoffs: maxHandoffs,
        pendingContexts: maxPendingContexts,
        overflow: maxOverflow,
        concurrentStatusIssuedBeforeMaxSettled:
          statusDuringMax.startedAtMs <= maxSettledAtMs,
        statusCode: statusDuringMax.status,
        statusElapsedMs: Math.round(statusDuringMax.elapsedMs * 100) / 100,
      },
    );

    const isolatedMaxRuns: Awaited<ReturnType<typeof isolatedMaxRecordRun>>[] = [];
    for (let index = 0; index < 10; index += 1) {
      isolatedMaxRuns.push(await isolatedMaxRecordRun(index));
    }
    check(
      "ten_isolated_max128_runs_preserve_status_and_exact_capture_budgets",
      isolatedMaxRuns.length === 10 && isolatedMaxRuns.every((run) =>
        run.acceptedStatus === 202 && run.accepted === true &&
        run.acceptedEvents === LOCAL_HTTP_LIMITS.otlpRecords &&
        run.acceptedRecordCount === LOCAL_HTTP_LIMITS.otlpRecords &&
        run.acceptedMs <= 500 &&
        run.statusCode === 200 && run.statusMs <= 250 &&
        run.facts.events === LOCAL_HTTP_LIMITS.otlpRecords &&
        run.facts.inputTokens === LOCAL_HTTP_LIMITS.otlpRecords &&
        run.facts.outputTokens === LOCAL_HTTP_LIMITS.otlpRecords * 2 &&
        run.facts.rawCwdMatches === 0 &&
        run.contexts === LOCAL_HTTP_LIMITS.otlpRecords &&
        run.pending === LOCAL_HTTP_LIMITS.otlpRecords &&
        run.handoffs === LOCAL_HTTP_LIMITS.otlpRecords &&
        run.overflow === 0 && run.signal === 1
      ),
      isolatedMaxRuns,
    );

    const accepted = await request(
      port,
      "/hooks/codex",
      JSON.stringify({
        hook_event_name: "Stop",
        session_id: "11111111-2222-4333-8444-555555555555",
        timestamp: "2026-07-17T12:00:00.000Z",
      }),
      { host: `localhost:${port}`, "x-plimsoll-source": "codex" },
    );
    check(
      "valid_loopback_source_still_writes_once",
      accepted.status === 202 && totalChanges(buffer) > changesAfterRejections,
      { status: accepted.status, accepted: accepted.body.accepted },
    );
  } finally {
    console.warn = originalWarn;
    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
    fs.rmSync(tempDir, { recursive: true, force: true });
  }

  for (const result of checks) {
    console.log(`${result.passed ? "PASS" : "FAIL"} ${result.name} ${JSON.stringify(result.detail)}`);
  }
  const failed = checks.filter((result) => !result.passed);
  console.log(
    JSON.stringify({
      checks: checks.length,
      passed: checks.length - failed.length,
      failed: failed.length,
      limits: LOCAL_HTTP_LIMITS,
    }),
  );
  if (failed.length > 0) process.exitCode = 1;
}

main().catch((error) => {
  const name = error instanceof Error ? error.name : "ProofFailure";
  const message = error instanceof Error ? error.message : "";
  const safeReason = /^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(message)
    ? message
    : "http_boundary_proof_failed";
  console.error(JSON.stringify({ error: name, reason: safeReason }));
  process.exitCode = 1;
});
