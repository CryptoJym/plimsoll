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
import zlib from "node:zlib";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { LOCAL_HTTP_LIMITS } from "../packages/collector-cli/src/http-boundary";
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
) {
  const startedAt = performance.now();
  const bodyBuffer = Buffer.isBuffer(body) ? body : Buffer.from(body);
  return new Promise<HttpResult>((resolve, reject) => {
    const client = http.request(
      {
        host: "127.0.0.1",
        port,
        path: route,
        method: "POST",
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

    const invalidHost = await request(port, "/v1/logs", "{}", {
      host: SENTINELS[0],
      "x-plimsoll-source": "codex",
    });
    check(
      "host_allowlist_rejects_non_loopback_before_write",
      stableRejection(invalidHost, "host_not_allowed", 421),
      invalidHost,
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
    check(
      "otlp_record_cardinality_rejected_before_write",
      stableRejection(highRecords, "otlp_record_limit_exceeded", 413) &&
        highRecords.elapsedMs < 500,
      highRecords,
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
    check(
      "all_rejection_receipts_are_bounded_and_value_free",
      warnings.length === 14 &&
        warnings.every((warning) => Buffer.byteLength(warning) <= 128) &&
        SENTINELS.every((sentinel) => !warningText.includes(sentinel)),
      {
        warningCount: warnings.length,
        maxWarningBytes: Math.max(...warnings.map((warning) => Buffer.byteLength(warning))),
      },
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
  console.error(error instanceof Error ? error.name : "ProofFailure");
  process.exitCode = 1;
});
