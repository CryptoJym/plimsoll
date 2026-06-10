import fs from "node:fs";
import http from "node:http";
import zlib from "node:zlib";

import { LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import type { ToolSource } from "../../shared/src/index";
import { appendForwardedHook } from "./forwarder";
import { explodeOtlpPayload } from "./otlp";
import {
  dashboardRepos,
  dashboardSessionDetail,
  dashboardSessions,
  dashboardSummary,
} from "./dashboard-api";

let dashboardHtml: string | undefined;
function loadDashboardHtml() {
  dashboardHtml ??= fs.readFileSync(new URL("./dashboard.html", import.meta.url), "utf8");
  return dashboardHtml;
}

function sendJson(response: http.ServerResponse, body: unknown, status = 200) {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

function readRequestBody(request: http.IncomingMessage) {
  return new Promise<Buffer>((resolve, reject) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });
    request.on("end", () => resolve(Buffer.concat(chunks)));
    request.on("error", reject);
  });
}

function firstHeader(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}

function decodeRequestBody(request: http.IncomingMessage, body: Buffer) {
  const contentEncoding = firstHeader(request.headers["content-encoding"])?.toLowerCase() ?? "identity";
  const decoded =
    contentEncoding === "gzip"
      ? zlib.gunzipSync(body)
      : contentEncoding === "deflate"
        ? zlib.inflateSync(body)
        : contentEncoding === "br"
          ? zlib.brotliDecompressSync(body)
          : body;

  return {
    bodyBytes: body.length,
    contentEncoding,
    decodedBytes: decoded.length,
    text: decoded.toString("utf8"),
  };
}

function sourceFromPath(url = ""): ToolSource {
  if (url.includes("claude")) return "claude_code";
  if (url.includes("codex")) return "codex";
  return "unknown";
}

function sourceFromHeaders(request: http.IncomingMessage): ToolSource | undefined {
  const value = request.headers["x-plimsoll-source"] ?? request.headers["x-cfo-one-source"];
  const source = Array.isArray(value) ? value[0] : value;
  if (source === "claude_code" || source === "codex") {
    return source;
  }

  return undefined;
}

export function createCollectorServer(config: CollectorConfig, buffer: LocalEventBuffer) {
  return http.createServer(async (request, response) => {
    try {
      if (request.method === "GET" && request.url === "/status") {
        sendJson(response, {
          ok: true,
          dataMode: config.policy.dataMode,
          retentionDays: config.retentionDays,
          stats: buffer.stats(),
        });
        return;
      }

      if (request.method === "GET" && (request.url === "/" || request.url === "/index.html")) {
        response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        response.end(loadDashboardHtml());
        return;
      }

      if (request.method === "GET" && request.url?.startsWith("/api/")) {
        const url = new URL(request.url, "http://127.0.0.1");
        const days = Number(url.searchParams.get("days") ?? 30) || 30;
        if (url.pathname === "/api/summary") {
          sendJson(response, dashboardSummary(buffer.database, days));
          return;
        }
        if (url.pathname === "/api/sessions") {
          sendJson(response, dashboardSessions(buffer.database, days));
          return;
        }
        if (url.pathname === "/api/repos") {
          sendJson(response, dashboardRepos(buffer.database, days));
          return;
        }
        if (url.pathname === "/api/session") {
          const id = url.searchParams.get("id");
          const detail = id ? dashboardSessionDetail(buffer.database, id) : null;
          if (!detail) {
            sendJson(response, { error: "session_not_found" }, 404);
            return;
          }
          sendJson(response, detail);
          return;
        }
        sendJson(response, { error: "not_found" }, 404);
        return;
      }

      if (request.method === "POST" && request.url?.startsWith("/hooks/")) {
        const body = decodeRequestBody(request, await readRequestBody(request));
        const payload = JSON.parse(body.text || "{}");
        const normalized = appendForwardedHook(payload, {
          config,
          buffer,
          source: sourceFromPath(request.url),
        });
        response.writeHead(202, { "content-type": "application/json" });
        response.end(
          JSON.stringify({
            accepted: true,
            continue: true,
            eventId: normalized.event.id,
            suppressedFields: normalized.suppressedFields,
          }),
        );
        return;
      }

      if (
        request.method === "POST" &&
        (request.url === "/v1/logs" || request.url === "/v1/traces" || request.url === "/v1/metrics")
      ) {
        const body = decodeRequestBody(request, await readRequestBody(request));
        const source = sourceFromHeaders(request) ?? "unknown";
        let parsedEnvelope: unknown;
        try {
          parsedEnvelope = JSON.parse(body.text || "{}");
        } catch {
          parsedEnvelope = undefined;
        }

        if (parsedEnvelope !== undefined) {
          const exploded = explodeOtlpPayload(parsedEnvelope, {
            policy: config.policy,
            source,
            transportPath: request.url,
          });

          if (exploded.events.length > 0 || exploded.metricSamples.length > 0) {
            buffer.appendMany(exploded.events, exploded.metricSamples);
            response.writeHead(202, { "content-type": "application/json" });
            response.end(
              JSON.stringify({
                accepted: true,
                events: exploded.events.length,
                metricSamples: exploded.metricSamples.length,
                recordCount: exploded.recordCount,
                datapointCount: exploded.datapointCount,
                parseFailures: exploded.parseFailures,
              }),
            );
            return;
          }
        }

        // Unknown or non-JSON OTLP shape: keep a metadata-only transport row, never the body.
        const fallbackPayload = {
          id: `${request.url.slice(1).replace(/\//g, "_")}_${Date.now()}`,
          event_type: "otel_span",
          content_type: request.headers["content-type"] ?? "unknown",
          transport_path: request.url,
          body_bytes: body.bodyBytes,
          body_decoded_bytes: body.decodedBytes,
          body_parse_error:
            parsedEnvelope === undefined
              ? "non_json_or_unsupported_otlp_payload"
              : "unrecognized_otlp_envelope_shape",
          content_encoding: body.contentEncoding,
        };
        const normalized = appendForwardedHook(fallbackPayload, {
          config,
          buffer,
          source,
        });
        response.writeHead(202, { "content-type": "application/json" });
        response.end(
          JSON.stringify({
            accepted: true,
            eventId: normalized.event.id,
            suppressedFields: normalized.suppressedFields,
          }),
        );
        return;
      }

      response.writeHead(404, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "not_found" }));
    } catch (error) {
      console.warn(
        JSON.stringify({
          warning: "collector_request_rejected",
          method: request.method,
          path: request.url,
          message: error instanceof Error ? error.message : String(error),
        }),
      );
      response.writeHead(400, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          error: "collector_error",
          message: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  });
}
