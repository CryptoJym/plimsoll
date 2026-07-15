import fs from "node:fs";
import http from "node:http";
import zlib from "node:zlib";

import { LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import {
  canonicalizeSuppressionReceipts,
  normalizeGitRemote,
  remoteLinkageHash,
  type ToolSource,
} from "../../shared/src/index";
import { appendForwardedHook } from "./forwarder";
import { explodeOtlpPayload } from "./otlp";
import { saveCollectorConfig } from "./config";
import { readLocalIdentities } from "./local-identity";
import type { CollectorRuntimeIdentity } from "./runtime-ownership";
import { codexReconciliationStatus } from "./codex-reconciliation";

let dashboardHtml: string | undefined;
function loadDashboardHtml() {
  dashboardHtml ??= fs.readFileSync(new URL("./dashboard.html", import.meta.url), "utf8");
  return dashboardHtml;
}

function sendJson(
  response: http.ServerResponse,
  body: unknown,
  status = 200,
  headers: Record<string, string> = {},
) {
  response.writeHead(status, { "content-type": "application/json", ...headers });
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

const REQUEST_METHODS = new Set(["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]);

function sanitizedRequestMethod(method: string | undefined) {
  const candidate = method?.toUpperCase() ?? "";
  return REQUEST_METHODS.has(candidate) ? candidate : "OTHER";
}

/**
 * Request failures are observability metadata, not an echo surface. Keep the
 * route useful while ensuring an arbitrary URL/query can never enter logs or
 * responses.
 */
function sanitizedRequestPath(rawUrl: string | undefined) {
  let pathname: string;
  try {
    pathname = new URL(rawUrl ?? "", "http://127.0.0.1").pathname;
  } catch {
    return "/invalid";
  }
  if (pathname.startsWith("/hooks/")) return "/hooks/:source";
  if (pathname.startsWith("/api/settings/")) return "/api/settings/:action";
  if (pathname.startsWith("/api/")) return "/api/:route";
  if (pathname === "/v1/logs" || pathname === "/v1/traces" || pathname === "/v1/metrics") {
    return pathname;
  }
  if (pathname === "/" || pathname === "/index.html" || pathname === "/status") {
    return pathname;
  }
  return "/other";
}

function allowlistedErrorClass(error: unknown) {
  if (error instanceof SyntaxError) return "SyntaxError";
  if (error instanceof TypeError) return "TypeError";
  if (error instanceof RangeError) return "RangeError";
  if (error instanceof Error) return "Error";
  return "UnknownError";
}

/**
 * Cross-origin defense for localhost write endpoints: browsers attach an
 * Origin header to cross-site requests and cannot remove it. We accept writes
 * only when the request provably came from our own dashboard (same-origin
 * Origin) or a non-browser client (no Origin), and require a custom header
 * that cross-origin pages cannot set without a CORS preflight we never grant.
 */
function isTrustedLocalWrite(request: http.IncomingMessage) {
  const origin = firstHeader(request.headers.origin);
  const originOk =
    !origin || /^https?:\/\/(127\.0\.0\.1|localhost)(:\d+)?$/.test(origin);
  return originOk && firstHeader(request.headers["x-plimsoll-local"]) === "1";
}

export function createCollectorServer(
  config: CollectorConfig,
  buffer: LocalEventBuffer,
  options: {
    runtimeIdentity?: CollectorRuntimeIdentity;
    maintenanceStatus?: () => unknown;
  } = {},
) {
  const snapshotResponse = (days: number) => {
    const read = buffer.projection.readSnapshot(days, config.subscriptions);
    if (read.kind !== "ready") return read;
    const delivery = buffer.delivery.status();
    const maintenance = options.maintenanceStatus?.() ?? null;
    const status = read.snapshot.status as Record<string, unknown>;
    const stats = (status.stats ?? {}) as Record<string, unknown>;
    stats.unuploadedCount = delivery.remainingDelivery;
    Object.assign(status, {
      ok: true,
      runtimeIdentity: options.runtimeIdentity ?? null,
      dataMode: config.policy.dataMode,
      retentionDays: config.retentionDays,
      stats,
      otlpAdmission: {
        counterLifetime: "durable",
        dropped: buffer.otlpAdmissionCounters(),
      },
      delivery,
      reconciliation: codexReconciliationStatus(buffer.database),
      maintenance,
    });
    const maintenanceRun =
      maintenance && typeof maintenance === "object" && "runCount" in maintenance
        ? Number((maintenance as { runCount?: number }).runCount ?? 0)
        : 0;
    return {
      ...read,
      etagSeed: `${days}-${read.etagSeed}-${delivery.remainingDelivery}-${delivery.receipts.dead}-${maintenanceRun}`,
    };
  };

  return http.createServer(async (request, response) => {
    try {
      if (request.method === "GET" && request.url === "/status") {
        const read = snapshotResponse(30);
        if (read.kind === "ready") {
          sendJson(response, read.snapshot.status, 200, {
            "x-plimsoll-projection-generation": String(read.snapshot.generation),
          });
        } else {
          sendJson(response, {
            ok: true,
            runtimeIdentity: options.runtimeIdentity ?? null,
            dataMode: config.policy.dataMode,
            retentionDays: config.retentionDays,
            stats: null,
            delivery: buffer.delivery.status(),
            reconciliation: codexReconciliationStatus(buffer.database),
            maintenance: options.maintenanceStatus?.() ?? null,
            projection: read.kind === "backfilling" ? read.status : {
              ready: false,
              degraded: true,
              degradedReason: "unsupported_projection_window",
            },
            health: {
              generatedAt: new Date().toISOString(),
              overall: "amber",
              sources: [],
              reason: "projection backfill has not published a coherent health snapshot",
            },
          });
        }
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
        if (url.pathname === "/api/settings") {
          const accounts = buffer.database
            .prepare(
              `select account_hash as accountHash, label, email, auto_seeded as autoSeeded from account_labels order by first_seen`,
            )
            .all();
          // Detected local identities (emails/plans from each tool's own
          // config). Served to the loopback page only — nothing leaves the
          // machine from here; attachment to an account row is the human's call.
          let detectedIdentities: Array<Record<string, unknown>> = [];
          try {
            detectedIdentities = readLocalIdentities().map((entry) => ({
              source: entry.source,
              email: entry.email ?? null,
              planType: entry.planType ?? null,
              actorHash: entry.actorHash ?? null,
            }));
          } catch {
            detectedIdentities = [];
          }
          sendJson(response, {
            accounts,
            accountAliases: buffer.listAccountAliases(),
            priorityRepos: buffer.listPriorityRepos(),
            subscriptions: config.subscriptions,
            detectedIdentities,
          });
          return;
        }
        if (url.pathname === "/api/snapshot") {
          const read = snapshotResponse(days);
          if (read.kind === "unsupported") {
            sendJson(response, { error: "unsupported_projection_window", supportedDays: read.supportedDays }, 400);
            return;
          }
          if (read.kind === "backfilling") {
            sendJson(response, { error: "projection_backfilling", projection: read.status }, 202);
            return;
          }
          const etag = `W/\"plimsoll-${read.etagSeed}\"`;
          if (request.headers["if-none-match"] === etag) {
            response.writeHead(304, { etag });
            response.end();
            return;
          }
          sendJson(response, read.snapshot, 200, {
            etag,
            "cache-control": "private, no-cache",
            "x-plimsoll-projection-generation": String(read.snapshot.generation),
          });
          return;
        }
        const compatible = snapshotResponse(days);
        if (compatible.kind === "unsupported") {
          sendJson(response, { error: "unsupported_projection_window", supportedDays: compatible.supportedDays }, 400);
          return;
        }
        if (compatible.kind === "backfilling") {
          sendJson(response, { error: "projection_backfilling", projection: compatible.status }, 202);
          return;
        }
        const generationHeader = {
          "x-plimsoll-projection-generation": String(compatible.snapshot.generation),
        };
        if (url.pathname === "/api/summary") {
          sendJson(response, compatible.snapshot.summary, 200, generationHeader);
          return;
        }
        if (url.pathname === "/api/sessions") {
          sendJson(response, compatible.snapshot.sessions, 200, generationHeader);
          return;
        }
        if (url.pathname === "/api/repos") {
          sendJson(response, compatible.snapshot.repos, 200, generationHeader);
          return;
        }
        if (url.pathname === "/api/accounts") {
          sendJson(response, compatible.snapshot.accounts, 200, generationHeader);
          return;
        }
        if (url.pathname === "/api/repo") {
          const hash = url.searchParams.get("hash");
          const detail = hash ? buffer.projection.repoDetail(hash, days) : null;
          if (!detail) {
            sendJson(response, { error: "repo_not_found" }, 404);
            return;
          }
          sendJson(response, detail);
          return;
        }
        if (url.pathname === "/api/session") {
          const id = url.searchParams.get("id");
          const detail = id ? buffer.projection.sessionDetail(id) : null;
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

      if (request.method === "POST" && request.url?.startsWith("/api/settings/")) {
        if (!isTrustedLocalWrite(request)) {
          sendJson(response, { error: "untrusted_write_origin" }, 403);
          return;
        }
        const body = decodeRequestBody(request, await readRequestBody(request));
        let parsed: Record<string, unknown>;
        try {
          parsed = JSON.parse(body.text || "{}") as Record<string, unknown>;
        } catch {
          sendJson(response, { error: "invalid_json" }, 400);
          return;
        }

        if (request.url === "/api/settings/account-label") {
          const accountHash = typeof parsed.accountHash === "string" ? parsed.accountHash : "";
          const label = typeof parsed.label === "string" ? parsed.label.trim().slice(0, 80) : "";
          if (!accountHash.startsWith("sha256:") || !label) {
            sendJson(response, { error: "expected accountHash (sha256:...) and label" }, 400);
            return;
          }
          buffer.setAccountLabel(accountHash, label);
          sendJson(response, { ok: true, accountHash, label });
          return;
        }

        if (request.url === "/api/settings/account-email") {
          const accountHash = typeof parsed.accountHash === "string" ? parsed.accountHash : "";
          const email = typeof parsed.email === "string" ? parsed.email.trim() : "";
          if (!accountHash.startsWith("sha256:")) {
            sendJson(response, { error: "expected accountHash (sha256:...)" }, 400);
            return;
          }
          if (email && (!email.includes("@") || email.length > 120)) {
            sendJson(response, { error: "that does not look like an email" }, 400);
            return;
          }
          buffer.setAccountEmail(accountHash, email);
          sendJson(response, { ok: true, accountHash, email: email || null });
          return;
        }

        if (request.url === "/api/settings/account-merge") {
          const aliasHash = typeof parsed.aliasHash === "string" ? parsed.aliasHash : "";
          if (!aliasHash.startsWith("sha256:")) {
            sendJson(response, { error: "expected aliasHash (sha256:...)" }, 400);
            return;
          }
          if (parsed.action === "remove") {
            buffer.removeAccountAlias(aliasHash);
            sendJson(response, { ok: true, removed: aliasHash, aliases: buffer.listAccountAliases() });
            return;
          }
          const canonicalHash = typeof parsed.canonicalHash === "string" ? parsed.canonicalHash : "";
          if (!canonicalHash.startsWith("sha256:")) {
            sendJson(response, { error: "expected canonicalHash (sha256:...)" }, 400);
            return;
          }
          try {
            buffer.setAccountAlias(aliasHash, canonicalHash);
          } catch {
            sendJson(response, { error: "invalid_account_alias" }, 400);
            return;
          }
          sendJson(response, { ok: true, aliases: buffer.listAccountAliases() });
          return;
        }

        if (request.url === "/api/settings/priority") {
          const action = parsed.action === "remove" ? "remove" : "add";
          const urlValue = typeof parsed.url === "string" ? parsed.url : "";
          const repoHash = remoteLinkageHash(urlValue);
          if (!repoHash) {
            sendJson(response, { error: "could not parse a git repo from that URL" }, 400);
            return;
          }
          if (action === "add") {
            buffer.setPriorityRepo(repoHash, normalizeGitRemote(urlValue) ?? urlValue);
          } else {
            buffer.removePriorityRepo(repoHash);
          }
          sendJson(response, { ok: true, action, repoHash, repos: buffer.listPriorityRepos() });
          return;
        }

        if (request.url === "/api/settings/subscriptions") {
          if (!Array.isArray(parsed.subscriptions)) {
            sendJson(response, { error: "expected subscriptions array" }, 400);
            return;
          }
          try {
            const updated = saveCollectorConfig({
              ...config,
              subscriptions: parsed.subscriptions as CollectorConfig["subscriptions"],
            });
            config.subscriptions = updated.subscriptions;
            buffer.projection.invalidatePresentation();
            sendJson(response, { ok: true, subscriptions: updated.subscriptions });
          } catch {
            sendJson(response, { error: "invalid_subscriptions" }, 400);
          }
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
            onRepoLabel: (hash, label) => buffer.recordRepoLabel(hash, label),
          });

          if (
            exploded.events.length > 0 ||
            exploded.metricSamples.length > 0 ||
            exploded.droppedEventCount > 0
          ) {
            buffer.appendMany(
              exploded.events,
              exploded.metricSamples,
              exploded.admissionDrops,
            );
            response.writeHead(202, { "content-type": "application/json" });
            response.end(
              JSON.stringify({
                accepted: true,
                events: exploded.events.length,
                metricSamples: exploded.metricSamples.length,
                recordCount: exploded.recordCount,
                datapointCount: exploded.datapointCount,
                parseFailures: exploded.parseFailures,
                droppedEvents: exploded.droppedEventCount,
                droppedByReason: exploded.admissionDrops,
                suppressedFields: canonicalizeSuppressionReceipts([
                  ...exploded.events.flatMap((entry) => entry.suppressedFields),
                  ...exploded.metricSamples.flatMap((sample) => sample.suppressedFields),
                ]),
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
      const rejection = {
        error: "collector_request_rejected",
        errorClass: allowlistedErrorClass(error),
        method: sanitizedRequestMethod(request.method),
        path: sanitizedRequestPath(request.url),
      };
      console.warn(JSON.stringify(rejection));
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify(rejection));
    }
  });
}
