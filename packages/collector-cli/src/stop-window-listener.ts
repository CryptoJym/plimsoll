import http from "node:http";

import type { CollectorConfig } from "./config";
import { normalizeForwardedHook } from "./forwarder";
import {
  HttpBoundaryRejection,
  asHttpBoundaryRejection,
  assertAllowedHost,
  assertBoundedJsonNodes,
  assertBoundedOtlpCardinality,
  assertHookSource,
  assertNoBrowserOrigin,
  canonicalOtlpTransportPath,
  createRequestBudget,
  createSourceRateLimiter,
  decodeBoundedRequestBody,
  hookSourceFromPath,
  isOtlpPath,
  parseBoundedJson,
  readBoundedRequestBody,
  requireOtlpSource,
} from "./http-boundary";
import { blankForbiddenRawContent, hookSpoolEnabled, isHookSpoolSource, writeHookSpoolEnvelope } from "./hook-spool";
import { assertManagementCredential, assertProducerToken, readLiveProducerAuth } from "./local-auth";
import { hasLiveUsageClaim } from "./codex-live-usage-protocol";
import { conflictingOtlpServiceSource, explodeOtlpPayload } from "./otlp";
import { OtlpIntakeSpool } from "./otlp-spool";
import { readProducerEventIdHeader, PRODUCER_EVENT_ID_HEADER } from "./producer-parity";
import { markStopWindowProbe, STOP_WINDOW_PROBE_HEADER } from "./stop-window-probe";

export const STOP_WINDOW_RELEASE_PATH = "/api/stop-window/release";

function reply(response: http.ServerResponse, status: number, body: Record<string, unknown>) {
  response.writeHead(status, { "content-type": "application/json", "connection": "close" });
  response.end(JSON.stringify(body));
}

/** The update child never opens a ledger. A 202 means a private spool file is durable. */
export async function runStopWindowListener(config: CollectorConfig, home: string) {
  if (!hookSpoolEnabled() || !new OtlpIntakeSpool({ home }).enabled) {
    throw new Error("stop_window_spool_disabled");
  }
  const managementAuth = readLiveProducerAuth(home);
  if (!managementAuth) throw new Error("stop_window_auth_unavailable");
  const limiter = createSourceRateLimiter();
  const otlpSpool = new OtlpIntakeSpool({ home });
  let releasing = false;
  let inFlight = 0;
  const server = http.createServer(async (request, response) => {
    inFlight += 1;
    try {
      assertAllowedHost(request);
      assertNoBrowserOrigin(request);
      const url = new URL(request.url ?? "/", "http://127.0.0.1");
      if (request.method === "GET" && url.pathname === "/healthz") {
        reply(response, 200, { ok: true, mode: "stop_window" });
        return;
      }
      if (request.method === "POST" && url.pathname === STOP_WINDOW_RELEASE_PATH) {
        assertManagementCredential(request, managementAuth, url);
        releasing = true;
        reply(response, 200, { status: "releasing" });
        // Closing after this response lets already admitted writes finish.
        setImmediate(() => server.close());
        return;
      }
      if (releasing) throw new HttpBoundaryRejection("internal_rejection", 503);
      const source = request.method === "POST" && request.url?.startsWith("/hooks/")
        ? hookSourceFromPath(request.url)
        : request.method === "POST" && isOtlpPath(request.url)
          ? requireOtlpSource(request)
          : undefined;
      if (!source) throw new HttpBoundaryRejection("source_not_allowed", 404);
      if (request.url?.startsWith("/hooks/")) assertHookSource(request, source);
      limiter.assertAdmissible(source);
      const auth = readLiveProducerAuth(home);
      if (!auth) throw new HttpBoundaryRejection("producer_token_invalid", 401);
      assertProducerToken(request, auth, source, url);
      const budget = createRequestBudget();
      const body = decodeBoundedRequestBody(request, await readBoundedRequestBody(request, budget));
      const payload = parseBoundedJson(body.text);
      const probe = request.headers[STOP_WINDOW_PROBE_HEADER] === "1";
      if (request.url?.startsWith("/hooks/")) {
        if (!isHookSpoolSource(source)) throw new HttpBoundaryRejection("source_not_allowed", 401);
        assertBoundedJsonNodes(payload);
        if (hasLiveUsageClaim(payload)) throw new HttpBoundaryRejection("source_not_allowed", 403);
        if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
          throw new HttpBoundaryRejection("internal_rejection", 400);
        }
        const candidate = payload;
        // The daemon's normalizer is the contract for accepted hook shapes.
        normalizeForwardedHook(candidate, {
          config, source,
          producerEventId: readProducerEventIdHeader(request.headers[PRODUCER_EVENT_ID_HEADER]),
        });
        const sanitized = blankForbiddenRawContent(JSON.stringify(candidate));
        if (!sanitized) throw new HttpBoundaryRejection("internal_rejection", 400);
        const write = writeHookSpoolEnvelope({ home, source, body: sanitized.text, blanked: sanitized.blanked, probe });
        if (!write.ok) throw new HttpBoundaryRejection("storage_busy_retry", 503);
        reply(response, 202, { status: "hook_spooled", source });
        return;
      }
      assertBoundedOtlpCardinality(payload, body.decodedBytes);
      if (hasLiveUsageClaim(payload)) throw new HttpBoundaryRejection("source_not_allowed", 403);
      if (conflictingOtlpServiceSource(payload, source)) {
        throw new HttpBoundaryRejection("source_mismatch", 401);
      }
      const transportPath = canonicalOtlpTransportPath(request.url);
      const exploded = explodeOtlpPayload(payload, { policy: config.policy, source, transportPath });
      const result = await otlpSpool.write({
        receivedAtMs: Date.now(), source, transportPath, cause: "update_window", committedChunks: 0,
        batch: {
          events: probe ? exploded.events.map((entry) => ({ ...entry, event: markStopWindowProbe(entry.event) })) : exploded.events,
          metricSamples: probe ? exploded.metricSamples.map((sample) => ({
            ...sample, model: undefined, value: 0, attrs: { stopWindowProbe: true },
          })) : exploded.metricSamples,
          admissionDrops: exploded.admissionDrops,
        },
      });
      if (!result.ok) throw new HttpBoundaryRejection("storage_busy_retry", 503);
      reply(response, 202, {
        status: "otlp_spooled", source, events: exploded.events.length,
        metricSamples: exploded.metricSamples.length,
      });
    } catch (error) {
      const rejection = asHttpBoundaryRejection(error);
      reply(response, rejection.status, { error: rejection.reason });
    } finally {
      inFlight -= 1;
      if (releasing && inFlight === 0) server.close();
    }
  });
  server.keepAliveTimeout = 1000;
  const deadline = Date.now() + 90_000;
  for (;;) {
    try {
      await new Promise<void>((resolve, reject) => {
        const onError = (error: Error) => { server.off("listening", onListening); reject(error); };
        const onListening = () => { server.off("error", onError); resolve(); };
        server.once("error", onError);
        server.once("listening", onListening);
        server.listen(config.port, "127.0.0.1");
      });
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EADDRINUSE" || Date.now() >= deadline) throw error;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  process.send?.({ status: "ready", port: config.port });
  await new Promise<void>((resolve) => server.once("close", resolve));
}

/**
 * Run a lifecycle update that started the listener, and release the listener
 * unless the update completed. Only a completed update hands the port to the
 * new runtime, whose load-launch-agent releases the listener. After a thrown,
 * refused or rolled-back update the restored runtime's loader does not know the
 * listener, and its daemon could not bind the port.
 */
export async function withStopWindowRelease<T extends { receipt?: { status?: unknown } }>(
  started: boolean, release: () => Promise<unknown>, run: () => Promise<T>,
): Promise<{ result: T; releaseError: Error | null }> {
  let result: T;
  try {
    result = await run();
  } catch (error) {
    if (started) await release().catch(() => undefined);
    throw error;
  }
  if (!started || result.receipt?.status === "completed") return { result, releaseError: null };
  try {
    await release();
    return { result, releaseError: null };
  } catch (error) {
    return { result, releaseError: error instanceof Error ? error : new Error(String(error)) };
  }
}

/** Only the update listener answers this route; a normal daemon yields 404. */
export async function releaseStopWindowListener(port: number, home: string) {
  const auth = readLiveProducerAuth(home);
  if (!auth) return false;
  const status = await new Promise<number | null>((resolve, reject) => {
    const request = http.request({
      hostname: "127.0.0.1", port, path: STOP_WINDOW_RELEASE_PATH, method: "POST",
      headers: { "x-plimsoll-token": auth.managementRead }, timeout: 3000,
    }, (response) => { response.resume(); response.on("end", () => resolve(response.statusCode ?? 0)); });
    request.on("timeout", () => request.destroy(new Error("stop_window_release_timeout")));
    request.on("error", (error: NodeJS.ErrnoException) => {
      if (error.code === "ECONNREFUSED") resolve(null);
      else reject(error);
    });
    request.end();
  });
  if (status === null || status === 404) return false;
  if (status !== 200) throw new Error(`stop_window_release_refused:${status}`);
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const open = await new Promise<boolean>((resolve) => {
      const request = http.get({ hostname: "127.0.0.1", port, path: "/healthz", timeout: 500 },
        (response) => { response.resume(); resolve(true); });
      request.on("error", (error: NodeJS.ErrnoException) => resolve(error.code !== "ECONNREFUSED"));
    });
    if (!open) return true;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("stop_window_release_port_still_bound");
}
