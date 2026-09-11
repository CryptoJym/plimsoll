import { resolveCollectorHome } from "./collector-home";
import { selectsLiveUsage, assertLiveRoute, authenticateLiveProducer, readLiveBody } from "./codex-live-usage-auth";
import { canonicalJson, parseLivePacket, liveSha256, liveReceipt, hasLiveUsageClaim } from "./codex-live-usage-protocol";
import { ingestLiveUsage } from "./codex-live-usage-ledger";
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";

import { LocalEventBuffer } from "./buffer";
import { evidenceAge, projectionValidity, STATUS_MAX_AGE_MS } from "./projection-validity";
import { automaticRepairServiceStatus } from "./maintenance";
import {
  assertCollectorPrivacyMode,
  collectorPrivacyReadiness,
  type CollectorConfig,
} from "./config";
import {
  canonicalizeSuppressionReceipts,
  normalizeGitRemote,
  remoteLinkageHash,
} from "../../shared/src/index";
import { appendForwardedHook } from "./forwarder";
import { explodeOtlpPayload } from "./otlp";
import { saveCollectorConfig } from "./config";
import type { CollectorRuntimeIdentity } from "./runtime-ownership";
import { codexReconciliationStatus } from "./codex-reconciliation";
import { historyCoverageStatus } from "./history-coverage";
import { captureBaselineStatus } from "./capture-baseline";
import {
  accountAssertionStatus,
  defaultAccountAssertionStatus,
  formatAccountAssertionStatusLine,
  formatAccountAssertionStatusRows,
} from "./account-assertion";
import {
  HttpBoundaryRejection,
  LOCAL_HTTP_LIMITS,
  asHttpBoundaryRejection,
  assertBoundedJsonNodes,
  assertAllowedHost,
  assertBoundedOtlpCardinality,
  assertHookSource,
  assertNoBrowserOrigin,
  canonicalOtlpTransportPath,
  createRequestBudget,
  createSourceRateLimiter,
  decodeBoundedRequestBody,
  isOtlpPath,
  parseBoundedJson,
  readBoundedRequestBody,
  requireOtlpSource,
  retryStorageBusy,
  type LocalProducerSource,
} from "./http-boundary";
import {
  assertManagementCredential,
  assertProducerToken,
  type LocalIngestAuth,
} from "./local-auth";
import {
  classifyRejectionClient,
  createRejectionDiagnostics,
  type CollectorServer,
} from "./rejection-diagnostics";

let dashboardHtml: string | undefined;
function loadDashboardHtml() {
  dashboardHtml ??= fs.readFileSync(new URL("./dashboard.html", import.meta.url), "utf8");
  return dashboardHtml;
}

let dashboardHeaders: Record<string, string> | undefined;
function hashInlineDashboardBlock(html: string, tag: "script" | "style") {
  const opening = `<${tag}>`;
  const start = html.indexOf(opening);
  const end = html.indexOf(`</${tag}>`, start + opening.length);
  if (start < 0 || end < 0) throw new Error(`dashboard_${tag}_missing`);
  return crypto
    .createHash("sha256")
    .update(html.slice(start + opening.length, end))
    .digest("base64");
}

function loadDashboardHeaders() {
  if (dashboardHeaders) return dashboardHeaders;
  const html = loadDashboardHtml();
  const scriptHash = hashInlineDashboardBlock(html, "script");
  const styleHash = hashInlineDashboardBlock(html, "style");
  dashboardHeaders = {
    "content-type": "text/html; charset=utf-8",
    "content-security-policy": [
      "default-src 'none'",
      `script-src 'sha256-${scriptHash}'`,
      "script-src-attr 'none'",
      `style-src 'sha256-${styleHash}'`,
      "style-src-attr 'none'",
      "connect-src 'self'",
      "base-uri 'none'",
      "form-action 'none'",
      "frame-ancestors 'none'",
      "object-src 'none'",
    ].join("; "),
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "no-referrer",
    "cross-origin-opener-policy": "same-origin",
    "cross-origin-resource-policy": "same-origin",
    "permissions-policy": "camera=(), geolocation=(), microphone=()",
    "cache-control": "no-store",
  };
  return dashboardHeaders;
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

function firstHeader(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}

function requestUrl(request: http.IncomingMessage) {
  return new URL(request.url ?? "/", "http://127.0.0.1");
}

function hookSourceFromPath(rawUrl: string | undefined): LocalProducerSource | undefined {
  try {
    const pathname = new URL(rawUrl ?? "", "http://127.0.0.1").pathname;
    if (pathname === "/hooks/claude-code") return "claude_code";
    if (pathname === "/hooks/codex") return "codex";
  } catch {
    return undefined;
  }
  return undefined;
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
    /** Path-free identity of the collector home this daemon binds (issue #135). */
    homeIdentityHash?: string;
    maintenanceStatus?: () => unknown;
    /** Sanitized startup cache. HTTP handlers never discover user files. */
    detectedIdentities?: () => Array<Record<string, unknown>>;
    /** Local materialized outcome read model; no provider/network path. */
    outcomePerformance?: (days: number, asOf: string) => Record<string, unknown>;
    /** Registers a refresh callable for startup/child-receipt points only. */
    registerStatusRefresher?: (refresh: (failure?: "maintenance_failed") => boolean) => void;
    /**
     * Injectable clock for rejection-diagnostics windows (proof fixtures).
     * Production defaults to wall-clock time.
     */
    diagnosticsNowMs?: () => number;
    /**
     * Issue 0056 (#104): provisioned Plimsoll-local credentials. When present,
     * ingestion requires the source-bound producer token, management reads
     * require the separate read credential, and only minimal /healthz stays
     * unauthenticated. Absent (isolated proof fixtures), the legacy loopback
     * boundary applies unchanged.
     */
    localAuth?: LocalIngestAuth;
    /** Private hash registry home; no provisioning occurs on the listener. */
    liveProducerHome?: string;
    /** Proof-injectable per-source admission ceiling (defaults to the limit). */
    perSourceRequestLimit?: number;
  } = {},
) {
  assertCollectorPrivacyMode(config, "collector server");

  const localAuth = options.localAuth ?? null;
  const authEnforced = localAuth !== null;
  const sourceRateLimiter = createSourceRateLimiter(
    options.perSourceRequestLimit ?? LOCAL_HTTP_LIMITS.perSourceRequestsPerWindow,
  );
  const assertSourceAdmission = (source: LocalProducerSource) => {
    if (!authEnforced) return;
    sourceRateLimiter.assertAdmissible(source);
  };
  const assertManagementRead = (request: http.IncomingMessage) => {
    if (!authEnforced || !localAuth) return;
    assertManagementCredential(request, localAuth, requestUrl(request));
  };

  // Issue #0075 (#144): repeated identical admission rejections are
  // aggregated. Decisions at the HTTP boundary stay fail-closed and their
  // responses stay byte-for-byte identical; only the terminal/log stream is
  // bounded by reason class plus interval summary.
  const rejectionDiagnostics = createRejectionDiagnostics({
    nowMs: options.diagnosticsNowMs,
  });
  const invalidateStatus = (body: Record<string, unknown>, reason: string) => {
    const projection = (body.projection ?? {}) as Record<string, unknown>;
    body.projection = { ...projection, ...projectionValidity({
      ready: projection.ready === true,
      parityReady: projection.parityReady === true,
      dirty: projection.dirty === true,
      degradedReason: typeof projection.degradedReason === "string" ? projection.degradedReason : null,
      lastSuccessAt: typeof projection.lastSuccessAt === "string" ? projection.lastSuccessAt : null,
    }, Date.now(), reason) };
    for (const field of ["health", "captureHealth"]) {
      const health = (body[field] ?? {}) as Record<string, unknown>;
      body[field] = { ...health, overall: "amber", reason };
    }
  };
  const snapshotResponse = (days: number, refreshControl = false) => {
    const read = buffer.projection.readSnapshot(days, config.subscriptions);
    if (read.kind !== "ready") return read;
    // Keep dashboard transport to its one snapshot request. The performance
    // model is already materialized from immutable facts; this read never
    // fetches GitHub or runs a backfill.
    const performanceAsOf = new Date(
      Date.parse(read.snapshot.window.since) + days * 24 * 60 * 60 * 1000,
    ).toISOString();
    const outcomePerformance = options.outcomePerformance?.(days, performanceAsOf) ?? null;
    (read.snapshot as Record<string, unknown>).outcomePerformance = outcomePerformance;
    const cachedControl = lastCoherentStatus?.body;
    const delivery = refreshControl ? buffer.delivery.status()
      : cachedControl?.delivery as ReturnType<LocalEventBuffer["delivery"]["status"]> | undefined;
    const maintenance = options.maintenanceStatus?.() ?? null;
    const historyCoverage = refreshControl ? historyCoverageStatus(buffer.database)
      : cachedControl?.historyCoverage as ReturnType<typeof historyCoverageStatus> | undefined;
    const status = read.snapshot.status as Record<string, unknown>;
    const stats = (status.stats ?? {}) as Record<string, unknown>;
    stats.unuploadedCount = delivery?.remainingDelivery ?? null;
    const accountAssertions = refreshControl ? accountAssertionStatus(buffer.database) : cachedControl?.accountAssertions ?? null;
    Object.assign(status, {
      ok: true,
      runtimeIdentity: options.runtimeIdentity ?? null,
      homeIdentityHash: options.homeIdentityHash ?? null,
      dataMode: config.policy.dataMode,
      privacyMode: "metadata_only",
      privacy: collectorPrivacyReadiness(config),
      retentionDays: config.retentionDays,
      retention: refreshControl ? buffer.retentionProgressStatus(config.retentionDays) : cachedControl?.retention ?? null,
      enrollment: { futureOnlyEnrollment: true, inspection: "not_inspected", quarantinedHistoryRows: null },
      stats,
      otlpAdmission: {
        counterLifetime: "durable",
        dropped: refreshControl ? buffer.otlpAdmissionCounters() : (cachedControl?.otlpAdmission as {dropped?:unknown})?.dropped ?? null,
      },
      ingestIntegrity: refreshControl ? buffer.eventCollisionSummary() : cachedControl?.ingestIntegrity ?? null,
      delivery,
      reconciliation: refreshControl ? codexReconciliationStatus(buffer.database) : cachedControl?.reconciliation ?? null,
      maintenance,
      captureHealth: status.health ?? null,
      historyCoverage,
      captureBaseline: refreshControl ? captureBaselineStatus(buffer.database) : cachedControl?.captureBaseline ?? null,
      accountAssertions,
      accountAssertionAdapters: refreshControl ? accountAssertions : cachedControl?.accountAssertionAdapters ?? null,
      accountAssertionStatusLine: refreshControl ? formatAccountAssertionStatusLine(buffer.database)
        : cachedControl?.accountAssertionStatusLine ?? null,
      accountLabelCompatibility:
        "label account <sha256:hash> remains a local-only display label; it never changes assertions or history",
    });
    const cacheAge = evidenceAge(lastCoherentStatus?.cachedAt);
    const invalidReason = refreshControl ? null : lastStatusRefreshError ??
      (cacheAge === null || cacheAge > STATUS_MAX_AGE_MS ? "status_cache_expired" : null);
    if (invalidReason) {
      invalidateStatus(status, invalidReason);
      Object.assign(read.snapshot.projection, status.projection);
    }
    const maintenanceDigest = crypto
      .createHash("sha256")
      .update(JSON.stringify(maintenance))
      .digest("hex")
      .slice(0, 16);
    const outcomeGeneration = outcomePerformance && typeof outcomePerformance.generation === "number"
      ? outcomePerformance.generation
      : "none";
    return {
      ...read,
      etagSeed: `${days}-${read.etagSeed}-${invalidReason ?? "valid"}-${outcomeGeneration}-${delivery?.remainingDelivery ?? "unknown"}-${delivery?.receipts.dead ?? "unknown"}-${maintenanceDigest}-${historyCoverage?.sources.map((source) => `${source.completedAt ?? "incomplete"}:${source.latestFullAttempt?.attemptedAt ?? "none"}:${source.latestFullAttempt?.status ?? "none"}`).join(":") ?? "unknown"}`,
    };
  };

  let lastCoherentStatus: {
    body: Record<string, unknown>;
    generation: number | null;
    cachedAt: string;
  } | null = null;
  let lastStatusRefreshError: "database_busy" | "status_refresh_failed" | "maintenance_failed" | null = null;
  let lastGoodAt: string | null = null;
  const statusRefreshCounters = { attempts: 0, failures: 0, expiredResponses: 0 };
  const currentStatus = () => {
    const read = snapshotResponse(30, true);
    if (read.kind === "ready") {
      const body = read.snapshot.status as Record<string, unknown>;
      lastCoherentStatus = {
        body,
        generation: read.snapshot.generation,
        cachedAt: new Date(Date.now()).toISOString(),
      };
      return { body, generation: read.snapshot.generation };
    }
    const accountAssertions = accountAssertionStatus(buffer.database);
    const body: Record<string, unknown> = {
      ok: true,
      runtimeIdentity: options.runtimeIdentity ?? null,
      homeIdentityHash: options.homeIdentityHash ?? null,
      dataMode: config.policy.dataMode,
      privacyMode: "metadata_only",
      privacy: collectorPrivacyReadiness(config),
      retentionDays: config.retentionDays,
      retention: {
        inspection: "not_inspected",
        policy: { retentionDays: config.retentionDays, cutoffAt: null },
        states: {
          retained: null,
          pendingDelivery: null,
          quarantined: null,
          expired: null,
          notInspected: 1,
        },
        lastPass: null,
      },
      enrollment: {
        futureOnlyEnrollment: true,
        inspection: "not_inspected",
        quarantinedHistoryRows: null,
      },
      stats: null,
      delivery: buffer.delivery.status(),
      reconciliation: codexReconciliationStatus(buffer.database),
      maintenance: options.maintenanceStatus?.() ?? null,
      historyCoverage: historyCoverageStatus(buffer.database),
      captureBaseline: captureBaselineStatus(buffer.database),
      accountAssertions,
      accountAssertionAdapters: accountAssertions,
      accountAssertionStatusLine: formatAccountAssertionStatusLine(buffer.database),
      accountLabelCompatibility:
        "label account <sha256:hash> remains a local-only display label; it never changes assertions or history",
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
      captureHealth: {
        generatedAt: new Date().toISOString(),
        overall: "amber",
        sources: [],
        reason: "projection backfill has not published a coherent health snapshot",
      },
    };
    lastCoherentStatus = { body, generation: null, cachedAt: new Date(Date.now()).toISOString() };
    return { body, generation: null };
  };

  // Prime one coherent in-memory response before accepting requests. The
  // parent connection is fail-fast, so a rare startup writer collision falls
  // through to the bounded minimal status below rather than waiting seconds.
  const refreshStatus = (failure?: "maintenance_failed") => {
    statusRefreshCounters.attempts += 1;
    try {
      currentStatus();
      if (lastCoherentStatus) lastCoherentStatus.body.repairService = automaticRepairServiceStatus(buffer.database);
      lastStatusRefreshError = failure ?? null;
      if (failure) statusRefreshCounters.failures += 1;
      else if (lastCoherentStatus &&
          (lastCoherentStatus.body.projection as { parityReady?: boolean })?.parityReady === true) {
        lastGoodAt = lastCoherentStatus.cachedAt;
      }
      return true;
    } catch (error) {
      statusRefreshCounters.failures += 1;
      const code = error && typeof error === "object" && "code" in error
        ? String((error as { code?: unknown }).code ?? "")
        : "";
      lastStatusRefreshError = code === "SQLITE_BUSY" || code === "SQLITE_LOCKED"
        ? "database_busy"
        : "status_refresh_failed";
      return false;
    }
  };
  refreshStatus();
  options.registerStatusRefresher?.(refreshStatus);

  const httpServer = http.createServer(async (request, response) => {
    const budget = createRequestBudget();
    try {
      assertAllowedHost(request);
      if (selectsLiveUsage(request)) {
        const selected = assertLiveRoute(request);
        sourceRateLimiter.assertAdmissible("codex");
        const home = options.liveProducerHome ?? resolveCollectorHome().home;
        const authenticate = () => authenticateLiveProducer(home, buffer, config, selected.producerId, selected.token, localAuth);
        const binding = authenticate();
        const bytes = await readLiveBody(request, budget);
        let packet: ReturnType<typeof parseLivePacket>;
        try { packet = parseLivePacket(bytes); }
        catch { throw new HttpBoundaryRejection("invalid_json", 400); }
        const digest = liveSha256(bytes);
        // Body identity never selects a dedupe scope. Echo failure has no ledger lookup.
        const result = packet.producerId !== binding.binding.producerId || packet.credentialId !== binding.binding.credentialId
          ? liveReceipt(packet, digest, "enrollment_rejected", false, null)
          : ingestLiveUsage(buffer, packet, digest, authenticate);
        response.writeHead(result.disposition === "retryable" ? 503 : result.disposition === "enrollment_rejected" ? 403 : 200,
          { "content-type": "application/json", "cache-control": "no-store" });
        response.end(canonicalJson(result));
        return;
      }

      // Issue 0056 (#104): the only unauthenticated surface. Minimal by
      // construction — no runtime identity, counters, delivery, or ledger
      // state of any kind.
      if (request.method === "GET" && request.url === "/healthz") {
        sendJson(response, { ok: true });
        return;
      }

      if (request.method === "GET" && request.url === "/status") {
          assertManagementRead(request);
          // Cache-only by construction: no SQLite or user-path filesystem call
          // executes on the availability endpoint.
          const cached = lastCoherentStatus;
          const ageMs = evidenceAge(cached?.cachedAt);
          const expired = cached !== null && (ageMs === null || ageMs > STATUS_MAX_AGE_MS);
          const invalidReason = lastStatusRefreshError ?? (expired ? "status_cache_expired" : null);
          if (expired) statusRefreshCounters.expiredResponses += 1;
          const body: Record<string, unknown> = cached
            ? {
                ...cached.body,
                maintenance: options.maintenanceStatus?.() ?? null,
                statusFreshness: {
                  state: expired ? "expired" : lastStatusRefreshError ? "last_coherent" : "coherent",
                  reason: invalidReason,
                  lastGoodAt,
                  maxAgeMs: STATUS_MAX_AGE_MS,
                  cachedAt: cached.cachedAt,
                  ageMs,
                },
              }
            : {
            ok: true,
            runtimeIdentity: options.runtimeIdentity ?? null,
            homeIdentityHash: options.homeIdentityHash ?? null,
            dataMode: config.policy.dataMode,
            privacyMode: "metadata_only",
            privacy: collectorPrivacyReadiness(config),
            retentionDays: config.retentionDays,
            retention: {
              inspection: "not_inspected",
              policy: { retentionDays: config.retentionDays, cutoffAt: null },
              states: {
                retained: null,
                pendingDelivery: null,
                quarantined: null,
                expired: null,
                notInspected: 1,
              },
              lastPass: null,
            },
            enrollment: {
              futureOnlyEnrollment: true,
              inspection: "not_inspected",
              quarantinedHistoryRows: null,
            },
            stats: null,
            delivery: null,
            reconciliation: null,
            maintenance: options.maintenanceStatus?.() ?? null,
            historyCoverage: null,
            captureBaseline: null,
            accountAssertions: defaultAccountAssertionStatus(),
            accountAssertionAdapters: defaultAccountAssertionStatus(),
            accountAssertionStatusLine: formatAccountAssertionStatusRows(defaultAccountAssertionStatus()),
            accountLabelCompatibility:
              "label account <sha256:hash> remains a local-only display label; it never changes assertions or history",
            projection: {
              ready: false,
              degraded: true,
              degradedReason: "database_busy_no_coherent_snapshot",
            },
            health: {
              generatedAt: new Date().toISOString(),
              overall: "amber",
              sources: [],
              reason: "projection backfill has not published a coherent health snapshot",
            },
            captureHealth: {
              generatedAt: new Date().toISOString(),
              overall: "amber",
              sources: [],
              reason: "projection backfill has not published a coherent health snapshot",
            },
            statusFreshness: {
              state: "unavailable",
              reason: lastStatusRefreshError ?? "coherent_snapshot_not_established",
              cachedAt: null,
              lastGoodAt,
              maxAgeMs: STATUS_MAX_AGE_MS,
              ageMs: null,
            },
          };
          // In-memory monotonic admission counters by bounded reason class.
          // No ledger or filesystem work executes on this path.
          if (invalidReason) invalidateStatus(body, invalidReason);
          const captureHealth = body.captureHealth as { sources?: Array<{
            source: string; lastEventAt?: string; activityState?: { lastScanAt?: string };
          }> } | null;
          const projection = body.projection as { parityReady?: boolean; lastSuccessAt?: string } | null;
          body.evidenceWatermarks = {
            capture: { state: !cached ? "unavailable" : invalidReason ? "stale" : "observed",
              observedAt: cached?.cachedAt ?? null,
              sources: (captureHealth?.sources ?? []).map((source) => ({
                source: source.source, coveredThrough: source.lastEventAt ?? null,
                scannedAt: source.activityState?.lastScanAt ?? null,
              })) },
            projection: { state: projection?.parityReady ? "current" : "stale",
              generation: cached?.generation ?? null,
              coveredThrough: projection?.lastSuccessAt ?? null },
            attribution: { state: "unavailable", coveredThrough: null,
              reason: "project_attribution_watermark_unavailable",
              reconciliationThrough: (body.reconciliation as { lastSuccessAt?: string })?.lastSuccessAt ?? null },
            outcomes: { state: "unavailable", coveredThrough: null,
              reason: "outcome_coverage_watermark_unavailable" },
          };
          body.statusRefreshCounters = { ...statusRefreshCounters };
          body.httpAdmission = rejectionDiagnostics.counters();
          sendJson(response, body, 200, cached?.generation === null || cached?.generation === undefined ? {} : {
            "x-plimsoll-projection-generation": String(cached.generation),
          });
        return;
      }

      if (request.method === "GET" && (request.url === "/" || request.url === "/index.html")) {
        response.writeHead(200, loadDashboardHeaders());
        response.end(loadDashboardHtml());
        return;
      }

      if (request.method === "GET" && request.url?.startsWith("/api/")) {
        assertManagementRead(request);
        const url = requestUrl(request);
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
          sendJson(response, {
            accounts,
            accountAliases: buffer.listAccountAliases(),
            priorityRepos: buffer.listPriorityRepos(),
            subscriptions: config.subscriptions,
            detectedIdentities: options.detectedIdentities?.() ?? [],
            // Issue 0056 (#104): live ingest-integrity counts on the
            // credential-gated management surface. /status stays cache-only.
            ingestIntegrity: lastCoherentStatus?.body.ingestIntegrity ?? null,
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
        assertManagementRead(request);
        const body = decodeBoundedRequestBody(
          request,
          await readBoundedRequestBody(request, budget),
        );
        let parsed: Record<string, unknown>;
        try {
          parsed = parseBoundedJson(body.text) as Record<string, unknown>;
        } catch (error) {
          if (error instanceof HttpBoundaryRejection) throw error;
          throw new HttpBoundaryRejection("invalid_json", 400);
        }
        assertBoundedJsonNodes(parsed);
        budget.checkpoint();

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
        assertNoBrowserOrigin(request);
        const source = hookSourceFromPath(request.url);
        if (!source) throw new HttpBoundaryRejection("source_not_allowed", 401);
        assertHookSource(request, source);
        assertSourceAdmission(source);
        if (localAuth) {
          assertProducerToken(request, localAuth, source, requestUrl(request));
        }
        const body = decodeBoundedRequestBody(
          request,
          await readBoundedRequestBody(request, budget),
        );
        const payload = parseBoundedJson(body.text);
        assertBoundedJsonNodes(payload);
        if (hasLiveUsageClaim(payload)) throw new HttpBoundaryRejection("source_not_allowed", 403);
        budget.checkpoint();
        const normalized = await retryStorageBusy(budget, () =>
          appendForwardedHook(payload, {
            config,
            buffer,
            source,
          })
        );
        rejectionDiagnostics.recordAccepted(source);
        response.writeHead(202, { "content-type": "application/json" });
        response.end(
          JSON.stringify({
            accepted: true,
            continue: true,
            eventId: normalized.event.id,
            suppressedFields: normalized.suppressedFields,
            ...(normalized.deduplicated ? { deduplicated: true } : {}),
            ...(normalized.collisionQuarantined
              ? { collisionQuarantined: true }
              : {}),
          }),
        );
        return;
      }

      if (
        request.method === "POST" &&
        isOtlpPath(request.url)
      ) {
        assertNoBrowserOrigin(request);
        const source = requireOtlpSource(request);
        assertSourceAdmission(source);
        if (localAuth) {
          assertProducerToken(request, localAuth, source, requestUrl(request));
        }
        const body = decodeBoundedRequestBody(
          request,
          await readBoundedRequestBody(request, budget),
        );
        const parsedEnvelope = parseBoundedJson(body.text);
        assertBoundedOtlpCardinality(parsedEnvelope);
        if (hasLiveUsageClaim(parsedEnvelope)) throw new HttpBoundaryRejection("source_not_allowed", 403);

        const repoLabels: Array<{ hash: string; label: string }> = [];
        const exploded = explodeOtlpPayload(parsedEnvelope, {
          policy: config.policy,
          source,
          transportPath: canonicalOtlpTransportPath(request.url),
          onRepoLabel: (hash, label) => repoLabels.push({ hash, label }),
        });

        if (
          exploded.events.length > 0 ||
          exploded.metricSamples.length > 0 ||
          exploded.droppedEventCount > 0
        ) {
          budget.checkpoint();
          for (const { hash, label } of repoLabels) {
            await retryStorageBusy(budget, () => buffer.recordRepoLabel(hash, label));
          }
          // Admission is durable in small transactions. Yield between chunks
          // so availability reads and other producers receive a turn. A retry
          // after any partial commit retains the existing deterministic IDs.
          const integrity = { deduplicatedCount: 0, collisionQuarantinedCount: 0 };
          const projectionDeadlineMs = performance.now() + 25;
          const chunks = Math.max(Math.ceil(exploded.events.length / 16),
            Math.ceil(exploded.metricSamples.length / 16), 1);
          for (let chunk = 0; chunk < chunks; chunk += 1) {
            if (chunk > 0) await new Promise<void>(resolve => setImmediate(resolve));
            budget.checkpoint();
            const result = await retryStorageBusy(
              budget,
              () => buffer.appendMany(
                exploded.events.slice(chunk * 16, (chunk + 1) * 16),
                exploded.metricSamples.slice(chunk * 16, (chunk + 1) * 16),
                chunk === 0 ? exploded.admissionDrops : [],
                { projectionDeadlineMs },
              ),
            );
            integrity.deduplicatedCount += result.deduplicatedCount;
            integrity.collisionQuarantinedCount += result.collisionQuarantinedCount;
          }
          rejectionDiagnostics.recordAccepted(source);
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
              ...(integrity.deduplicatedCount
                ? { deduplicated: integrity.deduplicatedCount }
                : {}),
              ...(integrity.collisionQuarantinedCount
                ? { collisionQuarantined: integrity.collisionQuarantinedCount }
                : {}),
              suppressedFields: canonicalizeSuppressionReceipts([
                ...exploded.events.flatMap((entry) => entry.suppressedFields),
                ...exploded.metricSamples.flatMap((sample) => sample.suppressedFields),
              ]),
            }),
          );
          return;
        }

        // Unknown JSON OTLP shape: keep a metadata-only transport row, never the body.
        const fallbackPayload = {
          event_type: "otel_span",
          content_type: request.headers["content-type"] ?? "unknown",
          body_bytes: body.bodyBytes,
          body_decoded_bytes: body.decodedBytes,
          body_parse_error: "unrecognized_otlp_envelope_shape",
          content_encoding: body.contentEncoding,
        };
        budget.checkpoint();
        const normalized = await retryStorageBusy(budget, () =>
          appendForwardedHook(fallbackPayload, {
            config,
            buffer,
            source,
            transportPath: canonicalOtlpTransportPath(request.url),
          })
        );
        rejectionDiagnostics.recordAccepted(source);
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
      const failure = asHttpBoundaryRejection(error);
      const clientClass = classifyRejectionClient(request);
      const rejection = {
        error: "collector_request_rejected",
        reason: failure.reason,
      };
      const diagnosticRejection = { ...rejection, clientClass };
      // Aggregate identical rejections: emit the first occurrence of a
      // bounded reason promptly plus any window summaries it just closed.
      // The HTTP response below stays byte-for-byte unchanged.
      const observed = rejectionDiagnostics.observeRejection(failure.reason, clientClass);
      for (const line of observed.summaries) console.warn(JSON.stringify(line));
      if (observed.first) console.warn(JSON.stringify(diagnosticRejection));
      if (!response.headersSent) {
        response.writeHead(failure.status, {
          connection: "close",
          "content-type": "application/json",
        });
        response.end(JSON.stringify(rejection));
      } else {
        response.destroy();
      }
    }
  });

  // Issue #0015: bounded projection work can block this loop for well over
  // Node's default 5s keepAliveTimeout. The expiry timer then fires on
  // unblock and closes the idle socket at the same instant the next
  // dashboard poll or hook POST arrives on it — the client sees ECONNRESET
  // instead of a served response (proof gate
  // raw_history_growth_does_not_change_snapshot_work_shape failed exactly
  // this way). Serve every request on its own connection: loopback setup
  // cost is negligible, and a connection that carries one request cannot be
  // reaped mid-request.
  httpServer.keepAliveTimeout = 0;
  const server = httpServer as CollectorServer;
  server.plimsollHttpDiagnostics = {
    flush: () => rejectionDiagnostics.flush(),
    counters: () => rejectionDiagnostics.counters(),
  };
  return server;
}
