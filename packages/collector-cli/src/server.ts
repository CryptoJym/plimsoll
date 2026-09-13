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
  ANALYTICAL_METADATA_LIMITS,
  canonicalizeSuppressionReceipts,
  normalizeGitRemote,
  remoteLinkageHash,
  validatedMetadataAttribute,
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
  hookSourceFromPath,
  isOtlpPath,
  parseBoundedJson,
  readBoundedRequestBody,
  requireOtlpSource,
  retryStorageBusy,
  type LocalProducerSource,
  type RequestBudget,
} from "./http-boundary";
import { HOOK_AUTHORITY_CONTRACT } from "./hook-authority";
// The drain reuses the normalizer's own readers rather than re-implementing
// them, so the two cannot drift on what counts as a usable time (review r3, N2).
import { otelScalar, unixNanoToIso } from "./normalizer";
import {
  HOOK_SPOOL_LIMITS,
  blankForbiddenRawContent,
  hookSpoolDirectory,
  hookSpoolEnabled,
  hookSpoolEntryTrusted,
  hookSpoolPending,
  isHookSpoolSource,
  listHookSpoolFiles,
  pruneHookSpoolRejected,
  readHookSpoolCounters,
  readHookSpoolFile,
  reapHookSpoolTemporaries,
  recordHookSpoolIntake,
  rejectHookSpoolFile,
  resolveHookSpoolHome,
  writeHookSpoolCounters,
  writeHookSpoolFile,
  type HookSpoolBounds,
  type HookSpoolSource,
  type HookSpoolStatus,
} from "./hook-spool";
import {
  assertManagementCredential,
  assertProducerToken,
  localIngestAuthStamp,
  readLiveProducerAuth,
  type LocalIngestAuth,
} from "./local-auth";
import {
  REJECTION_SUMMARY_INTERVAL_MS,
  classifyRejectionClient,
  createRejectionDiagnostics,
  type CollectorServer,
  type RejectionClientClass,
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

/**
 * Everything the `/hooks/<source>` route does after producer-token
 * authentication: parse, bound, refuse a live-usage claim, and durably append
 * with the storage-busy retry. The spool drain (bead eco-6hoxj.61) replays a
 * recovered body through this exact callable, so a recovered row is normalized,
 * validated, deduplicated and attributed identically to a live one — there is
 * no second ingest path to keep in step.
 */
function admitHookBody(
  bodyText: string,
  source: LocalProducerSource,
  context: { config: CollectorConfig; buffer: LocalEventBuffer; budget: RequestBudget },
) {
  const payload = parseBoundedJson(bodyText);
  assertBoundedJsonNodes(payload);
  if (hasLiveUsageClaim(payload)) throw new HttpBoundaryRejection("source_not_allowed", 403);
  context.budget.checkpoint();
  return retryStorageBusy(context.budget, () =>
    appendForwardedHook(payload, {
      config: context.config,
      buffer: context.buffer,
      source,
    }),
  );
}

/**
 * The time aliases `normalizeHookPayload` honours, taken from the contract
 * itself so this cannot drift from it: `observedAt`, `observed_at`,
 * `event.timestamp`, `timestamp`, `time`
 * (`hook-authority.ts`, `HOOK_AUTHORITY_CONTRACT.observedAt`).
 */
const OBSERVED_AT_ALIASES = new Set<string>(HOOK_AUTHORITY_CONTRACT.observedAt.aliases);

/**
 * The OTLP time keys `normalizer.ts` reads as its second-choice timestamp
 * (`collectOtelSignals`), before it gives up and stamps the clock.
 */
const OTEL_TIME_KEYS = ["timeUnixNano", "observedTimeUnixNano", "startTimeUnixNano"] as const;

/**
 * True when the normalizer would actually SELECT this value as the event's
 * time. Its own acceptance test, not a weaker one: `validatedMetadataAttribute`
 * admitting the value as a STRING for that alias, which is literally what the
 * `observedAt` selection in `normalizeHookPayload` calls. A numeric epoch, an
 * empty string, `null`, a boolean, an object and an unparseable or future-dated
 * string are all rejected there, so none of them counts here either.
 */
function usableObservedAtValue(key: string, value: unknown) {
  const validated = validatedMetadataAttribute(key, value);
  return validated.accepted && typeof validated.value === "string";
}

/**
 * True when a unix-nano value survives the filter `collectOtelSignals` puts it
 * through before it can reach `otelSignals.timestamps[0]`: it parses
 * (`unixNanoToIso`, imported from the normalizer rather than re-implemented)
 * and it is not from the future. `timestampIsNotFromTheFuture` is
 * module-private in `normalizer.ts` and this file may not edit that module, so
 * its one condition is rebuilt from the same shared limit it reads.
 */
function usableOtelTime(value: unknown) {
  if (typeof value !== "string" && typeof value !== "number") return false;
  const timestamp = unixNanoToIso(value);
  if (!timestamp) return false;
  const parsedAt = Date.parse(timestamp);
  return (
    !Number.isNaN(parsedAt) &&
    parsedAt <= Date.now() + ANALYTICAL_METADATA_LIMITS.maxFutureTimestampSkewMs
  );
}

type SpooledTimeSignals = {
  /** Alias attributes, flattened last-one-wins exactly as `collectOtelSignals` flattens them. */
  aliasAttributes: Record<string, unknown>;
  /** How many unix-nano times would reach `otelSignals.timestamps`. */
  usableOtelTimes: number;
};

/**
 * The same traversal `collectOtelSignals` runs, narrowed to the two things that
 * decide an event's time. Same pre-order, same flattening, so a body with two
 * `timestamp` attributes is judged on the one the normalizer would end up with.
 */
function collectSpooledTimeSignals(value: unknown, signals: SpooledTimeSignals) {
  if (!value || typeof value !== "object") return;
  if (Array.isArray(value)) {
    for (const item of value) collectSpooledTimeSignals(item, signals);
    return;
  }
  const record = value as Record<string, unknown>;
  if (typeof record.key === "string" && "value" in record && OBSERVED_AT_ALIASES.has(record.key)) {
    signals.aliasAttributes[record.key] = otelScalar(record.value);
  }
  for (const key of OTEL_TIME_KEYS) {
    if (usableOtelTime(record[key])) signals.usableOtelTimes += 1;
  }
  for (const nested of Object.values(record)) collectSpooledTimeSignals(nested, signals);
}

/**
 * True when this body already carries a time the normalizer will USE, so the
 * drain must not supply one (review r3, N2).
 *
 * Presence is not usability. r3 asked only whether an alias KEY was there, so a
 * body whose `timestamp` held a millisecond epoch — an ordinary producer
 * convention — or an empty string, `null`, a boolean or an object opted itself
 * out of the fix and was stamped with the recovery clock again: the exact r2
 * defect, reached by a different door. This decides the way the normalizer
 * decides:
 *   - a TOP-LEVEL alias counts only when `validatedMetadataAttribute` accepts
 *     its value as a string (the `observedAt` selection runs over the RAW
 *     top-level partition);
 *   - an OTLP `{key, value}` attribute alias counts only when its scalar
 *     (`otelScalar` — i.e. a usable `stringValue`) is accepted the same way;
 *   - a `timeUnixNano`/`observedTimeUnixNano`/`startTimeUnixNano` counts only
 *     when it parses and is not from the future.
 * When the body's own time is unusable this is false and the drain supplies the
 * envelope's `receivedAt` as a top-level `observedAt`. That is safe precisely
 * because the normalizer's own precedence then ignores the unusable field — it
 * rejects it live for the same reason.
 */
function bodyCarriesItsOwnTime(payload: Record<string, unknown>) {
  for (const alias of OBSERVED_AT_ALIASES) {
    if (alias in payload && usableObservedAtValue(alias, payload[alias])) return true;
  }
  const signals: SpooledTimeSignals = { aliasAttributes: {}, usableOtelTimes: 0 };
  collectSpooledTimeSignals(payload, signals);
  for (const [key, value] of Object.entries(signals.aliasAttributes)) {
    if (usableObservedAtValue(key, value)) return true;
  }
  return signals.usableOtelTimes > 0;
}

/**
 * Give a recovered event the time the HOOK fired, not the time the drain got to
 * it (review r2, F1).
 *
 * `normalizeHookPayload` falls back to `new Date()` for a body with no time of
 * its own, and no real hook body carries one — so before r3 every recovered
 * event was stamped with the recovery clock, skewed by the whole spool latency
 * (a managed-update restart, a deferred tick, up to doctor's 600 s stall
 * threshold). `observed_at` drives retention cutoffs and every cost/usage time
 * bucket, so that skew moved real spend into the wrong window.
 *
 * The envelope's `receivedAt` is the hook process's own stamp, written before
 * the file (`writeHookSpoolFile`), so it is the right answer and it is already
 * on disk. It is supplied as a DEFAULT: a body that carries any time the
 * normalizer honours keeps it, untouched.
 */
export function spooledBodyWithHookTime(bodyText: string, receivedAt: string) {
  const receivedAtMs = Date.parse(receivedAt);
  if (!Number.isFinite(receivedAtMs)) return bodyText;
  let parsed: unknown;
  try {
    parsed = JSON.parse(bodyText);
  } catch {
    // Not JSON: hand it over unchanged and let the route reject it exactly as
    // it would have. The drain does not decide what is admissible.
    return bodyText;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return bodyText;
  const payload = parsed as Record<string, unknown>;
  if (bodyCarriesItsOwnTime(payload)) return bodyText;
  try {
    return JSON.stringify({ ...payload, observedAt: new Date(receivedAtMs).toISOString() });
  } catch {
    return bodyText;
  }
}

/**
 * A receipt-safe error label: the `code` an fs/system error carries, never its
 * message, which embeds the absolute path it failed on.
 */
function errorCodeOnly(error: unknown) {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" ? code : "unknown";
}

export type HookSpoolDrainTick = {
  recovered: number;
  rejected: number;
  deferred: number;
  /** True when a busy ledger stopped this tick before the queue was empty. */
  deferredTick: boolean;
  attempted: number;
};

export type HookSpoolDrain = {
  start: () => void;
  stop: () => void;
  /** Cached snapshot; no filesystem or SQLite work runs on the request path. */
  status: () => HookSpoolStatus;
  /** One drain pass. Exported for the proof and for the startup pass. */
  tick: () => Promise<HookSpoolDrainTick>;
};

/**
 * The collector side of the hook spool: every 5 s, apply the oldest spooled
 * events through the hook route's own callable.
 *
 * Three outcomes per file and no fourth. Applied -> unlink and count
 * `recovered`. The ledger is busy again -> the file stays exactly where it is,
 * this tick stops (there is no point walking 200 files into the same lock),
 * and `deferred` counts the wait; there is no attempt cap, because a busy
 * ledger is a wait, not a verdict. The route would answer 4xx -> the file is
 * quarantined under `rejected/` and counted, because replaying a body the
 * contract refuses forever is the failure mode this bead exists to avoid.
 */
export function createHookSpoolDrain(
  config: CollectorConfig,
  buffer: LocalEventBuffer,
  options: {
    home: string;
    env?: NodeJS.ProcessEnv;
    intervalMs?: number;
    maxFilesPerTick?: number;
    nowMs?: () => number;
    onWarning?: (line: Record<string, unknown>) => void;
  },
): HookSpoolDrain {
  const env = options.env ?? process.env;
  const nowMs = options.nowMs ?? (() => Date.now());
  const intervalMs = options.intervalMs ?? HOOK_SPOOL_LIMITS.drainIntervalMs;
  const maxFilesPerTick = options.maxFilesPerTick ?? HOOK_SPOOL_LIMITS.maxFilesPerTick;
  const warn = options.onWarning ?? ((line) => console.warn(JSON.stringify(line)));
  const enabled = hookSpoolEnabled(env);
  let counters = readHookSpoolCounters(options.home);
  let pending = hookSpoolPending(options.home, nowMs());
  let timer: NodeJS.Timeout | undefined;
  let inFlight = false;
  let ticks = 0;

  const snapshot = (): HookSpoolStatus => ({ enabled, ...counters, ...pending });

  const tick = async (): Promise<HookSpoolDrainTick> => {
    const result: HookSpoolDrainTick = {
      recovered: 0,
      rejected: 0,
      deferred: 0,
      deferredTick: false,
      attempted: 0,
    };
    if (!enabled) return result;
    ticks += 1;
    // Retention and orphan reaping run on the tick's own cadence, not on the
    // arrival of a new rejection (review r1, F3/F4): a quiet spool used to keep
    // quarantined files past their age bound forever, and nothing ever deleted
    // a `*.json.tmp` left by a crashed hook process.
    if (ticks % HOOK_SPOOL_LIMITS.rejectedPruneEveryTicks === 0) {
      pruneHookSpoolRejected(options.home, nowMs());
    }
    reapHookSpoolTemporaries(options.home, nowMs());
    const files = listHookSpoolFiles(options.home, maxFilesPerTick);
    if (files.length === 0) {
      pending = hookSpoolPending(options.home, nowMs());
      return result;
    }
    // The trust boundary is the directory first: a spool directory this uid
    // does not privately own is never replayed, whatever it holds.
    const directoryTrusted = hookSpoolEntryTrusted(hookSpoolDirectory(options.home), "directory");
    for (const file of files) {
      result.attempted += 1;
      if (!directoryTrusted || !hookSpoolEntryTrusted(file.path, "file")) {
        rejectHookSpoolFile(options.home, file, "spool_untrusted");
        result.rejected += 1;
        continue;
      }
      const read = readHookSpoolFile(file.path);
      if (!read.ok) {
        rejectHookSpoolFile(options.home, file, read.reason);
        result.rejected += 1;
        continue;
      }
      try {
        // The hook's own time, not the drain's (review r2, F1).
        await admitHookBody(
          spooledBodyWithHookTime(read.envelope.body, read.envelope.receivedAt),
          read.envelope.source,
          { config, buffer, budget: createRequestBudget() },
        );
        try {
          fs.unlinkSync(file.path);
        } catch {
          /* already gone; the counter still reflects the applied event */
        }
        result.recovered += 1;
      } catch (error) {
        const failure = asHttpBoundaryRejection(error);
        if (failure.status === 503) {
          result.deferred += 1;
          result.deferredTick = true;
          break;
        }
        rejectHookSpoolFile(options.home, file, failure.reason);
        result.rejected += 1;
      }
      // Yield between files: the drain shares this loop with /hooks/* and the
      // OTLP receiver and must never hold it for a whole tick.
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    // `spooledAtIntake` belongs to the intake, which read-modify-writes this
    // same file whenever it spools (`recordHookSpoolIntake`). Read it here and
    // write it back unchanged: this read and the write below are one
    // synchronous span, so the daemon's single thread cannot interleave a
    // spool between them and lose an increment.
    counters = {
      recovered: counters.recovered + result.recovered,
      rejected: counters.rejected + result.rejected,
      deferred: counters.deferred + result.deferred,
      spooledAtIntake: readHookSpoolCounters(options.home).spooledAtIntake,
      lastDrainAt: new Date(nowMs()).toISOString(),
    };
    try {
      writeHookSpoolCounters(options.home, counters);
    } catch (error) {
      // The code and nothing else (review r1, F7): an fs failure message
      // embeds the absolute path it failed on, and home paths stay out of
      // receipts everywhere else in this code base.
      warn({
        warning: "hook_spool_counters_write_failed",
        code: errorCodeOnly(error),
      });
    }
    if (result.rejected > 0) pruneHookSpoolRejected(options.home, nowMs());
    pending = hookSpoolPending(options.home, nowMs());
    if (result.recovered > 0 || result.rejected > 0) {
      console.log(
        JSON.stringify({
          status: "hook_spool_drain",
          recovered: result.recovered,
          rejected: result.rejected,
          deferred: result.deferred,
          pendingFiles: pending.pendingFiles,
        }),
      );
    }
    return result;
  };

  return {
    start() {
      if (!enabled || timer) return;
      // Start-up prune: a host that rejected a burst and then went quiet (or
      // was restarted) must still lose its quarantine at the age bound.
      pruneHookSpoolRejected(options.home, nowMs());
      reapHookSpoolTemporaries(options.home, nowMs());
      timer = setInterval(() => {
        if (inFlight) return;
        inFlight = true;
        void tick()
          .catch((error) => {
            warn({
              warning: "hook_spool_drain_failed",
              message: error instanceof Error ? error.message : String(error),
            });
          })
          .finally(() => {
            inFlight = false;
          });
      }, intervalMs);
      timer.unref();
    },
    stop() {
      if (timer) clearInterval(timer);
      timer = undefined;
    },
    status: snapshot,
    tick,
  };
}

function requestUrl(request: http.IncomingMessage) {
  return new URL(request.url ?? "/", "http://127.0.0.1");
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
    /**
     * Plimsoll home backing `localAuth`. When set, every producer request first
     * stats the credential file and re-reads it when it has moved on, so
     * `rotate-producer-token` takes effect on a running daemon without a
     * restart and a closed grace window stops admitting the superseded token.
     * Absent, the cached authority is final.
     */
    localAuthHome?: string;
    /** Private hash registry home; no provisioning occurs on the listener. */
    liveProducerHome?: string;
    /** Proof-injectable per-source admission ceiling (defaults to the limit). */
    perSourceRequestLimit?: number;
    /**
     * Cached hook-spool snapshot (bead eco-6hoxj.61), supplied by the drain the
     * daemon arms. Reading it is in-memory only: /status stays cache-only and
     * never stats the spool directory on the request path.
     */
    hookSpoolStatus?: () => HookSpoolStatus | null;
    /** Process-local upload scheduler state; no DB or filesystem work. */
    syncStatus?: () => unknown;
    /**
     * Environment the intake spool reads its kill switch and its home from.
     * Production is `process.env`, exactly as the drain's is.
     */
    env?: NodeJS.ProcessEnv;
    /**
     * Proof-injectable spool home. Absent, the intake resolves the SAME
     * canonical Plimsoll home the drain is armed with (`resolveHookSpoolHome`
     * -> `resolveCollectorHome`, the resolver `collectorHome()` calls), so a
     * spooled post lands in the directory this daemon drains.
     */
    hookSpoolHome?: string;
    /** Proof-injectable directory bounds. Production uses HOOK_SPOOL_LIMITS. */
    hookSpoolLimits?: Partial<HookSpoolBounds>;
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
  // Rotation reload seam. Management credentials deliberately keep using the
  // authority loaded at start; only the producer audiences follow a rotation.
  // Freshness is decided before the admission decision, never after a miss: a
  // rejection must not be the only thing that can install a rotation, or the
  // superseded token outlives its deadline and the new one is locked out.
  let producerAuth = localAuth;
  let producerAuthStamp = options.localAuthHome ? localIngestAuthStamp(options.localAuthHome) : null;
  // Stamp of a credential file that failed to load. An operator can leave the
  // file truncated, malformed, or no longer private for as long as they like;
  // re-reading and re-parsing it on every request buys nothing, because
  // admission keeps using the installed authority either way. The stat still
  // runs, so the repaired file is picked up on the first request after it moves.
  let unreadableAuthStamp: string | null = null;
  const graceWindowClosed = (auth: LocalIngestAuth) => {
    const now = Date.now();
    return Object.values(auth.rotations ?? {}).some((rotation) => rotation.expiresAt <= now);
  };
  const refreshProducerAuth = (loaded: LocalIngestAuth) => {
    const home = options.localAuthHome;
    if (!home) return loaded;
    const stamp = localIngestAuthStamp(home);
    // No readable credential file: the authority this daemon started with
    // stays in force rather than admitting or rejecting on a guess.
    if (stamp === null) return loaded;
    if (stamp === unreadableAuthStamp) return loaded;
    if (stamp === producerAuthStamp && !graceWindowClosed(loaded)) return loaded;
    // One load per observed change, plus one more once a window has closed so
    // the closed row also leaves the authority in memory. The read never
    // writes: the credential file belongs to `rotate-producer-token`. The
    // stamp advances only after the load succeeded, and it is the stamp read
    // *before* the load, so a file that changed mid-read is re-read next time.
    const reloaded = readLiveProducerAuth(home);
    if (!reloaded) {
      unreadableAuthStamp = stamp;
      return loaded;
    }
    unreadableAuthStamp = null;
    producerAuth = reloaded;
    producerAuthStamp = stamp;
    return reloaded;
  };
  const assertProducer = (request: http.IncomingMessage, source: LocalProducerSource) => {
    const loaded = producerAuth;
    if (!loaded) return;
    assertProducerToken(request, refreshProducerAuth(loaded), source, requestUrl(request));
  };

  // Issue #0075 (#144): repeated identical admission rejections are
  // aggregated. Decisions at the HTTP boundary stay fail-closed and their
  // responses stay byte-for-byte identical; only the terminal/log stream is
  // bounded by reason class plus interval summary.
  const rejectionDiagnostics = createRejectionDiagnostics({
    nowMs: options.diagnosticsNowMs,
  });

  // ---------------------------------------------------------------------
  // Intake spool (bead eco-6hoxj.61, round r5).
  //
  // The managed hooks the fleet installs do not run `forward-hook-http`:
  // Claude Code's is an `http` hook posting straight to
  // /hooks/claude-code, and the Codex/Grok hooks are `curl` commands posting
  // straight to /hooks/codex and /hooks/grok. So the client-side spool never
  // sees their events, and on a busy ledger they were answered 503 and lost.
  // The spool goes where those posts actually arrive: here.
  // ---------------------------------------------------------------------
  const spoolEnv = options.env ?? process.env;
  // Read once, like the drain reads it once when it starts: the kill switch
  // belongs to the daemon's environment (review r1, F5), and the intake and
  // the drain must never disagree about whether this home is spooling.
  const hookSpoolIntakeEnabled = hookSpoolEnabled(spoolEnv);
  // Resolved on first use and cached: a CollectorHomeError must not change
  // start-up behaviour, it must simply leave the 503 exactly as it is today.
  let hookSpoolIntakeHome: string | null | undefined;
  const intakeSpoolHome = () => {
    if (hookSpoolIntakeHome !== undefined) return hookSpoolIntakeHome;
    if (options.hookSpoolHome !== undefined) {
      hookSpoolIntakeHome = options.hookSpoolHome;
      return hookSpoolIntakeHome;
    }
    try {
      hookSpoolIntakeHome = resolveHookSpoolHome(spoolEnv);
    } catch {
      hookSpoolIntakeHome = null;
    }
    return hookSpoolIntakeHome;
  };
  const spoolSummaryNowMs = options.diagnosticsNowMs ?? Date.now;
  /**
   * Per-interval aggregation for the intake's own log line, on exactly the
   * shape and cadence `rejection-diagnostics.ts` uses for a rejection: the
   * first spool of a window is announced immediately, the rest are counted and
   * reported by one summary when the window closes. Counts and a client class,
   * never a body, never a path, never a token.
   */
  const spoolWindows = new Map<
    RejectionClientClass,
    { firstAtMs: number; count: number; suppressed: number }
  >();
  const closeSpoolWindows = (nowMs: number, all: boolean) => {
    const lines: Array<Record<string, unknown>> = [];
    for (const [clientClass, window] of spoolWindows) {
      if (!all && nowMs - window.firstAtMs < REJECTION_SUMMARY_INTERVAL_MS) continue;
      spoolWindows.delete(clientClass);
      lines.push({
        status: "hook_spooled_at_intake_summary",
        clientClass,
        count: window.count,
        suppressed: window.suppressed,
        intervalMs: REJECTION_SUMMARY_INTERVAL_MS,
      });
    }
    return lines;
  };
  const observeIntakeSpool = (source: HookSpoolSource, clientClass: RejectionClientClass) => {
    const nowMs = spoolSummaryNowMs();
    for (const line of closeSpoolWindows(nowMs, false)) console.warn(JSON.stringify(line));
    const window = spoolWindows.get(clientClass);
    if (!window) {
      spoolWindows.set(clientClass, { firstAtMs: nowMs, count: 1, suppressed: 0 });
      console.warn(JSON.stringify({ status: "hook_spooled_at_intake", source, clientClass }));
      return;
    }
    window.count += 1;
    window.suppressed += 1;
  };
  /**
   * Write the refused post to the spool, or answer null so the caller keeps
   * today's 503 exactly as it is. Same envelope, same blanking, same bounds,
   * same write-temporary-then-rename durability as the client's spool —
   * literally the same `writeHookSpoolFile` — and the caller answers 202 only
   * after that rename returned.
   */
  const spoolHookAtIntake = (
    source: LocalProducerSource,
    bodyText: string,
    receivedAtMs: number,
  ) => {
    if (!hookSpoolIntakeEnabled) return null;
    // `/hooks/<source>` resolves only the three hook sources, so this is a
    // type narrowing rather than a filter; a source the spool cannot name is
    // refused rather than guessed.
    if (!isHookSpoolSource(source)) return null;
    // Suppressed BEFORE the write, with the collector's own DROP rule, exactly
    // as the client spool suppresses it: the spool is a local write that
    // happens before the collector's suppression can run.
    const blanked = blankForbiddenRawContent(bodyText);
    if (!blanked) return null;
    const home = intakeSpoolHome();
    if (home === null) return null;
    const written = writeHookSpoolFile({
      home,
      source,
      body: blanked.text,
      blanked: blanked.blanked,
      // The daemon's request receive time, so the drain replays the event with
      // the time it ARRIVED rather than the time the ledger freed up.
      nowMs: receivedAtMs,
      limits: options.hookSpoolLimits,
    });
    if (!written) return null;
    try {
      recordHookSpoolIntake(home);
    } catch (error) {
      // The event is already durable; a counters write that failed is a
      // reporting gap, not a loss. Code only, never the path it failed on.
      console.warn(JSON.stringify({
        warning: "hook_spool_counters_write_failed",
        code: errorCodeOnly(error),
      }));
    }
    return { path: written.path, source };
  };

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
      // Bead eco-6hoxj.61: hook events the collector refused with 503 or could
      // not receive at all, and what the drain has since done with them.
      hookSpool: options.hookSpoolStatus?.() ?? null,
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
    // The daemon's request receive time. A hook post the intake has to spool
    // carries this into the envelope's `receivedAt`, so the drain replays the
    // event with the time it arrived here (bead eco-6hoxj.61).
    const receivedAtMs = Date.now();
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
          body.hookSpool = options.hookSpoolStatus?.() ?? null;
          body.sync = options.syncStatus?.() ?? null;
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
        assertProducer(request, source);
        const body = decodeBoundedRequestBody(
          request,
          await readBoundedRequestBody(request, budget),
        );
        let normalized: Awaited<ReturnType<typeof admitHookBody>>;
        try {
          normalized = await admitHookBody(body.text, source, { config, buffer, budget });
        } catch (error) {
          // The ONE outcome that is spooled here: the busy class that answers
          // 503 `storage_busy_retry` today. It is raised only when the ledger
          // write did not commit — `admitHookBody`'s `retryStorageBusy`
          // (`http-boundary.ts:265`) rethrows the SQLite contention error its
          // retry budget could not get past, `asHttpBoundaryRejection`
          // (`http-boundary.ts:117-118`) is the only place that turns one into
          // this rejection, and the contention error can only escape
          // `LocalEventBuffer.append`'s `db.transaction(run).immediate()`
          // (`buffer.ts:2559`), which SQLite has rolled back by the time it
          // throws. Everything after that commit is in-memory handoff
          // bookkeeping that catches its own failures (`buffer.ts:1466`), so
          // there is no committed-then-timed-out outcome to be ambiguous about.
          //
          // Not spooled: 408 `request_deadline_exceeded` (the body is
          // incomplete — there is nothing whole to spool), authorization
          // failures and admission rejections (they never reached the ledger
          // on their merits and must stay visible), and every other 5xx.
          const failure = asHttpBoundaryRejection(error);
          const spooled =
            failure.reason === "storage_busy_retry" && failure.status === 503
              ? spoolHookAtIntake(source, body.text, receivedAtMs)
              : null;
          // A spool that could not be written — bounds exhausted, disk, EACCES,
          // kill switch — keeps today's answer exactly: the loss stays visible.
          if (!spooled) throw error;
          observeIntakeSpool(spooled.source, classifyRejectionClient(request));
          // 202 only after the file and directory flushes returned. The event
          // is private and blanked; the drain uses this same admission callable.
          response.writeHead(202, { "content-type": "application/json" });
          response.end(JSON.stringify({ status: "hook_spooled", source }));
          return;
        }
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
        assertProducer(request, source);
        const body = decodeBoundedRequestBody(
          request,
          await readBoundedRequestBody(request, budget),
        );
        const parsedEnvelope = parseBoundedJson(body.text);
        assertBoundedOtlpCardinality(parsedEnvelope, body.decodedBytes);
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
      const recordDiagnostic = failure.reason === "otlp_record_limit_exceeded"
        ? failure.diagnostic
        : undefined;
      const diagnosticRejection = {
        ...rejection,
        clientClass,
        ...(recordDiagnostic ?? {}),
      };
      // Aggregate identical rejections: emit the first occurrence of a
      // bounded reason promptly plus any window summaries it just closed.
      // The HTTP response below stays byte-for-byte unchanged.
      const observed = rejectionDiagnostics.observeRejection(
        failure.reason,
        clientClass,
        recordDiagnostic,
      );
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
    // Shutdown flush closes the intake-spool window too, so a daemon going
    // down does not take an open count with it. Its line shape is not a
    // rejection summary, so it is printed here rather than returned.
    flush: () => {
      for (const line of closeSpoolWindows(spoolSummaryNowMs(), true)) {
        console.warn(JSON.stringify(line));
      }
      return rejectionDiagnostics.flush();
    },
    counters: () => rejectionDiagnostics.counters(),
  };
  return server;
}
