import type http from "node:http";

import { HTTP_BOUNDARY_REASONS, OTLP_RECORD_ARRAY_KEYS } from "./http-boundary";
import type {
  HttpBoundaryReason,
  LocalProducerSource,
  OtlpRecordArrayKey,
  OtlpRecordRejectionDiagnostic,
  RejectionRoute,
} from "./http-boundary";

export { HTTP_BOUNDARY_REASONS, OTLP_RECORD_ARRAY_KEYS } from "./http-boundary";
export type {
  HttpBoundaryReason,
  OtlpRecordArrayKey,
  OtlpRecordRejectionDiagnostic,
  RejectionRoute,
} from "./http-boundary";

/**
 * Fixed suppression window for repeated identical admission rejections. The
 * first occurrence of a bounded reason is logged immediately; identical
 * rejections inside the window are counted silently and reported by one
 * aggregate summary when the window closes (on the next observation past the
 * boundary or on shutdown flush).
 */
export const REJECTION_SUMMARY_INTERVAL_MS = 60_000;

/**
 * Fixed maximum for a serialized summary, including both last/max maps with
 * every closed record-array key and saturated lifetime counters. Record and
 * byte values originate behind the 100,000-node and 4 MiB decoded ceilings.
 */
export const REJECTION_SUMMARY_LINE_MAX_BYTES = 640;

/**
 * Hard saturation bound for every monotonic counter. At the bound counting
 * freezes (no wraparound) so the conservation identity — rejected equals
 * emitted-first plus suppressed, and rejected equals summarized plus any open
 * window — holds at every instant.
 */
export const REJECTION_COUNTER_CAP = Number.MAX_SAFE_INTEGER;

export const REJECTION_CLIENT_CLASSES = [
  "codex",
  "claude_code",
  "grok",
  "otlp_exporter",
  "unknown",
] as const;
export type RejectionClientClass = (typeof REJECTION_CLIENT_CLASSES)[number];

/** Reduce request identity to a closed enum; never retain raw header values. */
export function classifyRejectionClient(request: {
  headers: http.IncomingHttpHeaders;
  socket?: { remotePort?: number };
}): RejectionClientClass {
  const source = request.headers["x-plimsoll-source"];
  const firstSource = Array.isArray(source) ? source[0] : source;
  if (firstSource === "codex" || firstSource === "claude_code" || firstSource === "grok") return firstSource;
  const agent = request.headers["user-agent"];
  const firstAgent = (Array.isArray(agent) ? agent[0] : agent)?.toLowerCase() ?? "";
  if (/\b(?:otel|opentelemetry|otlp)[\s/_-]/.test(firstAgent)) return "otlp_exporter";
  return "unknown";
}

/**
 * Symbolic next operator action per bounded reason. Compile-time exhaustive:
 * adding a reason to `HTTP_BOUNDARY_REASONS` without an action here fails
 * typecheck. Actions name the next step only; config values, header values,
 * and credentials are never echoed.
 */
export const HTTP_REJECTION_NEXT_ACTIONS: Record<HttpBoundaryReason, string> = {
  browser_origin_not_allowed: "use_non_browser_client",
  compressed_body_too_large: "reduce_request_body_bytes",
  compression_ratio_too_large: "reduce_request_body_bytes",
  decoded_body_too_large: "reduce_request_body_bytes",
  host_not_allowed: "send_loopback_host_header",
  internal_rejection: "inspect_collector_log",
  invalid_compressed_body: "fix_payload_encoding",
  invalid_json: "fix_payload_json",
  json_depth_exceeded: "reduce_envelope_nesting",
  json_node_limit_exceeded: "reduce_envelope_cardinality",
  management_credential_invalid: "present_valid_management_credential",
  management_credential_required: "configure_management_credential",
  otlp_attribute_limit_exceeded: "reduce_envelope_cardinality",
  otlp_record_limit_exceeded: "reduce_batch_record_count",
  otlp_resource_limit_exceeded: "reduce_envelope_cardinality",
  otlp_scope_limit_exceeded: "reduce_envelope_cardinality",
  producer_token_invalid: "present_source_bound_producer_token",
  producer_token_required: "configure_producer_token_header",
  request_deadline_exceeded: "stream_request_body_promptly",
  request_stream_error: "repair_producer_connection",
  source_mismatch: "match_source_to_endpoint_path",
  source_not_allowed: "set_supported_producer_source",
  source_rate_limit_exceeded: "retry_after_backoff",
  source_required: "configure_producer_source_header",
  storage_busy_retry: "retry_after_backoff",
  unsupported_content_encoding: "use_supported_content_encoding",
};

/**
 * Emission order for the closed route vocabulary, so a window's breakdown
 * serializes the same way whatever order the routes arrived in. Keyed by the
 * route union: adding a route to `RejectionRoute` without ordering it here
 * fails typecheck, the same way `HTTP_REJECTION_NEXT_ACTIONS` bounds reasons.
 */
const REJECTION_ROUTE_ORDER: Record<RejectionRoute, number> = {
  "/hooks/claude-code": 0,
  "/hooks/codex": 1,
  "/hooks/grok": 2,
  otlp: 3,
  other: 4,
};

export const REJECTION_ROUTES = (Object.keys(REJECTION_ROUTE_ORDER) as RejectionRoute[]).sort(
  (left, right) => REJECTION_ROUTE_ORDER[left] - REJECTION_ROUTE_ORDER[right],
);

/**
 * The reasons whose window carries a per-route breakdown. Only the busy class
 * does, matching the emitter rule that the busy class, and only it, carries
 * route diagnostics (`server.ts`): a busy 503 can arrive on a hook route the
 * intake spool covers or on OTLP, which it does not, and `clientClass` alone
 * cannot tell those apart for one producer. Bounding the breakdown to this one
 * reason lets the window enforce exclusivity: ingest builds no record
 * statistics for a route-classified reason (it counts the discarded
 * diagnostics instead), so a summary contains record-array maps or a route
 * map, never both, and stays inside `REJECTION_SUMMARY_LINE_MAX_BYTES`.
 *
 * The ingest gate is the only reader, and it reads the reason normalised with
 * `String()` — the same value the window is keyed by and stores — so no caller
 * can classify one way and land on a window classified the other way.
 *
 * Frozen, not merely `readonly`: a runtime push between a window's open and
 * its close would widen the vocabulary under an already-open window and strip
 * that window's record diagnostics with no counter and no log. `Object.freeze`
 * makes that mutation a `TypeError` instead.
 */
export const ROUTE_CLASSIFIED_REASONS: readonly HttpBoundaryReason[] = Object.freeze([
  "storage_busy_retry",
] as const);

export type RejectionSummaryLine = {
  error: "collector_request_rejected_summary";
  reason: HttpBoundaryReason;
  clientClass: RejectionClientClass;
  /** Total rejections accounted by this closed window (first + suppressed). */
  count: number;
  /** Rejections inside the window after its first occurrence. */
  suppressed: number;
  intervalMs: number;
  action: string;
  recordCountLast?: number;
  recordCountMax?: number;
  recordArraysLast?: Partial<Record<OtlpRecordArrayKey, number>>;
  recordArraysMax?: Partial<Record<OtlpRecordArrayKey, number>>;
  decodedBytesLast?: number;
  decodedBytesMax?: number;
  /**
   * Record diagnostics this window was handed and could not report, because a
   * route-classified reason carries a route map instead of record-array maps.
   * Absent when none were discarded, which is every production window: the one
   * production busy caller never attaches a record diagnostic. Present, it says
   * the diagnostics were dropped by the emitter rather than never collected.
   */
  recordDiagnosticsDiscarded?: number;
  /**
   * Per-route split of `count`, emitted last so the rest of the line is the
   * byte-identical pre-0.7.27 line. Present only when the window holds a
   * route-classified rejection. For unseeded production windows its values
   * sum to `count`; proof/recovery seeds have no route attribution and are not
   * included in this split. `count` remains the whole window total.
   */
  routes?: Partial<Record<RejectionRoute, number>>;
};

export type RejectionObservation = {
  /** True when this observation is a window's first, i.e. must be logged. */
  first: boolean;
  /** Summaries of windows this observation closed (emit before the first). */
  summaries: RejectionSummaryLine[];
};

export type RejectionCounterRow = {
  reason: HttpBoundaryReason;
  clientClass: RejectionClientClass;
  rejected: number;
  suppressed: number;
  emittedFirst: number;
  /** Lifetime rejections accounted by already-emitted summaries. */
  summarized: number;
  openWindow: { count: number; suppressed: number } | null;
};

export type RejectionDiagnosticsCounters = {
  counterLifetime: "ephemeral_process";
  intervalMs: number;
  counterCap: number;
  acceptedBySource: Record<LocalProducerSource, number>;
  totals: {
    acceptedTotal: number;
    rejectedTotal: number;
    suppressedTotal: number;
    emittedFirstTotal: number;
    summarizedTotal: number;
  };
  /** Only active reason/client pairs; cardinality bounded by two closed enums. */
  reasons: RejectionCounterRow[];
};

type RecordWindowStats = {
  last: OtlpRecordRejectionDiagnostic;
  max: OtlpRecordRejectionDiagnostic;
};

type WindowState = {
  firstAtMs: number;
  count: number;
  suppressed: number;
  recordStats?: RecordWindowStats;
  recordDiagnosticsDiscarded?: number;
  routes?: Partial<Record<RejectionRoute, number>>;
};

type ReasonState = {
  reason: HttpBoundaryReason;
  clientClass: RejectionClientClass;
  rejected: number;
  suppressed: number;
  emittedFirst: number;
  summarized: number;
  window: WindowState | null;
};

export type SeedReasonState = {
  rejected: number;
  suppressed: number;
  emittedFirst: number;
  summarized: number;
  openWindow?: { count: number; suppressed: number };
};

function nextCount(current: number) {
  return current >= REJECTION_COUNTER_CAP ? REJECTION_COUNTER_CAP : current + 1;
}

function copyRecordDiagnostic(
  diagnostic: OtlpRecordRejectionDiagnostic,
): OtlpRecordRejectionDiagnostic {
  const recordArrays: Partial<Record<OtlpRecordArrayKey, number>> = {};
  for (const key of OTLP_RECORD_ARRAY_KEYS) {
    const count = diagnostic.recordArrays[key];
    if (typeof count === "number" && count > 0) recordArrays[key] = count;
  }
  return {
    recordCount: diagnostic.recordCount,
    recordArrays,
    decodedBytes: diagnostic.decodedBytes,
  };
}

function updateRecordStats(
  stats: RecordWindowStats | undefined,
  diagnostic: OtlpRecordRejectionDiagnostic | undefined,
) {
  if (!diagnostic) return stats;
  const last = copyRecordDiagnostic(diagnostic);
  if (!stats) return { last, max: copyRecordDiagnostic(diagnostic) };
  const maxArrays: Partial<Record<OtlpRecordArrayKey, number>> = {};
  for (const key of OTLP_RECORD_ARRAY_KEYS) {
    const max = Math.max(stats.max.recordArrays[key] ?? 0, diagnostic.recordArrays[key] ?? 0);
    if (max > 0) maxArrays[key] = max;
  }
  return {
    last,
    max: {
      recordCount: Math.max(stats.max.recordCount, diagnostic.recordCount),
      recordArrays: maxArrays,
      decodedBytes: Math.max(stats.max.decodedBytes, diagnostic.decodedBytes),
    },
  };
}

/**
 * Fold one observation's record diagnostic into its window. A route-classified
 * window can never emit record statistics, so it never builds them: it counts
 * the discarded diagnostics instead of paying a copy and a `Math.max` over
 * every closed record-array key per suppressed rejection for state that has no
 * way out. `window.recordStats` stays undefined for those reasons, and because
 * the gate reads the same normalised reason the window is keyed by, this is
 * the only place the exclusivity has to be enforced.
 */
function applyRecordDiagnostic(
  window: WindowState,
  routeClassified: boolean,
  diagnostic: OtlpRecordRejectionDiagnostic | undefined,
) {
  if (!routeClassified) {
    window.recordStats = updateRecordStats(window.recordStats, diagnostic);
    return;
  }
  if (diagnostic) {
    window.recordDiagnosticsDiscarded = nextCount(window.recordDiagnosticsDiscarded ?? 0);
  }
}

/**
 * Count one rejection against its route. Each route counter saturates with the
 * same bound as the window count, so the values keep summing to `count`.
 */
function updateRoutes(
  routes: Partial<Record<RejectionRoute, number>> | undefined,
  route: RejectionRoute | undefined,
) {
  if (!route) return routes;
  const next: Partial<Record<RejectionRoute, number>> = { ...(routes ?? {}) };
  next[route] = nextCount(next[route] ?? 0);
  return next;
}

/** Serialize a window's routes in the closed vocabulary's fixed order. */
function orderRoutes(routes: Partial<Record<RejectionRoute, number>>) {
  const ordered: Partial<Record<RejectionRoute, number>> = {};
  for (const route of REJECTION_ROUTES) {
    const count = routes[route];
    if (typeof count === "number" && count > 0) ordered[route] = count;
  }
  return ordered;
}

export function createRejectionDiagnostics(options: {
  nowMs?: () => number;
  /**
   * Proof/recovery seeding only. Production never passes it. Seeded state is
   * trusted to satisfy the conservation identity per reason.
   */
  initialByReason?: Partial<Record<HttpBoundaryReason, SeedReasonState>>;
} = {}) {
  const nowMs = options.nowMs ?? Date.now;
  const states = new Map<string, ReasonState>();
  const acceptedBySource: Record<LocalProducerSource, number> = {
    claude_code: 0,
    codex: 0,
    gemini_cli: 0,
    grok: 0,
  };
  if (options.initialByReason) {
    for (const [reason, seed] of Object.entries(options.initialByReason)) {
      states.set(`${reason}:unknown`, {
        reason: reason as HttpBoundaryReason,
        clientClass: "unknown",
        rejected: seed.rejected,
        suppressed: seed.suppressed,
        emittedFirst: seed.emittedFirst,
        summarized: seed.summarized,
        window: seed.openWindow
          ? { firstAtMs: nowMs(), count: seed.openWindow.count, suppressed: seed.openWindow.suppressed }
          : null,
      });
    }
  }

  /**
   * `reason` arrives normalised from `observeRejection`, so the window key and
   * the reason this state stores are that one value and cannot disagree.
   */
  const stateFor = (reason: HttpBoundaryReason, clientClass: RejectionClientClass): ReasonState => {
    const key = `${reason}:${clientClass}`;
    let state = states.get(key);
    if (!state) {
      state = { reason, clientClass, rejected: 0, suppressed: 0, emittedFirst: 0, summarized: 0, window: null };
      states.set(key, state);
    }
    return state;
  };

  const closeWindow = (
    state: ReasonState,
  ): RejectionSummaryLine => {
    const window = state.window!;
    state.window = null;
    if (state.summarized < REJECTION_COUNTER_CAP) {
      state.summarized += window.count;
      if (state.summarized > REJECTION_COUNTER_CAP) state.summarized = REJECTION_COUNTER_CAP;
    }
    return {
      error: "collector_request_rejected_summary",
      reason: state.reason,
      clientClass: state.clientClass,
      count: window.count,
      suppressed: window.suppressed,
      intervalMs: REJECTION_SUMMARY_INTERVAL_MS,
      action: HTTP_REJECTION_NEXT_ACTIONS[state.reason],
      ...(window.recordStats
        ? {
            recordCountLast: window.recordStats.last.recordCount,
            recordCountMax: window.recordStats.max.recordCount,
            recordArraysLast: window.recordStats.last.recordArrays,
            recordArraysMax: window.recordStats.max.recordArrays,
            decodedBytesLast: window.recordStats.last.decodedBytes,
            decodedBytesMax: window.recordStats.max.decodedBytes,
          }
        : {}),
      // Only ever set on a route-classified window, which production cannot
      // give a record diagnostic, so no production line shape moves.
      ...(window.recordDiagnosticsDiscarded
        ? { recordDiagnosticsDiscarded: window.recordDiagnosticsDiscarded }
        : {}),
      // Last, so a line that carries a breakdown is the byte-identical
      // pre-0.7.27 line with `,"routes":{…}` appended before the brace.
      ...(window.routes ? { routes: orderRoutes(window.routes) } : {}),
    };
  };

  return {
    observeRejection(
      reason: HttpBoundaryReason,
      clientClass: RejectionClientClass = "unknown",
      recordDiagnostic?: OtlpRecordRejectionDiagnostic,
      route?: RejectionRoute,
    ): RejectionObservation {
      const now = nowMs();
      // Normalise once, at the gate. `stateFor` keys a window by a template
      // literal, which coerces; `Array.prototype.includes` compares with
      // SameValueZero, which does not. A `String` object therefore used to
      // classify as unclassified and still land on the route-classified
      // window, building statistics that window could never emit. The gate,
      // the window key and the stored reason all read this one value, so no
      // reason shape can make them disagree; a boxed reason now takes the
      // route-classified branch and its diagnostics are counted, not dropped
      // silently. Coerced once, not three times: an object whose `toString`
      // returns a different string per call cannot split them either.
      const normalizedReason = String(reason) as HttpBoundaryReason;
      const routeClassified = ROUTE_CLASSIFIED_REASONS.includes(normalizedReason);
      const classifiedRoute = routeClassified ? route : undefined;
      const summaries: RejectionSummaryLine[] = [];
      for (const state of states.values()) {
        if (
          state.window !== null &&
          now - state.window.firstAtMs >= REJECTION_SUMMARY_INTERVAL_MS
        ) {
          summaries.push(closeWindow(state));
        }
      }

      const state = stateFor(normalizedReason, clientClass);
      // Saturated: keep decisions and HTTP behavior unchanged, freeze counting.
      if (state.rejected >= REJECTION_COUNTER_CAP) {
        return { first: false, summaries };
      }

      if (state.window === null) {
        state.window = {
          firstAtMs: now,
          count: 1,
          suppressed: 0,
          routes: updateRoutes(undefined, classifiedRoute),
        };
        applyRecordDiagnostic(state.window, routeClassified, recordDiagnostic);
        state.rejected = nextCount(state.rejected);
        state.emittedFirst = nextCount(state.emittedFirst);
        return { first: true, summaries };
      }

      state.window.count = nextCount(state.window.count);
      state.window.suppressed = nextCount(state.window.suppressed);
      applyRecordDiagnostic(state.window, routeClassified, recordDiagnostic);
      state.window.routes = updateRoutes(state.window.routes, classifiedRoute);
      state.rejected = nextCount(state.rejected);
      state.suppressed = nextCount(state.suppressed);
      return { first: false, summaries };
    },

    flush(): RejectionSummaryLine[] {
      const summaries: RejectionSummaryLine[] = [];
      for (const state of states.values()) {
        if (state.window !== null) summaries.push(closeWindow(state));
      }
      return summaries;
    },

    recordAccepted(source: LocalProducerSource) {
      acceptedBySource[source] = nextCount(acceptedBySource[source]);
    },

    counters(): RejectionDiagnosticsCounters {
      let acceptedTotal = 0;
      for (const count of Object.values(acceptedBySource)) acceptedTotal += count;
      let rejectedTotal = 0;
      let suppressedTotal = 0;
      let emittedFirstTotal = 0;
      let summarizedTotal = 0;
      const rows: RejectionCounterRow[] = [];
      for (const state of states.values()) {
        if (
          state.rejected === 0 &&
          state.suppressed === 0 &&
          state.emittedFirst === 0 &&
          state.summarized === 0 &&
          state.window === null
        ) {
          continue;
        }
        rejectedTotal += state.rejected;
        suppressedTotal += state.suppressed;
        emittedFirstTotal += state.emittedFirst;
        summarizedTotal += state.summarized;
        rows.push({
          reason: state.reason,
          clientClass: state.clientClass,
          rejected: state.rejected,
          suppressed: state.suppressed,
          emittedFirst: state.emittedFirst,
          summarized: state.summarized,
          openWindow: state.window
            ? { count: state.window.count, suppressed: state.window.suppressed }
            : null,
        });
      }
      return {
        counterLifetime: "ephemeral_process",
        intervalMs: REJECTION_SUMMARY_INTERVAL_MS,
        counterCap: REJECTION_COUNTER_CAP,
        acceptedBySource: { ...acceptedBySource },
        totals: {
          acceptedTotal,
          rejectedTotal,
          suppressedTotal,
          emittedFirstTotal,
          summarizedTotal,
        },
        reasons: rows,
      };
    },
  };
}

export type RejectionDiagnostics = ReturnType<typeof createRejectionDiagnostics>;

/** Diagnostics surface attached to the collector HTTP server instance. */
export type CollectorServerDiagnostics = Pick<
  RejectionDiagnostics,
  "flush" | "counters"
>;

export type CollectorServer = http.Server & {
  plimsollHttpDiagnostics: CollectorServerDiagnostics;
};
