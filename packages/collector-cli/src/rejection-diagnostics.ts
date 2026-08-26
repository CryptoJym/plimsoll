import type http from "node:http";

import { HTTP_BOUNDARY_REASONS } from "./http-boundary";
import type { HttpBoundaryReason, LocalProducerSource } from "./http-boundary";

export { HTTP_BOUNDARY_REASONS } from "./http-boundary";
export type { HttpBoundaryReason } from "./http-boundary";

/**
 * Fixed suppression window for repeated identical admission rejections. The
 * first occurrence of a bounded reason is logged immediately; identical
 * rejections inside the window are counted silently and reported by one
 * aggregate summary when the window closes (on the next observation past the
 * boundary or on shutdown flush).
 */
export const REJECTION_SUMMARY_INTERVAL_MS = 60_000;

/**
 * Hard saturation bound for every monotonic counter. At the bound counting
 * freezes (no wraparound) so the conservation identity — rejected equals
 * emitted-first plus suppressed, and rejected equals summarized plus any open
 * window — holds at every instant.
 */
export const REJECTION_COUNTER_CAP = Number.MAX_SAFE_INTEGER;

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

export type RejectionSummaryLine = {
  error: "collector_request_rejected_summary";
  reason: HttpBoundaryReason;
  /** Total rejections accounted by this closed window (first + suppressed). */
  count: number;
  /** Rejections inside the window after its first occurrence. */
  suppressed: number;
  intervalMs: number;
  action: string;
};

export type RejectionObservation = {
  /** True when this observation is a window's first, i.e. must be logged. */
  first: boolean;
  /** Summaries of windows this observation closed (emit before the first). */
  summaries: RejectionSummaryLine[];
};

export type RejectionCounterRow = {
  reason: HttpBoundaryReason;
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
  /** Only reasons with nonzero activity; cardinality bounded by the enum. */
  reasons: RejectionCounterRow[];
};

type WindowState = { firstAtMs: number; count: number; suppressed: number };

type ReasonState = {
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

export function createRejectionDiagnostics(options: {
  nowMs?: () => number;
  /**
   * Proof/recovery seeding only. Production never passes it. Seeded state is
   * trusted to satisfy the conservation identity per reason.
   */
  initialByReason?: Partial<Record<HttpBoundaryReason, SeedReasonState>>;
} = {}) {
  const nowMs = options.nowMs ?? Date.now;
  const states = new Map<HttpBoundaryReason, ReasonState>();
  const acceptedBySource: Record<LocalProducerSource, number> = {
    claude_code: 0,
    codex: 0,
    cursor: 0,
  };
  if (options.initialByReason) {
    for (const [reason, seed] of Object.entries(options.initialByReason)) {
      states.set(reason as HttpBoundaryReason, {
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

  const stateFor = (reason: HttpBoundaryReason): ReasonState => {
    let state = states.get(reason);
    if (!state) {
      state = { rejected: 0, suppressed: 0, emittedFirst: 0, summarized: 0, window: null };
      states.set(reason, state);
    }
    return state;
  };

  const closeWindow = (
    reason: HttpBoundaryReason,
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
      reason,
      count: window.count,
      suppressed: window.suppressed,
      intervalMs: REJECTION_SUMMARY_INTERVAL_MS,
      action: HTTP_REJECTION_NEXT_ACTIONS[reason],
    };
  };

  return {
    observeRejection(reason: HttpBoundaryReason): RejectionObservation {
      const now = nowMs();
      const summaries: RejectionSummaryLine[] = [];
      for (const [key, state] of states) {
        if (
          state.window !== null &&
          now - state.window.firstAtMs >= REJECTION_SUMMARY_INTERVAL_MS
        ) {
          summaries.push(closeWindow(key, state));
        }
      }

      const state = stateFor(reason);
      // Saturated: keep decisions and HTTP behavior unchanged, freeze counting.
      if (state.rejected >= REJECTION_COUNTER_CAP) {
        return { first: false, summaries };
      }

      if (state.window === null) {
        state.window = { firstAtMs: now, count: 1, suppressed: 0 };
        state.rejected = nextCount(state.rejected);
        state.emittedFirst = nextCount(state.emittedFirst);
        return { first: true, summaries };
      }

      state.window.count = nextCount(state.window.count);
      state.window.suppressed = nextCount(state.window.suppressed);
      state.rejected = nextCount(state.rejected);
      state.suppressed = nextCount(state.suppressed);
      return { first: false, summaries };
    },

    flush(): RejectionSummaryLine[] {
      const summaries: RejectionSummaryLine[] = [];
      for (const [key, state] of states) {
        if (state.window !== null) summaries.push(closeWindow(key, state));
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
      for (const reason of HTTP_BOUNDARY_REASONS) {
        const state = states.get(reason);
        if (!state) continue;
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
          reason,
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
