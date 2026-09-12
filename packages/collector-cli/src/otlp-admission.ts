import type { AiInteractionEvent, ToolSource } from "../../shared/src/index";

export const OTLP_DROP_REASONS = ["generic_zero_value_span", "app_server_internal_span"] as const;
export type OtlpDropReason = (typeof OTLP_DROP_REASONS)[number];

export type OtlpAdmissionDrop = {
  source: ToolSource;
  reason: OtlpDropReason;
  count: number;
};

export type OtlpAdmissionDecision =
  | { admitted: true }
  | { admitted: false; reason: OtlpDropReason };

/**
 * Names are deliberately narrow. An unknown vendor span fails open because a
 * collector upgrade must never silently erase a new signal. The set contains
 * only live-observed Codex wrapper/control-plane spans from the 2026-07-15
 * sampled ledger. Each is useful only when one of the retained dimensions
 * below is present.
 */
const KNOWN_GENERIC_SPAN_NAMES = new Set([
  "app_server.serialized_request_queue",
  "codex.websocket_event",
  "thread/resume",
  "thread/read",
  "thread/list",
  "thread/goal/get",
  "handle_responses",
  "receiving",
  "resume_running_thread",
  "auth",
  "append_items",
  "remotecontrol/enable",
  "codex.sse_event",
  "codex.websocket_request",
  "list_tools_for_server",
  "persist_rollout_items",
]);

/**
 * `codex-app-server` (Codex 0.153.x) exports its whole internal `tracing` span
 * tree over OTLP. The 2026-09-12 Studio0 census measured 95,739 of 106,668
 * ingested events in one hour as `otel_span` rows from this one service across
 * 147 distinct names (`realtime_conversation.running_state` alone 24,808), none
 * of which carry a product signal. The service is the stable dimension, not the
 * name: a per-name deny list cannot keep up with an internal call graph that
 * changes every release. The retained-dimension checks above still run first,
 * so an app-server span with usage, an error/exception, an explicit action, a
 * tool name, or analytical linkage is admitted exactly as before. A span that
 * carries only a model and no usage stays zero-value under this rule — a model
 * name alone joins to no session, actor, cost, or outcome.
 */
const APP_SERVER_SERVICE_NAME = "codex-app-server";

function serviceName(event: AiInteractionEvent) {
  const value = (event.metadata as Record<string, unknown>).serviceName;
  return typeof value === "string" ? value.trim().toLowerCase() : undefined;
}

function canonicalSpanName(event: AiInteractionEvent) {
  const value = (event.metadata as Record<string, unknown>).otelEventName;
  return typeof value === "string" ? value.trim().toLowerCase() : undefined;
}

function hasUsage(event: AiInteractionEvent) {
  return (
    event.inputTokens !== undefined ||
    event.outputTokens !== undefined ||
    event.cacheReadTokens !== undefined ||
    event.cacheCreationTokens !== undefined ||
    event.costUsd !== undefined
  );
}

function hasAnalyticalLinkage(event: AiInteractionEvent) {
  const metadata = event.metadata as Record<string, unknown>;
  return Boolean(
    event.sessionId ||
      event.actorId ||
      event.projectKey ||
      event.customerKey ||
      event.workflowKey ||
      metadata.git ||
      metadata.request_id ||
      metadata.call_id ||
      metadata["gen_ai.response.id"],
  );
}

/**
 * Admission runs on the normalized event, which is constructed only from the
 * privacy-sanitized OTLP record. This predicate is intentionally conservative:
 * a span is discarded only when it carries no retained dimension AND it is
 * either a known Codex wrapper name or a `codex-app-server` internal span.
 * Every other service and source still fails open.
 */
export function decideOtlpSpanAdmission(event: AiInteractionEvent): OtlpAdmissionDecision {
  if (event.eventType !== "otel_span") return { admitted: true };
  if (hasUsage(event)) return { admitted: true };

  const metadata = event.metadata as Record<string, unknown>;
  if (
    metadata.otelHasError === true ||
    metadata.otelHasException === true ||
    metadata.otelExplicitAction === true ||
    metadata.toolName ||
    hasAnalyticalLinkage(event)
  ) {
    return { admitted: true };
  }

  if (event.source === "codex" && serviceName(event) === APP_SERVER_SERVICE_NAME) {
    return { admitted: false, reason: "app_server_internal_span" };
  }

  const name = canonicalSpanName(event);
  if (event.source === "codex" && name && KNOWN_GENERIC_SPAN_NAMES.has(name)) {
    return { admitted: false, reason: "generic_zero_value_span" };
  }

  return { admitted: true };
}

export function addOtlpAdmissionDrop(
  drops: OtlpAdmissionDrop[],
  source: ToolSource,
  reason: OtlpDropReason,
) {
  const existing = drops.find((drop) => drop.source === source && drop.reason === reason);
  if (existing) {
    existing.count += 1;
  } else {
    drops.push({ source, reason, count: 1 });
  }
}
