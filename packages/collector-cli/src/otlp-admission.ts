import type { AiInteractionEvent, ToolSource } from "../../shared/src/index";

export const OTLP_DROP_REASONS = ["generic_zero_value_span"] as const;
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
  "app_server_serialized_request_queue",
  "codex_websocket_event",
  "thread_resume",
  "thread_read",
  "thread_list",
  "thread_goal_get",
  "handle_responses",
  "receiving",
  "resume_running_thread",
  "auth",
  "append_items",
  "remotecontrol_enable",
  "codex_sse_event",
  "codex_websocket_request",
  "list_tools_for_server",
  "persist_rollout_items",
]);

function normalizedSpanName(event: AiInteractionEvent) {
  const value = (event.metadata as Record<string, unknown>).otelEventName;
  return typeof value === "string"
    ? value.trim().replace(/[^a-zA-Z0-9]+/g, "_").replace(/^_+|_+$/g, "").toLowerCase()
    : undefined;
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
 * only a known wrapper span with no retained signal is discarded.
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

  const name = normalizedSpanName(event);
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
