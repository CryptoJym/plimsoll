import crypto from "node:crypto";

import {
  DEFAULT_POLICY,
  aiInteractionEventSchema,
  sanitizeForPolicy,
  usageFieldKeys,
  type ActionClass,
  type AiInteractionEvent,
  type PolicyConfig,
  type ToolSource,
} from "../../shared/src/index";

type NormalizeOptions = {
  policy?: PolicyConfig;
  source?: ToolSource;
  gitContext?: import("../../shared/src/index").GitLinkageContext;
};

type OTelSignals = {
  attributes: Record<string, unknown>;
  names: string[];
  timestamps: string[];
};

export function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function stringField(record: Record<string, unknown>, keys: readonly string[]) {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }

  return undefined;
}

export function numberField(record: Record<string, unknown>, keys: readonly string[]) {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }

    if (typeof value === "string" && value.trim() && !Number.isNaN(Number(value))) {
      return Number(value);
    }
  }

  return undefined;
}

export function otelScalar(value: unknown): unknown {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return value;
  }

  const record = value as Record<string, unknown>;
  if ("stringValue" in record) return record.stringValue;
  if ("intValue" in record) return record.intValue;
  if ("doubleValue" in record) return record.doubleValue;
  if ("boolValue" in record) return record.boolValue;
  if ("bytesValue" in record) return record.bytesValue;
  if ("arrayValue" in record) return record.arrayValue;
  if ("kvlistValue" in record) return record.kvlistValue;
  return value;
}

function otelAttributeKey(value: Record<string, unknown>) {
  return typeof value.key === "string" && "value" in value ? value.key : undefined;
}

function collectOtelSignals(value: unknown, signals: OTelSignals) {
  if (!value || typeof value !== "object") return;

  if (Array.isArray(value)) {
    for (const item of value) {
      collectOtelSignals(item, signals);
    }
    return;
  }

  const record = value as Record<string, unknown>;
  const attrKey = otelAttributeKey(record);
  if (attrKey) {
    signals.attributes[attrKey] = otelScalar(record.value);
  }

  if (
    typeof record.name === "string" &&
    (
      Array.isArray(record.attributes) ||
      Array.isArray(record.events) ||
      "traceId" in record ||
      "spanId" in record ||
      "kind" in record ||
      "startTimeUnixNano" in record ||
      "timeUnixNano" in record
    )
  ) {
    signals.names.push(record.name);
  }

  for (const key of ["timeUnixNano", "observedTimeUnixNano", "startTimeUnixNano"]) {
    const valueAtKey = record[key];
    if (typeof valueAtKey === "string" || typeof valueAtKey === "number") {
      const timestamp = unixNanoToIso(valueAtKey);
      if (timestamp) signals.timestamps.push(timestamp);
    }
  }

  for (const nested of Object.values(record)) {
    collectOtelSignals(nested, signals);
  }
}

function extractOtelSignals(payload: Record<string, unknown>): OTelSignals {
  const signals: OTelSignals = {
    attributes: {},
    names: [],
    timestamps: [],
  };
  collectOtelSignals(payload, signals);
  return {
    attributes: signals.attributes,
    names: [...new Set(signals.names)],
    timestamps: [...new Set(signals.timestamps)],
  };
}

export function stringFromRecords(records: Record<string, unknown>[], keys: readonly string[]) {
  for (const record of records) {
    const value = stringField(record, keys);
    if (value) return value;
  }

  return undefined;
}

export function numberFromRecords(records: Record<string, unknown>[], keys: readonly string[]) {
  for (const record of records) {
    const value = numberField(record, keys);
    if (value !== undefined) return value;
  }

  return undefined;
}

export function isUuid(value: string | undefined) {
  return Boolean(
    value?.match(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    ),
  );
}

export function deterministicEventId(parts: Array<string | number | undefined>) {
  const digest = crypto
    .createHash("sha256")
    .update(parts.map((part) => String(part ?? "")).join("|"))
    .digest("hex");
  return [
    digest.slice(0, 8),
    digest.slice(8, 12),
    `5${digest.slice(13, 16)}`,
    `9${digest.slice(17, 20)}`,
    digest.slice(20, 32),
  ].join("-");
}

export function unixNanoToIso(value: string | number) {
  const milliseconds =
    typeof value === "string" && /^\d+$/.test(value)
      ? Number(BigInt(value) / BigInt(1_000_000))
      : Math.floor(Number(value) / 1_000_000);
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) return undefined;
  const date = new Date(milliseconds);
  if (Number.isNaN(date.getTime())) return undefined;
  return date.toISOString();
}

export function inferSource(payload: Record<string, unknown>, fallback?: ToolSource): ToolSource {
  const source = stringField(payload, ["source", "tool", "provider"]);
  if (source?.toLowerCase().includes("claude")) {
    return "claude_code";
  }

  if (source?.toLowerCase().includes("codex")) {
    return "codex";
  }

  return fallback ?? "unknown";
}

export { usageFieldKeys } from "../../shared/src/index";

const TOOL_NAME_KEYS = ["tool_name", "toolName", "tool", "name"];

const TOOL_CLASS_RULES: Array<{ pattern: RegExp; actionClass: ActionClass; detail?: string }> = [
  { pattern: /^mcp__claude-in-chrome__/i, actionClass: "browser" },
  { pattern: /^mcp__|^mcp[._]/i, actionClass: "mcp" },
  { pattern: /^(bash|shell|exec_command|local_shell|run_command|run_terminal_cmd|execute_command|container\.exec|unified_exec)$/i, actionClass: "shell" },
  { pattern: /^(write|create_file|write_file)$/i, actionClass: "write" },
  { pattern: /^(edit|multiedit|notebookedit|apply_patch|applypatch|str_replace_editor|str_replace|update_file)$/i, actionClass: "edit" },
  { pattern: /^(read|grep|glob|ls|list_dir|list_files|view|view_image|read_file|codebase_search|file_search|search|find|rg)$/i, actionClass: "read" },
  { pattern: /^(webfetch|websearch|web_search|web_fetch|fetch|open_url|browser.*)$/i, actionClass: "browser" },
  { pattern: /^(task|agent|dispatch_agent|delegate)$/i, actionClass: "other", detail: "delegate" },
  { pattern: /^(todowrite|todoread|exitplanmode|enterplanmode|update_plan|taskcreate|taskupdate)$/i, actionClass: "other", detail: "plan" },
];

export function deriveActionClass(toolName: string | undefined): {
  actionClass: ActionClass;
  detail?: string;
} {
  if (!toolName) return { actionClass: "other" };
  for (const rule of TOOL_CLASS_RULES) {
    if (rule.pattern.test(toolName)) {
      return { actionClass: rule.actionClass, detail: rule.detail };
    }
  }

  return { actionClass: "other" };
}

export function classifyEventType(candidate: string | undefined): AiInteractionEvent["eventType"] | undefined {
  const normalized = candidate?.replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
  if (!normalized) return undefined;
  if (normalized === "otelspan" || normalized === "span") return "otel_span";

  if (normalized.includes("userprompt")) return "user_prompt_submit";
  if (normalized.includes("stop") || normalized.includes("sessionstop")) return "session_stop";
  if (normalized.includes("sessionstart")) return "session_start";
  if (normalized.includes("toolresult")) return "tool_result";
  if (
    normalized.includes("tool") ||
    normalized.includes("pretool") ||
    normalized.includes("posttool")
  ) return "tool_use";
  if (normalized.includes("assistant")) return "assistant_response";
  if (
    normalized.includes("genai") ||
    normalized.includes("chat") ||
    normalized.includes("completion")
  ) return "otel_span";
  if (normalized.includes("otel")) return "otel_span";

  return undefined;
}

function inferEventType(
  payload: Record<string, unknown>,
  otelNames: string[] = [],
): AiInteractionEvent["eventType"] {
  const rawType = stringField(payload, ["eventType", "event_type", "hook_event_name", "type", "name"]);
  const otelName = stringField(payload, ["name", "span_name", "body"]);
  const rawClass = classifyEventType(rawType);
  if (rawClass && rawClass !== "otel_span") return rawClass;

  for (const candidate of [otelName, ...otelNames]) {
    const inferred = classifyEventType(candidate);
    if (inferred) return inferred;
  }

  if (rawClass) return rawClass;
  return "unknown";
}

export function normalizeHookPayload(
  payload: unknown,
  options: NormalizeOptions = {},
): {
  event: AiInteractionEvent;
  suppressedFields: string[];
} {
  const raw = asRecord(payload);
  const policy = options.policy ?? DEFAULT_POLICY;
  const sanitized = sanitizeForPolicy(raw, policy);
  const safe = asRecord(sanitized.value);
  const otelSignals = extractOtelSignals(safe);
  const otelAttributes = otelSignals.attributes;
  const sourceRecords = [safe, otelAttributes];
  const suppliedId = stringField(safe, ["id", "event_id"]);
  const eventId = isUuid(suppliedId) ? suppliedId : crypto.randomUUID();
  const eventType = inferEventType(safe, otelSignals.names);
  const toolName =
    eventType === "tool_use" || eventType === "tool_result"
      ? stringFromRecords(sourceRecords, TOOL_NAME_KEYS)
      : undefined;
  const explicitActionClass = stringFromRecords(sourceRecords, [
    "actionClass",
    "action_class",
    "plimsoll.action_class", "cfo_one.action_class",
  ]);
  const derived = explicitActionClass ? undefined : deriveActionClass(toolName);
  const metadata = {
    ...safe,
    otelAttributes,
    ...(otelSignals.names.length ? { otelSignalNames: otelSignals.names } : {}),
    ...(otelSignals.timestamps.length ? { otelSignalTimestamps: otelSignals.timestamps } : {}),
    ...(suppliedId && !isUuid(suppliedId) ? { external_event_id: suppliedId } : {}),
    ...(toolName ? { toolName } : {}),
    ...(derived?.detail ? { toolClassDetail: derived.detail } : {}),
    ...(options.gitContext ? { git: options.gitContext } : {}),
  };

  const event = aiInteractionEventSchema.parse({
    id: eventId,
    sessionId: stringFromRecords(sourceRecords, usageFieldKeys.sessionId),
    tenantId: stringFromRecords(sourceRecords, ["tenantId", "tenant_id"]) ?? policy.tenantId,
    actorId: stringFromRecords(sourceRecords, usageFieldKeys.actorId),
    source: inferSource(safe, options.source),
    dataMode: policy.dataMode,
    eventType,
    observedAt:
      stringField(safe, ["observedAt", "observed_at", "timestamp", "time"]) ??
      otelSignals.timestamps[0] ??
      new Date().toISOString(),
    model: stringFromRecords(sourceRecords, usageFieldKeys.model),
    projectKey: stringFromRecords(sourceRecords, ["projectKey", "project_key", "project", "plimsoll.project", "cfo_one.project"]),
    customerKey: stringFromRecords(sourceRecords, ["customerKey", "customer_key", "customer", "plimsoll.customer", "cfo_one.customer"]),
    workflowKey: stringFromRecords(sourceRecords, ["workflowKey", "workflow_key", "workflow", "plimsoll.workflow", "cfo_one.workflow"]),
    actionClass: explicitActionClass ?? derived?.actionClass ?? "other",
    inputTokens: numberFromRecords(sourceRecords, usageFieldKeys.inputTokens),
    outputTokens: numberFromRecords(sourceRecords, usageFieldKeys.outputTokens),
    cacheReadTokens: numberFromRecords(sourceRecords, usageFieldKeys.cacheReadTokens),
    cacheCreationTokens: numberFromRecords(sourceRecords, usageFieldKeys.cacheCreationTokens),
    costUsd: numberFromRecords(sourceRecords, usageFieldKeys.costUsd),
    metadata,
  });

  return {
    event,
    suppressedFields: sanitized.evaluation.suppressedFields,
  };
}
