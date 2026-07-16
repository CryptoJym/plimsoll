import {
  DEFAULT_POLICY,
  GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT,
  aiInteractionEventSchema,
  canonicalizeSuppressionReceipts,
  estimateCostUsd,
  isApprovedAnalyticalScalarAttribute,
  isDispositionAllowedOnOtlpSurface,
  metadataKeyDisposition,
  safeMetadataStringAttribute,
  sanitizeForPolicy,
  suppressionReceiptForAttributeKey,
  usageFieldKeys,
  type ActionClass,
  type AiInteractionEvent,
  type OtlpAttributeSurface,
  type PolicyConfig,
  type ToolSource,
} from "../../shared/src/index";

import { resolveGitContext } from "./git-context";
import {
  asRecord,
  classifyEventType,
  deriveActionClass,
  deterministicEventId,
  numberField,
  otelScalar,
  stringField,
  unixNanoToIso,
} from "./normalizer";
import {
  addOtlpAdmissionDrop,
  decideOtlpSpanAdmission,
  type OtlpAdmissionDrop,
} from "./otlp-admission";

export type MetricSample = {
  id: string;
  source: ToolSource;
  metricName: string;
  observedAt: string;
  sessionId?: string;
  model?: string;
  sampleType?: string;
  value: number;
  attrs: Record<string, unknown>;
  suppressedFields: string[];
};

export type ExplodedOtlp = {
  events: Array<{ event: AiInteractionEvent; suppressedFields: string[] }>;
  metricSamples: MetricSample[];
  admissionDrops: OtlpAdmissionDrop[];
  droppedEventCount: number;
  recordCount: number;
  datapointCount: number;
  parseFailures: number;
};

type ExplodeOptions = {
  policy?: PolicyConfig;
  source?: ToolSource;
  transportPath?: string;
  /**
   * Resolve git linkage keys from working directories found in raw record
   * attributes (before sanitization removes them). Must be disabled for
   * archive backfills: replaying old workdirs against current git state
   * would attribute today's HEAD to historical sessions.
   */
  resolveGit?: boolean;
  /** Receives (repoHash, label) for local-only repo_labels recording. */
  onRepoLabel?: (repoHash: string, label: string) => void;
};

function flattenOtelAttributes(attributes: unknown): Record<string, unknown> {
  const flat: Record<string, unknown> = {};
  if (!Array.isArray(attributes)) return flat;
  for (const attribute of attributes) {
    const record = asRecord(attribute);
    if (typeof record.key === "string" && "value" in record) {
      flat[record.key] = otelScalar(record.value);
    }
  }

  return flat;
}

function resourceSummary(resource: unknown, dataMode: PolicyConfig["dataMode"]) {
  const attrs = flattenOtelAttributes(asRecord(resource).attributes);
  const admitted = metadataSafeOtlpAttributes(attrs, dataMode, "resource");
  return {
    serviceName: admitted.attrs["service.name"] as string | undefined,
    serviceVersion: admitted.attrs["service.version"] as string | undefined,
    suppressedFields: admitted.suppressedFields,
  };
}

function scopeSuppressedFields(scope: unknown, dataMode: PolicyConfig["dataMode"]) {
  const attrs = flattenOtelAttributes(asRecord(scope).attributes);
  return metadataSafeOtlpAttributes(attrs, dataMode, "scope").suppressedFields;
}

function recordTimestamp(record: Record<string, unknown>, attrs: Record<string, unknown>) {
  for (const key of ["timeUnixNano", "observedTimeUnixNano", "startTimeUnixNano"]) {
    const value = record[key];
    if (typeof value === "string" || typeof value === "number") {
      const iso = unixNanoToIso(value);
      if (iso) return iso;
    }
  }

  const attrTimestamp = stringField(attrs, ["event.timestamp", "timestamp"]);
  if (attrTimestamp && !Number.isNaN(Date.parse(attrTimestamp))) {
    return new Date(attrTimestamp).toISOString();
  }

  return new Date().toISOString();
}

function intTokens(value: number | undefined) {
  if (value === undefined || !Number.isFinite(value) || value < 0) return undefined;
  return Math.round(value);
}

function eventSourceFor(attrs: Record<string, unknown>, fallback: ToolSource | undefined, serviceName: string | undefined): ToolSource {
  const service = (serviceName ?? "").toLowerCase();
  if (service.includes("claude")) return "claude_code";
  if (service.includes("codex")) return "codex";
  return fallback ?? "unknown";
}

function workdirFromRawRecord(record: Record<string, unknown>): string | undefined {
  const rawAttrs = flattenOtelAttributes(record.attributes);
  const direct = stringField(rawAttrs, ["cwd", "workdir", "working_directory"]);
  if (direct) return direct;

  const argumentsBlob = rawAttrs.arguments;
  if (typeof argumentsBlob === "string" && argumentsBlob.includes("workdir")) {
    try {
      const parsed = asRecord(JSON.parse(argumentsBlob));
      return stringField(parsed, ["workdir", "cwd", "working_directory"]);
    } catch {
      return undefined;
    }
  }

  return undefined;
}

function looksLikePathOrUrl(value: string) {
  const candidate = value.trim();
  return (
    /^(?:\/|~\/|file:\/\/|https?:\/\/|[a-zA-Z]:[\\/])/.test(candidate) ||
    candidate.includes("\\")
  );
}

function boundedSignalName(value: unknown) {
  if (typeof value !== "string") return undefined;
  const candidate = value.trim();
  return /^[a-zA-Z0-9_./:+-]{1,160}$/.test(candidate) && !looksLikePathOrUrl(candidate)
    ? candidate
    : undefined;
}

export function metadataSafeOtlpAttributes(
  attrs: Record<string, unknown>,
  _dataMode: PolicyConfig["dataMode"],
  surface: OtlpAttributeSurface = "record",
) {
  const safe: Record<string, unknown> = {};
  const suppressedFields: string[] = [];
  for (const [key, value] of Object.entries(attrs)) {
    const disposition = metadataKeyDisposition(key);
    const allowed = disposition && isDispositionAllowedOnOtlpSurface(disposition, surface);
    if (
      allowed &&
      disposition.valueKind === "analytical_scalar" &&
      isApprovedAnalyticalScalarAttribute(key, value)
    ) {
      safe[key] = value;
    } else if (allowed && disposition.valueKind === "string") {
      const stringValue = safeMetadataStringAttribute(key, value);
      if (stringValue !== null) {
        safe[key] = stringValue;
      } else {
        suppressedFields.push(suppressionReceiptForAttributeKey(key));
      }
    } else {
      suppressedFields.push(
        typeof value === "number" || typeof value === "boolean"
          ? GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT
          : suppressionReceiptForAttributeKey(key),
      );
    }
  }
  return { attrs: safe, suppressedFields: canonicalizeSuppressionReceipts(suppressedFields) };
}

function eventNameFromAttributes(attrs: Record<string, unknown>) {
  return boundedSignalName(stringField(attrs, ["event.name"]));
}

const ACTION_CLASSES = new Set<ActionClass>([
  "continue",
  "validate",
  "test",
  "edit",
  "read",
  "write",
  "shell",
  "mcp",
  "browser",
  "review",
  "other",
]);

function explicitActionFrom(attrs: Record<string, unknown>) {
  const raw = stringField(attrs, [
    "plimsoll.action_class",
    "cfo_one.action_class",
    "action_class",
  ]);
  if (!raw) return { present: false as const };
  const normalized = raw.toLowerCase();
  if (ACTION_CLASSES.has(normalized as ActionClass)) {
    return { present: true as const, actionClass: normalized as ActionClass };
  }
  return {
    present: true as const,
    actionClass: "other" as const,
    originalActionClass: boundedSignalName(raw),
  };
}

function statusCode(record: Record<string, unknown>) {
  const value = asRecord(record.status).code;
  return typeof value === "number" || typeof value === "string" ? value : undefined;
}

function isErrorStatus(value: string | number | undefined) {
  if (value === 2) return true;
  return typeof value === "string" && (value === "2" || value.toUpperCase().includes("ERROR"));
}

function spanHasExceptionEvent(span: Record<string, unknown>) {
  if (!Array.isArray(span.events)) return false;
  return span.events.some((candidate) => {
    const name = boundedSignalName(asRecord(candidate).name);
    return name?.toLowerCase().includes("exception") ?? false;
  });
}

function attributesHaveError(attrs: Record<string, unknown>) {
  return Boolean(
    stringField(attrs, ["error.type", "exception.type"]) ||
      attrs.error === true ||
      attrs.failed === true ||
      attrs.success === false,
  );
}

function buildLogEvent(
  record: Record<string, unknown>,
  context: {
    policy: PolicyConfig;
    source: ToolSource;
    transportPath?: string;
    serviceName?: string;
    serviceVersion?: string;
    containerSuppressedFields?: string[];
    resolveGit?: boolean;
    onRepoLabel?: (repoHash: string, label: string) => void;
  },
): { event: AiInteractionEvent; suppressedFields: string[] } {
  const resolvedGit = context.resolveGit
    ? resolveGitContext(workdirFromRawRecord(record))
    : undefined;
  if (resolvedGit?.remoteUrlHash && resolvedGit.remoteLabel) {
    context.onRepoLabel?.(resolvedGit.remoteUrlHash, resolvedGit.remoteLabel);
  }
  const gitContext = resolvedGit
    ? (({ remoteLabel: _l, ...rest }) => rest)(resolvedGit)
    : undefined;
  const sanitized = sanitizeForPolicy(record, context.policy);
  const safeRecord = asRecord(sanitized.value);
  const attrs = flattenOtelAttributes(safeRecord.attributes);
  const metadataAttrs = metadataSafeOtlpAttributes(attrs, context.policy.dataMode);
  const otelEventName = eventNameFromAttributes(attrs);

  const inputTokens = intTokens(numberField(attrs, [...usageFieldKeys.inputTokens]));
  const outputTokens = intTokens(numberField(attrs, [...usageFieldKeys.outputTokens]));
  const cacheReadTokens = intTokens(numberField(attrs, [...usageFieldKeys.cacheReadTokens]));
  const cacheCreationTokens = intTokens(numberField(attrs, [...usageFieldKeys.cacheCreationTokens]));
  let costUsd = numberField(attrs, [...usageFieldKeys.costUsd]);
  let costEstimated = false;
  if (costUsd === undefined) {
    const estimate = estimateCostUsd({
      model: stringField(attrs, [...usageFieldKeys.model]),
      inputTokens,
      outputTokens,
      cacheReadTokens,
      cacheCreationTokens,
    });
    if (estimate) {
      costUsd = estimate.costUsd;
      costEstimated = true;
    }
  }
  const hasUsage =
    inputTokens !== undefined ||
    outputTokens !== undefined ||
    cacheReadTokens !== undefined ||
    cacheCreationTokens !== undefined ||
    costUsd !== undefined;

  const toolName = boundedSignalName(stringField(attrs, ["tool_name", "toolName", "tool"]));
  const explicitAction = explicitActionFrom(attrs);
  const derived = explicitAction.present ? undefined : deriveActionClass(toolName);
  const mcpServer = stringField(attrs, ["mcp_server"]);
  const actionClass =
    explicitAction.present
      ? explicitAction.actionClass
      : (derived?.actionClass === "other" && mcpServer ? "mcp" : derived?.actionClass) ??
        "other";

  const eventType = hasUsage
    ? "assistant_response"
    : classifyEventType(otelEventName) ?? "otel_span";

  const observedAt = recordTimestamp(safeRecord, attrs);
  const sessionId = stringField(attrs, [...usageFieldKeys.sessionId]);

  const event = aiInteractionEventSchema.parse({
    actorId: stringField(attrs, [...usageFieldKeys.actorId]),
    id: deterministicEventId([
      context.source,
      otelEventName,
      sessionId,
      observedAt,
      stringField(attrs, ["request_id", "call_id"]),
      numberField(attrs, ["event.sequence"]),
      String(safeRecord.timeUnixNano ?? safeRecord.observedTimeUnixNano ?? ""),
      JSON.stringify(attrs),
    ]),
    sessionId,
    tenantId: context.policy.tenantId,
    source: eventSourceFor(attrs, context.source, context.serviceName),
    dataMode: context.policy.dataMode,
    eventType,
    observedAt,
    model: stringField(attrs, [...usageFieldKeys.model]),
    projectKey: stringField(attrs, ["plimsoll.project", "cfo_one.project", "project_key", "project"]),
    customerKey: stringField(attrs, ["plimsoll.customer", "cfo_one.customer", "customer_key", "customer"]),
    workflowKey: stringField(attrs, ["plimsoll.workflow", "cfo_one.workflow", "workflow_key", "workflow"]),
    actionClass,
    inputTokens,
    outputTokens,
    cacheReadTokens,
    cacheCreationTokens,
    costUsd: costUsd !== undefined && costUsd >= 0 ? costUsd : undefined,
    metadata: {
      ...metadataAttrs.attrs,
      ...(otelEventName ? { otelEventName } : {}),
      ...(toolName ? { toolName } : {}),
      ...(derived?.detail ? { toolClassDetail: derived.detail } : {}),
      ...(explicitAction.present ? { otelExplicitAction: true } : {}),
      ...(explicitAction.present && explicitAction.originalActionClass
        ? { otelOriginalActionClass: explicitAction.originalActionClass }
        : {}),
      ...(context.transportPath ? { transport_path: context.transportPath } : {}),
      ...(context.serviceName ? { serviceName: context.serviceName } : {}),
      ...(context.serviceVersion ? { serviceVersion: context.serviceVersion } : {}),
      ...(gitContext ? { git: gitContext } : {}),
      ...(costEstimated ? { costEstimated: true } : {}),
    },
  });

  return {
    event,
    suppressedFields: canonicalizeSuppressionReceipts([
      ...sanitized.evaluation.suppressedFields,
      ...(context.containerSuppressedFields ?? []),
      ...metadataAttrs.suppressedFields,
      ...(context.policy.dataMode !== "evidence" && "body" in safeRecord ? ["body"] : []),
    ]),
  };
}

function buildSpanEvent(
  span: Record<string, unknown>,
  context: {
    policy: PolicyConfig;
    source: ToolSource;
    transportPath?: string;
    serviceName?: string;
    serviceVersion?: string;
    containerSuppressedFields?: string[];
  },
): { event: AiInteractionEvent; suppressedFields: string[] } {
  const sanitized = sanitizeForPolicy(span, context.policy);
  const safeSpan = asRecord(sanitized.value);
  const attrs = flattenOtelAttributes(safeSpan.attributes);
  const metadataAttrs = metadataSafeOtlpAttributes(attrs, context.policy.dataMode);
  const rawSpanName = typeof safeSpan.name === "string" ? safeSpan.name : undefined;
  const spanName = boundedSignalName(rawSpanName);
  const observedAt = recordTimestamp(safeSpan, attrs);
  const sessionId = stringField(attrs, [...usageFieldKeys.sessionId]);
  const inputTokens = intTokens(numberField(attrs, [...usageFieldKeys.inputTokens]));
  const outputTokens = intTokens(numberField(attrs, [...usageFieldKeys.outputTokens]));
  const cacheReadTokensSpan = intTokens(numberField(attrs, [...usageFieldKeys.cacheReadTokens]));
  const cacheCreationTokensSpan = intTokens(numberField(attrs, [...usageFieldKeys.cacheCreationTokens]));
  let costUsd = numberField(attrs, [...usageFieldKeys.costUsd]);
  let costEstimated = false;
  if (costUsd === undefined) {
    const estimate = estimateCostUsd({
      model: stringField(attrs, [...usageFieldKeys.model]),
      inputTokens,
      outputTokens,
      cacheReadTokens: cacheReadTokensSpan,
      cacheCreationTokens: cacheCreationTokensSpan,
    });
    if (estimate) {
      costUsd = estimate.costUsd;
      costEstimated = true;
    }
  }

  const hasUsage =
    inputTokens !== undefined ||
    outputTokens !== undefined ||
    cacheReadTokensSpan !== undefined ||
    cacheCreationTokensSpan !== undefined ||
    costUsd !== undefined;
  const toolName = boundedSignalName(
    stringField(attrs, ["tool_name", "toolName", "tool", "gen_ai.tool.name"]),
  );
  const explicitAction = explicitActionFrom(attrs);
  const derived = explicitAction.present ? undefined : deriveActionClass(toolName);
  const classifiedType = classifyEventType(spanName);
  const lifecycleType =
    classifiedType === "session_start" ||
    classifiedType === "session_stop" ||
    classifiedType === "user_prompt_submit"
      ? classifiedType
      : undefined;
  const eventType = hasUsage
    ? "assistant_response"
    : toolName || explicitAction.present
      ? classifiedType === "tool_result"
        ? "tool_result"
        : "tool_use"
      : lifecycleType
        ? lifecycleType
        : "otel_span";
  const otelStatusCode = statusCode(safeSpan);
  const otelHasException = spanHasExceptionEvent(safeSpan);
  const otelHasError =
    isErrorStatus(otelStatusCode) || otelHasException || attributesHaveError(attrs);

  const event = aiInteractionEventSchema.parse({
    actorId: stringField(attrs, [...usageFieldKeys.actorId]),
    id: deterministicEventId([
      context.source,
      "span",
      spanName,
      sessionId,
      observedAt,
      String(safeSpan.spanId ?? ""),
      String(safeSpan.traceId ?? ""),
    ]),
    sessionId,
    tenantId: context.policy.tenantId,
    source: eventSourceFor(attrs, context.source, context.serviceName),
    dataMode: context.policy.dataMode,
    eventType,
    observedAt,
    model: stringField(attrs, [...usageFieldKeys.model]),
    projectKey: stringField(attrs, ["plimsoll.project", "cfo_one.project", "project_key", "project"]),
    customerKey: stringField(attrs, ["plimsoll.customer", "cfo_one.customer", "customer_key", "customer"]),
    workflowKey: stringField(attrs, ["plimsoll.workflow", "cfo_one.workflow", "workflow_key", "workflow"]),
    actionClass: explicitAction.present ? explicitAction.actionClass : derived?.actionClass ?? "other",
    inputTokens,
    outputTokens,
    cacheReadTokens: cacheReadTokensSpan,
    cacheCreationTokens: cacheCreationTokensSpan,
    costUsd: costUsd !== undefined && costUsd >= 0 ? costUsd : undefined,
    metadata: {
      ...metadataAttrs.attrs,
      ...(costEstimated ? { costEstimated: true } : {}),
      ...(spanName ? { otelEventName: spanName } : {}),
      ...(toolName ? { toolName } : {}),
      ...(derived?.detail ? { toolClassDetail: derived.detail } : {}),
      ...(explicitAction.present ? { otelExplicitAction: true } : {}),
      ...(explicitAction.present && explicitAction.originalActionClass
        ? { otelOriginalActionClass: explicitAction.originalActionClass }
        : {}),
      ...(otelStatusCode !== undefined ? { otelStatusCode } : {}),
      ...(otelHasException ? { otelHasException: true } : {}),
      ...(otelHasError ? { otelHasError: true } : {}),
      ...(typeof safeSpan.traceId === "string" ? { traceId: safeSpan.traceId } : {}),
      ...(typeof safeSpan.spanId === "string" ? { spanId: safeSpan.spanId } : {}),
      ...(context.transportPath ? { transport_path: context.transportPath } : {}),
      ...(context.serviceName ? { serviceName: context.serviceName } : {}),
    },
  });

  return {
    event,
    suppressedFields: canonicalizeSuppressionReceipts([
      ...sanitized.evaluation.suppressedFields,
      ...(context.containerSuppressedFields ?? []),
      ...metadataAttrs.suppressedFields,
      ...(rawSpanName && !spanName ? ["span.name"] : []),
    ]),
  };
}

function buildMetricSamples(
  metric: Record<string, unknown>,
  context: {
    policy: PolicyConfig;
    source: ToolSource;
    serviceName?: string;
    containerSuppressedFields?: string[];
  },
): MetricSample[] {
  const metricName = boundedSignalName(metric.name) ?? "unknown_metric";
  const samples: MetricSample[] = [];
  const shapes: Array<{ kind: string; dataPoints: unknown }> = [
    { kind: "sum", dataPoints: asRecord(metric.sum).dataPoints },
    { kind: "gauge", dataPoints: asRecord(metric.gauge).dataPoints },
    { kind: "histogram", dataPoints: asRecord(metric.histogram).dataPoints },
  ];

  for (const shape of shapes) {
    if (!Array.isArray(shape.dataPoints)) continue;
    for (const dataPointRaw of shape.dataPoints) {
      const sanitized = sanitizeForPolicy(dataPointRaw, context.policy);
      const dataPoint = asRecord(sanitized.value);
      const attrs = flattenOtelAttributes(dataPoint.attributes);
      const metadataAttrs = metadataSafeOtlpAttributes(attrs, context.policy.dataMode);
      const value =
        shape.kind === "histogram"
          ? numberField(dataPoint, ["sum", "count"])
          : numberField(dataPoint, ["asDouble", "asInt"]);
      if (value === undefined) continue;
      const observedAt = recordTimestamp(dataPoint, attrs);
      const sessionId = stringField(attrs, [...usageFieldKeys.sessionId]);
      const model = stringField(attrs, [...usageFieldKeys.model]);
      const sampleType =
        stringField(attrs, ["type"]) ?? (shape.kind === "histogram" ? "histogram_sum" : undefined);
      samples.push({
        id: deterministicEventId([
          context.source,
          metricName,
          sessionId,
          observedAt,
          sampleType,
          model,
          String(dataPoint.timeUnixNano ?? ""),
          JSON.stringify(attrs),
        ]),
        source: eventSourceFor(attrs, context.source, context.serviceName),
        metricName,
        observedAt,
        sessionId,
        model,
        sampleType,
        value,
        attrs: metadataAttrs.attrs,
        suppressedFields: canonicalizeSuppressionReceipts([
          ...sanitized.evaluation.suppressedFields,
          ...(context.containerSuppressedFields ?? []),
          ...metadataAttrs.suppressedFields,
        ]),
      });
    }
  }

  return samples;
}

export function explodeOtlpPayload(
  payload: unknown,
  options: ExplodeOptions = {},
): ExplodedOtlp {
  const policy = options.policy ?? DEFAULT_POLICY;
  const source = options.source ?? "unknown";
  const root = asRecord(payload);
  const result: ExplodedOtlp = {
    events: [],
    metricSamples: [],
    admissionDrops: [],
    droppedEventCount: 0,
    recordCount: 0,
    datapointCount: 0,
    parseFailures: 0,
  };

  for (const resourceLog of Array.isArray(root.resourceLogs) ? root.resourceLogs : []) {
    const resource = resourceSummary(asRecord(resourceLog).resource, policy.dataMode);
    for (const scopeLog of Array.isArray(asRecord(resourceLog).scopeLogs)
      ? (asRecord(resourceLog).scopeLogs as unknown[])
      : []) {
      const scopeReceipts = scopeSuppressedFields(asRecord(scopeLog).scope, policy.dataMode);
      for (const record of Array.isArray(asRecord(scopeLog).logRecords)
        ? (asRecord(scopeLog).logRecords as unknown[])
        : []) {
        result.recordCount += 1;
        try {
          result.events.push(
            buildLogEvent(asRecord(record), {
              policy,
              source,
              transportPath: options.transportPath,
              serviceName: resource.serviceName,
              serviceVersion: resource.serviceVersion,
              containerSuppressedFields: [
                ...resource.suppressedFields,
                ...scopeReceipts,
              ],
              resolveGit: options.resolveGit ?? true,
              onRepoLabel: options.onRepoLabel,
            }),
          );
        } catch {
          result.parseFailures += 1;
        }
      }
    }
  }

  for (const resourceSpan of Array.isArray(root.resourceSpans) ? root.resourceSpans : []) {
    const resource = resourceSummary(asRecord(resourceSpan).resource, policy.dataMode);
    for (const scopeSpan of Array.isArray(asRecord(resourceSpan).scopeSpans)
      ? (asRecord(resourceSpan).scopeSpans as unknown[])
      : []) {
      const scopeReceipts = scopeSuppressedFields(asRecord(scopeSpan).scope, policy.dataMode);
      for (const span of Array.isArray(asRecord(scopeSpan).spans)
        ? (asRecord(scopeSpan).spans as unknown[])
        : []) {
        result.recordCount += 1;
        try {
          const entry = buildSpanEvent(asRecord(span), {
              policy,
              source,
              transportPath: options.transportPath,
              serviceName: resource.serviceName,
              serviceVersion: resource.serviceVersion,
              containerSuppressedFields: [
                ...resource.suppressedFields,
                ...scopeReceipts,
              ],
            });
          const decision = decideOtlpSpanAdmission(entry.event);
          if (decision.admitted) {
            result.events.push(entry);
          } else {
            result.droppedEventCount += 1;
            addOtlpAdmissionDrop(result.admissionDrops, entry.event.source, decision.reason);
          }
        } catch {
          result.parseFailures += 1;
        }
      }
    }
  }

  for (const resourceMetric of Array.isArray(root.resourceMetrics) ? root.resourceMetrics : []) {
    const resource = resourceSummary(asRecord(resourceMetric).resource, policy.dataMode);
    for (const scopeMetric of Array.isArray(asRecord(resourceMetric).scopeMetrics)
      ? (asRecord(resourceMetric).scopeMetrics as unknown[])
      : []) {
      const scopeReceipts = scopeSuppressedFields(asRecord(scopeMetric).scope, policy.dataMode);
      for (const metric of Array.isArray(asRecord(scopeMetric).metrics)
        ? (asRecord(scopeMetric).metrics as unknown[])
        : []) {
        const samples = buildMetricSamples(asRecord(metric), {
          policy,
          source,
          serviceName: resource.serviceName,
          containerSuppressedFields: [...resource.suppressedFields, ...scopeReceipts],
        });
        result.datapointCount += samples.length;
        result.metricSamples.push(...samples);
      }
    }
  }

  return result;
}
