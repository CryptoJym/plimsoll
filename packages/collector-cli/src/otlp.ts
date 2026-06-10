import {
  DEFAULT_POLICY,
  aiInteractionEventSchema,
  estimateCostUsd,
  sanitizeForPolicy,
  type AiInteractionEvent,
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
  usageFieldKeys,
} from "./normalizer";

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
};

export type ExplodedOtlp = {
  events: Array<{ event: AiInteractionEvent; suppressedFields: string[] }>;
  metricSamples: MetricSample[];
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

function resourceSummary(resource: unknown) {
  const attrs = flattenOtelAttributes(asRecord(resource).attributes);
  return {
    serviceName: typeof attrs["service.name"] === "string" ? attrs["service.name"] : undefined,
    serviceVersion:
      typeof attrs["service.version"] === "string" ? attrs["service.version"] : undefined,
  };
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

function buildLogEvent(
  record: Record<string, unknown>,
  context: {
    policy: PolicyConfig;
    source: ToolSource;
    transportPath?: string;
    serviceName?: string;
    serviceVersion?: string;
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
  const body = otelScalar(asRecord(safeRecord.body));
  const bodyText = typeof body === "string" ? body : undefined;
  const otelEventName = stringField(attrs, ["event.name"]) ?? bodyText;

  const inputTokens = intTokens(numberField(attrs, [...usageFieldKeys.inputTokens]));
  const outputTokens = intTokens(numberField(attrs, [...usageFieldKeys.outputTokens]));
  const cacheReadTokens = intTokens(numberField(attrs, [...usageFieldKeys.cacheReadTokens]));
  let costUsd = numberField(attrs, [...usageFieldKeys.costUsd]);
  let costEstimated = false;
  if (costUsd === undefined) {
    const estimate = estimateCostUsd({
      model: stringField(attrs, [...usageFieldKeys.model]),
      inputTokens,
      outputTokens,
      cacheReadTokens,
    });
    if (estimate) {
      costUsd = estimate.costUsd;
      costEstimated = true;
    }
  }
  const hasUsage =
    inputTokens !== undefined || outputTokens !== undefined || costUsd !== undefined;

  const toolName = stringField(attrs, ["tool_name", "toolName", "tool"]);
  const explicitActionClass = stringField(attrs, ["plimsoll.action_class", "cfo_one.action_class", "action_class"]);
  const derived = explicitActionClass ? undefined : deriveActionClass(toolName);
  const mcpServer = stringField(attrs, ["mcp_server"]);
  const actionClass =
    explicitActionClass ??
    (derived?.actionClass === "other" && mcpServer ? "mcp" : derived?.actionClass) ??
    "other";

  const eventType = hasUsage
    ? "assistant_response"
    : classifyEventType(otelEventName) ?? "otel_span";

  const observedAt = recordTimestamp(safeRecord, attrs);
  const sessionId = stringField(attrs, [...usageFieldKeys.sessionId]);

  const event = aiInteractionEventSchema.parse({
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
    costUsd: costUsd !== undefined && costUsd >= 0 ? costUsd : undefined,
    metadata: {
      ...attrs,
      ...(otelEventName ? { otelEventName } : {}),
      ...(toolName ? { toolName } : {}),
      ...(derived?.detail ? { toolClassDetail: derived.detail } : {}),
      ...(context.transportPath ? { transport_path: context.transportPath } : {}),
      ...(context.serviceName ? { serviceName: context.serviceName } : {}),
      ...(context.serviceVersion ? { serviceVersion: context.serviceVersion } : {}),
      ...(gitContext ? { git: gitContext } : {}),
      ...(costEstimated ? { costEstimated: true } : {}),
    },
  });

  return { event, suppressedFields: sanitized.evaluation.suppressedFields };
}

function buildSpanEvent(
  span: Record<string, unknown>,
  context: {
    policy: PolicyConfig;
    source: ToolSource;
    transportPath?: string;
    serviceName?: string;
    serviceVersion?: string;
  },
): { event: AiInteractionEvent; suppressedFields: string[] } {
  const sanitized = sanitizeForPolicy(span, context.policy);
  const safeSpan = asRecord(sanitized.value);
  const attrs = flattenOtelAttributes(safeSpan.attributes);
  const spanName = typeof safeSpan.name === "string" ? safeSpan.name : undefined;
  const observedAt = recordTimestamp(safeSpan, attrs);
  const sessionId = stringField(attrs, [...usageFieldKeys.sessionId]);
  const inputTokens = intTokens(numberField(attrs, [...usageFieldKeys.inputTokens]));
  const outputTokens = intTokens(numberField(attrs, [...usageFieldKeys.outputTokens]));
  const cacheReadTokensSpan = intTokens(numberField(attrs, [...usageFieldKeys.cacheReadTokens]));
  let costUsd = numberField(attrs, [...usageFieldKeys.costUsd]);
  let costEstimated = false;
  if (costUsd === undefined) {
    const estimate = estimateCostUsd({
      model: stringField(attrs, [...usageFieldKeys.model]),
      inputTokens,
      outputTokens,
      cacheReadTokens: cacheReadTokensSpan,
    });
    if (estimate) {
      costUsd = estimate.costUsd;
      costEstimated = true;
    }
  }

  const event = aiInteractionEventSchema.parse({
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
    eventType: inputTokens !== undefined || outputTokens !== undefined || costUsd !== undefined
      ? "assistant_response"
      : "otel_span",
    observedAt,
    model: stringField(attrs, [...usageFieldKeys.model]),
    actionClass: "other",
    inputTokens,
    outputTokens,
    cacheReadTokens: cacheReadTokensSpan,
    costUsd: costUsd !== undefined && costUsd >= 0 ? costUsd : undefined,
    metadata: {
      ...attrs,
      ...(costEstimated ? { costEstimated: true } : {}),
      ...(spanName ? { otelEventName: spanName } : {}),
      ...(typeof safeSpan.traceId === "string" ? { traceId: safeSpan.traceId } : {}),
      ...(typeof safeSpan.spanId === "string" ? { spanId: safeSpan.spanId } : {}),
      ...(context.transportPath ? { transport_path: context.transportPath } : {}),
      ...(context.serviceName ? { serviceName: context.serviceName } : {}),
    },
  });

  return { event, suppressedFields: sanitized.evaluation.suppressedFields };
}

function buildMetricSamples(
  metric: Record<string, unknown>,
  context: {
    policy: PolicyConfig;
    source: ToolSource;
    serviceName?: string;
  },
): MetricSample[] {
  const metricName = typeof metric.name === "string" ? metric.name : "unknown_metric";
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
        attrs,
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
    recordCount: 0,
    datapointCount: 0,
    parseFailures: 0,
  };

  for (const resourceLog of Array.isArray(root.resourceLogs) ? root.resourceLogs : []) {
    const resource = resourceSummary(asRecord(resourceLog).resource);
    for (const scopeLog of Array.isArray(asRecord(resourceLog).scopeLogs)
      ? (asRecord(resourceLog).scopeLogs as unknown[])
      : []) {
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
    const resource = resourceSummary(asRecord(resourceSpan).resource);
    for (const scopeSpan of Array.isArray(asRecord(resourceSpan).scopeSpans)
      ? (asRecord(resourceSpan).scopeSpans as unknown[])
      : []) {
      for (const span of Array.isArray(asRecord(scopeSpan).spans)
        ? (asRecord(scopeSpan).spans as unknown[])
        : []) {
        result.recordCount += 1;
        try {
          result.events.push(
            buildSpanEvent(asRecord(span), {
              policy,
              source,
              transportPath: options.transportPath,
              serviceName: resource.serviceName,
              serviceVersion: resource.serviceVersion,
            }),
          );
        } catch {
          result.parseFailures += 1;
        }
      }
    }
  }

  for (const resourceMetric of Array.isArray(root.resourceMetrics) ? root.resourceMetrics : []) {
    const resource = resourceSummary(asRecord(resourceMetric).resource);
    for (const scopeMetric of Array.isArray(asRecord(resourceMetric).scopeMetrics)
      ? (asRecord(resourceMetric).scopeMetrics as unknown[])
      : []) {
      for (const metric of Array.isArray(asRecord(scopeMetric).metrics)
        ? (asRecord(scopeMetric).metrics as unknown[])
        : []) {
        const samples = buildMetricSamples(asRecord(metric), {
          policy,
          source,
          serviceName: resource.serviceName,
        });
        result.datapointCount += samples.length;
        result.metricSamples.push(...samples);
      }
    }
  }

  return result;
}
