/**
 * Focused proof for issue 0042 / GitHub #78.
 *
 * Uses a temporary HOME, SQLite ledger, and ephemeral port. It never reads or
 * writes the installed collector, live ledger, or telemetry configuration.
 *
 * Run: pnpm exec tsx scripts/otlp-admission-proof.ts
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { metadataSafeOtlpAttributes } from "../packages/collector-cli/src/otlp";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import {
  DEFAULT_POLICY,
  GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT,
  isCanonicalSuppressionReceipt,
} from "../packages/shared/src/index";

type Check = { name: string; passed: boolean; detail: string };
const checks: Check[] = [];

function check(name: string, passed: boolean, detail: unknown) {
  checks.push({ name, passed, detail: typeof detail === "string" ? detail : JSON.stringify(detail) });
}

function attr(key: string, value: string | number | boolean) {
  if (typeof value === "number" && Number.isInteger(value)) {
    return { key, value: { intValue: String(value) } };
  }
  if (typeof value === "number") return { key, value: { doubleValue: value } };
  if (typeof value === "boolean") return { key, value: { boolValue: value } };
  return { key, value: { stringValue: value } };
}

const RAW_SENTINELS = [
  "RAW_EXCEPTION_MESSAGE_SENTINEL",
  "RAW_EXCEPTION_STACK_SENTINEL",
  "RAW_PROMPT_SENTINEL",
  "RAW_LOG_BODY_SENTINEL",
  "RAW_METRIC_PROMPT_SENTINEL",
  "RAW_HTTP_REQUEST_BODY_SENTINEL",
  "RAW_HTTP_RESPONSE_BODY_SENTINEL",
  "RAW_DB_STATEMENT_SENTINEL",
  "https://customer.example/RAW_URL_FULL_SENTINEL",
  "CUSTOMER_SECRET_123",
  "/Users/james/Client/secret.ts",
];
const LINKED_SESSION = "11111111-2222-4333-8444-555555555555";
const GENERIC_CONTROL_SPANS = [
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
  "remoteControl/enable",
  "codex.sse_event",
  "codex.websocket_request",
  "list_tools_for_server",
  "persist_rollout_items",
] as const;
const PUNCTUATION_COLLISION_VARIANTS = [
  "thread:resume",
  "thread--read",
  "auth---",
  "codex-sse-event",
  "persist/rollout/items",
] as const;

function span(
  name: string,
  index: number,
  attributes: ReturnType<typeof attr>[] = [],
  extra: Record<string, unknown> = {},
) {
  return {
    name,
    traceId: index.toString(16).padStart(32, "0"),
    spanId: index.toString(16).padStart(16, "0"),
    startTimeUnixNano: String(1781400000000000000n + BigInt(index) * 1000000000n),
    attributes,
    ...extra,
  };
}

const spanMatrix = {
  resourceSpans: [
    {
      resource: { attributes: [attr("service.name", "codex_exec")] },
      scopeSpans: [
        {
          scope: { name: "codex_otel" },
          spans: [
            // Live-observed high-volume wrappers with no analytical signal: drop.
            ...GENERIC_CONTROL_SPANS.map((name, index) => span(name, index + 1)),
            // Unknown vendor signals fail open; a new integration is not noise by default.
            span("vendor.custom_work", 20),
            ...PUNCTUATION_COLLISION_VARIANTS.map((name, index) => span(name, 31 + index)),
            // Error status and exception events remain even when the wrapper name is generic.
            span("handle_responses", 21, [], { status: { code: 2, message: "do not persist" } }),
            span(
              "handle_responses",
              22,
              [
                attr("exception.type", "TimeoutError"),
                attr("exception.message", RAW_SENTINELS[0]),
                attr("exception.stacktrace", RAW_SENTINELS[1]),
                attr("prompt", RAW_SENTINELS[2]),
              ],
              {
                events: [
                  {
                    name: "exception",
                    attributes: [attr("exception.message", RAW_SENTINELS[0])],
                  },
                ],
              },
            ),
            // Usage, action, lifecycle, and analytical linkage are retained dimensions.
            span("handle_responses", 23, [attr("gen_ai.usage.input_tokens", 2400)]),
            span("gen_ai.tool.exec", 24, [attr("tool_name", "exec_command")]),
            span("session.start", 25),
            span("handle_responses", 26, [
              attr("conversation.id", LINKED_SESSION),
              attr("call_id", "call_linkage_only"),
            ]),
            span(RAW_SENTINELS[10], 40),
            span("vendor.privacy_probe", 41, [
              attr("call_id", "privacy_probe"),
              attr("http.request.body", RAW_SENTINELS[5]),
              attr("http.response.body", RAW_SENTINELS[6]),
              attr("db.statement", RAW_SENTINELS[7]),
              attr("url.full", RAW_SENTINELS[8]),
            ]),
            span("handle_responses", 42, [
              attr("action_class", "new_vendor_action"),
            ]),
          ],
        },
      ],
    },
    {
      resource: { attributes: [attr("service.name", "claude-code")] },
      scopeSpans: [
        {
          scope: { name: "vendor_otel" },
          // The measured deny set is Codex-specific; the same name elsewhere fails open.
          spans: [span("handle_responses", 30)],
        },
      ],
    },
  ],
};

const logEnvelope = {
  resourceLogs: [
    {
      resource: { attributes: [attr("service.name", "claude-code")] },
      scopeLogs: [
        {
          logRecords: [
            {
              timeUnixNano: "1781400010000000000",
              body: { stringValue: RAW_SENTINELS[3] },
              attributes: [
                attr("event.name", "claude_code.api_request"),
                attr("session.id", LINKED_SESSION),
                attr("input_tokens", 100),
                attr("output_tokens", 20),
              ],
            },
            {
              timeUnixNano: "1781400010500000000",
              body: { stringValue: RAW_SENTINELS[9] },
              attributes: [
                attr("call_id", "body_only_log"),
                attr("session.id", LINKED_SESSION),
              ],
            },
          ],
        },
      ],
    },
  ],
};

const metricEnvelope = {
  resourceMetrics: [
    {
      resource: { attributes: [attr("service.name", "claude-code")] },
      scopeMetrics: [
        {
          metrics: [
            {
              name: "claude_code.token.usage",
              sum: {
                dataPoints: [
                  {
                    timeUnixNano: "1781400011000000000",
                    asInt: "120",
                    attributes: [
                      attr("session.id", LINKED_SESSION),
                      attr("type", "input"),
                      attr("prompt", RAW_SENTINELS[4]),
                    ],
                  },
                ],
              },
            },
          ],
        },
      ],
    },
  ],
};

const HOSTILE_SCALAR_ENTRIES: Array<readonly [string, number | boolean]> = [
  ["api_token", 94_001.5],
  ["API-TOKEN2", true],
  ["authorization", 94_002.5],
  ["Authorization2", false],
  ["cookie", 94_003.5],
  ["cookie_2", true],
  ["password", 94_004.5],
  ["password.2", false],
  ["secret", 94_005.5],
  ["Secret2", true],
  ["user.email", 94_006.5],
  ["USER-EMAIL2", false],
  ["file.path", 94_007.5],
  ["filePath2", true],
  ["analytics.count", 94_008.5],
  ["analytics-count2", false],
  ["apі_token", 94_009.5],
  ["ａｐｉ_token", true],
];

const POSITIVE_SCALAR_ENTRIES: Array<readonly [string, string | number | boolean]> = [
  ["gen_ai.usage.input_tokens", 401],
  ["llm.usage.prompt_tokens", 402],
  ["gen_ai.usage.cost_usd", 0.25],
  ["duration_ms", 12.5],
  ["http.response.status_code", 202],
  ["success", true],
];

const scalarPrivacyLogEnvelope = {
  resourceLogs: [
    {
      resource: { attributes: [attr("service.name", "claude-code")] },
      scopeLogs: [
        {
          logRecords: [
            {
              timeUnixNano: "1781400012000000000",
              attributes: [
                attr("event.name", "scalar.privacy.log"),
                attr("call_id", "scalar_privacy_log"),
                ...POSITIVE_SCALAR_ENTRIES.map(([key, value]) => attr(key, value)),
                ...HOSTILE_SCALAR_ENTRIES.map(([key, value]) => attr(key, value)),
              ],
            },
          ],
        },
      ],
    },
  ],
};

const scalarPrivacyMetricEnvelope = {
  resourceMetrics: [
    {
      resource: { attributes: [attr("service.name", "claude-code")] },
      scopeMetrics: [
        {
          metrics: [
            {
              name: "scalar.privacy.metric",
              gauge: {
                dataPoints: [
                  {
                    timeUnixNano: "1781400013000000000",
                    asDouble: 5.5,
                    attributes: [
                      attr("type", "input"),
                      ...POSITIVE_SCALAR_ENTRIES.map(([key, value]) => attr(key, value)),
                      ...HOSTILE_SCALAR_ENTRIES.map(([key, value]) => attr(key, value)),
                    ],
                  },
                ],
              },
            },
          ],
        },
      ],
    },
  ],
};

async function post(port: number, route: string, payload: unknown, source: string) {
  const response = await fetch(`http://127.0.0.1:${port}${route}`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-plimsoll-source": source },
    body: JSON.stringify(payload),
  });
  return {
    status: response.status,
    body: (await response.json()) as Record<string, unknown>,
  };
}

async function main() {
  const tempHome = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-otlp-admission-"));
  process.env.HOME = tempHome;
  process.env.PLIMSOLL_HOME = tempHome;
  const ledgerPath = path.join(tempHome, "work-ledger.sqlite");
  let buffer: LocalEventBuffer | undefined;

  const directInput = Object.fromEntries([
    ...POSITIVE_SCALAR_ENTRIES,
    ...HOSTILE_SCALAR_ENTRIES,
  ]);
  const direct = metadataSafeOtlpAttributes(directInput, DEFAULT_POLICY.dataMode);
  check(
    "key_first_scalar_classifier_direct_matrix",
    POSITIVE_SCALAR_ENTRIES.every(([key, value]) => direct.attrs[key] === value) &&
      HOSTILE_SCALAR_ENTRIES.every(([key]) => !(key in direct.attrs)) &&
      direct.suppressedFields.length === 1 &&
      direct.suppressedFields[0] === GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT &&
      direct.suppressedFields.every(isCanonicalSuppressionReceipt),
    {
      positiveRetained: POSITIVE_SCALAR_ENTRIES.filter(
        ([key, value]) => direct.attrs[key] === value,
      ).length,
      hostileOmitted: HOSTILE_SCALAR_ENTRIES.filter(([key]) => !(key in direct.attrs)).length,
      receipts: direct.suppressedFields,
    },
  );

  try {
    buffer = new LocalEventBuffer(ledgerPath);
    const server = createCollectorServer(collectorConfigSchema.parse({}), buffer);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;

    const traces = await post(port, "/v1/traces", spanMatrix, "codex");
    check(
      "adversarial_span_matrix",
      traces.status === 202 &&
        traces.body.recordCount === 32 &&
        traces.body.events === 16 &&
        traces.body.droppedEvents === GENERIC_CONTROL_SPANS.length,
      traces,
    );
    check(
      "drop_reason_transparent",
      Array.isArray(traces.body.droppedByReason) &&
        (traces.body.droppedByReason as Array<Record<string, unknown>>).some(
          (row) =>
            row.source === "codex" &&
            row.reason === "generic_zero_value_span" &&
            row.count === GENERIC_CONTROL_SPANS.length,
        ),
      traces.body.droppedByReason,
    );

    const logs = await post(port, "/v1/logs", logEnvelope, "claude_code");
    const metrics = await post(port, "/v1/metrics", metricEnvelope, "claude_code");
    const scalarLogs = await post(
      port,
      "/v1/logs",
      scalarPrivacyLogEnvelope,
      "claude_code",
    );
    const scalarMetrics = await post(
      port,
      "/v1/metrics",
      scalarPrivacyMetricEnvelope,
      "claude_code",
    );
    check("existing_log_compatibility", logs.status === 202 && logs.body.events === 2, logs);
    check(
      "existing_metric_compatibility",
      metrics.status === 202 && metrics.body.metricSamples === 1,
      metrics,
    );

    const scalarLogRow = buffer
      .list(200)
      .find((row) => (row.payload.metadata as Record<string, unknown>).call_id === "scalar_privacy_log");
    const scalarMetricRow = buffer.database
      .prepare(
        `select attrs_json as attrs, suppressed_fields_json as suppressed
         from metric_samples where metric_name = ?`,
      )
      .get("scalar.privacy.metric") as { attrs: string; suppressed: string } | undefined;
    const scalarLogMetadata = scalarLogRow?.payload.metadata as Record<string, unknown> | undefined;
    const scalarMetricAttrs = JSON.parse(scalarMetricRow?.attrs ?? "{}") as Record<string, unknown>;
    const scalarMetricReceipts = JSON.parse(scalarMetricRow?.suppressed ?? "[]") as string[];
    const scalarLogResponseReceipts = Array.isArray(scalarLogs.body.suppressedFields)
      ? (scalarLogs.body.suppressedFields as string[])
      : [];
    const scalarMetricResponseReceipts = Array.isArray(scalarMetrics.body.suppressedFields)
      ? (scalarMetrics.body.suppressedFields as string[])
      : [];
    const positiveMetadataExact = (metadata: Record<string, unknown> | undefined) =>
      Boolean(
        metadata &&
          POSITIVE_SCALAR_ENTRIES.every(([key, value]) => {
            const expected = Number.isInteger(value as number) && typeof value === "number"
              ? String(value)
              : value;
            return metadata[key] === expected;
          }),
      );
    check(
      "production_log_scalar_privacy_and_response_raw_parity",
      scalarLogs.status === 202 &&
        scalarLogs.body.events === 1 &&
        scalarLogRow?.payload.inputTokens === 401 &&
        scalarLogRow.payload.costUsd === 0.25 &&
        positiveMetadataExact(scalarLogMetadata) &&
        HOSTILE_SCALAR_ENTRIES.every(([key]) => !(key in (scalarLogMetadata ?? {}))) &&
        JSON.stringify(scalarLogResponseReceipts) ===
          JSON.stringify(scalarLogRow.suppressedFields) &&
        scalarLogResponseReceipts.length === 1 &&
        scalarLogResponseReceipts[0] === GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT &&
        scalarLogResponseReceipts.every(isCanonicalSuppressionReceipt),
      {
        status: scalarLogs.status,
        promotedInput: scalarLogRow?.payload.inputTokens,
        promotedCost: scalarLogRow?.payload.costUsd,
        hostileOmitted: HOSTILE_SCALAR_ENTRIES.filter(
          ([key]) => !(key in (scalarLogMetadata ?? {})),
        ).length,
        parity: JSON.stringify(scalarLogResponseReceipts) ===
          JSON.stringify(scalarLogRow?.suppressedFields ?? []),
      },
    );
    check(
      "production_metric_datapoint_scalar_privacy_receipts_and_promotion",
      scalarMetrics.status === 202 &&
        scalarMetrics.body.metricSamples === 1 &&
        positiveMetadataExact(scalarMetricAttrs) &&
        HOSTILE_SCALAR_ENTRIES.every(([key]) => !(key in scalarMetricAttrs)) &&
        JSON.stringify(scalarMetricResponseReceipts) === JSON.stringify(scalarMetricReceipts) &&
        scalarMetricReceipts.length === 1 &&
        scalarMetricReceipts[0] === GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT &&
        scalarMetricReceipts.every(isCanonicalSuppressionReceipt),
      {
        status: scalarMetrics.status,
        hostileOmitted: HOSTILE_SCALAR_ENTRIES.filter(([key]) => !(key in scalarMetricAttrs)).length,
        parity: JSON.stringify(scalarMetricResponseReceipts) === JSON.stringify(scalarMetricReceipts),
      },
    );

    const rows = buffer.list(100);
    const payloads = rows.map((row) => row.payload);
    const metadata = (eventName: string) =>
      payloads.find(
        (event) =>
          (event.metadata as Record<string, unknown>).otelEventName === eventName,
      )?.metadata as Record<string, unknown> | undefined;
    const exception = payloads.find(
      (event) => (event.metadata as Record<string, unknown>).otelHasException === true,
    );
    const error = payloads.find(
      (event) =>
        (event.metadata as Record<string, unknown>).otelHasError === true && event !== exception,
    );
    const tool = payloads.find(
      (event) => event.eventType === "tool_use" && event.actionClass === "shell",
    );
    const lifecycle = payloads.find((event) => event.eventType === "session_start");
    const linked = payloads.find((event) => event.sessionId === LINKED_SESSION && event.source === "codex");
    const token = payloads.find(
      (event) => event.source === "codex" && event.inputTokens === 2400,
    );
    const unknownAction = payloads.find(
      (event) =>
        (event.metadata as Record<string, unknown>).otelOriginalActionClass ===
        "new_vendor_action",
    );
    const privacyRow = rows.find(
      (row) => (row.payload.metadata as Record<string, unknown>).call_id === "privacy_probe",
    );
    const bodyOnlyLog = rows.find(
      (row) => (row.payload.metadata as Record<string, unknown>).call_id === "body_only_log",
    );
    const pathNameRow = rows.find((row) => row.suppressedFields.includes("span.name"));

    check(
      "retained_error_and_exception",
      Boolean(
        error &&
          exception &&
          (error.metadata as Record<string, unknown>).otelStatusCode === 2 &&
          (exception.metadata as Record<string, unknown>).otelHasException === true,
      ),
      { error: error?.id, exception: exception?.id },
    );
    check(
      "retained_usage_tool_lifecycle_linkage",
      Boolean(
        token?.eventType === "assistant_response" &&
          tool?.actionClass === "shell" &&
          lifecycle &&
          linked,
      ),
      {
        token: token?.eventType,
        tool: tool?.actionClass,
        lifecycle: lifecycle?.id,
        linked: linked?.id,
      },
    );
    check(
      "ambiguous_unknown_fails_open",
      Boolean(metadata("vendor.custom_work")),
      metadata("vendor.custom_work") ?? "missing",
    );
    check(
      "punctuation_collision_variants_fail_open",
      PUNCTUATION_COLLISION_VARIANTS.every((name) =>
        payloads.some(
          (event) =>
            event.source === "codex" &&
            (event.metadata as Record<string, unknown>).otelEventName === name,
        ),
      ),
      payloads
        .map((event) => (event.metadata as Record<string, unknown>).otelEventName)
        .filter(
          (name): name is (typeof PUNCTUATION_COLLISION_VARIANTS)[number] =>
            typeof name === "string" &&
            (PUNCTUATION_COLLISION_VARIANTS as readonly string[]).includes(name),
        ),
    );
    check(
      "generic_name_is_source_scoped",
      payloads.some(
        (event) =>
          event.source === "claude_code" &&
          (event.metadata as Record<string, unknown>).otelEventName === "handle_responses",
      ),
      payloads.filter(
        (event) =>
          (event.metadata as Record<string, unknown>).otelEventName === "handle_responses",
      ).map((event) => event.source),
    );
    check(
      "generic_span_not_persisted",
      payloads.filter(
        (event) =>
          event.source === "codex" &&
          (event.metadata as Record<string, unknown>).otelEventName === "handle_responses",
      ).length === 5,
      payloads.map((event) => (event.metadata as Record<string, unknown>).otelEventName),
    );
    check(
      "unknown_explicit_action_degrades_safely",
      Boolean(
        unknownAction?.actionClass === "other" &&
          (unknownAction.metadata as Record<string, unknown>).otelExplicitAction === true &&
          (unknownAction.metadata as Record<string, unknown>).otelOriginalActionClass ===
            "new_vendor_action",
      ),
      unknownAction
        ? {
            actionClass: unknownAction.actionClass,
            original: (unknownAction.metadata as Record<string, unknown>)
              .otelOriginalActionClass,
          }
        : "missing",
    );

    const persisted = JSON.stringify({
      events: buffer.database.prepare("select payload_json, suppressed_fields_json from buffered_events").all(),
      metrics: buffer.database.prepare("select attrs_json, suppressed_fields_json from metric_samples").all(),
    });
    check(
      "raw_content_privacy",
      RAW_SENTINELS.every((sentinel) => !persisted.includes(sentinel)),
      "no raw body, SQL, URL, path, error, prompt, or metric sentinel persisted",
    );
    check(
      "semantic_privacy_suppression_receipts",
      Boolean(
        exception?.id &&
          rows
            .find((row) => row.id === exception.id)
            ?.suppressedFields.includes(GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT) &&
          privacyRow &&
          privacyRow.suppressedFields.includes(GENERIC_ATTRIBUTE_SUPPRESSION_RECEIPT) &&
          pathNameRow?.suppressedFields.includes("span.name") &&
          bodyOnlyLog?.suppressedFields.includes("body") &&
          !("otelEventName" in (bodyOnlyLog.payload.metadata as Record<string, unknown>)) &&
          Boolean(metadata("claude_code.api_request")),
      ),
      {
        exception: rows.find((row) => row.id === exception?.id)?.suppressedFields ?? [],
        privacy: privacyRow?.suppressedFields ?? [],
        path: pathNameRow?.suppressedFields ?? [],
        body: bodyOnlyLog?.suppressedFields ?? [],
      },
    );

    const statusResponse = await fetch(`http://127.0.0.1:${port}/status`);
    const status = (await statusResponse.json()) as {
      otlpAdmission?: {
        counterLifetime?: string;
        dropped?: Array<{ source: string; reason: string; droppedCount: number }>;
      };
    };
    check(
      "status_exposes_durable_bounded_counter",
      status.otlpAdmission?.counterLifetime === "durable" &&
        status.otlpAdmission.dropped?.length === 1 &&
        status.otlpAdmission.dropped[0]?.source === "codex" &&
        status.otlpAdmission.dropped[0]?.reason === "generic_zero_value_span" &&
        status.otlpAdmission.dropped[0]?.droppedCount === GENERIC_CONTROL_SPANS.length,
      status.otlpAdmission,
    );

    const openArtifactCopies = [ledgerPath, `${ledgerPath}-wal`, `${ledgerPath}-shm`]
      .filter((candidate) => fs.existsSync(candidate))
      .map((candidate) => fs.readFileSync(candidate));
    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
    const closedArtifacts = [
      ...openArtifactCopies,
      ...[ledgerPath, `${ledgerPath}-wal`, `${ledgerPath}-shm`]
        .filter((candidate) => fs.existsSync(candidate))
        .map((candidate) => fs.readFileSync(candidate)),
    ];
    // Boolean literals are intentionally not byte-scanned: `true`/`false`
    // legitimately occur throughout SQLite control state. Their association
    // is disproved by the key-absence and parsed-metadata assertions above.
    const hostileTerms = HOSTILE_SCALAR_ENTRIES.flatMap(([key, value]) => [
      key,
      ...(typeof value === "number" ? [String(value)] : []),
    ]);
    check(
      "scalar_hostile_keys_and_values_absent_from_open_copies_and_closed_ledger",
      hostileTerms.every((term) =>
        closedArtifacts.every((artifact) => !artifact.includes(Buffer.from(term))),
      ),
      {
        artifacts: closedArtifacts.length,
        privateTerms: hostileTerms.length,
        leaks: hostileTerms.filter((term) =>
          closedArtifacts.some((artifact) => artifact.includes(Buffer.from(term))),
        ).length,
      },
    );
    buffer = new LocalEventBuffer(ledgerPath);
    check(
      "drop_counter_survives_restart",
      buffer.otlpAdmissionCounters()[0]?.droppedCount === GENERIC_CONTROL_SPANS.length,
      buffer.otlpAdmissionCounters(),
    );
  } finally {
    buffer?.close();
    fs.rmSync(tempHome, { recursive: true, force: true });
  }

  const failed = checks.filter((entry) => !entry.passed);
  console.log(JSON.stringify({ proof: "otlp_admission", checks, passed: failed.length === 0 }, null, 2));
  if (failed.length > 0) {
    throw new Error(`OTLP admission proof failed: ${failed.map((entry) => entry.name).join(", ")}`);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
