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
import { createCollectorServer } from "../packages/collector-cli/src/server";

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
    check("existing_log_compatibility", logs.status === 202 && logs.body.events === 2, logs);
    check(
      "existing_metric_compatibility",
      metrics.status === 202 && metrics.body.metricSamples === 1,
      metrics,
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
      metrics: buffer.database.prepare("select attrs_json from metric_samples").all(),
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
            ?.suppressedFields.some((field) => field.includes("exception.message")) &&
          privacyRow &&
          [
            "attributes.http.request.body",
            "attributes.http.response.body",
            "attributes.db.statement",
            "attributes.url.full",
          ].every((field) => privacyRow.suppressedFields.includes(field)) &&
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

    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
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
