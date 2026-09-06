/** Focused capture and privacy proof for the Gemini CLI OTLP adapter. */
import fs from "node:fs";
import type http from "node:http";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";

import { guardProofCompletion } from "./lib/proof-completion";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  deriveActionClass,
  inferSource,
} from "../packages/collector-cli/src/index";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { assertProducerToken, loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import {
  HttpBoundaryRejection,
  canonicalOtlpTransportPath,
  isOtlpPath,
  requireOtlpSource,
} from "../packages/collector-cli/src/http-boundary";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { generateGeminiCliSettings } from "../packages/collector-config/src/templates";
import {
  DEFAULT_POLICY,
  findForbiddenRawContentFields,
  forbiddenRawContentFieldNames,
  toolSourceSchema,
} from "../packages/shared/src/index";

type Check = { name: string; passed: boolean; detail: unknown };
const checks: Check[] = [];

function check(name: string, passed: boolean, detail: unknown) {
  checks.push({ name, passed, detail });
}

function attrKeys(payload: unknown) {
  const root = payload as { resourceLogs?: Array<{ scopeLogs?: Array<{ logRecords?: Array<{ attributes?: Array<{ key?: string }> }> }> }> };
  return root.resourceLogs?.flatMap((resource) => resource.scopeLogs ?? [])
    .flatMap((scope) => scope.logRecords ?? [])
    .flatMap((record) => (record.attributes ?? []).flatMap((attribute) => attribute.key ? [attribute.key] : [])) ?? [];
}

function rejectionReason(action: () => void) {
  try {
    action();
    return undefined;
  } catch (error) {
    return error instanceof HttpBoundaryRejection ? error.reason : String(error);
  }
}

function redactEndpoint(endpoint: string) {
  return endpoint.replace(/([?&]x-plimsoll-token=)[^&]*/, "$1<redacted>");
}

async function dispatchRequest(server: http.Server, request: http.IncomingMessage) {
  return new Promise<{ statusCode: number | undefined; body: string }>((resolve, reject) => {
    let statusCode: number | undefined;
    let headersSent = false;
    const response = {
      get headersSent() {
        return headersSent;
      },
      writeHead(status: number) {
        statusCode = status;
        headersSent = true;
      },
      end(body?: string | Buffer) {
        resolve({ statusCode, body: body === undefined ? "" : String(body) });
      },
      destroy(error?: Error) {
        reject(error ?? new Error("response_destroyed"));
      },
    } as unknown as http.ServerResponse;
    try {
      server.emit("request", request, response);
    } catch (error) {
      reject(error);
    }
  });
}

// Refuses two silent-green failure modes: an early event-loop drain and a
// hang that never exits. See scripts/lib/proof-completion.ts for details.
const guard = guardProofCompletion({ countChecks: () => checks.length });

async function main() {
  const fixturePath = path.join(
    import.meta.dirname,
    "../packages/shared/fixtures/otel-inputs/gemini-cli-api-response.json",
  );
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8")) as unknown;
  const rawKeys = attrKeys(fixture);
  const rawSentinels = [
    "RAW_GEMINI_FUNCTION_ARGS_SENTINEL",
    "RAW_GEMINI_REQUEST_TEXT_SENTINEL",
    "RAW_GEMINI_RESPONSE_TEXT_SENTINEL",
    "RAW_GEMINI_INPUT_MESSAGES_SENTINEL",
    "RAW_GEMINI_OUTPUT_MESSAGES_SENTINEL",
    "RAW_GEMINI_SYSTEM_INSTRUCTIONS_SENTINEL",
  ];

  check(
    "gemini_cli_is_a_valid_tool_source",
    toolSourceSchema.safeParse("gemini_cli").success,
    "gemini_cli",
  );
  check(
    "gemini_service_names_infer_gemini_cli_source",
    inferSource({ source: "gemini-cli" }) === "gemini_cli",
    inferSource({ source: "gemini-cli" }),
  );
  check(
    "gemini_function_name_maps_to_shell_action",
    deriveActionClass("run_shell_command").actionClass === "shell",
    deriveActionClass("run_shell_command"),
  );

  const exploded = explodeOtlpPayload(fixture, { policy: DEFAULT_POLICY });
  const event = exploded.events[0]?.event;
  check(
    "documented_api_response_shape_emits_one_gemini_ledger_event",
    exploded.events.length === 1 && event?.source === "gemini_cli" && event.sessionId === "gemini-session-fixture" &&
      event.actionClass === "shell" && event.metadata.toolName === "run_shell_command",
    { records: exploded.recordCount, events: exploded.events.length, source: event?.source, sessionId: event?.sessionId },
  );
  check(
    "gemini_api_response_tokens_are_attributed",
    event?.inputTokens === 1200 && event.outputTokens === 300 && event.cacheReadTokens === 50,
    { inputTokens: event?.inputTokens, outputTokens: event?.outputTokens, cacheReadTokens: event?.cacheReadTokens },
  );
  check(
    "gemini_raw_content_attribute_names_are_forbidden",
    ["function_args", "request_text", "response_text", "gen_ai.input.messages", "gen_ai.output.messages", "gen_ai.system_instructions"]
      .every((key) => forbiddenRawContentFieldNames.includes(key as never)),
    { forbidden: forbiddenRawContentFieldNames },
  );
  check(
    "gemini_fixture_raw_content_is_detected_before_sanitization",
    rawSentinels.every((sentinel) => JSON.stringify(fixture).includes(sentinel)) &&
      rawKeys.some((key) => key === "function_args"),
    { rawKeys, rawSentinels },
  );
  check(
    "gemini_raw_content_does_not_reach_event_or_ledger",
    event !== undefined &&
      rawSentinels.every((sentinel) => !JSON.stringify(event).includes(sentinel)) &&
      findForbiddenRawContentFields(event).length === 0,
    { suppressedFields: exploded.events[0]?.suppressedFields, event },
  );

  const home = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-gemini-adapter-proof-"));
  try {
    const buffer = new LocalEventBuffer(path.join(home, "ledger.sqlite"));
    try {
      buffer.appendMany(exploded.events, exploded.metricSamples, exploded.admissionDrops);
      const stored = buffer.database.prepare(
        "select source, session_id as sessionId, input_tokens as inputTokens, output_tokens as outputTokens, payload_json as payloadJson from buffered_events",
      ).get() as Record<string, unknown> | undefined;
      check(
        "gemini_event_is_persisted_in_the_ledger_with_attribution",
        stored?.source === "gemini_cli" && stored.sessionId === "gemini-session-fixture" &&
          stored.inputTokens === 1200 && stored.outputTokens === 300 &&
          rawSentinels.every((sentinel) => !String(stored.payloadJson).includes(sentinel)),
        stored,
      );
    } finally {
      buffer.close();
    }
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }

  const authHome = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-gemini-boundary-proof-"));
  const auth = loadOrCreateLocalIngestAuth(authHome);
  const settings = generateGeminiCliSettings({
    repoRoot: "/proof/repo",
    port: 48271,
    geminiCliProducerToken: auth.geminiCliProducer,
  });
  check(
    "gemini_settings_use_current_http_telemetry_keys_and_privacy_defaults",
    settings.telemetry.enabled === true &&
      settings.telemetry.target === "local" &&
      settings.telemetry.otlpProtocol === "http" &&
      settings.telemetry.useCollector === true &&
      settings.telemetry.traces === false &&
      settings.telemetry.logPrompts === false &&
      settings.telemetry.otlpEndpoint === `http://127.0.0.1:48271/gemini?x-plimsoll-token=${encodeURIComponent(auth.geminiCliProducer ?? "")}`,
    {
      telemetryKeys: Object.keys(settings.telemetry).sort(),
      otlpEndpoint: redactEndpoint(settings.telemetry.otlpEndpoint),
    },
  );

  const geminiRoute = {
    url: "/gemini/v1/logs?x-plimsoll-token=" + encodeURIComponent(auth.geminiCliProducer ?? ""),
    headers: {},
  } as http.IncomingMessage;
  const wrongGeminiRoute = {
    url: "/gemini/v1/logs?x-plimsoll-token=wrong-token",
    headers: {},
  } as http.IncomingMessage;
  const geminiUrl = geminiRoute.url ?? "";
  const wrongGeminiUrl = wrongGeminiRoute.url ?? "";
  const validToken = rejectionReason(() => assertProducerToken(
    geminiRoute,
    auth,
    "gemini_cli",
    new URL(geminiUrl, "http://127.0.0.1"),
  )) === undefined;
  const wrongToken = rejectionReason(() => assertProducerToken(
    wrongGeminiRoute,
    auth,
    "gemini_cli",
    new URL(wrongGeminiUrl, "http://127.0.0.1"),
  ));
  try {
    check(
      "native_gemini_otlp_route_is_source_qualified_and_token_bound",
      isOtlpPath(geminiUrl) &&
        requireOtlpSource(geminiRoute) === "gemini_cli" &&
        validToken &&
        canonicalOtlpTransportPath(geminiUrl) === "/v1/logs" &&
        wrongToken === "producer_token_invalid",
      {
        isOtlpPath: isOtlpPath(geminiUrl),
        source: requireOtlpSource(geminiRoute),
        canonicalPath: canonicalOtlpTransportPath(geminiUrl),
        validToken,
        wrongToken,
        settingsEndpoint: redactEndpoint(settings.telemetry.otlpEndpoint),
      },
    );
  } finally {
    fs.rmSync(authHome, { recursive: true, force: true });
  }

  const serverHome = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-gemini-server-proof-"));
  const serverBuffer = new LocalEventBuffer(path.join(serverHome, "ledger.sqlite"));
  const server = createCollectorServer(
    collectorConfigSchema.parse({}),
    serverBuffer,
    { localAuth: auth },
  );
  const serverRequest = Object.assign(
    Readable.from([JSON.stringify(fixture)]),
    {
      method: "POST",
      url: geminiUrl,
      headers: { host: "127.0.0.1", "content-type": "application/json" },
      rawHeaders: ["Host", "127.0.0.1"],
    },
  ) as unknown as http.IncomingMessage;
  try {
    const serverResponse = await dispatchRequest(server, serverRequest);
    const stored = serverBuffer.database.prepare(
      "select source, session_id as sessionId, payload_json as payloadJson from buffered_events",
    ).get() as Record<string, unknown> | undefined;
    check(
      "gemini_source_qualified_http_request_reaches_server_ledger",
      serverResponse.statusCode === 202 &&
        JSON.parse(serverResponse.body).accepted === true &&
        stored?.source === "gemini_cli" &&
        stored.sessionId === "gemini-session-fixture" &&
        !String(stored.payloadJson).includes("x-plimsoll-token") &&
        rawSentinels.every((sentinel) => !String(stored.payloadJson).includes(sentinel)),
      { statusCode: serverResponse.statusCode, response: JSON.parse(serverResponse.body), stored },
    );
  } finally {
    serverBuffer.close();
    fs.rmSync(serverHome, { recursive: true, force: true });
  }

  for (const result of checks) {
    console.log(`${result.passed ? "PASS" : "FAIL"} ${result.name} ${JSON.stringify(result.detail)}`);
  }
  const failed = checks.filter((result) => !result.passed);
  console.log(JSON.stringify({ checks: checks.length, passed: checks.length - failed.length, failed: failed.length }));
  if (failed.length > 0) process.exitCode = 1;
}

void main().then(() => guard.complete());
