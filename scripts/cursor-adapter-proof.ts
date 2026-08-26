import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import {
  DEFAULT_POLICY,
  findForbiddenRawContentFields,
  toolSourceSchema,
  type ToolSource,
} from "../packages/shared/src/index";
import {
  deriveActionClass,
  inferSource,
  normalizeHookPayload,
} from "../packages/collector-cli/src/normalizer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";

const checks: Array<{ name: string; passed: boolean; detail: unknown }> = [];

function check(name: string, passed: boolean, detail: unknown) {
  checks.push({ name, passed, detail });
  console.log(`${passed ? "PASS" : "FAIL"} ${name} ${JSON.stringify(detail)}`);
  assert.equal(passed, true, name);
}

const cursor = "cursor" as ToolSource;
const sessionId = "cursor-conversation-proof";

check("cursor is a ToolSource", toolSourceSchema.safeParse(cursor).success, cursor);
check("Cursor source inference", inferSource({ provider: "Cursor" }) === cursor, inferSource({ provider: "Cursor" }));

const actionMatrix = [
  ["read_file", "read"],
  ["run_terminal_cmd", "shell"],
  ["edit_file", "edit"],
  ["mcp", "mcp"],
] as const;
for (const [toolName, expected] of actionMatrix) {
  check(`Cursor ${toolName} action class`, deriveActionClass(toolName).actionClass === expected, deriveActionClass(toolName));
}

const rawSentinel = "CURSOR_RAW_CONTENT_SENTINEL_7D31";
const hook = normalizeHookPayload(
  {
    conversation_id: sessionId,
    generation_id: "cursor-generation-proof",
    hook_event_name: "postToolUse",
    tool_name: "run_terminal_cmd",
    command: rawSentinel,
    output: rawSentinel,
    tool_input: rawSentinel,
    result: rawSentinel,
    task: rawSentinel,
    transcript_path: `/private/${rawSentinel}.jsonl`,
    workspace_roots: [`/private/${rawSentinel}`],
  },
  { policy: DEFAULT_POLICY, source: cursor },
);
check("Cursor hook keeps the conversation session id", hook.event.sessionId === sessionId, hook.event.sessionId);
check("Cursor hook derives a non-other action class", hook.event.actionClass === "shell", hook.event.actionClass);
check(
  "Cursor hook has no raw content",
  !JSON.stringify(hook.event).includes(rawSentinel),
  hook.suppressedFields,
);

const shellHook = normalizeHookPayload(
  {
    conversation_id: sessionId,
    hook_event_name: "beforeShellExecution",
    command: rawSentinel,
  },
  { policy: DEFAULT_POLICY, source: cursor },
);
check("Cursor shell hook is a shell tool event", shellHook.event.eventType === "tool_use" && shellHook.event.actionClass === "shell", shellHook.event);

const editHook = normalizeHookPayload(
  {
    conversation_id: sessionId,
    hook_event_name: "afterFileEdit",
    edits: rawSentinel,
  },
  { policy: DEFAULT_POLICY, source: cursor },
);
check("Cursor file hook is an edit tool event", editHook.event.eventType === "tool_result" && editHook.event.actionClass === "edit", editHook.event);

const mcpHook = normalizeHookPayload(
  {
    conversation_id: sessionId,
    hook_event_name: "beforeMCPExecution",
    tool_name: "customer_defined_tool",
    tool_input: rawSentinel,
  },
  { policy: DEFAULT_POLICY, source: cursor },
);
check("Cursor MCP hook classifies arbitrary MCP names", mcpHook.event.eventType === "tool_use" && mcpHook.event.actionClass === "mcp", mcpHook.event);

const otlp = explodeOtlpPayload(
  {
    resourceLogs: [
      {
        resource: { attributes: [{ key: "service.name", value: { stringValue: "cursor" } }] },
        scopeLogs: [
          {
            scope: { name: "cursor.telemetry", version: "0.1.0" },
            logRecords: [
              {
                timeUnixNano: "1781400000000000000",
                body: { stringValue: "api_request" },
                attributes: [
                  { key: "cursor.conversation.id", value: { stringValue: sessionId } },
                  { key: "cursor.model.name", value: { stringValue: "Auto" } },
                  { key: "cursor.api.request.input_tokens", value: { intValue: "120" } },
                  { key: "cursor.api.request.output_tokens", value: { intValue: "45" } },
                  { key: "cursor.event.id", value: { stringValue: "cursor-event-proof" } },
                ],
              },
            ],
          },
        ],
      },
    ],
  },
  { policy: DEFAULT_POLICY, source: cursor, transportPath: "/v1/logs" },
);
const cursorEvent = otlp.events[0]?.event;
check("Cursor OTLP produces an event", cursorEvent !== undefined, otlp);
check("Cursor OTLP event has source and session", cursorEvent?.source === cursor && cursorEvent?.sessionId === sessionId, cursorEvent);
check("Cursor OTLP preserves exposed token counts", cursorEvent?.inputTokens === 120 && cursorEvent?.outputTokens === 45, cursorEvent);

const forbiddenHits = findForbiddenRawContentFields({
  cursor: {
    tool_input: rawSentinel,
    tool_output: rawSentinel,
    result_json: rawSentinel,
    text: rawSentinel,
    error_message: rawSentinel,
    workspace_roots: [`/private/${rawSentinel}`],
    transcript_path: `/private/${rawSentinel}.jsonl`,
  },
});
check("Cursor raw-content field sentinel is forbidden", forbiddenHits.length === 7, forbiddenHits);

const proofHome = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-cursor-adapter-proof-"));
const buffer = new LocalEventBuffer(path.join(proofHome, "work-ledger.sqlite"));
const server = createCollectorServer(collectorConfigSchema.parse({}), buffer);
await new Promise<void>((resolve, reject) => {
  server.once("error", reject);
  server.listen(0, "127.0.0.1", () => resolve());
});
try {
  const port = (server.address() as AddressInfo).port;
  const body = Buffer.from(JSON.stringify({
    id: "11111111-2222-4333-8444-555555555556",
    conversation_id: sessionId,
    hook_event_name: "postToolUse",
    tool_name: "read_file",
    tool_input: rawSentinel,
  }));
  const response = await new Promise<{ status: number; body: string }>((resolve, reject) => {
    const request = http.request({
      host: "127.0.0.1",
      port,
      path: "/hooks/cursor",
      method: "POST",
      headers: {
        "content-type": "application/json",
        "content-length": String(body.length),
      },
    }, (incoming) => {
      const chunks: Buffer[] = [];
      incoming.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
      incoming.on("end", () => resolve({
        status: incoming.statusCode ?? 0,
        body: Buffer.concat(chunks).toString("utf8"),
      }));
    });
    request.on("error", reject);
    request.end(body);
  });
  const ledgerRow = buffer.database.prepare(
    "select source, session_id as sessionId, action_class as actionClass, payload_json as payloadJson from buffered_events where id = ?",
  ).get("11111111-2222-4333-8444-555555555556") as {
    source: string;
    sessionId: string;
    actionClass: string;
    payloadJson: string;
  } | undefined;
  check("Cursor hook route accepts a session", response.status === 202 && ledgerRow !== undefined, response);
  check(
    "Cursor ledger row keeps source, session, and action class",
    ledgerRow?.source === cursor && ledgerRow.sessionId === sessionId && ledgerRow.actionClass === "read",
    ledgerRow,
  );
  check("Cursor ledger row has no raw content", !ledgerRow?.payloadJson.includes(rawSentinel), ledgerRow);
} finally {
  await new Promise<void>((resolve) => server.close(() => resolve()));
  buffer.close();
}

const adaptersDoc = fs.readFileSync(path.join(process.cwd(), "docs", "adapters.md"), "utf8");
for (const term of ["source id", "transport", "session key", "raw-content field names"]) {
  check(`adapter template documents ${term}`, adaptersDoc.toLowerCase().includes(term), term);
}

console.log(JSON.stringify({ status: "cursor_adapter_proof_passed", checks: checks.length }, null, 2));
