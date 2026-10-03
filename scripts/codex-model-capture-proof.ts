import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("codex-model-capture", 17);
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { captureFrontier } from "../packages/collector-cli/src/capture-frontier";
import { buildIngestBatch } from "../packages/collector-cli/src/upload";
import type { CollectorConfig } from "../packages/collector-cli/src/config";
import type { AiInteractionEvent } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "codex-model-capture-"));
const at = Date.now() - 300_000,
  session = "11111111-1111-4111-8111-111111111111";
const trace = "a".repeat(32),
  account = "sha256:0123456789abcdef";
const attr = (key: string, value: string | number) => ({
  key,
  value:
    typeof value === "number"
      ? { intValue: String(value) }
      : { stringValue: value },
});
const resource = { attributes: [attr("service.name", "codex-app-server")] };
let sequence = 1,
  checks = 0;
function span(
  extra: ReturnType<typeof attr>[] = [],
  name = "handle_responses",
) {
  return explodeOtlpPayload(
    {
      resourceSpans: [
        {
          resource,
          scopeSpans: [
            {
              spans: [
                {
                  name,
                  traceId: trace,
                  spanId: (sequence++).toString(16).padStart(16, "0"),
                  startTimeUnixNano: String(BigInt(at) * 1_000_000n),
                  endTimeUnixNano: String(BigInt(at + 1000) * 1_000_000n),
                  attributes: [
                    attr("gen_ai.usage.input_tokens", 5555),
                    attr("gen_ai.usage.output_tokens", 55),
                    attr("gen_ai.usage.cache_read.input_tokens", 5000),
                    ...extra,
                  ],
                },
              ],
            },
          ],
        },
      ],
    },
    { source: "codex" },
  ).events[0]!.event;
}
function log(
  model: string,
  traced: boolean,
  usage = false,
): AiInteractionEvent {
  return explodeOtlpPayload(
    {
      resourceLogs: [
        {
          resource,
          scopeLogs: [
            {
              logRecords: [
                {
                  timeUnixNano: String(BigInt(at + 1000) * 1_000_000n),
                  ...(traced ? { traceId: trace } : {}),
                  attributes: [
                    attr("event.name", "codex.sse_event"),
                    attr("conversation.id", session),
                    attr("model", model),
                    attr("user.account_id", account),
                    ...(usage
                      ? [
                          attr("input_token_count", 5555),
                          attr("output_token_count", 55),
                          attr("cached_token_count", 5000),
                        ]
                      : []),
                  ],
                },
              ],
            },
          ],
        },
      ],
    },
    { source: "codex" },
  ).events[0]!.event;
}
async function run(
  name: string,
  seed: (
    b: LocalEventBuffer,
  ) => AiInteractionEvent | Promise<AiInteractionEvent>,
  expected: string | null,
  reason?: string,
) {
  let now = new Date(at + 2000);
  const b = new LocalEventBuffer(path.join(root, name + ".sqlite"), {
    workspaceId: session,
    deviceId: "fixture-device",
    enrollmentNow: () => new Date(at - 1000),
    delivery: { enabled: true, now: () => now },
  });
  try {
    const target = await seed(b);
    b.append(target);
    assert.equal(
      b.delivery
        .lease({ now })
        .items.some((item) => item.envelope.event.id === target.id),
      false,
      "bounded wait before pair timeout",
    );
    now = new Date(at + 63_000);
    const leased = b.delivery.lease({ now });
    const events = leased.items.map((item) => item.envelope.event);
    const result = events.find((e) => e.id === target.id)!;
    assert.ok(result, name + ": delivered or gap");
    assert.ok(
      result.metadata.installationEpochId,
      "machine identity survives upload",
    );
    if (expected) {
      assert.equal(result.model, expected);
      assert.equal(result.inputTokens, 5555);
      assert.ok(result.metadata.modelCaptureSource);
      assert.ok(result.metadata["user.account_id"]);
    } else {
      assert.equal(result.model, undefined);
      assert.equal(result.inputTokens, undefined);
      assert.equal(result.outputTokens, undefined);
      assert.equal(result.costUsd, undefined);
      assert.equal(result.metadata.captureGap, true);
      assert.equal(result.metadata.modelGapReason, reason);
      assert.equal(result.metadata.modelGapInputTokens, 5555);
      b.delivery.acknowledge(
        leased.leaseId,
        leased.items.map((item) => item.deliveryId),
        now,
      );
      assert.ok(
        captureFrontier(b.database)?.gaps.some(
          (g) =>
            g.fromMs <= Date.parse(target.observedAt) &&
            g.toMs >= Date.parse(target.observedAt),
        ),
        "capture claim retains the token gap after acknowledgement",
      );
    }
    checks++;
    completion.check(name);
  } finally {
    b.close();
  }
}
async function main() {
  try {
    await run(
      "production-span-only",
      () => span(),
      null,
      "model_evidence_missing",
    );
    await run(
      "session-span-only",
      () => span([attr("conversation.id", session)]),
      null,
      "model_evidence_missing",
    );
    await run(
      "generic-gen-ai-span",
      () => span([], "vendor.response"),
      null,
      "model_evidence_missing",
    );
    await run(
      "unique-trace",
      (b) => {
        b.append(log("gpt-6.1-sol", true));
        return span();
      },
      "gpt-6.1-sol",
    );
    await run(
      "ambiguous-trace",
      (b) => {
        b.append(log("gpt-6.1-sol", true));
        b.append(log("gpt-6-astra", true));
        return span();
      },
      null,
      "ambiguous_trace_model",
    );
    await run(
      "foreign-trace",
      (b) => {
        const e = log("gpt-6-astra", true);
        e.metadata.traceId = "b".repeat(32);
        b.append(e);
        return span();
      },
      null,
      "model_evidence_missing",
    );
    await run(
      "local-turn",
      (b) => {
        b.append({
          ...log("gpt-6.1-sol", false),
          eventType: "otel_span",
          metadata: {
            usageSource: "codex_local_turn",
            codexTurnId: "turn-a",
            "user.account_id": account,
          },
        });
        return span([
          attr("conversation.id", session),
          attr("turn.id", "turn-a"),
        ]);
      },
      "gpt-6.1-sol",
    );
    await run(
      "ambiguous-local-turn",
      (b) => {
        for (const model of ["gpt-6.1-sol", "gpt-6-astra"])
          b.append({
            ...log(model, false),
            eventType: "otel_span",
            metadata: {
              usageSource: "codex_local_turn",
              codexTurnId: "turn-a",
              "user.account_id": account,
            },
          });
        return span([
          attr("conversation.id", session),
          attr("turn.id", "turn-a"),
        ]);
      },
      null,
      "ambiguous_local_turn_model",
    );
    await run(
      "wrong-turn",
      (b) => {
        b.append({
          ...log("gpt-6-astra", false),
          eventType: "otel_span",
          metadata: {
            usageSource: "codex_local_turn",
            codexTurnId: "turn-b",
            "user.account_id": account,
          },
        });
        return span([
          attr("conversation.id", session),
          attr("turn.id", "turn-a"),
        ]);
      },
      null,
      "model_evidence_missing",
    );
    await run(
      "no-model-log",
      () => {
        const e = log("gpt-6.1-sol", false, true);
        delete e.model;
        delete e.metadata.model;
        return e;
      },
      null,
      "model_evidence_missing",
    );
    await run(
      "no-model-rollout",
      () => ({
        ...span(),
        eventType: "usage_rollout",
        sessionId: session,
        metadata: { usageSource: "rollout" },
      }),
      null,
      "model_evidence_missing",
    );
    await run(
      "conflicting-native-models",
      () =>
        span([
          attr("model", "gpt-6.1-sol"),
          attr("gen_ai.request.model", "gpt-6-astra"),
        ]),
      null,
      "conflicting_model_attributes",
    );
    await run(
      "claude-incomplete-model",
      () => ({
        ...span(),
        source: "claude_code",
        cacheCreationTokens: undefined,
        metadata: {
          serviceName: "claude-code",
          otelEventName: "claude_code.api_request",
        },
      }),
      null,
      "claude_model_evidence_missing",
    );
    await run(
      "legacy-class2-model",
      () => ({
        ...span(),
        source: "claude_code",
        metadata: { serviceName: "Codex_Desktop" },
      }),
      null,
      "codex_service_under_claude_source",
    );
    await run(
      "real-local-turn-reader",
      async (b) => {
        const target = span([
          attr("conversation.id", session),
          attr("turn.id", "turn-real"),
          attr("user.account_id", account),
        ]);
        b.append(target);
        const sessions = path.join(root, "native-records"),
          day = path.join(
            sessions,
            ...new Date(at).toISOString().slice(0, 10).split("-"),
          );
        fs.mkdirSync(day, { recursive: true });
        fs.writeFileSync(
          path.join(day, `rollout-fixture-${session}.jsonl`),
          [
            {
              timestamp: new Date(at).toISOString(),
              type: "session_meta",
              payload: { id: session },
            },
            {
              timestamp: new Date(at).toISOString(),
              type: "turn_context",
              payload: { turn_id: "turn-real", model: "gpt-6.1-sol" },
            },
            {
              timestamp: new Date(at + 1000).toISOString(),
              type: "event_msg",
              payload: {
                type: "token_count",
                info: {
                  total_token_usage: {
                    input_tokens: 5555,
                    output_tokens: 55,
                    cached_input_tokens: 5000,
                  },
                },
              },
            },
          ]
            .map((row) => JSON.stringify(row))
            .join("\n") + "\n",
        );
        const tailer = new RolloutTailer(b, sessions, () => []);
        try {
          const scan = await tailer.scan({
            scope: "full",
            now: new Date(at + 2000),
          });
          assert.equal(
            scan.sessionsSkippedOtlpCovered,
            1,
            JSON.stringify(scan),
          );
          assert.equal(
            (
              b.database
                .prepare("select count(*) as n from codex_turn_model_evidence")
                .get() as { n: number }
            ).n,
            1,
          );
        } finally {
          tailer.close();
        }
        return target;
      },
      "gpt-6.1-sol",
    );
    // The stateless --no-mark path has the same hold and never emits unknown usage.
    {
      const b = new LocalEventBuffer(path.join(root, "stateless.sqlite"), {
        workspaceId: session,
        deviceId: "fixture-device",
        enrollmentNow: () => new Date(at - 1000),
      });
      try {
        const target = span();
        b.append(target);
        const config = {
          tenantId: session,
          deviceId: "fixture-device",
          installKey: "fixture-install",
        } as CollectorConfig;
        const result = buildIngestBatch(config, b, {
          now: () => new Date(Date.now() + 61000),
        });
        assert.equal(result.batch?.events[0]?.event.metadata.captureGap, true);
        assert.equal(result.batch?.events[0]?.event.inputTokens, undefined);
        checks++;
        completion.check("stateless-upload-gaps");
      } finally {
        b.close();
      }
    }
    {
      let now = new Date(at + 2000);
      const b = new LocalEventBuffer(path.join(root, "legacy-sealed.sqlite"), {
        workspaceId: session,
        deviceId: "fixture-device",
        enrollmentNow: () => new Date(at - 1000),
        delivery: { enabled: true, now: () => now },
      });
      try {
        const target = span();
        b.append(target);
        const rawBefore = (
          b.database
            .prepare(
              "select payload_json as payload from buffered_events where id=?",
            )
            .get(target.id) as { payload: string }
        ).payload;
        const queued = b.database
          .prepare(
            "select delivery_id as id,base_envelope_json as payload from upload_outbox",
          )
          .get() as { id: string; payload: string };
        const old = JSON.parse(queued.payload);
        delete old.event.metadata.installationEpochId;
        const frozen = JSON.stringify(old);
        b.database
          .prepare(
            "update upload_outbox set sealed_envelope_json=?,sealed_bytes=?,attempt_count=1,state='retry'",
          )
          .run(frozen, Buffer.byteLength(frozen));
        now = new Date(at + 63000);
        const oldLease = b.delivery.lease({ now });
        assert.equal(
          oldLease.items.length,
          0,
          "old usage retry is retired without changing frozen request bytes",
        );
        const second = b.delivery.lease({ now });
        const gap = second.items[0]!;
        assert.ok(
          gap,
          JSON.stringify({
            second,
            receipts: b.database
              .prepare("select reason from upload_receipts")
              .all(),
            queue: b.database
              .prepare("select delivery_id,state from upload_outbox")
              .all(),
          }),
        );
        assert.notEqual(gap.deliveryId, queued.id);
        assert.equal(gap.envelope.event.metadata.captureGap, true);
        assert.equal(
          gap.envelope.event.metadata.modelGapReason,
          "legacy_sealed_model_missing",
        );
        assert.equal(gap.envelope.event.inputTokens, undefined);
        assert.ok(gap.envelope.event.metadata.installationEpochId);
        assert.equal(
          (
            b.database
              .prepare(
                "select payload_json as payload from buffered_events where id=?",
              )
              .get(target.id) as { payload: string }
          ).payload,
          rawBefore,
        );
        assert.equal(
          (
            b.database
              .prepare("select reason from upload_receipts where delivery_id=?")
              .get(queued.id) as { reason: string }
          ).reason,
          "local_model_capture_gap",
        );
        checks++;
        completion.check("legacy-sealed-gap-replacement");
      } finally {
        b.close();
      }
    }
    completion.complete();
    console.log(
      JSON.stringify({ proof: "codex-model-capture", checks, status: "PASS" }),
    );
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}
void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
