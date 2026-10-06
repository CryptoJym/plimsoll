import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("codex-model-capture", 38);
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import {
  captureCodexModel,
  recordCodexTurnModel,
} from "../packages/collector-cli/src/codex-model-capture";
import { captureFrontier } from "../packages/collector-cli/src/capture-frontier";
import {
  deterministicEventId,
  normalizeHookPayload,
} from "../packages/collector-cli/src/normalizer";
import { runWorkspaceHistoryUpload } from "../packages/collector-cli/src/upload-history";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
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
    const captured = captureCodexModel(b.database, target);
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
      assert.equal(captured.model, expected);
      if (reason === "paired_sse_event") {
        assert.equal(result.inputTokens, undefined);
        assert.equal(captured.metadata.usageDuplicateReason, reason);
        assert.equal(captured.metadata.modelCaptureInputTokens, 5555);
        assert.equal(
          events.filter((e) => e.inputTokens === 5555).length,
          1,
          "pair counts once",
        );
      } else assert.equal(result.inputTokens, 5555);
      assert.ok(captured.metadata.modelCaptureSource);
      assert.ok(result.metadata["user.account_id"]);
    } else {
      assert.equal(captured.model, undefined);
      assert.equal(captured.metadata.captureGap, true);
      assert.equal(captured.metadata.modelGapReason, reason);
      assert.equal(captured.metadata.modelGapInputTokens, 5555);
      assert.equal(result.model, undefined);
      assert.equal(result.inputTokens, undefined);
      assert.equal(result.outputTokens, undefined);
      assert.equal(result.costUsd, undefined);
      assert.equal(result.metadata.usageSource, "capture_gap");
      assert.equal(result.metadata.captureGap, undefined);
      assert.equal(result.metadata.modelGapReason, undefined);
      assert.equal(result.metadata.modelGapInputTokens, undefined);
      if (target.metadata["user.account_id"]) {
        assert.equal(
          result.metadata["user.account_id"],
          target.metadata["user.account_id"],
        );
        assert.equal(captured.metadata.accountIdentityState, "reported");
      }
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
      "conflicting-peer-with-clean-trace-log",
      (b) => {
        const peer = log("gpt-6.1-sol", true);
        peer.id = deterministicEventId([peer.id, "conflicting-trace-peer"]);
        peer.metadata["gen_ai.request.model"] = "gpt-6-astra";
        b.append(peer);
        b.append(log("gpt-6.1-sol", true));
        return span();
      },
      null,
      "conflicting_trace_model_evidence",
    );
    await run(
      "conflicting-exact-pair-peer",
      (b) => {
        const peer = log("gpt-6.1-sol", false, true);
        peer.id = deterministicEventId([peer.id, "conflicting-pair-peer"]);
        // Exercise the fallback on an older typed shape before the durable
        // pairer can retire the span. Both logs retain their native SSE name.
        peer.eventType = "unknown";
        peer.metadata["gen_ai.request.model"] = "gpt-6-astra";
        b.append(peer);
        const clean = log("gpt-6.1-sol", false, true);
        clean.eventType = "unknown";
        b.append(clean);
        return span();
      },
      null,
      "conflicting_pair_model_evidence",
    );
    await run(
      "conflicting-local-peer-with-clean-context",
      (b) => {
        recordCodexTurnModel(
          b.database,
          session,
          "turn-conflict-peer",
          "gpt-6.1-sol",
          account,
        );
        const peer = {
          ...span(),
          eventType: "otel_span" as const,
          sessionId: session,
          model: "gpt-6.1-sol",
          inputTokens: undefined,
          outputTokens: undefined,
          cacheReadTokens: undefined,
          metadata: {
            usageSource: "codex_local_turn",
            codexTurnId: "turn-conflict-peer",
            "gen_ai.request.model": "gpt-6-astra",
          },
        };
        b.append(peer);
        return span([
          attr("conversation.id", session),
          attr("turn.id", "turn-conflict-peer"),
        ]);
      },
      null,
      "conflicting_local_model_evidence",
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
      "hook-nested-model-conflict",
      () =>
        normalizeHookPayload(
          {
            source: "codex",
            observedAt: new Date(at).toISOString(),
            model: "gpt-6.1-sol",
            inputTokens: 5555,
            outputTokens: 55,
            attributes: [attr("gen_ai.request.model", "gpt-6-astra")],
          },
          {
            source: "codex",
            now: () => at,
            fallbackEventId: deterministicEventId(["hook-nested-model-conflict"]),
          },
        ).event,
      null,
      "conflicting_model_attributes",
    );
    await run(
      "paired-actor-account-conflict",
      (b) => {
        const peer = log("gpt-6.1-sol", false, true);
        peer.metadata["user.account_id"] = "sha256:fedcba9876543210";
        b.append(peer);
        const target = span();
        target.actorId = "sha256:aaaaaaaaaaaaaaaa";
        return target;
      },
      null,
      "model_evidence_missing",
    );
    {
      // This gate owns Codex, including Codex under a Claude credential.
      // Genuine Claude missing-model observations retain their existing
      // delivery semantics and remain part of the cloud's plain unknown line.
      const b = new LocalEventBuffer(
        path.join(root, "claude-incomplete.sqlite"),
        {
          workspaceId: session,
          deviceId: "fixture-device",
          enrollmentNow: () => new Date(at - 1000),
          delivery: { enabled: true },
        },
      );
      try {
        const target: AiInteractionEvent = {
          ...span(),
          source: "claude_code",
          cacheCreationTokens: undefined,
          metadata: {
            serviceName: "claude-code",
            otelEventName: "claude_code.api_request",
          },
        };
        b.append(target);
        assert.deepEqual(captureCodexModel(b.database, target), target);
        const result = b.delivery
          .lease()
          .items.find((i) => i.envelope.event.id === target.id)!.envelope.event;
        assert.equal(result.inputTokens, target.inputTokens);
        assert.equal(result.outputTokens, target.outputTokens);
        assert.equal(result.cacheCreationTokens, undefined);
        assert.equal(result.metadata.captureGap, undefined);
        checks++;
        completion.check("genuine-claude-delivery-unchanged");
      } finally {
        b.close();
      }
    }
    await run(
      "legacy-class2-model",
      () => ({
        ...span(),
        source: "claude_code",
        metadata: { serviceName: "Codex_Desktop", "user.account_id": account },
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
            0,
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
          assert.equal(scan.unvalidatedFirstRows, 1,
            "the span does not discard rollout evidence; unknown initial counter stays excluded");
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
        assert.equal(result.batch?.events[0]?.event.metadata.usageSource, "capture_gap");
        assert.equal(result.batch?.events[0]?.event.metadata.captureGap, undefined);
        assert.equal(result.batch?.events[0]?.event.inputTokens, undefined);
        checks++;
        completion.check("stateless-upload-gaps");
      } finally {
        b.close();
      }
    }
    for (const namedClass2 of [false, true]) {
      let now = new Date(at + 2000);
      const b = new LocalEventBuffer(
        path.join(
          root,
          namedClass2 ? "legacy-named-class2.sqlite" : "legacy-sealed.sqlite",
        ),
        {
          workspaceId: session,
          deviceId: "fixture-device",
          enrollmentNow: () => new Date(at - 1000),
          delivery: { enabled: true, now: () => now },
        },
      );
      try {
        const target = span();
        if (namedClass2) {
          target.source = "claude_code";
          target.model = "gpt-6-astra";
          target.metadata.serviceName = "Codex_Desktop";
        }
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
        assert.equal(gap.envelope.event.metadata.usageSource, "capture_gap");
        assert.equal(gap.envelope.event.metadata.captureGap, undefined);
        assert.equal(
          gap.envelope.event.metadata.modelGapReason,
          undefined,
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
        completion.check(
          namedClass2
            ? "legacy-sealed-class2-gap"
            : "legacy-sealed-gap-replacement",
        );
      } finally {
        b.close();
      }
    }
    await run(
      "paired-model-cannot-overwrite-native-trace",
      (b) => {
        // Missing typed pairing shape on an older ledger cannot hide a
        // contradictory native model on the target or witness trace. The
        // target gaps before fresh pair finance can be selected.
        const nativeLog = log("gpt-6.1-sol", false, true);
        b.append(nativeLog);
        b.database
          .prepare(
            "update buffered_events set event_type='otel_log' where id=?",
          )
          .run(nativeLog.id);
        b.append(log("gpt-6-astra", true));
        return span();
      },
      null,
      "conflicting_pair_target_model",
    );
    await run(
      "two-model-pair",
      (b) => {
        b.append(log("gpt-6.1-sol", false, true));
        b.append(log("gpt-6-astra", false, true));
        return span();
      },
      null,
      "ambiguous_pair_model",
    );
    await run(
      "foreign-account",
      (b) => {
        b.append(log("gpt-6-astra", true));
        return span([attr("user.account_id", "sha256:fedcba9876543210")]);
      },
      null,
      "ambiguous_trace_identity",
    );
    await run(
      "resource-only-model",
      () => {
        resource.attributes.push(attr("gen_ai.request.model", "gpt-6-astra"));
        try {
          return span();
        } finally {
          resource.attributes.pop();
        }
      },
      null,
      "model_evidence_missing",
    );
    await run(
      "no-model-hook",
      () =>
        normalizeHookPayload(
          {
            inputTokens: 5555,
            outputTokens: 55,
            cacheReadTokens: 5000,
            observedAt: new Date(at).toISOString(),
            eventType: "assistant_response",
          },
          { source: "codex", now: () => at + 1000 },
        ).event,
      null,
      "model_evidence_missing",
    );
    await run(
      "named-class2-model",
      () => ({
        ...span(),
        model: "gpt-6-astra",
        source: "claude_code",
        metadata: { serviceName: "Codex_Desktop" },
      }),
      null,
      "codex_service_under_claude_source",
    );
    {
      const ledgerPath = path.join(root, "readonly-history.sqlite");
      const b = new LocalEventBuffer(ledgerPath, {
        workspaceId: session,
        deviceId: "fixture-device",
        enrollmentNow: () => new Date(at - 1000),
      });
      try {
        b.append(span());
        const snapshot = () => JSON.stringify(b.database.prepare("select * from buffered_events").all());
        const before = snapshot();
        const sent: AiInteractionEvent[] = [];
        const cfg = collectorConfigSchema.parse({
          tenantId: session,
          deviceId: "fixture-device",
          installKey: "fixture-install",
          uploadUrl: "http://127.0.0.1:49997/api/ai-work/ingest",
        });
        const result = await runWorkspaceHistoryUpload(cfg, {
          ledgerPath,
          statePath: path.join(root, "history-state.json"),
          full: true,
          delayMs: 0,
          developmentLoopbackUrl: true,
          now: () => new Date(Date.now() + 61000),
          sleep: async () => {},
          log: () => {},
          fetchImpl: async (_url, init) => {
            const body = String(init?.body);
            const batch = JSON.parse(body);
            sent.push(
              ...batch.events.map(
                (e: { event: AiInteractionEvent }) => e.event,
              ),
            );
            return new Response(
              JSON.stringify(acceptedFixtureDelivery(body, cfg.installKey)),
              {
                status: 200,
                headers: { "content-type": "application/json" },
              },
            );
          },
        });
        assert.equal(result.ok, true);
        assert.equal(sent.length, 1);
        assert.equal(sent[0]!.metadata.usageSource, "capture_gap");
        assert.equal(sent[0]!.metadata.captureGap, undefined);
        assert.equal(sent[0]!.inputTokens, undefined);
        assert.ok(sent[0]!.metadata.installationEpochId);
        assert.equal(snapshot(), before, "history model gate leaves raw rows unchanged");
        assert.equal(
          (b.database.prepare("select count(*) as n from codex_capture_decisions where decision='gap'").get() as { n: number }).n,
          1,
          "history model gap is durable by raw lineage",
        );
        checks++;
        completion.check("readonly-history-gap");
      } finally {
        b.close();
      }
    }
    {
      const b = new LocalEventBuffer(path.join(root, "zero-token.sqlite"), {
        workspaceId: session,
        deviceId: "fixture-device",
        enrollmentNow: () => new Date(at - 1000),
      });
      try {
        const event = {
          ...span(),
          inputTokens: 0,
          outputTokens: 0,
          cacheReadTokens: 0,
        };
        b.append(event);
        const result = buildIngestBatch(
          collectorConfigSchema.parse({
            tenantId: session,
            deviceId: "fixture-device",
            installKey: "fixture-install",
          }),
          b,
          { now: () => new Date(Date.now() + 61000) },
        );
        const captured = captureCodexModel(b.database, event);
        assert.equal(captured.metadata.captureGap, true);
        assert.equal(captured.metadata.modelGapInputTokens, 0);
        assert.equal(result.batch?.events[0]?.event.metadata.usageSource, "capture_gap");
        assert.equal(result.batch?.events[0]?.event.metadata.captureGap, undefined);
        assert.equal(result.batch?.events[0]?.event.inputTokens, undefined);
        checks++;
        completion.check("zero-token-model-gap");
      } finally {
        b.close();
      }
    }
    {
      const b = new LocalEventBuffer(
        path.join(root, "nonfinancial-observer.sqlite"),
      );
      try {
        const observer = { ...span(), eventType: "usage_live" as const };
        assert.equal(
          captureCodexModel(b.database, observer),
          observer,
          "nonfinancial runtime intervals never acquire a per-request model",
        );
        checks++;
        completion.check("unqualified-observer-preserved");
      } finally {
        b.close();
      }
    }
    {
      const b = new LocalEventBuffer(
        path.join(root, "unbound-evidence.sqlite"),
      );
      try {
        b.append(log("gpt-6-astra", true));
        const target = span();
        b.append(target);
        const captured = captureCodexModel(b.database, target);
        assert.equal(captured.model, undefined);
        assert.equal(captured.inputTokens, undefined);
        assert.equal(
          captured.metadata.modelGapReason,
          "capture_identity_missing",
        );
        checks++;
        completion.check("unbound-evidence-is-not-a-model");
      } finally {
        b.close();
      }
    }
    await run(
      "local-turn-account-preserved",
      (b) => {
        recordCodexTurnModel(
          b.database,
          session,
          "turn-account",
          "gpt-6.1-sol",
          account,
        );
        return span([
          attr("conversation.id", session),
          attr("turn.id", "turn-account"),
        ]);
      },
      "gpt-6.1-sol",
    );
    await run(
      "local-turn-account-conflict",
      (b) => {
        recordCodexTurnModel(
          b.database,
          session,
          "turn-conflict",
          "gpt-6-astra",
          account,
        );
        return span([
          attr("conversation.id", session),
          attr("turn.id", "turn-conflict"),
          attr("user.account_id", "sha256:fedcba9876543210"),
        ]);
      },
      null,
      "ambiguous_local_turn_identity",
    );
    await run(
      "conflicting-model-peer",
      (b) => {
        const peer = log("gpt-6.1-sol", true);
        peer.metadata["gen_ai.request.model"] = "gpt-6-astra";
        b.append(peer);
        return span();
      },
      null,
      "conflicting_trace_model_evidence",
    );
    await run(
      "one-trace-model-two-accounts",
      (b) => {
        b.append(log("gpt-6.1-sol", true));
        const peer = log("gpt-6.1-sol", true);
        peer.id = deterministicEventId([peer.id, "second-account"]);
        peer.metadata["user.account_id"] = "sha256:fedcba9876543210";
        peer.actorId = undefined;
        b.append(peer);
        return span();
      },
      null,
      "ambiguous_trace_identity",
    );
    await run(
      "one-local-turn-model-two-accounts",
      (b) => {
        recordCodexTurnModel(
          b.database,
          session,
          "turn-two-accounts",
          "gpt-6.1-sol",
          account,
        );
        recordCodexTurnModel(
          b.database,
          session,
          "turn-two-accounts",
          "gpt-6.1-sol",
          "sha256:fedcba9876543210",
        );
        return span([
          attr("conversation.id", session),
          attr("turn.id", "turn-two-accounts"),
        ]);
      },
      null,
      "ambiguous_local_turn_identity",
    );
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
