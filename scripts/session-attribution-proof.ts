import assert from "node:assert/strict";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { DEFAULT_POLICY } from "../packages/shared/src/index";
import {
  applyProjectAttribution,
  SessionAttributionBatch,
  type SessionRepoContext,
} from "../packages/collector-cli/src/session-attribution";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";
import type { AiInteractionEvent } from "../packages/shared/src/index";

const REPO_A = `sha256:${"a".repeat(64)}`;
const REPO_B = `sha256:${"b".repeat(64)}`;

function event(id: string, observedAt: string, options: Partial<AiInteractionEvent> = {}): AiInteractionEvent {
  return {
    id,
    sessionId: "session-fixture",
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt,
    intent: "unknown",
    actionClass: "other",
    inputTokens: 10,
    outputTokens: 20,
    metadata: {},
    ...options,
  };
}

function context(rowid: number, observedAt: string, repoHash: string, sessionId = "session-fixture"): SessionRepoContext {
  return { rowid, sessionId, observedAt, repoHash };
}

function otelAttr(key: string, value: string | number) {
  return { key, value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value } };
}

function main() {
  // Reproduce the production shape: an OTLP assistant_response carries
  // tokens, while a neighboring tool_result is the event whose resolved
  // repo_hash supplies the only project evidence.
  const ledger = new LocalEventBuffer(":memory:", {
    enrollmentNow: () => new Date("2026-09-23T11:00:00.000Z"),
    workspaceId: "fixture-workspace",
    deviceId: "fixture-device",
    delivery: {
      enabled: true,
      now: () => new Date("2026-09-23T12:00:00.000Z"),
    },
  });
  const exploded = explodeOtlpPayload(
    {
      resourceLogs: [{
        resource: { attributes: [otelAttr("service.name", "codex_exec")] },
        scopeLogs: [{
          logRecords: [{
            observedTimeUnixNano: "1790164800000000000",
            attributes: [
              otelAttr("event.name", "assistant_response"),
              otelAttr("session.id", "session-fixture"),
              otelAttr("gen_ai.usage.input_tokens", 10),
              otelAttr("gen_ai.usage.output_tokens", 20),
            ],
          }],
        }],
      }],
    },
    { policy: DEFAULT_POLICY, source: "codex", transportPath: "/v1/logs" },
  );
  assert.equal(exploded.events.length, 1);
  const otelAssistant = exploded.events[0]!.event;
  assert.equal(otelAssistant.eventType, "assistant_response");
  assert.equal(otelAssistant.projectKey, undefined);
  assert.equal(otelAssistant.sessionId, "session-fixture");
  const toolResult = {
    ...event("00000000-0000-4000-8000-000000000102", "2026-09-23T11:59:00.000Z"),
    eventType: "tool_result" as const,
    inputTokens: undefined,
    outputTokens: undefined,
  };
  assert.equal(ledger.append(otelAssistant), true);
  assert.equal(ledger.append(toolResult), true);
  ledger.database
    .prepare("update buffered_events set repo_hash = ? where id = ?")
    .run(REPO_A, toolResult.id);
  // Plan the statements the lookup actually prepares, not a copy of them.
  // A ledger whose capture-time context index is complete reads it
  // (eco-6hoxj.163.21); `contextIndex: false` selects the 0.7.36 session scan,
  // which serves an older ledger until its backfill completes.
  const plannedLookup = (options: { contextIndex?: boolean }) => {
    const prepare = ledger.database.prepare;
    const lookupSql: string[] = [];
    ledger.database.prepare = ((source: string) => {
      lookupSql.push(source);
      return prepare.call(ledger.database, source);
    }) as typeof prepare;
    const attributed = new SessionAttributionBatch(ledger.database, [{ event: otelAssistant }], options)
      .attribute(otelAssistant);
    ledger.database.prepare = prepare;
    const relevantSql = lookupSql.filter((source) =>
      /\bsession_id\s*=\s*\?/.test(source) ||
      /from session_repo_context_control\s+where singleton = 1/.test(source),
    );
    const queryPlans = relevantSql.map((source) => {
      const parameterCount = (source.match(/\?/g) ?? []).length;
      const parameters = Array.from({ length: parameterCount }, (_, index) =>
        ["session-fixture", "2026-09-23T06:00:00.000Z", "2026-09-23T18:00:00.000Z", 257][index] ?? 0,
      );
      return ledger.database
        .prepare(`explain query plan ${source}`)
        .all(...parameters) as Array<{ detail: string }>;
    });
    return { attributed, queryPlans };
  };
  const scanned = plannedLookup({ contextIndex: false });
  assert.equal(scanned.queryPlans.length, 2);
  assert.ok(scanned.queryPlans.every((plan) => plan.some((row) => row.detail.includes("idx_events_session"))));
  const indexed = plannedLookup({});
  // The coverage-marker read and one primary-key range over the index.
  assert.equal(indexed.queryPlans.length, 2);
  assert.ok(indexed.queryPlans.some((plan) => plan.some((row) =>
    /SEARCH (?:session_repo_contexts|c) USING PRIMARY KEY/.test(row.detail))));
  assert.ok(indexed.queryPlans.every((plan) => plan.every((row) => !/^SCAN /.test(row.detail))));
  for (const { attributed } of [scanned, indexed]) {
    assert.equal(attributed.event.projectKey, REPO_A);
    assert.equal(attributed.event.metadata.projectBasis, "session_inherited");
  }
  const otelAttribution = indexed.attributed;
  assert.equal(sealOutboundEnvelope({ event: otelAttribution.event, suppressedFields: [] }).ok, true);
  const lease = ledger.delivery.lease({
    maxRows: 10,
    now: new Date("2026-09-23T12:01:00.000Z"),
  });
  const leasedAssistant = lease.items.find((item) => item.envelope.event.id === otelAssistant.id);
  const leasedTool = lease.items.find((item) => item.envelope.event.id === toolResult.id);
  assert.equal(leasedAssistant?.envelope.event.projectKey, REPO_A);
  assert.equal(leasedAssistant?.envelope.event.metadata.projectBasis, "session_inherited");
  assert.equal(leasedTool?.envelope.event.metadata.projectBasis, "repo_context");

  const single = applyProjectAttribution(
    event("single-token", "2026-09-23T12:00:00.000Z"),
    { sessionContexts: [context(2, "2026-09-23T11:59:00.000Z", REPO_A)] },
  );
  assert.equal(single.event.projectKey, REPO_A);
  assert.equal(single.event.metadata.projectBasis, "session_inherited");
  assert.equal(single.basis, "session_inherited");

  const multi = applyProjectAttribution(
    event("multi-token", "2026-09-23T12:00:00.000Z"),
    {
      sessionContexts: [
        context(3, "2026-09-23T11:58:00.000Z", REPO_A),
        context(4, "2026-09-23T11:59:00.000Z", REPO_B),
      ],
    },
  );
  assert.equal(multi.event.projectKey, REPO_B);
  assert.equal(multi.event.metadata.projectBasis, "session_inherited");

  const noRepo = applyProjectAttribution(
    event("none-token", "2026-09-23T12:00:00.000Z"),
    { sessionContexts: [] },
  );
  assert.equal(noRepo.event.projectKey, undefined);
  assert.equal(noRepo.event.metadata.projectBasis, "unallocated");
  assert.equal(noRepo.basis, "unallocated");

  const explicit = applyProjectAttribution(
    event("explicit-token", "2026-09-23T12:00:00.000Z", { projectKey: REPO_A }),
    { repoHash: REPO_B, sessionContexts: [context(5, "2026-09-23T11:59:00.000Z", REPO_B)] },
  );
  assert.equal(explicit.event.projectKey, REPO_A);
  assert.equal(explicit.event.metadata.projectBasis, "explicit");
  assert.equal(explicit.basis, "explicit");

  const forgedMarker = applyProjectAttribution(
    event("forged-marker-token", "2026-09-23T12:00:00.000Z", {
      projectKey: REPO_A,
      metadata: { projectBasis: "session_inherited" },
    }),
    { sessionContexts: [context(8, "2026-09-23T11:59:00.000Z", REPO_B)] },
  );
  assert.equal(forgedMarker.event.projectKey, REPO_A);
  assert.equal(forgedMarker.event.metadata.projectBasis, "explicit");

  const truncated = applyProjectAttribution(
    event("truncated-token", "2026-09-23T12:00:00.000Z"),
    {
      sessionContexts: [context(6, "2026-09-23T11:59:00.000Z", REPO_A)],
      sessionContextsTruncated: true,
    },
  );
  assert.equal(truncated.event.projectKey, undefined);
  assert.equal(truncated.event.metadata.projectBasis, "unallocated");

  const otherSession = applyProjectAttribution(
    event("other-session-token", "2026-09-23T12:00:00.000Z"),
    { sessionContexts: [context(7, "2026-09-23T11:59:00.000Z", REPO_B, "different-session")] },
  );
  assert.equal(otherSession.event.projectKey, undefined);
  assert.equal(otherSession.event.metadata.projectBasis, "unallocated");

  const stable = applyProjectAttribution(single.event, { sessionContexts: [context(2, "2026-09-23T11:59:00.000Z", REPO_A)] });
  assert.deepEqual(stable.event, single.event);
  ledger.close();
  console.log(JSON.stringify({ status: "PASS", checks: 14, fixture: "otel-assistant_response-tool_result-session" }));
}

main();
