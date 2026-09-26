/** The bound Codex log remains the canonical usage row after response pairing. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { bindDispatch } from "../packages/collector-cli/src/dispatch-command";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { terminalPrivacyEligibilitySql } from "../packages/collector-cli/src/privacy-disposition";
import { createProofCompletion } from "./lib/proof-completion";

const proof = createProofCompletion("dispatch-pairing-integration");
const home = process.env.HOME!;
const plimsoll = process.env.PLIMSOLL_HOME!;
const sessionId = "019e9100-0000-7000-8000-00000000000a";
const observedAt = "2026-09-25T18:00:00.000Z";
const observedMs = Date.parse(observedAt);
const projectKey = `sha256:${"a".repeat(64)}`;
const traceId = "00000000000000000000000000000001";
const attr = (key: string, value: string | number) => ({
  key, value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value },
});
const nano = (milliseconds: number) => String(BigInt(milliseconds) * 1_000_000n);
const dispatchFields = {
  workItemId: "beads:eco-6hoxj.165.3",
  dispatchProjectKey: projectKey,
  attemptId: "integration-lane",
  parentAttemptId: "lead-session",
  role: "author",
  workClass: "implementation",
  complexityBand: "medium",
  techniqueId: "tech-1",
  techniqueVersion: "1",
  assignmentId: "assign-1",
  arm: "control",
  launchedBy: "fleet-delegate",
};

async function main() {
  const codexDirectory = path.join(home, ".codex", "sessions");
  fs.mkdirSync(codexDirectory, { recursive: true, mode: 0o700 });
  fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
  const config = collectorConfigSchema.parse({
    deviceId: "dev_dispatch-pairing-integration",
    uploadUrl: "http://127.0.0.1:1/unused",
  });
  const buffer = new LocalEventBuffer(path.join(plimsoll, "work-ledger.sqlite"), {
    workspaceId: config.tenantId, deviceId: config.deviceId,
    enrollmentNow: () => new Date("2026-09-25T17:00:00.000Z"),
    delivery: { enabled: true },
  });
  try {
    const installationEpochId = buffer.workspaceBinding()?.currentInstallationEpochId;
    assert.ok(installationEpochId);
    const configured = collectorConfigSchema.parse({ ...config, captureRoots: [{
      rootId: "codex-root", profileId: "codex-profile", installationEpochId,
      source: "codex", directory: codexDirectory,
    }] });
    fs.writeFileSync(path.join(plimsoll, "collector.config.json"), `${JSON.stringify(configured)}\n`, { mode: 0o600 });
    const bound = bindDispatch([
      "--session-id", sessionId, "--work-item-id", dispatchFields.workItemId,
      "--project-key", projectKey, "--attempt-id", dispatchFields.attemptId,
      "--parent-attempt-id", dispatchFields.parentAttemptId, "--role", dispatchFields.role,
      "--work-class", dispatchFields.workClass, "--complexity-band", dispatchFields.complexityBand,
      "--technique-id", dispatchFields.techniqueId,
      "--technique-version", dispatchFields.techniqueVersion,
      "--assignment-id", dispatchFields.assignmentId, "--arm", dispatchFields.arm,
      "--launched-by", dispatchFields.launchedBy,
      "--valid-from", "2026-09-25T17:00:00.000Z",
    ], new Date("2026-09-26T00:00:00.000Z"));
    assert.equal(bound.status, "dispatch_bound");
    assert.equal(bound.roots, 1);
    proof.check("session_bound_to_codex_capture_root");

    const resource = { attributes: [attr("service.name", "codex-app-server")] };
    const exploded = explodeOtlpPayload({
      resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{
        timeUnixNano: nano(observedMs), traceId,
        attributes: [
          attr("event.name", "codex.sse_event"), attr("event.kind", "response.completed"),
          attr("conversation.id", sessionId), attr("user.account_id", "synthetic-account"),
          attr("input_token_count", "24261"), attr("output_token_count", "1902"),
          attr("cached_token_count", 1190), attr("event.timestamp", observedAt),
        ],
      }] }] }],
      resourceSpans: [{ resource, scopeSpans: [{ spans: [{
        name: "handle_responses", traceId, spanId: "0000000000000001",
        startTimeUnixNano: nano(observedMs - 1_500),
        endTimeUnixNano: nano(observedMs + 20),
        attributes: [
          attr("gen_ai.usage.input_tokens", 24261),
          attr("gen_ai.usage.output_tokens", 1902),
          attr("gen_ai.usage.cache_read.input_tokens", 1190),
        ],
      }] }] }],
    }, { source: "codex" });
    assert.equal(exploded.parseFailures, 0);
    assert.equal(exploded.events.length, 2);
    const [log, span] = exploded.events;
    assert.ok(log && span);
    assert.equal(log.event.metadata.traceId, traceId);
    assert.equal(log.event.cacheReadTokens, 1190);
    for (const [field, expected] of Object.entries(dispatchFields))
      assert.equal(log.event.metadata[field], expected, field);
    for (const field of Object.keys(dispatchFields))
      assert.equal(span.event.metadata[field], undefined, field);
    proof.check("codex_log_has_pairing_trace_cache_and_dispatch_fields_span_has_none");

    assert.equal(buffer.append(log.event, log.suppressedFields), true);
    assert.equal(buffer.append(span.event, span.suppressedFields), true);
    const rows = buffer.database.prepare(`select id, event_type as eventType,
      input_tokens as inputTokens, cache_read_tokens as cacheReadTokens,
      usage_duplicate_reason as duplicateReason, payload_json as payloadJson
      from buffered_events where id in (?, ?) order by id`).all(log.event.id, span.event.id) as Array<{
      id: string; eventType: string; inputTokens: number | null; cacheReadTokens: number | null;
      duplicateReason: string | null; payloadJson: string;
    }>;
    assert.equal(rows.length, 2);
    const kept = rows.find(row => row.id === log.event.id)!;
    const neutral = rows.find(row => row.id === span.event.id)!;
    assert.equal(kept.duplicateReason, null);
    assert.equal(kept.inputTokens, 24261);
    assert.equal(kept.cacheReadTokens, 1190);
    assert.equal(neutral.duplicateReason, "codex_sse_event_span");
    assert.equal(neutral.eventType, "otel_span");
    assert.equal(neutral.inputTokens, null);
    assert.equal(neutral.cacheReadTokens, null);
    const eligibility = terminalPrivacyEligibilitySql(buffer.database, "e");
    const eligible = buffer.database.prepare(`select id from buffered_events e
      where id in (?, ?) and input_tokens is not null and ${eligibility}`)
      .all(log.event.id, span.event.id) as Array<{ id: string }>;
    assert.deepEqual(eligible.map(row => row.id), [log.event.id]);
    proof.check("pair_keeps_exactly_one_eligible_codex_usage_row");

    const keptMetadata = JSON.parse(kept.payloadJson).metadata as Record<string, unknown>;
    const neutralMetadata = JSON.parse(neutral.payloadJson).metadata as Record<string, unknown>;
    for (const [field, expected] of Object.entries(dispatchFields)) {
      assert.equal(keptMetadata[field], expected, field);
      assert.equal(neutralMetadata[field], undefined, field);
    }
    proof.check("canonical_ledger_row_keeps_dispatch_neutral_span_carries_none");

    const queued = buffer.database.prepare(`select raw_id as rawId, base_envelope_json as envelope
      from upload_outbox where raw_id in (?, ?)`).all(log.event.id, span.event.id) as
      Array<{ rawId: string; envelope: string }>;
    assert.equal(queued.length, 1);
    assert.equal(queued[0]!.rawId, log.event.id);
    const outbound = JSON.parse(queued[0]!.envelope).event;
    assert.equal(outbound.cacheReadTokens, 1190);
    for (const [field, expected] of Object.entries(dispatchFields))
      assert.equal(outbound.metadata[field], expected, field);
    proof.check("only_kept_log_queues_with_dispatch_and_cached_tokens");
  } finally {
    buffer.close();
  }
  proof.complete();
}

main().catch(error => { console.error(error); process.exitCode = 1; });
