#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { appendForwardedHook } from "../packages/collector-cli/src/forwarder";
import {
  buildWorkEpisodeFact,
} from "../packages/collector-cli/src/learning-facts";
import {
  ensureRuntimeFactDropSchema,
  promoteRuntimeLearningFacts,
  recordExplicitTechniqueAssignment,
  runtimeCorrelationKeyOf,
  runtimeFactDropCounters,
} from "../packages/collector-cli/src/runtime-facts";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import {
  aiInteractionEventSchema,
} from "../packages/shared/src/index";

const SCHEMA = "plimsoll.runtime-facts-proof.v1" as const;

const HOSTILE = {
  command: "COMMAND156_Q4W8_private_shell_command",
  secret: "SECRET156_R7T2_private_credential",
  hookOpKey: "opk-hook77x4-sentinel-156",
  otlpOpKeys: ["opk-otlpa31-sentinel", "opk-otlpb62-sentinel", "opk-otlpc93-sentinel"],
} as const;

const BASE_MS = Date.parse("2026-08-25T10:00:00.000Z");

function iso(offsetMs: number) {
  return new Date(BASE_MS + offsetMs).toISOString();
}

function nano(offsetMs: number) {
  return String((BASE_MS + offsetMs) * 1_000_000);
}

const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];
let proofStage = "startup";

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function factTableText(db: Database.Database) {
  const tables = [
    "work_episode_facts",
    "tool_attempt_facts",
    "technique_exposure_facts",
    "technique_identity_registry",
    "runtime_fact_drops",
  ];
  const existing = new Set(
    (db.prepare(`select name from sqlite_master where type='table'`).all() as Array<{ name: string }>)
      .map((row) => row.name),
  );
  const values: string[] = [];
  for (const table of tables) {
    if (!existing.has(table)) continue;
    const rows = db.prepare(`select * from ${table}`).all() as Array<Record<string, unknown>>;
    values.push(JSON.stringify(rows));
  }
  return values.join("\n");
}

function kv(key: string, value: string | number | boolean) {
  return { key, value: { stringValue: String(value) } };
}

function toolSpan(input: {
  spanId: string;
  callId: string;
  session: string;
  startOffsetMs: number;
  endOffsetMs: number;
  status?: "OK" | "ERROR";
  retryOf?: string;
}) {
  const attributes = [
    kv("gen_ai.tool.name", "Bash"),
    kv("session_id", input.session),
    kv("call_id", input.callId),
  ];
  if (input.retryOf) attributes.push(kv("plimsoll.retry_of", input.retryOf));
  return {
    traceId: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    spanId: input.spanId,
    name: "execute_tool bash",
    startTimeUnixNano: nano(input.startOffsetMs),
    endTimeUnixNano: nano(input.endOffsetMs),
    attributes,
    ...(input.status ? { status: { code: input.status } } : {}),
  };
}

function otlpEnvelope(spans: ReturnType<typeof toolSpan>[]) {
  return {
    resourceSpans: [
      {
        resource: { attributes: [kv("service.name", "codex")] },
        scopeSpans: [{ spans }],
      },
    ],
  };
}

async function main() {
  check("node_22_runtime", Number(process.versions.node.split(".")[0]) === 22, {
    nodeMajor: Number(process.versions.node.split(".")[0]),
  });
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-runtime-facts-proof-"));
  const ledger = path.join(root, "facts.sqlite");
  let buffer: LocalEventBuffer | undefined;
  const config = collectorConfigSchema.parse({
    uploadUrl: "http://127.0.0.1/fake-runtime-facts-ingest",
    installKey: "runtime-facts-proof-install",
  });
  try {
    proofStage = "buffer_open";
    buffer = new LocalEventBuffer(ledger, {
      workspaceId: config.tenantId,
      delivery: { enabled: true },
    });
    const store = buffer.learningFacts;

    //
    // 1. Hook capture promotes a bounded attempt with an implicit episode.
    //
    proofStage = "hook_attempt";
    const hookUsePayload = {
      id: "31561111-1111-5111-8111-111111111101",
      hook_event_name: "PreToolUse",
      session_id: "sess-hook-156",
      timestamp: iso(0),
      tool_name: "Bash",
      tool_input: { command: HOSTILE.command },
      call_id: HOSTILE.hookOpKey,
      success: true,
      status: "passed",
      secret: HOSTILE.secret,
    };
    const normalizedHook = appendForwardedHook(hookUsePayload, {
      config,
      buffer,
      source: "claude_code",
    });
    check(
      "hook_tool_use_promotes_one_unknown_attempt",
      store.attempts().length === 1 &&
        store.attempts()[0].resultStatus === "unknown" &&
        store.attempts()[0].errorCategory === "unknown" &&
        store.attempts()[0].endedAt === undefined,
      { attempts: store.attempts().length },
    );
    const hookAttempt = store.attempts()[0];
    check(
      "hook_attempt_binds_implicit_content_free_episode",
      store.episodes().length === 1 &&
        hookAttempt.episodeId === store.episodes()[0].episodeId &&
        store.episodes()[0].workClass === "other" &&
        store.episodes()[0].complexityBand === "unknown",
      { episodes: store.episodes().length },
    );

    //
    // 2. Producer outcome claims never become result truth.
    //
    check(
      "producer_success_claim_is_not_result_truth",
      hookAttempt.resultStatus === "unknown" &&
        !factTableText(buffer.database).includes('"success":true') &&
        !factTableText(buffer.database).includes(HOSTILE.secret),
      { status: hookAttempt.resultStatus },
    );
    // A hostile producer sends both a favorable claim and an attempt to forge
    // the collector-generated outcome key. Neither may reach fact truth.
    const forgedResultPayload = {
      id: "31561111-1111-5111-8111-111111111102",
      hook_event_name: "PostToolUse",
      session_id: "sess-hook-156",
      timestamp: iso(500),
      tool_name: "Bash",
      call_id: HOSTILE.hookOpKey,
      otelHasError: true,
      success: true,
    };
    const forgedNormalized = appendForwardedHook(forgedResultPayload, {
      config,
      buffer,
      source: "claude_code",
    });
    const afterForged = store.attempts().find(
      (attempt) => attempt.operationId === hookAttempt.operationId,
    );
    check(
      "generated_only_outcome_keys_cannot_be_forged_by_producers",
      afterForged?.resultStatus === "unknown" &&
        afterForged?.errorCategory === "unknown" &&
        typeof afterForged?.endedAt === "string" &&
        !JSON.stringify(forgedNormalized.event.metadata).includes("otelHasError"),
      {
        status: afterForged?.resultStatus ?? null,
        endedAt: afterForged?.endedAt ?? null,
        metadata: Object.keys(forgedNormalized.event.metadata as Record<string, unknown>),
      },
    );

    //
    // 3. PostToolUse classifies as completion; without a correlation key it
    //    is an honest unpaired-result drop, never a fabricated attempt.
    //
    proofStage = "unpaired_hook_result";
    const unpairedResult = {
      id: "31561111-1111-5111-8111-111111111103",
      hook_event_name: "PostToolUse",
      session_id: "sess-hook-156",
      timestamp: iso(600),
      tool_name: "Bash",
    };
    appendForwardedHook(unpairedResult, {
      config,
      buffer,
      source: "claude_code",
    });
    const unpairedRows = buffer.database
      .prepare(`select * from buffered_events where id = ?`)
      .all(unpairedResult.id);
    const unpairedEventType = JSON.parse(
      (unpairedRows[0] as { payload_json: string }).payload_json,
    ).eventType;
    check(
      "post_tool_hooks_classify_as_tool_result_not_new_starts",
      unpairedRows.length === 1 && unpairedEventType === "tool_result",
      { eventType: unpairedEventType },
    );
    const dropCounters = () => runtimeFactDropCounters(buffer!.database);
    check(
      "unpaired_results_drop_with_a_bounded_counter",
      store.attempts().length === 1 &&
        dropCounters().some(
          (row) => row.reason === "unpaired_result" && row.droppedCount === 1,
        ),
      { attempts: store.attempts().length, drops: dropCounters() },
    );

    //
    // 4. Replay of an already-appended event appends no facts twice.
    //
    proofStage = "replay";
    const replayed = appendForwardedHook(hookUsePayload, {
      config,
      buffer,
      source: "claude_code",
    });
    promoteRuntimeLearningFacts(buffer, replayed.event);
    check(
      "event_and_signal_replays_are_idempotent",
      store.attempts().length === 1 && store.episodes().length === 1,
      { attempts: store.attempts().length, episodes: store.episodes().length },
    );

    //
    // 5. OTLP spans: fail -> fail -> pass with explicit retry chain.
    //
    proofStage = "otlp_chain";
    const envelope = otlpEnvelope([
      toolSpan({
        spanId: "bbbbbbbbbbbbbbbb",
        callId: HOSTILE.otlpOpKeys[0],
        session: "sess-otlp-156",
        startOffsetMs: 1_000,
        endOffsetMs: 2_000,
        status: "ERROR",
      }),
      toolSpan({
        spanId: "cccccccccccccccc",
        callId: HOSTILE.otlpOpKeys[1],
        session: "sess-otlp-156",
        startOffsetMs: 3_000,
        endOffsetMs: 4_000,
        status: "ERROR",
        retryOf: HOSTILE.otlpOpKeys[0],
      }),
      toolSpan({
        spanId: "dddddddddddddddd",
        callId: HOSTILE.otlpOpKeys[2],
        session: "sess-otlp-156",
        startOffsetMs: 5_000,
        endOffsetMs: 6_000,
        status: "OK",
        retryOf: HOSTILE.otlpOpKeys[1],
      }),
    ]);
    const exploded = explodeOtlpPayload(envelope, {
      policy: config.policy,
      source: "codex",
      transportPath: "/v1/traces",
    });
    buffer.appendMany(exploded.events);
    const chain = store
      .attempts()
      .filter((attempt) => attempt.sessionId === "sess-otlp-156")
      .sort((a, b) => a.startedAt.localeCompare(b.startedAt));
    check(
      "fail_fail_pass_yields_three_attempts_with_distinct_outcomes",
      chain.length === 3 &&
        chain[0].resultStatus === "failure" &&
        chain[1].resultStatus === "failure" &&
        chain[2].resultStatus === "success",
      { statuses: chain.map((attempt) => attempt.resultStatus) },
    );
    check(
      "explicit_retry_relationships_are_preserved_end_to_end",
      chain[0].retryOf === undefined &&
        chain[1].retryOf === chain[0].operationId &&
        chain[2].retryOf === chain[1].operationId,
      { links: chain.filter((attempt) => attempt.retryOf).length },
    );
    check(
      "failures_carry_known_status_without_invented_error_categories",
      chain[0].errorCategory === "unknown" &&
        chain[2].errorCategory === "none" &&
        chain.every((attempt) => typeof attempt.durationMs === "number"),
      { categories: chain.map((attempt) => attempt.errorCategory) },
    );
    const otlpSessionEpisodes = store
      .episodes()
      .filter((episode) => episode.sessionId === "sess-otlp-156");
    check(
      "otlp_attempts_share_the_session_episode",
      otlpSessionEpisodes.length === 1 &&
        chain.every((attempt) => attempt.episodeId === otlpSessionEpisodes[0].episodeId),
      { episodes: otlpSessionEpisodes.length },
    );

    //
    // 6. Raw correlation keys persist only as ledger-local one-way identity.
    //
    proofStage = "correlation_privacy";
    const factText = factTableText(buffer.database);
    check(
      "fact_tables_hold_only_one_way_operation_identity",
      !factText.includes(HOSTILE.hookOpKey) &&
        !HOSTILE.otlpOpKeys.some((key) => factText.includes(key)) &&
        !HOSTILE.otlpOpKeys.some((key) =>
          chain.some((attempt) => attempt.operationId.includes(key)),
        ),
      { rawKeysInFacts: 0 },
    );
    const ephemeralProbe = aiInteractionEventSchema.parse({
      ...normalizedHook.event,
      metadata: { call_id: HOSTILE.hookOpKey },
    });
    check(
      "correlation_key_reader_sees_only_validated_top_level_metadata",
      runtimeCorrelationKeyOf(ephemeralProbe) === HOSTILE.hookOpKey &&
        runtimeCorrelationKeyOf(aiInteractionEventSchema.parse({
          ...normalizedHook.event,
          metadata: { nested: { call_id: HOSTILE.hookOpKey } },
        })) === undefined,
      { shallowOnly: true },
    );

    //
    // 7. Episodes: explicit parent linkage with fail-closed validation.
    //
    proofStage = "parent_linkage";
    const sessionEpisode = store
      .episodes()
      .find((episode) => episode.sessionId === "sess-hook-156")!;
    const childEpisode = buildWorkEpisodeFact({
      source: "claude_code",
      sessionId: "sess-hook-156",
      sourceEpisodeKey: "explicit-subtask",
      workClass: "debugging",
      complexityBand: "high",
      parentEpisodeId: sessionEpisode.episodeId,
      startedAt: iso(700),
    });
    const childWrite = store.recordWorkEpisode(childEpisode);
    check(
      "explicit_episodes_accept_declared_parent_linkage",
      childWrite.inserted === true &&
        store.episodes().find((e) => e.episodeId === childEpisode.episodeId)
          ?.parentEpisodeId === sessionEpisode.episodeId,
      { parentLinked: true },
    );
    const repeatChild = store.recordWorkEpisode(childEpisode);
    check(
      "episode_replay_is_idempotent_including_parent_linkage",
      repeatChild.inserted === false &&
        repeatChild.fact.parentEpisodeId === sessionEpisode.episodeId,
      { inserted: repeatChild.inserted },
    );
    const orphanChild = buildWorkEpisodeFact({
      source: "claude_code",
      sessionId: "sess-hook-156",
      sourceEpisodeKey: "orphan-child",
      workClass: "review",
      complexityBand: "low",
      parentEpisodeId: "00000000-0000-5000-9000-000001567777",
      startedAt: iso(800),
    });
    assert.throws(
      () => store.recordWorkEpisode(orphanChild),
      /WorkEpisodeParentMissing/,
    );
    const crossSessionChild = buildWorkEpisodeFact({
      source: "codex",
      sessionId: "sess-otlp-156",
      sourceEpisodeKey: "cross-session-child",
      workClass: "review",
      complexityBand: "low",
      parentEpisodeId: sessionEpisode.episodeId,
      startedAt: iso(900),
    });
    assert.throws(
      () => store.recordWorkEpisode(crossSessionChild),
      /WorkEpisodeParentIdentityConflict/,
    );
    const selfReferencingId = buildWorkEpisodeFact({
      source: "claude_code",
      sessionId: "sess-hook-156",
      sourceEpisodeKey: "self-parent",
      workClass: "review",
      complexityBand: "low",
      startedAt: iso(950),
    }).episodeId;
    assert.throws(
      () =>
        buildWorkEpisodeFact({
          source: "claude_code",
          sessionId: "sess-hook-156",
          sourceEpisodeKey: "self-parent",
          workClass: "review",
          complexityBand: "low",
          parentEpisodeId: selfReferencingId,
          startedAt: iso(950),
        }),
    );
    checks.push({
      name: "self_parent_linkage_rejected_before_any_io",
      detail: { rejected: true },
    });

    //
    // 8. Exposure exists only through explicit prospective assignment.
    //
    proofStage = "explicit_exposure";
    check(
      "capture_traffic_never_infers_exposure",
      store.exposures().length === 0,
      { exposures: store.exposures().length },
    );
    const exposure = recordExplicitTechniqueAssignment(buffer, {
      episodeId: sessionEpisode.episodeId,
      techniqueId: "bounded-retry-playbook",
      techniqueVersion: "1.0.0",
      contentDigest: `sha256:${"b".repeat(64)}`,
      assignmentId: "assignment-156-a",
      workClass: "other",
      complexityBand: "unknown",
      exposedAt: iso(400),
      mode: "treatment",
    });
    check(
      "explicit_prospective_assignment_records_exposure_only",
      exposure.inserted === true &&
        store.exposures().length === 1 &&
        store.exposures()[0].assertion === "exposure_only",
      { exposures: store.exposures().length },
    );
    assert.throws(
      () =>
        recordExplicitTechniqueAssignment(buffer!, {
          episodeId: sessionEpisode.episodeId,
          techniqueId: "bounded-retry-playbook",
          techniqueVersion: "1.0.0",
          assignmentId: "assignment-156-late",
          workClass: "other",
          complexityBand: "unknown",
          exposedAt: new Date(Date.now() + 3_600_000).toISOString(),
          mode: "control",
        }),
      /RetrospectiveTechniqueExposureRejected/,
    );
    checks.push({
      name: "retrospective_assignment_via_wrapper_is_rejected",
      detail: { exposuresUnchanged: store.exposures().length === 1 },
    });

    //
    // 9. Capacity pressure degrades facts, never capture.
    //
    proofStage = "capacity_resilience";
    const cappedLedger = path.join(root, "capped.sqlite");
    const capped = new LocalEventBuffer(cappedLedger, {
      learningFacts: { limits: { attempts: 1, episodes: 10_000, exposures: 10_000, techniqueIdentities: 256 } },
    });
    const cappedEvents = [0, 1].map((index) => ({
      id: `31562222-2222-5222-8222-22222222220${index}`,
      hook_event_name: "PreToolUse",
      session_id: `sess-cap-${index}`,
      timestamp: iso(index),
      tool_name: "Read",
      call_id: `cap-op-key-${index}`,
    }));
    let captureAccepted = 0;
    for (const payload of cappedEvents) {
      appendForwardedHook(payload, { config, buffer: capped, source: "claude_code" });
      captureAccepted += 1;
    }
    const cappedDrops = runtimeFactDropCounters(capped.database);
    const cappedEventRows = (
      capped.database.prepare(`select count(*) as n from buffered_events`).get() as { n: number }
    ).n;
    check(
      "capacity_exceeded_degrades_facts_without_blocking_capture",
      captureAccepted === 2 &&
        cappedEventRows === 2 &&
        capped.learningFacts.attempts().length === 1 &&
        cappedDrops.some((row) => row.reason === "capacity_exceeded"),
      {
        captureAccepted,
        eventRows: cappedEventRows,
        attempts: capped.learningFacts.attempts().length,
        drops: cappedDrops,
      },
    );
    capped.close();

    //
    // 10. Bounded drop accounting.
    //
    ensureRuntimeFactDropSchema(buffer.database);
    const finalDrops = runtimeFactDropCounters(buffer.database);
    check(
      "drop_accounting_stays_one_row_per_reason",
      finalDrops.length <= 7 &&
        finalDrops.every((row) => Number.isSafeInteger(row.droppedCount) && row.droppedCount >= 1),
      { reasons: finalDrops.map((row) => row.reason) },
    );

    const measurements = {
      attempts: store.attempts().length,
      knownFailures: store.attempts().filter((a) => a.resultStatus === "failure").length,
      knownSuccesses: store.attempts().filter((a) => a.resultStatus === "success").length,
      retryLinks: store.attempts().filter((a) => a.retryOf).length,
      episodes: store.episodes().length,
      explicitExposures: store.exposures().length,
      dropReasons: finalDrops.map((row) => `${row.reason}:${row.droppedCount}`),
      nodeMajor: Number(process.versions.node.split(".")[0]),
    };
    process.stdout.write(`${JSON.stringify({
      schema: SCHEMA,
      passed: true,
      checks: checks.length,
      measurements,
      liveStateTouched: false,
      providerNetworkCalled: false,
      backgroundScansStarted: false,
      llmCalled: false,
    }, null, 2)}\n`);
  } finally {
    buffer?.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : "";
  const safePrefix = message.match(/^([a-zA-Z0-9_.:-]{1,120})/)?.[1];
  process.stderr.write(`${JSON.stringify({
    schema: SCHEMA,
    passed: false,
    errorClass: error instanceof Error ? error.name : "UnknownError",
    errorCode: safePrefix ?? "RuntimeFactsProofAssertionFailed",
    proofStage,
  })}\n`);
  process.exitCode = 1;
});
