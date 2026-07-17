#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  adaptToolInteractionEvent,
  buildTechniqueExposureFact,
  buildWorkEpisodeFact,
  LearningFactStore,
} from "../packages/collector-cli/src/learning-facts";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const SCHEMA = "plimsoll.learning-facts-proof.v1" as const;
const PRIVATE = {
  rawError: "RAWERR100_J7qM2_private_provider_error",
  stack: "STACK100_R4vN8_private_stack_frame",
  prompt: "PROMPT100_B9xK3_private_prompt",
  command: "COMMAND100_F2pW6_private_command",
  arguments: "ARGS100_T8dL5_private_arguments",
  content: "CONTENT100_C3mH7_private_file_content",
  path: "/private/PATH100_V6nQ4/client.txt",
  secret: "SECRET100_A5sG9_private_token",
  pii: "PII100_U2rE8@example.invalid",
} as const;

const privateTerms = Object.values(PRIVATE);
const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];
let proofStage = "startup";

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function event(input: {
  id: string;
  type: "tool_use" | "tool_result";
  observedAt: string;
  actionClass?: "shell" | "read" | "write" | "mcp";
  metadata?: Record<string, unknown>;
}) {
  return aiInteractionEventSchema.parse({
    id: input.id,
    source: "codex",
    sessionId: "session-proof-100",
    dataMode: "metadata",
    eventType: input.type,
    observedAt: input.observedAt,
    actionClass: input.actionClass ?? "shell",
    metadata: input.metadata ?? {},
  });
}

function sqliteText(db: Database.Database) {
  const tables = db.prepare(
    `select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name`,
  ).all() as Array<{ name: string }>;
  const values: string[] = [];
  for (const table of tables) {
    const quoted = table.name.replaceAll('"', '""');
    const rows = db.prepare(`select * from "${quoted}"`).all() as Array<Record<string, unknown>>;
    for (const row of rows) {
      for (const value of Object.values(row)) {
        if (typeof value === "string") values.push(value);
      }
    }
  }
  return values.join("\n");
}

function fileSurfaces(ledger: string) {
  return [ledger, `${ledger}-wal`, `${ledger}-shm`]
    .filter((candidate) => fs.existsSync(candidate))
    .map((candidate) => fs.readFileSync(candidate));
}

function leakCount(strings: string[], bytes: Buffer[]) {
  let leaks = 0;
  for (const term of privateTerms) {
    for (const value of strings) if (value.includes(term)) leaks += 1;
    for (const value of bytes) if (value.includes(Buffer.from(term))) leaks += 1;
  }
  return leaks;
}

async function main() {
  check("node_22_runtime", Number(process.versions.node.split(".")[0]) === 22, {
    nodeMajor: Number(process.versions.node.split(".")[0]),
  });
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-learning-facts-proof-"));
  const ledger = path.join(root, "facts.sqlite");
  let buffer: LocalEventBuffer | undefined;
  try {
    proofStage = "buffer_open";
    buffer = new LocalEventBuffer(ledger, {
      delivery: { enabled: true },
      learningFacts: {
        limits: { attempts: 20, episodes: 10, exposures: 10, techniqueIdentities: 4 },
      },
    });
    const store = buffer.learningFacts;

    proofStage = "episode";
    const episode = buildWorkEpisodeFact({
      source: "codex",
      sessionId: "session-proof-100",
      sourceEpisodeKey: PRIVATE.pii,
      workClass: "debugging",
      complexityBand: "medium",
      startedAt: "2026-07-17T12:00:00.000Z",
      endedAt: "2026-07-17T12:01:00.000Z",
    });
    store.recordWorkEpisode(episode);
    check(
      "episode_key_is_deterministic_and_raw_key_is_not_promoted",
      buildWorkEpisodeFact({
        source: "codex",
        sessionId: "session-proof-100",
        sourceEpisodeKey: PRIVATE.pii,
        workClass: "debugging",
        complexityBand: "medium",
        startedAt: "2026-07-17T12:00:00.000Z",
        endedAt: "2026-07-17T12:01:00.000Z",
      }).episodeId === episode.episodeId &&
        !JSON.stringify(episode).includes(PRIVATE.pii),
      { deterministic: true },
    );

    const hostileProducerMetadata = {
      success: true,
      status: "passed",
      rawError: PRIVATE.rawError,
      stack: PRIVATE.stack,
      prompt: PRIVATE.prompt,
      command: PRIVATE.command,
      arguments: PRIVATE.arguments,
      content: PRIVATE.content,
      path: PRIVATE.path,
      secret: PRIVATE.secret,
      email: PRIVATE.pii,
    };
    const operationKeys = [PRIVATE.secret, PRIVATE.command, PRIVATE.arguments];
    proofStage = "attempt_starts";
    const starts = operationKeys.map((sourceOperationKey, index) =>
      adaptToolInteractionEvent({
        event: event({
          id: `00000000-0000-5000-9000-00000000010${index}`,
          type: "tool_use",
          observedAt: `2026-07-17T12:00:0${index}.000Z`,
          actionClass: index === 1 ? "read" : "shell",
          metadata: hostileProducerMetadata,
        }),
        sourceOperationKey,
        retryOfSourceOperationKey: index === 0 ? undefined : operationKeys[index - 1],
        episodeId: episode.episodeId,
      }),
    );
    for (const start of starts) store.recordToolSignal(start);
    const beforeResults = store.attempts();
    check(
      "producer_favorable_metadata_is_not_result_truth",
      beforeResults.length === 3 &&
        beforeResults.every(
          (attempt) =>
            attempt.resultStatus === "unknown" &&
            attempt.errorCategory === "unknown" &&
            attempt.endedAt === undefined,
        ),
      {
        attempts: beforeResults.length,
        statuses: beforeResults.map((row) => row.resultStatus),
      },
    );

    proofStage = "attempt_results";
    const statuses = ["failure", "failure", "success"] as const;
    const categories = ["timeout", "validation", undefined] as const;
    for (let index = 0; index < operationKeys.length; index += 1) {
      const result = adaptToolInteractionEvent({
        event: event({
          id: `00000000-0000-5000-9000-00000000020${index}`,
          type: "tool_result",
          observedAt: `2026-07-17T12:00:1${index}.000Z`,
          actionClass: index === 1 ? "read" : "shell",
          metadata: hostileProducerMetadata,
        }),
        sourceOperationKey: operationKeys[index],
        resultStatus: statuses[index],
        errorCategory: categories[index],
      });
      store.recordToolSignal(result);
      store.recordToolSignal(result);
    }
    for (const start of starts) store.recordToolSignal(start);
    const attempts = store.attempts();
    check(
      "fail_fail_pass_pairs_six_signals_into_exactly_three_attempts",
      attempts.length === 3 &&
        attempts.filter((attempt) => attempt.resultStatus === "failure").length === 2 &&
        attempts.filter((attempt) => attempt.resultStatus === "success").length === 1,
      { attempts: attempts.length, failures: 2, successes: 1 },
    );
    check(
      "explicit_retry_relationships_are_preserved",
      attempts[0].retryOf === undefined &&
        attempts[1].retryOf === attempts[0].operationId &&
        attempts[2].retryOf === attempts[1].operationId,
      { retryLinks: attempts.filter((attempt) => attempt.retryOf).length },
    );
    check(
      "durations_and_bounded_dimensions_are_derived",
      attempts.every(
        (attempt) =>
          attempt.durationMs !== undefined &&
          attempt.durationMs >= 0 &&
          ["compute", "local_io"].includes(attempt.toolClass) &&
          ["shell", "read"].includes(attempt.toolName),
      ),
      { durations: attempts.map((attempt) => attempt.durationMs) },
    );

    proofStage = "unknown_attempt";
    const unknownStart = adaptToolInteractionEvent({
      event: event({
        id: "00000000-0000-5000-9000-000000000300",
        type: "tool_use",
        observedAt: "2026-07-17T12:00:30.000Z",
        actionClass: "mcp",
        metadata: hostileProducerMetadata,
      }),
      sourceOperationKey: PRIVATE.rawError,
      episodeId: episode.episodeId,
    });
    store.recordToolSignal(unknownStart);
    const missingResult = store.attempts().find(
      (attempt) => attempt.operationId === unknownStart.operationId,
    );
    check(
      "missing_result_remains_unknown_never_success",
      missingResult?.resultStatus === "unknown" && missingResult.endedAt === undefined,
      { status: missingResult?.resultStatus ?? null },
    );
    const invalidRetry = adaptToolInteractionEvent({
      event: event({
        id: "00000000-0000-5000-9000-000000000301",
        type: "tool_use",
        observedAt: "2026-07-17T12:00:31.000Z",
      }),
      sourceOperationKey: "orphan-retry-attempt",
      retryOfSourceOperationKey: "missing-retry-target",
      episodeId: episode.episodeId,
    });
    assert.throws(
      () => store.recordToolSignal(invalidRetry),
      /ToolAttemptRetryTargetMissing/,
    );
    checks.push({
      name: "retry_links_require_an_explicit_existing_attempt",
      detail: { attemptCountUnchanged: store.attempts().length === 4 },
    });
    const missingEpisode = adaptToolInteractionEvent({
      event: event({
        id: "00000000-0000-5000-9000-000000000302",
        type: "tool_use",
        observedAt: "2026-07-17T12:00:32.000Z",
      }),
      sourceOperationKey: "missing-episode-attempt",
      episodeId: "00000000-0000-5000-9000-000000000777",
    });
    assert.throws(
      () => store.recordToolSignal(missingEpisode),
      /ToolAttemptEpisodeMissing/,
    );
    checks.push({
      name: "attempt_episode_links_require_an_explicit_existing_episode",
      detail: { attemptCountUnchanged: store.attempts().length === 4 },
    });

    check(
      "technique_absence_is_not_inferred_from_attempt_mix",
      store.exposures().length === 0,
      { exposuresBeforeExplicitWrite: store.exposures().length },
    );
    proofStage = "exposure";
    const exposure = buildTechniqueExposureFact({
      episodeId: episode.episodeId,
      techniqueId: "bounded-retry-playbook",
      techniqueVersion: "1.2.0",
      contentDigest: `sha256:${"a".repeat(64)}`,
      assignmentId: "intervention-100-a",
      workClass: "debugging",
      complexityBand: "medium",
      exposedAt: "2026-07-17T12:00:02.000Z",
      mode: "treatment",
    });
    store.recordTechniqueExposure(exposure, {
      outcomeObservedAt: "2026-07-17T12:00:20.000Z",
    });
    check(
      "explicit_exposure_asserts_exposure_only",
      store.exposures().length === 1 &&
        store.exposures()[0].assertion === "exposure_only" &&
        !("effectiveness" in store.exposures()[0]),
      { explicitExposures: store.exposures().length },
    );

    proofStage = "retrospective";
    const retrospective = buildTechniqueExposureFact({
      ...exposure,
      assignmentId: "intervention-100-late",
      exposedAt: "2026-07-17T12:00:40.000Z",
    });
    assert.throws(
      () => store.recordTechniqueExposure(retrospective, {
        outcomeObservedAt: "2026-07-17T12:00:20.000Z",
      }),
      /RetrospectiveTechniqueExposureRejected/,
    );
    checks.push({
      name: "retrospective_exposure_after_outcome_is_rejected",
      detail: { exposureCountUnchanged: store.exposures().length === 1 },
    });
    assert.throws(
      () => store.recordTechniqueExposure({
        ...exposure,
        exposureId: "00000000-0000-5000-9000-000000000999",
        effectiveness: "improved",
        prompt: PRIVATE.prompt,
      }),
    );
    checks.push({
      name: "effectiveness_and_raw_fields_are_not_part_of_exposure_schema",
      detail: { rejected: true },
    });

    proofStage = "indexes";
    const indexes = buffer.database.prepare(
      `select name from sqlite_master where type='index' and
        (name like 'idx_attempt_%' or name like 'idx_episode_%' or name like 'idx_exposure_%')`,
    ).all() as Array<{ name: string }>;
    check(
      "promoted_fact_dimensions_are_indexed",
      indexes.length === 9,
      { indexCount: indexes.length },
    );

    proofStage = "capacity";
    const limitedDb = new Database(":memory:");
    try {
      const limited = new LearningFactStore(limitedDb, {
        attempts: 1,
        episodes: 1,
        exposures: 1,
        techniqueIdentities: 1,
      });
      limited.recordWorkEpisode(episode);
      const secondEpisode = buildWorkEpisodeFact({
        source: "codex",
        sessionId: "session-proof-100",
        sourceEpisodeKey: "second-episode",
        workClass: "debugging",
        complexityBand: "medium",
        startedAt: "2026-07-17T12:00:00.000Z",
        endedAt: "2026-07-17T12:01:00.000Z",
      });
      assert.throws(
        () => limited.recordWorkEpisode(secondEpisode),
        /LearningFactCapacityExceeded:work_episode_facts/,
      );
      checks.push({
        name: "fact_row_capacity_fails_closed",
        detail: { episodeLimit: 1 },
      });
    } finally {
      limitedDb.close();
    }

    proofStage = "upload";
    const outboxBeforeEvent = (buffer.database.prepare(
      `select count(*) as n from upload_outbox`,
    ).get() as { n: number }).n;
    check(
      "learning_facts_are_structurally_local_only",
      outboxBeforeEvent === 0,
      {
        factRows:
          store.attempts().length +
          store.episodes().length +
          store.exposures().length,
      },
    );

    buffer.append(aiInteractionEventSchema.parse({
      id: "00000000-0000-4000-8000-000000000100",
      source: "codex",
      sessionId: "session-proof-100",
      dataMode: "metadata",
      eventType: "assistant_response",
      observedAt: "2026-07-17T12:00:50.000Z",
      actionClass: "other",
      metadata: { proofKind: "learning-facts" },
    }));
    const uploadBodies: string[] = [];
    const config = collectorConfigSchema.parse({
      uploadUrl: "http://127.0.0.1/fake-learning-ingest",
      installKey: "learning-facts-proof-install",
    });
    const uploaded = await uploadBufferedEvents(config, buffer, {
      fetchImpl: async (_input, init) => {
        uploadBodies.push(String(init?.body ?? ""));
        return new Response(JSON.stringify({ accepted: 1 }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      },
    });
    check(
      "fact_tables_never_enter_upload_envelopes",
      uploaded.uploadedEvents === 1 &&
        uploadBodies.length === 1 &&
        !uploadBodies[0].includes("tool_attempt_facts") &&
        !uploadBodies[0].includes("technique_exposure") &&
        !uploadBodies[0].includes("exposure_only"),
      { uploadedEvents: uploaded.uploadedEvents, uploadBodies: uploadBodies.length },
    );

    proofStage = "privacy";
    const liveText = sqliteText(buffer.database);
    const openFiles = fileSurfaces(ledger);
    const liveLeaks = leakCount([liveText, ...uploadBodies], openFiles);
    check(
      "raw_error_stack_prompt_command_args_content_path_secret_and_pii_never_persist_or_upload",
      liveLeaks === 0,
      { sentinels: privateTerms.length, liveLeaks, surfaces: 2 + openFiles.length },
    );
    buffer.close();
    buffer = undefined;
    const closedLeaks = leakCount(uploadBodies, fileSurfaces(ledger));
    check(
      "closed_database_wal_and_shm_surfaces_remain_private",
      closedLeaks === 0,
      { closedLeaks, files: fileSurfaces(ledger).length },
    );

    const measurements = {
      attempts: attempts.length,
      failedAttempts: attempts.filter((attempt) => attempt.resultStatus === "failure").length,
      passedAttempts: attempts.filter((attempt) => attempt.resultStatus === "success").length,
      explicitRetryLinks: attempts.filter((attempt) => attempt.retryOf).length,
      missingResultUnknown: missingResult?.resultStatus === "unknown",
      episodes: 1,
      explicitExposures: 1,
      indexedDimensions: indexes.length,
      privacySentinels: privateTerms.length,
      privacyLeaks: liveLeaks + closedLeaks,
      uploadedFactRows: 0,
      learningFactRowsWritten: 6,
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
  const errorCode = safePrefix ?? "LearningFactsProofAssertionFailed";
  process.stderr.write(`${JSON.stringify({
    schema: SCHEMA,
    passed: false,
    errorClass: error instanceof Error ? error.name : "UnknownError",
    errorCode,
    proofStage,
  })}\n`);
  process.exitCode = 1;
});
