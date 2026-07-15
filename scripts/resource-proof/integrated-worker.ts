#!/usr/bin/env node

import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import type Database from "better-sqlite3";

import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../../packages/collector-cli/src/config";
import { createCollectorServer } from "../../packages/collector-cli/src/server";
import { uploadBufferedEvents } from "../../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../../packages/shared/src/index";

const WORKER_SCHEMA = "plimsoll.resource-proof.integrated-worker.v1" as const;
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

type WorkerMode = "integrated" | "privacy";
type PrivacyFixture = {
  schemaVersion: number;
  prefixLength: number;
  sentinels: Record<string, string>;
};

type WorkerResult = {
  schema: typeof WORKER_SCHEMA;
  scenario: WorkerMode;
  passed: boolean;
  checks: Record<string, boolean>;
  counters: {
    eventsObserved: number;
    eventsAdmitted: number;
    eventsDropped: number;
    rawEventWrites: number;
    projectionRowsWritten: number;
    outboxRowsEnqueued: number;
  };
  measurements: Record<string, number | boolean>;
};

function option(name: string) {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

function within(parent: string, candidate: string) {
  const relative = path.relative(path.resolve(parent), path.resolve(candidate));
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function sqliteCount(db: Database.Database, table: string) {
  return (db.prepare(`select count(*) as count from "${table}"`).get() as { count: number }).count;
}

function durableState(buffer: LocalEventBuffer) {
  const upload = buffer.database
    .prepare(
      `select outbox_enqueued_total as enqueued, outbox_attempts_total as attempts,
         receipt_acknowledged as acknowledged, active_pending as pending,
         active_retry as retry, active_in_flight as inFlight
       from upload_control where singleton=1`,
    )
    .get() as {
      enqueued: number;
      attempts: number;
      acknowledged: number;
      pending: number;
      retry: number;
      inFlight: number;
    };
  const projection = buffer.database
    .prepare(
      `select generation, dirty, projection_rows_visited as visited,
         projection_rows_written as written, repair_backlog as repairBacklog
       from dashboard_projection_control where singleton=1`,
    )
    .get() as {
      generation: number;
      dirty: number;
      visited: number;
      written: number;
      repairBacklog: number;
    };
  const admission = buffer.database
    .prepare(`select coalesce(sum(dropped_count),0) as dropped from otlp_admission_counters`)
    .get() as { dropped: number };
  return {
    raw: sqliteCount(buffer.database, "buffered_events"),
    facts: sqliteCount(buffer.database, "dashboard_event_facts"),
    repairs: sqliteCount(buffer.database, "dashboard_projection_repairs"),
    outbox: sqliteCount(buffer.database, "upload_outbox"),
    receipts: sqliteCount(buffer.database, "upload_receipts"),
    enqueued: upload.enqueued,
    attempts: upload.attempts,
    acknowledged: upload.acknowledged,
    pending: upload.pending,
    retry: upload.retry,
    inFlight: upload.inFlight,
    projectionGeneration: projection.generation,
    projectionDirty: projection.dirty,
    projectionVisited: projection.visited,
    projectionWritten: projection.written,
    repairBacklog: projection.repairBacklog,
    admissionDropped: admission.dropped,
  };
}

function stableState(state: ReturnType<typeof durableState>) {
  return JSON.stringify(state);
}

function attr(key: string, value: string | number) {
  if (typeof value === "number") return { key, value: { intValue: String(value) } };
  return { key, value: { stringValue: value } };
}

function otlpEnvelope(mode: WorkerMode, fixture: PrivacyFixture) {
  const nowNanos = String(BigInt(Date.now() - 1_000) * 1_000_000n);
  const privateAttributes =
    mode === "privacy"
      ? [
          attr("prompt", fixture.sentinels.prompt!),
          attr("response.body", fixture.sentinels.response!),
          attr("arguments", fixture.sentinels.toolArguments!),
          attr("cwd", fixture.sentinels.absolutePath!),
          attr("url.full", fixture.sentinels.repositoryUrl!),
          attr("user.email", fixture.sentinels.email!),
          attr("authorization", fixture.sentinels.credential!),
          attr("http.request.header.cookie", fixture.sentinels.cookie!),
          attr("message", fixture.sentinels.multibyte!),
        ]
      : [];
  const span = (name: string, sequence: number, attributes: ReturnType<typeof attr>[]) => ({
    name,
    traceId: sequence.toString(16).padStart(32, "0"),
    spanId: sequence.toString(16).padStart(16, "0"),
    startTimeUnixNano: nowNanos,
    attributes,
  });
  return {
    resourceSpans: [
      {
        resource: { attributes: [attr("service.name", "codex_exec")] },
        scopeSpans: [
          {
            scope: { name: "resource_proof" },
            spans: [
              span("handle_responses", 81, [
                attr("gen_ai.usage.input_tokens", 81),
                attr("gen_ai.usage.output_tokens", 8),
                attr("gen_ai.request.model", "resource-proof-model"),
                ...privateAttributes,
              ]),
              span("handle_responses", 82, []),
            ],
          },
        ],
      },
    ],
  };
}

async function listen(server: http.Server) {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("WorkerLoopbackUnavailable");
  return address.port;
}

async function closeServer(server: http.Server | undefined) {
  if (!server?.listening) return;
  await new Promise<void>((resolve) => server.close(() => resolve()));
}

async function postJson(port: number, route: string, body: string) {
  const response = await fetch(`http://127.0.0.1:${port}${route}`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-plimsoll-source": "codex" },
    body,
  });
  return { status: response.status, text: await response.text() };
}

function drainProjection(buffer: LocalEventBuffer) {
  for (let slice = 0; slice < 30; slice += 1) {
    const status = buffer.projection.status();
    if (
      status.ready &&
      !status.dirty &&
      Object.values(status.backlog).every((value) => value === 0)
    ) {
      return slice;
    }
    buffer.projection.runMaintenance(new Date());
  }
  return 30;
}

function sqliteText(db: Database.Database) {
  const tables = db
    .prepare(
      `select name from sqlite_master
       where type='table' and name not like 'sqlite_%' order by name`,
    )
    .all() as Array<{ name: string }>;
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
  const surfaces: Buffer[] = [];
  for (const candidate of [ledger, `${ledger}-wal`, `${ledger}-shm`]) {
    if (fs.existsSync(candidate)) surfaces.push(fs.readFileSync(candidate));
  }
  return surfaces;
}

function copyOpenLedgerArtifacts(ledger: string, root: string) {
  const copies: Array<{ kind: "database" | "wal"; path: string }> = [];
  for (const [kind, suffix] of [
    ["database", ""],
    ["wal", "-wal"],
  ] as const) {
    const source = `${ledger}${suffix}`;
    if (!fs.existsSync(source)) continue;
    const target = path.join(root, `closed-scan-${kind}.bin`);
    fs.copyFileSync(source, target);
    copies.push({ kind, path: target });
  }
  return copies;
}

function stringLeakCount(surfaces: string[], terms: string[]) {
  let leaks = 0;
  for (const surface of surfaces) {
    for (const term of terms) if (term && surface.includes(term)) leaks += 1;
  }
  return leaks;
}

function byteLeakCount(surfaces: Buffer[], terms: string[]) {
  let leaks = 0;
  for (const surface of surfaces) {
    for (const term of terms) if (term && surface.includes(Buffer.from(term))) leaks += 1;
  }
  return leaks;
}

async function run(mode: WorkerMode, root: string, operatorHome: string): Promise<WorkerResult> {
  const fixturePath = path.join(
    repoRoot,
    "scripts",
    "resource-proof",
    "fixtures",
    "metadata-privacy-sentinels.json",
  );
  const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8")) as PrivacyFixture;
  if (fixture.schemaVersion !== 1 || fixture.prefixLength < 8) {
    throw new Error("PrivacyFixtureInvalid");
  }
  const sandboxRoot = path.resolve(process.env.TMPDIR ?? os.tmpdir());
  const resolvedRoot = path.resolve(root);
  if (!within(sandboxRoot, resolvedRoot) || resolvedRoot === sandboxRoot) {
    throw new Error("WorkerRootNotIsolated");
  }
  fs.mkdirSync(resolvedRoot, { recursive: true, mode: 0o700 });
  process.env.PLIMSOLL_HOME = resolvedRoot;
  const ledger = path.join(resolvedRoot, "work-ledger.sqlite");
  const config = collectorConfigSchema.parse({
    uploadUrl: "http://127.0.0.1/fake-ingest",
    installKey: "resource-proof-install",
    delivery: { maxOldestAgeDays: 3650, requestTimeoutSeconds: 1 },
  });
  const checks: Record<string, boolean> = {};
  const counters = {
    eventsObserved: 0,
    eventsAdmitted: 0,
    eventsDropped: 0,
    rawEventWrites: 0,
    projectionRowsWritten: 0,
    outboxRowsEnqueued: 0,
  };
  const capturedLogs: string[] = [];
  const malformedResponses: string[] = [];
  const uploadBodies: string[] = [];
  let fakeUploadCalls = 0;
  let providerNetworkAvoided = true;
  let projectionSlices = 0;
  let server: http.Server | undefined;
  let buffer: LocalEventBuffer | undefined;
  const originalWarn = console.warn;
  console.warn = (...parts: unknown[]) => {
    capturedLogs.push(parts.map((part) => (typeof part === "string" ? part : "[non_string]")).join(" "));
  };
  try {
    buffer = new LocalEventBuffer(ledger, { delivery: { enabled: true, limits: config.delivery } });
    server = createCollectorServer(config, buffer);
    const port = await listen(server);
    const before = durableState(buffer);
    const payload = JSON.stringify(otlpEnvelope(mode, fixture));
    const admittedResponse = await postJson(port, "/v1/traces", payload);
    const admittedBody = JSON.parse(admittedResponse.text) as Record<string, unknown>;
    const afterAdmission = durableState(buffer);
    counters.eventsObserved = Number(admittedBody.recordCount ?? 0);
    counters.eventsAdmitted = Number(admittedBody.events ?? 0);
    counters.eventsDropped = Number(admittedBody.droppedEvents ?? 0);
    counters.rawEventWrites = afterAdmission.raw - before.raw;
    counters.projectionRowsWritten = afterAdmission.projectionWritten - before.projectionWritten;
    counters.outboxRowsEnqueued = afterAdmission.enqueued - before.enqueued;
    checks.loopbackAdmission =
      admittedResponse.status === 202 &&
      counters.eventsObserved === 2 &&
      counters.eventsAdmitted === 1 &&
      counters.eventsDropped === 1;
    checks.atomicCapture =
      counters.rawEventWrites === 1 &&
      counters.projectionRowsWritten > 0 &&
      counters.outboxRowsEnqueued === 1 &&
      afterAdmission.raw === 1 &&
      afterAdmission.facts - before.facts === 1 &&
      afterAdmission.repairs === 0 &&
      afterAdmission.repairBacklog === 0 &&
      afterAdmission.outbox === 1;

    projectionSlices = drainProjection(buffer);
    const snapshotResponse = await fetch(`http://127.0.0.1:${port}/api/snapshot?days=30`);
    const snapshot = (await snapshotResponse.json()) as {
      summary?: { totals?: { events?: number } };
    };
    checks.snapshotExactlyOne =
      snapshotResponse.status === 200 && snapshot.summary?.totals?.events === 1;

    const persisted = buffer.list(2)[0];
    const beforeDuplicate = stableState(durableState(buffer));
    const duplicateInserted = persisted
      ? buffer.append(persisted.payload, persisted.suppressedFields)
      : true;
    checks.duplicateIdempotent =
      duplicateInserted === false && stableState(durableState(buffer)) === beforeDuplicate;

    const rollbackBaseline = stableState(durableState(buffer));
    const rollbackEvent = aiInteractionEventSchema.parse({
      id: "00000000-0000-4000-8000-000000000181",
      source: "codex",
      dataMode: "metadata",
      eventType: "assistant_response",
      observedAt: new Date().toISOString(),
      actionClass: "other",
      inputTokens: 1,
      outputTokens: 1,
      metadata: { resourceProof: true },
    });
    let rollbackRaised = false;
    try {
      buffer.database.transaction(() => {
        buffer!.append(rollbackEvent);
        throw new Error("InjectedRollback");
      })();
    } catch {
      rollbackRaised = true;
    }
    checks.outerRollbackAtomic =
      rollbackRaised && stableState(durableState(buffer)) === rollbackBaseline;

    await closeServer(server);
    server = undefined;
    buffer.close();
    buffer = undefined;

    buffer = new LocalEventBuffer(ledger, { delivery: { enabled: true, limits: config.delivery } });
    const reopened = durableState(buffer);
    checks.reopenPreservesBoundary =
      reopened.raw === 1 && reopened.facts === 1 && reopened.outbox === 1 && reopened.repairs === 0;
    server = createCollectorServer(config, buffer);
    const reopenedPort = await listen(server);
    const reopenedSnapshot = await fetch(
      `http://127.0.0.1:${reopenedPort}/api/snapshot?days=30`,
    );
    const reopenedSnapshotBody = (await reopenedSnapshot.json()) as {
      summary?: { totals?: { events?: number } };
    };
    checks.reopenedSnapshotExactlyOne =
      reopenedSnapshot.status === 200 && reopenedSnapshotBody.summary?.totals?.events === 1;

    const upload = await uploadBufferedEvents(config, buffer, {
      fetchImpl: async (input, init) => {
        fakeUploadCalls += 1;
        const target =
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.href
              : input.url;
        providerNetworkAvoided &&= target.startsWith("http://127.0.0.1/");
        uploadBodies.push(String(init?.body ?? ""));
        return new Response(JSON.stringify({ accepted: 1 }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      },
      maxProbes: 3,
    });
    const afterUpload = durableState(buffer);
    const uploadedEnvelopeCount = uploadBodies.reduce((total, body) => {
      const parsed = JSON.parse(body) as { events?: unknown[] };
      return total + (Array.isArray(parsed.events) ? parsed.events.length : 0);
    }, 0);
    checks.fakeUploadAcknowledged =
      providerNetworkAvoided &&
      fakeUploadCalls === 1 &&
      uploadedEnvelopeCount === 1 &&
      upload.uploadedEvents === 1 &&
      afterUpload.acknowledged === 1 &&
      afterUpload.receipts === 1 &&
      afterUpload.outbox === 0 &&
      afterUpload.raw === 1 &&
      afterUpload.facts === 1;

    if (mode === "privacy") {
      const malformed = [
        fixture.sentinels.prompt!,
        fixture.sentinels.credential!,
        fixture.sentinels.absolutePath!,
        fixture.sentinels.multibyte!,
      ];
      for (const value of malformed) {
        const response = await postJson(
          reopenedPort,
          `/hooks/codex?probe=${encodeURIComponent(fixture.sentinels.repositoryUrl!)}`,
          `{"probe":${JSON.stringify(value)}`,
        );
        malformedResponses.push(response.text);
        const body = JSON.parse(response.text) as Record<string, unknown>;
        checks.malformedRequestsAreStable =
          (checks.malformedRequestsAreStable ?? true) &&
          response.status === 400 &&
          body.error === "collector_request_rejected" &&
          body.errorClass === "SyntaxError" &&
          body.method === "POST" &&
          body.path === "/hooks/:source" &&
          !("message" in body) &&
          !("stack" in body);
      }
    } else {
      checks.malformedRequestsAreStable = true;
    }

    const liveText = sqliteText(buffer.database);
    const copiedLedgerArtifacts = copyOpenLedgerArtifacts(ledger, resolvedRoot);
    await closeServer(server);
    server = undefined;
    buffer.close();
    buffer = undefined;

    const sentinelValues = Object.values(fixture.sentinels);
    const sentinelPrefixes = sentinelValues.map((value) => value.slice(0, fixture.prefixLength));
    const privateTerms = [...sentinelValues, ...sentinelPrefixes, operatorHome];
    const stringSurfaces = [
      liveText,
      ...uploadBodies,
      ...capturedLogs,
      ...malformedResponses,
    ];
    const closedFileSurfaces = [
      ...fileSurfaces(ledger),
      ...copiedLedgerArtifacts.map((artifact) => fs.readFileSync(artifact.path)),
    ];
    const privacyLeaks =
      stringLeakCount(stringSurfaces, privateTerms) +
      byteLeakCount(closedFileSurfaces, privateTerms);
    checks.privacySurfacesClean = privacyLeaks === 0;
    checks.closedDatabaseAndWalScanned =
      copiedLedgerArtifacts.some((artifact) => artifact.kind === "database") &&
      copiedLedgerArtifacts.some((artifact) => artifact.kind === "wal");

    const result: WorkerResult = {
      schema: WORKER_SCHEMA,
      scenario: mode,
      passed: false,
      checks,
      counters,
      measurements: {
        nodeMajor: Number(process.versions.node.split(".")[0]),
        projectionSlices,
        projectionFactWrites: afterAdmission.facts - before.facts,
        productionProjectionRowsWritten: counters.projectionRowsWritten,
        rawRowsAfterAck: afterUpload.raw,
        projectionFactsAfterAck: afterUpload.facts,
        outboxRowsBeforeUpload: reopened.outbox,
        uploadCalls: fakeUploadCalls,
        uploadEnvelopes: uploadedEnvelopeCount,
        acknowledgementReceipts: afterUpload.receipts,
        malformedRequests: malformedResponses.length,
        privacySentinelCount: sentinelValues.length,
        privacyPrefixCount: sentinelPrefixes.length,
        privacySurfacesScanned: stringSurfaces.length + closedFileSurfaces.length,
        privacyLeaks,
        providerNetworkAvoided,
        closedLedgerFilesScanned: closedFileSurfaces.length,
        closedDatabaseAndWalScanned: checks.closedDatabaseAndWalScanned,
      },
    };
    result.passed = Object.values(checks).every(Boolean);
    const serialized = JSON.stringify(result);
    const resultLeaks = stringLeakCount([serialized], privateTerms);
    if (resultLeaks > 0) {
      result.passed = false;
      result.checks.workerReceiptClean = false;
      result.measurements.privacyLeaks = Number(result.measurements.privacyLeaks) + resultLeaks;
    } else {
      result.checks.workerReceiptClean = true;
    }
    result.passed = Object.values(result.checks).every(Boolean);
    return result;
  } finally {
    console.warn = originalWarn;
    await closeServer(server);
    buffer?.close();
  }
}

async function main() {
  const mode = option("--scenario");
  const root = option("--root");
  const operatorHome = option("--operator-home");
  if ((mode !== "integrated" && mode !== "privacy") || !root || !operatorHome) {
    throw new Error("WorkerArgumentsInvalid");
  }
  const result = await run(mode, root, operatorHome);
  process.stdout.write(`${JSON.stringify(result)}\n`);
  if (!result.passed) process.exitCode = 1;
}

main().catch(() => {
  process.stdout.write(
    `${JSON.stringify({
      schema: WORKER_SCHEMA,
      passed: false,
      failedSafely: true,
    })}\n`,
  );
  process.exitCode = 1;
});
