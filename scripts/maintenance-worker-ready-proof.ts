import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";

import {
  runMaintenanceWorkerService,
  type MaintenanceWorkerStageReceipt,
  type MaintenanceWorkerTransport,
} from "../packages/collector-cli/src/maintenance-worker";
import { MAINTENANCE_PROTOCOL_SCHEMA } from "../packages/collector-cli/src/maintenance-protocol";

class FakeTransport extends EventEmitter implements MaintenanceWorkerTransport {
  sent: unknown[] = [];

  send(receipt: unknown, callback?: () => void) {
    this.sent.push(receipt);
    queueMicrotask(() => callback?.());
    return true;
  }

  disconnect() {}
}

async function main() {
  const transport = new FakeTransport();
  const stages: MaintenanceWorkerStageReceipt[] = [];
  let initialized = 0;
  let durableCommit: ((progress: {
    stage: "fill_pending_event_links";
    rows: number;
    ms: number;
    remaining: number;
  }) => boolean) | undefined;
  const never = new Promise<never>(() => undefined);
  runMaintenanceWorkerService({
    spawnNonce: randomUUID(),
    transport,
    onStage: (receipt) => stages.push(receipt),
    initialize: () => {
      initialized += 1;
      return {
        buffer: {
          beginChildRepoContextRun() {},
          close() {},
        },
        maintenance: {
          close() {},
          runRecent: (options: { onDurableCommit?: typeof durableCommit }) => {
            durableCommit = options.onDurableCommit;
            return never;
          },
        },
      } as never;
    },
  });

  assert.equal(initialized, 0, "ledger initialization must not precede readiness");
  assert.equal((transport.sent[0] as { type?: string }).type, "ready");
  assert.deepEqual(stages.map((stage) => stage.stage), ["process_up", "ready_sent"]);
  assert.ok(stages.every((stage) => Number.isFinite(stage.ms) && stage.ms >= 0));

  transport.emit("message", {
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "run",
    generation: 1,
    nonce: randomUUID(),
    deadlineMs: 5,
    quarantine: null,
    repoContexts: [],
  });
  assert.equal(initialized, 1, "ledger initialization belongs to the first job");
  assert.equal(stages.some((stage) => stage.stage === "initialization_start"), true);
  assert.ok(durableCommit, "busy worker must retain its durable-commit reporter");
  assert.equal(durableCommit({
    stage: "fill_pending_event_links",
    rows: 100,
    ms: 12,
    remaining: 50,
  }), true);
  const progress = transport.sent.find((receipt) => (
    (receipt as { type?: string }).type === "maintenance_job_progress"
  )) as { sequence: number; rows: number } | undefined;
  assert.equal(progress?.rows, 100, "busy worker must send durable progress immediately");

  const failureTransport = new FakeTransport();
  runMaintenanceWorkerService({
    spawnNonce: randomUUID(),
    transport: failureTransport,
    initialize: () => {
      const error = new Error("database is locked at /Users/private/customer/ledger.sqlite") as Error & { code: string };
      error.code = "SQLITE_BUSY";
      throw error;
    },
  });
  failureTransport.emit("message", {
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "run",
    generation: 1,
    nonce: randomUUID(),
    deadlineMs: 500,
    quarantine: null,
    repoContexts: [],
  });
  const failure = failureTransport.sent.find((receipt) => (
    (receipt as { type?: string }).type === "error"
  )) as Record<string, unknown> | undefined;
  assert.deepEqual(failure && {
    reason: failure.reason,
    errorClass: failure.errorClass,
    message: failure.message,
    stage: failure.stage,
    progressAcknowledged: failure.progressAcknowledged,
  }, {
    reason: "maintenance_failed",
    errorClass: "SQLITE_BUSY",
    message: "database is locked at [path]",
    stage: "initialization",
    progressAcknowledged: false,
  });
  assert.equal(typeof failure?.elapsedMs, "number");
  assert.equal(JSON.stringify(failure).includes("/Users/private"), false);

  console.log(JSON.stringify({
    proof: "maintenance_worker_ready",
    checks: 8,
    passed: 8,
    readyBeforeInitialization: true,
    failureDiagnosticsPathFree: true,
    stageLogs: stages,
  }));
}

main().catch((error) => {
  console.error(JSON.stringify({
    proof: "maintenance_worker_ready",
    ok: false,
    reason: error instanceof Error ? error.message : "proof_failed",
  }));
  process.exitCode = 1;
});
