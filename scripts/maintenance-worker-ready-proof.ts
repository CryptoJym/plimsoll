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
          runRecent: () => never,
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

  console.log(JSON.stringify({
    proof: "maintenance_worker_ready",
    checks: 4,
    passed: 4,
    readyBeforeInitialization: true,
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
