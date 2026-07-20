import type { LocalEventBuffer } from "./buffer";
import type { CollectorMaintenance } from "./maintenance";
import {
  MAINTENANCE_PROTOCOL_SCHEMA,
  maintenanceProtocolFrameBytes,
  parseMaintenanceWorkerRequest,
  projectMaintenanceResult,
  type MaintenanceWorkerReceipt,
} from "./maintenance-protocol";

export type MaintenanceWorkerServiceInput = {
  maintenance: CollectorMaintenance;
  buffer: LocalEventBuffer;
  spawnNonce: string;
};

export function runMaintenanceWorkerService(input: MaintenanceWorkerServiceInput) {
  let active = false;
  let closed = false;
  let progressFrames = 0;
  let lastProgressKey = "";
  let sequence = 0;
  let pendingSends = 0;
  let sendWaiters: Array<() => void> = [];
  let activeJob: {
    generation: number;
    nonce: string;
    lastAckedSequence: number;
    ackWaiters: Array<{ target: number; resolve: () => void }>;
  } | null = null;

  const send = (receipt: MaintenanceWorkerReceipt) => {
    if (!process.send || maintenanceProtocolFrameBytes(receipt) > 64 * 1024) return false;
    try {
      // `process.send()` returning false is backpressure, not rejection: the
      // frame is already queued. Emitting a second terminal receipt would
      // become a stale frame in the next generation.
      pendingSends += 1;
      process.send(receipt, () => {
        pendingSends = Math.max(0, pendingSends - 1);
        if (pendingSends === 0) {
          const waiters = sendWaiters;
          sendWaiters = [];
          for (const resolve of waiters) resolve();
        }
      });
      return true;
    } catch {
      return false;
    }
  };

  const flushSends = () => pendingSends === 0
    ? Promise.resolve()
    : new Promise<void>((resolve) => sendWaiters.push(resolve));

  const waitForAck = (target: number, timeoutMs: number) => {
    if (target <= 0 || (activeJob?.lastAckedSequence ?? 0) >= target) return Promise.resolve(true);
    return new Promise<boolean>((resolve) => {
      let settled = false;
      const done = (acked: boolean) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve(acked);
      };
      activeJob?.ackWaiters.push({ target, resolve: () => done(true) });
      const timer = setTimeout(() => done(false), Math.max(1, Math.min(timeoutMs, 1_000)));
      timer.unref();
    });
  };

  const close = (nonce: string) => {
    if (closed) return;
    closed = true;
    input.maintenance.close();
    input.buffer.close();
    send({ schema: MAINTENANCE_PROTOCOL_SCHEMA, type: "closed", nonce });
    process.disconnect?.();
  };

  process.on("message", (raw) => {
    const request = parseMaintenanceWorkerRequest(raw);
    if (!request) return;
    if (request.type === "ack") {
      if (activeJob && request.generation === activeJob.generation && request.nonce === activeJob.nonce) {
        activeJob.lastAckedSequence = Math.max(activeJob.lastAckedSequence, request.sequence);
        const ready = activeJob.ackWaiters.filter((waiter) => waiter.target <= activeJob!.lastAckedSequence);
        activeJob.ackWaiters = activeJob.ackWaiters.filter((waiter) => waiter.target > activeJob!.lastAckedSequence);
        for (const waiter of ready) waiter.resolve();
      }
      return;
    }
    if (request.type === "shutdown") {
      if (!active) close(request.nonce);
      return;
    }
    if (active || closed) {
      send({
        schema: MAINTENANCE_PROTOCOL_SCHEMA,
        type: "error",
        generation: request.generation,
        nonce: request.nonce,
        sequence: 1,
        reason: "worker_busy",
      });
      return;
    }
    active = true;
    progressFrames = 0;
    lastProgressKey = "";
    sequence = 0;
    activeJob = {
      generation: request.generation,
      nonce: request.nonce,
      lastAckedSequence: 0,
      ackWaiters: [],
    };
    void input.maintenance.runRecent({
      quarantine: request.quarantine ?? undefined,
      onProgress: (progress) => {
        const key = `${progress.source}:${progress.stage}:${progress.candidateHash ?? "none"}`;
        if (key === lastProgressKey) return true;
        if (progress.stage === "jsonl_open" && progressFrames >= 118) return false;
        const critical = progress.stage === "source_scan" || progress.stage === "jsonl_validation";
        if (progressFrames >= (critical ? 120 : 112)) return false;
        const sent = send({
          schema: MAINTENANCE_PROTOCOL_SCHEMA,
          type: "progress",
          generation: request.generation,
          nonce: request.nonce,
          sequence: ++sequence,
          stage: progress.stage,
          source: progress.source,
          candidateHash: progress.candidateHash,
        });
        if (sent) {
          progressFrames += 1;
          lastProgressKey = key;
        }
        return sent;
      },
    }).then(
      async (result) => {
        if (closed) return;
        await flushSends();
        const acked = await waitForAck(sequence, request.deadlineMs);
        if (!acked || closed) return;
        active = false;
        activeJob = null;
        send({
          schema: MAINTENANCE_PROTOCOL_SCHEMA,
          type: "result",
          generation: request.generation,
          nonce: request.nonce,
          sequence: ++sequence,
          result: projectMaintenanceResult(result),
        });
      },
      async () => {
        await flushSends();
        const acked = await waitForAck(sequence, request.deadlineMs);
        if (!acked || closed) return;
        active = false;
        activeJob = null;
        if (!closed) send({
          schema: MAINTENANCE_PROTOCOL_SCHEMA,
          type: "error",
          generation: request.generation,
          nonce: request.nonce,
          sequence: ++sequence,
          reason: "maintenance_failed",
        });
      },
    );
  });

  process.on("disconnect", () => {
    if (!active) close("00000000-0000-0000-0000-000000000000");
  });

  send({
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "ready",
    spawnNonce: input.spawnNonce,
  });
}
