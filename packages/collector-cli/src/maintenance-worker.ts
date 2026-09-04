import type { LocalEventBuffer } from "./buffer";
import type { CollectorMaintenance } from "./maintenance";
import {
  MAINTENANCE_PROTOCOL_MAX_BYTES,
  MAINTENANCE_PROTOCOL_SCHEMA,
  maintenanceProtocolFrameBytes,
  parseMaintenanceWorkerRequest,
  projectMaintenanceResult,
  type MaintenanceWorkerReceipt,
  type MaintenanceJobProgress,
} from "./maintenance-protocol";
import { maintenanceCandidateHash, type MaintenanceProgress } from "./maintenance-progress";
import { recordGitContextBatchProgress } from "./maintenance-starvation";
import {
  REPO_CONTEXT_RESOLVER_VERSION,
  resolveRepoContextRequests,
  type RepoContextRequest,
  type RepoContextResult,
} from "./repo-context";

export type MaintenanceWorkerServiceInput = {
  maintenance?: CollectorMaintenance;
  buffer?: LocalEventBuffer;
  initialize?: () => {
    maintenance: CollectorMaintenance;
    buffer: LocalEventBuffer;
  };
  spawnNonce: string;
  transport?: MaintenanceWorkerTransport;
  onStage?: (receipt: MaintenanceWorkerStageReceipt) => void;
};

export type MaintenanceWorkerStageReceipt = { stage: string; ms: number };

export type MaintenanceWorkerTransport = {
  send: (receipt: unknown, callback?: () => void) => boolean;
  on: (event: "message" | "disconnect", listener: (value?: unknown) => void) => unknown;
  disconnect?: () => void;
};

type RepoContextResolver = typeof resolveRepoContextRequests;

/**
 * Issue #181. `git_context` resolution used to be the one all-or-nothing unit
 * left in the maintenance cycle: every context in the batch was resolved into
 * an in-memory array and the whole array was committed once, at the end. A
 * deadline kill landing anywhere inside that loop therefore discarded every
 * context the run had already resolved, and the next cycle began the same
 * batch again from zero — measured on a real ledger as 48,280 event links
 * stuck `fill_pending` against only 93 contexts ever resolved.
 *
 * The batch is now bounded and resumable instead:
 *   - each result is committed as it resolves (`commit`), so a kill keeps
 *     every context that completed;
 *   - the pass stops at a wall budget derived from the job deadline; and
 *   - the unresolved remainder is DEFERRED, never burned as UNKNOWN, so the
 *     next cycle continues from it rather than restarting.
 */
export const GIT_CONTEXT_BUDGET_SHARE = 0.5;
export const GIT_CONTEXT_MAX_BUDGET_MS = 10_000;
/** Bound on the resumable carry-over so a stuck resolver cannot grow it. */
export const GIT_CONTEXT_CARRY_OVER_LIMIT = 64;

/**
 * Half the job deadline, never more than ten seconds. The other half stays
 * with capture, the protocol ack and child teardown. This is a share of the
 * EXISTING deadline: issue #181 is fixed by making the work resumable, not by
 * enlarging the budget it overran.
 */
export function gitContextBudgetMs(deadlineMs: number) {
  const bounded = Number.isSafeInteger(deadlineMs) && deadlineMs > 0
    ? Math.min(deadlineMs, 60_000)
    : 1;
  return Math.max(
    1,
    Math.min(Math.floor(bounded * GIT_CONTEXT_BUDGET_SHARE), GIT_CONTEXT_MAX_BUDGET_MS),
  );
}

export type MaintenanceRepoContextBatch = {
  results: RepoContextResult[];
  /** Contexts this pass actually resolved (committed when `commit` is given). */
  resolved: number;
  /** Contexts left for the next cycle. Never burned as UNKNOWN. */
  deferred: RepoContextRequest[];
  budgetExhausted: boolean;
  elapsedMs: number;
};

export type MaintenanceRepoContextBatchOptions = {
  quarantine: MaintenanceProgress | null;
  reportProgress: (progress: MaintenanceProgress) => boolean;
  recordRepoLabel: (repoHash: string, label: string) => void;
  resolveRequests?: RepoContextResolver;
  /** Wall budget for this pass. Exhaustion defers; it never burns a context. */
  budgetMs?: number;
  now?: () => number;
  /**
   * Durably commit each result AS IT RESOLVES. Without this the batch is
   * again all-or-nothing and a kill discards it (the pre-#181 behavior).
   */
  commit?: (results: RepoContextResult[]) => void;
  /**
   * Only a caller that can carry the remainder to a later cycle may defer.
   * The parent-supplied batch owes the protocol one result per requested id,
   * so it keeps the original burn-on-exhaustion behavior.
   */
  deferrable?: boolean;
};

export function resolveRepoContextBatch(
  requests: readonly RepoContextRequest[],
  options: MaintenanceRepoContextBatchOptions,
): MaintenanceRepoContextBatch {
  const unknownResult = (repoContext: RepoContextRequest): RepoContextResult => ({
    contextId: repoContext.contextId,
    repoHash: null,
    branchHash: null,
    headSha: null,
    resolvedAt: new Date().toISOString(),
    resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
  });
  const now = options.now ?? (() => performance.now());
  const startedAtMs = now();
  const budgetMs = options.budgetMs;
  const canDefer = options.deferrable === true;
  const results: RepoContextResult[] = [];
  const deferred: RepoContextRequest[] = [];
  let resolved = 0;
  let budgetExhausted = false;

  const emit = (result: RepoContextResult) => {
    results.push(result);
    options.commit?.([result]);
  };

  for (let index = 0; index < requests.length; index += 1) {
    const repoContext = requests[index]!;
    if (canDefer && budgetMs !== undefined && now() - startedAtMs >= budgetMs) {
      budgetExhausted = true;
      deferred.push(...requests.slice(index));
      break;
    }
    if (repoContext.source !== "codex" && repoContext.source !== "claude_code") {
      emit(unknownResult(repoContext));
      continue;
    }
    const candidateHash = maintenanceCandidateHash(repoContext.cwd);
    // A quarantined candidate is PROVEN slow, so skipping it is a decision,
    // not a deferral: it stays an exact UNKNOWN result.
    if (
      options.quarantine?.source === repoContext.source &&
      options.quarantine.stage === "git_context" &&
      options.quarantine.candidateHash === candidateHash
    ) {
      emit(unknownResult(repoContext));
      continue;
    }
    if (!options.reportProgress({
      source: repoContext.source,
      stage: "git_context",
      candidateHash,
    })) {
      // A refused progress frame means the parent stopped accepting work for
      // this job, not that this context failed. Carry it when we can.
      if (canDefer) {
        budgetExhausted = true;
        deferred.push(...requests.slice(index));
        break;
      }
      emit(unknownResult(repoContext));
      continue;
    }
    try {
      const [result] = (options.resolveRequests ?? resolveRepoContextRequests)([repoContext], {
        onRepoLabel: options.recordRepoLabel,
      });
      emit(result ?? unknownResult(repoContext));
    } catch {
      // Git attribution is best-effort and happens after token/cursor commit.
      // A resolver or local label-write fault degrades only this exact context.
      emit(unknownResult(repoContext));
    }
    resolved += 1;
  }

  return {
    results,
    resolved,
    deferred,
    budgetExhausted,
    elapsedMs: Math.max(0, now() - startedAtMs),
  };
}

export function resolveMaintenanceRepoContexts(
  requests: readonly RepoContextRequest[],
  options: {
    quarantine: MaintenanceProgress | null;
    reportProgress: (progress: MaintenanceProgress) => boolean;
    recordRepoLabel: (repoHash: string, label: string) => void;
    resolveRequests?: RepoContextResolver;
  },
) {
  return resolveRepoContextBatch(requests, options).results;
}

/**
 * Bound the resumable carry-over. Contexts already waiting keep their place;
 * anything past the limit is returned so the caller can retire it rather than
 * leaving an inflight row behind forever.
 */
export function boundRepoContextCarryOver(
  carried: readonly RepoContextRequest[],
  fresh: readonly RepoContextRequest[],
  limit = GIT_CONTEXT_CARRY_OVER_LIMIT,
) {
  const bounded = Math.max(1, Math.trunc(limit));
  const seen = new Set<string>();
  const kept: RepoContextRequest[] = [];
  const overflow: RepoContextRequest[] = [];
  for (const request of [...carried, ...fresh]) {
    if (seen.has(request.contextId)) continue;
    seen.add(request.contextId);
    if (kept.length < bounded) kept.push(request);
    else overflow.push(request);
  }
  return { kept, overflow };
}

export function runMaintenanceWorkerService(input: MaintenanceWorkerServiceInput) {
  const serviceStartedAt = performance.now();
  const transport: MaintenanceWorkerTransport = input.transport ?? {
    send: (receipt, callback) => callback
      ? process.send?.(receipt, callback) ?? false
      : process.send?.(receipt) ?? false,
    on: (event, listener) => process.on(event, listener),
    disconnect: () => process.disconnect?.(),
  };
  let runtime = input.maintenance && input.buffer
    ? { maintenance: input.maintenance, buffer: input.buffer }
    : null;
  const reportStage = (stage: string, startedAt = serviceStartedAt) => {
    try {
      input.onStage?.({ stage, ms: Math.max(0, Math.round(performance.now() - startedAt)) });
    } catch {
      // Diagnostics must never prevent readiness or maintenance.
    }
  };
  const initialize = () => {
    if (runtime) return runtime;
    const startedAt = performance.now();
    reportStage("initialization_start", startedAt);
    runtime = input.initialize?.() ?? null;
    if (!runtime) throw new Error("maintenance_worker_initializer_missing");
    reportStage("initialization_complete", startedAt);
    return runtime;
  };
  let active = false;
  let closed = false;
  // Issue #181 cursor: contexts a budget-bounded pass did not reach. They keep
  // their durable inflight rows, so the next job continues from here instead
  // of restarting the batch.
  let carriedRepoContexts: RepoContextRequest[] = [];
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
    if (maintenanceProtocolFrameBytes(receipt) > MAINTENANCE_PROTOCOL_MAX_BYTES) {
      return false;
    }
    try {
      // `process.send()` returning false is backpressure, not rejection: the
      // frame is already queued. Emitting a second terminal receipt would
      // become a stale frame in the next generation.
      pendingSends += 1;
      transport.send(receipt, () => {
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
    runtime?.maintenance.close();
    runtime?.buffer.close();
    send({ schema: MAINTENANCE_PROTOCOL_SCHEMA, type: "closed", nonce });
    transport.disconnect?.();
  };

  transport.on("message", (raw) => {
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
    let worker;
    try {
      worker = initialize();
    } catch {
      active = false;
      activeJob = null;
      send({
        schema: MAINTENANCE_PROTOCOL_SCHEMA,
        type: "error",
        generation: request.generation,
        nonce: request.nonce,
        sequence: 1,
        reason: "maintenance_failed",
      });
      return;
    }
    const reportProgress = (progress: MaintenanceProgress) => {
      const key = `${progress.source}:${progress.stage}:${progress.candidateHash ?? "none"}`;
      if (progress.stage !== "git_context" && key === lastProgressKey) return true;
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
    };
    const reportJobProgress = (progress: MaintenanceJobProgress) => {
      if (progressFrames >= 120) return false;
      const sent = send({
        schema: MAINTENANCE_PROTOCOL_SCHEMA,
        type: "maintenance_job_progress",
        generation: request.generation,
        nonce: request.nonce,
        sequence: ++sequence,
        ...progress,
      });
      if (sent) progressFrames += 1;
      return sent;
    };
    const resolveWithProgress = (requests: readonly RepoContextRequest[]) => {
      return resolveMaintenanceRepoContexts(requests, {
        quarantine: request.quarantine,
        reportProgress,
        recordRepoLabel: (repoHash, label) => worker.buffer.recordRepoLabel(repoHash, label),
      });
    };
    try {
      worker.buffer.beginChildRepoContextRun();
    } catch {
      active = false;
      activeJob = null;
      send({
        schema: MAINTENANCE_PROTOCOL_SCHEMA,
        type: "error",
        generation: request.generation,
        nonce: request.nonce,
        sequence: ++sequence,
        reason: "maintenance_failed",
      });
      return;
    }
    void worker.maintenance.runRecent({
      quarantine: request.quarantine ?? undefined,
      onProgress: reportProgress,
      onDurableCommit: reportJobProgress,
    }).then(
      async (result) => {
        if (closed) return;
        let repoContexts;
        try {
          // runRecent resolves only after both capture sources and their
          // cursor/event transactions have committed. Filesystem attribution
          // therefore cannot make already-captured usage disappear.
          worker.buffer.drainRepoContextFills();
          // Issue #181: the child batch is bounded, committed per context and
          // resumable. Carry-over is processed first so a deferred context
          // cannot be starved by a steady arrival of fresh ones.
          const { kept, overflow } = boundRepoContextCarryOver(
            carriedRepoContexts,
            worker.buffer.finishChildRepoContextRun(),
          );
          carriedRepoContexts = [];
          const gitContextStartedAt = performance.now();
          const childBatch = resolveRepoContextBatch(kept, {
            quarantine: request.quarantine,
            reportProgress,
            recordRepoLabel: (repoHash, label) => worker.buffer.recordRepoLabel(repoHash, label),
            budgetMs: gitContextBudgetMs(request.deadlineMs),
            commit: (committed) => {
              worker.buffer.applyRepoContextResults(committed);
            },
            deferrable: true,
          });
          carriedRepoContexts = childBatch.deferred;
          // Anything past the carry-over bound is retired exactly, so its
          // durable inflight row can never outlive this worker unnoticed.
          if (overflow.length > 0) {
            worker.buffer.applyRepoContextResults(overflow.map((repoContext) => ({
              contextId: repoContext.contextId,
              repoHash: null,
              branchHash: null,
              headSha: null,
              resolvedAt: new Date().toISOString(),
              resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
            })));
          }
          recordGitContextBatchProgress(worker.buffer.database, {
            committed: childBatch.results.length,
            deferred: childBatch.deferred.length + overflow.length,
          });
          reportJobProgress({
            stage: "git_context",
            rows: childBatch.results.length + overflow.length,
            ms: Math.max(0, Math.round(performance.now() - gitContextStartedAt)),
            remaining: childBatch.deferred.length,
          });
          repoContexts = resolveWithProgress(request.repoContexts);
        } catch {
          try {
            worker.buffer.abandonChildRepoContextRun();
          } catch {
            // Residual inflight truth is recovered by the parent failure gate.
          }
          active = false;
          activeJob = null;
          send({
            schema: MAINTENANCE_PROTOCOL_SCHEMA,
            type: "error",
            generation: request.generation,
            nonce: request.nonce,
            sequence: ++sequence,
            reason: "maintenance_failed",
          });
          return;
        }
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
          repoContexts,
        });
      },
      async () => {
        try {
          worker.buffer.abandonChildRepoContextRun();
        } catch {
          // Residual inflight truth is recovered by the parent failure gate.
        }
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

  transport.on("disconnect", () => {
    if (!active) close("00000000-0000-0000-0000-000000000000");
  });

  reportStage("process_up");
  send({
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "ready",
    spawnNonce: input.spawnNonce,
  });
  reportStage("ready_sent");
}
