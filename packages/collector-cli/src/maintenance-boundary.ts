import { fork, execFile, type ChildProcess } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";

import type { MaintenanceRunOutcome } from "./maintenance";
import type { MaintenanceProgress } from "./maintenance-progress";
import {
  MAINTENANCE_PROTOCOL_MAX_BYTES,
  MAINTENANCE_PROTOCOL_MAX_FRAMES_PER_JOB,
  MAINTENANCE_PROTOCOL_SCHEMA,
  maintenanceProtocolFrameBytes,
  parseMaintenanceWorkerReceipt,
  type MaintenanceWorkerReceipt,
} from "./maintenance-protocol";

export type MaintenanceBoundaryState =
  | "ready"
  | "in_flight"
  | "timed_out"
  | "circuit_open"
  | "recovering"
  | "stopping"
  | "stopped";

export type MaintenanceBoundaryStatus = {
  state: MaintenanceBoundaryState;
  accepting: boolean;
  stage: "idle" | "spawning" | "automatic_capture" | "terminating" | "closed";
  source: "codex" | "claude_code" | "unknown";
  deadlineMs: number;
  generation: number;
  inFlight: boolean;
  childPresent: boolean;
  childReady: boolean;
  childFingerprint: string | null;
  lastStartedAt: string | null;
  lastCompletedAt: string | null;
  lastDurationMs: number | null;
  lastResult: {
    rawEventWrites: number;
    rolloutFilesRead: number;
    transcriptFilesRead: number;
  } | null;
  lastFailure: string | null;
  lastOutcome: "completed" | "timed_out" | "failed" | null;
  lastTimedOutAt: string | null;
  circuit: {
    failureCount: number;
    openUntil: string | null;
    skippedJobs: number;
    initialDelayMs: number;
    escalatedDelayMs: number;
  };
  quarantine: {
    source: "codex" | "claude_code" | null;
    stage: MaintenanceProgress["stage"] | null;
    candidateHash: string | null;
    until: string | null;
  };
  protocol: {
    invalidFrames: number;
    oversizedFrames: number;
    lateFrames: number;
    framesThisJob: number;
  };
  reap: {
    termSignals: number;
    killSignals: number;
    pidMismatches: number;
    reapedChildren: number;
    orphanRisk: boolean;
  };
};

export type MaintenanceBoundaryChild = Pick<
  ChildProcess,
  "pid" | "connected" | "send" | "kill" | "on" | "once" | "removeListener"
>;

type TimerHandle = ReturnType<typeof setTimeout>;

export type MaintenanceProcessFingerprintBinding = {
  parentPid: number;
  spawnNonce: string;
};

export type MaintenanceBoundaryOptions = {
  entryPath: string;
  execArgv?: string[];
  env?: NodeJS.ProcessEnv;
  deadlineMs?: number;
  readyDeadlineMs?: number;
  termGraceMs?: number;
  killGraceMs?: number;
  initialCircuitMs?: number;
  escalatedCircuitMs?: number;
  now?: () => number;
  setTimer?: (callback: () => void, delayMs: number) => TimerHandle;
  clearTimer?: (handle: TimerHandle) => void;
  spawnChild?: (spawnNonce: string) => MaintenanceBoundaryChild;
  fingerprint?: (
    pid: number,
    binding: MaintenanceProcessFingerprintBinding,
  ) => Promise<string | null>;
};

type ActiveJob = {
  generation: number;
  nonce: string;
  startedAtMs: number;
  timer: TimerHandle;
  resolve: (result: MaintenanceRunOutcome) => void;
  reject: (error: Error) => void;
  settled: boolean;
  nextSequence: number;
};

function iso(ms: number | null) {
  return ms === null ? null : new Date(ms).toISOString();
}

function asyncProcessFingerprint(
  pid: number,
  binding: MaintenanceProcessFingerprintBinding,
): Promise<string | null> {
  if (
    !Number.isSafeInteger(pid) || pid <= 0 ||
    !Number.isSafeInteger(binding.parentPid) || binding.parentPid <= 0 ||
    !/^[a-f0-9-]{16,80}$/i.test(binding.spawnNonce)
  ) return Promise.resolve(null);
  return new Promise((resolve) => {
    execFile(
      "/bin/ps",
      ["-ww", "-p", String(pid), "-o", "ppid=", "-o", "lstart=", "-o", "command="],
      {
        encoding: "utf8",
        env: { ...process.env, LANG: "C", LC_ALL: "C" },
        timeout: 500,
      },
      (error, stdout) => {
        const processRecord = !error && typeof stdout === "string"
          ? stdout.trim().replace(/\s+/g, " ")
          : "";
        const parentMatch = /^(\d+)\s+/.exec(processRecord);
        const nonceBinding = `__maintenance_worker ${binding.spawnNonce}`;
        if (
          !parentMatch || Number(parentMatch[1]) !== binding.parentPid ||
          !processRecord.includes(nonceBinding)
        ) return resolve(null);
        resolve(
          "sha256:" + createHash("sha256")
            .update(
              "plimsoll-process-start-v2\0" + pid + "\0" +
              binding.parentPid + "\0" + binding.spawnNonce + "\0" + processRecord,
            )
            .digest("hex"),
        );
      },
    );
  });
}

const MAINTENANCE_ENV_ALLOWLIST = [
  "HOME",
  "PATH",
  "TMPDIR",
  "LANG",
  "LC_ALL",
  "PLIMSOLL_HOME",
  "PLIMSOLL_DATA_MODE",
  "PLIMSOLL_EVIDENCE_MODE",
  "OTEL_LOG_USER_PROMPTS",
  "OTEL_LOG_TOOL_DETAILS",
  "OTEL_LOG_TOOL_CONTENT",
  "OTEL_LOG_RAW_API_BODIES",
] as const;

export function maintenanceWorkerEnvironment(
  source: NodeJS.ProcessEnv,
  spawnNonce: string,
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {
    PLIMSOLL_MAINTENANCE_SPAWN_NONCE: spawnNonce,
  };
  for (const key of MAINTENANCE_ENV_ALLOWLIST) {
    const value = source[key];
    if (value !== undefined) env[key] = value;
  }
  return env;
}

/**
 * A single lazy, persistent maintenance child. The parent owns deadlines,
 * circuit state, process identity checks, listener/PID state and shutdown.
 * The child owns only automatic capture and is disposable after any ambiguity.
 */
export class MaintenanceProcessBoundary {
  private accepting = true;
  private state: MaintenanceBoundaryState = "ready";
  private stage: MaintenanceBoundaryStatus["stage"] = "idle";
  private child: MaintenanceBoundaryChild | null = null;
  private childReady = false;
  private childFingerprint: string | null = null;
  private parentFingerprintPromise: Promise<string | null> | null = null;
  private spawnNonce: string | null = null;
  private readyPromise: Promise<void> | null = null;
  private readyResolve: (() => void) | null = null;
  private readyReject: ((error: Error) => void) | null = null;
  private readyTimer: TimerHandle | null = null;
  private active: ActiveJob | null = null;
  private runReserved = false;
  private generation = 0;
  private lastStartedAtMs: number | null = null;
  private lastCompletedAtMs: number | null = null;
  private lastDurationMs: number | null = null;
  private lastResult: MaintenanceBoundaryStatus["lastResult"] = null;
  private lastFailure: string | null = null;
  private lastOutcome: "completed" | "timed_out" | "failed" | null = null;
  private lastTimedOutAt: string | null = null;
  private activeProgress: MaintenanceProgress | null = null;
  private quarantine: MaintenanceProgress | null = null;
  private quarantineUntilMs: number | null = null;
  private failureCount = 0;
  private circuitOpenUntilMs: number | null = null;
  private skippedJobs = 0;
  private invalidFrames = 0;
  private oversizedFrames = 0;
  private lateFrames = 0;
  private framesThisJob = 0;
  private controlFramesThisChild = 0;
  private controlFrameLimitExceeded = false;
  private termSignals = 0;
  private killSignals = 0;
  private pidMismatches = 0;
  private reapedChildren = 0;
  private orphanRisk = false;
  private terminating: Promise<boolean> | null = null;
  private idleFailure: Promise<void> | null = null;
  private closeWaiters: Array<() => void> = [];

  constructor(private readonly options: MaintenanceBoundaryOptions) {}

  status(): MaintenanceBoundaryStatus {
    return {
      state: this.state,
      accepting: this.accepting,
      stage: this.stage,
      source: this.activeProgress?.source ?? "unknown",
      deadlineMs: this.deadlineMs(),
      generation: this.generation,
      inFlight: this.runReserved || this.active !== null,
      childPresent: this.child !== null,
      childReady: this.childReady,
      childFingerprint: this.childFingerprint,
      lastStartedAt: iso(this.lastStartedAtMs),
      lastCompletedAt: iso(this.lastCompletedAtMs),
      lastDurationMs: this.lastDurationMs,
      lastResult: this.lastResult,
      lastFailure: this.lastFailure,
      lastOutcome: this.lastOutcome,
      lastTimedOutAt: this.lastTimedOutAt,
      circuit: {
        failureCount: this.failureCount,
        openUntil: iso(this.circuitOpenUntilMs),
        skippedJobs: this.skippedJobs,
        initialDelayMs: this.initialCircuitMs(),
        escalatedDelayMs: this.escalatedCircuitMs(),
      },
      quarantine: {
        source: this.quarantine?.source ?? null,
        stage: this.quarantine?.stage ?? null,
        candidateHash: this.quarantine?.candidateHash ?? null,
        until: iso(this.quarantineUntilMs),
      },
      protocol: {
        invalidFrames: this.invalidFrames,
        oversizedFrames: this.oversizedFrames,
        lateFrames: this.lateFrames,
        framesThisJob: this.framesThisJob,
      },
      reap: {
        termSignals: this.termSignals,
        killSignals: this.killSignals,
        pidMismatches: this.pidMismatches,
        reapedChildren: this.reapedChildren,
        orphanRisk: this.orphanRisk,
      },
    };
  }

  async run(): Promise<MaintenanceRunOutcome> {
    if (!this.accepting) throw new Error("maintenance_boundary_stopping");
    if (this.runReserved || this.active) throw new Error("maintenance_job_already_in_flight");
    if (this.terminating || this.orphanRisk) throw new Error("maintenance_child_not_reaped");
    const now = this.now();
    if (this.circuitOpenUntilMs !== null && now < this.circuitOpenUntilMs) {
      this.state = "circuit_open";
      this.stage = "idle";
      this.skippedJobs += 1;
      throw new Error("maintenance_circuit_open");
    }
    if (this.circuitOpenUntilMs !== null) {
      this.state = "recovering";
      this.circuitOpenUntilMs = null;
    }
    if (this.quarantineUntilMs !== null && now >= this.quarantineUntilMs) {
      this.quarantineUntilMs = null;
      this.quarantine = null;
    }
    this.runReserved = true;
    let job!: Promise<MaintenanceRunOutcome>;
    try {
      await this.ensureWorker();
      if (!this.accepting) throw new Error("maintenance_boundary_stopping");
      if (!this.child || !this.childReady || !this.child.connected) {
        await this.handleChildFailure("maintenance_worker_unavailable");
        throw new Error("maintenance_worker_unavailable");
      }

      const generation = ++this.generation;
      const nonce = randomUUID();
      const startedAtMs = this.now();
      this.lastStartedAtMs = startedAtMs;
      this.framesThisJob = 0;
      this.state = "in_flight";
      this.stage = "automatic_capture";
      this.activeProgress = null;
      job = new Promise<MaintenanceRunOutcome>((resolve, reject) => {
        const timer = this.setTimer(() => {
          void this.failActive("maintenance_deadline_exceeded", true);
        }, this.deadlineMs());
        this.active = {
          generation,
          nonce,
          startedAtMs,
          timer,
          resolve,
          reject,
          settled: false,
          nextSequence: 1,
        };
        try {
          this.child!.send({
            schema: MAINTENANCE_PROTOCOL_SCHEMA,
            type: "run",
            generation,
            nonce,
            deadlineMs: this.deadlineMs(),
            quarantine: this.quarantine,
          });
        } catch {
          void this.failActive("maintenance_send_failed", false);
        }
      });
    } finally {
      this.runReserved = false;
    }
    return job;
  }

  async shutdown(): Promise<boolean> {
    if (!this.accepting && this.state === "stopped") return !this.orphanRisk;
    this.accepting = false;
    this.state = "stopping";
    this.stage = "terminating";
    if (this.readyTimer) this.clearTimer(this.readyTimer);
    this.readyTimer = null;
    this.readyReject?.(new Error("maintenance_boundary_stopping"));
    this.clearReadyWaiters();
    if (this.active && !this.active.settled) {
      this.active.settled = true;
      this.clearTimer(this.active.timer);
      this.active.reject(new Error("maintenance_boundary_stopping"));
      this.active = null;
    }
    const child = this.child;
    if (child?.connected) {
      try {
        child.send({
          schema: MAINTENANCE_PROTOCOL_SCHEMA,
          type: "shutdown",
          nonce: randomUUID(),
        });
      } catch {
        // The fingerprint-safe termination path below is authoritative.
      }
    }
    if (child) {
      const closed = await this.waitForClose(this.termGraceMs());
      if (!closed) await this.terminateChild("shutdown");
    }
    const stopped = this.child === null && !this.orphanRisk;
    this.state = stopped ? "stopped" : "stopping";
    this.stage = stopped ? "closed" : "terminating";
    return stopped;
  }

  private async ensureWorker() {
    if (this.child && this.childReady) return;
    if (this.readyPromise) return this.readyPromise;
    if (this.child) throw new Error("maintenance_child_not_ready");
    this.state = this.failureCount > 0 ? "recovering" : "ready";
    this.stage = "spawning";
    const spawnNonce = randomUUID();
    let child: MaintenanceBoundaryChild;
    try {
      child = this.spawnChild(spawnNonce);
    } catch {
      const reason = "maintenance_worker_spawn_failed";
      this.child = null;
      this.childReady = false;
      this.childFingerprint = null;
      this.parentFingerprintPromise = null;
      this.spawnNonce = null;
      this.recordOutcome("failed", this.now());
      this.openCircuit(reason);
      throw new Error(reason);
    }
    this.child = child;
    this.controlFramesThisChild = 0;
    this.controlFrameLimitExceeded = false;
    this.spawnNonce = spawnNonce;
    this.childReady = false;
    this.childFingerprint = null;
    this.parentFingerprintPromise = child.pid
      ? this.fingerprint(child.pid, spawnNonce).then((fingerprint) => {
          if (this.child === child && fingerprint) this.childFingerprint = fingerprint;
          return fingerprint;
        })
      : Promise.resolve(null);
    this.readyPromise = new Promise<void>((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;
    });
    child.on("message", this.onMessage);
    child.on("error", this.onChildError);
    child.on("disconnect", this.onChildDisconnect);
    child.on("close", this.onChildClose);
    this.readyTimer = this.setTimer(() => {
      void this.failReady("maintenance_worker_ready_timeout");
    }, this.readyDeadlineMs());
    return this.readyPromise;
  }

  private readonly onMessage = (raw: unknown) => {
    const bytes = maintenanceProtocolFrameBytes(raw);
    if (bytes > MAINTENANCE_PROTOCOL_MAX_BYTES) this.oversizedFrames += 1;
    const receipt = parseMaintenanceWorkerReceipt(raw);
    if (!receipt) {
      this.invalidFrames += 1;
      void this.failProtocol("maintenance_protocol_invalid");
      return;
    }
    if (receipt.type === "ready" || receipt.type === "closed") {
      if (this.controlFrameLimitExceeded) return;
      this.controlFramesThisChild += 1;
      if (this.controlFramesThisChild > MAINTENANCE_PROTOCOL_MAX_FRAMES_PER_JOB) {
        this.controlFrameLimitExceeded = true;
        if (!this.accepting || this.state === "stopping" || this.state === "stopped") return;
        this.invalidFrames += 1;
        void this.failProtocol("maintenance_protocol_control_frame_limit");
        return;
      }
    }
    if (receipt.type === "ready") {
      if (!this.readyPromise || this.childReady || this.active) {
        this.invalidFrames += 1;
        void this.failProtocol("maintenance_protocol_unexpected_ready");
      } else void this.acceptReady(receipt);
      return;
    }
    if (receipt.type === "closed") {
      if (this.accepting && this.state !== "stopping") {
        this.invalidFrames += 1;
        void this.failProtocol("maintenance_protocol_false_closed");
      }
      return;
    }
    this.framesThisJob += 1;
    if (this.framesThisJob > MAINTENANCE_PROTOCOL_MAX_FRAMES_PER_JOB) {
      this.invalidFrames += 1;
      void this.failProtocol("maintenance_protocol_frame_limit");
      return;
    }
    const active = this.active;
    if (!active || active.settled) {
      this.lateFrames += 1;
      if (!this.terminating) void this.failProtocol("maintenance_protocol_unsolicited_frame");
      return;
    }
    if (receipt.generation < active.generation) {
      this.lateFrames += 1;
      return;
    }
    if (receipt.generation > active.generation || receipt.nonce !== active.nonce) {
      this.invalidFrames += 1;
      void this.failActive(
        `maintenance_protocol_identity_mismatch_expected_${active.generation}_received_${receipt.generation}`,
        false,
      );
      return;
    }
    if (receipt.sequence !== active.nextSequence) {
      this.invalidFrames += 1;
      void this.failActive(
        `maintenance_protocol_sequence_mismatch_expected_${active.nextSequence}_received_${receipt.sequence}`,
        false,
      );
      return;
    }
    active.nextSequence += 1;
    if (receipt.type === "progress") {
      this.activeProgress = {
        source: receipt.source,
        stage: receipt.stage,
        candidateHash: receipt.candidateHash,
      };
      try {
        this.child?.send({
          schema: MAINTENANCE_PROTOCOL_SCHEMA,
          type: "ack",
          generation: receipt.generation,
          nonce: receipt.nonce,
          sequence: receipt.sequence,
        });
      } catch {
        void this.failActive("maintenance_protocol_ack_failed", false);
      }
      return;
    }
    if (receipt.type === "error") {
      void this.failActive(receipt.reason, false);
      return;
    }
    active.settled = true;
    this.clearTimer(active.timer);
    this.active = null;
    this.activeProgress = null;
    const completedAtMs = this.now();
    this.lastCompletedAtMs = completedAtMs;
    this.lastDurationMs = Math.max(0, completedAtMs - active.startedAtMs);
    this.lastResult = {
      rawEventWrites: receipt.result.rawEventWrites,
      rolloutFilesRead: receipt.result.rollout.filesRead,
      transcriptFilesRead: receipt.result.transcript.filesRead,
    };
    this.lastFailure = null;
    this.recordOutcome("completed", completedAtMs);
    this.failureCount = 0;
    this.circuitOpenUntilMs = null;
    this.state = "ready";
    this.stage = "idle";
    active.resolve(receipt.result);
  };

  private readonly onChildError = () => {
    void this.handleChildFailure("maintenance_worker_error");
  };

  private readonly onChildDisconnect = () => {
    void this.handleChildFailure("maintenance_worker_disconnected");
  };

  private readonly onChildClose = () => {
    const child = this.child;
    const wasReady = this.childReady;
    const wasStarting = this.readyPromise !== null;
    const expectedClose = this.terminating !== null || this.orphanRisk ||
      !this.accepting || this.state === "stopping";
    if (child) {
      child.removeListener("message", this.onMessage);
      child.removeListener("error", this.onChildError);
      child.removeListener("disconnect", this.onChildDisconnect);
      child.removeListener("close", this.onChildClose);
    }
    this.child = null;
    this.childReady = false;
    this.childFingerprint = null;
    this.parentFingerprintPromise = null;
    this.spawnNonce = null;
    this.controlFramesThisChild = 0;
    this.controlFrameLimitExceeded = false;
    this.reapedChildren += 1;
    this.orphanRisk = false;
    const waiters = this.closeWaiters;
    this.closeWaiters = [];
    for (const resolve of waiters) resolve();
    if (wasStarting) {
      this.readyReject?.(new Error("maintenance_worker_closed_before_ready"));
      this.clearReadyWaiters();
      if (!expectedClose) {
        this.recordOutcome("failed", this.now());
        this.openCircuit("maintenance_worker_closed_before_ready");
      }
    }
    if (this.active && !this.active.settled) {
      void this.failActive("maintenance_worker_closed", false);
    } else if (wasReady && !expectedClose) {
      this.recordOutcome("failed", this.now());
      this.openCircuit("maintenance_worker_closed_idle");
    }
  };

  private async acceptReady(receipt: Extract<MaintenanceWorkerReceipt, { type: "ready" }>) {
    const child = this.child;
    const readyToken = this.readyPromise;
    if (!child || !readyToken || receipt.spawnNonce !== this.spawnNonce) {
      this.invalidFrames += 1;
      if (this.readyPromise) await this.failReady("maintenance_ready_identity_mismatch");
      return;
    }
    const observed = await this.parentFingerprintPromise;
    if (this.child !== child || this.readyPromise !== readyToken || receipt.spawnNonce !== this.spawnNonce) {
      this.lateFrames += 1;
      return;
    }
    if (!observed) {
      this.pidMismatches += 1;
      await this.failReady("maintenance_ready_pid_mismatch");
      return;
    }
    this.childFingerprint = observed;
    this.childReady = true;
    if (this.readyTimer) this.clearTimer(this.readyTimer);
    this.readyTimer = null;
    const resolve = this.readyResolve;
    this.clearReadyWaiters();
    this.state = "ready";
    this.stage = "idle";
    resolve?.();
  }

  private async failReady(reason: string) {
    if (!this.readyPromise) return;
    this.lastFailure = reason;
    if (this.readyTimer) this.clearTimer(this.readyTimer);
    this.readyTimer = null;
    const reject = this.readyReject;
    this.clearReadyWaiters();
    this.recordOutcome("failed", this.now());
    await this.terminateChild(reason);
    this.openCircuit(reason);
    reject?.(new Error(reason));
  }

  private async failProtocol(reason: string) {
    if (this.active) return this.failActive(reason, false);
    if (this.readyPromise) return this.failReady(reason);
    if (!this.child || this.terminating) return;
    this.lastFailure = reason;
    this.recordOutcome("failed", this.now());
    this.state = "recovering";
    this.stage = "terminating";
    await this.terminateChild(reason);
    this.openCircuit(reason);
  }

  private handleChildFailure(reason: string): Promise<void> {
    if (!this.accepting || this.state === "stopping" || this.state === "stopped") {
      return Promise.resolve();
    }
    if (this.active) return this.failActive(reason, false);
    if (this.readyPromise) return this.failReady(reason);
    if (this.terminating) return this.terminating.then(() => undefined);
    if (!this.child) return Promise.resolve();
    if (this.idleFailure) return this.idleFailure;
    this.lastFailure = reason;
    this.recordOutcome("failed", this.now());
    this.state = "recovering";
    this.stage = "terminating";
    this.idleFailure = (async () => {
      await this.terminateChild(reason);
      this.openCircuit(reason);
    })().finally(() => {
      this.idleFailure = null;
    });
    return this.idleFailure;
  }

  private async failActive(reason: string, timedOut: boolean) {
    const active = this.active;
    if (!active || active.settled) return;
    active.settled = true;
    this.clearTimer(active.timer);
    this.active = null;
    const timedOutProgress = this.activeProgress;
    this.activeProgress = null;
    this.lastFailure = reason;
    this.lastCompletedAtMs = this.now();
    this.lastDurationMs = Math.max(0, this.lastCompletedAtMs - active.startedAtMs);
    this.recordOutcome(timedOut ? "timed_out" : "failed", this.lastCompletedAtMs);
    this.state = timedOut ? "timed_out" : "recovering";
    this.stage = "terminating";
    if (timedOut && timedOutProgress) {
      this.quarantine = timedOutProgress;
      this.quarantineUntilMs = this.now() + this.escalatedCircuitMs();
    }
    await this.terminateChild(reason);
    this.openCircuit(reason);
    active.reject(new Error(reason));
  }

  private openCircuit(reason: string) {
    // Shutdown owns a monotonic terminal state. An older async failure may
    // finish termination after shutdown has already proved stopped/closed;
    // preserve its recorded receipt, but never reopen the terminal boundary.
    if (!this.accepting || this.state === "stopping" || this.state === "stopped") return;
    this.failureCount += 1;
    const delay = this.failureCount === 1 ? this.initialCircuitMs() : this.escalatedCircuitMs();
    this.circuitOpenUntilMs = this.now() + delay;
    this.lastFailure = reason;
    this.state = "circuit_open";
    this.stage = "idle";
  }

  private async terminateChild(_reason: string) {
    if (this.terminating) return this.terminating;
    const child = this.child;
    if (!child) return true;
    this.stage = "terminating";
    this.terminating = (async () => {
      const pid = child.pid;
      const spawnNonce = this.spawnNonce;
      // Await the parent observation even when the worker never became ready;
      // malformed/blocked startup must still be fingerprint-safe and killable.
      const expected = this.childFingerprint ?? await this.parentFingerprintPromise;
      const signalIfSame = async (signal: NodeJS.Signals) => {
        const observed = pid && spawnNonce ? await this.fingerprint(pid, spawnNonce) : null;
        if (!expected || !observed || observed !== expected || this.child !== child) {
          this.pidMismatches += 1;
          return false;
        }
        try {
          const signaled = child.kill(signal);
          if (signaled) {
            if (signal === "SIGTERM") this.termSignals += 1;
            if (signal === "SIGKILL") this.killSignals += 1;
          }
          return signaled;
        } catch {
          return false;
        }
      };
      await signalIfSame("SIGTERM");
      if (await this.waitForClose(this.termGraceMs())) return true;
      await signalIfSame("SIGKILL");
      if (await this.waitForClose(this.killGraceMs())) return true;
      this.orphanRisk = this.child === child;
      return !this.orphanRisk;
    })().finally(() => {
      this.terminating = null;
    });
    return this.terminating;
  }


  private waitForClose(timeoutMs: number) {
    if (!this.child) return Promise.resolve(true);
    return new Promise<boolean>((resolve) => {
      let settled = false;
      const done = (closed: boolean) => {
        if (settled) return;
        settled = true;
        this.clearTimer(timer);
        resolve(closed);
      };
      const waiter = () => done(true);
      this.closeWaiters.push(waiter);
      const timer = this.setTimer(() => {
        this.closeWaiters = this.closeWaiters.filter((candidate) => candidate !== waiter);
        done(false);
      }, timeoutMs);
    });
  }

  private spawnChild(spawnNonce: string) {
    if (this.options.spawnChild) return this.options.spawnChild(spawnNonce);
    return fork(this.options.entryPath, ["__maintenance_worker", spawnNonce], {
      execArgv: this.options.execArgv ?? process.execArgv,
      env: maintenanceWorkerEnvironment(this.options.env ?? process.env, spawnNonce),
      stdio: ["ignore", "ignore", "ignore", "ipc"],
    });
  }

  private clearReadyWaiters() {
    this.readyPromise = null;
    this.readyResolve = null;
    this.readyReject = null;
  }

  private deadlineMs() { return Math.max(1, Math.min(this.options.deadlineMs ?? 1_000, 60_000)); }
  private readyDeadlineMs() { return Math.max(1, Math.min(this.options.readyDeadlineMs ?? 1_000, 60_000)); }
  private termGraceMs() { return Math.max(1, Math.min(this.options.termGraceMs ?? 250, 5_000)); }
  private killGraceMs() { return Math.max(1, Math.min(this.options.killGraceMs ?? 750, 5_000)); }
  private initialCircuitMs() { return Math.max(1, this.options.initialCircuitMs ?? 5 * 60_000); }
  private escalatedCircuitMs() { return Math.max(this.initialCircuitMs(), this.options.escalatedCircuitMs ?? 15 * 60_000); }
  private now() { return this.options.now?.() ?? Date.now(); }
  private setTimer(callback: () => void, delayMs: number) { return (this.options.setTimer ?? setTimeout)(callback, delayMs); }
  private clearTimer(handle: TimerHandle) { (this.options.clearTimer ?? clearTimeout)(handle); }
  private fingerprint(pid: number, spawnNonce: string) {
    return (this.options.fingerprint ?? asyncProcessFingerprint)(pid, {
      parentPid: process.pid,
      spawnNonce,
    });
  }

  private recordOutcome(state: "completed" | "timed_out" | "failed", atMs: number) {
    const at = new Date(atMs).toISOString();
    this.lastOutcome = state;
    if (state === "timed_out") this.lastTimedOutAt = at;
  }
}
