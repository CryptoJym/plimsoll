import { execFile, fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { setPriority } from "node:os";

export const ENRICHMENT_PROTOCOL_SCHEMA = 1 as const;

export function lowerEnrichmentProcessPriority(
  apply: (pid: number, priority: number) => void = setPriority,
) {
  try {
    apply(0, 15);
    return true;
  } catch {
    return false;
  }
}

export type EnrichmentJobResult = { rows: number; ms: number };
export type EnrichmentJobOutcome =
  | ({ outcome: "completed" } & EnrichmentJobResult)
  | { outcome: "PARTIAL_OK"; rows: number; ms: number };

export type EnrichmentBoundaryChild = {
  pid?: number;
  connected: boolean;
  send: (message: unknown, callback?: (error: Error | null) => void) => boolean;
  kill: (signal: NodeJS.Signals) => boolean;
  on: (event: string, listener: (...args: any[]) => void) => unknown;
  removeListener: (event: string, listener: (...args: any[]) => void) => unknown;
};

export type EnrichmentBoundaryStatus = {
  inFlight: boolean;
  childPresent: boolean;
  lastOutcome: "completed" | "PARTIAL_OK" | "timed_out" | "failed" | null;
  termSignals: number;
  killSignals: number;
  reapedChildren: number;
  orphanRisk: boolean;
};

type EnrichmentBoundaryOptions = {
  entryPath: string;
  execArgv?: string[];
  env?: NodeJS.ProcessEnv;
  deadlineMs?: number;
  readyDeadlineMs?: number;
  termGraceMs?: number;
  killGraceMs?: number;
  spawnChild?: (spawnNonce: string) => EnrichmentBoundaryChild;
  verifyChild?: (pid: number, spawnNonce: string) => Promise<boolean>;
};

type Timer = ReturnType<typeof setTimeout>;

function validNonce(value: unknown) {
  return typeof value === "string" && /^[a-f0-9-]{16,80}$/i.test(value);
}

function enrichmentEnvironment(source: NodeJS.ProcessEnv, spawnNonce: string) {
  const env: NodeJS.ProcessEnv = { PLIMSOLL_ENRICHMENT_SPAWN_NONCE: spawnNonce };
  for (const key of ["HOME", "PATH", "TMPDIR", "LANG", "LC_ALL", "PLIMSOLL_HOME", "PLIMSOLL_DATA_MODE", "PLIMSOLL_EVIDENCE_MODE"] as const) {
    if (source[key] !== undefined) env[key] = source[key];
  }
  return env;
}

function verifyProcess(pid: number, spawnNonce: string) {
  return new Promise<boolean>((resolve) => {
    execFile("/bin/ps", ["-ww", "-p", String(pid), "-o", "ppid=", "-o", "command="], {
      encoding: "utf8", timeout: 500,
      env: { ...process.env, LANG: "C", LC_ALL: "C", TZ: "UTC" },
    }, (error, stdout) => {
      const row = !error && typeof stdout === "string" ? stdout.trim().replace(/\s+/g, " ") : "";
      const parent = /^(\d+)\s+/.exec(row);
      resolve(Boolean(parent && Number(parent[1]) === process.pid && row.includes(`__enrichment_worker ${spawnNonce}`)));
    });
  });
}

/** A new disposable child is spawned and confirmed gone for every row job. */
export class EnrichmentProcessBoundary {
  private child: EnrichmentBoundaryChild | null = null;
  private running = false;
  private lastOutcome: EnrichmentBoundaryStatus["lastOutcome"] = null;
  private termSignals = 0;
  private killSignals = 0;
  private reapedChildren = 0;
  private orphanRisk = false;
  private abortCurrent: (() => Promise<void>) | null = null;
  private idleWaiters: Array<() => void> = [];

  constructor(private readonly options: EnrichmentBoundaryOptions) {}

  status(): EnrichmentBoundaryStatus {
    return {
      inFlight: this.running,
      childPresent: this.child !== null,
      lastOutcome: this.lastOutcome,
      termSignals: this.termSignals,
      killSignals: this.killSignals,
      reapedChildren: this.reapedChildren,
      orphanRisk: this.orphanRisk,
    };
  }

  async run(options: { acceptPartial?: boolean } = {}): Promise<EnrichmentJobOutcome> {
    if (this.running) throw new Error("enrichment_job_already_in_flight");
    if (this.orphanRisk || this.child) throw new Error("enrichment_child_not_reaped");
    this.running = true;
    this.lastOutcome = null;
    const spawnNonce = randomUUID();
    const generation = 1;
    const nonce = randomUUID();
    let child: EnrichmentBoundaryChild;
    try {
      child = this.options.spawnChild?.(spawnNonce) ?? fork(
        this.options.entryPath,
        ["__enrichment_worker", spawnNonce],
        {
          execArgv: this.options.execArgv ?? process.execArgv,
          env: enrichmentEnvironment(this.options.env ?? process.env, spawnNonce),
          stdio: ["ignore", "ignore", "inherit", "ipc"],
        },
      ) as EnrichmentBoundaryChild;
      this.child = child;
    } catch {
      this.running = false;
      this.lastOutcome = "failed";
      throw new Error("enrichment_worker_spawn_failed");
    }

    return new Promise<EnrichmentJobOutcome>((resolve, reject) => {
      let settled = false;
      let ready = false;
      let nextSequence = 1;
      let acknowledged: EnrichmentJobResult | null = null;
      let completed: EnrichmentJobResult | null = null;
      let readyTimer: Timer | null = null;
      let deadlineTimer: Timer | null = null;
      let closeWaiter: (() => void) | null = null;
      let ending = false;

      const cleanupTimers = () => {
        if (readyTimer) clearTimeout(readyTimer);
        if (deadlineTimer) clearTimeout(deadlineTimer);
        readyTimer = null;
        deadlineTimer = null;
      };
      const detach = () => {
        child.removeListener("message", onMessage);
        child.removeListener("error", onError);
        child.removeListener("disconnect", onDisconnect);
        child.removeListener("close", onClose);
      };
      const finish = (outcome: EnrichmentJobOutcome) => {
        if (settled) return;
        settled = true;
        cleanupTimers();
        detach();
        this.running = false;
        this.abortCurrent = null;
        this.lastOutcome = outcome.outcome;
        for (const waiter of this.idleWaiters.splice(0)) waiter();
        resolve(outcome);
      };
      const fail = (reason: string) => {
        if (settled) return;
        settled = true;
        cleanupTimers();
        detach();
        this.running = false;
        this.abortCurrent = null;
        this.lastOutcome = reason === "enrichment_deadline_exceeded" ? "timed_out" : "failed";
        for (const waiter of this.idleWaiters.splice(0)) waiter();
        reject(new Error(reason));
      };
      const waitForClose = (ms: number) => this.child === null
        ? Promise.resolve(true)
        : new Promise<boolean>((done) => {
            let ended = false;
            const timer = setTimeout(() => { if (!ended) { ended = true; closeWaiter = null; done(false); } }, ms);
            closeWaiter = () => { if (!ended) { ended = true; clearTimeout(timer); closeWaiter = null; done(true); } };
          });
      const terminate = async () => {
        if (!this.child) return true;
        const pid = child.pid;
        const verified = pid && await (this.options.verifyChild ?? verifyProcess)(pid, spawnNonce);
        if (!verified || this.child !== child) {
          this.orphanRisk = this.child === child;
          return false;
        }
        this.termSignals += 1;
        child.kill("SIGTERM");
        if (await waitForClose(this.termGraceMs())) return true;
        const verifiedAgain = pid && await (this.options.verifyChild ?? verifyProcess)(pid, spawnNonce);
        if (!verifiedAgain || this.child !== child) {
          this.orphanRisk = this.child === child;
          return false;
        }
        this.killSignals += 1;
        child.kill("SIGKILL");
        const gone = await waitForClose(this.killGraceMs());
        this.orphanRisk = !gone;
        return gone;
      };
      const timeout = async (reason: string) => {
        if (ending || settled) return;
        ending = true;
        cleanupTimers();
        const gone = await terminate();
        if (!gone) return fail("enrichment_child_not_reaped");
        if (acknowledged && options.acceptPartial) {
          return finish({ outcome: "PARTIAL_OK", ...acknowledged });
        }
        fail(reason);
      };
      this.abortCurrent = () => timeout("enrichment_boundary_stopping");
      const onMessage = (raw: unknown) => {
        if (!raw || typeof raw !== "object") return void timeout("enrichment_protocol_invalid");
        const row = raw as Record<string, unknown>;
        if (row.schema !== ENRICHMENT_PROTOCOL_SCHEMA) return void timeout("enrichment_protocol_invalid");
        if (row.type === "ready") {
          if (ready || row.spawnNonce !== spawnNonce) return void timeout("enrichment_ready_identity_mismatch");
          void (async () => {
            if (!child.pid || !await (this.options.verifyChild ?? verifyProcess)(child.pid, spawnNonce)) {
              return timeout("enrichment_ready_pid_mismatch");
            }
            ready = true;
            if (readyTimer) clearTimeout(readyTimer);
            readyTimer = null;
            child.send({
              schema: ENRICHMENT_PROTOCOL_SCHEMA, type: "run", generation, nonce,
              deadlineMs: this.deadlineMs(),
            });
          })();
          return;
        }
        if (!ready || row.generation !== generation || row.nonce !== nonce || row.sequence !== nextSequence++) {
          return void timeout("enrichment_protocol_invalid");
        }
        if (row.type === "enrichment_job_progress") {
          if (!Number.isSafeInteger(row.rows) || Number(row.rows) < 0 ||
              !Number.isSafeInteger(row.ms) || Number(row.ms) < 0) {
            return void timeout("enrichment_protocol_invalid");
          }
          const progress = { rows: Number(row.rows), ms: Number(row.ms) };
          try {
            child.send({
              schema: ENRICHMENT_PROTOCOL_SCHEMA, type: "ack", generation, nonce,
              sequence: row.sequence,
            }, (error: Error | null) => { if (!error) acknowledged = progress; });
          } catch {
            void timeout("enrichment_protocol_ack_failed");
          }
          return;
        }
        if (row.type === "result" && Number.isSafeInteger(row.rows) && Number(row.rows) >= 0 &&
            Number.isSafeInteger(row.ms) && Number(row.ms) >= 0) {
          completed = { rows: Number(row.rows), ms: Number(row.ms) };
          return;
        }
        void timeout("enrichment_protocol_invalid");
      };
      const onError = () => { if (!completed) void timeout("enrichment_worker_error"); };
      const onDisconnect = () => { if (!completed) void timeout("enrichment_worker_disconnected"); };
      const onClose = () => {
        this.child = null;
        this.reapedChildren += 1;
        this.orphanRisk = false;
        closeWaiter?.();
        if (completed) finish({ outcome: "completed", ...completed });
      };
      child.on("message", onMessage);
      child.on("error", onError);
      child.on("disconnect", onDisconnect);
      child.on("close", onClose);
      readyTimer = setTimeout(() => void timeout("enrichment_worker_ready_timeout"), this.readyDeadlineMs());
      deadlineTimer = setTimeout(() => void timeout("enrichment_deadline_exceeded"), this.deadlineMs());
    });
  }

  waitForIdle() {
    if (!this.running) return Promise.resolve();
    return new Promise<void>((resolve) => this.idleWaiters.push(resolve));
  }

  async shutdown() {
    await this.abortCurrent?.();
    await this.waitForIdle();
    return this.child === null && !this.orphanRisk;
  }

  private deadlineMs() { return Math.max(1, Math.min(this.options.deadlineMs ?? 10_000, 60_000)); }
  private readyDeadlineMs() { return Math.max(1, Math.min(this.options.readyDeadlineMs ?? 5_000, this.deadlineMs())); }
  private termGraceMs() { return Math.max(1, Math.min(this.options.termGraceMs ?? 250, 5_000)); }
  private killGraceMs() { return Math.max(1, Math.min(this.options.killGraceMs ?? 750, 5_000)); }
}

export type IdleEnrichmentOutcome = EnrichmentJobOutcome | { outcome: "skipped_main_busy" };

/** Low-priority gate: a cadence tick is discarded unless the main job is idle. */
export class IdleEnrichmentScheduler {
  private running = false;
  private accepting = true;
  private idleWaiters: Array<() => void> = [];

  constructor(
    private readonly mainIdle: () => boolean,
    private readonly runJob: () => Promise<EnrichmentJobOutcome>,
  ) {}

  async trigger(): Promise<IdleEnrichmentOutcome> {
    if (!this.accepting) throw new Error("enrichment_scheduler_stopping");
    if (this.running || !this.mainIdle()) return { outcome: "skipped_main_busy" };
    this.running = true;
    try { return await this.runJob(); }
    finally {
      this.running = false;
      for (const waiter of this.idleWaiters.splice(0)) waiter();
    }
  }

  status() { return { accepting: this.accepting, inFlight: this.running }; }
  waitForIdle() {
    if (!this.running) return Promise.resolve();
    return new Promise<void>((resolve) => this.idleWaiters.push(resolve));
  }
  stopAccepting() { this.accepting = false; }
}

export class AutomaticEnrichmentCadence {
  private timer: Timer | null = null;
  private accepting = true;
  constructor(
    private readonly scheduler: IdleEnrichmentScheduler,
    private readonly options: { intervalMs?: number; onError?: (error: unknown) => void } = {},
  ) {}
  start() {
    if (!this.accepting || this.timer) return;
    this.timer = setInterval(() => {
      void this.scheduler.trigger().catch((error) => this.options.onError?.(error));
    }, Math.max(1, this.options.intervalMs ?? 5 * 60_000));
    this.timer.unref();
  }
  stop() {
    this.accepting = false;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }
}

export type EnrichmentWorkerTransport = {
  send: (receipt: unknown, callback?: (error: Error | null) => void) => boolean;
  on: (event: "message" | "disconnect", listener: (raw?: unknown) => void) => unknown;
  disconnect?: () => void;
};

export function runEnrichmentWorkerService(input: {
  spawnNonce: string;
  execute: (deadlineMs: number) => EnrichmentJobResult;
  close?: () => void;
  transport?: EnrichmentWorkerTransport;
}) {
  const transport = input.transport ?? {
    send: (receipt: unknown, callback?: (error: Error | null) => void) => callback
      ? process.send?.(receipt, callback) ?? false
      : process.send?.(receipt) ?? false,
    on: (event: "message" | "disconnect", listener: (raw?: unknown) => void) => process.on(event, listener),
    disconnect: () => process.disconnect?.(),
  };
  let active = false;
  let awaitingAck: { generation: number; nonce: string; sequence: number; result: EnrichmentJobResult } | null = null;
  transport.on("message", (raw) => {
    if (!raw || typeof raw !== "object") return;
    const row = raw as Record<string, unknown>;
    if (row.type === "ack" && awaitingAck && row.schema === ENRICHMENT_PROTOCOL_SCHEMA &&
        row.generation === awaitingAck.generation && row.nonce === awaitingAck.nonce &&
        row.sequence === awaitingAck.sequence) {
      const acked = awaitingAck;
      awaitingAck = null;
      transport.send({
        schema: ENRICHMENT_PROTOCOL_SCHEMA, type: "result",
        generation: acked.generation, nonce: acked.nonce, sequence: 2,
        ...acked.result,
      }, () => {
        input.close?.();
        transport.disconnect?.();
      });
      return;
    }
    if (active || row.schema !== ENRICHMENT_PROTOCOL_SCHEMA || row.type !== "run" ||
        !Number.isSafeInteger(row.generation) || Number(row.generation) < 1 ||
        !validNonce(row.nonce) || !Number.isSafeInteger(row.deadlineMs) || Number(row.deadlineMs) < 1) return;
    active = true;
    const started = performance.now();
    try {
      const result = input.execute(Number(row.deadlineMs));
      const progress = { rows: result.rows, ms: Math.max(result.ms, Math.round(performance.now() - started)) };
      awaitingAck = {
        generation: Number(row.generation), nonce: String(row.nonce), sequence: 1, result: progress,
      };
      transport.send({
        schema: ENRICHMENT_PROTOCOL_SCHEMA, type: "enrichment_job_progress",
        generation: row.generation, nonce: row.nonce, sequence: 1,
        ...progress, remaining: Math.max(0, Number(row.deadlineMs) - progress.ms),
      });
    } catch {
      input.close?.();
      transport.disconnect?.();
    }
  });
  transport.on("disconnect", () => input.close?.());
  transport.send({ schema: ENRICHMENT_PROTOCOL_SCHEMA, type: "ready", spawnNonce: input.spawnNonce });
}
