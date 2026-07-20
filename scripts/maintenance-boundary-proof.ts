/**
 * Failure-hunting proof for GitHub #150's maintenance process boundary.
 *
 * The proof uses only temporary homes, ledgers, FIFO files, loopback ports,
 * injected child adapters, and a temporary CLI bundle. It never reads the
 * live collector ledger, starts a LaunchAgent, or changes installed config.
 *
 * Run with Node 22:
 *   pnpm exec tsx scripts/maintenance-boundary-proof.ts
 */
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";

import { build } from "esbuild";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  MaintenanceProcessBoundary,
  type MaintenanceBoundaryChild,
  type MaintenanceBoundaryOptions,
} from "../packages/collector-cli/src/maintenance-boundary";
import {
  maintenanceCandidateHash,
  type MaintenanceProgressStage,
} from "../packages/collector-cli/src/maintenance-progress";
import {
  MAINTENANCE_PROTOCOL_MAX_FRAMES_PER_JOB,
  MAINTENANCE_PROTOCOL_SCHEMA,
  type MaintenanceRunRequest,
} from "../packages/collector-cli/src/maintenance-protocol";
import type { MaintenanceRunOutcome } from "../packages/collector-cli/src/maintenance";
import { createCollectorServer } from "../packages/collector-cli/src/server";

type Check = { name: string; passed: true; detail: Record<string, unknown> };
type TimerHandle = ReturnType<typeof setTimeout>;
type RequestResult = { status: number; elapsedMs: number; body: Record<string, unknown> };

const checks: Check[] = [];
const PRIVATE_PATH_SENTINEL = "maintenance-boundary-private-path-sentinel";

function pass(name: string, detail: Record<string, unknown>) {
  checks.push({ name, passed: true, detail });
}

function tick() {
  return new Promise<void>((resolve) => setImmediate(resolve));
}

async function waitFor(predicate: () => boolean, label: string, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`timed_out_waiting_for_${label}`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

async function rejectsWith(promise: Promise<unknown>, message: string) {
  await assert.rejects(promise, (error: unknown) => (
    error instanceof Error && error.message === message
  ));
}

function outcome(rawEventWrites = 1): MaintenanceRunOutcome {
  return {
    recentOnly: true,
    rollout: {
      filesRead: 1,
      parseErrors: 0,
      eventsAppended: rawEventWrites,
      activity: { discoveryEntries: 1 },
    },
    transcript: {
      filesRead: 0,
      parseErrors: 0,
      eventsAppended: 0,
      activity: { discoveryEntries: 0 },
    },
    reconciliation: { rowsChanged: 0, rowsVisited: 0 },
    repricing: { repriced: 0, rowsVisited: 0 },
    enrichment: { backward: 0, forward: 0, rowsVisited: 0 },
    rawEventWrites,
  };
}

function resultReceipt(request: MaintenanceRunRequest, value = outcome()) {
  return {
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "result" as const,
    generation: request.generation,
    nonce: request.nonce,
    sequence: 1,
    result: value,
  };
}

class FakeChild extends EventEmitter {
  readonly pid: number;
  connected = true;
  readonly sent: unknown[] = [];
  readonly signals: NodeJS.Signals[] = [];
  onSend: ((message: unknown) => void) | null = null;
  closeOn: NodeJS.Signals | null = "SIGTERM";
  closeOnShutdown = true;
  private closed = false;

  constructor(pid: number) {
    super();
    this.pid = pid;
  }

  send(message: unknown, ...args: unknown[]) {
    this.sent.push(message);
    this.onSend?.(message);
    if (this.closeOnShutdown && (message as { type?: string })?.type === "shutdown") {
      queueMicrotask(() => this.close(null));
    }
    const callback = [...args].reverse().find((value) => typeof value === "function") as
      | ((error: Error | null) => void)
      | undefined;
    queueMicrotask(() => callback?.(null));
    return true;
  }

  kill(signal: NodeJS.Signals = "SIGTERM") {
    this.signals.push(signal);
    if (this.closeOn === signal) queueMicrotask(() => this.close(signal));
    return true;
  }

  close(signal: NodeJS.Signals | null = null) {
    if (this.closed) return;
    this.closed = true;
    this.connected = false;
    this.emit("close", null, signal);
  }
}

function asBoundaryChild(child: FakeChild) {
  return child as unknown as MaintenanceBoundaryChild;
}

function ready(child: FakeChild, spawnNonce: string) {
  queueMicrotask(() => child.emit("message", {
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "ready",
    spawnNonce,
  }));
}

function fakeBoundary(
  factory: (spawnNonce: string, spawnIndex: number) => FakeChild,
  overrides: Partial<MaintenanceBoundaryOptions> = {},
) {
  let spawns = 0;
  const children: FakeChild[] = [];
  const boundary = new MaintenanceProcessBoundary({
    entryPath: "unused-by-injected-child",
    deadlineMs: 100,
    readyDeadlineMs: 100,
    termGraceMs: 10,
    killGraceMs: 10,
    initialCircuitMs: 50,
    escalatedCircuitMs: 200,
    fingerprint: async (pid) => `proof-fingerprint-${pid}`,
    ...overrides,
    spawnChild: (spawnNonce) => {
      const child = factory(spawnNonce, spawns++);
      children.push(child);
      return asBoundaryChild(child);
    },
  });
  return { boundary, children, spawnCount: () => spawns };
}

class ManualClock {
  nowMs = Date.parse("2026-07-19T12:00:00.000Z");
  private ordinal = 0;
  private timers = new Map<object, {
    due: number;
    ordinal: number;
    callback: () => void;
  }>();

  readonly now = () => this.nowMs;

  readonly setTimer = (callback: () => void, delayMs: number) => {
    const handle = {};
    this.timers.set(handle, {
      due: this.nowMs + delayMs,
      ordinal: this.ordinal++,
      callback,
    });
    return handle as TimerHandle;
  };

  readonly clearTimer = (handle: TimerHandle) => {
    this.timers.delete(handle as unknown as object);
  };

  advanceBy(ms: number) {
    this.nowMs += ms;
    for (;;) {
      const due = [...this.timers.entries()]
        .filter(([, timer]) => timer.due <= this.nowMs)
        .sort((a, b) => a[1].due - b[1].due || a[1].ordinal - b[1].ordinal)[0];
      if (!due) break;
      this.timers.delete(due[0]);
      due[1].callback();
    }
  }
}

function request(
  agent: http.Agent,
  port: number,
  method: "GET" | "POST",
  route: string,
  body = "",
  headers: Record<string, string> = {},
) {
  const startedAt = performance.now();
  return new Promise<RequestResult>((resolve, reject) => {
    const client = http.request({
      agent,
      host: "127.0.0.1",
      port,
      method,
      path: route,
      headers: {
        connection: "keep-alive",
        ...(body ? {
          "content-type": "application/json",
          "content-length": String(Buffer.byteLength(body)),
        } : {}),
        ...headers,
      },
    }, (response) => {
      const chunks: Buffer[] = [];
      response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
      response.on("end", () => {
        let parsed: Record<string, unknown> = {};
        try {
          parsed = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
        } catch {
          // Status code and latency remain useful if an unexpected body fails.
        }
        resolve({
          status: response.statusCode ?? 0,
          elapsedMs: performance.now() - startedAt,
          body: parsed,
        });
      });
    });
    client.setTimeout(5_000, () => client.destroy(new Error("proof_http_timeout")));
    client.on("error", reject);
    client.end(body);
  });
}

function percentile(values: number[], fraction: number) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)] ?? 0;
}

async function fifoAvailabilityProof() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `${PRIVATE_PATH_SENTINEL}-`));
  const fifoPath = path.join(root, "maintenance-boundary-block.fifo");
  const markerPath = path.join(root, "maintenance-boundary-blocked.marker");
  const ledgerPath = path.join(root, "proof-ledger.sqlite");
  const fixturePath = path.resolve("scripts/fixtures/maintenance-boundary-fifo-child.mjs");
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const mkfifo = await import("node:child_process").then(({ spawnSync }) => (
    spawnSync("/usr/bin/mkfifo", [fifoPath], { encoding: "utf8" })
  ));
  assert.equal(mkfifo.status, 0, "mkfifo must create the blocking fixture");

  const config = collectorConfigSchema.parse({});
  const buffer = new LocalEventBuffer(ledgerPath, { databaseBusyTimeoutMs: 0 });
  const boundary = new MaintenanceProcessBoundary({
    entryPath: fixturePath,
    execArgv: [],
    env: {
      ...process.env,
      HOME: root,
      TMPDIR: root,
      PLIMSOLL_HOME: root,
    },
    deadlineMs: 1_500,
    readyDeadlineMs: 1_000,
    termGraceMs: 100,
    killGraceMs: 800,
  });
  const server = createCollectorServer(config, buffer, {
    maintenanceStatus: () => boundary.status(),
  });
  const waveConcurrency = 100;
  const agent = new http.Agent({ keepAlive: true, maxSockets: waveConcurrency });
  let runPromise: Promise<MaintenanceRunOutcome> | null = null;
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;
    const rowsBefore = Number((buffer.database.prepare(
      "select count(*) as n from buffered_events",
    ).get() as { n: number }).n);

    const runStartedAt = performance.now();
    runPromise = boundary.run();
    void runPromise.catch(() => undefined);
    await waitFor(() => fs.existsSync(markerPath), "fifo_child_block_marker");

    const statuses = await Promise.all(Array.from({ length: waveConcurrency }, () => (
      request(agent, port, "GET", "/status")
    )));
    assert.ok(statuses.every((row) => row.status === 200), "all status requests must return 200");

    const hooks = await Promise.all(Array.from({ length: waveConcurrency }, (_, index) => request(
      agent,
      port,
      "POST",
      "/hooks/codex",
      JSON.stringify({
        hook_event_name: "PostToolUse",
        session_id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
        tool_name: "proof_tool",
      }),
      { "x-plimsoll-source": "codex" },
    )));

    assert.ok(hooks.every((row) => row.status === 202), "all hook requests must return 202");
    const statusP95 = percentile(statuses.map((row) => row.elapsedMs), 0.95);
    const statusMax = Math.max(...statuses.map((row) => row.elapsedMs));
    const hookP95 = percentile(hooks.map((row) => row.elapsedMs), 0.95);
    const hookMax = Math.max(...hooks.map((row) => row.elapsedMs));
    assert.ok(statusP95 <= 100, `status p95 ${statusP95.toFixed(1)}ms exceeded 100ms`);
    assert.ok(statusMax <= 500, `status max ${statusMax.toFixed(1)}ms exceeded 500ms`);
    assert.ok(hookP95 <= 500, `hook p95 ${hookP95.toFixed(1)}ms exceeded 500ms`);
    assert.ok(hookMax <= 1_200, `hook max ${hookMax.toFixed(1)}ms exceeded 1200ms`);

    await rejectsWith(runPromise, "maintenance_deadline_exceeded");
    const elapsedMs = performance.now() - runStartedAt;
    const status = boundary.status();
    const rowsAfter = Number((buffer.database.prepare(
      "select count(*) as n from buffered_events",
    ).get() as { n: number }).n);
    assert.equal(
      rowsAfter - rowsBefore,
      waveConcurrency,
      "each accepted hook must be durable exactly once",
    );
    assert.equal(status.reap.termSignals, 1, "deadline must send one TERM");
    assert.equal(status.reap.killSignals, 1, "stalled child must require one KILL");
    assert.equal(status.reap.reapedChildren, 1, "stalled child must emit close and be reaped");
    assert.equal(status.reap.orphanRisk, false, "reaped child must leave no orphan risk");
    assert.equal(status.childPresent, false, "reaped child must not remain attached");
    assert.ok(elapsedMs <= 2_500, `deadline and reap took ${elapsedMs.toFixed(1)}ms`);

    const serialized = JSON.stringify({ statuses: statuses.map((row) => row.body), status });
    assert.equal(serialized.includes(PRIVATE_PATH_SENTINEL), false, "receipts must remain path-free");
    pass("fifo_stall_preserves_local_http_and_reaps", {
      statusResponses: statuses.length,
      hookResponses: hooks.length,
      acceptedExactlyOnce: rowsAfter - rowsBefore,
      requestWaves: [
        { route: "/status", concurrency: waveConcurrency },
        { route: "/hooks/codex", concurrency: waveConcurrency },
      ],
      agentMaxSockets: waveConcurrency,
      statusP95Ms: Number(statusP95.toFixed(3)),
      statusMaxMs: Number(statusMax.toFixed(3)),
      hookP95Ms: Number(hookP95.toFixed(3)),
      hookMaxMs: Number(hookMax.toFixed(3)),
      deadlineToReapMs: Number(elapsedMs.toFixed(3)),
      termSignals: status.reap.termSignals,
      killSignals: status.reap.killSignals,
      reapedChildren: status.reap.reapedChildren,
      pathFree: true,
    });
  } finally {
    runPromise?.catch(() => undefined);
    agent.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await boundary.shutdown();
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

async function blockedShutdownProof() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `${PRIVATE_PATH_SENTINEL}-shutdown-`));
  const fifoPath = path.join(root, "maintenance-boundary-block.fifo");
  const markerPath = path.join(root, "maintenance-boundary-blocked.marker");
  const fixturePath = path.resolve("scripts/fixtures/maintenance-boundary-fifo-child.mjs");
  const mkfifo = await import("node:child_process").then(({ spawnSync }) => (
    spawnSync("/usr/bin/mkfifo", [fifoPath], { encoding: "utf8" })
  ));
  assert.equal(mkfifo.status, 0);
  const boundary = new MaintenanceProcessBoundary({
    entryPath: fixturePath,
    execArgv: [],
    env: { ...process.env, HOME: root, TMPDIR: root, PLIMSOLL_HOME: root },
    deadlineMs: 10_000,
    readyDeadlineMs: 1_000,
    termGraceMs: 100,
    killGraceMs: 800,
  });
  const active = boundary.run();
  void active.catch(() => undefined);
  try {
    await waitFor(() => fs.existsSync(markerPath), "shutdown_fifo_child_block_marker");
    const startedAt = performance.now();
    const stopped = await boundary.shutdown();
    const elapsedMs = performance.now() - startedAt;
    await rejectsWith(active, "maintenance_boundary_stopping");
    const status = boundary.status();
    assert.equal(stopped, true);
    assert.ok(elapsedMs <= 2_500, `blocked shutdown took ${elapsedMs.toFixed(1)}ms`);
    assert.equal(status.reap.termSignals, 1);
    assert.equal(status.reap.killSignals, 1);
    assert.equal(status.reap.reapedChildren, 1);
    assert.equal(status.reap.orphanRisk, false);
    pass("shutdown_reaps_synchronously_blocked_child", {
      shutdownMs: Number(elapsedMs.toFixed(3)),
      termSignals: status.reap.termSignals,
      killSignals: status.reap.killSignals,
      reapedChildren: status.reap.reapedChildren,
      orphanRisk: status.reap.orphanRisk,
    });
  } finally {
    await boundary.shutdown();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

async function circuitAndRecoveryProof() {
  const clock = new ManualClock();
  const candidateHash = maintenanceCandidateHash(`${PRIVATE_PATH_SENTINEL}/candidate.jsonl`);
  const runRequests: MaintenanceRunRequest[] = [];
  const harness = fakeBoundary((spawnNonce, index) => {
    const child = new FakeChild(20_000 + index);
    child.closeOn = "SIGTERM";
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type !== "run") return;
      runRequests.push(request);
      if (index === 0) {
        queueMicrotask(() => child.emit("message", {
          schema: MAINTENANCE_PROTOCOL_SCHEMA,
          type: "progress",
          generation: request.generation,
          nonce: request.nonce,
          sequence: 1,
          stage: "jsonl_open",
          source: "codex",
          candidateHash,
        }));
      } else {
        queueMicrotask(() => child.emit("message", resultReceipt(request, outcome(1))));
      }
    };
    ready(child, spawnNonce);
    return child;
  }, {
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    deadlineMs: 100,
    initialCircuitMs: 50,
    escalatedCircuitMs: 200,
  });

  const failed = harness.boundary.run();
  void failed.catch(() => undefined);
  await tick();
  clock.advanceBy(100);
  await tick();
  await rejectsWith(failed, "maintenance_deadline_exceeded");
  const afterDeadline = harness.boundary.status();
  assert.equal(afterDeadline.quarantine.candidateHash, candidateHash);
  assert.equal(afterDeadline.quarantine.stage, "jsonl_open");
  assert.equal(harness.spawnCount(), 1);

  for (let index = 0; index < 10; index += 1) {
    await rejectsWith(harness.boundary.run(), "maintenance_circuit_open");
  }
  assert.equal(harness.spawnCount(), 1, "circuit skips must not spawn replacement children");
  assert.equal(harness.boundary.status().circuit.skippedJobs, 10);

  clock.advanceBy(50);
  const recovered = await harness.boundary.run();
  assert.equal(recovered.rawEventWrites, 1);
  assert.equal(harness.spawnCount(), 2, "half-open recovery must create one replacement child");
  assert.equal(runRequests.length, 2);
  assert.deepEqual(runRequests[1]?.quarantine, {
    source: "codex",
    stage: "jsonl_open",
    candidateHash,
  });
  assert.equal(
    runRequests.filter((request) => request.quarantine?.candidateHash === candidateHash).length,
    1,
    "the deferred candidate must cross the recovery boundary exactly once",
  );

  const second = await harness.boundary.run();
  assert.equal(second.rawEventWrites, 1, "immediate second job must complete");
  assert.equal(harness.spawnCount(), 2, "immediate second job must reuse the ready child");
  assert.equal(harness.children[1]?.sent.filter((message) => (
    (message as { type?: string }).type === "run"
  )).length, 2);
  const stopped = await harness.boundary.shutdown();
  assert.equal(stopped, true);

  pass("circuit_skips_half_open_candidate_recovery", {
    skippedWithoutSpawn: 10,
    totalSpawns: harness.spawnCount(),
    deferredCandidateCrossings: 1,
    recoveredWrites: recovered.rawEventWrites,
    immediateSecondJobWrites: second.rawEventWrites,
    stopped,
  });
}

async function spawnFailureAndConcurrentStartupProof() {
  const clock = new ManualClock();
  const spawnFailure = fakeBoundary((spawnNonce, index) => {
    if (index === 0) throw new Error(`${PRIVATE_PATH_SENTINEL}/spawn-detail`);
    const child = new FakeChild(20_100 + index);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type === "run") {
        queueMicrotask(() => child.emit("message", resultReceipt(request, outcome(4))));
      }
    };
    ready(child, spawnNonce);
    return child;
  }, {
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    initialCircuitMs: 50,
  });
  await rejectsWith(spawnFailure.boundary.run(), "maintenance_worker_spawn_failed");
  const failedStatus = spawnFailure.boundary.status();
  assert.equal(failedStatus.state, "circuit_open");
  assert.equal(failedStatus.lastOutcome, "failed");
  assert.equal(failedStatus.lastFailure, "maintenance_worker_spawn_failed");
  assert.equal(failedStatus.childPresent, false);
  assert.equal(failedStatus.childReady, false);
  assert.equal(failedStatus.inFlight, false);
  await rejectsWith(spawnFailure.boundary.run(), "maintenance_circuit_open");
  assert.equal(spawnFailure.spawnCount(), 1, "open circuit must not retry a throwing spawn");
  clock.advanceBy(50);
  assert.equal((await spawnFailure.boundary.run()).rawEventWrites, 4);
  assert.equal(spawnFailure.spawnCount(), 2);
  assert.equal(await spawnFailure.boundary.shutdown(), true);

  let heldNonce = "";
  const exactConcurrent = fakeBoundary((spawnNonce) => {
    heldNonce = spawnNonce;
    const child = new FakeChild(20_201);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type === "run") {
        queueMicrotask(() => child.emit("message", resultReceipt(request, outcome(6))));
      }
    };
    return child;
  });
  const exactFirst = exactConcurrent.boundary.run();
  void exactFirst.catch(() => undefined);
  await tick();
  assert.equal(exactConcurrent.boundary.status().inFlight, true, "lazy startup reserves the run slot");
  await rejectsWith(exactConcurrent.boundary.run(), "maintenance_job_already_in_flight");
  const heldChild = exactConcurrent.children[0];
  assert.ok(heldChild);
  ready(heldChild, heldNonce);
  assert.equal((await exactFirst).rawEventWrites, 6);
  assert.equal(exactConcurrent.boundary.status().generation, 1);
  assert.equal(heldChild.sent.filter((message) => (
    (message as { type?: string }).type === "run"
  )).length, 1);
  assert.equal(await exactConcurrent.boundary.shutdown(), true);

  pass("spawn_failure_circuits_and_lazy_start_is_single_flight", {
    spawnFailureNormalized: true,
    openCircuitSpawnAttempts: 1,
    recoveredWrites: 4,
    concurrentSecondRejected: true,
    acceptedGenerations: 1,
  });
}

async function shutdownDuringLazyStartupProof() {
  let releaseFingerprint!: (value: string | null) => void;
  let spawnNonce = "";
  const harness = fakeBoundary((nonce) => {
    spawnNonce = nonce;
    const child = new FakeChild(20_250);
    ready(child, nonce);
    return child;
  }, {
    fingerprint: async () => new Promise<string | null>((resolve) => {
      releaseFingerprint = resolve;
    }),
  });
  const pending = harness.boundary.run();
  void pending.catch(() => undefined);
  await tick();
  assert.ok(spawnNonce);
  const stopping = harness.boundary.shutdown();
  releaseFingerprint("startup-process");
  await rejectsWith(pending, "maintenance_boundary_stopping");
  assert.equal(await stopping, true);
  const status = harness.boundary.status();
  assert.equal(status.generation, 0);
  assert.equal(status.state, "stopped");
  assert.equal(status.accepting, false);
  assert.equal(harness.children[0]!.sent.some((message) => (
    (message as { type?: string }).type === "run"
  )), false);
  pass("shutdown_during_lazy_startup_admits_no_job", {
    generation: status.generation,
    stopped: status.state === "stopped",
    runFrames: 0,
    reapedChildren: status.reap.reapedChildren,
  });
}

async function disconnectErrorAndPidReuseProof() {
  const clock = new ManualClock();
  const lifecycle = fakeBoundary((spawnNonce, index) => {
    const child = new FakeChild(20_300 + index);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type === "run") {
        queueMicrotask(() => child.emit("message", resultReceipt(request, outcome(index + 1))));
      }
    };
    ready(child, spawnNonce);
    return child;
  }, {
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    initialCircuitMs: 50,
  });
  await lifecycle.boundary.run();
  const disconnected = lifecycle.children[0]!;
  disconnected.connected = false;
  disconnected.emit("disconnect");
  await waitFor(() => lifecycle.boundary.status().state === "circuit_open", "disconnect_circuit");
  const disconnectStatus = lifecycle.boundary.status();
  assert.equal(disconnectStatus.lastOutcome, "failed");
  assert.equal(disconnectStatus.lastFailure, "maintenance_worker_disconnected");
  assert.equal(disconnectStatus.childPresent, false);
  assert.equal(disconnectStatus.reap.reapedChildren, 1);
  await rejectsWith(lifecycle.boundary.run(), "maintenance_circuit_open");

  clock.advanceBy(50);
  await lifecycle.boundary.run();
  lifecycle.children[1]!.emit("error", new Error(`${PRIVATE_PATH_SENTINEL}/idle-error`));
  await waitFor(() => lifecycle.boundary.status().state === "circuit_open", "idle_error_circuit");
  assert.equal(lifecycle.boundary.status().lastFailure, "maintenance_worker_error");
  assert.equal(lifecycle.boundary.status().reap.reapedChildren, 2);

  clock.advanceBy(50);
  await lifecycle.boundary.run();
  const stoppingChild = lifecycle.children[2]!;
  const stopped = lifecycle.boundary.shutdown();
  stoppingChild.emit("error", new Error(`${PRIVATE_PATH_SENTINEL}/expected-stop-error`));
  assert.equal(await stopped, true);
  assert.equal(lifecycle.boundary.status().circuit.failureCount, 0);

  let fingerprintCalls = 0;
  const fingerprintBindings: Array<{ parentPid: number; spawnNonce: string }> = [];
  const reuse = fakeBoundary((spawnNonce) => {
    const child = new FakeChild(20_400);
    child.closeOn = null;
    ready(child, spawnNonce);
    return child;
  }, {
    deadlineMs: 5,
    termGraceMs: 5,
    killGraceMs: 5,
    fingerprint: async (_pid, binding) => {
      fingerprintBindings.push(binding);
      fingerprintCalls += 1;
      return fingerprintCalls === 1 ? "original-process" : "same-second-reused-process";
    },
  });
  const failed = reuse.boundary.run();
  void failed.catch(() => undefined);
  await rejectsWith(failed, "maintenance_deadline_exceeded");
  const reuseStatus = reuse.boundary.status();
  assert.equal(reuseStatus.reap.termSignals, 0);
  assert.equal(reuseStatus.reap.killSignals, 0);
  assert.ok(reuseStatus.reap.pidMismatches >= 2);
  assert.equal(reuseStatus.reap.orphanRisk, true);
  assert.ok(fingerprintBindings.length >= 3);
  assert.ok(fingerprintBindings.every((binding) => (
    binding.parentPid === process.pid && binding.spawnNonce === fingerprintBindings[0]!.spawnNonce
  )));
  const timedOutAt = reuseStatus.lastTimedOutAt;
  const failureCount = reuseStatus.circuit.failureCount;
  reuse.children[0]!.close();
  await tick();
  const lateCloseStatus = reuse.boundary.status();
  assert.equal(lateCloseStatus.childPresent, false);
  assert.equal(lateCloseStatus.reap.orphanRisk, false);
  assert.equal(lateCloseStatus.lastOutcome, "timed_out");
  assert.equal(lateCloseStatus.lastFailure, "maintenance_deadline_exceeded");
  assert.equal(lateCloseStatus.lastTimedOutAt, timedOutAt);
  assert.equal(lateCloseStatus.circuit.failureCount, failureCount);
  assert.equal(lateCloseStatus.state, "circuit_open");
  assert.equal(await reuse.boundary.shutdown(), true);

  pass("disconnect_error_and_pid_reuse_fail_closed", {
    disconnectedChildReaped: disconnectStatus.reap.reapedChildren,
    idleErrorReaped: true,
    stoppingErrorIgnored: true,
    unrelatedTermSignals: reuseStatus.reap.termSignals,
    unrelatedKillSignals: reuseStatus.reap.killSignals,
    pidMismatches: reuseStatus.reap.pidMismatches,
    nonceAndParentBound: true,
    lateTrackedClosePreservedTimeout: true,
    lateTrackedCloseFailureCount: lateCloseStatus.circuit.failureCount,
  });
}

async function controlFrameFloodProof() {
  let releaseReadyFingerprint!: (value: string | null) => void;
  let readyFingerprintReleased = false;
  let readySpawnNonce = "";
  const readyFlood = fakeBoundary((spawnNonce) => {
    readySpawnNonce = spawnNonce;
    return new FakeChild(20_450);
  }, {
    fingerprint: async () => {
      if (readyFingerprintReleased) return "ready-flood-process";
      return new Promise<string | null>((resolve) => {
        releaseReadyFingerprint = (value) => {
          readyFingerprintReleased = true;
          resolve(value);
        };
      });
    },
  });
  const readyRun = readyFlood.boundary.run();
  void readyRun.catch(() => undefined);
  await tick();
  const readyChild = readyFlood.children[0]!;
  for (let index = 0; index < 200; index += 1) {
    readyChild.emit("message", {
      schema: MAINTENANCE_PROTOCOL_SCHEMA,
      type: "ready",
      spawnNonce: readySpawnNonce,
    });
  }
  assert.equal(readyFlood.boundary.status().generation, 0);
  releaseReadyFingerprint("ready-flood-process");
  await rejectsWith(readyRun, "maintenance_protocol_control_frame_limit");
  await waitFor(() => readyFlood.boundary.status().state === "circuit_open", "ready_flood_circuit");
  const readyStatus = readyFlood.boundary.status();
  assert.equal(readyStatus.protocol.invalidFrames, 1);
  assert.ok(
    readyStatus.protocol.lateFrames <= MAINTENANCE_PROTOCOL_MAX_FRAMES_PER_JOB,
    "ready flood must leave only bounded pending readiness continuations",
  );
  assert.equal(readyStatus.generation, 0);
  assert.equal(readyStatus.childPresent, false);
  assert.equal(readyStatus.reap.reapedChildren, 1);
  assert.equal(readyStatus.reap.orphanRisk, false);
  assert.equal(readyStatus.circuit.failureCount, 1);
  assert.equal(await readyFlood.boundary.shutdown(), true);

  const closedFlood = fakeBoundary((spawnNonce) => {
    const child = new FakeChild(20_451);
    child.closeOnShutdown = false;
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type === "run") {
        queueMicrotask(() => child.emit("message", resultReceipt(request)));
      }
    };
    ready(child, spawnNonce);
    return child;
  }, {
    termGraceMs: 100,
  });
  await closedFlood.boundary.run();
  const closedChild = closedFlood.children[0]!;
  const beforeClosedFlood = closedFlood.boundary.status();
  const stopping = closedFlood.boundary.shutdown();
  for (let index = 0; index < 200; index += 1) {
    closedChild.emit("message", {
      schema: MAINTENANCE_PROTOCOL_SCHEMA,
      type: "closed",
      nonce: randomUUID(),
    });
  }
  assert.equal(await stopping, true);
  await tick();
  const closedStatus = closedFlood.boundary.status();
  assert.equal(closedStatus.accepting, false);
  assert.equal(closedStatus.state, "stopped");
  assert.equal(closedStatus.stage, "closed");
  assert.equal(closedStatus.protocol.invalidFrames, beforeClosedFlood.protocol.invalidFrames);
  assert.equal(closedStatus.lastFailure, beforeClosedFlood.lastFailure);
  assert.equal(closedStatus.lastOutcome, beforeClosedFlood.lastOutcome);
  assert.equal(closedStatus.childPresent, false);
  assert.equal(closedStatus.reap.reapedChildren, 1);
  assert.equal(closedStatus.reap.orphanRisk, false);
  assert.equal(closedStatus.circuit.failureCount, beforeClosedFlood.circuit.failureCount);

  pass("ready_and_closed_control_frame_floods_fail_closed", {
    injectedReadyFrames: 200,
    admittedReadyGenerations: readyStatus.generation,
    boundedReadyContinuations: readyStatus.protocol.lateFrames,
    readyFloodReaped: readyStatus.reap.reapedChildren,
    injectedClosedFrames: 200,
    closedFloodReaped: closedStatus.reap.reapedChildren,
    readyCircuitFailures: readyStatus.circuit.failureCount,
    closedCircuitFailures: closedStatus.circuit.failureCount,
    closedShutdownState: closedStatus.state,
    closedShutdownStage: closedStatus.stage,
  });
}

async function terminalShutdownMonotonicityProof() {
  let fingerprintCalls = 0;
  let releaseSignalFingerprint!: (value: string | null) => void;
  const harness = fakeBoundary((spawnNonce) => {
    const child = new FakeChild(20_452);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type === "run") {
        queueMicrotask(() => child.emit("message", resultReceipt(request)));
      }
    };
    ready(child, spawnNonce);
    return child;
  }, {
    fingerprint: async () => {
      fingerprintCalls += 1;
      if (fingerprintCalls === 1) return "terminal-race-process";
      return new Promise<string | null>((resolve) => {
        releaseSignalFingerprint = resolve;
      });
    },
    termGraceMs: 100,
  });
  await harness.boundary.run();
  const child = harness.children[0]!;
  child.emit("message", {
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "invalid_control_for_terminal_race",
  });
  await waitFor(() => fingerprintCalls === 2, "terminal_race_signal_fingerprint");
  const failureBeforeShutdown = harness.boundary.status();
  assert.equal(failureBeforeShutdown.lastOutcome, "failed");
  assert.equal(failureBeforeShutdown.lastFailure, "maintenance_protocol_invalid");
  assert.equal(failureBeforeShutdown.circuit.failureCount, 0);

  const stopped = await harness.boundary.shutdown();
  assert.equal(stopped, true);
  const terminalBeforeRelease = harness.boundary.status();
  assert.equal(terminalBeforeRelease.accepting, false);
  assert.equal(terminalBeforeRelease.state, "stopped");
  assert.equal(terminalBeforeRelease.stage, "closed");

  releaseSignalFingerprint("terminal-race-process");
  await waitFor(
    () => harness.boundary.status().reap.pidMismatches >= 1,
    "terminal_race_failure_continuation",
  );
  await tick();
  const terminalAfterRelease = harness.boundary.status();
  assert.equal(terminalAfterRelease.accepting, false);
  assert.equal(terminalAfterRelease.state, "stopped");
  assert.equal(terminalAfterRelease.stage, "closed");
  assert.equal(terminalAfterRelease.lastOutcome, "failed");
  assert.equal(terminalAfterRelease.lastFailure, "maintenance_protocol_invalid");
  assert.equal(terminalAfterRelease.circuit.failureCount, 0);
  assert.equal(terminalAfterRelease.childPresent, false);
  assert.equal(terminalAfterRelease.reap.orphanRisk, false);

  pass("terminal_shutdown_is_monotonic_after_async_failure", {
    shutdownReturned: stopped,
    accepting: terminalAfterRelease.accepting,
    state: terminalAfterRelease.state,
    stage: terminalAfterRelease.stage,
    preexistingFailurePreserved: terminalAfterRelease.lastFailure,
    postShutdownCircuitFailures: terminalAfterRelease.circuit.failureCount,
    childPresent: terminalAfterRelease.childPresent,
    orphanRisk: terminalAfterRelease.reap.orphanRisk,
  });
}

async function progressStageTimeoutProof() {
  const stageCases = {
    source_scan: "none",
    discovery_directory: "sha256",
    discovery_read: "sha256",
    candidate_metadata: "sha256",
    jsonl_open: "sha256",
    jsonl_validation: "sha256",
    git_context: "sha256",
  } as const satisfies Record<MaintenanceProgressStage, "none" | "sha256">;
  const stages = Object.keys(stageCases) as MaintenanceProgressStage[];
  const stageReceipts: Array<Record<string, unknown>> = [];

  for (const [index, stage] of stages.entries()) {
    const privateCandidate = `${PRIVATE_PATH_SENTINEL}/${stage}/candidate.jsonl`;
    const candidateHash = stageCases[stage] === "none"
      ? null
      : maintenanceCandidateHash(privateCandidate);
    const harness = fakeBoundary((spawnNonce) => {
      const child = new FakeChild(20_460 + index);
      child.onSend = (raw) => {
        const request = raw as MaintenanceRunRequest;
        if (request.type !== "run") return;
        queueMicrotask(() => child.emit("message", {
          schema: MAINTENANCE_PROTOCOL_SCHEMA,
          type: "progress",
          generation: request.generation,
          nonce: request.nonce,
          sequence: 1,
          stage,
          source: index % 2 === 0 ? "codex" : "claude_code",
          candidateHash,
        }));
      };
      ready(child, spawnNonce);
      return child;
    }, {
      deadlineMs: 5,
      termGraceMs: 5,
      killGraceMs: 5,
    });

    const stalled = harness.boundary.run();
    void stalled.catch(() => undefined);
    await rejectsWith(stalled, "maintenance_deadline_exceeded");
    const status = harness.boundary.status();
    const child = harness.children[0]!;
    assert.equal(status.lastOutcome, "timed_out");
    assert.equal(status.lastFailure, "maintenance_deadline_exceeded");
    assert.equal(status.state, "circuit_open");
    assert.equal(status.circuit.failureCount, 1);
    assert.equal(status.quarantine.stage, stage);
    assert.equal(status.quarantine.candidateHash, candidateHash);
    assert.ok(status.quarantine.until);
    assert.equal(status.childPresent, false);
    assert.equal(status.reap.reapedChildren, 1);
    assert.equal(status.reap.orphanRisk, false);
    assert.equal(
      child.sent.some((message) => (
        (message as { type?: string; sequence?: number }).type === "ack" &&
        (message as { sequence?: number }).sequence === 1
      )),
      true,
      `${stage} progress receipt must be acknowledged before the induced stall`,
    );
    assert.equal(
      JSON.stringify(status).includes(PRIVATE_PATH_SENTINEL),
      false,
      `${stage} quarantine must remain path-free`,
    );
    stageReceipts.push({
      stage,
      candidateIdentity: candidateHash === null ? "none" : "sha256",
      timedOut: status.lastOutcome === "timed_out",
      circuitFailures: status.circuit.failureCount,
      reapedChildren: status.reap.reapedChildren,
      pathFree: true,
    });
    assert.equal(await harness.boundary.shutdown(), true);
  }

  pass("all_progress_stages_timeout_quarantine_and_reap", {
    stages: stageReceipts,
    injectedAfterProductionSchemaReceipt: true,
    operatingSystemRegularFileStallClaimed: false,
  });
}

function staticParentFilesystemIsolationProof() {
  const boundarySource = fs.readFileSync(
    path.resolve("packages/collector-cli/src/maintenance-boundary.ts"),
    "utf8",
  );
  assert.doesNotMatch(boundarySource, /from\s+["'](?:node:)?fs["']/);
  assert.doesNotMatch(
    boundarySource,
    /\b(?:readFile|readdir|lstat|stat|open|createReadStream)(?:Sync)?\s*\(/,
  );
  pass("parent_boundary_has_no_user_filesystem_reads", {
    staticSourceChecked: "packages/collector-cli/src/maintenance-boundary.ts",
    filesystemModuleImported: false,
    userFilesystemReadPrimitivePresent: false,
    processFingerprintSurface: "/bin/ps bounded child identity only",
  });
}

async function malformedOversizedFrameProof() {
  const harness = fakeBoundary((spawnNonce) => {
    const child = new FakeChild(20_500);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type !== "run") return;
      queueMicrotask(() => child.emit("message", {
        ...resultReceipt(request),
        unexpected: "x".repeat(70 * 1024),
      }));
    };
    ready(child, spawnNonce);
    return child;
  });
  const failed = harness.boundary.run();
  void failed.catch(() => undefined);
  await rejectsWith(failed, "maintenance_protocol_invalid");
  const status = harness.boundary.status();
  assert.equal(status.protocol.oversizedFrames, 1);
  assert.equal(status.protocol.invalidFrames, 1);
  assert.equal(status.lastOutcome, "failed");
  assert.equal(status.reap.reapedChildren, 1);
  assert.equal(status.reap.orphanRisk, false);
  assert.equal(await harness.boundary.shutdown(), true);
  pass("malformed_oversized_frame_fails_closed", {
    oversizedFrames: status.protocol.oversizedFrames,
    invalidFrames: status.protocol.invalidFrames,
    reapedChildren: status.reap.reapedChildren,
    rawValueInReceipt: false,
  });
}

async function realWorkerCrashProof() {
  const fixturePath = path.resolve("scripts/fixtures/maintenance-boundary-crash-child.mjs");
  const boundary = new MaintenanceProcessBoundary({
    entryPath: fixturePath,
    execArgv: [],
    env: { ...process.env },
    deadlineMs: 3_000,
    readyDeadlineMs: 1_000,
    termGraceMs: 100,
    killGraceMs: 500,
  });
  const failed = boundary.run();
  void failed.catch(() => undefined);
  await assert.rejects(failed, (error: unknown) => (
    error instanceof Error && [
      "maintenance_worker_disconnected",
      "maintenance_worker_closed",
    ].includes(error.message)
  ));
  await waitFor(() => boundary.status().state === "circuit_open", "real_crash_circuit");
  const status = boundary.status();
  assert.equal(status.lastOutcome, "failed");
  assert.equal(status.childPresent, false);
  assert.equal(status.reap.reapedChildren, 1);
  assert.equal(status.reap.orphanRisk, false);
  assert.equal(await boundary.shutdown(), true);
  pass("real_worker_crash_is_reaped_and_circuited", {
    lastFailure: status.lastFailure,
    reapedChildren: status.reap.reapedChildren,
    orphanRisk: status.reap.orphanRisk,
    childPresent: status.childPresent,
  });
}

async function staleFenceAndImmediateSecondJobProof() {
  let firstRequest: MaintenanceRunRequest | null = null;
  let runNumber = 0;
  const harness = fakeBoundary((spawnNonce) => {
    const child = new FakeChild(21_000);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type !== "run") return;
      runNumber += 1;
      if (runNumber === 1) {
        firstRequest = request;
        queueMicrotask(() => child.emit("message", resultReceipt(request, outcome(1))));
        return;
      }
      const old = firstRequest;
      assert.ok(old);
      queueMicrotask(() => {
        child.emit("message", resultReceipt(old, outcome(99)));
        child.emit("message", resultReceipt(request, outcome(2)));
      });
    };
    ready(child, spawnNonce);
    return child;
  });

  const first = await harness.boundary.run();
  const second = await harness.boundary.run();
  const status = harness.boundary.status();
  assert.equal(first.rawEventWrites, 1);
  assert.equal(second.rawEventWrites, 2, "late prior result must not settle the current job");
  assert.equal(status.protocol.lateFrames, 1);
  assert.equal(harness.spawnCount(), 1);
  assert.equal(status.reap.termSignals, 0);
  assert.equal(await harness.boundary.shutdown(), true);

  pass("stale_result_fenced_and_second_job_immediate", {
    spawns: harness.spawnCount(),
    lateFrames: status.protocol.lateFrames,
    acceptedSecondResultWrites: second.rawEventWrites,
    terminationSignalsBeforeShutdown: status.reap.termSignals,
  });
}

async function deterministicRaceProof() {
  // A result one injected millisecond before the deadline wins.
  const beforeClock = new ManualClock();
  const before = fakeBoundary((spawnNonce) => {
    const child = new FakeChild(22_000);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type === "run") {
        beforeClock.setTimer(() => child.emit("message", resultReceipt(request, outcome(7))), 9);
      }
    };
    ready(child, spawnNonce);
    return child;
  }, {
    now: beforeClock.now,
    setTimer: beforeClock.setTimer,
    clearTimer: beforeClock.clearTimer,
    deadlineMs: 10,
  });
  const beforeRun = before.boundary.run();
  await tick();
  beforeClock.advanceBy(9);
  await tick();
  assert.equal((await beforeRun).rawEventWrites, 7);
  assert.equal(await before.boundary.shutdown(), true);

  // At the exact deadline the already-registered deadline callback wins;
  // the same-time result is stale and cannot resurrect the timed-out job.
  const equalClock = new ManualClock();
  const equal = fakeBoundary((spawnNonce) => {
    const child = new FakeChild(22_001);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type === "run") {
        equalClock.setTimer(() => child.emit("message", resultReceipt(request, outcome(8))), 10);
      }
    };
    ready(child, spawnNonce);
    return child;
  }, {
    now: equalClock.now,
    setTimer: equalClock.setTimer,
    clearTimer: equalClock.clearTimer,
    deadlineMs: 10,
  });
  const equalRun = equal.boundary.run();
  void equalRun.catch(() => undefined);
  await tick();
  equalClock.advanceBy(10);
  await tick();
  await rejectsWith(equalRun, "maintenance_deadline_exceeded");
  assert.equal(equal.boundary.status().protocol.lateFrames, 1);

  // Ready at deadline-1 wins after fingerprinting; ready exactly at the
  // deadline fails closed because readiness was not fully established.
  const readyBeforeClock = new ManualClock();
  const readyBefore = fakeBoundary((spawnNonce) => {
    const child = new FakeChild(22_002);
    readyBeforeClock.setTimer(() => child.emit("message", {
      schema: MAINTENANCE_PROTOCOL_SCHEMA,
      type: "ready",
      spawnNonce,
    }), 9);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type === "run") queueMicrotask(() => child.emit("message", resultReceipt(request)));
    };
    return child;
  }, {
    now: readyBeforeClock.now,
    setTimer: readyBeforeClock.setTimer,
    clearTimer: readyBeforeClock.clearTimer,
    readyDeadlineMs: 10,
  });
  const readyBeforeRun = readyBefore.boundary.run();
  readyBeforeClock.advanceBy(9);
  await tick();
  assert.equal((await readyBeforeRun).rawEventWrites, 1);
  assert.equal(await readyBefore.boundary.shutdown(), true);

  const readyEqualClock = new ManualClock();
  const readyEqual = fakeBoundary((spawnNonce) => {
    const child = new FakeChild(22_003);
    readyEqualClock.setTimer(() => child.emit("message", {
      schema: MAINTENANCE_PROTOCOL_SCHEMA,
      type: "ready",
      spawnNonce,
    }), 10);
    return child;
  }, {
    now: readyEqualClock.now,
    setTimer: readyEqualClock.setTimer,
    clearTimer: readyEqualClock.clearTimer,
    readyDeadlineMs: 10,
  });
  const readyEqualRun = readyEqual.boundary.run();
  void readyEqualRun.catch(() => undefined);
  readyEqualClock.advanceBy(10);
  await tick();
  await rejectsWith(readyEqualRun, "maintenance_worker_ready_timeout");
  assert.equal(readyEqual.boundary.status().reap.reapedChildren, 1);

  pass("deterministic_result_and_ready_deadline_races", {
    resultBeforeDeadline: "accepted",
    resultAtDeadline: "timed_out_and_stale",
    readyBeforeDeadline: "accepted",
    readyAtDeadline: "failed_closed_and_reaped",
  });
}

async function runHiddenWorker(entryPath: string, execArgv: string[], label: string) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-${label}-`));
  const boundary = new MaintenanceProcessBoundary({
    entryPath,
    execArgv,
    env: {
      ...process.env,
      HOME: home,
      TMPDIR: home,
      PLIMSOLL_HOME: home,
    },
    deadlineMs: 3_000,
    readyDeadlineMs: 3_000,
    termGraceMs: 250,
    killGraceMs: 750,
  });
  const startedAt = performance.now();
  try {
    const result = await boundary.run();
    const shutdownStartedAt = performance.now();
    const stopped = await boundary.shutdown();
    const shutdownMs = performance.now() - shutdownStartedAt;
    assert.equal(result.recentOnly, true);
    assert.equal(stopped, true);
    assert.ok(shutdownMs <= 2_500, `${label} shutdown exceeded 2500ms`);
    assert.equal(boundary.status().childPresent, false);
    assert.equal(boundary.status().reap.orphanRisk, false);
    return {
      startupRunShutdownMs: Number((performance.now() - startedAt).toFixed(3)),
      shutdownMs: Number(shutdownMs.toFixed(3)),
      reapedChildren: boundary.status().reap.reapedChildren,
    };
  } finally {
    await boundary.shutdown();
    fs.rmSync(home, { recursive: true, force: true });
  }
}

async function sourceAndDistWorkerProof() {
  const sourceEntry = path.resolve("packages/collector-cli/src/cli.ts");
  const buildRoot = fs.mkdtempSync(path.resolve(`.maintenance-boundary-build-${randomUUID()}-`));
  const distEntry = path.join(buildRoot, "cli.mjs");
  try {
    const source = await runHiddenWorker(sourceEntry, process.execArgv, "source-worker");
    await build({
      entryPoints: [sourceEntry],
      bundle: true,
      platform: "node",
      target: "node20",
      format: "esm",
      outfile: distEntry,
      external: ["better-sqlite3"],
      logLevel: "silent",
    });
    const bundledText = fs.readFileSync(distEntry, "utf8");
    assert.ok(bundledText.includes("__maintenance_worker"), "bundle must retain hidden worker mode");
    const dist = await runHiddenWorker(distEntry, [], "dist-worker");
    pass("source_and_dist_hidden_worker_startup", {
      source,
      dist,
      hiddenModeBundled: true,
      temporaryHomesOnly: true,
    });
  } finally {
    fs.rmSync(buildRoot, { recursive: true, force: true });
  }
}

async function pathFreeReceiptProof() {
  const candidateHash = maintenanceCandidateHash(`${PRIVATE_PATH_SENTINEL}/rollout.jsonl`);
  let requestSeen: MaintenanceRunRequest | null = null;
  let progressStatus: ReturnType<MaintenanceProcessBoundary["status"]> | null = null;
  const harness = fakeBoundary((spawnNonce) => {
    const child = new FakeChild(23_000);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type !== "run") return;
      requestSeen = request;
      queueMicrotask(() => {
        child.emit("message", {
          schema: MAINTENANCE_PROTOCOL_SCHEMA,
          type: "progress",
          generation: request.generation,
          nonce: request.nonce,
          sequence: 1,
          stage: "candidate_metadata",
          source: "claude_code",
          candidateHash,
        });
        progressStatus = harness.boundary.status();
        child.emit("message", {
          ...resultReceipt(request, outcome(3)),
          sequence: 2,
        });
      });
    };
    ready(child, spawnNonce);
    return child;
  });
  const result = await harness.boundary.run();
  const projected = {
    status: harness.boundary.status(),
    progressStatus,
    result,
    request: requestSeen,
  };
  assert.equal(JSON.stringify(projected).includes(PRIVATE_PATH_SENTINEL), false);
  assert.equal(
    JSON.stringify(projected).includes(candidateHash),
    false,
    "successful candidate progress must remain transient rather than enter status/result receipts",
  );
  assert.equal(await harness.boundary.shutdown(), true);
  pass("maintenance_receipts_are_path_free", {
    candidateIdentity: "transient_child_frame_only",
    rawPathPresent: false,
    resultWrites: result.rawEventWrites,
  });
}

async function main() {
  assert.equal(process.versions.node.split(".")[0], "22", "proof requires Node 22");
  await fifoAvailabilityProof();
  await blockedShutdownProof();
  await circuitAndRecoveryProof();
  await spawnFailureAndConcurrentStartupProof();
  await shutdownDuringLazyStartupProof();
  await disconnectErrorAndPidReuseProof();
  await controlFrameFloodProof();
  await terminalShutdownMonotonicityProof();
  await progressStageTimeoutProof();
  staticParentFilesystemIsolationProof();
  await malformedOversizedFrameProof();
  await realWorkerCrashProof();
  await staleFenceAndImmediateSecondJobProof();
  await deterministicRaceProof();
  await sourceAndDistWorkerProof();
  await pathFreeReceiptProof();

  const receipt = {
    schemaVersion: 1,
    proof: "maintenance_boundary",
    node: process.versions.node,
    checks,
    summary: {
      passed: checks.length,
      failed: 0,
      liveServicesTouched: false,
      productionFilesChanged: false,
    },
  };
  assert.equal(JSON.stringify(receipt).includes(PRIVATE_PATH_SENTINEL), false);
  console.log(JSON.stringify(receipt, null, 2));
}

main().catch((error) => {
  console.error(JSON.stringify({
    proof: "maintenance_boundary",
    ok: false,
    reason: error instanceof Error ? error.message : "proof_failed",
  }));
  process.exitCode = 1;
});
