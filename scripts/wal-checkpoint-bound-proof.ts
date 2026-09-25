/**
 * eco-6hoxj.163.24 (round 2): the ledger WAL stays bounded with no
 * maintenance child, and the checkpoint worker is watched and visible.
 *
 * The daemon copies its WAL back on a worker thread (WalCheckpointWorker).
 * SQLite rewinds the WAL only when a write begins while every frame has been
 * copied back; intake commits that land during a concurrent pass prevent that,
 * so with the daemon connection's own automatic checkpoint turned off the WAL
 * grew without limit whenever the maintenance child's TRUNCATE was not running
 * (review B1: 11 -> 647 MB in 300 s at ~19 commits/s; 21 GB in 60 s under
 * continuous commits). Now the worker finishes with a FULL checkpoint once the
 * WAL holds 10,000 frames, so the next write rewinds it, and the daemon
 * connection keeps a 20,000-frame automatic checkpoint as the hard bound.
 *
 * This proof drives continuous intake through LocalEventBuffer.appendMany (the
 * OTLP route's own commit, retried on SQLITE_BUSY as the route retries) with
 * the worker started exactly as the daemon starts it and nothing else
 * checkpointing, and requires the WAL file to stay under WAL_LIMIT_MIB. With
 * the hard bound turned off, the worker alone must keep the WAL under that
 * bound, so its checkpoint need not run inside a commit. A reader that holds
 * the WAL past the bound must not hand its backlog to that checkpoint when it
 * lets go: the worker copies it instead.
 *
 *   pnpm proof:wal-checkpoint-bound
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { isSqliteContentionError } from "../packages/collector-cli/src/sqlite-contention";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import * as walCheckpointModule from "../packages/collector-cli/src/wal-checkpoint-worker";

/** Twice the 20,000-frame bound at 4 KiB pages: room for the commits of one concurrent pass. */
const WAL_LIMIT_MIB = 160;
/**
 * Counted, not timed, so a slow runner proves as much as a fast one: each
 * commit adds ~0.8 MiB of WAL here, so an unbounded WAL passes the limit
 * after ~200 of them.
 */
const INTAKE_COMMITS = Number(process.env.WAL_BOUND_INTAKE_COMMITS ?? 800);
const INTAKE_CAP_MS = 240_000;
const EVENTS_PER_COMMIT = 16;
/** Waits only bound how long a failure takes to show; a slow runner needs them long. */
const WAIT_MS = 60_000;

const root = process.env.TMPDIR;
assert.ok(root && fs.realpathSync(root) === root && root.startsWith(process.env.HOME + path.sep),
  "run under the CI layout: TMPDIR inside the synthetic HOME");
const dir = fs.mkdtempSync(path.join(root, "wal-checkpoint-bound-"));
const tenantId = "00000000-0000-4000-8000-000000000b01";
const deviceId = "wal-checkpoint-bound-device";
const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest", tenantId, installKey: "wal-bound-install", deviceId, managed: true,
});

type Check = { name: string; passed: boolean; detail?: unknown };
const checks: Check[] = [];
const measurements: Record<string, unknown> = { walLimitMiB: WAL_LIMIT_MIB, intakeCommits: INTAKE_COMMITS };

async function check(name: string, run: () => unknown | Promise<unknown>) {
  try {
    const detail = await run();
    checks.push({ name, passed: true, ...(detail === undefined ? {} : { detail }) });
  } catch (error) {
    checks.push({ name, passed: false, detail: error instanceof Error ? error.message : String(error) });
  }
  const last = checks.at(-1)!;
  console.log(JSON.stringify({ check: name, passed: last.passed, ...(last.passed ? {} : { detail: last.detail }) }));
}

function openLedger(name: string) {
  return new LocalEventBuffer(path.join(dir, name), {
    workspaceId: tenantId, deviceId, databaseBusyTimeoutMs: 0,
    delivery: { enabled: true, limits: config.delivery },
  });
}

const walBytes = (buffer: LocalEventBuffer) => {
  try {
    return fs.statSync(`${buffer.database.name}-wal`).size;
  } catch {
    return 0;
  }
};
const mib = (bytes: number) => Math.round((bytes / 1_048_576) * 10) / 10;
let eventNumber = 0;
let busyRetries = 0;

/** One commit, retried while the worker's FULL holds the writer lock (the OTLP route retries the same way). */
async function commit(buffer: LocalEventBuffer, events: ReturnType<typeof chunk>) {
  for (;;) {
    try {
      return buffer.appendMany(events);
    } catch (error) {
      if (!isSqliteContentionError(error)) throw error;
      busyRetries += 1;
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
  }
}

/** One OTLP-sized commit of Codex events: SSE deltas and a usage record, across a few sessions. */
function chunk() {
  return Array.from({ length: EVENTS_PER_COMMIT }, (_, index) => {
    eventNumber += 1;
    const usage = index === 0;
    return {
      event: aiInteractionEventSchema.parse({
        id: `00000000-0000-4000-8000-${String(eventNumber).padStart(12, "0")}`,
        sessionId: `019b0000-0000-7000-8000-00000000${String(eventNumber % 8).padStart(4, "0")}`,
        source: "codex",
        dataMode: "metadata",
        eventType: usage ? "assistant_response" : "otel_span",
        observedAt: new Date().toISOString(),
        actionClass: "other",
        ...(usage ? { model: "gpt-5.1-codex-max", inputTokens: 42_000, outputTokens: 800 } : {}),
        metadata: { otelEventName: "codex.sse_event", pad: "x".repeat(480) },
      }),
      suppressedFields: [],
    };
  });
}

async function main() {
  await check("wal_stays_bounded_under_continuous_intake_without_maintenance", async () => {
    const buffer = openLedger("continuous.sqlite");
    // Exactly the daemon's wiring (cli.ts): construct on its connection, start.
    const walCheckpoint = new walCheckpointModule.WalCheckpointWorker(buffer.database);
    walCheckpoint.start();
    let commits = 0;
    let walMax = 0;
    let longestCommitMs = 0;
    const commitMs: number[] = [];
    const started = performance.now();
    try {
      while (commits < INTAKE_COMMITS && performance.now() - started < INTAKE_CAP_MS) {
        const events = chunk();
        const at = performance.now();
        await commit(buffer, events);
        const took = performance.now() - at;
        commitMs.push(took);
        if (took > longestCommitMs) longestCommitMs = took;
        commits += 1;
        walMax = Math.max(walMax, walBytes(buffer));
        // Stop at once past the limit: an unbounded WAL would otherwise fill the disk.
        if (walMax > WAL_LIMIT_MIB * 1_048_576) break;
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
    } finally {
      await walCheckpoint.stop();
      buffer.close();
    }
    commitMs.sort((a, b) => a - b);
    measurements.continuous = {
      commits, events: commits * EVENTS_PER_COMMIT, seconds: Math.round((performance.now() - started) / 100) / 10,
      walMaxMiB: mib(walMax), longestCommitMs: Math.round(longestCommitMs),
      p99CommitMs: Math.round(commitMs[Math.floor(commitMs.length * 0.99)] ?? 0), busyRetries,
    };
    assert.ok(walMax <= WAL_LIMIT_MIB * 1_048_576,
      `WAL reached ${mib(walMax)} MiB after ${commits} commits (limit ${WAL_LIMIT_MIB} MiB): nothing bounds it without maintenance`);
    assert.equal(commits, INTAKE_COMMITS, `only ${commits} commits in ${INTAKE_CAP_MS / 1_000} s; too few to prove the bound`);
    return measurements.continuous;
  });

  await check("status_reports_the_worker_and_the_wal", async () => {
    const buffer = openLedger("status.sqlite");
    const walCheckpoint = new walCheckpointModule.WalCheckpointWorker(buffer.database, 50);
    try {
      assert.equal(typeof walCheckpoint.status, "function", "no status(): the worker is invisible to /status");
      assert.equal(walCheckpoint.start(), true);
      assert.equal(buffer.database.pragma("wal_autocheckpoint", { simple: true }),
        walCheckpointModule.WAL_AUTOCHECKPOINT_VALVE_FRAMES, "the daemon connection lost its WAL bound");
      await commit(buffer, chunk());
      const deadline = Date.now() + WAIT_MS;
      while (!walCheckpoint.status().lastSuccessAt && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
      const status = walCheckpoint.status();
      assert.equal(status.mode, "worker");
      assert.equal(status.autocheckpointFrames, walCheckpointModule.WAL_AUTOCHECKPOINT_VALVE_FRAMES);
      assert.ok(status.lastSuccessAt, "no successful pass reported");
      assert.ok(status.lastPass && typeof status.lastPass.walFrames === "number", "last pass not reported");
      assert.ok(typeof status.walBytes === "number" && status.walBytes > 0, "WAL size not reported");
      return { mode: status.mode, runs: status.runs, walBytes: status.walBytes, lastPass: status.lastPass };
    } finally {
      await walCheckpoint.stop();
      buffer.close();
    }
  });

  await check("the_hard_bound_holds_while_the_worker_is_silent", async () => {
    const buffer = openLedger("silent.sqlite");
    // Started as the daemon starts it, but its first pass is a minute away: a
    // stalled worker, before the watchdog replaces it.
    const walCheckpoint = new walCheckpointModule.WalCheckpointWorker(buffer.database, 60_000);
    const limit = 1.25 * walCheckpointModule.WAL_AUTOCHECKPOINT_VALVE_FRAMES * 4_120;
    let walMax = 0;
    try {
      assert.equal(walCheckpoint.start(), true);
      for (let commits = 0; commits < INTAKE_COMMITS / 2; commits += 1) {
        await commit(buffer, chunk());
        walMax = Math.max(walMax, walBytes(buffer));
        if (walMax > 2 * limit) break;
      }
      measurements.workerSilent = { walMaxMiB: mib(walMax), passes: walCheckpoint.status().runs };
      assert.equal(walCheckpoint.status().runs, 0, "the worker passed; the hard bound was not alone");
      assert.ok(walMax <= Math.min(limit, WAL_LIMIT_MIB * 1_048_576),
        `with the worker silent the WAL reached ${mib(walMax)} MiB: the daemon connection has no hard bound`);
      return measurements.workerSilent;
    } finally {
      await walCheckpoint.stop();
      buffer.close();
    }
  });

  await check("the_worker_alone_bounds_the_wal_under_continuous_intake", async () => {
    const buffer = openLedger("worker-alone.sqlite");
    const walCheckpoint = new walCheckpointModule.WalCheckpointWorker(buffer.database, 50);
    try {
      assert.equal(typeof walCheckpointModule.WAL_TARGET_FRAMES, "number",
        "no target: nothing but the hard bound's checkpoint, inside a commit, rewinds the WAL");
      assert.equal(walCheckpoint.start(), true);
      // Turn the hard bound off so that only the worker can bound the WAL: it
      // must, so the hard bound's checkpoint never has to run inside a commit.
      buffer.database.pragma("wal_autocheckpoint = 0");
      // The hard bound's size, with room for a runner ~10x slower than a Mac Studio.
      const limit = 1.5 * walCheckpointModule.WAL_AUTOCHECKPOINT_VALVE_FRAMES * 4_120;
      let walMax = 0;
      // Dense enough that commits land during the worker's passes, so PASSIVE
      // alone never rewinds the WAL (it passes 150 MiB), with the writer lock
      // free between commits, as intake leaves it, for FULL to take.
      for (let commits = 0; commits < INTAKE_COMMITS / 2; commits += 1) {
        await commit(buffer, chunk());
        walMax = Math.max(walMax, walBytes(buffer));
        if (walMax > 2 * limit) break;
        await new Promise((resolve) => setTimeout(resolve, 2));
      }
      const status = walCheckpoint.status();
      measurements.workerAlone = { walMaxMiB: mib(walMax), fullRuns: status.fullRuns, fullCompleted: status.fullCompleted };
      assert.ok(status.fullCompleted > 0, "the worker never completed a FULL pass");
      assert.ok(walMax <= limit,
        `with only the worker checkpointing the WAL reached ${mib(walMax)} MiB (limit ${mib(limit)} MiB)`);
      return measurements.workerAlone;
    } finally {
      await walCheckpoint.stop();
      buffer.close();
    }
  });

  await check("a_reader_holding_the_wal_raises_the_bound_until_the_worker_copies_the_backlog", async () => {
    const buffer = openLedger("held.sqlite");
    const walCheckpoint = new walCheckpointModule.WalCheckpointWorker(buffer.database, 50);
    const reader = new Database(buffer.database.name, { readonly: true, fileMustExist: true });
    const valve = walCheckpointModule.WAL_AUTOCHECKPOINT_VALVE_FRAMES;
    const waitFor = async (condition: () => boolean) => {
      const deadline = Date.now() + WAIT_MS;
      while (!condition() && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10));
      return condition();
    };
    try {
      assert.equal(walCheckpoint.start(), true);
      await commit(buffer, chunk());
      assert.ok(await waitFor(() => walCheckpoint.status().lastSuccessAt !== null), "worker never answered");
      // A long read, as the session-sync catch-up does: no checkpoint can copy past its snapshot.
      reader.exec("begin");
      reader.prepare("select count(*) from buffered_events").get();
      // Hold more than the bound, so the daemon's own checkpoint would otherwise take the backlog.
      while (walBytes(buffer) < (valve + walCheckpointModule.WAL_HELD_FRAMES) * 4_120) {
        await commit(buffer, chunk());
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
      assert.ok(await waitFor(() => (walCheckpoint.status().lastPass?.walFrames ?? 0) > valve &&
        walCheckpoint.status().heldByReader), "a WAL held past the bound was not noticed");
      const held = walCheckpoint.status();
      assert.ok(held.autocheckpointFrames! > held.lastPass!.walFrames!,
        `the daemon's own checkpoint (${held.autocheckpointFrames} frames) would copy the held backlog (${held.lastPass!.walFrames} frames) on the event loop`);
      reader.exec("commit");
      assert.ok(await waitFor(() => !walCheckpoint.status().heldByReader), "the released WAL still counts as held");
      const released = walCheckpoint.status();
      assert.equal(released.lastPass!.checkpointedFrames, released.lastPass!.walFrames, "the worker did not copy the backlog");
      assert.equal(released.autocheckpointFrames, valve, "the bound was not restored");
      return { heldWalFrames: held.lastPass!.walFrames, raisedTo: held.autocheckpointFrames };
    } finally {
      if (reader.inTransaction) reader.exec("commit");
      reader.close();
      await walCheckpoint.stop();
      buffer.close();
    }
  });

  await check("a_stalled_or_lost_worker_is_replaced_then_falls_back", async () => {
    const buffer = openLedger("watchdog.sqlite");
    const walCheckpoint = new walCheckpointModule.WalCheckpointWorker(buffer.database, 25, 5_000);
    const warnings: string[] = [];
    const originalWarn = console.warn;
    console.warn = (line: unknown) => { warnings.push(String(line)); };
    type Internals = { worker: { terminate(): Promise<number>; postMessage(value: unknown): void } | null };
    const internals = walCheckpoint as unknown as Internals;
    const waitFor = async (condition: () => boolean) => {
      const deadline = Date.now() + WAIT_MS;
      while (!condition() && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10));
      return condition();
    };
    try {
      assert.equal(walCheckpoint.start(), true);
      assert.ok(await waitFor(() => walCheckpoint.status().runs > 0), "worker never answered");
      // A pass that never answers: the worker never hears of it, so the
      // watchdog must replace the worker.
      assert.ok(await waitFor(() => internals.worker !== null && walCheckpoint.status().inFlightSince === null));
      internals.worker!.postMessage = () => undefined;
      assert.ok(await waitFor(() => walCheckpoint.status().restarts === 1), "stalled worker not replaced");
      const runsAfterRestart = walCheckpoint.status().runs;
      assert.ok(await waitFor(() => walCheckpoint.status().runs > runsAfterRestart), "replacement worker never answered");
      // Worker threads that keep dying: after three replacements, SQLite's own checkpoint returns.
      for (let loss = 0; loss < 3; loss += 1) {
        assert.ok(await waitFor(() => internals.worker !== null), "no worker to lose");
        await internals.worker!.terminate();
        assert.ok(await waitFor(() => walCheckpoint.status().restarts === loss + 2 ||
          walCheckpoint.status().mode === "sqlite_autocheckpoint"), `loss ${loss + 1} not handled`);
      }
      const status = walCheckpoint.status();
      assert.equal(status.mode, "sqlite_autocheckpoint");
      assert.equal(buffer.database.pragma("wal_autocheckpoint", { simple: true }),
        walCheckpointModule.SQLITE_DEFAULT_AUTOCHECKPOINT_FRAMES);
      assert.ok(warnings.some((line) => line.includes("wal_checkpoint_worker_restarted")), "restart not reported");
      assert.ok(warnings.some((line) => line.includes("wal_checkpoint_worker_unavailable")), "fallback not reported");
      return { restarts: status.restarts, fallbackReason: status.fallbackReason };
    } finally {
      console.warn = originalWarn;
      await walCheckpoint.stop();
      buffer.close();
    }
  });

  await check("failing_passes_are_counted_and_reported_once", async () => {
    const buffer = openLedger("errors.sqlite");
    const walCheckpoint = new walCheckpointModule.WalCheckpointWorker(buffer.database, 60_000);
    const warnings: string[] = [];
    const originalWarn = console.warn;
    console.warn = (line: unknown) => { warnings.push(String(line)); };
    try {
      // The reply a worker posts when its checkpoint pragma throws.
      const record = (walCheckpoint as unknown as { record(reply: unknown): void }).record.bind(walCheckpoint);
      for (let index = 0; index < 5; index += 1) record({ ok: false, code: "SQLITE_IOERR", ms: 1, walBytes: 4096 });
      const status = walCheckpoint.status();
      assert.equal(status.errors, 5);
      assert.equal(status.consecutiveErrors, 5);
      assert.equal(status.lastErrorCode, "SQLITE_IOERR");
      assert.equal(warnings.filter((line) => line.includes("wal_checkpoint_worker_failing")).length, 1);
      record({ ok: true, counts: { busy: 0, log: 10, checkpointed: 10 }, ms: 1, walBytes: 4096 });
      assert.equal(walCheckpoint.status().consecutiveErrors, 0);
      return { errors: status.errors };
    } finally {
      console.warn = originalWarn;
      await walCheckpoint.stop();
      buffer.close();
    }
  });

  fs.rmSync(dir, { recursive: true, force: true });
  const failed = checks.filter((entry) => !entry.passed);
  console.log(JSON.stringify({
    proof: "wal-checkpoint-bound",
    bead: "eco-6hoxj.163.24",
    passed: failed.length === 0,
    checks: checks.length,
    failures: failed.map((entry) => ({ name: entry.name, detail: entry.detail })),
    measurements,
  }, null, 2));
  if (failed.length > 0) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
