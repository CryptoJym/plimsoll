import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("status-summary", 20);
/**
 * eco-6hoxj.163.34: the daemon keeps a private status-summary.json that local
 * readers (the macOS menubar) read instead of running `plimsoll status`, and
 * GET /healthz names the same run with a random instanceId. A reader of the
 * file proves the listener is that run with GET /healthz?challenge=: the
 * answer is an HMAC of the challenge under the file's per-run healthzKey.
 *
 * Proves: the file is private, exactly shaped, atomic for a concurrent reader,
 * written from the /status cache without a single SQL statement, free of
 * credentials and paths; /healthz and the file name the same run; each run
 * has its own id; /status and the management routes are unchanged; and a
 * real `plimsoll start` daemon writes it and leaves no temp file on shutdown.
 * The writer never blocks the event loop (a 250 ms fsync delays neither a
 * timer nor /healthz), costs one small write per 15 s interval, stays 0600
 * under any umask, refuses a home that was swapped after it started, and
 * leaves no temp file when a write fails or the writer stops mid-write.
 * The challenge proof verifies with the file's key and matches the shared
 * test vector the menubar tests pin; a responder that knows the instanceId
 * but not the key (the round-3 reply, a wrong key, a replayed proof, a proof
 * relayed from another port) is refused; the key is in no HTTP response and
 * no daemon output.
 * Isolated proof root and loopback only; never the live collector.
 */
import { spawn } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import type { AddressInfo } from "node:net";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth, type LocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import {
  STATUS_SUMMARY_FILE,
  STATUS_SUMMARY_INTERVAL_MS,
  STATUS_SUMMARY_SCHEMA,
  anchorStatusSummaryHome,
  healthzProof,
  startStatusSummaryWriter,
  writeStatusSummary,
  type StatusSummary,
  type StatusSummaryWriterOptions,
} from "../packages/collector-cli/src/status-summary";
import { PLIMSOLL_VERSION } from "../packages/collector-cli/src/version";

type Result = { status: number; body: Record<string, unknown>; text: string };
type Method = (...args: unknown[]) => unknown;

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cliSource = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const SUMMARY_KEYS = "collectorVersion,healthzKey,instanceId,port,schema,stats,updatedAt";
const BASE64URL_32_BYTES = /^[A-Za-z0-9_-]{43}$/;
/** Shared with the menubar tests (healthzProofMatchesTheCollectorsTestVector). */
const TEST_VECTOR = {
  key: "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
  port: 49123,
  instanceId: "0f5b9a52-3c1e-4a8b-9d2e-6f7a8b9c0d1e",
  challenge: "ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8",
  proof: "lnozo0nCndram-7O1AV5MwmsEMnovjQYlD58Bw5wX0U",
};
const STATS_KEYS = "count,tokenAttributedEvents,totalInputTokens,totalOutputTokens";
const INJECTED_FSYNC_MS = 250;
const checks: Array<{ name: string; passed: boolean; detail: unknown }> = [];

function check(name: string, passed: boolean, detail: unknown) {
  checks.push({ name, passed, detail });
  completion.check(name, passed);
}

function privateDir(parent: string, name: string) {
  const directory = path.join(parent, name);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  fs.chmodSync(directory, 0o700);
  return directory;
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function request(port: number, route: string, method: "GET" | "POST" = "GET", body = "",
  headers: Record<string, string> = {}): Promise<Result> {
  return new Promise((resolve, reject) => {
    const payload = Buffer.from(body);
    const client = http.request({
      host: "127.0.0.1", port, path: route, method,
      headers: {
        connection: "close",
        ...(method === "POST" ? { "content-type": "application/json", "content-length": String(payload.length) } : {}),
        ...headers,
      },
    }, (response) => {
      const chunks: Buffer[] = [];
      response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
      response.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        let parsed: Record<string, unknown> = {};
        try {
          parsed = JSON.parse(text) as Record<string, unknown>;
        } catch {
          parsed = {};
        }
        resolve({ status: response.statusCode ?? 0, body: parsed, text });
      });
    });
    client.setTimeout(5_000, () => client.destroy(new Error("proof_request_timeout")));
    client.on("error", reject);
    client.end(payload);
  });
}

function readSummary(home: string): StatusSummary | null {
  try {
    return JSON.parse(fs.readFileSync(path.join(home, STATUS_SUMMARY_FILE), "utf8")) as StatusSummary;
  } catch {
    return null;
  }
}

function tempFiles(home: string) {
  return fs.readdirSync(home).filter((name) => name.startsWith(`${STATUS_SUMMARY_FILE}.`));
}

function waitFor(condition: () => boolean, timeoutMs: number, stepMs = 50): Promise<boolean> {
  return new Promise((resolve) => {
    const deadline = Date.now() + timeoutMs;
    const tick = () => {
      if (condition()) return resolve(true);
      if (Date.now() >= deadline) return resolve(false);
      setTimeout(tick, stepMs);
    };
    tick();
  });
}

function reserveLoopbackPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = http.createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const port = (probe.address() as AddressInfo).port;
      probe.close(() => resolve(port));
    });
  });
}

function credentialValues(auth: LocalIngestAuth) {
  return [auth.claudeCodeProducer, auth.codexProducer, auth.geminiCliProducer, auth.grokProducer, auth.managementRead]
    .filter((value): value is string => typeof value === "string");
}

function freshChallenge() {
  return crypto.randomBytes(32).toString("base64url");
}

/**
 * What the menubar accepts (packages/mac-menubar LivenessProbe): HTTP 200 and
 * exactly {ok: true, instanceId, proof}, the instanceId the file names and a
 * proof that is the HMAC of this challenge under the file's key for the
 * file's port, compared in constant time.
 */
function isCollectorProof(result: Result, summary: StatusSummary, challenge: string) {
  const body = result.body;
  if (result.status !== 200 || Object.keys(body).sort().join(",") !== "instanceId,ok,proof" ||
    body.ok !== true || body.instanceId !== summary.instanceId || typeof body.proof !== "string" ||
    !BASE64URL_32_BYTES.test(body.proof)) return false;
  const expected = Buffer.from(
    healthzProof(Buffer.from(summary.healthzKey, "base64url"), summary.port, summary.instanceId, challenge),
    "base64url",
  );
  const actual = Buffer.from(body.proof, "base64url");
  return actual.length === expected.length && crypto.timingSafeEqual(actual, expected);
}

/** A loopback service that is not the collector: it answers every request with `answer(path)`. */
async function impostor(answer: (route: string) => Promise<string> | string) {
  const server = http.createServer((incoming, response) => {
    void Promise.resolve(answer(incoming.url ?? "/")).then((body) => {
      response.writeHead(200, { "content-type": "application/json", connection: "close" });
      response.end(body);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    port: (server.address() as AddressInfo).port,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

/** Replaces `target[name]` for the length of `action`, restoring it after. */
async function patched<T>(
  patches: Array<[Record<string, unknown>, string, (original: Method) => Method]>,
  action: () => Promise<T>,
): Promise<T> {
  const originals = patches.map(([target, name]) => [target, name, target[name]] as const);
  for (const [target, name, wrap] of patches) target[name] = wrap(target[name] as Method);
  try {
    return await action();
  } finally {
    for (const [target, name, original] of originals) target[name] = original;
  }
}

/** Counts every statement the buffer's SQLite connection prepares or runs while `action` runs. */
async function countStatements(buffer: LocalEventBuffer, action: () => Promise<void>) {
  const database = buffer.database as unknown as Record<string, unknown>;
  const statementPrototype = Object.getPrototypeOf(buffer.database.prepare("select 1")) as Record<string, unknown>;
  let statements = 0;
  const counted = (original: Method): Method => function (this: unknown, ...args: unknown[]) {
    statements += 1;
    return original.apply(this, args);
  };
  await patched([
    [database, "prepare", counted], [database, "exec", counted], [database, "pragma", counted],
    [statementPrototype, "run", counted], [statementPrototype, "get", counted], [statementPrototype, "all", counted],
    [statementPrototype, "iterate", counted],
  ], action);
  return statements;
}

const SYNC_FILE_CALLS = Object.keys(fs)
  .filter((name) => name.endsWith("Sync") && typeof (fs as unknown as Record<string, unknown>)[name] === "function");
const ASYNC_FILE_CALLS = ["lstat", "stat", "open", "rename", "rm", "unlink", "readFile", "writeFile", "readdir"];
// FileHandle#close is an instance field, so it is counted on each opened handle.
const HANDLE_CALLS = ["chmod", "writeFile", "write", "sync", "datasync", "read", "readFile", "stat"];

/**
 * Every node:fs call `action` makes: synchronous ones (which would block the
 * event loop), fs.promises ones and FileHandle ones, with the bytes written
 * and the exclusive no-follow 0600 creates.
 */
async function countFileWork(fileHandle: Record<string, unknown>, action: () => Promise<void>) {
  const sync: Record<string, number> = {};
  const asynchronous: Record<string, number> = {};
  let bytesWritten = 0;
  let privateExclusiveCreates = 0;
  const exclusive = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW;
  const tally = (bucket: Record<string, number>, name: string, inspect?: (args: unknown[]) => void) =>
    (original: Method): Method => function (this: unknown, ...args: unknown[]) {
      bucket[name] = (bucket[name] ?? 0) + 1;
      inspect?.(args);
      return original.apply(this, args);
    };
  const promises = fs.promises as unknown as Record<string, unknown>;
  await patched([
    ...SYNC_FILE_CALLS.map((name) => [fs as unknown as Record<string, unknown>, name, tally(sync, name)] as [Record<string, unknown>, string, (original: Method) => Method]),
    ...ASYNC_FILE_CALLS.map((name) => [promises, name, (original: Method) => {
      const counted = tally(asynchronous, name, (args) => {
        if (name === "open" && args[1] === exclusive && args[2] === 0o600) privateExclusiveCreates += 1;
      })(original);
      if (name !== "open") return counted;
      return function (this: unknown, ...args: unknown[]) {
        return (counted.apply(this, args) as Promise<Record<string, unknown>>).then((handle) => {
          const close = handle.close as Method;
          handle.close = (...closeArgs: unknown[]) => {
            asynchronous.close = (asynchronous.close ?? 0) + 1;
            return close.apply(handle, closeArgs);
          };
          return handle;
        });
      };
    }] as [Record<string, unknown>, string, (original: Method) => Method]),
    ...HANDLE_CALLS.map((name) => [fileHandle, name, tally(asynchronous, name, (args) => {
      if (name === "writeFile" || name === "write") bytesWritten += Buffer.byteLength(String(args[0]));
    })] as [Record<string, unknown>, string, (original: Method) => Method]),
  ], action);
  const sorted = (bucket: Record<string, number>) =>
    Object.fromEntries(Object.entries(bucket).sort(([left], [right]) => left.localeCompare(right)));
  return { sync: sorted(sync), asynchronous: sorted(asynchronous), bytesWritten, privateExclusiveCreates };
}

/** Warnings the writer prints while `action` runs. */
async function captureWarnings(action: () => Promise<void>) {
  const lines: string[] = [];
  await patched([[console as unknown as Record<string, unknown>, "warn", () => (line: unknown) => {
    lines.push(String(line));
  }]], action);
  return lines;
}

/** A reader on another thread parses the file in a loop; returns reads and failures. */
function concurrentReader(file: string, durationMs: number): Promise<{ reads: number; failures: number }> {
  const source = `
    const { parentPort, workerData } = require("node:worker_threads");
    const fs = require("node:fs");
    let reads = 0, failures = 0;
    const until = Date.now() + workerData.durationMs;
    while (Date.now() < until) {
      try { JSON.parse(fs.readFileSync(workerData.file, "utf8")); reads += 1; }
      catch { failures += 1; } // rename(2) replaces atomically: even ENOENT is a failure
    }
    parentPort.postMessage({ reads, failures });
  `;
  return new Promise((resolve, reject) => {
    const worker = new Worker(source, { eval: true, workerData: { file, durationMs } });
    worker.once("message", resolve);
    worker.once("error", reject);
  });
}

async function inProcessChecks(root: string) {
  const home = privateDir(root, "in-process-home");
  const auth = loadOrCreateLocalIngestAuth(home);
  const buffer = new LocalEventBuffer(path.join(home, "work-ledger.sqlite"));
  let refreshStatus: (() => boolean) | null = null;
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, {
    localAuth: auth,
    perSourceRequestLimit: 10_000,
    registerStatusRefresher: (refresh) => { refreshStatus = () => refresh(); },
  });
  const second = createCollectorServer(collectorConfigSchema.parse({}), buffer, { localAuth: auth });
  // FileHandle methods live on one prototype shared by every open file.
  const probeHandle = await fs.promises.open(path.join(root, "file-handle-probe"), "w");
  const fileHandle = Object.getPrototypeOf(probeHandle) as Record<string, unknown>;
  await probeHandle.close();
  const writers: Array<{ stop(): Promise<void> }> = [];
  try {
    for (const listener of [server, second]) {
      await new Promise<void>((resolve, reject) => {
        listener.once("error", reject);
        listener.listen(0, "127.0.0.1", resolve);
      });
    }
    const port = (server.address() as AddressInfo).port;
    const options = (overrides: Partial<StatusSummaryWriterOptions> = {}): StatusSummaryWriterOptions => ({
      home, instanceId: server.plimsollInstanceId, healthzKey: server.plimsollHealthzKey,
      collectorVersion: PLIMSOLL_VERSION, port,
      stats: server.plimsollCachedStats, intervalMs: 60_000, ...overrides,
    });
    const writeOnce = async (overrides: Partial<StatusSummaryWriterOptions> = {}) => {
      const writer = startStatusSummaryWriter(options(overrides));
      await writer.firstWrite;
      await writer.stop();
    };

    // Real events through the collector's own authenticated intake, then the
    // projection work the maintenance child does, then a status refresh.
    for (let index = 0; index < 3; index += 1) {
      await request(port, "/hooks/claude-code", "POST", JSON.stringify({
        id: `status-summary-proof-${index}`,
        hook_event_name: "Stop",
        timestamp: new Date().toISOString(),
      }), { "x-plimsoll-token": auth.claudeCodeProducer });
    }
    for (let round = 0; round < 200; round += 1) {
      buffer.projection.runMaintenance();
      const status = buffer.projection.status();
      const backlog = status.backlog.repairs + status.backlog.compactMutations;
      if (backlog === 0 && status.backfill.complete && status.backfill.parityComplete) break;
    }
    const refreshed = (refreshStatus as (() => boolean) | null)?.() ?? false;

    await writeOnce();
    const file = path.join(home, STATUS_SUMMARY_FILE);
    const summary = readSummary(home);
    const stat = fs.lstatSync(file);
    const stats = summary?.stats as Record<string, unknown> | null | undefined;
    check(
      "summary_is_private_and_exactly_shaped",
      stat.isFile() && !stat.isSymbolicLink() && (stat.mode & 0o777) === 0o600 &&
        summary !== null && Object.keys(summary).sort().join(",") === SUMMARY_KEYS &&
        summary.schema === STATUS_SUMMARY_SCHEMA && summary.collectorVersion === PLIMSOLL_VERSION &&
        summary.port === port && UUID_V4.test(summary.instanceId) &&
        BASE64URL_32_BYTES.test(summary.healthzKey) && Buffer.from(summary.healthzKey, "base64url").length === 32 &&
        Math.abs(Date.now() - Date.parse(summary.updatedAt)) < 10_000 &&
        stats !== null && stats !== undefined && Object.keys(stats).sort().join(",") === STATS_KEYS,
      { mode: (stat.mode & 0o777).toString(8), keys: summary ? Object.keys(summary).sort() : null },
    );

    const health = await request(port, "/healthz");
    check(
      "healthz_names_the_same_run_as_the_summary",
      health.status === 200 && Object.keys(health.body).sort().join(",") === "instanceId,ok" &&
        health.body.ok === true && health.body.instanceId === summary?.instanceId,
      { status: health.status, keys: Object.keys(health.body).sort() },
    );

    // Review blocker: any process could read the public instanceId and replay
    // it once it held the port. With a fresh challenge the collector answers
    // an HMAC under the key only it and a reader of the 0600 file hold.
    const responses: Result[] = [health];
    const challenges = [freshChallenge(), freshChallenge()];
    const challenged = [];
    for (const challenge of challenges) {
      const answered = await request(port, `/healthz?challenge=${challenge}`);
      responses.push(answered);
      challenged.push(answered);
    }
    const malformed = [
      "/healthz?challenge=short",
      `/healthz?challenge=${challenges[0]!.slice(0, 42)}+`,
      `/healthz?challenge=${challenges[0]}&challenge=${challenges[1]}`,
      `/healthz?challenge=${challenges[0]}&note=1`,
      "/healthz?note=1",
    ];
    const refusedMalformed = [];
    for (const route of malformed) {
      const answered = await request(port, route);
      responses.push(answered);
      refusedMalformed.push(answered.status === 400 && answered.body.reason === "invalid_challenge" && !("proof" in answered.body));
    }
    check(
      "healthz_proves_the_run_with_an_hmac_of_a_fresh_challenge",
      summary !== null && challenged.every((answered, index) => isCollectorProof(answered, summary, challenges[index]!)) &&
        challenged[0]!.body.proof !== challenged[1]!.body.proof &&
        !isCollectorProof(challenged[0]!, summary, challenges[1]!) && refusedMalformed.every(Boolean),
      {
        keys: Object.keys(challenged[0]!.body).sort(),
        verified: summary ? challenged.map((answered, index) => isCollectorProof(answered, summary, challenges[index]!)) : null,
        refusedMalformed,
      },
    );

    const vector = healthzProof(Buffer.from(TEST_VECTOR.key, "base64url"), TEST_VECTOR.port, TEST_VECTOR.instanceId,
      TEST_VECTOR.challenge);
    check("healthz_proof_matches_the_shared_test_vector", vector === TEST_VECTOR.proof, { proof: vector });

    // The review's attack and its variants, each from a loopback service that
    // took the summary's port: it knows the public instanceId, not the key.
    const captured = challenged[0]!.body.proof as string;
    const impostors = {
      roundThreeReply: () => JSON.stringify({ ok: true, instanceId: server.plimsollInstanceId }),
      wrongKey: (route: string) => JSON.stringify({
        ok: true, instanceId: server.plimsollInstanceId,
        proof: healthzProof(crypto.randomBytes(32), 0, server.plimsollInstanceId, route.split("challenge=")[1] ?? ""),
      }),
      replayedProof: () => JSON.stringify({ ok: true, instanceId: server.plimsollInstanceId, proof: captured }),
      relayedFromTheCollectorsPort: async (route: string) => (await request(port, route)).text,
    };
    const refused: Record<string, boolean> = {};
    for (const [name, answer] of Object.entries(impostors)) {
      const fake = await impostor(answer);
      try {
        const challenge = freshChallenge();
        const answered = await request(fake.port, `/healthz?challenge=${challenge}`);
        refused[name] = summary !== null && !isCollectorProof(answered, { ...summary, port: fake.port }, challenge);
      } finally {
        await fake.close();
      }
    }
    const genuineChallenge = freshChallenge();
    const genuine = summary !== null && isCollectorProof(await request(port, `/healthz?challenge=${genuineChallenge}`), summary, genuineChallenge);
    check(
      "a_responder_without_the_key_is_refused",
      genuine && Object.keys(refused).length === 4 && Object.values(refused).every(Boolean),
      { genuine, refused },
    );

    const opened = await request(port, "/status", "GET", "", { "x-plimsoll-token": auth.managementRead });
    responses.push(opened);
    const cached = opened.body.stats as Record<string, unknown> | null;
    check(
      "summary_counts_are_the_status_cache_counts",
      refreshed && cached !== null && typeof cached === "object" && Number(cached.count) >= 3 &&
        STATS_KEYS.split(",").every((key) => stats?.[key] === cached[key]),
      { refreshed, summary: stats ?? null, status: cached ? Object.fromEntries(STATS_KEYS.split(",").map((key) => [key, cached[key]])) : null },
    );

    // A whole write, with every SQLite entry point counted: the summary comes
    // from memory, so a 69 GB ledger costs the same as an empty one.
    const statements = await countStatements(buffer, () => writeOnce());
    check("summary_write_reads_no_ledger", statements === 0, { statements });

    // Its whole idle cost with the daemon's own interval: per write, the
    // home's identity checked twice (plus once when the writer starts), one
    // exclusive no-follow 0600 create, chmod, write, fsync, close, rename;
    // no read and no synchronous call; an unref'd timer.
    const timersBefore = process.getActiveResourcesInfo().filter((type) => type === "Timeout").length;
    let idleTimerUnref = false;
    const idle = await countFileWork(fileHandle, async () => {
      const writer = startStatusSummaryWriter(options({ intervalMs: undefined }));
      await writer.firstWrite;
      idleTimerUnref = process.getActiveResourcesInfo().filter((type) => type === "Timeout").length === timersBefore;
      await writer.stop();
    });
    const expectedIdleCalls = { chmod: 1, close: 1, lstat: 3, open: 1, rename: 1, sync: 1, writeFile: 1 };
    check(
      "summary_idle_cost_is_one_small_write_per_interval",
      JSON.stringify(idle.asynchronous) === JSON.stringify(expectedIdleCalls) &&
        Object.keys(idle.sync).length === 0 && idle.privateExclusiveCreates === 1 &&
        idle.bytesWritten > 0 && idle.bytesWritten <= 512 && STATUS_SUMMARY_INTERVAL_MS === 15_000 && idleTimerUnref,
      {
        calls: idle.asynchronous, syncCalls: idle.sync, bytesWritten: idle.bytesWritten,
        intervalMs: STATUS_SUMMARY_INTERVAL_MS, writesPerHour: 3_600_000 / STATUS_SUMMARY_INTERVAL_MS, idleTimerUnref,
      },
    );

    // Review blocker: the writer's open/write/fsync/close/rename ran on the
    // event loop, and a 250 ms fsync held a zero-delay timer for 452.7 ms.
    // Here fsync takes 250 ms whichever API a writer uses (the synchronous
    // call blocks its thread, the FileHandle one resolves late), while the
    // writer ticks every 20 ms.
    const blocker = new Int32Array(new SharedArrayBuffer(4));
    const promises = fs.promises as unknown as Record<string, unknown>;
    let slowWriteMs = 0;
    let lateTimerMs = 0;
    let timerSamples = 0;
    let healthzMs = Number.POSITIVE_INFINITY;
    let opens = 0;
    let opensWhenFirstSettled = -1;
    const slow = await patched([
      [fileHandle, "sync", (original) => async function (this: unknown, ...args: unknown[]) {
        await sleep(INJECTED_FSYNC_MS);
        return original.apply(this, args);
      }],
      [fs as unknown as Record<string, unknown>, "fsyncSync", (original) => function (this: unknown, ...args: unknown[]) {
        Atomics.wait(blocker, 0, 0, INJECTED_FSYNC_MS);
        return original.apply(this, args);
      }],
      [promises, "open", (original) => function (this: unknown, ...args: unknown[]) {
        opens += 1;
        return original.apply(this, args);
      }],
    ], () => countFileWork(fileHandle, async () => {
      const started = performance.now();
      const writer = startStatusSummaryWriter(options({ intervalMs: 20 }));
      let settled = false;
      const first = writer.firstWrite.then(() => {
        slowWriteMs = performance.now() - started;
        opensWhenFirstSettled = opens;
        settled = true;
      });
      const healthzStarted = performance.now();
      const answered = request(port, "/healthz").then(() => {
        healthzMs = performance.now() - healthzStarted;
      });
      while (!settled) {
        const scheduled = performance.now();
        await sleep(0);
        lateTimerMs = Math.max(lateTimerMs, performance.now() - scheduled);
        timerSamples += 1;
      }
      await first;
      await answered;
      await writer.stop();
    }));
    check(
      "summary_write_never_blocks_the_event_loop",
      slowWriteMs >= INJECTED_FSYNC_MS && lateTimerMs < 100 && healthzMs < 100 && timerSamples > 10 &&
        Object.keys(slow.sync).length === 0 && opensWhenFirstSettled === 1,
      {
        injectedFsyncMs: INJECTED_FSYNC_MS, slowWriteMs: Math.round(slowWriteMs),
        zeroDelayTimerWorstMs: Number(lateTimerMs.toFixed(1)), timerSamples, healthzMs: Number(healthzMs.toFixed(1)),
        syncCalls: slow.sync, writesStartedDuringSlowWrite: opensWhenFirstSettled,
      },
    );

    // Shutdown stops the writer while a slow write is in flight: stop() waits
    // for it, so the new summary is in place and no temp file is left.
    const stopHome = privateDir(root, "stop-home");
    let stoppedMs = 0;
    await patched([[fileHandle, "sync", (original) => async function (this: unknown, ...args: unknown[]) {
      await sleep(INJECTED_FSYNC_MS);
      return original.apply(this, args);
    }]], async () => {
      const writer = startStatusSummaryWriter(options({ home: stopHome }));
      await sleep(20);
      const started = performance.now();
      await writer.stop();
      stoppedMs = performance.now() - started;
    });
    check(
      "summary_stop_waits_for_a_write_in_progress",
      readSummary(stopHome)?.instanceId === server.plimsollInstanceId && tempFiles(stopHome).length === 0 &&
        stoppedMs > 0,
      { stoppedMs: Math.round(stoppedMs), leftovers: tempFiles(stopHome).length },
    );

    // Review should-fix: a umask of 0777 left the summary mode 000, which the
    // menubar cannot read. The file is chmod'ed to 0600 after it is created.
    const umaskHome = privateDir(root, "umask-home");
    const umasks: Array<{ umask: string; mode: string }> = [];
    for (const umask of [0o000, 0o022, 0o077, 0o777]) {
      const previous = process.umask(umask);
      try {
        await writeOnce({ home: umaskHome });
      } finally {
        process.umask(previous);
      }
      umasks.push({ umask: umask.toString(8).padStart(3, "0"), mode: (fs.statSync(path.join(umaskHome, STATUS_SUMMARY_FILE)).mode & 0o777).toString(8) });
    }
    check("summary_file_is_0600_under_any_umask", umasks.every((entry) => entry.mode === "600"), { umasks });

    // Review should-fix: replacing the validated home with a symlink after the
    // writer started redirected its next write. The home is pinned by device
    // and inode and checked before each create and rename.
    const swapRoot = privateDir(root, "swap");
    const swaps: Array<Record<string, unknown>> = [];
    for (const replacement of ["symlink", "directory"] as const) {
      const swapped = privateDir(swapRoot, `home-${replacement}`);
      const moved = `${swapped}-moved`;
      const elsewhere = privateDir(swapRoot, `elsewhere-${replacement}`);
      let writesAfterSwap = 0;
      let swappedYet = false;
      const warnings = await captureWarnings(async () => {
        const writer = startStatusSummaryWriter(options({
          home: swapped, intervalMs: 20, stats: () => { writesAfterSwap += swappedYet ? 1 : 0; return null; },
        }));
        await writer.firstWrite;
        fs.renameSync(swapped, moved);
        if (replacement === "symlink") fs.symlinkSync(elsewhere, swapped);
        else privateDir(swapRoot, `home-${replacement}`);
        swappedYet = true;
        await waitFor(() => writesAfterSwap >= 3, 2_000, 10);
        await writer.stop();
      });
      const landed = replacement === "symlink" ? fs.readdirSync(elsewhere) : fs.readdirSync(swapped);
      swaps.push({
        replacement, writesAfterSwap, landed: landed.length, warnings,
        originalKept: readSummary(moved) !== null,
      });
    }
    check(
      "summary_writer_refuses_a_swapped_home",
      swaps.every((swap) => Number(swap.writesAfterSwap) >= 3 && swap.landed === 0 && swap.originalKept === true &&
        JSON.stringify(swap.warnings) === JSON.stringify([JSON.stringify({ warning: "status_summary_write_failed", code: "home_changed" })])),
      swaps,
    );

    // A write that fails (here the rename, onto a directory) removes its temp file.
    const failedHome = privateDir(root, "failed-write-home");
    fs.mkdirSync(path.join(failedHome, STATUS_SUMMARY_FILE));
    let failedCode: string | null = null;
    try {
      await writeStatusSummary(await anchorStatusSummaryHome(failedHome), {
        schema: STATUS_SUMMARY_SCHEMA, instanceId: server.plimsollInstanceId, healthzKey: server.plimsollHealthzKey,
        collectorVersion: PLIMSOLL_VERSION,
        port, updatedAt: new Date().toISOString(), stats: null,
      });
    } catch (error) {
      failedCode = String((error as NodeJS.ErrnoException).code ?? "unknown");
    }
    const failedLeftovers = fs.readdirSync(failedHome).filter((name) => name !== STATUS_SUMMARY_FILE);
    check(
      "summary_failed_write_leaves_no_temp_file",
      failedCode !== null && failedLeftovers.length === 0,
      { failedCode, leftovers: failedLeftovers.length },
    );

    let writes = 0;
    const racing = startStatusSummaryWriter(options({
      intervalMs: 5,
      stats: () => {
        writes += 1;
        return server.plimsollCachedStats();
      },
    }));
    writers.push(racing);
    await racing.firstWrite;
    const writesBefore = writes;
    const reader = await concurrentReader(file, 600);
    const writesDuring = writes - writesBefore;
    await racing.stop();
    const leftovers = tempFiles(home);
    check(
      "summary_rewrites_are_atomic_for_a_concurrent_reader",
      reader.reads > 100 && reader.failures === 0 && writesDuring > 20 && leftovers.length === 0,
      { ...reader, writesDuring, leftovers },
    );

    const text = fs.readFileSync(file, "utf8");
    const parsed = JSON.parse(text) as Record<string, unknown>;
    const strings = Object.values(parsed).filter((value): value is string => typeof value === "string");
    check(
      "summary_names_no_credential_or_path",
      credentialValues(auth).every((token) => !text.includes(token)) && !text.includes(home) &&
        !text.includes(root) &&
        strings.every((value) => value === STATUS_SUMMARY_SCHEMA || (!value.includes("/") && !value.includes("\\"))) &&
        credentialValues(auth).every((token) => !JSON.stringify(health.body).includes(token)),
      { stringFields: Object.keys(parsed).filter((key) => typeof parsed[key] === "string").sort() },
    );

    const closedStatus = await request(port, "/status");
    const closedApi = await request(port, "/api/settings");
    responses.push(closedStatus, closedApi);
    const key = summary?.healthzKey ?? "";
    check(
      "healthz_key_never_leaves_the_file",
      key.length === 43 && responses.length >= 11 && responses.every((answered) => !answered.text.includes(key)) &&
        !credentialValues(auth).includes(key) && key !== server.plimsollInstanceId,
      { responsesChecked: responses.length },
    );
    check(
      "status_and_management_routes_unchanged",
      closedStatus.status === 401 && closedStatus.body.reason === "management_credential_required" &&
        closedApi.status === 401 && closedApi.body.reason === "management_credential_required" &&
        opened.status === 200 && opened.body.ok === true && !("instanceId" in opened.body),
      { closedStatus: closedStatus.status, closedApi: closedApi.status, opened: opened.status },
    );

    const secondPort = (second.address() as AddressInfo).port;
    const secondHealth = await request(secondPort, "/healthz");
    const secondChallenge = freshChallenge();
    const secondProof = await request(secondPort, `/healthz?challenge=${secondChallenge}`);
    check(
      "each_server_run_has_its_own_instance_id_and_key",
      UUID_V4.test(second.plimsollInstanceId) && second.plimsollInstanceId !== server.plimsollInstanceId &&
        secondHealth.body.instanceId === second.plimsollInstanceId &&
        second.plimsollHealthzKey !== server.plimsollHealthzKey && summary !== null &&
        !isCollectorProof(secondProof, { ...summary, port: secondPort, instanceId: second.plimsollInstanceId }, secondChallenge),
      { distinctIds: second.plimsollInstanceId !== server.plimsollInstanceId, distinctKeys: second.plimsollHealthzKey !== server.plimsollHealthzKey },
    );
  } finally {
    for (const writer of writers) await writer.stop();
    for (const listener of [server, second]) {
      if (listener.listening) await new Promise<void>((resolve) => listener.close(() => resolve()));
    }
    buffer.close();
  }
}

async function daemonCheck(root: string) {
  const home = privateDir(root, "daemon-home");
  const port = await reserveLoopbackPort();
  fs.writeFileSync(path.join(home, "collector.config.json"), `${JSON.stringify({ port }, null, 2)}\n`, { mode: 0o600 });
  const child = spawn(process.execPath, ["--import", "tsx", cliSource, "start"], {
    cwd: repoRoot,
    env: { ...process.env, PLIMSOLL_HOME: home },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (chunk) => { stdout += chunk; });
  child.stderr?.on("data", (chunk) => { stderr += chunk; });
  let exited = false;
  try {
    const active = await waitFor(() => stdout.includes('"active"'), 60_000);
    // The first write is asynchronous: it lands just after the daemon listens.
    const written = active && await waitFor(() => readSummary(home) !== null, 10_000);
    const summary = written ? readSummary(home) : null;
    const health = active ? await request(port, "/healthz") : null;
    const challenge = freshChallenge();
    const proven = summary !== null && isCollectorProof(await request(port, `/healthz?challenge=${challenge}`), summary, challenge);
    check(
      "daemon_start_writes_the_summary_for_its_run",
      active && summary !== null && summary.port === port && summary.collectorVersion === PLIMSOLL_VERSION &&
        UUID_V4.test(summary.instanceId) && health?.body.instanceId === summary.instanceId && proven &&
        (fs.lstatSync(path.join(home, STATUS_SUMMARY_FILE)).mode & 0o777) === 0o600,
      { active, port, summaryPort: summary?.port ?? null, sameRun: health?.body.instanceId === summary?.instanceId, proven },
    );
  } finally {
    child.kill("SIGTERM");
    exited = await waitFor(() => child.exitCode !== null || child.signalCode !== null, 10_000);
    if (!exited) child.kill("SIGKILL");
  }
  const kept = readSummary(home);
  check(
    "daemon_shutdown_leaves_the_summary_and_no_temp_file",
    exited && child.exitCode === 0 && kept !== null && tempFiles(home).length === 0 &&
      (fs.lstatSync(path.join(home, STATUS_SUMMARY_FILE)).mode & 0o777) === 0o600 &&
      // Nothing the daemon printed carries its key.
      kept.healthzKey.length === 43 && !stdout.includes(kept.healthzKey) && !stderr.includes(kept.healthzKey),
    { exited, exitCode: child.exitCode, kept: kept !== null, leftovers: tempFiles(home).length },
  );
}

async function main() {
  const root = process.env.PLIMSOLL_PROOF_ROOT!;
  await inProcessChecks(root);
  await daemonCheck(root);
  for (const result of checks) {
    console.log(`${result.passed ? "PASS" : "FAIL"} ${result.name} ${JSON.stringify(result.detail)}`);
  }
  const failed = checks.filter((result) => !result.passed);
  console.log(JSON.stringify({ checks: checks.length, passed: checks.length - failed.length, failed: failed.length }));
  if (failed.length > 0) process.exitCode = 1;
  completion.complete();
}

main().catch((error) => {
  console.error(JSON.stringify({
    error: "status_summary_proof_failed",
    reason: error instanceof Error ? error.message : "unknown",
  }));
  process.exitCode = 1;
});
