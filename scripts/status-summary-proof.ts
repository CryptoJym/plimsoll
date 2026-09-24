import { createProofCompletion } from "./lib/proof-completion";
const completion = createProofCompletion("status-summary", 9);
/**
 * eco-6hoxj.163.34: the daemon keeps a private status-summary.json that local
 * readers (the macOS menubar) read instead of running `plimsoll status`, and
 * GET /healthz names the same run with a random instanceId.
 *
 * Proves: the file is private, exactly shaped, atomic for a concurrent reader,
 * written from the /status cache without a single SQL statement, free of
 * credentials and paths; /healthz and the file name the same run; each run
 * has its own id; /status and the management routes are unchanged; and a
 * real `plimsoll start` daemon writes it. Isolated proof root and loopback
 * only; never the live collector.
 */
import { spawn } from "node:child_process";
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
  STATUS_SUMMARY_SCHEMA,
  startStatusSummaryWriter,
  type StatusSummary,
} from "../packages/collector-cli/src/status-summary";
import { PLIMSOLL_VERSION } from "../packages/collector-cli/src/version";

type Result = { status: number; body: Record<string, unknown> };

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cliSource = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const SUMMARY_KEYS = "collectorVersion,instanceId,port,schema,stats,updatedAt";
const STATS_KEYS = "count,tokenAttributedEvents,totalInputTokens,totalOutputTokens";
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
        let parsed: Record<string, unknown> = {};
        try {
          parsed = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
        } catch {
          parsed = {};
        }
        resolve({ status: response.statusCode ?? 0, body: parsed });
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

/** Counts every statement the buffer's SQLite connection prepares or runs while `action` runs. */
function countStatements(buffer: LocalEventBuffer, action: () => void) {
  const database = buffer.database as unknown as Record<string, unknown>;
  const statementPrototype = Object.getPrototypeOf(buffer.database.prepare("select 1")) as Record<string, unknown>;
  let statements = 0;
  const wrap = (target: Record<string, unknown>, method: string) => {
    const original = target[method] as (...args: unknown[]) => unknown;
    target[method] = function (this: unknown, ...args: unknown[]) {
      statements += 1;
      return original.apply(this, args);
    };
    return () => { target[method] = original; };
  };
  const restore = [
    wrap(database, "prepare"), wrap(database, "exec"), wrap(database, "pragma"),
    wrap(statementPrototype, "run"), wrap(statementPrototype, "get"), wrap(statementPrototype, "all"),
    wrap(statementPrototype, "iterate"),
  ];
  try {
    action();
  } finally {
    for (const undo of restore) undo();
  }
  return statements;
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
  let timer: NodeJS.Timeout | undefined;
  try {
    for (const listener of [server, second]) {
      await new Promise<void>((resolve, reject) => {
        listener.once("error", reject);
        listener.listen(0, "127.0.0.1", resolve);
      });
    }
    const port = (server.address() as AddressInfo).port;

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

    let writes = 0;
    timer = startStatusSummaryWriter({
      home,
      instanceId: server.plimsollInstanceId,
      collectorVersion: PLIMSOLL_VERSION,
      port,
      stats: () => {
        writes += 1;
        return server.plimsollCachedStats();
      },
      intervalMs: 5,
    });
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

    const opened = await request(port, "/status", "GET", "", { "x-plimsoll-token": auth.managementRead });
    const cached = opened.body.stats as Record<string, unknown> | null;
    check(
      "summary_counts_are_the_status_cache_counts",
      refreshed && cached !== null && typeof cached === "object" && Number(cached.count) >= 3 &&
        STATS_KEYS.split(",").every((key) => stats?.[key] === cached[key]),
      { refreshed, summary: stats ?? null, status: cached ? Object.fromEntries(STATS_KEYS.split(",").map((key) => [key, cached[key]])) : null },
    );

    // One more write with every SQLite entry point counted: the summary comes
    // from memory, so a 69 GB ledger costs the same as an empty one.
    const statements = countStatements(buffer, () => {
      clearInterval(startStatusSummaryWriter({
        home, instanceId: server.plimsollInstanceId, collectorVersion: PLIMSOLL_VERSION, port,
        stats: server.plimsollCachedStats, intervalMs: 60_000,
      }));
    });
    check("summary_write_reads_no_ledger", statements === 0, { statements });

    const writesBefore = writes;
    const reader = await concurrentReader(file, 600);
    const writesDuring = writes - writesBefore;
    clearInterval(timer);
    timer = undefined;
    const leftovers = fs.readdirSync(home).filter((name) => name.endsWith(".tmp"));
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
    check(
      "status_and_management_routes_unchanged",
      closedStatus.status === 401 && closedStatus.body.reason === "management_credential_required" &&
        closedApi.status === 401 && closedApi.body.reason === "management_credential_required" &&
        opened.status === 200 && opened.body.ok === true && !("instanceId" in opened.body),
      { closedStatus: closedStatus.status, closedApi: closedApi.status, opened: opened.status },
    );

    const secondPort = (second.address() as AddressInfo).port;
    const secondHealth = await request(secondPort, "/healthz");
    check(
      "each_server_run_has_its_own_instance_id",
      UUID_V4.test(second.plimsollInstanceId) && second.plimsollInstanceId !== server.plimsollInstanceId &&
        secondHealth.body.instanceId === second.plimsollInstanceId,
      { distinct: second.plimsollInstanceId !== server.plimsollInstanceId },
    );
  } finally {
    if (timer) clearInterval(timer);
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
  child.stdout?.on("data", (chunk) => { stdout += chunk; });
  child.stderr?.on("data", () => {});
  try {
    const active = await waitFor(() => stdout.includes('"active"'), 60_000);
    const summary = active ? readSummary(home) : null;
    const health = active ? await request(port, "/healthz") : null;
    check(
      "daemon_start_writes_the_summary_for_its_run",
      active && summary !== null && summary.port === port && summary.collectorVersion === PLIMSOLL_VERSION &&
        UUID_V4.test(summary.instanceId) && health?.body.instanceId === summary.instanceId &&
        (fs.lstatSync(path.join(home, STATUS_SUMMARY_FILE)).mode & 0o777) === 0o600,
      { active, port, summaryPort: summary?.port ?? null, sameRun: health?.body.instanceId === summary?.instanceId },
    );
  } finally {
    child.kill("SIGTERM");
    const exited = await waitFor(() => child.exitCode !== null || child.signalCode !== null, 10_000);
    if (!exited) child.kill("SIGKILL");
  }
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
