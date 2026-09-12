/**
 * Bead eco-6hoxj.61: hook events survive a collector 503, a collector 408 and a
 * refused connection through the per-home spool, and land in `buffered_events`
 * attributed exactly like a live post — same columns, same suppression
 * receipts, and the same event time, because the drain replays the hook's own
 * stamp rather than its own clock — with everything the collector would strip
 * emptied before the file is written, so the spool never holds what the ledger
 * is not allowed to hold.
 *
 * The loss path this closes, measured on Studio0 on 2026-09-12: the collector
 * answers 503 `storage_busy_retry` when the ledger stays contended past the
 * 750 ms retry budget, and `forwardHookOverLoopback` threw
 * `hook_forward_http_rejected:503` with no retry and no spool — 701 rejection
 * summaries stood for roughly 1,200 posts, claude_code user prompts and tool
 * events among them, that never reached the ledger. The same happens for every
 * hook fired while the collector is stopped for a managed update window.
 *
 * Each case runs in its own disposable Plimsoll home; the write lock is held
 * from a second SQLite connection exactly as `pnpm proof:sqlite-lock` does, so
 * the 503 is the real admission decision and not a stub. Nothing here reads or
 * writes the operator's real home, real ledger, or live collector.
 *
 * Run: pnpm proof:hook-spool
 */
import { spawn } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import { createRequire } from "node:module";
import net from "node:net";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  collectorConfigSchema,
  saveCollectorConfig,
} from "../packages/collector-cli/src/config";
import {
  HOOK_SPOOL_COLLECTOR_TOO_OLD,
  HOOK_SPOOL_COLLECTOR_UNREACHABLE,
  HOOK_SPOOL_LIMITS,
  SPOOL_DERIVATION_INPUT_DISCLOSURE,
  SPOOL_DERIVATION_INPUT_KEYS,
  SPOOL_PROTECTED_IDENTITY_KEYS,
  blankForbiddenRawContent,
  hookSpoolDaemonEnabled,
  hookSpoolCountersPath,
  hookSpoolDirectory,
  hookSpoolDoctorSection,
  hookSpoolOperatorStatus,
  hookSpoolRejectedDirectory,
  listHookSpoolFiles,
  readHookSpoolCounters,
  writeHookSpoolFile,
} from "../packages/collector-cli/src/hook-spool";
import { forwardHookOverLoopback } from "../packages/collector-cli/src/local-hook-client";
import { extractRepoContextCwd } from "../packages/collector-cli/src/repo-context";
import {
  DEFAULT_POLICY,
  hashProtectedValue,
  isSensitiveMetadataSemanticKey,
  protectedMetadataFieldNames,
  sanitizeForPolicy,
} from "../packages/shared/src/index";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import {
  createCollectorServer,
  createHookSpoolDrain,
  type HookSpoolDrain,
} from "../packages/collector-cli/src/server";

type Check = { name: string; passed: boolean; detail: unknown };
const checks: Check[] = [];
function check(name: string, passed: boolean, detail: unknown) {
  checks.push({ name, passed: Boolean(passed), detail });
}

const require = createRequire(path.resolve("package.json"));
const Database = require("better-sqlite3");
const repoRoot = path.resolve(import.meta.dirname, "..");
const tsxLoader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
const cliEntry = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");

/** The recovery window the bead states: a spooled event is in the ledger inside 15 s. */
const RECOVERY_DEADLINE_MS = 15_000;

const roots: string[] = [];
const previousEnv = new Map<string, string | undefined>();
function setEnv(key: string, value: string | undefined) {
  if (!previousEnv.has(key)) previousEnv.set(key, process.env[key]);
  if (value === undefined) delete process.env[key];
  else process.env[key] = value;
}

/**
 * One disposable home per case. HOME is pinned inside it as well as
 * PLIMSOLL_HOME, so nothing in this proof can compose a path in the real home.
 */
function fixtureHome(label: string) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `plimsoll-hook-spool-${label}-`));
  fs.chmodSync(root, 0o700);
  roots.push(root);
  const home = path.join(root, ".plimsoll");
  fs.mkdirSync(home, { mode: 0o700 });
  fs.chmodSync(home, 0o700);
  setEnv("HOME", root);
  setEnv("USERPROFILE", root);
  setEnv("PLIMSOLL_HOME", home);
  return { root, home };
}

type Collector = {
  home: string;
  ledgerPath: string;
  port: number;
  buffer: LocalEventBuffer;
  auth: ReturnType<typeof loadOrCreateLocalIngestAuth>;
  drain: HookSpoolDrain;
  statusBody: () => Promise<Record<string, unknown>>;
  close: () => Promise<void>;
};

async function startCollector(home: string): Promise<Collector> {
  const ledgerPath = path.join(home, "work-ledger.sqlite");
  // Fail-fast busy handling is what the listener uses in production reasoning
  // ("the better-sqlite3 default is never appropriate on the listener event
  // loop"); at 0 the contended append raises SQLITE_BUSY immediately, the
  // 750 ms retry budget expires, and the boundary answers the real 503.
  const buffer = new LocalEventBuffer(ledgerPath, { databaseBusyTimeoutMs: 0 });
  const config = collectorConfigSchema.parse({});
  const auth = loadOrCreateLocalIngestAuth(home);
  let drain: HookSpoolDrain | undefined;
  const server = createCollectorServer(config, buffer, {
    localAuth: auth,
    localAuthHome: home,
    hookSpoolStatus: () => drain?.status() ?? null,
  });
  drain = createHookSpoolDrain(config, buffer, { home });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const port = (server.address() as AddressInfo).port;
  return {
    home,
    ledgerPath,
    port,
    buffer,
    auth,
    drain,
    statusBody: () => getJson(port, "/status", auth.managementRead),
    async close() {
      drain!.stop();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      buffer.close();
    },
  };
}

function getJson(port: number, route: string, managementToken: string) {
  return new Promise<Record<string, unknown>>((resolve, reject) => {
    const request = http.request(
      {
        host: "127.0.0.1",
        port,
        path: route,
        method: "GET",
        headers: { connection: "close", "x-plimsoll-token": managementToken },
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
        response.on("end", () => {
          try {
            resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>);
          } catch (error) {
            reject(error);
          }
        });
      },
    );
    request.on("error", reject);
    request.end();
  });
}

/** Hold the ledger's single WAL writer from a second connection. */
function holdWriteLock(ledgerPath: string) {
  const lock = new Database(ledgerPath, { timeout: 0 });
  lock.pragma("journal_mode = WAL");
  lock.exec("begin immediate");
  lock.prepare(
    `insert or ignore into buffered_events
       (id, source, event_type, data_mode, observed_at, payload_json, created_at)
     values ('hook-spool-proof-lock-holder', 'codex', 'assistant_response', 'metadata', ?, '{}', ?)`,
  ).run("2099-01-01T00:00:00.000Z", "2099-01-01T00:00:00.000Z");
  let released = false;
  return {
    release() {
      if (released) return;
      released = true;
      try {
        lock.exec("rollback");
      } catch {
        /* already unwound */
      }
      lock.close();
    },
  };
}

async function freePort() {
  const probe = net.createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  const port = (probe.address() as AddressInfo).port;
  await new Promise<void>((resolve) => probe.close(() => resolve()));
  return port;
}

async function waitFor(predicate: () => boolean, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) return false;
    await new Promise<void>((resolve) => setTimeout(resolve, 100));
  }
}

/**
 * Wait for a background drain to FINISH, not for its first side effect
 * (review r3, N1).
 *
 * The drain appends the event, unlinks the file and then yields
 * `setImmediate` before the next file; it writes its counters and
 * `lastDrainAt` once, after the whole loop. So a ledger row is observable
 * while `recovered` is still 0 — and a check that polled the row and then
 * asserted the counters lost that race roughly one run in five, blaming the
 * drain for a proof bug. Every check that asserts counters after a background
 * recovery waits here instead, on the counters themselves, inside the same
 * 15 s budget.
 */
async function waitForDrainedCounters(
  home: string,
  recovered: number,
  timeoutMs = RECOVERY_DEADLINE_MS,
) {
  const startedAt = Date.now();
  const settled = await waitFor(() => {
    const counters = readHookSpoolCounters(home);
    return counters.recovered >= recovered && typeof counters.lastDrainAt === "string";
  }, timeoutMs);
  return { settled, elapsedMs: Date.now() - startedAt, counters: readHookSpoolCounters(home) };
}

/**
 * Never spawnSync here: this process IS the collector, so a synchronous child
 * that posts to it would block the very event loop that has to answer.
 */
function runCli(
  home: string,
  root: string,
  args: string[],
  input?: string,
  /** Per-run environment overrides; `undefined` unsets the variable. */
  envOverrides: Record<string, string | undefined> = {},
) {
  return new Promise<{ status: number | null; stdout: string; stderr: string }>((resolve, reject) => {
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      HOME: root,
      USERPROFILE: root,
      PLIMSOLL_HOME: home,
    };
    for (const [key, value] of Object.entries(envOverrides)) {
      if (value === undefined) delete env[key];
      else env[key] = value;
    }
    const child = spawn(process.execPath, ["--import", tsxLoader, cliEntry, ...args], {
      env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => { stdout += chunk; });
    child.stderr.on("data", (chunk: string) => { stderr += chunk; });
    child.on("error", reject);
    child.on("close", (status) => resolve({ status, stdout, stderr }));
    child.stdin.end(input ?? "");
  });
}

function parseJsonStdout(stdout: string) {
  const start = stdout.indexOf("{");
  if (start < 0) return null;
  try {
    return JSON.parse(stdout.slice(start)) as Record<string, unknown>;
  } catch {
    return null;
  }
}

function sessionRows(buffer: LocalEventBuffer, sessionId: string) {
  return (
    buffer.database
      .prepare("select count(*) as rows from buffered_events where session_id = ?")
      .get(sessionId) as { rows: number }
  ).rows;
}

function spoolFileNames(directory: string) {
  try {
    return fs.readdirSync(directory).sort();
  } catch {
    return [] as string[];
  }
}

const HOOK_SPOOL_STATUS_FIELDS = [
  "recovered",
  "rejected",
  "deferred",
  "pendingFiles",
  "pendingBytes",
  "oldestPendingAgeSeconds",
  "lastDrainAt",
  "enabled",
] as const;

function hasHookSpoolFields(value: unknown) {
  if (!value || typeof value !== "object") return false;
  const record = value as Record<string, unknown>;
  return HOOK_SPOOL_STATUS_FIELDS.every((field) => field in record);
}


/**
 * A real HTTP listener that answers one fixed status. The trigger set has to be
 * exercised against a socket that really produced that status line, not a
 * stubbed fetch: the client's behaviour on 408 and 503 is the whole bead.
 */
async function startStatusListener(status: number, reason: string) {
  const server = http.createServer((request, response) => {
    request.resume();
    request.on("end", () => {
      response.writeHead(status, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "collector_request_rejected", reason }));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    port: (server.address() as AddressInfo).port,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

/** A listener that RSTs the connection after the request arrives. */
async function startResettingListener() {
  const server = net.createServer((socket) => {
    socket.once("data", () => {
      socket.resetAndDestroy();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    port: (server.address() as AddressInfo).port,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

function errorCode(error: unknown): string | undefined {
  let cursor: unknown = error;
  for (let depth = 0; depth < 4 && cursor && typeof cursor === "object"; depth += 1) {
    const code = (cursor as { code?: unknown }).code;
    if (typeof code === "string") return code;
    cursor = (cursor as { cause?: unknown }).cause;
  }
  return undefined;
}

/**
 * Post to a real collector declaring a body it never finishes sending, so the
 * collector's own 1.5 s request deadline fires while it is still reading —
 * the real `request_deadline_exceeded` 408 the client now spools on.
 */
function postNeverFinishedBody(port: number, token: string) {
  return new Promise<string>((resolve, reject) => {
    const socket = net.connect(port, "127.0.0.1", () => {
      socket.write(
        [
          "POST /hooks/claude-code HTTP/1.1",
          `Host: 127.0.0.1:${port}`,
          "content-type: application/json",
          "x-plimsoll-source: claude_code",
          `x-plimsoll-token: ${token}`,
          "content-length: 4096",
          "connection: close",
          "",
          "",
        ].join("\r\n"),
      );
      // One byte, then silence: the collector waits, its budget expires.
      socket.write("{");
    });
    let received = "";
    socket.setTimeout(10_000, () => {
      socket.destroy();
      reject(new Error("slow_body_probe_timed_out"));
    });
    socket.on("data", (chunk) => {
      received += chunk.toString("utf8");
    });
    socket.on("close", () => resolve(received));
    socket.on("error", (error) => (received ? resolve(received) : reject(error)));
  });
}

const OPERATOR_HOOK_SPOOL_FIELDS = [...HOOK_SPOOL_STATUS_FIELDS, "enabledSource"] as const;

function hasOperatorHookSpoolFields(value: unknown) {
  if (!value || typeof value !== "object") return false;
  const record = value as Record<string, unknown>;
  return OPERATOR_HOOK_SPOOL_FIELDS.every((field) => field in record);
}

/**
 * Real hook bodies, in the shape the hooks send today (the same fixtures the
 * signal-fidelity proof posts to /hooks/claude-code), so the drain exercises
 * the real normalizer path rather than a synthetic minimum.
 */
const SESSION_LOCKED = "b3f1c2d4-5e6a-4b7c-8d9e-0f1a2b3c4d5e";
const SESSION_CLI = "c4a2d3e5-6f7b-4c8d-9e0f-1a2b3c4d5e6f";
const SESSION_REFUSED = "d5b3e4f6-7a8c-4d9e-8f01-2b3c4d5e6f70";
const SESSION_UNAUTHORIZED = "e6c4f5a7-8b9d-4e0f-9012-3c4d5e6f7081";

function claudeHookBody(sessionId: string, prompt: string) {
  return JSON.stringify({
    hook_event_name: "UserPromptSubmit",
    session_id: sessionId,
    timestamp: "2026-09-12T17:04:11.000Z",
    cwd: "/Users/proof/workspace/plimsoll",
    prompt,
  });
}

function codexHookBody(sessionId: string) {
  return JSON.stringify({
    hook_event_name: "PostToolUse",
    session_id: sessionId,
    timestamp: "2026-09-12T17:04:12.000Z",
    tool_name: "shell",
    tool_input: { command: "git status --porcelain" },
  });
}

async function caseLockedLedgerRecovers() {
  const { root, home } = fixtureHome("a");
  const collector = await startCollector(home);
  // The child hook command reads the port from the home's collector config.
  saveCollectorConfig(collectorConfigSchema.parse({ port: collector.port }));
  const lock = holdWriteLock(collector.ledgerPath);
  try {
    const body = claudeHookBody(SESSION_LOCKED, "RAW_PROMPT_MUST_NOT_REACH_DISK");
    const forwarded = await forwardHookOverLoopback(body, {
      source: "claude_code",
      port: collector.port,
      auth: collector.auth,
    });
    check(
      "a_locked_ledger_503_spools_instead_of_throwing",
      "spooled" in forwarded && forwarded.spooled === true,
      forwarded,
    );

    const spooled = listHookSpoolFiles(home);
    const envelope = spooled.length === 1
      ? (JSON.parse(fs.readFileSync(spooled[0]!.path, "utf8")) as Record<string, unknown>)
      : null;
    const blanked = blankForbiddenRawContent(body)!;
    check(
      "a_spool_file_holds_the_blanked_body_under_a_versioned_envelope",
      spooled.length === 1 &&
        envelope?.v === 1 &&
        envelope?.source === "claude_code" &&
        typeof envelope?.receivedAt === "string" &&
        envelope?.blanked === 1 &&
        envelope?.body === blanked.text &&
        // Every non-content field of the body survives untouched; only the
        // forbidden value is empty.
        JSON.stringify(JSON.parse(blanked.text as string)) ===
          JSON.stringify({ ...JSON.parse(body), prompt: "" }),
      {
        files: spooled.length,
        source: envelope?.source,
        blanked: envelope?.blanked,
        bodyMatchesBlanked: envelope?.body === blanked.text,
      },
    );

    const raw = spooled.length === 1 ? fs.readFileSync(spooled[0]!.path, "utf8") : "";
    const tokens = [
      collector.auth.claudeCodeProducer,
      collector.auth.codexProducer,
      collector.auth.grokProducer,
      collector.auth.geminiCliProducer,
      collector.auth.managementRead,
    ].filter((token): token is string => typeof token === "string");
    const fileMode = spooled.length === 1 ? fs.statSync(spooled[0]!.path).mode & 0o777 : -1;
    const directoryMode = fs.statSync(hookSpoolDirectory(home)).mode & 0o777;
    check(
      "a_spool_file_carries_no_producer_token_and_stays_private",
      tokens.length === 5 &&
        tokens.every((token) => !raw.includes(token)) &&
        fileMode === 0o600 &&
        directoryMode === 0o700,
      { tokens: tokens.length, fileMode: fileMode.toString(8), directoryMode: directoryMode.toString(8) },
    );

    // The managed hook command itself: a 503 must leave it exiting 0.
    const child = await runCli(home, root, ["forward-hook-http", "claude-code"], claudeHookBody(SESSION_CLI, "SECOND_PROMPT"));
    check(
      "a_hook_command_exits_zero_and_reports_the_spooled_capture",
      child.status === 0 &&
        child.stderr.includes("hook_spooled") &&
        child.stdout.trim() === "" &&
        listHookSpoolFiles(home).length === 2,
      { status: child.status, stderr: child.stderr.trim().slice(0, 200), pending: listHookSpoolFiles(home).length },
    );

    check(
      "a_nothing_reached_the_ledger_while_the_writer_was_held",
      sessionRows(collector.buffer, SESSION_LOCKED) === 0 &&
        sessionRows(collector.buffer, SESSION_CLI) === 0,
      {
        locked: sessionRows(collector.buffer, SESSION_LOCKED),
        cli: sessionRows(collector.buffer, SESSION_CLI),
      },
    );

    // A busy ledger is a wait, not a verdict: the drain defers, keeps the file,
    // and stops the tick rather than walking the rest of the queue into the
    // same lock or quarantining a body that is perfectly valid.
    const busyTick = await collector.drain.tick();
    check(
      "a_a_busy_ledger_defers_the_drain_without_losing_or_rejecting_a_file",
      busyTick.deferred === 1 &&
        busyTick.deferredTick === true &&
        busyTick.attempted === 1 &&
        busyTick.recovered === 0 &&
        busyTick.rejected === 0 &&
        listHookSpoolFiles(home).length === 2 &&
        readHookSpoolCounters(home).deferred === 1 &&
        !fs.existsSync(hookSpoolRejectedDirectory(home)),
      { tick: busyTick, pending: listHookSpoolFiles(home).length },
    );

    lock.release();
    collector.drain.start();
    const drained = await waitForDrainedCounters(home, 2);
    const counters = drained.counters;
    check(
      "a_released_lock_lands_both_events_with_their_session_ids_inside_15s",
      drained.settled &&
        drained.elapsedMs <= RECOVERY_DEADLINE_MS &&
        sessionRows(collector.buffer, SESSION_LOCKED) === 1 &&
        sessionRows(collector.buffer, SESSION_CLI) === 1 &&
        listHookSpoolFiles(home).length === 0 &&
        counters.recovered === 2 &&
        counters.rejected === 0 &&
        typeof counters.lastDrainAt === "string",
      {
        elapsedMs: drained.elapsedMs,
        budgetMs: RECOVERY_DEADLINE_MS,
        rows: {
          locked: sessionRows(collector.buffer, SESSION_LOCKED),
          cli: sessionRows(collector.buffer, SESSION_CLI),
        },
        pending: listHookSpoolFiles(home).length,
        counters,
      },
    );

    const eventSource = collector.buffer.database
      .prepare("select source, event_type as eventType from buffered_events where session_id = ?")
      .get(SESSION_LOCKED) as { source?: string; eventType?: string } | undefined;
    const ledgerBytes = fs.readFileSync(collector.ledgerPath);
    check(
      "a_recovered_row_is_attributed_and_still_content_suppressed",
      eventSource?.source === "claude_code" &&
        !ledgerBytes.includes(Buffer.from("RAW_PROMPT_MUST_NOT_REACH_DISK")),
      { eventSource },
    );

    // Case (h): the counters are visible where an operator looks for them.
    const status = await collector.statusBody();
    const httpHookSpool = status.hookSpool as Record<string, unknown> | null;
    check(
      "h_http_status_exposes_the_hook_spool_counters",
      hasHookSpoolFields(httpHookSpool) &&
        httpHookSpool?.recovered === 2 &&
        httpHookSpool?.pendingFiles === 0 &&
        httpHookSpool?.enabled === true,
      httpHookSpool,
    );

    const statusCli = await runCli(home, root, ["status"]);
    const statusJson = parseJsonStdout(statusCli.stdout);
    const cliHookSpool = statusJson?.hookSpool as Record<string, unknown> | undefined;
    check(
      "h_plimsoll_status_json_exposes_the_hook_spool_counters",
      statusCli.status === 0 &&
        hasHookSpoolFields(cliHookSpool) &&
        cliHookSpool?.recovered === 2 &&
        cliHookSpool?.pendingFiles === 0,
      { status: statusCli.status, hookSpool: cliHookSpool },
    );

    const doctorCli = await runCli(home, root, ["doctor", "--read-only", "--json"]);
    const doctorJson = parseJsonStdout(doctorCli.stdout);
    const doctorHookSpool = doctorJson?.hookSpool as Record<string, unknown> | undefined;
    check(
      "h_plimsoll_doctor_reports_the_hook_spool_section",
      hasHookSpoolFields(doctorHookSpool) &&
        doctorHookSpool?.recovered === 2 &&
        doctorHookSpool?.draining === true &&
        !("diagnostic" in (doctorHookSpool ?? {})),
      doctorHookSpool,
    );
  } finally {
    lock.release();
    await collector.close();
  }
}

async function caseRefusedConnectionRecovers() {
  const { home } = fixtureHome("b");
  const auth = loadOrCreateLocalIngestAuth(home);
  const deadPort = await freePort();
  const body = codexHookBody(SESSION_REFUSED);
  const forwarded = await forwardHookOverLoopback(body, {
    source: "codex",
    port: deadPort,
    auth,
  });
  const spooled = listHookSpoolFiles(home);
  const envelope = spooled.length === 1
    ? (JSON.parse(fs.readFileSync(spooled[0]!.path, "utf8")) as Record<string, unknown>)
    : null;
  check(
    "b_refused_connection_spools_the_codex_body",
    "spooled" in forwarded &&
      forwarded.spooled === true &&
      spooled.length === 1 &&
      envelope?.source === "codex" &&
      envelope?.body === blankForbiddenRawContent(body)!.text &&
      envelope?.blanked === 1,
    { forwarded, files: spooled.length, source: envelope?.source, blanked: envelope?.blanked },
  );

  const collector = await startCollector(home);
  try {
    collector.drain.start();
    const drained = await waitForDrainedCounters(home, 1);
    check(
      "b_started_collector_recovers_the_refused_event_inside_15s",
      drained.settled &&
        drained.elapsedMs <= RECOVERY_DEADLINE_MS &&
        sessionRows(collector.buffer, SESSION_REFUSED) === 1 &&
        listHookSpoolFiles(home).length === 0 &&
        drained.counters.recovered === 1,
      {
        elapsedMs: drained.elapsedMs,
        budgetMs: RECOVERY_DEADLINE_MS,
        rows: sessionRows(collector.buffer, SESSION_REFUSED),
        counters: drained.counters,
      },
    );
  } finally {
    await collector.close();
  }
}

async function caseContractRejectionStillSurfaces() {
  const { home } = fixtureHome("c");
  const collector = await startCollector(home);
  try {
    const wrongAuth = {
      ...collector.auth,
      claudeCodeProducer: crypto.randomBytes(32).toString("base64url"),
    };
    let thrown: unknown;
    try {
      await forwardHookOverLoopback(claudeHookBody(SESSION_UNAUTHORIZED, "unauthorized"), {
        source: "claude_code",
        port: collector.port,
        auth: wrongAuth,
      });
    } catch (error) {
      thrown = error;
    }
    check(
      "c_wrong_producer_token_still_throws_401_and_spools_nothing",
      thrown instanceof Error &&
        thrown.message === "hook_forward_http_rejected:401" &&
        listHookSpoolFiles(home).length === 0 &&
        !fs.existsSync(hookSpoolDirectory(home)) &&
        sessionRows(collector.buffer, SESSION_UNAUTHORIZED) === 0,
      {
        message: thrown instanceof Error ? thrown.message : String(thrown),
        spoolDirectoryCreated: fs.existsSync(hookSpoolDirectory(home)),
      },
    );
  } finally {
    await collector.close();
  }
}

async function caseInvalidBodyIsQuarantined() {
  const { home } = fixtureHome("d");
  const collector = await startCollector(home);
  try {
    const written = writeHookSpoolFile({
      home,
      source: "claude_code",
      body: '{"hook_event_name":"PostToolUse", this is not valid json',
    });
    const first = await collector.drain.tick();
    const rejectedNames = spoolFileNames(hookSpoolRejectedDirectory(home));
    const counters = readHookSpoolCounters(home);
    check(
      "d_a_body_the_route_would_reject_moves_to_rejected_and_counts_once",
      written !== null &&
        first.rejected === 1 &&
        first.recovered === 0 &&
        listHookSpoolFiles(home).length === 0 &&
        rejectedNames.length === 1 &&
        rejectedNames[0]!.endsWith(".invalid_json.json") &&
        counters.rejected === 1,
      { tick: first, rejectedNames, counters },
    );

    const second = await collector.drain.tick();
    check(
      "d_a_quarantined_body_is_not_retried_on_the_next_tick",
      second.attempted === 0 &&
        second.rejected === 0 &&
        spoolFileNames(hookSpoolRejectedDirectory(home)).length === 1 &&
        readHookSpoolCounters(home).rejected === 1,
      { tick: second, counters: readHookSpoolCounters(home) },
    );
  } finally {
    await collector.close();
  }
}

async function caseUntrustedFilesAreQuarantined() {
  const { home } = fixtureHome("e");
  const collector = await startCollector(home);
  try {
    // A world-readable file: not the 0600 this client writes, so not replayed.
    const looseMode = writeHookSpoolFile({
      home,
      source: "codex",
      body: codexHookBody("f7d5a6b8-9c0e-4f12-a345-6d7e8f901234"),
    });
    fs.chmodSync(looseMode!.path, 0o644);
    // A source outside the closed hook set.
    const foreignName = `${Date.now()}-${process.pid}-abcdef.json`;
    const foreignPath = path.join(hookSpoolDirectory(home), foreignName);
    fs.writeFileSync(
      foreignPath,
      JSON.stringify({
        v: 1,
        source: "gemini",
        receivedAt: new Date().toISOString(),
        body: codexHookBody("a1b2c3d4-e5f6-4712-8934-56789abcdef0"),
      }),
      { mode: 0o600 },
    );
    fs.chmodSync(foreignPath, 0o600);

    const tick = await collector.drain.tick();
    const rejectedNames = spoolFileNames(hookSpoolRejectedDirectory(home));
    const rowsBefore = (
      collector.buffer.database
        .prepare("select count(*) as rows from buffered_events")
        .get() as { rows: number }
    ).rows;
    check(
      "e_untrusted_source_and_mode_are_quarantined_as_spool_untrusted",
      tick.rejected === 2 &&
        tick.recovered === 0 &&
        rowsBefore === 0 &&
        listHookSpoolFiles(home).length === 0 &&
        rejectedNames.length === 2 &&
        rejectedNames.every((name) => name.endsWith(".spool_untrusted.json")) &&
        readHookSpoolCounters(home).rejected === 2,
      { tick, rejectedNames, rowsBefore },
    );
  } finally {
    await collector.close();
  }
}

async function caseDirectoryBoundKeepsLossVisible() {
  const { home } = fixtureHome("f");
  const collector = await startCollector(home);
  const lock = holdWriteLock(collector.ledgerPath);
  try {
    const limits = { maxFiles: 2 };
    const first = await forwardHookOverLoopback(claudeHookBody("11111111-2222-4333-8444-555555555551", "one"), {
      source: "claude_code",
      port: collector.port,
      auth: collector.auth,
      spoolLimits: limits,
    });
    const second = await forwardHookOverLoopback(claudeHookBody("11111111-2222-4333-8444-555555555552", "two"), {
      source: "claude_code",
      port: collector.port,
      auth: collector.auth,
      spoolLimits: limits,
    });
    let thrown: unknown;
    try {
      await forwardHookOverLoopback(claudeHookBody("11111111-2222-4333-8444-555555555553", "three"), {
        source: "claude_code",
        port: collector.port,
        auth: collector.auth,
        spoolLimits: limits,
      });
    } catch (error) {
      thrown = error;
    }
    const names = spoolFileNames(hookSpoolDirectory(home));
    check(
      "f_the_file_bound_stops_spooling_and_surfaces_the_503",
      "spooled" in first &&
        "spooled" in second &&
        thrown instanceof Error &&
        thrown.message === "hook_forward_http_rejected:503" &&
        listHookSpoolFiles(home).length === 2 &&
        names.every((name) => !name.endsWith(".tmp")),
      {
        message: thrown instanceof Error ? thrown.message : String(thrown),
        pending: listHookSpoolFiles(home).length,
        names,
      },
    );
  } finally {
    lock.release();
    await collector.close();
  }
}

async function caseKillSwitchRestoresTodaysBehaviour() {
  const { home } = fixtureHome("g");
  setEnv("PLIMSOLL_HOOK_SPOOL", "off");
  const collector = await startCollector(home);
  const lock = holdWriteLock(collector.ledgerPath);
  try {
    let thrown: unknown;
    try {
      await forwardHookOverLoopback(claudeHookBody("22222222-3333-4444-8555-666666666666", "off"), {
        source: "claude_code",
        port: collector.port,
        auth: collector.auth,
      });
    } catch (error) {
      thrown = error;
    }
    const tick = await collector.drain.tick();
    check(
      "g_kill_switch_throws_exactly_as_today_and_creates_no_directory",
      thrown instanceof Error &&
        thrown.message === "hook_forward_http_rejected:503" &&
        !fs.existsSync(hookSpoolDirectory(home)) &&
        collector.drain.status().enabled === false &&
        tick.attempted === 0 &&
        tick.recovered === 0,
      {
        message: thrown instanceof Error ? thrown.message : String(thrown),
        spoolDirectoryCreated: fs.existsSync(hookSpoolDirectory(home)),
        tick,
      },
    );
  } finally {
    lock.release();
    await collector.close();
    setEnv("PLIMSOLL_HOOK_SPOOL", undefined);
  }
}

/** Doctor has to say plainly that a spool nobody is draining is holding events. */
function caseDoctorNamesAStalledSpool() {
  const { home } = fixtureHome("i");
  writeHookSpoolFile({
    home,
    source: "grok",
    body: codexHookBody("33333333-4444-4555-8666-777777777777"),
    nowMs: Date.now() - 11 * 60_000,
  });
  const stalled = hookSpoolDoctorSection(home, hookSpoolDaemonEnabled(true));
  check(
    "i_doctor_names_a_spool_that_has_held_events_past_ten_minutes",
    stalled.pendingFiles === 1 &&
      stalled.draining === false &&
      stalled.diagnostic === "hook_spool_pending_not_draining" &&
      typeof stalled.note === "string" &&
      stalled.note.includes("not draining") &&
      stalled.stalePendingSeconds === HOOK_SPOOL_LIMITS.stalePendingSeconds,
    stalled,
  );
  // The same section stays quiet while the drain is simply between ticks.
  const { home: freshHome } = fixtureHome("j");
  writeHookSpoolFile({
    home: freshHome,
    source: "claude_code",
    body: claudeHookBody("44444444-5555-4666-8777-888888888888", "just spooled"),
  });
  const fresh = hookSpoolDoctorSection(freshHome, hookSpoolDaemonEnabled(true));
  check(
    "i_doctor_stays_quiet_for_a_spool_that_was_written_a_moment_ago",
    fresh.pendingFiles === 1 &&
      fresh.draining === true &&
      !("diagnostic" in fresh) &&
      !("note" in fresh),
    fresh,
  );
  check(
    "i_status_helper_reports_the_pending_bytes_and_age_it_measured",
    hookSpoolOperatorStatus(home, hookSpoolDaemonEnabled(true)).pendingBytes > 0 &&
      (hookSpoolOperatorStatus(home, hookSpoolDaemonEnabled(true)).oldestPendingAgeSeconds ?? 0) >= 600,
    hookSpoolOperatorStatus(home, hookSpoolDaemonEnabled(true)),
  );
}


/**
 * The trigger set, driven against real listeners: exactly the outcomes that
 * prove the collector stored nothing spool; everything else still throws.
 *
 * 408 is here because Studio0 is losing codex hook posts to it right now, and
 * ECONNRESET is deliberately NOT here (review r1, F2): a reset can arrive after
 * the row was committed, and nothing dedups a replay.
 */
async function caseTriggerSetIsExactlyTheOutcomesThatStoredNothing() {
  const { home } = fixtureHome("k");
  const auth = loadOrCreateLocalIngestAuth(home);
  const body = claudeHookBody("55555555-6666-4777-8888-999999999991", "trigger set");

  // 503 and 408 spool.
  const spoolable: Array<{ status: number; reason: string }> = [
    { status: 503, reason: "storage_busy_retry" },
    { status: 408, reason: "request_deadline_exceeded" },
  ];
  const spooledOutcomes: Record<string, unknown> = {};
  for (const candidate of spoolable) {
    const listener = await startStatusListener(candidate.status, candidate.reason);
    try {
      const forwarded = await forwardHookOverLoopback(body, {
        source: "claude_code",
        port: listener.port,
        auth,
      });
      spooledOutcomes[String(candidate.status)] = forwarded;
    } catch (error) {
      spooledOutcomes[String(candidate.status)] = `threw:${(error as Error).message}`;
    } finally {
      await listener.close();
    }
  }
  check(
    "k_503_and_408_spool_because_the_collector_stored_nothing",
    spoolable.every((candidate) => {
      const outcome = spooledOutcomes[String(candidate.status)];
      return Boolean(outcome && typeof outcome === "object" && "spooled" in outcome);
    }) && listHookSpoolFiles(home).length === 2,
    { outcomes: spooledOutcomes, pending: listHookSpoolFiles(home).length },
  );

  // Every other status still throws, and writes nothing.
  const pendingBefore = listHookSpoolFiles(home).length;
  const refused = [400, 401, 403, 404, 413, 415, 429, 500, 502, 504];
  const thrownMessages: Record<string, string> = {};
  for (const status of refused) {
    const listener = await startStatusListener(status, "contract_rejection");
    try {
      await forwardHookOverLoopback(body, { source: "claude_code", port: listener.port, auth });
      thrownMessages[String(status)] = "did_not_throw";
    } catch (error) {
      thrownMessages[String(status)] = error instanceof Error ? error.message : String(error);
    } finally {
      await listener.close();
    }
  }
  check(
    "k_every_other_status_still_throws_and_spools_nothing",
    refused.every((status) => thrownMessages[String(status)] === `hook_forward_http_rejected:${status}`) &&
      listHookSpoolFiles(home).length === pendingBefore,
    { thrownMessages, pending: listHookSpoolFiles(home).length },
  );

  // A reset connection: the collector may already have committed the row.
  const resetHome = fixtureHome("k_reset");
  const resetAuth = loadOrCreateLocalIngestAuth(resetHome.home);
  const resetter = await startResettingListener();
  let resetThrown: unknown;
  try {
    await forwardHookOverLoopback(body, {
      source: "claude_code",
      port: resetter.port,
      auth: resetAuth,
    });
  } catch (error) {
    resetThrown = error;
  } finally {
    await resetter.close();
  }
  check(
    "k_a_reset_connection_throws_and_spools_nothing_at_most_once",
    resetThrown instanceof Error &&
      errorCode(resetThrown) !== undefined &&
      listHookSpoolFiles(resetHome.home).length === 0 &&
      !fs.existsSync(hookSpoolDirectory(resetHome.home)),
    {
      code: errorCode(resetThrown),
      message: resetThrown instanceof Error ? resetThrown.message : String(resetThrown),
      pending: listHookSpoolFiles(resetHome.home).length,
      spoolDirectoryCreated: fs.existsSync(hookSpoolDirectory(resetHome.home)),
    },
  );

  // A refused connection still spools (unchanged from r1, re-asserted here so
  // the whole trigger set reads in one place).
  const refusedHome = fixtureHome("k_refused");
  const refusedAuth = loadOrCreateLocalIngestAuth(refusedHome.home);
  const deadPort = await freePort();
  const refusedForwarded = await forwardHookOverLoopback(body, {
    source: "claude_code",
    port: deadPort,
    auth: refusedAuth,
  });
  check(
    "k_a_refused_connection_still_spools",
    "spooled" in refusedForwarded && listHookSpoolFiles(refusedHome.home).length === 1,
    { refusedForwarded, pending: listHookSpoolFiles(refusedHome.home).length },
  );
}

/** The 408 the client spools on, end to end: spooled, then recovered. */
async function case408SpoolsAndRecovers() {
  const { home } = fixtureHome("k2");
  const auth = loadOrCreateLocalIngestAuth(home);
  const session = "66666666-7777-4888-8999-aaaaaaaaaaa1";
  const listener = await startStatusListener(408, "request_deadline_exceeded");
  let forwarded: unknown;
  try {
    forwarded = await forwardHookOverLoopback(claudeHookBody(session, "deadline"), {
      source: "claude_code",
      port: listener.port,
      auth,
    });
  } finally {
    await listener.close();
  }
  check(
    "k_408_request_deadline_exceeded_spools_the_event",
    Boolean(forwarded && typeof forwarded === "object" && "spooled" in forwarded) &&
      listHookSpoolFiles(home).length === 1,
    { forwarded, pending: listHookSpoolFiles(home).length },
  );

  const collector = await startCollector(home);
  try {
    collector.drain.start();
    const drained = await waitForDrainedCounters(home, 1);
    check(
      "k_the_408_event_is_recovered_into_the_ledger_inside_15s",
      drained.settled &&
        drained.elapsedMs <= RECOVERY_DEADLINE_MS &&
        sessionRows(collector.buffer, session) === 1 &&
        listHookSpoolFiles(home).length === 0 &&
        drained.counters.recovered === 1,
      {
        elapsedMs: drained.elapsedMs,
        budgetMs: RECOVERY_DEADLINE_MS,
        rows: sessionRows(collector.buffer, session),
        counters: drained.counters,
      },
    );
  } finally {
    await collector.close();
  }
}

/**
 * The 408 is not a synthetic status: a real collector answers
 * `request_deadline_exceeded` while it is still reading a body, and nothing
 * reached the ledger when it did.
 */
async function caseTheRealCollectorAnswers408WhileReadingTheBody() {
  const { home } = fixtureHome("k3");
  const collector = await startCollector(home);
  try {
    const rowsBefore = (
      collector.buffer.database.prepare("select count(*) as rows from buffered_events").get() as
        { rows: number }
    ).rows;
    const raw = await postNeverFinishedBody(collector.port, collector.auth.claudeCodeProducer!);
    const rowsAfter = (
      collector.buffer.database.prepare("select count(*) as rows from buffered_events").get() as
        { rows: number }
    ).rows;
    check(
      "k_a_real_collector_answers_408_request_deadline_exceeded_and_stores_nothing",
      raw.startsWith("HTTP/1.1 408") &&
        raw.includes("request_deadline_exceeded") &&
        rowsBefore === 0 &&
        rowsAfter === 0,
      { statusLine: raw.split("\r\n")[0], rowsBefore, rowsAfter },
    );
  } finally {
    await collector.close();
  }
}

/**
 * Review r1, F3: the quarantine age bound has to fire on a cadence of its own.
 * Nothing is rejected in this case at all — the start-up prune is what runs.
 */
async function caseQuarantineRetentionRunsWithoutANewRejection() {
  const { home } = fixtureHome("l");
  const rejectedDirectory = hookSpoolRejectedDirectory(home);
  fs.mkdirSync(rejectedDirectory, { recursive: true, mode: 0o700 });
  const thirtyDaysAgo = Date.now() - 30 * 24 * 60 * 60 * 1000;
  const stale: string[] = [];
  for (let index = 0; index < 5; index += 1) {
    const name = `${thirtyDaysAgo + index}-${process.pid}-ab${index}def.invalid_json.json`;
    fs.writeFileSync(path.join(rejectedDirectory, name), "{}", { mode: 0o600 });
    stale.push(name);
  }
  // A file inside the age bound must survive, so this proves an age rule and
  // not a "delete everything" rule.
  const fresh = `${Date.now()}-${process.pid}-beef01.invalid_json.json`;
  fs.writeFileSync(path.join(rejectedDirectory, fresh), "{}", { mode: 0o600 });

  const collector = await startCollector(home);
  try {
    const before = fs.readdirSync(rejectedDirectory).sort();
    collector.drain.start();
    const after = fs.readdirSync(rejectedDirectory).sort();
    const tick = await collector.drain.tick();
    check(
      "l_startup_prune_expires_old_quarantine_with_no_new_rejection",
      before.length === 6 &&
        after.length === 1 &&
        after[0] === fresh &&
        stale.every((name) => !after.includes(name)) &&
        tick.rejected === 0 &&
        tick.attempted === 0,
      { before: before.length, after, tick },
    );
  } finally {
    await collector.close();
  }
}

/**
 * Review r1, F4: a `*.json.tmp` left by a hook process that died between write
 * and rename is invisible to every listing, so it needed its own reaper and its
 * own place in the byte bound.
 */
async function caseOrphanTemporariesAreReapedAndCounted() {
  const { home } = fixtureHome("m");
  const directory = hookSpoolDirectory(home);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stalePath = path.join(directory, `${Date.now() - 600_000}-999-abcdef.json.tmp`);
  fs.writeFileSync(stalePath, "x".repeat(5_000), { mode: 0o600 });
  const staleAge = Date.now() - 10 * 60_000;
  fs.utimesSync(stalePath, staleAge / 1000, staleAge / 1000);
  const freshPath = path.join(directory, `${Date.now()}-998-fedcba.json.tmp`);
  fs.writeFileSync(freshPath, "y".repeat(1_000), { mode: 0o600 });

  const collector = await startCollector(home);
  try {
    const tick = await collector.drain.tick();
    check(
      "m_a_stale_temporary_is_reaped_by_a_tick_and_a_fresh_one_is_left_alone",
      !fs.existsSync(stalePath) &&
        fs.existsSync(freshPath) &&
        fs.statSync(freshPath).size === 1_000 &&
        tick.attempted === 0 &&
        listHookSpoolFiles(home).length === 0,
      { staleRemoved: !fs.existsSync(stalePath), freshKept: fs.existsSync(freshPath), tick },
    );

    // The bytes of a temporary still in flight are charged to the byte bound,
    // which is the hole the review found: 1,000 live temporary bytes against a
    // 1,200-byte ceiling leaves no room for an envelope.
    const written = writeHookSpoolFile({
      home,
      source: "claude_code",
      body: claudeHookBody("77777777-8888-4999-8aaa-bbbbbbbbbbb1", "bounded"),
      limits: { maxBytes: 1_200 },
    });
    check(
      "m_live_temporary_bytes_count_against_the_byte_bound",
      written === null && listHookSpoolFiles(home).length === 0,
      { written, pending: listHookSpoolFiles(home).length, temporaryBytes: 1_000 },
    );
  } finally {
    await collector.close();
  }
}

/**
 * Review r1, F5: `enabled` is the daemon's kill switch, read from the daemon.
 * The variable is in the COLLECTOR's environment here and deliberately not in
 * the CLI child's, which is the exact shape of the real LaunchAgent deployment.
 */
async function caseOperatorSurfacesReportTheDaemonsKillSwitch() {
  const { root, home } = fixtureHome("n");
  setEnv("PLIMSOLL_HOOK_SPOOL", "off");
  const collector = await startCollector(home);
  saveCollectorConfig(collectorConfigSchema.parse({ port: collector.port }));
  const withoutTheVariable = { PLIMSOLL_HOOK_SPOOL: undefined };
  try {
    const statusCli = await runCli(home, root, ["status"], undefined, withoutTheVariable);
    const statusJson = parseJsonStdout(statusCli.stdout);
    const statusSpool = statusJson?.hookSpool as Record<string, unknown> | undefined;
    check(
      "n_plimsoll_status_reports_the_daemons_kill_switch_not_the_shells",
      statusCli.status === 0 &&
        hasOperatorHookSpoolFields(statusSpool) &&
        statusSpool?.enabled === false &&
        statusSpool?.enabledSource === "collector",
      { status: statusCli.status, hookSpool: statusSpool },
    );

    const doctorCli = await runCli(
      home,
      root,
      ["doctor", "--read-only", "--json"],
      undefined,
      withoutTheVariable,
    );
    const doctorJson = parseJsonStdout(doctorCli.stdout);
    const doctorSpool = doctorJson?.hookSpool as Record<string, unknown> | undefined;
    check(
      "n_plimsoll_doctor_reports_the_daemons_kill_switch_not_the_shells",
      hasOperatorHookSpoolFields(doctorSpool) &&
        doctorSpool?.enabled === false &&
        doctorSpool?.enabledSource === "collector",
      { hookSpool: doctorSpool },
    );
  } finally {
    await collector.close();
    setEnv("PLIMSOLL_HOOK_SPOOL", undefined);
  }

  // No daemon to ask: say so, rather than answering from this shell.
  const unreachable = fixtureHome("n2");
  saveCollectorConfig(collectorConfigSchema.parse({ port: await freePort() }));
  const statusCli = await runCli(unreachable.home, unreachable.root, ["status"]);
  const statusJson = parseJsonStdout(statusCli.stdout);
  const statusSpool = statusJson?.hookSpool as Record<string, unknown> | undefined;
  check(
    "n_an_unreachable_collector_reports_enabled_null_and_says_why",
    statusCli.status === 0 &&
      hasOperatorHookSpoolFields(statusSpool) &&
      statusSpool?.enabled === null &&
      statusSpool?.enabledSource === "collector_unreachable",
    { status: statusCli.status, hookSpool: statusSpool },
  );
}

/**
 * Review r1, F7 and F8: a counters-write failure logs its code and nothing
 * else, and an unusable Plimsoll home does not replace the rejection the caller
 * is about to surface.
 */
async function caseWarningsAndHomeErrorsStayReceiptSafe() {
  const { home } = fixtureHome("o");
  const directory = hookSpoolDirectory(home);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  // The counters file's own temporary, as a directory: the write fails with
  // EISDIR, and an fs message would carry the absolute path with it.
  fs.mkdirSync(`${hookSpoolCountersPath(home)}.tmp`, { mode: 0o700 });
  writeHookSpoolFile({
    home,
    source: "claude_code",
    body: claudeHookBody("88888888-9999-4aaa-8bbb-ccccccccccc1", "counters"),
  });
  const warnings: Record<string, unknown>[] = [];
  const collector = await startCollector(home);
  try {
    const drain = createHookSpoolDrain(
      collectorConfigSchema.parse({}),
      collector.buffer,
      { home, onWarning: (line) => warnings.push(line) },
    );
    await drain.tick();
    const failure = warnings.find((line) => line.warning === "hook_spool_counters_write_failed");
    const serialized = JSON.stringify(failure ?? {});
    check(
      "o_a_counters_write_failure_logs_the_code_and_never_a_path",
      Boolean(failure) &&
        failure?.code === "EISDIR" &&
        !("message" in (failure ?? {})) &&
        !serialized.includes(home) &&
        !serialized.includes("/"),
      { failure, warnings: warnings.length },
    );
  } finally {
    await collector.close();
  }

  // F8: an unusable PLIMSOLL_HOME must fall through to the 503, not replace it.
  const { home: liveHome } = fixtureHome("o2");
  const auth = loadOrCreateLocalIngestAuth(liveHome);
  const listener = await startStatusListener(503, "storage_busy_retry");
  let thrown: unknown;
  try {
    await forwardHookOverLoopback(claudeHookBody("99999999-aaaa-4bbb-8ccc-ddddddddddd1", "home"), {
      source: "claude_code",
      port: listener.port,
      auth,
      env: { ...process.env, PLIMSOLL_HOME: "relative/not/absolute" },
    });
  } catch (error) {
    thrown = error;
  } finally {
    await listener.close();
  }
  check(
    "o_an_unusable_plimsoll_home_falls_through_to_the_original_rejection",
    thrown instanceof Error && thrown.message === "hook_forward_http_rejected:503",
    { message: thrown instanceof Error ? thrown.message : String(thrown) },
  );
}


const PROMPT_CANARY = "SECRET-PROMPT-CANARY";

/** A body carrying raw content in three forbidden fields at two depths. */
function canaryHookBody(sessionId: string) {
  return JSON.stringify({
    hook_event_name: "PostToolUse",
    session_id: sessionId,
    timestamp: "2026-09-12T17:04:14.000Z",
    cwd: "/Users/proof/workspace/plimsoll",
    tool_name: "Bash",
    prompt: `${PROMPT_CANARY} what the operator typed`,
    raw_prompt: `${PROMPT_CANARY} verbatim`,
    tool_input: { command: `echo ${PROMPT_CANARY}` },
  });
}

/**
 * Columns that cannot match between two rows and say nothing about the change:
 * `id` and `privacy_generation` are fresh UUIDs per append, `created_at` is
 * arrival time. Everything else is compared value for value.
 */
const ARRIVAL_ONLY_COLUMNS = new Set(["id", "created_at", "privacy_generation"]);

function comparableRow(row: Record<string, unknown>) {
  const comparable: Record<string, unknown> = {};
  for (const [column, value] of Object.entries(row)) {
    if (ARRIVAL_ONLY_COLUMNS.has(column)) continue;
    if (column === "payload_json") {
      const payload = JSON.parse(String(value)) as Record<string, unknown>;
      delete payload.id;
      comparable[column] = JSON.stringify(payload);
      continue;
    }
    comparable[column] = value;
  }
  return comparable;
}

/**
 * The privacy question the Astra review asked, answered both ways:
 * (i) the raw content never reaches the disk, and
 * (ii) the ledger outcome is nevertheless field-for-field what the live path
 *      would have written for the same original body.
 * (iii) is the negative control: without the blanking step, it does reach disk.
 *
 * This body carries its own `timestamp`, so its parity is total — all 27
 * columns, `observed_at` included. Review r2 (F1) was right that a fixture
 * with a timestamp cannot prove the recovery clock is not used: the case that
 * does is `q`, over the shipped fixtures, none of which carries one, and the
 * case that proves the spool holds no more than the ledger would is `r`.
 */
async function casePrivacyBlankingKeepsTheLedgerIdentical() {
  const { home } = fixtureHome("p");
  const collector = await startCollector(home);
  const session = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeee1";
  const body = canaryHookBody(session);
  try {
    // The live path, with the ledger free.
    const live = await forwardHookOverLoopback(body, {
      source: "claude_code",
      port: collector.port,
      auth: collector.auth,
    });
    check(
      "p_the_live_path_accepts_the_original_body",
      "accepted" in live && sessionRows(collector.buffer, session) === 1,
      { live, rows: sessionRows(collector.buffer, session) },
    );

    // The spooled path, with the ledger held.
    const lock = holdWriteLock(collector.ledgerPath);
    let spooledOutcome: unknown;
    try {
      spooledOutcome = await forwardHookOverLoopback(body, {
        source: "claude_code",
        port: collector.port,
        auth: collector.auth,
      });
    } finally {
      lock.release();
    }
    const spooled = listHookSpoolFiles(home);
    const bytes = spooled.length === 1 ? fs.readFileSync(spooled[0]!.path, "utf8") : "";
    const envelope = spooled.length === 1
      ? (JSON.parse(bytes) as Record<string, unknown>)
      : null;
    const spooledBody = typeof envelope?.body === "string"
      ? (JSON.parse(envelope.body) as Record<string, unknown>)
      : null;
    const blankedKeys = ["prompt", "raw_prompt", "tool_input"];
    check(
      "p_the_spool_file_keeps_the_forbidden_keys_and_none_of_their_content",
      Boolean(spooledOutcome && typeof spooledOutcome === "object" && "spooled" in spooledOutcome) &&
        spooled.length === 1 &&
        envelope?.blanked === 3 &&
        // The canary is absent from the FILE's bytes, not merely from a field.
        !bytes.includes(PROMPT_CANARY) &&
        spooledBody !== null &&
        blankedKeys.every((key) => key in spooledBody && spooledBody[key] === "") &&
        // Metadata the ledger keeps must survive untouched.
        spooledBody.tool_name === "Bash" &&
        spooledBody.session_id === session,
      {
        blanked: envelope?.blanked,
        canaryOnDisk: bytes.includes(PROMPT_CANARY),
        keysOnDisk: blankedKeys.filter((key) => Boolean(spooledBody) && key in spooledBody!),
        blankedValues: blankedKeys.map((key) => spooledBody?.[key]),
        toolName: spooledBody?.tool_name,
      },
    );

    const tick = await collector.drain.tick();
    check(
      "p_the_spooled_event_is_recovered_into_the_ledger",
      tick.recovered === 1 && sessionRows(collector.buffer, session) === 2,
      { tick, rows: sessionRows(collector.buffer, session) },
    );

    const rows = collector.buffer.database
      .prepare("select * from buffered_events where session_id = ? order by rowid")
      .all(session) as Record<string, unknown>[];
    const liveRow = rows[0] ? comparableRow(rows[0]) : {};
    const recoveredRow = rows[1] ? comparableRow(rows[1]) : {};
    const differing = Object.keys(liveRow).filter(
      (column) => JSON.stringify(liveRow[column]) !== JSON.stringify(recoveredRow[column]),
    );
    // The parity table is a deliverable: print it whatever the verdict, so a
    // mismatch names the exact column rather than hiding behind a boolean.
    console.log(
      JSON.stringify({
        proof: "hook_spool",
        table: "live_vs_recovered_parity",
        comparedColumns: Object.keys(liveRow).length,
        ignoredColumns: [...ARRIVAL_ONLY_COLUMNS],
        differingColumns: differing,
        rows: { live: liveRow, recovered: recoveredRow },
      }),
    );
    check(
      "p_the_recovered_row_is_field_for_field_the_row_the_live_path_wrote",
      rows.length === 2 && Object.keys(liveRow).length > 0 && differing.length === 0,
      { rows: rows.length, comparedColumns: Object.keys(liveRow).length, differing },
    );

    const ledgerBytes = fs.readFileSync(collector.ledgerPath);
    check(
      "p_neither_row_put_the_canary_in_the_ledger",
      !ledgerBytes.includes(Buffer.from(PROMPT_CANARY)),
      { canaryInLedger: ledgerBytes.includes(Buffer.from(PROMPT_CANARY)) },
    );
  } finally {
    await collector.close();
  }

  // (iii) Negative control: the same body written WITHOUT the blanking step —
  // straight through `writeHookSpoolFile`, which is what the client used to do.
  const control = fixtureHome("p2");
  const written = writeHookSpoolFile({
    home: control.home,
    source: "claude_code",
    body: canaryHookBody("bbbbbbbb-cccc-4ddd-8eee-fffffffffff1"),
  });
  const controlBytes = written ? fs.readFileSync(written.path, "utf8") : "";
  check(
    "p_negative_control_without_blanking_the_canary_reaches_the_spool_file",
    written !== null && controlBytes.includes(PROMPT_CANARY),
    { canaryOnDisk: controlBytes.includes(PROMPT_CANARY) },
  );
}

/**
 * The four hook-input fixtures the repo ships and the signal-fidelity proof
 * posts — real bodies in the shape the hooks actually send. NONE of them
 * carries a timestamp of any kind, which is precisely why review r2 (F1) found
 * the recovered row stamped with the drain's clock.
 */
const HOOK_INPUT_FIXTURES = ["claude-tool-action", "claude-user-prompt-submit", "codex-stop-session", "codex-unknown-event"] as const;

function hookInputFixture(name: string) {
  const file = path.join(repoRoot, "packages", "shared", "fixtures", "hook-inputs", `${name}.json`);
  const body = fs.readFileSync(file, "utf8");
  const parsed = JSON.parse(body) as Record<string, unknown>;
  return {
    name,
    // Posted exactly as shipped — no added timestamp, no added field.
    body: JSON.stringify(parsed),
    sessionId: String(parsed.session_id),
    source: String(parsed.source) as "claude_code" | "codex",
  };
}

/** The one persisted column that legitimately moves, plus the payload it is embedded in. */
function payloadDifference(live: unknown, recovered: unknown) {
  const strip = (value: unknown) => {
    const payload = JSON.parse(String(value)) as Record<string, unknown>;
    const observedAt = payload.observedAt;
    delete payload.id;
    delete payload.observedAt;
    return { rest: JSON.stringify(payload), observedAt };
  };
  const liveSide = strip(live);
  const recoveredSide = strip(recovered);
  return {
    identicalApartFromObservedAt: liveSide.rest === recoveredSide.rest,
    liveObservedAt: liveSide.observedAt,
    recoveredObservedAt: recoveredSide.observedAt,
  };
}

function sessionRowsOf(buffer: LocalEventBuffer, sessionId: string) {
  return buffer.database
    .prepare("select * from buffered_events where session_id = ? order by rowid")
    .all(sessionId) as Record<string, unknown>[];
}

/**
 * Post one body live (ledger free), then again with the ledger held so the
 * collector answers its real 503 and the hook spools it. Returns the spooled
 * file's envelope, read off disk before anything drains it.
 */
async function liveThenSpool(collector: Collector, body: string, source: "claude_code" | "codex") {
  const before = new Set(listHookSpoolFiles(collector.home).map((file) => file.name));
  const live = await forwardHookOverLoopback(body, {
    source,
    port: collector.port,
    auth: collector.auth,
  });
  const lock = holdWriteLock(collector.ledgerPath);
  let spooledOutcome: unknown;
  try {
    spooledOutcome = await forwardHookOverLoopback(body, {
      source,
      port: collector.port,
      auth: collector.auth,
    });
  } finally {
    lock.release();
  }
  const written = listHookSpoolFiles(collector.home).filter((file) => !before.has(file.name));
  const envelope = written.length === 1
    ? (JSON.parse(fs.readFileSync(written[0]!.path, "utf8")) as Record<string, unknown>)
    : null;
  return { live, spooledOutcome, envelope, file: written[0] ?? null };
}

/**
 * Review r2, F1 — a recovered event carries the time the HOOK fired.
 *
 * Driven on every shipped fixture, with no synthetic timestamp added to any of
 * them (the r2 proof's canary carried one, which is what masked the defect):
 * live post, then a real 503 from a held write lock, a deliberate wait longer
 * than any plausible tick, then the drain. The recovered row must equal the
 * live row on every persisted column except `observed_at` — which must be the
 * envelope's `receivedAt`, the hook process's own stamp — and `payload_json`,
 * which embeds it. A fifth body that DOES carry a `timestamp` must keep it.
 */
async function caseRecoveredEventsKeepTheHooksTime() {
  const { home } = fixtureHome("q");
  const collector = await startCollector(home);
  /** Longer than the 5 s drain interval, so a drain clock cannot pass for a hook clock. */
  const SPOOL_LATENCY_MS = 6_000;
  const table: Record<string, unknown>[] = [];
  try {
    const fixtures = HOOK_INPUT_FIXTURES.map(hookInputFixture);
    const spooled: Array<{ fixture: ReturnType<typeof hookInputFixture>; receivedAt: string }> = [];
    for (const fixture of fixtures) {
      const outcome = await liveThenSpool(collector, fixture.body, fixture.source);
      check(
        `q_${fixture.name.replace(/-/g, "_")}_posts_live_and_then_spools`,
        Boolean(outcome.live && "accepted" in outcome.live) &&
          Boolean(outcome.spooledOutcome && typeof outcome.spooledOutcome === "object" &&
            "spooled" in outcome.spooledOutcome) &&
          typeof outcome.envelope?.receivedAt === "string",
        { live: outcome.live, spooled: outcome.spooledOutcome, receivedAt: outcome.envelope?.receivedAt },
      );
      spooled.push({ fixture, receivedAt: String(outcome.envelope?.receivedAt ?? "") });
    }

    // The wait is the point: everything below has to come out the same anyway.
    await new Promise<void>((resolve) => setTimeout(resolve, SPOOL_LATENCY_MS));
    const drainedAt = Date.now();
    const tick = await collector.drain.tick();
    check(
      "q_the_drain_recovers_every_shipped_fixture",
      tick.recovered === fixtures.length && tick.rejected === 0,
      { tick, fixtures: fixtures.length },
    );

    for (const { fixture, receivedAt } of spooled) {
      const rows = sessionRowsOf(collector.buffer, fixture.sessionId);
      const liveRow = rows[0] ? comparableRow(rows[0]) : {};
      const recoveredRow = rows[1] ? comparableRow(rows[1]) : {};
      const differing = Object.keys(liveRow).filter(
        (column) => JSON.stringify(liveRow[column]) !== JSON.stringify(recoveredRow[column]),
      );
      const payload = payloadDifference(liveRow.payload_json, recoveredRow.payload_json);
      const recoveredObservedAtMs = Date.parse(String(recoveredRow.observed_at));
      const receivedAtMs = Date.parse(receivedAt);
      const stampSkewMs = Math.abs(recoveredObservedAtMs - receivedAtMs);
      const drainSkewMs = Math.abs(drainedAt - recoveredObservedAtMs);
      const row = {
        fixture: fixture.name,
        comparedColumns: Object.keys(liveRow).length,
        differingColumns: differing,
        liveObservedAt: liveRow.observed_at,
        recoveredObservedAt: recoveredRow.observed_at,
        envelopeReceivedAt: receivedAt,
        stampSkewFromEnvelopeMs: stampSkewMs,
        distanceFromDrainClockMs: drainSkewMs,
        payloadIdenticalApartFromObservedAt: payload.identicalApartFromObservedAt,
        suppressedFieldsIdentical:
          liveRow.suppressed_fields_json === recoveredRow.suppressed_fields_json,
      };
      table.push(row);
      check(
        `q_${fixture.name.replace(/-/g, "_")}_recovers_with_the_hooks_own_time`,
        rows.length === 2 &&
          Object.keys(liveRow).length > 0 &&
          // Every persisted column matches except the time and the payload it sits in.
          differing.length === 2 &&
          differing.includes("observed_at") &&
          differing.includes("payload_json") &&
          payload.identicalApartFromObservedAt &&
          // The recovered stamp IS the envelope's receivedAt, to the millisecond.
          stampSkewMs <= 1 &&
          // ...and it is nowhere near the clock the drain ran on.
          drainSkewMs >= SPOOL_LATENCY_MS &&
          liveRow.suppressed_fields_json === recoveredRow.suppressed_fields_json,
        row,
      );
    }

    // The other half of the rule: a body that carries its own time keeps it.
    const stampedSession = "11112222-3333-4444-8555-666677778881";
    const stamped = JSON.stringify({
      hook_event_name: "UserPromptSubmit",
      session_id: stampedSession,
      timestamp: "2026-09-12T09:15:00.000Z",
      model: "claude-code-fixture",
      prompt: "raw content the ledger never keeps",
    });
    const stampedOutcome = await liveThenSpool(collector, stamped, "claude_code");
    await new Promise<void>((resolve) => setTimeout(resolve, 1_000));
    const stampedTick = await collector.drain.tick();
    const stampedRows = sessionRowsOf(collector.buffer, stampedSession);
    const stampedLive = stampedRows[0] ? comparableRow(stampedRows[0]) : {};
    const stampedRecovered = stampedRows[1] ? comparableRow(stampedRows[1]) : {};
    const stampedDiffering = Object.keys(stampedLive).filter(
      (column) => JSON.stringify(stampedLive[column]) !== JSON.stringify(stampedRecovered[column]),
    );
    const stampedRow = {
      fixture: "body_carrying_its_own_timestamp",
      comparedColumns: Object.keys(stampedLive).length,
      differingColumns: stampedDiffering,
      liveObservedAt: stampedLive.observed_at,
      recoveredObservedAt: stampedRecovered.observed_at,
      envelopeReceivedAt: String(stampedOutcome.envelope?.receivedAt ?? ""),
      bodyTimestamp: "2026-09-12T09:15:00.000Z",
    };
    table.push(stampedRow);
    check(
      "q_a_body_that_carries_its_own_timestamp_keeps_it",
      stampedTick.recovered === 1 &&
        stampedRows.length === 2 &&
        stampedDiffering.length === 0 &&
        stampedRecovered.observed_at === "2026-09-12T09:15:00.000Z" &&
        stampedLive.observed_at === "2026-09-12T09:15:00.000Z",
      stampedRow,
    );
  } finally {
    // The per-fixture table is a deliverable: print it whatever the verdict.
    console.log(
      JSON.stringify({
        proof: "hook_spool",
        table: "per_fixture_live_vs_recovered_parity",
        ignoredColumns: [...ARRIVAL_ONLY_COLUMNS],
        rows: table,
      }),
    );
    await collector.close();
  }
}

const ACCOUNT_NAME = "REALNAME";
const CLIENT_DIRECTORY = "acme-secret-merger";
const EDITED_FILE = "deal-terms.md";
const TRANSCRIPT_FILE = "00-transcript.jsonl";
const WORKING_DIRECTORY = `/Users/${ACCOUNT_NAME}/clients/${CLIENT_DIRECTORY}`;
/** The operator identity review r3 (N3) found resting raw in the spool, undeclared. */
const IDENTITY_CANARY = "REALNAME_OPERATOR_IDENTITY";

/**
 * The body review r2 (F2) reproduced with: a realistic Claude Code PostToolUse
 * hook whose paths carry the operator's account name, the client directory, the
 * edited file and the transcript location.
 */
function postToolUseBodyWithPaths(sessionId: string) {
  return JSON.stringify({
    hook_event_name: "PostToolUse",
    session_id: sessionId,
    transcript_path: `/Users/${ACCOUNT_NAME}/.claude/projects/${CLIENT_DIRECTORY}/${TRANSCRIPT_FILE}`,
    cwd: WORKING_DIRECTORY,
    tool_name: "Edit",
    tool_input: {
      file_path: `${WORKING_DIRECTORY}/${EDITED_FILE}`,
      old_string: "the old terms",
      new_string: "the new terms",
    },
    prompt: `${PROMPT_CANARY} rewrite the terms`,
  });
}

/**
 * Review r2, F2 — the spool holds no more than the ledger would.
 *
 * The rule is now the collector's own pre-write rule, so the paths the ledger
 * strips are gone from the file too. The exception is exact and small:
 * `SPOOL_DERIVATION_INPUT_KEYS`, the keys the collector reads from the raw body
 * BEFORE suppressing them, whose values it turns into something it persists.
 */
async function caseTheSpoolHoldsNoMoreThanTheLedgerWould() {
  const { home } = fixtureHome("r");
  const collector = await startCollector(home);
  const session = "22223333-4444-4555-8666-777788889991";
  try {
    const outcome = await liveThenSpool(collector, postToolUseBodyWithPaths(session), "claude_code");
    const bytes = outcome.file ? fs.readFileSync(outcome.file.path, "utf8") : "";
    const spooledBody = typeof outcome.envelope?.body === "string"
      ? (JSON.parse(outcome.envelope.body) as Record<string, unknown>)
      : null;
    // Everything the paths disclose, and where it is allowed to appear: the
    // account name is in `cwd`, which is allowlisted, and nowhere else.
    const withoutAllowlisted = bytes.split(JSON.stringify(WORKING_DIRECTORY).slice(1, -1)).join("");
    const leaked = [ACCOUNT_NAME, CLIENT_DIRECTORY, EDITED_FILE, TRANSCRIPT_FILE, PROMPT_CANARY]
      .filter((needle) => withoutAllowlisted.includes(needle));
    check(
      "r_the_spool_file_holds_only_the_allowlisted_path_value",
      Boolean(outcome.spooledOutcome && typeof outcome.spooledOutcome === "object" &&
        "spooled" in outcome.spooledOutcome) &&
        spooledBody !== null &&
        // Blanked: the transcript path, the tool input (with the file path in
        // it) and the prompt. Kept: cwd, a derivation input.
        spooledBody.transcript_path === "" &&
        spooledBody.tool_input === "" &&
        spooledBody.prompt === "" &&
        spooledBody.cwd === WORKING_DIRECTORY &&
        spooledBody.tool_name === "Edit" &&
        leaked.length === 0,
      {
        blanked: outcome.envelope?.blanked,
        keptCwd: spooledBody?.cwd,
        blankedValues: {
          transcript_path: spooledBody?.transcript_path,
          tool_input: spooledBody?.tool_input,
          prompt: spooledBody?.prompt,
        },
        leakedOutsideTheAllowlistedValue: leaked,
      },
    );

    await new Promise<void>((resolve) => setTimeout(resolve, 1_000));
    const tick = await collector.drain.tick();
    const rows = sessionRowsOf(collector.buffer, session);
    const liveRow = rows[0] ? comparableRow(rows[0]) : {};
    const recoveredRow = rows[1] ? comparableRow(rows[1]) : {};
    const differing = Object.keys(liveRow).filter(
      (column) => JSON.stringify(liveRow[column]) !== JSON.stringify(recoveredRow[column]),
    );
    const payload = payloadDifference(liveRow.payload_json, recoveredRow.payload_json);
    check(
      "r_the_widened_blanking_leaves_the_ledger_row_unchanged",
      tick.recovered === 1 &&
        rows.length === 2 &&
        differing.length === 2 &&
        differing.includes("observed_at") &&
        differing.includes("payload_json") &&
        payload.identicalApartFromObservedAt,
      { tick, rows: rows.length, differing, payload },
    );
    check(
      "r_the_suppression_receipts_are_identical_live_and_recovered",
      typeof liveRow.suppressed_fields_json === "string" &&
        liveRow.suppressed_fields_json === recoveredRow.suppressed_fields_json,
      {
        live: liveRow.suppressed_fields_json,
        recovered: recoveredRow.suppressed_fields_json,
      },
    );
    const ledgerBytes = fs.readFileSync(collector.ledgerPath, "utf8");
    check(
      "r_the_ledger_never_held_the_paths_either",
      [ACCOUNT_NAME, EDITED_FILE, TRANSCRIPT_FILE, PROMPT_CANARY].every(
        (needle) => !ledgerBytes.includes(needle),
      ),
      {
        inLedger: [ACCOUNT_NAME, EDITED_FILE, TRANSCRIPT_FILE, PROMPT_CANARY].filter((needle) =>
          ledgerBytes.includes(needle),
        ),
      },
    );

    // Review r3, N3 — the other bucket, end to end: a body carrying protected
    // identity names at three shapes. The spool keeps those values RAW, on
    // purpose, and this is what that buys: the recovered row carries the hash
    // of the real identity, exactly as the live row does.
    const identitySession = "33334444-5555-4666-8777-888899990001";
    const identityBody = JSON.stringify({
      hook_event_name: "UserPromptSubmit",
      session_id: identitySession,
      username: IDENTITY_CANARY,
      "user.id": IDENTITY_CANARY,
      account_id: IDENTITY_CANARY,
      resource: { attributes: [{ key: "user.id", value: { stringValue: IDENTITY_CANARY } }] },
    });
    const identityOutcome = await liveThenSpool(collector, identityBody, "claude_code");
    const identityFileBytes = identityOutcome.file
      ? fs.readFileSync(identityOutcome.file.path, "utf8")
      : "";
    await new Promise<void>((resolve) => setTimeout(resolve, 1_000));
    const identityTick = await collector.drain.tick();
    const identityRows = sessionRowsOf(collector.buffer, identitySession);
    const identityLive = identityRows[0] ? comparableRow(identityRows[0]) : {};
    const identityRecovered = identityRows[1] ? comparableRow(identityRows[1]) : {};
    const identityDiffering = Object.keys(identityLive).filter(
      (column) => JSON.stringify(identityLive[column]) !== JSON.stringify(identityRecovered[column]),
    );
    const identityPayload = payloadDifference(identityLive.payload_json, identityRecovered.payload_json);
    const identityHash = hashProtectedValue(IDENTITY_CANARY);
    const emptyHash = hashProtectedValue("");
    const identityLedgerBytes = fs.readFileSync(collector.ledgerPath, "utf8");
    check(
      "r_a_declared_protected_identity_is_raw_in_the_spool_and_hashed_identically_in_both_rows",
      identityTick.recovered === 1 &&
        identityRows.length === 2 &&
        identityDiffering.length === 2 &&
        identityDiffering.includes("observed_at") &&
        identityDiffering.includes("payload_json") &&
        identityPayload.identicalApartFromObservedAt &&
        identityLive.suppressed_fields_json === identityRecovered.suppressed_fields_json &&
        // The declared exemption, visible: the value IS in the spool file...
        identityFileBytes.includes(IDENTITY_CANARY) &&
        // ...and never in the ledger, which holds only the hash OF it — the
        // hash of the real identity, not the hash of an emptied value.
        !identityLedgerBytes.includes(IDENTITY_CANARY) &&
        identityLedgerBytes.includes(identityHash) &&
        !identityLedgerBytes.includes(emptyHash),
      {
        tick: identityTick,
        differing: identityDiffering,
        rawValueInSpoolFile: identityFileBytes.includes(IDENTITY_CANARY),
        canaryInLedger: identityLedgerBytes.includes(IDENTITY_CANARY),
        hashOfTheIdentityInLedger: identityLedgerBytes.includes(identityHash),
        hashOfAnEmptyValueInLedger: identityLedgerBytes.includes(emptyHash),
        identityHash,
        emptyHash,
        receipts: identityLive.suppressed_fields_json,
      },
    );
  } finally {
    await collector.close();
  }

  // The allowlist itself, both directions.
  const blanked = blankForbiddenRawContent(
    JSON.stringify({ cwd: WORKING_DIRECTORY, workdir: WORKING_DIRECTORY, transcript_path: "/x/y", file_path: "/x/z" }),
  );
  const blankedBody = blanked ? (JSON.parse(blanked.text) as Record<string, unknown>) : null;
  check(
    "r_every_derivation_input_is_a_key_the_collector_would_otherwise_strip",
    SPOOL_DERIVATION_INPUT_KEYS.length > 0 &&
      SPOOL_DERIVATION_INPUT_KEYS.every((key) => isSensitiveMetadataSemanticKey(key)),
    {
      keys: [...SPOOL_DERIVATION_INPUT_KEYS],
      nonSensitive: SPOOL_DERIVATION_INPUT_KEYS.filter((key) => !isSensitiveMetadataSemanticKey(key)),
    },
  );
  check(
    "r_the_repo_context_reader_really_reads_every_allowlisted_cwd_key",
    ["cwd", "current_working_directory", "workdir", "working_directory"].every(
      (key) =>
        (SPOOL_DERIVATION_INPUT_KEYS as readonly string[]).includes(key) &&
        extractRepoContextCwd({ [key]: WORKING_DIRECTORY }) === WORKING_DIRECTORY &&
        // ...and that blanking it would have cost the event its repo linkage.
        extractRepoContextCwd({ [key]: "" }) === undefined,
    ),
    {
      read: ["cwd", "current_working_directory", "workdir", "working_directory"].map((key) => ({
        key,
        extracted: extractRepoContextCwd({ [key]: WORKING_DIRECTORY }),
      })),
    },
  );
  check(
    "r_a_path_key_outside_the_allowlist_is_blanked_and_an_allowlisted_one_is_not",
    blankedBody?.cwd === WORKING_DIRECTORY &&
      blankedBody?.workdir === WORKING_DIRECTORY &&
      blankedBody?.transcript_path === "" &&
      blankedBody?.file_path === "" &&
      blanked?.blanked === 2,
    { blanked: blanked?.blanked, body: blankedBody },
  );

  // Review r3, N3 — completeness, in the only direction that matters: no
  // protected name may keep its raw value in a spool file WITHOUT being
  // declared. Run over the shared list itself, so a name added to
  // `protectedMetadataFieldNames` later fails here until it is blanked or
  // disclosed, instead of resting undeclared on disk.
  const declared = new Set(SPOOL_DERIVATION_INPUT_DISCLOSURE.map((entry) => entry.key));
  const protectedOutcomes = protectedMetadataFieldNames.map((name) => {
    const result = blankForbiddenRawContent(JSON.stringify({ [name]: IDENTITY_CANARY }));
    const kept = result ? (JSON.parse(result.text) as Record<string, unknown>)[name] : undefined;
    return { name, blanked: kept === "", declared: declared.has(name) };
  });
  const undeclaredRaw = protectedOutcomes.filter(
    (outcome) => !outcome.blanked && !outcome.declared,
  );
  check(
    "r_every_protected_identity_name_is_blanked_or_declared",
    protectedOutcomes.length === protectedMetadataFieldNames.length && undeclaredRaw.length === 0,
    {
      protectedNames: protectedOutcomes.length,
      blanked: protectedOutcomes.filter((outcome) => outcome.blanked).length,
      declared: protectedOutcomes.filter((outcome) => !outcome.blanked && outcome.declared).length,
      undeclaredRaw: undeclaredRaw.map((outcome) => outcome.name),
    },
  );

  // ...and the reason each declared one is exempt, measured rather than
  // asserted: the ledger keeps the hash OF the value, so an emptied value
  // would persist the hash of `""` instead of the hash of the identity.
  const hashOfEmpty = hashProtectedValue("");
  const protectedTradeoff = SPOOL_PROTECTED_IDENTITY_KEYS.map((name) => {
    const real = sanitizeForPolicy({ [name]: IDENTITY_CANARY }, DEFAULT_POLICY).value as
      Record<string, unknown>;
    const emptied = sanitizeForPolicy({ [name]: "" }, DEFAULT_POLICY).value as
      Record<string, unknown>;
    return {
      name,
      ledgerKeeps: real[name],
      ledgerWouldKeepIfBlanked: emptied[name],
      reason: SPOOL_DERIVATION_INPUT_DISCLOSURE.find((entry) => entry.key === name)?.reason,
    };
  });
  check(
    "r_blanking_a_declared_protected_identity_would_change_what_the_ledger_persists",
    SPOOL_PROTECTED_IDENTITY_KEYS.length > 0 &&
      protectedTradeoff.every(
        (row) =>
          row.ledgerKeeps === hashProtectedValue(IDENTITY_CANARY) &&
          row.ledgerWouldKeepIfBlanked === hashOfEmpty &&
          row.reason === "the ledger stores the protected hash of this value",
      ),
    {
      keys: [...SPOOL_PROTECTED_IDENTITY_KEYS],
      hashOfTheIdentity: hashProtectedValue(IDENTITY_CANARY),
      hashOfAnEmptyValue: hashOfEmpty,
      sample: protectedTradeoff[0],
    },
  );

  // The disclosure as the privacy spec renders it, printed as a deliverable.
  console.log(
    JSON.stringify({
      proof: "hook_spool",
      table: "spool_derivation_input_disclosure",
      rows: SPOOL_DERIVATION_INPUT_DISCLOSURE,
    }),
  );
}

/**
 * Review r3, N2 — presence is not usability.
 *
 * The nine bodies the reviewer measured, run as one case. r3 asked only
 * whether a time alias KEY was present, so seven of these nine opted
 * themselves out of the hook-time fix and landed stamped with the recovery
 * clock — the r2 defect, reached through a body the drain decided (wrongly)
 * already carried its own time. The rule is now the normalizer's own
 * acceptance test, so every body whose time the normalizer cannot use gets the
 * hook's stamp, and the one body carrying a usable string keeps its own.
 */
const UNUSABLE_TIME_ROWS = [
  { slug: "no_time_at_all", label: "no time at all", field: {} as Record<string, unknown> },
  {
    slug: "a_numeric_epoch_timestamp",
    label: "timestamp: 1757000000000 (numeric epoch)",
    field: { timestamp: 1_757_000_000_000 },
  },
  { slug: "an_empty_timestamp", label: 'timestamp: ""', field: { timestamp: "" } },
  { slug: "a_null_timestamp", label: "timestamp: null", field: { timestamp: null } },
  { slug: "a_boolean_time", label: "time: true", field: { time: true } },
  {
    slug: "an_object_observed_at",
    label: "observed_at: { iso: … }",
    field: { observed_at: { iso: "2026-09-01T00:00:00.000Z" } },
  },
  {
    slug: "an_unparseable_time_unix_nano",
    label: 'timeUnixNano: "not-a-number"',
    field: { timeUnixNano: "not-a-number" },
  },
] as const;

/** The one row that must NOT move: a string the normalizer accepts. */
const USABLE_BODY_TIME = "2026-09-01T00:00:00.000Z";

async function caseAnUnusableBodyTimeStillGetsTheHooksTime() {
  const { home } = fixtureHome("u");
  const collector = await startCollector(home);
  /** Longer than the 5 s drain interval, so a drain clock cannot pass for a hook clock. */
  const SPOOL_LATENCY_MS = 6_000;
  const table: Record<string, unknown>[] = [];
  const futureUnixNano = String((Date.now() + 3 * 24 * 60 * 60 * 1_000) * 1_000_000);
  const rows = [
    ...UNUSABLE_TIME_ROWS.map((row) => ({ ...row, field: { ...row.field }, expect: "hook" as const })),
    {
      slug: "a_future_time_unix_nano",
      label: "timeUnixNano: <3 days ahead>",
      field: { timeUnixNano: futureUnixNano } as Record<string, unknown>,
      expect: "hook" as const,
    },
    {
      slug: "a_usable_string_timestamp",
      label: `timestamp: "${USABLE_BODY_TIME}"`,
      field: { timestamp: USABLE_BODY_TIME } as Record<string, unknown>,
      expect: "body" as const,
    },
  ];
  try {
    const spooled: Array<{ row: (typeof rows)[number]; session: string; receivedAt: string }> = [];
    for (const [index, row] of rows.entries()) {
      const session = `aaaabbbb-cccc-4ddd-8eee-ffff0000000${index.toString(16)}`;
      const body = JSON.stringify({
        hook_event_name: "UserPromptSubmit",
        session_id: session,
        model: "claude-code-fixture",
        ...row.field,
      });
      const outcome = await liveThenSpool(collector, body, "claude_code");
      spooled.push({ row, session, receivedAt: String(outcome.envelope?.receivedAt ?? "") });
    }

    // The wait is the point: a recovery clock is now 6 s from the hook's.
    await new Promise<void>((resolve) => setTimeout(resolve, SPOOL_LATENCY_MS));
    const drainedAt = Date.now();
    const tick = await collector.drain.tick();

    for (const { row, session, receivedAt } of spooled) {
      const ledgerRows = sessionRowsOf(collector.buffer, session);
      const live = ledgerRows[0] ? comparableRow(ledgerRows[0]) : {};
      const recovered = ledgerRows[1] ? comparableRow(ledgerRows[1]) : {};
      const recoveredObservedAt = String(recovered.observed_at ?? "");
      const stampSkewMs = Math.abs(Date.parse(recoveredObservedAt) - Date.parse(receivedAt));
      const drainSkewMs = Math.abs(drainedAt - Date.parse(recoveredObservedAt));
      const landedWith = row.expect === "body"
        ? recoveredObservedAt === USABLE_BODY_TIME
        : stampSkewMs <= 1 && drainSkewMs >= SPOOL_LATENCY_MS;
      const tableRow = {
        body: row.label,
        expected: row.expect === "body" ? "the body's own time" : "the hook's time",
        liveObservedAt: live.observed_at,
        recoveredObservedAt,
        envelopeReceivedAt: receivedAt,
        stampSkewFromEnvelopeMs: stampSkewMs,
        distanceFromDrainClockMs: drainSkewMs,
        landedWithTheExpectedTime: landedWith,
      };
      table.push(tableRow);
      check(`u_${row.slug}_lands_with_${row.expect === "body" ? "its_own_time" : "the_hooks_time"}`,
        ledgerRows.length === 2 && landedWith,
        tableRow,
      );
    }
    check(
      "u_the_drain_recovered_every_one_of_them",
      tick.recovered === rows.length && tick.rejected === 0,
      { tick, bodies: rows.length },
    );
  } finally {
    console.log(
      JSON.stringify({
        proof: "hook_spool",
        table: "unusable_body_time_falls_back_to_the_hooks_stamp",
        rows: table,
      }),
    );
    await collector.close();
  }
}

/**
 * Review r3, N5 — a working directory reported only inside `args`.
 *
 * `extractRepoContextCwd` also reads a JSON string under `args`/`arguments`/
 * `tool_arguments`, but those keys are raw command content and are blanked
 * before the write. So a hook source that reports its working directory ONLY
 * there gets repository attribution live and none on replay. That is a
 * deliberate trade — keeping raw argument strings off disk is worth more than
 * the linkage — and this case exists so it is a CAUGHT, printed divergence
 * rather than a gap no fixture happens to cover.
 */
async function caseAnArgsNestedCwdIsADocumentedDivergence() {
  const { home } = fixtureHome("v");
  const collector = await startCollector(home);
  const session = "44445555-6666-4777-8888-999900001112";
  try {
    const body = JSON.stringify({
      hook_event_name: "PreToolUse",
      session_id: session,
      tool_name: "Bash",
      // The exact shape `extractRepoContextCwd` reads: a JSON string.
      args: JSON.stringify({ cwd: WORKING_DIRECTORY, command: "pnpm test" }),
    });
    const outcome = await liveThenSpool(collector, body, "claude_code");
    const liveCwd = extractRepoContextCwd(JSON.parse(body) as Record<string, unknown>);
    const spooledBody = typeof outcome.envelope?.body === "string"
      ? (JSON.parse(outcome.envelope.body) as Record<string, unknown>)
      : null;
    const recoveredCwd = spooledBody ? extractRepoContextCwd(spooledBody) : undefined;
    const divergence = {
      knownGap: "a working directory reported only inside args/arguments/tool_arguments",
      liveCwd,
      recoveredCwd: recoveredCwd ?? null,
      argsInTheSpoolFile: spooledBody?.args,
      consequence: "the recovered event gets no repo/branch/head linkage; the event itself is not lost",
      whyNotFixed: "args is raw command content; allowlisting it would put command strings on disk",
    };
    check(
      "v_a_cwd_only_inside_args_is_lost_to_the_spool_and_this_is_the_declared_trade",
      liveCwd === WORKING_DIRECTORY && recoveredCwd === undefined && spooledBody?.args === "",
      divergence,
    );

    await new Promise<void>((resolve) => setTimeout(resolve, 1_000));
    const tick = await collector.drain.tick();
    const ledgerRows = sessionRowsOf(collector.buffer, session);
    check(
      "v_the_event_itself_is_still_recovered_it_is_the_linkage_that_is_lost",
      tick.recovered === 1 && ledgerRows.length === 2,
      { tick, rows: ledgerRows.length },
    );
    console.log(
      JSON.stringify({ proof: "hook_spool", divergence: "args_nested_cwd", ...divergence }),
    );
  } finally {
    await collector.close();
  }
}

/**
 * A /status listener shaped like the released 0.7.21 collector: healthy,
 * answering, and with no `hookSpool` section at all.
 */
async function startLegacyStatusListener() {
  const server = http.createServer((request, response) => {
    request.resume();
    response.writeHead(200, { "content-type": "application/json" });
    response.end(
      JSON.stringify({
        ok: true,
        appVersion: "0.7.21",
        port: (server.address() as AddressInfo).port,
        dataMode: "metadata",
        privacyMode: "metadata_only",
        stats: { tokenAttributedEvents: 0 },
      }),
    );
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    port: (server.address() as AddressInfo).port,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

/**
 * Review r2, F3 — a reachable collector that predates the drain says so.
 *
 * This is the mixed-version rollout window: the new hook client spools, the old
 * daemon cannot drain, and the operator asking "is the new collector up?" was
 * told "collector_unreachable" about a collector that was answering fine.
 */
async function caseACollectorTooOldToDrainSaysSo() {
  const { root, home } = fixtureHome("s");
  const listener = await startLegacyStatusListener();
  saveCollectorConfig(collectorConfigSchema.parse({ port: listener.port }));
  writeHookSpoolFile({
    home,
    source: "claude_code",
    body: claudeHookBody("33334444-5555-4666-8777-888899990001", "mixed version window"),
  });
  try {
    const statusCli = await runCli(home, root, ["status"]);
    const statusJson = parseJsonStdout(statusCli.stdout);
    const statusSpool = statusJson?.hookSpool as Record<string, unknown> | undefined;
    check(
      "s_status_names_a_collector_that_predates_the_spool",
      statusCli.status === 0 &&
        hasOperatorHookSpoolFields(statusSpool) &&
        statusSpool?.enabled === null &&
        statusSpool?.enabledSource === "collector_too_old" &&
        statusSpool?.pendingFiles === 1,
      { status: statusCli.status, hookSpool: statusSpool },
    );

    const doctorCli = await runCli(home, root, ["doctor", "--read-only", "--json"]);
    const doctorJson = parseJsonStdout(doctorCli.stdout);
    const doctorSpool = doctorJson?.hookSpool as Record<string, unknown> | undefined;
    check(
      "s_doctor_names_it_too_and_says_what_happens_next",
      hasOperatorHookSpoolFields(doctorSpool) &&
        doctorSpool?.enabled === null &&
        doctorSpool?.enabledSource === "collector_too_old" &&
        doctorSpool?.draining === false &&
        typeof doctorSpool?.note === "string" &&
        String(doctorSpool?.note).includes("0.7.22") &&
        String(doctorSpool?.note).includes("updated"),
      { hookSpool: doctorSpool },
    );
  } finally {
    await listener.close();
  }

  // The unreachable case is still its own answer, not this one.
  const unreachable = fixtureHome("s2");
  saveCollectorConfig(collectorConfigSchema.parse({ port: await freePort() }));
  const statusCli = await runCli(unreachable.home, unreachable.root, ["status"]);
  const statusSpool = parseJsonStdout(statusCli.stdout)?.hookSpool as Record<string, unknown> | undefined;
  check(
    "s_an_unreachable_collector_is_still_reported_as_unreachable",
    statusCli.status === 0 &&
      statusSpool?.enabled === null &&
      statusSpool?.enabledSource === "collector_unreachable",
    { status: statusCli.status, hookSpool: statusSpool },
  );
}

/**
 * Review r2, F5 — `draining` is a claim about the collector, so it is false
 * whenever the collector is not draining: the kill switch is off, or nobody
 * could be asked. The age diagnostic is unchanged and still fires on its own.
 */
function caseDrainingIsFalseWhenNothingCanDrain() {
  const { home } = fixtureHome("t");
  writeHookSpoolFile({
    home,
    source: "claude_code",
    body: claudeHookBody("44445555-6666-4777-8888-999900001111", "fresh pending file"),
  });
  const off = hookSpoolDoctorSection(home, hookSpoolDaemonEnabled(false));
  check(
    "t_a_fresh_pending_file_is_not_draining_when_the_kill_switch_is_off",
    off.pendingFiles === 1 &&
      off.enabled === false &&
      off.enabledSource === "collector" &&
      off.draining === false &&
      !("diagnostic" in off),
    off,
  );
  const unreachable = hookSpoolDoctorSection(home, HOOK_SPOOL_COLLECTOR_UNREACHABLE);
  const tooOld = hookSpoolDoctorSection(home, HOOK_SPOOL_COLLECTOR_TOO_OLD);
  check(
    "t_a_daemon_that_did_not_answer_is_not_draining_either",
    unreachable.draining === false &&
      unreachable.enabled === null &&
      tooOld.draining === false &&
      tooOld.enabled === null,
    { unreachable, tooOld },
  );
  // ...and the stall diagnostic still fires on age alone, kill switch or not.
  const { home: staleHome } = fixtureHome("t2");
  writeHookSpoolFile({
    home: staleHome,
    source: "claude_code",
    body: claudeHookBody("55556666-7777-4888-8999-000011112222", "stale pending file"),
    nowMs: Date.now() - 11 * 60_000,
  });
  const stalledAndOff = hookSpoolDoctorSection(staleHome, hookSpoolDaemonEnabled(false));
  check(
    "t_the_stalled_spool_diagnostic_still_fires_on_age",
    stalledAndOff.diagnostic === "hook_spool_pending_not_draining" &&
      stalledAndOff.draining === false &&
      typeof stalledAndOff.note === "string" &&
      stalledAndOff.note.includes("not draining"),
    stalledAndOff,
  );
}


async function main() {
  // Stage markers on stderr: a hosted-runner hang has to name the case it hung
  // in without waiting for the final report.
  const stage = (name: string) => console.error(JSON.stringify({ proof: "hook_spool", stage: name }));
  try {
    stage("a_locked_ledger");
    await caseLockedLedgerRecovers();
    stage("b_refused_connection");
    await caseRefusedConnectionRecovers();
    stage("c_contract_rejection");
    await caseContractRejectionStillSurfaces();
    stage("d_invalid_body");
    await caseInvalidBodyIsQuarantined();
    stage("e_untrusted_files");
    await caseUntrustedFilesAreQuarantined();
    stage("f_directory_bound");
    await caseDirectoryBoundKeepsLossVisible();
    stage("g_kill_switch");
    await caseKillSwitchRestoresTodaysBehaviour();
    stage("i_doctor_stalled_spool");
    caseDoctorNamesAStalledSpool();
    stage("k_trigger_set");
    await caseTriggerSetIsExactlyTheOutcomesThatStoredNothing();
    stage("k_408_recovery");
    await case408SpoolsAndRecovers();
    stage("k_real_408");
    await caseTheRealCollectorAnswers408WhileReadingTheBody();
    stage("l_quarantine_retention");
    await caseQuarantineRetentionRunsWithoutANewRejection();
    stage("m_orphan_temporaries");
    await caseOrphanTemporariesAreReapedAndCounted();
    stage("n_daemon_kill_switch");
    await caseOperatorSurfacesReportTheDaemonsKillSwitch();
    stage("o_receipt_safety");
    await caseWarningsAndHomeErrorsStayReceiptSafe();
    stage("p_privacy_blanking");
    await casePrivacyBlankingKeepsTheLedgerIdentical();
    stage("q_hook_time_parity");
    await caseRecoveredEventsKeepTheHooksTime();
    stage("r_spool_holds_no_more_than_the_ledger");
    await caseTheSpoolHoldsNoMoreThanTheLedgerWould();
    stage("u_unusable_body_time");
    await caseAnUnusableBodyTimeStillGetsTheHooksTime();
    stage("v_args_nested_cwd");
    await caseAnArgsNestedCwdIsADocumentedDivergence();
    stage("s_collector_too_old");
    await caseACollectorTooOldToDrainSaysSo();
    stage("t_draining_needs_a_drain");
    caseDrainingIsFalseWhenNothingCanDrain();
    stage("report");
  } finally {
    for (const [key, value] of previousEnv) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
  }

  for (const result of checks) {
    console.log(`${result.passed ? "PASS" : "FAIL"} ${result.name} ${JSON.stringify(result.detail)}`);
  }
  const failed = checks.filter((result) => !result.passed);
  console.log(
    JSON.stringify({
      proof: "hook_spool",
      checks: checks.length,
      passed: checks.length - failed.length,
      failed: failed.length,
      limits: HOOK_SPOOL_LIMITS,
    }),
  );
  if (failed.length > 0) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
