/**
 * Provider capacity adapters (issue #169, child of #167/#168).
 *
 * Collects PROVIDER-REPORTED capacity locally without copying credentials,
 * making ad-hoc network calls, or starting inference. Two P0 adapters:
 *
 * - `claude_status_line`: a status-line PROXY. Claude Code feeds session JSON
 *   to whatever `statusLine.command` is configured in ~/.claude/settings.json.
 *   When Plimsoll owns that entry, the proxy snapshots ONLY the documented
 *   `rate_limits` windows (`five_hour`, `seven_day`, `seven_day_sonnet`) and
 *   the top-level `version` field through a bounded allowlist parser, then —
 *   if an operator status line was chained in — forwards the RAW stdin bytes
 *   to the prior command in memory and reproduces its stdout, stderr, exit
 *   status, and timeout behavior exactly. Absent quota windows stay absent:
 *   they are never defaulted, backfilled, or emitted as zero.
 * - `codex_app_server`: a short-lived stdio probe against the stable
 *   `codex app-server` JSON-RPC surface. Outbound traffic is EXACTLY three
 *   messages: `initialize` (request), `initialized` (notification),
 *   `account/rateLimits/read` (request). No prompts, threads, turns, browser
 *   state, or experimental APIs exist in this module. Concurrency is one per
 *   provider profile: a second overlapping probe returns a symbolic `busy`
 *   receipt instead of spawning. Failures arm an exponential backoff state;
 *   probes inside the window return `backoff_window_active` without spawning.
 *
 * Receipt doctrine (issue #169): every receipt carries ONLY the symbolic
 * adapter error code, phase, freshness, timing, adapter version, the
 * allowlisted capacity facts, and bounded resource-cost counters. Raw provider
 * JSON, file paths, free-text messages, and credentials are structurally
 * unrepresentable: the parsers copy validated scalars out of named slots and
 * drop everything else on the floor, so hostile payloads cannot echo into
 * receipts even by accident.
 *
 * Cadence: manual refresh only. Nothing here schedules, retries on its own,
 * or runs in the background.
 *
 * Profile scope: the DEFAULT provider profiles only. Alternate
 * CLAUDE_CONFIG_DIR / CODEX_HOME profiles belong to issue #172 and are
 * deliberately unreachable from this module.
 */

import {
  spawn,
  type ChildProcessWithoutNullStreams,
} from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

import {
  applyClaudeSettings,
  ClaudeConfigError,
} from "../../collector-config/src/index";

import { PLIMSOLL_VERSION } from "./version";
import { resolveCollectorHome } from "./collector-home";

// ---------------------------------------------------------------------------
// Contract constants
// ---------------------------------------------------------------------------

export const PROVIDER_CAPACITY_ADAPTER_RECEIPT_SCHEMA =
  "plimsoll.provider-capacity-adapter-receipt.v1" as const;

/** Bounded identifier: letters, digits, dots, underscores, colons, hyphens. */
const ADAPTER_IDENTIFIER_PATTERN =
  /^[a-zA-Z0-9](?:[a-zA-Z0-9._:-]{0,94}[a-zA-Z0-9])?$/;

/** Secret-shaped prefixes refused anywhere an identifier slot is filled. */
const SECRET_SHAPED_PREFIX_PATTERN =
  /^(?:eyJ|sk[_-]|sk(?:live|test)|ghp|gho|ghu|ghs|ghr|github_pat|xox|glpat|shp(?:at|pa|ca)|npm_|aiza|akia|asia|bearer|basic|authorization|cookie)/i;

export const ADAPTER_DEFAULT_PROFILE_ID = "default" as const;

export type CapacityAdapterId = "claude_status_line" | "codex_app_server";

export const CAPACITY_ADAPTER_IDS: readonly CapacityAdapterId[] = [
  "claude_status_line",
  "codex_app_server",
];

/** Closed set of symbolic adapter error codes. No free text ever. */
export const CAPACITY_ADAPTER_ERRORS = [
  "none",
  "busy",
  "backoff_window_active",
  "blocked_existing_statusline",
  "blocked_invalid_settings",
  "input_bound_exceeded",
  "output_bound_exceeded",
  "parse_failed",
  "protocol_violation",
  "provider_error",
  "spawn_failed",
  "timeout",
  "io_error",
] as const;
export type CapacityAdapterError = (typeof CAPACITY_ADAPTER_ERRORS)[number];

/** Closed set of lifecycle phases a receipt can name. */
export const CAPACITY_ADAPTER_PHASES = [
  "not_started",
  "capture",
  "chain",
  "spawn",
  "initialize",
  "rate_limits_read",
  "shutdown",
  "complete",
] as const;
export type CapacityAdapterPhase = (typeof CAPACITY_ADAPTER_PHASES)[number];

export type CapacityAdapterFreshnessStatus = "fresh" | "STALE" | "UNKNOWN";

/** One allowlisted quota-window fact copied out of a provider payload. */
export type CapacityWindowFact = {
  /** Validated bounded identifier; never echoes attacker-controlled text. */
  window: string;
  /** Provider-reported utilization, finite, clamped-checked 0..100. */
  usedPercent: number;
  /** Quota window length in minutes when the provider reports it. */
  windowMinutes: number | null;
  /** Reset instant normalized to ISO-8601 UTC; null when absent/unparsable. */
  resetsAt: string | null;
};

export type ProviderCapacityAdapterReceipt = {
  schema: typeof PROVIDER_CAPACITY_ADAPTER_RECEIPT_SCHEMA;
  adapter: CapacityAdapterId;
  adapterVersion: string;
  profileId: string;
  ok: boolean;
  phase: CapacityAdapterPhase;
  error: { code: CapacityAdapterError } | null;
  observedAt: string;
  freshness: {
    status: CapacityAdapterFreshnessStatus;
    ageMs: number | null;
    maxAgeMs: number;
  };
  /**
   * Allowlisted capacity facts only. Missing quota windows stay MISSING:
   * an empty array means nothing was reported, never "zero usage".
   */
  windows: CapacityWindowFact[];
  /** Provider-reported version string when it passes validation, else null. */
  providerVersion: string | null;
  timing: {
    startedAt: string;
    completedAt: string;
    elapsedMs: number;
  };
  resourceCost: {
    spawnCount: number;
    bytesWritten: number;
    bytesRead: number;
    exitCode: number | null;
    signal: string | null;
  };
};

// ---------------------------------------------------------------------------
// Shared pure helpers
// ---------------------------------------------------------------------------

function isValidAdapterIdentifier(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= 96 &&
    ADAPTER_IDENTIFIER_PATTERN.test(value) &&
    !SECRET_SHAPED_PREFIX_PATTERN.test(value)
  );
}

function requireIsoTimestamp(value: unknown): string | null {
  if (typeof value !== "string" || value.length === 0 || value.length > 64) {
    return null;
  }
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) return null;
  return new Date(ms).toISOString();
}

/**
 * Accepts a documented reset instant: an ISO-8601 timestamp string or Unix
 * epoch SECONDS (the Codex wire format). Epoch milliseconds are deliberately
 * NOT guessed: a number that large fails closed to null rather than being
 * silently reinterpreted.
 */
function normalizeResetInstant(value: unknown): string | null {
  if (typeof value === "string") return requireIsoTimestamp(value);
  if (
    typeof value === "number" &&
    Number.isFinite(value) &&
    Number.isInteger(value) &&
    value >= 0 &&
    value < 1e11
  ) {
    return new Date(value * 1000).toISOString();
  }
  return null;
}

/**
 * Local freshness classification with the same fail-closed semantics as the
 * capacity snapshot doctrine (#168/#195): stale past maxAgeMs, and a
 * FUTURE-dated observation is UNKNOWN — never fresh, never clamped to zero.
 * (Deliberately local: capacity modules cannot import each other under the
 * dependency-reachability gate.)
 */
export function classifyAdapterObservationFreshness(input: {
  observedAt: string;
  now: string;
  maxAgeMs: number;
}): { status: CapacityAdapterFreshnessStatus; ageMs: number | null } {
  const nowMs = Date.parse(input.now);
  const observedMs = Date.parse(input.observedAt);
  if (
    !Number.isFinite(nowMs) ||
    !Number.isFinite(observedMs) ||
    !Number.isFinite(input.maxAgeMs) ||
    input.maxAgeMs <= 0
  ) {
    return { status: "UNKNOWN", ageMs: null };
  }
  if (observedMs > nowMs) return { status: "UNKNOWN", ageMs: null };
  const ageMs = nowMs - observedMs;
  return { status: ageMs <= input.maxAgeMs ? "fresh" : "STALE", ageMs };
}

/** Validate a finite percentage in 0..100 (booleans are not numbers here). */
function isUtilizationPercent(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isFinite(value) &&
    value >= 0 &&
    value <= 100
  );
}

function buildReceipt(input: {
  adapter: CapacityAdapterId;
  profileId: string;
  ok: boolean;
  phase: CapacityAdapterPhase;
  errorCode: CapacityAdapterError;
  startedAt: Date;
  windows: CapacityWindowFact[];
  providerVersion: string | null;
  resourceCost: ProviderCapacityAdapterReceipt["resourceCost"];
  maxAgeMs: number;
  now?: Date;
}): ProviderCapacityAdapterReceipt {
  const completedAt = nowOrNow(input.now);
  const observedAt = completedAt.toISOString();
  const startedIso = input.startedAt.toISOString();
  const freshness = classifyAdapterObservationFreshness({
    observedAt,
    now: observedAt,
    maxAgeMs: input.maxAgeMs,
  });
  return {
    schema: PROVIDER_CAPACITY_ADAPTER_RECEIPT_SCHEMA,
    adapter: input.adapter,
    adapterVersion: ADAPTER_VERSION,
    profileId: input.profileId,
    ok: input.ok,
    phase: input.phase,
    error: input.ok ? null : { code: input.errorCode },
    observedAt,
    freshness: { ...freshness, maxAgeMs: input.maxAgeMs },
    windows: input.windows,
    providerVersion: input.providerVersion,
    timing: {
      startedAt: startedIso,
      completedAt: observedAt,
      elapsedMs: Math.max(0, completedAt.getTime() - input.startedAt.getTime()),
    },
    resourceCost: input.resourceCost,
  };
}

function nowOrNow(now: Date | undefined): Date {
  return now instanceof Date && !Number.isNaN(now.getTime()) ? now : new Date();
}

const ADAPTER_VERSION = isValidAdapterIdentifier(PLIMSOLL_VERSION)
  ? PLIMSOLL_VERSION
  : "unknown";

// ---------------------------------------------------------------------------
// Claude status-line stdin allowlist parser
// ---------------------------------------------------------------------------

/**
 * Hard bound on status-line stdin captured into memory. Claude Code status
 * payloads are small JSON documents; anything near this bound is hostile and
 * refused before buffering more.
 */
export const CLAUDE_STATUS_LINE_MAX_STDIN_BYTES = 1024 * 1024;

/**
 * Documented `rate_limits` window labels (Claude Code v2.1+ status-line
 * stdin JSON). Anything else inside `rate_limits` is IGNORED — never echoed,
 * never parsed, never defaulted.
 */
export const CLAUDE_RATE_LIMIT_WINDOW_LABELS = [
  "five_hour",
  "seven_day",
  "seven_day_sonnet",
] as const;

export type ClaudeStatusLineParseOutcome =
  | {
      kind: "ok";
      windows: CapacityWindowFact[];
      providerVersion: string | null;
    }
  | { kind: "parse_failed"; reason: "not_json" | "not_object" | "too_large" };

/**
 * Bounded allowlist parser for the documented Claude Code status-line stdin
 * fields: `rate_limits.{five_hour,seven_day,seven_day_sonnet}.{used_percentage,
 * resets_at}` and the top-level `version` string. Every other byte of the
 * payload — including transcript paths, cwd values, and any unknown key — is
 * dropped by construction.
 */
export function parseClaudeStatusLineStdin(raw: Buffer | string): ClaudeStatusLineParseOutcome {
  const text = typeof raw === "string" ? raw : raw.toString("utf8");
  if (text.length > CLAUDE_STATUS_LINE_MAX_STDIN_BYTES) {
    return { kind: "parse_failed", reason: "too_large" };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { kind: "parse_failed", reason: "not_json" };
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { kind: "parse_failed", reason: "not_object" };
  }
  const document = parsed as Record<string, unknown>;

  const providerVersion = isValidAdapterIdentifier(document.version)
    ? document.version
    : null;

  const windows: CapacityWindowFact[] = [];
  const rateLimits = document.rate_limits;
  if (rateLimits !== null && typeof rateLimits === "object" && !Array.isArray(rateLimits)) {
    const rateLimitsRecord = rateLimits as Record<string, unknown>;
    for (const label of CLAUDE_RATE_LIMIT_WINDOW_LABELS) {
      const slot = rateLimitsRecord[label];
      // An absent window stays absent: skipped entirely, never defaulted.
      if (slot === null || typeof slot !== "object" || Array.isArray(slot)) continue;
      const windowRecord = slot as Record<string, unknown>;
      if (!isUtilizationPercent(windowRecord.used_percentage)) continue;
      windows.push({
        window: label,
        usedPercent: windowRecord.used_percentage,
        windowMinutes: null,
        resetsAt: normalizeResetInstant(windowRecord.resets_at),
      });
    }
  }
  return { kind: "ok", windows, providerVersion };
}

// ---------------------------------------------------------------------------
// Codex account/rateLimits/read allowlist parser
// ---------------------------------------------------------------------------

/** Documented Codex rate-limit window slots (app-server stable API). */
export const CODEX_RATE_LIMIT_WINDOW_SLOTS = ["primary", "secondary"] as const;

export type CodexRateLimitsParseOutcome =
  | { kind: "ok"; windows: CapacityWindowFact[] }
  | { kind: "provider_error" }
  | { kind: "parse_failed"; reason: "not_object" | "missing_result" | "bad_shape" };

/**
 * Bounded allowlist parser for the documented `account/rateLimits/read`
 * response: `result.rateLimits.{primary,secondary}` with `usedPercent`,
 * `windowDurationMins`, and epoch-seconds `resetsAt`. A JSON-RPC `error`
 * member maps to `provider_error`. `null` windows stay absent. Everything
 * else (credits, spend control, plan metadata) is dropped.
 */
export function parseCodexRateLimitsResponse(response: unknown): CodexRateLimitsParseOutcome {
  if (response === null || typeof response !== "object" || Array.isArray(response)) {
    return { kind: "parse_failed", reason: "not_object" };
  }
  const record = response as Record<string, unknown>;
  if (record.error !== undefined && record.error !== null) {
    return { kind: "provider_error" };
  }
  const result = record.result;
  if (result === null || typeof result !== "object" || Array.isArray(result)) {
    return { kind: "parse_failed", reason: "missing_result" };
  }
  const resultRecord = result as Record<string, unknown>;
  const rateLimits = resultRecord.rateLimits;
  if (rateLimits === null || typeof rateLimits !== "object" || Array.isArray(rateLimits)) {
    // Documented as effectively required; a missing map fails closed.
    return { kind: "parse_failed", reason: "bad_shape" };
  }
  const rateLimitsRecord = rateLimits as Record<string, unknown>;
  const windows: CapacityWindowFact[] = [];
  for (const slot of CODEX_RATE_LIMIT_WINDOW_SLOTS) {
    const value = rateLimitsRecord[slot];
    // `secondary: null` is DOCUMENTED — an absent window stays absent.
    if (value === null || value === undefined) continue;
    if (typeof value !== "object" || Array.isArray(value)) {
      return { kind: "parse_failed", reason: "bad_shape" };
    }
    const windowRecord = value as Record<string, unknown>;
    if (!isUtilizationPercent(windowRecord.usedPercent)) {
      return { kind: "parse_failed", reason: "bad_shape" };
    }
    const windowMinutesRaw = windowRecord.windowDurationMins;
    const windowMinutes =
      typeof windowMinutesRaw === "number" &&
      Number.isFinite(windowMinutesRaw) &&
      windowMinutesRaw > 0 &&
      Number.isInteger(windowMinutesRaw)
        ? windowMinutesRaw
        : null;
    windows.push({
      window: slot,
      usedPercent: windowRecord.usedPercent,
      windowMinutes,
      resetsAt: normalizeResetInstant(windowRecord.resetsAt),
    });
  }
  return { kind: "ok", windows };
}

// ---------------------------------------------------------------------------
// Collector-home persistence (latest receipt + backoff state, private)
// ---------------------------------------------------------------------------

export function capacityAdaptersStateDir(home: string): string {
  return path.join(home, "capacity", "adapters");
}

function adapterStatePaths(home: string, adapter: CapacityAdapterId, profileId: string) {
  const base = isValidAdapterIdentifier(profileId) ? profileId : "default";
  const dir = capacityAdaptersStateDir(home);
  return {
    dir,
    receiptFile: path.join(dir, `${adapter}.${base}.receipt.json`),
    backoffFile: path.join(dir, `${adapter}.${base}.backoff.json`),
  };
}

function ensurePrivateDir(dir: string): void {
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  fs.chmodSync(dir, 0o700);
}

/** Atomic private-file replace: unique temp file in the same directory. */
function atomicWritePrivate(file: string, contents: string): void {
  const temp = `${file}.tmp-${process.pid}-${randomUUID()}`;
  const descriptor = fs.openSync(temp, "wx", 0o600);
  try {
    fs.writeFileSync(descriptor, contents, "utf8");
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  fs.renameSync(temp, file);
}

function readJsonIfExists(file: string): unknown {
  let text: string;
  try {
    text = fs.readFileSync(file, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

export function resolveAdapterStateHome(options: {
  homeDir?: string;
  env?: NodeJS.ProcessEnv;
} = {}): string {
  const resolved = resolveCollectorHome({
    env: options.env ?? process.env,
    homeDir: options.homeDir,
  });
  return resolved.home;
}

export function loadLatestAdapterReceipt(input: {
  home: string;
  adapter: CapacityAdapterId;
  profileId?: string;
  now?: Date;
  maxAgeMs?: number;
}): ProviderCapacityAdapterReceipt | null {
  const profileId = input.profileId ?? ADAPTER_DEFAULT_PROFILE_ID;
  const { receiptFile } = adapterStatePaths(input.home, input.adapter, profileId);
  const stored = readJsonIfExists(receiptFile);
  if (
    stored === null ||
    typeof stored !== "object" ||
    (stored as Record<string, unknown>).schema !== PROVIDER_CAPACITY_ADAPTER_RECEIPT_SCHEMA
  ) {
    return null;
  }
  const receipt = stored as ProviderCapacityAdapterReceipt;
  if (input.now !== undefined || input.maxAgeMs !== undefined) {
    const maxAgeMs = input.maxAgeMs ?? receipt.freshness?.maxAgeMs ?? 6 * 60 * 60 * 1000;
    const freshness = classifyAdapterObservationFreshness({
      observedAt: receipt.observedAt,
      now: (input.now ?? new Date()).toISOString(),
      maxAgeMs,
    });
    return {
      ...receipt,
      freshness: { ...freshness, maxAgeMs },
    };
  }
  return receipt;
}

function persistLatestAdapterReceipt(receipt: ProviderCapacityAdapterReceipt, home: string): void {
  const { dir, receiptFile } = adapterStatePaths(home, receipt.adapter, receipt.profileId);
  ensurePrivateDir(dir);
  atomicWritePrivate(receiptFile, `${JSON.stringify(receipt, null, 2)}\n`);
}

// ---------------------------------------------------------------------------
// Backoff state (manual refresh still refuses to hammer a failing provider)
// ---------------------------------------------------------------------------

export const ADAPTER_BACKOFF_BASE_MS = 2_000 as const;
export const ADAPTER_BACKOFF_MAX_MS = 60_000 as const;

export type AdapterBackoffState = {
  consecutiveFailures: number;
  nextAllowedAt: string | null;
  updatedAt: string;
};

export function computeAdapterBackoff(input: {
  previous: AdapterBackoffState | null;
  failed: boolean;
  now: Date;
}): AdapterBackoffState {
  const failures = input.failed
    ? Math.min(16, (input.previous?.consecutiveFailures ?? 0) + 1)
    : 0;
  const delayMs =
    failures === 0
      ? 0
      : Math.min(
          ADAPTER_BACKOFF_MAX_MS,
          ADAPTER_BACKOFF_BASE_MS * 2 ** (failures - 1),
        );
  return {
    consecutiveFailures: failures,
    nextAllowedAt:
      failures === 0 ? null : new Date(input.now.getTime() + delayMs).toISOString(),
    updatedAt: input.now.toISOString(),
  };
}

function loadBackoffState(file: string, now: Date): AdapterBackoffState | null {
  const stored = readJsonIfExists(file);
  if (stored === null || typeof stored !== "object") return null;
  const candidate = stored as Partial<AdapterBackoffState>;
  if (
    typeof candidate.consecutiveFailures !== "number" ||
    !Number.isFinite(candidate.consecutiveFailures) ||
    candidate.consecutiveFailures < 0
  ) {
    return null;
  }
  const state: AdapterBackoffState = {
    consecutiveFailures: Math.floor(candidate.consecutiveFailures),
    nextAllowedAt:
      typeof candidate.nextAllowedAt === "string" &&
      Number.isFinite(Date.parse(candidate.nextAllowedAt))
        ? candidate.nextAllowedAt
        : null,
    updatedAt:
      typeof candidate.updatedAt === "string" &&
      Number.isFinite(Date.parse(candidate.updatedAt))
        ? candidate.updatedAt
        : now.toISOString(),
  };
  // Expired windows collapse to "allowed" on read so stale files cannot
  // wedge the adapter shut forever.
  if (state.nextAllowedAt !== null && Date.parse(state.nextAllowedAt) <= now.getTime()) {
    return { ...state, nextAllowedAt: null };
  }
  return state;
}

function persistBackoffState(state: AdapterBackoffState, file: string, dir: string): void {
  ensurePrivateDir(dir);
  atomicWritePrivate(file, `${JSON.stringify(state, null, 2)}\n`);
}

// ---------------------------------------------------------------------------
// Per-profile single-flight concurrency (one probe per profile, ever overlapped)
// ---------------------------------------------------------------------------

const inFlightProbes = new Map<string, Promise<ProviderCapacityAdapterReceipt>>();

function probeKey(adapter: CapacityAdapterId, profileId: string): string {
  return `${adapter}\u0000${profileId}`;
}

/**
 * Claim the profile slot. `busy` carries the in-flight probe so callers can
 * turn it into the symbolic `busy` receipt without spawning anything;
 * `acquired` carries THIS caller's own runner promise.
 */
function claimProfileSlot(
  key: string,
  runner: () => Promise<ProviderCapacityAdapterReceipt>,
): { slot: "acquired"; promise: Promise<ProviderCapacityAdapterReceipt> } | {
  slot: "busy";
  inFlight: Promise<ProviderCapacityAdapterReceipt>;
} {
  const existing = inFlightProbes.get(key);
  if (existing) return { slot: "busy", inFlight: existing };
  const promise = runner().finally(() => {
    inFlightProbes.delete(key);
  });
  inFlightProbes.set(key, promise);
  return { slot: "acquired", promise };
}

// ---------------------------------------------------------------------------
// Codex app-server probe
// ---------------------------------------------------------------------------

export const CODEX_APP_SERVER_DEFAULT_COMMAND: readonly string[] = ["codex", "app-server"];
export const CODEX_PROBE_TIMEOUT_MS_DEFAULT = 15_000 as const;
export const CODEX_PROBE_MAX_OUTPUT_BYTES = 1024 * 1024;
export const CODEX_PROBE_SHUTDOWN_GRACE_MS = 2_000 as const;
export const CODEX_PROTOCOL_INITIALIZE_ID = 1 as const;
export const CODEX_PROTOCOL_RATE_LIMITS_ID = 2 as const;

export type CodexAppServerProbeOptions = {
  /** Default profile only; alternate CODEX_HOME profiles are issue #172. */
  profileId?: string;
  /** Injectable for proofs; production defaults to the stable CLI command. */
  command?: readonly string[];
  timeoutMs?: number;
  maxOutputBytes?: number;
  shutdownGraceMs?: number;
  homeDir?: string;
  env?: NodeJS.ProcessEnv;
  maxAgeMs?: number;
  now?: () => Date;
};

type ProbeRuntime = {
  command: readonly string[];
  timeoutMs: number;
  maxOutputBytes: number;
  shutdownGraceMs: number;
  maxAgeMs: number;
  home: string;
  profileId: string;
  now: () => Date;
};

function resolveProbeRuntime(options: CodexAppServerProbeOptions): ProbeRuntime {
  const now = options.now ?? (() => new Date());
  return {
    command: options.command ?? CODEX_APP_SERVER_DEFAULT_COMMAND,
    timeoutMs: options.timeoutMs ?? CODEX_PROBE_TIMEOUT_MS_DEFAULT,
    maxOutputBytes: options.maxOutputBytes ?? CODEX_PROBE_MAX_OUTPUT_BYTES,
    shutdownGraceMs: options.shutdownGraceMs ?? CODEX_PROBE_SHUTDOWN_GRACE_MS,
    maxAgeMs: options.maxAgeMs ?? 6 * 60 * 60 * 1000,
    home: resolveAdapterStateHome({ homeDir: options.homeDir, env: options.env }),
    profileId: options.profileId ?? ADAPTER_DEFAULT_PROFILE_ID,
    now,
  };
}

function symbolicReceipt(input: {
  runtime: ProbeRuntime;
  adapter: CapacityAdapterId;
  errorCode: CapacityAdapterError;
  phase: CapacityAdapterPhase;
  startedAt: Date;
  resourceCost?: Partial<ProviderCapacityAdapterReceipt["resourceCost"]>;
}): ProviderCapacityAdapterReceipt {
  return buildReceipt({
    adapter: input.adapter,
    profileId: input.runtime.profileId,
    ok: false,
    phase: input.phase,
    errorCode: input.errorCode,
    startedAt: input.startedAt,
    windows: [],
    providerVersion: null,
    resourceCost: {
      spawnCount: 0,
      bytesWritten: 0,
      bytesRead: 0,
      exitCode: null,
      signal: null,
      ...input.resourceCost,
    },
    maxAgeMs: input.runtime.maxAgeMs,
    now: input.runtime.now(),
  });
}

class OutputBoundExceededError extends Error {
  constructor(readonly bytesRead: number) {
    super("output_bound_exceeded");
  }
}

class ProbeTimeoutError extends Error {
  constructor() {
    super("timeout");
  }
}

/**
 * Read newline-delimited JSON-RPC messages from the child stdout with a hard
 * byte ceiling and an absolute deadline. Returns the parsed message matching
 * `wantedId`; notifications and foreign ids are drained and ignored. Any
 * unparsable non-empty line is a protocol violation (the stable API speaks
 * JSONL only), failing closed instead of being skipped.
 */
async function readRpcResponse(input: {
  child: ChildProcessWithoutNullStreams;
  wantedId: number;
  maxOutputBytes: number;
  deadline: number;
  byteCounter: { total: number };
}): Promise<Record<string, unknown>> {
  return new Promise<Record<string, unknown>>((resolve, reject) => {
    let settled = false;
    let lineBuffer = "";
    const settle = (error: Error | null, message?: Record<string, unknown>) => {
      if (settled) return;
      settled = true;
      clearInterval(timer);
      input.child.stdout.off("data", onData);
      input.child.stdout.off("end", onEnd);
      input.child.stdout.off("error", onError);
      if (error !== null) reject(error);
      else resolve(message!);
    };
    const onData = (chunk: Buffer) => {
      input.byteCounter.total += chunk.length;
      if (input.byteCounter.total > input.maxOutputBytes) {
        settle(new OutputBoundExceededError(input.byteCounter.total));
        return;
      }
      lineBuffer += chunk.toString("utf8");
      let newlineIndex = lineBuffer.indexOf("\n");
      while (newlineIndex !== -1 && !settled) {
        const line = lineBuffer.slice(0, newlineIndex).trim();
        lineBuffer = lineBuffer.slice(newlineIndex + 1);
        newlineIndex = lineBuffer.indexOf("\n");
        if (line.length === 0) continue;
        let parsed: unknown;
        try {
          parsed = JSON.parse(line);
        } catch {
          settle(new Error("protocol_violation:unparsable_line"));
          return;
        }
        if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
          settle(new Error("protocol_violation:not_object"));
          return;
        }
        const message = parsed as Record<string, unknown>;
        if (message.id !== input.wantedId) continue; // drain notifications
        settle(null, message);
        return;
      }
    };
    const onEnd = () => {
      settle(new Error("protocol_violation:stdout_closed"));
    };
    const onError = (error: Error) => {
      settle(error);
    };
    const timer = setInterval(() => {
      if (Date.now() > input.deadline) {
        settle(new ProbeTimeoutError());
      }
    }, 25);
    input.child.stdout.on("data", onData);
    input.child.stdout.on("end", onEnd);
    input.child.stdout.on("error", onError);
  });
}

/**
 * Clean-shutdown ladder: end stdin (EOF is the app-server's exit cue), wait
 * through a grace window, then SIGTERM, then SIGKILL. Never leaves a probe
 * child behind.
 */
async function shutdownChild(
  child: ChildProcessWithoutNullStreams,
  graceMs: number,
): Promise<{ exitCode: number | null; signal: string | null }> {
  const exited = new Promise<{ exitCode: number | null; signal: string | null }>((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      resolve({ exitCode: child.exitCode, signal: child.signalCode });
      return;
    }
    child.once("close", (code, signal) => resolve({ exitCode: code, signal }));
  });
  await new Promise<void>((resolve) => {
    try {
      child.stdin.end(() => resolve());
    } catch {
      resolve();
    }
    setTimeout(resolve, graceMs).unref();
  });
  if (!(await raceExited(exited, graceMs))) {
    try {
      child.kill("SIGTERM");
    } catch {
      // already dead
    }
    if (!(await raceExited(exited, graceMs))) {
      try {
        child.kill("SIGKILL");
      } catch {
        // already dead
      }
    }
  }
  return exited;
}

async function raceExited(
  exited: Promise<{ exitCode: number | null; signal: string | null }>,
  ms: number,
): Promise<boolean> {
  return await Promise.race([
    exited.then(() => true),
    new Promise<boolean>((resolve) => setTimeout(() => resolve(false), ms).unref()),
  ]);
}

/**
 * Exact stable-API outbound sequence over newline-delimited JSON-RPC (the
 * app-server wire omits the `jsonrpc` member):
 *   1. {"method":"initialize","id":1,"params":{"clientInfo":{...}}}
 *   2. {"method":"initialized"}                     (notification)
 *   3. {"method":"account/rateLimits/read","id":2}
 * then clean shutdown. Bounded input, output, and wall time throughout.
 */
export async function runCodexAppServerCapacityProbe(
  options: CodexAppServerProbeOptions = {},
): Promise<ProviderCapacityAdapterReceipt> {
  const runtime = resolveProbeRuntime(options);
  const startedAt = runtime.now();

  // Single-flight: an overlapping probe for the same profile returns a
  // symbolic busy receipt and spawns NOTHING.
  const claim = claimProfileSlot(probeKey("codex_app_server", runtime.profileId), () =>
    executeCodexProbe(runtime),
  );
  if (claim.slot === "busy") {
    void claim.inFlight;
    return symbolicReceipt({
      runtime,
      adapter: "codex_app_server",
      errorCode: "busy",
      phase: "not_started",
      startedAt,
    });
  }
  return claim.promise;
}

async function executeCodexProbe(runtime: ProbeRuntime): Promise<ProviderCapacityAdapterReceipt> {
  const startedAt = runtime.now();
  const { dir, backoffFile } = adapterStatePaths(runtime.home, "codex_app_server", runtime.profileId);

  // Backoff gate: inside an active window the probe refuses BEFORE spawning.
  const backoff = loadBackoffState(backoffFile, startedAt);
  if (backoff !== null && backoff.nextAllowedAt !== null) {
    const receipt = symbolicReceipt({
      runtime,
      adapter: "codex_app_server",
      errorCode: "backoff_window_active",
      phase: "not_started",
      startedAt,
    });
    persistLatestAdapterReceipt(receipt, runtime.home);
    return receipt;
  }

  const spawnErrorState = { errored: false };
  let child: ChildProcessWithoutNullStreams;
  try {
    child = spawn(runtime.command[0]!, runtime.command.slice(1), {
      stdio: ["pipe", "pipe", "pipe"],
      shell: false,
    }) as ChildProcessWithoutNullStreams;
  } catch (error) {
    void error;
    return finishCodexProbe({
      runtime,
      startedAt,
      child: null,
      spawnErrorState,
      failure: { code: "spawn_failed" },
      phase: "spawn",
      windows: [],
      providerVersion: null,
      bytesWritten: 0,
      bytesRead: 0,
      exitCode: null,
      signal: null,
    });
  }
  child.once("error", () => {
    spawnErrorState.errored = true;
  });

  const byteCounter = { total: 0 };
  let bytesWritten = 0;
  const deadline = startedAt.getTime() + runtime.timeoutMs;
  let phase: CapacityAdapterPhase = "spawn";
  let failure: { code: CapacityAdapterError } | null = null;
  let windows: CapacityWindowFact[] = [];
  let providerVersion: string | null = null;

  // Exact stable-API outbound sequence (the app-server wire omits the
  // `jsonrpc` member):
  //   1. initialize request            -> await its response
  //   2. initialized NOTIFICATION      (no id, no response expected)
  //   3. account/rateLimits/read       -> await its response
  // Nothing else is ever sent.
  const initializeMessage: Record<string, unknown> = {
    method: "initialize",
    id: CODEX_PROTOCOL_INITIALIZE_ID,
    params: {
      clientInfo: {
        name: "plimsoll",
        title: "Plimsoll capacity adapter",
        version: ADAPTER_VERSION,
      },
    },
  };
  const rateLimitsMessage: Record<string, unknown> = {
    method: "account/rateLimits/read",
    id: CODEX_PROTOCOL_RATE_LIMITS_ID,
  };

  try {
    phase = "initialize";
    bytesWritten += writeRpcLine(child, initializeMessage);
    await readRpcResponse({
      child,
      wantedId: CODEX_PROTOCOL_INITIALIZE_ID,
      maxOutputBytes: runtime.maxOutputBytes,
      deadline,
      byteCounter,
    });

    phase = "rate_limits_read";
    bytesWritten += writeRpcLine(child, { method: "initialized" });
    bytesWritten += writeRpcLine(child, rateLimitsMessage);
    const rateLimitsResponse = await readRpcResponse({
      child,
      wantedId: CODEX_PROTOCOL_RATE_LIMITS_ID,
      maxOutputBytes: runtime.maxOutputBytes,
      deadline,
      byteCounter,
    });

    const parsed = parseCodexRateLimitsResponse(rateLimitsResponse);
    if (parsed.kind === "provider_error") {
      failure = { code: "provider_error" };
    } else if (parsed.kind !== "ok") {
      failure = { code: "parse_failed" };
    } else {
      windows = parsed.windows;
      phase = "shutdown";
    }
  } catch (error) {
    if (spawnErrorState.errored) {
      failure = { code: "spawn_failed" };
    } else {
      failure = failureFromError(error);
    }
  }

  const shutdown = await shutdownChild(child, runtime.shutdownGraceMs);
  return finishCodexProbe({
    runtime,
    startedAt,
    child,
    spawnErrorState,
    failure,
    phase,
    windows,
    providerVersion,
    bytesWritten,
    bytesRead: byteCounter.total,
    exitCode: shutdown.exitCode,
    signal: shutdown.signal,
  });
}

function writeRpcLine(child: ChildProcessWithoutNullStreams, message: Record<string, unknown>): number {
  const line = `${JSON.stringify(message)}\n`;
  child.stdin.write(line);
  return Buffer.byteLength(line, "utf8");
}

function failureFromError(error: unknown): { code: CapacityAdapterError } {
  if (error instanceof OutputBoundExceededError) return { code: "output_bound_exceeded" };
  if (error instanceof ProbeTimeoutError) return { code: "timeout" };
  const message = error instanceof Error ? error.message : String(error);
  if (message.startsWith("protocol_violation")) return { code: "protocol_violation" };
  return { code: "io_error" };
}

function finishCodexProbe(input: {
  runtime: ProbeRuntime;
  startedAt: Date;
  child: ChildProcessWithoutNullStreams | null;
  spawnErrorState: { errored: boolean };
  failure: { code: CapacityAdapterError } | null;
  phase: CapacityAdapterPhase;
  windows: CapacityWindowFact[];
  providerVersion: string | null;
  bytesWritten: number;
  bytesRead: number;
  exitCode: number | null;
  signal: string | null;
}): ProviderCapacityAdapterReceipt {
  const ok = input.failure === null;
  const receipt = buildReceipt({
    adapter: "codex_app_server",
    profileId: input.runtime.profileId,
    ok,
    phase: ok ? "complete" : input.phase,
    errorCode: ok ? "none" : input.failure!.code,
    startedAt: input.startedAt,
    windows: input.windows,
    providerVersion: input.providerVersion,
    resourceCost: {
      spawnCount: input.child === null ? 0 : 1,
      bytesWritten: input.bytesWritten,
      bytesRead: input.bytesRead,
      exitCode: input.exitCode,
      signal: input.signal,
    },
    maxAgeMs: input.runtime.maxAgeMs,
    now: input.runtime.now(),
  });
  const { dir, backoffFile } = adapterStatePaths(
    input.runtime.home,
    "codex_app_server",
    input.runtime.profileId,
  );
  try {
    const backoff = computeAdapterBackoff({
      previous: loadBackoffState(backoffFile, input.runtime.now()),
      failed: !ok,
      now: input.runtime.now(),
    });
    persistBackoffState(backoff, backoffFile, dir);
    persistLatestAdapterReceipt(receipt, input.runtime.home);
  } catch {
    // Persistence is best-effort bookkeeping; the receipt is still returned
    // even when the state directory is unwritable.
  }
  return receipt;
}

// ---------------------------------------------------------------------------
// Claude status-line proxy runtime
// ---------------------------------------------------------------------------

export const STATUS_LINE_CHAIN_DEFAULT_TIMEOUT_MS = 10_000 as const;
export const STATUS_LINE_CHAIN_SHUTDOWN_GRACE_MS = 2_000 as const;
export const STATUS_LINE_CHAIN_MAX_STREAM_BYTES = 4 * 1024 * 1024;

export type ClaudeStatusLineProxyOptions = {
  /** Raw Claude Code stdin bytes, already fully buffered by the caller. */
  stdinBytes: Buffer;
  /** Prior operator status-line command, or null when Plimsoll is alone. */
  chainCommand: string | null;
  chainTimeoutMs?: number;
  shutdownGraceMs?: number;
  maxStreamBytes?: number;
  /** Persistence seam; defaults to the resolved collector home. */
  homeDir?: string;
  env?: NodeJS.ProcessEnv;
  maxAgeMs?: number;
  now?: () => Date;
};

export type ClaudeStatusLineProxyResult = {
  stdout: Buffer;
  stderr: Buffer;
  exitCode: number;
  receipt: ProviderCapacityAdapterReceipt;
};

/**
 * One status-line invocation. Snapshots the allowlisted capacity facts, then
 * forwards the RAW stdin bytes to the chained operator command (if any) and
 * preserves its stdout, stderr, exit status, and timeout behavior exactly.
 * A capture failure NEVER breaks the operator's status line: parsing problems
 * become symbolic receipt codes while the chain still runs.
 */
export async function runClaudeStatusLineProxy(
  options: ClaudeStatusLineProxyOptions,
): Promise<ClaudeStatusLineProxyResult> {
  const now = options.now ?? (() => new Date());
  const startedAt = now();
  const chainTimeoutMs = options.chainTimeoutMs ?? STATUS_LINE_CHAIN_DEFAULT_TIMEOUT_MS;
  const shutdownGraceMs = options.shutdownGraceMs ?? STATUS_LINE_CHAIN_SHUTDOWN_GRACE_MS;
  const maxStreamBytes = options.maxStreamBytes ?? STATUS_LINE_CHAIN_MAX_STREAM_BYTES;
  const maxAgeMs = options.maxAgeMs ?? 6 * 60 * 60 * 1000;
  const home = resolveAdapterStateHome({ homeDir: options.homeDir, env: options.env });

  let parse: ClaudeStatusLineParseOutcome = { kind: "parse_failed", reason: "not_json" };
  if (options.stdinBytes.length <= CLAUDE_STATUS_LINE_MAX_STDIN_BYTES) {
    parse = parseClaudeStatusLineStdin(options.stdinBytes);
  }

  const captureErrorCode: CapacityAdapterError =
    options.stdinBytes.length > CLAUDE_STATUS_LINE_MAX_STDIN_BYTES
      ? "input_bound_exceeded"
      : parse.kind === "ok"
        ? "none"
        : "parse_failed";

  let stdout: Buffer<ArrayBufferLike> = Buffer.alloc(0);
  let stderr: Buffer<ArrayBufferLike> = Buffer.alloc(0);
  let exitCode = 0;
  let signal: string | null = null;
  let spawnCount = 0;
  let bytesWritten = 0;

  if (options.chainCommand !== null && captureErrorCode !== "input_bound_exceeded") {
    const chained = await runChainedStatusLineCommand({
      command: options.chainCommand,
      stdinBytes: options.stdinBytes,
      timeoutMs: chainTimeoutMs,
      shutdownGraceMs,
      maxStreamBytes,
    });
    spawnCount = 1;
    bytesWritten = chained.bytesWritten;
    stdout = chained.stdout;
    stderr = chained.stderr;
    exitCode = chained.exitCode;
    signal = chained.signal;
  } else if (captureErrorCode === "input_bound_exceeded") {
    stderr = Buffer.from(
      "plimsoll-capacity-status-line: input_bound_exceeded; status line passthrough skipped\n",
      "utf8",
    );
    exitCode = 0;
  }

  const completedAt = now();
  const observedAt = completedAt.toISOString();
  const windows = parse.kind === "ok" ? parse.windows : [];
  const providerVersion = parse.kind === "ok" ? parse.providerVersion : null;
  const freshness = classifyAdapterObservationFreshness({
    observedAt,
    now: observedAt,
    maxAgeMs,
  });
  const receipt: ProviderCapacityAdapterReceipt = {
    schema: PROVIDER_CAPACITY_ADAPTER_RECEIPT_SCHEMA,
    adapter: "claude_status_line",
    adapterVersion: ADAPTER_VERSION,
    profileId: ADAPTER_DEFAULT_PROFILE_ID,
    ok: captureErrorCode === "none",
    phase: "complete",
    error: captureErrorCode === "none" ? null : { code: captureErrorCode },
    observedAt,
    freshness: { ...freshness, maxAgeMs },
    windows,
    providerVersion,
    timing: {
      startedAt: startedAt.toISOString(),
      completedAt: observedAt,
      elapsedMs: Math.max(0, completedAt.getTime() - startedAt.getTime()),
    },
    resourceCost: {
      spawnCount,
      bytesWritten,
      bytesRead: options.stdinBytes.length,
      exitCode,
      signal,
    },
  };
  try {
    const { dir, receiptFile } = adapterStatePaths(home, "claude_status_line", ADAPTER_DEFAULT_PROFILE_ID);
    ensurePrivateDir(dir);
    atomicWritePrivate(receiptFile, `${JSON.stringify(receipt, null, 2)}\n`);
  } catch {
    // Best-effort persistence; the passthrough behavior above is unaffected.
  }
  return { stdout, stderr, exitCode, receipt };
}

type ChainedRun = {
  stdout: Buffer;
  stderr: Buffer;
  exitCode: number;
  signal: string | null;
  bytesWritten: number;
};

/**
 * Reproduce the operator status line: `/bin/sh -c <command>` with the raw
 * stdin forwarded in memory, bounded stdout/stderr capture, and the classic
 * timeout ladder (deadline → SIGTERM → grace → SIGKILL, exit 124 on timeout,
 * mirroring GNU timeout's convention).
 */
async function runChainedStatusLineCommand(input: {
  command: string;
  stdinBytes: Buffer;
  timeoutMs: number;
  shutdownGraceMs: number;
  maxStreamBytes: number;
}): Promise<ChainedRun> {
  return new Promise<ChainedRun>((resolve) => {
    let child: ChildProcessWithoutNullStreams;
    try {
      child = spawn("/bin/sh", ["-c", input.command], {
        stdio: ["pipe", "pipe", "pipe"],
        shell: false,
      }) as ChildProcessWithoutNullStreams;
    } catch {
      resolve({
        stdout: Buffer.alloc(0),
        stderr: Buffer.alloc(0),
        exitCode: 126,
        signal: null,
        bytesWritten: 0,
      });
      return;
    }
    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];
    let stdoutTotal = 0;
    let stderrTotal = 0;
    let settled = false;
    let timedOut = false;

    const finish = (exitCode: number, signal: string | null, bytesWritten: number) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      clearTimeout(killTimer);
      resolve({
        stdout: Buffer.concat(stdoutChunks),
        stderr: Buffer.concat(stderrChunks),
        exitCode,
        signal,
        bytesWritten,
      });
    };

    child.stdout.on("data", (chunk: Buffer) => {
      stdoutTotal += chunk.length;
      if (stdoutTotal <= input.maxStreamBytes) stdoutChunks.push(chunk);
      if (stdoutTotal > input.maxStreamBytes || stderrTotal > input.maxStreamBytes) {
        try {
          child.kill("SIGKILL");
        } catch {
          // already dead
        }
        finish(124, "SIGKILL", input.stdinBytes.length);
      }
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderrTotal += chunk.length;
      if (stderrTotal <= input.maxStreamBytes) stderrChunks.push(chunk);
      if (stdoutTotal > input.maxStreamBytes || stderrTotal > input.maxStreamBytes) {
        try {
          child.kill("SIGKILL");
        } catch {
          // already dead
        }
        finish(124, "SIGKILL", input.stdinBytes.length);
      }
    });
    child.once("error", () => finish(126, null, 0));
    child.once("close", (code, closeSignal) => {
      if (timedOut) {
        finish(124, closeSignal ?? "SIGTERM", input.stdinBytes.length);
        return;
      }
      finish(code ?? (closeSignal ? 128 + 15 : 1), closeSignal, input.stdinBytes.length);
    });

    const timer = setTimeout(() => {
      timedOut = true;
      try {
        child.kill("SIGTERM");
      } catch {
        // already dead
      }
    }, input.timeoutMs);
    const killTimer = setTimeout(() => {
      if (!timedOut) return;
      try {
        child.kill("SIGKILL");
      } catch {
        // already dead
      }
    }, input.timeoutMs + input.shutdownGraceMs);

    child.stdin.on("error", () => {
      // EPIPE when the operator command does not read stdin: not fatal.
    });
    child.stdin.end(input.stdinBytes);
  });
}

// ---------------------------------------------------------------------------
// Claude status-line configuration (install / chain / block)
// ---------------------------------------------------------------------------

export const STATUS_LINE_PROXY_INVOCATION_MARKER =
  "__plimsoll-capacity-statusline-proxy" as const;

export type ClaudeStatusLineConfigureInput = {
  settingsPath: string;
  /**
   * Absolute, shell-safe base command that invokes THIS module's hidden proxy
   * entry point (everything before the marker argument), e.g.
   *   tsx /repo/packages/collector-cli/src/provider-capacity-adapters.ts
   * Callers own resolution; this module never guesses launchers.
   */
  baseProxyCommand: string;
  transactionHooks?: NonNullable<Parameters<typeof applyClaudeSettings>[2]>["transactionHooks"];
};

export type ClaudeStatusLineConfigureResult =
  | {
      outcome: "installed" | "already_installed" | "chained" | "unchanged_chain";
      changes: string[];
      backupPath?: string;
      chainedCommand: string | null;
    }
  | { outcome: "blocked_existing_statusline" | "blocked_invalid_settings"; reason: string };

function statusLineEntryFor(chainCommand: string | null, baseProxyCommand: string): Record<string, unknown> {
  const chainArgument =
    chainCommand === null ? "-" : Buffer.from(chainCommand, "utf8").toString("base64");
  return {
    type: "command",
    command: `${baseProxyCommand} ${STATUS_LINE_PROXY_INVOCATION_MARKER} ${chainArgument}`,
  };
}

function decodeConfiguredChainCommand(command: unknown): string | null | undefined {
  if (typeof command !== "string") return undefined;
  const prefix = ` ${STATUS_LINE_PROXY_INVOCATION_MARKER} `;
  const markerIndex = command.indexOf(prefix);
  if (markerIndex === -1) return undefined;
  const argument = command.slice(markerIndex + prefix.length).trim();
  if (argument === "-") return null;
  if (!/^[A-Za-z0-9+/=]+$/.test(argument) || argument.length > 8192) return undefined;
  try {
    const decoded = Buffer.from(argument, "base64").toString("utf8");
    return decoded.length > 0 && decoded.length <= 4096 ? decoded : undefined;
  } catch {
    return undefined;
  }
}

function looksLikeOperatorStatusLine(value: unknown): { command: string } | null {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
  const record = value as Record<string, unknown>;
  const allowedKeys = ["type", "command", "padding", "refreshInterval"];
  const keys = Object.keys(record);
  if (keys.some((key) => !allowedKeys.includes(key))) return null;
  if (record.type !== "command") return null;
  if (typeof record.command !== "string" || record.command.trim().length === 0) return null;
  if (record.command.length > 4096) return null;
  if (
    record.padding !== undefined &&
    record.padding !== null &&
    typeof record.padding !== "number"
  ) {
    return null;
  }
  return { command: record.command };
}

function readSettingsSource(settingsPath: string): { source: string; exists: boolean } {
  try {
    return { source: fs.readFileSync(settingsPath, "utf8"), exists: true };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return { source: "", exists: false };
    }
    throw error;
  }
}

/**
 * Install (or chain) the capacity status line through the CURRENT atomic
 * settings transaction. Decision matrix:
 *
 * - no `statusLine` key            → install ours (no chain)
 * - ours, identical                → already_installed (no-op)
 * - ours, same embedded chain      → unchanged_chain (no-op)
 * - operator entry (known shape)   → chain: forward raw stdin to it
 * - unrecognized/malicious entry   → blocked_existing_statusline, ZERO mutation
 * - unparsable settings            → blocked_invalid_settings, ZERO mutation
 *
 * Safety is proven before anything is written: a dry-run application plans the
 * merge first, so every rejection happens without touching the file, and a
 * failed/interrupted commit restores the original bytes (transaction-owned).
 */
export function configureClaudeStatusLineProxy(
  input: ClaudeStatusLineConfigureInput,
): ClaudeStatusLineConfigureResult {
  let currentSource: string;
  try {
    currentSource = readSettingsSource(input.settingsPath).source;
  } catch {
    return { outcome: "blocked_invalid_settings", reason: "settings_unreadable" };
  }
  let currentDocument: Record<string, unknown> = {};
  if (currentSource.trim().length > 0) {
    try {
      const parsed: unknown = JSON.parse(currentSource);
      if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        return { outcome: "blocked_invalid_settings", reason: "settings_root_not_object" };
      }
      currentDocument = parsed as Record<string, unknown>;
    } catch {
      return { outcome: "blocked_invalid_settings", reason: "settings_malformed_json" };
    }
  }

  const foldedKeys = Object.keys(currentDocument).filter(
    (key) => key !== "statusLine" && key.toLowerCase() === "statusline",
  );
  if (foldedKeys.length > 0) {
    return {
      outcome: "blocked_existing_statusline",
      reason: "status_line_key_alias_present",
    };
  }

  const existing = currentDocument.statusLine;
  let chainCommand: string | null = null;
  if (existing !== undefined) {
    const oursDecode = decodeConfiguredChainCommand(
      (existing as Record<string, unknown> | null)?.command,
    );
    if (
      existing !== null &&
      typeof existing === "object" &&
      typeof (existing as Record<string, unknown>).command === "string" &&
      oursDecode !== undefined
    ) {
      // Already ours; keep whatever chain is embedded.
      chainCommand = oursDecode;
    } else {
      const operator = looksLikeOperatorStatusLine(existing);
      if (operator === null) {
        return {
          outcome: "blocked_existing_statusline",
          reason: "unrecognized_existing_entry_shape",
        };
      }
      chainCommand = operator.command;
    }
  }

  const desired = statusLineEntryFor(chainCommand, input.baseProxyCommand);
  if (isDeepStrictEqual(existing ?? undefined, desired)) {
    return {
      outcome: chainCommand === null ? "already_installed" : "unchanged_chain",
      changes: [],
      chainedCommand: chainCommand,
    };
  }

  // Prove the plan first: a dry run executes the full reconciliation (alias
  // checks, shape checks) WITHOUT writing, so rejection never mutates.
  try {
    applyClaudeSettings(input.settingsPath, { env: {}, statusLine: desired }, { dryRun: true });
  } catch (error) {
    return {
      outcome:
        error instanceof ClaudeConfigError && error.code === "MALFORMED_JSON"
          ? "blocked_invalid_settings"
          : "blocked_existing_statusline",
      reason: error instanceof ClaudeConfigError ? error.code : "planning_failed",
    };
  }

  try {
    const applied = applyClaudeSettings(
      input.settingsPath,
      { env: {}, statusLine: desired },
      { transactionHooks: input.transactionHooks },
    );
    return {
      outcome: chainCommand === null ? "installed" : "chained",
      changes: applied.changes,
      backupPath: applied.backupPath,
      chainedCommand: chainCommand,
    };
  } catch (error) {
    return {
      outcome: "blocked_existing_statusline",
      reason: error instanceof ClaudeConfigError ? `commit_${error.code}` : "commit_failed",
    };
  }
}

// ---------------------------------------------------------------------------
// Direct-invocation command surface (manual refresh only)
// ---------------------------------------------------------------------------

function printJson(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

async function refreshCommand(argv: string[]): Promise<void> {
  let homeDir: string | undefined;
  const homeIndex = argv.indexOf("--home");
  if (homeIndex !== -1) homeDir = argv[homeIndex + 1];

  const codexReceipt = await runCodexAppServerCapacityProbe({ homeDir });
  const claudeConfiguration = configureClaudeStatusLineProxySafe();
  const claudeLatest = loadLatestAdapterReceipt({
    home: resolveAdapterStateHome({ homeDir }),
    adapter: "claude_status_line",
  });
  const output: Record<string, unknown> = {
    schema: "plimsoll.capacity-refresh.v1",
    cadence: "manual_only",
    codex_app_server: { receipt: codexReceipt },
    claude_status_line: {
      configuration: claudeConfiguration,
      latestReceipt: claudeLatest,
      observationAbsent: claudeLatest === null,
    },
  };
  printJson(output);
  if (!codexReceipt.ok) process.exitCode = 1;
}

function configureClaudeStatusLineProxySafe(): Record<string, unknown> {
  // Configuration requires a launcher decision (packaged CLI wiring is a
  // director decision). Until the installer supplies the absolute proxy
  // command, refresh reports the configuration lane as NOT_CONFIGURED instead
  // of guessing a command that could silently differ from the installed
  // binary.
  return { state: "NOT_CONFIGURED", reason: "proxy_command_requires_installer_decision" };
}

function installClaudeStatuslineCommand(argv: string[]): void {
  const commandIndex = argv.indexOf("--proxy-command");
  const proxyCommand = commandIndex !== -1 ? argv[commandIndex + 1] : undefined;
  const settingsIndex = argv.indexOf("--settings-path");
  const settingsPath = settingsIndex !== -1 ? argv[settingsIndex + 1] : undefined;
  const homeIndex = argv.indexOf("--home");
  const homeDir = homeIndex !== -1 ? argv[homeIndex + 1] : undefined;
  if (
    !proxyCommand ||
    !settingsPath ||
    proxyCommand.includes(STATUS_LINE_PROXY_INVOCATION_MARKER)
  ) {
    process.stderr.write(
      "usage: install-claude-statusline --proxy-command '<absolute launcher>' --settings-path <settings.json>\n",
    );
    process.exitCode = 64;
    return;
  }
  const result = configureClaudeStatusLineProxy({
    settingsPath,
    baseProxyCommand: proxyCommand,
  });
  printJson(result);
  if (result.outcome.startsWith("blocked")) process.exitCode = 1;
}

async function statusLineProxyMain(argv: string[]): Promise<void> {
  const encodedChain = argv[0] ?? "-";
  const chainCommand =
    encodedChain === "-"
      ? null
      : Buffer.from(encodedChain, "base64").toString("utf8");
  const stdinChunks: Buffer[] = [];
  let stdinTotal = 0;
  let overBound = false;
  for await (const chunk of process.stdin) {
    const buffer = chunk as Buffer;
    stdinTotal += buffer.length;
    if (stdinTotal <= CLAUDE_STATUS_LINE_MAX_STDIN_BYTES) stdinChunks.push(buffer);
    else overBound = true;
  }
  const stdinBytes = overBound
    ? Buffer.concat([...stdinChunks, Buffer.alloc(1)])
    : Buffer.concat(stdinChunks);
  const result = await runClaudeStatusLineProxy({
    stdinBytes,
    chainCommand: chainCommand === null || chainCommand.length === 0 ? null : chainCommand,
  });
  if (result.stdout.length > 0) process.stdout.write(result.stdout);
  if (result.stderr.length > 0) process.stderr.write(result.stderr);
  process.exitCode = result.exitCode === 0 ? 0 : result.exitCode;
}

/** True when this module file itself was launched as a program. */
export function invokedAsCapacityAdaptersCli(
  moduleUrl: string,
  argv1: string | undefined,
  command: string | undefined,
): boolean {
  if (!argv1 || !command) return false;
  const knownCommands = [
    "refresh",
    "install-claude-statusline",
    STATUS_LINE_PROXY_INVOCATION_MARKER,
  ];
  if (!knownCommands.includes(command)) return false;
  let modulePath: string;
  try {
    modulePath = fileURLToPath(moduleUrl);
  } catch {
    return false;
  }
  try {
    return fs.realpathSync(path.resolve(argv1)) === fs.realpathSync(modulePath);
  } catch {
    return false;
  }
}

export async function capacityAdaptersCliMain(argv: string[]): Promise<void> {
  const command = argv[0];
  const rest = argv.slice(1);
  if (command === "refresh") {
    await refreshCommand(rest);
    return;
  }
  if (command === "install-claude-statusline") {
    installClaudeStatuslineCommand(rest);
    return;
  }
  if (command === STATUS_LINE_PROXY_INVOCATION_MARKER) {
    await statusLineProxyMain(rest);
    return;
  }
  process.stderr.write(
    "usage: provider-capacity-adapters.ts refresh | install-claude-statusline | (hidden proxy)\n",
  );
  process.exitCode = 64;
}

if (invokedAsCapacityAdaptersCli(import.meta.url, process.argv[1], process.argv[2])) {
  void capacityAdaptersCliMain(process.argv.slice(2));
}
