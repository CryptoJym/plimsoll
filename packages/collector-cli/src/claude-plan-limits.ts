import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import https from "node:https";
import os from "node:os";
import path from "node:path";
import type { LocalEventBuffer } from "./buffer";
import type { CollectorConfig } from "./config";
import { providerAccountKey } from "../../shared/src/policy";
import { readBoundedRegularFile } from "./safe-file-read";
import { PlanLimitEmitter } from "./plan-limit-observation";
import { buildCapacityAdapterReceipt, capacityAdaptersStateDir, computeAdapterBackoff,
  loadBackoffState, persistBackoffState, persistLatestAdapterReceipt, parseClaudeUsageResponse,
  CLAUDE_USAGE_MAX_BYTES, type CapacityAdapterError, type CapacityWindowFact } from "./provider-capacity-adapters";

// Ten-minute reads leave room for the daemon's one-minute tick and request
// latency inside the fifteen-minute freshness target. Refusals have the same
// minimum spacing, including across restarts; there is no token refresh here.
export const CLAUDE_PLAN_LIMIT_POLL_MS = 10 * 60_000;
const REQUEST_TIMEOUT_MS = 5_000;
const AUTH_MAX_BYTES = 4 * 1024 * 1024;
type Json = Record<string, unknown>;
function record(value: unknown): Json {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
function json(text: string): Json {
  try { return record(JSON.parse(text)); } catch { return {}; }
}
function readJson(file: string): Json {
  const read = readBoundedRegularFile(file, AUTH_MAX_BYTES);
  return read.kind === "ok" ? json(read.value) : {};
}
function text(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 && value.length <= 256 ? value : undefined;
}
function planType(value: unknown): string | undefined {
  return typeof value === "string" && /^(?:claude_)?(?:free|pro|max|team|enterprise)$/.test(value) ? value : undefined;
}

export type ClaudeUsageCredential = { accessToken: string; expiresAt: number; planType?: string };
export function parseClaudeUsageCredential(raw: string, now: Date): ClaudeUsageCredential | null {
  const oauth = record(json(raw).claudeAiOauth);
  if (typeof oauth.accessToken !== "string" || oauth.accessToken.length > 16_384 ||
      !/^[\x21-\x7e]+$/.test(oauth.accessToken) || typeof oauth.expiresAt !== "number" ||
      !Number.isFinite(oauth.expiresAt) || oauth.expiresAt <= now.getTime() + REQUEST_TIMEOUT_MS) return null;
  return { accessToken: oauth.accessToken, expiresAt: oauth.expiresAt,
    planType: planType(oauth.subscriptionType) };
}

export function claudeKeychainService(configDir: string, defaultDir: string, explicit = false): string {
  return "Claude Code-credentials" + (configDir === defaultDir && !explicit ? "" :
    `-${createHash("sha256").update(configDir).digest("hex").slice(0, 8)}`);
}

// Security.framework refuses UI for this process. A locked/denied Keychain
// therefore returns no credential instead of asking for a new permission.
// stdout is a private pipe consumed in memory, never inherited or logged.
// These are JXA framework loads, not JavaScript module imports.
const KEYCHAIN_READ_SCRIPT = `ObjC['import']('Foundation'); ObjC['import']('Security');
function run(args) {
  $.SecKeychainSetUserInteractionAllowed(false);
  var q = $.NSMutableDictionary.alloc.init;
  q.setObjectForKey($.kSecClassGenericPassword, $.kSecClass);
  q.setObjectForKey($(args[0]), $.kSecAttrService);
  q.setObjectForKey($.kCFBooleanTrue, $.kSecReturnData);
  q.setObjectForKey($.kSecMatchLimitOne, $.kSecMatchLimit);
  q.setObjectForKey($.kSecUseAuthenticationUIFail, $.kSecUseAuthenticationUI);
  var result = Ref();
  if ($.SecItemCopyMatching(q, result) !== 0) return '';
  var raw = JSON.parse(ObjC.unwrap($.NSString.alloc.initWithDataEncoding(result[0], $.NSUTF8StringEncoding)));
  var o = raw.claudeAiOauth || {};
  return JSON.stringify({claudeAiOauth:{accessToken:o.accessToken, expiresAt:o.expiresAt, subscriptionType:o.subscriptionType}});
}`;

async function readCredential(configDir: string, defaultDir: string, now: Date,
  signal: AbortSignal, explicit: boolean, onSpawn: () => void): Promise<ClaudeUsageCredential | null> {
  if (process.platform === "darwin") {
    const raw = await new Promise<string>((resolve) => {
      const child = execFile("/usr/bin/osascript", ["-l", "JavaScript", "-e", KEYCHAIN_READ_SCRIPT,
        claudeKeychainService(configDir, defaultDir, explicit)],
      { encoding: "utf8", timeout: 2_000, maxBuffer: AUTH_MAX_BYTES, signal },
      (error, stdout) => resolve(error ? "" : stdout));
      if (child.pid) onSpawn();
    });
    const credential = parseClaudeUsageCredential(raw, now);
    if (credential) return credential;
  }
  const file = readBoundedRegularFile(path.join(configDir, ".credentials.json"), AUTH_MAX_BYTES);
  return file.kind === "ok" ? parseClaudeUsageCredential(file.value, now) : null;
}

type UsagePath = "/api/oauth/profile" | "/api/oauth/usage";
type UsageResponse = { status: number; body: string; retryAfterMs?: number };
class UsageError extends Error {
  constructor(readonly code: CapacityAdapterError) { super(code); }
}

/** Fixed Anthropic origin, no redirects or retries, one deadline over the
 * complete response. Error bodies are never read, and raw errors never escape.
 */
function requestUsage(endpoint: UsagePath, token: string, signal: AbortSignal): Promise<UsageResponse> {
  return new Promise((resolve, reject) => {
    const controller = new AbortController();
    const abort = () => controller.abort();
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) controller.abort();
    const timer = setTimeout(abort, REQUEST_TIMEOUT_MS);
    const request = https.get({ hostname: "api.anthropic.com", path: endpoint,
      headers: { Authorization: `Bearer ${token}`, "anthropic-beta": "oauth-2025-04-20",
        Accept: "application/json" }, signal: controller.signal }, (response) => {
      const status = response.statusCode ?? 0;
      if (status !== 200) {
        const raw = response.headers["retry-after"];
        const seconds = typeof raw === "string" ? Number(raw) : NaN;
        const retryAfterMs = Number.isFinite(seconds) ? Math.max(0, seconds * 1000) :
          typeof raw === "string" ? Math.max(0, Date.parse(raw) - Date.now()) : 0;
        response.destroy();
        resolve({ status, body: "", retryAfterMs: Number.isFinite(retryAfterMs) ? Math.min(24 * 60 * 60_000, retryAfterMs) : 0 });
        return;
      }
      const chunks: Buffer[] = [];
      let bytes = 0;
      response.on("data", (chunk: Buffer) => {
        bytes += chunk.length;
        if (bytes > CLAUDE_USAGE_MAX_BYTES) {
          reject(new UsageError("output_bound_exceeded"));
          response.destroy();
        } else chunks.push(chunk);
      });
      response.once("error", () => reject(new UsageError(controller.signal.aborted ? "timeout" : "io_error")));
      response.once("end", () => resolve({ status, body: Buffer.concat(chunks).toString("utf8") }));
    });
    request.once("error", () => reject(new UsageError(controller.signal.aborted ? "timeout" : "io_error")));
    request.once("close", () => { clearTimeout(timer); signal.removeEventListener("abort", abort); });
  });
}

type Account = { uuid: string; email?: string; organization?: string };
export type ClaudePlanLimitsOptions = {
  home: string;
  captureRoots?: CollectorConfig["captureRoots"];
  homeDir?: string;
  env?: NodeJS.ProcessEnv;
  now?: () => Date;
  // Offline proof seams: neither default is evaluated when supplied.
  readCredential?: (configDir: string, now: Date, signal: AbortSignal) => Promise<ClaudeUsageCredential | null>;
  request?: (endpoint: UsagePath, token: string, signal: AbortSignal) => Promise<UsageResponse>;
};

function emptyCounters() {
  return { accountsChecked: 0, planLimitReadingsSeen: 0, planLimitReadingsEmitted: 0,
    planLimitReadingsSkippedNoAccount: 0, planLimitReadingsSkippedNoCredential: 0,
    planLimitReadingsSkippedAccountMismatch: 0, planLimitReadingsSkippedBackoff: 0,
    planLimitReadingsFailed: 0 };
}

/** Reads only enrolled roots (or the existing default when inventory is absent).
 * No directory discovery, auth changes, inference, or status-line setup.
 */
export class ClaudePlanLimitCollector {
  private readonly emitter: PlanLimitEmitter;
  private readonly abort = new AbortController();
  private timer?: NodeJS.Timeout;
  private inFlight: Promise<ReturnType<typeof emptyCounters>> | null = null;
  private stopped = false;
  private latest = emptyCounters();
  private readonly totals = emptyCounters();
  private readonly pollState = new Map<string, ReturnType<typeof computeAdapterBackoff>>();
  private readonly now: () => Date;
  private readonly defaultDir: string;
  private readonly configDirs: string[];
  private readonly homeDir: string;
  private readonly env: NodeJS.ProcessEnv;

  constructor(private readonly buffer: LocalEventBuffer, private readonly options: ClaudePlanLimitsOptions) {
    this.emitter = new PlanLimitEmitter(buffer);
    this.now = options.now ?? (() => new Date());
    this.homeDir = options.homeDir ?? os.homedir();
    this.env = options.env ?? process.env;
    this.defaultDir = path.join(this.homeDir, ".claude");
    this.configDirs = [...new Set(options.captureRoots === undefined
      ? [path.resolve(this.env.CLAUDE_CONFIG_DIR || this.defaultDir)]
      : options.captureRoots.filter(root => root.source === "claude_code").map(root => path.dirname(root.directory)))];
  }

  private account(configDir: string): Account | null {
    // Exactly the identity path used by ClaudeAccountKeyCache for events.
    const explicit = this.env.CLAUDE_CONFIG_DIR && path.resolve(this.env.CLAUDE_CONFIG_DIR) === configDir;
    const file = configDir === this.defaultDir && !explicit
      ? path.join(this.homeDir, ".claude.json") : path.join(configDir, ".claude.json");
    const oauth = record(readJson(file).oauthAccount);
    const uuid = text(oauth.accountUuid);
    return uuid ? { uuid, email: text(oauth.emailAddress), organization: text(oauth.organizationUuid) } : null;
  }

  status() { return { ...this.totals, lastPoll: { ...this.latest }, inFlight: this.inFlight !== null }; }

  start() {
    if (this.timer || this.stopped) return;
    void this.tick();
    this.timer = setInterval(() => void this.tick(), 60_000);
    this.timer.unref();
  }

  stop() {
    this.stopped = true;
    this.abort.abort();
    if (this.timer) clearInterval(this.timer);
  }

  tick(): Promise<ReturnType<typeof emptyCounters>> {
    if (this.inFlight) return this.inFlight;
    if (this.stopped) return Promise.resolve(emptyCounters());
    this.inFlight = this.collect().catch(() => {
      // A filesystem or ledger failure must never crash the intake daemon.
      this.latest = { ...emptyCounters(), planLimitReadingsFailed: 1 };
      this.totals.planLimitReadingsFailed++;
      return this.latest;
    }).finally(() => { this.inFlight = null; });
    return this.inFlight;
  }

  private async collect() {
    const counters = emptyCounters();
    const readAccounts = new Set<string>();
    for (const configDir of this.configDirs) {
      if (this.stopped) break;
      const startedAt = this.now();
      const profileId = createHash("sha256").update(configDir).digest("hex").slice(0, 16);
      const stateDir = capacityAdaptersStateDir(this.options.home);
      const backoffFile = path.join(stateDir, `claude_oauth_usage.${profileId}.backoff.json`);
      let previous = this.pollState.get(profileId) ?? null;
      try { previous ??= loadBackoffState(backoffFile, startedAt); } catch { /* Use in-memory spacing. */ }
      if (previous?.nextAllowedAt && Date.parse(previous.nextAllowedAt) > startedAt.getTime()) {
        counters.planLimitReadingsSkippedBackoff++;
        continue;
      }
      let error: CapacityAdapterError = "none";
      let accountKey: string | undefined;
      let windows: CapacityWindowFact[] = [];
      let retryAfterMs = 0;
      let spawnCount = 0;
      let bytesRead = 0;
      try {
        const account = this.account(configDir);
        if (!account) throw new UsageError("no_account");
        accountKey = providerAccountKey(account.uuid);
        if (readAccounts.has(accountKey)) continue;
        // Several enrolled seats may share one account. A successful account
        // read spaces all of them, including a different seat after restart.
        const accountStateKey = `account:${accountKey}`;
        let accountState = this.pollState.get(accountStateKey) ?? null;
        try { accountState ??= loadBackoffState(path.join(stateDir,
          `claude_oauth_usage.account-${accountKey.slice(7)}.backoff.json`), startedAt); } catch { /* Memory remains authoritative this run. */ }
        if (accountState?.nextAllowedAt && Date.parse(accountState.nextAllowedAt) > startedAt.getTime()) {
          counters.planLimitReadingsSkippedBackoff++;
          continue;
        }
        counters.accountsChecked++;
        const credential = await (this.options.readCredential
          ? this.options.readCredential(configDir, startedAt, this.abort.signal)
          : readCredential(configDir, this.defaultDir, startedAt, this.abort.signal,
            Boolean(this.env.CLAUDE_CONFIG_DIR && path.resolve(this.env.CLAUDE_CONFIG_DIR) === configDir),
            () => { spawnCount++; }));
        if (!credential || credential.expiresAt <= this.now().getTime() + REQUEST_TIMEOUT_MS)
          throw new UsageError("no_credential");
        const get = async (endpoint: UsagePath) => {
          const response = await (this.options.request ?? requestUsage)(endpoint, credential.accessToken, this.abort.signal);
          bytesRead += Buffer.byteLength(response.body);
          retryAfterMs = Math.max(retryAfterMs, response.retryAfterMs ?? 0);
          if (response.status !== 200) throw new UsageError(response.status === 401 ? "no_credential" : "provider_error");
          return response.body;
        };
        // Usage has no identity of its own. Check the same token's profile
        // before attributing it to the UUID in the seat's event auth state.
        const profile = json(await get("/api/oauth/profile"));
        const actual = record(profile.account);
        if (actual.uuid !== account.uuid || (account.email &&
            (typeof actual.email !== "string" || actual.email.toLowerCase() !== account.email.toLowerCase())) ||
            (account.organization && record(profile.organization).uuid !== account.organization))
          throw new UsageError("account_mismatch");
        const parsed = parseClaudeUsageResponse(await get("/api/oauth/usage"));
        if (parsed.kind !== "ok" || !parsed.windows.length) throw new UsageError("parse_failed");
        // Scoped buckets stay in the local receipt. The cloud contract for
        // this reader is the account's five-hour and seven-day windows.
        const accountWindows = parsed.windows.filter(window => window.window === "five_hour" || window.window === "seven_day");
        if (!accountWindows.length) throw new UsageError("parse_failed");
        // A seat may rotate while the requests are outstanding. Its new UUID
        // cannot inherit the previous account's response.
        if (JSON.stringify(this.account(configDir)) !== JSON.stringify(account)) throw new UsageError("account_mismatch");
        if (this.stopped) break;
        windows = parsed.windows;
        const observedAt = this.now().toISOString();
        for (const window of accountWindows) {
          counters.planLimitReadingsSeen++;
          if (this.emitter.observe({ source: "claude_code", accountKey, observedAt,
            planLimitSource: "claude_usage_api", planType: planType(credential.planType),
            window: { window: window.window, minutes: window.windowMinutes ?? undefined,
              usedPercent: window.usedPercent, resetsAt: window.resetsAt! } })) counters.planLimitReadingsEmitted++;
        }
        readAccounts.add(accountKey);
      } catch (failure) {
        error = failure instanceof UsageError ? failure.code : "io_error";
        windows = [];
        if (error === "no_account") counters.planLimitReadingsSkippedNoAccount++;
        else if (error === "no_credential") counters.planLimitReadingsSkippedNoCredential++;
        else if (error === "account_mismatch") counters.planLimitReadingsSkippedAccountMismatch++;
        else counters.planLimitReadingsFailed++;
      }
      if (this.stopped) break;
      const completed = this.now();
      const backoff = computeAdapterBackoff({ previous, failed: error !== "none", now: completed });
      backoff.nextAllowedAt = new Date(Math.max(completed.getTime() + CLAUDE_PLAN_LIMIT_POLL_MS,
        Date.parse(backoff.nextAllowedAt ?? completed.toISOString()), completed.getTime() + retryAfterMs)).toISOString();
      this.pollState.set(profileId, backoff);
      if (error === "none" && accountKey) this.pollState.set(`account:${accountKey}`, backoff);
      try {
        persistBackoffState(backoff, backoffFile, stateDir);
        if (error === "none" && accountKey) persistBackoffState(backoff,
          path.join(stateDir, `claude_oauth_usage.account-${accountKey.slice(7)}.backoff.json`), stateDir);
        persistLatestAdapterReceipt(buildCapacityAdapterReceipt({ adapter: "claude_oauth_usage", profileId,
          ok: error === "none", phase: error === "none" ? "complete" : "rate_limits_read", errorCode: error,
          startedAt, now: completed, windows, providerVersion: null, maxAgeMs: 15 * 60_000,
          resourceCost: { spawnCount, bytesWritten: 0, bytesRead, exitCode: null, signal: null } }), this.options.home);
      } catch { /* In-memory backoff still protects the daemon; no raw errors are logged. */ }
    }
    this.latest = counters;
    for (const key of Object.keys(counters) as Array<keyof typeof counters>) this.totals[key] += counters[key];
    return counters;
  }
}
