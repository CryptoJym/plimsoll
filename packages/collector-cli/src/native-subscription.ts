import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";
import { resolveCollectorHome, resolveGrokHome } from "./collector-home";
import { readNativeLedger, type NativeLedgerReading } from "./native-ledger";
import { nativeAccountLinkEvidence, nativeProviderAccountLinkEvidence, readExistingNativeAccountSalt, type NativeAccountLinkEvidence, type NativeIdentityProvenance } from "./native-account-links";

export const NATIVE_SUBSCRIPTION_SCHEMA = "plimsoll.native-subscription.v1" as const;

export type NativeProvider = "openai" | "anthropic" | "xai" | "cursor";
export type NativeEvidenceState = "verified" | "partial" | "unknown";
export type NativeFreshness = "fresh" | "stale" | "unknown";
export type NativeSurface =
  | "native_account_collector"
  | "local_plan_metadata"
  | "cursor_tracking_db"
  | "local_thread_telemetry"
  | "dashboard_export"
  | "compliance_api";

type JsonRecord = Record<string, unknown>;

export type NativeSubscriptionPaths = {
  nativeUsageFile: string;
  codexAuthFile: string;
  claudeConfigFile: string;
  claudeSeatsRoot?: string;
  cursorCliConfigFile: string;
  cursorTrackingDb: string;
  grokConfigFile: string;
  collectorDb: string;
  nativeSubscriptionDb: string;
  dashboardEvidenceFile: string;
};

export type NativeQuotaSnapshot = {
  provider: NativeProvider;
  accountRef: string | null;
  planType: string | null;
  surface: NativeSurface;
  windowSlot: string;
  windowLabel: string | null;
  modelScope: string | null;
  usedPercent: number | null;
  remainingPercent: number | null;
  resetAt: string | null;
  resetPrecision: string | null;
  observedAt: string | null;
  freshness: NativeFreshness;
  evidenceState: NativeEvidenceState;
  sourceDigest: string;
};

export type NativeThreadObservation = {
  provider: NativeProvider;
  threadId: string;
  model: string | null;
  firstSeenAt: string | null;
  lastSeenAt: string | null;
  eventCount: number;
  tokenState: NativeEvidenceState;
  inputTokens: number | null;
  outputTokens: number | null;
  source: NativeSurface;
  observedAt: string | null;
  sourceDigest: string;
};

export type NativeExportSurface = {
  state: NativeEvidenceState;
  access: "connected" | "not_connected" | "unknown";
  route: string;
  scope: string;
  note: string;
};

export type NativeThreadTelemetry = {
  state: NativeEvidenceState;
  eventCount: number | null;
  sources: string[];
  latestObservedAt: string | null;
  tokenState: NativeEvidenceState;
  note: string;
};

export type NativeAccountObservation = {
  provider: NativeProvider;
  accountRef: string | null;
  authenticated: boolean;
  planType: string | null;
  proof: string | null;
  state: string;
  observedAt: string | null;
  freshness: NativeFreshness;
  subscriptionState: NativeEvidenceState;
  errorCodes: string[];
  accountLink?: NativeAccountLinkEvidence | null;
  accountLinks?: NativeAccountLinkEvidence[];
};

export type NativeProviderSurface = {
  provider: NativeProvider;
  product: string;
  accountCount: number;
  authenticatedAccountCount: number;
  planTypes: string[];
  accountStates: Record<string, number>;
  subscription: {
    state: NativeEvidenceState;
    authority: "native_local" | "unknown";
    note: string;
  };
  quota: {
    state: NativeEvidenceState;
    snapshotCount: number;
    freshSnapshotCount: number;
    note: string;
  };
  threadTelemetry: NativeThreadTelemetry;
  dashboardExport: NativeExportSurface;
  complianceExport: NativeExportSurface;
  notes: string[];
};

export type NativeSubscriptionInspection = {
  schema: typeof NATIVE_SUBSCRIPTION_SCHEMA;
  observedAt: string;
  collector: {
    available: boolean;
    generatedAt: string | null;
    state: string;
    coverage: Record<string, number>;
    freshness: NativeFreshness;
  };
  metadata: {
    codexPlanType: string | null;
    claudePlanType: string | null;
    claudeBillingType: string | null;
    claudeOrganizationType: string | null;
    cursorConfigAvailable: boolean;
    grokTelemetryEnabled: boolean;
    grokExternalOtelEnabled: boolean;
  };
  cursorTracking: {
    available: boolean;
    conversationCount: number;
    observationCount: number;
    modelCount: number;
    firstSeenAt: string | null;
    lastSeenAt: string | null;
    tokenState: NativeEvidenceState;
    freshness: NativeFreshness;
  };
  localThreadTelemetry: {
    available: boolean;
    totalEvents: number | null;
    bySource: Record<string, number>;
    latestObservedAt: string | null;
    state: NativeEvidenceState;
    note: string;
    coverage: "recent_window" | "incremental" | "unknown";
  };
  accounts: NativeAccountObservation[];
  ledgerReading: NativeLedgerReading;
  providers: NativeProviderSurface[];
  quotaSnapshots: NativeQuotaSnapshot[];
  threadObservations: NativeThreadObservation[];
};

export type NativeSubscriptionImportReceipt = {
  schema: typeof NATIVE_SUBSCRIPTION_SCHEMA;
  quotaRecords: number;
  quotaInserted: number;
  quotaReplayed: number;
  quotaRevised: number;
  threadRecords: number;
  threadInserted: number;
  threadReplayed: number;
  threadRevised: number;
};

function object(value: unknown): JsonRecord {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as JsonRecord
    : {};
}

function stringValue(value: unknown, max = 256): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed && trimmed.length <= max ? trimmed : null;
}

function numberValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() && /^-?\d+(?:\.\d+)?$/.test(value.trim())) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function percentage(value: unknown): number | null {
  const parsed = numberValue(value);
  return parsed !== null && parsed >= 0 && parsed <= 100 ? Number(parsed.toFixed(4)) : null;
}

function iso(value: unknown): string | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    const milliseconds = Math.abs(value) < 10_000_000_000 ? value * 1_000 : value;
    const date = new Date(milliseconds);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }
  if (typeof value === "string" && Number.isFinite(Date.parse(value))) return new Date(Date.parse(value)).toISOString();
  return null;
}

function freshness(value: string | null, now: string): NativeFreshness {
  if (!value) return "unknown";
  const age = Date.parse(now) - Date.parse(value);
  if (!Number.isFinite(age) || age < -5 * 60_000) return "unknown";
  return age <= 15 * 60_000 ? "fresh" : "stale";
}

function digest(value: unknown): string {
  const canonical = (input: unknown): string => {
    if (input === null || typeof input === "string" || typeof input === "boolean") return JSON.stringify(input);
    if (typeof input === "number") return Number.isFinite(input) ? JSON.stringify(input) : "null";
    if (Array.isArray(input)) return `[${input.map(canonical).join(",")}]`;
    if (typeof input === "object") {
      const record = input as Record<string, unknown>;
      return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${canonical(record[key])}`).join(",")}}`;
    }
    return "null";
  };
  return `sha256:${crypto.createHash("sha256").update(canonical(value)).digest("hex")}`;
}

function regularFile(file: string): boolean {
  try {
    const stat = fs.lstatSync(file);
    return stat.isFile() && !stat.isSymbolicLink();
  } catch {
    return false;
  }
}

function readJson(file: string): JsonRecord | null {
  if (!regularFile(file)) return null;
  try {
    const stat = fs.statSync(file);
    if (stat.size > 64 * 1024 * 1024) return null;
    return object(JSON.parse(fs.readFileSync(file, "utf8")));
  } catch {
    return null;
  }
}

function providerFromNative(value: unknown): NativeProvider | null {
  if (value === "claude" || value === "anthropic") return "anthropic";
  if (value === "codex" || value === "openai" || value === "chatgpt") return "openai";
  if (value === "grok" || value === "xai") return "xai";
  if (value === "cursor") return "cursor";
  return null;
}

function accountRef(value: unknown): string | null {
  const raw = stringValue(value, 512);
  return raw ? `native:${crypto.createHash("sha256").update(raw).digest("hex").slice(0, 32)}` : null;
}

/** Only the explicitly named configuration is read. Reject writable or
 * redirected files and ancestors, and bound the read without opening secrets. */
function readClaudeConfig(file: string): JsonRecord | null {
  let fd: number | undefined;
  try {
    if (!path.isAbsolute(file) || /(?:credentials|auth\.json)/i.test(path.basename(file))) return null;
    const uid = process.getuid?.();
    const checkAncestors = () => {
      let directory = path.dirname(file);
      for (let depth = 0; depth < 64; depth += 1) {
        let stat = fs.lstatSync(directory);
        if (stat.isSymbolicLink()) {
          // Only macOS's root-owned /var and /tmp aliases are followed.
          // Every user/profile symlink fails, including for a root agent.
          if (process.platform !== "darwin" || !["/var","/tmp"].includes(directory) || stat.uid !== 0) return false;
          const parent = fs.lstatSync(path.dirname(directory));
          const target = fs.statSync(directory);
          if (!parent.isDirectory() || parent.isSymbolicLink() || parent.uid !== 0 || (parent.mode & 0o022) !== 0 ||
            !target.isDirectory() || target.uid !== 0) return false;
          directory = fs.realpathSync(directory);
          stat = fs.lstatSync(directory);
        }
        const trustedSticky = stat.uid === 0 && (stat.mode & 0o1000) !== 0;
        if (!stat.isDirectory() || stat.isSymbolicLink() ||
          uid !== undefined && stat.uid !== uid && stat.uid !== 0 ||
          (stat.mode & 0o022) !== 0 && !trustedSticky) return false;
        const parent = path.dirname(directory);
        if (parent === directory) return true;
        directory = parent;
      }
      return false;
    };
    if (!checkAncestors()) return null;
    const before = fs.lstatSync(file);
    if (!before.isFile() || before.isSymbolicLink() || (before.mode & 0o022) !== 0 ||
      uid !== undefined && before.uid !== uid || before.size > 1024 * 1024) return null;
    fd = fs.openSync(file,fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const opened = fs.fstatSync(fd);
    if (opened.dev !== before.dev || opened.ino !== before.ino || opened.size > 1024 * 1024) return null;
    const bytes = Buffer.alloc(opened.size);
    let length = 0;
    while (length < bytes.length) {
      const read = fs.readSync(fd,bytes,length,bytes.length - length,length);
      if (!read) return null;
      length += read;
    }
    const after = fs.lstatSync(file);
    if (!checkAncestors() || after.dev !== opened.dev || after.ino !== opened.ino ||
      after.size !== opened.size || (after.mode & 0o022) !== 0 || uid !== undefined && after.uid !== uid) return null;
    return object(JSON.parse(bytes.toString("utf8")));
  } catch { return null; }
  finally { if (fd !== undefined) fs.closeSync(fd); }
}

function claudeAccountIdentity(identity: JsonRecord, source: JsonRecord, paths: NativeSubscriptionPaths,
  localConfig: JsonRecord | null): { value: unknown; reason: string | null; provenance: NativeIdentityProvenance | null } {
  const hasProfile = source.profile !== undefined && source.profile !== null;
  if (hasProfile && (typeof source.profile !== "string" || !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$/.test(source.profile))) {
    return { value: null, reason: "invalid_claude_profile", provenance: null };
  }
  const config = hasProfile ? readClaudeConfig(path.join(paths.claudeSeatsRoot!,source.profile as string,".claude.json")) : localConfig;
  if (!config) return { value: null, reason: "missing_or_unsafe_claude_profile", provenance: null };
  const oauth = object(config.oauthAccount);
  // Exact scalar equality: neither an email-only match nor a tenant UUID is
  // evidence of the personal account UUID. Missing feed metadata cannot prove it.
  const valid = (value: unknown): value is string => typeof value === "string" && value.length > 0 &&
    value.length <= 512 && value === value.trim() && !/[\u0000-\u001f]/.test(value);
  if (!valid(identity.email) || !valid(identity.organizationId) || !valid(oauth.emailAddress) || !valid(oauth.organizationUuid)) {
    return { value: null, reason: "missing_claude_identity_metadata", provenance: null };
  }
  if (identity.email !== oauth.emailAddress || identity.organizationId !== oauth.organizationUuid) {
    return { value: null, reason: "mismatched_claude_profile_identity", provenance: null };
  }
  return { value: oauth.accountUuid, reason: null,
    provenance: hasProfile ? "registered_claude_profile_account_uuid" : "matching_local_claude_account_uuid" };
}

function parseNativeUsageFile(
  payload: JsonRecord | null,
  now: string,
  salt: Buffer | null,
  paths: NativeSubscriptionPaths,
  localClaudeConfig: JsonRecord | null,
): { snapshots: NativeQuotaSnapshot[]; accounts: NativeAccountObservation[] } {
  const snapshots: NativeQuotaSnapshot[] = [];
  const accounts: NativeAccountObservation[] = [];
  if (!payload) return { snapshots, accounts };
  const rawAccounts = Array.isArray(payload.accounts) ? payload.accounts : [];
  rawAccounts.forEach((rawValue) => {
    const raw = object(rawValue);
    const provider = providerFromNative(raw.provider);
    if (!provider) return;
    const identity = object(raw.identity);
    const source = object(raw.source);
    const planType = stringValue(identity.planType) ?? stringValue(raw.planType);
    const authenticated = identity.authenticated === true;
    const proof = stringValue(identity.proof) ?? stringValue(source.proof);
    const state = stringValue(raw.state) ?? "UNKNOWN";
    const observedAt = iso(source.observedAt);
    const identityObservedAt = iso(identity.observedAt) ?? observedAt;
    const accountFreshness = freshness(identityObservedAt, now);
    const ref = accountRef(raw.accountKey);
    const identityValid = authenticated && identity.matchesExpected !== false && Boolean(proof && proof !== "configured_only") && Boolean(ref);
    const subscriptionState: NativeEvidenceState = identityValid && planType && planType.toLowerCase() !== "free" && accountFreshness === "fresh" ? "verified" : "unknown";
    const errorCodes = (Array.isArray(raw.errors) ? raw.errors : []).map((value) => stringValue(object(value).code, 128)).filter((value): value is string => Boolean(value && /^[A-Z0-9_]+$/.test(value)));
    const capturedIdentity = provider === "anthropic" ? claudeAccountIdentity(identity,source,paths,localClaudeConfig) :
      { value: provider === "openai" ? identity.providerAccountId : null, reason: null,
        provenance: provider === "openai" ? "native_feed_provider_account_id" as const : null };
    const linkOptions = { provider, accountRef: ref, nativeIdentity: capturedIdentity.value,
      identityReason: capturedIdentity.reason, identityProvenance: capturedIdentity.provenance,
      authenticated, matchesExpected: identity.matchesExpected, proof, observedAt: identityObservedAt, now };
    const accountLink = nativeAccountLinkEvidence(linkOptions, salt);
    const protectedAccountLink = nativeProviderAccountLinkEvidence(linkOptions);
    const accountLinks = [accountLink,protectedAccountLink].filter((item): item is NativeAccountLinkEvidence => item !== null);
    accounts.push({ provider, accountRef: ref, authenticated, planType, proof, state, observedAt: identityObservedAt, freshness: accountFreshness, subscriptionState, errorCodes, accountLink, accountLinks });

    const evidenceState: NativeEvidenceState = identityValid && source.state === "OK" && state === "OK" && planType ? "verified" : "unknown";
    const buckets = Array.isArray(raw.quotaBuckets) ? raw.quotaBuckets : [];
    let emitted = 0;
    buckets.forEach((bucketValue, bucketIndex) => {
      const bucket = object(bucketValue);
      const bucketObservedAt = iso(bucket.observedAt) ?? observedAt;
      const windows = Array.isArray(bucket.windows) ? bucket.windows : [];
      windows.forEach((windowValue, windowIndex) => {
        const window = object(windowValue);
        const windowObservedAt = iso(window.observedAt) ?? bucketObservedAt;
        const usedPercent = percentage(window.usedPercent);
        const remainingPercent = percentage(window.remainingPercent);
        const windowFreshness = freshness(windowObservedAt, now);
        const resetAt = iso(window.resetAt);
        const resetPassed = Boolean(resetAt && Date.parse(resetAt) <= Date.parse(now));
        const validPercentages = usedPercent !== null || remainingPercent !== null;
        const consistentPercentages = usedPercent === null || remainingPercent === null || Math.abs(usedPercent + remainingPercent - 100) < 0.1;
        const windowEvidence = validPercentages && consistentPercentages && windowFreshness === "fresh" && !resetPassed ? evidenceState : "unknown";
        const slot = stringValue(window.slot) ?? `window_${windowIndex}`;
        const windowSlot = `${stringValue(bucket.limitId) ?? `bucket_${bucketIndex}`}:${slot}`;
        const sourceView = {
          provider,
          accountRef: ref,
          planType,
          surface: "native_account_collector" as const,
          windowSlot,
          windowLabel: stringValue(window.label) ?? stringValue(bucket.label),
          modelScope: stringValue(bucket.modelScope),
          usedPercent,
          remainingPercent,
          resetAt,
          resetPrecision: stringValue(window.resetPrecision),
          observedAt: windowObservedAt,
          evidenceState: windowEvidence,
        };
        snapshots.push({
          ...sourceView,
          freshness: windowFreshness,
          // Sampling time is metadata, not a revision by itself. A repeated
          // reading with the same meter values is a replay; a changed value
          // produces a revision.
          sourceDigest: digest({ ...sourceView, observedAt: null }),
        });
        emitted += 1;
      });
    });
    if (emitted === 0) {
      const sourceView = {
        provider,
        accountRef: ref,
        planType,
        surface: "native_account_collector" as const,
        windowSlot: "UNKNOWN",
        windowLabel: null,
        modelScope: null,
        usedPercent: null,
        remainingPercent: null,
        resetAt: null,
        resetPrecision: null,
        observedAt,
        evidenceState: "unknown" as const,
      };
      snapshots.push({ ...sourceView, freshness: "unknown", sourceDigest: digest({ ...sourceView, observedAt: null }) });
    }
  });
  return { snapshots, accounts };
}

function readGrokFlags(file: string): { telemetryEnabled: boolean; externalOtelEnabled: boolean } {
  if (!regularFile(file)) return { telemetryEnabled: false, externalOtelEnabled: false };
  let content = "";
  try { content = fs.readFileSync(file, "utf8"); } catch { return { telemetryEnabled: false, externalOtelEnabled: false }; }
  const telemetry = /(?:^|\n)\s*telemetry\s*=\s*(true|false)\s*(?:\n|$)/i.exec(content)?.[1]?.toLowerCase() === "true";
  // Generic OTLP variables may belong to Claude/Codex in the same shell. The
  // Grok master switch is the only value that proves Grok's external stream.
  const external = process.env.GROK_EXTERNAL_OTEL === "1" || process.env.GROK_EXTERNAL_OTEL === "true";
  return { telemetryEnabled: telemetry, externalOtelEnabled: external };
}

function readCursorTracking(file: string, now: string): {
  available: boolean;
  conversationCount: number;
  observationCount: number;
  modelCount: number;
  firstSeenAt: string | null;
  lastSeenAt: string | null;
  tokenState: NativeEvidenceState;
  freshness: NativeFreshness;
  observations: NativeThreadObservation[];
} {
  if (!regularFile(file)) return {
    available: false, conversationCount: 0, observationCount: 0, modelCount: 0,
    firstSeenAt: null, lastSeenAt: null, tokenState: "unknown", freshness: "unknown", observations: [],
  };
  let database: Database.Database | null = null;
  try {
    database = new Database(file, { readonly: true, fileMustExist: true });
    const hasTable = Boolean(database.prepare("select 1 from sqlite_master where type='table' and name='ai_code_hashes'").get());
    if (!hasTable) throw new Error("cursor_tracking_table_missing");
    const summary = database.prepare(`select
      count(*) as observationCount,
      count(distinct conversationId) as conversationCount,
      count(distinct model) as modelCount,
      min(timestamp) as firstSeen,
      max(timestamp) as lastSeen
      from ai_code_hashes where conversationId is not null`).get() as JsonRecord;
    const rows = database.prepare(`select conversationId as threadId,
      case when count(distinct model)=1 then min(model) else null end as model,
      min(timestamp) as firstSeen, max(timestamp) as lastSeen, count(*) as eventCount
      from ai_code_hashes where conversationId is not null
      group by conversationId order by lastSeen desc limit 10000`).all() as JsonRecord[];
    const observations: NativeThreadObservation[] = [];
    for (const row of rows) {
      const threadId = stringValue(row.threadId, 256);
      if (!threadId) continue;
      const firstSeenAt = iso(row.firstSeen);
      const lastSeenAt = iso(row.lastSeen);
      const sourceView: NativeThreadObservation = {
        provider: "cursor",
        threadId,
        model: stringValue(row.model),
        firstSeenAt,
        lastSeenAt,
        eventCount: Math.max(0, Math.floor(numberValue(row.eventCount) ?? 0)),
        tokenState: "unknown",
        inputTokens: null,
        outputTokens: null,
        source: "cursor_tracking_db",
        observedAt: lastSeenAt,
        sourceDigest: "",
      };
      sourceView.sourceDigest = digest({ ...sourceView, sourceDigest: undefined });
      observations.push(sourceView);
    }
    const firstSeenAt = iso(summary.firstSeen);
    const lastSeenAt = iso(summary.lastSeen);
    return {
      available: true,
      conversationCount: Math.max(0, Math.floor(numberValue(summary.conversationCount) ?? 0)),
      observationCount: Math.max(0, Math.floor(numberValue(summary.observationCount) ?? 0)),
      modelCount: Math.max(0, Math.floor(numberValue(summary.modelCount) ?? 0)),
      firstSeenAt,
      lastSeenAt,
      tokenState: "unknown",
      freshness: freshness(lastSeenAt, now),
      observations,
    };
  } catch {
    return {
      available: false, conversationCount: 0, observationCount: 0, modelCount: 0,
      firstSeenAt: null, lastSeenAt: null, tokenState: "unknown", freshness: "unknown", observations: [],
    };
  } finally {
    database?.close();
  }
}

function summarizeLocalThreadTelemetry(reading: NativeLedgerReading): NativeSubscriptionInspection["localThreadTelemetry"] {
  if (reading.state === "unknown") return { available: false, totalEvents: null, bySource: {}, latestObservedAt: null, state: "unknown", coverage: "unknown", note: reading.note };
  const bySource: Record<string, number> = {};
  let latestObservedAt: string | null = null;
  for (const event of reading.events) {
    bySource[event.source] = (bySource[event.source] ?? 0) + 1;
    if (event.observedAt && (!latestObservedAt || event.observedAt > latestObservedAt)) latestObservedAt = event.observedAt;
  }
  return { available: true, totalEvents: reading.events.length, bySource, latestObservedAt,
    state: "partial", coverage: reading.coverage, note: reading.note };
}

function providerProduct(provider: NativeProvider): string {
  return provider === "openai" ? "ChatGPT Pro/Max / Codex" :
    provider === "anthropic" ? "Claude Pro/Max / Claude Code" :
      provider === "cursor" ? "Cursor individual subscription" : "Grok subscription";
}

function exportSurface(provider: NativeProvider, kind: "dashboard" | "compliance"): NativeExportSurface {
  if (kind === "dashboard") {
    if (provider === "cursor") return {
      state: "unknown", access: "not_connected", route: "Cursor individual usage dashboard/export",
      scope: "individual account", note: "Dashboard route is documented; no export has been imported in this run.",
    };
    if (provider === "openai") return {
      state: "unknown", access: "not_connected", route: "ChatGPT/Codex usage dashboard",
      scope: "personal subscription", note: "Native dashboard access is not read by the local collector.",
    };
    return {
      state: "unknown", access: "unknown", route: provider === "anthropic" ? "Claude usage surface" : "Grok account usage surface",
      scope: "personal subscription", note: "No dashboard export was imported in this run.",
    };
  }
  if (provider === "openai") return {
    state: "unknown", access: "unknown", route: "OpenAI Compliance API",
    scope: "eligibility/admin-controlled", note: "The documented surface is separate from the personal subscription; entitlement is UNKNOWN.",
  };
  if (provider === "anthropic") return {
    state: "unknown", access: "unknown", route: "Claude Code Analytics/Admin API",
    scope: "organization admin", note: "Individual-account admin access is not proven; subscription usage stays on the local native source.",
  };
  if (provider === "cursor") return {
    state: "unknown", access: "unknown", route: "Cursor team Analytics/Admin API",
    scope: "team admin", note: "Teams are out of scope here; individual dashboard export remains the relevant route.",
  };
  return {
    state: "unknown", access: "unknown", route: "xAI management/usage API",
    scope: "API management", note: "No documented personal subscription compliance export was verified.",
  };
}

function accountSummary(
  provider: NativeProvider,
  accounts: NativeAccountObservation[],
  snapshots: NativeQuotaSnapshot[],
  telemetry: NativeThreadTelemetry,
): NativeProviderSurface {
  const owned = accounts.filter((account) => account.provider === provider);
  const providerSnapshots = snapshots.filter((snapshot) => snapshot.provider === provider);
  const planTypes = [...new Set(owned.map((account) => account.planType).filter((value): value is string => Boolean(value)))].sort();
  const accountStates: Record<string, number> = {};
  for (const account of owned) accountStates[account.state] = (accountStates[account.state] ?? 0) + 1;
  const authenticatedAccountCount = owned.filter((account) => account.authenticated).length;
  const verifiedAccounts = owned.filter((account) => account.subscriptionState === "verified").length;
  const freshSnapshotCount = providerSnapshots.filter((snapshot) => snapshot.evidenceState === "verified").length;
  const subscriptionState: NativeEvidenceState = verifiedAccounts > 0 && verifiedAccounts === owned.length ? "verified" : verifiedAccounts > 0 ? "partial" : "unknown";
  const quotaState: NativeEvidenceState = freshSnapshotCount > 0 && freshSnapshotCount === providerSnapshots.length ? "verified" : freshSnapshotCount > 0 ? "partial" : "unknown";
  const notes: string[] = [];
  if (provider === "openai") notes.push("Native account quota is subscription evidence; OpenAI API organization usage is kept separate.");
  if (provider === "anthropic") notes.push("Claude local quota windows are subscription evidence; individual Analytics/Admin API access remains UNKNOWN.");
  if (provider === "cursor") notes.push("Cursor local tracking records conversations and models but no token counters; token usage remains UNKNOWN until a dashboard export is imported.");
  if (provider === "xai") notes.push("Grok native included credits are observed locally; external OpenTelemetry token metrics are not enabled in the inspected config.");
  return {
    provider,
    product: providerProduct(provider),
    accountCount: owned.length,
    authenticatedAccountCount,
    planTypes,
    accountStates,
    subscription: {
      state: subscriptionState,
      authority: verifiedAccounts > 0 ? "native_local" : "unknown",
      note: verifiedAccounts > 0 ? "Authenticated native account metadata and quota windows were observed locally." : "No authenticated native subscription proof was observed.",
    },
    quota: {
      state: quotaState,
      snapshotCount: providerSnapshots.length,
      freshSnapshotCount,
      note: providerSnapshots.length > 0 ? "Quota values are native account readings; missing values are UNKNOWN." : "No native quota window was observed.",
    },
    threadTelemetry: telemetry,
    dashboardExport: exportSurface(provider, "dashboard"),
    complianceExport: exportSurface(provider, "compliance"),
    notes,
  };
}

export function defaultNativeSubscriptionPaths(home = os.homedir()): NativeSubscriptionPaths {
  const plimsollHome = resolveCollectorHome({ homeDir: home }).home;
  return {
    nativeUsageFile: path.join(home, "Library/Memory/ops/data/native-account-usage.json"),
    codexAuthFile: path.join(home, ".codex/auth.json"),
    claudeConfigFile: path.join(home, ".claude.json"),
    claudeSeatsRoot: path.join(home, ".claude-seats"),
    cursorCliConfigFile: path.join(home, ".cursor/cli-config.json"),
    cursorTrackingDb: path.join(home, ".cursor/ai-tracking/ai-code-tracking.db"),
    grokConfigFile: path.join(resolveGrokHome({ homeDir: home }).home, "config.toml"),
    collectorDb: path.join(plimsollHome, "work-ledger.sqlite"),
    nativeSubscriptionDb: path.join(plimsollHome, "native-subscription.sqlite"),
    dashboardEvidenceFile: path.join(plimsollHome, "subscription-dashboard-evidence.json"),
  };
}

export function inspectNativeSubscriptionSurfaces(
  options: { paths?: Partial<NativeSubscriptionPaths>; now?: string; ledgerReading?: NativeLedgerReading; identityEvidenceOnly?: boolean } = {},
): NativeSubscriptionInspection {
  const paths = { ...defaultNativeSubscriptionPaths(), ...options.paths };
  const observedAt = iso(options.now) ?? new Date().toISOString();
  const rawNativePayload = readJson(paths.nativeUsageFile);
  const nativePayload = rawNativePayload && Array.isArray(rawNativePayload.accounts) && iso(rawNativePayload.generatedAt) ? rawNativePayload : null;
  const claudeConfig = readClaudeConfig(paths.claudeConfigFile);
  const salt = nativePayload ? readExistingNativeAccountSalt(path.dirname(paths.collectorDb)) : null;
  let parsed: ReturnType<typeof parseNativeUsageFile>;
  try { parsed = parseNativeUsageFile(nativePayload, observedAt, salt, paths, claudeConfig); }
  finally { salt?.fill(0); }
  const cursorTracking: ReturnType<typeof readCursorTracking> = options.identityEvidenceOnly ? {
    available: false, conversationCount: 0, observationCount: 0, modelCount: 0,
    firstSeenAt: null, lastSeenAt: null, tokenState: "unknown", freshness: "unknown", observations: [],
  } : readCursorTracking(paths.cursorTrackingDb, observedAt);
  const ledgerReading = options.ledgerReading ?? readNativeLedger(paths.collectorDb);
  const localThreadTelemetry = summarizeLocalThreadTelemetry(ledgerReading);
  const claudeOauth = object(claudeConfig?.oauthAccount);
  const cursorConfig = options.identityEvidenceOnly ? null : readJson(paths.cursorCliConfigFile);
  const grokFlags = options.identityEvidenceOnly ? {telemetryEnabled:false,externalOtelEnabled:false} : readGrokFlags(paths.grokConfigFile);
  const providerTelemetry = (provider: NativeProvider): NativeThreadTelemetry => {
    const source = provider === "openai" ? "codex" : provider === "anthropic" ? "claude_code" : provider === "xai" ? "grok" : "cursor";
    if (provider === "cursor") {
      return {
        state: cursorTracking.available && cursorTracking.observationCount > 0 ? "partial" : cursorTracking.available ? "partial" : "unknown",
        eventCount: cursorTracking.observationCount,
        sources: cursorTracking.available ? ["cursor_tracking_db"] : [],
        latestObservedAt: cursorTracking.lastSeenAt,
        tokenState: "unknown",
        note: "Conversation/model observations are present; the local database has no token counters.",
      };
    }
    const count = localThreadTelemetry.available ? localThreadTelemetry.bySource[source] ?? 0 : null;
    const sourceEvents = ledgerReading.events.filter((event) => event.source === source);
    const tokenEvents = sourceEvents.filter((event) => event.inputTokens !== null || event.outputTokens !== null);
    return {
      state: count !== null && count > 0 ? "partial" : "unknown",
      eventCount: count,
      sources: count !== null && count > 0 ? [source] : [],
      latestObservedAt: sourceEvents.map((event) => event.observedAt).filter((value): value is string => value !== null).sort().at(-1) ?? null,
      tokenState: tokenEvents.length > 0 ? "partial" : "unknown",
      note: count !== null && count > 0 ? "Bounded local thread events observed; historical and fleet completeness are UNKNOWN. Subscription quota cannot be converted to token totals or cash." : localThreadTelemetry.available ? "No events for this source in the bounded sample; total usage is UNKNOWN." : localThreadTelemetry.note,
    };
  };
  const telemetryByProvider = new Map<NativeProvider, NativeThreadTelemetry>([
    ["openai", providerTelemetry("openai")],
    ["anthropic", providerTelemetry("anthropic")],
    ["cursor", providerTelemetry("cursor")],
    ["xai", providerTelemetry("xai")],
  ]);
  const providers = (["openai", "anthropic", "cursor", "xai"] as NativeProvider[]).map((provider) =>
    accountSummary(provider, parsed.accounts, parsed.snapshots, telemetryByProvider.get(provider)!));
  const dashboardEvidence = object(options.identityEvidenceOnly ? null : readJson(paths.dashboardEvidenceFile)?.providers);
  for (const provider of providers) {
    const dashboard = object(dashboardEvidence[provider.provider]);
    const matchedAccount = parsed.accounts.find((account) => account.provider === provider.provider && account.accountRef === dashboard.accountRef);
    const dashboardCheckedAt=iso(dashboard.checkedAt);
    const dashboardAge=dashboardCheckedAt ? Date.parse(observedAt)-Date.parse(dashboardCheckedAt) : NaN;
    if (matchedAccount && dashboard.nativeIdentityMatched === true && dashboardAge >= -5*60_000 && dashboardAge <= 24*60*60_000) {
      provider.dashboardExport = {
        state: dashboard.csvExportState === "verified" ? "verified" : "partial",
        access: dashboard.csvExportState === "verified" ? "connected" : "unknown",
        scope: "individual subscription",route: provider.dashboardExport.route,
        note: dashboard.csvExportState === "verified" ? `CSV export was verified in the native individual dashboard at ${dashboardCheckedAt}; import freshness is reported separately.` : `Native dashboard was inspected at ${dashboardCheckedAt}; token export availability remains UNKNOWN.`,
      };
      if (provider.accountCount === 1 && dashboard.dashboardState === "verified") {
        provider.subscription.state = "verified";
        provider.subscription.authority = "native_local";
        provider.subscription.note = "Plan was verified in the account-matched native dashboard; this does not certify settled payment.";
      }
    }
  }
  const collectorGeneratedAt = iso(nativePayload?.generatedAt);
  const coverage = object(nativePayload?.coverage);
  const coverageNumbers: Record<string, number> = {};
  for (const [key, value] of Object.entries(coverage)) {
    const parsedValue = numberValue(value);
    if (parsedValue !== null) coverageNumbers[key] = Math.floor(parsedValue);
  }
  return {
    schema: NATIVE_SUBSCRIPTION_SCHEMA,
    observedAt,
    collector: {
      available: Boolean(nativePayload),
      generatedAt: collectorGeneratedAt,
      state: stringValue(nativePayload?.state) ?? "UNKNOWN",
      coverage: coverageNumbers,
      freshness: freshness(collectorGeneratedAt, observedAt),
    },
    metadata: {
      codexPlanType: (() => {
        const plans = [...new Set(parsed.accounts.filter(account => account.provider === "openai").map(account => account.planType).filter(Boolean))];
        return plans.length === 1 ? plans[0] : null;
      })(),
      claudePlanType: stringValue(claudeOauth.organizationType),
      claudeBillingType: stringValue(claudeOauth.billingType),
      claudeOrganizationType: stringValue(claudeOauth.organizationType),
      cursorConfigAvailable: Boolean(cursorConfig),
      grokTelemetryEnabled: grokFlags.telemetryEnabled,
      grokExternalOtelEnabled: grokFlags.externalOtelEnabled,
    },
    cursorTracking: {
      available: cursorTracking.available,
      conversationCount: cursorTracking.conversationCount,
      observationCount: cursorTracking.observationCount,
      modelCount: cursorTracking.modelCount,
      firstSeenAt: cursorTracking.firstSeenAt,
      lastSeenAt: cursorTracking.lastSeenAt,
      tokenState: cursorTracking.tokenState,
      freshness: cursorTracking.freshness,
    },
    localThreadTelemetry,
    ledgerReading,
    accounts: parsed.accounts,
    providers,
    quotaSnapshots: parsed.snapshots,
    threadObservations: cursorTracking.observations,
  };
}

export function ensureNativeSubscriptionSchema(database: Database.Database): void {
  database.exec(`
    create table if not exists native_subscription_snapshots (
      snapshot_key text primary key,
      source_digest text not null,
      revision integer not null default 1 check (revision >= 1),
      provider text not null check (provider in ('openai','anthropic','xai','cursor')),
      account_ref text,
      plan_type text,
      surface text not null check (surface in ('native_account_collector','local_plan_metadata','cursor_tracking_db','local_thread_telemetry','dashboard_export','compliance_api')),
      window_slot text not null,
      window_label text,
      model_scope text,
      used_percent real,
      remaining_percent real,
      reset_at text,
      reset_precision text,
      observed_at text,
      freshness text not null check (freshness in ('fresh','stale','unknown')),
      evidence_state text not null check (evidence_state in ('verified','partial','unknown')),
      updated_at text not null
    );
    create index if not exists idx_native_subscription_provider
      on native_subscription_snapshots (provider, account_ref, observed_at);
    create table if not exists native_thread_observations (
      observation_key text primary key,
      source_digest text not null,
      revision integer not null default 1 check (revision >= 1),
      provider text not null check (provider in ('openai','anthropic','xai','cursor')),
      thread_id text not null,
      model text,
      first_seen_at text,
      last_seen_at text,
      event_count integer not null check (event_count >= 0),
      token_state text not null check (token_state in ('verified','partial','unknown')),
      input_tokens integer,
      output_tokens integer,
      source text not null check (source in ('native_account_collector','local_plan_metadata','cursor_tracking_db','local_thread_telemetry','dashboard_export','compliance_api')),
      observed_at text,
      updated_at text not null
    );
    create index if not exists idx_native_thread_provider
      on native_thread_observations (provider, thread_id);
  `);
}

function quotaSnapshotKey(snapshot: NativeQuotaSnapshot): string {
  return digest(["native_subscription_snapshot", snapshot.provider, snapshot.accountRef, snapshot.surface, snapshot.windowSlot, snapshot.windowLabel, snapshot.modelScope]);
}

function threadObservationKey(observation: NativeThreadObservation): string {
  return digest(["native_thread_observation", observation.provider, observation.threadId, observation.source]);
}

export function importNativeSubscription(
  database: Database.Database,
  inspection: NativeSubscriptionInspection,
  now = new Date().toISOString(),
): NativeSubscriptionImportReceipt {
  ensureNativeSubscriptionSchema(database);
  const quotaInsert = database.prepare(`insert into native_subscription_snapshots (
    snapshot_key, source_digest, revision, provider, account_ref, plan_type, surface,
    window_slot, window_label, model_scope, used_percent, remaining_percent, reset_at,
    reset_precision, observed_at, freshness, evidence_state, updated_at
  ) values (@snapshotKey,@sourceDigest,1,@provider,@accountRef,@planType,@surface,
    @windowSlot,@windowLabel,@modelScope,@usedPercent,@remainingPercent,@resetAt,
    @resetPrecision,@observedAt,@freshness,@evidenceState,@updatedAt)
  on conflict(snapshot_key) do update set
    revision = native_subscription_snapshots.revision + case when native_subscription_snapshots.source_digest = excluded.source_digest then 0 else 1 end,
    source_digest=excluded.source_digest, plan_type=excluded.plan_type,
    window_label=excluded.window_label, model_scope=excluded.model_scope,
    used_percent=excluded.used_percent, remaining_percent=excluded.remaining_percent,
    reset_at=excluded.reset_at, reset_precision=excluded.reset_precision,
    observed_at=excluded.observed_at, freshness=excluded.freshness,
    evidence_state=excluded.evidence_state, updated_at=excluded.updated_at`);
  const threadInsert = database.prepare(`insert into native_thread_observations (
    observation_key, source_digest, revision, provider, thread_id, model,
    first_seen_at, last_seen_at, event_count, token_state, input_tokens,
    output_tokens, source, observed_at, updated_at
  ) values (@observationKey,@sourceDigest,1,@provider,@threadId,@model,
    @firstSeenAt,@lastSeenAt,@eventCount,@tokenState,@inputTokens,
    @outputTokens,@source,@observedAt,@updatedAt)
  on conflict(observation_key) do update set
    revision = native_thread_observations.revision + case when native_thread_observations.source_digest = excluded.source_digest then 0 else 1 end,
    source_digest=excluded.source_digest, model=excluded.model,
    first_seen_at=excluded.first_seen_at, last_seen_at=excluded.last_seen_at,
    event_count=excluded.event_count, token_state=excluded.token_state,
    input_tokens=excluded.input_tokens, output_tokens=excluded.output_tokens,
    observed_at=excluded.observed_at, updated_at=excluded.updated_at`);
  let quotaInserted = 0;
  let quotaReplayed = 0;
  let quotaRevised = 0;
  let threadInserted = 0;
  let threadReplayed = 0;
  let threadRevised = 0;
  const run = database.transaction(() => {
    for (const snapshot of inspection.quotaSnapshots) {
      const key = quotaSnapshotKey(snapshot);
      const existing = database.prepare("select source_digest as sourceDigest from native_subscription_snapshots where snapshot_key=?").get(key) as { sourceDigest: string } | undefined;
      quotaInsert.run({
        snapshotKey: key,
        sourceDigest: snapshot.sourceDigest,
        provider: snapshot.provider,
        accountRef: snapshot.accountRef,
        planType: snapshot.planType,
        surface: snapshot.surface,
        windowSlot: snapshot.windowSlot,
        windowLabel: snapshot.windowLabel,
        modelScope: snapshot.modelScope,
        usedPercent: snapshot.usedPercent,
        remainingPercent: snapshot.remainingPercent,
        resetAt: snapshot.resetAt,
        resetPrecision: snapshot.resetPrecision,
        observedAt: snapshot.observedAt,
        freshness: snapshot.freshness,
        evidenceState: snapshot.evidenceState,
        updatedAt: now,
      });
      if (!existing) quotaInserted += 1;
      else if (existing.sourceDigest === snapshot.sourceDigest) quotaReplayed += 1;
      else quotaRevised += 1;
    }
    for (const observation of inspection.threadObservations) {
      const key = threadObservationKey(observation);
      const existing = database.prepare("select source_digest as sourceDigest from native_thread_observations where observation_key=?").get(key) as { sourceDigest: string } | undefined;
      threadInsert.run({
        observationKey: key,
        sourceDigest: observation.sourceDigest,
        provider: observation.provider,
        threadId: observation.threadId,
        model: observation.model,
        firstSeenAt: observation.firstSeenAt,
        lastSeenAt: observation.lastSeenAt,
        eventCount: observation.eventCount,
        tokenState: observation.tokenState,
        inputTokens: observation.inputTokens,
        outputTokens: observation.outputTokens,
        source: observation.source,
        observedAt: observation.observedAt,
        updatedAt: now,
      });
      if (!existing) threadInserted += 1;
      else if (existing.sourceDigest === observation.sourceDigest) threadReplayed += 1;
      else threadRevised += 1;
    }
  });
  run();
  return {
    schema: NATIVE_SUBSCRIPTION_SCHEMA,
    quotaRecords: inspection.quotaSnapshots.length,
    quotaInserted,
    quotaReplayed,
    quotaRevised,
    threadRecords: inspection.threadObservations.length,
    threadInserted,
    threadReplayed,
    threadRevised,
  };
}

export function readNativeSubscriptionCounts(database: Database.Database): {
  quotaSnapshots: number;
  threadObservations: number;
  providers: Record<string, number>;
} {
  ensureNativeSubscriptionSchema(database);
  const quotaSnapshots = Number((database.prepare("select count(*) as count from native_subscription_snapshots").get() as JsonRecord).count ?? 0);
  const threadObservations = Number((database.prepare("select count(*) as count from native_thread_observations").get() as JsonRecord).count ?? 0);
  const providers: Record<string, number> = {};
  const rows = database.prepare("select provider, count(*) as count from native_subscription_snapshots group by provider").all() as JsonRecord[];
  for (const row of rows) {
    const provider = stringValue(row.provider, 32);
    if (provider) providers[provider] = Number(row.count ?? 0);
  }
  return { quotaSnapshots, threadObservations, providers };
}
