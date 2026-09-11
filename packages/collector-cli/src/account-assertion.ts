import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type Database from "better-sqlite3";
import { z } from "zod";

/**
 * Versioned account continuity contract.
 *
 * The value in an assertion is deliberately an actor hash, never the native
 * provider account id.  The salt is issued by the hosted tenant and is not
 * part of an event, receipt, or upload.  A device with no tenant salt is
 * deliberately unallocated; it must never silently fall back to a local
 * random salt (which would make cross-host grouping impossible).
 */
export const ACCOUNT_ASSERTION_SCHEMA = "account-assertion/v1" as const;
export const ACCOUNT_ASSERTION_STATE_KEY = "account_assertion_adapters_v1" as const;
export const ACCOUNT_ASSERTION_SALT_FILE = "account-assertion.salt" as const;
export const ACCOUNT_ASSERTION_SALT_META_FILE = "account-assertion.salt.meta.json" as const;
export const ACCOUNT_ASSERTION_SOURCES = ["codex", "claude_code", "conductor"] as const;
export type AccountAssertionSource = (typeof ACCOUNT_ASSERTION_SOURCES)[number];

const digest = z.string().regex(/^sha256:[a-f0-9]{64}$/);
const timestamp = z.string().datetime({ offset: true });
const ROOT_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

export const accountAssertionV1Schema = z.object({
  actorHash: digest,
  validFrom: timestamp,
  validUntil: timestamp.nullable(),
  evidenceRef: digest,
  schema: z.literal(ACCOUNT_ASSERTION_SCHEMA),
  source: z.enum(ACCOUNT_ASSERTION_SOURCES),
}).strict().superRefine((value, context) => {
  if (value.validUntil !== null && Date.parse(value.validUntil) <= Date.parse(value.validFrom)) {
    context.addIssue({ code: "custom", path: ["validUntil"], message: "validUntil must be after validFrom" });
  }
});
export type AccountAssertionV1 = z.infer<typeof accountAssertionV1Schema>;
export const accountAssertionSchema = accountAssertionV1Schema;

export type AccountAssertionAdapterState = {
  schema: "account-assertion-adapters/v1";
  adapters: Record<AccountAssertionSource, {
    enabled: boolean;
    lastAssertionAt: string | null;
    rootDigests: string[];
  }>;
  /** Enrollment records live in the same maintenance key, not the event ledger. */
  bindings: Record<AccountAssertionSource, AccountAssertionBindingState[]>;
  salt?: { tenantId: string; version: string };
};

export type AccountAssertionBindingState = {
  rootId: string;
  bindingDigest: string;
  /** Digest of the stable Codex binding epoch; no provider credential value. */
  bindingKey?: string;
  assertion: AccountAssertionV1;
  active: boolean;
  createdAt: string;
};

const adapterStateSchema = z.object({
    enabled: z.boolean(),
    lastAssertionAt: timestamp.nullable(),
    rootDigests: z.array(digest).max(1024),
}).strict();
const bindingStateSchema = z.object({
  rootId: z.string().regex(ROOT_ID),
  bindingDigest: digest,
  bindingKey: digest.optional(),
  assertion: accountAssertionV1Schema,
  active: z.boolean(),
  createdAt: timestamp,
}).strict();
const stateSchema = z.object({
  schema: z.literal("account-assertion-adapters/v1"),
  adapters: z.record(z.string(), adapterStateSchema),
  bindings: z.record(z.string(), z.array(bindingStateSchema).max(1024)).optional(),
  salt: z.object({ tenantId: z.string().min(1), version: z.string().min(1) }).optional(),
}).strict();

const SOURCE_SET = new Set<string>(ACCOUNT_ASSERTION_SOURCES);
const HASH = /^sha256:[a-f0-9]{64}$/;
const HEX_DIGEST = /^[a-f0-9]{64}$/;
const saltMetadataSchema = z.object({
  schema: z.literal("account-actor-salt/v1"),
  tenantId: z.string().min(1),
  version: z.string().min(1),
  saltDigest: digest,
}).strict();

function privateDirectory(directory: string) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stat = fs.lstatSync(directory);
  if (stat.isSymbolicLink() || !stat.isDirectory() || (stat.mode & 0o7077) !== 0 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid())) {
    throw new Error("account_assertion_home_unsafe");
  }
  return stat;
}

function privateSalt(file: string) {
  const stat = fs.lstatSync(file);
  if (stat.isSymbolicLink() || !stat.isFile() || stat.size !== 32 ||
      (stat.mode & 0o7077) !== 0 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid())) {
    throw new Error("account_assertion_salt_unsafe");
  }
}

/** Read an already provisioned tenant salt.  This function never creates one. */
export function readAccountAssertionSalt(collectorHome: string): Buffer | null {
  privateDirectory(collectorHome);
  const file = path.join(collectorHome, ACCOUNT_ASSERTION_SALT_FILE);
  if (!fs.existsSync(file)) return null;
  privateSalt(file);
  const value = fs.readFileSync(file);
  if (value.length !== 32) throw new Error("account_assertion_salt_invalid");
  return value;
}

function readAccountAssertionSaltMetadata(collectorHome: string) {
  const file = path.join(collectorHome, ACCOUNT_ASSERTION_SALT_META_FILE);
  if (!fs.existsSync(file)) return null;
  const stat = fs.lstatSync(file);
  if (stat.isSymbolicLink() || !stat.isFile() || stat.size > 4 * 1024 || (stat.mode & 0o7077) !== 0 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid())) {
    throw new Error("account_assertion_salt_metadata_unsafe");
  }
  try { return saltMetadataSchema.parse(JSON.parse(fs.readFileSync(file, "utf8"))); }
  catch { throw new Error("account_assertion_salt_metadata_invalid"); }
}

function privateTemporaryFile(file: string, bytes: Buffer | string) {
  const temporary = `${file}.${crypto.randomBytes(8).toString("hex")}.tmp`;
  let descriptor: number | undefined;
  try {
    descriptor = fs.openSync(temporary, "wx", 0o600);
    fs.writeFileSync(descriptor, bytes);
    fs.fsyncSync(descriptor);
    fs.closeSync(descriptor);
    descriptor = undefined;
    return temporary;
  } catch (error) {
    if (descriptor !== undefined) fs.closeSync(descriptor);
    if (fs.existsSync(temporary)) fs.unlinkSync(temporary);
    throw error;
  }
}

function syncDirectory(directory: string) {
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
}

/**
 * Install a tenant-issued salt after an authenticated join/sync.  The binary
 * value is owner-only and published atomically; the sidecar carries only the
 * tenant/version binding needed to reject a stale salt after reassignment.
 */
export function storeAccountAssertionSalt(collectorHome: string, value: Buffer | Uint8Array,
  metadata: { tenantId: string; version: string }) {
  privateDirectory(collectorHome);
  const salt = Buffer.from(value);
  const tenantId = metadata.tenantId.trim();
  const version = metadata.version.trim();
  if (salt.length !== 32) throw new Error("account_assertion_salt_invalid");
  if (!tenantId || !version) throw new Error("account_assertion_salt_metadata_invalid");
  const file = path.join(collectorHome, ACCOUNT_ASSERTION_SALT_FILE);
  const metaFile = path.join(collectorHome, ACCOUNT_ASSERTION_SALT_META_FILE);
  const existing = readAccountAssertionSalt(collectorHome);
  const existingMetadata = readAccountAssertionSaltMetadata(collectorHome);
  const saltDigest = `sha256:${crypto.createHash("sha256").update(salt).digest("hex")}`;
  if (existingMetadata?.tenantId === tenantId && existingMetadata.version === version) {
    if (!existing || existingMetadata.saltDigest !== saltDigest || !crypto.timingSafeEqual(existing, salt)) {
      throw new Error("account_assertion_salt_version_conflict");
    }
    return false;
  }
  const nextMetadata = `${JSON.stringify({
    schema: "account-actor-salt/v1", tenantId, version, saltDigest,
  })}\n`;
  const priorSaltBytes = existing ? Buffer.from(existing) : null;
  const priorMetadataBytes = fs.existsSync(metaFile) ? fs.readFileSync(metaFile) : null;
  const saltTemporary = privateTemporaryFile(file, salt);
  let metadataTemporary: string | null = null;
  try {
    metadataTemporary = privateTemporaryFile(metaFile, nextMetadata);
    // The salt publishes first and metadata last. A crash between renames is
    // fail-closed because readers require the sidecar digest to match.
    fs.renameSync(saltTemporary, file);
    fs.renameSync(metadataTemporary, metaFile);
    syncDirectory(collectorHome);
  } catch (error) {
    // Restore the prior coherent pair on an observed publication failure. If
    // the process itself dies between renames, the digest check still makes
    // the half-published pair unusable rather than deriving the wrong actor.
    try {
      if (priorSaltBytes !== null) {
        const restoreSalt = privateTemporaryFile(file, priorSaltBytes);
        fs.renameSync(restoreSalt, file);
      } else if (fs.existsSync(file)) fs.unlinkSync(file);
      if (priorMetadataBytes !== null) {
        const restoreMetadata = privateTemporaryFile(metaFile, priorMetadataBytes);
        fs.renameSync(restoreMetadata, metaFile);
      } else if (fs.existsSync(metaFile)) fs.unlinkSync(metaFile);
      syncDirectory(collectorHome);
    } catch (compensation) {
      throw new Error(`account_assertion_salt_compensation_failed:${compensation instanceof Error ? compensation.message : String(compensation)}`, { cause: error });
    }
    throw error;
  } finally {
    if (fs.existsSync(saltTemporary)) fs.unlinkSync(saltTemporary);
    if (metadataTemporary && fs.existsSync(metadataTemporary)) fs.unlinkSync(metadataTemporary);
  }
  privateSalt(file);
  return true;
}

/** Compatibility name retained for callers; unlike r1 it fails closed. */
export function ensureAccountAssertionSalt(collectorHome: string): Buffer {
  const value = readAccountAssertionSalt(collectorHome);
  if (!value) throw new Error("account_assertion_salt_unavailable");
  return value;
}

export function readAccountAssertionSaltForTenant(collectorHome: string, tenantId?: string): Buffer | null {
  const value = readAccountAssertionSalt(collectorHome);
  if (!value || !tenantId) return value;
  let metadata: z.infer<typeof saltMetadataSchema> | null;
  try { metadata = readAccountAssertionSaltMetadata(collectorHome); } catch { return null; }
  if (!metadata || metadata.tenantId !== tenantId) return null;
  const actualDigest = `sha256:${crypto.createHash("sha256").update(value).digest("hex")}`;
  return metadata.saltDigest === actualDigest ? value : null;
}

function saltBytes(value: Buffer | Uint8Array | string): Buffer {
  if (Buffer.isBuffer(value)) return Buffer.from(value);
  if (value instanceof Uint8Array) return Buffer.from(value);
  return ensureAccountAssertionSalt(value);
}

/** Stable, full-width salted SHA-256.  The raw identity never leaves this function. */
export function deriveAccountActorHash(identity: string, salt: Buffer | Uint8Array | string): string {
  if (typeof identity !== "string" || identity.length === 0 || identity.length > 512) {
    throw new Error("account_identity_invalid");
  }
  const value = saltBytes(salt);
  if (value.length !== 32) throw new Error("account_assertion_salt_invalid");
  const hash = crypto.createHash("sha256").update(value).update("\0", "utf8").update(identity, "utf8").digest("hex");
  return `sha256:${hash}`;
}

export const hashAccountIdentity = deriveAccountActorHash;
export const stableAccountActorHash = deriveAccountActorHash;

export const ACCOUNT_ASSERTION_CANONICAL_BUDGETS = {
  maxDepth: 256,
  maxNodes: 10_000,
  maxBytes: 16 * 1024,
} as const;

/** Controlled canonicalisation: hostile depth/width is rejected before the JS
 * call stack can overflow and before an unbounded string is built. */
function canonical(value: unknown, seen = new WeakSet<object>(), depth = 0,
  budget = ACCOUNT_ASSERTION_CANONICAL_BUDGETS, counter = { nodes: 0, bytes: 0 }): string {
  if (depth > budget.maxDepth) throw new Error("account_binding_record_depth_exceeded");
  counter.nodes += 1;
  if (counter.nodes > budget.maxNodes) throw new Error("account_binding_record_node_budget_exceeded");
  const emit = (text: string) => {
    counter.bytes += Buffer.byteLength(text, "utf8");
    if (counter.bytes > budget.maxBytes) throw new Error("account_binding_record_too_large");
    return text;
  };
  if (value === undefined) return emit("null");
  if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return emit(JSON.stringify(value));
  }
  if (typeof value === "object") {
    if (seen.has(value)) throw new Error("account_binding_record_cyclic");
    seen.add(value);
    let result: string;
    if (Array.isArray(value)) {
      result = `[${value.map(item => canonical(item, seen, depth + 1, budget, counter)).join(",")}]`;
    } else {
      const record = value as Record<string, unknown>;
      result = `{${Object.keys(record).sort().map(key => `${JSON.stringify(key)}:${canonical(record[key], seen, depth + 1, budget, counter)}`).join(",")}}`;
    }
    seen.delete(value);
    return emit(result);
  }
  return emit("null");
}

/** Hash a bounded, non-secret binding receipt; only the digest is persisted. */
export function hashBindingRecord(record: unknown, budgets?: Partial<typeof ACCOUNT_ASSERTION_CANONICAL_BUDGETS>): string {
  const encoded = canonical(record, new WeakSet<object>(), 0, { ...ACCOUNT_ASSERTION_CANONICAL_BUDGETS, ...budgets });
  return `sha256:${crypto.createHash("sha256").update(encoded, "utf8").digest("hex")}`;
}
export const bindingEvidenceRef = hashBindingRecord;

function assertionBindingDigest(assertion: AccountAssertionV1) {
  // validUntil is deliberately excluded: closing a window is a state
  // transition, while this digest identifies the immutable enrollment epoch.
  const { validUntil: _window, ...immutable } = assertion;
  return hashBindingRecord(immutable);
}

/**
 * Identify a Codex enrollment epoch without retaining account credentials.
 * The live binding contains a token digest, but only these stable routing
 * fields participate in the key used to select its assertion window.
 */
export function codexBindingEpochDigest(binding: unknown): string {
  if (!binding || typeof binding !== "object") throw new Error("account_binding_invalid");
  const record = binding as Record<string, unknown>;
  const fields = ["producerId", "credentialId", "captureRootId", "profileId",
    "captureRootDigest", "installationEpochId", "enrolledAt"] as const;
  const stable = Object.fromEntries(fields.map(field => [field, record[field] ?? null]));
  if (fields.some(field => typeof record[field] !== "string" || !String(record[field]).trim())) {
    throw new Error("account_binding_invalid");
  }
  return hashBindingRecord({ source: "codex", ...stable });
}
export const hashCodexBindingEpoch = codexBindingEpochDigest;

export function accountAssertionContains(assertion: Pick<AccountAssertionV1, "validFrom" | "validUntil">, at: string): boolean {
  const observed = Date.parse(at);
  const from = Date.parse(assertion.validFrom);
  const until = assertion.validUntil === null ? Number.POSITIVE_INFINITY : Date.parse(assertion.validUntil);
  return Number.isFinite(observed) && Number.isFinite(from) && observed >= from && observed < until;
}

export function closeAccountAssertionWindow(assertion: AccountAssertionV1, validUntil: string): AccountAssertionV1 {
  if (!timestamp.safeParse(validUntil).success || Date.parse(validUntil) <= Date.parse(assertion.validFrom)) {
    throw new Error("account_assertion_window_invalid");
  }
  return accountAssertionV1Schema.parse({ ...assertion, validUntil });
}

export function validateAccountAssertionWindow(assertion: Pick<AccountAssertionV1, "validFrom" | "validUntil">) {
  if (!timestamp.safeParse(assertion.validFrom).success ||
      (assertion.validUntil !== null && (!timestamp.safeParse(assertion.validUntil).success ||
        Date.parse(assertion.validUntil) <= Date.parse(assertion.validFrom)))) {
    throw new Error("account_assertion_window_invalid");
  }
  return true;
}

function safeIdentity(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed || trimmed.length > 512 || trimmed.includes("@") ||
      /^(?:sk-|tok_|bearer\s|eyJ[A-Za-z0-9_-]+\.)/i.test(trimmed)) return null;
  return trimmed;
}

function decodeBase64UrlJson(value: string): Record<string, unknown> | null {
  if (!value || value.length > 192 * 1024 || !/^[A-Za-z0-9_-]+$/.test(value)) return null;
  try {
    const bytes = Buffer.from(value, "base64url");
    if (bytes.toString("base64url") !== value) return null;
    const parsed: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : null;
  } catch { return null; }
}

/**
 * Accept only the signed native Codex auth record.  Routing fields,
 * credentialId, caller-provided digests, and arbitrary profile objects are
 * intentionally not identity/evidence sources.
 */
export function resolveCodexSignedNativeEvidence(binding: unknown): { identity: string; evidenceRef: string } | null {
  if (!binding || typeof binding !== "object") return null;
  const root = binding as Record<string, unknown>;
  const tokens = root.tokens;
  if (!tokens || typeof tokens !== "object" || Array.isArray(tokens)) return null;
  const idToken = (tokens as Record<string, unknown>).id_token;
  if (typeof idToken !== "string" || idToken.length === 0 || idToken.length > 256 * 1024) return null;
  const parts = idToken.split(".");
  if (parts.length !== 3 || parts.some(part => !part || !/^[A-Za-z0-9_-]+$/.test(part))) return null;
  const header = decodeBase64UrlJson(parts[0]);
  const claims = decodeBase64UrlJson(parts[1]);
  let signature: Buffer;
  try { signature = Buffer.from(parts[2], "base64url"); } catch { return null; }
  if (!header || !claims || typeof header.alg !== "string" || header.alg.toLowerCase() === "none" ||
      signature.length === 0 || signature.toString("base64url") !== parts[2]) return null;
  const apiClaims = claims["https://api.openai.com/auth"];
  if (!apiClaims || typeof apiClaims !== "object" || Array.isArray(apiClaims)) return null;
  const identity = safeIdentity((apiClaims as Record<string, unknown>).chatgpt_account_id);
  if (!identity) return null;
  return { identity, evidenceRef: `sha256:${crypto.createHash("sha256").update(idToken, "utf8").digest("hex")}` };
}

/** Resolve only a stable identity from signed native claims. */
export function resolveCodexAccountIdentity(binding: unknown): string | null {
  return resolveCodexSignedNativeEvidence(binding)?.identity ?? null;
}

export function accountAssertionForBinding(options: {
  source?: AccountAssertionSource;
  binding?: unknown;
  /** Stable native provider id; never an email or credential/token value. */
  accountIdentity?: string;
  providerAccountId?: string;
  chatgptAccountId?: string;
  accountId?: string;
  accountUuid?: string;
  /** Hosted tenant whose private salt must already be installed. */
  tenantId?: string;
  collectorHome: string;
  validFrom: string;
  validUntil?: string | null;
  evidenceRef?: string;
}): AccountAssertionV1 {
  const source = options.source ?? "codex";
  if (!SOURCE_SET.has(source)) throw new Error("account_assertion_source_invalid");
  const binding = options.binding ?? {
    providerAccountId: options.accountIdentity ?? options.providerAccountId ?? options.chatgptAccountId ??
      options.accountId ?? options.accountUuid,
  };
  if (binding && typeof binding === "object" && "source" in binding &&
      typeof (binding as { source?: unknown }).source === "string" &&
      (binding as { source: string }).source !== source) {
    throw new Error("account_assertion_source_mismatch");
  }
  const native = source === "codex" ? resolveCodexSignedNativeEvidence(binding) : null;
  const identity = native?.identity ?? (source === "codex" ? null : safeIdentity(options.accountIdentity ?? options.providerAccountId ??
    options.chatgptAccountId ?? options.accountId ?? options.accountUuid));
  if (!identity) throw new Error("account_signed_evidence_unavailable");
  // A caller-supplied evidenceRef is accepted only as a consistency check
  // against the exact signed bytes; it can never create evidence by itself.
  const evidenceRef = native?.evidenceRef ?? null;
  if (!evidenceRef || (options.evidenceRef !== undefined && options.evidenceRef !== evidenceRef))
    throw new Error("account_signed_evidence_invalid");
  const salt = options.tenantId ? readAccountAssertionSaltForTenant(options.collectorHome, options.tenantId) : null;
  if (!salt) throw new Error("account_assertion_salt_unavailable");
  const assertion = {
    actorHash: deriveAccountActorHash(identity, salt),
    validFrom: options.validFrom,
    validUntil: options.validUntil ?? null,
    evidenceRef,
    schema: ACCOUNT_ASSERTION_SCHEMA,
    source,
  } satisfies AccountAssertionV1;
  validateAccountAssertionWindow(assertion);
  return accountAssertionV1Schema.parse(assertion);
}

/** Friendly contract aliases used by provider adapters and fixture authors. */
export const createAccountAssertion = accountAssertionForBinding;
export const createCodexAccountAssertion = (options: Omit<Parameters<typeof accountAssertionForBinding>[0], "source">) =>
  accountAssertionForBinding({ ...options, source: "codex" });

function defaultAdapterState(): AccountAssertionAdapterState {
  return {
    schema: "account-assertion-adapters/v1",
    adapters: Object.fromEntries(ACCOUNT_ASSERTION_SOURCES.map(source => [source, {
      enabled: true,
      lastAssertionAt: null,
      rootDigests: [],
    }])) as unknown as AccountAssertionAdapterState["adapters"],
    bindings: Object.fromEntries(ACCOUNT_ASSERTION_SOURCES.map(source => [source, []])) as unknown as AccountAssertionAdapterState["bindings"],
  };
}

function ensureStateTable(db: DB) {
  db.exec(`create table if not exists maintenance_state (
    key text primary key, value text not null, updated_at text not null)`);
}
type DB = Database.Database;

function parseState(value: string | null | undefined): AccountAssertionAdapterState {
  if (!value) return defaultAdapterState();
  const parsed = stateSchema.safeParse(JSON.parse(value));
  if (!parsed.success) throw new Error("account_assertion_state_invalid");
  const defaults = defaultAdapterState();
  for (const source of ACCOUNT_ASSERTION_SOURCES) {
    const candidate = parsed.data.adapters[source];
    if (candidate) defaults.adapters[source] = { ...candidate, rootDigests: [...new Set(candidate.rootDigests)] };
    const bindings = parsed.data.bindings?.[source];
    if (bindings) defaults.bindings[source] = bindings.map(binding => ({
      ...binding,
      assertion: accountAssertionV1Schema.parse(binding.assertion),
    }));
  }
  if (parsed.data.salt) defaults.salt = { ...parsed.data.salt };
  return defaults;
}

export function readAccountAssertionAdapterState(db: DB): AccountAssertionAdapterState {
  // Status and capability checks are read-only.  Older ledgers may not have
  // the maintenance table yet; do not create it merely to report defaults
  // (this also keeps SQLite read-only connections usable).
  const table = db.prepare("select 1 from sqlite_master where type='table' and name='maintenance_state'").get();
  if (!table) return defaultAdapterState();
  const row = db.prepare("select value from maintenance_state where key=?").get(ACCOUNT_ASSERTION_STATE_KEY) as { value: string } | undefined;
  return parseState(row?.value);
}
export const loadAccountAssertionAdapterState = readAccountAssertionAdapterState;

function writeState(db: DB, state: AccountAssertionAdapterState) {
  ensureStateTable(db);
  db.prepare(`insert into maintenance_state(key,value,updated_at) values(?,?,?)
    on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at`)
    .run(ACCOUNT_ASSERTION_STATE_KEY, JSON.stringify(state), new Date().toISOString());
}

/** Toggle one adapter and touch no root, event, receipt, or other state key. */
export function setAccountAssertionAdapterEnabled(db: DB, source: AccountAssertionSource, enabled: boolean,
  at = new Date().toISOString()) {
  if (!SOURCE_SET.has(source)) throw new Error("account_assertion_source_invalid");
  if (!timestamp.safeParse(at).success) throw new Error("account_assertion_time_invalid");
  const update = db.transaction(() => {
    const state = readAccountAssertionAdapterState(db);
    if (!enabled) {
      for (const binding of state.bindings[source]) {
        if (!binding.active) continue;
        if (Date.parse(at) <= Date.parse(binding.assertion.validFrom)) {
          throw new Error("account_assertion_disable_time_regression");
        }
        binding.assertion = closeAccountAssertionWindow(binding.assertion, at);
        binding.active = false;
      }
    }
    state.adapters[source].enabled = enabled;
    writeState(db, state);
    return state;
  });
  return update.immediate();
}

export const setAccountAssertionAdapterState = setAccountAssertionAdapterEnabled;

export function accountAssertionAdapterEnabled(db: DB, source: AccountAssertionSource) {
  if (!SOURCE_SET.has(source)) throw new Error("account_assertion_source_invalid");
  return readAccountAssertionAdapterState(db).adapters[source].enabled;
}

/** Persist the tenant salt binding in the additive maintenance record. */
export function recordAccountAssertionSalt(db: DB, tenantId: string, version: string) {
  if (!tenantId.trim() || !version.trim()) throw new Error("account_assertion_salt_metadata_invalid");
  const update = db.transaction(() => {
    const state = readAccountAssertionAdapterState(db);
    state.salt = { tenantId: tenantId.trim(), version: version.trim() };
    writeState(db, state);
    return state;
  });
  return update.immediate();
}

export function accountAssertionSaltForTenant(db: DB, collectorHome: string, tenantId: string): Buffer | null {
  const state = readAccountAssertionAdapterState(db);
  if (!state.salt || state.salt.tenantId !== tenantId) return null;
  return readAccountAssertionSaltForTenant(collectorHome, tenantId);
}

function recordAccountAssertionInTransaction(db: DB, source: AccountAssertionSource, rootDigest: string, at: string) {
  if (!SOURCE_SET.has(source)) throw new Error("account_assertion_source_invalid");
  const normalizedRootDigest = HASH.test(rootDigest) ? rootDigest : HEX_DIGEST.test(rootDigest) ? `sha256:${rootDigest}` : null;
  if (!normalizedRootDigest) throw new Error("account_root_digest_invalid");
  if (!timestamp.safeParse(at).success) throw new Error("account_assertion_time_invalid");
  const state = readAccountAssertionAdapterState(db);
  const adapter = state.adapters[source];
  if (!adapter.lastAssertionAt || Date.parse(at) >= Date.parse(adapter.lastAssertionAt)) {
    adapter.lastAssertionAt = at;
  }
  adapter.rootDigests = [normalizedRootDigest, ...adapter.rootDigests.filter(value => value !== normalizedRootDigest)].slice(0, 1024);
  writeState(db, state);
  return state;
}

export function recordAccountAssertion(db: DB, source: AccountAssertionSource, rootDigest: string, at: string) {
  if (db.inTransaction) return recordAccountAssertionInTransaction(db, source, rootDigest, at);
  const update = db.transaction(() => recordAccountAssertionInTransaction(db, source, rootDigest, at));
  return update.immediate();
}

export function accountAssertionStatus(db: DB) {
  const state = readAccountAssertionAdapterState(db);
  return ACCOUNT_ASSERTION_SOURCES.map(source => ({
    source,
    enabled: state.adapters[source].enabled,
    lastAssertionAt: state.adapters[source].lastAssertionAt,
    // Codex enrollment keeps the root id beside its epoch digest.  Count
    // roots, not epochs, so a failover does not inflate this status metric.
    rootsWithAssertion: new Set(state.bindings[source].map(binding => binding.rootId)).size ||
      state.adapters[source].rootDigests.length,
  }));
}

export type AccountAssertionStatusRow = {
  source: AccountAssertionSource;
  enabled: boolean;
  lastAssertionAt: string | null;
  rootsWithAssertion: number;
};

/** Cache-only status fallback; it performs no database or filesystem work. */
export function defaultAccountAssertionStatus(): AccountAssertionStatusRow[] {
  return ACCOUNT_ASSERTION_SOURCES.map(source => ({
    source, enabled: true, lastAssertionAt: null, rootsWithAssertion: 0,
  }));
}

export function formatAccountAssertionStatusRows(rows: readonly AccountAssertionStatusRow[]) {
  return rows.map(row =>
    `${row.source}=${row.enabled ? "enabled" : "disabled"};last=${row.lastAssertionAt ?? "never"};roots=${row.rootsWithAssertion}`,
  ).join(" | ");
}

export function formatAccountAssertionStatusLine(db: DB) {
  return formatAccountAssertionStatusRows(accountAssertionStatus(db));
}

export function enrollCodexAccountAssertion(options: {
  db?: DB;
  rootId: string;
  rootDigest: string;
  binding: unknown;
  collectorHome: string;
  validFrom: string;
  validUntil?: string | null;
  tenantId?: string;
}): AccountAssertionV1 | null {
  if (options.db && !accountAssertionAdapterEnabled(options.db, "codex")) return null;
  const assertion = accountAssertionForBinding({
    source: "codex", binding: options.binding, collectorHome: options.collectorHome,
    validFrom: options.validFrom, validUntil: options.validUntil, tenantId: options.tenantId,
  });
  if (!options.db) return assertion;
  persistCodexAccountAssertion(options.db, options.rootId, options.rootDigest, assertion);
  return assertion;
}

function closePriorCodexAssertions(state: AccountAssertionAdapterState, rootId: string, validUntil: string) {
  let changed = false;
  for (const prior of state.bindings.codex) {
    if (prior.rootId !== rootId || !prior.active) continue;
    const old = accountAssertionV1Schema.parse(prior.assertion);
    if (old.validUntil === null && Date.parse(old.validFrom) < Date.parse(validUntil)) {
      prior.assertion = closeAccountAssertionWindow(old, validUntil);
      prior.active = false;
      changed = true;
    }
  }
  return changed;
}

/** Close an open Codex window when a replacement binding is enrolled without an assertion. */
export function closeCodexAccountAssertionWindow(db: DB, rootId: string, validUntil: string) {
  if (!ROOT_ID.test(rootId) || !timestamp.safeParse(validUntil).success) throw new Error("account_assertion_window_invalid");
  const work = () => {
    const state = readAccountAssertionAdapterState(db);
    if (!closePriorCodexAssertions(state, rootId, validUntil)) return false;
    writeState(db, state);
    return true;
  };
  return db.inTransaction ? work() : db.transaction(work).immediate();
}

/** Persist one assertion and close the prior open window for this root. */
export function persistCodexAccountAssertion(db: DB, rootId: string, rootDigest: string, assertion: AccountAssertionV1,
  bindingKey?: string) {
  if (assertion.source !== "codex") throw new Error("account_assertion_source_invalid");
  if (!ROOT_ID.test(rootId)) throw new Error("account_root_id_invalid");
  validateAccountAssertionWindow(assertion);
  if (!HASH.test(rootDigest) && !HEX_DIGEST.test(rootDigest)) throw new Error("account_root_digest_invalid");
  if (bindingKey !== undefined && !HASH.test(bindingKey)) throw new Error("account_binding_key_invalid");
  // Keep each assertion epoch distinct even when a provider re-enrolls the
  // same native binding.  The evidence reference remains the native-record
  // digest exposed to events; this private key excludes the mutable window
  // close and contains no raw identity.
  const bindingDigest = hashBindingRecord({ assertion: assertionBindingDigest(assertion), bindingKey: bindingKey ?? null });
  const work = () => {
    const state = readAccountAssertionAdapterState(db);
    // Recheck inside the write transaction so a concurrent maintenance toggle
    // cannot sneak one more assertion in after the adapter was disabled.
    if (!state.adapters.codex.enabled) throw new Error("account_assertion_adapter_disabled");
    const bindings = state.bindings.codex;
    const activePrior = bindings.filter(row => row.rootId === rootId && row.active)
      .sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt))[0];
    if (activePrior && Date.parse(assertion.validFrom) <= Date.parse(activePrior.assertion.validFrom)) {
      throw new Error("account_assertion_time_regression");
    }
    closePriorCodexAssertions(state, rootId, assertion.validFrom);
    const next: AccountAssertionBindingState = {
      rootId, bindingDigest, ...(bindingKey ? { bindingKey } : {}), assertion, active: true, createdAt: assertion.validFrom,
    };
    state.bindings.codex = [next, ...bindings.filter(row => row.bindingDigest !== bindingDigest)].slice(0, 1024);
    const adapter = state.adapters.codex;
    if (!adapter.lastAssertionAt || Date.parse(assertion.validFrom) >= Date.parse(adapter.lastAssertionAt)) {
      adapter.lastAssertionAt = assertion.validFrom;
    }
    const normalizedRootDigest = HASH.test(rootDigest) ? rootDigest : `sha256:${rootDigest}`;
    adapter.rootDigests = [normalizedRootDigest, ...adapter.rootDigests.filter(value => value !== normalizedRootDigest)].slice(0, 1024);
    writeState(db, state);
  };
  if (db.inTransaction) work(); else db.transaction(work).immediate();
  return assertion;
}

export const enrollCodexCaptureRoot = enrollCodexAccountAssertion;

/** Pure root-manifest adapter for callers that persist the returned root. */
export function attachCodexAccountAssertion<TRoot extends { source: string; account?: unknown }>(
  root: TRoot,
  options: {
    binding: unknown;
    collectorHome: string;
    validFrom: string;
    validUntil?: string | null;
    tenantId?: string;
  },
): TRoot & { account: AccountAssertionV1 } {
  if (root.source !== "codex") throw new Error("account_assertion_source_invalid");
  const assertion = accountAssertionForBinding({ source: "codex", binding: options.binding,
    collectorHome: options.collectorHome, validFrom: options.validFrom, validUntil: options.validUntil,
    tenantId: options.tenantId });
  return { ...root, account: assertion } as TRoot & { account: AccountAssertionV1 };
}

export function activeCodexAccountAssertion(db: DB, rootId: string, bindingKey?: string): AccountAssertionV1 | null {
  const state = readAccountAssertionAdapterState(db);
  const row = state.bindings.codex
    .filter(binding => binding.rootId === rootId && binding.active &&
      (bindingKey === undefined || binding.bindingKey === bindingKey))
    .sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt))[0];
  if (!row) return null;
  const parsed = accountAssertionV1Schema.safeParse(row.assertion);
  if (!parsed.success) return null;
  try { validateAccountAssertionWindow(parsed.data); } catch { return null; }
  return parsed.data;
}

/** Every immutable interval, including closed failover windows. */
export function codexAccountAssertionIntervals(db: DB, rootId: string): AccountAssertionV1[] {
  const state = readAccountAssertionAdapterState(db);
  return state.bindings.codex
    .filter(binding => binding.rootId === rootId)
    .sort((a, b) => Date.parse(a.assertion.validFrom) - Date.parse(b.assertion.validFrom))
    .map(binding => accountAssertionV1Schema.safeParse(binding.assertion))
    .filter((parsed): parsed is { success: true; data: AccountAssertionV1 } => parsed.success)
    .map(parsed => parsed.data);
}

export function codexAccountAssertionAt(db: DB, rootId: string, at: string): AccountAssertionV1 | null {
  const matches = codexAccountAssertionIntervals(db, rootId).filter(assertion => accountAssertionContains(assertion, at));
  return matches.length === 1 ? matches[0] : null;
}
