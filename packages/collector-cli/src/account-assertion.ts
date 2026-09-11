import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type Database from "better-sqlite3";
import { z } from "zod";

/**
 * Versioned account continuity contract.
 *
 * The value in an assertion is deliberately an actor hash, never the native
 * provider account id.  The salt is installation-local and is not part of an
 * event, receipt, or upload.
 */
export const ACCOUNT_ASSERTION_SCHEMA = "account-assertion/v1" as const;
export const ACCOUNT_ASSERTION_STATE_KEY = "account_assertion_adapters_v1" as const;
export const ACCOUNT_ASSERTION_SALT_FILE = "account-assertion.salt" as const;
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
}).strict();

const SOURCE_SET = new Set<string>(ACCOUNT_ASSERTION_SOURCES);
const HASH = /^sha256:[a-f0-9]{64}$/;
const HEX_DIGEST = /^[a-f0-9]{64}$/;
const IDENTITY_FIELDS = [
  "providerAccountId",
  "provider_account_id",
  "chatgptAccountId",
  "chatgpt_account_id",
  "codexAccountId",
  "accountUuid",
  "account_uuid",
  "accountId",
  "account_id",
  "id",
  "credentialId",
] as const;

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

function writePrivateSalt(file: string, value: Buffer) {
  const directory = path.dirname(file);
  privateDirectory(directory);
  let fd: number | undefined;
  try {
    // O_EXCL makes the first enrollment the sole salt writer; a concurrent
    // enrollment re-reads the winner instead of replacing its salt.
    fd = fs.openSync(file, "wx", 0o600);
    fs.writeFileSync(fd, value);
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fd = undefined;
    const directoryFd = fs.openSync(directory, "r");
    try { fs.fsyncSync(directoryFd); } finally { fs.closeSync(directoryFd); }
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
  }
}

/** Return the installation-local salt, creating it with owner-only mode once. */
export function ensureAccountAssertionSalt(collectorHome: string): Buffer {
  privateDirectory(collectorHome);
  const file = path.join(collectorHome, ACCOUNT_ASSERTION_SALT_FILE);
  if (fs.existsSync(file)) {
    privateSalt(file);
    const value = fs.readFileSync(file);
    if (value.length !== 32) throw new Error("account_assertion_salt_invalid");
    return value;
  }
  const value = crypto.randomBytes(32);
  try {
    writePrivateSalt(file, value);
  } catch (error) {
    // A concurrent enrollment may have won the create race.  Re-read only
    // after checking the winner's permissions and exact size.
    if (!fs.existsSync(file)) throw error;
    privateSalt(file);
  }
  privateSalt(file);
  return fs.readFileSync(file);
}

export const readAccountAssertionSalt = ensureAccountAssertionSalt;

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

function canonical(value: unknown, seen = new WeakSet<object>()): string {
  if (value === undefined) return "null";
  if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "object") {
    if (seen.has(value)) throw new Error("account_binding_record_cyclic");
    seen.add(value);
    if (Array.isArray(value)) {
      const result = `[${value.map(item => canonical(item, seen)).join(",")}]`;
      seen.delete(value);
      return result;
    }
    const record = value as Record<string, unknown>;
    const result = `{${Object.keys(record).sort().map(key => `${JSON.stringify(key)}:${canonical(record[key], seen)}`).join(",")}}`;
    seen.delete(value);
    return result;
  }
  return JSON.stringify(null);
}

/** Hash a bounded, non-secret binding receipt; only the digest is persisted. */
export function hashBindingRecord(record: unknown): string {
  const encoded = canonical(record);
  if (Buffer.byteLength(encoded, "utf8") > 16 * 1024) throw new Error("account_binding_record_too_large");
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

function identityFromBinding(binding: unknown, seen = new WeakSet<object>()): { field: string; value: string } | null {
  if (!binding || typeof binding !== "object") return null;
  if (seen.has(binding)) return null;
  seen.add(binding);
  const record = binding as Record<string, unknown>;
  for (const field of IDENTITY_FIELDS) {
    const value = record[field];
    // Email addresses and token-looking values are not provider account
    // identities.  Skip them rather than allowing a caller to accidentally
    // turn a PII field into the actor input.
    if (typeof value === "string" && value.trim() && !value.includes("@") &&
        !/^(?:sk-|tok_|bearer\s|eyJ[A-Za-z0-9_-]+\.)/i.test(value.trim())) return { field, value: value.trim() };
  }
  // Native profile adapters may nest the stable provider id under account or
  // a binding/identity envelope.  These are still caller-supplied records;
  // no neighboring credential store is opened here.
  for (const key of ["account", "binding", "identity", "profile"] as const) {
    const nested = record[key];
    if (nested && typeof nested === "object") {
      const resolved = identityFromBinding(nested, seen);
      if (resolved) return resolved;
    }
  }
  return null;
}

/** Resolve only a stable binding id; token, email, and adjacent credential values are ignored. */
export function resolveCodexAccountIdentity(binding: unknown): string | null {
  return identityFromBinding(binding)?.value ?? null;
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
  const identity = identityFromBinding(binding);
  if (!identity) throw new Error("account_identity_unavailable");
  const evidenceRef = options.evidenceRef ?? hashBindingRecord(binding);
  if (!HASH.test(evidenceRef)) throw new Error("account_evidence_ref_invalid");
  const assertion = {
    actorHash: deriveAccountActorHash(identity.value, options.collectorHome),
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
export function setAccountAssertionAdapterEnabled(db: DB, source: AccountAssertionSource, enabled: boolean) {
  if (!SOURCE_SET.has(source)) throw new Error("account_assertion_source_invalid");
  const update = db.transaction(() => {
    const state = readAccountAssertionAdapterState(db);
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
}): AccountAssertionV1 | null {
  if (options.db && !accountAssertionAdapterEnabled(options.db, "codex")) return null;
  const assertion = accountAssertionForBinding({
    source: "codex", binding: options.binding, collectorHome: options.collectorHome,
    validFrom: options.validFrom, validUntil: options.validUntil,
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
  const close = db.transaction(() => {
    const state = readAccountAssertionAdapterState(db);
    if (!closePriorCodexAssertions(state, rootId, validUntil)) return false;
    writeState(db, state);
    return true;
  });
  return close.immediate();
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
  const bindingDigest = assertionBindingDigest(assertion);
  const enroll = db.transaction(() => {
    const state = readAccountAssertionAdapterState(db);
    // Recheck inside the write transaction so a concurrent maintenance toggle
    // cannot sneak one more assertion in after the adapter was disabled.
    if (!state.adapters.codex.enabled) throw new Error("account_assertion_adapter_disabled");
    const bindings = state.bindings.codex;
    const activePrior = bindings.find(row => row.rootId === rootId && row.active);
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
  });
  enroll.immediate();
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
  },
): TRoot & { account: AccountAssertionV1 } {
  if (root.source !== "codex") throw new Error("account_assertion_source_invalid");
  const assertion = accountAssertionForBinding({ source: "codex", binding: options.binding,
    collectorHome: options.collectorHome, validFrom: options.validFrom, validUntil: options.validUntil });
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
