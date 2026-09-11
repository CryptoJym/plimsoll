import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
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

declare const CODEX_NATIVE_AUTH_BINDING: unique symbol;
/** Capability returned only by an owner-only native auth.json read. */
export type CodexNativeAuthBinding = Readonly<{
  readonly [CODEX_NATIVE_AUTH_BINDING]: true;
}>;

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
  /** Source-binding epoch used for event-time failover and hosted dedupe. */
  installationEpochId?: string;
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
  installationEpochId: z.string().uuid().optional(),
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
function canonical(value: unknown, budget = ACCOUNT_ASSERTION_CANONICAL_BUDGETS): string {
  const seen = new WeakSet<object>();
  const counter = { nodes: 0, bytes: 0 };
  const chunks: string[] = [];
  const emit = (text: string) => {
    counter.bytes += Buffer.byteLength(text, "utf8");
    if (counter.bytes > budget.maxBytes) throw new Error("account_binding_record_too_large");
    chunks.push(text);
  };
  const emitJsonString = (value: string) => {
    emit("\"");
    for (let offset = 0; offset < value.length;) {
      let end = Math.min(value.length, offset + 1024);
      // Keep a surrogate pair in one bounded chunk so the result remains
      // byte-for-byte identical to JSON.stringify without allocating the full
      // escaped value first.
      if (end < value.length && end > offset && /[\uD800-\uDBFF]/.test(value[end - 1]) &&
          /[\uDC00-\uDFFF]/.test(value[end])) end -= 1;
      const encoded = JSON.stringify(value.slice(offset, end));
      emit(encoded.slice(1, -1));
      offset = end;
    }
    emit("\"");
  };
  const visit = (candidate: unknown, depth: number): void => {
    if (depth > budget.maxDepth) throw new Error("account_binding_record_depth_exceeded");
    counter.nodes += 1;
    if (counter.nodes > budget.maxNodes) throw new Error("account_binding_record_node_budget_exceeded");
    if (candidate === undefined) { emit("null"); return; }
    if (candidate === null || typeof candidate === "number" || typeof candidate === "boolean") {
      emit(JSON.stringify(candidate)); return;
    }
    if (typeof candidate === "string") {
      emitJsonString(candidate); return;
    }
    if (typeof candidate !== "object") { emit("null"); return; }
    if (seen.has(candidate)) throw new Error("account_binding_record_cyclic");
    seen.add(candidate);
    if (Array.isArray(candidate)) {
      if (candidate.length > budget.maxNodes - counter.nodes) throw new Error("account_binding_record_node_budget_exceeded");
      emit("[");
      for (let index = 0; index < candidate.length; index += 1) {
        if (index) emit(",");
        visit(candidate[index], depth + 1);
      }
      emit("]");
    } else {
      const record = candidate as Record<string, unknown>;
      const keys: string[] = [];
      for (const key in record) {
        if (!Object.prototype.hasOwnProperty.call(record, key)) continue;
        if (keys.length >= budget.maxNodes - counter.nodes) throw new Error("account_binding_record_node_budget_exceeded");
        keys.push(key);
      }
      keys.sort();
      emit("{");
      for (let index = 0; index < keys.length; index += 1) {
        if (index) emit(",");
        const key = keys[index];
        emitJsonString(key); emit(":"); visit(record[key], depth + 1);
      }
      emit("}");
    }
    seen.delete(candidate);
  };
  visit(value, 0);
  return chunks.join("");
}

/** Hash a bounded, non-secret binding receipt; only the digest is persisted. */
export function hashBindingRecord(record: unknown, budgets?: Partial<typeof ACCOUNT_ASSERTION_CANONICAL_BUDGETS>): string {
  const encoded = canonical(record, { ...ACCOUNT_ASSERTION_CANONICAL_BUDGETS, ...budgets });
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

type CodexSignedNativeEvidence = Readonly<{ identity: string; evidenceRef: string }>;
const codexNativeEvidenceCapabilities = new WeakMap<object, CodexSignedNativeEvidence>();
export const CODEX_AUTH_PROVIDER_ORIGIN = "https://auth.openai.com" as const;
export const CODEX_AUTH_OPENID_CONFIGURATION_URL =
  `${CODEX_AUTH_PROVIDER_ORIGIN}/.well-known/openid-configuration` as const;
export const CODEX_ID_TOKEN_ALGORITHMS = ["RS256"] as const;
const CODEX_ID_TOKEN_ALGORITHM_SET = new Set<string>(CODEX_ID_TOKEN_ALGORITHMS);
const CODEX_JWKS_TIMEOUT_MS = 5_000;
const CODEX_JWKS_MAX_BYTES = 256 * 1024;
const CODEX_JWKS_CACHE_MS = 5 * 60_000;
const openIdConfigurationSchema = z.object({
  issuer: z.string().url(),
  jwks_uri: z.string().url(),
  id_token_signing_alg_values_supported: z.array(z.string()).max(16),
}).passthrough();
const jwksSchema = z.object({
  keys: z.array(z.record(z.string(), z.unknown())).max(64),
}).passthrough();
type CodexVerificationMaterial = {
  issuer: string;
  algorithms: ReadonlySet<string>;
  keys: Record<string, unknown>[];
};
let cachedCodexVerificationMaterial: (CodexVerificationMaterial & { expiresAt: number }) | null = null;

async function boundedProviderJson(url: string, expectedOrigin: string): Promise<unknown> {
  const target = new URL(url);
  if (target.origin !== expectedOrigin || target.protocol !== "https:" || target.username || target.password) {
    throw new Error("account_signed_evidence_unavailable");
  }
  const controller = new AbortController();
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let timer: ReturnType<typeof setTimeout>;
  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      controller.abort();
      reject(new Error("account_signed_evidence_unavailable"));
    }, CODEX_JWKS_TIMEOUT_MS);
  });
  const read = async () => {
    const response = await fetch(target.href, {
      method: "GET",
      redirect: "manual",
      signal: controller.signal,
      headers: { accept: "application/json" },
    });
    if (controller.signal.aborted || response.redirected ||
        (response.status >= 300 && response.status < 400) || response.status !== 200 ||
        (response.url && new URL(response.url).origin !== expectedOrigin) ||
        !(response.headers.get("content-type") ?? "").toLowerCase().includes("json")) {
      void response.body?.cancel().catch(() => undefined);
      throw new Error("account_signed_evidence_unavailable");
    }
    const declaredLength = Number(response.headers.get("content-length"));
    if (declaredLength > CODEX_JWKS_MAX_BYTES) {
      void response.body?.cancel().catch(() => undefined);
      throw new Error("account_signed_evidence_unavailable");
    }
    reader = response.body?.getReader();
    if (!reader) throw new Error("account_signed_evidence_unavailable");
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      bytes += chunk.value.byteLength;
      if (bytes > CODEX_JWKS_MAX_BYTES) throw new Error("account_signed_evidence_unavailable");
      chunks.push(chunk.value);
    }
    const text = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks));
    return JSON.parse(text) as unknown;
  };
  try {
    return await Promise.race([read(), deadline]);
  } catch {
    throw new Error("account_signed_evidence_unavailable");
  } finally {
    clearTimeout(timer!);
    controller.abort();
    if (reader) {
      void reader.cancel().catch(() => undefined);
      try { reader.releaseLock(); } catch {}
    }
  }
}

async function codexVerificationMaterial(forceRefresh = false): Promise<CodexVerificationMaterial> {
  if (!forceRefresh && cachedCodexVerificationMaterial && cachedCodexVerificationMaterial.expiresAt > Date.now()) {
    return cachedCodexVerificationMaterial;
  }
  const providerOrigin = new URL(CODEX_AUTH_PROVIDER_ORIGIN).origin;
  const configuration = openIdConfigurationSchema.safeParse(
    await boundedProviderJson(CODEX_AUTH_OPENID_CONFIGURATION_URL, providerOrigin),
  );
  if (!configuration.success) throw new Error("account_signed_evidence_unavailable");
  const issuer = new URL(configuration.data.issuer);
  const jwks = new URL(configuration.data.jwks_uri);
  if ((issuer.href !== providerOrigin && issuer.href !== `${providerOrigin}/`) ||
      issuer.protocol !== "https:" || issuer.username || issuer.password ||
      jwks.origin !== providerOrigin || jwks.protocol !== "https:" || jwks.username || jwks.password) {
    throw new Error("account_signed_evidence_unavailable");
  }
  const algorithms = new Set(configuration.data.id_token_signing_alg_values_supported
    .filter(algorithm => CODEX_ID_TOKEN_ALGORITHM_SET.has(algorithm)));
  if (algorithms.size === 0) throw new Error("account_signed_evidence_unavailable");
  const keySet = jwksSchema.safeParse(await boundedProviderJson(jwks.href, providerOrigin));
  if (!keySet.success) throw new Error("account_signed_evidence_unavailable");
  cachedCodexVerificationMaterial = {
    issuer: configuration.data.issuer,
    algorithms,
    keys: keySet.data.keys,
    expiresAt: Date.now() + CODEX_JWKS_CACHE_MS,
  };
  return cachedCodexVerificationMaterial;
}

function canonicalBase64Url(value: unknown, maxLength: number) {
  if (typeof value !== "string" || !value || value.length > maxLength || !/^[A-Za-z0-9_-]+$/.test(value)) return false;
  try { return Buffer.from(value, "base64url").toString("base64url") === value; }
  catch { return false; }
}

function providerVerificationKey(keys: readonly Record<string, unknown>[], kid: string, algorithm: string) {
  const matches = keys.filter(key => key.kid === kid && key.kty === "RSA" &&
    (key.alg === undefined || key.alg === algorithm) && (key.use === undefined || key.use === "sig") &&
    (key.key_ops === undefined || (Array.isArray(key.key_ops) && key.key_ops.includes("verify"))) &&
    canonicalBase64Url(key.n, 2048) && canonicalBase64Url(key.e, 16) &&
    !["d", "p", "q", "dp", "dq", "qi", "oth", "k"].some(field => field in key));
  if (matches.length !== 1) return null;
  try {
    const key = crypto.createPublicKey({ key: matches[0] as crypto.JsonWebKey, format: "jwk" });
    const bits = key.asymmetricKeyDetails?.modulusLength;
    return key.asymmetricKeyType === "rsa" && bits !== undefined && bits >= 2048 && bits <= 8192 ? key : null;
  } catch { return null; }
}

/**
 * Parse and verify the exact signed field inside a native Codex auth record.
 * The identity exists only long enough to derive the hash-only assertion.
 */
async function parseCodexSignedNativeEvidence(binding: unknown, enrolledAt: string): Promise<CodexSignedNativeEvidence | null> {
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
  if (!header || !claims || !CODEX_ID_TOKEN_ALGORITHM_SET.has(String(header.alg)) ||
      typeof header.kid !== "string" || !/^[A-Za-z0-9._:-]{1,256}$/.test(header.kid) ||
      header.crit !== undefined || signature.length === 0 || signature.length > 1024 ||
      signature.toString("base64url") !== parts[2]) return null;
  let material = await codexVerificationMaterial();
  if (!material.algorithms.has(String(header.alg))) return null;
  let key = providerVerificationKey(material.keys, header.kid, String(header.alg));
  if (!key) {
    material = await codexVerificationMaterial(true);
    if (!material.algorithms.has(String(header.alg))) return null;
    key = providerVerificationKey(material.keys, header.kid, String(header.alg));
  }
  if (!key || !crypto.verify("RSA-SHA256", Buffer.from(`${parts[0]}.${parts[1]}`, "utf8"), key, signature)) return null;
  const enrollmentMillis = Date.parse(enrolledAt);
  if (claims.iss !== material.issuer || !Number.isSafeInteger(claims.exp) ||
      !Number.isFinite(enrollmentMillis) || Number(claims.exp) <= enrollmentMillis / 1000) return null;
  const apiClaims = claims["https://api.openai.com/auth"];
  if (!apiClaims || typeof apiClaims !== "object" || Array.isArray(apiClaims)) return null;
  const identity = safeIdentity((apiClaims as Record<string, unknown>).chatgpt_account_id);
  if (!identity) return null;
  return { identity, evidenceRef: `sha256:${crypto.createHash("sha256").update(idToken, "utf8").digest("hex")}` };
}

function canonicalCodexAuthPath() {
  const configured = process.env.CODEX_HOME?.trim();
  const directory = path.resolve(configured || path.join(os.homedir(), ".codex"));
  if (!path.isAbsolute(configured || directory)) throw new Error("codex_native_auth_path_invalid");
  return { directory, file: path.join(directory, "auth.json") };
}

/**
 * Read the native Codex auth.json through a no-follow, owner-only boundary.
 * Callers cannot mint evidence by constructing a JWT-shaped object: the
 * returned object carries an in-process capability whose value is derived
 * while the verified file descriptor is open.
 */
export async function loadCodexNativeAccountBinding(options: { enrolledAt: string }): Promise<CodexNativeAuthBinding> {
  if (!options || typeof options !== "object" || !timestamp.safeParse(options.enrolledAt).success) {
    throw new Error("codex_native_auth_path_invalid");
  }
  const { directory, file } = canonicalCodexAuthPath();
  let directoryStat: fs.Stats;
  try { directoryStat = fs.lstatSync(directory); }
  catch { throw new Error("codex_native_auth_directory_unsafe"); }
  if (directoryStat.isSymbolicLink() || !directoryStat.isDirectory() || fs.realpathSync(directory) !== directory ||
      (directoryStat.mode & 0o7077) !== 0 ||
      (typeof process.getuid === "function" && directoryStat.uid !== process.getuid())) {
    throw new Error("codex_native_auth_directory_unsafe");
  }
  let before: fs.Stats;
  try { before = fs.lstatSync(file); }
  catch { throw new Error("codex_native_auth_file_unsafe"); }
  if (before.isSymbolicLink() || !before.isFile() || before.size <= 0 || before.size > 512 * 1024 ||
      (before.mode & 0o7077) !== 0 ||
      (typeof process.getuid === "function" && before.uid !== process.getuid())) {
    throw new Error("codex_native_auth_file_unsafe");
  }
  let descriptor: number;
  try { descriptor = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW); }
  catch { throw new Error("codex_native_auth_file_unsafe"); }
  let bytes: string;
  try {
    const opened = fs.fstatSync(descriptor);
    if (!opened.isFile() || opened.dev !== before.dev || opened.ino !== before.ino ||
        opened.size <= 0 || opened.size > 512 * 1024 || (opened.mode & 0o7077) !== 0 ||
        (typeof process.getuid === "function" && opened.uid !== process.getuid())) {
      throw new Error("codex_native_auth_file_unsafe");
    }
    const raw = fs.readFileSync(descriptor);
    const after = fs.fstatSync(descriptor);
    let current: fs.Stats;
    try { current = fs.lstatSync(file); }
    catch { throw new Error("codex_native_auth_file_changed"); }
    if (raw.length !== opened.size || after.size !== opened.size || after.mtimeMs !== opened.mtimeMs ||
        after.ctimeMs !== opened.ctimeMs || current.isSymbolicLink() || current.dev !== opened.dev ||
        current.ino !== opened.ino || current.size !== opened.size) {
      throw new Error("codex_native_auth_file_changed");
    }
    try { bytes = new TextDecoder("utf-8", { fatal: true }).decode(raw); }
    catch { throw new Error("codex_native_auth_record_invalid"); }
  } finally {
    fs.closeSync(descriptor);
  }
  let parsed: unknown;
  try { parsed = JSON.parse(bytes); } catch { throw new Error("codex_native_auth_record_invalid"); }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("codex_native_auth_record_invalid");
  let evidence: CodexSignedNativeEvidence | null;
  try { evidence = await parseCodexSignedNativeEvidence(parsed, options.enrolledAt); }
  catch { evidence = null; }
  if (!evidence) throw new Error("account_signed_evidence_unavailable");
  const capability = Object.freeze({}) as CodexNativeAuthBinding;
  codexNativeEvidenceCapabilities.set(capability, Object.freeze({ ...evidence }));
  return capability;
}

/** Resolve evidence only inside this module from the verified capability. */
function resolveCodexSignedNativeEvidence(binding: unknown): CodexSignedNativeEvidence | null {
  if (!binding || typeof binding !== "object") return null;
  return codexNativeEvidenceCapabilities.get(binding) ?? null;
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
  const binding = options.binding ?? (source === "codex" ? undefined : {
    providerAccountId: options.accountIdentity ?? options.providerAccountId ?? options.chatgptAccountId ??
      options.accountId ?? options.accountUuid,
  });
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
  binding: CodexNativeAuthBinding;
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
  bindingKey?: string, installationEpochId?: string) {
  if (assertion.source !== "codex") throw new Error("account_assertion_source_invalid");
  if (!ROOT_ID.test(rootId)) throw new Error("account_root_id_invalid");
  validateAccountAssertionWindow(assertion);
  if (!HASH.test(rootDigest) && !HEX_DIGEST.test(rootDigest)) throw new Error("account_root_digest_invalid");
  if (bindingKey !== undefined && !HASH.test(bindingKey)) throw new Error("account_binding_key_invalid");
  if (installationEpochId !== undefined && !z.string().uuid().safeParse(installationEpochId).success)
    throw new Error("installation_epoch_id_invalid");
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
    const rootBindings = bindings.filter(row => row.rootId === rootId);
    const activeBindings = rootBindings.filter(row => row.active);
    if (activeBindings.length > 1) throw new Error("account_assertion_state_conflict");
    const activePrior = activeBindings[0];
    if (activePrior && Date.parse(assertion.validFrom) <= Date.parse(activePrior.assertion.validFrom)) {
      throw new Error("account_assertion_time_regression");
    }
    const nextFrom = Date.parse(assertion.validFrom);
    const nextUntil = assertion.validUntil === null ? Number.POSITIVE_INFINITY : Date.parse(assertion.validUntil);
    if (rootBindings.some(row => !row.active &&
        nextFrom < (row.assertion.validUntil === null ? Number.POSITIVE_INFINITY : Date.parse(row.assertion.validUntil)) &&
        Date.parse(row.assertion.validFrom) < nextUntil)) {
      throw new Error("account_assertion_window_overlap");
    }
    closePriorCodexAssertions(state, rootId, assertion.validFrom);
    const next: AccountAssertionBindingState = {
      rootId, bindingDigest, ...(bindingKey ? { bindingKey } : {}),
      ...(installationEpochId ? { installationEpochId } : {}), assertion, active: true, createdAt: assertion.validFrom,
    };
    const candidates = [next, ...bindings.filter(row => row.bindingDigest !== bindingDigest)];
    // Never let churn on one root evict an active interval for another root.
    // If active state alone exceeds the bounded maintenance record, refuse the
    // mutation and leave the transaction unchanged.
    const active = candidates.filter(row => row.active);
    if (active.length > 1024) throw new Error("account_assertion_state_capacity");
    const inactive = candidates.filter(row => !row.active);
    state.bindings.codex = [...active, ...inactive.slice(0, 1024 - active.length)];
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
    binding: CodexNativeAuthBinding;
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
export function codexAccountAssertionBindings(db: DB, rootId: string): AccountAssertionBindingState[] {
  return readAccountAssertionAdapterState(db).bindings.codex
    .filter(binding => binding.rootId === rootId)
    .sort((a, b) => Date.parse(a.assertion.validFrom) - Date.parse(b.assertion.validFrom))
    .map(binding => ({ ...binding, assertion: accountAssertionV1Schema.parse(binding.assertion) }));
}

export function codexAccountAssertionIntervals(db: DB, rootId: string): AccountAssertionV1[] {
  return codexAccountAssertionBindings(db, rootId).map(binding => binding.assertion);
}

export function codexAccountAssertionAt(db: DB, rootId: string, at: string): AccountAssertionV1 | null {
  const matches = codexAccountAssertionIntervals(db, rootId).filter(assertion => accountAssertionContains(assertion, at));
  return matches.length === 1 ? matches[0] : null;
}
