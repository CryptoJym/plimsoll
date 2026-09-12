import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";

import type { LocalProducerSource } from "./http-boundary";
import { HttpBoundaryRejection } from "./http-boundary";

/**
 * These are Plimsoll credentials, not provider credentials. They are created
 * from local randomness and are only ever persisted in the private Plimsoll
 * home. Each value deliberately has a separate audience: a producer token
 * cannot be replayed as a management credential, and Claude/Codex/Gemini/Grok
 * cannot impersonate one another.
 */
export type LocalIngestAuth = {
  version: 1;
  claudeCodeProducer: string;
  codexProducer: string;
  /** Added for the Gemini CLI OTLP source; absent only in legacy files. */
  geminiCliProducer?: string;
  /** Added for the Grok hook source; absent only in legacy files. */
  grokProducer?: string;
  managementRead: string;
  /**
   * Bounded rotation grace windows keyed by producer audience. A rotation
   * cannot atomically restart every already-running producer, so the
   * superseded token stays acceptable until `expiresAt` and then stops. Only
   * producer audiences are covered; the management credential has no window.
   */
  rotations?: Partial<Record<LocalProducerSource, LocalProducerRotation>>;
};

export type LocalProducerRotation = {
  /** Superseded producer token; never equal to a current token. */
  token: string;
  /** Epoch milliseconds at which the superseded token stops being accepted. */
  expiresAt: number;
};

export const LOCAL_INGEST_AUTH_FILE = "local-ingest-auth.json";
/** Default grace window: long enough to restart a tool, short enough to matter. */
export const DEFAULT_PRODUCER_ROTATION_GRACE_MS = 15 * 60 * 1000;
export const MAX_PRODUCER_ROTATION_GRACE_MS = 24 * 60 * 60 * 1000;
const LOCAL_INGEST_AUTH_VERSION = 1;
const TOKEN_BYTES = 32;
const TOKEN_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const PRODUCER_TOKEN_FIELDS = {
  claude_code: "claudeCodeProducer",
  codex: "codexProducer",
  gemini_cli: "geminiCliProducer",
  grok: "grokProducer",
} as const satisfies Record<LocalProducerSource, keyof LocalIngestAuth>;

function authPath(home: string) {
  return path.join(home, LOCAL_INGEST_AUTH_FILE);
}

function newToken() {
  return crypto.randomBytes(TOKEN_BYTES).toString("base64url");
}

function newAuth(): LocalIngestAuth {
  return Object.freeze({
    version: LOCAL_INGEST_AUTH_VERSION,
    claudeCodeProducer: newToken(),
    codexProducer: newToken(),
    geminiCliProducer: newToken(),
    grokProducer: newToken(),
    managementRead: newToken(),
  });
}

function validRotations(value: unknown, currentTokens: string[]): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const entries = Object.entries(value as Record<string, unknown>);
  if (entries.length === 0) return false;
  return entries.every(([source, rotation]) => {
    if (!Object.hasOwn(PRODUCER_TOKEN_FIELDS, source)) return false;
    if (!rotation || typeof rotation !== "object" || Array.isArray(rotation)) return false;
    const record = rotation as Record<string, unknown>;
    if (Object.keys(record).sort().join(",") !== "expiresAt,token") return false;
    return typeof record.token === "string" &&
      TOKEN_PATTERN.test(record.token) &&
      !currentTokens.includes(record.token) &&
      typeof record.expiresAt === "number" &&
      Number.isSafeInteger(record.expiresAt) &&
      record.expiresAt > 0;
  });
}

function validAuth(value: unknown): value is LocalIngestAuth {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  const legacyKeys = "claudeCodeProducer,codexProducer,managementRead,version";
  const geminiKeys = "claudeCodeProducer,codexProducer,geminiCliProducer,managementRead,version";
  const currentKeys = "claudeCodeProducer,codexProducer,geminiCliProducer,grokProducer,managementRead,version";
  const rotatedKeys = "claudeCodeProducer,codexProducer,geminiCliProducer,grokProducer,managementRead,rotations,version";
  if (keys.join(",") !== legacyKeys && keys.join(",") !== geminiKeys &&
    keys.join(",") !== currentKeys && keys.join(",") !== rotatedKeys) {
    return false;
  }
  if (keys.join(",") === rotatedKeys && !validRotations(record.rotations, [
    record.claudeCodeProducer,
    record.codexProducer,
    record.geminiCliProducer,
    record.grokProducer,
    record.managementRead,
  ].filter((token): token is string => typeof token === "string"))) {
    return false;
  }
  const geminiValid = record.geminiCliProducer === undefined ||
    (typeof record.geminiCliProducer === "string" && TOKEN_PATTERN.test(record.geminiCliProducer));
  const grokValid = record.grokProducer === undefined ||
    (typeof record.grokProducer === "string" && TOKEN_PATTERN.test(record.grokProducer));
  return record.version === LOCAL_INGEST_AUTH_VERSION &&
    typeof record.claudeCodeProducer === "string" &&
    typeof record.codexProducer === "string" &&
    typeof record.managementRead === "string" &&
    geminiValid &&
    grokValid &&
    TOKEN_PATTERN.test(record.claudeCodeProducer) &&
    TOKEN_PATTERN.test(record.codexProducer) &&
    TOKEN_PATTERN.test(record.managementRead) &&
    new Set([
      record.claudeCodeProducer,
      record.codexProducer,
      record.geminiCliProducer,
      record.grokProducer,
      record.managementRead,
    ].filter((token): token is string => typeof token === "string")).size ===
      3 + Number(record.geminiCliProducer !== undefined) + Number(record.grokProducer !== undefined);
}

function isPrivateRegularFile(file: string) {
  try {
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink()) return false;
    if ((stat.mode & 0o7077) !== 0) return false;
    if (typeof process.getuid === "function" && stat.uid !== process.getuid()) return false;
    return true;
  } catch {
    return false;
  }
}

function isPrivateDirectory(directory: string) {
  try {
    const stat = fs.lstatSync(directory);
    return stat.isDirectory() &&
      !stat.isSymbolicLink() &&
      (stat.mode & 0o7077) === 0 &&
      (typeof process.getuid !== "function" || stat.uid === process.getuid());
  } catch {
    return false;
  }
}

/**
 * Read-only credential lookup. A missing or malformed file is represented as
 * null so doctor and lifecycle probes can remain side-effect free. The
 * provisioning path distinguishes missing from malformed and fails closed on
 * the latter rather than silently replacing an operator's file.
 */
export function readLocalIngestAuth(home: string): LocalIngestAuth | null {
  if (!isPrivateDirectory(home)) return null;
  const file = authPath(home);
  if (!isPrivateRegularFile(file)) return null;
  try {
    const parsed = JSON.parse(fs.readFileSync(file, "utf8")) as unknown;
    return validAuth(parsed) ? Object.freeze({ ...parsed }) : null;
  } catch {
    return null;
  }
}

function authFileExists(home: string) {
  try {
    return fs.lstatSync(authPath(home)).isFile();
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ENOENT";
  }
}

/**
 * Internal: the fill write found the credential file had moved on while it was
 * being written, so it abandoned rather than rename a stale copy over it.
 */
class LocalIngestAuthDrift extends Error {
  constructor() {
    super("local_ingest_auth_drift");
  }
}

type WriteAuthOptions = {
  /**
   * Abandon the write instead of renaming when the credential file no longer
   * carries this stamp. Omitted means write unconditionally.
   */
  abandonUnlessStamp?: string | null;
};

function writeAuth(
  home: string,
  auth: LocalIngestAuth,
  overwrite: boolean,
  options: WriteAuthOptions = {},
) {
  fs.mkdirSync(home, { recursive: true, mode: 0o700 });
  if (!isPrivateDirectory(home)) throw new Error("local_ingest_auth_home_unsafe");
  const file = authPath(home);
  const temporary = `${file}.${process.pid}.${crypto.randomBytes(8).toString("hex")}.tmp`;
  let descriptor: number | undefined;
  try {
    descriptor = fs.openSync(temporary, "wx", 0o600);
    const body = `${JSON.stringify(auth)}\n`;
    fs.writeFileSync(descriptor, body, "utf8");
    fs.fsyncSync(descriptor);
    fs.closeSync(descriptor);
    descriptor = undefined;
    if (!overwrite) {
      try {
        fs.linkSync(temporary, file);
        fs.unlinkSync(temporary);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        fs.unlinkSync(temporary);
        const existing = readLocalIngestAuth(home);
        if (existing) return existing;
        throw new Error("local_ingest_auth_invalid");
      }
    } else {
      // The drift guard belongs here, not before the write: `mkdirSync` ->
      // `openSync` -> `writeFileSync` -> `fsyncSync` is a real fsync latency,
      // and a rotation that renames its file into place inside it would be
      // overwritten by a copy of the file this write started from -- putting
      // the operator-revoked token back as `current` with no deadline. Checked
      // immediately before the rename, the only window left is the rename
      // itself, with no syscall in between to widen it.
      if (options.abandonUnlessStamp !== undefined &&
        localIngestAuthStamp(home) !== options.abandonUnlessStamp) {
        throw new LocalIngestAuthDrift();
      }
      fs.renameSync(temporary, file);
    }
    return auth;
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
    try {
      fs.unlinkSync(temporary);
    } catch {
      // The temporary file was already linked, renamed, or never created.
    }
  }
}

function writeNewAuth(home: string, overwrite: boolean) {
  return writeAuth(home, newAuth(), overwrite);
}

/**
 * Producer audiences this process minted for a legacy credential file whose
 * fill write could not land, keyed by resolved credential home. Without it a
 * load in an unwritable home hands out a different gemini/grok token on every
 * call, so a producer configured against one of them is refused by the next
 * one. Process-lifetime only: the stored file, once writable again, wins.
 */
const unpersistedFills = new Map<string, UnpersistedFill>();

type UnpersistedFill = {
  geminiCliProducer: string;
  grokProducer: string;
  /** The audiences the stored file was actually missing. */
  audiences: LocalProducerSource[];
};

function homeKey(home: string) {
  return path.resolve(home);
}

/** Value-blind: which producer audiences the stored authority is missing. */
function missingProducerAudiences(auth: LocalIngestAuth): LocalProducerSource[] {
  const missing: LocalProducerSource[] = [];
  if (!auth.geminiCliProducer) missing.push("gemini_cli");
  if (!auth.grokProducer) missing.push("grok");
  return missing;
}

function settledAuth(existing: LocalIngestAuth, now: number, home: string) {
  const live = withoutClosedRotations(existing, now);
  const minted = unpersistedFills.get(homeKey(home));
  return {
    ...live,
    geminiCliProducer: live.geminiCliProducer ?? minted?.geminiCliProducer ?? newToken(),
    grokProducer: live.grokProducer ?? minted?.grokProducer ?? newToken(),
  };
}

/**
 * The authority to return when the fill write was abandoned. The writer that
 * won the race fills both producer audiences on its own write, so re-reading
 * costs one read and hands back the values that are actually on disk instead
 * of this process's discarded copy.
 */
function authAfterDrift(home: string, now: number, abandoned: LocalIngestAuth): LocalIngestAuth {
  const reread = readLocalIngestAuth(home);
  if (!reread) return abandoned;
  const live = withoutClosedRotations(reread, now);
  return live.geminiCliProducer && live.grokProducer ? Object.freeze(live) : abandoned;
}

function homeAcceptsAFillWrite(home: string) {
  try {
    fs.accessSync(home, fs.constants.W_OK | fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

/**
 * Value-blind: the producer audiences this host is serving from memory because
 * a legacy credential file is missing them and the fill write cannot land. In
 * the process that took the fail-soft load these are the audiences it actually
 * minted; in a separate read-only process (doctor) the condition is derived
 * from the stored file plus the home's write permission, so it is reportable
 * without a write of any kind. Never returns or hashes a token.
 */
export function unpersistedProducerAudiences(home: string): LocalProducerSource[] {
  const minted = unpersistedFills.get(homeKey(home));
  if (minted) return [...minted.audiences];
  const existing = readLocalIngestAuth(home);
  if (!existing) return [];
  const missing = missingProducerAudiences(existing);
  if (missing.length === 0 || homeAcceptsAFillWrite(home)) return [];
  return missing;
}

/**
 * Provision once and return the same values on every subsequent call. A load
 * drops rotation windows that have already closed, so a restart never carries
 * a superseded token forward -- but it drops them in memory only. The stored
 * file belongs to `rotateLocalProducerToken`, which composes its own live
 * rotations on the write that supersedes a token. The single write left here
 * fills producer audiences a legacy file is missing; it abandons that write
 * when the file moved on underneath it -- re-checked immediately before the
 * rename, not merely before the write -- and never crashes the caller when the
 * home cannot be written. A fill that cannot land is remembered per home, so
 * the authority this process serves stays the same on every later call, and
 * `unpersistedProducerAudiences` names the audiences that are not on disk.
 */
export function loadOrCreateLocalIngestAuth(
  home: string,
  options: { dryRun?: boolean } = {},
): LocalIngestAuth {
  const now = Date.now();
  const stamp = localIngestAuthStamp(home);
  const existing = readLocalIngestAuth(home);
  if (existing) {
    const live = withoutClosedRotations(existing, now);
    if (live.geminiCliProducer && live.grokProducer) {
      // The stored file carries both audiences, so nothing is held in memory
      // for this home any more and doctor must stop reporting it.
      unpersistedFills.delete(homeKey(home));
      return Object.freeze(live);
    }
    const missing = missingProducerAudiences(live);
    const settled = Object.freeze(settledAuth(existing, now, home));
    if (options.dryRun) return settled;
    // Abandon on drift: a write landed between the read above and here, so
    // that file is the newer one and this copy must not be renamed over it.
    if (localIngestAuthStamp(home) !== stamp) return authAfterDrift(home, now, settled);
    try {
      const written = writeAuth(home, settled, true, { abandonUnlessStamp: stamp });
      unpersistedFills.delete(homeKey(home));
      return written;
    } catch (error) {
      // The same abandon-on-drift decision, taken inside the write where the
      // rename actually happens.
      if (error instanceof LocalIngestAuthDrift) return authAfterDrift(home, now, settled);
      // A home this process cannot write must not turn a load into a crash on
      // the path that feeds ingestion. Remember exactly what was minted so the
      // authority this host serves is stable for the life of the process, and
      // so doctor can report which audiences are not on disk.
      unpersistedFills.set(homeKey(home), {
        geminiCliProducer: settled.geminiCliProducer!,
        grokProducer: settled.grokProducer!,
        audiences: missing,
      });
      return settled;
    }
  }
  if (authFileExists(home)) throw new Error("local_ingest_auth_invalid");
  return options.dryRun ? newAuth() : writeNewAuth(home, false);
}

/** Explicit rotation boundary for local operators; never reads tool accounts. */
export function rotateLocalIngestAuth(home: string): LocalIngestAuth {
  return writeNewAuth(home, true);
}

function liveRotations(auth: LocalIngestAuth, now: number) {
  return Object.fromEntries(
    Object.entries(auth.rotations ?? {}).filter(([, rotation]) => rotation.expiresAt > now),
  ) as Partial<Record<LocalProducerSource, LocalProducerRotation>>;
}

function closedRotationCount(auth: LocalIngestAuth, now: number) {
  return Object.values(auth.rotations ?? {}).filter((rotation) => rotation.expiresAt <= now).length;
}

/**
 * The same authority with every closed rotation window removed. The key is
 * dropped entirely when nothing is left, because an empty `rotations` object
 * is not a valid stored shape.
 */
function withoutClosedRotations(auth: LocalIngestAuth, now: number): LocalIngestAuth {
  if (closedRotationCount(auth, now) === 0) return auth;
  const live = liveRotations(auth, now);
  const { rotations: _closed, ...rest } = auth;
  return Object.keys(live).length === 0 ? { ...rest } : { ...rest, rotations: live };
}

/**
 * Mint a new producer token for one source and keep the superseded value
 * acceptable for a bounded window. Expired windows for every source are
 * dropped in the same write so a stale token can never be resurrected.
 */
export function rotateLocalProducerToken(
  home: string,
  source: LocalProducerSource,
  options: { graceMs?: number; now?: number } = {},
): { auth: LocalIngestAuth; expiresAt: number } {
  const graceMs = options.graceMs ?? DEFAULT_PRODUCER_ROTATION_GRACE_MS;
  if (!Number.isSafeInteger(graceMs) || graceMs <= 0 || graceMs > MAX_PRODUCER_ROTATION_GRACE_MS) {
    throw new Error("local_ingest_auth_rotation_grace_invalid");
  }
  const existing = readLocalIngestAuth(home);
  if (!existing) {
    throw new Error(
      authFileExists(home) ? "local_ingest_auth_invalid" : "local_ingest_auth_missing",
    );
  }
  const field = PRODUCER_TOKEN_FIELDS[source];
  const superseded = existing[field];
  if (typeof superseded !== "string") throw new Error("local_ingest_auth_source_unprovisioned");
  const now = options.now ?? Date.now();
  const expiresAt = now + graceMs;
  let replacement = newToken();
  while (replacement === superseded) replacement = newToken();
  const rotations = { ...liveRotations(existing, now), [source]: { token: superseded, expiresAt } };
  const next = {
    version: LOCAL_INGEST_AUTH_VERSION,
    claudeCodeProducer: existing.claudeCodeProducer,
    codexProducer: existing.codexProducer,
    geminiCliProducer: existing.geminiCliProducer ?? newToken(),
    grokProducer: existing.grokProducer ?? newToken(),
    managementRead: existing.managementRead,
    [field]: replacement,
    rotations,
  } as LocalIngestAuth;
  return { auth: writeAuth(home, next, true), expiresAt };
}

/**
 * The stored authority with every rotation window whose deadline has passed
 * dropped in memory, so a reload can never carry a superseded token forward
 * even if the clock moved backwards afterwards.
 *
 * Deliberately read-only. A reader that rewrote the credential file would
 * race `rotateLocalProducerToken`: inside the reader's read -> rename window
 * the rotation renames its new file into place, the reader then renames its
 * pruned copy of the *old* file over it, and the token the operator just
 * revoked is live again as `current` with no deadline. Closed rows leave the
 * disk on the next rotation, which composes its own live rotations; until
 * then this prune and the deadline check in `assertProducerToken` both keep
 * the superseded token out, and doctor reports the row as expired.
 *
 * Null when the file is missing or malformed, in which case callers keep
 * whatever authority they already hold.
 */
export function readLiveProducerAuth(
  home: string,
  now = Date.now(),
): LocalIngestAuth | null {
  const existing = readLocalIngestAuth(home);
  if (!existing) return null;
  return Object.freeze(withoutClosedRotations(existing, now));
}

/**
 * Cheap change stamp for the credential file. The collector caches the
 * authority it loaded at start, so a rotation is only observable to a running
 * daemon if it can tell the file moved on without re-reading it every request.
 */
export function localIngestAuthStamp(home: string): string | null {
  try {
    const stat = fs.lstatSync(authPath(home));
    return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}`;
  } catch {
    return null;
  }
}

/** Value-blind rotation state for doctor; never returns or hashes a token. */
export function producerRotationState(
  auth: LocalIngestAuth | null,
  source: LocalProducerSource,
  now = Date.now(),
): { state: "none" | "active" | "expired"; expiresAt: string | null; secondsRemaining: number | null } {
  const rotation = auth?.rotations?.[source];
  if (!rotation) return { state: "none", expiresAt: null, secondsRemaining: null };
  const expiresAt = new Date(rotation.expiresAt).toISOString();
  if (rotation.expiresAt <= now) return { state: "expired", expiresAt, secondsRemaining: 0 };
  return {
    state: "active",
    expiresAt,
    secondsRemaining: Math.ceil((rotation.expiresAt - now) / 1000),
  };
}

function suppliedToken(request: http.IncomingMessage) {
  const value = request.headers["x-plimsoll-token"];
  return Array.isArray(value) ? undefined : value;
}

function tokenMatches(supplied: string | undefined, expected: string) {
  if (supplied === undefined) return false;
  const left = Buffer.from(supplied);
  const right = Buffer.from(expected);
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function assertCredentialRoute(url: URL, kind: "management" | "producer") {
  const allowed = kind === "management"
    ? url.pathname === "/status" || url.pathname === "/" || url.pathname === "/index.html" || url.pathname.startsWith("/api/")
    : url.pathname.startsWith("/hooks/") ||
      ["/v1/logs", "/v1/traces", "/v1/metrics", "/gemini/v1/logs", "/gemini/v1/traces", "/gemini/v1/metrics"].includes(url.pathname);
  if (!allowed) throw new HttpBoundaryRejection("internal_rejection", 400);
}

export function assertManagementCredential(
  request: http.IncomingMessage,
  auth: LocalIngestAuth,
  url: URL,
) {
  assertCredentialRoute(url, "management");
  const supplied = suppliedToken(request);
  if (supplied === undefined) {
    throw new HttpBoundaryRejection("management_credential_required", 401);
  }
  if (!tokenMatches(supplied, auth.managementRead)) {
    throw new HttpBoundaryRejection("management_credential_invalid", 401);
  }
}

export function assertProducerToken(
  request: http.IncomingMessage,
  auth: LocalIngestAuth,
  source: LocalProducerSource,
  url: URL,
) {
  assertCredentialRoute(url, "producer");
  const supplied = suppliedToken(request) ??
    (source === "gemini_cli" && url.pathname.startsWith("/gemini/")
      ? url.searchParams.get("x-plimsoll-token") ?? undefined
      : undefined);
  if (supplied === undefined) {
    throw new HttpBoundaryRejection("producer_token_required", 401);
  }
  const expected = source === "claude_code"
    ? auth.claudeCodeProducer
    : source === "codex"
      ? auth.codexProducer
      : source === "gemini_cli"
        ? auth.geminiCliProducer
        : auth.grokProducer;
  if (!expected) throw new HttpBoundaryRejection("producer_token_invalid", 401);
  if (tokenMatches(supplied, expected)) return;
  // Rotation grace: the superseded token stays acceptable only until its
  // recorded expiry, so a producer that has not been restarted yet keeps
  // reporting and a leaked old token still stops working on a fixed deadline.
  const rotation = auth.rotations?.[source];
  if (rotation && rotation.expiresAt > Date.now() && tokenMatches(supplied, rotation.token)) {
    return;
  }
  throw new HttpBoundaryRejection("producer_token_invalid", 401);
}
