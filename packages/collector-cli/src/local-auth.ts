import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";

import type { LocalProducerSource } from "./http-boundary";
import { HttpBoundaryRejection } from "./http-boundary";

/**
 * These are Plimsoll credentials, not provider credentials. They are created
 * from local randomness and are only ever persisted in the private Plimsoll
 * home. The producer values deliberately have separate audiences: a producer
 * token cannot be replayed as a management credential, and the supported
 * producers cannot impersonate one another.
 */
export type LocalIngestAuth = {
  version: 1;
  claudeCodeProducer: string;
  codexProducer: string;
  /** Added for Cursor without invalidating legacy two-producer auth files. */
  cursorProducer?: string;
  managementRead: string;
};

export const LOCAL_INGEST_AUTH_FILE = "local-ingest-auth.json";
const LOCAL_INGEST_AUTH_VERSION = 1;
const TOKEN_BYTES = 32;
const TOKEN_PATTERN = /^[A-Za-z0-9_-]{43}$/;

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
    cursorProducer: newToken(),
    managementRead: newToken(),
  });
}

function validAuth(value: unknown): value is LocalIngestAuth {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  const legacyKeys = "claudeCodeProducer,codexProducer,managementRead,version";
  const currentKeys = "claudeCodeProducer,codexProducer,cursorProducer,managementRead,version";
  if (keys.join(",") !== legacyKeys && keys.join(",") !== currentKeys) {
    return false;
  }
  const cursorValid = record.cursorProducer === undefined || (
    typeof record.cursorProducer === "string" && TOKEN_PATTERN.test(record.cursorProducer)
  );
  return record.version === LOCAL_INGEST_AUTH_VERSION &&
    typeof record.claudeCodeProducer === "string" &&
    typeof record.codexProducer === "string" &&
    typeof record.managementRead === "string" &&
    TOKEN_PATTERN.test(record.claudeCodeProducer) &&
    TOKEN_PATTERN.test(record.codexProducer) &&
    TOKEN_PATTERN.test(record.managementRead) &&
    cursorValid &&
    new Set([
      record.claudeCodeProducer,
      record.codexProducer,
      record.cursorProducer,
      record.managementRead,
    ].filter((token): token is string => typeof token === "string")).size ===
      (record.cursorProducer === undefined ? 3 : 4);
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

function writeNewAuth(home: string, overwrite: boolean) {
  fs.mkdirSync(home, { recursive: true, mode: 0o700 });
  if (!isPrivateDirectory(home)) throw new Error("local_ingest_auth_home_unsafe");
  const file = authPath(home);
  const temporary = `${file}.${process.pid}.${crypto.randomBytes(8).toString("hex")}.tmp`;
  const auth = newAuth();
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

/** Provision once and return the same values on every subsequent call. */
export function loadOrCreateLocalIngestAuth(home: string): LocalIngestAuth {
  const existing = readLocalIngestAuth(home);
  if (existing) return existing;
  if (authFileExists(home)) throw new Error("local_ingest_auth_invalid");
  return writeNewAuth(home, false);
}

/** Explicit rotation boundary for local operators; never reads tool accounts. */
export function rotateLocalIngestAuth(home: string): LocalIngestAuth {
  return writeNewAuth(home, true);
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
    : url.pathname.startsWith("/hooks/") || ["/v1/logs", "/v1/traces", "/v1/metrics"].includes(url.pathname);
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
  const supplied = suppliedToken(request);
  if (supplied === undefined) {
    throw new HttpBoundaryRejection("producer_token_required", 401);
  }
  const expected = source === "claude_code"
    ? auth.claudeCodeProducer
    : source === "codex"
      ? auth.codexProducer
      // Legacy auth files predate Cursor. Preserve their validity and use the
      // existing producer audience until the operator rotates credentials.
      : auth.cursorProducer ?? auth.codexProducer;
  if (!tokenMatches(supplied, expected)) {
    throw new HttpBoundaryRejection("producer_token_invalid", 401);
  }
}
