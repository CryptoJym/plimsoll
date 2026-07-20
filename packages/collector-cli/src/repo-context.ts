import path from "node:path";

import {
  toolSourceSchema,
  type AiInteractionEvent,
  type ToolSource,
} from "../../shared/src/index";
import { resolveGitContextUncached } from "./git-context";

const MAX_CWD_BYTES = 4_096;
const MAX_OCCURRENCE_BYTES = 512;
const MAX_REQUESTS = 8;
const MAX_EXTRACTION_NODES = 512;
const MAX_JSON_ARGUMENT_BYTES = 16 * 1_024;

const REPO_CONTEXT_ID = /^repoctx:v1:[0-9a-f]{64}$/;
const LINKAGE_HASH = /^sha256:[0-9a-f]{64}$/;
const HEAD_SHA = /^[0-9a-f]{40}$/;
const CONTROL_CHARACTER = /[\u0000-\u001f\u007f-\u009f]/;
export const REPO_CONTEXT_RESOLVER_VERSION = "git-context:v1";

const CWD_KEYS = new Set([
  "cwd",
  "current_working_directory",
  "workdir",
  "working_directory",
]);
const ARGUMENT_KEYS = new Set(["arguments", "args", "tool_arguments"]);

export type RepoContextRequest = {
  contextId: string;
  source: ToolSource;
  cwd: string;
};

export type RepoContextResult = {
  contextId: string;
  repoHash: string | null;
  branchHash: string | null;
  headSha: string | null;
  resolvedAt: string;
  resolverVersion: string;
};

export type RepoContextSidecar = Readonly<{
  occurrence: string;
  cwd: string;
}>;

const sidecars = new WeakMap<AiInteractionEvent, RepoContextSidecar>();
const boundContextIds = new WeakMap<AiInteractionEvent, string>();

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactKeys(record: Record<string, unknown>, expected: readonly string[]) {
  const actual = Object.keys(record).sort();
  const keys = [...expected].sort();
  return actual.length === keys.length && actual.every((key, index) => key === keys[index]);
}

function isWellFormedUnicode(value: string) {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next < 0xdc00 || next > 0xdfff) return false;
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return false;
    }
  }
  return true;
}

function boundedUtf8(value: string, maxBytes: number) {
  return isWellFormedUnicode(value) && Buffer.byteLength(value, "utf8") <= maxBytes;
}

export function canonicalRepoContextCwd(value: unknown): string | undefined {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    CONTROL_CHARACTER.test(value) ||
    !boundedUtf8(value, MAX_CWD_BYTES)
  ) return undefined;

  const windows = process.platform === "win32";
  if (windows) {
    const driveAbsolute = /^[a-zA-Z]:[\\/]/.test(value);
    const uncAbsolute = /^\\\\[^\\/]+[\\/][^\\/]+(?:[\\/]|$)/.test(value);
    if ((!driveAbsolute && !uncAbsolute) || !path.win32.isAbsolute(value)) return undefined;
  } else if (!path.posix.isAbsolute(value)) {
    return undefined;
  }
  const flavor = windows ? path.win32 : path.posix;
  let canonical = flavor.normalize(value);
  const root = flavor.parse(canonical).root;
  while (canonical.length > root.length && canonical.endsWith(flavor.sep)) {
    canonical = canonical.slice(0, -1);
  }
  if (
    canonical.length === 0 ||
    CONTROL_CHARACTER.test(canonical) ||
    !boundedUtf8(canonical, MAX_CWD_BYTES)
  ) return undefined;
  return canonical;
}

export function validRepoContextCwd(value: unknown): value is string {
  return typeof value === "string" && canonicalRepoContextCwd(value) === value;
}

export function validRepoContextOccurrence(value: unknown): value is string {
  return typeof value === "string" &&
    value.length > 0 &&
    !CONTROL_CHARACTER.test(value) &&
    boundedUtf8(value, MAX_OCCURRENCE_BYTES);
}

export function attachRepoContextSidecar(
  event: AiInteractionEvent,
  occurrence: string,
  cwd: string,
): boolean {
  const canonicalCwd = canonicalRepoContextCwd(cwd);
  if (!isRecord(event) || !validRepoContextOccurrence(occurrence) || !canonicalCwd) {
    return false;
  }
  if (sidecars.has(event) || boundContextIds.has(event)) return false;
  sidecars.set(event, Object.freeze({ occurrence, cwd: canonicalCwd }));
  return true;
}

/** Bind an event to a context occurrence that was staged in an earlier slice. */
export function attachRepoContextId(event: AiInteractionEvent, contextId: string): boolean {
  if (
    !isRecord(event) ||
    !validRepoContextId(contextId) ||
    sidecars.has(event) ||
    boundContextIds.has(event)
  ) return false;
  boundContextIds.set(event, contextId);
  return true;
}

export function peekRepoContextSidecar(event: AiInteractionEvent): RepoContextSidecar | undefined {
  return isRecord(event) ? sidecars.get(event) : undefined;
}

export function takeRepoContextSidecar(event: AiInteractionEvent): RepoContextSidecar | undefined {
  if (!isRecord(event)) return undefined;
  const sidecar = sidecars.get(event);
  sidecars.delete(event);
  return sidecar;
}

export function peekRepoContextId(event: AiInteractionEvent): string | undefined {
  return isRecord(event) ? boundContextIds.get(event) : undefined;
}

export function takeRepoContextId(event: AiInteractionEvent): string | undefined {
  if (!isRecord(event)) return undefined;
  const contextId = boundContextIds.get(event);
  boundContextIds.delete(event);
  return contextId;
}

function otlpScalar(value: unknown): unknown {
  if (!isRecord(value)) return value;
  for (const key of ["stringValue", "string_value"] as const) {
    if (key in value) return value[key];
  }
  return value;
}

function parseArguments(value: unknown): unknown {
  const scalar = otlpScalar(value);
  if (typeof scalar !== "string" || Buffer.byteLength(scalar, "utf8") > MAX_JSON_ARGUMENT_BYTES) {
    return undefined;
  }
  try {
    return JSON.parse(scalar) as unknown;
  } catch {
    return undefined;
  }
}

/**
 * Extract a validated working directory from raw hook payloads or OTLP
 * records before policy sanitization. Traversal is deliberately bounded and
 * the raw value is returned only to the transient sidecar/worker path.
 */
export function extractRepoContextCwd(value: unknown): string | undefined {
  const pending: unknown[] = [value];
  const seen = new Set<object>();
  let visited = 0;

  while (pending.length > 0 && visited < MAX_EXTRACTION_NODES) {
    const current = pending.shift();
    visited += 1;

    if (Array.isArray(current)) {
      if (seen.has(current)) continue;
      seen.add(current);
      pending.push(...current.slice(0, MAX_EXTRACTION_NODES - visited));
      continue;
    }
    if (!isRecord(current)) continue;
    if (seen.has(current)) continue;
    seen.add(current);

    for (const key of CWD_KEYS) {
      const candidate = otlpScalar(current[key]);
      const canonical = canonicalRepoContextCwd(candidate);
      if (canonical) return canonical;
    }

    if (typeof current.key === "string" && CWD_KEYS.has(current.key)) {
      const candidate = otlpScalar(current.value);
      const canonical = canonicalRepoContextCwd(candidate);
      if (canonical) return canonical;
    }

    if (typeof current.key === "string" && ARGUMENT_KEYS.has(current.key)) {
      const parsed = parseArguments(current.value);
      if (parsed !== undefined) pending.push(parsed);
    }

    for (const key of ARGUMENT_KEYS) {
      if (!(key in current)) continue;
      const parsed = parseArguments(current[key]);
      if (parsed !== undefined) pending.push(parsed);
    }

    for (const key of [
      "attributes",
      "otelAttributes",
      "resource",
      "scope",
      "body",
      "resourceLogs",
      "scopeLogs",
      "logRecords",
      "resourceSpans",
      "scopeSpans",
      "spans",
    ] as const) {
      if (key in current) pending.push(current[key]);
    }
  }

  return undefined;
}

export function validRepoContextId(value: unknown): value is string {
  return typeof value === "string" && REPO_CONTEXT_ID.test(value);
}

export function validRepoContextRequest(value: unknown): value is RepoContextRequest {
  if (!isRecord(value) || !exactKeys(value, ["contextId", "source", "cwd"])) return false;
  return validRepoContextId(value.contextId) &&
    toolSourceSchema.safeParse(value.source).success &&
    validRepoContextCwd(value.cwd);
}

function validResolvedAt(value: unknown): value is string {
  if (typeof value !== "string") return false;
  const milliseconds = Date.parse(value);
  return Number.isFinite(milliseconds) && new Date(milliseconds).toISOString() === value;
}

export function validRepoContextResult(value: unknown): value is RepoContextResult {
  if (
    !isRecord(value) ||
    !exactKeys(value, [
      "contextId",
      "repoHash",
      "branchHash",
      "headSha",
      "resolvedAt",
      "resolverVersion",
    ])
  ) return false;
  return validRepoContextId(value.contextId) &&
    (value.repoHash === null || (typeof value.repoHash === "string" && LINKAGE_HASH.test(value.repoHash))) &&
    (value.branchHash === null || (typeof value.branchHash === "string" && LINKAGE_HASH.test(value.branchHash))) &&
    (value.headSha === null || (typeof value.headSha === "string" && HEAD_SHA.test(value.headSha))) &&
    validResolvedAt(value.resolvedAt) &&
    value.resolverVersion === REPO_CONTEXT_RESOLVER_VERSION;
}

export function resolveRepoContextRequests(
  requests: readonly RepoContextRequest[],
  options: { onRepoLabel?: (repoHash: string, label: string) => void } = {},
): RepoContextResult[] {
  if (
    !Array.isArray(requests) ||
    requests.length > MAX_REQUESTS ||
    !requests.every(validRepoContextRequest)
  ) return [];

  return requests.map((request) => {
    const context = resolveGitContextUncached(request.cwd);
    const repoHash = context?.remoteUrlHash;
    const branchHash = context?.branchHash;
    const headSha = context?.headSha?.toLowerCase();
    if (repoHash && LINKAGE_HASH.test(repoHash) && context?.remoteLabel) {
      options.onRepoLabel?.(repoHash, context.remoteLabel);
    }
    return {
      contextId: request.contextId,
      repoHash: repoHash && LINKAGE_HASH.test(repoHash) ? repoHash : null,
      branchHash: branchHash && LINKAGE_HASH.test(branchHash) ? branchHash : null,
      headSha: headSha && HEAD_SHA.test(headSha) ? headSha : null,
      resolvedAt: new Date().toISOString(),
      resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
    };
  });
}
