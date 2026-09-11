import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import type { LocalEventBuffer } from "./buffer";
import {
  captureRootDigest,
  validateCaptureRoots,
  type CaptureRoot,
} from "./capture-root-inventory";
import type { RepoContextRequest } from "./repo-context";

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[8989][0-9a-f]{3}-[0-9a-f]{12}/i;
const MAX_REPLAY_READ_BYTES = 512 * 1024;
const MAX_DISCOVERED_SOURCES = 4_096;
const HEAD_PROBE_BYTES = 512;

const sha256 = (value: string | Buffer) =>
  crypto.createHash("sha256").update(value).digest("hex");

export type RepoContextReplaySource = {
  source: "codex" | "claude_code";
  sourceKey: string;
  sourceDigest: string;
  file: string;
  fileIdentity: string;
  size: number;
};

export type RepoContextReplayCandidate = {
  sourceKey: string;
  sourceDigest: string;
  position: string;
  nextPosition: string;
  request: RepoContextRequest;
};

export type RepoContextReplayDiscovery = {
  sources: RepoContextReplaySource[];
  complete: boolean;
  unavailableRoots: number;
  elapsedMs: number;
};

function safeSize(value: bigint) {
  const size = Number(value);
  if (!Number.isSafeInteger(size) || size < 0) throw new Error("repo_context_replay_source_too_large");
  return size;
}

export function repoContextReplayFileIdentity(file: string) {
  const stat = fs.lstatSync(file, { bigint: true });
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("repo_context_replay_source_unsafe");
  return `${String(stat.dev)}:${String(stat.ino)}:${String(stat.birthtimeNs)}`;
}

function sourceFromFile(root: CaptureRoot, file: string): RepoContextReplaySource {
  const stat = fs.lstatSync(file, { bigint: true });
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("repo_context_replay_source_unsafe");
  const fileIdentity = `${String(stat.dev)}:${String(stat.ino)}:${String(stat.birthtimeNs)}`;
  const size = safeSize(stat.size);
  const descriptor = fs.openSync(file, "r");
  let head: Buffer;
  try {
    head = Buffer.alloc(Math.min(size, HEAD_PROBE_BYTES));
    const read = fs.readSync(descriptor, head, 0, head.length, 0);
    head = head.subarray(0, read);
  } finally {
    fs.closeSync(descriptor);
  }
  const relative = path.relative(root.directory, file).split(path.sep).join("/");
  return {
    source: root.source,
    sourceKey: sha256(JSON.stringify([captureRootDigest(root), relative])),
    sourceDigest: sha256(JSON.stringify([root.source, fileIdentity, sha256(head)])),
    file,
    fileIdentity,
    size,
  };
}

function eligibleFile(source: CaptureRoot["source"], name: string) {
  return source === "codex"
    ? name.startsWith("rollout-") && name.endsWith(".jsonl")
    : name.endsWith(".jsonl");
}

export function discoverRepoContextReplaySources(
  inputRoots: readonly CaptureRoot[],
  options: { deadlineMs: number; now?: () => number },
): RepoContextReplayDiscovery {
  const roots = validateCaptureRoots([...inputRoots]);
  const now = options.now ?? (() => performance.now());
  const started = now();
  const allowed = Math.max(0, Math.min(Math.trunc(options.deadlineMs), 50));
  const sources: RepoContextReplaySource[] = [];
  let complete = allowed > 0;
  let unavailableRoots = 0;
  for (const root of roots) {
    if (now() - started >= allowed) { complete = false; break; }
    let rootStat: fs.Stats;
    try {
      rootStat = fs.lstatSync(root.directory);
      if (!rootStat.isDirectory() || rootStat.isSymbolicLink() || fs.realpathSync(root.directory) !== root.directory) {
        unavailableRoots += 1;
        continue;
      }
    } catch {
      unavailableRoots += 1;
      continue;
    }
    const pending = [root.directory];
    while (pending.length > 0) {
      if (now() - started >= allowed || sources.length >= MAX_DISCOVERED_SOURCES) {
        complete = false;
        break;
      }
      const directory = pending.shift()!;
      let entries: fs.Dirent[];
      try {
        entries = fs.readdirSync(directory, { withFileTypes: true })
          .sort((left, right) => left.name.localeCompare(right.name));
      } catch {
        unavailableRoots += 1;
        continue;
      }
      for (const entry of entries) {
        if (now() - started >= allowed || sources.length >= MAX_DISCOVERED_SOURCES) {
          complete = false;
          break;
        }
        const candidate = path.join(directory, entry.name);
        if (entry.isDirectory() && !entry.isSymbolicLink()) {
          pending.push(candidate);
        } else if (entry.isFile() && eligibleFile(root.source, entry.name)) {
          try { sources.push(sourceFromFile(root, candidate)); }
          catch { unavailableRoots += 1; }
        }
      }
      if (!complete) break;
    }
    if (!complete) break;
  }
  sources.sort((left, right) => left.sourceKey.localeCompare(right.sourceKey));
  return {
    sources,
    complete,
    unavailableRoots,
    elapsedMs: Math.max(0, now() - started),
  };
}

type Position = { offset: number; contextIndex: number };

export function encodeRepoContextReplayPosition(position: Position) {
  return `${position.offset}:${position.contextIndex}`;
}

export function decodeRepoContextReplayPosition(value: string | null): Position {
  if (!value) return { offset: 0, contextIndex: -1 };
  const match = /^(\d+):(-?\d+)$/.exec(value);
  if (!match) return { offset: 0, contextIndex: -1 };
  const offset = Number(match[1]);
  const contextIndex = Number(match[2]);
  return Number.isSafeInteger(offset) && offset >= 0 && Number.isSafeInteger(contextIndex) && contextIndex >= -1
    ? { offset, contextIndex }
    : { offset: 0, contextIndex: -1 };
}

export function readRepoContextReplaySlice(
  buffer: LocalEventBuffer,
  source: RepoContextReplaySource,
  cursor: string | null,
  options: { maxContexts: number; maxBytes?: number },
) {
  const start = decodeRepoContextReplayPosition(cursor);
  if (start.offset >= source.size) {
    return { candidates: [] as RepoContextReplayCandidate[], rowsInspected: 0,
      nextPosition: encodeRepoContextReplayPosition(start), complete: true, cutShort: false };
  }
  const maxContexts = Math.max(1, Math.min(Math.trunc(options.maxContexts), 64));
  const maxBytes = Math.max(1, Math.min(Math.trunc(options.maxBytes ?? 64 * 1024), MAX_REPLAY_READ_BYTES));
  const length = Math.min(maxBytes, source.size - start.offset);
  const bytes = Buffer.alloc(length);
  const descriptor = fs.openSync(source.file, "r");
  let read = 0;
  try { read = fs.readSync(descriptor, bytes, 0, length, start.offset); }
  finally { fs.closeSync(descriptor); }
  const view = bytes.subarray(0, read);
  const atEof = start.offset + read >= source.size;
  const candidates: RepoContextReplayCandidate[] = [];
  let relativeOffset = 0;
  let rowsInspected = 0;
  let contextIndex = start.contextIndex;
  const sessionFromName = path.basename(source.file).match(UUID_RE)?.[0]?.toLowerCase();
  let conversationId = sessionFromName;

  while (relativeOffset < view.length && candidates.length < maxContexts) {
    const newline = view.indexOf(0x0a, relativeOffset);
    if (newline === -1 && !atEof) break;
    const end = newline === -1 ? view.length : newline;
    const nextOffset = newline === -1 ? end : end + 1;
    const lineStart = start.offset + relativeOffset;
    const line = view.subarray(relativeOffset, end).toString("utf8").replace(/\r$/, "");
    relativeOffset = nextOffset;
    rowsInspected += 1;
    let parsed: Record<string, unknown>;
    try { parsed = JSON.parse(line) as Record<string, unknown>; }
    catch { continue; }

    let request: RepoContextRequest | null = null;
    if (source.source === "codex") {
      const type = parsed.type;
      if (type !== "session_meta" && type !== "turn_context") continue;
      const payload = (parsed.payload ?? {}) as Record<string, unknown>;
      if (type === "session_meta" && typeof payload.id === "string" && UUID_RE.test(payload.id)) {
        conversationId = payload.id.match(UUID_RE)?.[0]?.toLowerCase() ?? conversationId;
      }
      contextIndex += 1;
      const occurrence = [
        "codex-rollout", source.fileIdentity, conversationId ?? "unknown", type, String(contextIndex),
      ].join(":");
      request = typeof payload.cwd === "string"
        ? buffer.repoContextOccurrenceRequest("codex", occurrence, payload.cwd)
        : null;
    } else {
      if (parsed.type !== "assistant") continue;
      const message = (parsed.message ?? {}) as Record<string, unknown>;
      if (!message.usage || typeof message.id !== "string") continue;
      const sessionId = (typeof parsed.sessionId === "string" ? parsed.sessionId.match(UUID_RE)?.[0]?.toLowerCase() : undefined)
        ?? sessionFromName;
      if (!sessionId) continue;
      const occurrence = [
        "claude-transcript", sessionId, sha256(message.id),
      ].join(":");
      request = typeof parsed.cwd === "string"
        ? buffer.repoContextOccurrenceRequest("claude_code", occurrence, parsed.cwd)
        : null;
    }
    if (!request) continue;
    candidates.push({
      sourceKey: source.sourceKey,
      sourceDigest: source.sourceDigest,
      position: encodeRepoContextReplayPosition({ offset: lineStart, contextIndex }),
      nextPosition: encodeRepoContextReplayPosition({
        offset: start.offset + relativeOffset,
        contextIndex,
      }),
      request,
    });
  }
  const next = { offset: start.offset + relativeOffset, contextIndex };
  const complete = next.offset >= source.size;
  return {
    candidates,
    rowsInspected,
    nextPosition: encodeRepoContextReplayPosition(next),
    complete,
    cutShort: !complete,
  };
}
