import { createHmac, randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  branchLinkageHash,
  normalizeGitRemote,
  remoteLinkageHash,
  type GitLinkageContext,
} from "../../shared/src/index";
import { readBoundedRegularFile, type BoundedRegularFileRead } from "./safe-file-read";

/**
 * Resolve privacy-safe git linkage keys for a working directory using plain
 * bounded descriptor reads (no subprocess): hashed remote, hashed branch,
 * plain HEAD sha.
 * Worktree-aware (`.git` file with a `gitdir:` pointer). Best-effort — any
 * unreadable state returns undefined rather than throwing, because this runs
 * inline on hook ingestion.
 */

const cache = new Map<string, { at: number; context: GitLinkageContext | undefined }>();
// Process-local, opaque cache identities only. Neither raw paths nor a stable
// working-directory digest are retained, persisted or sent to a reader.
const cacheKeySalt = randomBytes(32);
const CACHE_TTL_MS = 30_000;
const POINTER_LIMIT_BYTES = 4 * 1024;
const HEAD_LIMIT_BYTES = 4 * 1024;
const REF_LIMIT_BYTES = 256;
const CONFIG_LIMIT_BYTES = 256 * 1024;
const PACKED_REFS_LIMIT_BYTES = 1024 * 1024;

type LocatedGitDir = { gitDir: string; commonDir: string; isWorktree: boolean };
type HomeBoundary = { directory: string; device: bigint; inode: bigint };
type GitLookup<T> = { kind: "ok"; value: T } | { kind: "missing" } | { kind: "unsafe" };

function readText(filePath: string, limitBytes: number): BoundedRegularFileRead {
  return readBoundedRegularFile(filePath, limitBytes);
}

function singleLine(value: string) {
  const match = value.match(/^([^\r\n]*)(?:\r?\n)?$/);
  return match?.[1];
}

function containedGitPath(root: string, relative: string) {
  if (!relative || path.isAbsolute(relative) || relative.includes("\0")) return undefined;
  const resolvedRoot = path.resolve(root);
  const resolved = path.resolve(resolvedRoot, relative);
  return resolved.startsWith(`${resolvedRoot}${path.sep}`) ? resolved : undefined;
}

function resolvedUserHome(): HomeBoundary | undefined {
  try {
    const home = os.homedir();
    if (!home || !path.isAbsolute(home)) return undefined;
    const resolved = fs.realpathSync(home);
    const stat = fs.statSync(resolved, { bigint: true });
    return stat.isDirectory()
      ? { directory: resolved, device: stat.dev, inode: stat.ino }
      : undefined;
  } catch {
    // Without a verified home boundary, no filesystem-derived linkage is safe.
    return undefined;
  }
}

function findGitDir(startDir: string, home: HomeBoundary): GitLookup<LocatedGitDir> {
  let dir = fs.realpathSync(path.resolve(startDir));
  for (let depth = 0; depth < 24; depth += 1) {
    // Test the boundary BEFORE inspecting .git, including when cwd is the
    // boundary itself. Never discover a repository above the user's home.
    if (dir === home.directory || dir === path.parse(dir).root) return { kind: "missing" };
    // realpath can preserve spelling on case-insensitive volumes. Directory
    // identity also catches case/Unicode aliases of the same home folder.
    const directoryStat = fs.statSync(dir, { bigint: true });
    if (!directoryStat.isDirectory()) return { kind: "unsafe" };
    if (directoryStat.dev === home.device && directoryStat.ino === home.inode) {
      return { kind: "missing" };
    }
    const dotGit = path.join(dir, ".git");
    let stat: fs.BigIntStats | undefined;
    try {
      stat = fs.lstatSync(dotGit, { bigint: true });
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "ENOENT" && code !== "ENOTDIR") return { kind: "unsafe" };
      stat = undefined;
    }

    if (stat?.isDirectory()) {
      return { kind: "ok", value: { gitDir: dotGit, commonDir: dotGit, isWorktree: false } };
    }

    if (stat?.isFile()) {
      const pointerRead = readText(dotGit, POINTER_LIMIT_BYTES);
      if (pointerRead.kind !== "ok") return { kind: "unsafe" };
      const pointer = singleLine(pointerRead.value)?.match(/^gitdir:[ \t]*(.+?)[ \t]*$/)?.[1];
      if (!pointer) return { kind: "unsafe" };
      const gitDir = path.resolve(dir, pointer);
      const commonRead = readText(path.join(gitDir, "commondir"), POINTER_LIMIT_BYTES);
      if (commonRead.kind === "unsafe") return { kind: "unsafe" };
      const commonPointer = commonRead.kind === "ok" ? singleLine(commonRead.value)?.trim() : undefined;
      if (commonRead.kind === "ok" && !commonPointer) return { kind: "unsafe" };
      const commonDir = commonPointer ? path.resolve(gitDir, commonPointer) : gitDir;
      return { kind: "ok", value: { gitDir, commonDir, isWorktree: true } };
    }

    if (stat) return { kind: "unsafe" };

    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }

  return { kind: "missing" };
}

function resolveHead(
  gitDir: string,
  commonDir: string,
): GitLookup<{ ref?: string; headSha?: string }> {
  const headRead = readText(path.join(gitDir, "HEAD"), HEAD_LIMIT_BYTES);
  if (headRead.kind !== "ok") return headRead;
  const head = singleLine(headRead.value)?.trim();
  if (!head) return { kind: "unsafe" };

  const refMatch = head.match(/^ref:\s*(.+)$/);
  if (!refMatch) {
    return /^[0-9a-f]{40}$/i.test(head)
      ? { kind: "ok", value: { headSha: head } }
      : { kind: "unsafe" };
  }

  const ref = refMatch[1].trim();
  const gitRefPath = containedGitPath(gitDir, ref);
  const commonRefPath = containedGitPath(commonDir, ref);
  if (!gitRefPath || !commonRefPath) return { kind: "unsafe" };
  const localRef = readText(gitRefPath, REF_LIMIT_BYTES);
  if (localRef.kind === "unsafe") return { kind: "unsafe" };
  const commonRef = localRef.kind === "missing"
    ? readText(commonRefPath, REF_LIMIT_BYTES)
    : localRef;
  if (commonRef.kind === "unsafe") return { kind: "unsafe" };
  if (commonRef.kind === "ok") {
    const direct = singleLine(commonRef.value)?.trim();
    if (!direct || !/^[0-9a-f]{40}$/i.test(direct)) return { kind: "unsafe" };
    return { kind: "ok", value: { ref, headSha: direct } };
  }

  const packed = readText(path.join(commonDir, "packed-refs"), PACKED_REFS_LIMIT_BYTES);
  if (packed.kind === "unsafe") return { kind: "unsafe" };
  if (packed.kind === "ok") {
    for (const line of packed.value.split("\n")) {
      const match = line.match(/^([0-9a-f]{40})\s+(.+)$/i);
      if (match && match[2].trim() === ref) {
        return { kind: "ok", value: { ref, headSha: match[1] } };
      }
    }
  }

  return { kind: "ok", value: { ref } };
}

function resolveRemoteUrl(commonDir: string): GitLookup<string | undefined> {
  const config = readText(path.join(commonDir, "config"), CONFIG_LIMIT_BYTES);
  if (config.kind === "unsafe") return { kind: "unsafe" };
  if (config.kind === "missing") return { kind: "ok", value: undefined };

  let inOrigin = false;
  let firstRemoteUrl: string | undefined;
  for (const rawLine of config.value.split("\n")) {
    const line = rawLine.trim();
    const sectionMatch = line.match(/^\[remote\s+"(.+)"\]$/);
    if (sectionMatch) {
      inOrigin = sectionMatch[1] === "origin";
      continue;
    }
    if (line.startsWith("[")) {
      inOrigin = false;
      continue;
    }
    const urlMatch = line.match(/^url\s*=\s*(.+)$/);
    if (urlMatch) {
      if (inOrigin) return { kind: "ok", value: urlMatch[1].trim() };
      firstRemoteUrl ??= urlMatch[1].trim();
    }
  }

  return { kind: "ok", value: firstRemoteUrl };
}

function resolveGitContextCore(cwd: string, home: HomeBoundary): GitLinkageContext | undefined {
  let context: GitLinkageContext | undefined;
  try {
    const located = findGitDir(cwd, home);
    if (located.kind === "ok") {
      const { gitDir, commonDir, isWorktree } = located.value;
      const head = resolveHead(gitDir, commonDir);
      const remote = resolveRemoteUrl(commonDir);
      if (head.kind !== "ok" || remote.kind !== "ok") {
        return undefined;
      }
      const { ref, headSha } = head.value;
      const remoteUrl = remote.value;
      context = {
        remoteUrlHash: remoteLinkageHash(remoteUrl),
        remoteLabel: normalizeGitRemote(remoteUrl),
        branchHash: branchLinkageHash(ref),
        headSha,
        ...(isWorktree ? { isWorktree: true } : {}),
      };
      if (!context.remoteUrlHash && !context.branchHash && !context.headSha) {
        context = undefined;
      }
    }
  } catch {
    context = undefined;
  }

  return context;
}

/**
 * Deferred attribution owns a fixed transient queue and must never retain raw
 * cwd strings in this module's compatibility cache. This entrypoint performs
 * neither cache reads nor cache writes.
 */
export function resolveGitContextUncached(cwd: string | undefined): GitLinkageContext | undefined {
  if (!cwd || typeof cwd !== "string") return undefined;
  const home = resolvedUserHome();
  return home ? resolveGitContextCore(cwd, home) : undefined;
}

export function resolveGitContext(cwd: string | undefined): GitLinkageContext | undefined {
  if (!cwd || typeof cwd !== "string") return undefined;

  const home = resolvedUserHome();
  if (!home) return undefined;
  const key = createHmac("sha256", cacheKeySalt)
    .update(JSON.stringify([home.directory, home.device.toString(), home.inode.toString(), cwd]))
    .digest("hex");
  const cached = cache.get(key);
  if (cached && Date.now() - cached.at < CACHE_TTL_MS) {
    return cached.context;
  }

  const context = resolveGitContextCore(cwd, home);

  cache.set(key, { at: Date.now(), context });
  return context;
}

/** Numeric-only proof seam. Raw cache keys and values remain inaccessible. */
export function gitContextCacheSizeForProof() {
  return cache.size;
}

/** Numeric-only privacy seam; it never exposes a cache key or raw path. */
export function gitContextCacheNonDigestKeyCountForProof() {
  let count = 0;
  for (const key of cache.keys()) if (!/^[0-9a-f]{64}$/.test(key)) count += 1;
  return count;
}
