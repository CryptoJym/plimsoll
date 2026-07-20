import fs from "node:fs";
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
const CACHE_TTL_MS = 30_000;
const POINTER_LIMIT_BYTES = 4 * 1024;
const HEAD_LIMIT_BYTES = 4 * 1024;
const REF_LIMIT_BYTES = 256;
const CONFIG_LIMIT_BYTES = 256 * 1024;
const PACKED_REFS_LIMIT_BYTES = 1024 * 1024;

type LocatedGitDir = { gitDir: string; commonDir: string; isWorktree: boolean };
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

function findGitDir(startDir: string): GitLookup<LocatedGitDir> {
  let dir = path.resolve(startDir);
  for (let depth = 0; depth < 24; depth += 1) {
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

export function resolveGitContext(cwd: string | undefined): GitLinkageContext | undefined {
  if (!cwd || typeof cwd !== "string") return undefined;

  const cached = cache.get(cwd);
  if (cached && Date.now() - cached.at < CACHE_TTL_MS) {
    return cached.context;
  }

  let context: GitLinkageContext | undefined;
  try {
    const located = findGitDir(cwd);
    if (located.kind === "ok") {
      const { gitDir, commonDir, isWorktree } = located.value;
      const head = resolveHead(gitDir, commonDir);
      const remote = resolveRemoteUrl(commonDir);
      if (head.kind !== "ok" || remote.kind !== "ok") {
        cache.set(cwd, { at: Date.now(), context: undefined });
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

  cache.set(cwd, { at: Date.now(), context });
  return context;
}
