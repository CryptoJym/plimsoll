import fs from "node:fs";
import path from "node:path";

import {
  branchLinkageHash,
  remoteLinkageHash,
  type GitLinkageContext,
} from "../../shared/src/index";

/**
 * Resolve privacy-safe git linkage keys for a working directory using plain
 * file reads (no subprocess): hashed remote, hashed branch, plain HEAD sha.
 * Worktree-aware (`.git` file with a `gitdir:` pointer). Best-effort — any
 * unreadable state returns undefined rather than throwing, because this runs
 * inline on hook ingestion.
 */

const cache = new Map<string, { at: number; context: GitLinkageContext | undefined }>();
const CACHE_TTL_MS = 30_000;

function readText(filePath: string) {
  try {
    return fs.readFileSync(filePath, "utf8");
  } catch {
    return undefined;
  }
}

function findGitDir(startDir: string) {
  let dir = path.resolve(startDir);
  for (let depth = 0; depth < 24; depth += 1) {
    const dotGit = path.join(dir, ".git");
    let stat: fs.Stats | undefined;
    try {
      stat = fs.statSync(dotGit);
    } catch {
      stat = undefined;
    }

    if (stat?.isDirectory()) {
      return { gitDir: dotGit, commonDir: dotGit, isWorktree: false };
    }

    if (stat?.isFile()) {
      const pointer = readText(dotGit)?.match(/^gitdir:\s*(.+)\s*$/m)?.[1];
      if (pointer) {
        const gitDir = path.resolve(dir, pointer);
        const commonPointer = readText(path.join(gitDir, "commondir"))?.trim();
        const commonDir = commonPointer ? path.resolve(gitDir, commonPointer) : gitDir;
        return { gitDir, commonDir, isWorktree: true };
      }
    }

    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }

  return undefined;
}

function resolveHead(gitDir: string, commonDir: string) {
  const head = readText(path.join(gitDir, "HEAD"))?.trim();
  if (!head) return {};

  const refMatch = head.match(/^ref:\s*(.+)$/);
  if (!refMatch) {
    return /^[0-9a-f]{40}$/i.test(head) ? { headSha: head } : {};
  }

  const ref = refMatch[1].trim();
  const direct =
    readText(path.join(gitDir, ref))?.trim() ?? readText(path.join(commonDir, ref))?.trim();
  if (direct && /^[0-9a-f]{40}$/i.test(direct)) {
    return { ref, headSha: direct };
  }

  const packed = readText(path.join(commonDir, "packed-refs"));
  if (packed) {
    for (const line of packed.split("\n")) {
      const match = line.match(/^([0-9a-f]{40})\s+(.+)$/i);
      if (match && match[2].trim() === ref) {
        return { ref, headSha: match[1] };
      }
    }
  }

  return { ref };
}

function resolveRemoteUrl(commonDir: string) {
  const config = readText(path.join(commonDir, "config"));
  if (!config) return undefined;

  let inOrigin = false;
  let firstRemoteUrl: string | undefined;
  for (const rawLine of config.split("\n")) {
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
      if (inOrigin) return urlMatch[1].trim();
      firstRemoteUrl ??= urlMatch[1].trim();
    }
  }

  return firstRemoteUrl;
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
    if (located) {
      const { ref, headSha } = resolveHead(located.gitDir, located.commonDir);
      context = {
        remoteUrlHash: remoteLinkageHash(resolveRemoteUrl(located.commonDir)),
        branchHash: branchLinkageHash(ref),
        headSha,
        ...(located.isWorktree ? { isWorktree: true } : {}),
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
