import fs from "node:fs";
import path from "node:path";

import { CaptureWorkBudget } from "./capture-work-budget";

type Frame = {
  directory: string;
  handle: fs.Dir;
  dev: number;
  ino: number;
  mtimeMs: number;
  ctimeMs: number;
};

export type DiscoveryChunk = {
  files: Array<{ file: string; stat: fs.Stats; precise: fs.BigIntStats }>;
  entriesVisited: number;
  errors: number;
  done: boolean;
  limitReached: boolean;
  yields: number;
  lastYieldAt: string | null;
};

/**
 * Stateful, bounded directory enumeration for automatic capture. One step
 * opens one directory or consumes one Dirent; it never materializes or sorts
 * a whole directory. The object survives cadence boundaries in its tailer.
 */
export class IncrementalJsonlDiscovery {
  private readonly roots: string[];
  private readonly rootSet: Set<string>;
  private readonly stack: Frame[] = [];
  private rootIndex = 0;
  private visited = 0;
  private errors = 0;
  private finished = false;
  private limitReached = false;

  constructor(
    roots: string[],
    private readonly options: {
      recursive: boolean;
      matches: (entryName: string) => boolean;
      maxEntries: number;
      missingRootsAreEmpty?: boolean;
    },
  ) {
    this.roots = roots.map((root) => path.resolve(root));
    this.rootSet = new Set(this.roots);
  }

  async collect(
    budget: CaptureWorkBudget,
    options: {
      maxFiles?: number;
      maxEntries?: number;
      maxWallMs?: number;
      signal?: AbortSignal;
    } = {},
  ): Promise<DiscoveryChunk> {
    const files: Array<{ file: string; stat: fs.Stats; precise: fs.BigIntStats }> = [];
    const startingVisited = this.visited;
    const startingErrors = this.errors;
    const maxFiles = Math.max(1, Math.min(options.maxFiles ?? 256, 1_024));
    const maxEntries = options.maxEntries === undefined
      ? Number.MAX_SAFE_INTEGER
      : Math.max(1, Math.min(options.maxEntries, 16_384));
    const maxWallMs = options.maxWallMs === undefined
      ? Number.POSITIVE_INFINITY
      : Math.max(1, Math.min(options.maxWallMs, 100));
    const collectStartedAt = performance.now();
    let stepsSinceYield = 0;
    let yields = 0;
    let lastYieldAt: string | null = null;
    while (
      !this.finished &&
      files.length < maxFiles &&
      this.visited - startingVisited < maxEntries &&
      performance.now() - collectStartedAt < maxWallMs &&
      !options.signal?.aborted &&
      budget.canContinue()
    ) {
      const file = this.step();
      if (file) files.push(file);
      stepsSinceYield += 1;
      if (stepsSinceYield >= 64 && !this.finished) {
        await new Promise<void>((resolve) => setImmediate(resolve));
        stepsSinceYield = 0;
        yields += 1;
        lastYieldAt = new Date().toISOString();
        budget.recordYield();
      }
    }
    return {
      files,
      entriesVisited: this.visited - startingVisited,
      errors: this.errors - startingErrors,
      done: this.finished,
      limitReached: this.limitReached,
      yields,
      lastYieldAt,
    };
  }

  close() {
    for (const frame of this.stack.splice(0)) {
      try {
        frame.handle.closeSync();
      } catch {
        // Discovery is already fail-closed by the observation receipt; close
        // failures must not keep descriptors alive or throw during shutdown.
      }
    }
    this.finished = true;
  }

  private step(): { file: string; stat: fs.Stats; precise: fs.BigIntStats } | null {
    if (this.finished) return null;
    if (this.visited >= this.options.maxEntries) {
      this.limitReached = true;
      this.errors += 1;
      this.close();
      return null;
    }
    if (this.stack.length === 0) {
      if (this.rootIndex >= this.roots.length) {
        this.finished = true;
        return null;
      }
      const root = this.roots[this.rootIndex++]!;
      this.openDirectory(root, true);
      return null;
    }

    const frame = this.stack[this.stack.length - 1]!;
    let entry: fs.Dirent | null;
    try {
      entry = frame.handle.readSync();
    } catch {
      this.errors += 1;
      this.popFrame();
      return null;
    }
    if (!entry) {
      this.popFrame();
      return null;
    }
    this.visited += 1;
    const candidate = path.resolve(frame.directory, entry.name);
    if (!isContainedByAnyRoot(candidate, this.roots)) {
      this.errors += 1;
      return null;
    }
    if (entry.isDirectory()) {
      if (this.options.recursive) this.openDirectory(candidate, false);
      return null;
    }
    if (!this.options.matches(entry.name)) return null;
    if (entry.isSymbolicLink()) {
      // A candidate alias can escape roots or cause one physical generation
      // to be observed under an attacker-controlled name. It is never
      // followed. Irrelevant aliases were rejected by the name predicate
      // above and do not make an otherwise complete discovery ambiguous.
      this.errors += 1;
      return null;
    }
    if (!entry.isFile()) {
      this.errors += 1;
      return null;
    }
    try {
      // Bind the discovered name to a physical generation in this same
      // bounded step. Later rename/replace races cannot cause a post-cutoff
      // generation to inherit the old pathname's exclusion.
      const metadata = fs.lstatSync(candidate);
      if (
        metadata.isSymbolicLink() ||
        !metadata.isFile() ||
        !this.hasStableContainedAncestors(candidate)
      ) {
        this.errors += 1;
        return null;
      }
      const precise = fs.lstatSync(candidate, { bigint: true });
      if (precise.isSymbolicLink() || !precise.isFile()) {
        this.errors += 1;
        return null;
      }
      return { file: candidate, stat: metadata, precise };
    } catch {
      this.errors += 1;
      return null;
    }
  }

  private openDirectory(directory: string, isRoot: boolean) {
    try {
      const metadata = fs.lstatSync(directory);
      if (metadata.isSymbolicLink() || !metadata.isDirectory()) {
        this.errors += 1;
        return;
      }
      if (!this.hasStableContainedAncestors(directory)) {
        this.errors += 1;
        return;
      }
      const handle = fs.opendirSync(directory);
      const afterOpen = fs.lstatSync(directory);
      if (
        afterOpen.isSymbolicLink() ||
        !afterOpen.isDirectory() ||
        afterOpen.dev !== metadata.dev ||
        afterOpen.ino !== metadata.ino
      ) {
        handle.closeSync();
        this.errors += 1;
        return;
      }
      this.stack.push({
        directory,
        handle,
        dev: afterOpen.dev,
        ino: afterOpen.ino,
        mtimeMs: afterOpen.mtimeMs,
        ctimeMs: afterOpen.ctimeMs,
      });
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (!(isRoot && this.options.missingRootsAreEmpty && code === "ENOENT")) {
        this.errors += 1;
      }
    }
  }

  private popFrame() {
    const frame = this.stack.pop();
    if (!frame) return;
    try {
      const current = fs.lstatSync(frame.directory);
      if (
        current.isSymbolicLink() ||
        !current.isDirectory() ||
        current.dev !== frame.dev ||
        current.ino !== frame.ino ||
        current.mtimeMs !== frame.mtimeMs ||
        current.ctimeMs !== frame.ctimeMs ||
        !this.hasStableContainedAncestors(frame.directory)
      ) this.errors += 1;
      frame.handle.closeSync();
    } catch {
      this.errors += 1;
    }
  }

  private hasStableContainedAncestors(candidate: string) {
    const root = this.roots.find(
      (possible) => candidate === possible || candidate.startsWith(`${possible}${path.sep}`),
    );
    if (!root) return false;
    try {
      let current = root;
      const relative = path.relative(root, candidate);
      const parts = relative ? relative.split(path.sep) : [];
      const directoryParts = parts.slice(0, Math.max(0, parts.length - 1));
      for (const part of directoryParts) {
        current = path.join(current, part);
        const metadata = fs.lstatSync(current);
        if (metadata.isSymbolicLink() || !metadata.isDirectory()) return false;
      }
      const realRoot = fs.realpathSync(root);
      const realCandidate = fs.realpathSync(candidate);
      return realCandidate === realRoot || realCandidate.startsWith(`${realRoot}${path.sep}`);
    } catch {
      return false;
    }
  }
}

function isContainedByAnyRoot(candidate: string, roots: string[]) {
  return roots.some((root) => candidate === root || candidate.startsWith(`${root}${path.sep}`));
}
