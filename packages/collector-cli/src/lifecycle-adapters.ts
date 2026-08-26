import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import {
  collectorBufferPath,
  collectorConfigPath,
  collectorHome,
  readCollectorConfig,
} from "./config";
import {
  defaultBackfillStatePath,
} from "./upload-history";
import {
  inspectLaunchAgentManifest,
  installLaunchAgent,
  launchAgentPlistPath,
  readLaunchAgentProgramArguments,
  uninstallLaunchAgent,
} from "./launch-agent";
import { defaultLifecycleAuthorityRoot, LifecycleMutationAuthority } from "./lifecycle-authority";
import {
  FilesystemLifecycleAdapter,
  type LifecycleDatabaseAdapter,
  type LifecycleServiceAdapter,
  type ManagedLifecyclePaths,
} from "./lifecycle-filesystem";
import {
  LIFECYCLE_SCHEMA_VERSION,
  validateRuntimeArtifact,
  type LifecycleAdapter,
  type LifecycleReadiness,
  type LifecycleSupportSnapshot,
  type RuntimeArtifact,
} from "./lifecycle";
import { PLIMSOLL_VERSION } from "./version";

/**
 * Production composition of the lifecycle transaction boundary (#103/#158).
 *
 * The deterministic coordinator in lifecycle.ts owns transaction semantics;
 * this module injects the real boundaries it may never touch itself:
 * - FilesystemLifecycleAdapter over the canonical collector home,
 * - a LaunchAgent-manifest service adapter that rewrites the owned plist and
 *   NEVER invokes launchctl (load/unload stay explicit operator commands),
 * - a SQLite online-backup database adapter (a live WAL database is never
 *   copied byte-wise; only quiesced backup snapshots are restorable),
 * - the shared cross-process mutation authority fencing every mutating step.
 *
 * Artifact resolution pins the packaged bundle plus its vendored native
 * dependency closure (better-sqlite3, bindings, file-uri-to-path) as a
 * digest-verified immutable runtime under versions/<version>/darwin-<arch>/,
 * so the daemon manifest never references npx caches, dist folders, or
 * source trees. Source checkouts refuse self-pinning.
 */

const BOUNDED_IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/;
const SAFE_LOG_CODE = /^[a-z0-9][a-z0-9._-]{0,63}$/;

const MAX_BUNDLE_BYTES = 64 * 1024 * 1024;
const MAX_STATE_JSON_BYTES = 4 * 1024;
const MAX_LOG_TAIL_BYTES = 64 * 1024;
const MAX_LOG_LINES_SCANNED = 4096;
const MAX_BOUNDED_LOG_ENTRIES = 32;
const MAX_COMPANION_FILES = 8192;
const MAX_COMPANION_TOTAL_BYTES = 512 * 1024 * 1024;
const MAX_PACKAGE_WALK_DEPTH = 24;
/** Native packages the packaged bundle loads at runtime and therefore must
 * be staged beside the pinned executable inside the immutable runtime. */
const VENDORED_NATIVE_PACKAGES = [
  "better-sqlite3",
  "bindings",
  "file-uri-to-path",
] as const;

const READINESS_POLL_MS = 25;

export type ComposeLifecycleAdapterOptions = {
  /** Overrides every home-derived path (tests, custom installs). */
  homeDir?: string;
  lifecycleRoot?: string;
  /** Root all artifact sources (bundle + companions) must live under. */
  artifactSourceRoot?: string;
  authorityRoot?: string;
  authorityLeaseMs?: number;
  service?: LifecycleServiceAdapter;
  database?: LifecycleDatabaseAdapter;
};

function sha256File(file: string): `sha256:${string}` {
  const hash = createHash("sha256");
  const descriptor = fs.openSync(file, fs.constants.O_RDONLY);
  try {
    const chunk = Buffer.allocUnsafe(256 * 1024);
    while (true) {
      const bytesRead = fs.readSync(descriptor, chunk, 0, chunk.length, null);
      if (bytesRead === 0) break;
      hash.update(bytesRead === chunk.length ? chunk : chunk.subarray(0, bytesRead));
    }
  } finally {
    fs.closeSync(descriptor);
  }
  return `sha256:${hash.digest("hex")}`;
}

function assertBoundedIdentifier(value: string, label: string) {
  if (typeof value !== "string" || !BOUNDED_IDENTIFIER.test(value)) {
    throw new Error(`${label} must be a bounded identifier`);
  }
}

function supportedArchitecture(): "arm64" | "x64" {
  if (process.arch === "arm64") return "arm64";
  if (process.arch === "x64") return "x64";
  throw new Error(`unsupported architecture for packaged runtimes: ${process.arch}`);
}

function runningNodeMajor(): number {
  const major = Number(process.versions.node.split(".", 1)[0]);
  if (!Number.isInteger(major)) throw new Error("running Node major is unknown");
  return major;
}

/** Longest shared directory prefix of two absolute paths. */
function longestCommonAncestor(left: string, right: string): string {
  const leftParts = path.resolve(left).split(path.sep);
  const rightParts = path.resolve(right).split(path.sep);
  const shared: string[] = [];
  for (let index = 0; index < Math.min(leftParts.length, rightParts.length); index += 1) {
    if (leftParts[index] !== rightParts[index]) break;
    shared.push(leftParts[index]!);
  }
  const joined = shared.join(path.sep);
  return joined === "" ? path.sep : joined;
}

function realpathTolerant(value: string): string {
  try {
    return fs.realpathSync(value);
  } catch {
    return value;
  }
}

/** Nearest ancestor directory whose node_modules holds better-sqlite3. */
function findNativeClosureRoot(startDirectory: string): string | null {
  let cursor = path.resolve(startDirectory);
  while (true) {
    if (fs.existsSync(path.join(cursor, "node_modules", "better-sqlite3", "package.json"))) {
      return cursor;
    }
    const parent = path.dirname(cursor);
    if (parent === cursor) return null;
    cursor = parent;
  }
}

type CompanionCandidate = { relativePath: string; sourcePath: string };

function listPackageFiles(
  packageRoot: string,
  directory: string,
  packageName: string,
  collected: CompanionCandidate[],
  seenBytes: { value: number },
  depth = 0,
): void {
  if (depth > MAX_PACKAGE_WALK_DEPTH) {
    throw new Error(`vendored dependency ${packageName} exceeds the package walk depth bound`);
  }
  if (collected.length >= MAX_COMPANION_FILES) {
    throw new Error("vendored dependency closure exceeds the file count bound");
  }
  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(directory, { withFileTypes: true });
  } catch (error) {
    throw new Error(`vendored dependency ${packageName} is unreadable: ${(error as Error).message}`);
  }
  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
    if (entry.isSymbolicLink()) continue;
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      listPackageFiles(packageRoot, absolute, packageName, collected, seenBytes, depth + 1);
      continue;
    }
    if (!entry.isFile()) continue;
    if (collected.length >= MAX_COMPANION_FILES) {
      throw new Error("vendored dependency closure exceeds the file count bound");
    }
    const stat = fs.statSync(absolute);
    seenBytes.value += stat.size;
    if (seenBytes.value > MAX_COMPANION_TOTAL_BYTES) {
      throw new Error("vendored dependency closure exceeds the total size bound");
    }
    collected.push({
      relativePath: path.posix.join("node_modules", packageName, ...path.relative(packageRoot, absolute).split(path.sep)),
      sourcePath: absolute,
    });
  }
}

/**
 * Resolves an explicit built plimsoll bundle into a validated RuntimeArtifact
 * whose companions are the vendored native dependency closure next to it.
 */
export function resolveArtifactFromBundle(input: { bundlePath: string; version: string }): RuntimeArtifact {
  assertBoundedIdentifier(input.version, "artifact version");
  let bundle: string;
  try {
    bundle = fs.realpathSync(input.bundlePath);
  } catch {
    throw new Error(`artifact bundle is missing: ${input.bundlePath}`);
  }
  const stat = fs.lstatSync(bundle);
  if (!stat.isFile()) throw new Error("artifact bundle must be a regular file");
  if (stat.size > MAX_BUNDLE_BYTES) throw new Error("artifact bundle exceeds the size bound");

  const closureRoot = findNativeClosureRoot(path.dirname(bundle));
  if (!closureRoot) {
    throw new Error(
      "no vendored native dependency closure (node_modules/better-sqlite3) found near the artifact bundle",
    );
  }
  const candidates: CompanionCandidate[] = [];
  const seenBytes = { value: 0 };
  let betterSqlite3VirtualStore: string | null = null;
  for (const packageName of VENDORED_NATIVE_PACKAGES) {
    const searchPaths = [
      path.join(closureRoot, "node_modules", packageName),
      // pnpm-style layouts keep transitive dependencies beside their parent
      // inside the virtual store rather than the top-level node_modules.
      ...(betterSqlite3VirtualStore
        ? [path.join(betterSqlite3VirtualStore, packageName)]
        : []),
      path.join(closureRoot, "node_modules", ".pnpm", "node_modules", packageName),
    ];
    let real: string | null = null;
    for (const candidateDirectory of searchPaths) {
      try {
        real = fs.realpathSync(candidateDirectory);
        break;
      } catch {
        // Try the next layout location.
      }
    }
    if (!real) {
      throw new Error(`vendored native dependency ${packageName} is missing near the artifact bundle`);
    }
    if (packageName === "better-sqlite3") {
      // …/node_modules/<virtual>/node_modules/better-sqlite3 → <virtual>/node_modules
      const parent = path.dirname(real);
      if (path.basename(parent) === "node_modules") betterSqlite3VirtualStore = parent;
    }
    listPackageFiles(real, real, packageName, candidates, seenBytes);
  }
  const files = candidates.map((candidate) => ({
    relativePath: candidate.relativePath,
    sha256: sha256File(candidate.sourcePath),
    sourcePath: candidate.sourcePath,
  }));
  const artifact: RuntimeArtifact = {
    version: input.version,
    platform: "darwin",
    architecture: supportedArchitecture(),
    nodeMajor: runningNodeMajor(),
    sha256: sha256File(bundle),
    sourcePath: bundle,
    files,
  };
  validateRuntimeArtifact(artifact);
  return artifact;
}

/**
 * Pins the RUNNING packaged bundle (`--artifact self`). Refuses source
 * checkouts: self-pinning only makes sense for the stable packaged runtime.
 */
export function resolveSelfArtifact(options: { version?: string } = {}): RuntimeArtifact {
  const script = process.argv[1];
  if (!script) throw new Error(selfArtifactRefusal());
  let resolved: string;
  try {
    resolved = fs.realpathSync(path.resolve(script));
  } catch {
    throw new Error(selfArtifactRefusal());
  }
  if (!/\.(?:mjs|cjs|js)$/.test(resolved) || !fs.existsSync(resolved)) {
    throw new Error(selfArtifactRefusal());
  }
  return resolveArtifactFromBundle({
    bundlePath: resolved,
    version: options.version ?? PLIMSOLL_VERSION,
  });
}

function selfArtifactRefusal() {
  return "--artifact self requires running from a packaged plimsoll bundle; source checkouts pass an explicit --artifact path";
}

function readInstalledStateTolerant(lifecycleRoot: string): { version: string; executablePath: string } | null {
  const statePath = path.join(lifecycleRoot, "state.json");
  try {
    if (!fs.existsSync(statePath)) return null;
    const stat = fs.lstatSync(statePath);
    if (!stat.isFile() || stat.size > MAX_STATE_JSON_BYTES) return null;
    const parsed = JSON.parse(fs.readFileSync(statePath, "utf8")) as {
      schemaVersion?: unknown;
      version?: unknown;
      executablePath?: unknown;
    };
    if (
      parsed?.schemaVersion !== LIFECYCLE_SCHEMA_VERSION ||
      typeof parsed.version !== "string" || !BOUNDED_IDENTIFIER.test(parsed.version) ||
      typeof parsed.executablePath !== "string" || !path.isAbsolute(parsed.executablePath)
    ) {
      return null;
    }
    return { version: parsed.version, executablePath: parsed.executablePath };
  } catch {
    return null;
  }
}

function deriveVersionFromRuntimePointer(pointer: unknown): string | null {
  if (typeof pointer !== "string") return null;
  const match = pointer.match(/[\\/]versions[\\/]([A-Za-z0-9][A-Za-z0-9._-]{0,95})[\\/]/);
  return match?.[1] ?? null;
}

function openLedgerReadOnly(homeDir?: string): InstanceType<typeof Database> | null {
  const databasePath = collectorBufferPath(homeDir);
  try {
    if (!fs.existsSync(databasePath)) return null;
    const stat = fs.lstatSync(databasePath);
    if (!stat.isFile()) return null;
    return new Database(databasePath, { readonly: true, fileMustExist: true });
  } catch {
    return null;
  }
}

function ledgerIsCompatible(homeDir?: string): boolean {
  const databasePath = collectorBufferPath(homeDir);
  try {
    if (!fs.existsSync(databasePath)) return true;
    const db = new Database(databasePath, { readonly: true, fileMustExist: true });
    try {
      db.prepare("select count(*) as n from sqlite_master").get();
      return true;
    } finally {
      db.close();
    }
  } catch {
    return false;
  }
}

/** Aggregated, content-free log health: counts only, no line bodies. */
function scanCollectorLogs(homeDir?: string): LifecycleSupportSnapshot["boundedLogs"] {
  const aggregates = new Map<string, number>();
  for (const [source, fileName] of [
    ["collector_stdout", "collector.out.log"],
    ["collector_stderr", "collector.err.log"],
  ] as const) {
    const logPath = path.join(collectorHome(homeDir), fileName);
    try {
      if (!fs.existsSync(logPath)) continue;
      const stat = fs.lstatSync(logPath);
      if (!stat.isFile() || stat.size === 0) continue;
      const start = Math.max(0, stat.size - MAX_LOG_TAIL_BYTES);
      const descriptor = fs.openSync(logPath, fs.constants.O_RDONLY);
      let text: string;
      try {
        const buffer = Buffer.alloc(Math.min(stat.size, MAX_LOG_TAIL_BYTES));
        let offset = 0;
        while (offset < buffer.length) {
          const bytesRead = fs.readSync(descriptor, buffer, offset, buffer.length - offset, start + offset);
          if (bytesRead === 0) break;
          offset += bytesRead;
        }
        text = buffer.subarray(0, offset).toString("utf8");
      } finally {
        fs.closeSync(descriptor);
      }
      const lines = text.split("\n");
      const truncated = start > 0 ? 1 : lines.length > MAX_LOG_LINES_SCANNED ? lines.length - MAX_LOG_LINES_SCANNED : 0;
      for (const line of lines.slice(truncated)) {
        if (!line.trim()) continue;
        const severity = /\berror\b/i.test(line) ? "error"
          : /\bwarn(?:ing)?\b/i.test(line) ? "warn"
          : "info";
        const firstToken = line.trim().split(/\s+/)[0] ?? "";
        const code = SAFE_LOG_CODE.test(firstToken) ? firstToken : "unparsed_line";
        const key = `${severity}|${code}|${source}`;
        aggregates.set(key, Math.min((aggregates.get(key) ?? 0) + 1, 1_000_000));
      }
    } catch {
      // A support snapshot reports what it could read; nothing more.
    }
  }
  return [...aggregates.entries()]
    .sort((left, right) => left[0].localeCompare(right[0]))
    .slice(0, MAX_BOUNDED_LOG_ENTRIES)
    .map(([key, count]) => {
      const [severity, code, source] = key.split("|");
      return {
        source: source as "collector_stdout" | "collector_stderr",
        severity: severity as "info" | "warn" | "error",
        code: code!,
        count,
      };
    });
}

function supportCounters(homeDir?: string): LifecycleSupportSnapshot["counters"] {
  const counters = {
    activeDelivery: 0,
    deadDelivery: 0,
    tokenAttributedEvents: 0,
    maintenancePending: 0,
  };
  const safe = (value: unknown) => (Number.isSafeInteger(value) ? value as number : 0);
  const db = openLedgerReadOnly(homeDir);
  if (!db) return counters;
  try {
    try {
      const row = db.prepare("select active_pending as v, receipt_dead as d from outbox_counters limit 1").get() as
        | { v?: unknown; d?: unknown }
        | undefined;
      counters.activeDelivery = safe(row?.v);
      counters.deadDelivery = safe(row?.d);
    } catch {
      // Outbox schema absent on fresh ledgers.
    }
    try {
      const row = db.prepare(
        "select count(*) as v from buffered_events where input_tokens is not null or output_tokens is not null",
      ).get() as { v?: unknown };
      counters.tokenAttributedEvents = safe(row?.v);
    } catch {
      // Event table absent on fresh ledgers.
    }
    try {
      const pending =
        (db.prepare("select count(*) as v from reprice_dirty_events").get() as { v?: unknown }).v;
      const enrichment =
        (db.prepare("select count(*) as v from repo_enrichment_dirty").get() as { v?: unknown }).v;
      counters.maintenancePending = Math.min(safe(pending) + safe(enrichment), 1_000_000_000);
    } catch {
      // Maintenance queues absent on fresh ledgers.
    }
  } finally {
    db.close();
  }
  return counters;
}

/**
 * The service boundary backed by the owned LaunchAgent manifest. Every write
 * goes through the transactional manifest installer/remover; NOTHING here
 * invokes launchctl — loading/unloading stays an explicit operator command.
 * Mutations always run inside an operation whose mutation lease the
 * FilesystemLifecycleAdapter already holds and revalidates (assertFence), so
 * this adapter deliberately acquires no lease of its own: a nested acquire
 * against the same authority domain would deadlock by design.
 */
export class LaunchAgentManifestLifecycleService implements LifecycleServiceAdapter {
  constructor(
    private readonly options: { homeDir?: string; lifecycleRoot: string },
  ) {}

  private expectedExecutable(version: string): string | null {
    let architecture: string;
    try {
      architecture = supportedArchitecture();
    } catch {
      return null;
    }
    return path.join(
      this.options.lifecycleRoot,
      "versions",
      version,
      `darwin-${architecture}`,
      "bin",
      "plimsoll.mjs",
    );
  }

  async activate(input: { executablePath: string; version: string }): Promise<void> {
    const workingDirectory = path.dirname(input.executablePath);
    const result = installLaunchAgent({
      ...(this.options.homeDir !== undefined ? { homeDir: this.options.homeDir } : {}),
      repoRoot: workingDirectory,
      programArguments: [process.execPath, input.executablePath, "start"],
      workingDirectory,
    });
    if (result.receipt.status !== "installed" && result.receipt.status !== "unchanged") {
      throw new Error(`launch agent manifest activation failed: ${result.receipt.status}`);
    }
    const visible = inspectLaunchAgentManifest({ ...(this.options.homeDir !== undefined ? { homeDir: this.options.homeDir } : {}) });
    if (!visible.ok || visible.manifestDigest !== result.receipt.manifestDigest) {
      throw new Error("launch agent manifest postcondition failed after activation");
    }
  }

  async restore(input: { executablePath: string | null; version: string | null }): Promise<void> {
    if (input.executablePath && input.version) {
      await this.activate({ executablePath: input.executablePath, version: input.version });
      return;
    }
    await this.remove();
  }

  async remove(): Promise<void> {
    uninstallLaunchAgent({ ...(this.options.homeDir !== undefined ? { homeDir: this.options.homeDir } : {}) });
    const visible = inspectLaunchAgentManifest({ ...(this.options.homeDir !== undefined ? { homeDir: this.options.homeDir } : {}) });
    if (visible.ok) throw new Error("launch agent manifest removal postcondition failed");
  }

  private attemptReadiness(expectedVersion: string): LifecycleReadiness {
    let serviceReady = false;
    let runtimeVersion: string | null = null;
    let reason: LifecycleReadiness["reason"] = "service_unready";
    const expectedExecutable = this.expectedExecutable(expectedVersion);
    try {
      const decision = readLaunchAgentProgramArguments({
        ...(this.options.homeDir !== undefined ? { homeDir: this.options.homeDir } : {}),
      });
      const args = decision.programArguments;
      runtimeVersion = deriveVersionFromRuntimePointer(args[1]);
      if (
        args.length === 3 &&
        args[0] === process.execPath &&
        expectedExecutable !== null &&
        args[1] === expectedExecutable &&
        args[2] === "start"
      ) {
        serviceReady = true;
        runtimeVersion = expectedVersion;
        reason = "ready";
      } else {
        reason = "runtime_mismatch";
      }
    } catch {
      // Missing or unowned manifest: the service decision is unavailable.
    }
    const configCompatible = readCollectorConfig(this.options.homeDir).status === "valid";
    const databaseCompatible = ledgerIsCompatible(this.options.homeDir);
    if (!configCompatible) reason = "config_incompatible";
    if (!databaseCompatible) reason = "database_incompatible";
    const ready = serviceReady && configCompatible && databaseCompatible && reason === "ready";
    return { ready, runtimeVersion, serviceReady, configCompatible, databaseCompatible, reason };
  }

  readiness(
    expectedVersion: string,
    input: { signal: AbortSignal; deadlineMs: number },
  ): Promise<LifecycleReadiness> {
    // All checks are local and deterministic; the short retry window exists
    // so a just-switched manifest becomes visible to the verifier. The
    // window stays far inside the coordinator's own deadline so the
    // transactional timeout can never mask the real readiness reason.
    const retryBudgetMs = Math.min(Math.max(input.deadlineMs - 1, 0), 200);
    const startedAt = Date.now();
    return new Promise((resolve, reject) => {
      const attempt = () => {
        if (input.signal.aborted) {
          reject(new Error("readiness aborted"));
          return;
        }
        try {
          const result = this.attemptReadiness(expectedVersion);
          if (result.ready || input.signal.aborted || Date.now() - startedAt >= retryBudgetMs) {
            resolve(result);
            return;
          }
          setTimeout(attempt, READINESS_POLL_MS).unref?.();
        } catch (error) {
          reject(error instanceof Error ? error : new Error(String(error)));
        }
      };
      attempt();
    });
  }

  async supportSnapshot(): Promise<LifecycleSupportSnapshot> {
    const state = readInstalledStateTolerant(this.options.lifecycleRoot);
    const installedVersion = state?.version ?? null;
    let runtimeVersion: string | null = null;
    try {
      const decision = readLaunchAgentProgramArguments({
        ...(this.options.homeDir !== undefined ? { homeDir: this.options.homeDir } : {}),
      });
      runtimeVersion = deriveVersionFromRuntimePointer(decision.programArguments[1]) ?? state?.version ?? null;
    } catch {
      runtimeVersion = state?.version ?? null;
    }
    let architecture: string;
    let nodeMajor: number;
    try {
      architecture = supportedArchitecture();
      nodeMajor = runningNodeMajor();
    } catch {
      architecture = "unsupported";
      nodeMajor = 0;
    }
    return {
      installedVersion,
      runtimeVersion,
      platform: "darwin",
      architecture,
      nodeMajor,
      readiness: installedVersion
        ? this.attemptReadiness(installedVersion)
        : {
            ready: false,
            runtimeVersion,
            serviceReady: false,
            configCompatible: false,
            databaseCompatible: false,
            reason: "service_unready",
          },
      counters: supportCounters(this.options.homeDir),
      boundedLogs: scanCollectorLogs(this.options.homeDir),
    };
  }
}

/** Quiesced SQLite snapshots via the online backup API; never a raw WAL copy. */
export class SqliteOnlineBackupAdapter implements LifecycleDatabaseAdapter {
  snapshot(input: { source: string; destination: string }): Promise<boolean> {
    return (async () => {
      if (!fs.existsSync(input.source)) return false;
      fs.rmSync(input.destination, { force: true });
      fs.rmSync(`${input.destination}-wal`, { force: true });
      fs.rmSync(`${input.destination}-shm`, { force: true });
      const db = new Database(input.source, { readonly: true, fileMustExist: true });
      try {
        await db.backup(input.destination);
      } finally {
        db.close();
      }
      fs.chmodSync(input.destination, 0o600);
      return true;
    })();
  }

  restore(input: { source: string; destination: string }): Promise<void> {
    return (async () => {
      const stat = fs.lstatSync(input.source);
      if (!stat.isFile()) throw new Error("database restore source must be a regular file");
      fs.rmSync(`${input.destination}-wal`, { force: true });
      fs.rmSync(`${input.destination}-shm`, { force: true });
      fs.rmSync(input.destination, { force: true });
      fs.copyFileSync(input.source, input.destination);
      fs.chmodSync(input.destination, 0o600);
    })();
  }
}

/** Builds the production ManagedLifecyclePaths for one collector home. */
export function managedLifecyclePaths(options: {
  homeDir?: string;
  lifecycleRoot?: string;
  artifactSourceRoot?: string;
}): ManagedLifecyclePaths {
  const home = realpathTolerant(options.homeDir !== undefined ? path.resolve(options.homeDir) : os.homedir());
  const scriptClosureRoot = (() => {
    try {
      if (!process.argv[1]) return null;
      return findNativeClosureRoot(path.dirname(fs.realpathSync(path.resolve(process.argv[1]))));
    } catch {
      return null;
    }
  })();
  // One ownership boundary must span the collector home AND the packaged
  // install tree the artifacts are staged from, so a mixed layout (custom
  // PLIMSOLL_HOME beside an npm/pnpm install elsewhere) still validates as
  // strictly-owned while every segment stays symlink-free on macOS.
  const ownershipRoot = scriptClosureRoot
    ? longestCommonAncestor(home, scriptClosureRoot)
    : home;
  const lifecycleRoot = options.lifecycleRoot
    ?? path.join(collectorHome(options.homeDir), "lifecycle");
  return {
    ownershipRoot,
    lifecycleRoot,
    // Artifact sources (bundle + vendored companions) always stage from the
    // install tree that owns them; foreign trees fail closed in stage().
    artifactSourceRoot: options.artifactSourceRoot ?? scriptClosureRoot ?? home,
    collectorConfig: collectorConfigPath(options.homeDir),
    database: collectorBufferPath(options.homeDir),
    serviceManifest: launchAgentPlistPath(options.homeDir),
    // Issue #103 slice: the real adapter owns no embedded tool-config
    // fragments yet; surgical fragment removal is still open work.
    ownedToolFragments: [],
    history: [defaultBackfillStatePath(options.homeDir)],
  };
}

export function composeLifecycleAdapter(options: ComposeLifecycleAdapterOptions = {}): LifecycleAdapter {
  const paths = managedLifecyclePaths({
    ...(options.homeDir !== undefined ? { homeDir: options.homeDir } : {}),
    ...(options.lifecycleRoot !== undefined ? { lifecycleRoot: options.lifecycleRoot } : {}),
    ...(options.artifactSourceRoot !== undefined ? { artifactSourceRoot: options.artifactSourceRoot } : {}),
  });
  const service = options.service ?? new LaunchAgentManifestLifecycleService({
    ...(options.homeDir !== undefined ? { homeDir: options.homeDir } : {}),
    lifecycleRoot: paths.lifecycleRoot,
  });
  const database = options.database ?? new SqliteOnlineBackupAdapter();
  const authority = new LifecycleMutationAuthority(
    options.authorityRoot ?? defaultLifecycleAuthorityRoot(options.homeDir),
    ...(options.authorityLeaseMs !== undefined ? [{ defaultLeaseMs: options.authorityLeaseMs }] : []),
  );
  return new FilesystemLifecycleAdapter(paths, service, database, authority);
}
