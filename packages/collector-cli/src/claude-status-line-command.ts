/** Claude status-line command and shared chain/configuration mechanics. */
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import { applyClaudeSettings, assertManagedConfigTarget, ClaudeConfigError } from "../../collector-config/src/index";
import { providerAccountKey } from "../../shared/src/policy";
import { LocalEventBuffer } from "./buffer";
import { collectorBufferPath, ensureCollectorHome } from "./config";
import { claudePlanLimitWindows, PlanLimitEmitter } from "./plan-limit-observation";

export const CLAUDE_STATUS_LINE_MAX_STDIN_BYTES = 1024 * 1024;
export const STATUS_LINE_CHAIN_DEFAULT_TIMEOUT_MS = 10_000 as const;
export const STATUS_LINE_CHAIN_SHUTDOWN_GRACE_MS = 2_000 as const;
export const STATUS_LINE_CHAIN_MAX_STREAM_BYTES = 4 * 1024 * 1024;

type ChainedRun = {
  stdout: Buffer;
  stderr: Buffer;
  exitCode: number;
  signal: string | null;
  bytesWritten: number;
};

/**
 * Reproduce the operator status line: `/bin/sh -c <command>` with the raw
 * stdin forwarded in memory, bounded stdout/stderr capture, and the classic
 * timeout ladder (deadline → SIGTERM → grace → SIGKILL, exit 124 on timeout,
 * mirroring GNU timeout's convention).
 */
export async function runChainedStatusLineCommand(input: {
  command: string;
  stdinBytes: Buffer;
  timeoutMs: number;
  shutdownGraceMs: number;
  maxStreamBytes: number;
}): Promise<ChainedRun> {
  return new Promise<ChainedRun>((resolve) => {
    let child: ChildProcessWithoutNullStreams;
    try {
      child = spawn("/bin/sh", ["-c", input.command], {
        stdio: ["pipe", "pipe", "pipe"],
        shell: false,
      }) as ChildProcessWithoutNullStreams;
    } catch {
      resolve({
        stdout: Buffer.alloc(0),
        stderr: Buffer.alloc(0),
        exitCode: 126,
        signal: null,
        bytesWritten: 0,
      });
      return;
    }
    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];
    let stdoutTotal = 0;
    let stderrTotal = 0;
    let settled = false;
    let timedOut = false;

    const finish = (exitCode: number, signal: string | null, bytesWritten: number) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      clearTimeout(killTimer);
      resolve({
        stdout: Buffer.concat(stdoutChunks),
        stderr: Buffer.concat(stderrChunks),
        exitCode,
        signal,
        bytesWritten,
      });
    };

    child.stdout.on("data", (chunk: Buffer) => {
      stdoutTotal += chunk.length;
      if (stdoutTotal <= input.maxStreamBytes) stdoutChunks.push(chunk);
      if (stdoutTotal > input.maxStreamBytes || stderrTotal > input.maxStreamBytes) {
        try {
          child.kill("SIGKILL");
        } catch {
          // already dead
        }
        finish(124, "SIGKILL", input.stdinBytes.length);
      }
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderrTotal += chunk.length;
      if (stderrTotal <= input.maxStreamBytes) stderrChunks.push(chunk);
      if (stdoutTotal > input.maxStreamBytes || stderrTotal > input.maxStreamBytes) {
        try {
          child.kill("SIGKILL");
        } catch {
          // already dead
        }
        finish(124, "SIGKILL", input.stdinBytes.length);
      }
    });
    child.once("error", () => finish(126, null, 0));
    child.once("close", (code, closeSignal) => {
      if (timedOut) {
        finish(124, closeSignal ?? "SIGTERM", input.stdinBytes.length);
        return;
      }
      finish(code ?? (closeSignal ? 128 + 15 : 1), closeSignal, input.stdinBytes.length);
    });

    const timer = setTimeout(() => {
      timedOut = true;
      try {
        child.kill("SIGTERM");
      } catch {
        // already dead
      }
    }, input.timeoutMs);
    const killTimer = setTimeout(() => {
      if (!timedOut) return;
      try {
        child.kill("SIGKILL");
      } catch {
        // already dead
      }
    }, input.timeoutMs + input.shutdownGraceMs);

    child.stdin.on("error", () => {
      // EPIPE when the operator command does not read stdin: not fatal.
    });
    child.stdin.end(input.stdinBytes);
  });
}

// ---------------------------------------------------------------------------
// Claude status-line configuration (install / chain / block)
// ---------------------------------------------------------------------------

export const STATUS_LINE_PROXY_INVOCATION_MARKER =
  "__plimsoll-capacity-statusline-proxy" as const;

export type ClaudeStatusLineConfigureInput = {
  settingsPath: string;
  /**
   * Absolute, shell-safe base command that invokes THIS module's hidden proxy
   * entry point (everything before the marker argument), e.g.
   *   tsx /repo/packages/collector-cli/src/provider-capacity-adapters.ts
   * Callers own resolution; this module never guesses launchers.
   */
  baseProxyCommand: string;
  transactionHooks?: NonNullable<Parameters<typeof applyClaudeSettings>[2]>["transactionHooks"];
};

export type ClaudeStatusLineConfigureResult =
  | {
      outcome: "installed" | "already_installed" | "chained" | "unchanged_chain";
      changes: string[];
      backupPath?: string;
      chainedCommand: string | null;
    }
  | { outcome: "blocked_existing_statusline" | "blocked_invalid_settings"; reason: string };

function statusLineEntryFor(chainCommand: string | null, baseProxyCommand: string,
  existing?: Record<string, unknown>): Record<string, unknown> {
  const chainArgument =
    chainCommand === null ? "-" : Buffer.from(chainCommand, "utf8").toString("base64");
  return {
    ...existing,
    type: "command",
    command: `${baseProxyCommand} ${STATUS_LINE_PROXY_INVOCATION_MARKER} ${chainArgument}`,
  };
}

function decodeConfiguredChainCommand(command: unknown): string | null | undefined {
  if (typeof command !== "string") return undefined;
  const prefix = ` ${STATUS_LINE_PROXY_INVOCATION_MARKER} `;
  const markerIndex = command.indexOf(prefix);
  if (markerIndex === -1) return undefined;
  const argument = command.slice(markerIndex + prefix.length).trim();
  if (argument === "-") return null;
  if (!/^[A-Za-z0-9+/=]+$/.test(argument) || argument.length > 8192) return undefined;
  try {
    const decoded = Buffer.from(argument, "base64").toString("utf8");
    return decoded.length > 0 && decoded.length <= 4096 ? decoded : undefined;
  } catch {
    return undefined;
  }
}

function looksLikeOperatorStatusLine(value: unknown): { command: string } | null {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
  const record = value as Record<string, unknown>;
  const allowedKeys = ["type", "command", "padding", "refreshInterval"];
  const keys = Object.keys(record);
  if (keys.some((key) => !allowedKeys.includes(key))) return null;
  if (record.type !== "command") return null;
  if (typeof record.command !== "string" || record.command.trim().length === 0) return null;
  if (record.command.length > 4096) return null;
  if (
    record.padding !== undefined &&
    record.padding !== null &&
    typeof record.padding !== "number"
  ) {
    return null;
  }
  return { command: record.command };
}

function readSettingsSource(settingsPath: string): { source: string; exists: boolean } {
  try {
    return { source: fs.readFileSync(settingsPath, "utf8"), exists: true };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return { source: "", exists: false };
    }
    throw error;
  }
}

/**
 * Install (or chain) the capacity status line through the CURRENT atomic
 * settings transaction. Decision matrix:
 *
 * - no `statusLine` key            → install ours (no chain)
 * - ours, identical                → already_installed (no-op)
 * - ours, same embedded chain      → unchanged_chain (no-op)
 * - operator entry (known shape)   → chain: forward raw stdin to it
 * - unrecognized/malicious entry   → blocked_existing_statusline, ZERO mutation
 * - unparsable settings            → blocked_invalid_settings, ZERO mutation
 *
 * Safety is proven before anything is written: a dry-run application plans the
 * merge first, so every rejection happens without touching the file, and a
 * failed/interrupted commit restores the original bytes (transaction-owned).
 */
export function configureClaudeStatusLineProxy(
  input: ClaudeStatusLineConfigureInput,
): ClaudeStatusLineConfigureResult {
  let currentSource: string;
  try {
    currentSource = readSettingsSource(input.settingsPath).source;
  } catch {
    return { outcome: "blocked_invalid_settings", reason: "settings_unreadable" };
  }
  let currentDocument: Record<string, unknown> = {};
  if (currentSource.trim().length > 0) {
    try {
      const parsed: unknown = JSON.parse(currentSource);
      if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        return { outcome: "blocked_invalid_settings", reason: "settings_root_not_object" };
      }
      currentDocument = parsed as Record<string, unknown>;
    } catch {
      return { outcome: "blocked_invalid_settings", reason: "settings_malformed_json" };
    }
  }

  const foldedKeys = Object.keys(currentDocument).filter(
    (key) => key !== "statusLine" && key.toLowerCase() === "statusline",
  );
  if (foldedKeys.length > 0) {
    return {
      outcome: "blocked_existing_statusline",
      reason: "status_line_key_alias_present",
    };
  }

  const existing = currentDocument.statusLine;
  let chainCommand: string | null = null;
  if (existing !== undefined) {
    const oursDecode = decodeConfiguredChainCommand(
      (existing as Record<string, unknown> | null)?.command,
    );
    if (
      existing !== null &&
      typeof existing === "object" &&
      typeof (existing as Record<string, unknown>).command === "string" &&
      oursDecode !== undefined
    ) {
      // Already ours; keep whatever chain is embedded.
      chainCommand = oursDecode;
    } else {
      const operator = looksLikeOperatorStatusLine(existing);
      if (operator === null) {
        return {
          outcome: "blocked_existing_statusline",
          reason: "unrecognized_existing_entry_shape",
        };
      }
      chainCommand = operator.command;
    }
  }

  const desired = statusLineEntryFor(chainCommand, input.baseProxyCommand,
    existing && typeof existing === "object" && !Array.isArray(existing)
      ? existing as Record<string, unknown> : undefined);
  if (isDeepStrictEqual(existing ?? undefined, desired)) {
    return {
      outcome: chainCommand === null ? "already_installed" : "unchanged_chain",
      changes: [],
      chainedCommand: chainCommand,
    };
  }

  // Prove the plan first: a dry run executes the full reconciliation (alias
  // checks, shape checks) WITHOUT writing, so rejection never mutates.
  try {
    applyClaudeSettings(input.settingsPath, { env: {}, statusLine: desired }, { dryRun: true });
  } catch (error) {
    return {
      outcome:
        error instanceof ClaudeConfigError && error.code === "MALFORMED_JSON"
          ? "blocked_invalid_settings"
          : "blocked_existing_statusline",
      reason: error instanceof ClaudeConfigError ? error.code : "planning_failed",
    };
  }

  try {
    const applied = applyClaudeSettings(
      input.settingsPath,
      { env: {}, statusLine: desired },
      { transactionHooks: input.transactionHooks },
    );
    return {
      outcome: chainCommand === null ? "installed" : "chained",
      changes: applied.changes,
      backupPath: applied.backupPath,
      chainedCommand: chainCommand,
    };
  } catch (error) {
    return {
      outcome: "blocked_existing_statusline",
      reason: error instanceof ClaudeConfigError ? `commit_${error.code}` : "commit_failed",
    };
  }
}

// ---------------------------------------------------------------------------
// Direct-invocation command surface (manual refresh only)
// ---------------------------------------------------------------------------

const STATUS_LINE_BACKUP_NAME = ".plimsoll-status-line-original.json";

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

function currentClaudeAccountKey(): string | undefined {
  try {
    const accountFile = process.env.CLAUDE_CONFIG_DIR
      ? path.join(process.env.CLAUDE_CONFIG_DIR, ".claude.json")
      : path.join(os.homedir(), ".claude.json");
    const stat = fs.lstatSync(accountFile);
    if (!stat.isFile() || stat.isSymbolicLink()) return undefined;
    const identity = JSON.parse(fs.readFileSync(accountFile, "utf8")) as
      { oauthAccount?: { accountUuid?: unknown } };
    const id = identity.oauthAccount?.accountUuid;
    return typeof id === "string" && id.length > 0 ? providerAccountKey(id) : undefined;
  } catch { return undefined; }
}

type StatusLineBackup = { existed: boolean; bytes: string; installedStatusLine?: unknown;
  installedOtherKeys?: Record<string, unknown> };

function writeStatusLineBackup(file: string, value: StatusLineBackup): void {
  const temp = `${file}.plimsoll-write-${process.pid}`;
  fs.writeFileSync(temp, JSON.stringify(value), { mode: 0o600, flag: "wx" });
  fs.renameSync(temp, file);
}

function restoreClaudeStatusLine(configDir: string): "restored" | "status_line_changed" {
  const settings = path.join(configDir, "settings.json");
  const backup = path.join(configDir, STATUS_LINE_BACKUP_NAME);
  const stat = fs.lstatSync(backup);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("status_line_backup_invalid");
  const saved = JSON.parse(fs.readFileSync(backup, "utf8")) as Partial<StatusLineBackup>;
  if (typeof saved.existed !== "boolean" || typeof saved.bytes !== "string") throw new Error("status_line_backup_invalid");
  let current: Record<string, unknown>;
  try {
    const parsed: unknown = JSON.parse(fs.readFileSync(settings, "utf8"));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return "status_line_changed";
    current = parsed as Record<string, unknown>;
  } catch { return "status_line_changed"; }
  // A marker alone does not prove ownership: the operator may have edited
  // this entry after setup. An older backup without the installed snapshot is
  // also insufficient evidence to restore safely.
  if (saved.installedStatusLine === undefined ||
      !isDeepStrictEqual(current.statusLine, saved.installedStatusLine)) return "status_line_changed";
  const originalBytes = Buffer.from(saved.bytes, "base64");
  const original = saved.existed ? JSON.parse(originalBytes.toString("utf8")) as Record<string, unknown> : {};
  if (Object.hasOwn(original, "statusLine")) current.statusLine = original.statusLine;
  else delete current.statusLine;
  for (const [key, installedValue] of Object.entries(saved.installedOtherKeys ?? {})) {
    if (!Object.hasOwn(original, key) && isDeepStrictEqual(current[key], installedValue)) delete current[key];
  }
  if (saved.existed && isDeepStrictEqual(current, original)) {
    const temp = `${settings}.plimsoll-restore-${process.pid}`;
    fs.writeFileSync(temp, originalBytes, { mode: 0o600, flag: "wx" });
    fs.renameSync(temp, settings);
  } else if (!saved.existed && Object.keys(current).length === 0) fs.rmSync(settings);
  else {
    const temp = `${settings}.plimsoll-restore-${process.pid}`;
    fs.writeFileSync(temp, `${JSON.stringify(current, null, 2)}\n`, { mode: 0o600, flag: "wx" });
    fs.renameSync(temp, settings);
  }
  fs.rmSync(backup);
  return "restored";
}

/** Standalone source command; each target preserves its original bytes for uninstall. */
export function setupClaudeStatusLine(argv: string[]): Array<{ configDir: string; outcome: string }> {
  const defaultDir = process.env.CLAUDE_CONFIG_DIR
    ? path.resolve(process.env.CLAUDE_CONFIG_DIR) : path.join(os.homedir(), ".claude");
  const dirs = [defaultDir];
  let uninstall = false;
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === "--uninstall") { uninstall = true; continue; }
    if (argv[index] === "--config-dir" && argv[index + 1] && !argv[index + 1]!.startsWith("--")) {
      dirs.push(path.resolve(argv[++index]!));
      continue;
    }
    throw new Error("usage: setup-claude-status-line [--config-dir <dir>]... [--uninstall]");
  }
  const results: Array<{ configDir: string; outcome: string }> = [];
  for (const dir of [...new Set(dirs)]) {
    const settings = path.join(dir, "settings.json");
    assertManagedConfigTarget(settings);
    const backup = path.join(dir, STATUS_LINE_BACKUP_NAME);
    if (uninstall) {
      if (fs.existsSync(backup)) results.push({ configDir: dir, outcome: restoreClaudeStatusLine(dir) });
      else results.push({ configDir: dir, outcome: "not_installed" });
      continue;
    }
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    const existed = fs.existsSync(settings);
    const newBackup = !fs.existsSync(backup);
    if (newBackup) {
      const bytes = existed ? fs.readFileSync(settings) : Buffer.alloc(0);
      fs.writeFileSync(backup, JSON.stringify({ existed, bytes: bytes.toString("base64") }), { flag: "wx", mode: 0o600 });
    }
    const command = ["env", dir === path.join(os.homedir(), ".claude") && !process.env.CLAUDE_CONFIG_DIR
      ? "-u CLAUDE_CONFIG_DIR" : `CLAUDE_CONFIG_DIR=${shellQuote(dir)}`,
      shellQuote(process.execPath), ...process.execArgv.map(shellQuote), shellQuote(path.resolve(process.argv[1]!))].join(" ");
    const outcome = configureClaudeStatusLineProxy({ settingsPath: settings, baseProxyCommand: command });
    if ("reason" in outcome) {
      if (newBackup) fs.rmSync(backup);
      throw new Error(`claude_status_line_${outcome.outcome}:${outcome.reason}`);
    }
    const saved = JSON.parse(fs.readFileSync(backup, "utf8")) as StatusLineBackup;
    const installed = JSON.parse(fs.readFileSync(settings, "utf8")) as Record<string, unknown>;
    const before = saved.existed ? JSON.parse(Buffer.from(saved.bytes, "base64").toString("utf8")) as
      Record<string, unknown> : {};
    const installedOtherKeys = newBackup ? Object.fromEntries(Object.entries(installed).filter(([key]) =>
      key !== "statusLine" && !Object.hasOwn(before, key))) : {};
    writeStatusLineBackup(backup, { ...saved, installedStatusLine: installed.statusLine,
      installedOtherKeys: { ...saved.installedOtherKeys, ...installedOtherKeys } });
    results.push({ configDir: dir, outcome: outcome.outcome });
  }
  return results;
}

/** The packaged CLI's command path records plan-limit rows without importing capacity planning. */
export async function claudeStatusLineCliMain(argv: string[]): Promise<void> {
  const command = argv[0];
  if (command === "setup-claude-status-line") {
    process.stdout.write(`${JSON.stringify({ status: "claude_status_line_setup", results: setupClaudeStatusLine(argv.slice(1)) }, null, 2)}\n`);
    return;
  }
  if (command !== STATUS_LINE_PROXY_INVOCATION_MARKER) {
    process.stderr.write("usage: setup-claude-status-line [--config-dir <dir>]... [--uninstall]\n");
    process.exitCode = 64;
    return;
  }

  const encodedChain = argv[1] ?? "-";
  const chainCommand = encodedChain === "-" ? null : Buffer.from(encodedChain, "base64").toString("utf8");
  const stdinChunks: Buffer[] = [];
  let stdinTotal = 0;
  let overBound = false;
  for await (const chunk of process.stdin) {
    const bytes = chunk as Buffer;
    stdinTotal += bytes.length;
    if (stdinTotal <= CLAUDE_STATUS_LINE_MAX_STDIN_BYTES) stdinChunks.push(bytes);
    else overBound = true;
  }
  const stdinBytes = overBound ? Buffer.concat([...stdinChunks, Buffer.alloc(1)]) : Buffer.concat(stdinChunks);
  const accountKeyBeforeChain = overBound ? undefined : currentClaudeAccountKey();
  const chained = overBound ? {
    stdout: Buffer.alloc(0), stderr: Buffer.from("plimsoll-capacity-status-line: input_bound_exceeded; status line passthrough skipped\n"),
    exitCode: 0,
  } : chainCommand === null || chainCommand.length === 0 ? {
    stdout: Buffer.alloc(0), stderr: Buffer.alloc(0), exitCode: 0,
  } : await runChainedStatusLineCommand({
    command: chainCommand, stdinBytes, timeoutMs: STATUS_LINE_CHAIN_DEFAULT_TIMEOUT_MS,
    shutdownGraceMs: STATUS_LINE_CHAIN_SHUTDOWN_GRACE_MS, maxStreamBytes: STATUS_LINE_CHAIN_MAX_STREAM_BYTES,
  });

  if (!overBound) {
    try {
      const accountKeyAfterChain = currentClaudeAccountKey();
      const accountKey = accountKeyBeforeChain !== undefined &&
        accountKeyBeforeChain === accountKeyAfterChain ? accountKeyBeforeChain : undefined;
      const windows = claudePlanLimitWindows(JSON.parse(stdinBytes.toString("utf8")) as unknown);
      if (windows.length > 0) {
        ensureCollectorHome();
        const buffer = new LocalEventBuffer(collectorBufferPath());
        try {
          const emitter = new PlanLimitEmitter(buffer);
          const observedAt = new Date().toISOString();
          for (const window of windows) emitter.observe({ source: "claude_code", accountKey,
            observedAt, window, planLimitSource: "claude_status_line" });
        } finally { buffer.close(); }
      }
    } catch {
      // Collection is best-effort; the operator's status line keeps its original output.
    }
  }
  if (chained.stdout.length > 0) process.stdout.write(chained.stdout);
  if (chained.stderr.length > 0) process.stderr.write(chained.stderr);
  process.exitCode = chained.exitCode;
}
