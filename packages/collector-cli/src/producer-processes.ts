import { execFile } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  discoverClaudeSeats,
  discoverCodexProfiles,
  managedConfigProofContext,
} from "../../collector-config/src/index";
import type { LocalProducerSource } from "./http-boundary";
import type { RejectionDiagnosticsCounters } from "./rejection-diagnostics";

/**
 * Producer processes older than their managed config (bead eco-6hoxj.153).
 *
 * Codex, Claude Code, Gemini CLI and Grok read their config once, at process
 * start. A producer started before the collector managed (or last rewrote) its
 * config keeps sending the headers it read then, and the boundary answers
 * `source_required` / `producer_token_required` for as long as it runs — for
 * days, on 2026-09-16. The rejection diagnostics cannot name that process;
 * this scan does, read-only:
 *
 *   - one `ps` read of the process table, plus one `ps -E` read restricted to
 *     the producer pids (the config home comes from the process environment
 *     when it is readable) and, only when there is a producer, one
 *     `launchctl print gui/<uid>` for service ownership. Each call is bounded
 *     by PRODUCER_PROCESS_TIMEOUT_MS; at most PRODUCER_PROCESS_ROW_LIMIT
 *     producer rows are examined (a normal Mac process table is far larger
 *     than the bound, so the bound applies to producers, not to every row);
 *   - the managed-apply time of a surface comes from what the collector already
 *     keeps: reconcile receipts naming the target `applied`, and the
 *     `<file>.plimsoll-backup-<stamp>` every managed write (setup, reconcile,
 *     rotation) leaves beside the file. Without either it falls back to the
 *     file mtime and says so;
 *   - a process whose environment cannot be read has home `unknown` and is
 *     never `staleConfig: true`;
 *   - nothing is printed from a command line or an environment except the pid,
 *     a home under $HOME, and a conductor seat or launchd label. Never a value.
 *
 * A proof, harness or test never reads the real process table: it reads the
 * fixture named by PRODUCER_PROCESS_FIXTURE_ENV or reports `not_inspected`.
 */

export const PRODUCER_PROCESS_ROW_LIMIT = 400;
export const PRODUCER_PROCESS_TIMEOUT_MS = 2_000;
export const STALE_PRODUCER_SCAN_MAX_AGE_MS = 60_000;
export const PRODUCER_PROCESS_FIXTURE_ENV = "PLIMSOLL_PRODUCER_PROCESS_FIXTURE";

/** Rejection reasons a producer running a pre-managed config produces. */
export const STALE_PRODUCER_REJECTION_REASONS = ["source_required", "producer_token_required"] as const;

const PRODUCER_SOURCES: readonly LocalProducerSource[] = ["claude_code", "codex", "gemini_cli", "grok"];
const MAX_ANCESTOR_HOPS = 16;
const MAX_RECONCILE_RECEIPTS_READ = 50;

type CommandRead = { ok: true; stdout: string } | { ok: false; error: "timeout" | "unavailable" };

/** The three native reads the scan makes; a fixture replaces all of them. */
export type ProducerProcessProvider = {
  kind: "process_table" | "fixture";
  uid: number;
  /** `ps -axo pid=,ppid=,lstart=,command=` */
  processTable(): Promise<CommandRead>;
  /** `ps -E -o pid=,command= -p <pids>` */
  environments(pids: number[]): Promise<CommandRead>;
  /** `launchctl print gui/<uid>` */
  launchdServices(): Promise<CommandRead>;
};

/**
 * Fixture file shape: the native command outputs verbatim, so the proof
 * exercises the same parsers the real process table does. A read given as
 * `{ "error": "timeout" }` simulates that failure.
 */
type ProducerProcessFixture = {
  uid: number;
  processTable: string | { error: "timeout" | "unavailable" };
  environments: string | { error: "timeout" | "unavailable" };
  launchctl: string | { error: "timeout" | "unavailable" };
};

function runBounded(executable: string, args: string[]): Promise<CommandRead> {
  return new Promise((resolve) => {
    execFile(
      executable,
      args,
      {
        timeout: PRODUCER_PROCESS_TIMEOUT_MS,
        maxBuffer: 32 * 1024 * 1024,
        encoding: "utf8",
        env: { ...process.env, LC_ALL: "C", LANG: "C" },
      },
      (error, stdout) => {
        if (error) {
          const killed = (error as { killed?: boolean }).killed === true ||
            (error as { signal?: string | null }).signal === "SIGTERM";
          resolve({ ok: false, error: killed ? "timeout" : "unavailable" });
          return;
        }
        resolve({ ok: true, stdout });
      },
    );
  });
}

function fixtureRead(value: ProducerProcessFixture[keyof Omit<ProducerProcessFixture, "uid">]): CommandRead {
  if (typeof value === "string") return { ok: true, stdout: value };
  return { ok: false, error: value?.error === "timeout" ? "timeout" : "unavailable" };
}

/**
 * The provider for this process: the fixture when one is declared, the native
 * process table outside a proof context, and nothing inside one.
 */
export function resolveProducerProcessProvider(
  env: NodeJS.ProcessEnv = process.env,
): ProducerProcessProvider | null {
  const fixturePath = env[PRODUCER_PROCESS_FIXTURE_ENV];
  if (typeof fixturePath === "string" && fixturePath.length > 0) {
    const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8")) as ProducerProcessFixture;
    return {
      kind: "fixture",
      uid: fixture.uid,
      processTable: async () => fixtureRead(fixture.processTable),
      environments: async () => fixtureRead(fixture.environments),
      launchdServices: async () => fixtureRead(fixture.launchctl),
    };
  }
  if (managedConfigProofContext(env)) return null;
  const uid = typeof process.getuid === "function" ? process.getuid() : -1;
  return {
    kind: "process_table",
    uid,
    processTable: () => runBounded("ps", ["-axww", "-o", "pid=,ppid=,lstart=,command="]),
    environments: (pids) => runBounded("ps", ["-Eww", "-o", "pid=,command=", "-p", pids.join(",")]),
    launchdServices: () => runBounded("launchctl", ["print", `gui/${uid}`]),
  };
}

type ProcessRow = { pid: number; ppid: number; startedAt: string | null; command: string };

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const PROCESS_ROW = /^\s*(\d+)\s+(\d+)\s+[A-Z][a-z]{2}\s+([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{2}):(\d{2}):(\d{2})\s+(\d{4})\s(.*)$/;

/** One `ps -o pid=,ppid=,lstart=,command=` line; `lstart` is local time in the C locale. */
export function parseProcessRow(line: string): ProcessRow | null {
  const match = PROCESS_ROW.exec(line);
  if (!match) return null;
  const [, pid, ppid, month, day, hour, minute, second, year, command] = match;
  const monthIndex = MONTHS.indexOf(month!);
  const started = new Date(Number(year), monthIndex, Number(day), Number(hour), Number(minute), Number(second));
  return {
    pid: Number(pid),
    ppid: Number(ppid),
    startedAt: monthIndex < 0 || Number.isNaN(started.getTime()) ? null : started.toISOString(),
    command: command!.trim(),
  };
}

/** The producer a command line runs, or null. Reads the executable name only. */
export function producerSourceForCommand(command: string): LocalProducerSource | null {
  const bundled = /^(.*?\.app\/Contents\/\S+)/.exec(command);
  const tokens = command.split(/\s+/);
  let executable = bundled ? bundled[1]! : tokens[0] ?? "";
  let name = path.basename(executable);
  if (/^(?:node\d*|bun|deno)$/.test(name) && tokens[1] && !tokens[1].startsWith("-")) {
    executable = tokens[1];
    name = path.basename(executable).replace(/\.(?:[cm]?js)$/, "");
  }
  if (name === "codex") return "codex";
  if (name === "claude" || (name === "cli" && executable.includes("/claude-code/"))) return "claude_code";
  if (name === "gemini") return "gemini_cli";
  if (name === "grok") return "grok";
  return null;
}

const ENVIRONMENT_KEYS = ["HOME", "CODEX_HOME", "CLAUDE_CONFIG_DIR", "GROK_HOME", "GEMINI_CLI_HOME"] as const;
type EnvironmentKey = (typeof ENVIRONMENT_KEYS)[number];

/**
 * The config-home variables of each pid from `ps -E` output, or no entry when
 * the environment is not readable (ps prints the bare command then). Only the
 * named keys are kept; every other variable is dropped unread.
 */
export function parseProcessEnvironments(
  stdout: string,
  commands: Map<number, string>,
): Map<number, Partial<Record<EnvironmentKey, string>>> {
  const environments = new Map<number, Partial<Record<EnvironmentKey, string>>>();
  for (const line of stdout.split("\n")) {
    const match = /^\s*(\d+)\s(.*)$/.exec(line);
    if (!match) continue;
    const pid = Number(match[1]);
    const command = commands.get(pid);
    const rest = match[2]!.trimStart();
    if (command === undefined || !rest.startsWith(command)) continue;
    const suffix = rest.slice(command.length);
    if (!/^\s+[A-Za-z_][A-Za-z0-9_]*=/.test(suffix)) continue;
    const values: Partial<Record<EnvironmentKey, string>> = {};
    for (const key of ENVIRONMENT_KEYS) {
      const value = new RegExp(`\\s${key}=(\\S+)`).exec(suffix)?.[1];
      if (value) values[key] = value;
    }
    environments.set(pid, values);
  }
  return environments;
}

/** Running launchd services of the GUI domain: pid → label. */
export function parseLaunchdServices(stdout: string): Map<number, string> {
  const services = new Map<number, string>();
  const section = /\n\s*services = \{\n([\s\S]*?)\n\s*\}/.exec(`\n${stdout}`)?.[1] ?? "";
  for (const line of section.split("\n")) {
    const match = /^\s*(\d+)\s+\S+\s+(\S+)\s*$/.exec(line);
    if (match && Number(match[1]) > 0) services.set(Number(match[1]), match[2]!);
  }
  return services;
}

function realish(value: string) {
  try {
    return fs.realpathSync(value);
  } catch {
    return path.resolve(value);
  }
}

function samePath(left: string, right: string) {
  return path.resolve(left) === path.resolve(right) || realish(left) === realish(right);
}

type ManagedSurface = { name: string; file: string };

/** The config home a producer reads, and the managed surface the collector applies there. */
function resolveHome(
  source: LocalProducerSource,
  environment: Partial<Record<EnvironmentKey, string>>,
  home: string,
  grokHome: string,
): { directory: string; homeSource: "env" | "default"; surface: ManagedSurface | null } {
  const processHome = environment.HOME ?? home;
  if (source === "codex") {
    const directory = environment.CODEX_HOME ?? path.join(processHome, ".codex");
    const homeSource = environment.CODEX_HOME ? "env" as const : "default" as const;
    if (samePath(directory, path.join(home, ".codex"))) {
      return { directory, homeSource, surface: { name: "codex", file: path.join(directory, "config.toml") } };
    }
    const profile = discoverCodexProfiles(home).find((entry) => samePath(path.dirname(entry.path), directory));
    return {
      directory,
      homeSource,
      surface: profile ? { name: `codexProfile[${profile.slug}]`, file: profile.path } : null,
    };
  }
  if (source === "claude_code") {
    const directory = environment.CLAUDE_CONFIG_DIR ?? path.join(processHome, ".claude");
    const homeSource = environment.CLAUDE_CONFIG_DIR ? "env" as const : "default" as const;
    if (samePath(directory, path.join(home, ".claude"))) {
      return { directory, homeSource, surface: { name: "claude", file: path.join(directory, "settings.json") } };
    }
    const seat = discoverClaudeSeats(home).find((entry) => samePath(path.dirname(entry.path), directory));
    return {
      directory,
      homeSource,
      surface: seat ? { name: `claudeSeat[${seat.slug}]`, file: seat.path } : null,
    };
  }
  if (source === "gemini_cli") {
    const directory = path.join(environment.GEMINI_CLI_HOME ?? processHome, ".gemini");
    const homeSource = environment.GEMINI_CLI_HOME ? "env" as const : "default" as const;
    const managed = samePath(directory, path.join(home, ".gemini"));
    return {
      directory,
      homeSource,
      surface: managed ? { name: "gemini", file: path.join(directory, "settings.json") } : null,
    };
  }
  const directory = environment.GROK_HOME ?? path.join(processHome, ".grok");
  const homeSource = environment.GROK_HOME ? "env" as const : "default" as const;
  const managed = samePath(directory, grokHome);
  return {
    directory,
    homeSource,
    surface: managed ? { name: "grok", file: path.join(directory, "hooks", "plimsoll.json") } : null,
  };
}

/** A home under $HOME as `~/…`; anything else is never printed. */
function reportableHome(directory: string, home: string) {
  const relative = path.relative(home, directory);
  if (relative === "") return "~";
  if (relative.startsWith("..") || path.isAbsolute(relative)) return "outside_home";
  return `~/${relative.split(path.sep).join("/")}`;
}

export type ManagedAppliedAtSource = "reconcile_receipt" | "apply_backup" | "file_mtime" | "unavailable";

const BACKUP_STAMP = /^(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})-(\d{3})Z/;

function newestIso(values: string[]) {
  return values.reduce<string | null>((newest, value) =>
    newest === null || Date.parse(value) > Date.parse(newest) ? value : newest, null);
}

/** Newest `applied` time per managed target name from the reconcile receipts. */
function readReconcileAppliedAt(collectorHome: string): Map<string, string> {
  const applied = new Map<string, string>();
  const directory = path.join(collectorHome, "receipts");
  let names: string[];
  try {
    names = fs.readdirSync(directory)
      .filter((name) => name.startsWith("managed-config-reconcile-") && name.endsWith(".json"))
      .sort()
      .slice(-MAX_RECONCILE_RECEIPTS_READ);
  } catch {
    return applied;
  }
  for (const name of names) {
    try {
      const receipt = JSON.parse(fs.readFileSync(path.join(directory, name), "utf8")) as {
        startedAt?: unknown;
        targets?: Array<{ name?: unknown; status?: unknown }>;
      };
      if (typeof receipt.startedAt !== "string" || Number.isNaN(Date.parse(receipt.startedAt))) continue;
      for (const target of receipt.targets ?? []) {
        if (target.status !== "applied" || typeof target.name !== "string") continue;
        applied.set(target.name, newestIso([receipt.startedAt, ...(applied.has(target.name) ? [applied.get(target.name)!] : [])])!);
      }
    } catch {
      // An unreadable receipt is not evidence of an apply.
    }
  }
  return applied;
}

function managedAppliedAt(
  surface: ManagedSurface,
  reconcileApplied: Map<string, string>,
): { at: string | null; source: ManagedAppliedAtSource } {
  const backups: string[] = [];
  const prefix = `${path.basename(surface.file)}.plimsoll-backup-`;
  try {
    for (const name of fs.readdirSync(path.dirname(surface.file))) {
      if (!name.startsWith(prefix)) continue;
      const stamp = BACKUP_STAMP.exec(name.slice(prefix.length));
      if (stamp) backups.push(`${stamp[1]}T${stamp[2]}:${stamp[3]}:${stamp[4]}.${stamp[5]}Z`);
    }
  } catch {
    // No readable directory: fall through to the other records.
  }
  const fromReceipt = reconcileApplied.get(surface.name) ?? null;
  const fromBackup = newestIso(backups);
  if (fromReceipt !== null || fromBackup !== null) {
    const newest = newestIso([fromReceipt, fromBackup].filter((value): value is string => value !== null))!;
    return { at: newest, source: newest === fromReceipt ? "reconcile_receipt" : "apply_backup" };
  }
  try {
    return { at: fs.statSync(surface.file).mtime.toISOString(), source: "file_mtime" };
  } catch {
    return { at: null, source: "unavailable" };
  }
}

export type ProducerProcessOwner = `launchd:${string}` | `conductor:${string}` | "desktop-app" | "other";

export type ProducerProcess = {
  pid: number;
  source: LocalProducerSource;
  /** `~/…`, `outside_home`, or `unknown` when the environment was not readable. */
  home: string;
  homeSource: "env" | "default" | "unreadable";
  startedAt: string | null;
  managedSurface: string | null;
  managedAppliedAt: string | null;
  managedAppliedAtSource: ManagedAppliedAtSource | null;
  staleConfig: boolean;
  owner: ProducerProcessOwner;
  restartHint: string | null;
};

export type ProducerProcessScan = {
  inspection: "complete" | "truncated" | "timeout" | "unavailable" | "not_inspected";
  reason?: string;
  provider: "process_table" | "fixture" | null;
  scannedAt: string;
  rowLimit: number;
  producerRows: number;
  truncated: boolean;
  environmentsRead: "complete" | "timeout" | "unavailable" | "not_needed";
  launchdServicesRead: "complete" | "timeout" | "unavailable" | "not_needed";
  staleCount: number;
  processes: ProducerProcess[];
  /** One operator line when a producer runs a config older than the managed one. */
  summary: string | null;
};

const CONDUCTOR_COMMAND = /conductor/i;

function conductorSeat(parentCommand: string, parentLabel: string | undefined, surface: ManagedSurface | null) {
  const fromCommand = /nr-conductors\/([^/\s]+)/.exec(parentCommand)?.[1] ??
    /--(?:seat|profile)[=\s]([^\s/]+)/.exec(parentCommand)?.[1];
  if (fromCommand) return fromCommand;
  const fromLabel = parentLabel ? /conductor\.(.+)$/.exec(parentLabel)?.[1] : undefined;
  if (fromLabel) return fromLabel;
  const slug = surface ? /\[(.+)\]$/.exec(surface.name)?.[1] : undefined;
  if (slug) return slug;
  return surface?.name === "codex" || surface?.name === "claude" ? "default" : "unknown";
}

function resolveOwner(
  row: ProcessRow,
  source: LocalProducerSource,
  table: Map<number, ProcessRow>,
  services: Map<number, string>,
  surface: ManagedSurface | null,
  uid: number,
): { owner: ProducerProcessOwner; hint: string } {
  const appName = (command: string) => /([^/]+)\.app\/Contents\//.exec(command)?.[1];
  const ownLabel = services.get(row.pid);
  if (ownLabel && !ownLabel.startsWith("application.")) {
    return { owner: `launchd:${ownLabel}`, hint: `launchctl kickstart -k gui/${uid}/${ownLabel}` };
  }
  const parent = table.get(row.ppid);
  if (parent && CONDUCTOR_COMMAND.test(parent.command)) {
    const parentLabel = services.get(parent.pid);
    const seat = conductorSeat(parent.command, parentLabel, surface);
    return {
      owner: `conductor:${seat}`,
      hint: parentLabel && !parentLabel.startsWith("application.")
        ? `launchctl kickstart -k gui/${uid}/${parentLabel}`
        : `restart conductor seat ${seat} through the conductor (not by killing pid ${row.pid})`,
    };
  }
  let current: ProcessRow | undefined = row;
  for (let hop = 0; current && hop < MAX_ANCESTOR_HOPS; hop += 1) {
    const label = services.get(current.pid);
    const app = appName(current.command);
    if (app || label?.startsWith("application.")) {
      return {
        owner: "desktop-app",
        hint: `quit and reopen ${app ? `the ${app} app` : "the desktop app"} that owns pid ${row.pid}`,
      };
    }
    if (label && current.pid !== row.pid) {
      return { owner: `launchd:${label}`, hint: `launchctl kickstart -k gui/${uid}/${label}` };
    }
    if (current.ppid <= 1 || current.ppid === current.pid) break;
    current = table.get(current.ppid);
  }
  return { owner: "other", hint: `restart pid ${row.pid} (${source}) where it was started` };
}

function summaryLine(processes: ProducerProcess[]) {
  const stale = processes.filter((entry) => entry.staleConfig);
  if (stale.length === 0) return null;
  const hints = [...new Set(stale.map((entry) => entry.restartHint).filter((hint): hint is string => hint !== null))];
  const shown = hints.slice(0, 5).join("; ");
  const more = hints.length > 5 ? `; and ${hints.length - 5} more (see producerProcesses)` : "";
  const byMtime = stale.filter((entry) => entry.managedAppliedAtSource === "file_mtime").length;
  const mtimeNote = byMtime > 0 ? ` (${byMtime} judged by file mtime only, no managed-apply record)` : "";
  return `${stale.length} producer process(es) older than their managed config${mtimeNote} — they send stale headers; restart: ${shown}${more}`;
}

function emptyScan(
  inspection: ProducerProcessScan["inspection"],
  provider: ProducerProcessProvider | null,
  now: Date,
  reason?: string,
): ProducerProcessScan {
  return {
    inspection,
    ...(reason ? { reason } : {}),
    provider: provider?.kind ?? null,
    scannedAt: now.toISOString(),
    rowLimit: PRODUCER_PROCESS_ROW_LIMIT,
    producerRows: 0,
    truncated: false,
    environmentsRead: "not_needed",
    launchdServicesRead: "not_needed",
    staleCount: 0,
    processes: [],
    summary: null,
  };
}

export type ProducerProcessScanOptions = {
  collectorHome: string;
  /** The home whose managed surfaces the collector applies (doctor's $HOME). */
  home?: string;
  grokHome?: string;
  provider?: ProducerProcessProvider | null;
  now?: () => Date;
};

/** Enumerate local producer processes once and compare each with its managed config. */
export async function scanProducerProcesses(options: ProducerProcessScanOptions): Promise<ProducerProcessScan> {
  const now = options.now ?? (() => new Date());
  const home = options.home ?? os.homedir();
  const grokHome = options.grokHome ?? path.join(home, ".grok");
  let provider: ProducerProcessProvider | null;
  try {
    provider = options.provider === undefined ? resolveProducerProcessProvider() : options.provider;
  } catch {
    return emptyScan("unavailable", null, now(), "process_fixture_unreadable");
  }
  if (!provider) return emptyScan("not_inspected", null, now(), "proof_context_without_process_fixture");

  const tableRead = await provider.processTable();
  if (!tableRead.ok) return emptyScan(tableRead.error, provider, now());
  const table = new Map<number, ProcessRow>();
  const producers: Array<{ row: ProcessRow; source: LocalProducerSource }> = [];
  let producerRows = 0;
  for (const line of tableRead.stdout.split("\n")) {
    const row = parseProcessRow(line);
    if (!row) continue;
    table.set(row.pid, row);
    const source = producerSourceForCommand(row.command);
    if (!source) continue;
    producerRows += 1;
    if (producers.length < PRODUCER_PROCESS_ROW_LIMIT) producers.push({ row, source });
  }
  const truncated = producerRows > PRODUCER_PROCESS_ROW_LIMIT;
  if (producers.length === 0) {
    return { ...emptyScan("complete", provider, now()), producerRows };
  }

  const environmentRead = await provider.environments(producers.map(({ row }) => row.pid));
  const environments = environmentRead.ok
    ? parseProcessEnvironments(environmentRead.stdout, new Map(producers.map(({ row }) => [row.pid, row.command])))
    : new Map<number, Partial<Record<EnvironmentKey, string>>>();
  const servicesRead = await provider.launchdServices();
  const services = servicesRead.ok ? parseLaunchdServices(servicesRead.stdout) : new Map<number, string>();
  const reconcileApplied = readReconcileAppliedAt(options.collectorHome);
  const appliedBySurface = new Map<string, ReturnType<typeof managedAppliedAt>>();

  const processes = producers.map(({ row, source }): ProducerProcess => {
    const environment = environments.get(row.pid);
    if (!environment) {
      const { owner } = resolveOwner(row, source, table, services, null, provider.uid);
      return {
        pid: row.pid,
        source,
        home: "unknown",
        homeSource: "unreadable",
        startedAt: row.startedAt,
        managedSurface: null,
        managedAppliedAt: null,
        managedAppliedAtSource: null,
        staleConfig: false,
        owner,
        restartHint: null,
      };
    }
    const resolved = resolveHome(source, environment, home, grokHome);
    let applied: ReturnType<typeof managedAppliedAt> | null = null;
    if (resolved.surface) {
      applied = appliedBySurface.get(resolved.surface.name) ?? managedAppliedAt(resolved.surface, reconcileApplied);
      appliedBySurface.set(resolved.surface.name, applied);
    }
    const staleConfig = Boolean(
      row.startedAt && applied?.at && Date.parse(row.startedAt) < Date.parse(applied.at),
    );
    const { owner, hint } = resolveOwner(row, source, table, services, resolved.surface, provider.uid);
    return {
      pid: row.pid,
      source,
      home: reportableHome(resolved.directory, home),
      homeSource: resolved.homeSource,
      startedAt: row.startedAt,
      managedSurface: resolved.surface?.name ?? null,
      managedAppliedAt: applied?.at ?? null,
      managedAppliedAtSource: applied?.source ?? null,
      staleConfig,
      owner,
      restartHint: staleConfig ? hint : null,
    };
  });
  const staleCount = processes.filter((entry) => entry.staleConfig).length;
  return {
    inspection: truncated ? "truncated" : "complete",
    provider: provider.kind,
    scannedAt: now().toISOString(),
    rowLimit: PRODUCER_PROCESS_ROW_LIMIT,
    producerRows,
    truncated,
    environmentsRead: environmentRead.ok ? "complete" : environmentRead.error,
    launchdServicesRead: servicesRead.ok ? "complete" : servicesRead.error,
    staleCount,
    processes,
    summary: summaryLine(processes),
  };
}

/** Open rejection windows of the reasons a stale producer causes. */
export function openStaleProducerWindows(counters: RejectionDiagnosticsCounters | null | undefined) {
  return (counters?.reasons ?? []).filter((row) =>
    (STALE_PRODUCER_REJECTION_REASONS as readonly string[]).includes(row.reason) &&
    row.openWindow !== null &&
    row.openWindow.count > 0
  );
}

type CaptureHealthLike = { sources?: Array<{ source: string; reason: string } & Record<string, unknown>> } &
  Record<string, unknown>;

/**
 * Name the stale-producer count in the reason of every capture source a
 * `source_required` / `producer_token_required` window can belong to. Only the
 * `reason` text changes: status, overall and every counter stay as they were,
 * so this is a diagnostic, never a fault shape. A scan older than
 * STALE_PRODUCER_SCAN_MAX_AGE_MS is not used.
 */
export function annotateCaptureHealthWithStaleProducers<T>(
  value: T,
  counters: RejectionDiagnosticsCounters | null | undefined,
  scan: ProducerProcessScan | null,
  nowMs: number = Date.now(),
): T {
  const windows = openStaleProducerWindows(counters);
  const captureHealth = value as unknown as CaptureHealthLike | null | undefined;
  if (!captureHealth || !Array.isArray(captureHealth.sources) || windows.length === 0) return value;
  const fresh = scan && nowMs - Date.parse(scan.scannedAt) <= STALE_PRODUCER_SCAN_MAX_AGE_MS ? scan : null;
  const sources = captureHealth.sources.map((entry) => {
    if (!(PRODUCER_SOURCES as readonly string[]).includes(entry.source)) return entry;
    const reasons = [...new Set(windows
      .filter((row) => row.clientClass === entry.source || row.clientClass === "otlp_exporter" || row.clientClass === "unknown")
      .map((row) => row.reason))];
    if (reasons.length === 0) return entry;
    const scanText = !fresh
      ? "stale-producer scan pending"
      : fresh.inspection === "complete" || fresh.inspection === "truncated"
        ? `${fresh.processes.filter((process) => process.source === entry.source && process.staleConfig).length} ${entry.source} producer process(es) older than their managed config${fresh.truncated ? " (scan truncated)" : ""} — plimsoll doctor --read-only --json names them`
        : `stale-producer scan ${fresh.inspection}`;
    return { ...entry, reason: `${entry.reason}; ${reasons.join("/")} rejections open: ${scanText}` };
  });
  return { ...captureHealth, sources } as T;
}

/**
 * The daemon's cached scan. `latest()` never blocks and never starts a
 * process table read on the status path itself; `refresh()` starts at most one
 * background scan when the cached one is older than the bound.
 */
export function createStaleProducerScanCache(scan: () => Promise<ProducerProcessScan>, nowMs: () => number = Date.now) {
  let cached: ProducerProcessScan | null = null;
  let inflight: Promise<ProducerProcessScan> | null = null;
  const latest = (): ProducerProcessScan | null =>
    cached && nowMs() - Date.parse(cached.scannedAt) <= STALE_PRODUCER_SCAN_MAX_AGE_MS ? cached : null;
  return {
    latest,
    refresh(): Promise<ProducerProcessScan> {
      if (inflight) return inflight;
      const current = latest();
      if (current) return Promise.resolve(current);
      inflight = scan()
        .then((result) => {
          cached = result;
          return result;
        })
        .catch(() => {
          // A failed scan is cached like any other, so it is not retried on
          // every status read.
          cached = emptyScan("unavailable", null, new Date(nowMs()), "scan_failed");
          return cached;
        })
        .finally(() => {
          inflight = null;
        });
      return inflight;
    },
  };
}
