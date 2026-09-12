import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { collectorConfigSchema, withCollectorConfigMutationLock } from "./config";
import {
  type ApplyPlanEntry,
  type ApplyResult,
  type ToolConfigOptions,
  applyClaudeSettings,
  applyCodexConfig,
  discoverClaudeSeats,
  discoverCodexProfiles,
  generateClaudeCodeSettings,
  generateCodexConfigToml,
} from "../../collector-config/src/index";

/**
 * Self-healing reconcile of the managed Claude and Codex config (bead
 * eco-6hoxj.50).
 *
 * `setup --yes` is the only thing that ever applied the managed telemetry
 * block, but the fleet's seat and conductor tooling rewrites
 * ~/.claude-seats/<slug>/settings.json and ~/.codex-profiles/<slug>/config.toml
 * (and can rewrite ~/.claude/settings.json and ~/.codex/config.toml) whenever a
 * seat or profile churns. The rewritten file silently loses the managed block:
 * doctor reports `claude_seat_settings_unmanaged` / `codex_profile_config_unmanaged`
 * and that lane emits transcript/rollout rows only until someone re-runs setup.
 *
 * This module owns the reconcile itself so exactly one implementation serves
 * both callers: `plimsoll setup --reconcile` and the collector daemon's
 * maintenance loop. It is deliberately weaker than setup:
 *
 *   - it never provisions. A target whose file does not exist is skipped, never
 *     created, because the seat/conductor tooling owns that file's existence;
 *   - it never rewrites a file it cannot read or parse. That is fleet state, so
 *     it is reported and then backed off for an hour rather than retried every
 *     cadence;
 *   - it writes only where the dry run plans `added` or `updated`. An
 *     all-`unchanged` target is not opened for writing at all, so a healthy host
 *     does zero writes;
 *   - it never fights a concurrent writer. The file identity is sampled before
 *     the plan and re-checked immediately before the apply; a file the seat
 *     tooling touched inside that window is skipped with a reason and picked up
 *     on the next run. The apply itself is setup's own transactional,
 *     backup-first, no-follow path.
 */

/** The managed-config families this reconcile owns. Gemini and Grok are out of scope. */
export type ManagedConfigFamily = "claude" | "codex";

export type ManagedConfigTargetName =
  | "claude"
  | `claudeSeat[${string}]`
  | "codex"
  | `codexProfile[${string}]`;

export type ManagedConfigTarget = {
  name: ManagedConfigTargetName;
  path: string;
  family: ManagedConfigFamily;
  /** True for a target found on disk rather than declared by Plimsoll. */
  discovered?: true;
  run: (options: ToolConfigOptions, dryRun: boolean) => ApplyResult;
};

export type ManagedConfigTargetOptions = {
  /**
   * Also compose a discovered seat/profile directory whose config file does
   * not exist (review r1, F7). Reconcile then reports it as `skipped: absent`
   * instead of leaving it out of the report entirely, so an operator reading a
   * receipt can tell "this seat has no settings.json yet" from "this seat does
   * not exist". It is still never provisioned: the absent target is skipped at
   * the file witness, before anything can write.
   *
   * `setup` never passes this. Its target list is the set of files it may
   * write, and composing an absent seat there would make the installer create
   * a file the seat tooling owns.
   */
  includeAbsent?: boolean;
};

/** Identity of the file as the plan saw it; a concurrent writer changes it. */
export type FileWitness = { mtimeMs: number; size: number; inode: number; device: number };

function witness(file: string): FileWitness | null {
  try {
    const stat = fs.lstatSync(file);
    if (!stat.isFile()) return null;
    return { mtimeMs: stat.mtimeMs, size: stat.size, inode: stat.ino, device: stat.dev };
  } catch {
    return null;
  }
}

function sameWitness(left: FileWitness, right: FileWitness) {
  return (
    left.mtimeMs === right.mtimeMs &&
    left.size === right.size &&
    left.inode === right.inode &&
    left.device === right.device
  );
}

function readWitness(value: unknown): FileWitness | null {
  if (!value || typeof value !== "object") return null;
  const record = value as Record<string, unknown>;
  const numbers = ["mtimeMs", "size", "inode", "device"] as const;
  if (!numbers.every((key) => typeof record[key] === "number" && Number.isFinite(record[key]))) {
    return null;
  }
  return {
    mtimeMs: record.mtimeMs as number,
    size: record.size as number,
    inode: record.inode as number,
    device: record.device as number,
  };
}

/**
 * The `claude` target plus every discovered fleet seat, in setup's order.
 * A seat directory without settings.json is skipped rather than created, so the
 * composed list is exactly what setup composes today.
 */
export function composeManagedClaudeTargets(
  claudeFile: string,
  home: string,
  options: ManagedConfigTargetOptions = {},
): ManagedConfigTarget[] {
  return [
    {
      name: "claude",
      path: claudeFile,
      family: "claude",
      run: (toolOptions, preview) =>
        applyClaudeSettings(claudeFile, generateClaudeCodeSettings(toolOptions), { dryRun: preview }),
    },
    ...discoverClaudeSeats(home)
      .filter((seat) => seat.hasSettings || options.includeAbsent === true)
      .map((seat): ManagedConfigTarget => ({
        name: `claudeSeat[${seat.slug}]`,
        path: seat.path,
        family: "claude",
        discovered: true,
        run: (toolOptions, preview) =>
          applyClaudeSettings(seat.path, generateClaudeCodeSettings(toolOptions), {
            dryRun: preview,
            managedTarget: `claudeSeat[${seat.slug}]`,
          }),
      })),
  ];
}

/** The `codex` target plus every discovered fleet seat profile, in setup's order. */
export function composeManagedCodexTargets(
  codexFile: string,
  home: string,
  options: ManagedConfigTargetOptions = {},
): ManagedConfigTarget[] {
  return [
    {
      name: "codex",
      path: codexFile,
      family: "codex",
      run: (toolOptions, preview) =>
        applyCodexConfig(codexFile, generateCodexConfigToml(toolOptions), { dryRun: preview }),
    },
    ...discoverCodexProfiles(home)
      .filter((profile) => profile.hasConfig || options.includeAbsent === true)
      .map((profile): ManagedConfigTarget => ({
        name: `codexProfile[${profile.slug}]`,
        path: profile.path,
        family: "codex",
        discovered: true,
        run: (toolOptions, preview) =>
          applyCodexConfig(profile.path, generateCodexConfigToml(toolOptions), {
            dryRun: preview,
            managedTarget: `codexProfile[${profile.slug}]`,
          }),
      })),
  ];
}

/** Every managed Claude and Codex target, home targets first, in setup's order. */
export function composeManagedConfigTargets(
  claudeFile: string,
  codexFile: string,
  home: string,
  options: ManagedConfigTargetOptions = {},
): ManagedConfigTarget[] {
  return [
    ...composeManagedClaudeTargets(claudeFile, home, options),
    ...composeManagedCodexTargets(codexFile, home, options),
  ];
}

/** The readback doctor already performs for one managed target. */
export type ManagedConfigReadback = {
  ok: boolean;
  status: "valid" | "incomplete" | "missing" | "invalid";
  missing: string[];
};

export type ManagedConfigDriftEntry = {
  name: ManagedConfigTargetName;
  status: ManagedConfigReadback["status"];
  /** True only for a readable, parseable file whose managed block is missing or stale. */
  drifted: boolean;
};

export type ManagedConfigDriftReport = {
  drifted: number;
  unreadable: number;
  targets: ManagedConfigDriftEntry[];
};

/**
 * Doctor's own readback, counted.
 *
 * Only `incomplete` counts as drift: a readable file whose managed keys are
 * missing or stale is exactly what a reconcile can heal. `missing` and
 * `invalid` are reported but never counted, because reconcile refuses to
 * provision or rewrite them — counting them would make a host with one
 * malformed fleet profile trigger a run every cadence forever and never change
 * anything.
 */
export function managedConfigDriftReport(
  targets: ManagedConfigTarget[],
  options: ToolConfigOptions,
  read: (target: ManagedConfigTarget, options: ToolConfigOptions) => ManagedConfigReadback,
): ManagedConfigDriftReport {
  const entries = targets.map((target): ManagedConfigDriftEntry => {
    const readback = read(target, options);
    return { name: target.name, status: readback.status, drifted: readback.status === "incomplete" };
  });
  return {
    drifted: entries.filter((entry) => entry.drifted).length,
    unreadable: entries.filter((entry) => entry.status === "invalid").length,
    targets: entries,
  };
}

/**
 * The same readback, yielding to the event loop between targets (review r1,
 * F6). The daemon's drift readback runs on every due tick on a healthy host,
 * so it is the one path that must never hold the collector's HTTP loop for a
 * whole fleet of seats and profiles at once.
 */
export async function managedConfigDriftReportAsync(
  targets: ManagedConfigTarget[],
  options: ToolConfigOptions,
  read: (target: ManagedConfigTarget, options: ToolConfigOptions) => ManagedConfigReadback,
): Promise<ManagedConfigDriftReport> {
  const entries: ManagedConfigDriftEntry[] = [];
  for (const target of targets) {
    await yieldToEventLoop();
    const readback = read(target, options);
    entries.push({
      name: target.name,
      status: readback.status,
      drifted: readback.status === "incomplete",
    });
  }
  return {
    drifted: entries.filter((entry) => entry.drifted).length,
    unreadable: entries.filter((entry) => entry.status === "invalid").length,
    targets: entries,
  };
}

export type ManagedConfigReconcileTargetStatus =
  | "applied"
  | "unchanged"
  | "skipped"
  | "refused";

export type ManagedConfigReconcileSkipReason =
  | "absent"
  | "backoff"
  | "changed_during_plan"
  | "changed_during_apply";

export type ManagedConfigReconcileTargetReport = {
  name: ManagedConfigTargetName;
  path: string;
  discovered: boolean;
  status: ManagedConfigReconcileTargetStatus;
  /** Secret-free plan for the managed keys this target owns; absent when not planned. */
  plan?: ApplyPlanEntry[];
  reason?: string;
  backup?: string | null;
  /** Set when a refusal armed the per-file backoff. */
  nextEligibleAt?: string;
};

export type ManagedConfigReconcileResult = {
  status: "managed_config_reconciled" | "managed_config_unchanged";
  startedAt: string;
  durationMs: number;
  applied: number;
  unchanged: number;
  skipped: number;
  refused: number;
  /** Seat/profile directories that exist with no config file yet (review r1, F7). */
  absent: number;
  /** True when a target Plimsoll declares (not a discovered seat/profile) refused. */
  ownedRefusal: boolean;
  targets: ManagedConfigReconcileTargetReport[];
  receiptPath: string | null;
};

/** A refused or unparseable file is not retried more often than this. */
export const MANAGED_CONFIG_REFUSAL_BACKOFF_MS = 60 * 60 * 1000;

export const DEFAULT_MANAGED_CONFIG_RECONCILE_INTERVAL_SECONDS = 600;

/**
 * Litter bounds (review r1, F5). Backups are written *beside* the managed file,
 * i.e. inside the fleet-owned ~/.claude-seats/<slug>/ and
 * ~/.codex-profiles/<slug>/ directories, and receipts land in the collector
 * home; nothing pruned them, so a writer that drops the managed block on a
 * schedule shorter than the cadence littered both directories without bound.
 *
 * Only the reconcile prunes, and only the backups it wrote itself (review r2,
 * R2). `setup --yes` writes into the same `<basename>.plimsoll-backup-*`
 * namespace, so matching the namespace was not enough: the cadence could delete
 * the installer's pre-install backup — the only copy of a host's pre-Plimsoll
 * bytes. Two rules keep that safe:
 *
 *   - the reconcile prunes only the backup paths it recorded in its own state
 *     file when it wrote them, so a backup `setup --yes` (or anything else)
 *     wrote is never a candidate;
 *   - the oldest backup of a managed file is never deleted, whoever wrote it.
 *     A state file lost or hand-cleared cannot turn the pre-Plimsoll copy into
 *     a pruning candidate.
 *
 * `setup --yes` therefore keeps its existing backup policy (it has never
 * pruned) and its rollback material is untouched by this change.
 */
export const MANAGED_CONFIG_BACKUPS_KEPT_PER_FILE = 5;
export const MANAGED_CONFIG_RECEIPTS_KEPT = 20;
/** A backup this young is never pruned, whatever the count: it is live rollback material. */
export const MANAGED_CONFIG_BACKUP_MIN_AGE_MS = 24 * 60 * 60 * 1000;

export type ManagedConfigReconcilePruneOptions = {
  backupsPerFile?: number;
  receipts?: number;
  minBackupAgeMs?: number;
};

/**
 * What the last run did, for doctor (review r1, F2).
 *
 * `unavailable` is the tick that could not manage anything at all because the
 * host has no Plimsoll-local credentials yet, so its target list is empty
 * (review r2, R6). Without it that host reads exactly like a healthy one.
 */
export type ManagedConfigReconcileLastResult =
  | "unchanged"
  | "applied"
  | "refused"
  | "skipped"
  | "unavailable";

/** Per-target refusal backoff, with the file identity the refusal was about. */
type ManagedConfigBackoffEntry = {
  until: string;
  /** The refused file's identity; a different identity means the file was fixed. */
  witness: FileWitness | null;
};

/**
 * The backups this cadence wrote beside one managed file, newest last
 * (review r2, R2). Only these are pruning candidates.
 */
type ManagedConfigBackupRecord = { file: string; names: string[] };

type ManagedConfigReconcileState = {
  version: 1;
  lastRunAt: string | null;
  lastResult: ManagedConfigReconcileLastResult | null;
  lastApplied: number;
  lastRefused: number;
  lastAbsent: number;
  backoff: Record<string, ManagedConfigBackoffEntry>;
  /** Per target: the backup files this reconcile created and may prune. */
  backups: Record<string, ManagedConfigBackupRecord>;
};

const EMPTY_STATE: ManagedConfigReconcileState = {
  version: 1,
  lastRunAt: null,
  lastResult: null,
  lastApplied: 0,
  lastRefused: 0,
  lastAbsent: 0,
  backoff: {},
  backups: {},
};

const LAST_RESULTS: readonly ManagedConfigReconcileLastResult[] = [
  "unchanged",
  "applied",
  "refused",
  "skipped",
  "unavailable",
];

export function managedConfigReconcileStatePath(collectorHome: string) {
  return path.join(collectorHome, "managed-config-reconcile-state.json");
}

/** Last-run state, or the empty state when nothing has run or the file is unusable. */
export function readManagedConfigReconcileState(collectorHome: string): ManagedConfigReconcileState {
  try {
    const parsed = JSON.parse(
      fs.readFileSync(managedConfigReconcileStatePath(collectorHome), "utf8"),
    ) as Partial<ManagedConfigReconcileState>;
    if (!parsed || typeof parsed !== "object") return { ...EMPTY_STATE };
    const backoff: Record<string, ManagedConfigBackoffEntry> = {};
    for (const [name, entry] of Object.entries(parsed.backoff ?? {})) {
      // A state file written before review r1 stored the deadline alone; it is
      // read as a backoff with no witness, which simply never self-heals early.
      const until = typeof entry === "string" ? entry : (entry as ManagedConfigBackoffEntry)?.until;
      if (typeof until !== "string" || !Number.isFinite(Date.parse(until))) continue;
      backoff[name] = {
        until,
        witness: typeof entry === "string" ? null : readWitness((entry as ManagedConfigBackoffEntry)?.witness),
      };
    }
    const backups: Record<string, ManagedConfigBackupRecord> = {};
    for (const [name, entry] of Object.entries(parsed.backups ?? {})) {
      const record = entry as Partial<ManagedConfigBackupRecord> | null;
      if (!record || typeof record.file !== "string" || !Array.isArray(record.names)) continue;
      const names = record.names.filter((value): value is string => typeof value === "string");
      backups[name] = { file: record.file, names };
    }
    const lastResult = parsed.lastResult;
    return {
      version: 1,
      lastRunAt: typeof parsed.lastRunAt === "string" ? parsed.lastRunAt : null,
      lastResult:
        typeof lastResult === "string" && LAST_RESULTS.includes(lastResult as ManagedConfigReconcileLastResult)
          ? (lastResult as ManagedConfigReconcileLastResult)
          : null,
      lastApplied: Number.isSafeInteger(parsed.lastApplied) ? (parsed.lastApplied as number) : 0,
      lastRefused: Number.isSafeInteger(parsed.lastRefused) ? (parsed.lastRefused as number) : 0,
      lastAbsent: Number.isSafeInteger(parsed.lastAbsent) ? (parsed.lastAbsent as number) : 0,
      backoff,
      backups,
    };
  } catch {
    // No state yet, or state this build cannot read: the reconcile is
    // idempotent, so the worst case is one extra eligible run.
    return { ...EMPTY_STATE };
  }
}

function writeManagedConfigReconcileState(
  collectorHome: string,
  state: ManagedConfigReconcileState,
) {
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  const file = managedConfigReconcileStatePath(collectorHome);
  const temporary = `${file}.tmp`;
  fs.writeFileSync(temporary, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
  fs.renameSync(temporary, file);
}

/**
 * Read-modify-write the state file under a cross-process lock (review r2, R4).
 *
 * The daemon cadence and an operator's `plimsoll setup --reconcile` are two
 * processes over the same collector home. Both snapshot the state, work, and
 * write the whole file back, so without a lock whichever finished last silently
 * dropped the other's backoff entries, backup record and run stamp — a file
 * that had just been refused would be re-planned on the very next tick.
 *
 * This is the same SQLite mutation lock the collector config file uses, held on
 * a sibling lock file beside the state file. The critical section is the
 * read-modify-write itself (and the prune it authorises), never a whole
 * reconcile run: an operator must not be able to block the cadence, or the
 * cadence the operator, for the length of a fleet-scale apply.
 */
function updateManagedConfigReconcileState<T>(
  collectorHome: string,
  update: (current: ManagedConfigReconcileState) => { next: ManagedConfigReconcileState; result: T },
): T {
  fs.mkdirSync(collectorHome, { recursive: true, mode: 0o700 });
  return withCollectorConfigMutationLock(managedConfigReconcileStatePath(collectorHome), () => {
    const { next, result } = update(readManagedConfigReconcileState(collectorHome));
    writeManagedConfigReconcileState(collectorHome, next);
    return result;
  });
}

/** Whether `candidate` is at least as new as the stamp already on disk. */
function stampIsNotOlder(candidate: string, recorded: string | null): boolean {
  if (recorded === null) return true;
  const left = Date.parse(candidate);
  const right = Date.parse(recorded);
  if (!Number.isFinite(left) || !Number.isFinite(right)) return true;
  return left >= right;
}

/**
 * Stamp a cadence tick that decided not to reconcile (review r1, F2).
 *
 * The stamp is what lets an operator tell "the cadence ran and found nothing"
 * from "the cadence is wired wrong, crashed on its first tick, or was never
 * registered". It is written only for a tick that actually performed the drift
 * readback — never for `disabled` or `interval_not_elapsed`, because advancing
 * `lastRunAt` on a tick that read nothing would push `nextEligibleAt` forward
 * forever and make the interval gate inert.
 *
 * The backoff map is preserved: a no-drift tick is not a reason to retry a file
 * Plimsoll already refused.
 */
export function stampManagedConfigReconcileRun(
  collectorHome: string,
  stamp: {
    at: string;
    result: ManagedConfigReconcileLastResult;
    applied?: number;
    refused?: number;
    absent?: number;
  },
) {
  updateManagedConfigReconcileState(collectorHome, (state) => ({
    // A run that started earlier but finished later must not walk `lastRunAt`
    // backwards and hand the cadence a free early tick (review r2, R4).
    next: stampIsNotOlder(stamp.at, state.lastRunAt)
      ? {
          ...state,
          lastRunAt: stamp.at,
          lastResult: stamp.result,
          lastApplied: stamp.applied ?? 0,
          lastRefused: stamp.refused ?? 0,
          lastAbsent: stamp.absent ?? 0,
        }
      : state,
    result: undefined,
  }));
}

/**
 * Record a cadence tick that decided *not* to reconcile (review r1, F2).
 *
 * Exactly one decision stamps: `no_drift`, the tick that ran its readback over
 * every managed target and found nothing to heal. That is the state a healthy
 * fleet host sits in forever, and without the stamp doctor cannot tell it from
 * a cadence that is wired wrong, crashed on its first tick, or was never
 * registered. `disabled` and `interval_not_elapsed` deliberately do not stamp:
 * they read nothing, and advancing `lastRunAt` on them would push
 * `nextEligibleAt` forward forever and make the interval gate inert.
 *
 * `result` lets the caller say *why* there was nothing to do: a host with no
 * Plimsoll-local credentials manages no targets at all and stamps
 * `unavailable`, so doctor no longer reads exactly like a healthy host
 * (review r2, R6).
 *
 * Returns whether it stamped.
 */
export function stampManagedConfigReconcileDecision(
  collectorHome: string,
  decision: ManagedConfigReconcileDecision,
  stamp: { at: string; absent?: number; result?: "unchanged" | "unavailable" },
): boolean {
  if (decision.run || decision.reason !== "no_drift") return false;
  stampManagedConfigReconcileRun(collectorHome, {
    at: stamp.at,
    result: stamp.result ?? "unchanged",
    absent: stamp.absent,
  });
  return true;
}

/**
 * The doctor section. Key names and bounded counts only — never a managed value
 * and never a path.
 */
export function managedConfigReconcileDoctorSection(
  collectorHome: string,
  options: { enabled: boolean; intervalSeconds: number },
) {
  const state = readManagedConfigReconcileState(collectorHome);
  const lastRunAt = state.lastRunAt;
  const lastRunMs = lastRunAt ? Date.parse(lastRunAt) : Number.NaN;
  return {
    enabled: options.enabled,
    intervalSeconds: options.intervalSeconds,
    lastRunAt,
    lastResult: state.lastResult,
    lastApplied: state.lastApplied,
    lastRefused: state.lastRefused,
    lastAbsent: state.lastAbsent,
    nextEligibleAt: Number.isFinite(lastRunMs)
      ? new Date(lastRunMs + options.intervalSeconds * 1000).toISOString()
      : null,
  };
}

export type ManagedConfigReconcileSettings = {
  enabled: boolean;
  intervalSeconds: number;
  /** Which source answered: the live config file, or the settings captured at boot. */
  source: "config_file" | "boot_config";
};

/**
 * The cadence's live settings, re-read from the collector config file
 * (review r1, F3).
 *
 * The kill-switch has to take effect without a daemon restart: an operator who
 * sets `managedConfig.reconcile.enabled: false` to stop the collector writing
 * to seat files during a fleet seat migration sees doctor report `false`
 * immediately — doctor reads the file fresh — and the running collector must
 * agree rather than keep reconciling until someone restarts it. The interval is
 * re-read with it, so a shortened or lengthened cadence also takes effect.
 *
 * A missing or unparseable config file falls back to the settings captured at
 * boot, so a half-written config never silently disarms (or arms) the cadence.
 * This reads Plimsoll's own config, never a managed file: a disabled cadence
 * still performs no managed-config read or write of its own.
 */
export function readManagedConfigReconcileSettings(
  configFile: string,
  fallback: { enabled: boolean; intervalSeconds: number },
): ManagedConfigReconcileSettings {
  try {
    const parsed = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configFile, "utf8")));
    return {
      enabled: parsed.managedConfig.reconcile.enabled,
      intervalSeconds: parsed.managedConfig.reconcile.intervalSeconds,
      source: "config_file",
    };
  } catch {
    return {
      enabled: fallback.enabled,
      intervalSeconds: fallback.intervalSeconds,
      source: "boot_config",
    };
  }
}

export type ManagedConfigReconcileDecision = {
  run: boolean;
  reason: "disabled" | "interval_not_elapsed" | "no_drift" | "drift";
  nextEligibleAt: string | null;
};

type ManagedConfigReconcileGateInput = {
  enabled: boolean;
  intervalSeconds: number;
  now: number;
  lastRunAt: number | null;
};

/**
 * Everything the decision settles before it is allowed to read a managed file.
 *
 * Shared by the synchronous and the yielding decision so both honour the same
 * order: the kill-switch is checked before anything reads the clock, and the
 * interval is checked before `drift()` is called.
 */
function managedConfigReconcileGate(
  input: ManagedConfigReconcileGateInput,
): { decided: ManagedConfigReconcileDecision } | { nextEligibleAt: string } {
  if (!input.enabled) {
    return { decided: { run: false, reason: "disabled", nextEligibleAt: null } };
  }
  const intervalMs = Math.max(1, input.intervalSeconds) * 1000;
  const nextEligible = input.lastRunAt === null ? input.now : input.lastRunAt + intervalMs;
  const nextEligibleAt = new Date(nextEligible).toISOString();
  if (nextEligible > input.now) {
    return { decided: { run: false, reason: "interval_not_elapsed", nextEligibleAt } };
  }
  return { nextEligibleAt };
}

/**
 * The maintenance loop's decision, as a pure function so both states can be
 * pinned without waiting out a cadence.
 *
 * Order matters and is the point: the kill-switch is checked before anything
 * reads the clock, and the interval is checked before `drift()` is called, so a
 * disabled or not-yet-due collector performs no managed-config filesystem reads
 * at all.
 */
export function decideManagedConfigReconcile(
  input: ManagedConfigReconcileGateInput & { drift: () => number },
): ManagedConfigReconcileDecision {
  const gate = managedConfigReconcileGate(input);
  if ("decided" in gate) return gate.decided;
  return input.drift() > 0
    ? { run: true, reason: "drift", nextEligibleAt: gate.nextEligibleAt }
    : { run: false, reason: "no_drift", nextEligibleAt: gate.nextEligibleAt };
}

/**
 * The daemon's decision. Identical to `decideManagedConfigReconcile` except
 * that the drift readback may yield to the event loop between targets, so the
 * collector's HTTP loop is never held for a whole fleet-scale readback
 * (review r1, F6).
 */
export async function decideManagedConfigReconcileAsync(
  input: ManagedConfigReconcileGateInput & { drift: () => Promise<number> },
): Promise<ManagedConfigReconcileDecision> {
  const gate = managedConfigReconcileGate(input);
  if ("decided" in gate) return gate.decided;
  return (await input.drift()) > 0
    ? { run: true, reason: "drift", nextEligibleAt: gate.nextEligibleAt }
    : { run: false, reason: "no_drift", nextEligibleAt: gate.nextEligibleAt };
}

/** Hand the event loop back so one tick never becomes one long synchronous chunk. */
export function yieldToEventLoop(): Promise<void> {
  return new Promise((resolve) => {
    setImmediate(resolve);
  });
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

/**
 * A transactional apply that lost a race with another writer (review r1, F4).
 *
 * `writeClaudePlan` / the Codex commit re-verify the bound descriptor and the
 * visible content against the preimage at every step and fail closed when
 * another writer lands inside the apply. That is a concurrency outcome, not a
 * malformed file: it deserves the same response as `changed_during_plan` — skip
 * this tick, arm no backoff, re-plan on the next one. Arming the one-hour
 * refusal backoff instead would silence a perfectly healthy lane for an hour
 * because the conductor happened to rewrite its profile inside a ~20 ms window.
 *
 * Claude-family failures carry a `<FAMILY>_CONFIG_<CODE>` code; the Codex
 * family throws prose from `unsafePath`. Both are matched here, and both are
 * matched conservatively: anything that is not recognisably a lost race stays a
 * refusal with its backoff.
 */
const CONCURRENCY_FAILURE_CODES = new Set([
  "COMMIT_CLAIM_COLLISION",
  "COMMIT_CLAIM_MISMATCH",
  "COMMIT_CLAIM_MISSING",
  "COMMIT_CLAIM_RESTORE_FAILED",
  "BOUND_IDENTITY_CHANGED",
  "BOUND_CONTENT_CHANGED",
  "LEAF_CHANGED",
  "PATH_CHANGED",
  "ANCESTOR_CHANGED",
  "PARENT_CREATE_RACE",
  "VISIBLE_IDENTITY_MISMATCH",
  "VISIBLE_POSTCONDITION_CHANGED",
  "PREPARED_LINK_COUNT",
  "BACKUP_LINK_COUNT",
]);

const CONCURRENCY_FAILURE_PROSE = [
  "identity changed after planning",
  "ancestor changed after planning",
  "was replaced after planning",
  "was replaced while opening it",
  "identity changed before commit",
  "content changed before commit",
  "changed while verifying the committed plan",
  "does not match the committed plan",
  "preimage could not be bound before commit",
  "an ancestor appeared while inspecting",
];

export function isManagedConfigConcurrencyFailure(reason: string): boolean {
  const code = /(?:^|\s)[A-Z][A-Z0-9]*_CONFIG_([A-Z0-9_]+)/.exec(reason)?.[1];
  if (code && CONCURRENCY_FAILURE_CODES.has(code)) return true;
  return CONCURRENCY_FAILURE_PROSE.some((phrase) => reason.includes(phrase));
}

export type ManagedConfigReconcileOptions = {
  collectorHome: string;
  targets: ManagedConfigTarget[];
  toolOptions: ToolConfigOptions;
  /** Plan only: report what would be applied and write nothing at all. */
  dryRun?: boolean;
  now?: () => number;
  /**
   * Monotonic milliseconds, for `durationMs` only (review r2, R3). The daemon
   * passes a *fixed* `now` so every deadline in one tick is computed from one
   * instant; reading the duration from that clock made every receipt the daemon
   * wrote say `durationMs: 0`.
   */
  monotonicNow?: () => number;
  /** Deterministic proof seam: runs after a target's plan, before its apply. */
  onPlanned?: (target: ManagedConfigTarget) => void;
  /** Litter bounds; defaults are the exported constants. */
  prune?: ManagedConfigReconcilePruneOptions;
};

type ReconcileRun = {
  startedAtMs: number;
  /** Monotonic reading taken at the same instant; only `durationMs` uses it. */
  startedAtMonotonicMs: number;
  startedAt: string;
  backoff: Record<string, ManagedConfigBackoffEntry>;
  /**
   * Targets whose backoff this run decided about, armed or released. Only these
   * are merged over the state on disk, so a concurrent run's decisions about
   * *other* targets survive (review r2, R4).
   */
  backoffDecided: Set<string>;
  /** Backups this run wrote, by target: `{file, name}` per applied target. */
  createdBackups: { name: string; file: string; backup: string }[];
  reports: ManagedConfigReconcileTargetReport[];
};

function beginReconcile(options: ManagedConfigReconcileOptions): ReconcileRun {
  const now = options.now ?? (() => Date.now());
  const monotonicNow = options.monotonicNow ?? (() => performance.now());
  const startedAtMs = now();
  return {
    startedAtMs,
    startedAtMonotonicMs: monotonicNow(),
    startedAt: new Date(startedAtMs).toISOString(),
    backoff: { ...readManagedConfigReconcileState(options.collectorHome).backoff },
    backoffDecided: new Set<string>(),
    createdBackups: [],
    reports: [],
  };
}

/**
 * One target: backoff gate, witness, plan, re-witness, apply.
 *
 * Synchronous by construction — this is the unit of work the async driver
 * yields *between*, so the longest chunk the collector's event loop ever owes
 * this cadence is one target, not a whole fleet of them.
 */
function reconcileOneTarget(
  run: ReconcileRun,
  target: ManagedConfigTarget,
  options: ManagedConfigReconcileOptions,
) {
  const base = {
    name: target.name,
    path: target.path,
    discovered: target.discovered === true,
  };
  const held = run.backoff[target.name];
  const heldUntil = held ? Date.parse(held.until) : Number.NaN;
  if (held && Number.isFinite(heldUntil) && heldUntil > run.startedAtMs) {
    const current = witness(target.path);
    // A backoff is about a *file*, not a name. If the malformed file the
    // refusal was about has since been replaced or edited, the fleet fixed it
    // and the next tick should heal it rather than wait out the hour
    // (review r1, F4). A backoff carrying no witness (pre-r2 state file) keeps
    // the old behaviour and simply waits.
    const recorded = held.witness;
    const healed = recorded !== null && (current === null || !sameWitness(recorded, current));
    if (!healed) {
      // A file Plimsoll already refused is fleet state; retrying it every
      // cadence would only reprint the same refusal.
      run.reports.push({
        ...base,
        status: "skipped",
        reason: "backoff" satisfies ManagedConfigReconcileSkipReason,
        nextEligibleAt: held.until,
      });
      return;
    }
    delete run.backoff[target.name];
    run.backoffDecided.add(target.name);
  }
  const before = witness(target.path);
  if (before === null) {
    // Reconcile never provisions: the seat/conductor tooling owns whether the
    // file exists at all, and `setup --yes` owns first installation.
    run.reports.push({
      ...base,
      status: "skipped",
      reason: "absent" satisfies ManagedConfigReconcileSkipReason,
    });
    return;
  }
  const refuse = (reason: string, plan?: ApplyPlanEntry[]) => {
    const until = new Date(run.startedAtMs + MANAGED_CONFIG_REFUSAL_BACKOFF_MS).toISOString();
    run.backoff[target.name] = { until, witness: witness(target.path) };
    run.backoffDecided.add(target.name);
    run.reports.push({ ...base, status: "refused", ...(plan ? { plan } : {}), reason, nextEligibleAt: until });
  };
  const lostTheRace = (reason: ManagedConfigReconcileSkipReason, entries?: ApplyPlanEntry[]) => {
    // Another writer landed inside this run's own window. That is not a file
    // Plimsoll cannot manage, so it is skipped for this tick and arms no
    // backoff: the next tick plans the file as the other writer left it
    // (review r1, F4 for the apply window; review r2, R1 for the plan window).
    run.reports.push({
      ...base,
      status: "skipped",
      ...(entries ? { plan: entries } : {}),
      reason,
    });
  };
  let plan: ApplyResult;
  try {
    plan = target.run(options.toolOptions, true);
  } catch (error) {
    // The plan is a real read of the managed file: `applyCodexConfig` binds the
    // preimage before its dry-run early return, so a writer that replaces the
    // file inside that read window throws the same transactional concurrency
    // failure the apply throws. Classify it the way the apply path does, or a
    // lost race one window earlier silently arms the hour (review r2, R1).
    const reason = errorMessage(error);
    if (isManagedConfigConcurrencyFailure(reason)) {
      lostTheRace("changed_during_plan");
      return;
    }
    refuse(reason);
    return;
  }
  if (plan.conflict) {
    if (isManagedConfigConcurrencyFailure(plan.conflict)) {
      lostTheRace("changed_during_plan", plan.plan);
      return;
    }
    refuse(plan.conflict, plan.plan);
    return;
  }
  delete run.backoff[target.name];
  run.backoffDecided.add(target.name);
  if (!plan.changed) {
    run.reports.push({ ...base, status: "unchanged", plan: plan.plan });
    return;
  }
  if (options.dryRun) {
    run.reports.push({ ...base, status: "applied", plan: plan.plan, backup: null, reason: "dry_run" });
    return;
  }
  options.onPlanned?.(target);
  const after = witness(target.path);
  if (after === null || !sameWitness(before, after)) {
    // Another writer owns this file right now. The plan was computed against
    // content that no longer exists, so this run stands down; the next one
    // plans the file as it is.
    lostTheRace("changed_during_plan", plan.plan);
    return;
  }
  try {
    const applied = target.run(options.toolOptions, false);
    if (applied.conflict) {
      if (isManagedConfigConcurrencyFailure(applied.conflict)) {
        lostTheRace("changed_during_apply", applied.plan ?? plan.plan);
        return;
      }
      refuse(applied.conflict, applied.plan ?? plan.plan);
      return;
    }
    if (applied.backupPath) {
      // Recorded so the prune can tell this cadence's backups from
      // `setup --yes`'s pre-install copy (review r2, R2).
      run.createdBackups.push({ name: target.name, file: target.path, backup: applied.backupPath });
    }
    run.reports.push({
      ...base,
      status: "applied",
      plan: applied.plan ?? plan.plan,
      backup: applied.backupPath ?? null,
    });
  } catch (error) {
    const reason = errorMessage(error);
    if (isManagedConfigConcurrencyFailure(reason)) {
      lostTheRace("changed_during_apply", plan.plan);
      return;
    }
    refuse(reason, plan.plan);
  }
}

/**
 * What the last run did, for doctor.
 *
 * A refusal is the thing an operator must see, so it wins over an apply that
 * happened in the same run; `skipped` covers the concurrency and backoff skips
 * that mean "this run stood down on a file". An `absent` skip is not one of
 * those — a seat directory with no config file yet is reported in `lastAbsent`
 * and must not make a perfectly clean cadence read as `skipped`.
 */
function lastResultOf(result: {
  refused: number;
  applied: number;
  skipped: number;
}): ManagedConfigReconcileLastResult {
  if (result.refused > 0) return "refused";
  if (result.applied > 0) return "applied";
  if (result.skipped > 0) return "skipped";
  return "unchanged";
}

/**
 * Keep at most `keep` of the backups *this cadence wrote* beside one managed
 * file, oldest first, never deleting one younger than `minAgeMs` (review r1,
 * F5) and never deleting the oldest backup the file has (review r2, R2).
 *
 * `created` is the cadence's own record from the state file. A backup written
 * by `setup --yes` — the copy of the host's pre-Plimsoll bytes — is not in it
 * and is therefore never a candidate; the oldest-backup rule holds that copy
 * even if the record is lost. The backup name carries an ISO stamp, but the
 * file's own mtime is what the age rule reads: a backup Plimsoll wrote a
 * minute ago is live rollback material whatever the count says.
 *
 * Returns the record to keep: the cadence's own backups that still exist.
 */
function pruneBackups(
  file: string,
  created: readonly string[],
  keep: number,
  minAgeMs: number,
  nowMs: number,
): { survivors: string[]; removed: string[] } {
  const directory = path.dirname(file);
  const prefix = `${path.basename(file)}.plimsoll-backup-`;
  let entries: string[];
  try {
    entries = fs.readdirSync(directory).filter((name) => name.startsWith(prefix));
  } catch {
    return { survivors: [...created], removed: [] };
  }
  const onDisk = entries
    .map((name) => {
      const full = path.join(directory, name);
      try {
        const stat = fs.lstatSync(full);
        return stat.isFile() ? { full, mtimeMs: stat.mtimeMs, name } : null;
      } catch {
        return null;
      }
    })
    .filter((entry): entry is { full: string; mtimeMs: number; name: string } => entry !== null)
    // Newest first: everything past `keep` is a pruning candidate.
    .sort((left, right) => right.mtimeMs - left.mtimeMs || right.name.localeCompare(left.name));
  // Whoever wrote it, the file's oldest backup is its pre-Plimsoll bytes as far
  // as this host can tell. It is never a candidate.
  const oldest = onDisk[onDisk.length - 1]?.name;
  const mine = new Set(created);
  const ours = onDisk.filter((entry) => mine.has(entry.name));
  const removed: string[] = [];
  for (const entry of ours.slice(Math.max(0, keep))) {
    if (entry.name === oldest) continue;
    if (nowMs - entry.mtimeMs < minAgeMs) continue;
    try {
      fs.unlinkSync(entry.full);
      removed.push(entry.full);
    } catch {
      // Another writer owns it now; the next run re-counts.
    }
  }
  const gone = new Set(removed.map((full) => path.basename(full)));
  // Oldest last, matching the on-disk order, and dropping anything that is no
  // longer there so the record cannot grow past the litter it tracks.
  return {
    survivors: ours.filter((entry) => !gone.has(entry.name)).map((entry) => entry.name).reverse(),
    removed,
  };
}

/** Keep at most `keep` reconcile receipts, oldest first. Only this cadence's own receipts. */
function pruneReceipts(receiptsDirectory: string, keep: number): string[] {
  let entries: string[];
  try {
    entries = fs
      .readdirSync(receiptsDirectory)
      .filter((name) => name.startsWith("managed-config-reconcile-") && name.endsWith(".json"));
  } catch {
    return [];
  }
  // The name carries the run's ISO stamp with `:`/`.` folded to `-`, so a plain
  // lexicographic sort is chronological. Newest first.
  const removed: string[] = [];
  for (const name of entries.sort().reverse().slice(Math.max(0, keep))) {
    try {
      fs.unlinkSync(path.join(receiptsDirectory, name));
      removed.push(name);
    } catch {
      // Another writer owns it now; the next run re-counts.
    }
  }
  return removed;
}

function finishReconcile(
  run: ReconcileRun,
  options: ManagedConfigReconcileOptions,
): ManagedConfigReconcileResult {
  const monotonicNow = options.monotonicNow ?? (() => performance.now());
  const count = (status: ManagedConfigReconcileTargetStatus) =>
    run.reports.filter((report) => report.status === status).length;
  const applied = count("applied");
  const refused = count("refused");
  const skipped = count("skipped");
  const absent = run.reports.filter((report) => report.reason === "absent").length;
  const ownedRefusal = run.reports.some((report) => report.status === "refused" && !report.discovered);
  const result: ManagedConfigReconcileResult = {
    status: applied > 0 ? "managed_config_reconciled" : "managed_config_unchanged",
    startedAt: run.startedAt,
    // Wall time from a monotonic clock, never from the run's fixed `now`
    // (review r2, R3).
    durationMs: Math.max(0, Math.round(monotonicNow() - run.startedAtMonotonicMs)),
    applied,
    unchanged: count("unchanged"),
    skipped,
    refused,
    absent,
    ownedRefusal,
    targets: run.reports,
    receiptPath: null,
  };
  if (options.dryRun) return result;

  const keepBackups = options.prune?.backupsPerFile ?? MANAGED_CONFIG_BACKUPS_KEPT_PER_FILE;
  const minBackupAgeMs = options.prune?.minBackupAgeMs ?? MANAGED_CONFIG_BACKUP_MIN_AGE_MS;
  // One critical section: merge this run's decisions over whatever the other
  // process left on disk, prune only the backups the merged record owns, and
  // write the result (review r2, R4). Merging rather than overwriting is what
  // keeps a concurrent run's backoff entries and backup record alive.
  updateManagedConfigReconcileState(options.collectorHome, (current) => {
    const backoff = { ...current.backoff };
    for (const name of run.backoffDecided) {
      const entry = run.backoff[name];
      if (entry) backoff[name] = entry;
      else delete backoff[name];
    }
    const backups = { ...current.backups };
    for (const created of run.createdBackups) {
      const record = backups[created.name];
      const names = record && record.file === created.file ? [...record.names] : [];
      names.push(path.basename(created.backup));
      backups[created.name] = { file: created.file, names };
    }
    for (const report of run.reports) {
      if (report.status !== "applied" || !report.backup) continue;
      const record = backups[report.name];
      if (!record) continue;
      const { survivors } = pruneBackups(
        record.file,
        record.names,
        keepBackups,
        minBackupAgeMs,
        run.startedAtMs,
      );
      if (survivors.length === 0) delete backups[report.name];
      else backups[report.name] = { file: record.file, names: survivors };
    }
    const stamp = stampIsNotOlder(run.startedAt, current.lastRunAt)
      ? {
          lastRunAt: run.startedAt,
          lastResult: lastResultOf({ applied, refused, skipped: skipped - absent }),
          lastApplied: applied,
          lastRefused: refused,
          lastAbsent: absent,
        }
      : {
          lastRunAt: current.lastRunAt,
          lastResult: current.lastResult,
          lastApplied: current.lastApplied,
          lastRefused: current.lastRefused,
          lastAbsent: current.lastAbsent,
        };
    return { next: { version: 1, ...stamp, backoff, backups }, result: undefined };
  });
  if (applied === 0 && refused === 0) return result;
  const receiptsDirectory = path.join(options.collectorHome, "receipts");
  fs.mkdirSync(receiptsDirectory, { recursive: true, mode: 0o700 });
  const receiptPath = path.join(
    receiptsDirectory,
    `managed-config-reconcile-${run.startedAt.replace(/[:.]/g, "-")}.json`,
  );
  fs.writeFileSync(receiptPath, `${JSON.stringify({ ...result, receiptPath }, null, 2)}\n`, {
    mode: 0o600,
  });
  pruneReceipts(receiptsDirectory, options.prune?.receipts ?? MANAGED_CONFIG_RECEIPTS_KEPT);
  return { ...result, receiptPath };
}

/**
 * Plan every managed target, then apply only the ones the plan changes.
 *
 * A receipt is written only when the run applied or refused something. An
 * all-`unchanged` run is a true no-op: no backup, no receipt, no state write
 * beyond the run stamp, so the 10-minute daemon cadence on a healthy host
 * leaves the filesystem byte-identical apart from that stamp.
 *
 * This is the operator-command entrypoint (`setup --reconcile`), where holding
 * the process for the whole run is exactly what the operator asked for. The
 * daemon uses `runManagedConfigReconcileAsync` instead.
 */
export function runManagedConfigReconcile(
  options: ManagedConfigReconcileOptions,
): ManagedConfigReconcileResult {
  const run = beginReconcile(options);
  for (const target of options.targets) reconcileOneTarget(run, target, options);
  return finishReconcile(run, options);
}

/**
 * The daemon's entrypoint: the same reconcile, yielding to the event loop
 * between targets (review r1, F6).
 *
 * The collector serves /hooks/* and the OTLP receiver from this same process,
 * so a fleet-scale churn run must never be one synchronous chunk. Each target
 * is still applied synchronously — the transactional write must not be
 * interleaved with itself — but the loop hands the event loop back between
 * them, which bounds the longest stall this cadence can cause to one target's
 * plan-and-apply.
 */
export async function runManagedConfigReconcileAsync(
  options: ManagedConfigReconcileOptions,
): Promise<ManagedConfigReconcileResult> {
  const run = beginReconcile(options);
  for (const target of options.targets) {
    await yieldToEventLoop();
    reconcileOneTarget(run, target, options);
  }
  await yieldToEventLoop();
  return finishReconcile(run, options);
}
