import fs from "node:fs";
import path from "node:path";

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

/**
 * The `claude` target plus every discovered fleet seat, in setup's order.
 * A seat directory without settings.json is skipped rather than created, so the
 * composed list is exactly what setup composes today.
 */
export function composeManagedClaudeTargets(claudeFile: string, home: string): ManagedConfigTarget[] {
  return [
    {
      name: "claude",
      path: claudeFile,
      family: "claude",
      run: (options, preview) =>
        applyClaudeSettings(claudeFile, generateClaudeCodeSettings(options), { dryRun: preview }),
    },
    ...discoverClaudeSeats(home)
      .filter((seat) => seat.hasSettings)
      .map((seat): ManagedConfigTarget => ({
        name: `claudeSeat[${seat.slug}]`,
        path: seat.path,
        family: "claude",
        discovered: true,
        run: (options, preview) =>
          applyClaudeSettings(seat.path, generateClaudeCodeSettings(options), {
            dryRun: preview,
            managedTarget: `claudeSeat[${seat.slug}]`,
          }),
      })),
  ];
}

/** The `codex` target plus every discovered fleet seat profile, in setup's order. */
export function composeManagedCodexTargets(codexFile: string, home: string): ManagedConfigTarget[] {
  return [
    {
      name: "codex",
      path: codexFile,
      family: "codex",
      run: (options, preview) =>
        applyCodexConfig(codexFile, generateCodexConfigToml(options), { dryRun: preview }),
    },
    ...discoverCodexProfiles(home)
      .filter((profile) => profile.hasConfig)
      .map((profile): ManagedConfigTarget => ({
        name: `codexProfile[${profile.slug}]`,
        path: profile.path,
        family: "codex",
        discovered: true,
        run: (options, preview) =>
          applyCodexConfig(profile.path, generateCodexConfigToml(options), {
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
): ManagedConfigTarget[] {
  return [
    ...composeManagedClaudeTargets(claudeFile, home),
    ...composeManagedCodexTargets(codexFile, home),
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

export type ManagedConfigReconcileTargetStatus =
  | "applied"
  | "unchanged"
  | "skipped"
  | "refused";

export type ManagedConfigReconcileSkipReason =
  | "absent"
  | "backoff"
  | "changed_during_plan";

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
  status:
    | "managed_config_reconciled"
    | "managed_config_unchanged"
    | "managed_config_not_configured";
  startedAt: string;
  durationMs: number;
  applied: number;
  unchanged: number;
  skipped: number;
  refused: number;
  /** True when a target Plimsoll declares (not a discovered seat/profile) refused. */
  ownedRefusal: boolean;
  targets: ManagedConfigReconcileTargetReport[];
  receiptPath: string | null;
};

/** A refused or unparseable file is not retried more often than this. */
export const MANAGED_CONFIG_REFUSAL_BACKOFF_MS = 60 * 60 * 1000;

export const DEFAULT_MANAGED_CONFIG_RECONCILE_INTERVAL_SECONDS = 600;

type ManagedConfigReconcileState = {
  version: 1;
  lastRunAt: string | null;
  lastApplied: number;
  lastRefused: number;
  /** Per-target refusal backoff: target name -> ISO deadline. */
  backoff: Record<string, string>;
};

const EMPTY_STATE: ManagedConfigReconcileState = {
  version: 1,
  lastRunAt: null,
  lastApplied: 0,
  lastRefused: 0,
  backoff: {},
};

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
    const backoff: Record<string, string> = {};
    for (const [name, until] of Object.entries(parsed.backoff ?? {})) {
      if (typeof until === "string" && Number.isFinite(Date.parse(until))) backoff[name] = until;
    }
    return {
      version: 1,
      lastRunAt: typeof parsed.lastRunAt === "string" ? parsed.lastRunAt : null,
      lastApplied: Number.isSafeInteger(parsed.lastApplied) ? (parsed.lastApplied as number) : 0,
      lastRefused: Number.isSafeInteger(parsed.lastRefused) ? (parsed.lastRefused as number) : 0,
      backoff,
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
    lastApplied: state.lastApplied,
    lastRefused: state.lastRefused,
    nextEligibleAt: Number.isFinite(lastRunMs)
      ? new Date(lastRunMs + options.intervalSeconds * 1000).toISOString()
      : null,
  };
}

export type ManagedConfigReconcileDecision = {
  run: boolean;
  reason: "disabled" | "interval_not_elapsed" | "no_drift" | "drift";
  nextEligibleAt: string | null;
};

/**
 * The maintenance loop's decision, as a pure function so both states can be
 * pinned without waiting out a cadence.
 *
 * Order matters and is the point: the kill-switch is checked before anything
 * reads the clock, and the interval is checked before `drift()` is called, so a
 * disabled or not-yet-due collector performs no managed-config filesystem reads
 * at all.
 */
export function decideManagedConfigReconcile(input: {
  enabled: boolean;
  intervalSeconds: number;
  now: number;
  lastRunAt: number | null;
  drift: () => number;
}): ManagedConfigReconcileDecision {
  if (!input.enabled) return { run: false, reason: "disabled", nextEligibleAt: null };
  const intervalMs = Math.max(1, input.intervalSeconds) * 1000;
  const nextEligible = input.lastRunAt === null ? input.now : input.lastRunAt + intervalMs;
  const nextEligibleAt = new Date(nextEligible).toISOString();
  if (nextEligible > input.now) return { run: false, reason: "interval_not_elapsed", nextEligibleAt };
  return input.drift() > 0
    ? { run: true, reason: "drift", nextEligibleAt }
    : { run: false, reason: "no_drift", nextEligibleAt };
}

/** Identity of the file as the plan saw it; a concurrent writer changes it. */
type FileWitness = { mtimeMs: number; size: number; inode: number; device: number };

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

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

export type ManagedConfigReconcileOptions = {
  collectorHome: string;
  targets: ManagedConfigTarget[];
  toolOptions: ToolConfigOptions;
  /** Plan only: report what would be applied and write nothing at all. */
  dryRun?: boolean;
  now?: () => number;
  /** Deterministic proof seam: runs after a target's plan, before its apply. */
  onPlanned?: (target: ManagedConfigTarget) => void;
};

/**
 * Plan every managed target, then apply only the ones the plan changes.
 *
 * A receipt is written only when the run applied or refused something. An
 * all-`unchanged` run is a true no-op: no backup, no receipt, no state write
 * beyond the run stamp, so the 10-minute daemon cadence on a healthy host
 * leaves the filesystem byte-identical apart from that stamp.
 */
export function runManagedConfigReconcile(
  options: ManagedConfigReconcileOptions,
): ManagedConfigReconcileResult {
  const now = options.now ?? (() => Date.now());
  const startedAtMs = now();
  const startedAt = new Date(startedAtMs).toISOString();
  const state = readManagedConfigReconcileState(options.collectorHome);
  const backoff = { ...state.backoff };
  const reports: ManagedConfigReconcileTargetReport[] = [];

  for (const target of options.targets) {
    const base = {
      name: target.name,
      path: target.path,
      discovered: target.discovered === true,
    };
    const held = backoff[target.name];
    const heldUntil = held ? Date.parse(held) : Number.NaN;
    if (Number.isFinite(heldUntil) && heldUntil > startedAtMs) {
      // A file Plimsoll already refused is fleet state; retrying it every
      // cadence would only reprint the same refusal.
      reports.push({
        ...base,
        status: "skipped",
        reason: "backoff" satisfies ManagedConfigReconcileSkipReason,
        nextEligibleAt: held,
      });
      continue;
    }
    const before = witness(target.path);
    if (before === null) {
      // Reconcile never provisions: the seat/conductor tooling owns whether the
      // file exists at all, and `setup --yes` owns first installation.
      reports.push({ ...base, status: "skipped", reason: "absent" satisfies ManagedConfigReconcileSkipReason });
      continue;
    }
    let plan: ApplyResult;
    try {
      plan = target.run(options.toolOptions, true);
    } catch (error) {
      const until = new Date(startedAtMs + MANAGED_CONFIG_REFUSAL_BACKOFF_MS).toISOString();
      backoff[target.name] = until;
      reports.push({ ...base, status: "refused", reason: errorMessage(error), nextEligibleAt: until });
      continue;
    }
    if (plan.conflict) {
      const until = new Date(startedAtMs + MANAGED_CONFIG_REFUSAL_BACKOFF_MS).toISOString();
      backoff[target.name] = until;
      reports.push({
        ...base,
        status: "refused",
        plan: plan.plan,
        reason: plan.conflict,
        nextEligibleAt: until,
      });
      continue;
    }
    delete backoff[target.name];
    if (!plan.changed) {
      reports.push({ ...base, status: "unchanged", plan: plan.plan });
      continue;
    }
    if (options.dryRun) {
      reports.push({ ...base, status: "applied", plan: plan.plan, backup: null, reason: "dry_run" });
      continue;
    }
    options.onPlanned?.(target);
    const after = witness(target.path);
    if (after === null || !sameWitness(before, after)) {
      // Another writer owns this file right now. The plan was computed against
      // content that no longer exists, so this run stands down; the next one
      // plans the file as it is.
      reports.push({
        ...base,
        status: "skipped",
        plan: plan.plan,
        reason: "changed_during_plan" satisfies ManagedConfigReconcileSkipReason,
      });
      continue;
    }
    try {
      const applied = target.run(options.toolOptions, false);
      if (applied.conflict) {
        const until = new Date(startedAtMs + MANAGED_CONFIG_REFUSAL_BACKOFF_MS).toISOString();
        backoff[target.name] = until;
        reports.push({
          ...base,
          status: "refused",
          plan: applied.plan ?? plan.plan,
          reason: applied.conflict,
          nextEligibleAt: until,
        });
        continue;
      }
      reports.push({
        ...base,
        status: "applied",
        plan: applied.plan ?? plan.plan,
        backup: applied.backupPath ?? null,
      });
    } catch (error) {
      const until = new Date(startedAtMs + MANAGED_CONFIG_REFUSAL_BACKOFF_MS).toISOString();
      backoff[target.name] = until;
      reports.push({ ...base, status: "refused", plan: plan.plan, reason: errorMessage(error), nextEligibleAt: until });
    }
  }

  const count = (status: ManagedConfigReconcileTargetStatus) =>
    reports.filter((report) => report.status === status).length;
  const applied = count("applied");
  const refused = count("refused");
  const ownedRefusal = reports.some((report) => report.status === "refused" && !report.discovered);
  const result: ManagedConfigReconcileResult = {
    status: applied > 0 ? "managed_config_reconciled" : "managed_config_unchanged",
    startedAt,
    durationMs: now() - startedAtMs,
    applied,
    unchanged: count("unchanged"),
    skipped: count("skipped"),
    refused,
    ownedRefusal,
    targets: reports,
    receiptPath: null,
  };
  if (options.dryRun) return result;

  writeManagedConfigReconcileState(options.collectorHome, {
    version: 1,
    lastRunAt: startedAt,
    lastApplied: applied,
    lastRefused: refused,
    backoff,
  });
  if (applied === 0 && refused === 0) return result;
  const receiptsDirectory = path.join(options.collectorHome, "receipts");
  fs.mkdirSync(receiptsDirectory, { recursive: true, mode: 0o700 });
  const receiptPath = path.join(
    receiptsDirectory,
    `managed-config-reconcile-${startedAt.replace(/[:.]/g, "-")}.json`,
  );
  fs.writeFileSync(receiptPath, `${JSON.stringify({ ...result, receiptPath }, null, 2)}\n`, {
    mode: 0o600,
  });
  return { ...result, receiptPath };
}
