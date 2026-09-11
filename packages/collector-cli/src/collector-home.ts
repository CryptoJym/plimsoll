import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

/**
 * Issue #135: one canonical collector-home resolver. Every command, the
 * daemon, the LaunchAgent manifest, and every derived path (config, ledger,
 * WAL/SHM, PID, logs, history watermark, lifecycle root) must bind the SAME
 * validated home. A custom `PLIMSOLL_HOME` is validated — absolute, a real
 * directory (never a symlink), owned by the running user, and private
 * (no group/other permission bits, no setuid/setgid/sticky) — and no command
 * ever silently falls back to the default home when validation fails.
 *
 * The identity hash is path-free on purpose: receipts and doctor/status
 * output carry only `sha256:<hex>` of the canonical home path, never the
 * path itself.
 */

export class CollectorHomeError extends Error {
  constructor(
    readonly code:
      | "home_not_absolute"
      | "home_is_symlink"
      | "home_not_directory"
      | "home_owner_mismatch"
      | "home_not_private",
    detail: string,
  ) {
    super(`collector_home_${code}: ${detail}`);
    this.name = "CollectorHomeError";
  }
}

export type ResolvedCollectorHome = {
  /** Canonical absolute home directory. */
  home: string;
  source: "default" | "env";
};

export class GrokHomeError extends Error {
  constructor(
    readonly code:
      | "grok_home_ambiguous"
      | "grok_home_control_characters"
      | "grok_home_not_absolute"
      | "grok_home_not_normalized",
    detail: string,
  ) {
    super(`${code}: ${detail}`);
    this.name = "GrokHomeError";
  }
}

export type ResolvedGrokHome = {
  home: string;
  source: "default" | "env";
};

export function defaultCollectorHome(homeDir = os.homedir()) {
  return path.join(homeDir, "Library", "Application Support", "Plimsoll");
}

/** Validate a caller-supplied Grok root before setup derives any target. */
export function resolveGrokHome(
  options: { env?: NodeJS.ProcessEnv; homeDir?: string } = {},
): ResolvedGrokHome {
  const env = options.env ?? process.env;
  if (!Object.hasOwn(env, "GROK_HOME")) {
    return { home: path.join(options.homeDir ?? os.homedir(), ".grok"), source: "default" };
  }
  const raw = env.GROK_HOME;
  if (typeof raw !== "string" || raw.length === 0 || raw.trim() !== raw) {
    throw new GrokHomeError(
      "grok_home_ambiguous",
      "GROK_HOME must be a non-empty path without surrounding whitespace.",
    );
  }
  if (/[\u0000-\u001f\u007f-\u009f]/.test(raw)) {
    throw new GrokHomeError("grok_home_control_characters", "GROK_HOME contains control characters.");
  }
  if (!path.isAbsolute(raw)) {
    throw new GrokHomeError(
      "grok_home_not_absolute",
      `GROK_HOME=${JSON.stringify(raw)} is relative.`,
    );
  }
  const normalized = path.normalize(raw);
  if (
    normalized !== raw ||
    path.resolve(raw) !== raw ||
    raw.normalize("NFC") !== raw ||
    raw === path.parse(raw).root
  ) {
    throw new GrokHomeError(
      "grok_home_not_normalized",
      `GROK_HOME=${JSON.stringify(raw)} is not a safe normalized root.`,
    );
  }
  return { home: raw, source: "env" };
}

function lstatSafe(file: string): fs.Stats | null {
  try {
    return fs.lstatSync(file);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

export function resolveCollectorHome(
  options: { env?: NodeJS.ProcessEnv; homeDir?: string; uid?: number } = {},
): ResolvedCollectorHome {
  const env = options.env ?? process.env;
  const raw = env.PLIMSOLL_HOME?.trim();
  if (!raw) {
    return { home: defaultCollectorHome(options.homeDir), source: "default" };
  }
  if (/[\u0000-\u001f\u007f-\u009f]/.test(raw)) {
    throw new CollectorHomeError("home_not_absolute", "PLIMSOLL_HOME contains control characters.");
  }
  if (!path.isAbsolute(raw)) {
    throw new CollectorHomeError("home_not_absolute", `PLIMSOLL_HOME=${JSON.stringify(raw)} is relative.`);
  }
  const resolved = path.resolve(raw);
  // An absent home is allowed: first-run creation happens through
  // ensureCollectorHome() with mode 0700. Everything else must already be a
  // safe private directory owned by the current user.
  const stat = lstatSafe(resolved);
  if (!stat) {
    return { home: resolved, source: "env" };
  }
  if (stat.isSymbolicLink()) {
    throw new CollectorHomeError("home_is_symlink", `PLIMSOLL_HOME=${JSON.stringify(resolved)} is a symlink.`);
  }
  if (!stat.isDirectory()) {
    throw new CollectorHomeError("home_not_directory", `PLIMSOLL_HOME=${JSON.stringify(resolved)} is not a directory.`);
  }
  const uid = options.uid ?? (typeof process.getuid === "function" ? process.getuid() : undefined);
  if (uid !== undefined && stat.uid !== uid) {
    throw new CollectorHomeError(
      "home_owner_mismatch",
      `PLIMSOLL_HOME=${JSON.stringify(resolved)} is owned by uid ${stat.uid}, not ${uid}.`,
    );
  }
  if ((stat.mode & 0o7077) !== 0) {
    throw new CollectorHomeError(
      "home_not_private",
      `PLIMSOLL_HOME=${JSON.stringify(resolved)} has unsafe mode ${stat.mode.toString(8)}; group/other access or special bits are not allowed.`,
    );
  }
  return { home: resolved, source: "env" };
}

/** Path-free home identity for receipts and status/doctor comparison. */
export function collectorHomeIdentityHash(home: string) {
  return `sha256:${createHash("sha256").update(path.resolve(home)).digest("hex")}`;
}
