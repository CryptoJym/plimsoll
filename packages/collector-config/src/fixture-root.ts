import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { CLAUDE_SEATS_DIRECTORY } from "./claude-seats";

/**
 * Fixture-root contract (issue 0071): managed-config apply always takes an
 * explicit target path — nothing here resolves a home for the caller. What
 * this module adds is a fail-closed assertion for the one context that has
 * no business touching an operator's real tool config: a repository proof,
 * harness or test. Four lane runs rewrote a real ~/.grok/hooks/plimsoll.json
 * because a harness let apply default to the process home; under a proof
 * context the apply entries now refuse every target that is not inside the
 * run's declared fixture root, and refuse outright when no fixture root was
 * declared. Production `setup` is untouched: it legitimately targets the real
 * home and never runs under a proof context.
 */
export const FIXTURE_ROOT_ENV = "PLIMSOLL_FIXTURE_ROOT";

/** Managed tool roots a proof must never write inside the operator's real home. */
const MANAGED_HOME_DIRECTORIES = [
  ".grok",
  ".codex",
  ".claude",
  // Fleet Claude seat homes: `setup` now merges managed telemetry into every
  // ~/.claude-seats/<slug>/settings.json, so a proof must be refused there for
  // the same reason it is refused in ~/.claude.
  CLAUDE_SEATS_DIRECTORY,
  ".gemini",
  ".plimsoll",
];

export type ManagedConfigTargetCode =
  | "FIXTURE_ROOT_REQUIRED"
  | "FIXTURE_ROOT_NOT_ABSOLUTE"
  | "FIXTURE_ROOT_IS_REAL_HOME"
  | "TARGET_OUTSIDE_FIXTURE_ROOT"
  | "TARGET_INSIDE_REAL_HOME";

export class ManagedConfigTargetError extends Error {
  constructor(readonly code: ManagedConfigTargetCode, detail: string) {
    super(`MANAGED_CONFIG_${code}: ${detail}`);
    this.name = "ManagedConfigTargetError";
  }
}

const PROOF_ENTRY = /(?:-proof|\.test)\.[cm]?[jt]s$/;

/**
 * A proof entry is recognised by its own file name, never by the directory the
 * repository happens to sit in, so an operator whose checkout path contains
 * "-proof" still gets a working production `setup`.
 */
function looksLikeProofEntry(value: string) {
  const base = path.basename(value);
  if (PROOF_ENTRY.test(base)) return true;
  // scripts/resource-proof/index.ts and friends.
  return /^index\.[cm]?[jt]s$/.test(base) && path.basename(path.dirname(value)).endsWith("-proof");
}

/**
 * True when this process is a repository proof, harness or test — directly, via
 * the disposable proof runner, via a `pnpm proof:*` script, or as a child that
 * was handed the fixture contract.
 */
export function managedConfigProofContext(
  env: NodeJS.ProcessEnv = process.env,
  argv: string[] = process.argv,
): boolean {
  if (typeof env[FIXTURE_ROOT_ENV] === "string") return true;
  if (typeof env.PLIMSOLL_PROOF_ROOT === "string") return true;
  if (typeof env.NODE_TEST_CONTEXT === "string") return true;
  const lifecycle = env.npm_lifecycle_event ?? "";
  if (lifecycle === "proof" || lifecycle === "test" || lifecycle.startsWith("proof:")) return true;
  return argv.slice(1).some(looksLikeProofEntry);
}

/** Resolve through symlinks as far as the path exists (macOS /var -> /private/var). */
function resolveRealish(value: string) {
  const absolute = path.resolve(value);
  const pending: string[] = [];
  let current = absolute;
  for (;;) {
    try {
      const real = fs.realpathSync(current);
      return pending.length === 0 ? real : path.join(real, ...pending.reverse());
    } catch {
      const parent = path.dirname(current);
      if (parent === current) return absolute;
      pending.push(path.basename(current));
      current = parent;
    }
  }
}

function variants(value: string) {
  const absolute = path.resolve(value);
  return [absolute, resolveRealish(absolute)];
}

function inside(target: string, root: string) {
  for (const candidate of variants(target)) {
    for (const base of variants(root)) {
      if (candidate === base) return true;
      const relative = path.relative(base, candidate);
      if (relative !== "" && !relative.startsWith("..") && !path.isAbsolute(relative)) return true;
    }
  }
  return false;
}

function sameRoot(left: string, right: string) {
  const rights = variants(right);
  return variants(left).some((candidate) => rights.includes(candidate));
}

/**
 * The operator's real home from the password database. `os.homedir()` honours
 * $HOME, which a fixture deliberately overrides, so it cannot identify the home
 * this guard protects.
 */
function realHomeRoots(env: NodeJS.ProcessEnv) {
  const roots = new Set<string>();
  try {
    const passwd = os.userInfo().homedir;
    if (passwd) roots.add(passwd);
  } catch {
    /* No password entry: fall through to the $HOME-free reading below. */
  }
  if (typeof env.HOME !== "string" && typeof env.USERPROFILE !== "string") {
    try {
      roots.add(os.homedir());
    } catch {
      /* Nothing further to protect. */
    }
  }
  return [...roots];
}

/**
 * Fail closed before a managed-config apply touches the filesystem. No-op
 * outside a proof context so production `setup` keeps targeting the real home.
 */
export function assertManagedConfigTarget(
  file: string,
  env: NodeJS.ProcessEnv = process.env,
  argv: string[] = process.argv,
): void {
  if (!managedConfigProofContext(env, argv)) return;
  const declared = env[FIXTURE_ROOT_ENV];
  if (typeof declared !== "string" || declared.trim().length === 0) {
    throw new ManagedConfigTargetError(
      "FIXTURE_ROOT_REQUIRED",
      `${file}: a proof, harness or test must export ${FIXTURE_ROOT_ENV}=<per-run temp directory> before applying managed config.`,
    );
  }
  if (!path.isAbsolute(declared)) {
    throw new ManagedConfigTargetError(
      "FIXTURE_ROOT_NOT_ABSOLUTE",
      `${FIXTURE_ROOT_ENV}=${JSON.stringify(declared)} must be an absolute path.`,
    );
  }
  for (const home of realHomeRoots(env)) {
    if (sameRoot(declared, home)) {
      throw new ManagedConfigTargetError(
        "FIXTURE_ROOT_IS_REAL_HOME",
        `${FIXTURE_ROOT_ENV} resolves to the operator home ${home}; a fixture root must be a disposable directory.`,
      );
    }
    for (const directory of MANAGED_HOME_DIRECTORIES) {
      if (inside(file, path.join(home, directory))) {
        throw new ManagedConfigTargetError(
          "TARGET_INSIDE_REAL_HOME",
          `${file} is inside the operator's real ${path.join(home, directory)}; proofs must write only under ${FIXTURE_ROOT_ENV}.`,
        );
      }
    }
  }
  if (!inside(file, declared)) {
    throw new ManagedConfigTargetError(
      "TARGET_OUTSIDE_FIXTURE_ROOT",
      `${file} is outside ${FIXTURE_ROOT_ENV}=${declared}.`,
    );
  }
}
