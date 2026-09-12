import fs from "node:fs";
import path from "node:path";

/**
 * Fleet Codex seat profiles (bead eco-6hoxj.52). Fleet Codex lanes run with
 * CODEX_HOME=$HOME/.codex-profiles/<slug>, so the managed hooks and `[otel]`
 * exporters `setup` merges into ~/.codex/config.toml never reach them: a seat
 * lane was captured only by the rollout scanner — no live hook events, no
 * spans — while a default-home Codex session emits both.
 *
 * Discovery is the same contract the Claude seats use (see claude-seats.ts):
 * one flat level of profile directories under the process home, never a
 * provision. A profile directory without config.toml is reported and skipped,
 * never created; Codex homes outside ~/.codex-profiles belong to whoever set
 * CODEX_HOME and are out of scope.
 */
export const CODEX_PROFILES_DIRECTORY = ".codex-profiles";

export type CodexProfile = {
  /** Profile directory name, e.g. the account slug the fleet tooling created. */
  slug: string;
  /** Absolute config.toml path for the profile. The file need not exist. */
  path: string;
  /** False when the profile directory carries no config.toml. */
  hasConfig: boolean;
};

export function codexProfilesRoot(home: string) {
  return path.join(home, CODEX_PROFILES_DIRECTORY);
}

/** Every profile directory under `<home>/.codex-profiles`, slug-ordered. */
export function discoverCodexProfiles(home: string): CodexProfile[] {
  const root = codexProfilesRoot(home);
  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(root, { withFileTypes: true });
  } catch {
    // No Codex profiles on this host: nothing to manage, not an error.
    return [];
  }
  const profiles: CodexProfile[] = [];
  for (const entry of entries) {
    const directory = profileDirectory(root, entry);
    if (directory === undefined) continue;
    const file = path.join(directory, "config.toml");
    profiles.push({ slug: entry.name, path: file, hasConfig: isExistingFile(file) });
  }
  return profiles.sort((left, right) => (left.slug < right.slug ? -1 : left.slug > right.slug ? 1 : 0));
}

/**
 * The directory a profiles-root entry names, or undefined when the entry is not
 * a profile. readdirSync does not follow links, so a profile relocated to shared
 * storage and symlinked into ~/.codex-profiles would otherwise be invisible to
 * setup and doctor alike. A link is reported at its resolved path so the
 * managed-config guard and every receipt name the file actually written; a
 * dangling link stays visible as a profile without config.toml.
 */
function profileDirectory(root: string, entry: fs.Dirent) {
  const entryPath = path.join(root, entry.name);
  if (entry.isDirectory()) return entryPath;
  if (!entry.isSymbolicLink()) return undefined;
  try {
    const resolved = fs.realpathSync(entryPath);
    return fs.statSync(resolved).isDirectory() ? resolved : undefined;
  } catch {
    return entryPath;
  }
}

function isExistingFile(file: string) {
  try {
    return fs.statSync(file).isFile();
  } catch {
    return false;
  }
}
