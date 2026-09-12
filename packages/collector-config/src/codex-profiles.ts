import path from "node:path";

import { discoverHomeDirectories } from "./discovered-homes";

/**
 * Fleet Codex seat profiles (bead eco-6hoxj.52). Fleet Codex lanes run with
 * CODEX_HOME=$HOME/.codex-profiles/<slug>, so the managed hooks and `[otel]`
 * exporters `setup` merges into ~/.codex/config.toml never reach them: a seat
 * lane was captured only by the rollout scanner — no live hook events, no
 * spans — while a default-home Codex session emits both.
 *
 * Discovery is the same contract the Claude seats use, and since bead
 * eco-6hoxj.54 literally the same walk (see discovered-homes.ts): one flat
 * level of profile directories under the process home, never a provision. A
 * profile directory without config.toml is reported and skipped, never
 * created; Codex homes outside ~/.codex-profiles belong to whoever set
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
  return discoverHomeDirectories(home, CODEX_PROFILES_DIRECTORY, "config.toml")
    .map(({ slug, path: file, exists }) => ({ slug, path: file, hasConfig: exists }));
}
