import fs from "node:fs";
import path from "node:path";

import { FIXTURE_ROOT_ENV } from "../../packages/collector-config/src/fixture-root";

/**
 * Per-run fixture root for any proof, harness or script that applies or
 * generates managed tool config (issue 0071). Pins every tool-home variable
 * inside a disposable directory and exports the PLIMSOLL_FIXTURE_ROOT contract
 * that packages/collector-config enforces, so an apply that forgets a fixture
 * path fails closed instead of rewriting the operator's real ~/.grok, ~/.codex,
 * ~/.claude or ~/.gemini. `env` is the overlay to hand any spawned child.
 */
export type FixtureRoot = {
  root: string;
  home: string;
  env: Record<string, string>;
  restore(): void;
};

export function useFixtureRoot(
  root: string,
  options: { home?: string; plimsollHome?: string } = {},
): FixtureRoot {
  const resolved = path.resolve(root);
  fs.mkdirSync(resolved, { recursive: true, mode: 0o700 });
  // The fixture home itself stays byte-absent until a proof creates it; several
  // proofs assert exactly that.
  const home = path.resolve(options.home ?? path.join(resolved, "fixture-home"));
  const env: Record<string, string> = {
    HOME: home,
    USERPROFILE: home,
    PLIMSOLL_HOME: path.resolve(options.plimsollHome ?? path.join(home, ".plimsoll")),
    CODEX_HOME: path.join(home, ".codex"),
    GROK_HOME: path.join(home, ".grok"),
    CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
    [FIXTURE_ROOT_ENV]: resolved,
  };
  const previous = new Map<string, string | undefined>();
  for (const [key, value] of Object.entries(env)) {
    previous.set(key, process.env[key]);
    process.env[key] = value;
  }
  return {
    root: resolved,
    home,
    env,
    restore() {
      for (const [key, value] of previous) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    },
  };
}
