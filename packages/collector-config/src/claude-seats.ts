import path from "node:path";

import { discoverHomeDirectories } from "./discovered-homes";

/**
 * Fleet Claude seats (bead eco-6hoxj.48). Conductor and lead lanes run Claude
 * Code with CLAUDE_CONFIG_DIR=$HOME/.claude-seats/<slug>, so the managed
 * telemetry `setup` merges into ~/.claude/settings.json never reaches them: a
 * one-turn seat session produced a single transcript row, no spans and no hook
 * events, while a default-home session produced ~20 events.
 *
 * Discovery is the simplest contract that covers the fleet — one flat level of
 * seat directories under the process home, the walk the Codex seat profiles
 * share since bead eco-6hoxj.54 (see discovered-homes.ts) — and it never
 * provisions: a seat directory without settings.json is reported and skipped,
 * never created. Seat homes outside ~/.claude-seats are out of scope; the seat
 * tooling owns that layout.
 */
export const CLAUDE_SEATS_DIRECTORY = ".claude-seats";

export type ClaudeSeat = {
  /** Seat directory name, e.g. the seat slug the fleet tooling created. */
  slug: string;
  /** Absolute settings.json path for the seat. The file need not exist. */
  path: string;
  /** False when the seat directory carries no settings.json. */
  hasSettings: boolean;
};

export function claudeSeatsRoot(home: string) {
  return path.join(home, CLAUDE_SEATS_DIRECTORY);
}

/** Every seat directory under `<home>/.claude-seats`, slug-ordered. */
export function discoverClaudeSeats(home: string): ClaudeSeat[] {
  return discoverHomeDirectories(home, CLAUDE_SEATS_DIRECTORY, "settings.json")
    .map(({ slug, path: file, exists }) => ({ slug, path: file, hasSettings: exists }));
}
