import fs from "node:fs";
import path from "node:path";

/**
 * Fleet Claude seats (bead eco-6hoxj.48). Conductor and lead lanes run Claude
 * Code with CLAUDE_CONFIG_DIR=$HOME/.claude-seats/<slug>, so the managed
 * telemetry `setup` merges into ~/.claude/settings.json never reaches them: a
 * one-turn seat session produced a single transcript row, no spans and no hook
 * events, while a default-home session produced ~20 events.
 *
 * Discovery is the simplest contract that covers the fleet — one flat level of
 * seat directories under the process home — and it never provisions: a seat
 * directory without settings.json is reported and skipped, never created. Seat
 * homes outside ~/.claude-seats are out of scope; the seat tooling owns that
 * layout.
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
  const root = claudeSeatsRoot(home);
  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(root, { withFileTypes: true });
  } catch {
    // No seats on this host: not an error, there is simply nothing to manage.
    return [];
  }
  const seats: ClaudeSeat[] = [];
  for (const entry of entries) {
    const directory = seatDirectory(root, entry);
    if (directory === undefined) continue;
    const file = path.join(directory, "settings.json");
    seats.push({ slug: entry.name, path: file, hasSettings: isExistingFile(file) });
  }
  return seats.sort((left, right) => (left.slug < right.slug ? -1 : left.slug > right.slug ? 1 : 0));
}

/**
 * The directory a seats-root entry names, or undefined when the entry is not a
 * seat. readdirSync does not follow links, so a seat the tooling relocated to
 * shared storage and symlinked into ~/.claude-seats would otherwise be
 * invisible to both setup and doctor — the exact gap this bead exists to close.
 * A link is reported at its resolved path so the managed-config guard and every
 * receipt name the file actually written rather than the link; a dangling link
 * stays visible as a seat without settings.json instead of disappearing.
 */
function seatDirectory(root: string, entry: fs.Dirent) {
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
