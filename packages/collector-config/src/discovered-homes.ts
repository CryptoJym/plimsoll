import fs from "node:fs";
import path from "node:path";

/**
 * The discovery shape the fleet seat families share (review r1 finding 4 of
 * bead eco-6hoxj.52): one flat level of directories under the process home,
 * each holding one config file, never a provision. `claude-seats.ts` and
 * `codex-profiles.ts` differ only in the directory constant, the file name and
 * the name of the "the file is there" field, so the walk itself lives here and
 * the link-resolution semantics have one place to change rather than N.
 *
 * Behaviour is exactly what both families shipped: slug-ordered results, a
 * directory without its config file reported and skipped rather than created,
 * and a symlinked directory reported at its resolved path so the managed-config
 * guard and every receipt name the file actually written. A dangling link stays
 * visible as a seat/profile whose config file does not exist.
 */
export type DiscoveredHome = {
  /** Directory name, e.g. the slug the fleet tooling created. */
  slug: string;
  /** Absolute config-file path. The file need not exist. */
  path: string;
  /** False when the directory carries no config file. */
  exists: boolean;
};

export function discoverHomeDirectories(
  home: string,
  directory: string,
  fileName: string,
): DiscoveredHome[] {
  const root = path.join(home, directory);
  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(root, { withFileTypes: true });
  } catch {
    // Nothing of this family on this host: not an error, nothing to manage.
    return [];
  }
  const discovered: DiscoveredHome[] = [];
  for (const entry of entries) {
    const resolved = entryDirectory(root, entry);
    if (resolved === undefined) continue;
    const file = path.join(resolved, fileName);
    discovered.push({ slug: entry.name, path: file, exists: isExistingFile(file) });
  }
  return discovered.sort((left, right) => (left.slug < right.slug ? -1 : left.slug > right.slug ? 1 : 0));
}

/**
 * The directory a root entry names, or undefined when the entry is not one.
 * readdirSync does not follow links, so a seat or profile relocated to shared
 * storage and symlinked into the root would otherwise be invisible to setup and
 * doctor alike. A link is reported at its resolved path; a dangling link keeps
 * the link path so it stays visible without its config file.
 */
function entryDirectory(root: string, entry: fs.Dirent) {
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
