import fs from "node:fs";
import path from "node:path";

export type GrokSessionDiscovery = {
  sessionsRoot: string;
  summaryPaths: string[];
  truncated: boolean;
};

function regularDirectory(directory: string) {
  try {
    const stat = fs.lstatSync(directory);
    return stat.isDirectory() && !stat.isSymbolicLink();
  } catch {
    return false;
  }
}

function regularFile(file: string) {
  try {
    const stat = fs.lstatSync(file);
    return stat.isFile() && !stat.isSymbolicLink();
  } catch {
    return false;
  }
}

function boundedDirectoryNames(directory: string, limit: number) {
  if (!regularDirectory(directory)) return { names: [] as string[], truncated: false };
  const names = fs.readdirSync(directory).sort();
  return { names: names.slice(0, limit), truncated: names.length > limit };
}

/**
 * Discover the official `<GROK_HOME>/sessions/<cwd-group>/<session>/summary.json`
 * metadata index without reading any record body or following symlinks.
 */
export function discoverGrokSessionSummaries(
  grokHome: string,
  limits: { groups?: number; sessions?: number } = {},
): GrokSessionDiscovery {
  const groupLimit = limits.groups ?? 4_096;
  const sessionLimit = limits.sessions ?? 100_000;
  if (!Number.isInteger(groupLimit) || groupLimit <= 0 ||
      !Number.isInteger(sessionLimit) || sessionLimit <= 0) {
    throw new Error("grok_session_discovery_invalid_limit");
  }
  const sessionsRoot = path.resolve(grokHome, "sessions");
  const groups = boundedDirectoryNames(sessionsRoot, groupLimit);
  const summaryPaths: string[] = [];
  let truncated = groups.truncated;
  for (const groupName of groups.names) {
    const group = path.join(sessionsRoot, groupName);
    if (!regularDirectory(group)) continue;
    const remaining = sessionLimit - summaryPaths.length;
    if (remaining <= 0) {
      truncated = true;
      break;
    }
    const sessions = boundedDirectoryNames(group, remaining + 1);
    for (const sessionName of sessions.names.slice(0, remaining)) {
      const session = path.join(group, sessionName);
      if (!regularDirectory(session)) continue;
      const summary = path.join(session, "summary.json");
      if (regularFile(summary)) summaryPaths.push(summary);
    }
    if (sessions.truncated || sessions.names.length > remaining) truncated = true;
  }
  return { sessionsRoot, summaryPaths, truncated };
}
