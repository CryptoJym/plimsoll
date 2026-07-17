import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { collectorHome, ensureCollectorHome } from "./config";

export const LAUNCH_AGENT_LABEL = "com.plimsoll.collector";
export const LAUNCH_AGENT_SYSTEM_PATHS = [
  "/opt/homebrew/bin",
  "/usr/local/bin",
  "/usr/bin",
  "/bin",
  "/usr/sbin",
  "/sbin",
] as const;

export type LaunchAgentOptions = {
  homeDir?: string;
  repoRoot: string;
  pnpmPath?: string;
  label?: string;
  /** Packaged installs (npx/npm -g) exec the bundled cli directly instead of
   * pnpm-in-a-working-tree. */
  programArguments?: string[];
  restartThrottleSeconds?: number;
  workingDirectory?: string;
};

function escapeXml(value: string) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function stringElement(value: string) {
  return `    <string>${escapeXml(value)}</string>`;
}

export function launchAgentsDir(homeDir = os.homedir()) {
  return path.join(homeDir, "Library", "LaunchAgents");
}

export function launchAgentPlistPath(homeDir = os.homedir(), label = LAUNCH_AGENT_LABEL) {
  return path.join(launchAgentsDir(homeDir), `${label}.plist`);
}

export function renderLaunchAgentPlist(options: LaunchAgentOptions) {
  const homeDir = options.homeDir ?? os.homedir();
  const label = options.label ?? LAUNCH_AGENT_LABEL;
  const pnpmPath = options.pnpmPath ?? "pnpm";
  const restartThrottleSeconds = options.restartThrottleSeconds ?? 30;
  const logDirectory = collectorHome(homeDir);
  const programArguments = options.programArguments ?? [pnpmPath, "--dir", options.repoRoot, "collector", "start"];
  const workingDirectory = options.workingDirectory ?? options.repoRoot;
  const inheritedPathEntries = (process.env.PATH ?? "").split(path.delimiter);
  const pathCandidates = [
    path.dirname(process.execPath),
    path.dirname(pnpmPath),
    path.dirname(programArguments[0] ?? ""),
    ...inheritedPathEntries,
    ...LAUNCH_AGENT_SYSTEM_PATHS,
  ];
  const pathEntries: string[] = [];
  const normalizedEntries = new Set<string>();
  for (const candidate of pathCandidates) {
    if (!candidate || !path.isAbsolute(candidate) || /[\u0000-\u001f\u007f-\u009f]/.test(candidate)) {
      continue;
    }
    const normalized = path.resolve(candidate);
    if (normalizedEntries.has(normalized)) continue;
    normalizedEntries.add(normalized);
    pathEntries.push(normalized);
  }
  const launchAgentPath = pathEntries.join(path.delimiter);

  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${escapeXml(label)}</string>
  <key>ProgramArguments</key>
  <array>
${programArguments.map(stringElement).join("\n")}
  </array>
  <key>WorkingDirectory</key>
  <string>${escapeXml(workingDirectory)}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>${restartThrottleSeconds}</integer>
  <key>StandardOutPath</key>
  <string>${escapeXml(path.join(logDirectory, "collector.out.log"))}</string>
  <key>StandardErrorPath</key>
  <string>${escapeXml(path.join(logDirectory, "collector.err.log"))}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PLIMSOLL_COLLECTOR_DATA_MODE</key>
    <string>metadata</string>
    <key>PATH</key>
    <string>${escapeXml(launchAgentPath)}</string>
  </dict>
</dict>
</plist>
`;
}

export function installLaunchAgent(options: LaunchAgentOptions) {
  const homeDir = options.homeDir ?? os.homedir();
  ensureCollectorHome(homeDir);
  fs.mkdirSync(launchAgentsDir(homeDir), { recursive: true, mode: 0o700 });
  const plistPath = launchAgentPlistPath(homeDir, options.label);
  fs.writeFileSync(plistPath, renderLaunchAgentPlist({ ...options, homeDir }), {
    mode: 0o600,
  });

  return plistPath;
}

export function uninstallLaunchAgent(options: { homeDir?: string; label?: string }) {
  const plistPath = launchAgentPlistPath(options.homeDir ?? os.homedir(), options.label);
  if (fs.existsSync(plistPath)) {
    fs.rmSync(plistPath);
    return true;
  }

  return false;
}

export function launchctlBootstrapCommand(plistPath: string, uid = process.getuid?.()) {
  return ["launchctl", "bootstrap", `gui/${uid ?? ""}`, plistPath].filter(Boolean);
}

export function launchctlBootoutCommand(label = LAUNCH_AGENT_LABEL, uid = process.getuid?.()) {
  return ["launchctl", "bootout", `gui/${uid ?? ""}/${label}`].filter(Boolean);
}
