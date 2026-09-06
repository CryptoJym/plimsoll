import os from "node:os";
import path from "node:path";

const LAUNCH_AGENT_LABEL = "com.plimsoll.collector";

export const MACOS_DAEMON_KIND = "launch_agent" as const;

export function macosDaemonManifestPath(homeDir = os.homedir()) {
  return path.join(homeDir, "Library", "LaunchAgents", `${LAUNCH_AGENT_LABEL}.plist`);
}

export const macosDaemonBackend = {
  platform: "darwin" as const,
  kind: MACOS_DAEMON_KIND,
  manifestPath: macosDaemonManifestPath,
};
