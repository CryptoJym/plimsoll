import os from "node:os";

import {
  installSystemdUserUnit,
  readSystemdUserUnitState,
  renderSystemdUserUnit,
  systemdUserDirectory,
  systemdUserEnableCommand,
  systemdUserStopCommand,
  systemdUserUnitPath,
  systemdUserWantsPath,
  type DaemonCommandExecutor,
  type SystemdUserUnitInstallReceipt,
  type SystemdUserUnitInstallResult,
  type SystemdUserUnitOptions,
  type SystemdUserUnitState,
} from "./linux.ts";
import { macosDaemonBackend } from "./macos.ts";

export { macosDaemonManifestPath } from "./macos.ts";

export {
  installSystemdUserUnit,
  readSystemdUserUnitState,
  renderSystemdUserUnit,
  systemdUserDirectory,
  systemdUserEnableCommand,
  systemdUserStopCommand,
  systemdUserUnitPath,
  systemdUserWantsPath,
  type DaemonCommandExecutor,
  type SystemdUserUnitInstallReceipt,
  type SystemdUserUnitInstallResult,
  type SystemdUserUnitOptions,
  type SystemdUserUnitState,
};

export type SupportedDaemonPlatform = "darwin" | "linux";

export class UnsupportedDaemonPlatformError extends Error {
  constructor(readonly platform: NodeJS.Platform) {
    super(`Unsupported daemon platform: ${platform}. Plimsoll supports macOS and Linux.`);
    this.name = "UnsupportedDaemonPlatformError";
  }
}

export type DaemonBackend = {
  platform: SupportedDaemonPlatform;
  kind: "launch_agent" | "systemd_user";
  manifestPath: (homeDir?: string) => string;
};

const linuxDaemonBackend: DaemonBackend = {
  platform: "linux",
  kind: "systemd_user",
  manifestPath: (homeDir = os.homedir()) => systemdUserUnitPath(homeDir),
};

export function daemonBackend(platform: NodeJS.Platform = process.platform): DaemonBackend {
  if (platform === "linux") return linuxDaemonBackend;
  if (platform === "darwin") return macosDaemonBackend;
  throw new UnsupportedDaemonPlatformError(platform);
}

export function daemonManifestPath(
  platform: NodeJS.Platform = process.platform,
  homeDir = os.homedir(),
) {
  return daemonBackend(platform).manifestPath(homeDir);
}

export function daemonKind(platform: NodeJS.Platform = process.platform) {
  return daemonBackend(platform).kind;
}
