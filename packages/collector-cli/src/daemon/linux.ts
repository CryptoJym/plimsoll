import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { collectorHomeIdentityHash, resolveCollectorHome } from "../collector-home.ts";

export const SYSTEMD_USER_UNIT_NAME = "plimsoll-collector.service";

type CommandResult = {
  status: number | null;
  error?: Error;
};

export type DaemonCommandExecutor = (
  command: string,
  args: readonly string[],
) => CommandResult;

export type SystemdUserUnitOptions = {
  platform?: NodeJS.Platform;
  homeDir?: string;
  repoRoot: string;
  pnpmPath?: string;
  programArguments?: readonly string[];
  workingDirectory?: string;
  env?: NodeJS.ProcessEnv;
  dryRun?: boolean;
  enable?: boolean;
  execute?: DaemonCommandExecutor;
};

export type SystemdUserUnitInstallReceipt = {
  schema: "plimsoll.systemd-user-unit-install.v1";
  operation: "install";
  status: "preview" | "installed" | "unchanged";
  target: "systemd_user_unit";
  unitName: typeof SYSTEMD_USER_UNIT_NAME;
  changed: boolean;
  wouldChange: boolean;
  enabled: boolean;
  wouldEnable: boolean;
  enableStatus: "preview" | "enabled" | "not_requested" | "systemctl_failed";
  unitDigest: string;
  unitMode: "0600";
  privacyMode: "metadata_only";
  collectorHomeIdentityHash: string;
  commands: readonly (readonly string[])[];
};

export type SystemdUserUnitInstallResult = {
  unitPath: string;
  receipt: SystemdUserUnitInstallReceipt;
};

type ValidatedUnitOptions = SystemdUserUnitOptions & {
  homeDir: string;
  env: NodeJS.ProcessEnv;
  workingDirectory: string;
  programArguments: readonly string[];
  environmentPath: string;
  collectorHome: string;
};

function assertSafeText(value: string, label: string) {
  if (!value || /[\u0000-\u001f\u007f-\u009f]/.test(value)) {
    throw new Error(`${label}_INVALID`);
  }
}

function resolvePnpmPath(value: string, env: NodeJS.ProcessEnv) {
  if (path.isAbsolute(value)) return path.resolve(value);
  if (value !== "pnpm") throw new Error("PNPM_PATH_NOT_ABSOLUTE");
  for (const directory of (env.PATH ?? "").split(path.delimiter)) {
    if (!directory || !path.isAbsolute(directory)) continue;
    const candidate = path.join(directory, value);
    try {
      fs.accessSync(candidate, fs.constants.X_OK);
      return path.resolve(candidate);
    } catch {
      // Continue through the bounded PATH list.
    }
  }
  throw new Error("PNPM_NOT_FOUND");
}

function validateOptions(options: SystemdUserUnitOptions): ValidatedUnitOptions {
  if ((options.platform ?? process.platform) !== "linux") {
    throw new Error("SYSTEMD_PLATFORM_UNSUPPORTED");
  }
  const env = options.env ?? process.env;
  const homeDir = path.resolve(options.homeDir ?? os.homedir());
  const workingDirectoryInput = options.workingDirectory ?? options.repoRoot;
  if (!path.isAbsolute(options.repoRoot)) throw new Error("REPO_ROOT_NOT_ABSOLUTE");
  if (!path.isAbsolute(workingDirectoryInput)) throw new Error("WORKING_DIRECTORY_NOT_ABSOLUTE");
  assertSafeText(options.repoRoot, "REPO_ROOT");
  assertSafeText(workingDirectoryInput, "WORKING_DIRECTORY");
  const workingDirectory = path.resolve(workingDirectoryInput);
  const collectorHome = resolveCollectorHome({
    env,
    homeDir,
    platform: "linux",
  }).home;
  const programArguments = options.programArguments
    ? [...options.programArguments]
    : [
        resolvePnpmPath(options.pnpmPath ?? "pnpm", env),
        "--dir",
        options.repoRoot,
        "collector",
        "start",
      ];
  if (programArguments.length === 0) throw new Error("ARGUMENTS_EMPTY");
  programArguments.forEach((argument) => assertSafeText(argument, "ARGUMENT"));
  if (!path.isAbsolute(programArguments[0]!)) throw new Error("EXECUTABLE_NOT_ABSOLUTE");
  const environmentPathEntries = [
    path.dirname(process.execPath),
    path.dirname(programArguments[0]!),
    ...(env.PATH ?? "").split(path.delimiter),
  ]
    .filter((entry) => entry && path.isAbsolute(entry) && !/[\u0000-\u001f\u007f-\u009f]/.test(entry))
    .map((entry) => path.resolve(entry));
  const environmentPath = [...new Set(environmentPathEntries)].join(path.delimiter);
  return {
    ...options,
    homeDir,
    env,
    workingDirectory,
    programArguments,
    environmentPath,
    collectorHome,
  };
}

function systemdValue(value: string) {
  assertSafeText(value, "SYSTEMD_VALUE");
  if (/^[A-Za-z0-9_+./:@%=-]+$/.test(value)) return value.replaceAll("%", "%%");
  return `"${value.replaceAll("\\", "\\\\").replaceAll('"', '\\"').replaceAll("%", "%%")}"`;
}

function parseSystemdValue(value: string) {
  if (value.startsWith('"') && value.endsWith('"')) {
    return value.slice(1, -1)
      .replaceAll('\\"', '"')
      .replaceAll("\\\\", "\\")
      .replaceAll("%%", "%");
  }
  return value.replaceAll("%%", "%");
}

function digest(value: string) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

export function systemdUserDirectory(homeDir = os.homedir()) {
  return path.join(path.resolve(homeDir), ".config", "systemd", "user");
}

export function systemdUserUnitPath(
  homeDir = os.homedir(),
  unitName = SYSTEMD_USER_UNIT_NAME,
) {
  if (unitName !== SYSTEMD_USER_UNIT_NAME) throw new Error("UNIT_NAME_NOT_ALLOWLISTED");
  return path.join(systemdUserDirectory(homeDir), unitName);
}

export function systemdUserWantsPath(homeDir = os.homedir()) {
  return path.join(systemdUserDirectory(homeDir), "default.target.wants", SYSTEMD_USER_UNIT_NAME);
}

export function renderSystemdUserUnit(options: SystemdUserUnitOptions) {
  const normalized = validateOptions(options);
  const execStart = normalized.programArguments.map(systemdValue).join(" ");
  return `[Unit]
Description=Plimsoll Collector
After=default.target

[Service]
Type=simple
ExecStart=${execStart}
WorkingDirectory=${systemdValue(normalized.workingDirectory)}
Environment=PATH=${systemdValue(normalized.environmentPath)}
Environment=PLIMSOLL_COLLECTOR_DATA_MODE=metadata
Environment=PLIMSOLL_HOME=${systemdValue(normalized.collectorHome)}
Restart=on-failure
RestartSec=30

[Install]
WantedBy=default.target
`;
}

function ensurePrivateDirectory(directory: string) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stat = fs.lstatSync(directory);
  if (stat.isSymbolicLink() || !stat.isDirectory()) throw new Error("UNSAFE_SYSTEMD_DIRECTORY");
  const uid = typeof process.getuid === "function" ? process.getuid() : undefined;
  if (uid !== undefined && stat.uid !== uid) throw new Error("UNSAFE_SYSTEMD_DIRECTORY_OWNER");
  if ((stat.mode & 0o022) !== 0) {
    fs.chmodSync(directory, 0o700);
  }
  const checked = fs.lstatSync(directory);
  if ((checked.mode & 0o7777) !== 0o700) throw new Error("UNSAFE_SYSTEMD_DIRECTORY_MODE");
}

function defaultExecute(command: string, args: readonly string[]): CommandResult {
  const result = spawnSync(command, [...args], { stdio: "inherit" });
  return {
    status: result.status,
    error: result.error,
  };
}

function writeUnitIfNeeded(unitPath: string, content: string) {
  let existingStat: fs.Stats | null = null;
  try {
    existingStat = fs.lstatSync(unitPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  if (
    existingStat &&
    (existingStat.isSymbolicLink() || !existingStat.isFile() || existingStat.nlink !== 1)
  ) {
    throw new Error("UNSAFE_SYSTEMD_UNIT");
  }
  if (existingStat && (existingStat.mode & 0o7777) !== 0o600) {
    throw new Error("SYSTEMD_UNIT_MODE_INVALID");
  }
  const existing = existingStat ? fs.readFileSync(unitPath, "utf8") : null;
  if (existing === content) {
    return false;
  }
  const temporary = path.join(
    path.dirname(unitPath),
    `.${path.basename(unitPath)}.plimsoll-${process.pid}-${Date.now()}`,
  );
  const descriptor = fs.openSync(
    temporary,
    fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL,
    0o600,
  );
  try {
    fs.writeFileSync(descriptor, content, "utf8");
    fs.fsyncSync(descriptor);
    fs.closeSync(descriptor);
    fs.renameSync(temporary, unitPath);
    fs.chmodSync(unitPath, 0o600);
  } catch (error) {
    try {
      fs.closeSync(descriptor);
    } catch {
      // The descriptor may already be closed.
    }
    try {
      fs.unlinkSync(temporary);
    } catch {
      // Preserve the original failure.
    }
    throw error;
  }
  return true;
}

export function installSystemdUserUnit(options: SystemdUserUnitOptions): SystemdUserUnitInstallResult {
  const normalized = validateOptions(options);
  const unitPath = systemdUserUnitPath(normalized.homeDir);
  const content = renderSystemdUserUnit(normalized);
  const unitDigest = digest(content);
  const enable = options.enable !== false;
  const commands = enable
    ? [
        ["systemctl", "--user", "daemon-reload"],
        ["systemctl", "--user", "enable", "--now", SYSTEMD_USER_UNIT_NAME],
      ] as const
    : [] as const;

  if (options.dryRun) {
    return {
      unitPath,
      receipt: {
        schema: "plimsoll.systemd-user-unit-install.v1",
        operation: "install",
        status: "preview",
        target: "systemd_user_unit",
        unitName: SYSTEMD_USER_UNIT_NAME,
        changed: false,
        wouldChange: !fs.existsSync(unitPath) || fs.readFileSync(unitPath, "utf8") !== content,
        enabled: false,
        wouldEnable: enable,
        enableStatus: enable ? "preview" : "not_requested",
        unitDigest,
        unitMode: "0600",
        privacyMode: "metadata_only",
        collectorHomeIdentityHash: collectorHomeIdentityHash(normalized.collectorHome),
        commands,
      },
    };
  }

  ensurePrivateDirectory(systemdUserDirectory(normalized.homeDir));
  const changed = writeUnitIfNeeded(unitPath, content);
  const execute = options.execute ?? defaultExecute;
  let enabled = false;
  let enableStatus: SystemdUserUnitInstallReceipt["enableStatus"] = enable
    ? "systemctl_failed"
    : "not_requested";
  if (enable) {
    const reload = execute(commands[0]![0]!, commands[0]!.slice(1));
    const start = reload.status === 0
      ? execute(commands[1]![0]!, commands[1]!.slice(1))
      : { status: reload.status, error: reload.error };
    enabled = reload.status === 0 && start.status === 0;
    enableStatus = enabled ? "enabled" : "systemctl_failed";
  }
  return {
    unitPath,
    receipt: {
      schema: "plimsoll.systemd-user-unit-install.v1",
      operation: "install",
      status: changed ? "installed" : "unchanged",
      target: "systemd_user_unit",
      unitName: SYSTEMD_USER_UNIT_NAME,
      changed,
      wouldChange: changed,
      enabled,
      wouldEnable: enable,
      enableStatus,
      unitDigest,
      unitMode: "0600",
      privacyMode: "metadata_only",
      collectorHomeIdentityHash: collectorHomeIdentityHash(normalized.collectorHome),
      commands,
    },
  };
}

export type SystemdUserUnitState = {
  platform: "linux";
  target: "systemd_user_unit";
  ok: boolean;
  installed: boolean;
  status: "missing" | "invalid" | "conflicted" | "valid";
  unitPath: string;
  unitName: typeof SYSTEMD_USER_UNIT_NAME;
  unitDigest: string | null;
  enabled: boolean;
  homeIdentity: {
    ok: boolean;
    expectedHash: string;
    observedHash: string | null;
  } | null;
};

export function readSystemdUserUnitState(options: {
  homeDir?: string;
  env?: NodeJS.ProcessEnv;
} = {}): SystemdUserUnitState {
  const homeDir = path.resolve(options.homeDir ?? os.homedir());
  const env = options.env ?? process.env;
  const unitPath = systemdUserUnitPath(homeDir);
  const enabled = fs.existsSync(systemdUserWantsPath(homeDir));
  const expectedHome = resolveCollectorHome({ env, homeDir, platform: "linux" }).home;
  const expectedHash = collectorHomeIdentityHash(expectedHome);
  if (!fs.existsSync(unitPath)) {
    return {
      platform: "linux",
      target: "systemd_user_unit",
      ok: false,
      installed: false,
      status: "missing",
      unitPath,
      unitName: SYSTEMD_USER_UNIT_NAME,
      unitDigest: null,
      enabled,
      homeIdentity: null,
    };
  }
  try {
    const content = fs.readFileSync(unitPath, "utf8");
    const observedHomeValue = content.match(/^Environment=PLIMSOLL_HOME=(.+)$/m)?.[1] ?? null;
    const observedHome = observedHomeValue ? parseSystemdValue(observedHomeValue) : null;
    const valid =
      content.includes("[Unit]\n") &&
      content.includes("[Service]\n") &&
      content.includes("ExecStart=") &&
      content.includes("Restart=on-failure\n") &&
      content.includes("WantedBy=default.target\n") &&
      content.includes("Environment=PATH=") &&
      content.includes("Environment=PLIMSOLL_COLLECTOR_DATA_MODE=metadata\n") &&
      observedHomeValue === systemdValue(expectedHome) &&
      !content.includes("launchctl");
    const stat = fs.lstatSync(unitPath);
    const modeValid = stat.isFile() && !stat.isSymbolicLink() && (stat.mode & 0o7777) === 0o600;
    const homeOk = observedHome === expectedHome;
    return {
      platform: "linux",
      target: "systemd_user_unit",
      ok: valid && modeValid,
      installed: true,
      status: !modeValid ? "invalid" : valid ? "valid" : "conflicted",
      unitPath,
      unitName: SYSTEMD_USER_UNIT_NAME,
      unitDigest: digest(content),
      enabled,
      homeIdentity: {
        ok: homeOk,
        expectedHash,
        observedHash: observedHome ? collectorHomeIdentityHash(observedHome) : null,
      },
    };
  } catch {
    return {
      platform: "linux",
      target: "systemd_user_unit",
      ok: false,
      installed: true,
      status: "invalid",
      unitPath,
      unitName: SYSTEMD_USER_UNIT_NAME,
      unitDigest: null,
      enabled,
      homeIdentity: null,
    };
  }
}

export function systemdUserEnableCommand() {
  return ["systemctl", "--user", "enable", "--now", SYSTEMD_USER_UNIT_NAME] as const;
}

export function systemdUserStopCommand() {
  return ["systemctl", "--user", "stop", SYSTEMD_USER_UNIT_NAME] as const;
}
