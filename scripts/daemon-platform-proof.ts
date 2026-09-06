import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  daemonBackend,
  installSystemdUserUnit,
  readSystemdUserUnitState,
  renderSystemdUserUnit,
  systemdUserUnitPath,
} from "../packages/collector-cli/src/daemon/index.ts";
import {
  defaultCollectorHome,
  resolveCollectorHome,
} from "../packages/collector-cli/src/collector-home.ts";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-daemon-platform-proof-"));

try {
  const installer = fs.readFileSync(path.join(process.cwd(), "install.sh"), "utf8");
  const workflow = fs.readFileSync(path.join(process.cwd(), ".github", "workflows", "proof.yml"), "utf8");
  assert.match(installer, /Linux\)/);
  assert.match(installer, /install-daemon --dev/);
  assert.match(installer, /systemctl is required/);
  assert.match(workflow, /os: \[ubuntu-latest, macos-14\]/);

  const homeDir = path.join(root, "home");
  const xdgDataHome = path.join(root, "xdg-data");
  fs.mkdirSync(homeDir, { mode: 0o700 });

  assert.equal(
    defaultCollectorHome(homeDir, "linux", { XDG_DATA_HOME: xdgDataHome }),
    path.join(xdgDataHome, "plimsoll"),
  );
  assert.equal(
    resolveCollectorHome({ platform: "linux", env: { HOME: homeDir }, homeDir }).home,
    path.join(homeDir, ".local", "share", "plimsoll"),
  );
  assert.equal(
    defaultCollectorHome(homeDir, "darwin", { XDG_DATA_HOME: xdgDataHome }),
    path.join(homeDir, "Library", "Application Support", "Plimsoll"),
  );
  assert.equal(
    defaultCollectorHome(homeDir, "linux", { XDG_DATA_HOME: "relative-xdg" }),
    path.join(homeDir, ".local", "share", "plimsoll"),
  );

  assert.equal(daemonBackend("linux").kind, "systemd_user");
  assert.equal(daemonBackend("darwin").kind, "launch_agent");

  const programArguments = ["/usr/bin/node", "/opt/plimsoll/cli.mjs", "start"];
  const unit = renderSystemdUserUnit({
    platform: "linux",
    homeDir,
    repoRoot: "/opt/plimsoll",
    programArguments,
    workingDirectory: "/opt/plimsoll",
    env: { HOME: homeDir, XDG_DATA_HOME: xdgDataHome },
  });
  assert.match(unit, /^\[Unit\]$/m);
  assert.match(unit, /^ExecStart=/m);
  assert.match(unit, /^Environment=PATH=/m);
  assert.match(unit, /Environment=PLIMSOLL_COLLECTOR_DATA_MODE=metadata/);
  assert.match(unit, new RegExp(`Environment=PLIMSOLL_HOME=${xdgDataHome.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/plimsoll`));
  assert.match(unit, /^Restart=on-failure$/m);
  assert.match(unit, /^WantedBy=default\.target$/m);

  const commands: string[][] = [];
  const installed = installSystemdUserUnit({
    platform: "linux",
    homeDir,
    repoRoot: "/opt/plimsoll",
    programArguments,
    workingDirectory: "/opt/plimsoll",
    env: { HOME: homeDir, XDG_DATA_HOME: xdgDataHome },
    execute: (command, args) => {
      commands.push([command, ...args]);
      return { status: 0 };
    },
  });
  assert.equal(installed.receipt.status, "installed");
  assert.equal(installed.receipt.enabled, true);
  assert.equal(installed.unitPath, systemdUserUnitPath(homeDir));
  assert.equal(fs.readFileSync(installed.unitPath, "utf8"), unit);
  assert.equal(fs.statSync(installed.unitPath).mode & 0o7777, 0o600);
  assert.deepEqual(commands, [
    ["systemctl", "--user", "daemon-reload"],
    ["systemctl", "--user", "enable", "--now", "plimsoll-collector.service"],
  ]);

  const repeated = installSystemdUserUnit({
    platform: "linux",
    homeDir,
    repoRoot: "/opt/plimsoll",
    programArguments,
    workingDirectory: "/opt/plimsoll",
    env: { HOME: homeDir, XDG_DATA_HOME: xdgDataHome },
    execute: (command, args) => {
      commands.push([command, ...args]);
      return { status: 0 };
    },
  });
  assert.equal(repeated.receipt.status, "unchanged");
  assert.equal(repeated.receipt.enabled, true);

  const state = readSystemdUserUnitState({
    homeDir,
    env: { HOME: homeDir, XDG_DATA_HOME: xdgDataHome },
  });
  assert.equal(state.status, "valid");
  assert.equal(state.ok, true);
  assert.equal(state.homeIdentity?.ok, true);

  const cli = path.join(process.cwd(), "packages", "collector-cli", "src", "cli.ts");
  const tsx = path.join(process.cwd(), "node_modules", "tsx", "dist", "cli.mjs");
  const cliHome = path.join(root, "cli-home");
  const fakeBin = path.join(root, "fake-bin");
  const fakePnpm = path.join(fakeBin, "pnpm");
  fs.mkdirSync(cliHome, { mode: 0o700 });
  fs.mkdirSync(fakeBin, { mode: 0o700 });
  fs.writeFileSync(fakePnpm, "#!/bin/sh\nexit 0\n", { mode: 0o700 });
  const cliEnv = { ...process.env, HOME: cliHome, PATH: `${fakeBin}:/usr/bin:/bin` };
  delete cliEnv.PLIMSOLL_HOME;
  let cliIntegration = "not_run_missing_tsx";
  if (fs.existsSync(tsx) && process.platform === "linux") {
    fs.writeFileSync(path.join(fakeBin, "systemctl"), "#!/bin/sh\nexit 0\n", { mode: 0o700 });
    const cliInstall = spawnSync(process.execPath, [tsx, cli, "install-daemon", "--dev", "--repo-root", process.cwd(), "--pnpm", fakePnpm], {
      cwd: process.cwd(),
      env: cliEnv,
      encoding: "utf8",
    });
    const receipt = JSON.parse(cliInstall.stdout) as Record<string, unknown>;
    assert.equal(cliInstall.status, 0);
    assert.equal(receipt.enabled, true);
    assert.equal(receipt.target, "systemd_user_unit");
    cliIntegration = "linux_apply";
  } else if (fs.existsSync(tsx)) {
    const cliPreview = spawnSync(process.execPath, [tsx, cli, "install-daemon", "--dev", "--repo-root", process.cwd(), "--pnpm", fakePnpm, "--dry-run"], {
      cwd: process.cwd(),
      env: cliEnv,
      encoding: "utf8",
    });
    const receipt = JSON.parse(cliPreview.stdout) as Record<string, unknown>;
    assert.equal(cliPreview.status, 0);
    assert.equal(receipt.status, "preview");
    assert.equal(receipt.target, "launch_agent");
    cliIntegration = "macos_preview";
  }

  console.log(JSON.stringify({
    proof: "daemon-platform",
    passed: true,
    checks: cliIntegration === "not_run_missing_tsx" ? 28 : 31,
    cliIntegration,
    unitPath: installed.unitPath,
    systemdCommands: commands,
  }, null, 2));
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
