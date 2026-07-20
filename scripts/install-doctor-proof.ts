/**
 * Focused proof for issue 0058 / GitHub #107.
 *
 * Every fixture uses a temporary HOME and PLIMSOLL_HOME. The proof stubs
 * launchctl and the installer's external commands; it never registers, loads,
 * unloads, or starts a real LaunchAgent and never reads the operator's tool
 * config, ledger, or credentials.
 */
import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import { build } from "esbuild";

import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { generateCodexConfigToml } from "../packages/collector-config/src/index";
import {
  LAUNCH_AGENT_LABEL,
  launchAgentPlistPath,
  renderLaunchAgentPlist,
} from "../packages/collector-cli/src/launch-agent";
import { readProcessStartFingerprint } from "../packages/collector-cli/src/runtime-ownership";

type CommandResult = {
  code: number | null;
  stdout: string;
  stderr: string;
};

type Check = {
  name: string;
  passed: boolean;
  detail: unknown;
};

const root = path.resolve(import.meta.dirname, "..");
const cli = path.join(root, "packages", "collector-cli", "src", "cli.ts");
const tsx = path.join(root, "node_modules", "tsx", "dist", "cli.mjs");
const installScript = path.join(root, "install.sh");
const proofWorkflow = path.join(root, ".github", "workflows", "proof.yml");
const partialCodexFixture = path.join(root, "scripts", "fixtures", "codex-partial-legacy.toml");
const checks: Check[] = [];

function check(name: string, condition: unknown, detail: unknown) {
  checks.push({ name, passed: Boolean(condition), detail });
  if (!condition) throw new Error(`${name}: ${JSON.stringify(detail)}`);
}

function command(
  executable: string,
  args: string[],
  options: { cwd: string; env: NodeJS.ProcessEnv },
): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, {
      cwd: options.cwd,
      env: options.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => { stdout += chunk; });
    child.stderr.on("data", (chunk: string) => { stderr += chunk; });
    child.once("error", reject);
    child.once("close", (code) => resolve({ code, stdout, stderr }));
  });
}

function parseJson(stdout: string) {
  return JSON.parse(stdout) as Record<string, any>;
}

function writeExecutable(file: string, content: string) {
  fs.writeFileSync(file, content, { mode: 0o700 });
}

function digestTree(directory: string): string {
  if (!fs.existsSync(directory)) return "missing";
  const hash = createHash("sha256");
  const walk = (current: string) => {
    const entries = fs.readdirSync(current, { withFileTypes: true })
      .sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      const relative = path.relative(directory, full);
      hash.update(`${entry.isDirectory() ? "d" : "f"}\0${relative}\0`);
      if (entry.isDirectory()) walk(full);
      else hash.update(fs.readFileSync(full));
    }
  };
  walk(directory);
  return hash.digest("hex");
}

function backupCount(directory: string) {
  return fs.readdirSync(directory).filter((name) => name.includes(".plimsoll-backup-")).length;
}

async function main() {
  const nodeMajor = Number(process.versions.node.split(".")[0]);
  check("proof_runs_on_node_22", nodeMajor === 22, {
    execPath: process.execPath,
    version: process.versions.node,
  });
  check("tsx_entrypoint_exists", fs.existsSync(tsx), tsx);
  const workflow = fs.readFileSync(proofWorkflow, "utf8");
  const workflowCommands = [...workflow.matchAll(/^\s+run:\s*(.+?)\s*$/gm)]
    .map((match) => match[1]);
  const requiredStandaloneGates = [
    "pnpm proof:allocation",
    "pnpm proof:install-doctor",
    "pnpm proof:launch-agent",
    "pnpm proof:codex-config-apply",
    "pnpm proof:claude-config-apply",
    "pnpm proof:git-context",
    "pnpm proof:http-boundary",
    "pnpm proof:dashboard",
    "pnpm proof:dashboard-security",
    "pnpm proof:learning-facts",
    "pnpm proof",
    "pnpm proof:metric-truth",
    "pnpm proof:outbox",
    "pnpm proof:join-isolation",
    "pnpm proof:outcome-timeline",
    "pnpm proof:resource --require-integrated --receipt evidence/resource-proof.json",
    "pnpm proof:resource-finalization",
  ];
  const gateCounts = Object.fromEntries(requiredStandaloneGates.map((gate) => [
    gate,
    workflowCommands.filter((command) => command === gate).length,
  ]));
  check(
    "ci_workflow_runs_every_required_standalone_gate_once",
    Object.values(gateCounts).every((count) => count === 1),
    { workflow: proofWorkflow, gateCounts },
  );

  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-install-doctor-proof-"));
  const packagedCli = path.join(root, "packages", "collector-cli", "dist", "install-doctor-proof-cli.mjs");
  let server: http.Server | undefined;

  try {
  fs.mkdirSync(path.dirname(packagedCli), { recursive: true });
  await build({
    bundle: true,
    entryPoints: [cli],
    external: ["better-sqlite3"],
    format: "esm",
    outfile: packagedCli,
    platform: "node",
    target: "node20",
  });
  check("packaged_fixture_built_on_node_22", fs.existsSync(packagedCli), {
    execPath: process.execPath,
    version: process.versions.node,
    packagedCli,
  });

  const neutralCwd = path.join(sandbox, "neutral-cwd");
  const stubBin = path.join(sandbox, "stub-bin");
  const unreachableFetchFixture = path.join(sandbox, "unreachable-fetch.mjs");
  const commandLog = path.join(sandbox, "commands.log");
  const launchctlLog = path.join(sandbox, "launchctl.log");
  fs.mkdirSync(neutralCwd, { recursive: true });
  fs.mkdirSync(stubBin, { recursive: true });
  fs.writeFileSync(
    unreachableFetchFixture,
    "globalThis.fetch = async () => { throw new TypeError('synthetic unreachable'); };\n",
    { mode: 0o600 },
  );
  fs.symlinkSync(process.execPath, path.join(stubBin, "node"));
  writeExecutable(
    path.join(stubBin, "launchctl"),
    `#!/bin/sh\nprintf '%s\\n' "$*" >> "$PLIMSOLL_LAUNCHCTL_LOG"\nexit 97\n`,
  );
  writeExecutable(
    path.join(stubBin, "git"),
    `#!/bin/sh
printf 'git %s\\n' "$*" >> "$PLIMSOLL_COMMAND_LOG"
if [ "$1" = "clone" ]; then mkdir -p "$3/.git"; fi
exit 0
`,
  );
  writeExecutable(
    path.join(stubBin, "pnpm"),
    `#!/bin/sh
printf 'pnpm %s\\n' "$*" >> "$PLIMSOLL_COMMAND_LOG"
case " $* " in *" collector doctor --read-only --json "*) exit 17 ;; esac
exit 0
`,
  );
  const isolatedPath = `${stubBin}:/usr/bin:/bin`;
  const commonEnv = {
    ...process.env,
    PATH: isolatedPath,
    PLIMSOLL_COMMAND_LOG: commandLog,
    PLIMSOLL_LAUNCHCTL_LOG: launchctlLog,
  };

  const dryHome = path.join(sandbox, "dry-home");
  const dryPlimsoll = path.join(sandbox, "dry-plimsoll");
  const dryTarget = path.join(sandbox, "dry-target");
  const dryRun = await command("/bin/bash", [installScript, "--dry-run"], {
    cwd: neutralCwd,
    env: {
      ...commonEnv,
      HOME: dryHome,
      PLIMSOLL_HOME: dryPlimsoll,
      PLIMSOLL_DIR: dryTarget,
    },
  });
  check("source_installer_dry_run_succeeds", dryRun.code === 0, dryRun);
  check(
    "source_installer_dry_run_is_node_22",
    dryRun.stdout.includes(`Node: ${process.versions.node} (supported: >=20 <25)`),
    dryRun.stdout,
  );
  check(
    "source_installer_plans_supported_dev_path",
    dryRun.stdout.includes("install-launch-agent --dev --repo-root"),
    dryRun.stdout,
  );
  check(
    "source_installer_plans_strict_read_only_doctor",
    dryRun.stdout.includes("doctor --read-only --json (failure stops installation)"),
    dryRun.stdout,
  );
  check(
    "source_installer_dry_run_creates_nothing",
    !fs.existsSync(dryHome) && !fs.existsSync(dryPlimsoll) && !fs.existsSync(dryTarget),
    { dryHome, dryPlimsoll, dryTarget },
  );
  check("source_installer_dry_run_invokes_no_external_commands", !fs.existsSync(commandLog), commandLog);
  check(
    "source_installer_node_22_dry_run_is_zero_mutation",
    dryRun.code === 0 &&
      !fs.existsSync(dryHome) &&
      !fs.existsSync(dryPlimsoll) &&
      !fs.existsSync(dryTarget) &&
      !fs.existsSync(commandLog),
    { code: dryRun.code, dryHome, dryPlimsoll, dryTarget, commandLog },
  );

  const installHome = path.join(sandbox, "install-home");
  const installPlimsoll = path.join(sandbox, "install-plimsoll");
  const installTarget = path.join(sandbox, "install-target");
  const failedInstall = await command("/bin/bash", [installScript], {
    cwd: neutralCwd,
    env: {
      ...commonEnv,
      HOME: installHome,
      PLIMSOLL_HOME: installPlimsoll,
      PLIMSOLL_DIR: installTarget,
    },
  });
  const installCommands = fs.readFileSync(commandLog, "utf8");
  check("source_installer_fails_closed_on_doctor", failedInstall.code === 17, failedInstall);
  check(
    "source_installer_executes_supported_dev_path",
    installCommands.includes("collector install-launch-agent --dev --repo-root"),
    installCommands,
  );
  check(
    "source_installer_does_not_swallow_doctor_failure",
    installCommands.trimEnd().endsWith("collector doctor --read-only --json"),
    installCommands,
  );

  for (const major of [19, 25]) {
    const unsupportedBin = path.join(sandbox, `unsupported-bin-${major}`);
    const unsupportedHome = path.join(sandbox, `unsupported-home-${major}`);
    const unsupportedPlimsoll = path.join(sandbox, `unsupported-plimsoll-${major}`);
    const unsupportedTarget = path.join(sandbox, `unsupported-target-${major}`);
    const unsupportedCommandLog = path.join(sandbox, `unsupported-commands-${major}.log`);
    fs.mkdirSync(unsupportedBin);
    writeExecutable(
      path.join(unsupportedBin, "node"),
      `#!/bin/sh
case "$2" in
  *Number*) echo ${major} ;;
  *) echo ${major}.0.0 ;;
esac
`,
    );
    fs.symlinkSync(path.join(stubBin, "git"), path.join(unsupportedBin, "git"));
    fs.symlinkSync(path.join(stubBin, "pnpm"), path.join(unsupportedBin, "pnpm"));
    const unsupported = await command("/bin/bash", [installScript, "--dry-run"], {
      cwd: neutralCwd,
      env: {
        ...commonEnv,
        HOME: unsupportedHome,
        PATH: `${unsupportedBin}:/usr/bin:/bin`,
        PLIMSOLL_COMMAND_LOG: unsupportedCommandLog,
        PLIMSOLL_HOME: unsupportedPlimsoll,
        PLIMSOLL_DIR: unsupportedTarget,
      },
    });
    check(
      `source_installer_rejects_node_${major}_without_mutation`,
      unsupported.code !== 0 &&
        unsupported.stderr.includes(`Unsupported Node ${major}.0.0`) &&
        unsupported.stderr.includes("requires >=20 <25") &&
        !fs.existsSync(unsupportedHome) &&
        !fs.existsSync(unsupportedPlimsoll) &&
        !fs.existsSync(unsupportedTarget) &&
        !fs.existsSync(unsupportedCommandLog),
      { ...unsupported, unsupportedHome, unsupportedPlimsoll, unsupportedTarget, unsupportedCommandLog },
    );
  }

  const blankHome = path.join(sandbox, "blank-home");
  const blankPlimsoll = path.join(sandbox, "blank-plimsoll");
  const doctorBaseEnv = {
    ...process.env,
    HOME: blankHome,
    PATH: isolatedPath,
    PLIMSOLL_HOME: blankPlimsoll,
    PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "100",
    PLIMSOLL_LAUNCHCTL_LOG: launchctlLog,
  };
  const blankDoctorEnv = {
    ...doctorBaseEnv,
    NODE_OPTIONS: [process.env.NODE_OPTIONS, `--import=${unreachableFetchFixture}`]
      .filter(Boolean)
      .join(" "),
  };
  const blankDoctor = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: blankDoctorEnv },
  );
  if (blankDoctor.stdout.trim().length === 0) {
    const stderrFrames = [...blankDoctor.stderr.matchAll(/\s+at\s+([A-Za-z0-9_.<>]+)/g)]
      .slice(0, 8)
      .map((match) => match[1]);
    throw new Error(`blank_doctor_empty_json: ${JSON.stringify({
      code: blankDoctor.code,
      stdoutBytes: Buffer.byteLength(blankDoctor.stdout),
      stderrBytes: Buffer.byteLength(blankDoctor.stderr),
      stderrDigest: createHash("sha256").update(blankDoctor.stderr).digest("hex"),
      childCommandErrorCode: blankDoctor.stderr.match(/\b(ERR_[A-Z0-9_]+|E[A-Z]{3,})\b/)?.[1] ?? "none",
      childCommandErrorClass: blankDoctor.stderr.match(/\b([A-Z][A-Za-z]+Error)\b/)?.[1] ?? "none",
      stderrFrames,
    })}`);
  }
  const blankReceipt = parseJson(blankDoctor.stdout);
  check("blank_doctor_fails", blankDoctor.code !== 0 && blankReceipt.ok === false, blankReceipt);
  check("blank_doctor_reports_not_installed", blankReceipt.readiness === "not_installed", blankReceipt);
  check(
    "blank_doctor_creates_no_home_or_plimsoll_directory",
    !fs.existsSync(blankHome) && !fs.existsSync(blankPlimsoll),
    { blankHome, blankPlimsoll },
  );

  const packagedBlankHome = path.join(sandbox, "packaged-blank-home");
  const packagedBlankPlimsoll = path.join(sandbox, "packaged-blank-plimsoll");
  const packagedBlankDoctor = await command(
    process.execPath,
    [packagedCli, "doctor", "--read-only", "--json"],
    {
      cwd: neutralCwd,
      env: {
        ...blankDoctorEnv,
        HOME: packagedBlankHome,
        PLIMSOLL_HOME: packagedBlankPlimsoll,
      },
    },
  );
  const packagedBlankReceipt = parseJson(packagedBlankDoctor.stdout);
  check(
    "packaged_blank_doctor_fails_without_mutation",
    packagedBlankDoctor.code !== 0 &&
      packagedBlankReceipt.readiness === "not_installed" &&
      !fs.existsSync(packagedBlankHome) &&
      !fs.existsSync(packagedBlankPlimsoll),
    packagedBlankReceipt,
  );

  const cleanSetupHome = path.join(sandbox, "clean-setup-home");
  const cleanSetupPlimsoll = path.join(sandbox, "clean-setup-plimsoll");
  const cleanSetup = await command(
    process.execPath,
    [tsx, cli, "setup", "--dry-run"],
    {
      cwd: neutralCwd,
      env: {
        ...doctorBaseEnv,
        HOME: cleanSetupHome,
        PLIMSOLL_HOME: cleanSetupPlimsoll,
      },
    },
  );
  check(
    "fresh_home_setup_dry_run_is_byte_absent_preview",
    cleanSetup.code === 0 &&
      cleanSetup.stdout.includes('"status":"setup_dry_run"') &&
      !fs.existsSync(cleanSetupHome) &&
      !fs.existsSync(cleanSetupPlimsoll),
    {
      ...cleanSetup,
      homeExists: fs.existsSync(cleanSetupHome),
      plimsollHomeExists: fs.existsSync(cleanSetupPlimsoll),
    },
  );

  const rejectedSetupHome = path.join(sandbox, "rejected-setup-home");
  const rejectedSetupPlimsoll = path.join(sandbox, "rejected-setup-plimsoll");
  const rejectedToolDir = path.join(sandbox, "rejected-tool-config");
  const rejectedClaude = path.join(rejectedToolDir, "settings.json");
  const rejectedCodex = path.join(rejectedToolDir, "config.toml");
  fs.mkdirSync(rejectedToolDir);
  const malformedCodex = '[otel]\nenvironment = "first"\n[otel]\nenvironment = "duplicate"\n';
  fs.writeFileSync(rejectedCodex, malformedCodex);
  const rejectedSetup = await command(
    process.execPath,
    [
      tsx,
      cli,
      "setup",
      "--yes",
      "--claude-settings",
      rejectedClaude,
      "--codex-config",
      rejectedCodex,
    ],
    {
      cwd: neutralCwd,
      env: {
        ...doctorBaseEnv,
        HOME: rejectedSetupHome,
        PLIMSOLL_HOME: rejectedSetupPlimsoll,
      },
    },
  );
  check(
    "invalid_plan_blocks_default_config_and_all_tool_writes",
    rejectedSetup.code !== 0 &&
      rejectedSetup.stderr.includes("existing Codex config.toml is invalid") &&
      !fs.existsSync(rejectedSetupHome) &&
      !fs.existsSync(rejectedSetupPlimsoll) &&
      !fs.existsSync(rejectedClaude) &&
      fs.readFileSync(rejectedCodex, "utf8") === malformedCodex &&
      backupCount(rejectedToolDir) === 0,
    {
      ...rejectedSetup,
      homeExists: fs.existsSync(rejectedSetupHome),
      plimsollHomeExists: fs.existsSync(rejectedSetupPlimsoll),
    },
  );

  const freshApplyHome = path.join(sandbox, "fresh-apply-home");
  const freshApplyPlimsoll = path.join(sandbox, "fresh-apply-plimsoll");
  const freshToolDir = path.join(sandbox, "fresh-tool-config");
  const freshClaude = path.join(freshToolDir, "settings.json");
  const freshCodex = path.join(freshToolDir, "config.toml");
  fs.mkdirSync(freshToolDir);
  const freshApply = await command(
    process.execPath,
    [
      tsx,
      cli,
      "setup",
      "--yes",
      "--claude-settings",
      freshClaude,
      "--codex-config",
      freshCodex,
    ],
    {
      cwd: neutralCwd,
      env: {
        ...doctorBaseEnv,
        HOME: freshApplyHome,
        PLIMSOLL_HOME: freshApplyPlimsoll,
      },
    },
  );
  check(
    "fresh_apply_creates_default_only_after_valid_plan",
    freshApply.code === 0 &&
      freshApply.stdout.includes('"status": "setup_applied"') &&
      fs.existsSync(path.join(freshApplyPlimsoll, "collector.config.json")) &&
      fs.existsSync(freshClaude) &&
      fs.existsSync(freshCodex) &&
      !fs.existsSync(freshApplyHome),
    {
      ...freshApply,
      homeExists: fs.existsSync(freshApplyHome),
      defaultConfigExists: fs.existsSync(path.join(freshApplyPlimsoll, "collector.config.json")),
    },
  );

  const fixtureHome = path.join(sandbox, "fixture-home");
  const fixturePlimsoll = path.join(sandbox, "fixture-plimsoll");
  const claudeDir = path.join(fixtureHome, ".claude");
  const codexDir = path.join(fixtureHome, ".codex");
  fs.mkdirSync(fixturePlimsoll, { recursive: true, mode: 0o700 });
  fs.mkdirSync(claudeDir, { recursive: true, mode: 0o700 });
  fs.mkdirSync(codexDir, { recursive: true, mode: 0o700 });
  const credentialSentinel = "CREDENTIAL_SENTINEL_MUST_STAY_LOCAL";
  const claudeCredential = path.join(claudeDir, ".credentials.json");
  const codexCredential = path.join(codexDir, "auth.json");
  fs.writeFileSync(claudeCredential, credentialSentinel, { mode: 0o600 });
  fs.writeFileSync(codexCredential, credentialSentinel, { mode: 0o600 });
  const claudeSettings = path.join(claudeDir, "settings.json");
  const codexConfig = path.join(codexDir, "config.toml");
  fs.writeFileSync(claudeSettings, JSON.stringify({ existing: { keep: true } }, null, 2) + "\n");

  server = http.createServer();
  await new Promise<void>((resolve, reject) => {
    server!.once("error", reject);
    server!.listen(0, "127.0.0.1", resolve);
  });
  const port = (server.address() as AddressInfo).port;
  const config = collectorConfigSchema.parse({ port });
  fs.writeFileSync(
    path.join(fixturePlimsoll, "collector.config.json"),
    JSON.stringify(config, null, 2) + "\n",
    { mode: 0o600 },
  );
  const fixtureEnv = {
    ...process.env,
    HOME: fixtureHome,
    PATH: isolatedPath,
    PLIMSOLL_HOME: fixturePlimsoll,
    PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "500",
    PLIMSOLL_LAUNCHCTL_LOG: launchctlLog,
  };
  const partialCodexConfig = fs.readFileSync(partialCodexFixture, "utf8")
    .replaceAll("__PLIMSOLL_PORT__", String(port));
  fs.writeFileSync(codexConfig, partialCodexConfig);
  const firstSetup = await command(
    process.execPath,
    [tsx, cli, "setup", "--yes"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  check("isolated_setup_applies", firstSetup.code === 0, firstSetup);
  const firstBackups = backupCount(claudeDir) + backupCount(codexDir);
  const secondSetup = await command(
    process.execPath,
    [tsx, cli, "setup", "--yes"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  check(
    "isolated_setup_is_idempotent",
    secondSetup.code === 0 && secondSetup.stdout.includes('"status":"setup_noop"'),
    secondSetup,
  );
  check(
    "isolated_setup_writes_no_second_backup",
    firstBackups === 2 && backupCount(claudeDir) + backupCount(codexDir) === firstBackups,
    { firstBackups, finalBackups: backupCount(claudeDir) + backupCount(codexDir) },
  );
  check(
    "isolated_setup_preserves_existing_config",
    JSON.parse(fs.readFileSync(claudeSettings, "utf8")).existing.keep === true &&
      fs.readFileSync(codexConfig, "utf8").includes('model = "synthetic-neutral-model"') &&
      fs.readFileSync(codexConfig, "utf8").includes('state = "synthetic-existing-state"') &&
      fs.readFileSync(codexConfig, "utf8").includes('command = "printf synthetic-operator-hook"') &&
      fs.readFileSync(codexConfig, "utf8").includes('"x-synthetic-operator" = "keep"') &&
      !fs.readFileSync(codexConfig, "utf8").includes("x-cfo-one-source"),
    { claudeSettings, codexConfig },
  );
  check(
    "isolated_setup_does_not_copy_credentials",
    fs.readFileSync(claudeCredential, "utf8") === credentialSentinel &&
      fs.readFileSync(codexCredential, "utf8") === credentialSentinel &&
      !fs.readFileSync(claudeSettings, "utf8").includes(credentialSentinel) &&
      !fs.readFileSync(codexConfig, "utf8").includes(credentialSentinel),
    { claudeCredential, codexCredential },
  );

  const configuredDoctor = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  const configuredReceipt = parseJson(configuredDoctor.stdout);
  check(
    "doctor_distinguishes_configured_from_service_ready",
    configuredDoctor.code !== 0 &&
      configuredReceipt.ok === false &&
      configuredReceipt.readiness === "configured",
    configuredReceipt,
  );

  const plistPath = launchAgentPlistPath(fixtureHome);
  fs.mkdirSync(path.dirname(plistPath), { recursive: true, mode: 0o700 });
  const previousPlimsollHome = process.env.PLIMSOLL_HOME;
  process.env.PLIMSOLL_HOME = fixturePlimsoll;
  const renderedFixturePlist = renderLaunchAgentPlist({
    homeDir: fixtureHome,
    pnpmPath: "/neutral/bin/pnpm",
    repoRoot: "/neutral/plimsoll/source",
  });
  if (previousPlimsollHome === undefined) delete process.env.PLIMSOLL_HOME;
  else process.env.PLIMSOLL_HOME = previousPlimsollHome;
  fs.writeFileSync(
    plistPath,
    renderedFixturePlist,
    { mode: 0o600 },
  );
  const fingerprint = readProcessStartFingerprint(process.pid);
  check("proof_runtime_fingerprint_available", Boolean(fingerprint), { pid: process.pid });
  const runtimeIdentity = {
    instanceId: randomUUID(),
    pid: process.pid,
    processStartFingerprint: fingerprint!,
  };
  fs.writeFileSync(
    path.join(fixturePlimsoll, "collector.pid"),
    JSON.stringify({
      ...runtimeIdentity,
      command: ["neutral-plimsoll", "start"],
      cwd: neutralCwd,
      label: LAUNCH_AGENT_LABEL,
      startedAt: new Date().toISOString(),
      version: 2,
    }, null, 2) + "\n",
    { mode: 0o600 },
  );

  let tokenSignal = false;
  server.removeAllListeners("request");
  server.on("request", (_request, response) => {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      ok: true,
      runtimeIdentity,
      stats: { tokenAttributedEvents: tokenSignal ? 1 : 0 },
      health: {
        sources: [
          {
            source: "codex",
            lastTokenEventAt: tokenSignal ? "2026-07-17T00:00:00.000Z" : null,
          },
        ],
      },
    }));
  });

  const beforeColdDoctor = digestTree(sandbox);
  const coldDoctor = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  const coldReceipt = parseJson(coldDoctor.stdout);
  check(
    "cold_service_is_not_signal_verified",
    coldDoctor.code !== 0 && coldReceipt.ok === false && coldReceipt.readiness === "service_ready",
    coldReceipt,
  );
  check(
    "configured_doctor_is_byte_read_only",
    digestTree(sandbox) === beforeColdDoctor,
    { before: beforeColdDoctor, after: digestTree(sandbox) },
  );

  const fullCodexConfig = fs.readFileSync(codexConfig, "utf8");
  const generatedCodex = generateCodexConfigToml({ repoRoot: neutralCwd, port });
  const traceSection = generatedCodex.match(
    /\[otel\.trace_exporter\."otlp-http"\][\s\S]*?(?=\n\[otel\.metrics_exporter)/,
  )?.[0];
  check("proof_finds_generated_trace_section", Boolean(traceSection), generatedCodex);
  fs.writeFileSync(codexConfig, fullCodexConfig.replace(traceSection!, ""));
  const incompleteDoctor = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  const incompleteReceipt = parseJson(incompleteDoctor.stdout);
  check(
    "doctor_requires_full_codex_telemetry_config",
    incompleteDoctor.code !== 0 &&
      incompleteReceipt.readiness === "not_installed" &&
      incompleteReceipt.telemetry.codex.status === "incomplete",
    incompleteReceipt,
  );
  fs.writeFileSync(codexConfig, fullCodexConfig);

  tokenSignal = true;
  fs.writeFileSync(codexConfig, `${fullCodexConfig}\nthis is not valid TOML ???\n`);
  const beforeMalformedTomlDoctor = digestTree(sandbox);
  const malformedTomlDoctor = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  const malformedTomlReceipt = parseJson(malformedTomlDoctor.stdout);
  check(
    "malformed_codex_toml_fails_closed_with_live_signal",
    malformedTomlDoctor.code !== 0 &&
      malformedTomlReceipt.ok === false &&
      malformedTomlReceipt.readiness === "not_installed" &&
      malformedTomlReceipt.telemetry.codex.status === "invalid" &&
      digestTree(sandbox) === beforeMalformedTomlDoctor,
    malformedTomlReceipt,
  );
  fs.writeFileSync(codexConfig, fullCodexConfig);

  fs.writeFileSync(codexConfig, `${fullCodexConfig}\n[otel]\nenvironment = "duplicate"\n`);
  const duplicateTomlDoctor = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  const duplicateTomlReceipt = parseJson(duplicateTomlDoctor.stdout);
  check(
    "duplicate_codex_toml_fails_closed_with_live_signal",
    duplicateTomlDoctor.code !== 0 &&
      duplicateTomlReceipt.ok === false &&
      duplicateTomlReceipt.telemetry.codex.status === "invalid",
    duplicateTomlReceipt,
  );
  fs.writeFileSync(codexConfig, fullCodexConfig);

  const wrongTypeCodexConfig = fullCodexConfig.replace("hooks = true", 'hooks = "true"');
  check("proof_builds_wrong_type_toml_fixture", wrongTypeCodexConfig !== fullCodexConfig, codexConfig);
  fs.writeFileSync(codexConfig, wrongTypeCodexConfig);
  const wrongTypeTomlDoctor = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  const wrongTypeTomlReceipt = parseJson(wrongTypeTomlDoctor.stdout);
  check(
    "wrong_type_codex_toml_fails_closed_with_live_signal",
    wrongTypeTomlDoctor.code !== 0 &&
      wrongTypeTomlReceipt.ok === false &&
      wrongTypeTomlReceipt.telemetry.codex.status === "incomplete",
    wrongTypeTomlReceipt,
  );
  fs.writeFileSync(codexConfig, fullCodexConfig);

  const validPlist = fs.readFileSync(plistPath, "utf8");
  const pathMatch = validPlist.match(/<key>PATH<\/key>\s*<string>([\s\S]*?)<\/string>/);
  check("proof_extracts_valid_launch_agent_path", Boolean(pathMatch?.[1]), plistPath);
  const validLaunchAgentPath = pathMatch![1]!;
  const replaceLaunchAgentPath = (value: string) => validPlist.replace(
    /(<key>PATH<\/key>\s*<string>)[\s\S]*?(<\/string>)/,
    `$1${value}$2`,
  );
  const runPathFixture = async (name: string, pathValue: string) => {
    fs.writeFileSync(plistPath, replaceLaunchAgentPath(pathValue));
    const beforeDoctor = digestTree(sandbox);
    const result = await command(
      process.execPath,
      [tsx, cli, "doctor", "--read-only", "--json"],
      { cwd: neutralCwd, env: fixtureEnv },
    );
    const receipt = parseJson(result.stdout);
    check(
      name,
      result.code !== 0 &&
        receipt.ok === false &&
        receipt.readiness === "configured" &&
        receipt.launchAgent.status === "conflicted" &&
        receipt.launchAgent.path.ok === false &&
        digestTree(sandbox) === beforeDoctor,
      receipt,
    );
  };
  const validPathEntries = validLaunchAgentPath.split(path.delimiter);
  await runPathFixture("empty_launch_agent_path_fails_closed_with_live_signal", "");
  await runPathFixture("relative_launch_agent_path_fails_closed_with_live_signal", "relative/bin");
  await runPathFixture(
    "control_character_launch_agent_path_fails_closed_with_live_signal",
    `${validLaunchAgentPath}${path.delimiter}/control&#10;path`,
  );
  await runPathFixture(
    "normalized_duplicate_launch_agent_path_fails_closed_with_live_signal",
    `${validLaunchAgentPath}${path.delimiter}${validPathEntries[0]}/`,
  );
  await runPathFixture(
    "missing_node_runtime_path_fails_closed_with_live_signal",
    validPathEntries
      .filter((entry) => path.resolve(entry) !== path.resolve(path.dirname(process.execPath)))
      .join(path.delimiter),
  );
  await runPathFixture(
    "missing_pnpm_runtime_path_fails_closed_with_live_signal",
    validPathEntries
      .filter((entry) => path.resolve(entry) !== path.resolve("/neutral/bin"))
      .join(path.delimiter),
  );
  await runPathFixture(
    "missing_supported_system_path_fails_closed_with_live_signal",
    validPathEntries
      .filter((entry) => path.resolve(entry) !== "/usr/bin")
      .join(path.delimiter),
  );
  fs.writeFileSync(plistPath, validPlist);

  const malformedPlist = validPlist.replace("</plist>", "<broken>");
  check("proof_builds_malformed_plist_fixture", malformedPlist !== validPlist, plistPath);
  fs.writeFileSync(plistPath, malformedPlist);
  const beforeMalformedPlistDoctor = digestTree(sandbox);
  const malformedPlistDoctor = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  const malformedPlistReceipt = parseJson(malformedPlistDoctor.stdout);
  check(
    "malformed_launch_agent_fails_closed_with_live_signal",
    malformedPlistDoctor.code !== 0 &&
      malformedPlistReceipt.ok === false &&
      malformedPlistReceipt.readiness === "configured" &&
      malformedPlistReceipt.launchAgent.status === "invalid" &&
      digestTree(sandbox) === beforeMalformedPlistDoctor,
    malformedPlistReceipt,
  );
  fs.writeFileSync(plistPath, validPlist);

  const extraArgumentPlist = validPlist.replace(
    "  </array>",
    "    <string>unexpected</string>\n  </array>",
  );
  check("proof_builds_semantically_wrong_plist_fixture", extraArgumentPlist !== validPlist, plistPath);
  fs.writeFileSync(plistPath, extraArgumentPlist);
  const wrongRuntimePlistDoctor = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  const wrongRuntimePlistReceipt = parseJson(wrongRuntimePlistDoctor.stdout);
  check(
    "semantically_wrong_launch_agent_fails_closed_with_live_signal",
    wrongRuntimePlistDoctor.code !== 0 &&
      wrongRuntimePlistReceipt.ok === false &&
      wrongRuntimePlistReceipt.readiness === "configured" &&
      wrongRuntimePlistReceipt.launchAgent.status === "conflicted",
    wrongRuntimePlistReceipt,
  );
  fs.writeFileSync(plistPath, validPlist);

  const beforeSignalDoctor = digestTree(sandbox);
  const signalDoctor = await command(
    process.execPath,
    [tsx, cli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  const signalReceipt = parseJson(signalDoctor.stdout);
  check(
    "doctor_reports_signal_verified_only_with_token_signal",
    signalDoctor.code === 0 &&
      signalReceipt.ok === true &&
      signalReceipt.readiness === "signal_verified" &&
      signalReceipt.launchAgent.path.ok === true,
    signalReceipt,
  );
  check(
    "signal_verified_doctor_is_byte_read_only",
    digestTree(sandbox) === beforeSignalDoctor,
    { before: beforeSignalDoctor, after: digestTree(sandbox) },
  );
  const beforePackagedSignalDoctor = digestTree(sandbox);
  const packagedSignalDoctor = await command(
    process.execPath,
    [packagedCli, "doctor", "--read-only", "--json"],
    { cwd: neutralCwd, env: fixtureEnv },
  );
  const packagedSignalReceipt = parseJson(packagedSignalDoctor.stdout);
  check(
    "packaged_doctor_reports_signal_verified_read_only",
    packagedSignalDoctor.code === 0 &&
      packagedSignalReceipt.ok === true &&
      packagedSignalReceipt.readiness === "signal_verified" &&
      digestTree(sandbox) === beforePackagedSignalDoctor,
    packagedSignalReceipt,
  );
  check(
    "doctor_creates_no_ledger_wal_shm_or_logs",
    !fs.existsSync(path.join(fixturePlimsoll, "work-ledger.sqlite")) &&
      !fs.existsSync(path.join(fixturePlimsoll, "work-ledger.sqlite-wal")) &&
      !fs.existsSync(path.join(fixturePlimsoll, "work-ledger.sqlite-shm")) &&
      !fs.existsSync(path.join(fixturePlimsoll, "collector.out.log")) &&
      !fs.existsSync(path.join(fixturePlimsoll, "collector.err.log")),
    fixturePlimsoll,
  );
  check("proof_never_invokes_launchctl", !fs.existsSync(launchctlLog), launchctlLog);

  const receipt = {
    issue: 107,
    ok: checks.every((entry) => entry.passed),
    node: { execPath: process.execPath, version: process.versions.node },
    checks,
  };
  const evidenceDir = path.join(root, "evidence");
  fs.mkdirSync(evidenceDir, { recursive: true });
  fs.writeFileSync(
    path.join(evidenceDir, "install-doctor-proof.json"),
    JSON.stringify(receipt, null, 2) + "\n",
  );
  console.log(JSON.stringify(receipt, null, 2));
  } finally {
    if (server) {
      await new Promise<void>((resolve) => server!.close(() => resolve()));
    }
    fs.rmSync(packagedCli, { force: true });
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : String(error));
  process.exitCode = 1;
});
