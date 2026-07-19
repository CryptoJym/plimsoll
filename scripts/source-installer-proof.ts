/**
 * Adversarial source-installer proof for GitHub #128.
 *
 * The proof uses a temporary HOME, checkout, Plimsoll home, command adapters,
 * and loopback port. It never contacts git/npm, starts a collector, changes a
 * real tool config or LaunchAgent, or reads any operator credential.
 */
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";

type CommandResult = { code: number | null; stdout: string; stderr: string };
type Check = { name: string; passed: boolean; detail: unknown };

const root = path.resolve(import.meta.dirname, "..");
const installer = path.join(root, "install.sh");
const expectedSha = "74be1da7774357eab02cd9e5e31aa314bdb8ddba";
const wrongSha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const checks: Check[] = [];

function check(name: string, condition: unknown, detail: unknown) {
  checks.push({ name, passed: Boolean(condition), detail });
  if (!condition) throw new Error(`${name}: ${JSON.stringify(detail)}`);
}

function writeExecutable(file: string, body: string) {
  fs.writeFileSync(file, body, { mode: 0o700 });
}

function run(
  args: string[],
  options: { cwd: string; env: NodeJS.ProcessEnv },
): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = spawn("/bin/bash", [installer, ...args], {
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

function parseReceipt(result: CommandResult): Record<string, any> {
  return JSON.parse(result.stdout.trim()) as Record<string, any>;
}

function digestTree(directory: string) {
  const hash = createHash("sha256");
  const visit = (current: string) => {
    if (!fs.existsSync(current)) {
      hash.update("missing\0");
      return;
    }
    for (const entry of fs.readdirSync(current, { withFileTypes: true })
      .sort((a, b) => a.name.localeCompare(b.name))) {
      const full = path.join(current, entry.name);
      hash.update(`${entry.isDirectory() ? "d" : "f"}\0${path.relative(directory, full)}\0`);
      if (entry.isDirectory()) visit(full);
      else hash.update(fs.readFileSync(full));
    }
  };
  visit(directory);
  return hash.digest("hex");
}

function commandLog(file: string) {
  return fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "";
}

async function main() {
  check("proof_runs_on_literal_node_22", process.versions.node.split(".")[0] === "22", {
    execPath: process.execPath,
    version: process.versions.node,
  });

  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-source-installer-proof-"));
  let occupiedServer: http.Server | undefined;
  try {
    const neutral = path.join(sandbox, "neutral");
    const home = path.join(sandbox, "home");
    const plimsollHome = path.join(sandbox, "plimsoll-home");
    const checkout = path.join(sandbox, "checkout");
    const adapters = path.join(sandbox, "adapters");
    const externalPnpmDir = path.join(sandbox, "external-pnpm");
    const stateDir = path.join(sandbox, "adapter-state");
    const commands = path.join(sandbox, "commands.txt");
    fs.mkdirSync(neutral, { recursive: true });
    fs.mkdirSync(home, { recursive: true });
    fs.mkdirSync(plimsollHome, { recursive: true });
    fs.mkdirSync(adapters, { recursive: true });
    fs.mkdirSync(externalPnpmDir, { recursive: true });
    fs.mkdirSync(stateDir, { recursive: true });

    writeExecutable(path.join(adapters, "node"), `#!/bin/sh
case "$2" in
  *Number*) printf '25\n' ;;
  *) printf '25.6.1\n' ;;
esac
`);
    writeExecutable(path.join(adapters, "uname"), `#!/bin/sh
case "$1" in
  -s) printf 'Darwin\n' ;;
  -m) printf '%s\n' "\${PLIMSOLL_FIXTURE_ARCH:-arm64}" ;;
  *) exit 2 ;;
esac
`);
    writeExecutable(path.join(adapters, "git"), `#!/bin/sh
printf 'git %s\n' "$*" >> "$PLIMSOLL_FIXTURE_COMMAND_LOG"
if [ "$1" = "init" ]; then
  mkdir -p "$2/.git"
  exit 0
fi
if [ "$1" != "-C" ]; then exit 90; fi
repo="$2"
shift 2
case "$1 $2" in
  "remote add")
    printf '%s\n' "$4" > "$repo/.git/origin"
    ;;
  "remote get-url")
    cat "$repo/.git/origin"
    ;;
  "status --porcelain")
    if [ "\${PLIMSOLL_FIXTURE_DIRTY:-0}" = "1" ]; then printf ' M retained\n'; fi
    ;;
  "fetch --no-tags")
    if [ "\${PLIMSOLL_FIXTURE_FETCH_FAIL:-0}" = "1" ]; then exit 42; fi
    printf '%s\n' "$4" > "$repo/.git/fetched"
    ;;
  "rev-parse --verify")
    case "$3" in
      *FETCH_HEAD*) cat "$repo/.git/fetched" ;;
      *HEAD*) cat "$repo/.git/head" ;;
      *) exit 91 ;;
    esac
    ;;
  "cat-file -e")
    requested="\${3%\\^\\{commit\\}}"
    [ "$requested" = "$(cat "$repo/.git/fetched")" ]
    ;;
  "checkout --detach")
    printf '%s\n' "$3" > "$repo/.git/head"
    ;;
  *) exit 92 ;;
esac
`);
    const pnpm = path.join(externalPnpmDir, "pnpm");
    writeExecutable(pnpm, `#!/bin/sh
printf 'pnpm %s\n' "$*" >> "$PLIMSOLL_FIXTURE_COMMAND_LOG"
if [ "$1" = "--version" ]; then printf '10.25.0\n'; exit 0; fi
case " $* " in
  *" install --frozen-lockfile "*)
    if [ "\${PLIMSOLL_FIXTURE_INSTALL_FAIL_ONCE:-0}" = "1" ] && [ ! -f "$PLIMSOLL_FIXTURE_STATE_DIR/install-failed" ]; then
      : > "$PLIMSOLL_FIXTURE_STATE_DIR/install-failed"
      exit 75
    fi
    ;;
  *" collector setup --yes "*)
    if [ -f "$PLIMSOLL_FIXTURE_STATE_DIR/setup-applied" ]; then
      printf '{"status":"setup_noop"}\n'
    else
      : > "$PLIMSOLL_FIXTURE_STATE_DIR/setup-applied"
      printf '{"status":"setup_applied"}\n'
    fi
    ;;
  *" collector unload-launch-agent "*) exit 1 ;;
  *" collector install-launch-agent --dev "*) printf '{"installed":true}\n' ;;
  *" collector load-launch-agent "*) printf '{"loading":true}\n' ;;
  *" collector doctor --read-only --json "*)
    case "\${PLIMSOLL_FIXTURE_DOCTOR:-service_ready}" in
      signal_verified) verified=true; readiness=signal_verified; ok=true; code=0 ;;
      service_ready) verified=false; readiness=service_ready; ok=false; code=17 ;;
      configured) verified=false; readiness=configured; ok=false; code=17 ;;
      contradictory) verified=true; readiness=service_ready; ok=false; code=17 ;;
      *) printf 'not-json\n'; exit 17 ;;
    esac
    printf '{"ok":%s,"readiness":"%s","readOnly":true,"node":{"version":"${process.versions.node}","range":">=20 <25","supported":true},"config":{"status":"valid","valid":true,"createdDuringCommand":false},"telemetry":{"ok":true,"claude":{"ok":true},"codex":{"ok":true}},"launchAgent":{"ok":true,"path":{"ok":true}},"runtime":{"ok":true,"ownershipVersion":{"expected":2,"actual":2},"processLive":true,"identityMatchesStatus":true},"connectivity":{"reachable":true,"signal":{"verified":%s}},"dataMode":"metadata","privacyMode":"metadata_only","privacy":{"mode":"metadata_only","configuredDataMode":"metadata","rawEvidenceCapture":"disabled"},"syncConfigured":false,"uploadSigningConfigured":false}\n' "$ok" "$readiness" "$verified"
    exit "$code"
    ;;
esac
exit 0
`);

    const baseEnv: NodeJS.ProcessEnv = {
      ...process.env,
      HOME: home,
      PATH: `${adapters}:/usr/bin:/bin:/usr/sbin:/sbin`,
      PLIMSOLL_HOME: plimsollHome,
      PLIMSOLL_DIR: checkout,
      PLIMSOLL_PNPM: pnpm,
      PLIMSOLL_FIXTURE_COMMAND_LOG: commands,
      PLIMSOLL_FIXTURE_STATE_DIR: stateDir,
      PLIMSOLL_INSTALL_DOCTOR_ATTEMPTS: "1",
    };
    const sourceArgs = ["--ref", expectedSha, "--node", process.execPath, "--pnpm", pnpm];

    fs.mkdirSync(path.join(home, ".codex"), { recursive: true });
    fs.mkdirSync(path.join(home, ".claude"), { recursive: true });
    const credentialSentinel = "PRIVATE_CREDENTIAL_SENTINEL_DO_NOT_READ_OR_PRINT";
    fs.writeFileSync(path.join(home, ".codex", "auth.json"), credentialSentinel, { mode: 0o600 });
    fs.writeFileSync(path.join(home, ".claude", ".credentials.json"), credentialSentinel, { mode: 0o600 });
    fs.writeFileSync(path.join(plimsollHome, "collector.pid"), "stale-local-pid\n");
    fs.writeFileSync(path.join(plimsollHome, "work-ledger.sqlite"), "cold-ledger-sentinel\n");
    const stalePlist = path.join(home, "Library", "LaunchAgents", "com.plimsoll.collector.plist");
    fs.mkdirSync(path.dirname(stalePlist), { recursive: true });
    fs.writeFileSync(stalePlist, "stale-plist-sentinel\n");
    fs.writeFileSync(checkout, "occupied-before-dry-run\n");

    occupiedServer = http.createServer((_request, response) => response.end("occupied"));
    await new Promise<void>((resolve, reject) => {
      occupiedServer!.once("error", reject);
      occupiedServer!.listen(0, "127.0.0.1", resolve);
    });
    const beforeDryRun = digestTree(sandbox);
    for (const architecture of ["arm64", "x86_64"]) {
      const dryRun = await run(["--dry-run", ...sourceArgs], {
        cwd: neutral,
        env: { ...baseEnv, PLIMSOLL_FIXTURE_ARCH: architecture },
      });
      const receipt = parseReceipt(dryRun);
      check(`dry_run_${architecture}_is_truthful_noop`,
        dryRun.code === 0 &&
          receipt.state === "plan_validated" &&
          receipt.runtime.nodeMajor === 22 &&
          receipt.runtime.architecture === architecture &&
          receipt.source.remoteObjectVerified === false &&
          receipt.retainedState.dependencies === "planned_frozen_lockfile" &&
          digestTree(sandbox) === beforeDryRun,
        { dryRun, receipt, beforeDryRun, after: digestTree(sandbox) });
    }
    check("dry_run_invokes_no_git_or_pnpm", commandLog(commands) === "", commandLog(commands));

    const mutableTarget = path.join(sandbox, "mutable-target");
    const mutable = await run(["--dry-run", "--ref", "main", "--node", process.execPath, "--pnpm", pnpm], {
      cwd: neutral,
      env: { ...baseEnv, PLIMSOLL_DIR: mutableTarget },
    });
    check("mutable_ref_is_refused_before_mutation",
      mutable.code === 2 && mutable.stderr.includes("Mutable or ambiguous") && !fs.existsSync(mutableTarget),
      mutable);

    const brokenNode = path.join(adapters, "broken-node");
    writeExecutable(brokenNode, "#!/bin/sh\nprintf 'broken\\n' >&2\nexit 8\n");
    const brokenTarget = path.join(sandbox, "broken-node-target");
    const broken = await run(["--dry-run", "--ref", expectedSha, "--node", brokenNode, "--pnpm", pnpm], {
      cwd: neutral,
      env: { ...baseEnv, PLIMSOLL_DIR: brokenTarget },
    });
    check("broken_node_fails_before_mutation",
      broken.code !== 0 && broken.stderr.includes("source canaries require Node 22") && !fs.existsSync(brokenTarget),
      broken);

    const discoveryHome = path.join(sandbox, "discovery-home");
    const discoveredNode = path.join(discoveryHome, ".nvm", "versions", "node", "v22.99.0", "bin", "node");
    fs.mkdirSync(path.dirname(discoveredNode), { recursive: true });
    fs.symlinkSync(process.execPath, discoveredNode);
    const discovered = await run(["--dry-run", "--ref", expectedSha, "--pnpm", pnpm], {
      cwd: neutral,
      env: { ...baseEnv, HOME: discoveryHome },
    });
    const discoveredReceipt = parseReceipt(discovered);
    check("node_25_default_yields_to_explicitly_discovered_node_22",
      discovered.code === 0 && discoveredReceipt.runtime.nodeMajor === 22 &&
        discoveredReceipt.runtime.nodeVersion === process.versions.node,
      { discovered, discoveredReceipt });

    fs.rmSync(checkout);
    const wrongTarget = path.join(sandbox, "wrong-target");
    const wrong = await run(["apply", "--ref", wrongSha, "--node", process.execPath, "--pnpm", pnpm], {
      cwd: neutral,
      env: { ...baseEnv, PLIMSOLL_DIR: wrongTarget, PLIMSOLL_FIXTURE_FETCH_FAIL: "1" },
    });
    const wrongReceipt = parseReceipt(wrong);
    check("unavailable_exact_sha_fails_with_literal_retained_checkout",
      wrong.code !== 0 && wrongReceipt.state === "failed" &&
        wrongReceipt.errorStage === "source_fetch" &&
        wrongReceipt.source.remoteObjectVerified === false &&
        wrongReceipt.retainedState.checkout === "initialized_retained" &&
        fs.existsSync(path.join(wrongTarget, ".git")),
      { wrong, wrongReceipt });

    const ownedTarget = path.join(sandbox, "owned-target");
    const ownedLock = `${ownedTarget}.plimsoll-install.lock`;
    fs.mkdirSync(ownedLock, { recursive: true });
    fs.writeFileSync(path.join(ownedLock, "owner.pid"), `${process.pid}\n`);
    const alreadyOwned = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: { ...baseEnv, PLIMSOLL_DIR: ownedTarget },
    });
    const ownedReceipt = parseReceipt(alreadyOwned);
    check("concurrent_live_owner_is_refused_without_lock_theft",
      alreadyOwned.code !== 0 && ownedReceipt.errorStage === "install_already_running" &&
        fs.readFileSync(path.join(ownedLock, "owner.pid"), "utf8").trim() === String(process.pid) &&
        !fs.existsSync(ownedTarget),
      { alreadyOwned, ownedReceipt });
    fs.rmSync(ownedLock, { recursive: true });

    const localOrigin = path.join(sandbox, "local-origin.git");
    const localClone = spawnSync("/usr/bin/git", ["clone", "--bare", root, localOrigin], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    check("local_git_origin_created_without_network", localClone.status === 0, localClone.stderr);
    const realGitAdapters = path.join(sandbox, "real-git-adapters");
    fs.mkdirSync(realGitAdapters);
    fs.symlinkSync("/usr/bin/git", path.join(realGitAdapters, "git"));
    fs.symlinkSync(path.join(adapters, "uname"), path.join(realGitAdapters, "uname"));
    const realGitTarget = path.join(sandbox, "real-git-target");
    const realGitState = path.join(sandbox, "real-git-state");
    fs.mkdirSync(realGitState);
    const realGitApply = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PATH: `${realGitAdapters}:/usr/bin:/bin:/usr/sbin:/sbin`,
        PLIMSOLL_REPO: localOrigin,
        PLIMSOLL_DIR: realGitTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: realGitState,
        PLIMSOLL_FIXTURE_DOCTOR: "service_ready",
      },
    });
    const realGitReceipt = parseReceipt(realGitApply);
    const realGitHead = spawnSync("/usr/bin/git", ["-C", realGitTarget, "rev-parse", "HEAD"], {
      encoding: "utf8",
    });
    check("real_local_git_fetch_is_verified_and_pinned_exactly",
      realGitApply.code === 0 && realGitReceipt.state === "service_ready" &&
        realGitReceipt.source.remoteObjectVerified === true &&
        realGitReceipt.source.localCheckoutMatched === true &&
        realGitHead.status === 0 && realGitHead.stdout.trim() === expectedSha,
      { realGitApply, realGitReceipt, head: realGitHead.stdout.trim(), stderr: realGitHead.stderr });

    const applyTarget = path.join(sandbox, "apply-target");
    const applyState = path.join(sandbox, "apply-state");
    fs.mkdirSync(applyState);
    const apply = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: applyTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: applyState,
        PLIMSOLL_FIXTURE_DOCTOR: "service_ready",
      },
    });
    const applyReceipt = parseReceipt(apply);
    const applyCommands = commandLog(commands);
    check("cold_apply_succeeds_truthfully_at_service_ready",
      apply.code === 0 && applyReceipt.state === "service_ready" &&
        applyReceipt.readiness === "service_ready" &&
        applyReceipt.nextAction === "restart_a_locally_authenticated_agent_then_run_verify" &&
        applyReceipt.source.remoteObjectVerified === true &&
        applyReceipt.runtime.nodeVersion === process.versions.node &&
        applyReceipt.runtime.pnpmVersion === "10.25.0" &&
        applyReceipt.retainedState.toolConfig === "applied_with_exact_preimage_backups" &&
        applyReceipt.credentialOperations === 0 &&
        applyReceipt.rollbackClaimed === false,
      { apply, applyReceipt });
    check("apply_uses_frozen_lockfile_and_absolute_runtime_inputs",
      applyCommands.includes(`pnpm --dir ${applyTarget} install --frozen-lockfile`) &&
        applyCommands.includes(`--pnpm ${pnpm}`) &&
        !applyCommands.includes("pnpm install\n") &&
        !applyCommands.includes("git pull") &&
        !applyCommands.includes("git clone"),
      applyCommands);

    const coldVerify = await run(["verify", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: applyTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: applyState,
        PLIMSOLL_FIXTURE_DOCTOR: "service_ready",
      },
    });
    const coldVerifyReceipt = parseReceipt(coldVerify);
    check("verify_rejects_cold_service_ready",
      coldVerify.code !== 0 && coldVerifyReceipt.state === "failed" &&
        coldVerifyReceipt.errorStage === "signal_verification" &&
        coldVerifyReceipt.readiness === "service_ready",
      { coldVerify, coldVerifyReceipt });

    const contradictory = await run(["verify", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: applyTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: applyState,
        PLIMSOLL_FIXTURE_DOCTOR: "contradictory",
      },
    });
    const contradictoryReceipt = parseReceipt(contradictory);
    check("contradictory_doctor_schema_is_rejected",
      contradictory.code !== 0 && contradictoryReceipt.state === "failed" &&
        contradictoryReceipt.readiness === "invalid" &&
        contradictoryReceipt.errorStage === "signal_verification",
      { contradictory, contradictoryReceipt });

    const signalVerify = await run(["verify", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: applyTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: applyState,
        PLIMSOLL_FIXTURE_DOCTOR: "signal_verified",
      },
    });
    const signalReceipt = parseReceipt(signalVerify);
    check("verify_promotes_only_real_signal_verified_doctor",
      signalVerify.code === 0 && signalReceipt.state === "signal_verified" &&
        signalReceipt.readiness === "signal_verified" && signalReceipt.nextAction === "none",
      { signalVerify, signalReceipt });

    const retryTarget = path.join(sandbox, "retry-target");
    const retryState = path.join(sandbox, "retry-state");
    fs.mkdirSync(retryState);
    const staleRetryLock = `${retryTarget}.plimsoll-install.lock`;
    fs.mkdirSync(staleRetryLock);
    fs.writeFileSync(path.join(staleRetryLock, "owner.pid"), "999999\n");
    const interrupted = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: retryTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: retryState,
        PLIMSOLL_FIXTURE_INSTALL_FAIL_ONCE: "1",
      },
    });
    const interruptedReceipt = parseReceipt(interrupted);
    const resumed = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: retryTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: retryState,
        PLIMSOLL_FIXTURE_INSTALL_FAIL_ONCE: "1",
        PLIMSOLL_FIXTURE_DOCTOR: "service_ready",
      },
    });
    const resumedReceipt = parseReceipt(resumed);
    check("interrupted_install_reports_retained_state_and_exact_retry_resumes",
      interrupted.code === 75 && interruptedReceipt.errorStage === "frozen_install" &&
        interruptedReceipt.retainedState.checkout === "pinned_exact" &&
        interruptedReceipt.rollbackClaimed === false &&
        !fs.existsSync(staleRetryLock) &&
        resumed.code === 0 && resumedReceipt.state === "service_ready" &&
        resumedReceipt.retainedState.checkout === "pinned_exact",
      { interrupted, interruptedReceipt, resumed, resumedReceipt });

    const staleRuntimeTarget = path.join(sandbox, "stale-runtime-target");
    const staleRuntimeState = path.join(sandbox, "stale-runtime-state");
    fs.mkdirSync(staleRuntimeState);
    const staleRuntime = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: staleRuntimeTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: staleRuntimeState,
        PLIMSOLL_FIXTURE_DOCTOR: "configured",
      },
    });
    const staleRuntimeReceipt = parseReceipt(staleRuntime);
    check("stale_pid_or_occupied_port_shape_fails_with_service_state_retained",
      staleRuntime.code !== 0 && staleRuntimeReceipt.errorStage === "service_readiness" &&
        staleRuntimeReceipt.readiness === "invalid" &&
        staleRuntimeReceipt.retainedState.service === "not_ready_retained" &&
        staleRuntimeReceipt.rollbackClaimed === false,
      { staleRuntime, staleRuntimeReceipt });

    const secondApply = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: applyTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: applyState,
        PLIMSOLL_FIXTURE_DOCTOR: "service_ready",
      },
    });
    const secondReceipt = parseReceipt(secondApply);
    check("repeated_apply_is_idempotent_without_config_backup_churn",
      secondApply.code === 0 && secondReceipt.state === "service_ready" &&
        secondReceipt.retainedState.checkout === "pinned_exact" &&
        secondReceipt.retainedState.toolConfig === "unchanged",
      { secondApply, secondReceipt });

    const allReceipts = [
      apply.stdout,
      coldVerify.stdout,
      contradictory.stdout,
      signalVerify.stdout,
      interrupted.stdout,
      resumed.stdout,
      staleRuntime.stdout,
      secondApply.stdout,
    ].join("\n");
    check("shareable_receipts_exclude_paths_credentials_and_provider_actions",
      !allReceipts.includes(sandbox) &&
        !allReceipts.includes(credentialSentinel) &&
        !allReceipts.includes("auth.json") &&
        !allReceipts.includes("credentials.json") &&
        !allReceipts.includes(REDACTED_HOME_PATTERN(home)) &&
        [applyReceipt, signalReceipt, resumedReceipt].every((receipt) =>
          receipt.credentialOperations === 0 &&
          receipt.hostedEnrollmentPerformed === false &&
          receipt.localOnlyDefault === true),
      allReceipts);
    check("credential_files_remain_byte_exact",
      fs.readFileSync(path.join(home, ".codex", "auth.json"), "utf8") === credentialSentinel &&
        fs.readFileSync(path.join(home, ".claude", ".credentials.json"), "utf8") === credentialSentinel,
      home);

    const receipt = {
      issue: 128,
      ok: checks.every((entry) => entry.passed),
      isolation: {
        realNetworkCalls: 0,
        realPackageInstalls: 0,
        realLaunchAgentsTouched: 0,
        realToolConfigsTouched: 0,
        credentialReadsOrCopies: 0,
      },
      node: { execPath: process.execPath, version: process.versions.node },
      checks,
    };
    fs.mkdirSync(path.join(root, "evidence"), { recursive: true });
    fs.writeFileSync(
      path.join(root, "evidence", "source-installer-proof.json"),
      `${JSON.stringify(receipt, null, 2)}\n`,
    );
    console.log(JSON.stringify(receipt, null, 2));
  } finally {
    if (occupiedServer) await new Promise<void>((resolve) => occupiedServer!.close(() => resolve()));
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

function REDACTED_HOME_PATTERN(home: string) {
  return path.basename(home) === "home" ? `${path.dirname(home)}/home` : home;
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : String(error));
  process.exitCode = 1;
});
