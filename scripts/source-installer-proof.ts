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
  options: { cwd: string; env: NodeJS.ProcessEnv; umask?: "0777" },
): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const bashArgs = options.umask
      ? ["-c", "umask \"$1\"; shift; exec /bin/bash \"$@\"", "plimsoll-proof", options.umask, installer, ...args]
      : [installer, ...args];
    const child = spawn("/bin/bash", bashArgs, {
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
  if (!result.stdout.trim()) {
    throw new Error(`installer emitted no receipt: ${JSON.stringify(result)}`);
  }
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

function lockOwner(pid: number, ownerNonce: string) {
  const started = spawnSync("/bin/ps", ["-p", String(pid), "-o", "lstart="], {
    encoding: "utf8",
    env: { LANG: "C", LC_ALL: "C", PATH: "/usr/bin:/bin" },
  });
  if (started.status !== 0 || !started.stdout.trim()) throw new Error("lock fixture could not fingerprint owner");
  return `${JSON.stringify({
    version: 1,
    pid,
    processStartFingerprint: `sha256:${createHash("sha256").update(started.stdout.trim()).digest("hex")}`,
    ownerNonce,
  })}\n`;
}

async function withExclusiveLockFixture<T>(
  lock: string,
  contents: string,
  operation: () => Promise<T>,
  options: { ageMs?: number } = {},
): Promise<T> {
  let descriptor: number | undefined;
  let identity: fs.Stats | undefined;
  try {
    descriptor = fs.openSync(
      lock,
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW,
      0o600,
    );
    identity = fs.fstatSync(descriptor);
    fs.fchmodSync(descriptor, 0o600);
    identity = fs.fstatSync(descriptor);
    fs.writeFileSync(descriptor, contents, "utf8");
    fs.fsyncSync(descriptor);
    fs.closeSync(descriptor);
    descriptor = undefined;
    if (options.ageMs) {
      const timestamp = new Date(Date.now() - options.ageMs);
      fs.utimesSync(lock, timestamp, timestamp);
      identity = fs.lstatSync(lock);
    }
  } catch (error) {
    if (descriptor !== undefined) {
      try { fs.closeSync(descriptor); } catch {}
    }
    if (identity) {
      try {
        const visible = fs.lstatSync(lock);
        if (visible.dev === identity.dev && visible.ino === identity.ino &&
            visible.isFile() && !visible.isSymbolicLink()) fs.unlinkSync(lock);
      } catch {}
    }
    if ((error as NodeJS.ErrnoException).code === "EEXIST") {
      throw new Error(`proof refused to overwrite an existing coordination lock: ${lock}`);
    }
    throw error;
  }
  try {
    return await operation();
  } finally {
    try {
      const visible = fs.lstatSync(lock);
      if (identity && visible.dev === identity.dev && visible.ino === identity.ino &&
          visible.isFile() && !visible.isSymbolicLink()) {
        fs.unlinkSync(lock);
      } else {
        throw new Error(`proof fixture lock ownership changed; retained without removal: ${lock}`);
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
}

async function main() {
  check("proof_runs_on_literal_node_22", process.versions.node.split(".")[0] === "22", {
    execPath: process.execPath,
    version: process.versions.node,
  });

  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-source-installer-proof-"));
  let occupiedServer: http.Server | undefined;
  let occupiedPort = 0;
  let occupiedRequests = 0;
  try {
    const neutral = path.join(sandbox, "neutral");
    const home = path.join(sandbox, "home");
    const plimsollHome = path.join(home, "Library", "Application Support", "Plimsoll");
    const lockRoot = path.join(sandbox, "coordination-locks");
    const machineLock = path.join(lockRoot, `com.plimsoll.source-install.${process.getuid!()}.lock`);
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
    fs.mkdirSync(lockRoot, { mode: 0o700 });

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
    if [ "\${PLIMSOLL_FIXTURE_STATUS_FAIL:-0}" = "1" ]; then exit 93; fi
    if [ "\${PLIMSOLL_FIXTURE_DIRTY:-0}" = "1" ] || [ -f "$PLIMSOLL_FIXTURE_STATE_DIR/dirty-after-install" ]; then
      printf ' M retained\n'
    fi
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
    if [ "\${PLIMSOLL_FIXTURE_DIRTY_AFTER_INSTALL:-0}" = "1" ]; then
      : > "$PLIMSOLL_FIXTURE_STATE_DIR/dirty-after-install"
    fi
    if [ "\${PLIMSOLL_FIXTURE_INSTALL_FAIL_ONCE:-0}" = "1" ] && [ ! -f "$PLIMSOLL_FIXTURE_STATE_DIR/install-failed" ]; then
      : > "$PLIMSOLL_FIXTURE_STATE_DIR/install-failed"
      exit 75
    fi
    ;;
  *" collector setup --yes "*)
    if [ -f "$PLIMSOLL_FIXTURE_STATE_DIR/setup-applied" ]; then
      printf '%s: no changes\n' "$HOME/.claude/settings.json"
      printf '{"status":"setup_noop","claude":"local","codex":"local","conflict":null}\n'
    else
      : > "$PLIMSOLL_FIXTURE_STATE_DIR/setup-applied"
      printf '%s: add telemetry\n' "$HOME/.claude/settings.json"
      printf '{\n  "status": "setup_applied",\n  "privacyMode": "metadata_only",\n  "claude": { "changed": true },\n  "codex": { "changed": true }\n}\n'
    fi
    ;;
  *" collector unload-launch-agent "*)
    pid_file="$HOME/Library/Application Support/Plimsoll/collector.pid"
    if [ -f "$pid_file" ]; then rm -f "$pid_file"; exit 0; fi
    exit 1
    ;;
  *" collector install-launch-agent --dev "*)
    repo=""
    previous=""
    for argument in "$@"; do
      if [ "$previous" = "--repo-root" ]; then repo="$argument"; fi
      previous="$argument"
    done
    plist="$HOME/Library/LaunchAgents/com.plimsoll.collector.plist"
    mkdir -p "$(dirname "$plist")"
    node_path="$(command -v node)"
    node_dir="$(dirname "$node_path")"
    pnpm_dir="$(dirname "$0")"
    cat > "$plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>Label</key><string>com.plimsoll.collector</string>
<key>ProgramArguments</key><array><string>$0</string><string>--dir</string><string>$repo</string><string>collector</string><string>start</string></array>
<key>WorkingDirectory</key><string>$repo</string>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
<key>ThrottleInterval</key><integer>30</integer>
<key>StandardOutPath</key><string>$HOME/Library/Application Support/Plimsoll/collector.out.log</string>
<key>StandardErrorPath</key><string>$HOME/Library/Application Support/Plimsoll/collector.err.log</string>
<key>EnvironmentVariables</key><dict><key>PLIMSOLL_COLLECTOR_DATA_MODE</key><string>metadata</string><key>PATH</key><string>$node_dir:$pnpm_dir:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string></dict>
</dict></plist>
PLIST
    chmod 600 "$plist"
    printf '{"installed":true}\n'
    ;;
  *" collector load-launch-agent "*)
    repo=""
    previous=""
    for argument in "$@"; do
      if [ "$previous" = "--dir" ]; then repo="$argument"; fi
      previous="$argument"
    done
    mkdir -p "$HOME/Library/Application Support/Plimsoll"
    cat > "$HOME/Library/Application Support/Plimsoll/collector.pid" <<PID
{"version":2,"label":"com.plimsoll.collector","cwd":"$repo","instanceId":"11111111-1111-4111-8111-111111111111","pid":12345,"processStartFingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","command":["$repo/packages/collector-cli/src/cli.ts","start"]}
PID
    chmod 600 "$HOME/Library/Application Support/Plimsoll/collector.pid"
    printf '{"loading":true}\n'
    ;;
  *" collector doctor --read-only --json "*)
    if [ -n "\${PLIMSOLL_FIXTURE_OCCUPIED_PORT:-}" ]; then
      node -e 'const http=require("node:http"); const request=http.get({host:"127.0.0.1",port:Number(process.argv[1]),path:"/status"},response=>{response.resume();response.on("end",()=>process.exit(0))}); request.setTimeout(1000,()=>request.destroy(new Error("timeout"))); request.on("error",()=>process.exit(1));' "$PLIMSOLL_FIXTURE_OCCUPIED_PORT" || exit 88
    fi
    case "\${PLIMSOLL_FIXTURE_DOCTOR:-service_ready}" in
      signal_verified) verified=true; readiness=signal_verified; ok=true; code=0; tokens=1 ;;
      old_signal) verified=true; readiness=signal_verified; ok=true; code=0; tokens=5 ;;
      fresh_after_old) verified=true; readiness=signal_verified; ok=true; code=0; tokens=6 ;;
      signal_new_runtime) verified=true; readiness=signal_verified; ok=true; code=0; tokens=1; instance=22222222-2222-4222-8222-222222222222 ;;
      service_ready) verified=false; readiness=service_ready; ok=false; code=17; tokens=0 ;;
      configured) verified=false; readiness=configured; ok=false; code=17 ;;
      contradictory) verified=true; readiness=service_ready; ok=false; code=17 ;;
      *) printf 'not-json\n'; exit 17 ;;
    esac
    tokens="\${tokens:-0}"
    instance="\${instance:-11111111-1111-4111-8111-111111111111}"
    sync="\${PLIMSOLL_FIXTURE_SYNC:-false}"
    if [ "\${PLIMSOLL_FIXTURE_JSON_STYLE:-pretty}" = "compact" ]; then
      printf '{"ok":%s,"readiness":"%s","readOnly":true,"node":{"version":"${process.versions.node}","range":">=20 <25","supported":true},"config":{"status":"valid","valid":true,"createdDuringCommand":false},"telemetry":{"ok":true,"claude":{"ok":true},"codex":{"ok":true}},"launchAgent":{"ok":true,"path":{"ok":true}},"runtime":{"ok":true,"ownershipVersion":{"expected":2,"actual":2},"processLive":true,"identityMatchesStatus":true},"connectivity":{"reachable":true,"runtimeIdentity":{"instanceId":"%s"},"signal":{"verified":%s,"tokenAttributedEvents":%s}},"dataMode":"metadata","privacyMode":"metadata_only","privacy":{"mode":"metadata_only","configuredDataMode":"metadata","rawEvidenceCapture":"disabled"},"syncConfigured":%s,"uploadSigningConfigured":%s}\n' "$ok" "$readiness" "$instance" "$verified" "$tokens" "$sync" "$sync"
    else
      printf '{\n  "ok": %s,\n  "readiness": "%s",\n  "readOnly": true,\n  "node": {"version":"${process.versions.node}","range":">=20 <25","supported":true},\n  "config": {"status":"valid","valid":true,"createdDuringCommand":false},\n  "telemetry": {"ok":true,"claude":{"ok":true},"codex":{"ok":true}},\n  "launchAgent": {"ok":true,"path":{"ok":true}},\n  "runtime": {"ok":true,"ownershipVersion":{"expected":2,"actual":2},"processLive":true,"identityMatchesStatus":true},\n  "connectivity": {"reachable":true,"runtimeIdentity":{"instanceId":"%s"},"signal":{"verified":%s,"tokenAttributedEvents":%s}},\n  "dataMode":"metadata",\n  "privacyMode":"metadata_only",\n  "privacy":{"mode":"metadata_only","configuredDataMode":"metadata","rawEvidenceCapture":"disabled"},\n  "syncConfigured":%s,\n  "uploadSigningConfigured":%s\n}\n' "$ok" "$readiness" "$instance" "$verified" "$tokens" "$sync" "$sync"
    fi
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
      PLIMSOLL_SOURCE_INSTALL_TEST_ROOT: sandbox,
    };
    const sourceArgs = ["--ref", expectedSha, "--node", process.execPath, "--pnpm", pnpm];
    check("proof_coordination_lock_namespace_is_isolated",
      machineLock.startsWith(`${sandbox}${path.sep}`) && !machineLock.startsWith("/private/tmp/"),
      machineLock);

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

    occupiedServer = http.createServer((_request, response) => {
      occupiedRequests += 1;
      response.end("occupied");
    });
    await new Promise<void>((resolve, reject) => {
      occupiedServer!.once("error", reject);
      occupiedServer!.listen(0, "127.0.0.1", resolve);
    });
    occupiedPort = (occupiedServer.address() as { port: number }).port;
    const beforeDryRun = digestTree(sandbox);
    for (const architecture of ["arm64", "x86_64"]) {
      const dryRun = await run(["--dry-run", ...sourceArgs], {
        cwd: neutral,
        env: { ...baseEnv, PLIMSOLL_FIXTURE_ARCH: architecture },
      });
      const receipt = parseReceipt(dryRun);
      check(`dry_run_${architecture}_is_truthful_noop`,
        dryRun.code === 0 &&
          receipt.state === "plan_selected_no_execution" &&
          receipt.runtime.nodeMajor === 22 &&
          receipt.runtime.architecture === architecture &&
          receipt.source.remoteObjectVerified === false &&
          receipt.localOnlyRequired === true && receipt.localOnlyConfirmed === false &&
          receipt.retainedState.dependencies === "planned_frozen_lockfile" &&
          digestTree(sandbox) === beforeDryRun,
        { dryRun, receipt, beforeDryRun, after: digestTree(sandbox) });
    }
    check("dry_run_invokes_no_git_or_pnpm", commandLog(commands) === "", commandLog(commands));

    const customHomeTarget = path.join(sandbox, "custom-home-target");
    const customHome = await run(["--dry-run", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_HOME: path.join(sandbox, "unsupported-custom-home"),
        PLIMSOLL_DIR: customHomeTarget,
      },
    });
    check("custom_collector_home_is_refused_before_mutation",
      customHome.code !== 0 && customHome.stderr.includes("Custom PLIMSOLL_HOME is not supported") &&
        !fs.existsSync(customHomeTarget) && !fs.existsSync(path.join(sandbox, "unsupported-custom-home")),
      customHome);

    const nonRegularTargets = [externalPnpmDir, path.join(sandbox, "pnpm-fifo")];
    const fifo = spawnSync("/usr/bin/mkfifo", [nonRegularTargets[1]], { encoding: "utf8" });
    check("fifo_fixture_created", fifo.status === 0, fifo.stderr);
    fs.chmodSync(nonRegularTargets[1], 0o700);
    for (const [index, nonRegular] of nonRegularTargets.entries()) {
      const target = path.join(sandbox, `non-regular-pnpm-target-${index}`);
      const result = await run(["--dry-run", "--ref", expectedSha, "--node", process.execPath, "--pnpm", nonRegular], {
        cwd: neutral,
        env: { ...baseEnv, PLIMSOLL_DIR: target },
      });
      check(`non_regular_pnpm_${index}_is_refused_before_mutation`,
        result.code !== 0 && result.stderr.includes("pnpm was not found as an absolute executable") &&
          !fs.existsSync(target),
        result);
    }

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
      env: {
        ...baseEnv,
        HOME: discoveryHome,
        PLIMSOLL_HOME: path.join(discoveryHome, "Library", "Application Support", "Plimsoll"),
      },
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

    const inspectionTarget = path.join(sandbox, "inspection-target");
    const inspectionState = path.join(sandbox, "inspection-state");
    fs.mkdirSync(inspectionState);
    const inspectionFailure = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: inspectionTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: inspectionState,
        PLIMSOLL_FIXTURE_STATUS_FAIL: "1",
      },
    });
    const inspectionReceipt = parseReceipt(inspectionFailure);
    check("git_status_failure_is_not_treated_as_clean",
      inspectionFailure.code === 93 && inspectionReceipt.errorStage === "checkout_inspection" &&
        inspectionReceipt.retainedState.checkout === "inspection_failed_retained" &&
        !commandLog(commands).includes(`pnpm --dir ${inspectionTarget} install`),
      { inspectionFailure, inspectionReceipt });

    const ownedTarget = path.join(sandbox, "owned-target");
    const ownedLock = machineLock;
    const ownedLockBytes = lockOwner(process.pid, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
    const ownedObservation = await withExclusiveLockFixture(ownedLock, ownedLockBytes, async () => {
      const result = await run(["apply", ...sourceArgs], {
        cwd: neutral,
        env: { ...baseEnv, PLIMSOLL_DIR: ownedTarget },
      });
      return { result, retainedBytes: fs.readFileSync(ownedLock, "utf8") };
    });
    const alreadyOwned = ownedObservation.result;
    const ownedReceipt = parseReceipt(alreadyOwned);
    check("competing_publisher_refuses_live_owner_without_lock_theft",
      alreadyOwned.code !== 0 && ownedReceipt.errorStage === "install_already_running" &&
        ownedObservation.retainedBytes === ownedLockBytes &&
        !fs.existsSync(ownedTarget),
      { alreadyOwned, ownedReceipt });

    const proveTornRecovery = async (label: string, contents: string) => {
      const target = path.join(sandbox, `${label}-target`);
      const result = await withExclusiveLockFixture(machineLock, contents, () =>
        run(["apply", ...sourceArgs], {
          cwd: neutral,
          env: { ...baseEnv, PLIMSOLL_DIR: target, PLIMSOLL_FIXTURE_FETCH_FAIL: "1" },
        }), { ageMs: 60_000 });
      const receipt = parseReceipt(result);
      check(`${label}_lock_is_recovered_after_bounded_grace`,
        result.code === 42 && receipt.errorStage === "source_fetch" && !fs.existsSync(machineLock),
        { result, receipt });
    };
    await proveTornRecovery("zero_byte_torn", "");
    await proveTornRecovery("partial_json_torn", "{\"version\":1");

    const reusedPidTarget = path.join(sandbox, "reused-pid-target");
    const reusedPidBytes = `${JSON.stringify({
      version: 1,
      pid: process.pid,
      processStartFingerprint: `sha256:${"f".repeat(64)}`,
      ownerNonce: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
    })}\n`;
    const reusedPid = await withExclusiveLockFixture(machineLock, reusedPidBytes, () =>
      run(["apply", ...sourceArgs], {
        cwd: neutral,
        env: { ...baseEnv, PLIMSOLL_DIR: reusedPidTarget, PLIMSOLL_FIXTURE_FETCH_FAIL: "1" },
      }));
    const reusedPidReceipt = parseReceipt(reusedPid);
    check("live_pid_with_mismatched_start_fingerprint_is_reclaimed",
      reusedPid.code === 42 && reusedPidReceipt.errorStage === "source_fetch" && !fs.existsSync(machineLock),
      { reusedPid, reusedPidReceipt });

    const restrictiveUmaskTarget = path.join(sandbox, "restrictive-umask-occupied");
    fs.writeFileSync(restrictiveUmaskTarget, "occupied\n");
    const restrictiveUmask = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: { ...baseEnv, PLIMSOLL_DIR: restrictiveUmaskTarget },
      umask: "0777",
    });
    const restrictiveUmaskReceipt = parseReceipt(restrictiveUmask);
    check("restrictive_umask_cannot_wedge_coordination_lock",
      restrictiveUmask.code !== 0 && restrictiveUmaskReceipt.errorStage === "checkout_target" &&
        !fs.existsSync(machineLock) && fs.readdirSync(lockRoot).length === 0,
      { restrictiveUmask, restrictiveUmaskReceipt, lockArtifacts: fs.readdirSync(lockRoot) });

    const syncTarget = path.join(sandbox, "sync-target");
    const syncState = path.join(sandbox, "sync-state");
    fs.mkdirSync(syncState);
    const commandsBeforeSync = commandLog(commands).length;
    const stalePidBeforeSync = fs.readFileSync(path.join(plimsollHome, "collector.pid"), "utf8");
    const syncRefused = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: syncTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: syncState,
        PLIMSOLL_FIXTURE_SYNC: "true",
      },
    });
    const syncReceipt = parseReceipt(syncRefused);
    const syncCommands = commandLog(commands).slice(commandsBeforeSync);
    check("hosted_sync_is_refused_before_any_service_mutation",
      syncRefused.code !== 0 && syncReceipt.errorStage === "local_only_preflight" &&
        syncReceipt.localOnlyRequired === true && syncReceipt.localOnlyConfirmed === false &&
        syncReceipt.retainedState.service === "not_started" &&
        !syncCommands.includes("unload-launch-agent") &&
        !syncCommands.includes("install-launch-agent") &&
        !syncCommands.includes("load-launch-agent") &&
        fs.readFileSync(path.join(plimsollHome, "collector.pid"), "utf8") === stalePidBeforeSync,
      { syncRefused, syncReceipt, syncCommands });

    const dirtyInstallTarget = path.join(sandbox, "dirty-install-target");
    const dirtyInstallState = path.join(sandbox, "dirty-install-state");
    fs.mkdirSync(dirtyInstallState);
    const commandsBeforeDirtyInstall = commandLog(commands).length;
    const dirtyInstall = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: dirtyInstallTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: dirtyInstallState,
        PLIMSOLL_FIXTURE_DIRTY_AFTER_INSTALL: "1",
      },
    });
    const dirtyInstallReceipt = parseReceipt(dirtyInstall);
    const dirtyInstallCommands = commandLog(commands).slice(commandsBeforeDirtyInstall);
    check("frozen_install_must_leave_tracked_source_clean_before_config_or_service",
      dirtyInstall.code !== 0 && dirtyInstallReceipt.errorStage === "frozen_install_changed_source" &&
        dirtyInstallReceipt.retainedState.checkout === "dirty_retained" &&
        !dirtyInstallCommands.includes("collector setup") &&
        !dirtyInstallCommands.includes("unload-launch-agent") &&
        !dirtyInstallCommands.includes("install-launch-agent"),
      { dirtyInstall, dirtyInstallReceipt, dirtyInstallCommands });

    const manifestTarget = path.join(sandbox, "manifest-symlink-target");
    const manifestState = path.join(sandbox, "manifest-symlink-state");
    const externalManifest = path.join(sandbox, "external-manifest-sentinel");
    const externalManifestSentinel = "outside-manifest-must-not-change\n";
    fs.mkdirSync(manifestState);
    fs.writeFileSync(externalManifest, externalManifestSentinel);
    fs.unlinkSync(stalePlist);
    fs.symlinkSync(externalManifest, stalePlist);
    const commandsBeforeManifest = commandLog(commands).length;
    const manifestRefused = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: manifestTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: manifestState,
        PLIMSOLL_FIXTURE_DOCTOR: "service_ready",
      },
    });
    const manifestReceipt = parseReceipt(manifestRefused);
    const manifestCommands = commandLog(commands).slice(commandsBeforeManifest);
    check("obvious_launch_agent_symlink_is_refused_before_service_mutation",
      manifestRefused.code !== 0 && manifestReceipt.errorStage === "service_manifest_destination" &&
        manifestReceipt.retainedState.service === "manifest_destination_refused_retained" &&
        fs.readFileSync(externalManifest, "utf8") === externalManifestSentinel &&
        fs.lstatSync(stalePlist).isSymbolicLink() &&
        !manifestCommands.includes("unload-launch-agent") &&
        !manifestCommands.includes("install-launch-agent") &&
        !manifestCommands.includes("load-launch-agent"),
      { manifestRefused, manifestReceipt, manifestCommands });
    fs.unlinkSync(stalePlist);
    fs.writeFileSync(stalePlist, "stale-plist-sentinel\n");

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
        PLIMSOLL_FIXTURE_JSON_STYLE: "compact",
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
    check("single_line_doctor_json_after_command_output_is_parsed",
      realGitApply.code === 0 && realGitReceipt.readiness === "service_ready",
      { realGitApply, realGitReceipt });
    const reconciledPid = JSON.parse(fs.readFileSync(path.join(plimsollHome, "collector.pid"), "utf8"));
    check("existing_stale_pid_and_plist_are_reconciled_by_one_service_owner",
      realGitApply.code === 0 &&
        reconciledPid.cwd === realGitTarget &&
        !fs.readFileSync(stalePlist, "utf8").includes("stale-plist-sentinel") &&
        commandLog(commands).includes("collector unload-launch-agent") &&
        commandLog(commands).includes("collector install-launch-agent --dev"),
      { reconciledPid, plistMode: fs.statSync(stalePlist).mode & 0o777 });

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
        applyReceipt.retainedState.service === "service_ready_pending_fresh_signal" &&
        applyReceipt.runtime.installedIdentityMatched === true &&
        applyReceipt.credentialOperations === 0 &&
        applyReceipt.rollbackClaimed === false,
      { apply, applyReceipt });
    check("installed_launch_agent_manifest_is_private_regular_file",
      !fs.lstatSync(stalePlist).isSymbolicLink() &&
        fs.lstatSync(stalePlist).isFile() &&
        fs.lstatSync(stalePlist).nlink === 1 &&
        (fs.statSync(stalePlist).mode & 0o777) === 0o600,
      { mode: fs.statSync(stalePlist).mode & 0o777, path: stalePlist });
    check("apply_uses_frozen_lockfile_and_absolute_runtime_inputs",
      applyCommands.includes(`pnpm --dir ${applyTarget} install --frozen-lockfile`) &&
        applyCommands.includes(`--pnpm ${pnpm}`) &&
        !applyCommands.includes("pnpm install\n") &&
        !applyCommands.includes("git pull") &&
        !applyCommands.includes("git clone"),
      applyCommands);

    const installIdentity = path.join(applyTarget, ".git", "plimsoll-source-install.v1.json");
    check("apply_persists_private_hash_only_runtime_identity",
      (fs.statSync(installIdentity).mode & 0o777) === 0o600 &&
        !fs.readFileSync(installIdentity, "utf8").includes(process.execPath) &&
        !fs.readFileSync(installIdentity, "utf8").includes(pnpm) &&
        JSON.parse(fs.readFileSync(installIdentity, "utf8")).sourceCommit === expectedSha &&
        JSON.parse(fs.readFileSync(installIdentity, "utf8")).baselineTokenAttributedEvents === 0 &&
        /^sha256:[a-f0-9]{64}$/.test(JSON.parse(fs.readFileSync(installIdentity, "utf8")).runtimeInstanceHash) &&
        /^sha256:[a-f0-9]{64}$/.test(JSON.parse(fs.readFileSync(installIdentity, "utf8")).sourceRemoteHash),
      fs.readFileSync(installIdentity, "utf8"));

    fs.writeFileSync(path.join(applyTarget, ".git", "origin"), "https://example.invalid/wrong.git\n");
    const wrongRemoteVerify = await run(["verify", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: applyTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: applyState,
      },
    });
    const wrongRemoteReceipt = parseReceipt(wrongRemoteVerify);
    check("verify_refuses_checkout_from_a_different_remote",
      wrongRemoteVerify.code !== 0 && wrongRemoteReceipt.errorStage === "checkout_remote" &&
        wrongRemoteReceipt.retainedState.checkout === "remote_conflict",
      { wrongRemoteVerify, wrongRemoteReceipt });
    fs.writeFileSync(path.join(applyTarget, ".git", "origin"), "https://github.com/CryptoJym/plimsoll.git\n");

    const dirtyVerify = await run(["verify", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: applyTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: applyState,
        PLIMSOLL_FIXTURE_DIRTY: "1",
      },
    });
    const dirtyVerifyReceipt = parseReceipt(dirtyVerify);
    check("verify_refuses_dirty_tracked_source",
      dirtyVerify.code !== 0 && dirtyVerifyReceipt.errorStage === "checkout_dirty" &&
        dirtyVerifyReceipt.retainedState.checkout === "dirty_retained",
      { dirtyVerify, dirtyVerifyReceipt });

    const alternateNode = path.join(sandbox, "alternate-node-22");
    fs.symlinkSync(process.execPath, alternateNode);
    const wrongRuntimeVerify = await run([
      "verify",
      "--ref",
      expectedSha,
      "--node",
      alternateNode,
      "--pnpm",
      pnpm,
    ], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: applyTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: applyState,
        PLIMSOLL_FIXTURE_DOCTOR: "signal_verified",
      },
    });
    const wrongRuntimeReceipt = parseReceipt(wrongRuntimeVerify);
    check("verify_is_bound_to_installed_source_and_runtime_identity",
      wrongRuntimeVerify.code !== 0 && wrongRuntimeReceipt.errorStage === "installed_identity" &&
        wrongRuntimeReceipt.runtime.installedIdentityMatched === false &&
        wrongRuntimeReceipt.readiness === "not_checked",
      { wrongRuntimeVerify, wrongRuntimeReceipt });

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
        coldVerifyReceipt.errorStage === "fresh_signal_verification" &&
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
        contradictoryReceipt.errorStage === "fresh_signal_verification",
      { contradictory, contradictoryReceipt });

    const changedRuntime = await run(["verify", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: applyTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: applyState,
        PLIMSOLL_FIXTURE_DOCTOR: "signal_new_runtime",
      },
    });
    const changedRuntimeReceipt = parseReceipt(changedRuntime);
    check("verify_refuses_signal_from_a_different_runtime_instance",
      changedRuntime.code !== 0 && changedRuntimeReceipt.errorStage === "service_identity_binding" &&
        changedRuntimeReceipt.runtime.installedIdentityMatched === true,
      { changedRuntime, changedRuntimeReceipt });

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

    const oldSignalTarget = path.join(sandbox, "old-signal-target");
    const oldSignalState = path.join(sandbox, "old-signal-state");
    fs.mkdirSync(oldSignalState);
    const oldSignalApply = await run(["apply", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: oldSignalTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: oldSignalState,
        PLIMSOLL_FIXTURE_DOCTOR: "old_signal",
      },
    });
    const oldSignalApplyReceipt = parseReceipt(oldSignalApply);
    const oldSignalVerify = await run(["verify", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: oldSignalTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: oldSignalState,
        PLIMSOLL_FIXTURE_DOCTOR: "old_signal",
      },
    });
    const oldSignalVerifyReceipt = parseReceipt(oldSignalVerify);
    const freshAfterOld = await run(["verify", ...sourceArgs], {
      cwd: neutral,
      env: {
        ...baseEnv,
        PLIMSOLL_DIR: oldSignalTarget,
        PLIMSOLL_FIXTURE_STATE_DIR: oldSignalState,
        PLIMSOLL_FIXTURE_DOCTOR: "fresh_after_old",
      },
    });
    const freshAfterOldReceipt = parseReceipt(freshAfterOld);
    check("historical_signal_is_baselined_and_only_strict_growth_verifies",
      oldSignalApply.code === 0 && oldSignalApplyReceipt.state === "service_ready" &&
        oldSignalApplyReceipt.readiness === "service_ready" &&
        oldSignalVerify.code !== 0 && oldSignalVerifyReceipt.errorStage === "fresh_signal_verification" &&
        freshAfterOld.code === 0 && freshAfterOldReceipt.state === "signal_verified",
      { oldSignalApply, oldSignalApplyReceipt, oldSignalVerify, oldSignalVerifyReceipt, freshAfterOld, freshAfterOldReceipt });

    const retryTarget = path.join(sandbox, "retry-target");
    const retryState = path.join(sandbox, "retry-state");
    fs.mkdirSync(retryState);
    const staleRetryLock = machineLock;
    const staleRetryBytes = `${JSON.stringify({
      version: 1,
      pid: 999999,
      processStartFingerprint: `sha256:${"a".repeat(64)}`,
      ownerNonce: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    })}\n`;
    const interrupted = await withExclusiveLockFixture(staleRetryLock, staleRetryBytes, () =>
      run(["apply", ...sourceArgs], {
        cwd: neutral,
        env: {
          ...baseEnv,
          PLIMSOLL_DIR: retryTarget,
          PLIMSOLL_FIXTURE_STATE_DIR: retryState,
          PLIMSOLL_FIXTURE_INSTALL_FAIL_ONCE: "1",
        },
      }));
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
        PLIMSOLL_FIXTURE_OCCUPIED_PORT: String(occupiedPort),
      },
    });
    const staleRuntimeReceipt = parseReceipt(staleRuntime);
    check("occupied_port_readiness_failure_stops_loaded_service_and_retains_state",
      staleRuntime.code !== 0 && staleRuntimeReceipt.errorStage === "service_readiness" &&
        staleRuntimeReceipt.readiness === "invalid" &&
        ["unload_succeeded_stop_unverified", "unload_failed_stop_unverified"].includes(staleRuntimeReceipt.retainedState.service) &&
        !fs.existsSync(path.join(plimsollHome, "collector.pid")) &&
        occupiedRequests > 0 &&
        staleRuntimeReceipt.rollbackClaimed === false,
      { staleRuntime, staleRuntimeReceipt, occupiedPort, occupiedRequests });

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
      syncRefused.stdout,
      wrongRuntimeVerify.stdout,
      coldVerify.stdout,
      contradictory.stdout,
      signalVerify.stdout,
      oldSignalApply.stdout,
      oldSignalVerify.stdout,
      freshAfterOld.stdout,
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
          receipt.localOnlyRequired === true && receipt.localOnlyConfirmed === true),
      allReceipts);
    check("credential_files_remain_byte_exact",
      fs.readFileSync(path.join(home, ".codex", "auth.json"), "utf8") === credentialSentinel &&
        fs.readFileSync(path.join(home, ".claude", ".credentials.json"), "utf8") === credentialSentinel,
      home);
    check("per_uid_machine_coordination_lock_is_removed_after_each_operation",
      !fs.existsSync(machineLock) && fs.readdirSync(lockRoot).length === 0,
      { machineLock, lockArtifacts: fs.readdirSync(lockRoot) });

    const receipt = {
      issue: 128,
      ok: checks.every((entry) => entry.passed),
      isolation: {
        realNetworkCalls: 0,
        realPackageInstalls: 0,
        realLaunchAgentsTouched: 0,
        realToolConfigsTouched: 0,
        credentialReadsOrCopies: 0,
        perUidMachineCoordinationLock: "temporary_namespace_acquired_and_removed",
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
