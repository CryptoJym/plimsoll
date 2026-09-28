/** One packaged join through a local cloud and a fixture-only launchctl. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import path from "node:path";
import Database from "better-sqlite3";

import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { discoverCaptureRootCandidates } from "../packages/collector-cli/src/capture-root-inventory";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { useFixtureRoot } from "./lib/fixture-root";

const repo = path.resolve(import.meta.dirname, "..");
const cli = path.join(repo, "packages/collector-cli/dist/cli.mjs");
const root = fs.realpathSync(fs.mkdtempSync(path.join(repo, "join-setup-e2e-")));
const token = "pljt_fixture-only-secret-never-print";
const tenantId = "753a5a4f-c092-484b-b15e-0cfab3de4550";
const installKey = "pli_fixture_join_install_key";
const checks: string[] = [];

function check(name: string, condition: unknown) {
  assert.ok(condition, name);
  checks.push(name);
}

type ChildResult = { code: number | null; stdout: string; stderr: string };
function command(env: NodeJS.ProcessEnv, args: string[], stdin = "", executable = cli,
  tokenPrompt = false): Promise<ChildResult> {
  return new Promise((resolve, reject) => {
    const program = tokenPrompt ? "python3" : process.execPath;
    const argv = tokenPrompt
      ? [path.join(repo, "scripts/lib/join-token-prompt-pty.py"), process.execPath, executable, ...args]
      : [executable, ...args];
    const child = spawn(program, argv, {
      cwd: repo, env, stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      child.kill("SIGTERM");
      reject(new Error(`Timed out: ${args[0]}`));
    }, env.PLIMSOLL_PROOF_CLOCK_SKEW ? 27_000 : 90_000);
    child.stdout.setEncoding("utf8").on("data", (chunk: string) => { stdout += chunk; });
    child.stderr.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
    child.once("error", (error) => { clearTimeout(timer); reject(error); });
    child.once("close", (code) => { clearTimeout(timer); resolve({ code, stdout, stderr }); });
    child.stdin.end(stdin);
  });
}

function receipt(output: string): Record<string, any> {
  const start = output.lastIndexOf("\n{");
  return JSON.parse(output.slice(start < 0 ? 0 : start + 1)) as Record<string, any>;
}

function stubLaunchctl(bin: string) {
  fs.mkdirSync(bin, { recursive: true });
  const file = path.join(bin, "launchctl");
  fs.writeFileSync(file, [
    "#!/bin/sh",
    'state="${PLIMSOLL_PROOF_LAUNCHCTL_STATE:?}"',
    'trace="${PLIMSOLL_PROOF_LAUNCHCTL_TRACE:?}"',
    'if [ "$1" = "print" ]; then',
    '  if [ -f "$state" ]; then',
    '    pid="$(cat "$state")"',
    '    if kill -0 "$pid" 2>/dev/null; then',
    '      printf "    state = running\\n    pid = %s\\n    runs = 1\\n" "$pid"',
    '      exit 0',
    '    fi',
    '  fi',
    "  printf 'Could not find service \"com.plimsoll.collector\" in domain for user gui: %s\\n' \"$(id -u)\" >&2",
    '  exit 113',
    'fi',
    'if [ "$1" = "bootout" ]; then',
    '  if [ -f "$state" ]; then',
    '    pid="$(cat "$state")"',
    '    kill -TERM "$pid" 2>/dev/null || exit 1',
    '    i=0',
    '    while [ -f "$PLIMSOLL_HOME/collector.pid" ] && [ "$i" -lt 160 ]; do',
    '      i=$((i + 1)); sleep 0.05',
    '    done',
    '    rm -f "$state"',
    '    printf "bootout %s\\n" "$pid" >> "$trace"',
    '    if [ "${PLIMSOLL_PROOF_CRASH_AFTER_BOOTOUT:-}" = "1" ]; then kill -KILL "$PPID"; fi',
    '  fi',
    '  exit 0',
    'fi',
    'if [ "$1" = "bootstrap" ] || [ "$1" = "kickstart" ]; then',
    '  if [ -f "$state" ] && kill -0 "$(cat "$state")" 2>/dev/null; then exit 70; fi',
    '  "$PLIMSOLL_PROOF_NODE" "$PLIMSOLL_PROOF_CLI" start >> "$PLIMSOLL_PROOF_DAEMON_LOG" 2>&1 </dev/null &',
    '  pid=$!',
    '  printf "%s\\n" "$pid" > "$state"',
    '  printf "bootstrap %s\\n" "$pid" >> "$trace"',
    '  exit 0',
    'fi',
    'exit 64',
  ].join("\n") + "\n", { mode: 0o700 });
}

async function cloud(mode: "ack" | "no_ack" | "refuse") {
  const uploads: string[] = [];
  const server = http.createServer((request, response) => {
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk: string) => { body += chunk; });
    request.on("end", () => {
      if (request.url?.endsWith("/join")) {
        if (mode === "refuse") {
          response.writeHead(401, { "content-type": "application/json" });
          response.end(JSON.stringify({ ok: false, reason: "used" }));
          return;
        }
        const address = server.address();
        assert.ok(address && typeof address !== "string");
        response.writeHead(201, { "content-type": "application/json" });
        response.end(JSON.stringify({ ok: true, tenantId, installKey,
          uploadUrl: `http://127.0.0.1:${address.port}/api/work-intelligence/ingest` }));
        return;
      }
      if (!body) {
        response.writeHead(404, { "content-type": "application/json" });
        response.end(JSON.stringify({ ok: false, route: request.url ?? null }));
        return;
      }
      uploads.push(body);
      if (mode === "no_ack" && uploads.length > 1) {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ ok: true, accepted: 0 }));
        return;
      }
      const expectation = deliveryExpectation(body, JSON.parse(body).installKey);
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ ok: true, accepted: expectation.itemIds.length,
        ack: deliveryAcknowledgement(expectation, expectation.itemIds) }));
    });
  });
  let port = 0;
  for (let candidate = 49300; candidate <= 49399; candidate += 1) {
    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(candidate, "127.0.0.1", () => {
          server.removeListener("error", reject);
          resolve();
        });
      });
      port = candidate;
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EADDRINUSE") throw error;
    }
  }
  assert.ok(port, "No fixture port available in 49300-49399");
  return { port, uploads, close: async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  } };
}

async function collectorPort(except: number) {
  for (let candidate = 49300; candidate <= 49399; candidate += 1) {
    if (candidate === except) continue;
    const probe = net.createServer();
    try {
      await new Promise<void>((resolve, reject) => {
        probe.once("error", reject);
        probe.listen(candidate, "127.0.0.1", resolve);
      });
      await new Promise<void>((resolve) => probe.close(() => resolve()));
      return candidate;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EADDRINUSE") throw error;
    }
  }
  throw new Error("No collector fixture port available in 49300-49399");
}

async function waitForCollector(port: number) {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const ready = await new Promise<boolean>((resolve) => {
      const request = http.get(`http://127.0.0.1:${port}/healthz`, { timeout: 1_000 }, (response) => {
        response.resume();
        resolve(response.statusCode === 200);
      });
      request.once("error", () => resolve(false));
      request.once("timeout", () => { request.destroy(); resolve(false); });
    });
    if (ready) return true;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return false;
}

function fixture(name: string, port: number) {
  const directory = path.join(root, name);
  const home = path.join(directory, "home");
  const isolation = useFixtureRoot(directory, { home });
  fs.mkdirSync(home, { recursive: true, mode: 0o700 });
  const bin = path.join(directory, "bin");
  stubLaunchctl(bin);
  const installedCli = path.join(bin, "plimsoll");
  fs.symlinkSync(cli, installedCli);
  const state = path.join(directory, "launchctl.state");
  const trace = path.join(directory, "launchctl.trace");
  const env: NodeJS.ProcessEnv = { ...process.env, ...isolation.env,
    PATH: `${bin}${path.delimiter}${process.env.PATH ?? ""}`,
    PLIMSOLL_PROOF_JOIN_PORT: String(port),
    PLIMSOLL_PROOF_LAUNCHCTL_STATE: state,
    PLIMSOLL_PROOF_LAUNCHCTL_TRACE: trace,
    PLIMSOLL_PROOF_NODE: process.execPath,
    PLIMSOLL_PROOF_CLI: cli,
    PLIMSOLL_PROOF_DAEMON_LOG: path.join(directory, "daemon.log"),
    CI: "", GITHUB_ACTIONS: "",
  };
  isolation.restore();
  return { home, env, state, trace, data: env.PLIMSOLL_HOME!, installedCli };
}

async function stopFixture(env: NodeJS.ProcessEnv, state: string) {
  if (!fs.existsSync(state)) return;
  const result = await command(env, ["unload-launch-agent"]);
  if (result.code !== 0 && fs.existsSync(state)) {
    const pid = Number(fs.readFileSync(state, "utf8").trim());
    if (Number.isSafeInteger(pid) && pid > 0) process.kill(pid, "SIGTERM");
  }
}

async function joinedScenario(name: string, running: boolean, mode: "ack" | "no_ack") {
  const remote = await cloud(mode);
  const f = fixture(name, await collectorPort(remote.port));
  try {
    const codex = path.join(f.home, ".codex", "sessions");
    const studio = path.join(f.home, ".clientai", "studio", "borg", "conductors", "primary", "profile", "sessions");
    const unrelated = path.join(f.home, ".clientai", "studio", "borg", "conductors", "primary", "cache", "sessions");
    const privateFolder = path.join(f.home, "Documents", "private-sessions");
    for (const directory of [codex, unrelated, ...(name === "symlink_private" ? [privateFolder] : [studio])])
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    if (name === "symlink_private") {
      fs.mkdirSync(path.dirname(studio), { recursive: true, mode: 0o700 });
      fs.symlinkSync(privateFolder, studio);
    }
    if (name === "partial_roots") {
      const claude = path.join(f.home, ".claude", "projects");
      fs.mkdirSync(claude, { recursive: true, mode: 0o700 });
      fs.symlinkSync(path.join(f.home, "missing-target"), path.join(claude, "ambiguous.jsonl"));
    }
    fs.writeFileSync(path.join(studio, "rollout-prejoin.jsonl"), "{}\n", { mode: 0o600 });
    const candidates = discoverCaptureRootCandidates(f.home);
    if (name !== "symlink_private") check(`${name}_named_studio_rule_only`,
      candidates.some((entry) => entry.shape === "studio_codex_conductor" && entry.directory === studio) &&
      !candidates.some((entry) => entry.directory === unrelated));
    if (running) {
      fs.mkdirSync(f.data, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(f.data, "collector.config.json"),
        `${JSON.stringify(collectorConfigSchema.parse({ port: Number(f.env.PLIMSOLL_PROOF_JOIN_PORT) }), null, 2)}\n`, { mode: 0o600 });
      const installed = await command(f.env, ["install-launch-agent", "--load"]);
      check(`${name}_fixture_collector_started_before_join`, installed.code === 0 && fs.existsSync(f.state) &&
        await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)));
      if (name === "crash_after_bootout") f.env.PLIMSOLL_PROOF_CRASH_AFTER_BOOTOUT = "1";
      if (name === "edited_manifest") {
        const plist = path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist");
        const prior = fs.readFileSync(plist, "utf8");
        const changed = prior.replace(/(<key>PATH<\/key>\s*<string>)([^<]*)(<\/string>)/,
          (_match, open: string, value: string, close: string) => `${open}${value}:/opt/owner-custom-bin${close}`);
        assert.notEqual(changed, prior);
        fs.writeFileSync(plist, changed);
      }
    } else {
      check(`${name}_no_collector_installed_before_join`, !fs.existsSync(path.join(f.data, "collector.config.json")) &&
        !fs.existsSync(path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist")));
    }
    if (name === "clock_skew") {
      const preload = path.join(f.home, "freeze-join-clock.mjs");
      fs.writeFileSync(preload, 'if (process.argv[2] === "join") { const native = Date.now; Date.now = () => new Error().stack?.includes("acknowledgeJoinedCollector") ? 0 : native(); }\n');
      f.env.NODE_OPTIONS = `--import=${preload}`;
      f.env.PLIMSOLL_PROOF_CLOCK_SKEW = "1";
    }
    const prompt = name === "fresh";
    let joined: ChildResult;
    try {
      joined = await command(f.env, ["join", prompt ? "--token-prompt" : "--token-stdin", "--url",
        `http://127.0.0.1:${remote.port}`], `${token}\n`, f.installedCli, prompt);
    } catch (error) {
      if (name === "clock_skew") throw new Error("clock_skew_no_ack_failed_to_exit_within_27_seconds", { cause: error });
      throw error;
    }
    if (prompt && (joined.code !== 0 || joined.stdout.includes(token) || joined.stderr.includes(token))) {
      throw new Error(`fresh_prompt_hides_token_and_exits: ${JSON.stringify({ code: joined.code,
        tokenEchoed: joined.stdout.includes(token) || joined.stderr.includes(token),
        stderr: joined.stderr.slice(-600), stdoutTail: joined.stdout.slice(-600).replaceAll(token, "<fixture-token>") })}`);
    }
    if (prompt) check("fresh_prompt_hides_token_and_exits", true);
    const result = receipt(joined.stdout);
    if (name === "clock_skew") console.log(JSON.stringify({ scenario: name, joinCode: joined.code,
      joinStatus: result.status, reason: result.reason ?? null }));
    const config = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(path.join(f.data, "collector.config.json"), "utf8")));
    if (name === "symlink_private") {
      const enrolled = config.captureRoots?.some((entry) => entry.directory === privateFolder) ?? false;
      console.log(JSON.stringify({ scenario: name, joinCode: joined.code, joinStatus: result.status,
        privateFolderRegistered: enrolled }));
      check("join_never_registers_symlinked_private_folder", !enrolled);
      return;
    }
    if (name === "partial_roots") {
      console.log(JSON.stringify({ scenario: name, joinStatus: result.status, joinCode: joined.code,
        registeredRoots: config.captureRoots?.map((entry) => entry.directory) ?? [] }));
      check("partial_registration_leaves_no_roots", config.captureRoots?.length === 0);
      return;
    }
    if (name === "crash_after_bootout") {
      console.log(JSON.stringify({ scenario: name, joinStatus: result.status, joinCode: joined.code,
        collectorStateFileExists: fs.existsSync(f.state) }));
      check("crashed_add_restores_running_collector", fs.existsSync(f.state) &&
        await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)));
      return;
    }
    check(`${name}_registers_exactly_two_native_folders`, config.captureRoots?.length === 2 &&
      config.captureRoots.some((entry) => entry.directory === studio && entry.source === "codex") &&
      config.captureRoots.some((entry) => entry.directory === codex && entry.source === "codex") &&
      !config.captureRoots.some((entry) => entry.directory === unrelated));
    check(`${name}_announces_folders_before_capture`, joined.stdout.indexOf("Will record 2 agent folders") >= 0 &&
      joined.stdout.indexOf("Will record 2 agent folders") < joined.stdout.indexOf('"status":'));
    check(`${name}_joined_forward_only`, result.enrollment?.mode === "future_only" &&
      result.enrollment?.quarantinedHistoryRows === 0 &&
      !remote.uploads.some((body) => body.includes("rollout-prejoin")));
    check(`${name}_no_restart_instruction`, !joined.stdout.includes("nextSteps") &&
      !joined.stdout.includes("restart a running collector"));
    if (!fs.existsSync(f.trace)) throw new Error(`${name}: no fixture launchctl calls; ` +
      JSON.stringify({ code: joined.code, status: result.status, reason: result.reason,
        stderr: joined.stderr.slice(-600) }));
    const trace = fs.readFileSync(f.trace, "utf8").trim().split("\n");
    check(`${name}_never_bootstraps_two_live_daemons`, trace.filter((line) => line.startsWith("bootstrap")).length >= 1 &&
      trace.every((line, index) => !line.startsWith("bootstrap") || index === 0 || trace[index - 1]?.startsWith("bootout")));
    if (mode === "ack") {
      const ledger = new Database(path.join(f.data, "work-ledger.sqlite"), { readonly: true, fileMustExist: true });
      const probe = ledger.prepare("select uploaded_at as uploadedAt from buffered_events where id like 'join-setup-%'")
        .get() as { uploadedAt: string | null } | undefined;
      ledger.close();
      const finished = joined.code === 0 && result.status === "joined" &&
        result.daemon?.running === true && result.daemon?.readinessVerified === true &&
        result.daemon?.syncArmed === true && result.firstUploadKind === "setup_probe" &&
        typeof result.firstUploadAt === "string" && remote.uploads.length >= 2 &&
        probe?.uploadedAt === result.firstUploadAt &&
        joined.stdout.includes("First upload acknowledged");
      if (!finished) throw new Error(`${name}_single_command_finishes_ready_and_acknowledged: ` +
        JSON.stringify({ code: joined.code, status: result.status, reason: result.reason,
          daemon: result.daemon, firstUploadAt: result.firstUploadAt,
          uploadCount: remote.uploads.length,
          setupProbeAcknowledged: probe?.uploadedAt === result.firstUploadAt,
          plainResultSeen: joined.stdout.includes("First upload acknowledged") }));
      check(`${name}_single_command_finishes_ready_and_acknowledged`, true);
      if (name === "edited_manifest") {
        const plist = path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist");
        const kept = fs.readFileSync(plist, "utf8").includes("/opt/owner-custom-bin");
        console.log(JSON.stringify({ scenario: name, ownerPathPreserved: kept }));
        check("join_preserves_valid_owner_edited_launchagent_path", kept);
      }
      if (name === "fresh" && process.env.PLIMSOLL_PROOF_0744_CLI) {
        const old = await command(f.env, ["status", "--json"], "", process.env.PLIMSOLL_PROOF_0744_CLI);
        check("released_0744_cli_opens_head_joined_ledger", old.code === 0 &&
          old.stdout.includes('"syncConfigured": true'));
      }
    } else {
      check(`${name}_missing_ack_is_nonzero_but_collector_keeps_running`, joined.code !== 0 &&
        result.status === "joined_setup_incomplete" && /No first upload acknowledgement/.test(result.reason) &&
        result.daemon?.running === true && joined.stderr.includes("Collector remains running") &&
        fs.existsSync(f.state) && fs.existsSync(path.join(f.data, "collector.config.json")));
    }
  } finally {
    await stopFixture(f.env, f.state);
    await remote.close();
  }
}

async function joinOnlyScenario(name: string, option: "--no-daemon" | "ci") {
  const remote = await cloud("ack");
  const f = fixture(name, await collectorPort(remote.port));
  try {
    if (option === "ci") f.env.CI = "true";
    const joined = await command(f.env, ["join", "--token-stdin", "--url", `http://127.0.0.1:${remote.port}`,
      ...(option === "--no-daemon" ? ["--no-daemon"] : [])], `${token}\n`, f.installedCli);
    const result = receipt(joined.stdout);
    check(`${name}_join_only_keeps_daemon_absent`, joined.code === 0 && result.status === "joined" &&
      result.daemon?.setup === "skipped" && !fs.existsSync(f.state) &&
      !fs.existsSync(path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist")) &&
      remote.uploads.length === 1);
  } finally { await stopFixture(f.env, f.state); await remote.close(); }
}

async function refusedScenario(name: string, network: boolean) {
  const remote = await cloud(network ? "ack" : "refuse");
  const f = fixture(name, await collectorPort(remote.port));
  if (network) await remote.close();
  try {
    const before = fs.readdirSync(f.home);
    const joined = await command(f.env, ["join", "--token-stdin", "--url",
      `http://127.0.0.1:${remote.port}`], `${token}\n`, f.installedCli);
    check(`${name}_refusal_changes_no_collector_state`, joined.code !== 0 &&
      !fs.existsSync(path.join(f.data, "collector.config.json")) &&
      !fs.existsSync(path.join(f.data, "work-ledger.sqlite")) &&
      !fs.existsSync(f.state) && JSON.stringify(fs.readdirSync(f.home)) === JSON.stringify(before));
  } finally { if (!network) await remote.close(); }
}

async function main() {
  check("packaged_cli_exists", fs.statSync(cli).isFile());
  try {
    if (process.env.PR428_REVIEW_SCENARIO === "fresh_only") {
      await joinedScenario("fresh", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "running_only") {
      await joinedScenario("running_0744_layout", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "no_daemon_only") {
      await joinOnlyScenario("explicit_no_daemon", "--no-daemon");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "refused_only") {
      await refusedScenario("refused_token", false);
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "missing_ack_only") {
      await joinedScenario("missing_ack", false, "no_ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "clock_skew") {
      await joinedScenario("clock_skew", false, "no_ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "edited_manifest") {
      await joinedScenario("edited_manifest", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "symlink_private") {
      await joinedScenario("symlink_private", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "partial_roots") {
      await joinedScenario("partial_roots", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "crash_after_bootout") {
      await joinedScenario("crash_after_bootout", true, "ack");
      return;
    }
    await joinedScenario("fresh", false, "ack");
    await joinedScenario("running_0744_layout", true, "ack");
    await joinedScenario("missing_ack", false, "no_ack");
    await joinOnlyScenario("explicit_no_daemon", "--no-daemon");
    await joinOnlyScenario("ci_home", "ci");
    await refusedScenario("refused_token", false);
    await refusedScenario("network_failure", true);
    console.log(JSON.stringify({ proof: "join-setup-e2e", status: "passed", checks }));
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
