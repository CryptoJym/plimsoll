/** One packaged join through a local cloud and a fixture-only launchctl. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, captureBaselineStatus, completeAutomaticCaptureBaseline } from "../packages/collector-cli/src/capture-baseline";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { deriveCaptureRootIdentity, discoverCaptureRootCandidates } from "../packages/collector-cli/src/capture-root-inventory";
import { deliveryAcknowledgement, deliveryExpectation } from "../packages/collector-cli/src/delivery-ack";
import { dashboardSummary } from "../packages/collector-cli/src/dashboard-api";
import { LifecycleMutationAuthority } from
  "../packages/collector-cli/src/lifecycle-authority";
import { useFixtureRoot } from "./lib/fixture-root";

const repo = path.resolve(import.meta.dirname, "..");
const cli = path.join(repo, "packages/collector-cli/dist/cli.mjs");
const root = fs.realpathSync(fs.mkdtempSync(path.join(repo, "join-setup-e2e-")));
const token = "pljt_fixture-only-secret-never-print";
const tenantId = "753a5a4f-c092-484b-b15e-0cfab3de4550";
const installKey = "pli_fixture_join_install_key";
const studioSession = "12345678-1234-4234-8234-123456789abc";
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
    }, env.PLIMSOLL_PROOF_CLOCK_SKEW ? 90_000 : 90_000);
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
    '  if [ -f "${PLIMSOLL_PROOF_LOADED_STOPPED_MARKER:-/dev/null}" ]; then',
    '    printf "    state = not running\\n    runs = 1\\n"',
    '    exit 0',
    '  fi',
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
    '  if [ -f "${PLIMSOLL_PROOF_LOADED_STOPPED_MARKER:-/dev/null}" ]; then',
    '    rm -f "$PLIMSOLL_PROOF_LOADED_STOPPED_MARKER"',
    '    printf "bootout stopped\\n" >> "$trace"',
    '    exit 0',
    '  fi',
    '  if [ -f "$state" ]; then',
    '    pid="$(cat "$state")"',
    '    kill -TERM "$pid" 2>/dev/null || exit 1',
    '    i=0',
    '    while [ -f "$PLIMSOLL_HOME/collector.pid" ] && [ "$i" -lt 160 ]; do',
    '      i=$((i + 1)); sleep 0.05',
    '    done',
    '    rm -f "$state"',
    '    printf "bootout %s\\n" "$pid" >> "$trace"',
    '    bootouts="$(grep -c "^bootout " "$trace")"',
    '    if { [ "${PLIMSOLL_PROOF_EDIT_ON_SECOND_BOOTOUT:-}" = "1" ] && [ "$bootouts" -eq 2 ]; } || { [ "${PLIMSOLL_PROOF_EDIT_ON_FIRST_BOOTOUT:-}" = "1" ] && [ "$bootouts" -eq 1 ]; }; then',
    '      plist="$HOME/Library/LaunchAgents/com.plimsoll.collector.plist"',
    "      /usr/bin/perl -0777 -i -pe 's{(<key>PATH</key>\\s*<string>)([^<]*)(</string>)}{$1$2:/opt/owner-custom-bin$3}' \"$plist\"",
    '    fi',
    '    if [ "${PLIMSOLL_PROOF_EDIT_CONFIG_ON_SECOND_BOOTOUT:-}" = "1" ] && [ "$bootouts" -eq 2 ]; then',
    '      printf " " >> "$PLIMSOLL_HOME/collector.config.json"',
    '    fi',
    '    if [ "${PLIMSOLL_PROOF_TRUNCATE_PLIST_ON_SECOND_BOOTOUT:-}" = "1" ] && [ "$bootouts" -eq 2 ]; then',
    '      plist="$HOME/Library/LaunchAgents/com.plimsoll.collector.plist"',
    '      printf "<plist>" > "$plist"',
    '    fi',
    '    if [ "${PLIMSOLL_PROOF_SEAL_BASELINE_AFTER_BOOTOUT:-}" = "1" ]; then',
    '      /usr/bin/sqlite3 "$PLIMSOLL_HOME/work-ledger.sqlite" "update automatic_capture_baseline_state set status=\'complete\', completed_at=strftime(\'%Y-%m-%dT%H:%M:%fZ\',\'now\'), updated_at=strftime(\'%Y-%m-%dT%H:%M:%fZ\',\'now\'), files_discovered=files_validated, discovery_errors=0, stat_errors=0, error_code=null, error_at=null where source=\'codex\'"',
    '    fi',
    '    if [ "${PLIMSOLL_PROOF_CRASH_AFTER_BOOTOUT:-}" = "1" ]; then kill -KILL "$PPID"; fi',
    '    if [ "${PLIMSOLL_PROOF_KILL_JOIN_AND_ADD:-}" = "1" ]; then',
    '      join_pid="$(ps -p "$PPID" -o ppid= | tr -d " ")"',
    '      kill -KILL "$PPID" "$join_pid"',
    '    fi',
    '  fi',
    '  exit 0',
    'fi',
    'if [ "$1" = "bootstrap" ] || [ "$1" = "kickstart" ]; then',
    '  if [ -f "$state" ] && kill -0 "$(cat "$state")" 2>/dev/null; then exit 70; fi',
    '  plist="${3:-$HOME/Library/LaunchAgents/com.plimsoll.collector.plist}"',
    '  cli="$(/usr/libexec/PlistBuddy -c "Print :ProgramArguments:1" "$plist")" || exit 68',
    '  working_dir="$(/usr/libexec/PlistBuddy -c "Print :WorkingDirectory" "$plist")" || exit 68',
    '  ( cd "$working_dir" || exit 68; if [ "${PLIMSOLL_PROOF_DELAY_RESTART:-}" = "1" ]; then sleep 3; fi; exec "$PLIMSOLL_PROOF_NODE" "$cli" start ) >> "$PLIMSOLL_PROOF_DAEMON_LOG" 2>&1 </dev/null &',
    '  pid=$!',
    '  printf "%s\\n" "$pid" > "$state"',
    '  printf "bootstrap %s %s\\n" "$pid" "$cli" >> "$trace"',
    '  if [ "${PLIMSOLL_PROOF_KILL_PARENT_AFTER_BOOTSTRAP:-}" = "1" ]; then kill -KILL "$PPID"; fi',
    '  if [ "${PLIMSOLL_PROOF_WAIT_READY_AFTER_BOOTSTRAP:-}" = "1" ]; then',
    '    i=0',
    '    while ! /usr/bin/curl --silent --fail --max-time 1 "http://127.0.0.1:$PLIMSOLL_PROOF_JOIN_PORT/healthz" >/dev/null 2>&1 && [ "$i" -lt 160 ]; do',
    '      i=$((i + 1)); sleep 0.05',
    '    done',
    '  fi',
    '  exit 0',
    'fi',
    'exit 64',
  ].join("\n") + "\n", { mode: 0o700 });
}

async function cloud(mode: "ack" | "no_ack" | "refuse" | "timeout_once") {
  const uploads: string[] = [];
  const joins: string[] = [];
  const uniqueEvents = new Set<string>();
  const server = http.createServer((request, response) => {
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk: string) => { body += chunk; });
    request.on("end", () => {
      if (request.url?.endsWith("/join")) {
        joins.push(body);
        if (mode === "refuse") {
          response.writeHead(401, { "content-type": "application/json" });
          response.end(JSON.stringify({ ok: false, reason: "used" }));
          return;
        }
        const address = server.address();
        assert.ok(address && typeof address !== "string");
        response.writeHead(201, { "content-type": "application/json" });
        response.end(JSON.stringify({ ok: true, tenantId, workspaceName: "Utlyze | AI Native", installKey,
          uploadUrl: `http://127.0.0.1:${address.port}/api/work-intelligence/ingest` }));
        return;
      }
      if (!body) {
        response.writeHead(404, { "content-type": "application/json" });
        response.end(JSON.stringify({ ok: false, route: request.url ?? null }));
        return;
      }
      uploads.push(body);
      const payload = JSON.parse(body) as { events?: Array<{ event?: { id?: string } }> };
      for (const entry of payload.events ?? []) if (entry.event?.id) uniqueEvents.add(entry.event.id);
      if (mode === "timeout_once" && uploads.length === 2) return;
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
  return { port, uploads, joins, uniqueEvents, close: async () => {
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
  const installedCli = path.join(bin, "plimsoll.mjs");
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

async function joinedScenario(name: string, running: boolean, mode: "ack" | "no_ack" | "timeout_once") {
  const remote = await cloud(mode);
  const f = fixture(name, await collectorPort(remote.port));
  let priorGenerationCount = 0;
  try {
    const codex = path.join(f.home, ".codex", "sessions");
    const studio = path.join(f.home, ".clientai", "studio", "borg", "conductors", "primary", "profile", "sessions");
    const unrelated = path.join(f.home, ".clientai", "studio", "borg", "conductors", "primary", "cache", "sessions");
    const privateFolder = path.join(f.home, "Documents", "private-sessions");
    for (const directory of [codex, unrelated, ...(name === "symlink_private" ? [privateFolder] : [studio])])
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(path.dirname(codex), "config.toml"), 'model = "gpt-6-sol"\n', { mode: 0o600 });
    fs.writeFileSync(path.join(codex, `rollout-2026-09-27T00-00-00-${studioSession}.jsonl`),
      `${JSON.stringify({ type: "session_meta", timestamp: "2026-09-27T00:00:00.000Z",
        payload: { id: studioSession, originator: "codex_exec" } })}\n`, { mode: 0o600 });
    const foreign = path.join(f.home,
      ".clientai/studio/borg/conductors/other-tool/profile/sessions");
    if (name === "foreign_rollout" || name === "foreign_rollout_beyond_limit") {
      fs.mkdirSync(foreign, { recursive: true, mode: 0o700 });
      const positive = `${JSON.stringify({ type: "session_meta", timestamp: "2026-09-27T00:00:00.000Z",
        payload: { id: studioSession, originator: "codex_exec" } })}\n`;
      const padding = `${JSON.stringify({ type: "fixture_other", padding: "x".repeat(20_000) })}\n`;
      fs.writeFileSync(path.join(foreign,
        `rollout-2026-09-27T00-00-00-${studioSession}.jsonl`),
        `${name === "foreign_rollout_beyond_limit" ? positive + padding : ""}${JSON.stringify({
          type: "session_meta", timestamp: "2026-09-27T00:00:00.000Z",
          payload: { id: studioSession, originator: "claude_code" } })}\n`, { mode: 0o600 });
    }
    if (name.startsWith("exhausted_")) {
      const recent = path.join(codex, "2026", "09", "28");
      fs.mkdirSync(recent, { recursive: true, mode: 0o700 });
      for (let index = 0; index < 130; index += 1)
        fs.writeFileSync(path.join(recent, `rollout-2026-09-28T12-${String(index).padStart(3, "0")}-00-${studioSession}.jsonl`),
          '{"type":"other"}\n', { mode: 0o600 });
    }
    if (name === "symlink_private") {
      fs.mkdirSync(path.dirname(studio), { recursive: true, mode: 0o700 });
      fs.symlinkSync(privateFolder, studio);
    }
    if (name === "partial_roots" || name === "mixed_roots") {
      const claude = path.join(f.home, ".claude", "projects");
      fs.mkdirSync(claude, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(path.dirname(claude), "settings.json"), '{"permissions":{}}\n', { mode: 0o600 });
      fs.writeFileSync(path.join(claude, `${studioSession}.jsonl`), '{"type":"user","message":{}}\n',
        { mode: 0o600 });
      if (name === "partial_roots")
        fs.symlinkSync(path.join(f.home, "missing-target"), path.join(claude, "ambiguous.jsonl"));
    }
    fs.writeFileSync(path.join(path.dirname(studio), "config.toml"),
      'model = "gpt-6-sol"\n', { mode: 0o600 });
    fs.writeFileSync(path.join(studio, `rollout-2026-09-27T00-00-00-${studioSession}.jsonl`),
      `${JSON.stringify({ type: "session_meta", timestamp: "2026-09-27T00:00:00.000Z",
        payload: { id: studioSession, originator: "codex_exec" } })}\n`, { mode: 0o600 });
    const candidates = discoverCaptureRootCandidates(f.home);
    if (name !== "symlink_private") check(`${name}_named_studio_rule_only`,
      candidates.some((entry) => entry.shape === "studio_codex_conductor" && entry.directory === studio) &&
      !candidates.some((entry) => entry.directory === unrelated));
    if (name === "replay_timeout_then_ack") {
      fs.mkdirSync(f.data, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(f.data, "collector.config.json"),
        `${JSON.stringify(collectorConfigSchema.parse({ port: Number(f.env.PLIMSOLL_PROOF_JOIN_PORT),
          delivery: { requestTimeoutSeconds: 1 } }), null, 2)}\n`, { mode: 0o600 });
    }
    if (running) {
      fs.mkdirSync(f.data, { recursive: true, mode: 0o700 });
      const fleetRoot = name === "fleet_label" || name === "crash_after_config_commit" ? [{
        ...deriveCaptureRootIdentity("fleet-label-not-hostname", "codex", codex),
        source: "codex" as const, directory: codex, installationEpochId: randomUUID(),
      }] : [];
      fs.writeFileSync(path.join(f.data, "collector.config.json"),
        `${JSON.stringify(collectorConfigSchema.parse({ port: Number(f.env.PLIMSOLL_PROOF_JOIN_PORT),
          ...(fleetRoot.length ? { captureRoots: fleetRoot,
            enrollmentMachineLabel: "fleet-label-not-hostname" } : {}) }), null, 2)}\n`, { mode: 0o600 });
      if (name === "crash_after_config_commit") {
        const buffer = new LocalEventBuffer(path.join(f.data, "work-ledger.sqlite"));
        try {
          const timestamp = new Date().toISOString();
          const begun = beginAutomaticCaptureBaseline(buffer.database, "codex", {
            startedAt: timestamp, filesDiscovered: 0,
          });
          completeAutomaticCaptureBaseline(buffer.database, "codex", {
            runId: begun.latestRun!.runId, completedAt: timestamp,
          });
        } finally { buffer.close(); }
      }
      const installed = await command(f.env, ["install-launch-agent", "--load"], "",
        (["running_0744_layout", "stopped_0744_layout", "loaded_stopped_0744_layout",
          "mid_restart_0744_layout", "legacy_path_drift_0744",
          "crash_manifest_link_running"].includes(name)) && process.env.PLIMSOLL_PROOF_0744_CLI
          ? process.env.PLIMSOLL_PROOF_0744_CLI : f.installedCli);
      const started = installed.code === 0 && fs.existsSync(f.state) &&
        await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      if (!started) throw new Error(`${name}_fixture_collector_started_before_join: ${JSON.stringify({
        code: installed.code, stdout: installed.stdout.slice(-1200), stderr: installed.stderr.slice(-1200),
        trace: fs.existsSync(f.trace) ? fs.readFileSync(f.trace, "utf8") : null,
        daemonLog: fs.existsSync(f.env.PLIMSOLL_PROOF_DAEMON_LOG!)
          ? fs.readFileSync(f.env.PLIMSOLL_PROOF_DAEMON_LOG!, "utf8").slice(-1200) : null,
      })}`);
      check(`${name}_fixture_collector_started_before_join`, true);
      if (name === "stopped_0744_layout") {
        const stopped = await command(f.env, ["unload-launch-agent"], "",
          process.env.PLIMSOLL_PROOF_0744_CLI);
        check("released_0744_agent_unloaded_before_join", stopped.code === 0 &&
          !fs.existsSync(f.state) &&
          !await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)));
        console.log(JSON.stringify({ scenario: name, priorTemplateExists: fs.existsSync(path.join(
          f.home, "Library/LaunchAgents/com.plimsoll.collector.plist.plimsoll-owned-template.json")),
          priorLifecycleStateExists: fs.existsSync(path.join(f.data, "lifecycle/state.json")) }));
      }
      if (name === "loaded_stopped_0744_layout") {
        const pid = Number(fs.readFileSync(f.state, "utf8").trim());
        process.kill(pid, "SIGTERM");
        check("released_0744_agent_stopped_before_join",
          !await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)));
        fs.rmSync(f.state, { force: true });
        const marker = path.join(path.dirname(f.state), "loaded-stopped.marker");
        fs.writeFileSync(marker, "1\n", { mode: 0o600 });
        f.env.PLIMSOLL_PROOF_LOADED_STOPPED_MARKER = marker;
      }
      if (name === "mid_restart_0744_layout") f.env.PLIMSOLL_PROOF_DELAY_RESTART = "1";
      if (name === "corrupt_obligation_loaded")
        fs.writeFileSync(path.join(f.data, "join.restart-obligation.json"), "", { mode: 0o600 });
      if (name === "crash_after_config_commit") {
        const buffer = new LocalEventBuffer(path.join(f.data, "work-ledger.sqlite"));
        try {
          const at = new Date().toISOString();
          const before = captureBaselineStatus(buffer.database).sources.find((row) => row.source === "codex");
          const runId = before?.latestRun?.runId ??
            beginAutomaticCaptureBaseline(buffer.database, "codex", { startedAt: at, filesDiscovered: 0 }).latestRun!.runId;
          if (before?.status !== "complete")
            completeAutomaticCaptureBaseline(buffer.database, "codex", { runId, completedAt: at });
          check("postcommit_fixture_baseline_complete_before_join",
            captureBaselineStatus(buffer.database).sources.find((row) => row.source === "codex")?.status === "complete");
          priorGenerationCount = (buffer.database.prepare("select count(*) as n from automatic_capture_baseline_generations")
            .get() as { n: number }).n;
        } finally { buffer.close(); }
      }
      if (name === "crash_after_bootout") f.env.PLIMSOLL_PROOF_CRASH_AFTER_BOOTOUT = "1";
      if (name === "crash_parent_after_bootout") f.env.PLIMSOLL_PROOF_KILL_JOIN_AND_ADD = "1";
      if (name === "delayed_restart") f.env.PLIMSOLL_PROOF_DELAY_RESTART = "1";
      // The published 0.7.44 CLI changes the manifest by itself. In CI,
      // where that optional artifact is absent, PATH drift still forces the
      // installer through the same interrupted publish while a daemon serves.
      if (name === "path_drift" || name === "owner_edit_during_join" ||
          name === "owner_edit_restore_conflict" || name === "owner_edit_unreadable" ||
          name === "owner_edit_after_recheck" ||
          name === "legacy_path_drift_0744" ||
          (name === "crash_manifest_link_running" && !process.env.PLIMSOLL_PROOF_0744_CLI))
        f.env.PATH = `${f.env.PATH}:/opt/new-toolchain`;
      if (name === "owner_edit_during_join") f.env.PLIMSOLL_PROOF_EDIT_ON_SECOND_BOOTOUT = "1";
      if (name === "owner_edit_restore_conflict") {
        f.env.PLIMSOLL_PROOF_EDIT_ON_SECOND_BOOTOUT = "1";
        f.env.PLIMSOLL_PROOF_EDIT_CONFIG_ON_SECOND_BOOTOUT = "1";
      }
      if (name === "owner_edit_after_recheck") {
        f.env.PLIMSOLL_PROOF_EDIT_ON_SECOND_BOOTOUT = "1";
        const preload = path.join(f.home, "edit-after-recheck.mjs");
        fs.writeFileSync(preload, [
          'import fs from "node:fs";',
          'const original = fs.renameSync;',
          'fs.renameSync = (...args) => {',
          '  original(...args);',
          '  if (process.argv[2] === "join" && String(args[0]).includes(".join-root-rollback-") &&',
          '      String(args[1]).endsWith("/collector.config.json")) {',
          '    const plist = process.env.HOME + "/Library/LaunchAgents/com.plimsoll.collector.plist";',
          '    const value = fs.readFileSync(plist, "utf8");',
          '    fs.writeFileSync(plist, value.replace(/(<key>PATH<\\/key>\\s*<string>)([^<]*)(<\\/string>)/,',
          '      "$1$2:/opt/owner-after-recheck$3"));',
          '  }',
          '};',
        ].join("\n") + "\n", { mode: 0o600 });
        f.env.NODE_OPTIONS = `--import=${preload}`;
      }
      if (name === "owner_edit_unreadable")
        f.env.PLIMSOLL_PROOF_TRUNCATE_PLIST_ON_SECOND_BOOTOUT = "1";
      if (name === "owner_edit_first_unload") f.env.PLIMSOLL_PROOF_EDIT_ON_FIRST_BOOTOUT = "1";
      if (name === "crash_after_config_commit") f.env.PLIMSOLL_PROOF_SEAL_BASELINE_AFTER_BOOTOUT = "1";
      if (name === "crash_after_config_commit" || name === "crash_fresh_after_config_commit") {
        const preload = path.join(f.home, "kill-after-config-commit.mjs");
        fs.writeFileSync(preload, [
          'import fs from "node:fs";',
          'const original = fs.renameSync;',
          'fs.renameSync = (...args) => {',
          '  original(...args);',
          '  if (process.argv.includes("--join-setup-child") && String(args[1]).endsWith("/collector.config.json"))',
          '    process.kill(process.pid, "SIGKILL");',
          '};',
        ].join("\n") + "\n", { mode: 0o600 });
        f.env.NODE_OPTIONS = `--import=${preload}`;
      }
      if (name === "edited_manifest") {
        const plist = path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist");
        const prior = fs.readFileSync(plist, "utf8");
        const changed = prior.replace(/(<key>PATH<\/key>\s*<string>)([^<]*)(<\/string>)/,
          (_match, open: string, value: string, close: string) => `${open}${value}:/opt/owner-custom-bin${close}`);
        assert.notEqual(changed, prior);
        fs.writeFileSync(plist, changed);
      }
      if (name === "program_edit") {
        const plist = path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist");
        const prior = fs.readFileSync(plist, "utf8");
        const changed = prior.replace(`<string>${cli}</string>`,
          `<string>${path.join(path.dirname(cli), "custom-cli.mjs")}</string>`);
        assert.notEqual(changed, prior);
        fs.writeFileSync(plist, changed);
      }
    } else if (name !== "replay_timeout_then_ack") {
      check(`${name}_no_collector_installed_before_join`, !fs.existsSync(path.join(f.data, "collector.config.json")) &&
        !fs.existsSync(path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist")));
    }
    if (name === "crash_fresh_after_config_commit") {
      const preload = path.join(f.home, "kill-after-config-commit.mjs");
      fs.writeFileSync(preload, [
        'import fs from "node:fs";',
        'const original = fs.renameSync;',
        'fs.renameSync = (...args) => {',
        '  original(...args);',
        '  if (process.argv.includes("--join-setup-child") && String(args[1]).endsWith("/collector.config.json"))',
        '    process.kill(process.pid, "SIGKILL");',
        '};',
      ].join("\n") + "\n", { mode: 0o600 });
      f.env.NODE_OPTIONS = `--import=${preload}`;
    }
    if (name === "crash_after_bootstrap") f.env.PLIMSOLL_PROOF_KILL_PARENT_AFTER_BOOTSTRAP = "1";
    if (name.startsWith("crash_manifest_link")) {
      const preload = path.join(f.home, "crash-manifest-link.mjs");
      fs.writeFileSync(preload, [
        'import fs from "node:fs";',
        'const native = fs.linkSync;',
        'fs.linkSync = (...args) => { native(...args); if (process.argv[2] === "join" && String(args[1]).endsWith("/com.plimsoll.collector.plist")) process.kill(process.pid, "SIGKILL"); };',
      ].join("\n") + "\n", { mode: 0o600 });
      f.env.NODE_OPTIONS = `--import=${preload}`;
    }
    if (name.startsWith("crash_obligation_")) {
      const preload = path.join(f.home, "crash-obligation.mjs");
      const atOpen = name === "crash_obligation_open";
      const atRename = name === "crash_obligation_rename";
      const atLink = name === "crash_obligation_link";
      const atDirectoryFsync = name === "crash_obligation_directory_fsync";
      fs.writeFileSync(preload, [
        'import fs from "node:fs";',
        'const tracked = new Set();',
        'const nativeOpen = fs.openSync, nativeFsync = fs.fsyncSync, nativeRename = fs.renameSync, nativeLink = fs.linkSync;',
        'fs.openSync = (...args) => { const fd = nativeOpen(...args); if (process.argv[2] === "join" && String(args[0]).includes("/join.restart-obligation.json")) { tracked.add(fd); if (' + String(atOpen) + ') process.kill(process.pid, "SIGKILL"); } return fd; };',
        'fs.fsyncSync = (fd) => { nativeFsync(fd); if (process.argv[2] === "join" && tracked.has(fd) && !' + String(atRename || atLink || atDirectoryFsync) + ') process.kill(process.pid, "SIGKILL"); if (process.argv[2] === "join" && ' + String(atDirectoryFsync) + ' && fs.fstatSync(fd).isDirectory() && fs.existsSync(process.env.PLIMSOLL_HOME + "/join.restart-obligation.json")) process.kill(process.pid, "SIGKILL"); };',
        'fs.renameSync = (...args) => { nativeRename(...args); if (process.argv[2] === "join" && ' + String(atRename) + ' && String(args[1]).endsWith("/join.restart-obligation.json")) process.kill(process.pid, "SIGKILL"); };',
        'fs.linkSync = (...args) => { nativeLink(...args); if (process.argv[2] === "join" && ' + String(atLink) + ' && String(args[1]).endsWith("/join.restart-obligation.json")) process.kill(process.pid, "SIGKILL"); };',
      ].join("\n") + "\n", { mode: 0o600 });
      f.env.NODE_OPTIONS = `--import=${preload}`;
    }
    if (name === "clock_skew") {
      const preload = path.join(f.home, "freeze-join-clock.mjs");
      fs.writeFileSync(preload, 'if (process.argv[2] === "join") { const native = Date.now; Date.now = () => new Error().stack?.includes("acknowledgeJoinedCollector") ? 0 : native(); }\n');
      f.env.NODE_OPTIONS = `--import=${preload}`;
      f.env.PLIMSOLL_PROOF_CLOCK_SKEW = "1";
    }
    const prompt = name === "fresh";
    if (name === "two_join_race") {
      const args = ["join", "--token-stdin", "--url", `http://127.0.0.1:${remote.port}`];
      const pair = await Promise.all([
        command(f.env, args, `${token}\n`, f.installedCli),
        command(f.env, args, `${token}\n`, f.installedCli),
      ]);
      const statuses = pair.map((entry) => receipt(entry.stdout).status);
      const runningAfter = fs.existsSync(f.state) &&
        await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      const obligationPresent = fs.existsSync(path.join(f.data, "join.restart-obligation.json"));
      const config = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(
        path.join(f.data, "collector.config.json"), "utf8")));
      console.log(JSON.stringify({ scenario: name, exits: pair.map((entry) => entry.code),
        statuses, cloudJoins: remote.joins.length, runningAfter, obligationPresent,
        roots: config.captureRoots?.length ?? 0 }));
      check("two_concurrent_joins_leave_one_serving_collector_and_no_obligation",
        statuses.includes("joined") && runningAfter && !obligationPresent &&
        (config.captureRoots?.length ?? 0) === 2);
      return;
    }
    let joined: ChildResult;
    try {
      joined = await command(f.env, ["join", prompt ? "--token-prompt" : "--token-stdin", "--url",
        `http://127.0.0.1:${remote.port}`,
        ...(name === "exhausted_explicit" ? ["--add-root", "codex", codex] : [])],
      `${token}\n`, f.installedCli, prompt);
    } catch (error) {
      if (name === "clock_skew") throw new Error("clock_skew_no_ack_failed_to_exit_within_27_seconds", { cause: error });
      throw error;
    }
    if (name === "crash_after_bootstrap") {
      delete f.env.PLIMSOLL_PROOF_KILL_PARENT_AFTER_BOOTSTRAP;
      const retry = await command(f.env, ["join", "--token-stdin", "--url",
        `http://127.0.0.1:${remote.port}`], `${token}\n`, f.installedCli);
      const retryResult = receipt(retry.stdout);
      const running = fs.existsSync(f.state) && await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      console.log(JSON.stringify({ scenario: name, firstExit: joined.code, retryExit: retry.code,
        retryStatus: retryResult.status, retryReason: retryResult.reason, retryMessage: retryResult.message, retryCloudJoins: remote.joins.length, runningAfterRetry: running }));
      check("post_bootstrap_parent_crash_retry_recovers", joined.code === null && retry.code === 0 &&
        retryResult.status === "joined" && running);
      return;
    }
    if (name.startsWith("crash_manifest_link")) {
      const plist = path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist");
      const stats = fs.existsSync(plist) ? fs.statSync(plist) : null;
      delete f.env.NODE_OPTIONS;
      const retry = await command(f.env, ["join", "--token-stdin", "--url",
        `http://127.0.0.1:${remote.port}`], `${token}\n`, f.installedCli);
      const retryResult = receipt(retry.stdout);
      const running = fs.existsSync(f.state) && await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      const ownedTemplate = `${plist}.plimsoll-owned-template.json`;
      const identityCount = fs.readdirSync(f.data).filter((entry) =>
        /^launch-agent-template-[0-9a-f]{64}\.identity\.json$/.test(entry)).length;
      const ownershipRepaired = fs.existsSync(ownedTemplate) && identityCount === 1;
      console.log(JSON.stringify({ scenario: name, firstExit: joined.code, plistLinks: stats?.nlink ?? null,
        retryExit: retry.code, retryStatus: retryResult.status, retryMessage: retryResult.message,
        collectorRunningAfterRetry: running, ownershipRepaired }));
      check("manifest_publish_crash_retry_recovers_and_serves", joined.code === null && retry.code === 0 &&
        retryResult.status === "joined" && running && ownershipRepaired);
      return;
    }
    if (name.startsWith("crash_obligation_")) {
      const obligation = path.join(f.data, "join.restart-obligation.json");
      const bytes = fs.existsSync(obligation) ? fs.statSync(obligation).size : -1;
      delete f.env.NODE_OPTIONS;
      const retry = await command(f.env, ["join", "--token-stdin", "--url",
        `http://127.0.0.1:${remote.port}`], `${token}\n`, f.installedCli);
      const retryResult = receipt(retry.stdout);
      const running = fs.existsSync(f.state) && await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      console.log(JSON.stringify({ scenario: name, firstExit: joined.code, obligationBytes: bytes,
        retryExit: retry.code, retryStatus: retryResult.status, retryCloudJoins: remote.joins.length,
        collectorRunningAfterRetry: running }));
      check("fresh_obligation_write_crash_retry_recovers_and_serves", joined.code === null && retry.code === 0 &&
        retryResult.status === "joined" && running);
      return;
    }
    if (name === "crash_fresh_after_config_commit") {
      const config = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(
        path.join(f.data, "collector.config.json"), "utf8")));
      const result = receipt(joined.stdout);
      const runningAfterRecovery = fs.existsSync(f.state) &&
        await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      console.log(JSON.stringify({ scenario: name, joinExit: joined.code, status: result.status,
        rootCount: config.captureRoots?.length ?? 0, runningAfterRecovery,
        obligationPresent: fs.existsSync(path.join(f.data, "join.restart-obligation.json")) }));
      check("fresh_child_crash_recovers_first_launch_agent_and_collector",
        joined.code !== 0 && result.status === "joined_setup_incomplete" &&
        (config.captureRoots?.length ?? 0) === 0 && runningAfterRecovery &&
        !fs.existsSync(path.join(f.data, "join.restart-obligation.json")) &&
        fs.existsSync(path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist")));
      return;
    }
    if (name === "crash_parent_after_bootout") {
      const obligation = path.join(f.data, "join.restart-obligation.json");
      const stopped = !fs.existsSync(f.state) &&
        !await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      delete f.env.PLIMSOLL_PROOF_KILL_JOIN_AND_ADD;
      const retry = await command(f.env, ["join", "--token-stdin", "--url",
        `http://127.0.0.1:${remote.port}`], `${token}\n`, f.installedCli);
      const retryResult = receipt(retry.stdout);
      const running = fs.existsSync(f.state) &&
        await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      const current = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(
        path.join(f.data, "collector.config.json"), "utf8")));
      console.log(JSON.stringify({ scenario: name, firstExit: joined.code,
        obligationPresentAfterRetry: fs.existsSync(obligation), stopped,
        retryExit: retry.code, retryStatus: retryResult.status, retryReason: retryResult.reason,
        retryCloudJoins: remote.joins.length, collectorRunningAfterRetry: running,
        rootsAfterRetry: current.captureRoots?.length ?? 0 }));
      check("parent_crash_retry_restores_prior_collector_and_consistent_roots",
        joined.code === null && stopped && !fs.existsSync(obligation) && running &&
        retry.code === 0 && retryResult.status === "joined" && current.captureRoots?.length === 2);
      return;
    }
    if (name === "crash_after_config_commit") {
      const configPath = path.join(f.data, "collector.config.json");
      const current = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
      const ledger = new Database(path.join(f.data, "work-ledger.sqlite"), { readonly: true });
      let generationCount: number;
      let baselineState: unknown;
      let journalState: unknown;
      try {
        generationCount = (ledger.prepare("select count(*) as n from automatic_capture_baseline_generations")
          .get() as { n: number }).n;
        baselineState = ledger.prepare("select source, status, files_baselined from automatic_capture_baseline_state")
          .all();
        journalState = (ledger.prepare("select count(*) as n from sqlite_master where type='table' and name='join_root_registration_journal'")
          .get() as { n: number }).n > 0
          ? ledger.prepare("select operation_id, state, seals_json from join_root_registration_journal").all()
          : "absent";
      } finally { ledger.close(); }
      console.log(JSON.stringify({ scenario: name, joinExit: joined.code,
        joinStatus: receipt(joined.stdout).status, rootsAfterParentRollback: current.captureRoots?.length ?? 0,
        obligationPresent: fs.existsSync(path.join(f.data, "join.restart-obligation.json")),
        priorGenerationCount, generationCount, baselineState, journalState, collectorRunning: fs.existsSync(f.state) &&
          await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)) }));
      check("postcommit_child_crash_restores_config_and_sealed_generations",
        joined.code !== 0 && current.captureRoots?.length === 1 &&
        generationCount === priorGenerationCount &&
        Array.isArray(journalState) && journalState.length === 0 &&
        !fs.existsSync(path.join(f.data, "join.restart-obligation.json")) &&
        Array.isArray(baselineState) && baselineState.every((row: any) => row.files_baselined === 0) &&
        fs.existsSync(f.state) && await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)));
      return;
    }
    if (name === "replay_timeout_default") {
      const ids = remote.uploads.flatMap((body) =>
        (JSON.parse(body) as { events?: Array<{ event: { id: string } }> }).events?.map((row) => row.event.id) ?? []);
      console.log(JSON.stringify({ scenario: name, joinCode: joined.code,
        joinStatus: receipt(joined.stdout).status, reason: receipt(joined.stdout).reason,
        uploadCount: ids.length, uniqueEventIds: new Set(ids).size }));
      check("default_contact_timeout_retries_same_event_and_joins", joined.code === 0 &&
        receipt(joined.stdout).status === "joined" && ids.length >= 3 && new Set(ids).size === 1 &&
        fs.existsSync(f.state) && await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)));
      return;
    }
    if (prompt && (joined.code !== 0 || joined.stdout.includes(token) || joined.stderr.includes(token))) {
      throw new Error(`fresh_prompt_hides_token_and_exits: ${JSON.stringify({ code: joined.code,
        tokenEchoed: joined.stdout.includes(token) || joined.stderr.includes(token),
        stderr: joined.stderr.slice(-600), stdoutTail: joined.stdout.slice(-600).replaceAll(token, "<fixture-token>") })}`);
    }
    if (prompt) check("fresh_prompt_hides_token_and_exits", true);
    const result = receipt(joined.stdout);
    if (name === "legacy_path_drift_0744") {
      const plist = path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist");
      console.log(JSON.stringify({ scenario: name, exit: joined.code, status: result.status,
        reason: result.message ?? result.reason ?? null, plistPresent: fs.existsSync(plist),
        cloudJoins: remote.joins.length }));
      check("released_0744_template_allows_shell_path_drift", joined.code === 0 &&
        result.status === "joined");
      return;
    }
    if (name === "owner_edit_during_join" || name === "owner_edit_first_unload") {
      const plist = path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist");
      const ownerPathPreserved = fs.readFileSync(plist, "utf8").includes("/opt/owner-custom-bin");
      const trace = fs.readFileSync(f.trace, "utf8");
      const running = fs.existsSync(f.state) &&
        await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      const config = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(
        path.join(f.data, "collector.config.json"), "utf8")));
      const obligationPresent = fs.existsSync(path.join(f.data, "join.restart-obligation.json"));
      console.log(JSON.stringify({ scenario: name, exit: joined.code, status: result.status,
        ownerPathPreserved, bootouts: trace.split("\n").filter((line) => line.startsWith("bootout ")).length,
        running, rootCount: config.captureRoots?.length ?? 0, obligationPresent,
        trace, reason: result.reason ?? null }));
      check("owner_edit_during_join_is_preserved_and_refused", joined.code !== 0 &&
        result.status === "joined_setup_incomplete" && ownerPathPreserved && running &&
        (config.captureRoots?.length ?? 0) === 0 && !obligationPresent &&
        trace.split("\n").filter((line) => line.startsWith("bootout ")).length === 2);
      return;
    }
    if (name === "owner_edit_restore_conflict" || name === "owner_edit_unreadable" ||
        name === "owner_edit_after_recheck") {
      const plist = path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist");
      const plistText = fs.readFileSync(plist, "utf8");
      const configText = fs.readFileSync(path.join(f.data, "collector.config.json"), "utf8");
      const trace = fs.readFileSync(f.trace, "utf8");
      const running = fs.existsSync(f.state) &&
        await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      const obligationPresent = fs.existsSync(path.join(f.data, "join.restart-obligation.json"));
      console.log(JSON.stringify({ scenario: name, exit: joined.code, status: result.status,
        running, obligationPresent, ownerPathPreserved: plistText.includes("/opt/owner-custom-bin"),
        ownerConfigPreserved: name === "owner_edit_restore_conflict" ? configText.endsWith(" ") : null,
        laterOwnerEditPreserved: plistText.includes("/opt/owner-after-recheck"),
        plistUnreadable: plistText === "<plist>", bootouts: trace.split("\n").filter((line) =>
          line.startsWith("bootout ")).length, reason: result.reason ?? null }));
      const ownerPlistIntact = name === "owner_edit_restore_conflict"
        ? plistText.includes("/opt/owner-custom-bin")
        : name === "owner_edit_after_recheck"
          ? plistText.includes("/opt/owner-custom-bin") &&
            plistText.includes("/opt/owner-after-recheck")
          : plistText === "<plist>";
      if (name === "owner_edit_unreadable") {
        check("unreadable_owner_plist_is_preserved_and_named", joined.code !== 0 &&
          !running && ownerPlistIntact && /unreadable|invalid/i.test(String(result.reason)));
      } else {
        check(`${name}_keeps_collector_serving_after_refusal`, joined.code !== 0 && running &&
          ownerPlistIntact && /restarted from the current owner LaunchAgent/i.test(String(result.reason)) &&
          (name !== "owner_edit_restore_conflict" || configText.endsWith(" ")));
      }
      if (name === "owner_edit_restore_conflict") {
        const retry = await command(f.env, ["join", "--resume"], "", f.installedCli);
        const afterRetry = await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
        const ownerBytesRetained = fs.readFileSync(plist, "utf8") === plistText &&
          fs.readFileSync(path.join(f.data, "collector.config.json"), "utf8") === configText;
        const retryStatus = /"status":\s*"([^"]+)"/.exec(retry.stdout)?.[1] ?? null;
        console.log(JSON.stringify({ scenario: "owner_edit_restore_conflict_retry",
          exit: retry.code, status: retryStatus,
          running: afterRetry, ownerBytesRetained, stderr: retry.stderr.slice(0, 300) }));
        check("config_conflict_retry_restores_owner_collector", retry.code !== 0 &&
          retryStatus === "join_recovery_failed" &&
          afterRetry && ownerBytesRetained);
      }
      return;
    }
    if (name === "corrupt_obligation_loaded") {
      const notes = fs.readdirSync(f.data).filter((entry) =>
        entry.startsWith("join.restart-obligation.recovery-") && entry.endsWith(".json"));
      const aside = fs.readdirSync(f.data).filter((entry) =>
        entry.startsWith("join.restart-obligation.json.unreadable-"));
      const runningAfter = fs.existsSync(f.state) &&
        await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
      console.log(JSON.stringify({ scenario: name, joinCode: joined.code, joinStatus: result.status,
        recoveryNotes: notes.length, asideFiles: aside.length, runningAfter }));
      check("unreadable_preunload_obligation_is_set_aside_only_with_verified_service",
        joined.code === 0 && result.status === "joined" && notes.length === 1 &&
        aside.length === 1 && runningAfter);
      return;
    }
    if (name === "fresh" && process.env.PLIMSOLL_PROOF_TRANSCRIPT === "1") {
      console.log(JSON.stringify({ scenario: "fresh_transcript", lines: joined.stdout.split("\n")
        .filter((line) => line.startsWith("Will record ") ||
          line.startsWith("Found, not recorded:") || line.startsWith("Connected to ")) }));
    }
    if (name === "partial_roots") {
      const inventoryExists = fs.existsSync(path.join(f.data, "collector.config.json"));
      console.log(JSON.stringify({ scenario: name, joinStatus: result.status, joinCode: joined.code,
        inventoryExists, cloudRequests: remote.uploads.length + remote.joins.length }));
      check("partial_registration_refuses_before_token_or_first_config_write", joined.code !== 0 &&
        result.status === "join_preflight_failed" && !inventoryExists &&
        remote.uploads.length === 0 && remote.joins.length === 0);
      return;
    }
    if (name === "program_edit") {
      const trace = fs.readFileSync(f.trace, "utf8");
      check("owner_program_edit_refused_before_token_and_unload", joined.code !== 0 &&
        result.status === "join_preflight_failed" &&
        String(result.message).includes("ProgramArguments") &&
        remote.joins.length === 0 && !trace.includes("bootout"));
      return;
    }
    if (name === "edited_manifest") {
      const plist = path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist");
      const kept = fs.readFileSync(plist, "utf8").includes("/opt/owner-custom-bin");
      const trace = fs.readFileSync(f.trace, "utf8");
      console.log(JSON.stringify({ scenario: name, joinStatus: result.status, joinCode: joined.code,
        reason: result.message, ownerPathPreserved: kept,
        cloudRequests: remote.uploads.length + remote.joins.length }));
      check("join_refuses_owner_edited_manifest_before_unload_or_token", joined.code !== 0 &&
        result.status === "join_preflight_failed" && kept &&
        result.message?.includes("EnvironmentVariables.PATH") &&
        !trace.includes("bootout") && remote.uploads.length === 0 && remote.joins.length === 0 &&
        fs.existsSync(f.state) && await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)));
      const explicit = await command(f.env, ["join", "--token-stdin", "--url",
        `http://127.0.0.1:${remote.port}`, "--replace-launch-agent"], `${token}\n`, f.installedCli);
      const explicitResult = receipt(explicit.stdout);
      console.log(JSON.stringify({ scenario: "edited_manifest_explicit_replace", code: explicit.code,
        status: explicitResult.status, reason: explicitResult.reason ?? null,
        trace: fs.readFileSync(f.trace, "utf8"), cloudJoins: remote.joins.length }));
      check("join_replaces_owner_manifest_only_with_explicit_flag", explicit.code === 0 &&
        explicitResult.status === "joined" && remote.joins.length === 1 &&
        !fs.readFileSync(plist, "utf8").includes("/opt/owner-custom-bin") &&
        fs.existsSync(f.state) && await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)));
      check("changed_manifest_requires_second_unload_before_new_bootstrap",
        fs.readFileSync(f.trace, "utf8").split("\n").filter((line) => line.startsWith("bootout ")).length === 2);
      return;
    }
    if (name === "clock_skew") console.log(JSON.stringify({ scenario: name, joinCode: joined.code,
      joinStatus: result.status, reason: result.reason ?? null }));
    const config = collectorConfigSchema.parse(JSON.parse(fs.readFileSync(path.join(f.data, "collector.config.json"), "utf8")));
    if (name === "foreign_rollout" || name === "foreign_rollout_beyond_limit") {
      const enrolled = config.captureRoots?.some((entry) => entry.directory === foreign) ?? false;
      console.log(JSON.stringify({ scenario: name, joinExit: joined.code,
        joinStatus: result.status, foreignFolderRegistered: enrolled,
        rootCount: config.captureRoots?.length ?? 0 }));
      check("foreign_tool_rollout_is_not_automatically_registered", !enrolled);
      return;
    }
    if (name.startsWith("exhausted_")) {
      const rootCount = config.captureRoots?.length ?? 0;
      const explicit = name === "exhausted_explicit";
      console.log(JSON.stringify({ scenario: name, joinCode: joined.code, joinStatus: result.status,
        rootCount, preview: joined.stdout.split("\n").find((line) => line.startsWith("Found, not verified")) ?? null }));
      check(`${name}_search_exhaustion_requires_explicit_root`, joined.code === 0 &&
        result.status === "joined" && rootCount === (explicit ? 2 : 1) &&
        (explicit || joined.stdout.includes("Found, not verified; add with --add-root") &&
          joined.stdout.includes("search exhausted")) &&
        fs.existsSync(f.state) && await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)));
      return;
    }
    if (name === "symlink_private") {
      const enrolled = config.captureRoots?.some((entry) => entry.directory === privateFolder) ?? false;
      console.log(JSON.stringify({ scenario: name, joinCode: joined.code, joinStatus: result.status,
        privateFolderRegistered: enrolled }));
      check("join_never_registers_symlinked_private_folder", !enrolled);
      return;
    }
    if (name === "crash_after_bootout") {
      console.log(JSON.stringify({ scenario: name, joinStatus: result.status, joinCode: joined.code,
        collectorStateFileExists: fs.existsSync(f.state), reason: result.reason,
        stderr: joined.stderr.slice(-800), trace: fs.existsSync(f.trace) ? fs.readFileSync(f.trace, "utf8") : null }));
      check("crashed_add_restores_running_collector", fs.existsSync(f.state) &&
        await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT)));
      return;
    }
    const expectedRoots = name === "mixed_roots" ? 3 : 2;
    if (name === "path_drift") check("path_drift_recorded_without_join_refusal",
      joined.code === 0 && result.launchAgent?.runtimeDriftKeys?.includes("EnvironmentVariables.PATH"));
    const rootsTogether = config.captureRoots?.length === expectedRoots &&
      config.captureRoots.some((entry) => entry.directory === studio && entry.source === "codex") &&
      config.captureRoots.some((entry) => entry.directory === codex && entry.source === "codex") &&
      (name !== "mixed_roots" || config.captureRoots.some((entry) =>
        entry.directory === path.join(f.home, ".claude", "projects") && entry.source === "claude_code")) &&
      !config.captureRoots.some((entry) => entry.directory === unrelated);
    if (!rootsTogether) throw new Error(`${name}_registers_native_folders_together: ${JSON.stringify({
      roots: config.captureRoots, joinCode: joined.code, joinStatus: result.status,
      reason: result.reason, stderr: joined.stderr.slice(-1200), stdout: joined.stdout.slice(-1200),
      cloudJoins: remote.joins.length, cloudUploads: remote.uploads.length,
      trace: fs.existsSync(f.trace) ? fs.readFileSync(f.trace, "utf8") : null,
    })}`);
    check(`${name}_registers_native_folders_together`, true);
    if (name === "mixed_roots") {
      const receipts = fs.readdirSync(path.join(f.data, "receipts"))
        .filter((file) => file.startsWith("capture-roots-add-") && file.endsWith(".json"));
      const batch = receipts.length === 1 ? JSON.parse(fs.readFileSync(
        path.join(f.data, "receipts", receipts[0]!), "utf8")) as { addedRoots?: unknown[] } : null;
      check("mixed_sources_commit_in_one_root_add_receipt", receipts.length === 1 &&
        batch?.addedRoots?.length === 3);
    }
    if (name === "fleet_label") check("join_reuses_persisted_nonhostname_fleet_label",
      config.enrollmentMachineLabel === "fleet-label-not-hostname" &&
      config.captureRoots?.some((entry) => entry.rootId ===
        deriveCaptureRootIdentity("fleet-label-not-hostname", "codex", studio).rootId));
    check(`${name}_announces_folders_before_capture`, joined.stdout.indexOf(`Will record ${expectedRoots} agent folders`) >= 0 &&
      joined.stdout.indexOf(`Will record ${expectedRoots} agent folders`) < joined.stdout.indexOf('"status":'));
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
    if ((name === "running_0744_layout" || name === "crash_manifest_link_running") && process.env.PLIMSOLL_PROOF_0744_CLI && mode === "ack") {
      console.log(JSON.stringify({ scenario: "running_actual_0744", trace }));
      check("running_0744_daemon_is_replaced_by_head_daemon",
        trace[0]?.includes(fs.realpathSync(process.env.PLIMSOLL_PROOF_0744_CLI)) &&
        trace.at(-1)?.includes(cli) && trace.filter((line) => line.startsWith("bootout")).length === 2);
    }
    if (mode !== "no_ack") {
      const ledger = new Database(path.join(f.data, "work-ledger.sqlite"), { readonly: true, fileMustExist: true });
      const totals = dashboardSummary(ledger).totals;
      const localEvents = totals.events;
      const setupRows = (ledger.prepare("select count(*) as n from buffered_events where id like 'join-setup-%'")
        .get() as { n: number }).n;
      ledger.close();
      const uploadedIds = remote.uploads.flatMap((body) =>
        (JSON.parse(body) as { events?: Array<{ event: { id: string } }> }).events?.map((row) => row.event.id) ?? []);
      check(`${name}_contact_adds_no_local_event_or_usage`, localEvents === 0 &&
        totals.inputTokens === 0 && totals.outputTokens === 0 && totals.costUsd === 0 && setupRows === 0);
      check(`${name}_contact_reuses_acknowledged_cloud_event_id`, uploadedIds.length >= 2 &&
        new Set(uploadedIds).size === 1 && remote.uniqueEvents.size === 1);
      if (name === "replay_timeout_then_ack") {
        console.log(JSON.stringify({ scenario: name, uploadCount: uploadedIds.length,
          uniqueEventIds: new Set(uploadedIds).size, localEvents, setupRows, totals }));
        check("timeout_retry_reuses_single_event_id", uploadedIds.length >= 3 &&
          new Set(uploadedIds).size === 1 && localEvents === 0 && setupRows === 0);
      }
      const finished = joined.code === 0 && result.status === "joined" &&
        result.daemon?.running === true && result.daemon?.readinessVerified === true &&
        result.daemon?.syncArmed === true && result.firstContactKind === "handshake_replay" &&
        typeof result.firstContactAt === "string" && remote.uploads.length >= 2 &&
        remote.uniqueEvents.size === 1 && localEvents === 0 && setupRows === 0 &&
        joined.stdout.includes("First contact confirmed");
      if (!finished) throw new Error(`${name}_single_command_finishes_ready_and_acknowledged: ` +
        JSON.stringify({ code: joined.code, status: result.status, reason: result.reason,
          daemon: result.daemon, firstContactAt: result.firstContactAt,
          uploadCount: remote.uploads.length, uniqueEvents: remote.uniqueEvents.size,
          localEvents, setupRows,
          plainResultSeen: joined.stdout.includes("First contact confirmed") }));
      check(`${name}_single_command_finishes_ready_and_acknowledged`, true);
      if (name === "busy_fence_serving_rejoin") {
        const held = new LifecycleMutationAuthority(path.join(f.data, "lifecycle-authority"))
          .acquire({ leaseMs: 30_000 });
        check("busy_fence_fixture_lease_acquired", held.kind === "acquired");
        if (held.kind !== "acquired") throw new Error("fixture lease was unavailable");
        try {
          const second = await command(f.env, ["join", "--token-stdin", "--url",
            `http://127.0.0.1:${remote.port}`], `${token}\n`, f.installedCli);
          const secondResult = receipt(second.stdout);
          const runningAfter = fs.existsSync(f.state) &&
            await waitForCollector(Number(f.env.PLIMSOLL_PROOF_JOIN_PORT));
          console.log(JSON.stringify({ scenario: name, secondExit: second.code,
            secondStatus: secondResult.status, reason: secondResult.reason ?? null,
            runningAfter }));
          check("serving_collector_satisfies_busy_lifecycle_fence",
            second.code === 0 && secondResult.status === "joined" && runningAfter);
        } finally { held.lease.release(); }
      }
      if (name === "fresh" && process.env.PLIMSOLL_PROOF_0744_CLI) {
        const old = await command(f.env, ["status", "--json"], "", process.env.PLIMSOLL_PROOF_0744_CLI);
        check("released_0744_cli_opens_head_joined_ledger", old.code === 0 &&
          old.stdout.includes('"syncConfigured": true'));
      }
    } else {
      check(`${name}_missing_ack_is_nonzero_but_collector_keeps_running`, joined.code !== 0 &&
        result.status === "joined_setup_incomplete" && /No first contact acknowledgement/.test(result.reason) &&
        result.daemon?.running === true && joined.stderr.includes("Collector remains running") &&
        fs.existsSync(f.state) && fs.existsSync(path.join(f.data, "collector.config.json")));
    }
  } finally {
    await stopFixture(f.env, f.state);
    await remote.close();
  }
}

async function joinOnlyScenario(name: string, option: "--no-daemon" | "ci" | "positional") {
  const remote = await cloud("ack");
  const f = fixture(name, await collectorPort(remote.port));
  try {
    if (option === "ci") f.env.CI = "true";
    const joined = option === "positional"
      ? await command(f.env, ["join", `http://127.0.0.1:${remote.port}#${token}`, "--no-daemon"],
        "", f.installedCli)
      : await command(f.env, ["join", "--token-stdin", "--url", `http://127.0.0.1:${remote.port}`,
        ...(option === "--no-daemon" ? ["--no-daemon"] : [])], `${token}\n`, f.installedCli);
    const result = receipt(joined.stdout);
    check(`${name}_join_only_keeps_daemon_absent`, joined.code === 0 && result.status === "joined" &&
      result.daemon?.setup === "skipped" && !fs.existsSync(f.state) &&
      !fs.existsSync(path.join(f.home, "Library/LaunchAgents/com.plimsoll.collector.plist")) &&
      remote.uploads.length === 1);
    if (option === "positional") check("positional_join_warns_to_use_token_prompt",
      joined.stderr.includes("Use --token-prompt") &&
      !joined.stderr.includes(token) && !joined.stdout.includes(token));
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
  const help = await command(process.env, ["--help"]);
  check("join_help_omits_positional_token_form", help.code === 0 &&
    help.stdout.includes("join --token-prompt --url") && !help.stdout.includes("#<token>"));
  try {
    if (process.env.PR428_REVIEW_SCENARIO === "fresh_only") {
      await joinedScenario("fresh", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "running_only") {
      await joinedScenario("running_0744_layout", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "stopped_0744_layout") {
      await joinedScenario("stopped_0744_layout", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "loaded_stopped_0744_layout") {
      await joinedScenario("loaded_stopped_0744_layout", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "mid_restart_0744_layout") {
      await joinedScenario("mid_restart_0744_layout", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "legacy_path_drift_0744") {
      await joinedScenario("legacy_path_drift_0744", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "busy_fence_serving_rejoin") {
      await joinedScenario("busy_fence_serving_rejoin", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "foreign_rollout") {
      await joinedScenario("foreign_rollout", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "foreign_rollout_beyond_limit") {
      await joinedScenario("foreign_rollout_beyond_limit", false, "ack");
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
    if (process.env.PR428_REVIEW_SCENARIO === "owner_edit_during_join") {
      await joinedScenario("owner_edit_during_join", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "owner_edit_restore_conflict") {
      await joinedScenario("owner_edit_restore_conflict", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "owner_edit_unreadable") {
      await joinedScenario("owner_edit_unreadable", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "owner_edit_after_recheck") {
      await joinedScenario("owner_edit_after_recheck", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "two_join_race") {
      await joinedScenario("two_join_race", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "owner_edit_first_unload") {
      await joinedScenario("owner_edit_first_unload", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "fleet_label") {
      await joinedScenario("fleet_label", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "mixed_roots") {
      await joinedScenario("mixed_roots", false, "ack");
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
    if (process.env.PR428_REVIEW_SCENARIO === "crash_parent_after_bootout") {
      await joinedScenario("crash_parent_after_bootout", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "crash_after_config_commit") {
      await joinedScenario("crash_after_config_commit", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "crash_fresh_after_config_commit") {
      await joinedScenario("crash_fresh_after_config_commit", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "crash_after_bootstrap") {
      await joinedScenario("crash_after_bootstrap", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "crash_manifest_link") {
      await joinedScenario("crash_manifest_link", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "crash_manifest_link_running") {
      await joinedScenario("crash_manifest_link_running", true, "ack");
      return;
    }
    if (["crash_obligation_open", "crash_obligation_fsync", "crash_obligation_rename",
      "crash_obligation_link",
      "crash_obligation_directory_fsync"].includes(process.env.PR428_REVIEW_SCENARIO ?? "")) {
      await joinedScenario(process.env.PR428_REVIEW_SCENARIO!, false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "corrupt_obligation_loaded") {
      await joinedScenario("corrupt_obligation_loaded", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "delayed_restart") {
      await joinedScenario("delayed_restart", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "path_drift") {
      await joinedScenario("path_drift", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "program_edit") {
      await joinedScenario("program_edit", true, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "replay_timeout_then_ack") {
      await joinedScenario("replay_timeout_then_ack", false, "timeout_once");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "replay_timeout_default") {
      await joinedScenario("replay_timeout_default", false, "timeout_once");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "exhausted_preview") {
      await joinedScenario("exhausted_preview", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "exhausted_explicit") {
      await joinedScenario("exhausted_explicit", false, "ack");
      return;
    }
    if (process.env.PR428_REVIEW_SCENARIO === "positional") {
      await joinOnlyScenario("positional_compatibility", "positional");
      return;
    }
    await joinedScenario("fresh", false, "ack");
    await joinedScenario("running_0744_layout", true, "ack");
    await joinedScenario("mixed_roots", false, "ack");
    await joinedScenario("fleet_label", true, "ack");
    await joinedScenario("symlink_private", false, "ack");
    await joinedScenario("partial_roots", false, "ack");
    await joinedScenario("crash_after_bootout", true, "ack");
    await joinedScenario("crash_parent_after_bootout", true, "ack");
    await joinedScenario("crash_after_config_commit", true, "ack");
    await joinedScenario("crash_fresh_after_config_commit", false, "ack");
    await joinedScenario("delayed_restart", true, "ack");
    await joinedScenario("path_drift", true, "ack");
    await joinedScenario("program_edit", true, "ack");
    await joinedScenario("replay_timeout_default", false, "timeout_once");
    await joinedScenario("replay_timeout_then_ack", false, "timeout_once");
    await joinedScenario("exhausted_preview", false, "ack");
    await joinedScenario("exhausted_explicit", false, "ack");
    await joinedScenario("edited_manifest", true, "ack");
    await joinedScenario("clock_skew", false, "no_ack");
    await joinedScenario("missing_ack", false, "no_ack");
    await joinOnlyScenario("explicit_no_daemon", "--no-daemon");
    await joinOnlyScenario("positional_compatibility", "positional");
    await joinOnlyScenario("ci_home", "ci");
    await refusedScenario("refused_token", false);
    await refusedScenario("network_failure", true);
    console.log(JSON.stringify({ proof: "join-setup-e2e", status: "passed", checks }));
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
