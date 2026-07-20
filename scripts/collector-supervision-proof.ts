import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  LAUNCH_AGENT_LABEL,
  renderLaunchAgentPlist,
} from "../packages/collector-cli/src/launch-agent";
import {
  readCollectorPidCleanupState,
  readProcessStartFingerprint,
  removeCollectorPidFileIfOwnedDetailed,
  runtimeIdentityMatches,
  START_LOCK_LEASE_MS,
  type CollectorPidRecord,
  type CollectorRuntimeIdentity,
} from "../packages/collector-cli/src/runtime-ownership";

type Receipt = Record<string, unknown>;

type WatchedChild = {
  child: ChildProcess;
  errors: string[];
  exit: { code: number | null; signal: NodeJS.Signals | null } | null;
  output: string;
  receipts: Receipt[];
};

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function watch(child: ChildProcess): WatchedChild {
  const watched: WatchedChild = {
    child,
    errors: [],
    exit: null,
    output: "",
    receipts: [],
  };
  let stdoutRemainder = "";
  let jsonBuffer = "";
  let stderr = "";
  const consumeLine = (line: string) => {
    const trimmed = line.trim();
    if (!jsonBuffer && !trimmed.startsWith("{")) return;
    jsonBuffer = jsonBuffer ? jsonBuffer + "\n" + line : line;
    try {
      watched.receipts.push(JSON.parse(jsonBuffer) as Receipt);
      jsonBuffer = "";
    } catch {
      // Pretty-printed JSON is complete only after its closing brace.
    }
  };
  child.stdout?.setEncoding("utf8");
  child.stderr?.setEncoding("utf8");
  child.stdout?.on("data", (chunk: string) => {
    watched.output += chunk;
    stdoutRemainder += chunk;
    const lines = stdoutRemainder.split("\n");
    stdoutRemainder = lines.pop() ?? "";
    for (const line of lines) consumeLine(line);
  });
  child.stderr?.on("data", (chunk: string) => {
    stderr += chunk;
    watched.errors.push(chunk);
  });
  child.on("error", (error) => {
    watched.errors.push("spawn_error: " + error.message);
  });
  child.on("exit", (code, signal) => {
    watched.exit = { code, signal };
    if (stdoutRemainder) consumeLine(stdoutRemainder);
    if (stderr.trim() && watched.errors.length === 0) watched.errors.push(stderr);
  });
  return watched;
}

function waitFor(
  predicate: () => boolean,
  message: string,
  timeoutMs = 15_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    const poll = () => {
      if (predicate()) {
        resolve();
        return;
      }
      if (Date.now() > deadline) {
        reject(new Error(message));
        return;
      }
      setTimeout(poll, 25);
    };
    poll();
  });
}

function waitForExit(child: ChildProcess, timeoutMs = 10_000) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve({ code: child.exitCode, signal: child.signalCode });
  }
  return new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Child did not exit in time.")), timeoutMs);
    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal });
    });
  });
}

async function availablePort() {
  const server = net.createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  check(address && typeof address === "object", "Could not reserve a temporary port.");
  const port = address.port;
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
  return port;
}

function writeConfig(home: string, port: number) {
  const config = collectorConfigSchema.parse({ port });
  fs.mkdirSync(home, { recursive: true, mode: 0o700 });
  fs.writeFileSync(
    path.join(home, "collector.config.json"),
    JSON.stringify(config, null, 2) + "\n",
    { mode: 0o600 },
  );
}

function runtimeIdentity(pid: number, instanceId = randomUUID()): CollectorRuntimeIdentity {
  const processStartFingerprint = readProcessStartFingerprint(pid);
  check(processStartFingerprint, "Could not fingerprint proof process " + pid + ".");
  return { instanceId, pid, processStartFingerprint };
}

function writePidRecord(
  home: string,
  identity: CollectorRuntimeIdentity,
  root: string,
) {
  const record: CollectorPidRecord = {
    ...identity,
    command: ["proof-owner"],
    cwd: root,
    label: LAUNCH_AGENT_LABEL,
    startedAt: new Date().toISOString(),
    version: 2,
  };
  fs.writeFileSync(
    path.join(home, "collector.pid"),
    JSON.stringify(record, null, 2) + "\n",
    { mode: 0o600 },
  );
  return record;
}

function writeStartLock(
  home: string,
  identity: CollectorRuntimeIdentity,
  createdAt: string,
) {
  fs.writeFileSync(
    path.join(home, "collector.pid.start.lock"),
    JSON.stringify(
      {
        ...identity,
        createdAt,
        label: LAUNCH_AGENT_LABEL,
        version: 2,
      },
      null,
      2,
    ) + "\n",
    { mode: 0o600 },
  );
}

async function startOneShotIdentityOwner(port: number) {
  const script = [
    'const http = require("node:http");',
    'process.on("message", ({ port, runtimeIdentity }) => {',
    '  let served = false;',
    '  const server = http.createServer((_request, response) => {',
    '    response.writeHead(200, { "content-type": "application/json" });',
    '    response.end(JSON.stringify({',
    '      ok: true, dataMode: "metadata", retentionDays: 90, stats: {}, health: {}, runtimeIdentity',
    '    }));',
    '    if (!served) {',
    '      served = true;',
    '      setTimeout(() => server.close(() => process.exit(0)), 10);',
    '    }',
    '  });',
    '  server.listen(port, "127.0.0.1", () => process.send({ ready: true }));',
    '});',
  ].join("\n");
  const watched = watch(
    spawn(process.execPath, ["-e", script], {
      stdio: ["ignore", "pipe", "pipe", "ipc"],
    }),
  );
  check(watched.child.pid, "Could not spawn the one-shot owner.");
  const identity = runtimeIdentity(watched.child.pid);
  const ready = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("One-shot owner did not bind.")), 5_000);
    watched.child.on("message", (message) => {
      if ((message as { ready?: unknown }).ready === true) {
        clearTimeout(timer);
        resolve();
      }
    });
  });
  watched.child.send?.({ port, runtimeIdentity: identity });
  await ready;
  return { identity, watched };
}

function startCollector(cliPath: string, home: string) {
  return collectorCommand(cliPath, home, path.dirname(cliPath), "start");
}

function collectorCommand(
  cliPath: string,
  home: string,
  cwd: string,
  ...args: string[]
) {
  return watch(
    spawn(process.execPath, [cliPath, ...args], {
      cwd,
      env: { ...process.env, PLIMSOLL_HOME: home },
      stdio: ["ignore", "pipe", "pipe"],
    }),
  );
}

function stopCollector(cliPath: string, home: string, cwd: string) {
  return collectorCommand(cliPath, home, cwd, "stop");
}

function launchAgentCommand(
  cliPath: string,
  home: string,
  cwd: string,
  command: "unload-launch-agent" | "uninstall-launch-agent",
  fakeBin: string,
  launchctlExit = 0,
  bootoutPid?: number,
  behavior: {
    initiallyLoaded?: boolean;
    printExit?: number;
    printStderr?: string;
  } = {},
) {
  const launchctlState = path.join(home, "launchctl.state");
  if (behavior.initiallyLoaded !== false) {
    fs.writeFileSync(launchctlState, String(bootoutPid ?? process.pid) + "\n", { mode: 0o600 });
  }
  return watch(
    spawn(process.execPath, [cliPath, command], {
      cwd,
      env: {
        ...process.env,
        PATH: `${fakeBin}${path.delimiter}${process.env.PATH ?? ""}`,
        PLIMSOLL_HOME: home,
        PLIMSOLL_PROOF_LAUNCHCTL_EXIT: String(launchctlExit),
        PLIMSOLL_PROOF_LAUNCHCTL_STATE: launchctlState,
        PLIMSOLL_PROOF_LAUNCHCTL_PRINT_EXIT: String(behavior.printExit ?? 0),
        PLIMSOLL_PROOF_LAUNCHCTL_PRINT_STDERR: behavior.printStderr ?? "",
        PLIMSOLL_PROOF_LAUNCHCTL_NOT_FOUND:
          `Could not find service "${LAUNCH_AGENT_LABEL}" in domain for user gui: ${process.getuid?.() ?? "unknown"}`,
        ...(bootoutPid ? { PLIMSOLL_PROOF_BOOTOUT_PID: String(bootoutPid) } : {}),
      },
      stdio: ["ignore", "pipe", "pipe"],
    }),
  );
}

function statusReceipt(watched: WatchedChild, status: string) {
  return watched.receipts.find((receipt) => receipt.status === status);
}

async function stopOwner(watched: WatchedChild) {
  if (watched.child.exitCode === null && watched.child.signalCode === null) {
    watched.child.kill("SIGTERM");
  }
  await waitForExit(watched.child);
}

async function main() {
  const root = path.resolve(import.meta.dirname, "..");
  const cliPath = path.join(root, "packages", "collector-cli", "dist", "cli.mjs");
  check(fs.existsSync(cliPath), "Build the packaged CLI before running this proof.");

  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-supervision-"));
  const children: WatchedChild[] = [];
  const auxiliaryChildren: ChildProcess[] = [];
  let sentinel: ChildProcess | null = null;
  let foreignServer: http.Server | null = null;

  try {
    const concurrentHome = path.join(tempRoot, "concurrent");
    const concurrentPort = await availablePort();
    writeConfig(concurrentHome, concurrentPort);
    const first = startCollector(cliPath, concurrentHome);
    const second = startCollector(cliPath, concurrentHome);
    children.push(first, second);

    await waitFor(
      () =>
        [first, second].some((child) => Boolean(statusReceipt(child, "active"))) &&
        [first, second].some((child) => Boolean(statusReceipt(child, "already_running"))),
      "Concurrent starts did not converge. first=" +
        JSON.stringify(first.receipts) +
        " output=" +
        first.output +
        " second=" +
        JSON.stringify(second.receipts) +
        " output=" +
        second.output +
        " errors=" +
        JSON.stringify([...first.errors, ...second.errors]) +
        " exits=" +
        JSON.stringify([first.exit, second.exit]) +
        " pids=" +
        JSON.stringify([first.child.pid, second.child.pid]),
    );

    const owner = statusReceipt(first, "active") ? first : second;
    const follower = owner === first ? second : first;
    const active = statusReceipt(owner, "active");
    const alreadyRunning = statusReceipt(follower, "already_running");
    check(active, "No active receipt was emitted.");
    check(alreadyRunning, "No already_running receipt was emitted.");
    check(active.pid === alreadyRunning.pid, "Follower did not identify the owner PID.");
    check(
      runtimeIdentityMatches(
        active.runtimeIdentity as CollectorRuntimeIdentity,
        alreadyRunning.runtimeIdentity as CollectorRuntimeIdentity,
      ),
      "Concurrent follower did not confirm the exact owner runtime identity.",
    );
    const pidPath = path.join(concurrentHome, "collector.pid");
    const pidRecord = JSON.parse(fs.readFileSync(pidPath, "utf8")) as CollectorPidRecord;
    check(pidRecord.pid === active.pid, "Concurrent follower replaced the owner PID file.");
    check(
      runtimeIdentityMatches(
        pidRecord,
        active.runtimeIdentity as CollectorRuntimeIdentity,
      ),
      "PID record and active status do not share one runtime identity.",
    );
    const followerExit = await waitForExit(follower.child);
    check(followerExit.code === 0, "already_running must be a successful exit.");
    const response = await fetch("http://127.0.0.1:" + concurrentPort + "/status");
    check(response.ok, "Exactly one collector listener was not healthy.");
    const status = (await response.json()) as {
      runtimeIdentity?: CollectorRuntimeIdentity;
    };
    check(
      runtimeIdentityMatches(status.runtimeIdentity, pidRecord),
      "/status did not return the exact PID-record runtime identity.",
    );
    const stopper = stopCollector(cliPath, concurrentHome, root);
    children.push(stopper);
    await waitFor(
      () => Boolean(stopper.receipts.find((receipt) => receipt.stopped === true)),
      "Packaged stop did not validate and signal its exact recorded CLI process.",
    );
    const stopperExit = await waitForExit(stopper.child);
    check(stopperExit.code === 0, "Packaged stop command did not exit successfully.");
    await waitForExit(owner.child);
    check(!fs.existsSync(pidPath), "Owner PID file survived graceful shutdown.");

    const staleHome = path.join(tempRoot, "stale");
    const stalePort = await availablePort();
    writeConfig(staleHome, stalePort);
    sentinel = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
      stdio: "ignore",
    });
    check(sentinel.pid, "Could not create unrelated sentinel process.");
    auxiliaryChildren.push(sentinel);
    const stalePidPath = path.join(staleHome, "collector.pid");
    const staleIdentity = runtimeIdentity(sentinel.pid);
    writePidRecord(staleHome, staleIdentity, root);

    const recovered = startCollector(cliPath, staleHome);
    children.push(recovered);
    await waitFor(
      () => Boolean(statusReceipt(recovered, "active")),
      "Stale PID did not recover: " +
        JSON.stringify(recovered.receipts) +
        " errors=" +
        JSON.stringify(recovered.errors),
      20_000,
    );
    process.kill(sentinel.pid, 0);
    const recoveredReceipt = statusReceipt(recovered, "active");
    const recoveredPid = JSON.parse(fs.readFileSync(stalePidPath, "utf8")) as CollectorPidRecord;
    check(recoveredPid.pid === recoveredReceipt?.pid, "Recovered owner PID was not recorded.");
    check(recoveredPid.pid !== sentinel.pid, "Stale unrelated PID remained authoritative.");
    await stopOwner(recovered);

    const ownerDeathHome = path.join(tempRoot, "owner-death");
    const ownerDeathPort = await availablePort();
    writeConfig(ownerDeathHome, ownerDeathPort);
    const transientOwner = await startOneShotIdentityOwner(ownerDeathPort);
    children.push(transientOwner.watched);
    writePidRecord(ownerDeathHome, transientOwner.identity, root);
    const replacement = startCollector(cliPath, ownerDeathHome);
    children.push(replacement);
    await waitFor(
      () => Boolean(statusReceipt(replacement, "active")),
      "Candidate did not recover after the owner died following its first valid status: " +
        JSON.stringify(replacement.receipts) +
        " errors=" +
        JSON.stringify(replacement.errors),
      20_000,
    );
    check(
      !statusReceipt(replacement, "already_running"),
      "Candidate returned already_running after the confirmed owner died.",
    );
    await waitForExit(transientOwner.watched.child);
    await stopOwner(replacement);

    const foreignHome = path.join(tempRoot, "foreign-listener");
    const foreignPort = await availablePort();
    writeConfig(foreignHome, foreignPort);
    const foreignPidIdentity = runtimeIdentity(process.pid);
    const foreignStatusIdentity = {
      ...foreignPidIdentity,
      instanceId: randomUUID(),
    };
    foreignServer = http.createServer((_request, response) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          ok: true,
          dataMode: "metadata",
          retentionDays: 90,
          stats: {},
          health: {},
          runtimeIdentity: foreignStatusIdentity,
        }),
      );
    });
    await new Promise<void>((resolve, reject) => {
      foreignServer?.once("error", reject);
      foreignServer?.listen(foreignPort, "127.0.0.1", resolve);
    });
    writePidRecord(foreignHome, foreignPidIdentity, root);
    const occupied = startCollector(cliPath, foreignHome);
    children.push(occupied);
    const occupiedExit = await waitForExit(occupied.child);
    check(occupiedExit.code === 1, "Foreign listener should cause a failed bind.");
    check(
      !statusReceipt(occupied, "already_running"),
      "Foreign exact-shape /status payload was misreported as the PID-record owner.",
    );
    check(
      occupied.errors.join("").includes('"code": "port_in_use"'),
      "Foreign listener did not produce the explicit port_in_use receipt.",
    );
    process.kill(process.pid, 0);
    await new Promise<void>((resolve, reject) => {
      foreignServer?.close((error) => (error ? reject(error) : resolve()));
    });
    foreignServer = null;

    const inertHome = path.join(tempRoot, "inert-argv");
    const inertPort = await availablePort();
    writeConfig(inertHome, inertPort);
    const inert = spawn(
      process.execPath,
      ["-e", "setInterval(() => {}, 1000)", cliPath, "start"],
      { stdio: "ignore" },
    );
    check(inert.pid, "Could not create inert CLI-shaped process.");
    auxiliaryChildren.push(inert);
    const inertIdentity = runtimeIdentity(inert.pid);
    writePidRecord(inertHome, inertIdentity, root);
    const deniedStop = stopCollector(cliPath, inertHome, root);
    children.push(deniedStop);
    await waitFor(
      () => deniedStop.receipts.some((receipt) => receipt.stopped === false),
      "Stop did not return a blocked receipt for the inert CLI-shaped process.",
    );
    check(
      deniedStop.receipts.some(
        (receipt) => receipt.reason === "runtime_identity_unverified",
      ),
      "Inert CLI-shaped process was not blocked by exact runtime identity.",
    );
    const deniedStopExit = await waitForExit(deniedStop.child);
    check(deniedStopExit.code !== 0, "Unverified live runtime stop exited successfully.");
    process.kill(inert.pid, 0);

    fs.writeFileSync(path.join(inertHome, "collector.pid"), String(inert.pid) + "\n", {
      mode: 0o600,
    });
    const legacyStop = stopCollector(cliPath, inertHome, root);
    children.push(legacyStop);
    await waitFor(
      () =>
        legacyStop.receipts.some(
          (receipt) => receipt.reason === "legacy_pid_file_blocked",
        ),
      "Legacy PID record was not safely blocked.",
    );
    const legacyStopExit = await waitForExit(legacyStop.child);
    check(legacyStopExit.code !== 0, "Legacy PID stop exited successfully.");
    process.kill(inert.pid, 0);

    const fakeBin = path.join(tempRoot, "fake-bin");
    fs.mkdirSync(fakeBin, { recursive: true });
    fs.writeFileSync(
      path.join(fakeBin, "launchctl"),
      [
        "#!/bin/sh",
        'state="${PLIMSOLL_PROOF_LAUNCHCTL_STATE:?}"',
        'if [ "$1" = "print" ]; then',
        '  code="${PLIMSOLL_PROOF_LAUNCHCTL_PRINT_EXIT:-0}"',
        '  if [ "$code" != "0" ]; then',
        '    printf "%s\\n" "$PLIMSOLL_PROOF_LAUNCHCTL_PRINT_STDERR" >&2',
        '    exit "$code"',
        '  fi',
        '  if [ ! -f "$state" ]; then',
        '    printf "%s\\n" "$PLIMSOLL_PROOF_LAUNCHCTL_NOT_FOUND" >&2',
        '    exit 113',
        '  fi',
        "  printf '    pid = %s\\n' \"$(cat \"$state\")\"",
        "  exit 0",
        "fi",
        'if [ "$1" = "bootout" ]; then',
        '  code="${PLIMSOLL_PROOF_LAUNCHCTL_EXIT:-0}"',
        '  [ "$code" = "0" ] || exit "$code"',
        '  if [ -n "${PLIMSOLL_PROOF_BOOTOUT_PID:-}" ]; then',
        '    kill -TERM "$PLIMSOLL_PROOF_BOOTOUT_PID"',
        "  fi",
        '  rm -f "$state"',
        "  exit 0",
        "fi",
        "exit 64",
        "",
      ].join("\n"),
      { mode: 0o755 },
    );
    const unloadHome = path.join(tempRoot, "truthful-unload");
    const unloadPort = await availablePort();
    writeConfig(unloadHome, unloadPort);
    const unloadOwner = startCollector(cliPath, unloadHome);
    children.push(unloadOwner);
    await waitFor(
      () => Boolean(statusReceipt(unloadOwner, "active")),
      "Unload fixture owner did not become active.",
    );
    check(unloadOwner.child.pid, "Unload fixture owner has no PID.");
    const truthfulUnload = launchAgentCommand(
      cliPath,
      unloadHome,
      root,
      "unload-launch-agent",
      fakeBin,
      0,
      unloadOwner.child.pid,
    );
    children.push(truthfulUnload);
    await waitFor(
      () => truthfulUnload.receipts.some(
        (receipt) =>
          receipt.unloaded === true &&
          receipt.status === "stopped" &&
          (receipt.terminal as Receipt | undefined)?.labelState === "not_reported" &&
          (receipt.terminal as Receipt | undefined)?.listenerState === "absent" &&
          (receipt.terminal as Receipt | undefined)?.pidRecordState === "missing",
      ),
      "Unload did not report the aggregate terminal state: " + truthfulUnload.output,
      7_000,
    );
    const truthfulUnloadExit = await waitForExit(truthfulUnload.child);
    check(truthfulUnloadExit.code === 0, "Truthful aggregate unload exited nonzero.");
    await waitForExit(unloadOwner.child);
    const truthfulReceipt = truthfulUnload.receipts.find((receipt) => receipt.unloaded === true);
    check(
      truthfulReceipt &&
        !JSON.stringify(truthfulReceipt).includes(tempRoot) &&
        !JSON.stringify(truthfulReceipt).includes(root),
      "Unload receipt exposed a filesystem path.",
    );

    const durableHome = path.join(tempRoot, "durable-cleanup-reopen");
    const durablePort = await availablePort();
    writeConfig(durableHome, durablePort);
    const durableOwner = startCollector(cliPath, durableHome);
    children.push(durableOwner);
    await waitFor(
      () => Boolean(statusReceipt(durableOwner, "active")),
      "Durable cleanup fixture owner did not become active.",
    );
    const durableActive = statusReceipt(durableOwner, "active");
    const durableIdentity = durableActive?.runtimeIdentity as
      | CollectorRuntimeIdentity
      | undefined;
    check(durableIdentity, "Durable cleanup fixture emitted no runtime identity.");
    const durablePidPath = path.join(durableHome, "collector.pid");
    const detailedAmbiguity = removeCollectorPidFileIfOwnedDetailed(
      durablePidPath,
      durableIdentity,
      LAUNCH_AGENT_LABEL,
      {
        beforeClaim: () => {
          const stat = fs.statSync(durablePidPath);
          fs.utimesSync(
            durablePidPath,
            new Date(stat.atimeMs),
            new Date(stat.mtimeMs + 10_000),
          );
        },
      },
    );
    const durableBeforeSignal = readCollectorPidCleanupState(
      durablePidPath,
      LAUNCH_AGENT_LABEL,
    );
    check(
      detailedAmbiguity.disposition === "preclaim_changed" &&
        detailedAmbiguity.ambiguous &&
        !detailedAmbiguity.removed &&
        durableBeforeSignal.markerState === "present" &&
        fs.existsSync(durablePidPath),
      "Production detailed cleanup did not durably mark its preclaim ambiguity.",
    );
    durableOwner.child.kill("SIGTERM");
    await waitFor(
      () => Boolean(statusReceipt(durableOwner, "shutdown_incomplete")),
      "SIGTERM finalizer did not consume the durable ambiguity.",
      7_000,
    );
    const durableShutdown = statusReceipt(durableOwner, "shutdown_incomplete");
    const durableOwnerExit = await waitForExit(durableOwner.child);
    const durableAfterExit = readCollectorPidCleanupState(
      durablePidPath,
      LAUNCH_AGENT_LABEL,
    );
    check(
      durableOwnerExit.code !== 0 &&
        durableShutdown?.pidCleaned === false &&
        durableShutdown?.pidRecordState === "current" &&
        durableShutdown?.processState === "exiting" &&
        (durableShutdown?.cleanupAttempt as Receipt | undefined)?.disposition ===
          "persistent_ambiguity" &&
        durableAfterExit.markerState === "present" &&
        fs.existsSync(durablePidPath),
      "SIGTERM promoted a durable cleanup ambiguity to stopped or PID-cleaned truth.",
    );

    const durableStop = stopCollector(cliPath, durableHome, root);
    children.push(durableStop);
    await waitFor(
      () => durableStop.receipts.some(
        (receipt) => receipt.reason === "pid_cleanup_ambiguous",
      ),
      "A separate stop process did not reopen the durable cleanup marker.",
    );
    const durableStopExit = await waitForExit(durableStop.child);
    const durableStopReceipt = durableStop.receipts.find(
      (receipt) => receipt.reason === "pid_cleanup_ambiguous",
    );
    check(
      durableStopExit.code !== 0 &&
        durableStopReceipt?.stopped === false &&
        durableStopReceipt.pidCleaned === false &&
        durableStopReceipt.pidRecordState === "current" &&
        (durableStopReceipt.pidCleanup as Receipt | undefined)?.markerState === "present" &&
        !JSON.stringify(durableStopReceipt).includes(tempRoot),
      "Separate stop lost marker truth, emitted a false terminal receipt, or exposed a path.",
    );

    const durableUnload = launchAgentCommand(
      cliPath,
      durableHome,
      root,
      "unload-launch-agent",
      fakeBin,
      0,
      undefined,
      { initiallyLoaded: false },
    );
    children.push(durableUnload);
    await waitFor(
      () => durableUnload.receipts.some(
        (receipt) => receipt.unloaded === false && receipt.reason === "indeterminate",
      ),
      "A later unload observer did not reopen the durable cleanup ambiguity.",
      7_000,
    );
    const durableUnloadExit = await waitForExit(durableUnload.child);
    const durableUnloadReceipt = durableUnload.receipts.find(
      (receipt) => receipt.unloaded === false,
    );
    check(
      durableUnloadExit.code !== 0 &&
        durableUnloadReceipt?.pidCleaned === false &&
        durableUnloadReceipt.bootoutAttempted === false &&
        (durableUnloadReceipt.terminal as Receipt | undefined)?.pidCleanupMarkerState ===
          "present" &&
        (durableUnloadReceipt.timing as Receipt | undefined)?.finalObservationPerformed === true &&
        !JSON.stringify(durableUnloadReceipt).includes(tempRoot),
      "Post-exit unload lost durable marker truth, skipped final observation, or exposed a path.",
    );

    const blockedStart = startCollector(cliPath, durableHome);
    children.push(blockedStart);
    const blockedStartExit = await waitForExit(blockedStart.child);
    const blockedStartRaw = blockedStart.errors.join("").trim();
    const blockedStartReceipt = JSON.parse(blockedStartRaw) as Receipt;
    check(
      blockedStartExit.code !== 0 &&
        blockedStartReceipt.status === "error" &&
        blockedStartReceipt.code === "pid_cleanup_ambiguous" &&
        typeof blockedStartReceipt.pidPathHash === "string" &&
        blockedStartReceipt.port === durablePort &&
        !blockedStartRaw.includes(tempRoot) &&
        !blockedStartRaw.includes(root) &&
        !blockedStartRaw.includes(" at "),
      "Packaged start ownership failure was not stable, path-free JSON with a nonzero exit.",
    );

    const blockedDoctor = collectorCommand(
      cliPath,
      durableHome,
      root,
      "doctor",
      "--read-only",
      "--json",
    );
    children.push(blockedDoctor);
    const blockedDoctorExit = await waitForExit(blockedDoctor.child);
    const blockedDoctorRuntime = blockedDoctor.receipts[0]?.runtime as Receipt | undefined;
    const blockedDoctorReconciliation = blockedDoctorRuntime
      ?.pidCleanupReconciliation as Receipt | undefined;
    check(
      blockedDoctorExit.code !== 0 &&
        blockedDoctorReconciliation?.disposition === "actor_live" &&
        blockedDoctorReconciliation.reconciled === false &&
        fs.existsSync(path.join(durableHome, ".collector.pid.plimsoll-cleanup-marker")),
      "Read-only doctor did not report the live cleanup actor without mutating its marker.",
    );

    const reconciledHome = path.join(tempRoot, "dead-actor-reconciliation");
    const reconciledPort = await availablePort();
    writeConfig(reconciledHome, reconciledPort);
    const deadActorProcess = spawn(
      process.execPath,
      ["-e", "setInterval(() => {}, 1000)"],
      { stdio: "ignore" },
    );
    check(deadActorProcess.pid, "Could not create cleanup actor fixture.");
    auxiliaryChildren.push(deadActorProcess);
    const deadActorIdentity = runtimeIdentity(deadActorProcess.pid);
    deadActorProcess.kill("SIGTERM");
    await waitForExit(deadActorProcess);
    for (let index = 0; index < 1_200; index += 1) {
      fs.writeFileSync(path.join(reconciledHome, `.unrelated-${index}`), "unrelated\n", {
        mode: 0o600,
      });
    }
    const reconciledMarkerPath = path.join(
      reconciledHome,
      ".collector.pid.plimsoll-cleanup-marker",
    );
    fs.writeFileSync(
      reconciledMarkerPath,
      JSON.stringify({
        actor: {
          pid: deadActorIdentity.pid,
          processStartFingerprint: deadActorIdentity.processStartFingerprint,
        },
        label: LAUNCH_AGENT_LABEL,
        schema: "plimsoll.collector-pid-cleanup.v2",
        state: "in_progress",
        target: durableIdentity,
        transactionId: randomUUID(),
      }) + "\n",
      { mode: 0o600 },
    );

    const eligibleDoctor = collectorCommand(
      cliPath,
      reconciledHome,
      root,
      "doctor",
      "--read-only",
      "--json",
    );
    children.push(eligibleDoctor);
    const eligibleDoctorExit = await waitForExit(eligibleDoctor.child);
    const eligibleDoctorRuntime = eligibleDoctor.receipts[0]?.runtime as Receipt | undefined;
    const eligibleDoctorReconciliation = eligibleDoctorRuntime
      ?.pidCleanupReconciliation as Receipt | undefined;
    check(
      eligibleDoctorExit.code !== 0 &&
        eligibleDoctorReconciliation?.disposition === "eligible_dead_actor" &&
        eligibleDoctorReconciliation.eligible === true &&
        eligibleDoctorReconciliation.reconciled === false &&
        fs.existsSync(reconciledMarkerPath),
      "Read-only doctor mutated or misreported an eligible dead-actor marker.",
    );

    const reconciledStop = stopCollector(cliPath, reconciledHome, root);
    children.push(reconciledStop);
    const reconciledStopExit = await waitForExit(reconciledStop.child);
    const reconciledStopReceipt = reconciledStop.receipts.find(
      (receipt) => receipt.status === "already_stopped",
    );
    check(
      reconciledStopExit.code === 0 &&
        reconciledStopReceipt?.stopped === true &&
        reconciledStopReceipt.reason === null &&
        reconciledStopReceipt.pidRecordState === "missing" &&
        reconciledStopReceipt.listenerState === "absent" &&
        (reconciledStopReceipt.pidCleanupReconciliation as Receipt | undefined)
          ?.disposition === "marker_cleared" &&
        !fs.existsSync(reconciledMarkerPath),
      "Packaged stop did not reconcile the dead actor and prove already-stopped truth.",
    );

    const cleanMissingStop = stopCollector(cliPath, reconciledHome, root);
    children.push(cleanMissingStop);
    const cleanMissingStopExit = await waitForExit(cleanMissingStop.child);
    const cleanMissingStopReceipt = cleanMissingStop.receipts.find(
      (receipt) => receipt.status === "already_stopped",
    );
    check(
      cleanMissingStopExit.code === 0 &&
        cleanMissingStopReceipt?.stopped === true &&
        cleanMissingStopReceipt.reason === null &&
        (cleanMissingStopReceipt.pidCleanupReconciliation as Receipt | undefined)
          ?.disposition === "clean",
      "Clean missing-PID stop did not return consistent already-stopped JSON and exit zero.",
    );

    foreignServer = http.createServer((_request, response) => {
      response.writeHead(404, { "content-type": "text/plain" });
      response.end("not a collector\n");
    });
    await new Promise<void>((resolve, reject) => {
      foreignServer?.once("error", reject);
      foreignServer?.listen(reconciledPort, "127.0.0.1", resolve);
    });
    const missingPidOccupiedStop = stopCollector(cliPath, reconciledHome, root);
    children.push(missingPidOccupiedStop);
    const missingPidOccupiedExit = await waitForExit(missingPidOccupiedStop.child);
    const missingPidOccupiedReceipt = missingPidOccupiedStop.receipts.find(
      (receipt) => receipt.stopped === false,
    );
    check(
      missingPidOccupiedExit.code !== 0 &&
        missingPidOccupiedReceipt?.status === "refused" &&
        missingPidOccupiedReceipt.reason === "listener_still_present" &&
        missingPidOccupiedReceipt.pidRecordState === "missing" &&
        missingPidOccupiedReceipt.listenerState === "unrelated",
      "Missing PID plus present listener was promoted to success or exited zero.",
    );
    await new Promise<void>((resolve, reject) => {
      foreignServer?.close((error) => (error ? reject(error) : resolve()));
    });
    foreignServer = null;

    const collisionHome = path.join(tempRoot, "durable-cleanup-collision");
    writeConfig(collisionHome, await availablePort());
    const collisionPidPath = path.join(collisionHome, "collector.pid");
    const collisionOwnerRecord = writePidRecord(collisionHome, durableIdentity, root);
    const collisionCleanup = removeCollectorPidFileIfOwnedDetailed(
      collisionPidPath,
      durableIdentity,
      LAUNCH_AGENT_LABEL,
      {
        afterClaim: () => {
          fs.writeFileSync(collisionPidPath, "operator-collision\n", { mode: 0o600 });
        },
      },
    );
    const fixedQuarantinePath = path.join(
      collisionHome,
      ".collector.pid.plimsoll-quarantine",
    );
    const collisionPersistent = readCollectorPidCleanupState(
      collisionPidPath,
      LAUNCH_AGENT_LABEL,
    );
    check(
      collisionCleanup.disposition === "destination_reappeared" &&
        collisionCleanup.quarantined &&
        collisionPersistent.markerState === "present" &&
        collisionPersistent.quarantineCount === 1 &&
        fs.readFileSync(collisionPidPath, "utf8") === "operator-collision\n" &&
        fs.readFileSync(fixedQuarantinePath, "utf8") ===
          JSON.stringify(collisionOwnerRecord, null, 2) + "\n",
      "The marker-plus-collision fixture did not retain its exact quarantine inventory.",
    );
    const collisionStop = stopCollector(cliPath, collisionHome, root);
    children.push(collisionStop);
    await waitFor(
      () => collisionStop.receipts.some(
        (receipt) => receipt.reason === "pid_cleanup_ambiguous",
      ),
      "Stop did not fail closed on marker plus visible collision.",
    );
    const collisionStopExit = await waitForExit(collisionStop.child);
    check(collisionStopExit.code !== 0, "Marker-plus-collision stop exited successfully.");
    fs.unlinkSync(collisionPidPath);
    const missingQuarantineUnload = launchAgentCommand(
      cliPath,
      collisionHome,
      root,
      "unload-launch-agent",
      fakeBin,
      0,
      undefined,
      { initiallyLoaded: false },
    );
    children.push(missingQuarantineUnload);
    await waitFor(
      () => missingQuarantineUnload.receipts.some(
        (receipt) => receipt.unloaded === false && receipt.reason === "indeterminate",
      ),
      "Missing-visible-PID plus quarantine was promoted to already stopped.",
      7_000,
    );
    const missingQuarantineExit = await waitForExit(missingQuarantineUnload.child);
    const missingQuarantineReceipt = missingQuarantineUnload.receipts.find(
      (receipt) => receipt.unloaded === false,
    );
    check(
      missingQuarantineExit.code !== 0 &&
        missingQuarantineReceipt?.pidCleaned === false &&
        (missingQuarantineReceipt.terminal as Receipt | undefined)?.pidRecordState === "missing" &&
        (missingQuarantineReceipt.terminal as Receipt | undefined)
          ?.pidCleanupQuarantineCount === 1 &&
        (missingQuarantineReceipt.terminal as Receipt | undefined)?.pidCleanupQuarantined ===
          true &&
        readCollectorPidCleanupState(collisionPidPath, LAUNCH_AGENT_LABEL).quarantineCount === 1,
      "Quarantine ambiguity did not survive visible PID removal and a later process reopen.",
    );

    const alreadyStoppedHome = path.join(tempRoot, "known-label-not-found");
    writeConfig(alreadyStoppedHome, await availablePort());
    const alreadyStopped = launchAgentCommand(
      cliPath,
      alreadyStoppedHome,
      root,
      "unload-launch-agent",
      fakeBin,
      0,
      undefined,
      { initiallyLoaded: false },
    );
    children.push(alreadyStopped);
    await waitFor(
      () => alreadyStopped.receipts.some(
        (receipt) =>
          receipt.unloaded === true &&
          receipt.status === "already_stopped" &&
          receipt.bootoutAttempted === false &&
          (receipt.prior as Receipt | undefined)?.labelState === "not_reported",
      ),
      "The exact launchctl label-not-found result was not recognized.",
    );
    const alreadyStoppedExit = await waitForExit(alreadyStopped.child);
    check(alreadyStoppedExit.code === 0, "Known label-not-found should be idempotent success.");

    const exactNotFound =
      `Could not find service "${LAUNCH_AGENT_LABEL}" in domain for user gui: ${process.getuid?.() ?? "unknown"}`;
    const unexpectedPrintCases = [
      { name: "permission-denied-77", code: 77, stderr: "permission denied" },
      { name: "wrong-output-113", code: 113, stderr: "permission denied" },
      { name: "wrong-code-known-output", code: 1, stderr: exactNotFound },
    ];
    for (const fixture of unexpectedPrintCases) {
      const fixtureHome = path.join(tempRoot, `launchctl-print-${fixture.name}`);
      writeConfig(fixtureHome, await availablePort());
      const command = launchAgentCommand(
        cliPath,
        fixtureHome,
        root,
        "unload-launch-agent",
        fakeBin,
        0,
        undefined,
        {
          initiallyLoaded: false,
          printExit: fixture.code,
          printStderr: fixture.stderr,
        },
      );
      children.push(command);
      await waitFor(
        () => command.receipts.some((receipt) => receipt.unloaded === false),
        `Unexpected launchctl print fixture ${fixture.name} produced no refusal receipt.`,
      );
      const exit = await waitForExit(command.child);
      const receipt = command.receipts.find((candidate) => candidate.unloaded === false);
      check(
        exit.code !== 0 &&
          receipt?.status === "refused" &&
          receipt.reason === "indeterminate" &&
          receipt.bootoutAttempted === false &&
          (receipt.prior as Receipt | undefined)?.labelState === "query_failed" &&
          (receipt.terminal as Receipt | undefined)?.labelState === "query_failed",
        `Unexpected launchctl print fixture ${fixture.name} was promoted to absence.`,
      );
    }

    const legacyUnloadHome = path.join(tempRoot, "legacy-unload");
    writeConfig(legacyUnloadHome, await availablePort());
    const legacyPidPath = path.join(legacyUnloadHome, "collector.pid");
    fs.writeFileSync(legacyPidPath, String(inert.pid) + "\n", { mode: 0o600 });
    const legacyUnload = launchAgentCommand(
      cliPath,
      legacyUnloadHome,
      root,
      "unload-launch-agent",
      fakeBin,
    );
    children.push(legacyUnload);
    await waitFor(
      () => legacyUnload.receipts.some(
        (receipt) =>
          receipt.unloaded === false && receipt.reason === "stale_owned_record",
      ),
      "Unload promoted a legacy PID residue to successful cleanup.",
      7_000,
    );
    const legacyUnloadExit = await waitForExit(legacyUnload.child);
    check(legacyUnloadExit.code !== 0, "Legacy PID unload did not fail closed.");
    check(fs.existsSync(legacyPidPath), "Legacy PID residue was deleted without ownership proof.");

    const failedBootoutHome = path.join(tempRoot, "failed-bootout");
    writeConfig(failedBootoutHome, await availablePort());
    const failedBootout = launchAgentCommand(
      cliPath,
      failedBootoutHome,
      root,
      "unload-launch-agent",
      fakeBin,
      7,
    );
    children.push(failedBootout);
    await waitFor(
      () => failedBootout.receipts.some(
        (receipt) => receipt.unloaded === false && receipt.reason === "launchctl_failed",
      ),
      "Unload promoted a failed launchctl bootout to success.",
    );
    const failedBootoutExit = await waitForExit(failedBootout.child);
    check(failedBootoutExit.code !== 0, "Failed launchctl bootout exited successfully.");

    const reusedPidHome = path.join(tempRoot, "reused-pid-lock");
    const reusedPidPort = await availablePort();
    writeConfig(reusedPidHome, reusedPidPort);
    writeStartLock(
      reusedPidHome,
      {
        ...inertIdentity,
        instanceId: randomUUID(),
        processStartFingerprint: "sha256:" + "0".repeat(64),
      },
      new Date().toISOString(),
    );
    const reusedPidRecovery = startCollector(cliPath, reusedPidHome);
    children.push(reusedPidRecovery);
    await waitFor(
      () => Boolean(statusReceipt(reusedPidRecovery, "active")),
      "A live reused PID with the wrong fingerprint pinned the stale start lock.",
    );
    process.kill(inert.pid, 0);
    await stopOwner(reusedPidRecovery);

    const expiredLeaseHome = path.join(tempRoot, "expired-lock-lease");
    const expiredLeasePort = await availablePort();
    writeConfig(expiredLeaseHome, expiredLeasePort);
    writeStartLock(
      expiredLeaseHome,
      { ...inertIdentity, instanceId: randomUUID() },
      new Date(Date.now() - START_LOCK_LEASE_MS - 1_000).toISOString(),
    );
    const expiredLeaseRecovery = startCollector(cliPath, expiredLeaseHome);
    children.push(expiredLeaseRecovery);
    await waitFor(
      () => Boolean(statusReceipt(expiredLeaseRecovery, "active")),
      "An expired start-lock lease remained authoritative.",
    );
    process.kill(inert.pid, 0);
    await stopOwner(expiredLeaseRecovery);

    const packagedPlist = renderLaunchAgentPlist({
      homeDir: tempRoot,
      programArguments: [process.execPath, cliPath, "start"],
      repoRoot: path.dirname(cliPath),
      workingDirectory: path.dirname(cliPath),
    });
    check(
      packagedPlist.includes("<string>" + process.execPath + "</string>") &&
        packagedPlist.includes("<string>" + cliPath + "</string>"),
      "Packaged plist does not pin Node plus the stable CLI path.",
    );
    check(!packagedPlist.includes("<string>pnpm</string>"), "Packaged plist invokes pnpm.");
    check(!packagedPlist.includes("tsx"), "Packaged plist invokes tsx.");
    check(
      packagedPlist.includes("<key>SuccessfulExit</key>\n    <false/>"),
      "LaunchAgent does not stop retrying successful already_running exits.",
    );
    check(
      packagedPlist.includes("<key>ThrottleInterval</key>\n  <integer>30</integer>"),
      "LaunchAgent restart throttle is not pinned to 30 seconds.",
    );
    if (process.platform === "darwin") {
      const lint = spawnSync("/usr/bin/plutil", ["-lint", "-"], {
        encoding: "utf8",
        input: packagedPlist,
      });
      check(lint.status === 0, "Packaged plist is not valid XML: " + lint.stderr);
    }

    const devPlist = renderLaunchAgentPlist({
      homeDir: tempRoot,
      pnpmPath: "/opt/homebrew/bin/pnpm",
      repoRoot: root,
    });
    check(devPlist.includes("<string>--dir</string>"), "Explicit dev plist lost --dir.");
    check(devPlist.includes("<string>" + root + "</string>"), "Explicit dev plist lost repo root.");
    check(devPlist.includes("<string>collector</string>"), "Explicit dev plist lost collector script.");

    console.log(
      JSON.stringify(
        {
          status: "passed",
          checks: [
            "concurrent starts converge without PID replacement",
            "packaged stop validates the recorded CLI path across working directories",
            "stale PID recovers without signaling unrelated process",
            "owner death after first valid status cannot yield already_running",
            "foreign exact-shape status cannot spoof collector ownership",
            "inert CLI-shaped and legacy processes cannot pass stop authorization",
            "unload records launchctl failure and requires aggregate terminal-state proof",
            "only exact launchctl label-not-found output becomes not_reported",
            "unexpected launchctl exit codes and output remain query_failed",
            "unload receipts are path-free and legacy PID residue is retained",
            "durable cleanup ambiguity survives SIGTERM, stop, and unload process reopen",
            "packaged start failures are stable path-free JSON with nonzero exit",
            "doctor reports live and eligible cleanup actors without mutation",
            "dead actor plus 1200 unrelated files reconciles to already-stopped truth",
            "missing PID stop exit status matches absent versus present listener truth",
            "marker-plus-collision and missing-PID quarantine remain nonterminal",
            "reused PID fingerprint and expired lock lease both recover",
            "crash-only restart is throttled",
            "rendered plist passes plutil on macOS",
            "packaged and explicit development arguments are separated",
          ],
        },
        null,
        2,
      ),
    );
  } finally {
    for (const watched of children) {
      if (watched.child.exitCode === null && watched.child.signalCode === null) {
        watched.child.kill("SIGTERM");
      }
    }
    if (sentinel && sentinel.exitCode === null && sentinel.signalCode === null) {
      sentinel.kill("SIGTERM");
    }
    for (const child of auxiliaryChildren) {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    }
    foreignServer?.closeAllConnections();
    foreignServer?.close();
    fs.rmSync(tempRoot, { force: true, recursive: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
