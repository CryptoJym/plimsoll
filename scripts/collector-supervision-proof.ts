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
  readProcessStartFingerprint,
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
  return watch(
    spawn(process.execPath, [cliPath, "start"], {
      cwd: path.dirname(cliPath),
      env: { ...process.env, PLIMSOLL_HOME: home },
      stdio: ["ignore", "pipe", "pipe"],
    }),
  );
}

function stopCollector(cliPath: string, home: string, cwd: string) {
  return watch(
    spawn(process.execPath, [cliPath, "stop"], {
      cwd,
      env: { ...process.env, PLIMSOLL_HOME: home },
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
    await waitForExit(deniedStop.child);
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
    await waitForExit(legacyStop.child);
    process.kill(inert.pid, 0);

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
