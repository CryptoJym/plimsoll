import { spawn, spawnSync, type ChildProcess } from "node:child_process";
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
    const pidPath = path.join(concurrentHome, "collector.pid");
    const pidRecord = JSON.parse(fs.readFileSync(pidPath, "utf8")) as { pid: number };
    check(pidRecord.pid === active.pid, "Concurrent follower replaced the owner PID file.");
    const followerExit = await waitForExit(follower.child);
    check(followerExit.code === 0, "already_running must be a successful exit.");
    const response = await fetch("http://127.0.0.1:" + concurrentPort + "/status");
    check(response.ok, "Exactly one collector listener was not healthy.");
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
    const stalePidPath = path.join(staleHome, "collector.pid");
    fs.writeFileSync(
      stalePidPath,
      JSON.stringify(
        {
          command: ["stale-collector"],
          cwd: root,
          label: LAUNCH_AGENT_LABEL,
          pid: sentinel.pid,
          startedAt: "2026-01-01T00:00:00.000Z",
          version: 1,
        },
        null,
        2,
      ) + "\n",
      { mode: 0o600 },
    );

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
    const recoveredPid = JSON.parse(fs.readFileSync(stalePidPath, "utf8")) as { pid: number };
    check(recoveredPid.pid === recoveredReceipt?.pid, "Recovered owner PID was not recorded.");
    check(recoveredPid.pid !== sentinel.pid, "Stale unrelated PID remained authoritative.");
    await stopOwner(recovered);

    const foreignHome = path.join(tempRoot, "foreign-listener");
    const foreignPort = await availablePort();
    writeConfig(foreignHome, foreignPort);
    foreignServer = http.createServer((_request, response) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ ok: true, service: "not-plimsoll" }));
    });
    await new Promise<void>((resolve, reject) => {
      foreignServer?.once("error", reject);
      foreignServer?.listen(foreignPort, "127.0.0.1", resolve);
    });
    fs.writeFileSync(
      path.join(foreignHome, "collector.pid"),
      JSON.stringify(
        {
          command: ["stale-collector"],
          cwd: root,
          label: LAUNCH_AGENT_LABEL,
          pid: process.pid,
          startedAt: "2026-01-01T00:00:00.000Z",
          version: 1,
        },
        null,
        2,
      ) + "\n",
      { mode: 0o600 },
    );
    const occupied = startCollector(cliPath, foreignHome);
    children.push(occupied);
    const occupiedExit = await waitForExit(occupied.child);
    check(occupiedExit.code === 1, "Foreign listener should cause a failed bind.");
    check(
      !statusReceipt(occupied, "already_running"),
      "Foreign /status payload was misreported as a running Plimsoll collector.",
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
            "foreign status payload is not accepted as collector ownership",
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
    foreignServer?.closeAllConnections();
    foreignServer?.close();
    fs.rmSync(tempRoot, { force: true, recursive: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
