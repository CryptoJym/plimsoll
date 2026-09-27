import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { releaseStopWindowListener, withStopWindowRelease } from "../packages/collector-cli/src/stop-window-listener";

// A managed update starts a stop-window listener on the collector port. Only a
// completed update hands the port to the new runtime's load-launch-agent, which
// releases it. Any other outcome restores the old runtime, whose loader does not
// know the listener, so the update itself must release it (eco-6hoxj.163.96).
let checks = 0;
function check(condition: unknown, message: string) {
  assert.ok(condition, message);
  checks += 1;
}

async function outcome(started: boolean, run: () => Promise<{ receipt?: { status?: unknown } }>, failRelease = false) {
  let releases = 0;
  const release = async () => {
    releases += 1;
    if (failRelease) throw new Error("stop_window_release_port_still_bound");
    return true;
  };
  try {
    const { result, releaseError } = await withStopWindowRelease(started, release, run);
    return { releases, result, releaseError, thrown: null as Error | null };
  } catch (error) {
    return { releases, result: null, releaseError: null, thrown: error as Error };
  }
}

async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port === 48271 ? freePort() : port;
}
function probe(port: number): Promise<string | null> {
  return new Promise((resolve) => {
    const request = http.get({ hostname: "127.0.0.1", port, path: "/healthz", timeout: 500 }, (response) => {
      let body = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => { body += chunk; });
      response.on("end", () => {
        try { resolve(String((JSON.parse(body) as { mode?: unknown }).mode ?? "daemon")); } catch { resolve("unparsed"); }
      });
    });
    request.on("timeout", () => request.destroy());
    request.on("error", () => resolve(null));
  });
}

async function main() {
  const failure = new Error("update_failed_before_receipt");
  const thrown = await outcome(true, async () => { throw failure; });
  check(thrown.thrown === failure && thrown.releases === 1, "a thrown update releases the listener once and rethrows the same error");
  const thrownReleaseFails = await outcome(true, async () => { throw failure; }, true);
  check(thrownReleaseFails.thrown === failure && thrownReleaseFails.releases === 1, "a failed release never masks the update's own error");
  for (const status of ["rolled_back", "rollback_required", "refused"]) {
    const run = await outcome(true, async () => ({ receipt: { status } }));
    check(run.thrown === null && run.releases === 1 && run.result?.receipt?.status === status && run.releaseError === null,
      `a ${status} update releases the listener and still returns its receipt`);
  }
  const completed = await outcome(true, async () => ({ receipt: { status: "completed" } }));
  check(completed.releases === 0 && completed.result?.receipt?.status === "completed",
    "a completed update keeps the listener for the new runtime's load-launch-agent");
  const unmanaged = await outcome(false, async () => ({ receipt: { status: "rolled_back" } }));
  check(unmanaged.releases === 0, "an update that started no listener releases nothing");
  const unmanagedThrow = await outcome(false, async () => { throw failure; });
  check(unmanagedThrow.thrown === failure && unmanagedThrow.releases === 0, "an unmanaged thrown update releases nothing");
  const releaseFails = await outcome(true, async () => ({ receipt: { status: "rolled_back" } }), true);
  check(releaseFails.result?.receipt?.status === "rolled_back" && releaseFails.releaseError?.message === "stop_window_release_port_still_bound",
    "a release failure after a rolled-back update is reported with the receipt, not swallowed");

  // End to end: a real managed update in a disposable home starts the real
  // listener, then fails (its artifact does not exist). The port must be free
  // when the command exits, so a restored runtime's daemon can bind it.
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "plimsoll-stop-window-release-"));
  const home = path.join(root, "home");
  const collectorHome = path.join(home, ".plimsoll");
  const stubBin = path.join(root, "bin");
  for (const directory of [home, collectorHome, path.join(home, ".codex"), path.join(home, ".claude"), path.join(home, "tmp"), stubBin]) {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  }
  fs.writeFileSync(path.join(stubBin, "launchctl"), "#!/bin/sh\nexit 127\n", { mode: 0o700 });
  const port = await freePort();
  fs.writeFileSync(path.join(collectorHome, "collector.config.json"), JSON.stringify({
    port, installKey: "stop-window-release-proof", managed: true,
    managedConfig: { reconcile: { enabled: false, intervalSeconds: 600 } },
  }), { mode: 0o600 });
  loadOrCreateLocalIngestAuth(collectorHome);
  const env = {
    ...process.env, HOME: home, USERPROFILE: home, PLIMSOLL_HOME: collectorHome,
    CODEX_HOME: path.join(home, ".codex"), CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
    XDG_CONFIG_HOME: path.join(home, ".config"), XDG_CACHE_HOME: path.join(home, ".cache"),
    XDG_STATE_HOME: path.join(home, ".local", "state"), TMPDIR: path.join(home, "tmp"),
    PATH: `${stubBin}:${process.env.PATH ?? "/usr/bin:/bin"}`,
  };
  const cli = path.resolve("packages/collector-cli/src/cli.ts");
  let observedDuringUpdate: string | null = null;
  let portAfterExit: string | null = "unchecked";
  try {
    check((await probe(port)) === null, "the disposable port is free before the update");
    const child = spawn(process.execPath, [...process.execArgv, cli, "lifecycle", "update",
      "--operation-id", "stop-window-release-proof", "--artifact", path.join(root, "missing-bundle")],
    { env, stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => { stderr += chunk; });
    child.stdout.resume();
    const exited = new Promise<number | null>((resolve) => child.on("exit", (code) => resolve(code)));
    let done = false;
    void exited.then(() => { done = true; });
    // Best-effort observation of the listener while the command runs; the
    // assertion below is what matters.
    while (!done) {
      const mode = await probe(port);
      if (mode === "stop_window") observedDuringUpdate = mode;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    const code = await exited;
    check(code !== 0 && /--artifact must be/.test(stderr), "the update fails on its missing artifact");
    portAfterExit = await probe(port);
    check(portAfterExit === null, `the port is free after the failed update exits (found ${portAfterExit ?? "nothing"})`);
  } finally {
    // Never leave a listener behind, whatever the result.
    if ((await probe(port)) !== null) {
      await releaseStopWindowListener(port, collectorHome).catch(() => false);
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
  console.log(JSON.stringify({ proof: "stop-window-release", checks, passed: checks, failed: 0, observedDuringUpdate, portAfterExit }));
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
