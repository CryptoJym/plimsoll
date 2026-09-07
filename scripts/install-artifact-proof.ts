/** Exact local npm artifact qualification. No publisher or service-manager calls. */
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import { buildRuntime, verifyRuntime } from "./build-packaged-runtime";
import { createProofCompletion } from "./lib/proof-completion";
import { isolatedEnvironment, runProof } from "./run-proof";

const completion = createProofCompletion("install-artifact");
const repo = path.resolve(import.meta.dirname, "..");
const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-install-")));
const env = isolatedEnvironment(root);
const evidence = path.join(repo, "evidence/install-artifact");
const sha = (p: string) => createHash("sha256").update(fs.readFileSync(p)).digest("hex");
const receipt: Record<string, unknown> = { schema: "plimsoll.install-artifact-proof.v1", status: "failed",
  coverage: { hosts: 1, platform: `${process.platform}-${process.arch}`, nativeProviderSession: "not_run",
    daemon: "owned disposable child", serviceManager: "not_called", registryPublication: "not_performed" } };
let daemon: ChildProcess | undefined;
function check(name: string, value: unknown) { completion.check(name, Boolean(value)); assert.ok(value, name); }
function command(executable: string, args: string[], cwd: string, extra: NodeJS.ProcessEnv = {}) {
  const result = spawnSync(executable, args, { cwd, env: { ...env, ...extra }, encoding: "utf8",
    timeout: 180_000, maxBuffer: 8 * 1024 * 1024 });
  assert.equal(result.status, 0, `${path.basename(executable)} ${args[0]}: ${(result.stderr ?? "").slice(-2000)}`);
  return result.stdout.trim();
}
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function stop() {
  const child = daemon;
  if (!child) return;
  if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
  const until = Date.now() + 10_000;
  while (child.exitCode === null && child.signalCode === null && Date.now() < until) await delay(50);
  if (child.exitCode === null && child.signalCode === null) { child.kill("SIGKILL"); throw new Error("owned daemon shutdown deadline exceeded"); }
  daemon = undefined;
}
async function freePort() {
  const server = http.createServer();
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>(resolve => server.close(() => resolve()));
  return port;
}
async function main() {
  fs.mkdirSync(evidence, { recursive: true });
  try {
    const manifestPath = path.join(repo, "packages/collector-cli/package.json");
    const dist = path.join(repo, "packages/collector-cli/dist");
    const build = () => buildRuntime({ repoRoot: repo, entry: path.join(repo, "packages/collector-cli/src/cli.ts"),
      dist, packageManifestPath: manifestPath });
    await build();
    const firstManifest = fs.readFileSync(path.join(dist, "runtime-manifest.json"));
    const npm = path.join(path.dirname(process.execPath), "npm");
    const pack = (directory: string) => {
      fs.mkdirSync(directory, { recursive: true });
      const result = JSON.parse(command(npm, ["pack", "--json", "--ignore-scripts", "--pack-destination", directory], path.dirname(manifestPath)));
      assert.equal(result.length, 1);
      const allowed = new Set(["package.json", "README.md", "LICENSE", "dist/cli.mjs", "dist/dashboard.html", "dist/runtime-manifest.json"]);
      assert.ok(result[0].files.every((file: { path: string }) => allowed.has(file.path)), "package contains only declared runtime assets");
      return path.join(directory, result[0].filename);
    };
    const first = pack(path.join(root, "pack1"));
    await build();
    const second = pack(path.join(root, "pack2"));
    check("two_builds_produce_identical_runtime_manifests", firstManifest.equals(fs.readFileSync(path.join(dist, "runtime-manifest.json"))));
    check("two_packs_produce_identical_tarball_bytes", sha(first) === sha(second));
    const manifest = JSON.parse(firstManifest.toString());
    check("source_and_dependency_provenance_present", /^[a-f0-9]{40}$/.test(manifest.source.commit) &&
      /^[a-f0-9]{64}$/.test(manifest.provenance.lockSha256) && /^[a-f0-9]{64}$/.test(manifest.provenance.inputsSha256));

    const install = path.join(root, "install");
    fs.mkdirSync(install, { mode: 0o700 });
    fs.writeFileSync(path.join(install, "package.json"), '{"name":"disposable-plimsoll-qualification","private":true}\n');
    fs.copyFileSync(first, path.join(install, "qualified-package.tgz"));
    command(npm, ["install", "--no-audit", "--no-fund", "--omit=dev", "--save-exact", "./qualified-package.tgz"], install);
    const packageRoot = path.join(install, "node_modules/@plimsoll/cli");
    const artifact = path.join(packageRoot, "dist/cli.mjs");
    const installedManifest = path.join(packageRoot, "dist/runtime-manifest.json");
    check("installed_tarball_matches_built_runtime_digest", sha(artifact) === manifest.artifact.sha256 && verifyRuntime(installedManifest).ok);
    check("npm_executable_points_to_exact_installed_artifact", fs.realpathSync(path.join(install, "node_modules/.bin/plimsoll")) === artifact);
    const requireInstalled = createRequire(path.join(packageRoot, "package.json"));
    const Database = requireInstalled("better-sqlite3") as typeof import("better-sqlite3");
    const nativePackage = requireInstalled("better-sqlite3/package.json") as { version: string };
    const sqlite = new Database(":memory:");
    const sqliteVersion = (sqlite.prepare("select sqlite_version() as version").get() as { version: string }).version;
    sqlite.close();
    check("installed_native_binding_loads_on_current_node_abi", nativePackage.version === "12.10.0" && typeof sqliteVersion === "string");
    const nativePath = path.join(path.dirname(requireInstalled.resolve("better-sqlite3/package.json")), "build/Release/better_sqlite3.node");
    receipt.artifact = { tarballSha256: sha(first), runtimeSha256: sha(artifact), manifestSha256: sha(installedManifest), manifest,
      installLockSha256: sha(path.join(install, "package-lock.json")) };
    receipt.runtime = { node: process.versions.node, abi: process.versions.modules, platform: process.platform, arch: process.arch,
      nodeSha256: sha(process.execPath), sqliteVersion, nativeVersion: nativePackage.version, nativeSha256: sha(nativePath) };
    fs.copyFileSync(first, path.join(evidence, "qualified-package.tgz"));
    fs.copyFileSync(installedManifest, path.join(evidence, "runtime-manifest.json"));
    fs.copyFileSync(path.join(install, "package-lock.json"), path.join(evidence, "install-lock.json"));
    fs.copyFileSync(path.join(install, "package.json"), path.join(evidence, "install-package.json"));

    const port = await freePort();
    const home = env.PLIMSOLL_HOME!;
    const ledger = path.join(home, "work-ledger.sqlite");
    fs.writeFileSync(path.join(home, "collector.config.json"), JSON.stringify({ port, privacyMode: "metadata_only" }), { mode: 0o600 });
    const daemonLog = fs.openSync(path.join(root, "daemon.log"), "w", 0o600);
    const start = async () => {
      daemon = spawn(process.execPath, [artifact, "start"], { cwd: root, env, stdio: ["ignore", daemonLog, daemonLog] });
      const deadline = Date.now() + 20_000;
      while (Date.now() < deadline) {
        if (daemon.exitCode !== null) throw new Error("owned daemon exited before readiness");
        try {
          const response = await fetch(`http://127.0.0.1:${port}/healthz`, { signal: AbortSignal.timeout(500) });
          if (response.status === 200 && (await response.json() as { ok: boolean }).ok) return;
        } catch { /* Only our new loopback listener is polled. */ }
        await delay(100);
      }
      throw new Error("installed daemon readiness deadline exceeded");
    };
    const totals = () => {
      const db = new Database(ledger, { readonly: true });
      try { return db.prepare("select count(*) as rows, coalesce(sum(input_tokens),0) as input, coalesce(sum(output_tokens),0) as output from buffered_events").get() as { rows: number; input: number; output: number }; }
      finally { db.close(); }
    };
    await start();
    const auth = JSON.parse(fs.readFileSync(path.join(home, "local-ingest-auth.json"), "utf8"));
    const started = performance.now();
    const response = await fetch(`http://127.0.0.1:${port}/v1/logs`, { method: "POST", signal: AbortSignal.timeout(5000),
      headers: { "content-type": "application/json", "x-plimsoll-source": "codex", "x-plimsoll-token": auth.codexProducer },
      body: JSON.stringify({ resourceLogs: [{ resource: { attributes: [{ key: "service.name", value: { stringValue: "codex" } }] },
        scopeLogs: [{ logRecords: [{ attributes: [
          { key: "gen_ai.usage.input_tokens", value: { intValue: "3" } },
          { key: "gen_ai.usage.output_tokens", value: { intValue: "2" } },
        ] }] }] }] }) });
    const acceptance = await response.json() as { accepted: boolean; events: number };
    const latencyMs = performance.now() - started;
    check("first_authenticated_synthetic_provider_tokens_are_durable", response.status === 202 && acceptance.accepted && acceptance.events === 1 && totals().input === 3 && totals().output === 2);
    const authDigest = sha(path.join(home, "local-ingest-auth.json"));
    const identityPath = path.join(home, "device.identity.json");
    const stableIdentity = () => {
      const identity = JSON.parse(fs.readFileSync(identityPath, "utf8"));
      return JSON.stringify({ deviceId: identity.deviceId, keyId: identity.keyId, createdAt: identity.createdAt });
    };
    const identityDigest = stableIdentity();
    const samples: Array<{ cpuOneCorePercent: number; treeRssKiB: number; processes: number }> = [];
    for (let index = 0; index < 12; index++) {
      const ps = command("/bin/ps", ["-axo", "pid=,ppid=,%cpu=,rss="], root);
      const rows = ps.split("\n").map(line => line.trim().split(/\s+/).map(Number));
      const pids = new Set([daemon!.pid]);
      for (let changed = true; changed;) { changed = false; for (const [pid, ppid] of rows) if (pids.has(ppid) && !pids.has(pid)) { pids.add(pid); changed = true; } }
      const own = rows.filter(row => pids.has(row[0]));
      samples.push({ cpuOneCorePercent: own.reduce((n, row) => n + row[2]!, 0), treeRssKiB: own.reduce((n, row) => n + row[3]!, 0), processes: own.length });
      await delay(250);
    }
    await stop();
    const firstTotals = totals();
    await start();
    check("restart_preserves_first_tokens_and_private_identity", JSON.stringify(totals()) === JSON.stringify(firstTotals) && sha(path.join(home, "local-ingest-auth.json")) === authDigest &&
      stableIdentity() === identityDigest);
    await stop();

    // A provider rollout appears while the owned daemon is offline. Explicit
    // full-history scan uses the existing tailer and is safely replayable.
    const session = randomUUID();
    const sessionDir = path.join(env.HOME!, ".codex/sessions", ...new Date().toISOString().slice(0, 10).split("-"));
    fs.mkdirSync(sessionDir, { recursive: true });
    const timestamp = new Date().toISOString();
    const line = (type: string, payload: unknown) => JSON.stringify({ timestamp, type, payload });
    fs.writeFileSync(path.join(sessionDir, `rollout-${session}.jsonl`), [
      line("session_meta", { id: session, cwd: root, originator: "qualification" }),
      line("turn_context", { model: "gpt-5.5", cwd: root }),
      line("event_msg", { type: "token_count", info: { total_token_usage: { input_tokens: 0, cached_input_tokens: 0,
        output_tokens: 0, reasoning_output_tokens: 0, total_tokens: 0 } } }),
      line("event_msg", { type: "token_count", info: { total_token_usage: { input_tokens: 11, cached_input_tokens: 0,
        output_tokens: 7, reasoning_output_tokens: 0, total_tokens: 18 } } }),
    ].join("\n") + "\n");
    const offlineScan = JSON.parse(command(process.execPath, [artifact, "scan-rollouts"], root));
    const offlineTotals = totals();
    receipt.offlineDiagnostic = { offlineScan, firstTotals, offlineTotals };
    check("offline_provider_rollout_replays_without_lost_tokens", offlineTotals.input === firstTotals.input + 11 && offlineTotals.output === firstTotals.output + 7);
    command(process.execPath, [artifact, "scan-rollouts"], root);
    check("offline_replay_is_idempotent", JSON.stringify(totals()) === JSON.stringify(offlineTotals));
    await start();
    check("restart_after_offline_replay_conserves_totals", JSON.stringify(totals()) === JSON.stringify(offlineTotals));
    await stop();
    fs.closeSync(daemonLog);
    receipt.capture = { firstToken: "synthetic authenticated Codex OTLP", offlineReplay: "synthetic Codex rollout", firstTotals, offlineTotals, ingestionLatencyMs: latencyMs };
    receipt.overhead = { method: "12 ps process-tree samples after first token; CPU percent of one core; RSS sums shared pages",
      claim: "short smoke observation, not settled pilot budget qualification", samples,
      peakTreeRssMiB: Math.max(...samples.map(s => s.treeRssKiB)) / 1024 };

    process.env.PLIMSOLL_QUALIFICATION_ARTIFACT = artifact;
    const lifecycle = await runProof(path.join(repo, "scripts/lifecycle-operator-proof.ts"), { quiet: true });
    fs.writeFileSync(path.join(evidence, "lifecycle-operator.json"), `${JSON.stringify(lifecycle, null, 2)}\n`);
    receipt.lifecycle = lifecycle;
    check("exact_installed_artifact_passes_lifecycle_update_interrupt_rollback_support_uninstall", lifecycle.status === "passed");
    check("qualified_artifact_bytes_unchanged_after_all_operations", sha(artifact) === manifest.artifact.sha256);
    receipt.status = "passed";
  } finally {
    try { await stop(); } finally {
      fs.writeFileSync(path.join(evidence, "acceptance.json"), `${JSON.stringify(receipt, null, 2)}\n`);
      fs.rmSync(root, { recursive: true, force: true });
      delete process.env.PLIMSOLL_QUALIFICATION_ARTIFACT;
    }
  }
  completion.complete();
}
main().catch(error => { console.error(error); process.exitCode = 1; });
