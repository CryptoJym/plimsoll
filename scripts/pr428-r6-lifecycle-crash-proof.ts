/** Six interrupted update paths: retry and snapshot restore at each commit rename. */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { LifecycleInterruption, LifecycleManager, type LifecycleJournal,
  type LifecycleReadiness, type RuntimeArtifact } from "../packages/collector-cli/src/lifecycle";
import { FilesystemLifecycleAdapter, type LifecycleDatabaseAdapter,
  type LifecycleServiceAdapter, type ManagedLifecyclePaths } from
  "../packages/collector-cli/src/lifecycle-filesystem";

type Point = "installation.json" | "state.json" | "current";
type Outcome = "retry" | "restore" | "restore_retry";
const root = fs.mkdtempSync(path.join(process.cwd(), "pr428-r6-lifecycle-crash-"));
const rows: Array<{ point: Point; outcome: Outcome; status: string; version: string }> = [];

function fixture(name: string) {
  const ownershipRoot = path.join(root, name);
  const paths: ManagedLifecyclePaths = {
    ownershipRoot,
    lifecycleRoot: path.join(ownershipRoot, "private", "lifecycle"),
    artifactSourceRoot: path.join(ownershipRoot, "artifacts"),
    collectorConfig: path.join(ownershipRoot, "private", "collector.config.json"),
    database: path.join(ownershipRoot, "private", "work-ledger.sqlite"),
    serviceManifest: path.join(ownershipRoot, "Library", "LaunchAgents", "collector.plist"),
    ownedToolFragments: [], history: [],
    statusSummary: path.join(ownershipRoot, "private", "status-summary.json"),
  };
  fs.mkdirSync(path.dirname(paths.collectorConfig), { recursive: true, mode: 0o700 });
  fs.writeFileSync(paths.collectorConfig, "{}\n", { mode: 0o600 });
  fs.writeFileSync(paths.database, "ledger\n", { mode: 0o600 });
  let running: string | null = null;
  const service: LifecycleServiceAdapter = {
    async activate(input) {
      running = input.version;
      fs.mkdirSync(path.dirname(paths.serviceManifest), { recursive: true, mode: 0o700 });
      fs.writeFileSync(paths.serviceManifest, `version=${input.version}\n`, { mode: 0o600 });
    },
    async restore(input) { running = input.version; },
    async remove() { running = null; },
    async readiness(version) {
      const ready = running === version;
      return { ready, runtimeVersion: running, serviceReady: ready,
        configCompatible: ready, databaseCompatible: ready,
        reason: ready ? "ready" : "runtime_mismatch" } as LifecycleReadiness;
    },
    async supportSnapshot() { return {} as never; },
  };
  const database: LifecycleDatabaseAdapter = {
    async snapshot(input) {
      fs.mkdirSync(path.dirname(input.destination), { recursive: true, mode: 0o700 });
      fs.copyFileSync(input.source, input.destination);
      return true;
    },
    async restore(input) { fs.copyFileSync(input.source, input.destination); },
  };
  const adapter = new FilesystemLifecycleAdapter(paths, service, database);
  const artifact = (version: string): RuntimeArtifact => {
    const sourcePath = path.join(paths.artifactSourceRoot, `${version}.mjs`);
    fs.mkdirSync(path.dirname(sourcePath), { recursive: true, mode: 0o700 });
    fs.writeFileSync(sourcePath, `// ${version}\n`, { mode: 0o700 });
    return { version, platform: "darwin", architecture: "arm64", nodeMajor: 22,
      sourcePath, sha256: `sha256:${createHash("sha256").update(fs.readFileSync(sourcePath)).digest("hex")}` };
  };
  return { paths, adapter, artifact, get running() { return running; } };
}

function assertConsistent(f: ReturnType<typeof fixture>, version: string) {
  const lifecycle = f.paths.lifecycleRoot;
  const state = JSON.parse(fs.readFileSync(path.join(lifecycle, "state.json"), "utf8")) as {
    version: string; executablePath: string; installId: string;
  };
  const installation = JSON.parse(fs.readFileSync(path.join(lifecycle, "installation.json"), "utf8")) as {
    executablePath: string; installId: string;
  };
  assert.equal(state.version, version);
  assert.equal(f.running, version);
  assert.match(state.installId, /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i);
  assert.equal(installation.installId, state.installId);
  assert.equal(installation.executablePath, state.executablePath);
  assert.equal(fs.realpathSync(path.join(lifecycle, "current")),
    path.dirname(path.dirname(state.executablePath)));
  assert.equal(fs.readFileSync(f.paths.serviceManifest, "utf8"), `version=${version}\n`);
  assert.equal(fs.existsSync(path.join(lifecycle, "journal.json")), false);
}

async function trial(point: Point, outcome: Outcome) {
  const f = fixture(`${point}-${outcome}`);
  const initial = f.artifact("0.7.44");
  const target = f.artifact("0.7.45");
  await new LifecycleManager(f.adapter).update({ operationId: "initial", artifact: initial });
  // Model an install made before state.json carried an install id.
  const statePath = path.join(f.paths.lifecycleRoot, "state.json");
  const state = JSON.parse(fs.readFileSync(statePath, "utf8")) as Record<string, unknown>;
  delete state.installId;
  fs.writeFileSync(statePath, `${JSON.stringify(state)}\n`, { mode: 0o600 });
  fs.rmSync(path.join(f.paths.lifecycleRoot, "installation.json"));
  const rename = fs.renameSync;
  let interrupted = false;
  fs.renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
    rename(from, to);
    if (!interrupted && String(to) === path.join(f.paths.lifecycleRoot, point)) {
      interrupted = true;
      throw new LifecycleInterruption(`crash after ${point} rename`);
    }
  }) as typeof fs.renameSync;
  try {
    await assert.rejects(new LifecycleManager(f.adapter).update({
      operationId: "update45", artifact: target,
    }), LifecycleInterruption);
  } finally { fs.renameSync = rename; }
  assert.equal(interrupted, true);
  assert.equal(f.running, "0.7.45");
  const journal = await f.adapter.readJournal();
  assert.equal(journal?.phase, "staged");
  let status: string;
  if (outcome === "retry") {
    const receipt = await new LifecycleManager(f.adapter).update({
      operationId: "update45", artifact: target,
    });
    status = receipt.status;
  } else {
    await f.adapter.writeJournal({ ...journal!, phase: "rollback_required" } as LifecycleJournal);
    const receipt = await new LifecycleManager(f.adapter).update({
      operationId: "update45", artifact: target,
    });
    status = receipt.status;
  }
  const version = outcome === "retry" ? "0.7.45" : "0.7.44";
  assert.equal(status, outcome === "retry" ? "completed" : "rolled_back");
  assertConsistent(f, version);
  rows.push({ point, outcome, status, version });
  console.log(`PASS ${point} ${outcome}: ${status}, running ${version}`);
}

async function restoreCrash(point: Point) {
  const f = fixture(`${point}-restore-retry`);
  const initial = f.artifact("0.7.44");
  const target = f.artifact("0.7.45");
  const manager = new LifecycleManager(f.adapter);
  await manager.update({ operationId: "initial", artifact: initial });
  const statePath = path.join(f.paths.lifecycleRoot, "state.json");
  const state = JSON.parse(fs.readFileSync(statePath, "utf8")) as Record<string, unknown>;
  delete state.installId;
  fs.writeFileSync(statePath, `${JSON.stringify(state)}\n`, { mode: 0o600 });
  fs.rmSync(path.join(f.paths.lifecycleRoot, "installation.json"));

  // Leave the update journal staged, then enter snapshot restoration. This
  // models a process ending after the runtime switch and before its receipt.
  const rename = fs.renameSync;
  fs.renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
    rename(from, to);
    if (String(to) === path.join(f.paths.lifecycleRoot, "current"))
      throw new LifecycleInterruption("crash after update current rename");
  }) as typeof fs.renameSync;
  try {
    await assert.rejects(manager.update({ operationId: "update45", artifact: target }),
      LifecycleInterruption);
  } finally { fs.renameSync = rename; }
  const journal = await f.adapter.readJournal();
  assert.equal(journal?.phase, "staged");
  await f.adapter.writeJournal({ ...journal!, phase: "rollback_required" } as LifecycleJournal);

  let interrupted = false;
  fs.renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
    rename(from, to);
    if (!interrupted && String(to) === path.join(f.paths.lifecycleRoot, point)) {
      interrupted = true;
      throw new LifecycleInterruption(`crash after restore ${point} rename`);
    }
  }) as typeof fs.renameSync;
  try {
    await assert.rejects(new LifecycleManager(f.adapter).update({
      operationId: "update45", artifact: target,
    }), LifecycleInterruption);
  } finally { fs.renameSync = rename; }
  assert.equal(interrupted, true);
  assert.equal((await f.adapter.readJournal())?.phase, "rollback_required");
  const receipt = await new LifecycleManager(f.adapter).update({
    operationId: "update45", artifact: target,
  });
  assert.equal(receipt.status, "rolled_back");
  assertConsistent(f, "0.7.44");
  rows.push({ point, outcome: "restore_retry", status: receipt.status, version: "0.7.44" });
  console.log(`PASS restore ${point} retry: ${receipt.status}, running 0.7.44`);
}

async function main() {
try {
  const selectedPoint = process.env.PR428_CRASH_POINT;
  const selectedOutcome = process.env.PR428_CRASH_OUTCOME;
  for (const point of ["installation.json", "state.json", "current"] as const)
    for (const outcome of ["retry", "restore"] as const)
      if ((!selectedPoint || point === selectedPoint) && (!selectedOutcome || outcome === selectedOutcome))
        await trial(point, outcome);
  for (const point of ["installation.json", "state.json", "current"] as const)
    if ((!selectedPoint || point === selectedPoint) && (!selectedOutcome || selectedOutcome === "restore_retry"))
      await restoreCrash(point);
  assert.equal(rows.length, selectedPoint && selectedOutcome ? 1 : 9);
  console.log(JSON.stringify({ proof: "pr428-r6-lifecycle-crash", passed: rows.length, rows }));
} finally { fs.rmSync(root, { recursive: true, force: true }); }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
