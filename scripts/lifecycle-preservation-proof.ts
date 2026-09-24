import { createProofCompletion } from "./lib/proof-completion";
/**
 * eco-6hoxj.163.49: no lifecycle command removes a receipt.
 *
 * Every display receipt under lifecycle/receipts/ stays. Before this, every
 * command except a keep-all update or rollback trimmed that directory to the
 * 32 names that sort last, so a diagnostic support bundle or an uninstall
 * preview could delete a refused or rollback_required receipt, the only
 * record of that operation, or the receipt the command had just written.
 *
 * eco-6hoxj.163.50: staging never changes a runtime version that already
 * exists. Before this, stage() removed and re-copied each companion of an
 * existing version before it checked the executable, and removed them again
 * when that check failed: a rebuilt bundle claiming an installed version
 * failed, rolled back "successfully", and left the installed runtime without
 * its native module. Now a different executable or companion fails before
 * any file of the version changes, identical files are kept as they are, and
 * a failed stage removes only what it created.
 *
 * Every case runs the production lifecycle composition (real filesystem
 * adapter, real SQLite ledger snapshot adapter, real mutation authority) over
 * its own disposable collector home; only the service boundary is a fixture,
 * and the real-CLI runs put a stub launchctl on PATH that must never be
 * called. The cases use only the lifecycle surface that existed before the
 * fix, so the same file runs against the base and shows each problem red
 * there. Cases are independent; the run fails if any check fails.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { collectorBufferPath, collectorConfigPath } from "../packages/collector-cli/src/config";
import { launchAgentPlistPath } from "../packages/collector-cli/src/launch-agent";
import { composeLifecycleAdapter, SqliteLedgerSnapshotAdapter } from "../packages/collector-cli/src/lifecycle-adapters";
import {
  LifecycleManager,
  PURGE_CONFIRMATION,
  type LifecycleAdapter,
  type LifecycleReadiness,
  type LifecycleReceipt,
  type LifecycleSupportSnapshot,
  type RuntimeArtifact,
} from "../packages/collector-cli/src/lifecycle";
import type { LifecycleDatabaseAdapter, LifecycleServiceAdapter } from "../packages/collector-cli/src/lifecycle-filesystem";

const CASES = {
  receipts: [
    "fixture_holds_more_than_32_receipts_including_a_refused_one",
    "support_bundle_keeps_every_receipt",
    "uninstall_preview_keeps_every_receipt",
    "purge_preview_keeps_every_receipt",
    "real_cli_support_bundle_and_previews_keep_every_receipt",
    "keep_all_update_keeps_every_receipt",
    "update_rollback_and_automatic_rollback_keep_every_receipt",
    "snapshots_prune_apply_keeps_every_receipt_and_a_pending_rollback_required_one",
    "uninstall_and_purge_apply_keep_every_receipt",
  ],
  stage: [
    "rebuilt_same_version_update_fails_before_any_file_of_the_installed_runtime_changes",
    "automatic_rollback_leaves_the_installed_runtime_whole",
    "same_version_with_a_different_companion_fails_without_changing_it",
    "same_bundle_restage_verifies_and_rewrites_nothing",
    "failed_companion_copy_leaves_no_unverified_file_in_the_runtime",
    "leftover_companion_temp_from_an_interrupted_copy_is_replaced",
  ],
} as const;
const EXPECTED_CHECKS = Object.values(CASES).reduce((total, names) => total + names.length, 0);
const completion = createProofCompletion("lifecycle-preservation", EXPECTED_CHECKS);

const results: Array<{ name: string; passed: boolean; detail?: unknown }> = [];
function check(name: string, condition: unknown, detail?: unknown) {
  const passed = Boolean(condition);
  results.push({ name, passed, ...(passed ? {} : { detail }) });
  completion.check(name, passed);
  console.log(`${passed ? "PASS" : "FAIL"} ${name}${passed ? "" : ` ${JSON.stringify(detail ?? null).slice(0, 1600)}`}`);
}

const REPO_ROOT = path.resolve(import.meta.dirname, "..");
const CLI_ENTRY = path.join(REPO_ROOT, "packages", "collector-cli", "src", "cli.ts");
const TSX_LOADER = path.join(REPO_ROOT, "node_modules", "tsx", "dist", "loader.mjs");
const NODE_MAJOR = Number(process.versions.node.split(".", 1)[0]);
const ARCHITECTURE = process.arch === "x64" ? "x64" as const : "arm64" as const;
const ROOT = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-preservation-")));

const sha256 = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
const exists = (file: string) => fs.existsSync(file);
const listDirectory = (directory: string) => exists(directory) ? fs.readdirSync(directory).sort() : [];

async function rejection(action: () => Promise<unknown>) {
  try {
    await action();
    return null;
  } catch (error) {
    return error as Error & { code?: string };
  }
}

/** A small WAL ledger with the proof table. */
function createLedger(file: string) {
  const db = new Database(file);
  try {
    db.pragma("journal_mode = WAL");
    db.exec("create table proof_rows (id integer primary key, label text not null)");
    const insert = db.prepare("insert into proof_rows (label) values (?)");
    db.transaction(() => {
      for (let index = 0; index < 64; index += 1) insert.run(`seed-${index}`);
    })();
    db.pragma("wal_checkpoint(TRUNCATE)");
  } finally {
    db.close();
  }
}

type Home = ReturnType<typeof createHome>;

/** One disposable collector home with its own PLIMSOLL_HOME, ledger and fixture service. */
function createHome(name: string) {
  const home = path.join(ROOT, name);
  const collector = path.join(home, ".plimsoll");
  fs.mkdirSync(collector, { recursive: true, mode: 0o700 });
  fs.chmodSync(home, 0o700);
  process.env.PLIMSOLL_HOME = collector;
  const ledger = collectorBufferPath(home);
  const lifecycleRoot = path.join(collector, "lifecycle");
  fs.writeFileSync(collectorConfigPath(home), "{}\n", { mode: 0o600 });
  createLedger(ledger);
  const manifest = launchAgentPlistPath(home);
  const service = { version: null as string | null, failReadiness: false };
  const writeManifest = (executablePath: string | null) => {
    if (executablePath === null) {
      fs.rmSync(manifest, { force: true });
      return;
    }
    fs.mkdirSync(path.dirname(manifest), { recursive: true, mode: 0o700 });
    fs.writeFileSync(manifest, `<plist><array><string>${executablePath}</string><string>start</string></array></plist>\n`, { mode: 0o600 });
  };
  const readiness = (expectedVersion: string | null): LifecycleReadiness => {
    const ready = !service.failReadiness && service.version !== null && service.version === expectedVersion;
    return {
      ready,
      runtimeVersion: service.version,
      serviceReady: ready,
      configCompatible: true,
      databaseCompatible: true,
      reason: ready ? "ready" : "service_unready",
    };
  };
  const fixtureService: LifecycleServiceAdapter = {
    async activate(input) {
      service.version = input.version;
      writeManifest(input.executablePath);
    },
    async restore(input) {
      service.version = input.version;
      writeManifest(input.executablePath);
    },
    async remove() {
      service.version = null;
      writeManifest(null);
    },
    async readiness(expectedVersion) {
      return readiness(expectedVersion);
    },
    async supportSnapshot(): Promise<LifecycleSupportSnapshot> {
      return {
        installedVersion: service.version,
        runtimeVersion: service.version,
        platform: "darwin",
        architecture: ARCHITECTURE,
        nodeMajor: NODE_MAJOR,
        readiness: readiness(service.version),
        counters: { activeDelivery: 0, deadDelivery: 0, tokenAttributedEvents: 0, maintenancePending: 0 },
        boundedLogs: [],
      };
    },
  };
  const artifact = (version: string): RuntimeArtifact => {
    const sourcePath = path.join(home, "artifacts", `plimsoll-${version}.mjs`);
    fs.mkdirSync(path.dirname(sourcePath), { recursive: true, mode: 0o700 });
    fs.writeFileSync(sourcePath, `// preservation proof runtime ${version}; never executed\n`, { mode: 0o700 });
    return {
      version,
      platform: "darwin",
      architecture: ARCHITECTURE,
      nodeMajor: NODE_MAJOR,
      sha256: `sha256:${sha256(fs.readFileSync(sourcePath))}`,
      sourcePath,
    };
  };
  const adapter = (options: { database?: LifecycleDatabaseAdapter; keepAll?: boolean } = {}): LifecycleAdapter => {
    process.env.PLIMSOLL_HOME = collector;
    return composeLifecycleAdapter({
      homeDir: home,
      lifecycleRoot,
      artifactSourceRoot: home,
      service: fixtureService,
      ...(options.database ? { database: options.database } : {}),
      ...(options.keepAll ? { keepAll: true } : {}),
    });
  };
  return {
    home,
    collector,
    lifecycleRoot,
    service,
    artifact,
    manager: (database?: LifecycleDatabaseAdapter) => new LifecycleManager(adapter({ ...(database ? { database } : {}) })),
    keepAllManager: () => new LifecycleManager(adapter({ keepAll: true })),
    receipt: (operationId: string, operation = "update") => {
      const file = path.join(lifecycleRoot, "receipts", `${operationId}-${operation}.json`);
      return exists(file) ? JSON.parse(fs.readFileSync(file, "utf8")) as LifecycleReceipt : null;
    },
  };
}

/** A launchctl on PATH that records any call; lifecycle commands must never make one. */
function stubLaunchctl() {
  const directory = path.join(ROOT, "stub-bin");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const calls = path.join(directory, "calls.log");
  fs.writeFileSync(path.join(directory, "launchctl"), `#!/bin/sh\necho "$@" >> ${JSON.stringify(calls)}\nexit 3\n`, { mode: 0o700 });
  return { directory, calls };
}

/** The real CLI over the home, with only the stub launchctl ahead of PATH. */
function cli(home: Home, args: readonly string[], stub: { directory: string }) {
  const result = spawnSync(process.execPath, ["--import", TSX_LOADER, CLI_ENTRY, ...args], {
    cwd: home.home,
    env: {
      PATH: `${stub.directory}${path.delimiter}${process.env.PATH ?? ""}`,
      HOME: home.home,
      PLIMSOLL_HOME: home.collector,
      TMPDIR: process.env.TMPDIR,
      CODEX_HOME: path.join(home.home, ".codex"),
      CLAUDE_CONFIG_DIR: path.join(home.home, ".claude"),
      LANG: "en_US.UTF-8",
      TZ: "UTC",
    },
    encoding: "utf8",
    timeout: 180_000,
  });
  return { code: result.status, stderr: (result.stderr ?? "").slice(-300) };
}

/** Runs one case; an exception fails only that case's remaining checks. */
async function runCase(names: readonly string[], body: (record: typeof check) => Promise<void>) {
  const recorded = new Set<string>();
  const record = (name: string, condition: unknown, detail?: unknown) => {
    recorded.add(name);
    check(name, condition, detail);
  };
  try {
    await body(record);
  } catch (error) {
    for (const name of names) {
      if (!recorded.has(name)) check(name, false, { threw: error instanceof Error ? error.stack?.split("\n").slice(0, 4).join(" | ") : String(error) });
    }
  }
}

// ---- eco-6hoxj.163.49: receipts are never trimmed -------------------------

/** Every display receipt: file name → content digest. */
function receiptFiles(home: Home) {
  const directory = path.join(home.lifecycleRoot, "receipts");
  return new Map(listDirectory(directory).filter((name) => name.endsWith(".json"))
    .map((name) => [name, sha256(fs.readFileSync(path.join(directory, name)))] as const));
}

/**
 * Runs one lifecycle command: which receipts that existed before it are gone
 * or changed after it, and which of its own receipts are missing.
 */
async function receiptsAcross(home: Home, command: () => Promise<unknown>, own: readonly string[]) {
  const before = receiptFiles(home);
  const error = await rejection(command);
  const after = receiptFiles(home);
  return {
    error: error?.message ?? null,
    lost: [...before].filter(([name, digest]) => after.get(name) !== digest).map(([name]) => name),
    missingOwn: own.filter((name) => !after.has(name)),
    receipts: after.size,
  };
}

const keptEvery = (step: { lost: readonly string[]; missingOwn: readonly string[] }) =>
  step.lost.length === 0 && step.missingOwn.length === 0;

async function receiptsAreNeverTrimmed() {
  await runCase(CASES.receipts, async (record) => {
    const home = createHome("receipts");
    await home.manager().update({ operationId: "u1", artifact: home.artifact("7.0.0") });
    await home.manager().update({ operationId: "u2", artifact: home.artifact("7.0.1") });
    // A real refusal (a full copy without room): its display receipt is the only record of it.
    const noRoom = new SqliteLedgerSnapshotAdapter({ clone: () => false, cloneSupported: () => false, freeBytes: () => 0 });
    const refused = await rejection(() => home.manager(noRoom).update({ operationId: "a-refused", artifact: home.artifact("7.0.2") }));
    // Earlier operations' receipts, past 32 in all. Their names sort after
    // every real one here, so a trim by name reaches the real ones first.
    for (let index = 0; index < 30; index += 1) {
      fs.writeFileSync(path.join(home.lifecycleRoot, "receipts", `z-seed-${String(index).padStart(2, "0")}-support_bundle.json`),
        `${JSON.stringify({ seed: index })}\n`, { mode: 0o600 });
    }
    const initial = receiptFiles(home);
    record(CASES.receipts[0],
      refused?.code === "LIFECYCLE_SNAPSHOT_REFUSED" && initial.size === 33 && home.receipt("a-refused")?.status === "refused" &&
        !exists(path.join(home.lifecycleRoot, "completed-operations", "a-refused.json")),
      { receipts: initial.size, refused: refused?.message });

    const bundle = await receiptsAcross(home, () => home.manager().supportBundle("s1"), ["s1-support_bundle.json"]);
    record(CASES.receipts[1], bundle.error === null && keptEvery(bundle), bundle);
    const uninstallPreview = await receiptsAcross(home, () => home.manager().uninstall({ operationId: "p-uninstall" }),
      ["p-uninstall-uninstall.json"]);
    record(CASES.receipts[2], uninstallPreview.error === null && keptEvery(uninstallPreview), uninstallPreview);
    const purgePreview = await receiptsAcross(home, () => home.manager().purge({ operationId: "p-purge" }), ["p-purge-purge.json"]);
    record(CASES.receipts[3], purgePreview.error === null && keptEvery(purgePreview), purgePreview);

    // The commands an agent runs by hand on a host, through the real CLI.
    const stub = stubLaunchctl();
    const runs: Array<ReturnType<typeof cli>> = [];
    const byHand = await receiptsAcross(home, async () => {
      runs.push(cli(home, ["lifecycle", "support-bundle", "--operation-id", "cli-s1"], stub));
      runs.push(cli(home, ["lifecycle", "uninstall", "--operation-id", "cli-p-uninstall"], stub));
      runs.push(cli(home, ["lifecycle", "purge", "--operation-id", "cli-p-purge"], stub));
    }, ["cli-s1-support_bundle.json", "cli-p-uninstall-uninstall.json", "cli-p-purge-purge.json"]);
    record(CASES.receipts[4], runs.every((run) => run.code === 0) && keptEvery(byHand) && !exists(stub.calls),
      { ...byHand, runs });

    const keepAll = await receiptsAcross(home, () => home.keepAllManager().update({ operationId: "k1", artifact: home.artifact("7.0.3") }),
      ["k1-update.json"]);
    record(CASES.receipts[5], keepAll.error === null && keptEvery(keepAll), keepAll);

    const update = await receiptsAcross(home, () => home.manager().update({ operationId: "u3", artifact: home.artifact("7.0.4") }),
      ["u3-update.json"]);
    const rollback = await receiptsAcross(home, () => home.manager().rollback({ operationId: "rb1", artifact: home.artifact("7.0.3") }),
      ["rb1-rollback.json"]);
    home.service.failReadiness = true;
    const rolledBack = await receiptsAcross(home, () => home.manager().update({ operationId: "f1", artifact: home.artifact("7.0.5") }),
      ["f1-update.json"]);
    home.service.failReadiness = false;
    record(CASES.receipts[6],
      update.error === null && rollback.error === null && /readiness failed/.test(rolledBack.error ?? "") &&
        home.receipt("f1")?.status === "rolled_back" && [update, rollback, rolledBack].every(keptEvery),
      { update, rollback, rolledBack });

    // A rollback whose ledger restore is refused stays rollback_required, and
    // its display receipt is the only record of it; prune may run meanwhile.
    const ledger = new SqliteLedgerSnapshotAdapter();
    const refusesRestore: LifecycleDatabaseAdapter = {
      snapshot: (input) => ledger.snapshot(input),
      restore: (input) => noRoom.restore(input),
    };
    home.service.failReadiness = true;
    const pending = await rejection(() => home.manager(refusesRestore).update({ operationId: "rr1", artifact: home.artifact("7.0.6") }));
    home.service.failReadiness = false;
    const prune = await receiptsAcross(home, () => home.manager().pruneSnapshots({ operationId: "prune-1", apply: true }),
      ["prune-1-snapshots_prune.json"]);
    record(CASES.receipts[7],
      /rollback required/.test(pending?.message ?? "") && home.receipt("rr1")?.status === "rollback_required" &&
        exists(path.join(home.lifecycleRoot, "journal.json")) && prune.error === null && keptEvery(prune),
      { pending: pending?.message, rr1: home.receipt("rr1")?.status, prune });
    // Recovering the same operation replaces its own receipt with the rolled_back one.
    await home.manager().update({ operationId: "rr1", artifact: home.artifact("7.0.6") });

    const uninstall = await receiptsAcross(home, () => home.manager().uninstall({ operationId: "x-uninstall", apply: true }),
      ["x-uninstall-uninstall.json"]);
    const purge = await receiptsAcross(home,
      () => home.manager().purge({ operationId: "x-purge", apply: true, confirmation: PURGE_CONFIRMATION }), ["x-purge-purge.json"]);
    record(CASES.receipts[8], uninstall.error === null && purge.error === null && keptEvery(uninstall) && keptEvery(purge),
      { uninstall, purge });
  });
}

// ---- eco-6hoxj.163.50: staging never touches an existing version ---------

type BundleContents = { executable: string; companions: Record<string, string>; wrongDigest?: string };

/** A bundle and its vendored companions in their own source directory; equal contents, equal digests. */
function bundleArtifact(home: Home, version: string, label: string, contents: BundleContents): RuntimeArtifact {
  const directory = path.join(home.home, "artifacts", label);
  const write = (relativePath: string, content: string, mode: number) => {
    const file = path.join(directory, ...relativePath.split("/"));
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
    fs.writeFileSync(file, content, { mode });
    return file;
  };
  const sourcePath = write("plimsoll.mjs", `// ${contents.executable}; never executed\n`, 0o700);
  return {
    version,
    platform: "darwin",
    architecture: ARCHITECTURE,
    nodeMajor: NODE_MAJOR,
    sha256: `sha256:${sha256(fs.readFileSync(sourcePath))}`,
    sourcePath,
    files: Object.entries(contents.companions).map(([relativePath, content]) => {
      const file = write(relativePath, content, 0o600);
      return {
        relativePath,
        sourcePath: file,
        sha256: `sha256:${relativePath === contents.wrongDigest ? sha256("not this file") : sha256(fs.readFileSync(file))}`,
      };
    }),
  };
}

const runtimeRoot = (home: Home, version: string) => path.join(home.lifecycleRoot, "versions", version, `darwin-${ARCHITECTURE}`);

/** Every entry under a runtime version: type, path, mode, size, mtime, inode and content digest. */
function runtimeTree(home: Home, version: string) {
  const root = path.join(home.lifecycleRoot, "versions", version);
  const rows: string[] = [];
  const walk = (entry: string) => {
    const stat = fs.lstatSync(entry, { bigint: true });
    const relative = path.relative(root, entry) || ".";
    if (stat.isDirectory()) {
      rows.push(`d ${relative} ${stat.mode} ${stat.mtimeNs}`);
      for (const name of fs.readdirSync(entry).sort()) walk(path.join(entry, name));
    } else {
      rows.push(`${stat.isFile() ? "f" : "o"} ${relative} ${stat.mode} ${stat.size} ${stat.mtimeNs} ${stat.ino} ${
        stat.isFile() ? sha256(fs.readFileSync(entry)) : ""}`);
    }
  };
  if (exists(root)) walk(root);
  return rows;
}

const changedRows = (before: readonly string[], after: readonly string[]) => ({
  gone: before.filter((row) => !after.includes(row)),
  appeared: after.filter((row) => !before.includes(row)),
});

/** The artifact's executable and companions, each with the digest it declares, as they are now. */
function stagedMatches(home: Home, artifact: RuntimeArtifact) {
  const root = runtimeRoot(home, artifact.version);
  const digestOf = (file: string) => exists(file) && fs.lstatSync(file).isFile() ? `sha256:${sha256(fs.readFileSync(file))}` : null;
  return [
    { file: "bin/plimsoll.mjs", sha256: artifact.sha256 },
    ...(artifact.files ?? []).map((file) => ({ file: file.relativePath, sha256: file.sha256 })),
  ].filter((entry) => digestOf(path.join(root, ...entry.file.split("/"))) !== entry.sha256).map((entry) => entry.file);
}

async function stagingNeverTouchesAnExistingVersion() {
  await runCase(CASES.stage, async (record) => {
    const home = createHome("stage");
    const native = "node_modules/better-sqlite3/build/Release/better_sqlite3.node";
    const dashboard = "bin/dashboard.html";
    const build = (version: string, executable: string, nativeBuild: string): BundleContents => ({
      executable: `plimsoll ${version} ${executable}`,
      companions: { [dashboard]: `<html>${version}</html>\n`, [native]: `native module ${version} ${nativeBuild}\n` },
    });
    await home.keepAllManager().update({ operationId: "c0", artifact: home.artifact("4.9.0") });
    const c1 = bundleArtifact(home, "5.0.0", "c1", build("5.0.0", "build 1", "build 1"));
    await home.keepAllManager().update({ operationId: "c1", artifact: c1 });
    const installed = runtimeTree(home, "5.0.0");

    // The reviewer's case: a rebuilt executable claiming the installed version.
    const rebuilt = await rejection(() => home.keepAllManager().update({
      operationId: "c2", artifact: bundleArtifact(home, "5.0.0", "c2", build("5.0.0", "build 2", "build 1")),
    }));
    const afterRebuilt = runtimeTree(home, "5.0.0");
    record(CASES.stage[0],
      /immutable runtime target already differs/.test(rebuilt?.message ?? "") && home.receipt("c2")?.status === "rolled_back" &&
        JSON.stringify(afterRebuilt) === JSON.stringify(installed),
      { error: rebuilt?.message, status: home.receipt("c2")?.status, ...changedRows(installed, afterRebuilt) });
    const state = JSON.parse(fs.readFileSync(path.join(home.lifecycleRoot, "state.json"), "utf8")) as { version: string };
    record(CASES.stage[1],
      state.version === "5.0.0" && fs.readlinkSync(path.join(home.lifecycleRoot, "current")) === runtimeRoot(home, "5.0.0") &&
        stagedMatches(home, c1).length === 0,
      { state, current: fs.readlinkSync(path.join(home.lifecycleRoot, "current")), missingOrChanged: stagedMatches(home, c1) });

    // The installed executable with a rebuilt native module, listed after a
    // companion the version does not hold yet: nothing may be written first.
    const rebuiltNative = build("5.0.0", "build 1", "build 2");
    const recompiled = await rejection(() => home.keepAllManager().update({
      operationId: "c3", artifact: bundleArtifact(home, "5.0.0", "c3", {
        ...rebuiltNative,
        companions: {
          [dashboard]: rebuiltNative.companions[dashboard]!,
          "node_modules/bindings/bindings.js": "module.exports = null;\n",
          [native]: rebuiltNative.companions[native]!,
        },
      }),
    }));
    const afterRecompiled = runtimeTree(home, "5.0.0");
    record(CASES.stage[2],
      /immutable runtime companion node_modules\/better-sqlite3\/build\/Release\/better_sqlite3\.node already differs/
        .test(recompiled?.message ?? "") && home.receipt("c3")?.status === "rolled_back" &&
        JSON.stringify(afterRecompiled) === JSON.stringify(installed),
      { error: recompiled?.message, status: home.receipt("c3")?.status, ...changedRows(installed, afterRecompiled) });

    // The identical bundle again, from fresh source files with the same bytes.
    const repin = await home.keepAllManager().update({
      operationId: "c4", artifact: bundleArtifact(home, "5.0.0", "c4", build("5.0.0", "build 1", "build 1")),
    });
    const afterRepin = runtimeTree(home, "5.0.0");
    record(CASES.stage[3], repin.status === "completed" && JSON.stringify(afterRepin) === JSON.stringify(installed),
      { status: repin.status, ...changedRows(installed, afterRepin) });

    // A new version whose native module fails its digest: nothing unverified stays behind.
    const failedCopy = await rejection(() => home.keepAllManager().update({
      operationId: "c5", artifact: bundleArtifact(home, "5.1.0", "c5", { ...build("5.1.0", "build 1", "build 1"), wrongDigest: native }),
    }));
    const leftover = runtimeTree(home, "5.1.0").filter((row) => !row.startsWith("d "));
    record(CASES.stage[4],
      /companion 1 digest mismatch/.test(failedCopy?.message ?? "") && home.receipt("c5")?.status === "rolled_back" && leftover.length === 0,
      { error: failedCopy?.message, status: home.receipt("c5")?.status, leftover });

    // A stage interrupted mid-copy left only its temp file; the next stage replaces it.
    const temp = path.join(runtimeRoot(home, "5.2.0"), ...`${native}+staging`.split("/"));
    fs.mkdirSync(path.dirname(temp), { recursive: true, mode: 0o700 });
    fs.writeFileSync(temp, "partial copy\n", { mode: 0o600 });
    const c6 = bundleArtifact(home, "5.2.0", "c6", build("5.2.0", "build 1", "build 1"));
    const retried = await home.keepAllManager().update({ operationId: "c6", artifact: c6 });
    record(CASES.stage[5], retried.status === "completed" && !exists(temp) && stagedMatches(home, c6).length === 0,
      { status: retried.status, temp: exists(temp), missingOrChanged: stagedMatches(home, c6) });
  });
}

async function main() {
  try {
    await receiptsAreNeverTrimmed();
    await stagingNeverTouchesAnExistingVersion();
    const failed = results.filter((row) => !row.passed).map((row) => row.name);
    console.log(JSON.stringify({ proof: "lifecycle-preservation", checks: results.length, passed: results.length - failed.length, failed }));
  } finally {
    fs.rmSync(ROOT, { recursive: true, force: true });
  }
  completion.complete();
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
