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

async function main() {
  try {
    await receiptsAreNeverTrimmed();
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
