import { createProofCompletion } from "./lib/proof-completion";
/**
 * eco-6hoxj.163.30 r2: update snapshots, rollback and retention are data-safe
 * in the worst case, not only in the tested one.
 *
 * Regression cases for the independent review's reproduced blockers. Every
 * case runs the production lifecycle composition (real filesystem adapter,
 * real SQLite ledger snapshot adapter, real mutation authority) over its own
 * disposable collector home; only the service boundary is a fixture. Nothing
 * here loads, unloads or inspects a real LaunchAgent, and no live collector,
 * ledger or config outside the proof root is touched.
 *
 * The cases use only the lifecycle surface that already existed before the
 * fixes, so the same file runs against the reviewed head and shows each
 * blocker red there. Cases are independent: one failing case never hides
 * another. The run fails if any check fails.
 */
import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { collectorBufferPath, collectorConfigPath } from "../packages/collector-cli/src/config";
import { launchAgentPlistPath } from "../packages/collector-cli/src/launch-agent";
import {
  composeLifecycleAdapter,
  integrityCheckOffThread,
  otherProcessesWithFilesOpen,
  SqliteLedgerSnapshotAdapter,
  whileKeepingLease,
} from "../packages/collector-cli/src/lifecycle-adapters";
import { formatSnapshotInventory } from "../packages/collector-cli/src/lifecycle-command";
import { LifecycleMutationAuthority, type LifecycleMutationLease } from "../packages/collector-cli/src/lifecycle-authority";
import {
  LifecycleManager,
  type LifecycleAdapter,
  type LifecycleReadiness,
  type LifecycleReceipt,
  type RuntimeArtifact,
} from "../packages/collector-cli/src/lifecycle";
import type { LifecycleDatabaseAdapter, LifecycleServiceAdapter } from "../packages/collector-cli/src/lifecycle-filesystem";

const CASES = {
  b1: [
    "update_refuses_a_ledger_another_process_has_open_before_any_change",
    "writer_left_open_through_a_failed_update_keeps_every_write_at_the_ledger_path",
    "rollback_refuses_to_replace_a_ledger_a_writer_opened_after_the_snapshot",
    "refused_rollback_completes_once_the_writer_stops",
    "rollback_to_no_ledger_refuses_to_remove_a_ledger_that_is_open",
  ],
  b2: [
    "restore_without_room_for_a_byte_copy_refuses_and_keeps_the_live_ledger",
    "restore_byte_copy_failure_keeps_the_live_ledger",
    "restore_refuses_a_restored_copy_that_fails_integrity_check",
    "rolled_back_receipt_records_how_the_ledger_was_restored",
  ],
  b3: [
    "a_two_field_completion_marker_is_unknown_and_never_pruned",
    "markers_with_missing_extra_mismatched_or_contradictory_fields_are_unknown_and_kept",
    "receipts_with_the_target_lists_up_to_0_7_38_are_known_and_mixed_lists_are_unknown",
    "genuine_0_7_39_target_lists_remain_prunable",
  ],
  b4: [
    "completion_sequence_is_durable_and_increases_with_each_completion",
    "backward_clock_step_never_prunes_the_newest_rollback_points",
    "pre_sequencing_receipts_are_ordered_by_version_chain_not_file_times",
    "ambiguous_pre_sequencing_order_keeps_every_snapshot",
    "duplicated_completion_sequence_keeps_every_snapshot",
  ],
  b5: [
    "crash_after_unlink_leaves_a_durable_removal_record",
    "next_prune_records_the_removal_the_crash_left_unrecorded",
    "failed_receipt_write_after_retention_is_recovered_by_the_next_prune",
    "unrecognized_trash_entries_are_left_alone_and_never_block_retention",
  ],
  preflight: [
    "preflight_is_read_only_and_creates_nothing",
    "clone_helper_works_detached_without_a_terminal_or_login_environment",
  ],
  r3Lease: [
    "rollback_renews_its_lease_through_a_long_integrity_check",
    "superseded_rollback_never_swaps_the_ledger_and_the_retry_completes",
    "resumed_rollback_keeps_its_restore_record",
    "rollback_renews_its_lease_through_a_long_byte_copy",
    "frequent_progress_does_not_rewrite_the_lease_every_time",
  ],
  guard: [
    "prune_waits_for_rollback_complete_before_receipt_commit",
  ],
  r3Damage: [
    "failed_update_on_an_already_damaged_ledger_still_rolls_back",
    "restore_refuses_damage_the_live_ledger_does_not_already_have",
  ],
  r3Repair: [
    "lost_order_record_leaves_new_receipts_sequenced_and_reconcile_rebuilds_it",
    "torn_order_record_with_older_receipts_is_sealed_only_by_an_explicit_keep_set",
    "older_cli_receipt_after_sequencing_is_resolved_by_an_explicit_keep_set",
    "unreadable_removal_record_blocks_retention_until_reconcile_moves_it_aside",
    "completed_receipt_flipped_to_rolled_back_is_unknown_and_kept",
    "sequence_beyond_the_order_record_keeps_every_snapshot",
    "seal_must_keep_a_way_back_to_an_earlier_version",
    "seal_never_releases_a_later_update_that_reuses_a_covered_id",
  ],
  r3Cleanup: [
    "stale_intent_and_order_temporaries_are_removed_by_the_next_prune",
    "stale_restore_copy_is_removed_by_the_next_update",
    "lifecycle_checkpoints_flush_the_ledger_to_stable_storage",
  ],
  r4FailClosed: [
    "open_handle_check_that_cannot_answer_refuses_the_update",
    "integrity_helper_that_crashes_or_prints_garbage_fails_the_check",
    "lsof_error_counts_as_unproven_not_as_nobody",
  ],
  r4Rebuild: [
    "lost_order_record_is_rebuilt_only_when_sequences_follow_the_version_chain",
  ],
  r4Seal: [
    "first_install_snapshot_is_not_a_way_back",
    "seal_that_releases_the_newest_way_back_needs_force",
    "seal_on_a_healthy_host_needs_force",
    "ambiguous_legacy_order_accepts_a_seal_without_force",
  ],
  r4OldReceipts: [
    "receipt_written_without_a_sequence_is_flagged_and_decided_by_reconcile",
  ],
  r5WayBack: [
    "forced_seal_never_keeps_only_a_snapshot_whose_runtime_is_gone",
    "snapshot_whose_runtime_no_longer_matches_its_recorded_digest_is_not_a_way_back",
    "snapshot_missing_its_own_files_is_not_a_way_back",
    "retention_keeps_a_usable_way_back_when_the_newest_one_cannot_restore",
    "snapshot_from_before_digests_were_recorded_counts_while_its_runtime_exists",
  ],
  r3Handles: [
    "update_refuses_a_ledger_another_process_opened_without_using_it",
    "rollback_refuses_to_swap_a_ledger_another_process_opened_without_using_it",
    "replaced_ledger_header_is_zeroed_for_an_opener_the_handle_check_misses",
    "fence_is_checked_again_immediately_before_the_swap",
  ],
  r8PruneSafety: [
    "partial_prune_move_is_undone_when_the_kept_runtime_disappears",
    "same_id_carried_removal_record_survives_a_refused_retry",
    "unstampable_planned_runtime_is_refused_before_any_move",
    "rollback_required_prune_is_allowed_to_free_recovery_space",
    "crashed_prune_restores_the_last_usable_way_back",
    "failed_crash_restore_refuses_and_preserves_the_removal_record",
  ],
} as const;
const EXPECTED_CHECKS = Object.values(CASES).reduce((total, names) => total + names.length, 0);
const completion = createProofCompletion("lifecycle-data-safety", EXPECTED_CHECKS);

const results: Array<{ name: string; passed: boolean; detail?: unknown }> = [];
function check(name: string, condition: unknown, detail?: unknown) {
  const passed = Boolean(condition);
  results.push({ name, passed, ...(passed ? {} : { detail }) });
  completion.check(name, passed);
  console.log(`${passed ? "PASS" : "FAIL"} ${name}${passed ? "" : ` ${JSON.stringify(detail ?? null).slice(0, 1600)}`}`);
}

const BETTER_SQLITE3 = createRequire(import.meta.url).resolve("better-sqlite3");
const NODE_MAJOR = Number(process.versions.node.split(".", 1)[0]);
const ARCHITECTURE = process.arch === "x64" ? "x64" as const : "arm64" as const;
const ROOT = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-data-safety-")));

const sha256 = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
const exists = (file: string) => fs.existsSync(file);
const listDirectory = (directory: string) => exists(directory) ? fs.readdirSync(directory).sort() : [];
const same = (left: readonly string[], right: readonly string[]) =>
  JSON.stringify([...left].sort()) === JSON.stringify([...right].sort());
async function rejection(action: () => Promise<unknown>) {
  try {
    await action();
    return null;
  } catch (error) {
    return error as Error & { code?: string };
  }
}

function rowsDigest(file: string) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try {
    return sha256(JSON.stringify(db.prepare("select id, label from proof_rows order by id").all()));
  } finally {
    db.close();
  }
}

function integrityOf(file: string) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try {
    return db.pragma("integrity_check", { simple: true });
  } finally {
    db.close();
  }
}

function countLabel(file: string, label: string) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try {
    return (db.prepare("select count(*) n from proof_rows where label = ?").get(label) as { n: number }).n;
  } finally {
    db.close();
  }
}

function appendRow(file: string, label: string) {
  const db = new Database(file);
  try {
    db.prepare("insert into proof_rows (label) values (?)").run(label);
  } finally {
    db.close();
  }
}

/** A small WAL ledger with the proof table. */
function createLedger(file: string, label: string) {
  const db = new Database(file);
  try {
    db.pragma("journal_mode = WAL");
    db.exec("create table proof_rows (id integer primary key, label text not null)");
    const insert = db.prepare("insert into proof_rows (label) values (?)");
    db.transaction(() => {
      for (let index = 0; index < 64; index += 1) insert.run(`${label}-${index}`);
    })();
    db.pragma("wal_checkpoint(TRUNCATE)");
  } finally {
    db.close();
  }
}

type Home = ReturnType<typeof createHome>;

/** One disposable collector home with its own PLIMSOLL_HOME, ledger and fixture service. */
function createHome(name: string, options: { ledger?: boolean } = {}) {
  const home = path.join(ROOT, name);
  const collector = path.join(home, ".plimsoll");
  fs.mkdirSync(collector, { recursive: true, mode: 0o700 });
  fs.chmodSync(home, 0o700);
  process.env.PLIMSOLL_HOME = collector;
  const ledger = collectorBufferPath(home);
  const lifecycleRoot = path.join(collector, "lifecycle");
  fs.writeFileSync(collectorConfigPath(home), "{}\n", { mode: 0o600 });
  if (options.ledger !== false) createLedger(ledger, "seed");
  const manifest = launchAgentPlistPath(home);
  const service = {
    version: null as string | null,
    failReadiness: false,
    activations: 0,
    onActivate: null as null | ((version: string) => Promise<void>),
  };
  const writeManifest = (executablePath: string | null) => {
    if (executablePath === null) {
      fs.rmSync(manifest, { force: true });
      return;
    }
    fs.mkdirSync(path.dirname(manifest), { recursive: true, mode: 0o700 });
    fs.writeFileSync(manifest, `<plist><array><string>${executablePath}</string><string>start</string></array></plist>\n`, { mode: 0o600 });
  };
  const fixtureService: LifecycleServiceAdapter = {
    async activate(input) {
      service.activations += 1;
      service.version = input.version;
      writeManifest(input.executablePath);
      await service.onActivate?.(input.version);
    },
    async restore(input) {
      service.version = input.version;
      writeManifest(input.executablePath);
    },
    async remove() {
      service.version = null;
      writeManifest(null);
    },
    async readiness(expectedVersion): Promise<LifecycleReadiness> {
      const ready = !service.failReadiness && service.version === expectedVersion;
      return {
        ready,
        runtimeVersion: service.version,
        serviceReady: ready,
        configCompatible: true,
        databaseCompatible: true,
        reason: ready ? "ready" : "service_unready",
      };
    },
    async supportSnapshot() {
      throw new Error("support snapshots are not exercised by this proof");
    },
  };
  const artifact = (version: string): RuntimeArtifact => {
    const sourcePath = path.join(home, "artifacts", `plimsoll-${version}.mjs`);
    fs.mkdirSync(path.dirname(sourcePath), { recursive: true, mode: 0o700 });
    fs.writeFileSync(sourcePath, `// data-safety proof runtime ${version}; never executed\n`, { mode: 0o700 });
    return {
      version,
      platform: "darwin",
      architecture: ARCHITECTURE,
      nodeMajor: NODE_MAJOR,
      sha256: `sha256:${sha256(fs.readFileSync(sourcePath))}`,
      sourcePath,
    };
  };
  const adapter = (database?: LifecycleDatabaseAdapter, options: { authorityLeaseMs?: number } = {}): LifecycleAdapter => {
    process.env.PLIMSOLL_HOME = collector;
    return composeLifecycleAdapter({
      homeDir: home,
      lifecycleRoot,
      artifactSourceRoot: home,
      service: fixtureService,
      ...(database ? { database } : {}),
      ...options,
    });
  };
  return {
    name,
    home,
    collector,
    ledger,
    lifecycleRoot,
    manifest,
    service,
    artifact,
    adapter,
    manager: (database?: LifecycleDatabaseAdapter) => new LifecycleManager(adapter(database)),
    snapshots: () => listDirectory(path.join(lifecycleRoot, "snapshots")),
    versions: () => listDirectory(path.join(lifecycleRoot, "versions")),
    receipt: (operationId: string, operation = "update") => {
      const file = path.join(lifecycleRoot, "receipts", `${operationId}-${operation}.json`);
      return exists(file) ? JSON.parse(fs.readFileSync(file, "utf8")) as LifecycleReceipt : null;
    },
  };
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

// ---- B1: no split-brain rollback -----------------------------------------

type Writer = {
  ready: string;
  write: (label: string) => Promise<string>;
  close: () => Promise<void>;
};

/** Another process that opens the ledger, commits a row and keeps the connection open. */
async function openWriter(ledger: string, firstLabel: string): Promise<Writer> {
  const child: ChildProcessWithoutNullStreams = spawn(process.execPath, ["-e", `
    const Database = require(${JSON.stringify(BETTER_SQLITE3)});
    const db = new Database(${JSON.stringify(ledger)});
    db.prepare("insert into proof_rows (label) values (?)").run(${JSON.stringify(firstLabel)});
    process.stdout.write("ready\\n");
    process.stdin.setEncoding("utf8");
    let buffered = "";
    process.stdin.on("data", (chunk) => {
      buffered += chunk;
      let newline;
      while ((newline = buffered.indexOf("\\n")) >= 0) {
        const command = buffered.slice(0, newline);
        buffered = buffered.slice(newline + 1);
        if (command.startsWith("write ")) {
          try {
            db.prepare("insert into proof_rows (label) values (?)").run(command.slice(6));
            process.stdout.write("wrote\\n");
          } catch (error) {
            process.stdout.write("write-error:" + String(error.code ?? error.message) + "\\n");
          }
        } else if (command === "close") {
          db.close();
          process.exit(0);
        }
      }
    });`], { stdio: ["pipe", "pipe", "pipe"] });
  child.stdout.setEncoding("utf8");
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += String(chunk); });
  const lines: string[] = [];
  const waiting: Array<(line: string) => void> = [];
  let pending = "";
  child.stdout.on("data", (chunk: string) => {
    pending += chunk;
    let newline;
    while ((newline = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, newline);
      pending = pending.slice(newline + 1);
      const next = waiting.shift();
      if (next) next(line); else lines.push(line);
    }
  });
  const nextLine = () => new Promise<string>((resolve, reject) => {
    const queued = lines.shift();
    if (queued !== undefined) return resolve(queued);
    const timer = setTimeout(() => reject(new Error(`writer did not answer: ${stderr.slice(-300)}`)), 30_000);
    waiting.push((line) => {
      clearTimeout(timer);
      resolve(line);
    });
  });
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  return {
    ready: await nextLine(),
    write: async (label) => {
      child.stdin.write(`write ${label}\n`);
      return nextLine();
    },
    close: async () => {
      if (child.exitCode === null) child.stdin.write("close\n");
      await exited;
    },
  };
}

function treeState(fixture: Home) {
  const read = (file: string) => exists(file) ? fs.readFileSync(file, "utf8") : null;
  return JSON.stringify({
    state: read(path.join(fixture.lifecycleRoot, "state.json")),
    journal: read(path.join(fixture.lifecycleRoot, "journal.json")),
    manifest: read(fixture.manifest),
    snapshots: fixture.snapshots(),
    versions: fixture.versions(),
  });
}

async function b1NoSplitBrainRollback() {
  await runCase([CASES.b1[0]], async (record) => {
    const fixture = createHome("b1-refuse");
    await fixture.manager().update({ operationId: "b1-initial", artifact: fixture.artifact("1.0.0") });
    const before = treeState(fixture);
    const activations = fixture.service.activations;
    const writer = await openWriter(fixture.ledger, "writer-before-update");
    let error: (Error & { code?: string }) | null;
    let receipt: LifecycleReceipt | null;
    try {
      error = await rejection(() => fixture.manager().update({ operationId: "b1-busy", artifact: fixture.artifact("1.0.1") }));
      receipt = fixture.receipt("b1-busy");
    } finally {
      await writer.close();
    }
    const refusal = receipt?.refusal as { reason?: unknown } | undefined;
    record(CASES.b1[0],
      writer.ready === "ready" && error?.code === "LIFECYCLE_SNAPSHOT_REFUSED" && receipt?.status === "refused" &&
        refusal?.reason === "ledger_in_use" && treeState(fixture) === before && fixture.service.activations === activations &&
        !exists(path.join(fixture.lifecycleRoot, "completed-operations", "b1-busy.json")) &&
        countLabel(fixture.ledger, "writer-before-update") === 1,
      { error: error?.message, receipt, before, after: treeState(fixture) });
  });
  // The review's fixture: a writer stays attached through an update whose
  // readiness fails. Every write it makes must stay visible at the ledger path.
  await runCase([CASES.b1[1]], async (record) => {
    const fixture = createHome("b1-review-writer");
    await fixture.manager().update({ operationId: "b1r-initial", artifact: fixture.artifact("1.0.0") });
    const writer = await openWriter(fixture.ledger, "writer-before-snapshot");
    let failed: (Error & { code?: string }) | null = null;
    let wrote = "";
    try {
      fixture.service.failReadiness = true;
      fixture.service.onActivate = async () => appendRow(fixture.ledger, "migration-that-must-roll-back");
      failed = await rejection(() => fixture.manager().update({ operationId: "b1r-failed", artifact: fixture.artifact("1.0.1") }));
      wrote = await writer.write("writer-after-update");
    } finally {
      fixture.service.failReadiness = false;
      fixture.service.onActivate = null;
      await writer.close();
    }
    record(CASES.b1[1],
      failed !== null && wrote === "wrote" && countLabel(fixture.ledger, "writer-after-update") === 1 &&
        countLabel(fixture.ledger, "writer-before-snapshot") === 1 && integrityOf(fixture.ledger) === "ok",
      { error: failed?.message, wrote, receipt: fixture.receipt("b1r-failed") });
  });
  // A writer that attaches after the quiesced snapshot (the new runtime
  // started, or any plimsoll command) is still attached when rollback runs.
  await runCase([CASES.b1[2], CASES.b1[3]], async (record) => {
    const fixture = createHome("b1-late-writer");
    await fixture.manager().update({ operationId: "b1l-initial", artifact: fixture.artifact("1.0.0") });
    const before = rowsDigest(fixture.ledger);
    let writer: Writer | null = null;
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => { writer = await openWriter(fixture.ledger, "late-writer-first-row"); };
    try {
      const failed = await rejection(() => fixture.manager().update({ operationId: "b1l-failed", artifact: fixture.artifact("1.0.1") }));
      fixture.service.onActivate = null;
      const attached = writer as Writer | null;
      const wrote = attached ? await attached.write("late-writer-after-rollback-attempt") : "no-writer";
      const receipt = fixture.receipt("b1l-failed");
      const journal = exists(path.join(fixture.lifecycleRoot, "journal.json"))
        ? JSON.parse(fs.readFileSync(path.join(fixture.lifecycleRoot, "journal.json"), "utf8")) as { phase?: string }
        : null;
      const refusal = (receipt as { restoreRefusal?: { reason?: unknown } } | null)?.restoreRefusal;
      record(CASES.b1[2],
        failed !== null && wrote === "wrote" && countLabel(fixture.ledger, "late-writer-after-rollback-attempt") === 1 &&
          receipt?.status === "rollback_required" && refusal?.reason === "ledger_in_use" &&
          journal?.phase === "rollback_required" && integrityOf(fixture.ledger) === "ok",
        { error: failed?.message, wrote, receipt, journal });
      await attached?.close();
      writer = null;
      fixture.service.failReadiness = false;
      const retried = await rejection(() => fixture.manager().update({ operationId: "b1l-failed", artifact: fixture.artifact("1.0.1") }));
      const final = fixture.receipt("b1l-failed");
      record(CASES.b1[3],
        receipt?.status === "rollback_required" && retried === null && final?.status === "rolled_back" &&
          rowsDigest(fixture.ledger) === before && integrityOf(fixture.ledger) === "ok" &&
          !exists(path.join(fixture.lifecycleRoot, "journal.json")) && fixture.service.version === "1.0.0",
        { retried: retried?.message, final });
    } finally {
      fixture.service.onActivate = null;
      fixture.service.failReadiness = false;
      await (writer as Writer | null)?.close();
    }
  });
  // The first update of a home without a ledger snapshots "no ledger"; its
  // rollback removes the ledger the new runtime created, but never while open.
  await runCase([CASES.b1[4]], async (record) => {
    const fixture = createHome("b1-no-ledger", { ledger: false });
    let writer: Writer | null = null;
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => {
      createLedger(fixture.ledger, "created-by-new-runtime");
      writer = await openWriter(fixture.ledger, "new-runtime-first-row");
    };
    try {
      const failed = await rejection(() => fixture.manager().update({ operationId: "b1n-failed", artifact: fixture.artifact("1.0.0") }));
      const attached = writer as Writer | null;
      const wrote = attached ? await attached.write("new-runtime-after-rollback-attempt") : "no-writer";
      const receipt = fixture.receipt("b1n-failed");
      record(CASES.b1[4],
        failed !== null && wrote === "wrote" && exists(fixture.ledger) &&
          countLabel(fixture.ledger, "new-runtime-after-rollback-attempt") === 1 && receipt?.status === "rollback_required",
        { error: failed?.message, wrote, receipt, ledgerExists: exists(fixture.ledger) });
    } finally {
      fixture.service.onActivate = null;
      fixture.service.failReadiness = false;
      await (writer as Writer | null)?.close();
    }
  });
}

// ---- B2: restore is atomic and capacity-checked --------------------------

/** A snapshot file and a live ledger with different content, side by side. */
function restorePair(name: string) {
  const directory = path.join(ROOT, name);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const snapshot = path.join(directory, "snapshot.sqlite");
  const live = path.join(directory, "ledger.sqlite");
  createLedger(snapshot, "snapshot");
  createLedger(live, "live");
  return { directory, snapshot, live, liveDigest: rowsDigest(live), snapshotDigest: rowsDigest(snapshot) };
}

/** The async byte copy a restore uses, replaceable by a proof case. */
const asyncFs = fs.promises as typeof fs.promises & { copyFile: (...args: unknown[]) => Promise<void> };

/** Makes every byte copy of `source` (sync or async) fail with ENOSPC while `action` runs. */
async function withCopyFailure<T>(source: string, action: () => Promise<T>) {
  const mutableFs = fs as typeof fs & { copyFileSync: (...args: unknown[]) => unknown };
  const originalCopy = mutableFs.copyFileSync;
  const originalAsyncCopy = asyncFs.copyFile;
  let failures = 0;
  const failure = () => {
    failures += 1;
    const error = new Error("proof: no space left on device") as NodeJS.ErrnoException;
    error.code = "ENOSPC";
    return error;
  };
  mutableFs.copyFileSync = (...args: unknown[]) => {
    if (String(args[0]) === source) throw failure();
    return originalCopy.apply(fs, args as Parameters<typeof fs.copyFileSync>);
  };
  asyncFs.copyFile = (...args: unknown[]) => String(args[0]) === source
    ? Promise.reject(failure())
    : originalAsyncCopy.apply(fs.promises, args as Parameters<typeof fs.promises.copyFile>);
  try {
    return { result: await rejection(action as () => Promise<unknown>), failures };
  } finally {
    mutableFs.copyFileSync = originalCopy;
    asyncFs.copyFile = originalAsyncCopy;
  }
}

/**
 * Only the two ledgers are left: no restore temporary or its sidecars. (The
 * proof's own read-only inspections leave ordinary -wal/-shm files.)
 */
function onlyLedgers(directory: string) {
  const names = listDirectory(directory).map((name) => name.replace(/-(wal|shm)$/, ""));
  return same([...new Set(names)], ["ledger.sqlite", "snapshot.sqlite"]);
}

async function b2RestoreIsAtomicAndCapacityChecked() {
  await runCase([CASES.b2[0]], async (record) => {
    const pair = restorePair("b2-no-room");
    const { result, failures } = await withCopyFailure(pair.snapshot, () =>
      new SqliteLedgerSnapshotAdapter({ clone: () => false, freeBytes: () => 0 }).restore({ source: pair.snapshot, destination: pair.live }));
    // Refused by the space check itself: no byte copy is even attempted.
    record(CASES.b2[0],
      result?.code === "LIFECYCLE_RESTORE_REFUSED" && failures === 0 && exists(pair.live) &&
        rowsDigest(pair.live) === pair.liveDigest && integrityOf(pair.live) === "ok" &&
        rowsDigest(pair.snapshot) === pair.snapshotDigest && onlyLedgers(pair.directory),
      { error: result?.message, code: result?.code, failures, liveExists: exists(pair.live), files: listDirectory(pair.directory) });
  });
  await runCase([CASES.b2[1]], async (record) => {
    const pair = restorePair("b2-copy-fails");
    const { result, failures } = await withCopyFailure(pair.snapshot, () =>
      new SqliteLedgerSnapshotAdapter({ clone: () => false, freeBytes: () => Number.MAX_SAFE_INTEGER })
        .restore({ source: pair.snapshot, destination: pair.live }));
    record(CASES.b2[1],
      result?.code === "ENOSPC" && failures === 1 && exists(pair.live) && rowsDigest(pair.live) === pair.liveDigest &&
        integrityOf(pair.live) === "ok" && onlyLedgers(pair.directory),
      { error: result?.message, code: result?.code, failures, liveExists: exists(pair.live), files: listDirectory(pair.directory) });
  });
  await runCase([CASES.b2[2]], async (record) => {
    const pair = restorePair("b2-corrupt-copy");
    // An index whose entries no longer match their rows: the file opens, but
    // integrity_check reports it. Copying it over the live ledger would
    // replace good data with a damaged ledger.
    const corrupt = new Database(pair.snapshot);
    corrupt.exec("create index proof_rows_label on proof_rows(label)");
    const rootPage = (corrupt.prepare("select rootpage from sqlite_master where name = 'proof_rows_label'").get() as { rootpage: number }).rootpage;
    const pageSize = corrupt.pragma("page_size", { simple: true }) as number;
    corrupt.pragma("wal_checkpoint(TRUNCATE)");
    corrupt.close();
    const bytes = fs.readFileSync(pair.snapshot);
    const page = (rootPage - 1) * pageSize;
    for (let offset = page + pageSize - 400; offset < page + pageSize - 16; offset += 1) {
      if (bytes[offset] === 0x73) bytes[offset] = 0x74; // "snapshot-N" keys become "tnapshot-N"
    }
    fs.writeFileSync(pair.snapshot, bytes);
    const damaged = (() => {
      try {
        return integrityOf(pair.snapshot);
      } catch (error) {
        return String((error as Error).message);
      }
    })();
    const error = await rejection(() =>
      new SqliteLedgerSnapshotAdapter({ clone: () => false, freeBytes: () => Number.MAX_SAFE_INTEGER })
        .restore({ source: pair.snapshot, destination: pair.live }));
    record(CASES.b2[2],
      damaged !== "ok" && error !== null && exists(pair.live) && rowsDigest(pair.live) === pair.liveDigest &&
        integrityOf(pair.live) === "ok" && onlyLedgers(pair.directory),
      { damaged, error: error?.message, liveExists: exists(pair.live), files: listDirectory(pair.directory) });
  });
  await runCase([CASES.b2[3]], async (record) => {
    const fixture = createHome("b2-receipt");
    await fixture.manager().update({ operationId: "b2-initial", artifact: fixture.artifact("1.0.0") });
    appendRow(fixture.ledger, "before-failed-update");
    const before = rowsDigest(fixture.ledger);
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => appendRow(fixture.ledger, "migration-that-must-roll-back");
    const failed = await rejection(() => fixture.manager().update({ operationId: "b2-failed", artifact: fixture.artifact("1.0.1") }));
    fixture.service.failReadiness = false;
    fixture.service.onActivate = null;
    const receipt = fixture.receipt("b2-failed") as (LifecycleReceipt & { restore?: { method?: unknown; cloneFallback?: unknown } }) | null;
    record(CASES.b2[3],
      /readiness failed/.test(failed?.message ?? "") && receipt?.status === "rolled_back" &&
        receipt.restore?.method === "clone" && receipt.restore.cloneFallback === null &&
        rowsDigest(fixture.ledger) === before && integrityOf(fixture.ledger) === "ok",
      { error: failed?.message, receipt });
  });
}

// ---- B3: completion receipts are validated in full -----------------------

/** The same adapter with automatic retention switched off (so every snapshot stays). */
function withoutRetention(adapter: LifecycleAdapter): LifecycleAdapter {
  return new Proxy(adapter, {
    get(target, property) {
      if (property === "retainSnapshots") return undefined;
      const value = target[property as keyof LifecycleAdapter];
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

/** Sequential real updates with retention off: every snapshot and marker is kept. */
async function updatesWithoutRetention(fixture: Home, operations: ReadonlyArray<readonly [string, string]>) {
  for (const [operationId, version] of operations) {
    appendRow(fixture.ledger, `before-${operationId}`);
    await new LifecycleManager(withoutRetention(fixture.adapter())).update({ operationId, artifact: fixture.artifact(version) });
  }
}

const markerPath = (fixture: Home, operationId: string) =>
  path.join(fixture.lifecycleRoot, "completed-operations", `${operationId}.json`);
const readMarker = (fixture: Home, operationId: string) =>
  JSON.parse(fs.readFileSync(markerPath(fixture, operationId), "utf8")) as Record<string, unknown>;
const writeMarker = (fixture: Home, operationId: string, value: unknown) =>
  fs.writeFileSync(markerPath(fixture, operationId), typeof value === "string" ? value : `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
/** The target lists every release from 0.7.0 to 0.7.39 wrote, before status_summary became purge-only. */
const TARGETS_UP_TO_0_7_39 = {
  retainedTargets: ["collector_config", "workspace_credentials", "ledger", "history", "lifecycle_snapshots",
    "workspace_membership"],
  purgeOnlyTargets: ["collector_config", "workspace_credentials", "ledger", "history", "lifecycle_snapshots"],
};

async function b3StrictCompletionReceipts() {
  // The review's fixture: hand-made snapshots whose markers are a bare
  // two-field object, missing, malformed, or a partial receipt.
  await runCase([CASES.b3[0]], async (record) => {
    const fixture = createHome("b3-review-markers");
    const snapshotsRoot = path.join(fixture.lifecycleRoot, "snapshots");
    const completedRoot = path.join(fixture.lifecycleRoot, "completed-operations");
    fs.mkdirSync(snapshotsRoot, { recursive: true, mode: 0o700 });
    fs.mkdirSync(completedRoot, { recursive: true, mode: 0o700 });
    for (const [id, createdAt] of [["semantic-corrupt", "2026-01-01"], ["missing-marker", "2026-01-02"],
      ["malformed-marker", "2026-01-03"], ["good-new", "2026-01-04"]] as const) {
      fs.mkdirSync(path.join(snapshotsRoot, id), { mode: 0o700 });
      fs.writeFileSync(path.join(snapshotsRoot, id, "snapshot.json"), `${JSON.stringify({
        schemaVersion: 1, currentVersion: null, currentExecutable: null,
        present: { config: false, database: false, service: false }, createdAt: `${createdAt}T00:00:00.000Z`,
      })}\n`, { mode: 0o600 });
    }
    fs.writeFileSync(path.join(completedRoot, "semantic-corrupt.json"), '{"operation":"update","status":"completed"}\n', { mode: 0o600 });
    fs.writeFileSync(path.join(completedRoot, "malformed-marker.json"), "{\n", { mode: 0o600 });
    fs.writeFileSync(path.join(completedRoot, "good-new.json"), `${JSON.stringify({
      schemaVersion: 1, operationId: "good-new", operation: "update", status: "completed", fromVersion: null, toVersion: "1.0.0",
    })}\n`, { mode: 0o600 });
    fs.utimesSync(path.join(completedRoot, "semantic-corrupt.json"), new Date(1_000), new Date(1_000));
    fs.utimesSync(path.join(completedRoot, "good-new.json"), new Date(2_000), new Date(2_000));
    const preview = await fixture.manager().listSnapshots({ keep: 1 });
    const decision = preview.snapshots.find((row) => row.id === "semantic-corrupt");
    const applied = await fixture.manager().pruneSnapshots({ operationId: "b3-review-prune", keep: 1, apply: true });
    record(CASES.b3[0],
      decision?.retention === "keep" && decision.operationState === "unknown" &&
        !applied.retention.removed.some((item) => item.name === "semantic-corrupt") &&
        same(fixture.snapshots(), ["good-new", "malformed-marker", "missing-marker", "semantic-corrupt"]),
      { decision, removed: applied.retention.removed, remains: fixture.snapshots() });
  });
  // Real completed updates whose markers were then damaged: each damaged
  // receipt must make its operation unknown, so its snapshot is kept.
  await runCase([CASES.b3[1]], async (record) => {
    const fixture = createHome("b3-damaged-markers");
    const operations = [["m1", "1.0.1"], ["m2", "1.0.2"], ["m3", "1.0.3"], ["m4", "1.0.4"], ["m5", "1.0.5"],
      ["m6", "1.0.6"], ["m7", "1.0.7"]] as const;
    await updatesWithoutRetention(fixture, operations);
    writeMarker(fixture, "m1", '{"operation":"update","status":"completed"}\n');
    writeMarker(fixture, "m2", { ...readMarker(fixture, "m2"), note: "extra field" });
    writeMarker(fixture, "m3", { ...readMarker(fixture, "m3"), operationId: "m9" });
    const { preserved: _dropped, ...missing } = readMarker(fixture, "m4");
    writeMarker(fixture, "m4", missing);
    writeMarker(fixture, "m5", { ...readMarker(fixture, "m5"), restoredVersion: "1.0.4" });
    // Markers keep their true completion order on disk (m1 oldest, m7 newest).
    operations.forEach(([id], index) =>
      fs.utimesSync(markerPath(fixture, id), new Date(10_000 + index * 1_000), new Date(10_000 + index * 1_000)));
    const preview = await fixture.manager().listSnapshots({ keep: 1 });
    const applied = await fixture.manager().pruneSnapshots({ operationId: "b3-damaged-prune", keep: 1, apply: true });
    const decisions = Object.fromEntries(preview.snapshots.map((row) => [row.id, `${row.retention}:${row.reason}`]));
    record(CASES.b3[1],
      ["m1", "m2", "m3", "m4", "m5"].every((id) => decisions[id] === "keep:operation_unknown") &&
        decisions.m7 === "keep:newest_completed" && decisions.m6 === "prune:older_completed" &&
        same(applied.retention.removed.filter((item) => item.kind === "snapshot").map((item) => item.name), ["m6"]) &&
        same(fixture.snapshots(), ["m1", "m2", "m3", "m4", "m5", "m7"]),
      { decisions, removed: applied.retention.removed, remains: fixture.snapshots() });
  });
  // Receipts through 0.7.39 (sequenced, without the status_summary
  // target) still prove their operation. A receipt mixing the old and new
  // target lists is not one any release wrote, so its operation is unknown.
  await runCase([CASES.b3[2]], async (record) => {
    const fixture = createHome("b3-target-lists");
    await updatesWithoutRetention(fixture, [["u1", "1.2.1"], ["u2", "1.2.2"], ["u3", "1.2.3"], ["u4", "1.2.4"], ["u5", "1.2.5"]]);
    for (const id of ["u1", "u4", "u5"]) writeMarker(fixture, id, { ...readMarker(fixture, id), ...TARGETS_UP_TO_0_7_39 });
    writeMarker(fixture, "u2", { ...readMarker(fixture, "u2"), purgeOnlyTargets: TARGETS_UP_TO_0_7_39.purgeOnlyTargets });
    writeMarker(fixture, "u3", { ...readMarker(fixture, "u3"), retainedTargets: TARGETS_UP_TO_0_7_39.retainedTargets });
    const result = await pruneDecisions(fixture, "b3-target-lists-prune", 2);
    record(CASES.b3[2],
      result.decisions.u5 === "keep:newest_completed" && result.decisions.u4 === "keep:newest_completed" &&
        result.decisions.u3 === "keep:operation_unknown" && result.decisions.u2 === "keep:operation_unknown" &&
        result.decisions.u1 === "prune:older_completed" &&
        same(result.removed, ["u1"]) && same(result.remains, ["u2", "u3", "u4", "u5"]),
      result);
  });
  await runCase([CASES.b3[3]], async (record) => {
    const fixture = createHome("b3-release-0739");
    await updatesWithoutRetention(fixture, [
      ["u1", "1.3.1"], ["u2", "1.3.2"], ["u3", "1.3.3"], ["u4", "1.3.4"],
    ]);
    writeMarker(fixture, "u2", {
      ...readMarker(fixture, "u2"),
      toolVersion: "0.7.39",
      ...TARGETS_UP_TO_0_7_39,
    });
    const result = await pruneDecisions(fixture, "b3-0739-prune", 1);
    record(CASES.b3[3],
      result.decisions.u2 === "prune:older_completed" &&
        result.removed.includes("u2") && !result.remains.includes("u2"),
      result);
  });
}

// ---- B4: completion order survives clock steps ----------------------------

/** Marker file times after a backward clock step: [oldest..newest] completions get these seconds. */
function stepClockBackward(fixture: Home, ids: readonly string[], seconds: readonly number[]) {
  ids.forEach((id, index) => {
    const at = new Date(seconds[index]! * 1_000);
    fs.utimesSync(markerPath(fixture, id), at, at);
  });
}

/** Rewrites markers as 0.7.37 and earlier wrote them: the 13 receipt fields and their target lists, no order record. */
function asPreSequencingMarkers(fixture: Home, ids: readonly string[]) {
  const legacyKeys = ["schemaVersion", "toolVersion", "operationId", "operation", "status", "fromVersion", "toVersion",
    "restoredVersion", "health", "ownedTargets", "retainedTargets", "purgeOnlyTargets", "preserved"];
  for (const id of ids) {
    const marker = readMarker(fixture, id);
    writeMarker(fixture, id, { ...Object.fromEntries(legacyKeys.map((key) => [key, marker[key]])), ...TARGETS_UP_TO_0_7_39 });
  }
  fs.rmSync(path.join(fixture.lifecycleRoot, "completion-order.json"), { force: true });
}

async function pruneDecisions(fixture: Home, operationId: string, keep: number) {
  const preview = await fixture.manager().listSnapshots({ keep });
  const applied = await fixture.manager().pruneSnapshots({ operationId, keep, apply: true });
  return {
    decisions: Object.fromEntries(preview.snapshots.map((row) => [row.id, `${row.retention}:${row.reason}`])),
    removed: applied.retention.removed.filter((item) => item.kind === "snapshot").map((item) => item.name),
    remains: fixture.snapshots(),
  };
}

async function b4OrderSurvivesClockSteps() {
  await runCase([CASES.b4[0], CASES.b4[1]], async (record) => {
    const fixture = createHome("b4-clock-step");
    const ids = ["k1", "k2", "k3", "k4"];
    await updatesWithoutRetention(fixture, ids.map((id, index) => [id, `1.1.${index}`] as const));
    const sequences = ids.map((id) => readMarker(fixture, id).completionSequence);
    const order = exists(path.join(fixture.lifecycleRoot, "completion-order.json"))
      ? JSON.parse(fs.readFileSync(path.join(fixture.lifecycleRoot, "completion-order.json"), "utf8")) as { lastSequence?: unknown }
      : null;
    record(CASES.b4[0],
      JSON.stringify(sequences) === JSON.stringify([1, 2, 3, 4]) && order?.lastSequence === 4 &&
        fixture.receipt("k4")?.completionSequence === 4,
      { sequences, order });
    // The clock stepped back between completions: k1 and k2 look newest by file time.
    stepClockBackward(fixture, ids, [400, 300, 100, 200]);
    const result = await pruneDecisions(fixture, "b4-clock-prune", 2);
    record(CASES.b4[1],
      result.decisions.k4 === "keep:newest_completed" && result.decisions.k3 === "keep:newest_completed" &&
        same(result.removed, ["k1", "k2"]) && same(result.remains, ["k3", "k4"]),
      result);
  });
  await runCase([CASES.b4[2]], async (record) => {
    const fixture = createHome("b4-legacy-chain");
    const ids = ["l1", "l2", "l3", "l4"];
    await updatesWithoutRetention(fixture, ids.map((id, index) => [id, `2.0.${index + 1}`] as const));
    asPreSequencingMarkers(fixture, ids);
    stepClockBackward(fixture, ids, [400, 300, 100, 200]);
    const result = await pruneDecisions(fixture, "b4-legacy-prune", 2);
    record(CASES.b4[2],
      result.decisions.l4 === "keep:newest_completed" && result.decisions.l3 === "keep:newest_completed" &&
        same(result.removed, ["l1", "l2"]) && same(result.remains, ["l3", "l4"]),
      result);
  });
  await runCase([CASES.b4[3]], async (record) => {
    const fixture = createHome("b4-legacy-ambiguous");
    const ids = ["r1", "r2", "r3", "r4"];
    // A fresh install and three same-version re-pins: no version chain can order them.
    await updatesWithoutRetention(fixture, ids.map((id) => [id, "3.0.0"] as const));
    asPreSequencingMarkers(fixture, ids);
    stepClockBackward(fixture, ids, [400, 300, 100, 200]);
    const result = await pruneDecisions(fixture, "b4-ambiguous-prune", 2);
    record(CASES.b4[3],
      result.removed.length === 0 && same(result.remains, ids) &&
        ids.every((id) => result.decisions[id] === "keep:completion_order_unproven"),
      result);
  });
  await runCase([CASES.b4[4]], async (record) => {
    const fixture = createHome("b4-duplicate-sequence");
    const ids = ["d1", "d2", "d3"];
    await updatesWithoutRetention(fixture, ids.map((id, index) => [id, `4.0.${index}`] as const));
    writeMarker(fixture, "d2", { ...readMarker(fixture, "d2"), completionSequence: readMarker(fixture, "d3").completionSequence ?? 3 });
    stepClockBackward(fixture, ids, [100, 200, 300]);
    const result = await pruneDecisions(fixture, "b4-duplicate-prune", 2);
    record(CASES.b4[4],
      result.removed.length === 0 && same(result.remains, ids) &&
        ids.every((id) => result.decisions[id] === "keep:completion_order_unproven"),
      result);
  });
}

// ---- B5: every removal is durably recorded --------------------------------

/** Durable records (outside receipts) naming a removed snapshot, written before any receipt. */
function removalRecordsNaming(fixture: Home, snapshotId: string) {
  const directory = path.join(fixture.lifecycleRoot, "removals");
  return listDirectory(directory).filter((name) => {
    const text = fs.readFileSync(path.join(directory, name), "utf8");
    return (JSON.parse(text) as { items?: Array<{ kind?: string; name?: string }> }).items
      ?.some((item) => item.kind === "snapshot" && item.name === snapshotId) === true;
  });
}

/** Every receipt and completion marker that reports `snapshotId` as removed or recovered. */
function receiptsNaming(fixture: Home, snapshotId: string) {
  return ["receipts", "completed-operations"].flatMap((directory) =>
    listDirectory(path.join(fixture.lifecycleRoot, directory)).filter((name) => {
      const receipt = JSON.parse(fs.readFileSync(path.join(fixture.lifecycleRoot, directory, name), "utf8")) as LifecycleReceipt;
      return [...receipt.retention?.removed ?? [], ...receipt.retention?.recovered ?? []]
        .some((item) => item.kind === "snapshot" && item.name === snapshotId);
    }).map((name) => `${directory}/${name}`));
}

async function b5RemovalsAreDurablyRecorded() {
  // The review's fixture: the process is lost right after the trash entry of
  // a pruned snapshot is unlinked, before any receipt is written.
  await runCase([CASES.b5[0], CASES.b5[1]], async (record) => {
    const fixture = createHome("b5-crash");
    await fixture.manager().update({ operationId: "a1", artifact: fixture.artifact("1.0.0") });
    await fixture.manager().update({ operationId: "a2", artifact: fixture.artifact("1.0.1") });
    const mutableFs = fs as typeof fs & { rmSync: (...args: unknown[]) => unknown };
    const originalRm = mutableFs.rmSync;
    let lostAfterUnlink = false;
    mutableFs.rmSync = (...args: unknown[]) => {
      const result = originalRm.apply(fs, args as Parameters<typeof fs.rmSync>);
      if (!lostAfterUnlink && String(args[0]).includes(`${path.sep}trash${path.sep}snapshot+a1+`)) {
        lostAfterUnlink = true;
        throw new Error("proof: process lost after unlink and before any receipt");
      }
      return result;
    };
    let crash: (Error & { code?: string }) | null;
    try {
      crash = await rejection(() => fixture.manager().pruneSnapshots({ operationId: "b5-crash-prune", keep: 1, apply: true }));
    } finally {
      mutableFs.rmSync = originalRm;
    }
    const records = removalRecordsNaming(fixture, "a1");
    record(CASES.b5[0],
      lostAfterUnlink && crash?.message === "retention trash delete failed" && !fixture.snapshots().includes("a1") &&
        fixture.receipt("b5-crash-prune", "snapshots_prune") === null && records.length === 1,
      { error: crash?.message, snapshots: fixture.snapshots(), records });
    const recovered = await fixture.manager().pruneSnapshots({ operationId: "b5-recover-prune", keep: 1, apply: true });
    record(CASES.b5[1],
      recovered.receipt?.status === "completed" &&
        recovered.retention.recovered.some((item) => item.kind === "snapshot" && item.name === "a1") &&
        same(receiptsNaming(fixture, "a1"), ["completed-operations/b5-recover-prune.json", "receipts/b5-recover-prune-snapshots_prune.json"]) &&
        listDirectory(path.join(fixture.lifecycleRoot, "removals")).length === 0 &&
        listDirectory(path.join(fixture.lifecycleRoot, "trash")).length === 0,
      { retention: recovered.retention, naming: receiptsNaming(fixture, "a1"),
        removals: listDirectory(path.join(fixture.lifecycleRoot, "removals")) });
  });
  // Automatic retention after an update deletes, then its receipt addendum
  // cannot be written: the deletion must still reach a durable receipt.
  await runCase([CASES.b5[2]], async (record) => {
    const fixture = createHome("b5-receipt-failure");
    await fixture.manager().update({ operationId: "w1", artifact: fixture.artifact("5.0.0") });
    await fixture.manager().update({ operationId: "w2", artifact: fixture.artifact("5.0.1") });
    const failingReceipts: LifecycleAdapter = new Proxy(fixture.adapter(), {
      get(target, property) {
        if (property === "persistReceipt") {
          return async (receipt: LifecycleReceipt) => {
            if (receipt.retention) throw new Error("proof: receipt write failed after retention");
            return target.persistReceipt(receipt);
          };
        }
        const value = target[property as keyof LifecycleAdapter];
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    const w3 = await new LifecycleManager(failingReceipts).update({ operationId: "w3", artifact: fixture.artifact("5.0.2") });
    const deleted = w3.retention?.removed.some((item) => item.kind === "snapshot" && item.name === "w1") === true &&
      !fixture.snapshots().includes("w1");
    const unrecordedBefore = receiptsNaming(fixture, "w1").length === 0;
    const next = await fixture.manager().pruneSnapshots({ operationId: "b5-after-failed-receipt", keep: 2, apply: true });
    record(CASES.b5[2],
      deleted && unrecordedBefore && next.receipt?.status === "completed" &&
        next.retention.recovered.some((item) => item.kind === "snapshot" && item.name === "w1") &&
        receiptsNaming(fixture, "w1").length > 0 && listDirectory(path.join(fixture.lifecycleRoot, "removals")).length === 0,
      { w3: w3.retention, next: next.retention, naming: receiptsNaming(fixture, "w1") });
  });
  // Entries the lifecycle never names this way are not its to delete, and must
  // not make a removal record unreadable.
  await runCase([CASES.b5[3]], async (record) => {
    const fixture = createHome("b5-foreign-trash");
    for (const [id, version] of [["f1", "6.0.0"], ["f2", "6.0.1"]] as const) {
      await fixture.manager().update({ operationId: id, artifact: fixture.artifact(version) });
    }
    const trash = path.join(fixture.lifecycleRoot, "trash");
    fs.mkdirSync(path.join(trash, "snapshot+odd+zz"), { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(trash, "notes.txt"), "not lifecycle trash\n", { mode: 0o600 });
    const first = await fixture.manager().pruneSnapshots({ operationId: "b5-foreign-1", keep: 1, apply: true });
    const second = await fixture.manager().pruneSnapshots({ operationId: "b5-foreign-2", keep: 1, apply: true });
    record(CASES.b5[3],
      first.retention.status === "applied" && second.retention.status === "applied" &&
        same(first.retention.removed.map((item) => `${item.kind}:${item.name}`), ["snapshot:f1"]) &&
        same(listDirectory(trash), ["notes.txt", "snapshot+odd+zz"]) &&
        listDirectory(path.join(fixture.lifecycleRoot, "removals")).length === 0,
      { first: first.retention, second: second.retention, trash: listDirectory(trash) });
  });
}

// ---- Should-fix: read-only preflight; the clone helper outside a terminal --

/** Relative path, type, mode, size, mtime and small-file content of a tree. */
function treeDigest(root: string) {
  const rows: string[] = [];
  const walk = (directory: string) => {
    for (const name of fs.readdirSync(directory).sort()) {
      const absolute = path.join(directory, name);
      const stat = fs.lstatSync(absolute, { bigint: true });
      const relative = path.relative(root, absolute);
      if (stat.isDirectory()) {
        rows.push(`d ${relative} ${stat.mode} ${stat.mtimeNs}`);
        walk(absolute);
      } else {
        const content = stat.isFile() && stat.size < 1024n * 1024n ? sha256(fs.readFileSync(absolute)) : `ino:${stat.ino}`;
        rows.push(`f ${relative} ${stat.mode} ${stat.size} ${stat.mtimeNs} ${content}`);
      }
    }
  };
  walk(root);
  return sha256(rows.join("\n"));
}

async function preflightAndCloneHelper() {
  await runCase([CASES.preflight[0]], async (record) => {
    const fixture = createHome("preflight-read-only");
    const before = treeDigest(fixture.home);
    const plan = await fixture.manager().preflightUpdate();
    record(CASES.preflight[0],
      treeDigest(fixture.home) === before && !exists(fixture.lifecycleRoot) && plan.method === "clone" && plan.ok &&
        plan.requiredFreeBytes === 0,
      { plan, lifecycleRootCreated: exists(fixture.lifecycleRoot) });
  });
  // A LaunchAgent or a non-interactive SSH command runs the helper with no
  // controlling terminal, in its own session, with a minimal environment.
  await runCase([CASES.preflight[1]], async (record) => {
    const directory = path.join(ROOT, "detached-helper");
    fs.mkdirSync(directory, { mode: 0o700 });
    const source = path.join(directory, "source.bin");
    const destination = path.join(directory, "clone.bin");
    fs.writeFileSync(source, randomBytes(1024 * 1024));
    const probe = path.join(directory, "probe.mts");
    const adapters = path.resolve(import.meta.dirname, "../packages/collector-cli/src/lifecycle-adapters.ts");
    fs.writeFileSync(probe, `import { cloneFileOrFail, volumeSupportsClone } from ${JSON.stringify(adapters)};
const [source, destination] = process.argv.slice(2);
process.stdout.write(JSON.stringify({
  terminal: Boolean(process.stdin.isTTY || process.stdout.isTTY),
  supported: volumeSupportsClone(source, destination),
      cloned: (() => {
        const evidence = cloneFileOrFail(source, destination);
        return evidence ? evidence.method : false;
      })(),
}));
`);
    const child = spawn(process.execPath, ["--import", path.resolve(import.meta.dirname, "../node_modules/tsx/dist/loader.mjs"),
      probe, source, destination], {
      detached: true,
      env: { PATH: "/usr/bin:/bin", TMPDIR: process.env.TMPDIR ?? directory },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += String(chunk); });
    child.stderr.on("data", (chunk) => { stderr += String(chunk); });
    const code = await new Promise<number | null>((resolve) => child.once("exit", resolve));
    const result = (() => {
      try {
        return JSON.parse(stdout) as { terminal?: boolean; supported?: boolean; cloned?: string | false };
      } catch {
        return null;
      }
    })();
    record(CASES.preflight[1],
      code === 0 && result?.terminal === false && result.supported === true && result.cloned === "clonefile" &&
        exists(destination) && fs.readFileSync(destination).equals(fs.readFileSync(source)),
      { code, result, stderr: stderr.slice(-400) });
  });
}

// ---- r3: a long rollback keeps its lease, and a lost lease never swaps -----

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
/** Blocks this thread (no timer can run): a stalled or suspended process. */
const stall = (ms: number) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
const readJournal = (fixture: Home) => {
  const file = path.join(fixture.lifecycleRoot, "journal.json");
  return exists(file) ? JSON.parse(fs.readFileSync(file, "utf8")) as { phase?: string; restore?: unknown } : null;
};

async function r3LeaseAndFence() {
  // The rollback's integrity check outlasts the lease (scaled down: 1.5 s
  // lease, 2.5 s check). The lease is renewed while the check runs, so a
  // second operation cannot take over mid-restore.
  await runCase([CASES.r3Lease[0]], async (record) => {
    const fixture = createHome("r3-lease-renewal");
    const quiet = new SqliteLedgerSnapshotAdapter({ openHandles: () => [] });
    await fixture.manager(quiet).update({ operationId: "lr-initial", artifact: fixture.artifact("1.0.0") });
    const before = rowsDigest(fixture.ledger);
    let integrityStarted = false;
    const slowCheck = new SqliteLedgerSnapshotAdapter({
      openHandles: () => [],
      integrityCheck: (file, guard) => {
        integrityStarted = true;
        return whileKeepingLease(guard, sleep(7_000).then(() => integrityCheckOffThread(file, guard)));
      },
    });
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => appendRow(fixture.ledger, "migration-that-must-roll-back");
    // The check deliberately outlasts the five-second lease. Waiting for its
    // start avoids making host scheduling before the guarded step part of the
    // assertion, which made the old two-second sleep flaky under load.
    const failing = rejection(() => new LifecycleManager(fixture.adapter(slowCheck, { authorityLeaseMs: 5_000 }))
      .update({ operationId: "lr-failed", artifact: fixture.artifact("1.0.1") }));
    const startDeadline = Date.now() + 10_000;
    while (!integrityStarted && Date.now() < startDeadline) await sleep(25);
    const concurrent = await rejection(() => new LifecycleManager(fixture.adapter(undefined, { authorityLeaseMs: 5_000 }))
      .pruneSnapshots({ operationId: "lr-concurrent-prune", keep: 1, apply: true }));
    const first = await failing;
    fixture.service.failReadiness = false;
    fixture.service.onActivate = null;
    const receipt = fixture.receipt("lr-failed") as (LifecycleReceipt & { restore?: { method?: string } }) | null;
    const pruneRefused = /another lifecycle operation owns the lock|lifecycle recovery is required before prune/.test(concurrent?.message ?? "") &&
      fixture.receipt("lr-concurrent-prune", "snapshots_prune") === null;
    const rollbackCompleted = /readiness failed/.test(first?.message ?? "") && receipt?.status === "rolled_back" &&
      receipt.restore?.method === "clone" && rowsDigest(fixture.ledger) === before;
    record(CASES.r3Lease[0], integrityStarted && pruneRefused && rollbackCompleted,
      { concurrent: concurrent?.message ?? "prune ran", first: first?.message, receipt, journal: readJournal(fixture) });
  });
  // The same when no clone is possible: the restore's byte copy outlasts the
  // lease (1.5 s lease, 2.5 s copy) and the lease is renewed while it runs.
  await runCase([CASES.r3Lease[3]], async (record) => {
    const fixture = createHome("r3-lease-copy");
    const quiet = new SqliteLedgerSnapshotAdapter({ openHandles: () => [] });
    await fixture.manager(quiet).update({ operationId: "lc-initial", artifact: fixture.artifact("1.0.0") });
    const before = rowsDigest(fixture.ledger);
    const noClone = new SqliteLedgerSnapshotAdapter({ clone: () => false, freeBytes: () => Number.MAX_SAFE_INTEGER, openHandles: () => [] });
    const originalAsyncCopy = asyncFs.copyFile;
    let slowCopies = 0;
    asyncFs.copyFile = async (...args: unknown[]) => {
      if (/\.restore-[0-9a-f]{12}$/.test(String(args[1]))) {
        slowCopies += 1;
        await sleep(2_500);
      }
      return originalAsyncCopy.apply(fs.promises, args as Parameters<typeof fs.promises.copyFile>);
    };
    let concurrent: Awaited<ReturnType<typeof rejection>> = null;
    let first: Awaited<ReturnType<typeof rejection>> = null;
    try {
      fixture.service.failReadiness = true;
      fixture.service.onActivate = async () => appendRow(fixture.ledger, "migration-that-must-roll-back");
      const failing = rejection(() => new LifecycleManager(fixture.adapter(noClone, { authorityLeaseMs: 1_500 }))
        .update({ operationId: "lc-failed", artifact: fixture.artifact("1.0.1") }));
      await sleep(2_000);
      concurrent = await rejection(() => new LifecycleManager(fixture.adapter(undefined, { authorityLeaseMs: 1_500 }))
        .pruneSnapshots({ operationId: "lc-concurrent-prune", keep: 1, apply: true }));
      first = await failing;
    } finally {
      asyncFs.copyFile = originalAsyncCopy;
      fixture.service.failReadiness = false;
      fixture.service.onActivate = null;
    }
    const receipt = fixture.receipt("lc-failed") as (LifecycleReceipt & { restore?: { method?: string } }) | null;
    const pruneRefused = /another lifecycle operation owns the lock|lifecycle recovery is required before prune/.test(concurrent?.message ?? "") &&
      fixture.receipt("lc-concurrent-prune", "snapshots_prune") === null;
    const rollbackCompleted = /readiness failed/.test(first?.message ?? "") && receipt?.status === "rolled_back" &&
      receipt.restore?.method === "copy" && rowsDigest(fixture.ledger) === before;
    record(CASES.r3Lease[3], slowCopies === 1 && pruneRefused && rollbackCompleted,
      { slowCopies, concurrent: concurrent?.message ?? "prune ran", first: first?.message, receipt, journal: readJournal(fixture) });
  });
  // A step that reports progress often (the online backup calls back every
  // 100 pages: about 170,000 times for a 69 GB ledger) must not rewrite and
  // fsync the lease record each time.
  await runCase([CASES.r3Lease[4]], async (record) => {
    const fixture = createHome("r3-renewal-throttle");
    const real = new SqliteLedgerSnapshotAdapter({ openHandles: () => [] });
    let syncs = -1;
    const chatty: LifecycleDatabaseAdapter = {
      snapshot: async (input) => {
        const mutableFs = fs as typeof fs & { fsyncSync: (descriptor: number) => void };
        const original = mutableFs.fsyncSync;
        let count = 0;
        mutableFs.fsyncSync = (descriptor: number) => {
          count += 1;
          original(descriptor);
        };
        try {
          for (let step = 0; step < 1_000; step += 1) input.guard?.keepAlive();
        } finally {
          mutableFs.fsyncSync = original;
        }
        syncs = count;
        return real.snapshot(input);
      },
      restore: (input) => real.restore(input),
      discard: (input) => real.discard(input),
    };
    const receipt = await new LifecycleManager(fixture.adapter(chatty)).update({ operationId: "rt-update", artifact: fixture.artifact("1.0.0") });
    record(CASES.r3Lease[4], receipt.status === "completed" && syncs >= 1 && syncs <= 2, { syncs, status: receipt.status });
  });
  // A stalled process loses its lease mid-restore and a successor takes it.
  // The superseded rollback must stop without replacing the ledger; the same
  // operation ID finishes the rollback once the successor is done.
  await runCase([CASES.r3Lease[1]], async (record) => {
    const fixture = createHome("r3-superseded");
    const quiet = new SqliteLedgerSnapshotAdapter({ openHandles: () => [] });
    await fixture.manager(quiet).update({ operationId: "sv-initial", artifact: fixture.artifact("1.0.0") });
    const before = rowsDigest(fixture.ledger);
    const real = new SqliteLedgerSnapshotAdapter();
    let successor: LifecycleMutationLease | null = null;
    const stalled: LifecycleDatabaseAdapter = {
      snapshot: (input) => quiet.snapshot(input),
      discard: (input) => real.discard(input),
      restore: async (input) => {
        stall(2_000);
        const taken = new LifecycleMutationAuthority(path.join(fixture.collector, "lifecycle-authority")).acquire({ leaseMs: 60_000 });
        if (taken.kind === "acquired") successor = taken.lease;
        return real.restore(input);
      },
    };
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => appendRow(fixture.ledger, "migration-that-must-roll-back");
    const error = await rejection(() => new LifecycleManager(fixture.adapter(stalled, { authorityLeaseMs: 1_000 }))
      .update({ operationId: "sv-failed", artifact: fixture.artifact("1.0.1") }));
    fixture.service.onActivate = null;
    const swappedWhileSuperseded = countLabel(fixture.ledger, "migration-that-must-roll-back") === 0;
    const journal = readJournal(fixture);
    const receiptAfterFirst = fixture.receipt("sv-failed");
    (successor as LifecycleMutationLease | null)?.release();
    fixture.service.failReadiness = false;
    const retry = await rejection(() => fixture.manager(quiet).update({ operationId: "sv-failed", artifact: fixture.artifact("1.0.1") }));
    const final = fixture.receipt("sv-failed") as (LifecycleReceipt & { restore?: { method?: string } }) | null;
    record(CASES.r3Lease[1],
      successor !== null && error?.code === "LIFECYCLE_INTERRUPTED" && !swappedWhileSuperseded &&
        journal?.phase === "rollback_required" && receiptAfterFirst?.status !== "rolled_back" &&
        retry === null && final?.status === "rolled_back" && final.restore?.method === "clone" &&
        rowsDigest(fixture.ledger) === before && integrityOf(fixture.ledger) === "ok",
      { error: error?.message, code: error?.code, swappedWhileSuperseded, journal, receiptAfterFirst: receiptAfterFirst?.status,
        retry: retry?.message ?? null, final });
  });
  // The restore finishes but the process stops before the receipt: the same
  // operation ID writes it later, still saying how the ledger was restored.
  await runCase([CASES.r3Lease[2]], async (record) => {
    const fixture = createHome("r3-restore-record");
    await fixture.manager().update({ operationId: "rr-initial", artifact: fixture.artifact("1.0.0") });
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => appendRow(fixture.ledger, "migration-that-must-roll-back");
    const lostReceipt: LifecycleAdapter = new Proxy(fixture.adapter(), {
      get(target, property) {
        if (property === "persistReceipt") {
          return async (receipt: LifecycleReceipt) => {
            if (receipt.status === "rolled_back") throw new Error("proof: process stopped before the rollback receipt");
            return target.persistReceipt(receipt);
          };
        }
        const value = target[property as keyof LifecycleAdapter];
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    const first = await rejection(() => new LifecycleManager(lostReceipt).update({ operationId: "rr-failed", artifact: fixture.artifact("1.0.1") }));
    fixture.service.failReadiness = false;
    fixture.service.onActivate = null;
    const journal = readJournal(fixture);
    const retry = await rejection(() => fixture.manager().update({ operationId: "rr-failed", artifact: fixture.artifact("1.0.1") }));
    const final = fixture.receipt("rr-failed") as (LifecycleReceipt & { restore?: { method?: string } }) | null;
    record(CASES.r3Lease[2],
      /process stopped before the rollback receipt/.test(first?.message ?? "") && journal?.phase === "rollback_complete" &&
        retry === null && final?.status === "rolled_back" && final.restore?.method === "clone",
      { first: first?.message, journal, retry: retry?.message ?? null, final });
  });
}

// A rollback whose ledger restore is complete but whose receipt was not
// durable must stop prune. This is separate from rollback_required: that
// state is intentionally allowed to prune so an operator can free space for
// the restore itself.
async function pruneRecoveryGuard() {
  await runCase([CASES.guard[0]], async (record) => {
    const fixture = createHome("r3-prune-rollback-complete");
    await fixture.manager().update({ operationId: "gc-initial", artifact: fixture.artifact("1.0.0") });
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => appendRow(fixture.ledger, "migration-that-must-roll-back");
    const lostReceipt: LifecycleAdapter = new Proxy(fixture.adapter(), {
      get(target, property) {
        if (property === "persistReceipt") {
          return async (receipt: LifecycleReceipt) => {
            if (receipt.status === "rolled_back") throw new Error("proof: process stopped before the rollback receipt");
            return target.persistReceipt(receipt);
          };
        }
        const value = target[property as keyof LifecycleAdapter];
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    const first = await rejection(() => new LifecycleManager(lostReceipt)
      .update({ operationId: "gc-failed", artifact: fixture.artifact("1.0.1") }));
    fixture.service.failReadiness = false;
    fixture.service.onActivate = null;
    const journal = readJournal(fixture);
    const guarded = await rejection(() => fixture.manager().pruneSnapshots({
      operationId: "gc-prune", keep: 1, apply: true,
    }));
    record(CASES.guard[0],
      /process stopped before the rollback receipt/.test(first?.message ?? "") &&
        journal?.phase === "rollback_complete" &&
        guarded?.message === "lifecycle recovery is required before prune" &&
        fixture.receipt("gc-prune", "snapshots_prune") === null,
      { first: first?.message, journal, guarded: guarded?.message ?? "prune ran" });
  });
}

// ---- r8: every prune race is covered by an in-repo safety case ------------

/** The three r8 cases use real update snapshots but isolate each mutation. */
async function r8PruneSafety() {
  await runCase([CASES.r8PruneSafety[0]], async (record) => {
    const fixture = createHome("r8-undo-move");
    await updatesWithoutRetention(fixture, [
      ["u1", "30.1.0"], ["u2", "30.1.1"], ["u3", "30.1.2"], ["u4", "30.1.3"],
    ]);
    const keptRuntime = path.join(fixture.lifecycleRoot, "versions", "30.1.2");
    const trashRoot = path.join(fixture.lifecycleRoot, "trash");
    const mutableFs = fs as typeof fs & { renameSync: (...args: Parameters<typeof fs.renameSync>) => void };
    const originalRename = mutableFs.renameSync;
    let injected = false;
    mutableFs.renameSync = (...args) => {
      const result = originalRename.apply(fs, args);
      if (!injected && String(args[1]).startsWith(`${trashRoot}${path.sep}`)) {
        injected = true;
        fs.rmSync(keptRuntime, { recursive: true, force: true });
      }
      return result;
    };
    let refused: Awaited<ReturnType<typeof rejection>> = null;
    try {
      refused = await rejection(() => fixture.manager().pruneSnapshots({
        operationId: "r8-undo-prune", keep: 1, apply: true,
      }));
    } finally {
      mutableFs.renameSync = originalRename;
    }
    record(CASES.r8PruneSafety[0],
      injected && refused !== null && /retention plan/.test(refused.message) &&
        same(fixture.snapshots(), ["u1", "u2", "u3", "u4"]) &&
        same(fixture.versions(), ["30.1.0", "30.1.1", "30.1.3"]) &&
        fixture.snapshots().includes("u2") && fixture.versions().includes("30.1.1") &&
        listDirectory(trashRoot).length === 0,
      { injected, error: refused?.message, snapshots: fixture.snapshots(), versions: fixture.versions(), trash: listDirectory(trashRoot) });
  });

  await runCase([CASES.r8PruneSafety[1]], async (record) => {
    const fixture = createHome("r8-carried-record");
    await updatesWithoutRetention(fixture, [
      ["c1", "31.1.0"], ["c2", "31.1.1"], ["c3", "31.1.2"], ["c4", "31.1.3"],
    ]);
    const snapshot = path.join(fixture.lifecycleRoot, "snapshots", "c1");
    const removalRoot = path.join(fixture.lifecycleRoot, "removals");
    const removalRecord = path.join(removalRoot, "r8-carried.json");
    const trashName = "snapshot+c1+0123456789ab";
    fs.mkdirSync(removalRoot, { recursive: true, mode: 0o700 });
    const trashRoot = path.join(fixture.lifecycleRoot, "trash");
    fs.mkdirSync(trashRoot, { recursive: true, mode: 0o700 });
    fs.renameSync(snapshot, path.join(trashRoot, trashName));
    fs.writeFileSync(removalRecord, `${JSON.stringify({
      schemaVersion: 1,
      operationId: "r8-carried",
      items: [{ kind: "snapshot", name: "c1", bytes: 0, trashName, origin: "planned" }],
    }, null, 2)}\n`, { mode: 0o600 });
    const outside = path.join(fixture.home, "r8-outside");
    fs.mkdirSync(outside, { recursive: true, mode: 0o700 });
    fs.symlinkSync(outside, path.join(fixture.lifecycleRoot, "versions", "31.1.0", "r8-link"), "dir");
    const refused = await rejection(() => fixture.manager().pruneSnapshots({
      operationId: "r8-carried", keep: 1, apply: true,
    }));
    record(CASES.r8PruneSafety[1],
      refused !== null && /unstampable:? ?runtime_version:31\.1\.0/.test(refused.message) &&
        exists(removalRecord) && same(listDirectory(trashRoot), [trashName]),
      { error: refused?.message, removalRecord: exists(removalRecord), trash: listDirectory(trashRoot) });
  });

  await runCase([CASES.r8PruneSafety[2]], async (record) => {
    const fixture = createHome("r8-unstampable");
    await updatesWithoutRetention(fixture, [
      ["s1", "32.1.0"], ["s2", "32.1.1"], ["s3", "32.1.2"], ["s4", "32.1.3"],
    ]);
    const outside = path.join(fixture.home, "r8-unstampable-outside");
    fs.mkdirSync(outside, { recursive: true, mode: 0o700 });
    fs.symlinkSync(outside, path.join(fixture.lifecycleRoot, "versions", "32.1.0", "r8-link"), "dir");
    const refused = await rejection(() => fixture.manager().pruneSnapshots({
      operationId: "r8-unstampable-prune", keep: 1, apply: true,
    }));
    record(CASES.r8PruneSafety[2],
      refused !== null && /unstampable:? ?runtime_version:32\.1\.0/.test(refused.message) &&
        same(fixture.snapshots(), ["s1", "s2", "s3", "s4"]) &&
        same(fixture.versions(), ["32.1.0", "32.1.1", "32.1.2", "32.1.3"]) &&
        listDirectory(path.join(fixture.lifecycleRoot, "trash")).length === 0 &&
        listDirectory(path.join(fixture.lifecycleRoot, "removals")).length === 0,
      { error: refused?.message, snapshots: fixture.snapshots(), versions: fixture.versions(), trash: listDirectory(path.join(fixture.lifecycleRoot, "trash")) });
  });

  await runCase([CASES.r8PruneSafety[3]], async (record) => {
    const fixture = createHome("r8-rollback-required");
    await updatesWithoutRetention(fixture, [["rr0", "33.0.9"], ["rr1", "33.1.0"], ["rr2", "33.1.1"]]);
    fs.writeFileSync(path.join(fixture.lifecycleRoot, "journal.json"), `${JSON.stringify({
      schemaVersion: 1,
      operationId: "rr2",
      kind: "update",
      fromVersion: "33.1.0",
      toVersion: "33.1.1",
      phase: "rollback_required",
      snapshotId: "rr2",
    }, null, 2)}\n`, { mode: 0o600 });
    const applied = await rejection(() => fixture.manager().pruneSnapshots({
      operationId: "r8-rollback-prune", keep: 1, apply: true,
    }));
    const receipt = fixture.receipt("r8-rollback-prune", "snapshots_prune");
    record(CASES.r8PruneSafety[3],
      applied === null && receipt?.status === "completed" &&
        receipt.retention?.removed.some((item) => item.kind === "snapshot" && item.name === "rr0") === true &&
        !fixture.snapshots().includes("rr0") && fixture.snapshots().includes("rr1") && fixture.snapshots().includes("rr2") &&
        fixture.versions().includes("33.1.0") && fixture.versions().includes("33.1.1"),
      { error: applied?.message, receipt, snapshots: fixture.snapshots(), versions: fixture.versions() });
  });

  const crashedPrune = async (name: string) => {
    const fixture = createHome(name);
    await updatesWithoutRetention(fixture, [["a1", "34.1.0"], ["a2", "34.1.1"], ["a3", "34.1.2"]]);
    const trashRoot = path.join(fixture.lifecycleRoot, "trash");
    const removalsRoot = path.join(fixture.lifecycleRoot, "removals");
    fs.mkdirSync(trashRoot, { recursive: true, mode: 0o700 });
    fs.mkdirSync(removalsRoot, { recursive: true, mode: 0o700 });
    const items = [
      { kind: "snapshot", name: "a2", bytes: 0, trashName: "snapshot+a2+0123456789ab", origin: "planned" },
      { kind: "runtime_version", name: "34.1.0", bytes: 0, trashName: "runtime_version+34.1.0+0123456789ab", origin: "planned" },
    ] as const;
    fs.renameSync(path.join(fixture.lifecycleRoot, "snapshots", "a2"), path.join(trashRoot, items[0].trashName));
    fs.renameSync(path.join(fixture.lifecycleRoot, "versions", "34.1.0"), path.join(trashRoot, items[1].trashName));
    const removalRecord = path.join(removalsRoot, `${name}.json`);
    fs.writeFileSync(removalRecord, `${JSON.stringify({ schemaVersion: 1, operationId: name, items }, null, 2)}\n`, { mode: 0o600 });
    fs.rmSync(path.join(fixture.lifecycleRoot, "versions", "34.1.1"), { recursive: true, force: true });
    return { fixture, trashRoot, removalRecord, items };
  };

  await runCase([CASES.r8PruneSafety[4]], async (record) => {
    const { fixture, trashRoot, removalRecord, items } = await crashedPrune("r9-crash-restore");
    const attempt = await rejection(() => fixture.manager().pruneSnapshots({
      operationId: "r9-crash-restore-prune", keep: 1, apply: true,
    }));
    const receipt = fixture.receipt("r9-crash-restore-prune", "snapshots_prune");
    record(CASES.r8PruneSafety[4],
      attempt === null && receipt?.status === "completed" &&
        same(receipt.retention?.restored?.map((item) => `${item.kind}:${item.name}`) ?? [],
          ["snapshot:a2", "runtime_version:34.1.0"]) &&
        receipt.retention?.recovered.length === 0 &&
        fixture.snapshots().includes("a2") && fixture.versions().includes("34.1.0") &&
        !exists(removalRecord) && items.every((item) => !exists(path.join(trashRoot, item.trashName))),
      { error: attempt?.message, receipt, snapshots: fixture.snapshots(), versions: fixture.versions(),
        trash: listDirectory(trashRoot), recordKept: exists(removalRecord) });
  });

  await runCase([CASES.r8PruneSafety[5]], async (record) => {
    const { fixture, trashRoot, removalRecord, items } = await crashedPrune("r9-crash-restore-failed");
    const conflict = path.join(fixture.lifecycleRoot, "snapshots", "a2");
    fs.mkdirSync(conflict, { mode: 0o700 });
    const refused = await rejection(() => fixture.manager().pruneSnapshots({
      operationId: "r9-crash-restore-refused", keep: 1, apply: true,
    }));
    record(CASES.r8PruneSafety[5],
      refused !== null && /needed_restore_incomplete/.test(refused.message) &&
        !refused.message.includes(fixture.home) &&
        exists(removalRecord) && exists(path.join(trashRoot, items[0].trashName)) &&
        fixture.receipt("r9-crash-restore-refused", "snapshots_prune") === null,
      { error: refused?.message, recordKept: exists(removalRecord), trash: listDirectory(trashRoot) });
  });
}

// ---- r3: a ledger that is merely open counts as in use -------------------

type Opener = { send: (command: string) => Promise<string>; exit: () => Promise<void> };

/**
 * Another process that opens the ledger (sqlite3_open) and then waits: it
 * holds no SQLite lock until its first statement, so only lsof can see it.
 */
async function openWithoutUsing(ledger: string): Promise<Opener> {
  const child: ChildProcessWithoutNullStreams = spawn(process.execPath, ["-e", `
    const Database = require(${JSON.stringify(BETTER_SQLITE3)});
    const db = new Database(${JSON.stringify(ledger)}, { timeout: 2000 });
    const out = (line) => process.stdout.write(line + "\\n");
    const run = (fn) => { try { out(fn()); } catch (error) { out("error:" + String(error.code ?? error.message)); } };
    let buffered = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
      buffered += chunk;
      let newline;
      while ((newline = buffered.indexOf("\\n")) >= 0) {
        const command = buffered.slice(0, newline);
        buffered = buffered.slice(newline + 1);
        if (command === "select") run(() => "count:" + db.prepare("select count(*) n from proof_rows").get().n);
        else if (command.startsWith("write ")) run(() => { db.prepare("insert into proof_rows (label) values (?)").run(command.slice(6)); return "wrote"; });
        else if (command === "close") { run(() => { db.close(); return "closed"; }); process.exit(0); }
      }
    });
    out("opened");`], { stdio: ["pipe", "pipe", "pipe"] });
  child.stdout.setEncoding("utf8");
  const lines: string[] = [];
  const waiting: Array<(line: string) => void> = [];
  let pending = "";
  child.stdout.on("data", (chunk: string) => {
    pending += chunk;
    let newline;
    while ((newline = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, newline);
      pending = pending.slice(newline + 1);
      const next = waiting.shift();
      if (next) next(line); else lines.push(line);
    }
  });
  const nextLine = () => new Promise<string>((resolve, reject) => {
    const queued = lines.shift();
    if (queued !== undefined) return resolve(queued);
    const timer = setTimeout(() => reject(new Error("opener did not answer")), 30_000);
    waiting.push((line) => {
      clearTimeout(timer);
      resolve(line);
    });
  });
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  if (await nextLine() !== "opened") throw new Error("opener did not open the ledger");
  return {
    send: async (command) => {
      child.stdin.write(`${command}\n`);
      return nextLine();
    },
    exit: async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await exited;
    },
  };
}

async function r3OpenHandles() {
  // Opened before the update and never used: no lock, but still in use.
  await runCase([CASES.r3Handles[0]], async (record) => {
    const fixture = createHome("r3-idle-before-update");
    await fixture.manager().update({ operationId: "ib-initial", artifact: fixture.artifact("1.0.0") });
    const before = treeState(fixture);
    const opener = await openWithoutUsing(fixture.ledger);
    let error: (Error & { code?: string }) | null;
    let receipt: LifecycleReceipt | null;
    let firstUse = "";
    try {
      error = await rejection(() => fixture.manager().update({ operationId: "ib-busy", artifact: fixture.artifact("1.0.1") }));
      receipt = fixture.receipt("ib-busy");
      firstUse = await opener.send("select");
    } finally {
      await opener.exit();
    }
    record(CASES.r3Handles[0],
      error?.code === "LIFECYCLE_SNAPSHOT_REFUSED" && receipt?.status === "refused" &&
        (receipt.refusal as { reason?: string } | undefined)?.reason === "ledger_in_use" && treeState(fixture) === before &&
        firstUse === "count:64",
      { error: error?.message, receipt, firstUse });
  });
  // Opened after the snapshot (the new runtime, another tool) and not used:
  // the rollback refuses the swap, then finishes once that process is gone.
  await runCase([CASES.r3Handles[1]], async (record) => {
    const fixture = createHome("r3-idle-before-swap");
    await fixture.manager().update({ operationId: "is-initial", artifact: fixture.artifact("1.0.0") });
    const before = rowsDigest(fixture.ledger);
    let opener: Opener | null = null;
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => {
      appendRow(fixture.ledger, "migration-that-must-roll-back");
      opener = await openWithoutUsing(fixture.ledger);
    };
    try {
      const failed = await rejection(() => fixture.manager().update({ operationId: "is-failed", artifact: fixture.artifact("1.0.1") }));
      fixture.service.onActivate = null;
      const receipt = fixture.receipt("is-failed");
      const refusal = (receipt as { restoreRefusal?: { reason?: string } } | null)?.restoreRefusal;
      const untouched = countLabel(fixture.ledger, "migration-that-must-roll-back") === 1;
      await (opener as Opener | null)?.exit();
      opener = null;
      fixture.service.failReadiness = false;
      const retry = await rejection(() => fixture.manager().update({ operationId: "is-failed", artifact: fixture.artifact("1.0.1") }));
      const final = fixture.receipt("is-failed");
      record(CASES.r3Handles[1],
        failed !== null && receipt?.status === "rollback_required" && refusal?.reason === "ledger_in_use" && untouched &&
          retry === null && final?.status === "rolled_back" && rowsDigest(fixture.ledger) === before,
        { failed: failed?.message, receipt: receipt?.status, refusal, untouched, retry: retry?.message ?? null, final: final?.status });
    } finally {
      fixture.service.onActivate = null;
      fixture.service.failReadiness = false;
      await (opener as Opener | null)?.exit();
    }
  });
  // The last defense, for a process that opens the ledger in the instant
  // between the lsof check and the swap (simulated with a check that sees
  // nothing): its first use of the replaced file fails loudly.
  await runCase([CASES.r3Handles[2]], async (record) => {
    const fixture = createHome("r3-header-zeroed");
    await fixture.manager().update({ operationId: "hz-initial", artifact: fixture.artifact("1.0.0") });
    const before = rowsDigest(fixture.ledger);
    const blind = new SqliteLedgerSnapshotAdapter({ openHandles: () => [] });
    let opener: Opener | null = null;
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => {
      appendRow(fixture.ledger, "migration-that-must-roll-back");
      opener = await openWithoutUsing(fixture.ledger);
    };
    try {
      const failed = await rejection(() => fixture.manager(blind).update({ operationId: "hz-failed", artifact: fixture.artifact("1.0.1") }));
      fixture.service.onActivate = null;
      fixture.service.failReadiness = false;
      const attached = opener as Opener | null;
      const firstSelect = attached ? await attached.send("select") : "no-opener";
      const firstWrite = attached ? await attached.send("write late-opener-row") : "no-opener";
      await attached?.exit();
      opener = null;
      record(CASES.r3Handles[2],
        /readiness failed/.test(failed?.message ?? "") && fixture.receipt("hz-failed")?.status === "rolled_back" &&
          /^error:SQLITE_NOTADB/.test(firstSelect) && /^error:SQLITE_NOTADB/.test(firstWrite) &&
          rowsDigest(fixture.ledger) === before && countLabel(fixture.ledger, "late-opener-row") === 0 &&
          integrityOf(fixture.ledger) === "ok",
        { failed: failed?.message, firstSelect, firstWrite });
    } finally {
      fixture.service.onActivate = null;
      fixture.service.failReadiness = false;
      await (opener as Opener | null)?.exit();
    }
  });
  // The fence is lost after the last renewal (a stall inside the final
  // handle check lets a successor take the lease): the check right before
  // the rename must still stop the swap.
  await runCase([CASES.r3Handles[3]], async (record) => {
    const fixture = createHome("r3-fence-before-swap");
    await fixture.manager().update({ operationId: "fs-initial", artifact: fixture.artifact("1.0.0") });
    let successor: LifecycleMutationLease | null = null;
    let checks = 0;
    const stallingCheck = new SqliteLedgerSnapshotAdapter({
      openHandles: () => {
        checks += 1;
        // First call: the snapshot's quiesce. Second: right before the swap.
        if (checks === 2) {
          stall(1_600);
          const taken = new LifecycleMutationAuthority(path.join(fixture.collector, "lifecycle-authority")).acquire({ leaseMs: 60_000 });
          if (taken.kind === "acquired") successor = taken.lease;
        }
        return [];
      },
    });
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => appendRow(fixture.ledger, "migration-that-must-roll-back");
    const error = await rejection(() => new LifecycleManager(fixture.adapter(stallingCheck, { authorityLeaseMs: 1_000 }))
      .update({ operationId: "fs-failed", artifact: fixture.artifact("1.0.1") }));
    fixture.service.onActivate = null;
    fixture.service.failReadiness = false;
    const notSwapped = countLabel(fixture.ledger, "migration-that-must-roll-back") === 1;
    const journal = readJournal(fixture);
    (successor as LifecycleMutationLease | null)?.release();
    record(CASES.r3Handles[3],
      checks === 2 && successor !== null && error?.code === "LIFECYCLE_INTERRUPTED" && notSwapped &&
        journal?.phase === "rollback_required" && fixture.receipt("fs-failed")?.status !== "rolled_back",
      { checks, error: error?.message, notSwapped, journal });
  });
}

// ---- r3: pre-existing damage does not trap a failed update ---------------

/** An index whose entries no longer match their rows: it opens, but integrity_check complains. */
function corruptIndex(file: string, index: string, columns: string) {
  const db = new Database(file);
  db.exec(`create index if not exists ${index} on proof_rows(${columns})`);
  const rootPage = (db.prepare("select rootpage from sqlite_master where name = ?").get(index) as { rootpage: number }).rootpage;
  const pageSize = db.pragma("page_size", { simple: true }) as number;
  db.pragma("wal_checkpoint(TRUNCATE)");
  db.close();
  const bytes = fs.readFileSync(file);
  const page = (rootPage - 1) * pageSize;
  let flipped = 0;
  for (let offset = page + 100; offset < page + pageSize - 16; offset += 1) {
    if (bytes[offset] === 0x73) {
      bytes[offset] = 0x74;
      flipped += 1;
    }
  }
  fs.writeFileSync(file, bytes);
  return flipped;
}

async function r3PreexistingDamage() {
  // The live ledger already fails integrity_check, so every snapshot does
  // too. A failed update must still roll back instead of being stuck.
  await runCase([CASES.r3Damage[0]], async (record) => {
    const fixture = createHome("r3-damaged-ledger");
    await fixture.manager().update({ operationId: "dl-initial", artifact: fixture.artifact("1.0.0") });
    const flipped = corruptIndex(fixture.ledger, "proof_rows_label", "label");
    const damagedBefore = integrityOf(fixture.ledger) !== "ok";
    fixture.service.failReadiness = true;
    fixture.service.onActivate = async () => appendRow(fixture.ledger, "migration-that-must-roll-back");
    const failed = await rejection(() => fixture.manager().update({ operationId: "dl-failed", artifact: fixture.artifact("1.0.1") }));
    fixture.service.failReadiness = false;
    fixture.service.onActivate = null;
    const receipt = fixture.receipt("dl-failed") as (LifecycleReceipt & { restore?: { integrity?: string } }) | null;
    const next = await rejection(() => fixture.manager().update({ operationId: "dl-next", artifact: fixture.artifact("1.0.2") }));
    record(CASES.r3Damage[0],
      flipped > 0 && damagedBefore && /readiness failed/.test(failed?.message ?? "") && receipt?.status === "rolled_back" &&
        receipt.restore?.integrity === "preexisting_damage" && countLabel(fixture.ledger, "migration-that-must-roll-back") === 0 &&
        next === null && fixture.receipt("dl-next")?.status === "completed",
      { failed: failed?.message, receipt, next: next?.message ?? null });
  });
  // A restored copy with damage the live ledger does not have (here the live
  // ledger is damaged too, but elsewhere) is still refused.
  await runCase([CASES.r3Damage[1]], async (record) => {
    const pair = restorePair("r3-other-damage");
    corruptIndex(pair.snapshot, "proof_rows_label", "label");
    corruptIndex(pair.live, "proof_rows_by_id_label", "id, label");
    const liveBefore = fs.readFileSync(pair.live);
    const error = await rejection(() =>
      new SqliteLedgerSnapshotAdapter({ clone: () => false, freeBytes: () => Number.MAX_SAFE_INTEGER })
        .restore({ source: pair.snapshot, destination: pair.live }));
    record(CASES.r3Damage[1],
      error?.code === "LIFECYCLE_RESTORE_REFUSED" && /integrity_check/.test(error.message) &&
        fs.readFileSync(pair.live).equals(liveBefore) && onlyLedgers(pair.directory),
      { error: error?.message, files: listDirectory(pair.directory) });
  });
}

// ---- r3: blocked retention has an audited way out -------------------------

const REPO_ROOT = path.resolve(import.meta.dirname, "..");
const CLI_ENTRY = path.join(REPO_ROOT, "packages", "collector-cli", "src", "cli.ts");
const TSX_LOADER = path.join(REPO_ROOT, "node_modules", "tsx", "dist", "loader.mjs");

/** The real CLI over one fixture home. */
function cli(fixture: Home, args: readonly string[]) {
  const result = spawnSync(process.execPath, ["--import", TSX_LOADER, CLI_ENTRY, ...args], {
    cwd: fixture.home,
    encoding: "utf8",
    timeout: 180_000,
    env: {
      PATH: process.env.PATH, HOME: fixture.home, PLIMSOLL_HOME: fixture.collector, TMPDIR: process.env.TMPDIR,
      CODEX_HOME: path.join(fixture.home, ".codex"), CLAUDE_CONFIG_DIR: path.join(fixture.home, ".claude"),
      LANG: "en_US.UTF-8", TZ: "UTC",
    },
  });
  const json = (() => {
    try {
      return JSON.parse(result.stdout) as Record<string, unknown>;
    } catch {
      return null;
    }
  })();
  return { code: result.status, stdout: result.stdout ?? "", stderr: result.stderr ?? "", json };
}

const orderPath = (fixture: Home) => path.join(fixture.lifecycleRoot, "completion-order.json");

async function listDecisions(fixture: Home, keep: number) {
  const inventory = await fixture.manager().listSnapshots({ keep });
  return {
    blockedReason: inventory.blockedReason,
    decisions: Object.fromEntries(inventory.snapshots.map((row) => [row.id, `${row.retention}:${row.reason}`])),
  };
}

async function r3BlockedRetentionRepair() {
  // The order record is lost. An update completed meanwhile must still get a
  // sequence (or its snapshot is unknown for ever), and reconcile rebuilds
  // the record from the receipts' own sequences without operator input.
  await runCase([CASES.r3Repair[0]], async (record) => {
    const fixture = createHome("r3-order-lost");
    await updatesWithoutRetention(fixture, [["o1", "1.0.1"], ["o2", "1.0.2"], ["o3", "1.0.3"]]);
    fs.rmSync(orderPath(fixture));
    const blocked = await listDecisions(fixture, 2);
    const o4 = await new LifecycleManager(withoutRetention(fixture.adapter())).update({ operationId: "o4", artifact: fixture.artifact("1.0.4") });
    const preview = await fixture.manager().reconcileSnapshots({ operationId: "o-reconcile-preview" });
    const orderAbsentAfterPreview = !exists(orderPath(fixture));
    const applied = await fixture.manager().reconcileSnapshots({ operationId: "o-reconcile", apply: true });
    const rebuilt = JSON.parse(fs.readFileSync(orderPath(fixture), "utf8")) as { lastSequence?: number; legacyOperations?: string[] };
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "o-prune", keep: 2, apply: true });
    record(CASES.r3Repair[0],
      blocked.blockedReason === "completion_order_unproven" && o4.completionSequence === 4 &&
        preview.receipt === null && preview.reconcile.repair === "rebuilt" && orderAbsentAfterPreview &&
        applied.receipt?.status === "completed" && applied.reconcile.repair === "rebuilt" && rebuilt.lastSequence === 4 &&
        JSON.stringify(rebuilt.legacyOperations) === "[]" &&
        same(pruned.retention.removed.filter((item) => item.kind === "snapshot").map((item) => item.name), ["o1", "o2"]) &&
        same(fixture.snapshots(), ["o3", "o4"]),
      { blocked, o4Sequence: o4.completionSequence ?? null, preview: preview.reconcile, applied: applied.reconcile, rebuilt,
        pruned: pruned.retention, remains: fixture.snapshots() });
  });
  // Older receipts plus a torn order record: the order cannot be rebuilt from
  // provable facts. Reconcile (the real CLI) refuses to guess, and seals the
  // order only with the operator's explicit keep-set.
  await runCase([CASES.r3Repair[1]], async (record) => {
    const fixture = createHome("r3-order-torn");
    await updatesWithoutRetention(fixture, [["l1", "2.1.0"], ["l2", "2.1.1"]]);
    asPreSequencingMarkers(fixture, ["l1", "l2"]);
    await updatesWithoutRetention(fixture, [["s3", "2.1.2"], ["s4", "2.1.3"]]);
    const saved = fs.readFileSync(orderPath(fixture), "utf8");
    fs.writeFileSync(orderPath(fixture), saved.slice(0, Math.floor(saved.length / 2)));
    const torn = fs.readFileSync(orderPath(fixture));
    const dryRun = cli(fixture, ["lifecycle", "snapshots", "reconcile"]);
    const refused = cli(fixture, ["lifecycle", "snapshots", "reconcile", "--apply", "--operation-id", "t-no-keep"]);
    const untouched = fs.readFileSync(orderPath(fixture)).equals(torn) && fixture.receipt("t-no-keep", "snapshots_reconcile") === null;
    const sealed = cli(fixture, ["lifecycle", "snapshots", "reconcile", "--keep-snapshots", "s4", "--apply", "--operation-id", "t-seal"]);
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "t-prune", keep: 2, apply: true });
    const afterPrune = fixture.snapshots();
    for (const [id, version] of [["n5", "2.1.4"], ["n6", "2.1.5"]] as const) {
      appendRow(fixture.ledger, `before-${id}`);
      await fixture.manager().update({ operationId: id, artifact: fixture.artifact(version) });
    }
    const dryReconcile = (dryRun.json?.reconcile ?? {}) as { repair?: string };
    const sealReconcile = ((sealed.json?.receipt as { reconcile?: { repair?: string; keep?: string[]; released?: string[] } } | undefined)?.reconcile) ?? {};
    record(CASES.r3Repair[1],
      dryRun.code === 0 && dryReconcile.repair === "needs_keep" && refused.code !== 0 && /--keep-snapshots/.test(refused.stderr) &&
        untouched && sealed.code === 0 && sealReconcile.repair === "sealed" && same(sealReconcile.keep ?? [], ["s4"]) &&
        same(sealReconcile.released ?? [], ["l1", "l2", "s3"]) &&
        same(pruned.retention.removed.filter((item) => item.kind === "snapshot").map((item) => item.name), ["l1", "l2", "s3"]) &&
        same(afterPrune, ["s4"]) && same(fixture.snapshots(), ["n5", "n6"]),
      { dryRun: dryReconcile, refused: refused.stderr.slice(-300), untouched, sealed: sealReconcile, sealedStderr: sealed.stderr.slice(-300),
        pruned: pruned.retention.removed, afterPrune, final: fixture.snapshots() });
  });
  // An update by a pre-0.7.38 CLI after sequencing began writes an
  // unsequenced receipt: retention blocks until the operator names what to keep.
  await runCase([CASES.r3Repair[2]], async (record) => {
    const fixture = createHome("r3-older-cli");
    await updatesWithoutRetention(fixture, [["n1", "3.1.1"], ["n2", "3.1.2"], ["n3", "3.1.3"]]);
    asOlderCliMarker(fixture, "n3");
    const blocked = await fixture.manager().pruneSnapshots({ operationId: "d-blocked-prune", keep: 1, apply: true });
    const preview = await fixture.manager().reconcileSnapshots({ operationId: "d-preview" });
    const applied = await fixture.manager().reconcileSnapshots({ operationId: "d-seal", keep: ["n3", "n2"], apply: true });
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "d-prune", keep: 2, apply: true });
    appendRow(fixture.ledger, "before-n4");
    const n4 = await fixture.manager().update({ operationId: "n4", artifact: fixture.artifact("3.1.4") });
    const afterN4 = fixture.snapshots();
    appendRow(fixture.ledger, "before-n5");
    await fixture.manager().update({ operationId: "n5", artifact: fixture.artifact("3.1.5") });
    record(CASES.r3Repair[2],
      blocked.retention.status === "skipped" && blocked.retention.skippedReason === "completion_order_unproven" &&
        preview.reconcile.repair === "needs_keep" && same(preview.reconcile.findings.unsequencedAfterSequencing, ["n3"]) &&
        applied.reconcile.repair === "sealed" && same(applied.reconcile.released, ["n1"]) &&
        same(pruned.retention.removed.filter((item) => item.kind === "snapshot").map((item) => item.name), ["n1"]) &&
        n4.retention?.status === "applied" && same(afterN4, ["n2", "n3", "n4"]) && same(fixture.snapshots(), ["n4", "n5"]),
      { blocked: blocked.retention, preview: preview.reconcile, applied: applied.reconcile, pruned: pruned.retention.removed,
        n4: n4.retention, afterN4, final: fixture.snapshots() });
  });
  // One unreadable removal record blocks every removal (never applies while
  // blocked); reconcile moves it aside byte for byte, and retention resumes.
  await runCase([CASES.r3Repair[3]], async (record) => {
    const fixture = createHome("r3-removal-unreadable");
    await updatesWithoutRetention(fixture, [["u1", "4.1.1"], ["u2", "4.1.2"], ["u3", "4.1.3"]]);
    const removals = path.join(fixture.lifecycleRoot, "removals");
    fs.mkdirSync(removals, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(removals, "u0.json"), "{ torn");
    const blocked = await fixture.manager().pruneSnapshots({ operationId: "u-blocked-prune", keep: 1, apply: true });
    const stillThere = same(fixture.snapshots(), ["u1", "u2", "u3"]);
    const applied = await fixture.manager().reconcileSnapshots({ operationId: "u-reconcile", apply: true });
    const quarantine = listDirectory(path.join(fixture.lifecycleRoot, "removals-unreadable"));
    const kept = quarantine.length === 1 ? fs.readFileSync(path.join(fixture.lifecycleRoot, "removals-unreadable", quarantine[0]!), "utf8") : null;
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "u-prune", keep: 1, apply: true });
    record(CASES.r3Repair[3],
      blocked.retention.status === "skipped" && blocked.retention.skippedReason === "removal_record_unreadable" &&
        blocked.retention.removed.length === 0 && stillThere && applied.reconcile.repair === "none" &&
        applied.reconcile.quarantined.length === 1 && applied.reconcile.quarantined[0]!.name === "u0.json" &&
        applied.reconcile.quarantined[0]!.bytes === 6 && kept === "{ torn" && !exists(path.join(removals, "u0.json")) &&
        pruned.retention.status === "applied" &&
        same(pruned.retention.removed.filter((item) => item.kind === "snapshot").map((item) => item.name), ["u1", "u2"]),
      { blocked: blocked.retention, stillThere, applied: applied.reconcile, quarantine, kept, pruned: pruned.retention });
  });
  // An explicit keep-set may not release every way back to an earlier version.
  await runCase([CASES.r3Repair[6]], async (record) => {
    const fixture = createHome("r3-seal-way-back");
    await updatesWithoutRetention(fixture, [["w1", "7.1.1"], ["w2", "7.1.2"], ["w3", "7.1.3"]]);
    // w3 now restores the installed version: keeping it alone keeps no way back.
    const metadataFile = path.join(fixture.lifecycleRoot, "snapshots", "w3", "snapshot.json");
    const metadata = JSON.parse(fs.readFileSync(metadataFile, "utf8")) as Record<string, unknown>;
    fs.writeFileSync(metadataFile, `${JSON.stringify({ ...metadata, currentVersion: "7.1.3" }, null, 2)}\n`);
    const orderBefore = fs.readFileSync(orderPath(fixture));
    // The order is proven, so any seal needs --force; the way-back rule holds even then.
    const refused = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "w-refused", keep: ["w3"], apply: true, force: true }));
    const unchanged = fs.readFileSync(orderPath(fixture)).equals(orderBefore) && fixture.receipt("w-refused", "snapshots_reconcile") === null;
    const sealed = await fixture.manager().reconcileSnapshots({ operationId: "w-seal", keep: ["w3", "w2"], apply: true, force: true });
    record(CASES.r3Repair[6],
      /must keep a way back/.test(refused?.message ?? "") && unchanged && sealed.reconcile.repair === "sealed" &&
        sealed.reconcile.forced === true && same(sealed.reconcile.released, ["w1"]),
      { refused: refused?.message ?? null, unchanged, sealed: sealed.reconcile });
  });
  // A covered snapshot without a receipt leaves its ID free once pruned; a
  // later update may take it, and the old seal must not release that update.
  await runCase([CASES.r3Repair[7]], async (record) => {
    const fixture = createHome("r3-seal-reused-id");
    await updatesWithoutRetention(fixture, [["r1", "11.1.1"], ["r2", "11.1.2"]]);
    const snapshotsRoot = path.join(fixture.lifecycleRoot, "snapshots");
    fs.cpSync(path.join(snapshotsRoot, "r1"), path.join(snapshotsRoot, "orphan"), { recursive: true });
    const sealed = await fixture.manager().reconcileSnapshots({ operationId: "r-seal", keep: ["r2"], apply: true });
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "r-prune", keep: 2, apply: true });
    appendRow(fixture.ledger, "before-orphan");
    const reused = await fixture.manager().update({ operationId: "orphan", artifact: fixture.artifact("11.1.3") });
    const after = await listDecisions(fixture, 2);
    record(CASES.r3Repair[7],
      same(sealed.reconcile.released, ["orphan", "r1"]) &&
        same(pruned.retention.removed.filter((item) => item.kind === "snapshot").map((item) => item.name), ["orphan", "r1"]) &&
        reused.status === "completed" && !(reused.retention?.removed ?? []).some((item) => item.name === "orphan") &&
        fixture.snapshots().includes("orphan") && after.decisions.orphan === "keep:newest_completed",
      { sealed: sealed.reconcile, pruned: pruned.retention.removed, reused: reused.retention, after, remains: fixture.snapshots() });
  });
  // A completed receipt whose status alone was flipped to rolled_back (the
  // restored version and health left as a completion wrote them) proves nothing.
  await runCase([CASES.r3Repair[4]], async (record) => {
    const fixture = createHome("r3-flipped-status");
    await updatesWithoutRetention(fixture, [["v1", "5.1.1"], ["v2", "5.1.2"], ["v3", "5.1.3"]]);
    writeMarker(fixture, "v2", { ...readMarker(fixture, "v2"), status: "rolled_back", ownedTargets: ["runtime", "config", "database", "service_manifest"] });
    const before = await listDecisions(fixture, 64);
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "v-prune", keep: 64, apply: true });
    record(CASES.r3Repair[4],
      before.decisions.v2 === "keep:operation_unknown" && fixture.snapshots().includes("v2") &&
        !pruned.retention.removed.some((item) => item.name === "v2"),
      { before, removed: pruned.retention.removed, remains: fixture.snapshots() });
  });
  // A sequence above the order record's last one (a bogus or rolled-back
  // record) must not make that receipt "newest": nothing is removed.
  await runCase([CASES.r3Repair[5]], async (record) => {
    const fixture = createHome("r3-sequence-beyond");
    await updatesWithoutRetention(fixture, [["q1", "6.1.1"], ["q2", "6.1.2"], ["q3", "6.1.3"]]);
    writeMarker(fixture, "q1", { ...readMarker(fixture, "q1"), completionSequence: 99 });
    const before = await listDecisions(fixture, 1);
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "q-prune", keep: 1, apply: true });
    record(CASES.r3Repair[5],
      before.blockedReason === "completion_order_unproven" && pruned.retention.removed.length === 0 &&
        same(fixture.snapshots(), ["q1", "q2", "q3"]),
      { before, pruned: pruned.retention, remains: fixture.snapshots() });
  });
}

// ---- r3: what a crash leaves behind, and flushes that reach the disk ------

async function r3CrashLeftovers() {
  // An intent or order record written but never renamed into place.
  await runCase([CASES.r3Cleanup[0]], async (record) => {
    const fixture = createHome("r3-stale-temporaries");
    await updatesWithoutRetention(fixture, [["t1", "8.1.1"], ["t2", "8.1.2"], ["t3", "8.1.3"]]);
    const removals = path.join(fixture.lifecycleRoot, "removals");
    fs.mkdirSync(removals, { recursive: true, mode: 0o700 });
    const intent = path.join(removals, "crashed.json.tmp");
    const order = `${orderPath(fixture)}.tmp`;
    fs.writeFileSync(intent, "{ partial");
    fs.writeFileSync(order, "{ partial");
    const preview = await fixture.manager().pruneSnapshots({ operationId: "t-preview", keep: 2 });
    const keptByPreview = exists(intent) && exists(order);
    const applied = await fixture.manager().pruneSnapshots({ operationId: "t-prune", keep: 2, apply: true });
    record(CASES.r3Cleanup[0],
      preview.retention.status === "preview" && keptByPreview && applied.retention.status === "applied" &&
        !exists(intent) && !exists(order) && same(fixture.snapshots(), ["t2", "t3"]),
      { keptByPreview, applied: applied.retention, removals: listDirectory(removals), remains: fixture.snapshots() });
  });
  // A byte-copy restore killed before its swap leaves a ledger-sized copy.
  await runCase([CASES.r3Cleanup[1]], async (record) => {
    const fixture = createHome("r3-stale-restore-copy");
    await fixture.manager().update({ operationId: "c1", artifact: fixture.artifact("9.1.1") });
    const directory = path.dirname(fixture.ledger);
    const stale = `${fixture.ledger}.restore-0123456789ab`;
    fs.copyFileSync(fixture.ledger, stale);
    fs.writeFileSync(`${stale}-wal`, "");
    const decoys = [`${fixture.ledger}.restore-notours`, path.join(directory, "other.sqlite.restore-0123456789ab")];
    for (const decoy of decoys) fs.writeFileSync(decoy, "not a lifecycle temporary");
    appendRow(fixture.ledger, "before-c2");
    await fixture.manager().update({ operationId: "c2", artifact: fixture.artifact("9.1.2") });
    record(CASES.r3Cleanup[1],
      !exists(stale) && !exists(`${stale}-wal`) && decoys.every(exists) && fixture.receipt("c2")?.status === "completed" &&
        countLabel(fixture.ledger, "before-c2") === 1,
      { files: listDirectory(directory) });
  });
  // Every checkpoint the lifecycle runs (the snapshot's and the restore's)
  // asks SQLite for F_FULLFSYNC first: the ledger must be on stable storage
  // before its WAL is emptied.
  await runCase([CASES.r3Cleanup[2]], async (record) => {
    const fixture = createHome("r3-checkpoint-fullfsync");
    await fixture.manager().update({ operationId: "f1", artifact: fixture.artifact("10.1.1") });
    appendRow(fixture.ledger, "before-f2");
    const calls: Array<{ connection: object; source: string }> = [];
    const prototype = Database.prototype as unknown as { pragma: (this: object, source: string, ...rest: unknown[]) => unknown };
    const original = prototype.pragma;
    prototype.pragma = function (this: object, source: string, ...rest: unknown[]) {
      calls.push({ connection: this, source: source.replace(/\s+/g, " ").trim() });
      return original.call(this, source, ...rest);
    };
    let failed: Awaited<ReturnType<typeof rejection>> = null;
    try {
      fixture.service.failReadiness = true;
      failed = await rejection(() => fixture.manager().update({ operationId: "f2", artifact: fixture.artifact("10.1.2") }));
    } finally {
      prototype.pragma = original;
      fixture.service.failReadiness = false;
    }
    const checkpoints = calls.flatMap((call, index) => /^wal_checkpoint/i.test(call.source)
      ? [calls.slice(0, index).some((earlier) => earlier.connection === call.connection && /^checkpoint_fullfsync = ON$/i.test(earlier.source))]
      : []);
    record(CASES.r3Cleanup[2],
      /readiness failed/.test(failed?.message ?? "") && fixture.receipt("f2")?.status === "rolled_back" &&
        checkpoints.length >= 2 && checkpoints.every(Boolean),
      { checkpoints, pragmas: calls.map((call) => call.source) });
  });
}

// ---- r4: tighter seals, a plausible rebuild, fail-closed checks, old receipts ----

/** Two completions sharing a sequence: the order cannot be proved, so a keep-set is needed. */
function duplicateSequence(fixture: Home, id: string, sharesWith: string) {
  writeMarker(fixture, id, { ...readMarker(fixture, id), completionSequence: readMarker(fixture, sharesWith).completionSequence });
}

const restoresOf = (fixture: Home, id: string) =>
  (JSON.parse(fs.readFileSync(path.join(fixture.lifecycleRoot, "snapshots", id, "snapshot.json"), "utf8")) as { currentVersion: string | null }).currentVersion;

const orderSequence = (fixture: Home) => (JSON.parse(fs.readFileSync(orderPath(fixture), "utf8")) as { lastSequence: number }).lastSequence;

/**
 * What 0.7.38 leaves on a host a newer release sealed: it cannot read the
 * sealed order record, so its update receipt has no completion sequence and
 * the order record is left as it was.
 */
function asReceiptWithoutSequence(fixture: Home, id: string, lastSequence: number) {
  const { completionSequence: _dropped, ...marker } = readMarker(fixture, id);
  writeMarker(fixture, id, marker);
  const order = JSON.parse(fs.readFileSync(orderPath(fixture), "utf8")) as Record<string, unknown>;
  fs.writeFileSync(orderPath(fixture), JSON.stringify({ ...order, lastSequence }));
}

function fakeExecutable(name: string, body: string) {
  const file = path.join(ROOT, "r4-fake-bin", name);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  fs.writeFileSync(file, `#!/bin/sh\n${body}\n`, { mode: 0o700 });
  return file;
}

async function r4FailClosedChecks() {
  // S3 (N1): an open-handle check that cannot answer proves nothing: the
  // update is refused before any change.
  await runCase([CASES.r4FailClosed[0]], async (record) => {
    const fixture = createHome("r4-handles-unanswered");
    await fixture.manager().update({ operationId: "hu-initial", artifact: fixture.artifact("17.1.1") });
    const before = treeState(fixture);
    const unanswered = new SqliteLedgerSnapshotAdapter({ openHandles: () => null });
    const error = await rejection(() => new LifecycleManager(fixture.adapter(unanswered))
      .update({ operationId: "hu-next", artifact: fixture.artifact("17.1.2") }));
    const receipt = fixture.receipt("hu-next");
    record(CASES.r4FailClosed[0],
      error?.code === "LIFECYCLE_SNAPSHOT_REFUSED" && receipt?.status === "refused" &&
        (receipt.refusal as { reason?: string } | undefined)?.reason === "quiescence_unproven" && treeState(fixture) === before,
      { error: error?.message, receipt });
  });
  // S3 (N2): an integrity helper that crashes or prints something other than
  // its JSON result is not a clean check, and the restore is refused.
  await runCase([CASES.r4FailClosed[1]], async (record) => {
    const pair = restorePair("r4-helper-fails");
    const crash = fakeExecutable("crashing-node", "exit 3");
    const garbage = fakeExecutable("garbled-node", "echo not-json");
    const realExecPath = process.execPath;
    let crashed: Awaited<ReturnType<typeof integrityCheckOffThread>> | null = null;
    let garbled: Awaited<ReturnType<typeof integrityCheckOffThread>> | null = null;
    let refused: Awaited<ReturnType<typeof rejection>> = null;
    try {
      process.execPath = crash;
      crashed = await integrityCheckOffThread(pair.snapshot);
      process.execPath = garbage;
      garbled = await integrityCheckOffThread(pair.snapshot);
      process.execPath = crash;
      refused = await rejection(() => new SqliteLedgerSnapshotAdapter({ clone: () => false, freeBytes: () => Number.MAX_SAFE_INTEGER })
        .restore({ source: pair.snapshot, destination: pair.live }));
    } finally {
      process.execPath = realExecPath;
    }
    const failedCheck = (result: typeof crashed) => result?.status === "unreadable" && result.code === "integrity_helper_failed";
    record(CASES.r4FailClosed[1],
      failedCheck(crashed) && failedCheck(garbled) && refused?.code === "LIFECYCLE_RESTORE_REFUSED" &&
        /integrity_check/.test(refused.message) && rowsDigest(pair.live) === pair.liveDigest && onlyLedgers(pair.directory),
      { crashed, garbled, refused: refused?.message ?? "restored", files: listDirectory(pair.directory) });
  });
  // S3 (N3): lsof exits 1 both when nobody has the files open and on errors.
  // A -shm symlink that lstat sees but lsof cannot stat is an error: it counts
  // as unproven, never as nobody, and the update is refused.
  await runCase([CASES.r4FailClosed[2]], async (record) => {
    const fixture = createHome("r4-lsof-error");
    await fixture.manager().update({ operationId: "le-initial", artifact: fixture.artifact("18.1.1") });
    const shm = `${fixture.ledger}-shm`;
    fs.rmSync(shm, { force: true });
    fs.symlinkSync(path.join(path.dirname(fixture.ledger), "missing-shm-target"), shm);
    let direct: number[] | null = [];
    let error: Awaited<ReturnType<typeof rejection>> = null;
    let receipt: LifecycleReceipt | null = null;
    try {
      direct = otherProcessesWithFilesOpen([fixture.ledger, `${fixture.ledger}-wal`, shm]);
      error = await rejection(() => fixture.manager().update({ operationId: "le-next", artifact: fixture.artifact("18.1.2") }));
      receipt = fixture.receipt("le-next");
    } finally {
      fs.rmSync(shm, { force: true });
    }
    record(CASES.r4FailClosed[2],
      direct === null && error?.code === "LIFECYCLE_SNAPSHOT_REFUSED" && receipt?.status === "refused" &&
        (receipt.refusal as { reason?: string } | undefined)?.reason === "quiescence_unproven",
      { direct, error: error?.message, receipt });
  });
}

async function r4PlausibleRebuild() {
  // S2: with the order record lost, one receipt carrying sequence 99 would
  // make the oldest snapshot the newest. Its sequences contradict the version
  // chain, so nothing is rebuilt and nothing is removed.
  await runCase([CASES.r4Rebuild[0]], async (record) => {
    const fixture = createHome("r4-implausible-sequence");
    const ids = ["q1", "q2", "q3", "q4"];
    await updatesWithoutRetention(fixture, [["q1", "16.1.1"], ["q2", "16.1.2"], ["q3", "16.1.3"], ["q4", "16.1.4"]]);
    writeMarker(fixture, "q1", { ...readMarker(fixture, "q1"), completionSequence: 99 });
    fs.rmSync(orderPath(fixture));
    const dry = await fixture.manager().reconcileSnapshots({ operationId: "is-dry" });
    const refused = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "is-apply", apply: true }));
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "is-prune", keep: 1, apply: true });
    record(CASES.r4Rebuild[0],
      dry.reconcile.repair === "needs_keep" && /from provable facts/.test(refused?.message ?? "") &&
        !exists(orderPath(fixture)) && pruned.retention.removed.length === 0 && same(fixture.snapshots(), ids),
      { dry: dry.reconcile, refused: refused?.message ?? "accepted", pruned: pruned.retention, remains: fixture.snapshots() });
  });
}

async function r4TighterSeals() {
  // S1: the first install's snapshot restores no version, so it is not a way
  // back; a keep-set of only it is refused even with --force.
  await runCase([CASES.r4Seal[0]], async (record) => {
    const fixture = createHome("r4-first-install");
    const ids = ["w1", "w2", "w3", "w4"];
    await updatesWithoutRetention(fixture, [["w1", "12.1.1"], ["w2", "12.1.2"], ["w3", "12.1.3"], ["w4", "12.1.4"]]);
    duplicateSequence(fixture, "w2", "w3");
    const orderBefore = fs.readFileSync(orderPath(fixture));
    const dry = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "fi-dry", keep: ["w1"] }));
    const forced = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "fi-apply", keep: ["w1"], apply: true, force: true }));
    record(CASES.r4Seal[0],
      restoresOf(fixture, "w1") === null && /must keep a way back/.test(dry?.message ?? "") &&
        /must keep a way back/.test(forced?.message ?? "") && fs.readFileSync(orderPath(fixture)).equals(orderBefore) &&
        fixture.receipt("fi-apply", "snapshots_reconcile") === null && same(fixture.snapshots(), ids),
      { w1Restores: restoresOf(fixture, "w1"), dry: dry?.message ?? "accepted", forced: forced?.message ?? "accepted" });
  });
  // S1: a keep-set that releases the newest snapshot restoring an earlier
  // version is flagged in the dry run and applied only with --force.
  await runCase([CASES.r4Seal[1]], async (record) => {
    const fixture = createHome("r4-newest-way-back");
    const ids = ["v1", "v2", "v3", "v4"];
    await updatesWithoutRetention(fixture, [["v1", "13.1.1"], ["v2", "13.1.2"], ["v3", "13.1.3"], ["v4", "13.1.4"]]);
    duplicateSequence(fixture, "v2", "v3");
    const orderBefore = fs.readFileSync(orderPath(fixture));
    const preview = await fixture.manager().reconcileSnapshots({ operationId: "nw-preview", keep: ["v2"] });
    const refused = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "nw-refused", keep: ["v2"], apply: true }));
    const unchanged = fs.readFileSync(orderPath(fixture)).equals(orderBefore) &&
      fixture.receipt("nw-refused", "snapshots_reconcile") === null && same(fixture.snapshots(), ids);
    const forced = await fixture.manager().reconcileSnapshots({ operationId: "nw-forced", keep: ["v2"], apply: true, force: true });
    const receipt = fixture.receipt("nw-forced", "snapshots_reconcile") as (LifecycleReceipt & { reconcile?: { forced?: boolean } }) | null;
    record(CASES.r4Seal[1],
      preview.reconcile.neededRepair === "needs_keep" && preview.reconcile.newestWayBack === "v4" &&
        preview.reconcile.newestWayBackReleased && /would release v4/.test(refused?.message ?? "") && unchanged &&
        forced.reconcile.forced === true && receipt?.reconcile?.forced === true && same(forced.reconcile.released, ["v1", "v3", "v4"]),
      { preview: preview.reconcile, refused: refused?.message ?? "accepted", unchanged, forced: forced.reconcile });
  });
  // S1: on a host whose order is proven and nothing is undecided, a seal is
  // refused without --force, so the newest two survive the next prune.
  await runCase([CASES.r4Seal[2]], async (record) => {
    const fixture = createHome("r4-healthy-seal");
    await updatesWithoutRetention(fixture, [["h1", "14.1.1"], ["h2", "14.1.2"], ["h3", "14.1.3"], ["h4", "14.1.4"]]);
    const dry = await fixture.manager().reconcileSnapshots({ operationId: "hs-dry", keep: ["h2"] });
    const refused = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "hs-refused", keep: ["h2"], apply: true }));
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "hs-prune", keep: 2, apply: true });
    record(CASES.r4Seal[2],
      dry.reconcile.neededRepair === "none" && /nothing needs a keep-set/.test(refused?.message ?? "") &&
        fixture.receipt("hs-refused", "snapshots_reconcile") === null &&
        same(pruned.retention.removed.filter((item) => item.kind === "snapshot").map((item) => item.name), ["h1", "h2"]) &&
        same(fixture.snapshots(), ["h3", "h4"]),
      { dry: dry.reconcile, refused: refused?.message ?? "accepted", pruned: pruned.retention.removed, remains: fixture.snapshots() });
  });
  // Control: a host upgraded from 0.7.37 whose version chain cannot order its
  // newest snapshots keeps them unproven; that needs a seal, without --force.
  await runCase([CASES.r4Seal[3]], async (record) => {
    const fixture = createHome("r4-ambiguous-seal");
    await updatesWithoutRetention(fixture, [["a1", "15.1.1"], ["a2", "15.1.2"], ["a3", "15.1.2"]]);
    asPreSequencingMarkers(fixture, ["a1", "a2", "a3"]);
    const dry = await fixture.manager().reconcileSnapshots({ operationId: "as-dry" });
    const sealed = await fixture.manager().reconcileSnapshots({ operationId: "as-seal", keep: ["a2"], apply: true });
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "as-prune", keep: 2, apply: true });
    record(CASES.r4Seal[3],
      dry.reconcile.repair === "needs_keep" && same(dry.reconcile.findings.undecidedSnapshots, ["a1", "a2", "a3"]) &&
        sealed.reconcile.repair === "sealed" && sealed.reconcile.forced === false && same(fixture.snapshots(), ["a2"]) &&
        same(pruned.retention.removed.filter((item) => item.kind === "snapshot").map((item) => item.name), ["a1", "a3"]),
      { dry: dry.reconcile, sealed: sealed.reconcile, pruned: pruned.retention.removed, remains: fixture.snapshots() });
  });
}

async function r4ReceiptsWithoutSequence() {
  // S4: on a sealed host, 0.7.38 cannot read the order record and its update
  // receipt has no completion sequence. That snapshot is kept, flagged with
  // what to run, and a new seal decides it without --force.
  await runCase([CASES.r4OldReceipts[0]], async (record) => {
    const fixture = createHome("r4-receipt-without-sequence");
    await updatesWithoutRetention(fixture, [["x1", "19.1.1"], ["x2", "19.1.2"], ["x3", "19.1.3"]]);
    duplicateSequence(fixture, "x2", "x3");
    await fixture.manager().reconcileSnapshots({ operationId: "ws-seal-1", keep: ["x3"], apply: true });
    const lastBefore = orderSequence(fixture);
    await updatesWithoutRetention(fixture, [["x4", "19.1.4"]]);
    asReceiptWithoutSequence(fixture, "x4", lastBefore);
    const listed = await fixture.manager().listSnapshots({ keep: 2 });
    const text = formatSnapshotInventory(listed);
    const x4 = listed.snapshots.find((row) => row.id === "x4");
    const dry = await fixture.manager().reconcileSnapshots({ operationId: "ws-dry" });
    const resealed = await fixture.manager().reconcileSnapshots({ operationId: "ws-seal-2", keep: ["x4"], apply: true });
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "ws-prune", keep: 2, apply: true });
    record(CASES.r4OldReceipts[0],
      x4?.retention === "keep" && x4.reason === "receipt_without_sequence" && /older than 0\.7\.41/.test(text) &&
        /snapshots reconcile/.test(text) && same(dry.reconcile.findings.receiptsWithoutSequence, ["x4"]) &&
        dry.reconcile.neededRepair === "needs_keep" && resealed.reconcile.repair === "sealed" && resealed.reconcile.forced === false &&
        same(pruned.retention.removed.filter((item) => item.kind === "snapshot").map((item) => item.name), ["x1", "x2", "x3"]) &&
        same(fixture.snapshots(), ["x4"]),
      { x4, hint: text.split("\n").filter((line) => /reconcile/.test(line)), dry: dry.reconcile, resealed: resealed.reconcile,
        pruned: pruned.retention.removed, remains: fixture.snapshots() });
  });
}

// ---- r5: a way back must actually be able to restore ------------------------

/** Sequential real updates with retention on (keep 2), as a host runs them. */
async function updatesWithRetention(fixture: Home, operations: ReadonlyArray<readonly [string, string]>) {
  for (const [operationId, version] of operations) {
    appendRow(fixture.ledger, `before-${operationId}`);
    await fixture.manager().update({ operationId, artifact: fixture.artifact(version) });
  }
}

const runtimeDirectory = (fixture: Home, version: string) => path.join(fixture.lifecycleRoot, "versions", version);
const runtimeExecutable = (fixture: Home, version: string) =>
  path.join(runtimeDirectory(fixture, version), `darwin-${ARCHITECTURE}`, "bin", "plimsoll.mjs");
const snapshotMetadata = (fixture: Home, id: string) =>
  JSON.parse(fs.readFileSync(path.join(fixture.lifecycleRoot, "snapshots", id, "snapshot.json"), "utf8")) as Record<string, unknown>;

async function r5UsableWaysBack() {
  // The review's repro: retention keeps a2 (restores 1.0.1) and a3 (restores
  // 1.0.2); runtime 1.0.1 then goes missing. A forced seal keeping only a2
  // would let prune delete a3, the last snapshot that can restore anything.
  await runCase([CASES.r5WayBack[0]], async (record) => {
    const fixture = createHome("r5-runtime-gone");
    await updatesWithRetention(fixture, [["a1", "20.1.1"], ["a2", "20.1.2"], ["a3", "20.1.3"]]);
    const kept = fixture.snapshots();
    fs.rmSync(runtimeDirectory(fixture, "20.1.1"), { recursive: true, force: true });
    const orderBefore = fs.readFileSync(orderPath(fixture));
    const dry = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "rg-dry", keep: ["a2"] }));
    const forced = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "rg-forced", keep: ["a2"], apply: true, force: true }));
    const unchanged = fs.readFileSync(orderPath(fixture)).equals(orderBefore) && fixture.receipt("rg-forced", "snapshots_reconcile") === null;
    const prunedFirst = await fixture.manager().pruneSnapshots({ operationId: "rg-prune-1", keep: 2, apply: true });
    const afterFirstPrune = fixture.snapshots();
    // Keeping the snapshot that can restore is still possible (forced: nothing needs a keep-set here).
    const sealed = await fixture.manager().reconcileSnapshots({ operationId: "rg-seal", keep: ["a3"], apply: true, force: true });
    await fixture.manager().pruneSnapshots({ operationId: "rg-prune-2", keep: 2, apply: true });
    const refusal = /must keep a way back.*a2 cannot restore 20\.1\.1: runtime 20\.1\.1 executable is missing/;
    record(CASES.r5WayBack[0],
      same(kept, ["a2", "a3"]) && refusal.test(dry?.message ?? "") && refusal.test(forced?.message ?? "") && unchanged &&
        prunedFirst.retention.removed.length === 0 && same(afterFirstPrune, ["a2", "a3"]) &&
        sealed.reconcile.unusableWaysBack.some((row) => row.id === "a2" && /executable is missing/.test(row.reason)) &&
        same(fixture.snapshots(), ["a3"]) && exists(runtimeExecutable(fixture, "20.1.2")),
      { kept, dry: dry?.message ?? "accepted", forced: forced?.message ?? "accepted", unchanged, prunedFirst: prunedFirst.retention.removed,
        sealed: sealed.reconcile, remains: fixture.snapshots() });
  });
  // The runtime is there but is no longer what the snapshot recorded.
  await runCase([CASES.r5WayBack[1]], async (record) => {
    const fixture = createHome("r5-runtime-changed");
    await updatesWithRetention(fixture, [["b1", "21.1.1"], ["b2", "21.1.2"], ["b3", "21.1.3"]]);
    const recorded = snapshotMetadata(fixture, "b2").currentExecutableSha256;
    fs.appendFileSync(runtimeExecutable(fixture, "21.1.1"), "// changed after the snapshot\n");
    const dry = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "rc-dry", keep: ["b2"], force: true }));
    record(CASES.r5WayBack[1],
      typeof recorded === "string" && /^sha256:[0-9a-f]{64}$/.test(recorded) &&
        /b2 cannot restore 21\.1\.1: runtime 21\.1\.1 executable no longer matches the digest the snapshot recorded/.test(dry?.message ?? ""),
      { recorded, dry: dry?.message ?? "accepted" });
  });
  // The snapshot's own database copy is gone.
  await runCase([CASES.r5WayBack[2]], async (record) => {
    const fixture = createHome("r5-copy-gone");
    await updatesWithRetention(fixture, [["c1", "22.1.1"], ["c2", "22.1.2"], ["c3", "22.1.3"]]);
    fs.rmSync(path.join(fixture.lifecycleRoot, "snapshots", "c2", "database"));
    const dry = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "cg-dry", keep: ["c2"], force: true }));
    record(CASES.r5WayBack[2],
      /c2 cannot restore 22\.1\.1: its database copy is missing/.test(dry?.message ?? ""),
      { dry: dry?.message ?? "accepted" });
  });
  // Retention alone: with --keep 1 the newest snapshot is also the newest way
  // back, but its runtime is gone. The older one that can restore stays.
  await runCase([CASES.r5WayBack[3]], async (record) => {
    const fixture = createHome("r5-newest-unusable");
    await updatesWithRetention(fixture, [["d1", "23.1.1"], ["d2", "23.1.2"], ["d3", "23.1.3"]]);
    fs.rmSync(runtimeDirectory(fixture, "23.1.2"), { recursive: true, force: true });
    const pruned = await fixture.manager().pruneSnapshots({ operationId: "nu-prune", keep: 1, apply: true });
    const listed = await listDecisions(fixture, 1);
    record(CASES.r5WayBack[3],
      pruned.retention.removed.filter((item) => item.kind === "snapshot").length === 0 && same(fixture.snapshots(), ["d2", "d3"]) &&
        listed.decisions.d2 === "keep:restores_previous_version" && exists(runtimeExecutable(fixture, "23.1.1")),
      { pruned: pruned.retention.removed, listed, remains: fixture.snapshots() });
  });
  // A snapshot taken before 0.7.41 recorded no runtime digest: it counts while
  // its runtime is there, and not once it is gone.
  await runCase([CASES.r5WayBack[4]], async (record) => {
    const fixture = createHome("r5-legacy-snapshot");
    await updatesWithRetention(fixture, [["e1", "24.1.1"], ["e2", "24.1.2"], ["e3", "24.1.3"]]);
    const { currentExecutableSha256: _recorded, ...legacy } = snapshotMetadata(fixture, "e2");
    fs.writeFileSync(path.join(fixture.lifecycleRoot, "snapshots", "e2", "snapshot.json"), `${JSON.stringify(legacy, null, 2)}\n`);
    const present = await fixture.manager().reconcileSnapshots({ operationId: "ls-present", keep: ["e2"] });
    fs.rmSync(runtimeDirectory(fixture, "24.1.1"), { recursive: true, force: true });
    const gone = await rejection(() => fixture.manager().reconcileSnapshots({ operationId: "ls-gone", keep: ["e2"], force: true }));
    record(CASES.r5WayBack[4],
      present.reconcile.unusableWaysBack.length === 0 && present.reconcile.newestWayBack === "e3" &&
        /e2 cannot restore 24\.1\.1: runtime 24\.1\.1 executable is missing/.test(gone?.message ?? ""),
      { present: present.reconcile, gone: gone?.message ?? "accepted" });
  });
}

/** Rewrites one receipt as 0.7.37 and earlier wrote it, without the sequence it reserved. */
function asOlderCliMarker(fixture: Home, id: string) {
  const legacyKeys = ["schemaVersion", "toolVersion", "operationId", "operation", "status", "fromVersion", "toVersion",
    "restoredVersion", "health", "ownedTargets", "retainedTargets", "purgeOnlyTargets", "preserved"];
  const marker = readMarker(fixture, id);
  writeMarker(fixture, id, Object.fromEntries(legacyKeys.map((key) => [key, marker[key]])));
  const order = JSON.parse(fs.readFileSync(orderPath(fixture), "utf8")) as { lastSequence: number };
  fs.writeFileSync(orderPath(fixture), JSON.stringify({ ...order, lastSequence: order.lastSequence - 1 }));
}

async function main() {
  try {
    await b1NoSplitBrainRollback();
    await b2RestoreIsAtomicAndCapacityChecked();
    await b3StrictCompletionReceipts();
    await b4OrderSurvivesClockSteps();
    await b5RemovalsAreDurablyRecorded();
    await preflightAndCloneHelper();
    await r3LeaseAndFence();
    await pruneRecoveryGuard();
    await r8PruneSafety();
    await r3OpenHandles();
    await r3PreexistingDamage();
    await r3BlockedRetentionRepair();
    await r3CrashLeftovers();
    await r4FailClosedChecks();
    await r4PlausibleRebuild();
    await r4TighterSeals();
    await r4ReceiptsWithoutSequence();
    await r5UsableWaysBack();
    const failed = results.filter((row) => !row.passed).map((row) => row.name);
    console.log(JSON.stringify({ proof: "lifecycle-data-safety", checks: results.length, passed: results.length - failed.length, failed, liveStateTouched: false }));
  } finally {
    fs.rmSync(ROOT, { recursive: true, force: true });
  }
  completion.complete();
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
