#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  captureLaunchAgentUnloadPriorState,
  observeLaunchAgentUnloadTerminalState,
  readCollectorPidFile,
  removeCollectorPidFileIfOwned,
  runtimeIdentityMatches,
  type CollectorListenerObservation,
  type CollectorPidFileRead,
  type CollectorRuntimeIdentity,
  type LaunchAgentLabelObservation,
  type LaunchAgentUnloadOutcome,
  type ProcessIdentity,
} from "../packages/collector-cli/src/runtime-ownership";

const LABEL = "com.plimsoll.collector";
const PID_PATH = "/fixture/collector.pid";
const PORT = 48_271;

type Snapshot = {
  label: LaunchAgentLabelObservation;
  listener: CollectorListenerObservation;
  pid: CollectorPidFileRead;
  live: CollectorRuntimeIdentity[];
};

type Scenario = {
  name: string;
  prior: Snapshot;
  snapshots: Snapshot[];
  receiptBoundarySnapshot?: Snapshot;
  timeoutMs?: number;
  pollIntervalMs?: number;
  removeOwnedPid?: boolean;
};

function identity(seed: number): CollectorRuntimeIdentity {
  return {
    instanceId: `00000000-0000-4000-8000-${String(seed).padStart(12, "0")}`,
    pid: 10_000 + seed,
    processStartFingerprint: `sha256:${seed.toString(16).padStart(64, "0")}`,
  };
}

function fixtureFileIdentity(owner: CollectorRuntimeIdentity) {
  return {
    device: 1,
    inode: owner.pid,
    mode: 0o100600,
    uid: 501,
    gid: 20,
    links: 1,
    size: 20,
    modifiedMs: 1,
    changedMs: 1,
  };
}

function current(owner: CollectorRuntimeIdentity): CollectorPidFileRead {
  return {
    kind: "current",
    fileIdentity: fixtureFileIdentity(owner),
    raw: "fixture-owned-record",
    record: {
      ...owner,
      command: ["fixture-collector"],
      cwd: "/fixture",
      label: LABEL,
      startedAt: "2026-07-19T00:00:00.000Z",
      version: 2,
    },
  };
}

const missing = (): CollectorPidFileRead => ({ kind: "missing" });
const absent = (): CollectorListenerObservation => ({ kind: "absent" });
const reported = (owner: CollectorRuntimeIdentity | null): LaunchAgentLabelObservation => ({
  kind: "reported",
  processIdentity: owner,
});
const gone = (): LaunchAgentLabelObservation => ({ kind: "not_reported" });

async function runScenario(scenario: Scenario) {
  let clock = 0;
  let clockReads = 0;
  let snapshotIndex = 0;
  let activeSnapshot = scenario.snapshots[0]!;
  let receiptBoundaryActive = false;
  let removals = 0;
  const snapshot = () => activeSnapshot;
  const observeLabel = () => {
    activeSnapshot = receiptBoundaryActive && scenario.receiptBoundarySnapshot
      ? scenario.receiptBoundarySnapshot
      : scenario.snapshots[Math.min(snapshotIndex, scenario.snapshots.length - 1)]!;
    return activeSnapshot.label;
  };
  const observeListener = async () => {
    const listener = snapshot().listener;
    snapshotIndex += 1;
    return listener;
  };
  const isLive = (candidate: ProcessIdentity) =>
    snapshot().live.some((owner) =>
      owner.pid === candidate.pid &&
      owner.processStartFingerprint === candidate.processStartFingerprint
    );
  const readPidFile = () => snapshot().pid;
  const removePidFile = (_pidPath: string, candidate: CollectorRuntimeIdentity) => {
    if (!scenario.removeOwnedPid) return false;
    const read = snapshot().pid;
    if (read.kind !== "current" || !runtimeIdentityMatches(read.record, candidate)) return false;
    snapshot().pid = missing();
    removals += 1;
    return true;
  };

  const prior = await captureLaunchAgentUnloadPriorState({
    label: LABEL,
    pidPath: PID_PATH,
    port: PORT,
    observeLabel: () => scenario.prior.label,
    observeListener: async () => scenario.prior.listener,
    readPidFile: () => scenario.prior.pid,
  });
  const outcome = await observeLaunchAgentUnloadTerminalState({
    label: LABEL,
    pidPath: PID_PATH,
    port: PORT,
    prior,
    timeoutMs: scenario.timeoutMs ?? 100,
    pollIntervalMs: scenario.pollIntervalMs ?? 50,
    observeLabel,
    observeListener,
    processIsLive: isLive,
    readPidFile,
    removePidFile,
    now: () => {
      clockReads += 1;
      if (clockReads === 2 && scenario.receiptBoundarySnapshot) {
        receiptBoundaryActive = true;
        activeSnapshot = scenario.receiptBoundarySnapshot;
      }
      return clock;
    },
    poll: async (milliseconds) => {
      clock += milliseconds;
    },
  });
  return { prior, outcome, removals };
}

function terminal(owner: CollectorRuntimeIdentity): Snapshot {
  return { label: gone(), listener: absent(), pid: missing(), live: [] };
}

function stopping(owner: CollectorRuntimeIdentity): Snapshot {
  return {
    label: reported(null),
    listener: { kind: "collector", runtimeIdentity: owner },
    pid: current(owner),
    live: [owner],
  };
}

const checks: Array<{ name: string; detail: string }> = [];
function check(name: string, condition: unknown, detail: string) {
  assert.ok(condition, `${name}: ${detail}`);
  checks.push({ name, detail });
}

async function main() {
  assert.equal(process.versions.node.split(".")[0], "22", "unload proof requires exact Node 22");
  const owner = identity(1);
  const wrapper = identity(2);
  const foreign = identity(3);

  const immediate = await runScenario({
    name: "immediate",
    prior: stopping(owner),
    snapshots: [terminal(owner)],
  });
  check(
    "immediate_exit_is_stopped",
    immediate.outcome.stopped && immediate.outcome.timing.observations === 2,
    "A terminal candidate is confirmed by the mandatory receipt-boundary observation.",
  );

  const beforeDeadline = await runScenario({
    name: "before-deadline",
    prior: stopping(owner),
    snapshots: [stopping(owner), terminal(owner)],
  });
  check(
    "delayed_exit_before_deadline_is_stopped",
    beforeDeadline.outcome.stopped && !beforeDeadline.outcome.timing.deadlineCrossed,
    "A graceful exit before the deadline remains a successful stopped receipt.",
  );

  const atReceiptBoundary = await runScenario({
    name: "receipt-boundary",
    prior: stopping(owner),
    snapshots: [stopping(owner), stopping(owner), stopping(owner), terminal(owner)],
  });
  check(
    "terminal_between_last_poll_and_receipt_is_stopped",
    atReceiptBoundary.outcome.stopped &&
      atReceiptBoundary.outcome.timing.deadlineCrossed &&
      atReceiptBoundary.outcome.timing.finalObservation,
    "The mandatory receipt-time observation closes the deadline race.",
  );

  const afterFinal = await runScenario({
    name: "after-final",
    prior: stopping(owner),
    snapshots: [stopping(owner)],
  });
  check(
    "truly_live_process_remains_a_failure",
    !afterFinal.outcome.stopped && afterFinal.outcome.state === "still_stopping",
    "A label, listener, process, and PID record still live after the final observation fail closed.",
  );

  const reappearedAtReceiptBoundary = await runScenario({
    name: "terminal-then-reappeared",
    prior: stopping(owner),
    snapshots: [terminal(owner)],
    receiptBoundarySnapshot: stopping(foreign),
  });
  check(
    "owner_reappearing_at_receipt_boundary_is_not_reported_stopped",
    !reappearedAtReceiptBoundary.outcome.stopped &&
      reappearedAtReceiptBoundary.outcome.state === "live_conflict" &&
      runtimeIdentityMatches(
        reappearedAtReceiptBoundary.outcome.final.currentPidRuntimeIdentity,
        foreign,
      ) &&
      runtimeIdentityMatches(
        reappearedAtReceiptBoundary.outcome.final.currentListenerRuntimeIdentity,
        foreign,
      ),
    "The final aggregate observation runs after the receipt clock and records the newly live owner.",
  );

  const listenerFirst = await runScenario({
    name: "listener-first",
    prior: stopping(owner),
    snapshots: [
      { label: gone(), listener: absent(), pid: current(owner), live: [owner] },
      terminal(owner),
    ],
  });
  check(
    "listener_close_before_pid_cleanup_converges",
    listenerFirst.outcome.stopped && listenerFirst.outcome.timing.observations === 3,
    "Listener closure alone is still_stopping until process/PID cleanup also completes.",
  );

  const pidFirst = await runScenario({
    name: "pid-first",
    prior: stopping(owner),
    snapshots: [
      { label: gone(), listener: absent(), pid: missing(), live: [owner] },
      terminal(owner),
    ],
  });
  check(
    "pid_cleanup_before_process_reap_converges",
    pidFirst.outcome.stopped && pidFirst.outcome.timing.observations === 3,
    "PID cleanup alone is not terminal while the captured runtime identity remains live.",
  );

  const wrapperPrior: Snapshot = {
    label: reported(wrapper),
    listener: { kind: "collector", runtimeIdentity: owner },
    pid: current(owner),
    live: [wrapper, owner],
  };
  const wrapperOutcome = await runScenario({
    name: "wrapper-child",
    prior: wrapperPrior,
    snapshots: [terminal(owner)],
  });
  check(
    "wrapper_parent_and_collector_child_are_one_owned_layout",
    wrapperOutcome.prior.ownership === "consistent" &&
      wrapperOutcome.outcome.stopped &&
      wrapperOutcome.outcome.final.priorRuntimeCount === 2,
    "The launchd wrapper and exact listener/PID child are observed separately, not treated as duplicates.",
  );

  const alreadyUnloaded = await runScenario({
    name: "already-unloaded",
    prior: terminal(owner),
    snapshots: [terminal(owner)],
    timeoutMs: 0,
  });
  check(
    "already_unloaded_is_idempotent_success",
    alreadyUnloaded.outcome.stopped && alreadyUnloaded.outcome.pidCleaned,
    "An absent label, listener, process, and PID record is already stopped.",
  );

  const unrelated = await runScenario({
    name: "unrelated-listener",
    prior: { label: gone(), listener: { kind: "unrelated" }, pid: current(owner), live: [] },
    snapshots: [{ label: gone(), listener: { kind: "unrelated" }, pid: current(owner), live: [] }],
    timeoutMs: 0,
    removeOwnedPid: true,
  });
  check(
    "unrelated_listener_is_retained_and_refused",
    !unrelated.outcome.stopped && unrelated.outcome.state === "unrelated_listener" && unrelated.removals === 0,
    "An occupied port without the exact collector identity is never promoted or mutated.",
  );

  const stalePid = await runScenario({
    name: "stale-pid",
    prior: { label: gone(), listener: absent(), pid: current(owner), live: [] },
    snapshots: [{ label: gone(), listener: absent(), pid: current(owner), live: [] }],
    timeoutMs: 0,
    removeOwnedPid: true,
  });
  check(
    "stale_exact_pid_record_is_safely_removed",
    stalePid.outcome.stopped && stalePid.removals === 1 && stalePid.outcome.removedPidFile,
    "A dead exact identity authorizes only compare-and-remove of its unchanged PID record.",
  );

  const pidReuse = await runScenario({
    name: "pid-reuse",
    prior: { label: gone(), listener: absent(), pid: current(owner), live: [foreign] },
    snapshots: [{ label: gone(), listener: absent(), pid: current(owner), live: [foreign] }],
    timeoutMs: 0,
    removeOwnedPid: true,
  });
  check(
    "pid_reuse_does_not_keep_or_signal_the_foreign_process",
    pidReuse.outcome.stopped && pidReuse.removals === 1,
    "The reused PID has a different fingerprint; only the stale exact record is removed.",
  );

  const staleUnproven = await runScenario({
    name: "legacy-record",
    prior: {
      label: gone(),
      listener: absent(),
      pid: {
        kind: "legacy",
        fileIdentity: fixtureFileIdentity(owner),
        pid: owner.pid,
        raw: String(owner.pid),
      },
      live: [],
    },
    snapshots: [{
      label: gone(),
      listener: absent(),
      pid: {
        kind: "legacy",
        fileIdentity: fixtureFileIdentity(owner),
        pid: owner.pid,
        raw: String(owner.pid),
      },
      live: [],
    }],
    timeoutMs: 0,
    removeOwnedPid: true,
  });
  check(
    "unproven_pid_record_is_retained",
    !staleUnproven.outcome.stopped &&
      staleUnproven.outcome.state === "stale_owned_record" &&
      staleUnproven.removals === 0,
    "Legacy residue has no exact identity authority and remains untouched.",
  );

  const ambiguous = await runScenario({
    name: "ambiguous-owner",
    prior: {
      label: reported(null),
      listener: { kind: "collector", runtimeIdentity: foreign },
      pid: current(owner),
      live: [owner, foreign],
    },
    snapshots: [{ label: gone(), listener: absent(), pid: current(owner), live: [] }],
    timeoutMs: 0,
    removeOwnedPid: true,
  });
  check(
    "ambiguous_prior_owner_never_becomes_stopped",
    ambiguous.prior.ownership === "ambiguous" &&
      !ambiguous.outcome.stopped &&
      ambiguous.outcome.state === "ambiguous_prior_owner" &&
      ambiguous.removals === 0,
    "A listener/PID identity disagreement fails closed even if later state appears empty.",
  );

  const indeterminate = await runScenario({
    name: "query-failure",
    prior: { label: { kind: "query_failed" }, listener: absent(), pid: missing(), live: [] },
    snapshots: [{ label: { kind: "query_failed" }, listener: absent(), pid: missing(), live: [] }],
    timeoutMs: 0,
  });
  check(
    "label_query_failure_is_indeterminate",
    !indeterminate.outcome.stopped && indeterminate.outcome.state === "indeterminate",
    "A launchctl query failure cannot become an absent-label claim.",
  );

  const unsafePid = await runScenario({
    name: "unsafe-pid-file",
    prior: {
      label: gone(),
      listener: absent(),
      pid: { kind: "unsafe", reason: "leaf_symlink" },
      live: [],
    },
    snapshots: [{
      label: gone(),
      listener: absent(),
      pid: { kind: "unsafe", reason: "leaf_symlink" },
      live: [],
    }],
    timeoutMs: 0,
  });
  check(
    "unsafe_pid_file_is_indeterminate",
    unsafePid.prior.ownership === "unproven" &&
      !unsafePid.outcome.stopped &&
      unsafePid.outcome.state === "indeterminate",
    "Unsafe PID-file state is retained as an ownership blocker, never promoted to missing.",
  );

  const pidFixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-unload-pid-"));
  try {
    fs.chmodSync(pidFixtureRoot, 0o700);
    const owned = current(owner);
    const replacement = current(foreign);
    assert.equal(owned.kind, "current");
    assert.equal(replacement.kind, "current");
    const ownedRaw = `${JSON.stringify(owned.record, null, 2)}\n`;
    const foreignRaw = `${JSON.stringify(replacement.record, null, 2)}\n`;
    const symlinkTarget = path.join(pidFixtureRoot, "symlink-target");
    const symlinkPid = path.join(pidFixtureRoot, "symlink.pid");
    fs.writeFileSync(symlinkTarget, ownedRaw, { mode: 0o600 });
    fs.symlinkSync(symlinkTarget, symlinkPid);
    const symlinkRead = readCollectorPidFile(symlinkPid, LABEL);
    const symlinkRemoved = removeCollectorPidFileIfOwned(symlinkPid, owner, LABEL);
    check(
      "pid_symlink_is_rejected_without_target_mutation",
      symlinkRead.kind === "unsafe" &&
        symlinkRead.reason === "leaf_symlink" &&
        !symlinkRemoved &&
        fs.lstatSync(symlinkPid).isSymbolicLink() &&
        fs.readFileSync(symlinkTarget, "utf8") === ownedRaw,
      "No-follow inspection leaves both the symlink and its target byte-exact.",
    );

    const permissivePid = path.join(pidFixtureRoot, "permissive.pid");
    fs.writeFileSync(permissivePid, ownedRaw, { mode: 0o644 });
    fs.chmodSync(permissivePid, 0o644);
    const permissiveRead = readCollectorPidFile(permissivePid, LABEL);
    check(
      "pid_file_with_unsafe_mode_is_retained",
      permissiveRead.kind === "unsafe" &&
        permissiveRead.reason === "mode" &&
        !removeCollectorPidFileIfOwned(permissivePid, owner, LABEL) &&
        fs.readFileSync(permissivePid, "utf8") === ownedRaw,
      "A non-private ownership record cannot authorize cleanup.",
    );

    const swapPid = path.join(pidFixtureRoot, "swap.pid");
    const preservedOwner = path.join(pidFixtureRoot, "preserved-owner.pid");
    fs.writeFileSync(swapPid, ownedRaw, { mode: 0o600 });
    const swapRemoved = removeCollectorPidFileIfOwned(swapPid, owner, LABEL, {
      beforeClaim: () => {
        fs.renameSync(swapPid, preservedOwner);
        fs.writeFileSync(swapPid, foreignRaw, { mode: 0o600 });
      },
    });
    check(
      "pid_swap_before_claim_preserves_both_objects",
      !swapRemoved &&
        fs.readFileSync(preservedOwner, "utf8") === ownedRaw &&
        fs.readFileSync(swapPid, "utf8") === foreignRaw &&
        readCollectorPidFile(swapPid, LABEL).kind === "current",
      "Identity-bound rename-claim cleanup restores the replacement and retains the original object.",
    );
  } finally {
    fs.rmSync(pidFixtureRoot, { recursive: true, force: true });
  }

  const pathFreeMaterial = JSON.stringify({
    prior: stalePid.prior,
    outcome: stalePid.outcome,
  });
  check(
    "unload_receipts_are_path_free",
    !pathFreeMaterial.includes(PID_PATH) && !pathFreeMaterial.includes(process.cwd()),
    "Prior and terminal receipts contain state and identity evidence but no filesystem path.",
  );

  const receipt = {
    issue: 143,
    ok: true,
    proof: "launch-agent-unload-terminal-truth",
    node: process.version,
    checks,
  };
  process.stdout.write(`${JSON.stringify(receipt, null, 2)}\n`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
