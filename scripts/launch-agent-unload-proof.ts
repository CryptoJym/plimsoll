#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  captureLaunchAgentUnloadPriorState,
  observeLaunchAgentUnloadTerminalState,
  readCollectorPidCleanupState,
  readCollectorPidFile,
  reconcileCollectorPidCleanupState,
  removeCollectorPidFileIfOwned,
  removeCollectorPidFileIfOwnedDetailed,
  runtimeIdentityMatches,
  type CollectorListenerObservation,
  type CollectorPidCleanupResult,
  type CollectorPidFileRead,
  type CollectorRuntimeIdentity,
  type LaunchAgentLabelObservation,
  type LaunchAgentUnloadOutcome,
  type ProcessIdentity,
} from "../packages/collector-cli/src/runtime-ownership";

const LABEL = "com.plimsoll.collector";
const PID_PATH = "/fixture/collector.pid";
const PORT = 48_271;

function cleanupSlots(pidPath: string) {
  const directory = path.dirname(pidPath);
  const basename = path.basename(pidPath);
  return {
    marker: path.join(directory, `.${basename}.plimsoll-cleanup-marker`),
    pidClaim: path.join(directory, `.${basename}.plimsoll-remove-claim`),
    markerClaim: path.join(directory, `.${basename}.plimsoll-cleanup-marker-claim`),
    quarantine: path.join(directory, `.${basename}.plimsoll-quarantine`),
  };
}

function makeMarkerActorDead(markerPath: string) {
  const marker = JSON.parse(fs.readFileSync(markerPath, "utf8")) as {
    actor: { pid: number; processStartFingerprint: string };
  };
  marker.actor.processStartFingerprint = `sha256:${"0".repeat(64)}`;
  fs.writeFileSync(markerPath, `${JSON.stringify(marker)}\n`, { mode: 0o600 });
}

function fixtureCleanupResult(removed: boolean): CollectorPidCleanupResult {
  return {
    removed,
    ambiguous: false,
    quarantined: false,
    persistent: {
      ambiguous: false,
      markerState: "missing",
      claimCount: 0,
      quarantineCount: 0,
      inventoryTruncated: false,
      unsafeArtifactCount: 0,
    },
    disposition: removed ? "removed" : "not_owned",
  };
}

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
    if (!scenario.removeOwnedPid) return fixtureCleanupResult(false);
    const read = snapshot().pid;
    if (read.kind !== "current" || !runtimeIdentityMatches(read.record, candidate)) {
      return fixtureCleanupResult(false);
    }
    snapshot().pid = missing();
    removals += 1;
    return fixtureCleanupResult(true);
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
      atReceiptBoundary.outcome.timing.finalObservationPerformed,
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
        !symlinkRemoved.removed &&
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
        !removeCollectorPidFileIfOwned(permissivePid, owner, LABEL).removed &&
        fs.readFileSync(permissivePid, "utf8") === ownedRaw,
      "A non-private ownership record cannot authorize cleanup.",
    );

    const swapPid = path.join(pidFixtureRoot, "swap.pid");
    const preservedOwner = path.join(pidFixtureRoot, "preserved-owner.pid");
    fs.writeFileSync(swapPid, ownedRaw, { mode: 0o600 });
    let regularReplacementInode = 0;
    const swapCleanup = removeCollectorPidFileIfOwnedDetailed(swapPid, owner, LABEL, {
      beforeClaim: () => {
        fs.renameSync(swapPid, preservedOwner);
        fs.writeFileSync(swapPid, foreignRaw, { mode: 0o600 });
        regularReplacementInode = fs.lstatSync(swapPid).ino;
      },
    });
    const swapCleanupState = readCollectorPidCleanupState(swapPid, LABEL);
    check(
      "pid_swap_before_claim_preserves_both_objects",
      !swapCleanup.removed &&
        swapCleanup.ambiguous &&
        !swapCleanup.quarantined &&
        swapCleanup.disposition === "preclaim_changed" &&
        swapCleanupState.markerState === "present" &&
        swapCleanupState.ambiguous &&
        fs.readFileSync(preservedOwner, "utf8") === ownedRaw &&
        fs.readFileSync(swapPid, "utf8") === foreignRaw &&
        fs.lstatSync(swapPid).ino === regularReplacementInode &&
        readCollectorPidFile(swapPid, LABEL).kind === "current",
      "The no-follow preclaim identity check leaves the exact regular replacement visible.",
    );
    const liveMarkerReconciliation = reconcileCollectorPidCleanupState(swapPid, LABEL);
    check(
      "live_cleanup_actor_marker_refuses_reconciliation",
      !liveMarkerReconciliation.reconciled &&
        !liveMarkerReconciliation.eligible &&
        liveMarkerReconciliation.disposition === "actor_live" &&
        liveMarkerReconciliation.after.markerState === "present",
      "A marker owned by the still-live exact cleanup actor remains explicit ambiguity.",
    );

    const midSymlinkPid = path.join(pidFixtureRoot, "mid-symlink.pid");
    const midSymlinkOwner = path.join(pidFixtureRoot, "mid-symlink-owner.pid");
    const midSymlinkTarget = path.join(pidFixtureRoot, "mid-symlink-target");
    fs.writeFileSync(midSymlinkPid, ownedRaw, { mode: 0o600 });
    fs.writeFileSync(midSymlinkTarget, foreignRaw, { mode: 0o600 });
    let symlinkReplacementInode = 0;
    const midSymlinkCleanup = removeCollectorPidFileIfOwnedDetailed(
      midSymlinkPid,
      owner,
      LABEL,
      {
        beforeClaim: () => {
          fs.renameSync(midSymlinkPid, midSymlinkOwner);
          fs.symlinkSync(midSymlinkTarget, midSymlinkPid);
          symlinkReplacementInode = fs.lstatSync(midSymlinkPid).ino;
        },
      },
    );
    const midSymlinkHidden = fs.readdirSync(pidFixtureRoot).filter((entry) =>
      entry.includes(`${path.basename(midSymlinkPid)}.plimsoll-`)
    );
    const midSymlinkCleanupState = readCollectorPidCleanupState(midSymlinkPid, LABEL);
    check(
      "mid_claim_symlink_replacement_stays_visible_and_unfollowed",
      !midSymlinkCleanup.removed &&
        midSymlinkCleanup.ambiguous &&
        !midSymlinkCleanup.quarantined &&
        fs.lstatSync(midSymlinkPid).isSymbolicLink() &&
        fs.lstatSync(midSymlinkPid).ino === symlinkReplacementInode &&
        fs.readlinkSync(midSymlinkPid) === midSymlinkTarget &&
        fs.readFileSync(midSymlinkTarget, "utf8") === foreignRaw &&
        fs.readFileSync(midSymlinkOwner, "utf8") === ownedRaw &&
        midSymlinkHidden.length === 1 &&
        midSymlinkHidden[0] === `.${path.basename(midSymlinkPid)}.plimsoll-cleanup-marker` &&
        midSymlinkCleanupState.markerState === "present",
      "A symlink swapped before claim is never moved or followed; the private durable marker records unresolved cleanup.",
    );

    const disappearedPid = path.join(pidFixtureRoot, "disappeared.pid");
    const disappearedOwner = path.join(pidFixtureRoot, "disappeared-owner.pid");
    fs.writeFileSync(disappearedPid, ownedRaw, { mode: 0o600 });
    const disappearedCleanup = removeCollectorPidFileIfOwnedDetailed(
      disappearedPid,
      owner,
      LABEL,
      {
        beforeClaim: () => fs.renameSync(disappearedPid, disappearedOwner),
      },
    );
    const disappearedCleanupState = readCollectorPidCleanupState(disappearedPid, LABEL);
    check(
      "replacement_disappearing_before_claim_latches_ambiguity",
      !disappearedCleanup.removed &&
        disappearedCleanup.ambiguous &&
        !disappearedCleanup.quarantined &&
        disappearedCleanup.disposition === "preclaim_changed" &&
        disappearedCleanupState.markerState === "present" &&
        !fs.existsSync(disappearedPid) &&
        fs.readFileSync(disappearedOwner, "utf8") === ownedRaw,
      "A missing preclaim path is an ambiguous race, not successful cleanup.",
    );

    const collisionPid = path.join(pidFixtureRoot, "collision.pid");
    const collisionBytes = "operator-collision\n";
    fs.writeFileSync(collisionPid, ownedRaw, { mode: 0o600 });
    const collisionCleanup = removeCollectorPidFileIfOwnedDetailed(
      collisionPid,
      owner,
      LABEL,
      {
        afterClaim: () => {
          fs.writeFileSync(collisionPid, collisionBytes, { mode: 0o600 });
        },
      },
    );
    const collisionQuarantines = fs.readdirSync(pidFixtureRoot).filter((entry) =>
      entry === `.${path.basename(collisionPid)}.plimsoll-quarantine`
    );
    const collisionCleanupState = readCollectorPidCleanupState(collisionPid, LABEL);
    const quarantinedReplacement = collisionQuarantines[0]
      ? path.join(pidFixtureRoot, collisionQuarantines[0])
      : "";
    check(
      "restore_collision_preserves_visible_object_and_quarantines_exact_replacement",
      !collisionCleanup.removed &&
        collisionCleanup.ambiguous &&
        collisionCleanup.quarantined &&
        collisionCleanup.disposition === "destination_reappeared" &&
        collisionCleanupState.markerState === "present" &&
        collisionCleanupState.quarantineCount === 1 &&
        fs.readFileSync(collisionPid, "utf8") === collisionBytes &&
        collisionQuarantines.length === 1 &&
        fs.readFileSync(quarantinedReplacement, "utf8") === ownedRaw,
      "A destination collision is retained literally beside the fixed exact owned-record quarantine.",
    );
    makeMarkerActorDead(cleanupSlots(collisionPid).marker);
    const quarantinedReconciliation = reconcileCollectorPidCleanupState(
      collisionPid,
      LABEL,
    );
    check(
      "claim_or_quarantine_never_auto_reconciles",
      !quarantinedReconciliation.reconciled &&
        quarantinedReconciliation.disposition === "quarantine_present" &&
        quarantinedReconciliation.after.quarantineCount === 1 &&
        fs.readFileSync(collisionPid, "utf8") === collisionBytes &&
        fs.readFileSync(quarantinedReplacement, "utf8") === ownedRaw,
      "Even a dead actor cannot clear a quarantine or reinterpret the visible PID collision.",
    );

    const deadMarkerPid = path.join(pidFixtureRoot, "dead-marker.pid");
    const deadMarkerPreserved = path.join(pidFixtureRoot, "dead-marker-preserved.pid");
    fs.writeFileSync(deadMarkerPid, ownedRaw, { mode: 0o600 });
    const deadMarkerCleanup = removeCollectorPidFileIfOwnedDetailed(
      deadMarkerPid,
      owner,
      LABEL,
      { beforeClaim: () => fs.renameSync(deadMarkerPid, deadMarkerPreserved) },
    );
    const deadMarkerSlots = cleanupSlots(deadMarkerPid);
    makeMarkerActorDead(deadMarkerSlots.marker);
    fs.writeFileSync(deadMarkerPid, foreignRaw, { mode: 0o600 });
    const visibleBeforeReconciliation = {
      inode: fs.lstatSync(deadMarkerPid).ino,
      raw: fs.readFileSync(deadMarkerPid, "utf8"),
    };
    const deadMarkerReconciliation = reconcileCollectorPidCleanupState(
      deadMarkerPid,
      LABEL,
    );
    check(
      "dead_marker_only_actor_reconciles_without_touching_visible_pid",
      deadMarkerCleanup.disposition === "preclaim_changed" &&
        deadMarkerReconciliation.reconciled &&
        deadMarkerReconciliation.disposition === "marker_cleared" &&
        !deadMarkerReconciliation.after.ambiguous &&
        fs.lstatSync(deadMarkerPid).ino === visibleBeforeReconciliation.inode &&
        fs.readFileSync(deadMarkerPid, "utf8") === visibleBeforeReconciliation.raw &&
        fs.readFileSync(deadMarkerPreserved, "utf8") === ownedRaw,
      "Only the exact unchanged dead-actor marker is cleared; visible PID identity and bytes are untouched.",
    );

    const markerRacePid = path.join(pidFixtureRoot, "marker-race.pid");
    const markerRacePreservedPid = path.join(pidFixtureRoot, "marker-race-preserved.pid");
    fs.writeFileSync(markerRacePid, ownedRaw, { mode: 0o600 });
    removeCollectorPidFileIfOwnedDetailed(markerRacePid, owner, LABEL, {
      beforeClaim: () => fs.renameSync(markerRacePid, markerRacePreservedPid),
    });
    const markerRaceSlots = cleanupSlots(markerRacePid);
    makeMarkerActorDead(markerRaceSlots.marker);
    const markerRaceOriginal = path.join(pidFixtureRoot, "marker-race-original");
    const markerRaceReconciliation = reconcileCollectorPidCleanupState(
      markerRacePid,
      LABEL,
      {
        hooks: {
          beforeMarkerClaim: () => {
            fs.renameSync(markerRaceSlots.marker, markerRaceOriginal);
            fs.writeFileSync(markerRaceSlots.marker, "replacement-marker\n", { mode: 0o600 });
          },
        },
      },
    );
    check(
      "marker_replacement_before_claim_fails_closed",
      !markerRaceReconciliation.reconciled &&
        markerRaceReconciliation.disposition === "clear_race" &&
        markerRaceReconciliation.after.markerState === "unsafe" &&
        fs.readFileSync(markerRaceSlots.marker, "utf8") === "replacement-marker\n" &&
        fs.existsSync(markerRaceOriginal),
      "Reconciliation never deletes a marker-path replacement or claims it as the dead actor's marker.",
    );

    const clearRacePid = path.join(pidFixtureRoot, "clear-race.pid");
    const clearRacePreservedPid = path.join(pidFixtureRoot, "clear-race-preserved.pid");
    fs.writeFileSync(clearRacePid, ownedRaw, { mode: 0o600 });
    removeCollectorPidFileIfOwnedDetailed(clearRacePid, owner, LABEL, {
      beforeClaim: () => fs.renameSync(clearRacePid, clearRacePreservedPid),
    });
    const clearRaceSlots = cleanupSlots(clearRacePid);
    makeMarkerActorDead(clearRaceSlots.marker);
    const clearRaceOriginal = path.join(pidFixtureRoot, "clear-race-original");
    const clearRaceReconciliation = reconcileCollectorPidCleanupState(
      clearRacePid,
      LABEL,
      {
        hooks: {
          beforeClear: () => {
            fs.renameSync(clearRaceSlots.markerClaim, clearRaceOriginal);
            fs.writeFileSync(clearRaceSlots.markerClaim, "replacement-claim\n", { mode: 0o600 });
          },
        },
      },
    );
    check(
      "marker_claim_replacement_before_clear_fails_closed",
      !clearRaceReconciliation.reconciled &&
        clearRaceReconciliation.disposition === "clear_race" &&
        clearRaceReconciliation.after.claimCount === 1 &&
        clearRaceReconciliation.after.unsafeArtifactCount === 1 &&
        fs.readFileSync(clearRaceSlots.markerClaim, "utf8") === "replacement-claim\n" &&
        fs.existsSync(clearRaceOriginal),
      "The exact marker claim is reverified before unlink; a replacement remains explicit ambiguity.",
    );

    const markerClaimPid = path.join(pidFixtureRoot, "marker-claim-only.pid");
    const markerClaimPreservedPid = path.join(
      pidFixtureRoot,
      "marker-claim-only-preserved.pid",
    );
    fs.writeFileSync(markerClaimPid, ownedRaw, { mode: 0o600 });
    removeCollectorPidFileIfOwnedDetailed(markerClaimPid, owner, LABEL, {
      beforeClaim: () => fs.renameSync(markerClaimPid, markerClaimPreservedPid),
    });
    const markerClaimSlots = cleanupSlots(markerClaimPid);
    makeMarkerActorDead(markerClaimSlots.marker);
    fs.renameSync(markerClaimSlots.marker, markerClaimSlots.markerClaim);
    const markerClaimReconciliation = reconcileCollectorPidCleanupState(
      markerClaimPid,
      LABEL,
    );
    check(
      "dead_marker_claim_only_actor_reconciles_exact_slot",
      markerClaimReconciliation.reconciled &&
        markerClaimReconciliation.disposition === "marker_claim_cleared" &&
        !markerClaimReconciliation.after.ambiguous &&
        fs.readFileSync(markerClaimPreservedPid, "utf8") === ownedRaw,
      "A valid dead-actor marker claim is cleared only from its exact fixed slot.",
    );

    const fixedClaimPid = path.join(pidFixtureRoot, "fixed-claim.pid");
    const fixedClaimSlots = cleanupSlots(fixedClaimPid);
    fs.writeFileSync(fixedClaimSlots.pidClaim, ownedRaw, { mode: 0o600 });
    const fixedClaimState = readCollectorPidCleanupState(fixedClaimPid, LABEL);
    const fixedClaimReconciliation = reconcileCollectorPidCleanupState(
      fixedClaimPid,
      LABEL,
    );
    check(
      "matching_fixed_claim_slot_is_counted_and_never_auto_cleared",
      fixedClaimState.ambiguous &&
        fixedClaimState.claimCount === 1 &&
        fixedClaimReconciliation.disposition === "pid_claim_present" &&
        fs.readFileSync(fixedClaimSlots.pidClaim, "utf8") === ownedRaw,
      "The exact PID-claim slot counts immediately and is never reconciled as marker-only residue.",
    );

    const aggregateSymlinkPid = path.join(pidFixtureRoot, "aggregate-symlink.pid");
    const aggregateSymlinkOwner = path.join(pidFixtureRoot, "aggregate-symlink-owner.pid");
    const aggregateSymlinkTarget = path.join(pidFixtureRoot, "aggregate-symlink-target");
    fs.writeFileSync(aggregateSymlinkPid, ownedRaw, { mode: 0o600 });
    fs.writeFileSync(aggregateSymlinkTarget, foreignRaw, { mode: 0o600 });
    const aggregateSymlinkPrior = await captureLaunchAgentUnloadPriorState({
      label: LABEL,
      pidPath: aggregateSymlinkPid,
      port: PORT,
      observeLabel: gone,
      observeListener: async () => absent(),
    });
    let aggregateSymlinkInode = 0;
    const aggregateSymlinkOutcome = await observeLaunchAgentUnloadTerminalState({
      label: LABEL,
      pidPath: aggregateSymlinkPid,
      port: PORT,
      prior: aggregateSymlinkPrior,
      timeoutMs: 0,
      observeLabel: gone,
      observeListener: async () => absent(),
      processIsLive: () => false,
      removePidFile: (pidPath, candidate, label) =>
        removeCollectorPidFileIfOwnedDetailed(pidPath, candidate, label, {
          beforeClaim: () => {
            fs.renameSync(aggregateSymlinkPid, aggregateSymlinkOwner);
            fs.symlinkSync(aggregateSymlinkTarget, aggregateSymlinkPid);
            aggregateSymlinkInode = fs.lstatSync(aggregateSymlinkPid).ino;
          },
        }),
      now: () => 0,
    });
    const aggregateSymlinkHidden = fs.readdirSync(pidFixtureRoot).filter((entry) =>
      entry.includes(`${path.basename(aggregateSymlinkPid)}.plimsoll-`)
    );
    check(
      "mid_claim_symlink_never_becomes_aggregate_stopped_truth",
      !aggregateSymlinkOutcome.stopped &&
        aggregateSymlinkOutcome.state === "indeterminate" &&
        !aggregateSymlinkOutcome.pidCleaned &&
        !aggregateSymlinkOutcome.removedPidFile &&
        aggregateSymlinkOutcome.pidCleanupAmbiguous &&
        !aggregateSymlinkOutcome.pidCleanupQuarantined &&
        aggregateSymlinkOutcome.final.pidCleanupMarkerState === "present" &&
        aggregateSymlinkOutcome.timing.finalObservationPerformed === true &&
        fs.lstatSync(aggregateSymlinkPid).isSymbolicLink() &&
        fs.lstatSync(aggregateSymlinkPid).ino === aggregateSymlinkInode &&
        fs.readlinkSync(aggregateSymlinkPid) === aggregateSymlinkTarget &&
        fs.readFileSync(aggregateSymlinkTarget, "utf8") === foreignRaw &&
        fs.readFileSync(aggregateSymlinkOwner, "utf8") === ownedRaw &&
        aggregateSymlinkHidden.length === 1,
      "The exact reviewer race remains visible, durably marked, ambiguous, and nonterminal even though the final observation was performed.",
    );

    const receiptPid = path.join(pidFixtureRoot, "receipt-race.pid");
    const receiptOwner = path.join(pidFixtureRoot, "receipt-race-owner.pid");
    fs.writeFileSync(receiptPid, ownedRaw, { mode: 0o600 });
    const receiptPrior = await captureLaunchAgentUnloadPriorState({
      label: LABEL,
      pidPath: receiptPid,
      port: PORT,
      observeLabel: gone,
      observeListener: async () => absent(),
    });
    let receiptClockReads = 0;
    let cleanupCalls = 0;
    const receiptRace = await observeLaunchAgentUnloadTerminalState({
      label: LABEL,
      pidPath: receiptPid,
      port: PORT,
      prior: receiptPrior,
      timeoutMs: 0,
      observeLabel: gone,
      observeListener: async () => absent(),
      processIsLive: () => false,
      removePidFile: (pidPath, candidate, label) => {
        cleanupCalls += 1;
        return removeCollectorPidFileIfOwnedDetailed(pidPath, candidate, label, {
          beforeClaim: () => fs.renameSync(receiptPid, receiptOwner),
        });
      },
      now: () => {
        receiptClockReads += 1;
        if (receiptClockReads === 3) {
          fs.writeFileSync(receiptPid, foreignRaw, { mode: 0o600 });
        }
        return 0;
      },
    });
    check(
      "cleanup_ambiguity_survives_reappearance_before_final_receipt",
      cleanupCalls === 1 &&
        !receiptRace.stopped &&
        !receiptRace.pidCleaned &&
        !receiptRace.removedPidFile &&
        receiptRace.pidCleanupAmbiguous &&
        !receiptRace.pidCleanupQuarantined &&
        receiptRace.final.pidCleanupAmbiguous &&
        receiptRace.final.pidCleanupMarkerState === "present" &&
        receiptRace.timing.finalObservationPerformed === true &&
        fs.readFileSync(receiptPid, "utf8") === foreignRaw &&
        fs.readFileSync(receiptOwner, "utf8") === ownedRaw,
      "A performed final observation remains separate from stopped truth when a new owner appears.",
    );

    const cleanPid = path.join(pidFixtureRoot, "clean.pid");
    fs.writeFileSync(cleanPid, ownedRaw, { mode: 0o600 });
    const cleanCleanup = removeCollectorPidFileIfOwned(cleanPid, owner, LABEL);
    const cleanCleanupState = readCollectorPidCleanupState(cleanPid, LABEL);
    check(
      "exact_cleanup_clears_its_exact_private_marker",
      cleanCleanup.removed &&
        !cleanCleanup.ambiguous &&
        cleanCleanup.disposition === "removed" &&
        !fs.existsSync(cleanPid) &&
        !cleanCleanupState.ambiguous &&
        cleanCleanupState.markerState === "missing" &&
        cleanCleanupState.claimCount === 0 &&
        cleanCleanupState.quarantineCount === 0,
      "The clean control removes the exact owned PID and only its exact identity-bound marker transaction.",
    );

    const boundedPid = path.join(pidFixtureRoot, "bounded-inventory.pid");
    for (let index = 0; index < 1_200; index += 1) {
      fs.writeFileSync(
        path.join(pidFixtureRoot, `bounded-entry-${String(index).padStart(3, "0")}`),
        "fixture\n",
        { mode: 0o600 },
      );
    }
    const boundedCleanupState = readCollectorPidCleanupState(boundedPid, LABEL);
    check(
      "unrelated_home_entries_do_not_consume_cleanup_inventory",
      !boundedCleanupState.ambiguous &&
        !boundedCleanupState.inventoryTruncated &&
        boundedCleanupState.claimCount === 0 &&
        boundedCleanupState.quarantineCount === 0 &&
        boundedCleanupState.unsafeArtifactCount === 0,
      "Twelve hundred unrelated entries do not affect the four exact cleanup artifact slots.",
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
