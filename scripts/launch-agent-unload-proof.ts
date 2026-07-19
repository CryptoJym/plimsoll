#!/usr/bin/env node

import assert from "node:assert/strict";

import {
  captureLaunchAgentUnloadPriorState,
  observeLaunchAgentUnloadTerminalState,
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

function current(owner: CollectorRuntimeIdentity): CollectorPidFileRead {
  return {
    kind: "current",
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
  let snapshotIndex = 0;
  let activeSnapshot = scenario.snapshots[0]!;
  let removals = 0;
  const snapshot = () => activeSnapshot;
  const observeLabel = () => {
    activeSnapshot = scenario.snapshots[Math.min(snapshotIndex, scenario.snapshots.length - 1)]!;
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
    now: () => clock,
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
    immediate.outcome.stopped && immediate.outcome.timing.observations === 1,
    "The first aggregate observation proves stopped without a synthetic delay.",
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
    listenerFirst.outcome.stopped && listenerFirst.outcome.timing.observations === 2,
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
    pidFirst.outcome.stopped && pidFirst.outcome.timing.observations === 2,
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
    prior: { label: gone(), listener: absent(), pid: { kind: "legacy", pid: owner.pid, raw: String(owner.pid) }, live: [] },
    snapshots: [{ label: gone(), listener: absent(), pid: { kind: "legacy", pid: owner.pid, raw: String(owner.pid) }, live: [] }],
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
