#!/usr/bin/env node

// Issue #148: lifecycle receipts must distinguish wrapper/child ownership
// from an unrelated listener, and bootstrap success from an actually ready
// collector. Every fixture-level scenario is deterministic (injected seams);
// the process-tree and port-owner units additionally run against REAL ps,
// REAL fingerprints, and stubbed lsof binaries so the frozen hashing and
// parsing paths are exercised, not simulated.

import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import {
  captureBoundedDescendantTree,
  captureLaunchAgentUnloadPriorState,
  observeLaunchAgentUnloadTerminalState,
  portOwnershipAgainstMembers,
  readUtcProcessStartFingerprint,
  UTC_PROCESS_START_ALGORITHM,
  type CollectorListenerObservation,
  type CollectorPidFileRead,
  type CollectorRuntimeIdentity,
  type LaunchAgentLabelObservation,
  type PortOwnershipVerdict,
  type ProcessIdentity,
} from "../packages/collector-cli/src/runtime-ownership";
import { installLaunchAgent } from "../packages/collector-cli/src/launch-agent";

const LABEL = "com.plimsoll.collector";
const PID_PATH = "/fixture/collector.pid";
const PORT = 48_273;

type Check = { name: string; detail: string };
const checks: Check[] = [];
function check(name: string, condition: unknown, detail: string | Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail: typeof detail === "string" ? detail : JSON.stringify(detail) });
}

function identity(seed: number): CollectorRuntimeIdentity {
  return {
    instanceId: `10000000-0000-4000-8000-${String(seed).padStart(12, "0")}`,
    pid: 20_000 + seed,
    processStartFingerprint: `sha256:${seed.toString(16).padStart(64, "0")}`,
    processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
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
      version: 3,
    },
  };
}

const missing = (): CollectorPidFileRead => ({ kind: "missing" });
const unrelatedPort = (): CollectorListenerObservation => ({ kind: "unrelated" });
const reported = (owner: CollectorRuntimeIdentity | null): LaunchAgentLabelObservation => ({
  kind: "reported",
  processIdentity: owner,
});
const gone = (): LaunchAgentLabelObservation => ({ kind: "not_reported" });

type ReadinessScenario = {
  prior: {
    label: LaunchAgentLabelObservation;
    listener: CollectorListenerObservation;
    pid: CollectorPidFileRead;
  };
  tree?: { root: ProcessIdentity | null; members: ProcessIdentity[]; truncated: boolean };
  captureVerdict?: PortOwnershipVerdict;
  snapshots: Array<{
    label: LaunchAgentLabelObservation;
    listener: CollectorListenerObservation;
    pid: CollectorPidFileRead;
    live: CollectorRuntimeIdentity[];
  }>;
  pollVerdict?: PortOwnershipVerdict;
};

async function runReadinessScenario(scenario: ReadinessScenario) {
  const prior = await captureLaunchAgentUnloadPriorState({
    label: LABEL,
    pidPath: PID_PATH,
    port: PORT,
    observeLabel: () => scenario.prior.label,
    observeListener: async () => scenario.prior.listener,
    readPidFile: () => scenario.prior.pid,
    ...(scenario.tree ? { captureProcessTree: () => scenario.tree! } : {}),
    ...(scenario.captureVerdict
      ? { classifyPortOwnership: () => scenario.captureVerdict! }
      : {}),
  });
  let snapshotIndex = 0;
  const outcome = await observeLaunchAgentUnloadTerminalState({
    label: LABEL,
    pidPath: PID_PATH,
    port: PORT,
    prior,
    timeoutMs: 60,
    pollIntervalMs: 10,
    observeLabel: () =>
      scenario.snapshots[Math.min(snapshotIndex, scenario.snapshots.length - 1)]!.label,
    observeListener: async () => {
      const snapshot =
        scenario.snapshots[Math.min(snapshotIndex, scenario.snapshots.length - 1)]!;
      snapshotIndex += 1;
      return snapshot.listener;
    },
    classifyIdentity: (candidate) =>
      scenario.snapshots[0]!.live.some(
        (owner) =>
          owner.pid === candidate.pid &&
          owner.processStartFingerprint === candidate.processStartFingerprint,
      )
        ? "live"
        : "stale",
    readPidFile: () =>
      scenario.snapshots[Math.min(snapshotIndex, scenario.snapshots.length - 1)]!.pid,
    removePidFile: () => ({
      removed: false,
      ambiguous: false,
      quarantined: false,
      persistent: {
        ambiguous: false,
        markerState: "missing" as const,
        claimCount: 0,
        quarantineCount: 0,
        inventoryTruncated: false,
        unsafeArtifactCount: 0,
      },
      disposition: "not_owned" as const,
    }),
    // Explicit seam everywhere the prior membership exists, so no fixture
    // ever touches the real lsof/ps classifier.
    ...(scenario.pollVerdict || prior.portOwnershipMembers
      ? { classifyPortOwnership: () => scenario.pollVerdict ?? "owned_by_members" }
      : {}),
  });
  return { prior, outcome };
}

async function main() {
  check("proof_runs_on_exact_node_22", process.versions.node.split(".")[0] === "22", {
    nodeMajor: Number(process.versions.node.split(".")[0]),
  });

  const wrapper = identity(1);
  const child = identity(2);
  const foreign = identity(3);

  // --- Fixture level: wrapper/child ownership -----------------------------

  const owned = await runReadinessScenario({
    prior: { label: reported(wrapper), listener: unrelatedPort(), pid: current(child) },
    tree: { root: wrapper, members: [wrapper, child], truncated: false },
    captureVerdict: "owned_by_members",
    snapshots: [
      { label: reported(wrapper), listener: unrelatedPort(), pid: current(child), live: [wrapper, child] },
      { label: reported(wrapper), listener: unrelatedPort(), pid: current(child), live: [wrapper, child] },
    ],
  });
  check(
    "silent_port_owned_by_prior_child_tree_is_prior_unresponsive_not_unrelated",
    owned.prior.ownership === "prior_unresponsive" &&
      Boolean(owned.prior.portOwnershipMembers?.some((m) => m.pid === wrapper.pid)) &&
      Boolean(owned.prior.portOwnershipMembers?.some((m) => m.pid === child.pid)),
    "A port held by the exact wrapper+child tree while /status times out belongs to our unresponsive runtime.",
  );
  check(
    "unload_receipt_refuses_with_prior_unresponsive_state",
    !owned.outcome.stopped && owned.outcome.state === "prior_unresponsive",
    "The unload receipt names the owned-but-unresponsive runtime instead of an unrelated listener.",
  );

  const legacyUnrelated = await runReadinessScenario({
    prior: { label: gone(), listener: unrelatedPort(), pid: missing() },
    snapshots: [
      { label: gone(), listener: unrelatedPort(), pid: missing(), live: [] },
    ],
  });
  check(
    "no_tree_evidence_keeps_legacy_unrelated_classification",
    legacyUnrelated.prior.ownership === "unrelated" &&
      legacyUnrelated.outcome.state === "unrelated_listener" &&
      legacyUnrelated.prior.portOwnershipMembers === undefined,
    "Without proven membership the transport verdict stands unchanged.",
  );

  const takeover = await runReadinessScenario({
    prior: { label: reported(wrapper), listener: unrelatedPort(), pid: current(child) },
    tree: { root: wrapper, members: [wrapper, child], truncated: false },
    captureVerdict: "owned_by_members",
    snapshots: [
      { label: gone(), listener: unrelatedPort(), pid: missing(), live: [] },
    ],
    pollVerdict: "foreign",
  });
  check(
    "proven_foreign_takeover_escalates_to_unrelated_listener",
    takeover.outcome.state === "unrelated_listener" && !takeover.outcome.stopped,
    "Only a live reclassification proving a foreign owner may report unrelated_listener.",
  );

  const unknownVerdict = await runReadinessScenario({
    prior: { label: reported(wrapper), listener: unrelatedPort(), pid: current(child) },
    tree: { root: wrapper, members: [wrapper, child], truncated: false },
    captureVerdict: "owned_by_members",
    snapshots: [
      { label: gone(), listener: unrelatedPort(), pid: current(child), live: [child] },
    ],
    pollVerdict: "unknown",
  });
  check(
    "unknown_poll_verdict_stands_on_proven_prior_membership",
    unknownVerdict.outcome.state === "prior_unresponsive",
    "An unresolvable port-owner read never fabricates foreignness over proven membership.",
  );

  const answeringAgain = await runReadinessScenario({
    prior: { label: reported(wrapper), listener: unrelatedPort(), pid: current(child) },
    tree: { root: wrapper, members: [wrapper, child], truncated: false },
    captureVerdict: "owned_by_members",
    snapshots: [
      {
        label: gone(),
        listener: { kind: "collector", runtimeIdentity: child },
        pid: current(child),
        live: [child],
      },
    ],
  });
  check(
    "once_status_answers_the_state_is_never_prior_unresponsive",
    answeringAgain.outcome.state === "live_conflict" && !answeringAgain.outcome.stopped,
    "A recovered /status answer leaves the prior_unresponsive lane immediately (label gone + runtime live is live_conflict).",
  );

  // --- Real processes: bounded tree capture -------------------------------

  const treeSandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-readiness-tree-"));
  const parent = spawn("/bin/sh", ["-c", "sleep 30 & sleep 30 & wait"], { stdio: "ignore" });
  try {
    await new Promise((resolve) => setTimeout(resolve, 300));
    const table = spawnSync("ps", ["-eo", "pid=,ppid="], { encoding: "utf8" });
    const childrenOfParent = (table.stdout ?? "")
      .split("\n")
      .map((line) => line.trim().match(/^(\d+)\s+(\d+)$/))
      .filter((match) => match && Number(match[2]) === parent.pid)
      .map((match) => Number(match![1]));
    check("fixture_wrapper_has_two_real_children", childrenOfParent.length === 2, {
      parent: parent.pid,
      children: childrenOfParent.length,
    });

    const tree = captureBoundedDescendantTree(parent.pid);
    const capturedPids = tree.members.map((member) => member.pid);
    check(
      "bounded_tree_captures_real_wrapper_and_children_with_fingerprints",
      tree.root?.pid === parent.pid &&
        childrenOfParent.every((pid) => capturedPids.includes(pid)) &&
        tree.members.every(
          (member) =>
            member.processStartFingerprint.startsWith("sha256:") &&
            member.processStartFingerprintAlgorithm === UTC_PROCESS_START_ALGORITHM,
        ) &&
        !tree.truncated,
      { root: tree.root?.pid ?? null, members: capturedPids.length, truncated: tree.truncated },
    );

    const capped = captureBoundedDescendantTree(parent.pid, { maxNodes: 2 });
    check(
      "tree_capture_truncates_at_node_bound_and_sets_truncated_flag",
      capped.members.length === 2 && capped.truncated,
      { members: capped.members.length, truncated: capped.truncated },
    );
  } finally {
    parent.kill("SIGKILL");
  }
  await new Promise((resolve) => setTimeout(resolve, 150));
  const deadTree = captureBoundedDescendantTree(parent.pid);
  check(
    "dead_root_yields_empty_proven_membership_without_invention",
    deadTree.root === null && deadTree.members.length === 0 && !deadTree.truncated,
    { root: deadTree.root, members: deadTree.members.length },
  );
  const bogusTree = captureBoundedDescendantTree(-5);
  check(
    "nonpositive_root_pid_is_rejected_without_ps_traffic",
    bogusTree.root === null && bogusTree.members.length === 0,
    "Invalid roots never reach process enumeration.",
  );

  // --- Classifier exactness: stub lsof + real fingerprints -----------------

  const binDir = path.join(treeSandbox, "bin");
  fs.mkdirSync(binDir, { mode: 0o700 });
  const liveFingerprint = readUtcProcessStartFingerprint(process.pid);
  assert.ok(liveFingerprint, "proof process must be fingerprintable");
  const stubLsof = (body: string, exitCode = 0) => {
    const file = path.join(binDir, `lsof-${Math.random().toString(36).slice(2)}`);
    fs.writeFileSync(file, `#!/bin/sh\nprintf '%s' '${body}'\nexit ${exitCode}\n`, { mode: 0o700 });
    return file;
  };
  const selfMember: ProcessIdentity = {
    pid: process.pid,
    processStartFingerprint: liveFingerprint,
    processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
  };
  check(
    "port_owner_matches_only_on_exact_pid_and_start_fingerprint",
    portOwnershipAgainstMembers(PORT, [selfMember], {
      lsofPath: stubLsof(`${process.pid}\n`),
    }) === "owned_by_members" &&
      portOwnershipAgainstMembers(
        PORT,
        [{ ...selfMember, processStartFingerprint: `sha256:${"0".repeat(64)}` }],
        { lsofPath: stubLsof(`${process.pid}\n`) },
      ) === "foreign" &&
      portOwnershipAgainstMembers(PORT, [{ ...selfMember, pid: selfMember.pid + 1 }], {
        lsofPath: stubLsof(`${process.pid}\n`),
      }) === "foreign",
    "PID reuse with a different start time never counts as ownership.",
  );
  check(
    "legacy_domain_member_never_matches_utc_owner_observation",
    portOwnershipAgainstMembers(
      PORT,
      [{ ...selfMember, processStartFingerprintAlgorithm: "plimsoll-ps-lstart-local-v1" }],
      { lsofPath: stubLsof(`${process.pid}\n`) },
    ) === "foreign",
    "Legacy timezone-rendered records cannot authorize ownership of a UTC-observed owner.",
  );
  check(
    "unresolvable_lsof_is_unknown_never_foreign_or_owned",
    portOwnershipAgainstMembers(PORT, [selfMember], {
      lsofPath: path.join(binDir, "does-not-exist"),
    }) === "unknown" &&
      portOwnershipAgainstMembers(PORT, [selfMember], { lsofPath: stubLsof("", 1) }) ===
        "unknown" &&
      portOwnershipAgainstMembers(PORT, [selfMember], { lsofPath: stubLsof("not-a-pid\n") }) ===
        "unknown",
    "Absent tooling, absent owners, and garbage output all fail closed to unknown.",
  );
  check(
    "empty_membership_is_foreign_without_probing",
    portOwnershipAgainstMembers(PORT, [], { lsofPath: stubLsof(`${process.pid}\n`) }) === "foreign",
    "No membership means nothing can be ours.",
  );

  await fs.promises.rm(treeSandbox, { recursive: true, force: true });

  // --- CLI level: load receipt readiness ----------------------------------

  const root = path.resolve(import.meta.dirname, "..");
  const tsx = path.join(root, "node_modules", "tsx", "dist", "cli.mjs");
  const cli = path.join(root, "packages", "collector-cli", "src", "cli.ts");
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-readiness-load-"));
  const stubDir = path.join(sandbox, "stub-bin");
  fs.mkdirSync(stubDir, { mode: 0o700 });
  const syntheticPnpm = path.join(sandbox, "pnpm");
  fs.writeFileSync(syntheticPnpm, "#!/bin/sh\nexit 0\n", { mode: 0o700 });

  function freePorts(count: number): Promise<number[]> {
    const servers: net.Server[] = [];
    return Promise.all(
      Array.from({ length: count }, () =>
        new Promise<number>((resolve) => {
          const server = net.createServer();
          servers.push(server);
          server.listen(0, "127.0.0.1", () => {
            const address = server.address();
            assert.ok(address && typeof address === "object");
            resolve(address.port);
          });
        }),
      ),
    ).then((ports) => {
      for (const server of servers) server.close();
      return ports;
    });
  }

  function pinPort(home: string, port: number) {
    // The CLI reads its port from collector.config.json under PLIMSOLL_HOME;
    // without this pin it would probe DEFAULT_COLLECTOR_PORT (48271), which
    // on a developer machine may host a real unrelated collector.
    const plimsollHome = path.join(sandbox, `${path.basename(home)}-data`);
    fs.mkdirSync(plimsollHome, { mode: 0o700 });
    fs.writeFileSync(
      path.join(plimsollHome, "collector.config.json"),
      `${JSON.stringify({ port })}\n`,
      { mode: 0o600 },
    );
    return plimsollHome;
  }

  function runLoadCli(home: string, port: number) {
    const result = spawnSync(process.execPath, [tsx, cli, "load-launch-agent"], {
      cwd: root,
      env: {
        HOME: home,
        PLIMSOLL_HOME: pinPort(home, port),
        PATH: `${stubDir}${path.delimiter}${path.dirname(process.execPath)}${path.delimiter}/usr/bin${path.delimiter}/bin`,
        PLIMSOLL_TEST_LAUNCHCTL_STATE: path.join(sandbox, "launchctl.state"),
      },
      encoding: "utf8",
      timeout: 30_000,
    });
    const stdout = result.stdout ?? "";
    const first = stdout.indexOf("{");
    assert.ok(first !== -1, `load-launch-agent emitted no JSON receipt: ${stdout} ${result.stderr}`);
    return {
      code: result.status,
      receipt: JSON.parse(stdout.slice(first)) as Record<string, any>,
    };
  }

  // Stub launchctl: print reports not-reported until bootstrapped; bootstrap
  // succeeds and optionally starts a fake collector on FAKE_PORT.
  const launchctl = path.join(stubDir, "launchctl");
  fs.writeFileSync(
    launchctl,
    `#!/bin/sh
case "$1" in
  print)
    if test -f "$PLIMSOLL_TEST_LAUNCHCTL_STATE"; then exit 0; fi
    printf '%s\\n' 'Could not find service "${LABEL}" in domain for user gui: '"$(id -u)" >&2
    exit 113
    ;;
  bootstrap)
    : > "$PLIMSOLL_TEST_LAUNCHCTL_STATE"
    if [ -n "\${PLIMSOLL_FAKE_STATUS_PORT:-}" ]; then
      FAKE_STATUS_PORT="\${PLIMSOLL_FAKE_STATUS_PORT}" FAKE_BODY="\${FAKE_BODY:-}" \
        "\${FAKE_NODE:-}" -e '
const http = require("http");
const body = process.env.FAKE_BODY;
const server = http.createServer((_req, res) => {
  res.writeHead(200, {"content-type": "application/json"});
  res.end(body);
});
server.listen(Number(process.env.FAKE_STATUS_PORT), "127.0.0.1");
' >/dev/null 2>&1 &
      echo $! >> "\${PLIMSOLL_FAKE_PIDS:-/dev/null}"
    fi
    exit 0
    ;;
  bootout)
    printf '%s\\n' 'booted_out' > "$PLIMSOLL_TEST_LAUNCHCTL_STATE"
    exit 0
    ;;
  *) exit 64 ;;
esac
`,
    { mode: 0o700 },
  );

  const [deadPort, readyPort, lyingPort] = await freePorts(3);
  const fakeIdentity = JSON.stringify({
    runtimeIdentity: {
      instanceId: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
      pid: process.pid,
      processStartFingerprint: liveFingerprint,
      processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
    },
  });

  const absentHome = path.join(sandbox, "home-absent-listener");
  fs.mkdirSync(absentHome, { mode: 0o700 });
  installLaunchAgent({ homeDir: absentHome, repoRoot: sandbox, pnpmPath: syntheticPnpm });
  const absentLoad = runLoadCli(absentHome, deadPort);
  check(
    "bootstrap_success_without_collector_reports_readiness_false_absent",
    absentLoad.receipt.loaded === true &&
      absentLoad.receipt.status === "bootstrap_succeeded" &&
      absentLoad.receipt.readiness?.verified === false &&
      absentLoad.receipt.readiness?.listenerState === "absent" &&
      absentLoad.receipt.readiness?.runtimeLive === null &&
      absentLoad.receipt.readiness?.deadlineCrossed === true,
    {
      code: absentLoad.code,
      loaded: absentLoad.receipt.loaded,
      readiness: JSON.stringify(absentLoad.receipt.readiness),
    },
  );

  const readyHome = path.join(sandbox, "home-ready-collector");
  fs.mkdirSync(readyHome, { mode: 0o700 });
  installLaunchAgent({ homeDir: readyHome, repoRoot: sandbox, pnpmPath: syntheticPnpm });
  const readyLoad = spawnSync(process.execPath, [tsx, cli, "load-launch-agent"], {
    cwd: root,
    env: {
      HOME: readyHome,
      PLIMSOLL_HOME: pinPort(readyHome, readyPort),
      PATH: `${stubDir}${path.delimiter}${path.dirname(process.execPath)}${path.delimiter}/usr/bin${path.delimiter}/bin`,
      PLIMSOLL_TEST_LAUNCHCTL_STATE: path.join(sandbox, "launchctl-ready.state"),
      PLIMSOLL_FAKE_STATUS_PORT: String(readyPort),
      PLIMSOLL_FAKE_PIDS: path.join(sandbox, "fake-server.pids"),
      FAKE_NODE: process.execPath,
      FAKE_BODY: fakeIdentity,
    },
    encoding: "utf8",
    timeout: 30_000,
  });
  const readyReceipt = JSON.parse((readyLoad.stdout ?? "").slice((readyLoad.stdout ?? "").indexOf("{"))) as Record<string, any>;
  check(
    "bootstrap_with_serving_collector_proves_readiness_true",
    readyReceipt.loaded === true &&
      readyReceipt.status === "bootstrap_succeeded" &&
      readyReceipt.readiness?.verified === true &&
      readyReceipt.readiness?.listenerState === "collector" &&
      readyReceipt.readiness?.runtimeLive === true &&
      readyReceipt.readiness?.deadlineCrossed === false &&
      readyLoad.status === 0,
    {
      code: readyLoad.status,
      readiness: JSON.stringify(readyReceipt.readiness),
    },
  );

  const lyingHome = path.join(sandbox, "home-lying-collector");
  fs.mkdirSync(lyingHome, { mode: 0o700 });
  installLaunchAgent({ homeDir: lyingHome, repoRoot: sandbox, pnpmPath: syntheticPnpm });
  const lyingLoad = spawnSync(process.execPath, [tsx, cli, "load-launch-agent"], {
    cwd: root,
    env: {
      HOME: lyingHome,
      PLIMSOLL_HOME: pinPort(lyingHome, lyingPort),
      PATH: `${stubDir}${path.delimiter}${path.dirname(process.execPath)}${path.delimiter}/usr/bin${path.delimiter}/bin`,
      PLIMSOLL_TEST_LAUNCHCTL_STATE: path.join(sandbox, "launchctl-lying.state"),
      PLIMSOLL_FAKE_STATUS_PORT: String(lyingPort),
      PLIMSOLL_FAKE_PIDS: path.join(sandbox, "fake-server-lying.pids"),
      FAKE_NODE: process.execPath,
      FAKE_BODY: '{"runtimeIdentity":{"pid":1,"instanceId":"short","processStartFingerprint":"nope"}}',
    },
    encoding: "utf8",
    timeout: 30_000,
  });
  const lyingReceipt = JSON.parse((lyingLoad.stdout ?? "").slice((lyingLoad.stdout ?? "").indexOf("{"))) as Record<string, any>;
  check(
    "open_port_with_invalid_identity_is_not_readiness",
    lyingReceipt.readiness?.verified === false &&
      lyingReceipt.readiness?.listenerState === "unrelated" &&
      lyingReceipt.readiness?.runtimeLive === null &&
      lyingReceipt.status === "bootstrap_succeeded" &&
      lyingReceipt.loaded === true,
    {
      code: lyingLoad.status,
      readiness: JSON.stringify(lyingReceipt.readiness),
    },
  );

  for (const pidFile of ["fake-server.pids", "fake-server-lying.pids"]) {
    const target = path.join(sandbox, pidFile);
    if (!fs.existsSync(target)) continue;
    for (const pid of fs.readFileSync(target, "utf8").split("\n").filter(Boolean)) {
      try {
        process.kill(Number(pid), "SIGKILL");
      } catch {
        // Already gone.
      }
    }
  }
  await fs.promises.rm(sandbox, { recursive: true, force: true });

  console.log(JSON.stringify({ issue: 148, ok: true, checks }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
