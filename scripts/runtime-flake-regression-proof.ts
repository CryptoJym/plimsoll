/**
 * Regression proof for GitHub issue #187: two distinct false-red classes in
 * the runtime-ownership / process-liveness observation family flaked on
 * hosted runners (identical content green on rerun, no changes).
 *
 * Class 1 (commit f50b507): the PID-file ancestor-chain re-walk compared
 * volatile directory metadata (mtime/ctime/links/size) between the initial
 * walk and the post-read re-walk, so benign neighbor activity inside a
 * shared ancestor read as identity_changed tampering.
 *
 * Class 2 (commit 69549e6): supervision assertions consumed receipt streams
 * at 'exit', but Node only guarantees drained stdio at 'close'; an
 * undrained pipe produced the empty-aggregate false red.
 *
 * The proof is deterministic: class-1 scenarios interpose a single mutation
 * between the two ancestor walks via a wrapped fs.lstatSync; class-2 uses a
 * child whose stdout tail is written by a longer-lived inheriting process,
 * so the tail provably cannot exist at 'exit'.
 *
 * Adversarial controls prove the narrowed comparison still fails closed on
 * real resolution changes (ancestor replacement, re-permissioning, leaf
 * replacement) and that a naive 'exit'-based watcher really misses the
 * tail the shipped helper captures.
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  classifyProcessIdentity,
  readCollectorPidFile,
  readUtcProcessStartFingerprint,
  UTC_PROCESS_START_ALGORITHM,
} from "../packages/collector-cli/src/runtime-ownership";
import { waitForExit, watch } from "./lib/supervision-watch";

// Synthetic label on purpose: these scenarios exercise the pid-file path
// safety walk, which is label-agnostic. Importing the production
// LAUNCH_AGENT_LABEL would drag the collector config graph (zod) into a
// proof that needs only runtime-ownership.
const PROOF_PID_LABEL = "com.plimsoll.flake-regression-proof";

type Check = { name: string; passed: boolean; detail: unknown };
const checks: Check[] = [];
function record(name: string, condition: unknown, detail: unknown) {
  checks.push({ name, passed: Boolean(condition), detail });
  if (!condition) throw new Error(`${name}: ${JSON.stringify(detail)}`);
}

function buildPidFixture(sandbox: string, name: string) {
  const deepDir = path.join(sandbox, name, "a", "b", "c");
  fs.mkdirSync(deepDir, { recursive: true, mode: 0o700 });
  const pidPath = path.join(deepDir, "collector.pid");
  const fingerprint = readUtcProcessStartFingerprint(process.pid);
  if (!fingerprint) throw new Error("fixture_fingerprint_unavailable");
  const recordBody = {
    instanceId: randomUUID(),
    pid: process.pid,
    processStartFingerprint: fingerprint,
    processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
    command: ["flake-regression-proof"],
    cwd: sandbox,
    label: PROOF_PID_LABEL,
    startedAt: new Date().toISOString(),
    version: 3,
  };
  fs.writeFileSync(pidPath, JSON.stringify(recordBody, null, 2) + "\n", { mode: 0o600 });
  return { deepDir, pidPath };
}

// --- Class 1 interposition -------------------------------------------------
//
// inspectCollectorPidFile lstats the leaf exactly twice: once for the initial
// identity and once for the post-read re-walk. Firing the armed mutation on
// the second leaf lstat places it strictly between the walks, reproducing the
// hosted-runner interleaving deterministically.

const realLstatSync = fs.lstatSync.bind(fs);
let armedMutation: (() => void) | null = null;
let armedLeafPath: string | null = null;
let leafLstatCount = 0;

const patchedLstatSync = ((pathArg: Parameters<typeof fs.lstatSync>[0], options?: unknown) => {
  if (armedMutation && armedLeafPath && path.resolve(String(pathArg)) === armedLeafPath) {
    leafLstatCount += 1;
    if (leafLstatCount === 2) {
      const mutation = armedMutation;
      armedMutation = null;
      mutation();
    }
  }
  return (
    options === undefined
      ? realLstatSync(pathArg)
      : (realLstatSync as (p: Parameters<typeof fs.lstatSync>[0], o: never) => fs.Stats)(
          pathArg,
          options as never,
        )
  );
}) as typeof fs.lstatSync;

function installLstatInterposition() {
  (fs as unknown as { lstatSync: typeof fs.lstatSync }).lstatSync = patchedLstatSync;
}
function removeLstatInterposition() {
  (fs as unknown as { lstatSync: typeof fs.lstatSync }).lstatSync = realLstatSync;
  armedMutation = null;
  armedLeafPath = null;
}

function armOnSecondLeafLstat(pidPath: string, mutation: () => void) {
  armedLeafPath = path.resolve(pidPath);
  armedMutation = mutation;
  leafLstatCount = 0;
}

type PidReadResult = ReturnType<typeof readCollectorPidFile>;

function expectCurrent(pidPath: string, scenario: string) {
  const read = readCollectorPidFile(pidPath, PROOF_PID_LABEL) as PidReadResult;
  record(
    `class1_${scenario}_pid_file_stays_current`,
    read.kind === "current" &&
      (read as { kind: string; record?: { version?: number } }).record?.version === 3,
    read.kind === "current"
      ? { kind: read.kind }
      : { kind: read.kind, reason: (read as { reason?: string }).reason ?? null },
  );
}

function expectIdentityChanged(pidPath: string, scenario: string) {
  const read = readCollectorPidFile(pidPath, PROOF_PID_LABEL) as PidReadResult;
  record(
    `class1_${scenario}_still_fails_closed`,
    read.kind === "unsafe" && (read as { kind: string; reason?: string }).reason === "identity_changed",
    { kind: read.kind, reason: (read as { reason?: string }).reason ?? null },
  );
}

function runClass1Scenarios(sandbox: string) {
  installLstatInterposition();
  try {
    // Control: no inter-walk mutation must read as current.
    const control = buildPidFixture(sandbox, "control");
    expectCurrent(control.pidPath, "no_mutation_control");

    // The original false red: neighbor activity moves ancestor mtime/ctime
    // between the walks. Must NOT read as tampering.
    for (let iteration = 0; iteration < 25; iteration++) {
      const fixture = buildPidFixture(sandbox, `benign-${iteration}`);
      armOnSecondLeafLstat(fixture.pidPath, () => {
        const scratch = path.join(fixture.deepDir, "neighbor-scratch");
        fs.writeFileSync(scratch, "x", { mode: 0o600 });
        fs.unlinkSync(scratch);
      });
      try {
        expectCurrent(fixture.pidPath, `benign_neighbor_metadata_${iteration}`);
      } finally {
        fs.rmSync(path.dirname(fixture.deepDir), { recursive: true, force: true });
      }
    }

    // Adversarial: the ancestor directory itself is replaced between the
    // walks (fresh device/inode) with an identical-looking copy. A real path
    // swap; must still fail closed.
    {
      const fixture = buildPidFixture(sandbox, "ancestor-replacement");
      armOnSecondLeafLstat(fixture.pidPath, () => {
        const retired = `${fixture.deepDir}.retired`;
        fs.renameSync(fixture.deepDir, retired);
        fs.mkdirSync(fixture.deepDir, { mode: 0o700 });
        fs.copyFileSync(retired + "/collector.pid", fixture.pidPath);
        fs.chmodSync(fixture.pidPath, 0o600);
        fs.rmSync(retired, { recursive: true, force: true });
      });
      try {
        expectIdentityChanged(fixture.pidPath, "ancestor_replacement");
      } finally {
        fs.rmSync(path.dirname(fixture.deepDir), { recursive: true, force: true });
      }
    }

    // Adversarial: re-permissioning an ancestor between the walks changes
    // what participates in resolving the path; must still fail closed.
    {
      const fixture = buildPidFixture(sandbox, "ancestor-mode");
      armOnSecondLeafLstat(fixture.pidPath, () => {
        fs.chmodSync(fixture.deepDir, 0o750);
      });
      try {
        expectIdentityChanged(fixture.pidPath, "ancestor_repermission");
      } finally {
        fs.chmodSync(fixture.deepDir, 0o700);
        fs.rmSync(path.dirname(fixture.deepDir), { recursive: true, force: true });
      }
    }

    // Adversarial: the leaf pid file itself is swapped between the walks.
    {
      const fixture = buildPidFixture(sandbox, "leaf-replacement");
      armOnSecondLeafLstat(fixture.pidPath, () => {
        const staged = `${fixture.pidPath}.staged`;
        fs.copyFileSync(fixture.pidPath, staged);
        fs.chmodSync(staged, 0o600);
        fs.renameSync(staged, fixture.pidPath);
      });
      try {
        expectIdentityChanged(fixture.pidPath, "leaf_replacement");
      } finally {
        fs.rmSync(path.dirname(fixture.deepDir), { recursive: true, force: true });
      }
    }
  } finally {
    removeLstatInterposition();
  }
}

// --- Class 2 fixtures ------------------------------------------------------

function writeClass2Fixtures(dir: string) {
  const holderPath = path.join(dir, "holder.mjs");
  fs.writeFileSync(
    holderPath,
    [
      "// Writes the tail of the receipt line long after the direct child exits.",
      "setTimeout(() => {",
      '  process.stdout.write("1}\\n");',
      "}, 300);",
      "",
    ].join("\n"),
    { mode: 0o600 },
  );
  const childPath = path.join(dir, "child.mjs");
  fs.writeFileSync(
    childPath,
    [
      'import { spawn } from "node:child_process";',
      "const holderPath = process.argv[2];",
      '// The holder inherits this stdout pipe and outlives this process.',
      'spawn(process.execPath, [holderPath], { stdio: ["ignore", "inherit", "inherit"] });',
      'process.stdout.write(\'{"tailMarker":\');',
      "setTimeout(() => process.exit(0), 40);",
      "",
    ].join("\n"),
    { mode: 0o600 },
  );
  return { childPath, holderPath };
}

function spawnClass2Child(childPath: string, holderPath: string) {
  return spawn(process.execPath, [childPath, holderPath], {
    stdio: ["ignore", "pipe", "pipe"],
  });
}

async function runClass2Scenarios(fixturesRoot: string) {
  const { childPath, holderPath } = writeClass2Fixtures(fixturesRoot);

  // Shipped semantics: waitForExit resolves on drained 'close', and watch()
  // consumes the trailing line there, so the completed receipt is captured.
  const watched = watch(spawnClass2Child(childPath, holderPath));
  const exitInfo = await waitForExit(watched.child, 10_000);
  record("class2_shipped_wait_for_exit_reports_zero", exitInfo.code === 0, exitInfo);
  record(
    "class2_shipped_watch_captures_tail_written_after_exit",
    watched.receipts.some((receipt) => receipt.tailMarker === 1),
    { receipts: watched.receipts, outputTail: watched.output.slice(-80) },
  );
  record("class2_shipped_output_complete", watched.output.endsWith("1}\n"), {
    outputTail: watched.output.slice(-40),
  });

  // Adversarial control: a watcher that finalizes its parse buffer at
  // 'exit' — the pre-#201 semantics — demonstrably loses the tail. This is
  // the exact mechanism behind the empty-aggregate false red.
  await new Promise<void>((resolveNaive) => {
    const naiveChild = spawnClass2Child(childPath, holderPath);
    let naiveOutput = "";
    let naiveRemainder = "";
    const naiveReceipts: Array<Record<string, unknown>> = [];
    naiveChild.stdout!.setEncoding("utf8");
    naiveChild.stdout!.on("data", (chunk: string) => {
      naiveOutput += chunk;
      naiveRemainder += chunk;
      const lines = naiveRemainder.split("\n");
      naiveRemainder = lines.pop() ?? "";
      for (const line of lines) {
        try {
          naiveReceipts.push(JSON.parse(line) as Record<string, unknown>);
        } catch {
          // Incomplete JSON at 'exit' time.
        }
      }
    });
    naiveChild.on("exit", () => {
      // Pre-fix behavior: finalize immediately at reap, no trailing flush.
      if (naiveRemainder.trim()) {
        try {
          naiveReceipts.push(JSON.parse(naiveRemainder) as Record<string, unknown>);
        } catch {
          // Undrained partial line stays lost — the false-red signature.
        }
      }
      record("class2_naive_control_saw_partial_output", naiveOutput.includes('{"tailMarker":'), {
        outputBytes: Buffer.byteLength(naiveOutput),
      });
      record(
        "class2_naive_exit_time_parse_misses_tail_false_red_reproduced",
        !naiveReceipts.some((receipt) => receipt.tailMarker === 1),
        { receipts: naiveReceipts },
      );
      resolveNaive();
    });
  });
}

// --- Golden observations on the fixed code ----------------------------------

function runGoldenObservations() {
  const selfFingerprint = readUtcProcessStartFingerprint(process.pid);
  record("golden_self_fingerprint_available", Boolean(selfFingerprint), { pid: process.pid });
  const repeats = [readUtcProcessStartFingerprint(process.pid), readUtcProcessStartFingerprint(process.pid)];
  record(
    "golden_fingerprint_observation_is_stable",
    Boolean(selfFingerprint) && repeats.every((value) => value === selfFingerprint),
    { expected: selfFingerprint, observed: repeats },
  );
  record(
    "golden_live_identity_classified_live",
    classifyProcessIdentity({
      instanceId: randomUUID(),
      pid: process.pid,
      processStartFingerprint: selfFingerprint!,
      processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
    }) === "live",
    { pid: process.pid },
  );
  // Unresolvable identities (macOS answers ESRCH for out-of-range pids) may
  // classify stale or indeterminate depending on errno, but never live.
  const hugePid = 2 ** 31 - 1;
  const hugePidClassification = classifyProcessIdentity({
    instanceId: randomUUID(),
    pid: hugePid,
    processStartFingerprint: "sha256:synthetic",
    processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
  });
  record(
    "green_unresolvable_identity_is_never_live",
    hugePidClassification === "stale" || hugePidClassification === "indeterminate",
    { pid: hugePid, classification: hugePidClassification },
  );
  const exited = spawnSyncExitThenClassify();
  record("freshly_exited_pid_classifies_stale_or_indeterminate_never_live", exited !== "live", {
    classification: exited,
  });
}

function spawnSyncExitThenClassify() {
  const child = spawnSync(process.execPath, ["-e", "process.exit(0)"]);
  return classifyProcessIdentity({
    instanceId: randomUUID(),
    pid: child.pid ?? -1,
    processStartFingerprint: readUtcProcessStartFingerprint(child.pid ?? -1) ?? "sha256:none",
    processStartFingerprintAlgorithm: UTC_PROCESS_START_ALGORITHM,
  });
}

async function main() {
  const nodeMajor = Number(process.versions.node.split(".")[0]);
  record("proof_runs_on_supported_node", nodeMajor >= 20 && nodeMajor < 25, {
    version: process.versions.node,
  });

  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-flake-regression-"));
  const class2Root = path.join(sandbox, "class2");
  fs.mkdirSync(class2Root, { mode: 0o700 });
  try {
    runGoldenObservations();
    runClass1Scenarios(sandbox);
    await runClass2Scenarios(class2Root);
  } finally {
    fs.rmSync(sandbox, { recursive: true, force: true });
  }

  const receipt = {
    issue: 187,
    ok: checks.every((entry) => entry.passed),
    node: { execPath: process.execPath, version: process.versions.node },
    checks,
  };
  const evidenceDir = path.join(import.meta.dirname, "..", "evidence");
  fs.mkdirSync(evidenceDir, { recursive: true });
  fs.writeFileSync(
    path.join(evidenceDir, "runtime-flake-regression-proof.json"),
    JSON.stringify(receipt, null, 2) + "\n",
  );
  console.log(JSON.stringify(receipt, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : String(error));
  process.exitCode = 1;
});
