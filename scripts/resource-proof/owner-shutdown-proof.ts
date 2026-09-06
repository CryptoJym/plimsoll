#!/usr/bin/env node
/**
 * #162 owner-shutdown determinism proof.
 *
 * Runs on stock Node >= 22.6 (`node --experimental-strip-types`) with zero
 * installed dependencies, so the harness guarantees are verifiable even
 * before `pnpm install`. Adversarial cases try to break each guarantee:
 *   - a fixture child that ignores SIGTERM must still be reaped (SIGKILL);
 *   - a child that survives SIGKILL must surface as CleanupFailure within a
 *     bounded wall-clock budget, never hang, never pass silently;
 *   - an owner that exits before any stop command exists must classify as
 *     OwnerEarlyExit even when a later stop reports success;
 *   - a failed stop command must classify as StopCommandFailure (never as
 *     OwnerShutdownTimeout);
 *   - deadline errors must carry their symbolic code as error.name (the old
 *     harness reported literal "Error").
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import os from "node:os";

// Extension-qualified: this proof runs on stock Node (--experimental-strip-types),
// whose resolver does not extension-guess. See the header note above.
import { guardProofCompletion } from "../lib/proof-completion.ts";
import {
  OWNER_SHUTDOWN_FAILURE_CLASSES,
  SymbolicProofError,
  classifyOwnerShutdown,
  classifyStopCommand,
  observeChildExit,
  reapFixtureChild,
  withSymbolicDeadline,
} from "./owner-shutdown.ts";

const checks: Record<string, boolean> = {};
let checksRun = 0;

function check(name: string, body: () => boolean | Promise<boolean>): Promise<void> {
  return Promise.resolve()
    .then(body)
    .then((ok) => {
      checks[name] = ok === true;
      checksRun += 1;
      if (!ok) throw new Error(`check_failed:${name}`);
    });
}

// Refuses two silent-green failure modes: an early event-loop drain and a
// hang that never exits. See scripts/lib/proof-completion.ts.
const guard = guardProofCompletion({ countChecks: () => checksRun });

function spawnNode(script: string) {
  const child = spawn(process.execPath, ["-e", script], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(
    (resolve) => {
      child.once("close", (code, signal) => resolve({ code, signal }));
    },
  );
  return { child, exit };
}

/**
 * Wait until the child has booted far enough to install its own signal
 * handlers: the child emits one stdout line once its handlers are armed.
 * Without this barrier the adversarial SIGTERM-ignorer could be killed by
 * default TERM semantics before its handler exists — a race that would make
 * the adversary useless instead of hostile.
 */
function waitForSignalArmed(child: import("node:child_process").ChildProcess) {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("signal_armer_timeout")), 5_000);
    child.stdout!.once("data", () => {
      clearTimeout(timer);
      resolve();
    });
    child.once("close", () => {
      clearTimeout(timer);
      reject(new Error("signal_armer_exited"));
    });
  });
}

/**
 * Narrow the stop classification to its failure reason. A `failed:false`
 * classification has no reason, mirroring the union shape in owner-shutdown.ts
 * while keeping the strip-types harness type-safe under tsc.
 */
function stopFailureReason(observation: Parameters<typeof classifyStopCommand>[0]) {
  const classification = classifyStopCommand(observation);
  return classification.failed ? classification.reason : undefined;
}

async function main() {
  // T1: stop-command classification table.
  await check("stop_timeout_classifies_StopCommandFailure", () =>
    classifyStopCommand({
      stopperSettled: false,
      stopperExitCode: null,
      stopperSignal: null,
      stopperReceiptParsed: false,
      stopReceiptReportedStopped: false,
    }).failed &&
    stopFailureReason({
      stopperSettled: false,
      stopperExitCode: null,
      stopperSignal: null,
      stopperReceiptParsed: false,
      stopReceiptReportedStopped: false,
    }) === "StopCommandTimeout",
  );
  await check("stop_nonzero_exit_classifies_StopCommandFailure", () =>
    stopFailureReason({
      stopperSettled: true,
      stopperExitCode: 1,
      stopperSignal: null,
      stopperReceiptParsed: true,
      stopReceiptReportedStopped: false,
    }) === "StopCommandExitNonZero",
  );
  await check("stop_signalled_classifies_StopCommandFailure", () =>
    stopFailureReason({
      stopperSettled: true,
      stopperExitCode: null,
      stopperSignal: "SIGKILL",
      stopperReceiptParsed: false,
      stopReceiptReportedStopped: false,
    }) === "StopCommandSignalled",
  );
  await check("stop_unparseable_receipt_classifies_StopCommandFailure", () =>
    stopFailureReason({
      stopperSettled: true,
      stopperExitCode: 0,
      stopperSignal: null,
      stopperReceiptParsed: false,
      stopReceiptReportedStopped: false,
    }) === "StopReceiptUnparseable",
  );
  await check("stop_not_stopped_receipt_classifies_StopCommandFailure", () =>
    stopFailureReason({
      stopperSettled: true,
      stopperExitCode: 0,
      stopperSignal: null,
      stopperReceiptParsed: true,
      stopReceiptReportedStopped: false,
    }) === "StopReceiptNotStopped",
  );
  await check(
    "clean_stop_receipt_does_not_fail",
    () =>
      !classifyStopCommand({
        stopperSettled: true,
        stopperExitCode: 0,
        stopperSignal: null,
        stopperReceiptParsed: true,
        stopReceiptReportedStopped: true,
      }).failed,
  );

  // T2: owner-shutdown classification order (causal, adversarial inputs).
  await check(
    "early_exit_wins_over_successful_stop_report",
    () =>
      classifyOwnerShutdown({
        ownerExitedBeforeStopSpawned: true,
        stopFailed: false,
        stopFailureReason: null,
        ownerExitSettled: true,
      }).failureClass === "OwnerEarlyExit",
  );
  await check(
    "failed_stop_masks_shutdown_timeout",
    () =>
      classifyOwnerShutdown({
        ownerExitedBeforeStopSpawned: false,
        stopFailed: true,
        stopFailureReason: "runtime_identity_unverified_like_symbolic",
        ownerExitSettled: false,
      }).failureClass === "StopCommandFailure",
  );
  await check(
    "successful_stop_with_live_owner_is_OwnerShutdownTimeout",
    () =>
      classifyOwnerShutdown({
        ownerExitedBeforeStopSpawned: false,
        stopFailed: false,
        stopFailureReason: null,
        ownerExitSettled: false,
      }).failureClass === "OwnerShutdownTimeout",
  );
  await check(
    "happy_path_classifies_null",
    () =>
      classifyOwnerShutdown({
        ownerExitedBeforeStopSpawned: false,
        stopFailed: false,
        stopFailureReason: null,
        ownerExitSettled: true,
      }).failureClass === null,
  );

  // T3: every class/reason string stays path-free and bounded.
  await check("symbolic_classes_are_path_free", () => {
    const observed = new Set<string>(OWNER_SHUTDOWN_FAILURE_CLASSES);
    observed.add("owner_exited_before_stop_command_spawned");
    observed.add("owner_live_after_bounded_shutdown_budget");
    for (const reason of [
      "StopCommandTimeout",
      "StopCommandExitNonZero",
      "StopCommandSignalled",
      "StopReceiptUnparseable",
      "StopReceiptNotStopped",
    ]) {
      observed.add(reason);
    }
    const tmpdir = os.tmpdir();
    for (const term of observed) {
      if (!/^[A-Za-z0-9_:]+$/.test(term)) return false;
      if (term.includes("/")) return false;
      if (tmpdir.length > 1 && term.includes(tmpdir)) return false;
    }
    return observed.size >= 11;
  });

  // T4: deadline errors carry their symbolic code, never literal "Error".
  await check("deadline_error_is_symbolic_not_generic", async () => {
    let caught: unknown = null;
    try {
      await withSymbolicDeadline(new Promise<never>(() => undefined), 25, "ProbeBudget");
    } catch (error) {
      caught = error;
    }
    return (
      caught instanceof SymbolicProofError &&
      caught instanceof Error &&
      caught.name === "ProbeBudget" &&
      caught.message === "ProbeBudget"
    );
  });

  // T5: observeChildExit resolves unsettled instead of throwing.
  await check("observe_child_exit_times_out_without_throwing", async () => {
    const { child, exit } = spawnNode("setInterval(() => {}, 1 << 30);");
    try {
      const observation = await observeChildExit(exit, 120);
      return observation.settled === false && child.exitCode === null;
    } finally {
      child.kill("SIGKILL");
      await exit;
    }
  });

  // T6: reap ladder against real children.
  await check("reap_reports_already_exited", async () => {
    const { child, exit } = spawnNode("process.exit(0);");
    await exit;
    return (await reapFixtureChild(child, exit, { termMs: 400, killMs: 400 })) ===
      "already_exited";
  });
  await check("reaps_sigterm_respecting_child_after_term", async () => {
    const { child, exit } = spawnNode(
      "process.on('SIGTERM', () => process.exit(0)); console.log('armed'); setInterval(() => {}, 1 << 30);",
    );
    await waitForSignalArmed(child);
    const outcome = await reapFixtureChild(child, exit, { termMs: 900, killMs: 400 });
    const dead = (() => {
      try {
        process.kill(child.pid!, 0);
        return false;
      } catch {
        return true;
      }
    })();
    return outcome === "reaped_after_term" && dead;
  });
  await check("adversarial_sigterm_ignorer_reaped_after_kill", async () => {
    const { child, exit } = spawnNode(
      "process.on('SIGTERM', () => {}); console.log('armed'); setInterval(() => {}, 1 << 30);",
    );
    await waitForSignalArmed(child);
    const outcome = await reapFixtureChild(child, exit, { termMs: 400, killMs: 900 });
    const dead = (() => {
      try {
        process.kill(child.pid!, 0);
        return false;
      } catch {
        return true;
      }
    })();
    return outcome === "reaped_after_kill" && dead;
  });
  await check("adversarial_unreapable_child_reports_CleanupFailure_bounded", async () => {
    const fakeChild = {
      exitCode: null,
      signalCode: null,
      kill: () => true,
    } as unknown as import("node:child_process").ChildProcess;
    const never = new Promise<{ code: number | null; signal: null }>(() => undefined);
    const startedAt = performance.now();
    const outcome = await reapFixtureChild(fakeChild, never, { termMs: 200, killMs: 200 });
    const elapsed = performance.now() - startedAt;
    return outcome === "CleanupFailure" && elapsed < 2_000;
  });
  await check("cleanup_failure_is_never_silent_in_type_space", () =>
    OWNER_SHUTDOWN_FAILURE_CLASSES.includes("CleanupFailure"),
  );

  const passed = Object.values(checks).every(Boolean);
  process.stdout.write(
    `${JSON.stringify({
      schema: "plimsoll.owner-shutdown-determinism-proof.v1",
      passed,
      checksRun,
      checks,
    }, null, 2)}\n`,
  );
  if (!passed) process.exitCode = 1;
}

main().then(() => guard.complete()).catch((error: unknown) => {
  console.error(String(error));
  process.exitCode = 1;
});
