/**
 * Shared completion guard for async proof scripts (issue #210).
 *
 * Failure mode being abolished: an unresolved await drains the event loop,
 * Node exits 0 having printed nothing, and every assertion in the tail of the
 * proof silently never runs. A green report then means "some prefix ran".
 *
 * Usage (every async proof wires this):
 *
 *   const completion = installProofCompletionGuard({
 *     proof: "my-proof",
 *     expectedChecks: 12,
 *   });
 *   // record each executed check / completed scenario:
 *   completion.check("scenario_name");
 *   // once, after the last check and before printing any pass receipt:
 *   completion.complete();
 *
 * Guarantees:
 * - If the process exits 0 without complete(), the exit handler flips the exit
 *   code to 1 and reports proof_exited_before_completion with how many checks
 *   ran versus how many were declared.
 * - If the loop stays alive forever on a leaked handle (hang), the unref'd
 *   watchdog fires, reports proof_watchdog_timeout, and exits 1. It can only
 *   fire while something else keeps the loop alive, which is exactly the hang
 *   case; it never delays a natural exit.
 * - complete() itself refuses to mark completion when the number of recorded
 *   checks differs from the declared expectation, so a truncated battery
 *   cannot print a green receipt.
 */

export type ProofCompletionGuardOptions = {
  /** Short proof identifier used in failure output. */
  proof: string;
  /** Exact number of checks/scenarios the proof must execute before passing. */
  expectedChecks: number;
  /**
   * Wall-clock budget after which a still-alive process is declared hung.
   * Must exceed the proof's legitimate worst-case runtime. Default 15 min.
   */
  watchdogMs?: number;
};

export type ProofCompletionGuard = {
  /** Record one executed check (or one completed scenario for assert-style proofs). */
  check: (name: string) => void;
  /**
   * Assert the declared expectation was met, clear the watchdog, and mark the
   * proof completed. Idempotent; throws on any count mismatch.
   */
  complete: () => void;
  /** Number of checks recorded so far. */
  readonly checksRun: () => number;
};

const DEFAULT_WATCHDOG_MS = 900_000;

export function installProofCompletionGuard(
  options: ProofCompletionGuardOptions,
): ProofCompletionGuard {
  const { proof } = options;
  const expectedChecks = options.expectedChecks;
  const watchdogMs = options.watchdogMs ?? DEFAULT_WATCHDOG_MS;

  if (typeof proof !== "string" || proof.length === 0) {
    throw new Error("completion_guard_requires_non_empty_proof_name");
  }
  if (!Number.isInteger(expectedChecks) || expectedChecks < 1) {
    throw new Error(`completion_guard_requires_positive_expected_checks: ${expectedChecks}`);
  }
  if (!Number.isFinite(watchdogMs) || watchdogMs <= 0) {
    throw new Error(`completion_guard_requires_positive_watchdog_ms: ${watchdogMs}`);
  }

  let checksRun = 0;
  let lastName: string | null = null;
  let completed = false;

  const failJson = (error: string) =>
    JSON.stringify(
      {
        status: "fail",
        error,
        proof,
        checksRun,
        expected: expectedChecks,
        ...(lastName ? { lastCheck: lastName } : {}),
      },
      null,
      2,
    );

  // A hang that still holds a live handle never reaches the exit guard below,
  // so it would sit forever instead of failing. Unref'd: it can only fire
  // while something else is keeping the loop alive — exactly the hang case.
  const watchdog = setTimeout(() => {
    console.error(failJson("proof_watchdog_timeout"));
    process.exit(1);
  }, watchdogMs);
  watchdog.unref();

  // An unresolved await inside a scenario drains the event loop and Node then
  // exits 0 with no output — a silent green that hides every unrun check.
  process.on("exit", (code) => {
    if (completed || code !== 0) return;
    console.error(failJson("proof_exited_before_completion"));
    process.exitCode = 1;
  });

  return {
    check(name: string) {
      if (typeof name !== "string" || name.length === 0) {
        throw new Error(`completion_guard_check_requires_name: ${String(name)}`);
      }
      if (completed) {
        throw new Error(`completion_guard_check_after_complete: ${name}`);
      }
      checksRun += 1;
      lastName = name;
    },
    complete() {
      if (completed) return;
      if (checksRun !== expectedChecks) {
        throw new Error(
          `proof_completed_with_wrong_check_count (${proof}): expected ${expectedChecks}, ran ${checksRun}` +
            (lastName ? `, last=${lastName}` : ""),
        );
      }
      completed = true;
      clearTimeout(watchdog);
    },
    checksRun() {
      return checksRun;
    },
  };
}
