/**
 * Completion guards for the `scripts/*-proof.ts` family.
 *
 * The hazard these abolish, first observed in `maintenance-starvation-proof.ts`
 * (issue #181): an `await` on a promise that never settles drains the Node
 * event loop. Node then exits 0 with NO output — `main()` never finished, no
 * receipt was printed, and every check after the stall never ran. Because the
 * runner reads only the exit code, that reads as GREEN while the defect the
 * proof exists to catch is live in production source.
 *
 * A proof that has never been seen to fail is not a control. Two distinct
 * silences are refused here, because they fail in different ways:
 *
 *   1. DRAIN — nothing is left holding the loop, so Node exits 0 early. The
 *      `exit` listener catches this: it fires on the way out and flips the
 *      code to 1.
 *   2. HANG — something (a live socket, an unref'd-by-nobody timer, a child
 *      process) still holds the loop, so the process never exits at all and
 *      the `exit` listener is never reached. The watchdog catches this. It is
 *      `unref()`'d on purpose: an unref'd timer cannot itself keep the process
 *      alive, so it can only ever fire while something ELSE is holding the
 *      loop open — precisely the hang case, and never a false alarm on a proof
 *      that finished cleanly.
 *
 * `complete()` also asserts the check count, so a run that silently skipped a
 * scenario — or exited early through a path that still printed something —
 * cannot read as a pass.
 *
 * Two installation forms, both valid. Pick by how much the proof can tell you.
 *
 * 1. INLINE — the proof knows its exact check count up front. Completion is
 *    asserted before the receipt is printed, so a short run never emits a
 *    "pass" receipt at all:
 *
 *      const guard = guardProofCompletion({
 *        expectedChecks: EXPECTED_CHECKS,
 *        countChecks: () => checks.length,
 *      });
 *
 *    then call `guard.complete()` inside `main()` immediately before the
 *    receipt is printed.
 *
 * 2. ENTRYPOINT — the proof registers checks dynamically, or its receipt is
 *    printed from several places. Complete on the settled main promise and let
 *    the script's own reporter handle the throw:
 *
 *      const guard = guardProofCompletion({ countChecks: () => checks.length });
 *
 *      main().then(() => guard.complete()).catch((error) => { ...existing... });
 *
 *    A count mismatch rejects into the script's existing `.catch`, which
 *    already prints a failure and sets a non-zero exit code.
 *
 * `expectedChecks` and `countChecks` are both optional. Omit `expectedChecks`
 * when the count is not fixed — the drain and hang refusals, which are the
 * reason this module exists, do not depend on it. Omit `countChecks` too when
 * the proof keeps no check tally; the guard then reports without counts rather
 * than inventing one.
 */
import assert from "node:assert/strict";

/** Default ceiling on a single proof run. Generous: it is a hang detector. */
export const DEFAULT_PROOF_WATCHDOG_MS = 120_000;

export type ProofCompletionGuard = {
  /**
   * Assert the full check set ran, then disarm both guards. Call this inside
   * `main()` immediately before printing the receipt — never at the top — or
   * on the settled main promise at the entrypoint.
   */
  complete: () => void;
  /**
   * The count `complete()` enforces, for inclusion in a receipt.
   * `undefined` when the proof does not declare a fixed count.
   */
  readonly expectedChecks: number | undefined;
};

export function guardProofCompletion(options: {
  /**
   * How many checks a complete run must register. Omit when the count is not
   * fixed; the drain and hang refusals do not depend on it.
   */
  expectedChecks?: number;
  /** Reads the live check count. Called lazily, at failure time. */
  countChecks?: () => number;
  /** Hang ceiling. Defaults to {@link DEFAULT_PROOF_WATCHDOG_MS}. */
  watchdogMs?: number;
} = {}): ProofCompletionGuard {
  const { expectedChecks, countChecks } = options;
  const watchdogMs = options.watchdogMs ?? DEFAULT_PROOF_WATCHDOG_MS;
  let completed = false;

  // Counting must never be what breaks the guard: a proof whose tally throws
  // mid-teardown still has to report the drain or hang it was installed for.
  const safeCount = (): number | undefined => {
    if (!countChecks) return undefined;
    try {
      return countChecks();
    } catch {
      return undefined;
    }
  };

  const report = (error: string) => {
    console.error(JSON.stringify({
      status: "fail",
      error,
      checksRun: safeCount(),
      expected: expectedChecks,
    }, null, 2));
  };

  // HANG: still holding a live handle, so `exit` below is never reached.
  const watchdog = setTimeout(() => {
    report("proof_watchdog_timeout");
    process.exit(1);
  }, watchdogMs);
  watchdog.unref();

  // DRAIN: loop emptied early, so Node exits 0 with nothing printed. A
  // non-zero code means the failure was already reported by main()'s catch —
  // do not double-report it.
  process.on("exit", (code) => {
    if (completed || code !== 0) return;
    report("proof_exited_before_completion");
    process.exitCode = 1;
  });

  return {
    expectedChecks,
    complete() {
      // The count assertion is an extra, only available when the proof both
      // declares a fixed total and can tally what it ran. When either is
      // absent the drain and hang refusals still stand on their own — they
      // are the reason this module exists. Asserting against an absent count
      // would turn a guard into a false alarm.
      if (expectedChecks !== undefined && countChecks) {
        const ran = countChecks();
        assert.equal(
          ran,
          expectedChecks,
          `expected ${expectedChecks} checks, ran ${ran}`,
        );
      }
      completed = true;
      clearTimeout(watchdog);
    },
  };
}
