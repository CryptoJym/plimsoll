#!/usr/bin/env node
/**
 * Self-proof for scripts/lib/completion-guard.ts (issue #210).
 *
 * Each case compiles a synthetic mini-proof into a temporary home and spawns
 * it with the lane toolchain. The adversarial cases try to make the guard
 * fail: a never-settling await (the original #181/#210 silent green), an
 * explicit process.exit(0) mid-battery, a hang on a leaked handle, wrong
 * check counts, and post-complete misuse. The unguarded control for the
 * silent-green case proves the harness itself would detect a guard removal.
 *
 * Run: pnpm proof:completion-guard
 */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  installProofCompletionGuard,
} from "./completion-guard";

const checks: Array<{ name: string; passed: boolean; detail: string }> = [];
let selfCompletion: ReturnType<typeof installProofCompletionGuard> | null = null;
function check(name: string, passed: boolean, detail: string) {
  checks.push({ name, passed, detail });
  selfCompletion?.check(name);
  if (process.env.COMPLETION_GUARD_PROOF_DEBUG === "1") {
    process.stderr.write(`DBG ${name} passed=${passed}: ${detail}\n`);
  }
}

const tsx = path.join(import.meta.dirname, "..", "..", "node_modules", "tsx", "dist", "cli.mjs");
const GUARD_IMPORT = path.join(import.meta.dirname, "completion-guard");

type CaseResult = { status: number | null; signal: string | null; stdout: string; stderr: string };

function runCase(home: string, name: string, source: string): CaseResult {
  const file = path.join(home, `${name}.ts`);
  const importLine = `import { installProofCompletionGuard } from ${JSON.stringify(GUARD_IMPORT)};\n`;
  fs.writeFileSync(file, `${importLine}${source}`);
  const result = spawnSync(process.execPath, [tsx, file], {
    encoding: "utf8",
    timeout: 30_000,
  });
  return {
    status: result.status,
    signal: result.signal ?? null,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
  };
}

const NORMAL_BODY = `
const completion = installProofCompletionGuard({ proof: "normal-child", expectedChecks: 3 });
for (const name of ["a", "b", "c"]) {
  if (!name) throw new Error("unreachable");
  completion.check(name);
}
completion.complete();
console.log(JSON.stringify({ status: "pass" }));
`;

// The exact #181/#210 failure shape: an awaited fixture promise that never
// settles leaves nothing scheduled, so Node drains the event loop and exits 0
// before the tail of the battery ever runs.
const SILENT_GREEN_BODY = `
const completion = installProofCompletionGuard({ proof: "silent-green-child", expectedChecks: 5 });
completion.check("only_check_that_ran");
void main();
async function main() {
  // Fixture promise that never settles, exactly like the #181 boundary timer.
  await new Promise<void>(() => {});
  completion.check("never_runs");
  completion.complete();
  console.log(JSON.stringify({ status: "pass" }));
}
`;

const UNGUARDED_CONTROL_BODY = `
(async () => {
  await new Promise<void>(() => {});
})();
`;

const COUNT_MISMATCH_BODY = `
const completion = installProofCompletionGuard({ proof: "mismatch-child", expectedChecks: 3 });
completion.check("a");
completion.check("b");
try {
  completion.complete();
} catch (error) {
  console.error(String(error.message));
  process.exit(1);
}
console.log(JSON.stringify({ status: "pass" }));
`;

const OVERCOUNT_BODY = `
const completion = installProofCompletionGuard({ proof: "overcount-child", expectedChecks: 3 });
for (let i = 0; i < 4; i += 1) completion.check("c" + i);
try {
  completion.complete();
} catch (error) {
  console.error(String(error.message));
  process.exit(1);
}
console.log(JSON.stringify({ status: "pass" }));
`;

const HANG_BODY = `
const completion = installProofCompletionGuard({
  proof: "hang-child",
  expectedChecks: 2,
  watchdogMs: 500,
});
completion.check("registered_then_leaked_handle");
setInterval(() => {}, 50);
`;

const LEGIT_FAILURE_BODY = `
const completion = installProofCompletionGuard({ proof: "legit-fail-child", expectedChecks: 3 });
completion.check("a");
completion.check("b");
throw new Error("real defect found by the battery");
`;

const EXIT_ZERO_MIDWAY_BODY = `
const completion = installProofCompletionGuard({ proof: "exit-zero-child", expectedChecks: 3 });
completion.check("a");
process.exit(0);
`;

const CHECK_AFTER_COMPLETE_BODY = `
const completion = installProofCompletionGuard({ proof: "after-complete-child", expectedChecks: 1 });
completion.check("a");
completion.complete();
try {
  completion.check("b");
} catch (error) {
  console.error(String(error.message));
  process.exit(1);
}
console.log(JSON.stringify({ status: "pass" }));
`;

async function main() {
  assertNode22();
  const home = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-completion-guard-proof-"));
  try {
    const normal = runCase(home, "normal", NORMAL_BODY);
    check(
      "normal_completion_exits_zero",
      normal.status === 0 && normal.stdout.includes('"status":"pass"'),
      describe(normal),
    );

    // Adversarial: the original silent-green failure mode must now exit 1.
    const silent = runCase(home, "silent_green", SILENT_GREEN_BODY);
    check(
      "silent_green_under_run_is_refused",
      silent.status === 1 &&
        silent.stderr.includes("proof_exited_before_completion") &&
        silent.stderr.includes('"checksRun": 1') &&
        silent.stderr.includes('"expected": 5'),
      describe(silent),
    );
    const unguarded = runCase(home, "unguarded_control", UNGUARDED_CONTROL_BODY);
    check(
      "unguarded_control_still_exits_zero_without_output",
      unguarded.status === 0 && unguarded.stdout.trim().length === 0,
      describe(unguarded),
    );

    const mismatch = runCase(home, "count_mismatch", COUNT_MISMATCH_BODY);
    check(
      "truncated_battery_cannot_complete",
      mismatch.status === 1 &&
        mismatch.stderr.includes("expected 3, ran 2"),
      describe(mismatch),
    );

    const overcount = runCase(home, "overcount", OVERCOUNT_BODY);
    check(
      "extra_checks_cannot_complete",
      overcount.status === 1 && overcount.stderr.includes("expected 3, ran 4"),
      describe(overcount),
    );

    const hangStart = Date.now();
    const hang = runCase(home, "hang", HANG_BODY);
    const hangWallMs = Date.now() - hangStart;
    check(
      "leaked_handle_hang_is_killed_with_watchdog_diagnostic",
      hang.status === 1 &&
        hang.stderr.includes("proof_watchdog_timeout") &&
        hang.stderr.includes('"checksRun": 1') &&
        hangWallMs >= 400 && hangWallMs < 20_000,
      `${describe(hang)} wallMs=${hangWallMs}`,
    );

    const legitFailure = runCase(home, "legit_failure", LEGIT_FAILURE_BODY);
    check(
      "legit_failure_keeps_nonzero_exit_and_no_false_guard_report",
      legitFailure.status === 1 &&
        legitFailure.stderr.includes("real defect found") &&
        !legitFailure.stderr.includes("proof_exited_before_completion"),
      describe(legitFailure),
    );

    const exitZero = runCase(home, "exit_zero_midway", EXIT_ZERO_MIDWAY_BODY);
    check(
      "explicit_exit_zero_midway_is_refused",
      exitZero.status === 1 &&
        exitZero.stderr.includes("proof_exited_before_completion") &&
        exitZero.stderr.includes('"checksRun": 1'),
      describe(exitZero),
    );

    const afterComplete = runCase(home, "check_after_complete", CHECK_AFTER_COMPLETE_BODY);
    check(
      "check_after_complete_is_rejected",
      afterComplete.status === 1 &&
        afterComplete.stderr.includes("completion_guard_check_after_complete"),
      describe(afterComplete),
    );
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }

  const failed = checks.filter((entry) => !entry.passed);
  console.log(
    JSON.stringify(
      {
        schema: "plimsoll.completion-guard-proof.v1",
        status: failed.length === 0 ? "pass" : "fail",
        checks: checks.length,
        failed: failed.length,
        names: checks.map((entry) => entry.name),
        liveStateTouched: false,
        providerNetworkCalled: false,
      },
      null,
      2,
    ),
  );
  if (failed.length > 0) process.exitCode = 1;
}

function describe(result: CaseResult): string {
  return JSON.stringify({
    status: result.status,
    signal: result.signal,
    stdoutHead: result.stdout.slice(0, 160),
    stderrHead: result.stderr.slice(0, 240),
  });
}

function assertNode22(): void {
  const major = Number(process.versions.node.split(".")[0]);
  if (major !== 22) {
    throw new Error(`completion-guard proof requires exact Node 22, received ${process.versions.node}`);
  }
}

selfCompletion = installProofCompletionGuard({
  proof: "completion-guard-self-proof",
  // 9 spawned cases + harness_started + all_cases_executed.
  expectedChecks: 11,
});
selfCompletion.check("harness_started");

main()
  .then(() => {
    selfCompletion.check("all_cases_executed");
    selfCompletion.complete();
  })
  .catch((error) => {
    console.error(error instanceof Error ? error.stack ?? error.message : String(error));
    process.exitCode = 1;
  });
