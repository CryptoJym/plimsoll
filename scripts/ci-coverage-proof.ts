/**
 * eco-6hoxj.163.23 — every proof script runs in CI or is reviewed local-only.
 *
 * Fails when a package.json proof script is neither run by
 * .github/workflows/proof.yml nor listed with a reason in
 * scripts/proof-local-only.json, when that list names a script CI runs or no
 * script at all, when proof.yml invokes a proof script package.json lacks, or
 * when this gate is not the first proof step. Adversarial checks prove each
 * rule on edited copies of the real inputs.
 *
 * Run: pnpm proof:ci-coverage [--root DIR]   (DIR: another checkout to audit)
 */
import path from "node:path";

import {
  PROOF_LOCAL_ONLY,
  PROOF_WORKFLOW,
  parseWorkflowSteps,
  proofCiCoverage,
  readProofCiCoverageInput,
  type ProofCiCoverage,
  type ProofCiCoverageInput,
} from "./lib/proof-ci-coverage";

const GATE = "proof:ci-coverage";
const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const rootFlag = process.argv.indexOf("--root");
const auditRoot = rootFlag === -1 ? repoRoot : path.resolve(process.argv[rootFlag + 1] ?? "");
const input = readProofCiCoverageInput(auditRoot);
const coverage = proofCiCoverage(input);

type Check = { name: string; adversarial: boolean; passed: boolean; detail?: unknown };
const checks: Check[] = [];

function check(name: string, adversarial: boolean, run: () => { passed: boolean; detail?: unknown }) {
  let result: { passed: boolean; detail?: unknown };
  try {
    result = run();
  } catch (error) {
    result = { passed: false, detail: error instanceof Error ? error.message : String(error) };
  }
  checks.push({ name, adversarial, ...result });
}

function edited(edit: (draft: ProofCiCoverageInput) => void): ProofCiCoverage {
  const draft = structuredClone(input);
  edit(draft);
  return proofCiCoverage(draft);
}

// The adversarial checks edit the CI line of one proof that proof.yml runs
// exactly once on a line of its own.
const target = coverage.scripts.find(
  (entry) =>
    entry.script !== GATE &&
    entry.invocations.length === 1 &&
    entry.invocations[0]!.via === "script" &&
    input.workflow.split("\n").filter((line) => line.trim() === entry.invocations[0]!.command).length === 1,
);

function editTargetLine(draft: ProofCiCoverageInput, replace: (line: string) => string[]) {
  const command = target!.invocations[0]!.command;
  draft.workflow = draft.workflow
    .split("\n")
    .flatMap((line) => (line.trim() === command ? replace(line) : [line]))
    .join("\n");
}

const indentOf = (line: string) => line.slice(0, line.length - line.trimStart().length);

/** Set a key of the target's step, replacing it if the step already has it. */
function editTargetStep(draft: ProofCiCoverageInput, key: string, value: string) {
  const lines = draft.workflow.split("\n");
  const dash = target!.invocations[0]!.line - 1;
  const dashIndent = indentOf(lines[dash]!).length;
  const keyLine = `${" ".repeat(dashIndent + 2)}${key}: ${value}`;
  let end = dash + 1;
  while (end < lines.length && (lines[end]!.trim() === "" || indentOf(lines[end]!).length > dashIndent)) end += 1;
  const existing = lines.slice(dash + 1, end).findIndex((line) => line.startsWith(`${" ".repeat(dashIndent + 2)}${key}:`));
  if (existing === -1) lines.splice(dash + 1, 0, keyLine);
  else lines[dash + 1 + existing] = keyLine;
  draft.workflow = lines.join("\n");
}

check("every_proof_script_runs_in_ci_or_is_reviewed_local_only", false, () => ({
  passed:
    coverage.uncovered.length === 0 &&
    coverage.localOnlyButRun.length === 0 &&
    coverage.invalidLocalOnly.length === 0 &&
    coverage.unknownInvocations.length === 0,
  detail: {
    uncovered: coverage.uncovered,
    localOnlyButRun: coverage.localOnlyButRun,
    invalidLocalOnly: coverage.invalidLocalOnly,
    unknownInvocations: coverage.unknownInvocations,
  },
}));

check("the_gate_is_the_first_proof_step_in_ci", false, () => {
  const firstProofStep = parseWorkflowSteps(input.workflow).find((step) =>
    coverage.scripts.some((entry) => entry.invocations.some((invocation) => invocation.line === step.line)),
  );
  const gate = coverage.scripts.find((entry) => entry.script === GATE);
  return {
    passed: Boolean(gate && gate.invocations.length > 0 && gate.invocations[0]!.line === firstProofStep?.line),
    detail: { firstProofStep: firstProofStep?.name ?? null, gateRuns: gate?.invocations.length ?? 0 },
  };
});

check("adversarial_fixture_target_found", true, () => ({ passed: Boolean(target), detail: target?.script }));

if (target) {
  const script = target.script;
  check("adversarial_removed_ci_line_reports_the_proof_uncovered", true, () => {
    const result = edited((draft) => editTargetLine(draft, () => []));
    return { passed: result.uncovered.includes(script), detail: result.uncovered };
  });
  check("adversarial_commented_out_invocation_does_not_count", true, () => {
    const result = edited((draft) => editTargetLine(draft, (line) => [`${indentOf(line)}# ${line.trim()}`]));
    return { passed: result.uncovered.includes(script), detail: result.uncovered };
  });
  check("adversarial_swallowed_exit_status_does_not_count", true, () => {
    const results = ["|| true", "| tee proof.log", "; true", "&"].map((suffix) =>
      edited((draft) => editTargetLine(draft, (line) => [`${line} ${suffix}`])),
    );
    return { passed: results.every((result) => result.uncovered.includes(script)) };
  });
  check("adversarial_continue_on_error_step_does_not_count", true, () => {
    const result = edited((draft) => editTargetStep(draft, "continue-on-error", "true"));
    return { passed: result.uncovered.includes(script), detail: result.uncovered };
  });
  check("adversarial_disabled_step_does_not_count", true, () => {
    const result = edited((draft) => editTargetStep(draft, "if", "false"));
    return { passed: result.uncovered.includes(script), detail: result.uncovered };
  });
  check("adversarial_entry_file_invocation_counts", true, () => {
    const result = edited((draft) =>
      editTargetLine(draft, (line) => [`${indentOf(line)}node ./node_modules/tsx/dist/cli.mjs ./${target.entry}`]),
    );
    const entry = result.scripts.find((candidate) => candidate.script === script);
    return { passed: !result.uncovered.includes(script) && entry?.invocations[0]?.via === "entry" };
  });
  check("adversarial_unknown_pnpm_proof_invocation_is_reported", true, () => {
    const result = edited((draft) =>
      editTargetLine(draft, (line) => [line, `${indentOf(line)}pnpm proof:no-such-proof-canary`]),
    );
    return { passed: result.unknownInvocations.includes("proof:no-such-proof-canary"), detail: result.unknownInvocations };
  });
  check("adversarial_local_only_entry_for_a_ci_proof_is_rejected", true, () => {
    const result = edited((draft) => {
      draft.localOnly[script] = "claims local-only while CI runs it";
    });
    return { passed: result.localOnlyButRun.includes(script), detail: result.localOnlyButRun };
  });
}

check("adversarial_new_proof_script_without_ci_step_is_reported", true, () => {
  const result = edited((draft) => {
    draft.scripts["proof:unwired-canary"] = "tsx scripts/unwired-canary-proof.ts";
  });
  return { passed: result.uncovered.includes("proof:unwired-canary"), detail: result.uncovered };
});

check("adversarial_local_only_entry_without_script_or_reason_is_rejected", true, () => {
  const result = edited((draft) => {
    draft.localOnly["proof:no-such-script-canary"] = "stale entry";
    draft.scripts["proof:reasonless-canary"] = "tsx scripts/reasonless-canary-proof.ts";
    draft.localOnly["proof:reasonless-canary"] = "  ";
  });
  return {
    passed:
      result.invalidLocalOnly.includes("proof:no-such-script-canary") &&
      result.invalidLocalOnly.includes("proof:reasonless-canary") &&
      result.uncovered.includes("proof:reasonless-canary"),
    detail: result.invalidLocalOnly,
  };
});

const failed = checks.filter((entry) => !entry.passed);
const runInCi = coverage.scripts.filter((entry) => entry.invocations.length > 0);
const localOnly = coverage.scripts.filter((entry) => entry.localOnly !== null && entry.invocations.length === 0);
console.log(
  JSON.stringify(
    {
      schema: "plimsoll.ci-coverage-proof.v1",
      status: failed.length === 0 ? "passed" : "failed",
      audited: path.relative(repoRoot, auditRoot) || ".",
      proofScripts: coverage.scripts.length,
      runInCi: runInCi.length,
      localOnly: localOnly.map((entry) => ({ script: entry.script, reason: entry.localOnly })),
      uncovered: coverage.uncovered,
      checks: checks.map(({ name, adversarial, passed }) => ({ name, adversarial, passed })),
      failures: failed.map(({ name, detail }) => ({ name, detail })),
    },
    null,
    2,
  ),
);
if (failed.length > 0) {
  if (coverage.uncovered.length > 0) {
    console.error(
      `${coverage.uncovered.length} proof script(s) are not run by ${PROOF_WORKFLOW} and not listed in ${PROOF_LOCAL_ONLY}:\n` +
        coverage.uncovered.map((script) => `  - ${script}`).join("\n"),
    );
  }
  process.exitCode = 1;
}
