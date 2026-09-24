import { Scalar, type YAMLMap, isMap, isScalar, isSeq, parseDocument, type Document } from "yaml";

import { GATE_ENTRY, proofCiCoverage, type CoverageInput, type CoverageReport } from "./proof-ci-coverage";

/**
 * Adversarial fixtures for proof:ci-coverage (eco-6hoxj.163.23).
 *
 * Each fixture edits a copy of the real inputs the way a careless (or clever)
 * change could, and states what a correct gate must conclude. `reviewer`
 * fixtures reproduce the first independent review of PR #397 (every one
 * fooled the textual gate at 496e34bc); `review2` fixtures reproduce the
 * second review's false greens against the execution model at e683d895. The
 * same edits can be written to a checkout (`files`) to replay them against
 * any gate.
 */

export const FIXTURE_WORKFLOW = ".github/workflows/proof.yml";
const PREFERRED_TARGETS = ["proof:usage-dedupe", "proof:enrollment-privacy", "proof:performance-layer"];

/** A fixture's edited inputs plus what a correct gate must conclude about them. */
export type FixtureCase = {
  input: CoverageInput;
  uncovered?: string[];
  covered?: string[];
  error?: RegExp;
  gateNotFirst?: boolean;
};

export type Fixture = {
  name: string;
  origin: "reviewer" | "review2" | "gate";
  /** The verdict a correct gate reaches on the edited repository. */
  expectGateGreen: boolean;
  describe: string;
  build(input: CoverageInput): FixtureCase;
  /** Extra files the edit adds to a checkout. */
  files?: Record<string, string>;
  /** False when the fixture edits concepts the textual gate never had (exceptions metadata, dates). */
  replayableOnTextualGate?: boolean;
};

type Target = { line: string; script: string; unit: string };

/**
 * Proofs CI runs from a one-line `run: pnpm proof:…` step of the fixture
 * workflow, the reviewer's three first; fixtures edit these, so they keep
 * working when any single CI line changes.
 */
export function fixtureTargets(input: CoverageInput): Target[] {
  const text = workflowText(input);
  const targets: Target[] = [];
  for (const unit of proofCiCoverage(input).units) {
    if (unit.status !== "ci") continue;
    for (const invocation of unit.covered) {
      const script = /^pnpm (proof:[\w:.-]+)$/.exec(invocation.command.trim())?.[1];
      if (!script || invocation.workflow !== FIXTURE_WORKFLOW || invocation.via.join() !== script) continue;
      if (text.split("\n").filter((line) => line.trim() === `run: pnpm ${script}`).length !== 1) continue;
      if (unit.unit === GATE_ENTRY || targets.some((target) => target.script === script)) continue;
      targets.push({ line: `pnpm ${script}`, script, unit: unit.unit });
    }
  }
  const rank = (target: Target) => (PREFERRED_TARGETS.includes(target.script) ? PREFERRED_TARGETS.indexOf(target.script) : PREFERRED_TARGETS.length);
  targets.sort((a, b) => rank(a) - rank(b) || a.script.localeCompare(b.script));
  if (targets.length < 3) throw new Error(`fixtures need three one-line proof steps in ${FIXTURE_WORKFLOW}`);
  return targets;
}

/** True when the report reaches every conclusion the case expects. */
export function fixtureHolds(fixtureCase: FixtureCase, report: CoverageReport, gateNotFirst: boolean) {
  return (
    (fixtureCase.uncovered ?? []).every((unit) => report.uncovered.includes(unit)) &&
    (fixtureCase.covered ?? []).every(
      (unit) => report.errors.length === 0 && report.units.find((entry) => entry.unit === unit)?.status === "ci",
    ) &&
    (!fixtureCase.error || report.errors.some((error) => fixtureCase.error!.test(error))) &&
    (!fixtureCase.gateNotFirst || gateNotFirst)
  );
}

function workflowText(input: CoverageInput) {
  const workflow = input.workflows.find((candidate) => candidate.path === FIXTURE_WORKFLOW);
  if (!workflow) throw new Error(`fixture workflow ${FIXTURE_WORKFLOW} missing`);
  return workflow.text;
}

function withWorkflowText(input: CoverageInput, text: string, path = FIXTURE_WORKFLOW): CoverageInput {
  return {
    ...input,
    workflows: input.workflows.map((workflow) => (workflow.path === FIXTURE_WORKFLOW ? { path, text } : workflow)),
  };
}

function editWorkflow(input: CoverageInput, edit: (document: Document) => void): CoverageInput {
  const document = parseDocument(workflowText(input));
  edit(document);
  return withWorkflowText(input, document.toString({ lineWidth: 0 }));
}

function jobs(document: Document) {
  const value = document.get("jobs", true);
  if (!isMap(value)) throw new Error("fixture workflow has no jobs mapping");
  return value;
}

/** The job and step whose run script has a line equal to `line`. */
function findStep(document: Document, line: string) {
  for (const job of jobs(document).items) {
    const steps = isMap(job.value) ? job.value.get("steps", true) : undefined;
    if (!isSeq(steps)) continue;
    for (const [index, step] of steps.items.entries()) {
      if (!isMap(step)) continue;
      const run = step.get("run");
      if (typeof run === "string" && run.split("\n").some((candidate) => candidate.trim() === line)) {
        return { job: job.value as YAMLMap, jobKey: String(isScalar(job.key) ? job.key.value : job.key), step, index, run };
      }
    }
  }
  throw new Error(`fixture target \`${line}\` not found in ${FIXTURE_WORKFLOW}`);
}

function block(document: Document, text: string) {
  const node = document.createNode(text) as Scalar;
  node.type = Scalar.BLOCK_LITERAL;
  return node;
}

/** Replace the target line inside its step's run script. */
function editRun(input: CoverageInput, line: string, replace: (line: string) => string[]) {
  return editWorkflow(input, (document) => {
    const found = findStep(document, line);
    const lines = found.run.split("\n").flatMap((candidate) => (candidate.trim() === line ? replace(candidate) : [candidate]));
    found.step.set("run", block(document, `${lines.join("\n").replace(/\n+$/, "")}\n`));
  });
}

function setStepKey(input: CoverageInput, line: string, key: string, value: unknown) {
  return editWorkflow(input, (document) => findStep(document, line).step.set(key, value));
}

function setJobKey(input: CoverageInput, line: string, key: string, value: unknown) {
  return editWorkflow(input, (document) => {
    const found = findStep(document, line);
    found.job.set(key, document.createNode(value));
    // Keep `steps` last so the job still reads naturally.
    const steps = found.job.items.findIndex((pair) => isScalar(pair.key) && pair.key.value === "steps");
    if (steps !== -1) found.job.items.push(...found.job.items.splice(steps, 1));
  });
}


const withScripts = (input: CoverageInput, scripts: Record<string, string>): CoverageInput => ({
  ...input,
  scripts: { ...input.scripts, ...scripts },
});
const withExceptions = (input: CoverageInput, edit: (exceptions: Record<string, Record<string, Record<string, string>>>) => void) => {
  const exceptions = JSON.parse(JSON.stringify(input.exceptions ?? {})) as Record<string, Record<string, Record<string, string>>>;
  edit(exceptions);
  return { ...input, exceptions };
};
const withSuites = (input: CoverageInput, edit: (suites: Record<string, string[]>) => void) => {
  const suites = JSON.parse(JSON.stringify(input.suites ?? {})) as Record<string, string[]>;
  edit(suites);
  return { ...input, suites };
};
/** The first suite scripts/proof-suites.json declares, and its sub-proofs. */
function firstSuite(input: CoverageInput) {
  const [suite, children] = Object.entries((input.suites ?? {}) as Record<string, string[]>)[0] ?? [];
  if (!suite || !children?.length) throw new Error("fixtures need a suite in scripts/proof-suites.json");
  return { suite, children };
}

const RENAMED_PROOF = "scripts/review-renamed-proof.ts";
const renamedProofFile = { [RENAMED_PROOF]: 'throw new Error("this proof must run, but CI never runs it");\n' };
const withRenamedProof = (input: CoverageInput) => ({
  ...withScripts(input, { "verify:renamed-proof": `tsx ${RENAMED_PROOF}` }),
  proofFiles: [...input.proofFiles, RENAMED_PROOF],
});

/** A case built on the first fixture target. */
function onTarget(edit: (input: CoverageInput, target: Target) => CoverageInput, expect: "uncovered" | "covered") {
  return (input: CoverageInput): FixtureCase => {
    const [target] = fixtureTargets(input);
    return { input: edit(input, target!), [expect]: [target!.unit] };
  };
}

export const FIXTURES: Fixture[] = [
  // ---- Review r1 (input/review-r1/checks/adversarial-*) -------------------
  {
    name: "reviewer_proof_script_named_outside_proof_namespace",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "an unwired package script `verify:renamed-proof` runs a proof file",
    build: (input) => ({ input: withRenamedProof(input), uncovered: [RENAMED_PROOF] }),
    files: renamedProofFile,
  },
  {
    name: "reviewer_echo_only_reference",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "the CI line becomes `echo pnpm proof:…`",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line.replace(t.line, `echo ${t.line}`)]), "uncovered"),
  },
  {
    name: "reviewer_statically_false_step_expression",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "the step runs only if `${{ false && always() }}`",
    build: onTarget((input, t) => setStepKey(input, t.line, "if", "${{ false && always() }}"), "uncovered"),
  },
  {
    name: "reviewer_job_level_if_false",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "the proof job gets `if: false`",
    build: (input) => {
      const [t] = fixtureTargets(input);
      return { input: setJobKey(input, t!.line, "if", false), uncovered: [t!.unit, GATE_ENTRY] };
    },
  },
  {
    name: "reviewer_matrix_only_false_step",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "the step runs only if `matrix.run_usage`, whose only value is false",
    build: onTarget(
      (input, t) => setStepKey(setJobKey(input, t.line, "strategy", { matrix: { run_usage: [false] } }), t.line, "if", "${{ matrix.run_usage }}"),
      "uncovered",
    ),
  },
  {
    name: "reviewer_unreachable_after_exit_zero",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "`exit 0` precedes the proof command",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line.replace(t.line, "exit 0"), line]), "uncovered"),
  },
  {
    name: "reviewer_yaml_folded_into_echo",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "a folded block turns `echo` and the proof line into one echo command",
    build: onTarget((input, t) => {
      const text = workflowText(input);
      const line = text.split("\n").find((candidate) => candidate.trim() === `run: ${t.line}`)!;
      const indent = line.slice(0, line.length - line.trimStart().length);
      return withWorkflowText(input, text.replace(line, `${indent}run: >\n${indent}  echo\n${indent}  ${t.line}`));
    }, "uncovered"),
  },
  {
    name: "reviewer_proof_referenced_only_through_package_alias",
    origin: "reviewer",
    expectGateGreen: true,
    describe: "CI runs `pnpm ci:alias`, a package script whose only command is the proof's",
    build: onTarget(
      (input, t) => editRun(withScripts(input, { "ci:alias": t.line }), t.line, (line) => [line.replace(t.line, "pnpm ci:alias")]),
      "covered",
    ),
  },
  {
    name: "reviewer_commented_out_reference",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "the proof line is commented out",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line.replace(t.line, `# ${t.line}`), line.replace(t.line, "echo skipped")]), "uncovered"),
  },
  {
    name: "reviewer_covered_package_script_replaced_by_alias",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "the proof's package script becomes an alias of another proof, orphaning its file",
    build: (input) => {
      const [t, other] = fixtureTargets(input);
      return { input: withScripts(input, { [t!.script]: other!.line }), uncovered: [t!.unit] };
    },
  },
  {
    name: "reviewer_renamed_workflow",
    origin: "reviewer",
    expectGateGreen: true,
    describe: "proof.yml is renamed verify.yml",
    build: (input) => ({ input: withWorkflowText(input, workflowText(input), ".github/workflows/verify.yml"), covered: [GATE_ENTRY] }),
  },
  {
    name: "reviewer_real_entrypoint_combined",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "false-only matrix on two proof steps, `echo` on a third, unwired verify:renamed-proof",
    build: (input) => {
      const [first, second, third] = fixtureTargets(input);
      let edited = setJobKey(input, first!.line, "strategy", { matrix: { run_usage: [false], run_enrollment: [false] } });
      edited = setStepKey(edited, first!.line, "if", "${{ matrix.run_usage }}");
      edited = setStepKey(edited, second!.line, "if", "${{ matrix.run_enrollment }}");
      edited = editRun(edited, third!.line, (line) => [line.replace("pnpm", "echo pnpm")]);
      return { input: withRenamedProof(edited), uncovered: [first!.unit, second!.unit, third!.unit, RENAMED_PROOF] };
    },
    files: renamedProofFile,
  },
  // ---- Further execution-model cases -------------------------------------
  {
    name: "matrix_value_that_can_be_false",
    origin: "gate",
    expectGateGreen: false,
    describe: "the step runs only if `matrix.run_usage` over [true, false]",
    build: onTarget(
      (input, t) => setStepKey(setJobKey(input, t.line, "strategy", { matrix: { run_usage: [true, false] } }), t.line, "if", "${{ matrix.run_usage }}"),
      "uncovered",
    ),
  },
  ...([
    ["or_true", "|| true"],
    ["pipe", "| tee proof.log"],
    ["semicolon", "; true"],
    ["background", "&"],
    ["and_chain", "&& true"],
  ] as const).map(([label, suffix]): Fixture => ({
    name: `swallowed_exit_status_${label}`,
    origin: "gate",
    expectGateGreen: false,
    describe: `the proof line gains \`${suffix}\``,
    build: onTarget((input, t) => editRun(input, t.line, (line) => [`${line} ${suffix}`]), "uncovered"),
  })),
  {
    name: "step_continue_on_error",
    origin: "gate",
    expectGateGreen: false,
    describe: "the proof step sets continue-on-error",
    build: onTarget((input, t) => setStepKey(input, t.line, "continue-on-error", true), "uncovered"),
  },
  {
    name: "job_continue_on_error",
    origin: "gate",
    expectGateGreen: false,
    describe: "the proof job sets continue-on-error",
    build: onTarget((input, t) => setJobKey(input, t.line, "continue-on-error", true), "uncovered"),
  },
  {
    name: "heredoc_body",
    origin: "gate",
    expectGateGreen: false,
    describe: "the proof line sits inside a heredoc fed to bash",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line.replace(t.line, "bash <<'EOF'"), line, line.replace(t.line, "EOF")]), "uncovered"),
  },
  {
    name: "errexit_disabled",
    origin: "gate",
    expectGateGreen: false,
    describe: "`set +e` precedes the proof line",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line.replace(t.line, "set +e"), line]), "uncovered"),
  },
  {
    name: "directory_changed",
    origin: "gate",
    expectGateGreen: false,
    describe: "`cd packages/collector-cli` precedes the proof line",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line.replace(t.line, "cd packages/collector-cli"), line]), "uncovered"),
  },
  {
    name: "step_working_directory",
    origin: "gate",
    expectGateGreen: false,
    describe: "the proof step runs in packages/collector-cli",
    build: onTarget((input, t) => setStepKey(input, t.line, "working-directory", "packages/collector-cli"), "uncovered"),
  },
  {
    name: "step_shell_not_bash",
    origin: "gate",
    expectGateGreen: false,
    describe: "the proof step runs under pwsh",
    build: onTarget((input, t) => setStepKey(input, t.line, "shell", "pwsh"), "uncovered"),
  },
  {
    name: "job_needs_a_skippable_job",
    origin: "gate",
    expectGateGreen: false,
    describe: "the proof job needs a job that runs only on push",
    build: (input) => {
      const [t] = fixtureTargets(input);
      const edited = editWorkflow(setJobKey(input, t!.line, "needs", ["push-only"]), (document) => {
        jobs(document).set(
          "push-only",
          document.createNode({ "runs-on": "macos-14", if: "github.event_name == 'push'", steps: [{ run: "true" }] }),
        );
      });
      return { input: edited, uncovered: [t!.unit, GATE_ENTRY] };
    },
  },
  {
    name: "pull_request_path_filter",
    origin: "gate",
    expectGateGreen: false,
    describe: "pull_request gains a paths filter",
    build: (input) => ({
      input: editWorkflow(input, (document) => document.setIn(["on", "pull_request"], document.createNode({ paths: ["packages/**"] }))),
      uncovered: [fixtureTargets(input)[0]!.unit, GATE_ENTRY],
    }),
  },
  {
    name: "typecheck_may_fail_quietly",
    origin: "gate",
    expectGateGreen: false,
    describe: "typecheck sets continue-on-error, so `steps.typecheck.outcome == 'success'` can be false in a green run",
    build: onTarget(
      (input) => editWorkflow(input, (document) => findStep(document, "pnpm exec tsc --noEmit").step.set("continue-on-error", true)),
      "uncovered",
    ),
  },
  {
    name: "entry_file_invocation_counts",
    origin: "gate",
    expectGateGreen: true,
    describe: "CI runs the proof by entry file through node and tsx",
    build: onTarget(
      (input, t) => editRun(input, t.line, (line) => [line.replace(t.line, `node ./node_modules/tsx/dist/cli.mjs ./${t.unit}`)]),
      "covered",
    ),
  },
  {
    name: "gate_not_first",
    origin: "gate",
    expectGateGreen: false,
    describe: "a proof runs before the gate in the gate's own step",
    build: (input) => {
      const [t] = fixtureTargets(input);
      return { input: editRun(input, "pnpm proof:ci-coverage", (line) => [line.replace("pnpm proof:ci-coverage", t!.line), line]), gateNotFirst: true };
    },
  },
  {
    name: "unknown_pnpm_proof_script",
    origin: "gate",
    expectGateGreen: false,
    describe: "CI runs `pnpm proof:no-such-proof`",
    build: (input) => {
      const [t] = fixtureTargets(input);
      return { input: editRun(input, t!.line, (line) => [line, line.replace(t!.line, "pnpm proof:no-such-proof")]), error: /proof:no-such-proof/ };
    },
  },
  {
    name: "no_workflows",
    origin: "gate",
    expectGateGreen: false,
    describe: "the workflow directory is empty",
    build: (input) => ({ input: { ...input, workflows: [] }, error: /no workflow files/ }),
    replayableOnTextualGate: false,
  },
  {
    name: "local_only_entry_for_a_ci_proof",
    origin: "gate",
    expectGateGreen: false,
    describe: "a local-only entry names a proof CI runs",
    build: (input) => {
      const [t] = fixtureTargets(input);
      return {
        input: withExceptions(input, (exceptions) => {
          exceptions.localOnly = { ...exceptions.localOnly, [t!.unit]: { reason: "claims local-only", owner: "fixture" } };
        }),
        error: /CI already runs it/,
      };
    },
    replayableOnTextualGate: false,
  },
  {
    name: "local_only_entry_without_owner",
    origin: "gate",
    expectGateGreen: false,
    describe: "a local-only entry has no owner",
    build: (input) => ({
      input: withExceptions(input, (exceptions) => {
        const first = Object.keys(exceptions.localOnly ?? {})[0]!;
        exceptions.localOnly![first] = { ...exceptions.localOnly![first]!, owner: " " };
      }),
      error: /missing owner/,
    }),
    replayableOnTextualGate: false,
  },
  {
    name: "quarantine_expired",
    origin: "gate",
    expectGateGreen: false,
    describe: "the date passes a quarantine's expiry",
    build: (input) => ({ input: { ...input, today: "2999-01-01" }, error: /quarantine expired/ }),
    replayableOnTextualGate: false,
  },
  {
    name: "stale_exception_entry",
    origin: "gate",
    expectGateGreen: false,
    describe: "an exception names a proof that no longer exists",
    build: (input) => ({
      input: withExceptions(input, (exceptions) => {
        exceptions.localOnly = { ...exceptions.localOnly, "scripts/no-such-proof.ts": { reason: "stale", owner: "fixture" } };
      }),
      error: /stale entry/,
    }),
    replayableOnTextualGate: false,
  },
  {
    name: "review2_suite_sub_proof_dropped",
    origin: "review2",
    expectGateGreen: false,
    describe: "a sub-proof is dropped from its suite's run list (the suite still names it elsewhere)",
    build: (input) => {
      const { suite, children } = firstSuite(input);
      const dropped = children.at(-2) ?? children[0]!;
      return {
        input: withSuites(input, (suites) => {
          suites[suite] = children.filter((child) => child !== dropped);
        }),
        uncovered: [dropped],
      };
    },
    replayableOnTextualGate: false,
  },
  {
    name: "suite_not_run_in_ci",
    origin: "gate",
    expectGateGreen: false,
    describe: "a sub-proof is declared under a suite CI does not run",
    build: (input) => {
      const { suite, children } = firstSuite(input);
      const parent = Object.keys((input.exceptions as { localOnly?: object }).localOnly ?? {})[0];
      if (!parent) throw new Error("fixture needs a local-only proof");
      return {
        input: withSuites(input, (suites) => {
          suites[suite] = children.slice(1);
          suites[parent] = [children[0]!];
        }),
        uncovered: [children[0]!],
        error: /not run by CI, so none of its sub-proofs are/,
      };
    },
    replayableOnTextualGate: false,
  },
  {
    name: "suite_declares_a_missing_file",
    origin: "gate",
    expectGateGreen: false,
    describe: "a suite declares a sub-proof file that does not exist",
    build: (input) => {
      const { suite, children } = firstSuite(input);
      return {
        input: withSuites(input, (suites) => {
          suites[suite] = [...children, "scripts/no-such-sub-proof.ts"];
        }),
        error: /no-such-sub-proof\.ts is not a proof file on disk/,
      };
    },
    replayableOnTextualGate: false,
  },
];

/**
 * Disable the given steps the way the PR #397 review did: give their jobs a
 * false-only matrix axis and make each step run only if that axis is true.
 */
export function disableSteps(input: CoverageInput, steps: Array<{ workflow: string; job: string; stepIndex: number }>): CoverageInput {
  const byWorkflow = new Map<string, typeof steps>();
  for (const step of steps) byWorkflow.set(step.workflow, [...(byWorkflow.get(step.workflow) ?? []), step]);
  return {
    ...input,
    workflows: input.workflows.map((workflow) => {
      const targets = byWorkflow.get(workflow.path);
      if (!targets) return workflow;
      const document = parseDocument(workflow.text);
      for (const target of targets) {
        document.setIn(["jobs", target.job, "strategy"], document.createNode({ matrix: { fixture_enabled: [false] } }));
        document.setIn(["jobs", target.job, "steps", target.stepIndex, "if"], "${{ matrix.fixture_enabled }}");
      }
      return { ...workflow, text: document.toString({ lineWidth: 0 }) };
    }),
  };
}
