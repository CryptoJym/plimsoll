import { Scalar, type YAMLMap, isMap, isScalar, isSeq, parseDocument, type Document } from "yaml";

import { GATE_ENTRY, MAX_QUARANTINE_DAYS, proofCiCoverage, type CoverageInput, type CoverageReport } from "./proof-ci-coverage";

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
 *
 * Fixtures edit the workflow whose step runs the gate, and pick script and
 * file names the repository does not use yet, so a legitimate change (a
 * renamed workflow, a new script that happens to share a fixture's name)
 * cannot turn the real gate red.
 */

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

// Fixture builders only copy their inputs. Reuse the real checkout's coverage
// model instead of parsing the same workflow again for every target lookup.
const coverageCache = new WeakMap<CoverageInput, CoverageReport>();
function coverage(input: CoverageInput): CoverageReport {
  let report = coverageCache.get(input);
  if (!report) {
    report = proofCiCoverage(input);
    coverageCache.set(input, report);
  }
  return report;
}

/** The workflow whose step runs the gate (counted or not, as an edit may have changed that): the one the fixtures edit. */
export function fixtureWorkflow(input: CoverageInput) {
  const gate = coverage(input).units.find((unit) => unit.unit === GATE_ENTRY);
  const invocation = gate?.covered[0] ?? gate?.ignored[0];
  if (!invocation) throw new Error(`no workflow step runs ${GATE_ENTRY}, so the fixtures have no workflow to edit`);
  return invocation.workflow;
}

/**
 * Proofs CI runs from a one-line `run: pnpm proof:…` step of the gate's own
 * workflow, the reviewer's three first; fixtures edit these, so they keep
 * working when any single CI line changes.
 */
export function fixtureTargets(input: CoverageInput): Target[] {
  const workflow = fixtureWorkflow(input);
  const text = workflowText(input);
  const targets: Target[] = [];
  for (const unit of coverage(input).units) {
    if (unit.status !== "ci") continue;
    for (const invocation of unit.covered) {
      const script = /^pnpm (proof:[\w:.-]+)$/.exec(invocation.command.trim())?.[1];
      if (!script || invocation.workflow !== workflow || invocation.via.join() !== script) continue;
      if (text.split("\n").filter((line) => line.trim() === `run: pnpm ${script}`).length !== 1) continue;
      if (unit.unit === GATE_ENTRY || targets.some((target) => target.script === script)) continue;
      targets.push({ line: `pnpm ${script}`, script, unit: unit.unit });
    }
  }
  const rank = (target: Target) => (PREFERRED_TARGETS.includes(target.script) ? PREFERRED_TARGETS.indexOf(target.script) : PREFERRED_TARGETS.length);
  targets.sort((a, b) => rank(a) - rank(b) || a.script.localeCompare(b.script));
  if (targets.length < 3) throw new Error(`fixtures need three one-line proof steps in ${workflow}`);
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
  const path = fixtureWorkflow(input);
  return input.workflows.find((candidate) => candidate.path === path)!.text;
}

/** Resolve a fixture's target from this checkout's workflow, including pnpm run and run-proof forms. */
function proofLine(input: CoverageInput, unit: string) {
  const workflow = fixtureWorkflow(input);
  const line = coverage(input).units.find((candidate) => candidate.unit === unit)?.covered
    .find((invocation) => invocation.workflow === workflow)?.command;
  if (!line) throw new Error(`fixture target ${unit} is not covered in ${workflow}`);
  return line;
}

function withWorkflowText(input: CoverageInput, text: string, path = fixtureWorkflow(input)): CoverageInput {
  const current = fixtureWorkflow(input);
  return {
    ...input,
    workflows: input.workflows.map((workflow) => (workflow.path === current ? { path, text } : workflow)),
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
  throw new Error(`fixture target \`${line}\` not found in the gate's workflow`);
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

/** Insert a new step just before the step whose run script has `line`. */
function insertStepBefore(input: CoverageInput, line: string, step: Record<string, unknown>) {
  return editWorkflow(input, (document) => {
    const found = findStep(document, line);
    const steps = found.job.get("steps", true);
    if (!isSeq(steps)) throw new Error("fixture job has no steps");
    steps.items.splice(found.index, 0, document.createNode(step));
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
type Exceptions = Record<string, Record<string, Record<string, unknown>>>;
const withExceptions = (input: CoverageInput, edit: (exceptions: Exceptions) => void) => {
  const exceptions = JSON.parse(JSON.stringify(input.exceptions ?? {})) as Exceptions;
  edit(exceptions);
  return { ...input, exceptions };
};
const withSuites = (input: CoverageInput, edit: (suites: Record<string, string[]>) => void) => {
  const suites = JSON.parse(JSON.stringify(input.suites ?? {})) as Record<string, string[]>;
  edit(suites);
  return { ...input, suites };
};
/** The same repository with some files added or replaced. */
const withFiles = (input: CoverageInput, files: Record<string, string>): CoverageInput => ({
  ...input,
  readFile: (file) => (Object.hasOwn(files, file) ? files[file]! : input.readFile(file)),
});
/** A fixture target's CI line replaced by an echo, so nothing in CI runs its proof. */
function outOfCi(input: CoverageInput, target: Target) {
  return editRun(input, target.line, (line) => [line.replace(target.line, "echo moved out of CI")]);
}

/** The first fixture target, out of CI and listed under `section` with `entry` (whatever the real exceptions hold). */
function asException(input: CoverageInput, section: "localOnly" | "quarantined", entry: Record<string, unknown>) {
  const [target] = fixtureTargets(input);
  return withExceptions(outOfCi(input, target!), (exceptions) => {
    const reviewed = section === "localOnly"
      ? { reviewedOn: input.today, expires: daysFromToday(input, MAX_QUARANTINE_DAYS), ...entry }
      : entry;
    exceptions[section] = { ...exceptions[section], [target!.unit]: reviewed };
  });
}

/** `days` after the input's today, YYYY-MM-DD. */
const daysFromToday = (input: CoverageInput, days: number) =>
  new Date(Date.parse(`${input.today}T00:00:00Z`) + days * 86_400_000).toISOString().slice(0, 10);

/** Fixture targets 2 and 3 out of CI and declared as sub-proofs of target 1 (whatever the real manifest holds). */
function syntheticSuite(input: CoverageInput) {
  const [parent, first, second] = fixtureTargets(input);
  const edited = withSuites(outOfCi(outOfCi(input, first!), second!), (suites) => {
    suites[parent!.unit] = [first!.unit, second!.unit];
  });
  return { input: edited, parent: parent!, children: [first!.unit, second!.unit] };
}

const RENAMED_PROOF = "scripts/review-renamed-proof.ts";
const renamedProofFile = { [RENAMED_PROOF]: 'throw new Error("this proof must run, but CI never runs it");\n' };
/** A proof file the repository does not have yet, run only by a new non-proof package script. */
function withRenamedProof(input: CoverageInput) {
  let file = RENAMED_PROOF;
  for (let suffix = 2; input.proofFiles.includes(file) || input.readFile(file) !== null; suffix += 1) {
    file = RENAMED_PROOF.replace(/-proof\.ts$/, `-${suffix}-proof.ts`);
  }
  const script = unusedScriptName(input.scripts, "verify:renamed-proof");
  return { input: { ...withScripts(input, { [script]: `tsx ${file}` }), proofFiles: [...input.proofFiles, file] }, file };
}

/** A case built on the first fixture target. */
function onTarget(edit: (input: CoverageInput, target: Target) => CoverageInput, expect: "uncovered" | "covered") {
  return (input: CoverageInput): FixtureCase => {
    const [target] = fixtureTargets(input);
    return { input: edit(input, target!), [expect]: [target!.unit] };
  };
}

/** A case on the first fixture target that must leave it uncovered and report `error`. */
function rejectedOnTarget(edit: (input: CoverageInput, target: Target) => CoverageInput, error: RegExp) {
  return (input: CoverageInput): FixtureCase => {
    const [target] = fixtureTargets(input);
    return { input: edit(input, target!), uncovered: [target!.unit], error };
  };
}

/** `base`, or `base-2`, `base-3`, ... whichever package.json does not use yet. */
export function unusedScriptName(scripts: Record<string, string>, base: string) {
  let name = base;
  for (let suffix = 2; Object.hasOwn(scripts, name); suffix += 1) name = `${base}-${suffix}`;
  return name;
}

export const FIXTURES: Fixture[] = [
  // ---- Review r1 (input/review-r1/checks/adversarial-*) -------------------
  {
    name: "reviewer_proof_script_named_outside_proof_namespace",
    origin: "reviewer",
    expectGateGreen: false,
    describe: "an unwired package script `verify:renamed-proof` runs a proof file",
    build: (input) => {
      const renamed = withRenamedProof(input);
      return { input: renamed.input, uncovered: [renamed.file] };
    },
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
    build: onTarget((input, t) => {
      const alias = unusedScriptName(input.scripts, "ci:alias");
      return editRun(withScripts(input, { [alias]: t.line }), t.line, (line) => [line.replace(t.line, `pnpm ${alias}`)]);
    }, "covered"),
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
    describe: "the gate's workflow file is renamed",
    build: (input) => {
      const current = fixtureWorkflow(input);
      const renamed = current.replace(/[^/]+$/, (name) => `renamed-${name}`);
      return { input: withWorkflowText(input, workflowText(input), renamed), covered: [GATE_ENTRY] };
    },
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
      const renamed = withRenamedProof(edited);
      return { input: renamed.input, uncovered: [first!.unit, second!.unit, third!.unit, renamed.file] };
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
      const gateLine = coverage(input).units.find((unit) => unit.unit === GATE_ENTRY)?.covered[0]?.command;
      if (!gateLine) throw new Error("the gate is not run by CI");
      return { input: editRun(input, gateLine, (line) => [line.replace(gateLine, t!.line), line]), gateNotFirst: true };
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
    describe: "a local-only entry has a blank owner",
    build: (input) => ({ input: asException(input, "localOnly", { owner: " ", needs: ["PLIMSOLL_FIXTURE_INPUT"], reason: "fixture" }), error: /missing owner/ }),
    replayableOnTextualGate: false,
  },
  {
    name: "quarantine_expired",
    origin: "gate",
    expectGateGreen: false,
    describe: "a quarantine expired yesterday",
    build: (input) => ({ input: asException(input, "quarantined", { owner: "fixture", expires: daysFromToday(input, -1), reason: "fixture" }), error: /quarantine expired/ }),
    replayableOnTextualGate: false,
  },
  {
    name: "quarantine_within_the_horizon_is_accepted",
    origin: "gate",
    expectGateGreen: true,
    describe: `a quarantine that expires in exactly ${MAX_QUARANTINE_DAYS} days`,
    build: (input) => ({ input: asException(input, "quarantined", { owner: "fixture", expires: daysFromToday(input, MAX_QUARANTINE_DAYS), reason: "fixture" }) }),
    replayableOnTextualGate: false,
  },
  ...([
    ["review2_quarantine_far_future_expiry", "review2", "9999-12-31", /more than 30 days away/],
    ["review2_quarantine_impossible_date", "review2", "2026-99-99", /must be real calendar dates/],
    ["quarantine_day_that_does_not_exist", "gate", "2026-02-30", /must be real calendar dates/],
  ] as const).map(([name, origin, expires, error]): Fixture => ({
    name,
    origin,
    expectGateGreen: false,
    describe: `a quarantine expires on ${expires}`,
    build: (input) => ({ input: asException(input, "quarantined", { owner: "fixture", expires, reason: "fixture" }), error }),
    replayableOnTextualGate: false,
  })),
  {
    name: "review2_known_red_moved_to_local_only",
    origin: "review2",
    expectGateGreen: false,
    describe: "a red proof is listed as local-only with just an owner and a reason",
    build: (input) => ({ input: asException(input, "localOnly", { owner: "fixture", reason: "moved out of quarantine" }), error: /missing needs/ }),
    replayableOnTextualGate: false,
  },
  {
    name: "local_only_need_the_proof_does_not_read",
    origin: "gate",
    expectGateGreen: false,
    describe: "a local-only entry names an input its proof never reads",
    build: (input) => ({
      input: asException(input, "localOnly", { owner: "fixture", needs: ["PLIMSOLL_FIXTURE_INPUT"], reason: "fixture" }),
      error: /does not require process\.env\.PLIMSOLL_FIXTURE_INPUT/,
    }),
    replayableOnTextualGate: false,
  },
  {
    name: "local_only_need_that_ci_provides",
    origin: "gate",
    expectGateGreen: false,
    describe: "a local-only entry names an input CI always sets",
    build: (input) => ({ input: asException(input, "localOnly", { owner: "fixture", needs: ["GITHUB_SHA"], reason: "fixture" }), error: /CI provides GITHUB_SHA/ }),
    replayableOnTextualGate: false,
  },
  {
    name: "local_only_review_metadata_required",
    origin: "gate",
    expectGateGreen: false,
    describe: "a local-only entry without a reviewedOn date and expiry cannot park a proof",
    build: (input) => {
      const [target] = fixtureTargets(input);
      const edited = withExceptions(outOfCi(input, target!), (exceptions) => {
        exceptions.localOnly = {
          ...exceptions.localOnly,
          [target!.unit]: { owner: "fixture", needs: ["PLIMSOLL_FIXTURE_INPUT"], reason: "fixture" },
        };
      });
      return { input: edited, error: /reviewedOn/ };
    },
    replayableOnTextualGate: false,
  },
  ...([
    ["assigned_variable", "scripts/provider-capacity-adapters-proof.ts", "FAKE_BEHAVIOR"],
    ["optional_knob", "scripts/otlp-intake-spool-proof.ts", "OTLP_SPOOL_PROOF_ONLY"],
  ] as const).map(([name, unit, need]): Fixture => ({
    name: `local_only_${name}_cannot_qualify`,
    origin: "gate",
    expectGateGreen: false,
    describe: `${need} does not establish an input CI lacks`,
    build: (input) => {
      const edited = editRun(input, proofLine(input, unit), () => ["echo moved out of CI"]);
      return {
        input: withExceptions(edited, (exceptions) => {
          exceptions.localOnly = { ...exceptions.localOnly, [unit]: {
            owner: "fixture", needs: [need], reviewedOn: input.today,
            expires: daysFromToday(input, MAX_QUARANTINE_DAYS), reason: "fixture",
          } };
        }),
        error: new RegExp(`does not require process\\.env\\.${need}`),
      };
    },
  })),
  {
    name: "local_only_stale_review_fresh_expiry",
    origin: "gate",
    expectGateGreen: false,
    describe: "a recent expiry cannot extend a review from 2025",
    build: (input) => ({
      input: withExceptions(input, (exceptions) => {
        const entry = exceptions.localOnly?.["scripts/oversized-continuation-rollback-proof.ts"];
        if (!entry) throw new Error("rollback local-only entry missing");
        entry.reviewedOn = "2025-01-01";
        entry.expires = daysFromToday(input, 26);
      }),
      error: /more than 30 days after reviewedOn/,
    }),
  },
  {
    name: "stale_exception_entry",
    origin: "gate",
    expectGateGreen: false,
    describe: "an exception names a proof that no longer exists",
    build: (input) => ({
      input: withExceptions(input, (exceptions) => {
        exceptions.localOnly = { ...exceptions.localOnly, "scripts/no-such-proof.ts": { owner: "fixture", needs: ["PLIMSOLL_FIXTURE_INPUT"], reason: "stale" } };
      }),
      error: /stale entry/,
    }),
    replayableOnTextualGate: false,
  },
  {
    name: "suite_sub_proofs_run_inside",
    origin: "gate",
    expectGateGreen: true,
    describe: "two proofs leave CI and are declared as sub-proofs of a suite CI runs",
    build: (input) => ({ input: syntheticSuite(input).input }),
    replayableOnTextualGate: false,
  },
  {
    name: "review2_suite_sub_proof_dropped",
    origin: "review2",
    expectGateGreen: false,
    describe: "a sub-proof is dropped from its suite's run list",
    build: (input) => {
      const suite = syntheticSuite(input);
      return {
        input: withSuites(suite.input, (suites) => {
          suites[suite.parent.unit] = suite.children.slice(1);
        }),
        uncovered: [suite.children[0]!],
      };
    },
    replayableOnTextualGate: false,
  },
  {
    name: "suite_not_run_in_ci",
    origin: "gate",
    expectGateGreen: false,
    describe: "the suite itself leaves CI",
    build: (input) => {
      const suite = syntheticSuite(input);
      return { input: outOfCi(suite.input, suite.parent), uncovered: suite.children, error: /not run by CI, so none of its sub-proofs are/ };
    },
    replayableOnTextualGate: false,
  },
  {
    name: "suite_declares_a_missing_file",
    origin: "gate",
    expectGateGreen: false,
    describe: "a suite declares a sub-proof file that does not exist",
    build: (input) => {
      const suite = syntheticSuite(input);
      return {
        input: withSuites(suite.input, (suites) => {
          suites[suite.parent.unit] = [...suite.children, "scripts/no-such-sub-proof.ts"];
        }),
        error: /no-such-sub-proof\.ts is not a proof file on disk/,
      };
    },
    replayableOnTextualGate: false,
  },
  // ---- Review 2: package scripts must be canonical -----------------------
  ...([
    ["disabled_prefix", "`echo '… disabled' && exit 0;` before the proof", (t: Target) => `echo '${t.script} temporarily disabled' && exit 0; tsx ${t.unit}`],
    ["exit_prefix", "`exit 0;` before the proof", (t: Target) => `exit 0; tsx ${t.unit}`],
    ["trap_exit", "`trap 'exit 0' EXIT;` before the proof", (t: Target) => `trap 'exit 0' EXIT; tsx ${t.unit}`],
    ["node_check_flag", "`node --check`, which parses the file without running it", (t: Target) => `node --check ${t.unit}`],
    ["self_alias_then_file", "the script calls itself before the proof", (t: Target) => `pnpm ${t.script} && tsx ${t.unit}`],
  ] as const).map(([label, what, body]): Fixture => ({
    name: `review2_package_script_${label}`,
    origin: "review2",
    expectGateGreen: false,
    describe: `the proof's package script gains ${what}`,
    build: rejectedOnTarget((input, t) => withScripts(input, { [t.script]: body(t) }), /is not a canonical proof command/),
  })),
  {
    name: "review2_package_script_alias_cycle",
    origin: "review2",
    expectGateGreen: false,
    describe: "the proof's package script becomes an alias loop through two other scripts",
    build: rejectedOnTarget((input, t) => {
      const first = unusedScriptName(input.scripts, "fixture:cycle-a");
      const second = unusedScriptName({ ...input.scripts, [first]: "" }, "fixture:cycle-b");
      return withScripts(input, { [t.script]: `pnpm ${first}`, [first]: `pnpm ${second}`, [second]: `pnpm ${t.script}` });
    }, /alias cycle/),
  },
  {
    name: "package_script_environment_prefix",
    origin: "gate",
    expectGateGreen: false,
    describe: "the proof's package script sets NODE_OPTIONS before the runner",
    build: rejectedOnTarget((input, t) => withScripts(input, { [t.script]: `NODE_OPTIONS=--require=./exit0.cjs tsx ${t.unit}` }), /sets the environment/),
  },
  {
    name: "package_script_pre_hook",
    origin: "gate",
    expectGateGreen: false,
    describe: "a pre<script> hook runs before the proof",
    build: rejectedOnTarget((input, t) => withScripts(input, { [`pre${t.script}`]: "echo before" }), /pnpm also runs pre/),
  },
  {
    name: "package_script_node_import_tsx_counts",
    origin: "gate",
    expectGateGreen: true,
    describe: "the proof's package script runs `node --expose-gc --import tsx <file>`",
    build: onTarget((input, t) => withScripts(input, { [t.script]: `node --expose-gc --import tsx ${t.unit}` }), "covered"),
  },
  // ---- Review 2: workflow lines must be canonical -------------------------
  {
    name: "review2_expression_after_the_proof_command",
    origin: "review2",
    expectGateGreen: false,
    describe: "the proof line gains `${{ vars.PROOF_ARGS }}`; a repository variable `|| true` would hide failures",
    build: rejectedOnTarget((input, t) => editRun(input, t.line, (line) => [line + " ${{ vars.PROOF_ARGS }}"]), /pastes a GitHub expression/),
  },
  {
    name: "review2_expression_echoed_before_the_proof",
    origin: "review2",
    expectGateGreen: false,
    describe: "the proof step first echoes the pull request title, which can end the script",
    build: rejectedOnTarget(
      (input, t) => editRun(input, t.line, (line) => [line.replace(t.line, 'echo "Checking ${{ github.event.pull_request.title }}"'), line]),
      /pastes a GitHub expression/,
    ),
  },
  {
    name: "expression_in_an_earlier_step",
    origin: "gate",
    expectGateGreen: false,
    describe: "an earlier step in the proof job echoes the pull request title",
    build: rejectedOnTarget(
      (input, t) => insertStepBefore(input, t.line, { name: "Show the pull request title", run: 'echo "${{ github.event.pull_request.title }}"' }),
      /pastes a GitHub expression/,
    ),
  },
  {
    name: "review2_heredoc_with_a_partly_quoted_delimiter",
    origin: "review2",
    expectGateGreen: false,
    describe: "a heredoc whose delimiter bash reads differently swallows the proof line",
    build: rejectedOnTarget(
      (input, t) => editRun(input, t.line, (line) => [line.replace(t.line, 'cat <<"E"OF'), line.replace(t.line, "E"), line, line.replace(t.line, "EOF")]),
      /heredocs are not allowed/,
    ),
  },
  {
    name: "head_sha_expression_counts",
    origin: "gate",
    expectGateGreen: true,
    describe: "the proof line passes the head-SHA expression as an argument",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line + ' -- --commit "${{ github.event.pull_request.head.sha || github.sha }}"']), "covered"),
  },
  {
    name: "assignment_prefix_and_redirect_count",
    origin: "gate",
    expectGateGreen: true,
    describe: "the proof line sets a plain variable and saves its output to a file",
    build: onTarget(
      (input, t) => editRun(input, t.line, (line) => [line.replace(t.line, `PLIMSOLL_PROOF_HOME="$TMPDIR" ${t.line} > evidence/fixture-proof.json`)]),
      "covered",
    ),
  },
  {
    name: "local_only_parked_with_optional_knob",
    origin: "gate",
    expectGateGreen: false,
    describe: "an optional proof knob alone cannot justify a local-only declaration without review metadata",
    build: (input) => {
      let edited = editWorkflow(input, (document) => {
        const found = findStep(document, proofLine(input, "scripts/otlp-intake-spool-proof.ts"));
        const steps = found.job.get("steps", true);
        if (!isSeq(steps)) throw new Error("fixture workflow has no steps");
        steps.items.splice(found.index, 1);
      });
      edited = withExceptions(edited, (exceptions) => {
        exceptions.localOnly = {
          ...exceptions.localOnly,
          "scripts/otlp-intake-spool-proof.ts": {
            owner: "fixture",
            needs: ["OTLP_SPOOL_PROOF_ONLY"],
            reason: "optional knob",
          },
        };
      });
      return { input: edited, error: /reviewedOn/ };
    },
    replayableOnTextualGate: false,
  },
  // ---- Review 2: nothing may change how the proofs run ---------------------
  {
    name: "review2_npm_config_script_shell_prefix",
    origin: "review2",
    expectGateGreen: false,
    describe: "the proof line sets npm_config_script_shell=/usr/bin/true first",
    build: (input) => {
      const [t] = fixtureTargets(input);
      return { input: editRun(input, t!.line, (line) => [line.replace(t!.line, `npm_config_script_shell=/usr/bin/true ${t!.line}`)]), error: /npm_config_script_shell/ };
    },
  },
  ...([
    ["review2_step_env_bash_env", "review2", "BASH_ENV", ".github/ci-skip.sh"],
    ["review2_step_env_node_options", "review2", "NODE_OPTIONS", "--require ./.github/ci-exit0.cjs"],
  ] as const).map(([name, origin, key, value]): Fixture => ({
    name,
    origin,
    expectGateGreen: false,
    describe: `the proof step's env sets ${key}`,
    build: (input) => {
      const [t] = fixtureTargets(input);
      return { input: setStepKey(input, t!.line, "env", { [key]: value }), error: new RegExp(`env: sets ${key}`) };
    },
  })),
  {
    name: "job_env_path",
    origin: "gate",
    expectGateGreen: false,
    describe: "the proof job's env replaces PATH",
    build: (input) => {
      const [t] = fixtureTargets(input);
      return { input: setJobKey(input, t!.line, "env", { PATH: "/tmp/fake-bin" }), error: /env: sets PATH/ };
    },
  },
  {
    name: "github_env_write_before_proofs",
    origin: "gate",
    expectGateGreen: false,
    describe: "an earlier step writes NODE_OPTIONS to $GITHUB_ENV",
    build: (input) => {
      const [t] = fixtureTargets(input);
      const run = 'echo "NODE_OPTIONS=--require ./.github/ci-exit0.cjs" >> "$GITHUB_ENV"';
      return { input: insertStepBefore(input, t!.line, { name: "Tune Node", run }), error: /NODE_OPTIONS.*environment allow-list/ };
    },
  },
  {
    name: "github_path_write_before_proofs",
    origin: "gate",
    expectGateGreen: false,
    describe: "an earlier step prepends a directory to $GITHUB_PATH",
    build: (input) => {
      const [t] = fixtureTargets(input);
      return { input: insertStepBefore(input, t!.line, { name: "Add tools", run: 'echo "$RUNNER_TEMP/bin" >> "$GITHUB_PATH"' }), error: /GITHUB_PATH/ };
    },
  },
  {
    name: "unknown_action_before_proofs",
    origin: "gate",
    expectGateGreen: false,
    describe: "an unknown action runs before the proofs",
    build: (input) => {
      const [t] = fixtureTargets(input);
      return { input: insertStepBefore(input, t!.line, { uses: "example/setup-anything@v1" }), error: /runs action example\/setup-anything@v1/ };
    },
  },
  // ---- Legitimate command forms that should remain green ------------------
  {
    name: "legit_pnpm_run_form",
    origin: "gate",
    expectGateGreen: true,
    describe: "pnpm run <script> is equivalent to pnpm <script>",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line.replace(t.line, `pnpm run ${t.script}`)]), "covered"),
  },
  {
    name: "legit_node_memory_flag_on_line",
    origin: "gate",
    expectGateGreen: true,
    describe: "node's benign --max-old-space-size=N flag is accepted",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line.replace(t.line, `node --max-old-space-size=8192 --import tsx ${t.unit}`)]), "covered"),
  },
  {
    name: "legit_set_euo_pipefail_preamble",
    origin: "gate",
    expectGateGreen: true,
    describe: "an explicit errexit/pipefail preamble is harmless",
    build: onTarget((input, t) => editRun(input, t.line, (line) => ["set -euo pipefail", line]), "covered"),
  },
  {
    name: "legit_group_echo_lines",
    origin: "gate",
    expectGateGreen: true,
    describe: "GitHub group markers around a proof are harmless",
    build: onTarget(
      (input, t) => editRun(input, t.line, (line) => ['echo "::group::proof"', line, 'echo "::endgroup::"']),
      "covered",
    ),
  },
  {
    name: "legit_github_sha_arg",
    origin: "gate",
    expectGateGreen: true,
    describe: "github.sha is an inert proof argument",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [`${line} --commit \"\${{ github.sha }}\"`]), "covered"),
  },
  {
    name: "legit_job_env_node_memory",
    origin: "gate",
    expectGateGreen: true,
    describe: "a benign job-level NODE_OPTIONS heap limit is accepted",
    build: (input) => {
      const [target] = fixtureTargets(input);
      return { input: setJobKey(input, target!.line, "env", { NODE_OPTIONS: "--max-old-space-size=8192" }), covered: [target!.unit] };
    },
  },
  {
    name: "legit_actions_cache_before_proofs",
    origin: "gate",
    expectGateGreen: true,
    describe: "the standard actions/cache action is allowed before proofs",
    build: (input) => {
      const [target] = fixtureTargets(input);
      return { input: insertStepBefore(input, target!.line, { uses: "actions/cache@v4", with: { path: ".cache/fixtures", key: "fixtures-v1" } }), covered: [target!.unit] };
    },
  },
  ...(["~/.npmrc", "node_modules", ".pnpmfile.cjs"] as const).map((cachePath): Fixture => ({
    name: `cache_proof_control_${cachePath.replace(/[^a-z]+/gi, "_")}`,
    origin: "gate",
    expectGateGreen: false,
    describe: `actions/cache cannot restore ${cachePath} before proofs`,
    build: (input) => ({
      input: insertStepBefore(input, proofLine(input, GATE_ENTRY), { uses: "actions/cache@v4", with: { path: cachePath, key: "fixture-v1" } }),
      error: /caches a proof-controlling path/,
    }),
  })),
  {
    name: "legit_runner_temp_expression",
    origin: "gate",
    expectGateGreen: true,
    describe: "runner.temp is inert when passed as a proof argument",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line.replace(t.line, `${t.line} --tmp \"\${{ runner.temp }}\"`)]), "covered"),
  },
  {
    name: "legit_github_workspace_expression",
    origin: "gate",
    expectGateGreen: true,
    describe: "github.workspace is inert when passed as a proof argument",
    build: onTarget((input, t) => editRun(input, t.line, (line) => [line.replace(t.line, `${t.line} --root \"\${{ github.workspace }}\"`)]), "covered"),
  },
  ...([
    ["review2_npmrc_script_shell", "review2", ".npmrc", "script-shell=/usr/bin/true\n", /\.npmrc sets script-shell/],
    ["npmrc_node_options", "gate", ".npmrc", "node-options=--require ./exit0.cjs\n", /\.npmrc sets node-options/],
    ["pnpm_workspace_script_shell", "gate", "pnpm-workspace.yaml", "packages: []\nscriptShell: /usr/bin/true\n", /pnpm-workspace\.yaml sets scriptShell/],
    ["pnpmfile_present", "gate", ".pnpmfile.cjs", "process.exit(0);\n", /\.pnpmfile\.cjs exists/],
  ] as const).map(([name, origin, file, text, error]): Fixture => ({
    name,
    origin,
    expectGateGreen: false,
    describe: `${file} gains \`${text.trim().split("\n").at(-1)}\``,
    build: (input) => ({ input: withFiles(input, { [file]: text }), error }),
    files: { [file]: text },
    replayableOnTextualGate: false,
  })),
  // ---- Round 3 follow-ups --------------------------------------------------
  ...(["--today 2026-10-06", "--root /tmp/other-checkout", "--audit-only"] as const).map((argument, index): Fixture => ({
    name: ["gate_today_pinned", "gate_root_argument", "gate_audit_only_argument"][index]!,
    origin: "gate",
    expectGateGreen: false,
    describe: `the gate's own CI invocation cannot pass ${argument}`,
    build: (input) => {
      const gate = coverage(input).units.find((unit) => unit.unit === GATE_ENTRY)?.covered[0];
      if (!gate) throw new Error("the gate is not run by CI");
      return { input: editRun(input, gate.command, (line) => [`${line} ${argument}`]), error: /passes arguments to .*ci-coverage-proof/ };
    },
  })),
  {
    name: "gate_direct_node_flag",
    origin: "gate",
    expectGateGreen: false,
    describe: "the gate may not be bundled into a disposable root",
    build: (input) => {
      const gate = proofLine(input, GATE_ENTRY);
      return {
        input: editRun(input, gate, () => [gate.replace(GATE_ENTRY, `--direct-node ${GATE_ENTRY}`)]),
        error: /passes arguments to .*ci-coverage-proof/,
      };
    },
  },
  ...(["none", "busy"] as const).map((value): Fixture => ({
    name: `otlp_only_env_prefix_${value === "none" ? "none" : "one_stage"}`,
    origin: "gate",
    expectGateGreen: false,
    describe: `the OTLP proof line sets its own OTLP_SPOOL_PROOF_ONLY=${value} setting`,
    build: (input) => ({
      input: editRun(input, proofLine(input, "scripts/otlp-intake-spool-proof.ts"), (line) => [`OTLP_SPOOL_PROOF_ONLY=${value} ${line}`]),
      error: /proof-owned CI settings/,
    }),
  })),
  ...(["step", "job", "workflow"] as const).map((level): Fixture => ({
    name: `otlp_only_${level}_env_none`,
    origin: "gate",
    expectGateGreen: false,
    describe: `the ${level} env sets OTLP_SPOOL_PROOF_ONLY=none for the counted OTLP proof`,
    build: (input) => {
      const line = proofLine(input, "scripts/otlp-intake-spool-proof.ts");
      const env = { OTLP_SPOOL_PROOF_ONLY: "none" };
      const edited = level === "step" ? setStepKey(input, line, "env", env)
        : level === "job" ? setJobKey(input, line, "env", env)
        : editWorkflow(input, (document) => document.set("env", env));
      return { input: edited, error: /proof-owned CI settings/ };
    },
  })),
  {
    name: "pnpm_config_set_script_shell_line",
    origin: "gate",
    expectGateGreen: false,
    describe: "a proof step cannot rewrite pnpm's script shell before running the proof",
    build: (input) => ({
      input: editRun(input, proofLine(input, "scripts/usage-dedupe-proof.ts"), (line) => ["pnpm config set script-shell /usr/bin/true", line]),
      error: /package-manager config\/set command/,
    }),
  },
  {
    name: "home_npmrc_written_before_proofs",
    origin: "gate",
    expectGateGreen: false,
    describe: "a step before proofs cannot write a user npmrc/pnpm rc file",
    build: (input) => ({
      input: insertStepBefore(input, proofLine(input, "scripts/usage-dedupe-proof.ts"), { name: "Configure package manager", run: 'echo "script-shell=/usr/bin/true" >> "$HOME/.npmrc"' }),
      error: /writes a package-manager rc file/,
    }),
  },
  {
    name: "npm_config_set_script_shell_line",
    origin: "gate",
    expectGateGreen: false,
    describe: "a proof step cannot rewrite npm config before running the proof",
    build: (input) => ({
      input: editRun(input, proofLine(input, "scripts/usage-dedupe-proof.ts"), (line) => ["npm config set script-shell /usr/bin/true", line]),
      error: /package-manager config\/set command/,
    }),
  },
  {
    name: "pnpm_rc_written_before_proofs",
    origin: "gate",
    expectGateGreen: false,
    describe: "a proof step cannot write a pnpm rc file",
    build: (input) => ({
      input: insertStepBefore(input, proofLine(input, "scripts/usage-dedupe-proof.ts"), { name: "Configure pnpm rc", run: 'echo "script-shell=/usr/bin/true" >> "$HOME/pnpm/rc"' }),
      error: /writes a package-manager rc file/,
    }),
  },
  ...([
    ["pnpm_c_set", "pnpm c set script-shell /usr/bin/true"],
    ["npm_set", "npm set script-shell=/usr/bin/true"],
    ["pnpm_flag_config_set", "pnpm --dir . config set script-shell /usr/bin/true"],
    ["continued_config_set", "pnpm \\\n config set script-shell /usr/bin/true"],
  ] as const).map(([name, command]): Fixture => ({
    name,
    origin: "gate",
    expectGateGreen: false,
    describe: `package-manager settings cannot be changed with ${name}`,
    build: (input) => ({
      input: insertStepBefore(input, proofLine(input, "scripts/usage-dedupe-proof.ts"), { name: "Configure package manager", run: command }),
      error: /package-manager config\/set command/,
    }),
  })),
  {
    name: "copy_rc_before_proof",
    origin: "gate",
    expectGateGreen: false,
    describe: "a copy cannot replace the user's npmrc before a proof",
    build: (input) => ({
      input: insertStepBefore(input, proofLine(input, "scripts/usage-dedupe-proof.ts"), { name: "Copy npmrc", run: 'cp .github/ci.rc "$HOME/.npmrc"' }),
      error: /writes a package-manager rc file/,
    }),
  },
  {
    name: "otlp_export_before_proof",
    origin: "gate",
    expectGateGreen: false,
    describe: "a proof knob exported in its step cannot skip the OTLP stages",
    build: (input) => ({
      input: editRun(input, proofLine(input, "scripts/otlp-intake-spool-proof.ts"), (line) => ["export OTLP_SPOOL_PROOF_ONLY=none", line]),
      error: /OTLP_SPOOL_PROOF_ONLY.*environment allow-list/,
    }),
  },
  {
    name: "otlp_github_env_before_proof",
    origin: "gate",
    expectGateGreen: false,
    describe: "an earlier GITHUB_ENV write cannot skip the OTLP stages",
    build: (input) => ({
      input: insertStepBefore(input, proofLine(input, "scripts/otlp-intake-spool-proof.ts"), { name: "Set OTLP knob", run: 'echo "OTLP_SPOOL_PROOF_ONLY=none" >> "$GITHUB_ENV"' }),
      error: /OTLP_SPOOL_PROOF_ONLY.*environment allow-list/,
    }),
  },
  {
    name: "qualification_artifact_export",
    origin: "gate",
    expectGateGreen: false,
    describe: "a proof cannot select a prebuilt qualification artifact in CI",
    build: (input) => ({
      input: editRun(input, proofLine(input, "scripts/lifecycle-operator-proof.ts"), (line) => ["export PLIMSOLL_QUALIFICATION_ARTIFACT=evidence/other-cli.mjs", line]),
      error: /PLIMSOLL_QUALIFICATION_ARTIFACT.*environment allow-list/,
    }),
  },
  // ---- Review 2: YAML the gate cannot read the way GitHub does -----------
  {
    name: "review2_merge_key_disabled_step",
    origin: "review2",
    expectGateGreen: false,
    describe: "a proof step merges in `<<: *skip_step`, whose `if:` is false",
    build: (input) => {
      const [, t] = fixtureTargets(input);
      const edited = editWorkflow(input, (document) => {
        const found = findStep(document, t!.line);
        const steps = found.job.get("steps", true);
        if (!isSeq(steps)) throw new Error("fixture job has no steps");
        const skip = document.createNode({ name: "Never", if: false, run: "echo never" });
        skip.anchor = "skip_step";
        steps.items.unshift(skip);
        findStep(document, t!.line).step.items.unshift(document.createPair("<<", document.createAlias(skip)));
      });
      return { input: edited, error: /YAML merge key/ };
    },
  },
  ...([
    ["unknown_step_key", "a proof step gains `timeout: 1`, which GitHub does not define", (input: CoverageInput, t: Target) => setStepKey(input, t.line, "timeout", 1)],
    ["unknown_job_key", "the proof job gains `skip-if: true`", (input: CoverageInput, t: Target) => setJobKey(input, t.line, "skip-if", true)],
    ["unknown_workflow_key", "the workflow gains a top-level `skip: true`", (input: CoverageInput) => editWorkflow(input, (document) => document.set("skip", true))],
  ] as const).map(([name, describe, edit]): Fixture => ({
    name,
    origin: "gate",
    expectGateGreen: false,
    describe,
    build: (input) => {
      const [t] = fixtureTargets(input);
      return { input: edit(input, t!), error: /unknown key/ };
    },
  })),
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

/** Append `suffix` to each given CI line, as the second PR #397 review did with `${{ vars.PROOF_ARGS }}`. */
export function appendToCiLines(
  input: CoverageInput,
  lines: Array<{ workflow: string; job: string; stepIndex: number; command: string }>,
  suffix: string,
): CoverageInput {
  return {
    ...input,
    workflows: input.workflows.map((workflow) => {
      const targets = lines.filter((line) => line.workflow === workflow.path);
      if (targets.length === 0) return workflow;
      const document = parseDocument(workflow.text);
      for (const target of targets) {
        const path = ["jobs", target.job, "steps", target.stepIndex, "run"];
        const run = String(document.getIn(path));
        const edited = run.split("\n").map((line) => (line.trim() === target.command ? `${line}${suffix}` : line));
        document.setIn(path, block(document, `${edited.join("\n").replace(/\n+$/, "")}\n`));
      }
      return { ...workflow, text: document.toString({ lineWidth: 0 }) };
    }),
  };
}

/** Prefix each given package script with a "temporarily disabled" exit, as the second PR #397 review did. */
export function disableScripts(input: CoverageInput, scripts: string[]): CoverageInput {
  const disabled = scripts.map((name) => [name, `echo '${name} temporarily disabled' && exit 0; ${input.scripts[name] ?? ""}`]);
  return { ...input, scripts: { ...input.scripts, ...Object.fromEntries(disabled) } };
}
