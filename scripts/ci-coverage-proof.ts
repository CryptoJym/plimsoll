/**
 * eco-6hoxj.163.23 — every proof runs in CI or is reviewed local-only.
 *
 * Audits this checkout with scripts/lib/proof-ci-coverage.ts: every proof
 * entry file (named `proof`/`proof:*` leaves, proof files any package script
 * names, and proof files under scripts/) must be run by a canonical line in a
 * workflow step that provably runs on every successful push and pull request
 * to main, or be a declared sub-proof of a suite CI runs
 * (scripts/proof-suites.json), or carry a reviewed entry in
 * scripts/proof-local-only.json (local-only with the input CI lacks and a
 * recent reviewedOn/expiry pair, or quarantined for at most 30 days). The gate must also be the first proof
 * step in its job. Then every adversarial fixture in
 * scripts/lib/ci-coverage-fixtures.ts, including each case from both reviews
 * of PR #397, must reach the verdict a correct gate reaches.
 *
 * What the gate is for (threat model). It is a hygiene gate: it stops a proof
 * from being switched off by accident or by a lazy edit (a commented or
 * echoed line, `|| true`, a "temporarily disabled" script, a false `if:`, a
 * matrix that skips a step, an environment variable or pnpm setting that
 * turns a run into a no-op, a local-only list that quietly grows). Its rule
 * is an allow-list: anything it cannot read exactly the way GitHub, bash and
 * pnpm will is a failure, not a guess. It is not a security boundary against
 * someone who can edit the workflows, who could just as well edit the proofs.
 * It reads files and cannot see:
 * - its own step: `continue-on-error`, a false `if:` or a job setting that
 *   stops the gate from starting leaves CI green however red the gate is, so
 *   branch protection must require the proof job;
 * - what allowed commands do when they run: a package script, proof, action
 *   or install hook that exits early, edits files or writes $GITHUB_ENV
 *   itself;
 * - what `pnpm install` installs (the lockfile, a replaced tsx), and user
 *   configuration under HOME or XDG_CONFIG_HOME (the workflow points both at
 *   fresh directories; the gate does not check where they point);
 * - a name assembled at run time (such as NODE_ + OPTIONS), or an expression
 *   in an allowed action's inputs;
 * - whether a suite really runs what its receipt names, or whether a
 *   local-only proof really needs the input it declares;
 * - GitHub settings: repository variables, rulesets and required checks.
 *
 * CI runs it through `./node_modules/.bin/tsx scripts/run-proof.ts
 * scripts/ci-coverage-proof.ts`, not pnpm, so no pnpm setting can turn the
 * gate itself into a no-op.
 * Locally: pnpm proof:ci-coverage [--audit-only]
 * [--root DIR] [--today YYYY-MM-DD]
 *   --root audits another checkout (self-tests are skipped); --today checks
 *   quarantine expiry against another date. These options are local-only: the
 *   CI invocation must use this file with no arguments.
 */
import fs from "node:fs";
import path from "node:path";

import { FIXTURES, fixtureHolds } from "./lib/ci-coverage-fixtures";
import { createProofCompletion } from "./lib/proof-completion";
import {
  PROOF_EXCEPTIONS,
  WORKFLOW_DIRECTORY,
  gateFirstProblem,
  proofCiCoverage,
  readCoverageInput,
  type CoverageReport,
} from "./lib/proof-ci-coverage";

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
if (process.env.CI && (process.argv.slice(2).length > 0 || fs.realpathSync(process.cwd()) !== fs.realpathSync(repoRoot))) {
  throw new Error("CI coverage must audit its checkout from that checkout with no arguments");
}
const rootFlag = process.argv.indexOf("--root");
const auditRoot = rootFlag === -1 ? repoRoot : path.resolve(process.argv[rootFlag + 1] ?? "");
const selfTests = rootFlag === -1 && !process.argv.includes("--audit-only");
const completion = createProofCompletion("ci-coverage", selfTests ? FIXTURES.length + 2 : 2);
const todayFlag = process.argv.indexOf("--today");
const input = readCoverageInput(auditRoot, todayFlag === -1 ? undefined : process.argv[todayFlag + 1]);
const report = proofCiCoverage(input);

type Check = { name: string; origin: string; passed: boolean; detail?: unknown };
const checks: Check[] = [];

const gateGreen = (candidate: CoverageReport) =>
  candidate.errors.length === 0 && candidate.uncovered.length === 0 && gateFirstProblem(candidate) === null;

checks.push({
  name: "every_proof_runs_in_ci_or_is_reviewed",
  origin: "audit",
  passed: report.errors.length === 0 && report.uncovered.length === 0,
  detail: { errors: report.errors, uncovered: report.uncovered },
});
checks.push({
  name: "the_gate_is_the_first_proof_step_in_its_job",
  origin: "audit",
  passed: gateFirstProblem(report) === null,
  detail: gateFirstProblem(report),
});

if (selfTests) {
  for (const fixture of FIXTURES) {
    try {
      const fixtureCase = fixture.build(input);
      const result = proofCiCoverage(fixtureCase.input);
      const holds = fixtureHolds(fixtureCase, result, gateFirstProblem(result) !== null);
      const verdictMatches = gateGreen(result) === fixture.expectGateGreen;
      checks.push({
        name: `fixture_${fixture.name}`,
        origin: fixture.origin,
        passed: holds && verdictMatches,
        detail: { expectGateGreen: fixture.expectGateGreen, holds, uncovered: result.uncovered, errors: result.errors },
      });
    } catch (error) {
      checks.push({ name: `fixture_${fixture.name}`, origin: fixture.origin, passed: false, detail: String(error) });
    }
  }
}

const count = (status: string) => report.units.filter((unit) => unit.status === status).length;
const failed = checks.filter((check) => !check.passed);
for (const result of checks) completion.check(result.name, result.passed);
console.log(
  JSON.stringify(
    {
      schema: "plimsoll.ci-coverage-proof.v3",
      status: failed.length === 0 ? "passed" : "failed",
      audited: path.relative(repoRoot, auditRoot) || ".",
      root: fs.realpathSync(auditRoot),
      forwardedArgs: process.argv.slice(2),
      workflows: report.workflows,
      proofEntries: report.units.length,
      runInCi: count("ci"),
      runInsideCoveredProofs: count("runs-inside"),
      localOnly: count("local-only"),
      quarantined: report.units
        .filter((unit) => unit.status === "quarantined")
        .map((unit) => ({ unit: unit.unit, owner: unit.exception?.owner, expires: unit.exception?.expires })),
      uncovered: report.uncovered,
      errors: report.errors,
      checks: checks.map(({ name, origin, passed }) => ({ name, origin, passed })),
      failures: failed.map(({ name, detail }) => ({ name, detail })),
    },
    null,
    2,
  ),
);
if (failed.length > 0) {
  const lines = [...report.errors];
  for (const unit of report.units.filter((candidate) => candidate.status === "uncovered")) {
    const why = unit.ignored[0] ? ` (CI mentions it in "${unit.ignored[0].step}" but ${unit.ignored[0].reason})` : "";
    lines.push(`${unit.unit} [${unit.scripts.join(", ") || unit.sources.join(", ")}] is not run by ${WORKFLOW_DIRECTORY} and has no entry in ${PROOF_EXCEPTIONS}${why}`);
  }
  if (lines.length > 0) console.error(lines.map((line) => `  - ${line}`).join("\n"));
  process.exitCode = 1;
}
completion.complete();
