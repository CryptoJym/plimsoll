/**
 * eco-6hoxj.163.23 — every proof runs in CI or is reviewed local-only.
 *
 * Audits this checkout with the execution model in scripts/lib/proof-ci-coverage.ts:
 * every proof entry file (named `proof`/`proof:*` leaves, proof files any
 * package script runs, and proof files under scripts/) must be executed by a
 * workflow step that provably runs on every successful push and pull request
 * to main, or carry a reviewed entry in scripts/proof-local-only.json (with an
 * owner, and an expiry for quarantined red proofs). The gate must also be the
 * first proof step in its job. Then every adversarial fixture in
 * scripts/lib/ci-coverage-fixtures.ts, including each case from the PR #397
 * review, must reach the verdict a correct gate reaches.
 *
 * CI runs it with node (`node ./node_modules/tsx/dist/cli.mjs
 * ./scripts/ci-coverage-proof.ts`), not pnpm, so no pnpm setting can turn the
 * gate itself into a no-op. Locally: pnpm proof:ci-coverage [--audit-only]
 * [--root DIR] [--today YYYY-MM-DD]
 *   --root audits another checkout (self-tests are skipped); --today checks
 *   quarantine expiry against another date.
 */
import path from "node:path";

import { FIXTURES, fixtureHolds } from "./lib/ci-coverage-fixtures";
import {
  PROOF_EXCEPTIONS,
  WORKFLOW_DIRECTORY,
  gateFirstProblem,
  proofCiCoverage,
  readCoverageInput,
  type CoverageReport,
} from "./lib/proof-ci-coverage";

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const rootFlag = process.argv.indexOf("--root");
const auditRoot = rootFlag === -1 ? repoRoot : path.resolve(process.argv[rootFlag + 1] ?? "");
const selfTests = rootFlag === -1 && !process.argv.includes("--audit-only");
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
console.log(
  JSON.stringify(
    {
      schema: "plimsoll.ci-coverage-proof.v2",
      status: failed.length === 0 ? "passed" : "failed",
      audited: path.relative(repoRoot, auditRoot) || ".",
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
