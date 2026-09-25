/**
 * Issue 0013 — proof for the generated privacy spec page.
 *
 * Proves that docs/privacy-spec.md is exactly what the real source lists
 * produce, that the generator is deterministic, and — adversarially — that
 * hand-edits to the page, edits to the source lists, and phantom proof-check
 * references are all detected instead of silently passing.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { appendToCiLines, disableScripts, disableSteps } from "./lib/ci-coverage-fixtures";
import {
  WORKFLOW_DIRECTORY,
  proofCiCoverage,
  proofFilesRunInCi,
  readCoverageInput,
  type CoverageReport,
} from "./lib/proof-ci-coverage";
import {
  PROOF_CHECKS,
  collectPrivacySpecModel,
  docIsStale,
  renderPrivacySpec,
  verifyProofChecks,
  type PrivacySpecModel,
} from "./privacy-spec";

type ProofCheck = { name: string; adversarial: boolean };
const checks: ProofCheck[] = [];

function check(
  name: string,
  adversarial: boolean,
  run: () => void,
): void {
  run();
  checks.push({ name, adversarial });
}

function mutatedModel(mutate: (model: PrivacySpecModel) => void): PrivacySpecModel {
  const model = collectPrivacySpecModel();
  mutate(model);
  return model;
}

/**
 * eco-6hoxj.163.23: a cited check is evidence only while CI runs it. Returns
 * each cited check defined in a proof file that no workflow step provably runs
 * on every successful push and pull request (the proof:ci-coverage model).
 */
function citedChecksNotRunInCi(found: Map<string, string[]>, coverage: CoverageReport): string[] {
  const runInCi = proofFilesRunInCi(coverage);
  const missing: string[] = [];
  for (const ref of Object.values(PROOF_CHECKS)) {
    for (const name of ref.checks) {
      const files = found.get(name) ?? [];
      const notRun = files.filter((file) => !runInCi.has(file));
      if (files.length === 0 || notRun.length > 0) missing.push(`${name} (${notRun.join(", ") || "no proof file"})`);
    }
  }
  return missing;
}

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const docPath = path.join(repoRoot, "docs", "privacy-spec.md");
assert.ok(fs.existsSync(docPath), `missing ${docPath}; run pnpm docs:privacy first`);
const committedDoc = fs.readFileSync(docPath, "utf8");

check(
  "learning_fact_state_schema_and_status_drop_read_are_documented",
  false,
  () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-privacy-state-"));
    fs.chmodSync(root, 0o700);
    let buffer: LocalEventBuffer | null = null;
    try {
      buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), { delivery: { enabled: false } });
      const row = committedDoc.split("\n").find((line) =>
        line.startsWith("| `learning_fact_table_state` |"));
      assert.ok(row, "privacy spec has no learning_fact_table_state row");
      const columns = (buffer.database.pragma("table_info(learning_fact_table_state)") as
        Array<{ name: string }>).map((column) => column.name);
      for (const column of columns) {
        assert.ok(row.includes(`\`${column}\``), `privacy spec omits ${column}`);
      }
      assert.match(row, /status refresh[^|]*`runtime_fact_drops`/);
      const now = Date.now();
      buffer.database.exec(`create table if not exists runtime_fact_drops (
        reason text primary key, dropped_count integer not null, last_dropped_at text not null)`);
      buffer.database.prepare(`insert into runtime_fact_drops
        (reason, dropped_count, last_dropped_at) values ('capacity_exceeded', 1, ?)`
      ).run(new Date(now).toISOString());
      const window = buffer.learningFacts.statusWithWindow(new Date(now + 86_400_000).toISOString(), 7)
        .analysisWindow;
      assert.equal(window.effectiveStartInclusive, new Date(now + 1).toISOString(),
        "status refresh must read the local capacity-drop history");
    } finally {
      buffer?.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  },
);

check(
  "render_is_byte_deterministic_across_runs",
  false,
  () =>
    assert.equal(
      renderPrivacySpec(collectPrivacySpecModel()),
      renderPrivacySpec(collectPrivacySpecModel()),
      "two renders from freshly collected models must be byte-identical",
    ),
);

check(
  "committed_doc_equals_regeneration_from_source_lists",
  false,
  () => {
    const verdict = docIsStale(renderPrivacySpec(collectPrivacySpecModel()), committedDoc);
    assert.equal(verdict.stale, false, `stale: ${verdict.firstDiffLine ?? "?"}`);
  },
);

check(
  "adversarial_hand_edited_doc_detected_as_stale",
  true,
  () => {
    const tampered = `${committedDoc}35 | \`keystroke_dwell_times\` |\n`;
    const verdict = docIsStale(renderPrivacySpec(collectPrivacySpecModel()), tampered);
    assert.equal(verdict.stale, true);
    assert.ok(verdict.firstDiffLine?.includes("line "), verdict.firstDiffLine);
  },
);

check(
  "adversarial_source_list_change_flows_into_render",
  true,
  () => {
    const drifted = renderPrivacySpec(
      mutatedModel((model) => {
        model.suppressedBeforeLocalWrite = [...model.suppressedBeforeLocalWrite, "keystroke_dwell_times"];
      }),
    );
    assert.notEqual(drifted, committedDoc);
    assert.ok(drifted.includes("`keystroke_dwell_times`"));
    const neverCollectedDrift = renderPrivacySpec(
      mutatedModel((model) => {
        model.neverCollectibleAnyMode = [...model.neverCollectibleAnyMode, "keystroke_dwell_times"];
      }),
    );
    assert.notEqual(neverCollectedDrift, drifted);
  },
);

check(
  "adversarial_phantom_proof_check_name_rejected",
  true,
  () => {
    const forgedChecks: string[] = [
      ...PROOF_CHECKS.suppression.checks,
      "this_check_never_existed_anywhere",
    ];
    const original = PROOF_CHECKS.suppression.checks;
    PROOF_CHECKS.suppression.checks = forgedChecks;
    try {
      assert.throws(() => renderPrivacySpec(collectPrivacySpecModel()), /this_check_never_existed_anywhere/);
    } finally {
      PROOF_CHECKS.suppression.checks = original;
    }
  },
);

check(
  "every_registered_proof_check_exists_in_proof_sources",
  false,
  () => {
    const found = verifyProofChecks();
    for (const ref of Object.values(PROOF_CHECKS)) {
      for (const name of ref.checks) {
        assert.ok(found.has(name), `${name} not found in any -proof.ts`);
        assert.ok(
          found.get(name)!.every((file) => file.endsWith("-proof.ts")),
          JSON.stringify(found.get(name)),
        );
      }
    }
  },
);

check(
  "every_cited_proof_check_runs_in_ci",
  false,
  () => {
    const missing = citedChecksNotRunInCi(verifyProofChecks(), proofCiCoverage(readCoverageInput(repoRoot)));
    assert.deepEqual(missing, [], `cited check(s) whose proof ${WORKFLOW_DIRECTORY} does not run: ${missing.join("; ")}`);
  },
);

check(
  "adversarial_cited_proof_disabled_in_ci_rejected",
  true,
  () => {
    // For every cited check, keep its CI lines but make the steps that run
    // its proof unreachable with a false-only matrix (the PR #397 review's
    // attack); the guard must name that check.
    const found = verifyProofChecks();
    const input = readCoverageInput(repoRoot);
    const coverage = proofCiCoverage(input);
    for (const [name, files] of found) {
      const steps = coverage.units.filter((unit) => files.includes(unit.unit)).flatMap((unit) => unit.covered);
      assert.ok(steps.length > 0, `${name}: no CI step runs its proof`);
      const missing = citedChecksNotRunInCi(found, proofCiCoverage(disableSteps(input, steps)));
      assert.ok(missing.some((entry) => entry.startsWith(`${name} (`)), `${name} not rejected: ${JSON.stringify(missing)}`);
    }
  },
);

check(
  "adversarial_cited_proof_script_or_ci_line_tampered_rejected",
  true,
  () => {
    // The second PR #397 review kept this guard green with a "temporarily
    // disabled" prefix on the cited proof's package script, and with a
    // `${{ vars.PROOF_ARGS }}` suffix on its CI line. Both must name the check.
    const found = verifyProofChecks();
    const input = readCoverageInput(repoRoot);
    const coverage = proofCiCoverage(input);
    for (const [name, files] of found) {
      const runs = coverage.units.filter((unit) => files.includes(unit.unit)).flatMap((unit) => unit.covered);
      const scripts = [...new Set(runs.flatMap((run) => run.via.slice(0, 1)))];
      const attacks = [appendToCiLines(input, runs, " ${{ vars.PROOF_ARGS }}"), ...(scripts.length > 0 ? [disableScripts(input, scripts)] : [])];
      for (const attacked of attacks) {
        const missing = citedChecksNotRunInCi(found, proofCiCoverage(attacked));
        assert.ok(missing.some((entry) => entry.startsWith(`${name} (`)), `${name} not rejected: ${JSON.stringify(missing)}`);
      }
    }
  },
);

check(
  "never_collectible_derivation_excludes_admissible_categories",
  false,
  () => {
    const model = collectPrivacySpecModel();
    const admissible = new Set(model.admissibleEvidenceCategories);
    for (const name of model.neverCollectibleAnyMode) {
      assert.ok(!admissible.has(name), `${name} must not be admissible`);
      assert.ok(model.suppressedBeforeLocalWrite.includes(name), `${name} must be forbidden`);
    }
    for (const category of ["clipboard", "screenshot", "browser_history", "stdin", "stdout", "stderr"]) {
      assert.ok(model.neverCollectibleAnyMode.includes(category), `${category} must be never-collectible`);
    }
    for (const admitted of admissible) {
      assert.ok(!model.neverCollectibleAnyMode.includes(admitted));
    }
    const suppressedNormalized = new Set(
      model.suppressedBeforeLocalWrite.map((name) => name.replace(/[^a-zA-Z0-9]/g, "").toLowerCase()),
    );
    const admittedForbidden = [...admissible].filter((category) =>
      suppressedNormalized.has(category.replace(/[^a-zA-Z0-9]/g, "").toLowerCase()),
    ).length;
    assert.equal(
      model.neverCollectibleAnyMode.length + admittedForbidden,
      model.suppressedBeforeLocalWrite.length,
      "never-collectible plus forbidden-but-admissible must partition the forbidden list",
    );
  },
);

check(
  "hashed_bucket_covers_protected_list_with_valid_example_hash",
  false,
  () => {
    const model = collectPrivacySpecModel();
    assert.ok(model.hashedKeys.length > 0);
    assert.match(model.hashedExample.output, /^sha256:[0-9a-f]{16}$/);
    const rendered = renderPrivacySpec(model);
    for (const key of model.hashedKeys) {
      assert.ok(rendered.includes(`\`${key}\``), `missing hashed key ${key}`);
    }
  },
);

check(
  "plain_bucket_lists_full_event_shape_from_schema_introspection",
  false,
  () => {
    const model = collectPrivacySpecModel();
    const rendered = renderPrivacySpec(model);
    for (const field of ["id", "sessionId", "actorId", "eventType", "observedAt", "inputTokens", "costUsd", "metadata"]) {
      assert.ok(model.eventFields.some((note) => note.name === field), `missing event field ${field}`);
    }
    assert.ok(rendered.includes("length ≥ 1, ≤ 500"));
    const eventsField = model.batchEnvelopeFields.find((field) => field.name === "events");
    assert.deepEqual(eventsField?.arrayBounds, { min: 1, max: 500 });
  },
);

console.log(`privacy spec proof: ${checks.length}/${checks.length} checks passed`);
for (const entry of checks) {
  console.log(`  ✓${entry.adversarial ? " [adversarial]" : ""} ${entry.name}`);
}
