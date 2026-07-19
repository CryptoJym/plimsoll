#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  createResourceSandbox,
  loadUnwiredIntegrationScenarios,
  removeResourceSandbox,
  runArchitectureContract,
  runBoundedCodexReconciliationContract,
  runChildEnvironmentContract,
  runEmptyLedgerContract,
  runExistingSignalFidelityProof,
  runIsolationContract,
  runNoChangeConstantWorkContract,
  runPortReservationContract,
  runDuplicateStartSingleOwnerContract,
  runPoisonContinuationContract,
  runDashboardProjectionBudgetContract,
  runIntegratedCaptureProjectionOutboxContract,
  runMetadataPrivacySentinelsContract,
  runLearningFactPrivacyAndResourceContract,
  resourceReceiptPrivacyLeakCount,
} from "./scenarios";
import { runBoundedCaptureContract } from "./bounded-capture";
import {
  RESOURCE_PROOF_SCHEMA,
  type ResourceProofReceipt,
  type ScenarioReceipt,
} from "./types";

function within(parent: string, candidate: string) {
  const relative = path.relative(path.resolve(parent), path.resolve(candidate));
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function optionValue(name: string) {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

function printHelp() {
  console.log(`Plimsoll resource-proof scaffold

Usage:
  pnpm exec tsx scripts/resource-proof/index.ts [options]

Options:
  --receipt <path>       Write the JSON receipt to a caller-selected path.
  --run-existing-proof   Run scripts/signal-fidelity-proof.ts in the isolated environment.
  --require-integrated   Exit non-zero while any required scenario is fail/not_wired/skipped.
  --help                 Show this help.

The default harness runs every required architecture, isolation, ownership,
maintenance, reconciliation, projection, delivery, integrated capture, and
metadata-privacy scenario. Pass --require-integrated for the release gate.`);
}

function summarize(scenarios: ScenarioReceipt[]) {
  const count = (status: ScenarioReceipt["status"]) =>
    scenarios.filter((scenario) => scenario.status === status).length;
  return {
    passed: count("pass"),
    failed: count("fail"),
    notWired: count("not_wired"),
    skipped: count("skipped"),
    requiredIncomplete: scenarios.filter(
      (scenario) => scenario.required && scenario.status !== "pass",
    ).length,
  };
}

async function main() {
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    printHelp();
    return;
  }
  const receiptPath = optionValue("--receipt");
  if (process.argv.includes("--receipt") && !receiptPath) {
    throw new Error("--receipt requires a path");
  }
  const requireIntegrated = process.argv.includes("--require-integrated");
  const runExistingProof = process.argv.includes("--run-existing-proof");
  const operatorHome = os.homedir();
  if (receiptPath) {
    const liveCollectorHome = path.join(
      operatorHome,
      "Library",
      "Application Support",
      "Plimsoll",
    );
    if (within(liveCollectorHome, receiptPath)) {
      throw new Error("--receipt must not point inside the operator's live Plimsoll directory");
    }
  }
  const sandbox = await createResourceSandbox();
  const scenarios: ScenarioReceipt[] = [];

  try {
    scenarios.push(runIsolationContract(sandbox, operatorHome));
    const portScenario = await runPortReservationContract(sandbox);
    scenarios.push(portScenario);
    const environmentScenario = runChildEnvironmentContract(sandbox);
    scenarios.push(environmentScenario);
    scenarios.push(runArchitectureContract());
    scenarios.push(runEmptyLedgerContract(sandbox));
    scenarios.push(runExistingSignalFidelityProof(sandbox, runExistingProof));
    scenarios.push(await runNoChangeConstantWorkContract(sandbox));
    scenarios.push(await runBoundedCaptureContract(sandbox));
    scenarios.push(runBoundedCodexReconciliationContract(sandbox));
    scenarios.push(await runDuplicateStartSingleOwnerContract(sandbox));
    scenarios.push(await runPoisonContinuationContract(sandbox));
    scenarios.push(await runDashboardProjectionBudgetContract(sandbox));
    scenarios.push(runIntegratedCaptureProjectionOutboxContract(sandbox, operatorHome));
    scenarios.push(runMetadataPrivacySentinelsContract(sandbox, operatorHome));
    scenarios.push(runLearningFactPrivacyAndResourceContract(sandbox, operatorHome));
    scenarios.push(
      ...loadUnwiredIntegrationScenarios(
        new Set([
          "no_change_constant_work",
          "bounded_generation_capture",
          "duplicate_start_single_owner",
          "poison_continuation",
          "bounded_codex_reconciliation",
          "dashboard_projection_budget",
          "integrated_capture_projection_outbox",
          "metadata_privacy_sentinels",
          "learning_fact_privacy_and_resource_bounds",
        ]),
      ),
    );

    const summary = summarize(scenarios);
    const gateReady = summary.failed === 0 && summary.requiredIncomplete === 0;
    const receipt: ResourceProofReceipt = {
      schema: RESOURCE_PROOF_SCHEMA,
      generatedAt: new Date().toISOString(),
      overall: summary.failed > 0 ? "fail" : gateReady ? "pass" : "scaffold_ready",
      gateReady,
      requireIntegrated,
      environment: {
        isolation: "temporary-home-db-and-session-roots",
        loopbackPort:
          portScenario.status === "pass" ? "held-and-challenged" : "unverified",
        providerNetwork: "not-configured",
        credentials:
          environmentScenario.status === "pass" ? "scrubbed-by-allowlist" : "unverified",
        liveStateTouched: false,
        node: process.version,
        platform: process.platform,
      },
      summary,
      scenarios,
    };
    let serialized = `${JSON.stringify(receipt, null, 2)}\n`;
    // Test-only seam: append one already-classified private term to the local
    // scan input, never to the receipt, so finalized exit state can be proved
    // without emitting the term.
    const privacyScanInput =
      process.env.PLIMSOLL_RESOURCE_PROOF_TEST_FINAL_PRIVACY_FAILURE === "1"
        ? `${serialized}${operatorHome}`
        : serialized;
    const receiptPrivacyLeaks = resourceReceiptPrivacyLeakCount(privacyScanInput, operatorHome);
    const privacyScenario = scenarios.find(
      (scenario) => scenario.id === "metadata_privacy_sentinels",
    );
    if (!privacyScenario) throw new Error("MetadataPrivacyScenarioMissing");
    privacyScenario.measurements = {
      ...privacyScenario.measurements,
      finalReceiptPrivacyLeaks: receiptPrivacyLeaks,
      finalReceiptPrivacyScanPassed: receiptPrivacyLeaks === 0,
    };
    if (receiptPrivacyLeaks > 0) {
      privacyScenario.status = "fail";
      privacyScenario.detail =
        "The final resource receipt contained a private-term leak; receipt content is omitted.";
    }
    receipt.summary = summarize(scenarios);
    receipt.gateReady = receipt.summary.failed === 0 && receipt.summary.requiredIncomplete === 0;
    receipt.overall = receipt.summary.failed > 0 ? "fail" : receipt.gateReady ? "pass" : "scaffold_ready";
    serialized = `${JSON.stringify(receipt, null, 2)}\n`;
    if (resourceReceiptPrivacyLeakCount(serialized, operatorHome) > 0) {
      throw new Error("ResourceReceiptPrivacyViolation");
    }
    if (receiptPath) {
      const resolved = path.resolve(receiptPath);
      fs.mkdirSync(path.dirname(resolved), { recursive: true });
      fs.writeFileSync(resolved, serialized, { mode: 0o600 });
      fs.chmodSync(resolved, 0o600);
    }
    process.stdout.write(serialized);

    if (receipt.summary.failed > 0 || (requireIntegrated && !receipt.gateReady)) {
      process.exitCode = 1;
    }
  } finally {
    await removeResourceSandbox(sandbox);
  }
}

main().catch((error) => {
  const errorClass = error instanceof Error ? error.name : "UnknownError";
  process.stderr.write(
    `${JSON.stringify({ schema: RESOURCE_PROOF_SCHEMA, overall: "fail", error: errorClass })}\n`,
  );
  process.exitCode = 1;
});
