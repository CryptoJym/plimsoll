#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  createResourceSandbox,
  loadUnwiredIntegrationScenarios,
  removeResourceSandbox,
  runArchitectureContract,
  runEmptyLedgerContract,
  runExistingSignalFidelityProof,
  runIsolationContract,
} from "./scenarios";
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

The default scaffold is truthful but not a release pass: required integration
scenarios remain not_wired until issues #76-#80 expose their test seams.`);
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
    scenarios.push(runArchitectureContract());
    scenarios.push(runEmptyLedgerContract(sandbox));
    scenarios.push(runExistingSignalFidelityProof(sandbox, runExistingProof));
    scenarios.push(...loadUnwiredIntegrationScenarios());

    const summary = summarize(scenarios);
    const anyFailure = summary.failed > 0;
    const gateReady = !anyFailure && summary.requiredIncomplete === 0;
    const receipt: ResourceProofReceipt = {
      schema: RESOURCE_PROOF_SCHEMA,
      generatedAt: new Date().toISOString(),
      overall: anyFailure ? "fail" : gateReady ? "pass" : "scaffold_ready",
      gateReady,
      requireIntegrated,
      environment: {
        isolation: "temporary-home-db-session-roots-and-loopback-port",
        providerNetwork: "not-configured",
        credentials: "scrubbed",
        liveStateTouched: false,
        node: process.version,
        platform: process.platform,
      },
      summary,
      scenarios,
    };
    const serialized = `${JSON.stringify(receipt, null, 2)}\n`;
    if (receiptPath) {
      const resolved = path.resolve(receiptPath);
      fs.mkdirSync(path.dirname(resolved), { recursive: true });
      fs.writeFileSync(resolved, serialized, { mode: 0o600 });
    }
    process.stdout.write(serialized);

    if (anyFailure || (requireIntegrated && !gateReady)) process.exitCode = 1;
  } finally {
    removeResourceSandbox(sandbox);
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${JSON.stringify({ schema: RESOURCE_PROOF_SCHEMA, overall: "fail", error: message })}\n`);
  process.exitCode = 1;
});
