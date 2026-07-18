#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

import {
  digest,
  loadTamperContract,
  tamperContractPath,
  type TamperMutation,
} from "./system-e2e/contract";
import { verifySystemE2EReceipt } from "./system-e2e/verifier";

const repoRoot = path.resolve(import.meta.dirname, "..");

function record(value: unknown, label: string): Record<string, unknown> {
  assert.ok(value && typeof value === "object" && !Array.isArray(value), `${label} must be an object`);
  return value as Record<string, unknown>;
}

function pointerParts(pointer: string) {
  return pointer.slice(1).split("/").map((part) => part.replace(/~1/g, "/").replace(/~0/g, "~"));
}

function mutate(root: unknown, mutation: TamperMutation) {
  const parts = pointerParts(mutation.pointer);
  assert.ok(parts.length > 0, `empty mutation pointer ${mutation.pointer}`);
  let parent: unknown = root;
  for (const part of parts.slice(0, -1)) {
    assert.ok(parent && typeof parent === "object", `mutation parent is missing at ${mutation.pointer}`);
    parent = Array.isArray(parent)
      ? parent[Number(part)]
      : (parent as Record<string, unknown>)[part];
  }
  assert.ok(parent && typeof parent === "object", `mutation target parent is missing at ${mutation.pointer}`);
  const key = parts.at(-1)!;
  if (mutation.remove) {
    if (Array.isArray(parent)) parent.splice(Number(key), 1);
    else delete (parent as Record<string, unknown>)[key];
  } else if (Array.isArray(parent)) {
    parent[Number(key)] = structuredClone(mutation.replacement);
  } else {
    (parent as Record<string, unknown>)[key] = structuredClone(mutation.replacement);
  }
}

function redigestStage(stageValue: unknown, keys: readonly string[]) {
  const stage = record(stageValue, "tampered stage");
  stage.digest = digest(Object.fromEntries(keys.map((key) => [key, stage[key]])));
}

/** Re-sign every content hash so semantic-negative cases cannot pass by relying on a stale outer digest. */
function resign(receiptValue: unknown) {
  const receipt = record(receiptValue, "tampered receipt");
  const flow = record(receipt.flow, "tampered flow");
  const shared = record(flow.sharedFlow, "tampered shared flow");
  const lineage = record(shared.lineage, "tampered lineage");
  redigestStage(lineage.capture, ["rows"]);
  redigestStage(lineage.delivery, ["offlineRequestEventIds", "acceptedEventIds", "poisonEventIds", "reconnectRequestEventIds", "acknowledgedReceipts", "deadReceipts"]);
  redigestStage(lineage.allocation, ["receipts", "pullRows", "coverage", "capturedPrimaryTokens", "allocatedPrimaryTokens", "unallocatedPrimaryTokens"]);
  redigestStage(lineage.outcome, ["facts", "coverage", "requiredChecks", "reworkWindowDays", "derived"]);
  redigestStage(lineage.learningFacts, ["episodeBindings", "attemptEventBindings", "attempts", "exposures"]);
  redigestStage(lineage.evidence, ["manifest", "sourceFingerprint", "packetFingerprint", "claimClass", "causalClaim", "prescriptiveClaim", "skillPublicationAuthorized", "skillInstallationAuthorized", "analysisWorkUnits", "unchangedAnalysisWorkUnits"]);
  shared.outputDigest = digest(lineage);

  const phases = flow.phaseChain as Array<Record<string, unknown>>;
  for (const phase of phases) {
    phase.artifactDigest = digest(phase.artifact);
    phase.semanticDigest = digest({
      name: phase.name,
      status: phase.status,
      expectedFlowFingerprint: phase.expectedFlowFingerprint,
      sourceHeadCommit: phase.sourceHeadCommit,
      testedTreeCommit: phase.testedTreeCommit,
      artifactDigest: phase.artifactDigest,
      artifact: phase.artifact,
    });
    phase.outputDigest = digest(Object.fromEntries(
      ["schema", "name", "status", "expectedFlowFingerprint", "sourceHeadCommit", "testedTreeCommit", "artifact", "artifactDigest", "semanticDigest"]
        .map((key) => [key, phase[key]]),
    ));
  }
  flow.phaseChainDigest = digest(phases);
  const deterministicMaterial = Object.fromEntries(
    Object.entries(receipt).filter(([key]) => !["measurements", "volatileFieldsExcludedFromDeterministicDigest", "deterministicDigest"].includes(key)),
  );
  receipt.deterministicDigest = digest(deterministicMaterial);
}

const receiptIndex = process.argv.indexOf("--receipt");
const expectedCommitIndex = process.argv.indexOf("--expected-source-commit");
const localHead = spawnSync("/usr/bin/git", ["rev-parse", "HEAD"], {
  cwd: repoRoot,
  encoding: "utf8",
});
assert.equal(localHead.status, 0, "could not derive local expected source commit");
const expectedSourceHeadCommit = expectedCommitIndex >= 0
  ? process.argv[expectedCommitIndex + 1]
  : localHead.stdout.trim();
assert.ok(expectedSourceHeadCommit, "--expected-source-commit requires a full SHA");
const receiptPath = path.resolve(
  receiptIndex >= 0 && process.argv[receiptIndex + 1]
    ? process.argv[receiptIndex + 1]!
    : path.join(repoRoot, "evidence", "system-e2e-proof.json"),
);
assert.ok(fs.existsSync(receiptPath), `system E2E receipt is missing: ${receiptPath}`);
const baseline = JSON.parse(fs.readFileSync(receiptPath, "utf8")) as unknown;
const baselineVerification = verifySystemE2EReceipt(baseline, repoRoot, expectedSourceHeadCommit);
const contract = loadTamperContract(tamperContractPath(repoRoot));
const results = contract.cases.map((tamperCase) => {
  const candidate = structuredClone(baseline);
  for (const mutation of tamperCase.mutations) mutate(candidate, mutation);
  if (tamperCase.resign) resign(candidate);
  let failure = "";
  try {
    verifySystemE2EReceipt(candidate, repoRoot, expectedSourceHeadCommit);
  } catch (error) {
    failure = error instanceof Error ? error.message : String(error);
  }
  assert.ok(failure, `tamper case ${tamperCase.id} unexpectedly verified`);
  assert.match(failure, new RegExp(tamperCase.expectedError, "i"), `tamper case ${tamperCase.id} failed at the wrong gate: ${failure}`);
  return { id: tamperCase.id, rejected: true };
});

console.log(JSON.stringify({
  schema: contract.schema,
  baselineDigest: baselineVerification.deterministicDigest,
  tamperCases: results.length,
  rejected: results.length,
  results,
}, null, 2));
