/** Independent oracle for the binding metadata key set. */
import assert from "node:assert/strict";
import { dispatchBindingMetadata, dispatchBindingSchema } from
  "../packages/collector-cli/src/capture-root-inventory";

const binding = dispatchBindingSchema.parse({
  sessionId: "22222222-2222-4222-8222-222222222222",
  workItemId: "beads:eco-6hoxj.165.97", projectKey: `sha256:${"a".repeat(64)}`,
  companyRef: "company-1", attemptId: "11111111-1111-4111-8111-111111111111",
  parentAttemptId: "parent-1", acceptedOutcomeId: "outcome-1",
  validFrom: "2026-09-28T00:00:00.000Z", validUntil: null,
  evidenceRef: "dispatch:synthetic-review", role: "reviewer",
  workClass: "implementation", complexityBand: "medium",
  techniqueId: "tech-1", techniqueVersion: "1", assignmentId: "assign-1",
  arm: "control", launchedBy: "fleet-delegate",
});
const metadata = dispatchBindingMetadata(binding);
const expected = ["workItemId", "dispatchProjectKey", "workEvidenceRef", "attemptId",
  "parentAttemptId", "companyRef", "acceptedOutcomeId", "role", "workClass",
  "complexityBand", "techniqueId", "techniqueVersion", "assignmentId", "arm", "launchedBy"];
assert.deepEqual(Object.keys(metadata).sort(), expected.sort());
assert.equal(metadata.workItemId, binding.workItemId);
assert.equal(metadata.dispatchProjectKey, binding.projectKey);
assert.equal(metadata.workEvidenceRef, binding.evidenceRef);
assert.equal(metadata.attemptId, binding.attemptId);
console.log(JSON.stringify({ expectedKeyCount: expected.length, actualKeyCount: Object.keys(metadata).length,
  forbiddenPromptTitlePathBranch: true }));
