import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  appendProjectIntentReceipt, projectIntentReceiptSchema, selectedProjectIntent,
} from "../../packages/shared/src/project-intent";

const fixture = JSON.parse(readFileSync(new URL("../../packages/shared/fixtures/project-intent-v1.json", import.meta.url), "utf8"));
const base = projectIntentReceiptSchema.parse(fixture.baseReceipt);
const receipt = (name: string) => projectIntentReceiptSchema.parse({
  ...fixture.baseReceipt, ...fixture.cases.find((item: { name: string }) => item.name === name).patch,
});

test("v1 identity derivation domains and evidence digests match every golden receipt", () => {
  const digest = (kind: string, parts: unknown[]) =>
    `sha256:${createHash("sha256").update(`plimsoll:project-intent:v1:${kind}\0${JSON.stringify(parts)}`, "utf8").digest("hex")}`;
  const vectors = fixture.derivationVectors as Record<string, { kind: string; normalizedParts: unknown[]; expected: string }>;
  assert.deepEqual(Object.keys(vectors), ["sourceRootKey", "accountKey", "nativeSessionKey", "sessionEpochKey", "workItemKey", "evidenceRef"]);
  for (const [field, vector] of Object.entries(vectors)) {
    assert.equal(digest(vector.kind, vector.normalizedParts), vector.expected, field);
    assert.equal(fixture.baseReceipt[field], vector.expected, field);
    assert.notEqual(digest("different-domain", vector.normalizedParts), vector.expected, field);
  }
  const evidenceFields = ["receiptId", "installId", "source", "sourceRootKey", "accountKey", "sessionId",
    "nativeSessionKey", "sessionEpochKey", "rootAttemptId", "attemptId", "parentAttemptId", "workItemKey",
    "projectKey", "projectRegistryRevision", "observedRepoKey", "effectiveFrom", "effectiveUntil",
    "basis", "adapterId", "adapterVersion"] as const;
  const rows = [base, ...fixture.cases.map((item: { name: string }) => receipt(item.name))];
  for (const row of rows)
    assert.equal(row.evidenceRef, digest("evidence", evidenceFields.map(field => row[field])), row.receiptId);
});

test("v1 golden receipts preserve resume and explicit attempt lineage across account rotation", () => {
  for (const name of ["resume", "account_rotation"]) {
    const next = receipt(name);
    const result = appendProjectIntentReceipt([base], next);
    assert.equal(result.replayed, false);
    assert.equal(result.receipts.length, 2);
    assert.equal(next.sessionEpochKey, base.sessionEpochKey);
    assert.equal(next.projectKey, base.projectKey);
  }
  assert.throws(() => appendProjectIntentReceipt([base], {
    ...receipt("account_rotation"), parentAttemptId: null,
  }), /attempt_lineage_invalid/);
});

test("v1 reused native IDs cannot reuse a ledger session incarnation", () => {
  assert.throws(() => appendProjectIntentReceipt([base], receipt("reused_native_id")), /session_identity_conflict/);
  const newSession = { ...receipt("reused_native_id"), sessionId: "24000000-0000-4000-8000-000000000099" };
  assert.equal(appendProjectIntentReceipt([], newSession).receipts.length, 1);
});

test("v1 missing key is explicit Unknown and observed evidence never replaces declared intent", () => {
  assert.equal(receipt("missing_key").projectKey, null);
  const observations = appendProjectIntentReceipt([base], receipt("conflicting_evidence")).receipts;
  assert.equal(selectedProjectIntent(observations, "2026-10-06T20:00:00.000Z")?.projectKey, base.projectKey);
  assert.equal(selectedProjectIntent([receipt("conflicting_evidence")], "2026-10-06T20:00:00.000Z"), null);
});

test("v1 refuses unsupported adapters and client approval claims", () => {
  assert.throws(() => appendProjectIntentReceipt([], receipt("unsupported_adapter")), /unsupported_adapter/);
  assert.equal(projectIntentReceiptSchema.safeParse({ ...base, basis: "owner" }).success, false);
  assert.equal(projectIntentReceiptSchema.safeParse({ ...base, approved: true }).success, false);
});

test("v1 outbound allowlist rejects content, paths, raw identities, slugs and incomplete keys", () => {
  const planted = ["prompt", "reply", "code", "command", "fileName", "cwd", "nativeSessionId", "accountEmail", "apiKey"];
  for (const field of planted) assert.equal(projectIntentReceiptSchema.safeParse({ ...base, [field]: "private planted sentinel" }).success, false, field);
  for (const field of ["sourceRootKey", "accountKey", "nativeSessionKey", "sessionEpochKey", "workItemKey", "evidenceRef", "observedRepoKey", "projectKey"]) {
    assert.equal(projectIntentReceiptSchema.safeParse({ ...base, [field]: "/private/secret-file.ts" }).success, false, field);
  }
  for (const projectKey of ["project:client", "company:new-reward", "sha256:abc"])
    assert.equal(projectIntentReceiptSchema.safeParse({ ...base, projectKey }).success, false);
  assert.equal(projectIntentReceiptSchema.safeParse({ ...base, effectiveUntil: base.effectiveFrom }).success, false);
  assert.equal(projectIntentReceiptSchema.safeParse({ ...base, schema: "plimsoll-project-intent/v2" }).success, false);
});

test("v1 receipt replay is idempotent, immutable, bounded and rejects conflicting windows", () => {
  const replay = appendProjectIntentReceipt([base], base);
  assert.equal(replay.replayed, true);
  assert.equal(replay.receipts.length, 1);
  assert.throws(() => appendProjectIntentReceipt([base], { ...base, projectKey: `sha256:${"c".repeat(64)}` }), /receipt_replay_conflict/);
  assert.throws(() => appendProjectIntentReceipt([base], {
    ...receipt("resume"), effectiveFrom: "2026-10-06T17:00:00.000Z",
  }), /intent_time_regression/);
  assert.throws(() => appendProjectIntentReceipt([base], {
    ...receipt("resume"), effectiveFrom: base.effectiveFrom, projectKey: `sha256:${"c".repeat(64)}`,
  }), /intent_conflict/);
  assert.throws(() => appendProjectIntentReceipt([{ ...base, effectiveUntil: "2026-10-06T21:00:00.000Z" }], receipt("resume")), /intent_interval_overlap/);
});

test("v1 project changes cut over open intervals without rewriting prior receipts or reviving old choices", () => {
  const switched = { ...receipt("resume"), projectKey: `sha256:${"c".repeat(64)}` };
  const rows = appendProjectIntentReceipt([base], switched).receipts;
  assert.deepEqual(rows[0], base);
  assert.equal(selectedProjectIntent(rows, "2026-10-06T18:30:00.000Z")?.projectKey, base.projectKey);
  assert.equal(selectedProjectIntent(rows, switched.effectiveFrom)?.projectKey, switched.projectKey);
  const cleared = { ...switched, receiptId: "24000000-0000-4000-8000-000000000098",
    effectiveFrom: "2026-10-06T20:00:00.000Z", projectKey: null, projectRegistryRevision: null };
  assert.equal(selectedProjectIntent(appendProjectIntentReceipt(rows, cleared).receipts, cleared.effectiveFrom)?.projectKey, null);
  assert.equal(selectedProjectIntent([{ ...base, effectiveUntil: "2026-10-06T19:00:00.000Z" }], "2026-10-06T19:00:00.000Z"), null);
});
