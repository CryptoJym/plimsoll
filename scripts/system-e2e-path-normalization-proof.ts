#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import {
  canonicalBytes,
  digest,
  exactKeys,
  normalizeSupportingArtifact,
  type SupportingNormalizationContext,
} from "./system-e2e/contract";

const repoRoot = path.resolve(import.meta.dirname, "..");
const fixturePath = path.join(repoRoot, "scripts", "system-e2e", "fixtures", "path-normalization.json");
const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8")) as unknown;
exactKeys(fixture, ["schema", "variants", "outsideRootTamper"], "path normalization fixture");
assert.equal(fixture.schema, "plimsoll.system-e2e-path-normalization.v1");
assert.ok(Array.isArray(fixture.variants) && fixture.variants.length === 2);
const variants = fixture.variants as unknown[];

const normalized = variants.map((variant, index) => {
  exactKeys(variant, ["baseDirectory", "roots", "artifact"], `path normalization variant ${index}`);
  assert.ok(Array.isArray(variant.roots) && variant.roots.length > 0, `path normalization variant ${index} roots missing`);
  const roots = variant.roots.map((root, rootIndex) => {
    exactKeys(root, ["label", "absolutePath"], `path normalization variant ${index} root ${rootIndex}`);
    return { label: String(root.label), absolutePath: String(root.absolutePath) };
  });
  const context = {
    baseDirectory: String(variant.baseDirectory),
    roots,
  } satisfies SupportingNormalizationContext;
  return normalizeSupportingArtifact(variant.artifact, context);
});
assert.deepEqual(normalized[0], normalized[1], "two temp roots changed child semantic artifact");
assert.equal(digest(normalized[0]), digest(normalized[1]), "two temp roots changed child semantic digest");
const normalizedBytes = canonicalBytes(normalized[0]);
assert.ok(!/run-a|run-b|proof-root-a|different-proof-root/.test(normalizedBytes), "temp-root basename survived normalization");
assert.ok(normalizedBytes.includes("<path-root:machine-home>"), "declared HOME path was not role-bound");
assert.ok(normalizedBytes.includes("<path-root:proof>"), "relative receipt path was not proof-root-bound");
assert.ok(normalizedBytes.includes("<path-root:node-runtime>"), "declared path-list entry was not node-root-bound");
assert.ok(normalizedBytes.includes("<required-system-path:/usr/bin>"), "stable required system path was not explicit");

exactKeys(fixture.outsideRootTamper, ["configPath", "expectedError"], "outside-root tamper fixture");
const firstVariant = variants[0];
exactKeys(firstVariant, ["baseDirectory", "roots", "artifact"], "first path normalization variant");
assert.ok(Array.isArray(firstVariant.roots), "first path normalization variant roots missing");
const tampered = structuredClone(firstVariant.artifact) as Record<string, unknown>;
const checks = tampered.checks as Array<Record<string, unknown>>;
const detail = checks[0]!.detail as Record<string, unknown>;
detail.configPath = fixture.outsideRootTamper.configPath;
assert.throws(
  () => normalizeSupportingArtifact(tampered, {
    baseDirectory: String(firstVariant.baseDirectory),
    roots: firstVariant.roots as SupportingNormalizationContext["roots"],
  }),
  new RegExp(String(fixture.outsideRootTamper.expectedError)),
  "outside-root declared path tamper did not fail",
);

console.log(JSON.stringify({
  schema: fixture.schema,
  status: "pass",
  variants: normalized.length,
  semanticDigest: digest(normalized[0]),
  outsideRootTamperRejected: true,
}, null, 2));
