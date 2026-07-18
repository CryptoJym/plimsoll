#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  canonicalBytes,
  digest,
  exactKeys,
  loadSupportContract,
  normalizeSupportingArtifact,
  parseSupportingArtifact,
  supportContractPath,
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

type ActualChildVariant = {
  artifact: unknown;
  context: SupportingNormalizationContext;
  stdout: string;
};

const systemPathEntries = [
  "/neutral/bin",
  "/opt/homebrew/bin",
  "/usr/local/bin",
  "/usr/bin",
  "/bin",
  "/usr/sbin",
  "/sbin",
];

function runActualInstallDoctorVariant(
  portabilityRoot: string,
  label: string,
  relativeRoot: string,
): ActualChildVariant {
  const variantRoot = path.join(portabilityRoot, relativeRoot);
  const home = path.join(variantRoot, "home");
  const temp = path.join(variantRoot, "tmp");
  const requestedNodeRoot = path.join(variantRoot, "runtime", "bin");
  fs.mkdirSync(home, { recursive: true, mode: 0o700 });
  fs.mkdirSync(temp, { recursive: true, mode: 0o700 });
  fs.mkdirSync(requestedNodeRoot, { recursive: true, mode: 0o700 });
  const requestedNode = path.join(requestedNodeRoot, "node");
  fs.copyFileSync(process.execPath, requestedNode, fs.constants.COPYFILE_FICLONE);
  fs.chmodSync(requestedNode, 0o700);
  const nodeExecutable = fs.realpathSync(requestedNode);
  const nodeRuntimeRoot = path.dirname(nodeExecutable);
  const child = spawnSync(
    nodeExecutable,
    ["--import", "tsx", path.join(repoRoot, "scripts", "install-doctor-proof.ts")],
    {
      cwd: repoRoot,
      encoding: "utf8",
      maxBuffer: 12 * 1024 * 1024,
      timeout: 90_000,
      env: {
        HOME: home,
        PLIMSOLL_HOME: path.join(home, ".plimsoll"),
        TMPDIR: temp,
        PATH: [nodeRuntimeRoot, ...systemPathEntries].join(":"),
        SHELL: "/bin/zsh",
        LANG: "C.UTF-8",
        LC_ALL: "C.UTF-8",
        USER: `plimsoll-portability-${label}`,
        LOGNAME: `plimsoll-portability-${label}`,
        TERM: "dumb",
        CI: "1",
        NO_COLOR: "1",
      },
    },
  );
  assert.equal(child.error, undefined, `${label} actual install-doctor child could not start`);
  assert.equal(child.signal, null, `${label} actual install-doctor child was terminated`);
  const failureCheck = child.stderr.match(/^Error: ([a-z0-9_]+):/m)?.[1] ?? "unclassified";
  const stackFrames = [...child.stderr.matchAll(/\s+at\s+([A-Za-z0-9_.<>]+).*?\/([^/():]+):(\d+):\d+/g)]
    .slice(0, 8)
    .map((match) => ({ function: match[1], file: match[2], line: Number(match[3]) }));
  const contentFreeFailure = {
    status: child.status,
    stdoutBytes: Buffer.byteLength(child.stdout),
    stderrBytes: Buffer.byteLength(child.stderr),
    stdoutDigest: digest(child.stdout),
    stderrDigest: digest(child.stderr),
    failureCheck,
    errorCode: child.stderr.match(/\b(ERR_[A-Z0-9_]+)\b/)?.[1] ?? "none",
    errorClass: child.stderr.match(/\b([A-Z][A-Za-z]+Error)\b/)?.[1] ?? "none",
    childCommandErrorCode: child.stderr.match(/"childCommandErrorCode":"([A-Z0-9_]+)"/)?.[1] ?? "none",
    childCommandErrorClass: child.stderr.match(/"childCommandErrorClass":"([A-Za-z]+Error|none)"/)?.[1] ?? "none",
    childCommandFrames: child.stderr.match(/"stderrFrames":(\[[^\]]*\])/)?.[1] ?? "[]",
    stackFrames,
    categories: {
      moduleResolution: /module|package|import/i.test(child.stderr),
      executableResolution: /ENOENT|EACCES|permission|executable/i.test(child.stderr),
      dynamicLinker: /dyld|dylib|library not loaded/i.test(child.stderr),
      transform: /transform|tsx|esbuild/i.test(child.stderr),
      assertion: /assert/i.test(child.stderr),
      json: /json|parse/i.test(child.stderr),
      unexpectedJsonEnd: /unexpected end of json/i.test(child.stderr),
      option: /option|argument|usage/i.test(child.stderr),
    },
  };
  assert.equal(
    child.status,
    0,
    `${label} actual install-doctor child failed: ${JSON.stringify(contentFreeFailure)}`,
  );
  assert.equal(child.stderr, "", `${label} actual install-doctor child emitted stderr`);
  const context = {
    baseDirectory: repoRoot,
    roots: [
      { label: "repository", absolutePath: repoRoot },
      { label: "proof", absolutePath: portabilityRoot },
      { label: "machine-home", absolutePath: home },
      { label: "machine-temp", absolutePath: temp },
      { label: "node-runtime", absolutePath: nodeRuntimeRoot },
    ],
  } satisfies SupportingNormalizationContext;
  return {
    artifact: parseSupportingArtifact("json_result", child.stdout, context),
    context,
    stdout: child.stdout,
  };
}

function contentFreeNormalizedDiff(left: unknown, right: unknown) {
  const differences: Array<{ pointer: string; leftDigest: string; rightDigest: string }> = [];
  const visit = (leftValue: unknown, rightValue: unknown, pointer: string) => {
    if (canonicalBytes(leftValue) === canonicalBytes(rightValue)) return;
    const leftRecord = leftValue && typeof leftValue === "object" ? leftValue as Record<string, unknown> : undefined;
    const rightRecord = rightValue && typeof rightValue === "object" ? rightValue as Record<string, unknown> : undefined;
    if (leftRecord && rightRecord && Array.isArray(leftValue) === Array.isArray(rightValue)) {
      const keys = [...new Set([...Object.keys(leftRecord), ...Object.keys(rightRecord)])].sort();
      for (const key of keys) {
        visit(
          leftRecord[key],
          rightRecord[key],
          `${pointer}/${key.replace(/~/g, "~0").replace(/\//g, "~1")}`,
        );
      }
      return;
    }
    differences.push({
      pointer,
      leftDigest: digest({ present: leftValue !== undefined, value: leftValue }),
      rightDigest: digest({ present: rightValue !== undefined, value: rightValue }),
    });
  };
  visit(left, right, "");
  return {
    leftDigest: digest(left),
    rightDigest: digest(right),
    differenceCount: differences.length,
    differences: differences.slice(0, 32),
    truncated: differences.length > 32,
  };
}

function replaceFirstMissingPath(value: unknown, replacement: string): boolean {
  if (Array.isArray(value)) return value.some((child) => replaceFirstMissingPath(child, replacement));
  if (!value || typeof value !== "object") return false;
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    if (key === "missingRequiredEntries" && Array.isArray(child) && child.length > 0) {
      child[0] = replacement;
      return true;
    }
    if (replaceFirstMissingPath(child, replacement)) return true;
  }
  return false;
}

const portabilityRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-system-e2e-portability-"));
let actualChildDigest = "";
try {
  const actualVariants = [
    runActualInstallDoctorVariant(portabilityRoot, "a", "short-a"),
    runActualInstallDoctorVariant(portabilityRoot, "b", path.join("different", "directory", "depth", "variant-b")),
  ];
  const actualDiff = contentFreeNormalizedDiff(actualVariants[0]!.artifact, actualVariants[1]!.artifact);
  assert.ok(
    actualDiff.differenceCount === 0,
    `actual install-doctor portability mismatch: ${JSON.stringify(actualDiff)}`,
  );
  actualChildDigest = digest(actualVariants[0]!.artifact);
  const supportContract = loadSupportContract(supportContractPath(repoRoot));
  const installContract = supportContract.phases.find((phase) => phase.name === "install_doctor");
  assert.equal(actualChildDigest, installContract?.expectedArtifactDigest, "actual install-doctor portability digest drifted");

  const mysteryPathChild = JSON.parse(actualVariants[0]!.stdout) as unknown;
  assert.equal(
    replaceFirstMissingPath(mysteryPathChild, "/mystery/outside-owned-roots/bin"),
    true,
    "actual install-doctor child had no declared PATH collection to challenge",
  );
  assert.throws(
    () => parseSupportingArtifact(
      "json_result",
      JSON.stringify(mysteryPathChild),
      actualVariants[0]!.context,
    ),
    /DeclaredPathOutsideAllowedRoots:missingRequiredEntries/,
    "actual install-doctor mystery PATH entry did not fail closed",
  );
} finally {
  fs.rmSync(portabilityRoot, { recursive: true, force: true });
}

console.log(JSON.stringify({
  schema: fixture.schema,
  status: "pass",
  variants: normalized.length,
  semanticDigest: digest(normalized[0]),
  outsideRootTamperRejected: true,
  actualChildVariants: 2,
  actualChildSemanticDigest: actualChildDigest,
  actualChildMysteryPathRejected: true,
}, null, 2));
