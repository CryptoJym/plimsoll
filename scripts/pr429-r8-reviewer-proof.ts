import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

import { createProofCompletion, requireIsolatedProofEnvironment } from "./lib/proof-completion";

// Stage the reviewer's fixture in the directory layout it was written for.
// PR #426 requires UUID installation epochs. Verify that the committed source
// differs from the reviewer's original bytes only at those two fixture IDs.
const root = requireIsolatedProofEnvironment();
const repo = path.resolve(import.meta.dirname, "..");
const source = path.join(repo, "review/pr429-r8/checks/continuation-prefix-rewrite.ts");
const staged = path.join(root, "output/r8/checks/continuation-prefix-rewrite.ts");
const adapted = fs.readFileSync(source, "utf8");
const epochA = 'installationEpochId:"48b5605a-6991-4eae-88c8-b565aa25a61a"';
const epochB = 'installationEpochId:"6b0519c3-601f-4fd3-876f-28d197e7cbf4"';
assert.equal(adapted.split(epochA).length, 2);
assert.equal(adapted.split(epochB).length, 2);
const reviewerBytes = adapted.replace(epochA, 'installationEpochId:"epoch-a"')
  .replace(epochB, 'installationEpochId:"epoch-b"');
assert.equal(createHash("sha256").update(reviewerBytes).digest("hex"),
  "b2f9309f1cbfb3c51f127a86cf82d2c13fa1958c935c705da05aa27fbb8356ed");
fs.mkdirSync(path.dirname(staged), {recursive: true});
fs.mkdirSync(path.join(root, "work"), {recursive: true});
fs.copyFileSync(source, staged);
fs.symlinkSync(repo, path.join(root, "work/plimsoll-r5"), "dir");
assert.deepEqual(fs.readFileSync(staged), fs.readFileSync(source));

const mode = process.argv[2] ?? "oversized";
assert.ok(mode === "oversized" || mode === "normal");
const child = spawnSync(process.execPath,
  ["--import", path.join(repo, "node_modules/tsx/dist/loader.mjs"), staged, mode],
  {cwd: repo, env: process.env, encoding: "utf8", timeout: 120_000});
process.stdout.write(child.stdout ?? "");
process.stderr.write(child.stderr ?? "");
if (child.error) throw child.error;
assert.equal(child.status, 0, `reviewer ${mode} fixture must pass unchanged`);
const proof = createProofCompletion(`pr429-r8-reviewer-${mode}`, 1);
proof.check(`reviewer_${mode}_fixture_passed`);
proof.complete();
