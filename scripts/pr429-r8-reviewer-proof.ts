import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

import { createProofCompletion, requireIsolatedProofEnvironment } from "./lib/proof-completion";

// Stage the reviewer's exact fixture in the directory layout it was written
// for. Its bytes are copied verbatim; only the fixture's old checkout path is
// supplied as a symlink into this proof's disposable root.
const root = requireIsolatedProofEnvironment();
const repo = path.resolve(import.meta.dirname, "..");
const source = path.join(repo, "review/pr429-r8/checks/continuation-prefix-rewrite.ts");
const staged = path.join(root, "output/r8/checks/continuation-prefix-rewrite.ts");
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
