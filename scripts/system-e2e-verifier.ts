#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

import { verifySystemE2EReceipt } from "./system-e2e/verifier";

const repoRoot = path.resolve(import.meta.dirname, "..");
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
const receiptIndex = process.argv.indexOf("--receipt");
const receiptPath = path.resolve(
  receiptIndex >= 0 && process.argv[receiptIndex + 1]
    ? process.argv[receiptIndex + 1]!
    : path.join(repoRoot, "evidence", "system-e2e-proof.json"),
);
assert.ok(fs.existsSync(receiptPath), `system E2E receipt is missing: ${receiptPath}`);
const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8")) as unknown;
const verified = verifySystemE2EReceipt(receipt, repoRoot, expectedSourceHeadCommit);
console.log(JSON.stringify({ ...verified, receipt: path.relative(repoRoot, receiptPath) }, null, 2));
