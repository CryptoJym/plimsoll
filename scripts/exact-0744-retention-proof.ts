import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

// CI fetches complete ancestry. Run the actual 0.7.44 buffer implementation
// against a fixture ledger without publishing or installing an old package.
const baseCommit = "1ae7bbc8186fb5b8061fd305f14fa530ae978315";
const proof = process.argv[2];
assert.ok(proof && /^review-tests\/[a-z-]+\.ts$/.test(proof), "expected a retention review test");
const repo = path.resolve(__dirname, "..");
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-exact-0744-"));
const baseRoot = path.join(scratch, "base");
let added = false;
function git(...args: string[]) {
  const result = spawnSync("git", args, { cwd: repo, encoding: "utf8" });
  if (result.status !== 0) throw new Error(`git ${args[0]} failed: ${result.stderr}`);
  return result.stdout.trim();
}
try {
  assert.equal(git("rev-parse", `${baseCommit}^{commit}`), baseCommit,
    "exact 0.7.44 source must be in the fetched ancestry");
  git("worktree", "add", "--detach", baseRoot, baseCommit);
  added = true;
  fs.symlinkSync(path.join(repo, "node_modules"), path.join(baseRoot, "node_modules"));
  const child = spawnSync(process.execPath,
    [path.join(repo, "node_modules/tsx/dist/cli.mjs"), proof], {
      cwd: repo, stdio: "inherit", timeout: 120_000,
      env: { ...process.env, PR417_BASE_WORKTREE: baseRoot },
    });
  if (child.error) throw child.error;
  if (child.signal) throw new Error(`proof stopped by ${child.signal}`);
  assert.equal(child.status, 0, `${proof} failed`);
} finally {
  if (added) git("worktree", "remove", "--force", baseRoot);
  fs.rmSync(scratch, { recursive: true, force: true });
}
