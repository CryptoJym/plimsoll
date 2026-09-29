import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

// Exercise the committed release sources against one disposable ledger. CI
// fetches complete ancestry; no released package or live collector is used.
const releases = [
  { version: "0.7.39", commit: "de20f7e1d7b9e50be6bcc1b2d760e580c60da02e", dir: "old39" },
  { version: "0.7.44", commit: "1ae7bbc8186fb5b8061fd305f14fa530ae978315", dir: "old44" },
] as const;
const proof = process.argv[2];
assert.ok(proof && /^review-tests\/[a-z0-9-]+\.ts$/.test(proof),
  "expected a retention review test");
const repo = path.resolve(__dirname, "..");
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-exact-legacy-retention-"));
const installed: string[] = [];
function git(...args: string[]) {
  const result = spawnSync("git", args, { cwd: repo, encoding: "utf8" });
  if (result.status !== 0) throw new Error(`git ${args[0]} failed: ${result.stderr}`);
  return result.stdout.trim();
}
try {
  for (const release of releases) {
    assert.equal(git("rev-parse", `${release.commit}^{commit}`), release.commit);
    const root = path.join(scratch, release.dir);
    git("worktree", "add", "--detach", root, release.commit);
    installed.push(root);
    fs.symlinkSync(path.join(repo, "node_modules"), path.join(root, "node_modules"));
    const packageJson = JSON.parse(fs.readFileSync(path.join(root,
      "packages/collector-cli/package.json"), "utf8")) as { version: string };
    assert.equal(packageJson.version, release.version);
  }
  const old39 = path.join(scratch, "old39");
  const old44 = path.join(scratch, "old44");
  const child = spawnSync(process.execPath,
    [path.join(repo, "node_modules/tsx/dist/cli.mjs"), proof], {
      cwd: repo, stdio: "inherit", timeout: 120_000,
      env: { ...process.env, PR417_0739_WORKTREE: old39,
        PR417_BASE_WORKTREE: proof.includes("null-generation-privacy") ? old39 : old44,
        PR417_SEED_WORKTREE: old39 },
    });
  if (child.error) throw child.error;
  if (child.signal) throw new Error(`proof stopped by ${child.signal}`);
  assert.equal(child.status, 0, `${proof} failed`);
} finally {
  for (const root of installed.reverse()) git("worktree", "remove", "--force", root);
  fs.rmSync(scratch, { recursive: true, force: true });
}
