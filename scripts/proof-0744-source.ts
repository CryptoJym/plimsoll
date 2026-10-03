/** Exact old source from this repository, usable in a clean bundle clone. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

export async function withProof0744Source<T>(action: (checkout: string) => Promise<T>): Promise<T> {
  const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "proof-0744-")));
  const checkout = path.join(root, "collector-0744");
  let added = false;
  try {
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", checkout,
      "375f277b85f7d4ede7db77bf4359c371c0e8a4aa"], { cwd: repo });
    added = true;
    fs.symlinkSync(path.join(repo, "node_modules"), path.join(checkout, "node_modules"), "dir");
    return await action(checkout);
  } finally {
    try { if (added) execFileSync("git", ["worktree", "remove", "--force", checkout], { cwd: repo }); }
    finally { fs.rmSync(root, { recursive: true, force: true }); }
  }
}
