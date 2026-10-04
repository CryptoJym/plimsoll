import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

/** Open a real historical reader in a disposable checkout for upgrade proofs. */
export async function withReader<T>(commit: string, run: (modules: {
  Buffer: any;
  reconciliation: any;
}) => Promise<T> | T): Promise<T> {
  const root = path.join(
    process.cwd(),
    "work",
    `r4-old-reader-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
  );
  fs.mkdirSync(path.dirname(root), { recursive: true });
  execFileSync("git", ["worktree", "add", "--detach", "--quiet", root,
    commit], { cwd: process.cwd(), stdio: "ignore" });
  const modules = path.join(process.cwd(), "node_modules");
  if (fs.existsSync(modules)) fs.symlinkSync(modules, path.join(root, "node_modules"), "dir");
  try {
    const [buffer, reconciliation] = await Promise.all([
      import(pathToFileURL(path.join(root, "packages/collector-cli/src/buffer.ts")).href),
      import(pathToFileURL(path.join(root, "packages/collector-cli/src/codex-reconciliation.ts")).href),
    ]);
    return await run({ Buffer: buffer.LocalEventBuffer, reconciliation });
  } finally {
    try { execFileSync("git", ["worktree", "remove", "--force", root], { cwd: process.cwd(), stdio: "ignore" }); } catch { /* best effort cleanup */ }
    fs.rmSync(root, { recursive: true, force: true });
  }
}

/** The real release immediately before this PR. */
export const withLegacyReader = <T>(run: (modules: { Buffer: any; reconciliation: any }) => Promise<T> | T) =>
  withReader("34d58bcd90865679e09fcbd1ee1703de5effda97", run);

/** The round-three reader used to prove upgrade inheritance. */
export const withRoundThreeReader = <T>(run: (modules: { Buffer: any; reconciliation: any }) => Promise<T> | T) =>
  withReader("98385d8b918147d58339fc5ab838552482384a00", run);

export function proofTempRoot(name: string) {
  return fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), `codex-${name}-`));
}
