/** Run each directed PR #428 crash witness under the join fixture cloud. */
import { spawnSync } from "node:child_process";
import path from "node:path";

const cases: Record<string, string[]> = {
  "r3-1": ["crash_manifest_link", "crash_manifest_link_running"],
  "r3-2": ["crash_obligation_open", "crash_obligation_fsync", "corrupt_obligation_loaded"],
  "r3-7": ["crash_after_bootstrap"],
};
const selected = process.argv[2] ?? "";
const scenarios = cases[selected];
if (!scenarios) throw new Error(`Unknown join crash review case: ${selected}`);

const repo = path.resolve(import.meta.dirname, "..");
const tsx = path.join(repo, "node_modules/tsx/dist/cli.mjs");
const proof = path.join(repo, "scripts/join-setup-e2e-proof.ts");
for (const scenario of scenarios) {
  const run = spawnSync(process.execPath, [tsx, proof], {
    cwd: repo,
    env: { ...process.env, PR428_REVIEW_SCENARIO: scenario },
    stdio: "inherit",
    timeout: 180_000,
  });
  if (run.error) throw run.error;
  if (run.status !== 0 || run.signal !== null)
    throw new Error(`${selected}/${scenario} failed: exit=${run.status}, signal=${run.signal}`);
}
console.log(JSON.stringify({ proof: "pr428-r3-join-crash", selected, scenarios, status: "pass" }));
