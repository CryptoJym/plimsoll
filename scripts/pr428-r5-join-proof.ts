/** Run the round-five join edge cases with the existing isolated fixture cloud. */
import { spawnSync } from "node:child_process";
import path from "node:path";

const selected = process.argv[2];
const scenarios: Record<string, string[]> = {
  "r5-1": [],
  "r5-2": [],
  "r5-3": ["owner_edit_restore_conflict", "owner_edit_after_recheck", "owner_edit_unreadable"],
  "r5-4": ["foreign_rollout_beyond_limit"],
};
const selectedScenarios = selected ? scenarios[selected] : undefined;
if (!selected || !selectedScenarios)
  throw new Error(`Unknown round-five join case: ${String(selected)}`);

const repo = path.resolve(import.meta.dirname, "..");
const tsx = path.join(repo, "node_modules/tsx/dist/cli.mjs");
const run = (proof: string, scenario?: string) => {
  const result = spawnSync(process.execPath, [tsx, path.join(repo, proof)], {
    cwd: repo,
    env: { ...process.env, ...(scenario ? { PR428_REVIEW_SCENARIO: scenario } : {}) },
    stdio: "inherit",
    timeout: 180_000,
  });
  if (result.error) throw result.error;
  if (result.status !== 0 || result.signal !== null)
    throw new Error(`${selected}/${scenario ?? "rollout_boundary"} failed: ` +
      `exit=${result.status}, signal=${result.signal}`);
};

if (selected === "r5-1")
  run("review/pr428-r5/pr428-r5-stale-lifecycle-proof.ts");
if (selected === "r5-2")
  run("review/pr428-r5/pr428-r5-prepared-link-proof.ts");
if (selected === "r5-4")
  run("review/pr428-r5/pr428-r5-rollout-boundary-proof.ts");
for (const scenario of selectedScenarios)
  run("scripts/join-setup-e2e-proof.ts", scenario);
console.log(JSON.stringify({ proof: "pr428-r5-join", selected,
  scenarios: selectedScenarios, status: "pass" }));
