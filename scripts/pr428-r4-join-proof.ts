/** Directed join fixtures from the independent PR #428 round-four review. */
import { spawnSync } from "./lib/proof-child-process";
import path from "node:path";

const selected = process.argv[2];
const cases: Record<string, Array<{ script: string; scenario?: string }>> = {
  "r4-3": [
    { script: "join-setup-e2e-proof.ts", scenario: "owner_edit_during_join" },
    { script: "join-setup-e2e-proof.ts", scenario: "owner_edit_first_unload" },
  ],
  "r4-4": [
    { script: "join-setup-e2e-proof.ts", scenario: "foreign_rollout" },
  ],
};
if (!selected || !cases[selected]) throw new Error(`Unknown join review case: ${selected}`);
const repo = path.resolve(import.meta.dirname, "..");
const tsx = path.join(repo, "node_modules/tsx/dist/cli.mjs");
for (const entry of cases[selected]) {
  const run = spawnSync(process.execPath, [tsx, path.join(repo, "scripts", entry.script)], {
    cwd: repo,
    env: { ...process.env, ...(entry.scenario ? { PR428_REVIEW_SCENARIO: entry.scenario } : {}) },
    stdio: "inherit",
    timeout: 180_000,
  });
  if (run.error) throw run.error;
  if (run.status !== 0 || run.signal !== null)
    throw new Error(`${selected}/${entry.scenario ?? entry.script} failed: exit=${run.status}, signal=${run.signal}`);
}
console.log(JSON.stringify({ proof: "pr428-r4-join", selected, status: "pass" }));
