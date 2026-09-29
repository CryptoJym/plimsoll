/** Named round-six review regressions, each in its own fixture process. */
import { spawnSync } from "node:child_process";
import path from "node:path";

const selected = process.argv[2];
const specs: Record<string, Array<{ proof: string; scenario?: string; failOnce?: boolean }>> = {
  "r6-1": [
    { proof: "review/pr428-r6/pr428-r6-copied-lifecycle-proof.ts" },
    { proof: "scripts/pr428-r6-live-pointer-proof.ts" },
  ],
  "r6-2": [
    { proof: "scripts/join-setup-e2e-proof.ts", scenario: "owner_edit_restore_conflict", failOnce: true },
    { proof: "scripts/join-setup-e2e-proof.ts", scenario: "owner_edit_one_bootstrap_failure" },
  ],
  "r6-3": [
    { proof: "review/pr428-r6/pr428-r6-lifecycle-publish-crash-proof.ts" },
    { proof: "scripts/pr428-r6-lifecycle-crash-proof.ts" },
  ],
};
const selectedSpecs = selected ? specs[selected] : undefined;
if (!selectedSpecs) throw new Error(`Unknown round-six join case: ${String(selected)}`);
const repo = path.resolve(import.meta.dirname, "..");
const tsx = path.join(repo, "node_modules/tsx/dist/cli.mjs");
for (const spec of selectedSpecs) {
  const result = spawnSync(process.execPath, [tsx, path.join(repo, spec.proof)], {
    cwd: repo,
    env: { ...process.env,
      ...(spec.scenario ? { PR428_REVIEW_SCENARIO: spec.scenario } : {}),
      ...(spec.failOnce ? { PLIMSOLL_PROOF_FAIL_RECOVERY_BOOTSTRAP_ONCE: "1" } : {}),
    },
    stdio: "inherit",
    timeout: 180_000,
  });
  if (result.error) throw result.error;
  if (result.status !== 0 || result.signal !== null)
    throw new Error(`${selected}/${spec.proof} failed: exit=${result.status}, signal=${result.signal}`);
}
console.log(JSON.stringify({ proof: "pr428-r6-join", selected, status: "pass" }));
