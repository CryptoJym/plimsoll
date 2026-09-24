import fs from "node:fs";
import path from "node:path";

/**
 * Proof suites and the sub-proofs each one runs (eco-6hoxj.163.23).
 *
 * scripts/proof-suites.json is the only run list a suite has. The suite runs
 * exactly its entry, its completion receipt must name every declared
 * sub-proof (scripts/run-proof.ts checks this), and proof:ci-coverage counts
 * a sub-proof as run in CI only through this file.
 */
export const PROOF_SUITES = "scripts/proof-suites.json";

export function readProofSuites(repoRoot: string): unknown {
  const file = path.join(repoRoot, PROOF_SUITES);
  return fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : {};
}

/** The sub-proofs a suite declares, or null when the file is not a suite. */
export function subProofsOf(repoRoot: string, suite: string): string[] | null {
  const manifest = readProofSuites(repoRoot);
  if (manifest === null || typeof manifest !== "object" || Array.isArray(manifest)) {
    throw new Error(`${PROOF_SUITES} is not a mapping of suite files to sub-proof files`);
  }
  if (!Object.hasOwn(manifest, suite)) return null;
  const declared = (manifest as Record<string, unknown>)[suite];
  if (!Array.isArray(declared) || declared.length === 0 || !declared.every((file) => typeof file === "string")) {
    throw new Error(`${PROOF_SUITES}: ${suite} must list at least one sub-proof file`);
  }
  return declared as string[];
}
