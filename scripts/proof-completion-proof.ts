import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createProofCompletion } from "./lib/proof-completion";
import { runProof } from "./run-proof";

const completion = createProofCompletion("proof-completion", 10);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "completion-canaries-"));
const helper = path.resolve(import.meta.dirname, "lib/proof-completion.ts");
const cases = [
  ["complete", 'const p = createProofCompletion("canary", 1); p.check("done"); p.complete();', "passed"],
  ["pending-promise", 'const p = createProofCompletion("canary", 1); async function main() { await new Promise(() => {}); p.check("unreachable"); p.complete(); } void main();', "failed"],
  ["early-zero-exit", 'createProofCompletion("canary", 1); process.exit(0);', "failed"],
  ["partial-count", 'const p = createProofCompletion("canary", 2); p.check("only-one"); p.complete();', "failed"],
  ["failed-check", 'const p = createProofCompletion("canary", 1); p.check("bad", false); p.complete();', "failed"],
  ["missing-receipt", 'process.exit(0);', "failed"],
  ["source-changed", 'const p = createProofCompletion("canary", 1); fs.appendFileSync(ENTRY, "// changed\\n"); p.check("done"); p.complete();', "failed"],
  ["unsafe-provider-root", 'process.env.CODEX_HOME = "/outside-proof-root"; createProofCompletion("canary", 1);', "failed"],
  ["portable-proof-home", 'const p = createProofCompletion("canary", 1); p.check("explicit_home_marker", process.env.PLIMSOLL_PROOF_HOME === process.env.HOME); p.complete();', "passed"],
  ["forged-home-marker", 'process.env.HOME = process.env.PLIMSOLL_PROOF_HOME = "/outside-proof-root"; process.env.PLIMSOLL_HOME = "/outside-proof-root/.plimsoll"; createProofCompletion("canary", 1);', "failed"],
] as const;
async function main() {
try {
  for (const [name, body, expected] of cases) {
    const entry = path.join(root, `${name}.ts`);
    fs.writeFileSync(entry, `import fs from "node:fs";\nimport { createProofCompletion } from ${JSON.stringify(helper)};\nconst ENTRY = ${JSON.stringify(entry)};\n${body}\n`);
    const result = await runProof(entry, { directNode: true, quiet: true });
    assert.equal(result.status, expected, name);
    const isolation = result.isolation as { nodeUnchanged: boolean; sentinelUnchanged: boolean };
    assert.ok(isolation.nodeUnchanged && isolation.sentinelUnchanged, name);
    completion.check(name);
  }
} finally { fs.rmSync(root, { recursive: true, force: true }); }
completion.complete();

}
main().catch(error => { console.error(error); process.exitCode = 1; });
