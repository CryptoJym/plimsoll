// Re-derive support-contract digests from the same isolated supporting proofs
// and normalization code as system-e2e. This only reports candidates; a human
// must inspect changed artifacts before updating the committed pins.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { digest, loadSupportContract, parseSupportingArtifact, supportContractPath } from "./system-e2e/contract.ts";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outArg = process.argv.indexOf("--output");
const output = outArg >= 0 ? path.resolve(process.argv[outArg + 1] ?? "") :
  path.join(repo, "evidence", "system-e2e-pin-candidates.json");
const checks = path.dirname(output);
const runLabel = process.env.PIN_RUN_LABEL ?? "pin";
const proof = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-e2e-pin-predict-"));
const evidence = path.join(proof, "evidence");
const homes = [path.join(proof, "machine-a", "home"), path.join(proof, "machine-b", "home")];
const temps = [path.join(proof, "machine-a", "tmp"), path.join(proof, "machine-b", "tmp")];
for (const directory of [evidence, ...homes, ...temps]) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
fs.mkdirSync(path.dirname(output), { recursive: true });

const specs = [
  ["install_doctor", "scripts/install-doctor-proof.ts", 0],
  ["transactional_join", "scripts/join-isolation-proof.ts", 1],
  ["metadata_only_privacy", "scripts/privacy-mode-proof.ts", 0, "privacy.json"],
  ["canonical_lifecycle", "scripts/lifecycle-proof.ts", 1],
  ["idle_dashboard_resources", "scripts/resource-proof/index.ts", 0, "resource.json"],
  ["launch_agent_unload_terminal_truth", "scripts/launch-agent-unload-proof.ts", 1],
];
const contract = loadSupportContract(supportContractPath(repo));
const predictions = [];
for (const [name, script, machine, receiptName] of specs) {
  const committed = contract.phases.find((phase) => phase.name === name);
  assert.ok(committed, `missing committed phase ${name}`);
  if (name === "launch_agent_unload_terminal_truth") {
    predictions.push({ name, state: "SKIPPED_HOST_ONLY", before: committed.expectedArtifactDigest, after: null });
    continue;
  }
  const home = homes[machine];
  const temp = temps[machine];
  const receipt = receiptName ? path.join(evidence, receiptName) : undefined;
  const args = name === "metadata_only_privacy" ? ["--receipt", receipt] :
    name === "idle_dashboard_resources" ? ["--require-integrated", "--receipt", receipt] : [];
  const env = {
    HOME: home, PLIMSOLL_HOME: path.join(home, ".plimsoll"), TMPDIR: temp,
    PATH: process.env.PATH ?? "/usr/bin:/bin", SHELL: "/bin/zsh",
    LANG: "C.UTF-8", LC_ALL: "C.UTF-8", USER: "plimsoll-e2e", LOGNAME: "plimsoll-e2e",
    TERM: "dumb", CI: "1", NO_COLOR: "1",
  };
  const result = spawnSync(process.execPath,
    ["--import", path.join(repo, "node_modules", "tsx", "dist", "loader.mjs"), path.join(repo, script), ...args],
    { cwd: repo, env, encoding: "utf8", maxBuffer: 12 * 1024 * 1024, timeout: 180_000 });
  const prefix = path.join(checks, `${runLabel}-${name}`);
  fs.writeFileSync(`${prefix}.stdout.log`, result.stdout ?? "");
  fs.writeFileSync(`${prefix}.stderr.log`, result.stderr ?? "");
  assert.equal(result.error, undefined, `${name} start failed: ${String(result.error)}`);
  assert.equal(result.signal, null, `${name} terminated by ${result.signal}`);
  assert.equal(result.status, 0, `${name} failed; see ${prefix}.stdout.log and .stderr.log`);
  const artifact = parseSupportingArtifact(committed.kind, result.stdout, {
    baseDirectory: repo,
    roots: [
      { label: "repository", absolutePath: repo },
      { label: "proof", absolutePath: proof },
      { label: "machine-home", absolutePath: home },
      { label: "machine-temp", absolutePath: temp },
      { label: "node-runtime", absolutePath: path.dirname(process.execPath) },
    ],
  }, receipt);
  fs.writeFileSync(`${prefix}.artifact.json`, JSON.stringify(artifact, null, 2) + "\n");
  const after = digest(artifact);
  predictions.push({ name, state: after === committed.expectedArtifactDigest ? "UNCHANGED" : "CHANGED",
    before: committed.expectedArtifactDigest, after, artifact: `${prefix}.artifact.json` });
  console.log(`${name}: ${committed.expectedArtifactDigest} -> ${after}`);
}
fs.writeFileSync(output, JSON.stringify({ schema: "plimsoll.system-e2e-pin-predictions.v1", predictions }, null, 2) + "\n");
console.log(`wrote ${output}`);
