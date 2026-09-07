import fs from "node:fs";
import path from "node:path";

export function requireIsolatedProofEnvironment() {
  const root = process.env.PLIMSOLL_PROOF_ROOT;
  if (!root || !path.isAbsolute(root) || fs.realpathSync(root) !== root) {
    throw new Error("proof requires scripts/run-proof.ts with a private disposable root");
  }
  if ((fs.statSync(root).mode & 0o077) !== 0) throw new Error("proof root is not private");
  const marker = JSON.parse(fs.readFileSync(path.join(root, ".proof-root.json"), "utf8"));
  if (marker.schema !== "plimsoll.disposable-proof.v1" || marker.runId !== process.env.PLIMSOLL_PROOF_RUN_ID) {
    throw new Error("proof root was not created for this run");
  }
  for (const key of ["HOME", "USERPROFILE", "PLIMSOLL_HOME", "CODEX_HOME", "CLAUDE_CONFIG_DIR",
    "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME", "TMPDIR", "PLIMSOLL_PROOF_RECEIPT"]) {
    const value = process.env[key];
    const relative = value && path.relative(root, value);
    if (!value || !path.isAbsolute(value) || !relative || relative.startsWith("..") || path.isAbsolute(relative)) {
      throw new Error(`proof isolation missing or unsafe: ${key}`);
    }
    let existing = value;
    while (!fs.existsSync(existing)) existing = path.dirname(existing);
    const real = fs.realpathSync(existing);
    if (real !== root && !real.startsWith(`${root}${path.sep}`)) throw new Error(`proof isolation symlink: ${key}`);
  }
  if (!process.env.PLIMSOLL_PROOF_RUN_ID) throw new Error("proof run identity missing");
  return root;
}

/** A promise is not an event-loop handle. Require the final awaited boundary. */
export function createProofCompletion(proof: string, expectedChecks?: number) {
  requireIsolatedProofEnvironment();
  const receiptPath = process.env.PLIMSOLL_PROOF_RECEIPT!;
  const runId = process.env.PLIMSOLL_PROOF_RUN_ID!;
  const checks: Array<{ name: string; passed: boolean }> = [];
  let completed = false;
  function write(status: "passed" | "failed", reason?: string) {
    const receipt = { schema: "plimsoll.proof-completion.v1", proof, runId, completed,
      status, expectedChecks: expectedChecks ?? null, checks,
      counts: { total: checks.length, passed: checks.filter(c => c.passed).length,
        failed: checks.filter(c => !c.passed).length }, ...(reason ? { reason } : {}) };
    fs.writeFileSync(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`, { mode: 0o600 });
    return receipt;
  }
  // Synchronous exit handler also catches explicit process.exit(0). No timer
  // is added here: a TS loader must not conceal the direct-Node failure class.
  process.on("exit", (code) => {
    if (!completed || code !== 0) {
      write("failed", !completed ? "completion_boundary_not_reached" : "nonzero_exit");
      process.exitCode = code || 1;
    }
  });
  return {
    check(name: string, passed = true) { checks.push({ name, passed: Boolean(passed) }); },
    complete() {
      if (completed) throw new Error("proof completed twice");
      if (checks.length === 0 || checks.some(c => !c.passed) ||
          (expectedChecks !== undefined && checks.length !== expectedChecks)) {
        write("failed", "assertion_count_or_result_mismatch");
        throw new Error("proof assertion count or result mismatch");
      }
      completed = true;
      const receipt = write("passed");
      console.log(JSON.stringify({ proof, completion: "passed", counts: receipt.counts }));
    },
  };
}
