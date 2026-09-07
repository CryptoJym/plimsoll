import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-labels-response-"));
process.env.HOME = path.join(root, "home");
process.env.PLIMSOLL_HOME = path.join(root, "state");
fs.mkdirSync(process.env.HOME);
fs.mkdirSync(process.env.PLIMSOLL_HOME);
const checks: { name: string; passed: boolean; detail?: string }[] = [];

async function check(name: string, action: () => Promise<void>) {
  try { await action(); checks.push({ name, passed: true }); }
  catch (error) { checks.push({ name, passed: false, detail: String(error) }); }
}

async function main() {
  const { collectorConfigSchema } = await import("../packages/collector-cli/src/config");
  const { buildRepoLabelCandidates, pushRepoLabels } = await import("../packages/collector-cli/src/repo-labels");
  const config = collectorConfigSchema.parse({
    tenantId: "00000000-0000-4000-8000-000000000001", installKey: "fixture-install",
    uploadUrl: "http://127.0.0.1:1/api/work-intelligence/ingest",
  });
  const { candidates } = buildRepoLabelCandidates([
    { repoHash: `sha256:${"a".repeat(64)}`, label: "github.com/fixture/labels" },
  ], []);
  assert.equal(candidates.length, 1);
  const malformed: [string, unknown][] = [
    ["string", "fixture-private-server-text"], ["number", 42], ["boolean", true],
    ["null", null], ["array", ["fixture-private-server-text"]],
    ["error_object", { created: 1, updated: 0, error: "fixture-private-server-text" }],
    ["partial_count", { created: 0, updated: 0 }],
  ];
  for (const [name, body] of malformed) await check(`malformed_${name}_is_symbolic_without_progress`, async () => {
    let calls = 0;
    const logs: string[] = [];
    await assert.rejects(pushRepoLabels(config, candidates, {
      fetchImpl: (async (_input, init) => {
        calls += 1;
        assert.equal(init?.redirect, "manual");
        return new Response(JSON.stringify(body));
      }) as typeof fetch,
      log: line => logs.push(line),
    }), error => {
      assert.ok(error instanceof Error);
      assert.equal(error.message, "Workspace repo-labels deferred: invalid_response");
      return true;
    });
    assert.equal(calls, 1);
    assert.deepEqual(logs, []);
  });
  await check("complete_label_counts_are_accepted", async () => {
    const result = await pushRepoLabels(config, candidates, {
      fetchImpl: (async () => new Response(JSON.stringify({ ok: true, created: 1, updated: 0 }))) as typeof fetch,
      log: () => {},
    });
    assert.deepEqual(result, { pushed: 1, created: 1, updated: 0, batches: 1 });
  });
}

let complete = false;
main().then(() => { complete = true; }).catch(error => {
  checks.push({ name: "execution", passed: false, detail: String(error) });
}).finally(() => {
  const receipt = { schema: "plimsoll.repo-labels-response-proof/v1", complete,
    passed: checks.filter(c => c.passed).length, failed: checks.filter(c => !c.passed).length, checks };
  console.log(JSON.stringify(receipt, null, 2));
  fs.rmSync(root, { recursive: true, force: true });
  if (!complete || receipt.failed) process.exitCode = 1;
});
