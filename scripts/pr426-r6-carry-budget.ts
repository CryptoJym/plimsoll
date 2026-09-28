import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
  "pr426-carry-budget-")));
try {
  const ledgerPath = path.join(fixture, "work-ledger.sqlite");
  const archiveDirectory = path.join(fixture, "archive");
  const root = path.join(fixture, "codex");
  fs.mkdirSync(root);
  fs.mkdirSync(archiveDirectory, { mode: 0o700 });
  const config = collectorConfigSchema.parse({
    tenantId: "30000000-0000-4000-8000-000000000003",
    deviceId: "40000000-0000-4000-8000-000000000004",
    installKey: "fixture-install-key", captureRoots: [{
      source: "codex", rootId: "root", profileId: "profile", directory: root,
      installationEpochId: "10000000-0000-4000-8000-000000000001",
    }],
  });
  const old = new LocalEventBuffer(ledgerPath, {
    workspaceId: config.tenantId, deviceId: config.deviceId,
    freshCaptureRootEpoch: config.captureRoots![0]!.installationEpochId,
  });
  old.database.prepare(`insert into codex_live_producers
    (producer_id,context_digest,context_json,credential_id,enabled)
    values(?,?,?,?,0)`).run("large-retained-row", "digest", `{"padding":"${"x".repeat(33 * 1024 * 1024)}"}`,
      "credential");
  old.close();
  const input = { ledgerPath, archivePath: path.join(archiveDirectory, "old-ledger.sqlite"),
    config, authorityRoot: path.join(fixture, "lifecycle-authority") };
  const plan = planFreshLedgerCutover(input);
  assert.equal(plan.status, "refused");
  assert.equal(plan.reason, "carried_state_exceeds_32_mib_budget");
  assert.equal(plan.carriedRows.codex_live_producers, 1);
  assert.ok(plan.carriedBytes.codex_live_producers > plan.carryBudgetBytes);
  assert.throws(() => switchFreshLedger(input), /carried_state_exceeds_32_mib_budget/);
  assert.equal(fs.existsSync(input.archivePath), false);
  console.log(JSON.stringify({ reason: plan.reason,
    rows: plan.carriedRows.codex_live_producers,
    bytes: plan.carriedBytes.codex_live_producers,
    budgetBytes: plan.carryBudgetBytes, archiveCreated: false }));
} finally {
  fs.rmSync(fixture, { recursive: true, force: true });
}
