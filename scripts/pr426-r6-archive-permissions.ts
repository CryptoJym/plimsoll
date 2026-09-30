import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger,
  restoreArchivedLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r2-archive-mode-")));
const ledger = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive-public");
const archivePath = path.join(archiveDirectory, "old-ledger.sqlite");
const root = path.join(fixture, "codex");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
fs.mkdirSync(root);
fs.mkdirSync(archiveDirectory, { mode: 0o755 });
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
  installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
    profileId: "profile", directory: root, installationEpochId: epoch }] });
try {
  const old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  old.close();
  const oldMode = fs.statSync(ledger).mode & 0o777;
  // Model a standard SQLite file in a private collector home. A hard link in
  // a public archive directory exposes its same inode and mode.
  fs.chmodSync(ledger, 0o644);
  const input = { ledgerPath: ledger, archivePath, config,
    authorityRoot: path.join(fixture, "lifecycle-authority") };
  const plan = planFreshLedgerCutover(input);
  assert.equal(plan.status, "refused", "a public archive directory must refuse before linking");
  assert.match(plan.reason ?? "", /archive_directory_unsafe/);
  assert.equal(fs.existsSync(archivePath), false);
  fs.chmodSync(archiveDirectory, 0o700);
  const privatePlan = planFreshLedgerCutover(input);
  assert.equal(privatePlan.status, "ready", privatePlan.reason ?? "");
  switchFreshLedger(input);
  const archiveMode = fs.statSync(archivePath).mode & 0o777;
  const replacementMode = fs.statSync(ledger).mode & 0o777;
  console.log(JSON.stringify({ originalSQLiteMode: oldMode.toString(8),
    publicPlanReason: plan.reason, privatePlanStatus: privatePlan.status,
    archiveMode: archiveMode.toString(8), replacementMode: replacementMode.toString(8) }));
  assert.equal(archiveMode, 0o600);
  assert.equal(replacementMode, 0o600);
  const attemptDirectory = path.join(fixture, "fresh-attempt-public");
  fs.mkdirSync(attemptDirectory, { mode: 0o755 });
  const freshAttemptPath = path.join(attemptDirectory, "fresh-attempt.sqlite");
  assert.throws(() => restoreArchivedLedger({ ledgerPath: ledger,
    archivePath, freshAttemptPath, authorityRoot: input.authorityRoot }),
    /fresh_attempt_directory_unsafe/);
  assert.equal(fs.existsSync(freshAttemptPath), false);
  fs.chmodSync(attemptDirectory, 0o700);
  restoreArchivedLedger({ ledgerPath: ledger, archivePath, freshAttemptPath,
    authorityRoot: input.authorityRoot });
  assert.equal(fs.statSync(freshAttemptPath).mode & 0o777, 0o600);
} finally {
  fs.rmSync(fixture, { recursive: true, force: true });
}
