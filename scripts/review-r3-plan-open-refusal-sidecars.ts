import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover } from "../packages/collector-cli/src/fresh-ledger-cutover";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
  "r3-open-plan-refusal-")));
const ledgerPath = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const root = path.join(fixture, "codex");
const epoch = "10000000-0000-4000-8000-000000000001";
fs.mkdirSync(root);
fs.mkdirSync(archiveDirectory, { mode: 0o700 });
const config = collectorConfigSchema.parse({
  tenantId: "30000000-0000-4000-8000-000000000003",
  deviceId: "40000000-0000-4000-8000-000000000004",
  installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
    profileId: "profile", directory: root, installationEpochId: epoch }],
});
let buffer: LocalEventBuffer | undefined;
try {
  buffer = new LocalEventBuffer(ledgerPath, { workspaceId: config.tenantId,
    deviceId: config.deviceId, freshCaptureRootEpoch: epoch });
  // A mismatched device forces a refusal after plan has opened the live WAL
  // ledger through its fallback path.
  const mismatched = { ...config, deviceId: "50000000-0000-4000-8000-000000000005" };
  const plan = planFreshLedgerCutover({ ledgerPath,
    archivePath: path.join(archiveDirectory, "old-ledger.sqlite"), config: mismatched });
  console.log(JSON.stringify({ status: plan.status, reason: plan.reason,
    sidecarsMayAppear: plan.sidecarsMayAppear,
    walPresent: fs.existsSync(`${ledgerPath}-wal`), shmPresent: fs.existsSync(`${ledgerPath}-shm`) }));
  assert.equal(plan.status, "refused");
  assert.equal(plan.reason, "archive_identity_mismatch");
  assert.equal(plan.sidecarsMayAppear, true,
    "a refused plan on an open ledger must still disclose possible sidecar writes");
} finally {
  buffer?.close();
  fs.rmSync(fixture, { recursive: true, force: true });
}
