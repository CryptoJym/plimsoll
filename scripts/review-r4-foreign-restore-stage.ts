import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, restoreArchivedLedger, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r4-foreign-stage-")));
const ledgerPath = path.join(fixture, "work-ledger.sqlite");
const archivePath = path.join(fixture, "archive", "old-ledger.sqlite");
const freshAttemptPath = path.join(fixture, "archive", "fresh-attempt.sqlite");
const stage = `${ledgerPath}.restore-stage`;
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
try {
  fs.mkdirSync(path.join(fixture, "codex"));
  fs.mkdirSync(path.dirname(archivePath), { mode: 0o700 });
  const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
    installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
      profileId: "profile", directory: path.join(fixture, "codex"), installationEpochId: epoch }] });
  const old = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  old.database.prepare("insert into maintenance_state(key,value,updated_at) values(?,?,?)")
    .run("archive-only-sentinel", "must-survive", new Date().toISOString());
  old.close();
  switchFreshLedger({ ledgerPath, archivePath, config,
    authorityRoot: path.join(fixture, "lifecycle-authority") });
  const replacement = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  replacement.close();
  // This is a structurally valid but unrelated stage, e.g. a stale local copy.
  // The restore must verify stage provenance before activating it.
  fs.copyFileSync(ledgerPath, stage);
  const archiveBefore = fs.readFileSync(archivePath);
  const foreignStageBefore = fs.readFileSync(stage);
  let restoreError: string | null = null;
  let returned = false;
  try {
    returned = restoreArchivedLedger({ ledgerPath, archivePath, freshAttemptPath,
      authorityRoot: path.join(fixture, "lifecycle-authority") }).archivePreserved;
  } catch (error) {
    restoreError = error instanceof Error ? error.message : String(error);
  }
  const marker = readReplacementLedgerMarker(ledgerPath);
  const active = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  const sentinel = active.database.prepare("select value from maintenance_state where key=?")
    .get("archive-only-sentinel") as { value: string } | undefined;
  active.close();
  console.log(JSON.stringify({ restoreReturned: returned, restoreError, markerStillActive: !!marker,
    sentinel: sentinel?.value ?? null, archiveByteIdentical: archiveBefore.equals(fs.readFileSync(archivePath)),
    foreignStagePreserved: fs.existsSync(stage) && foreignStageBefore.equals(fs.readFileSync(stage)) }));
  assert.equal(archiveBefore.equals(fs.readFileSync(archivePath)), true);
  assert.equal(fs.existsSync(stage) && foreignStageBefore.equals(fs.readFileSync(stage)), true,
    "refusal must not delete or change an unverified stage");
  if (restoreError === null) {
    assert.equal(marker, null, "restore must not activate a foreign stage as the archived ledger");
    assert.equal(sentinel?.value, "must-survive");
  } else {
    assert.match(restoreError, /restore_stage_(journal_unverified|identity_changed|not_archive_clone)/);
    assert.ok(marker, "a refused restore keeps the replacement active");
    assert.equal(sentinel, undefined);
  }

} finally {
  fs.rmSync(fixture, { recursive: true, force: true });
}
