// plimsoll N216: the carry budget and the staged copy count only cursors the
// replacement keeps. A cursor whose file is gone, or whose file is now another
// generation, is dropped at finalization and its file (if any) is fenced, so it
// must not spend the 8 MiB budget or be copied. Studio0 (2026-11-20) carried
// 20,340 cursors, all for files no longer on disk, and was refused at 19.8 MB.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
  "carry-observed-only-")));
const ledgerPath = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const claude = path.join(fixture, "claude");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const root = { source: "claude_code" as const, rootId: "claude-root", profileId: "claude-profile",
  directory: claude, installationEpochId: epoch };
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
  installKey: "fixture-install-key", captureRoots: [root] });
const line = (id: string, at: string) => JSON.stringify({ type: "assistant", timestamp: at,
  message: { id, model: "claude-opus-5", usage: { input_tokens: 10, output_tokens: 0 } } }) + "\n";
const transcript = (name: string) => path.join(claude, "project", `${name}.jsonl`);
const kept = transcript("70000000-0000-4000-8000-000000000001");
const gone = transcript("70000000-0000-4000-8000-000000000002");
const regenerated = transcript("70000000-0000-4000-8000-000000000003");

async function main() {
  fs.mkdirSync(path.dirname(kept), { recursive: true });
  fs.mkdirSync(archiveDirectory, { mode: 0o700 });
  const yesterday = new Date(Date.now() - 86_400_000).toISOString();
  const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
  const keys: string[] = [];
  const tailer = new TranscriptTailer(buffer, undefined, undefined, [root]);
  for (const [file, id] of [[kept, "kept"], [gone, "gone"], [regenerated, "old-generation"]] as const) {
    fs.writeFileSync(file, line(id, yesterday));
    await tailer.scan({ scope: "full" });
    const all = (buffer.database.prepare("select file from rollout_scan_state").all() as Array<{ file: string }>)
      .map(row => row.file);
    keys.push(all.find(key => !keys.includes(key))!);
  }
  tailer.close();
  buffer.close();
  assert.equal(new Set(keys).size, 3);
  const [keptKey, goneKey, regeneratedKey] = keys as [string, string, string];

  fs.rmSync(gone);                                             // the file left the root
  fs.writeFileSync(`${regenerated}.new`, line(`new-generation-${"x".repeat(64)}`, yesterday));
  fs.renameSync(`${regenerated}.new`, regenerated);            // same name, another generation

  const input = { ledgerPath, archivePath: path.join(archiveDirectory, "old-ledger.sqlite"),
    config, authorityRoot: path.join(fixture, "lifecycle-authority") };
  const plan = planFreshLedgerCutover(input);
  assert.equal(plan.status, "ready", plan.reason ?? "");
  // Before this change the gone file's cursor was counted and copied too (2 rows).
  assert.equal(plan.cursorRows, 1);
  assert.equal(plan.carriedRows.rollout_scan_state, 1);
  assert.equal(plan.untrackedFileFences, 1);                   // the regenerated file is fenced

  switchFreshLedger(input);
  const fresh = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  try {
    const carried = (fresh.prepare("select file from rollout_scan_state").all() as Array<{ file: string }>)
      .map(row => row.file);
    assert.deepEqual(carried, [keptKey]);
    assert.ok(!carried.includes(goneKey) && !carried.includes(regeneratedKey));
  } finally {
    fresh.close();
  }
  const archive = new Database(input.archivePath, { readonly: true, fileMustExist: true });
  try {   // the archive keeps every cursor it had: nothing is lost from the old epoch
    assert.equal((archive.prepare("select count(*) as n from rollout_scan_state").get() as { n: number }).n, 3);
  } finally {
    archive.close();
  }
  console.log(JSON.stringify({ archiveCursors: 3, plannedCarry: plan.cursorRows,
    plannedFences: plan.untrackedFileFences, carriedAfterSwitch: 1, goneCursorCarried: false }));
}

main().catch(error => { console.error(error); process.exitCode = 1; })
  .finally(() => fs.rmSync(fixture, { recursive: true, force: true }));
