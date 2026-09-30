import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { planFreshLedgerCutover, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
  "pr426-replaced-during-switch-")));
const ledgerPath = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const codex = path.join(fixture, "codex");
const claude = path.join(fixture, "claude");
const file = path.join(claude, "project", "70000000-0000-4000-8000-000000000007.jsonl");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const line = (id: string, at: string) => JSON.stringify({ type: "assistant", timestamp: at,
  message: { id, model: "claude-opus-5", usage: { input_tokens: 10, output_tokens: 0 } } }) + "\n";
fs.mkdirSync(codex);
fs.mkdirSync(path.dirname(file), { recursive: true });
fs.mkdirSync(archiveDirectory, { mode: 0o700 });
const roots = [
  { source: "codex" as const, rootId: "codex-root", profileId: "codex-profile",
    directory: codex, installationEpochId: epoch },
  { source: "claude_code" as const, rootId: "claude-root", profileId: "claude-profile",
    directory: claude, installationEpochId: epoch },
];
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
  installKey: "fixture-install-key", captureRoots: roots });
let buffer: LocalEventBuffer | undefined;
let transcript: TranscriptTailer | undefined;
let rollout: RolloutTailer | undefined;
async function main() {
  try {
    const yesterday = new Date(Date.now() - 86_400_000).toISOString();
    fs.writeFileSync(file, line("archive-generation", yesterday));
    buffer = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
    transcript = new TranscriptTailer(buffer, undefined, undefined, [roots[1]!]);
    await transcript.scan({ scope: "full" });
    assert.equal((buffer.database.prepare("select count(*) as n from rollout_scan_state")
      .get() as { n: number }).n, 1);
    transcript.close(); transcript = undefined;
    buffer.close(); buffer = undefined;
    const oldInode = fs.statSync(file).ino;
    const input = { ledgerPath, archivePath: path.join(archiveDirectory, "old-ledger.sqlite"),
      config, authorityRoot: path.join(fixture, "lifecycle-authority") };
    const plan = planFreshLedgerCutover(input);
    assert.equal(plan.status, "ready", plan.reason ?? "");
    assert.equal(plan.cursorRows, 1);
    assert.equal(plan.untrackedFileFences, 0);
    switchFreshLedger({ ...input, onStep: step => {
      if (step !== "stage_bound") return;
      const replacementFile = `${file}.new`;
      // A different writer atomically replaces the pathname after the switch's
      // root inventory, but before its active-ledger rename.
      fs.writeFileSync(replacementFile,
        line(`preexisting-new-generation-${"x".repeat(256)}`, new Date().toISOString()));
      fs.renameSync(replacementFile, file);
      assert.notEqual(fs.statSync(file).ino, oldInode);
    } });
    buffer = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    rollout = new RolloutTailer(buffer, undefined, () => [], undefined, [roots[0]!]);
    transcript = new TranscriptTailer(buffer, undefined, undefined, [roots[1]!]);
    for (let pass = 0; pass < 30 && captureBaselineStatus(buffer.database).status !== "complete"; pass++) {
      for (const tailer of [rollout, transcript])
        await tailer.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
    }
    assert.equal(captureBaselineStatus(buffer.database).status, "complete");
    const firstCapture = await transcript.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
    const historical = (buffer.database.prepare("select count(*) as n from buffered_events")
      .get() as { n: number }).n;
    assert.equal(historical, 0);
    fs.appendFileSync(file, line("post-switch", new Date().toISOString()));
    const laterCaptures = [];
    for (let pass = 0; pass < 5; pass++)
      laterCaptures.push(await transcript.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } }));
    const later = (buffer.database.prepare("select count(*) as n from buffered_events")
      .get() as { n: number }).n;
    console.log(JSON.stringify({ archiveCursorRows: 1, carriedCursorRows: plan.cursorRows,
      fencedFiles: plan.untrackedFileFences, historical, later,
      firstExcludedBytes: firstCapture.excludedBytes,
      afterAppendExcludedBytes: laterCaptures.map(result => result.excludedBytes),
      cursor: buffer.database.prepare("select file_identity,committed_offset,size from rollout_scan_state").all(),
      fileSize: fs.statSync(file).size }));
    assert.equal(later, 1, "a new inode at an old cursor path must admit only post-switch growth");
  } finally {
    transcript?.close(); rollout?.close(); buffer?.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
