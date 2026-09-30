import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr426-baseline-gap-")));
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const session = "70000000-0000-4000-8000-000000000007";
const codex = path.join(fixture, "codex");
const claude = path.join(fixture, "claude");
const transcript = path.join(claude, "project", `${session}.jsonl`);
fs.mkdirSync(codex, { recursive: true });
fs.mkdirSync(path.dirname(transcript), { recursive: true });
const line = (id: string, at: string, tokens: number) => JSON.stringify({
  type: "assistant", timestamp: at,
  message: { id, model: "claude-opus-5", usage: { input_tokens: tokens, output_tokens: 0 } },
}) + "\n";
fs.writeFileSync(transcript, line("old", new Date(Date.now() - 86_400_000).toISOString(), 10));

let buffer: LocalEventBuffer | undefined;
let rollout: RolloutTailer | undefined;
let tailer: TranscriptTailer | undefined;
async function main() {
try {
  const ledger = path.join(fixture, "work-ledger.sqlite");
  const archiveDirectory = path.join(fixture, "archive");
  fs.mkdirSync(archiveDirectory, { mode: 0o700 });
  const archivePath = path.join(archiveDirectory, "old-ledger.sqlite");
  const roots = [
    { source: "codex" as const, rootId: "codex-root", profileId: "codex-profile",
      directory: codex, installationEpochId: epoch },
    { source: "claude_code" as const, rootId: "claude-root", profileId: "claude-profile",
      directory: claude, installationEpochId: epoch },
  ];
  const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
    installKey: "fixture-install-key", captureRoots: roots });
  const old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
  const oldTailer = new TranscriptTailer(old, undefined, undefined, [roots[1]!]);
  await oldTailer.scan({ scope: "full" });
  assert.equal((old.database.prepare("select count(*) as n from rollout_scan_state").get() as {n:number}).n, 1);
  oldTailer.close();
  old.close();
  const planned = planFreshLedgerCutover({ ledgerPath: ledger, archivePath, config });
  assert.equal(planned.status, "ready", planned.reason ?? "");
  assert.equal(planned.cursorRows, 1);
  switchFreshLedger({ ledgerPath: ledger, archivePath, config,
    authorityRoot: path.join(fixture, "lifecycle-authority") });
  buffer = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  const startedAt = buffer.workspaceBinding()!.currentInstallationEpochStartedAt!;
  assert.equal(buffer.workspaceBinding()!.currentInstallationEpochId, epoch);
  const gapAt = new Date(Date.now() + 5).toISOString();
  fs.appendFileSync(transcript, line("after-ledger-before-baseline", gapAt, 17));

  rollout = new RolloutTailer(buffer, undefined, () => [], undefined, [roots[0]!]);
  tailer = new TranscriptTailer(buffer, undefined, undefined, [roots[1]!]);
  for (let pass = 0; pass < 30 && captureBaselineStatus(buffer.database).status !== "complete"; pass += 1) {
    for (const source of [rollout, tailer]) {
      await source.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
    }
  }
  assert.equal(captureBaselineStatus(buffer.database).status, "complete");
  const firstCapture = await tailer.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
  assert.equal(firstCapture.recordsParsed, 1, "only the post-switch append is parsed");
  console.log(JSON.stringify({ firstCapture, carried: buffer.database.prepare(
    "select file_key,source,file_identity,committed_offset from replacement_capture_cursors").all() }));
  const beforeGrowth = buffer.database.prepare("select observed_at as observedAt from buffered_events").all() as Array<{ observedAt: string }>;
  const postBaselineAt = new Date().toISOString();
  fs.appendFileSync(transcript, line("after-baseline", postBaselineAt, 20));
  for (let pass = 0; pass < 5; pass += 1) {
    await tailer.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
  }
  const afterGrowth = buffer.database.prepare("select observed_at as observedAt from buffered_events").all() as Array<{ observedAt: string }>;
  console.log(JSON.stringify({ startedAt, gapAt, postBaselineAt, baselineStatus: "complete",
    beforeGrowth, afterGrowth, transcriptBytes: fs.statSync(transcript).size }));
  assert.ok(afterGrowth.length > 0, "control: the same tailer captures a later append");
  assert.ok(beforeGrowth.some(row => row.observedAt === gapAt),
    "an event written after replacement-ledger binding and before baseline sealing must be captured");
  assert.equal(beforeGrowth.length, 1, "the archived event is not reread or appended");
  assert.equal(afterGrowth.length, 2, "each post-switch append is stored exactly once");
} finally {
  tailer?.close();
  rollout?.close();
  buffer?.close();
  fs.rmSync(fixture, { recursive: true, force: true });
}
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
