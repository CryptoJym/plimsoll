import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r2-cutover-append-")));
const ledger = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const archivePath = path.join(archiveDirectory, "old-ledger.sqlite");
const codex = path.join(fixture, "codex");
const claude = path.join(fixture, "claude");
const transcript = path.join(claude, "project", "70000000-0000-4000-8000-000000000007.jsonl");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
fs.mkdirSync(codex);
fs.mkdirSync(path.dirname(transcript), { recursive: true });
fs.mkdirSync(archiveDirectory, { mode: 0o700 });
const roots = [
  { source: "codex" as const, rootId: "codex-root", profileId: "codex-profile", directory: codex, installationEpochId: epoch },
  { source: "claude_code" as const, rootId: "claude-root", profileId: "claude-profile", directory: claude, installationEpochId: epoch },
];
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
  installKey: "fixture-install-key", captureRoots: roots });
const line = (id: string, timestamp: string) => JSON.stringify({
  type: "assistant", timestamp,
  message: { id, model: "claude-opus-5", usage: { input_tokens: 10, output_tokens: 0 } },
}) + "\n";
fs.writeFileSync(transcript, line("old", new Date(Date.now() - 86_400_000).toISOString()));
let old: LocalEventBuffer | undefined;
let newBuffer: LocalEventBuffer | undefined;
let oldTailer: TranscriptTailer | undefined;
let rollout: RolloutTailer | undefined;
let tailer: TranscriptTailer | undefined;
async function main() {
  try {
    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
    oldTailer = new TranscriptTailer(old, undefined, undefined, [roots[1]!]);
    await oldTailer.scan({ scope: "full" });
    assert.equal((old.database.prepare("select count(*) as n from rollout_scan_state").get() as {n:number}).n, 1);
    oldTailer.close(); oldTailer = undefined;
    old.close(); old = undefined;
    const steps: Record<string, string> = {};
    switchFreshLedger({ ledgerPath: ledger, archivePath, config,
      authorityRoot: path.join(fixture, "lifecycle-authority"),
      onStep: step => {
        if (!["old_locked", "stage_bound", "archive_linked"].includes(step)) return;
        const at = new Date().toISOString();
        steps[step] = at;
        fs.appendFileSync(transcript, line(step, at));
        if (step === "old_locked") Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30);
      } });
    newBuffer = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    rollout = new RolloutTailer(newBuffer, undefined, () => [], undefined, [roots[0]!]);
    tailer = new TranscriptTailer(newBuffer, undefined, undefined, [roots[1]!]);
    for (let pass = 0; pass < 30 && captureBaselineStatus(newBuffer.database).status !== "complete"; pass++)
      for (const source of [rollout, tailer])
        await source.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
    assert.equal(captureBaselineStatus(newBuffer.database).status, "complete");
    const first = await tailer.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
    const stored = newBuffer.database.prepare("select observed_at as observedAt from buffered_events order by observed_at")
      .all() as Array<{ observedAt: string }>;
    const cutoff = newBuffer.workspaceBinding()!.currentInstallationEpochStartedAt!;
    console.log(JSON.stringify({ steps, cutoff, parsed: first.recordsParsed,
      enrollmentExcludedEvents: first.enrollmentExcludedEvents, stored }));
    assert.equal(first.recordsParsed, 3);
    assert.equal(stored.length, 3, "every append during the switch must survive cursor carry and epoch admission");
  } finally {
    tailer?.close(); rollout?.close(); oldTailer?.close(); newBuffer?.close(); old?.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
