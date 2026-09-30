import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const RealDate = Date;
const shift = -86_400_000;
class ShiftedDate extends RealDate {
  constructor(...args: unknown[]) {
    if (!args.length) super(RealDate.now() + shift);
    else super(...(args as [string | number | Date]));
  }
  static now() { return RealDate.now() + shift; }
}
const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r2-unseen-skew-")));
const ledger = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const archivePath = path.join(archiveDirectory, "old-ledger.sqlite");
const codex = path.join(fixture, "codex");
const claude = path.join(fixture, "claude");
const file = path.join(claude, "project", "70000000-0000-4000-8000-000000000007.jsonl");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
fs.mkdirSync(codex);
fs.mkdirSync(claude);
fs.mkdirSync(archiveDirectory, { mode: 0o700 });
const roots = [
  { source: "codex" as const, rootId: "codex-root", profileId: "codex-profile", directory: codex, installationEpochId: epoch },
  { source: "claude_code" as const, rootId: "claude-root", profileId: "claude-profile", directory: claude, installationEpochId: epoch },
];
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
  installKey: "fixture-install-key", captureRoots: roots });
let old: LocalEventBuffer | undefined;
let replacement: LocalEventBuffer | undefined;
let rollout: RolloutTailer | undefined;
let tailer: TranscriptTailer | undefined;
async function main() {
  try {
    const virtualNow = RealDate.now() + shift;
    globalThis.Date = ShiftedDate as DateConstructor;
    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    console.log(JSON.stringify({ bindingBefore: old.workspaceBinding(),
      maintenanceLatest: old.database.prepare("select max(updated_at) as at from maintenance_state").get() }));
    old.close(); old = undefined;
    globalThis.Date = RealDate;
    fs.mkdirSync(path.dirname(file), { recursive: true });
    // The file is physically new under the old clock, but contains a record
    // within the regressed clock's admissible window.
    const historicalAt = new RealDate(virtualNow + 120_000).toISOString();
    fs.writeFileSync(file, JSON.stringify({ type: "assistant", timestamp: historicalAt,
      message: { id: "preexisting-unseen", model: "claude-opus-5",
        usage: { input_tokens: 10, output_tokens: 0 } } }) + "\n");
    const birthtime = fs.statSync(file).birthtime.toISOString();
    globalThis.Date = ShiftedDate as DateConstructor;
    const input = { ledgerPath: ledger, archivePath, config,
      now: () => new RealDate(virtualNow + 60_000), authorityRoot: path.join(fixture, "lifecycle-authority") };
    const plan = planFreshLedgerCutover(input);
    if (plan.status === "ready") switchFreshLedger(input);
    replacement = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    rollout = new RolloutTailer(replacement, undefined, () => [], undefined, [roots[0]!]);
    tailer = new TranscriptTailer(replacement, undefined, undefined, [roots[1]!]);
    for (let pass = 0; pass < 30 && captureBaselineStatus(replacement.database).status !== "complete"; pass++) {
      for (const source of [rollout, tailer])
        await source.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
    }
    assert.equal(captureBaselineStatus(replacement.database).status, "complete");
    const captures = [];
    for (let pass = 0; pass < 5; pass++)
      captures.push(await tailer.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } }));
    const count = (replacement.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
    console.log(JSON.stringify({ virtualNow: new RealDate(virtualNow + 60_000).toISOString(), birthtime,
      historicalAt, archiveLatest: plan.archiveLatestRecordedAt, planStatus: plan.status,
      planReason: plan.reason, carriedCursors: plan.cursorRows, preexistingFileIngested: count,
      baseline: captureBaselineStatus(replacement.database), captures: captures.map(c => ({
        recordsParsed: c.recordsParsed, eventsAppended: c.eventsAppended,
        filesSeen: c.filesSeen, filesSkippedOutsideRecentWindow: c.filesSkippedOutsideRecentWindow,
        statErrors: c.statErrors, excludedGenerations: c.excludedGenerations,
      })) }));
    assert.equal(count, 0, "a file existing before cutover must not become new solely because the clock regressed");
  } finally {
    globalThis.Date = RealDate;
    tailer?.close(); rollout?.close(); replacement?.close(); old?.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
