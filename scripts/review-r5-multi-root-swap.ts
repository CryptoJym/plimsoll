import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
  "r5-multi-root-swap-")));
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const ledger = path.join(fixture, "work-ledger.sqlite");
const archive = path.join(fixture, "archive", "old-ledger.sqlite");
const codex = path.join(fixture, "codex");
const line = (id: string) => JSON.stringify({ type: "assistant", timestamp: new Date().toISOString(),
  message: { id, model: "claude-opus-5", usage: { input_tokens: 10, output_tokens: 0 } } }) + "\n";
let old: LocalEventBuffer | undefined;
let fresh: LocalEventBuffer | undefined;
let tailer: TranscriptTailer | undefined;
let rollout: RolloutTailer | undefined;
async function main() {
  try {
    fs.mkdirSync(codex);
    fs.mkdirSync(path.dirname(archive), { mode: 0o700 });
    const roots = Array.from({ length: 22 }, (_, i) => {
      const directory = path.join(fixture, `claude-${i}`);
      fs.mkdirSync(path.join(directory, "project"), { recursive: true });
      return { source: "claude_code" as const, directory, installationEpochId: epoch,
        rootId: `50000000-0000-4000-8000-${String(i).padStart(12, "0")}`,
        profileId: `60000000-0000-4000-8000-${String(i).padStart(12, "0")}` };
    });
    const files = roots.map((root, i) => path.join(root.directory, "project",
      `70000000-0000-4000-8000-${String(i).padStart(12, "0")}.jsonl`));
    for (let i = 0; i < files.length; i++) fs.writeFileSync(files[i]!, line(`archive-${i}`));
    const codexRoot = { source: "codex" as const, directory: codex,
      rootId: "codex-root", profileId: "codex-profile", installationEpochId: epoch };
    const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
      installKey: "fixture-install-key", captureRoots: [codexRoot, ...roots] });
    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
    tailer = new TranscriptTailer(old, undefined, undefined, roots);
    await tailer.scan({ scope: "full" });
    const cursorCount = (old.database.prepare("select count(*) as n from rollout_scan_state")
      .get() as { n: number }).n;
    assert.equal(cursorCount, 22);
    tailer.close(); tailer = undefined; old.close(); old = undefined;
    const originalRename = fs.renameSync;
    let injected = false;
    fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
      if (String(source) === `${ledger}.replacement-stage` && String(destination) === ledger) {
        for (let i = 0; i < files.length; i++) {
          const next = `${files[i]}.new`;
          fs.writeFileSync(next, line(`new-${i}`));
          originalRename(next, files[i]!);
        }
        injected = true;
      }
      return originalRename(source, destination);
    }) as typeof fs.renameSync;
    try { switchFreshLedger({ ledgerPath: ledger, archivePath: archive, config,
      authorityRoot: path.join(fixture, "lifecycle-authority") }); }
    finally { fs.renameSync = originalRename; }
    assert.equal(injected, true);
    for (let i = 0; i < files.length; i++) fs.appendFileSync(files[i]!, line(`early-${i}`));
    fresh = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    rollout = new RolloutTailer(fresh, undefined, () => [], undefined, [codexRoot]);
    tailer = new TranscriptTailer(fresh, undefined, undefined, roots);
    for (let pass = 0; pass < 60 && captureBaselineStatus(fresh.database).status !== "complete"; pass++) {
      for (const source of [rollout, tailer]) await source.scan({ scope: "recent",
        automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
    }
    assert.equal(captureBaselineStatus(fresh.database).status, "complete");
    for (let pass = 0; pass < 16; pass++) await tailer.scan({ scope: "recent",
      automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
    const early = (fresh.database.prepare("select count(distinct session_id) as n from buffered_events")
      .get() as { n: number }).n;
    const carried = (fresh.database.prepare("select count(*) as n from replacement_capture_cursors")
      .get() as { n: number }).n;
    const fenced = (fresh.database.prepare("select count(*) as n from replacement_unseen_file_fences")
      .get() as { n: number }).n;
    console.log(JSON.stringify({ roots: 23, replaced: 22, oldCursors: cursorCount,
      carried, fenced, earlySessions: early }));
    assert.equal(carried, 0);
    assert.equal(fenced, 22);
    assert.equal(early, 22, "every root must admit its first post-switch record");
  } finally {
    tailer?.close(); rollout?.close(); old?.close(); fresh?.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
