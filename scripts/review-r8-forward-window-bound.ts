import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const sessionId = "70000000-0000-4000-8000-000000000007";

async function main() {
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
    "pr426-r8-window-bound-")));
  const originalReaddir = fs.readdirSync;
  const originalRename = fs.renameSync;
  try {
    const ledgerPath = path.join(fixture, "work-ledger.sqlite");
    const archivePath = path.join(fixture, "archive", "old-ledger.sqlite");
    const root = path.join(fixture, "claude");
    const codexRoot = path.join(fixture, "codex");
    const file = path.join(root, "project", `${sessionId}.jsonl`);
    fs.mkdirSync(root);
    fs.mkdirSync(codexRoot);
    fs.mkdirSync(path.dirname(archivePath), { mode: 0o700 });
    const captureRoot = { source: "claude_code" as const, rootId: "claude-root",
      profileId: "claude-profile", directory: root, installationEpochId: epoch };
    const codexCaptureRoot = { source: "codex" as const, rootId: "codex-root",
      profileId: "codex-profile", directory: codexRoot, installationEpochId: epoch };
    const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
      installKey: "fixture-install-key", captureRoots: [captureRoot, codexCaptureRoot] });
    const old = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
    old.close();
    let finalInventory = false;
    let missedAt: number | null = null;
    let renameStarted: number | null = null;
    fs.readdirSync = ((directory: fs.PathLike, options?: unknown) => {
      const entries = Reflect.apply(originalReaddir, fs, [directory, options]);
      if (finalInventory && missedAt === null && String(directory) === root) {
        // The root's directory entries were already sampled. This path is
        // absent from that final inventory even though inventory work remains.
        missedAt = performance.now();
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, JSON.stringify({ type: "assistant",
          timestamp: new Date(Date.now() + 3_600_000).toISOString(),
          message: { id: "forward-during-inventory", model: "claude-opus-5",
            usage: { input_tokens: 10, output_tokens: 0 } } }) + "\n");
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 200);
      }
      return entries;
    }) as typeof fs.readdirSync;
    fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
      if (String(source) === `${ledgerPath}.replacement-stage` && String(destination) === ledgerPath) {
        renameStarted = performance.now();
      }
      return originalRename(source, destination);
    }) as typeof fs.renameSync;
    switchFreshLedger({ ledgerPath, archivePath, config,
      authorityRoot: path.join(fixture, "lifecycle-authority"),
      onStep(step) { if (step === "archive_linked") finalInventory = true; } });
    fs.readdirSync = originalReaddir;
    fs.renameSync = originalRename;
    assert.ok(missedAt !== null && renameStarted !== null);
    const marker = readReplacementLedgerMarker(ledgerPath)!;
    const fresh = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    const rollout = new RolloutTailer(fresh, undefined, () => [], undefined, [codexCaptureRoot]);
    const tailer = new TranscriptTailer(fresh, undefined, undefined, [captureRoot]);
    try {
      const boundaryRows = (fresh.database.prepare("select count(*) as n from replacement_file_boundaries")
        .get() as { n: number }).n;
      for (let pass = 0; pass < 30 && captureBaselineStatus(fresh.database).status !== "complete"; pass++) {
        await rollout.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
        await tailer.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
      }
      for (let pass = 0; pass < 12; pass++) {
        await tailer.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
      }
      const eventId = deterministicEventId(["claude-transcript", sessionId, "forward-during-inventory"]);
      const freshCount = (fresh.database.prepare("select count(*) as n from buffered_events where id=?")
        .get(eventId) as { n: number }).n;
      const archive = new Database(archivePath, { readonly: true, fileMustExist: true });
      const archiveCount = (archive.prepare("select count(*) as n from buffered_events where id=?")
        .get(eventId) as { n: number }).n;
      archive.close();
      const observedGapMs = renameStarted - missedAt;
      console.log(JSON.stringify({ boundaryRows, freshCount, archiveCount, observedGapMs,
        reportedInventoryToRenameDelayMs: marker.inventoryToRenameDelayMs,
        renameToSampleDelayMs: marker.renameToSampleDelayMs }));
      assert.equal(boundaryRows, 0);
      assert.equal(freshCount, 1);
      assert.equal(archiveCount, 0);
      assert.ok(marker.inventoryToRenameDelayMs !== null &&
        marker.inventoryToRenameDelayMs >= observedGapMs,
      "the published forward window must bound a path missed during final inventory");
    } finally { tailer.close(); rollout.close(); fresh.close(); }
  } finally {
    fs.readdirSync = originalReaddir;
    fs.renameSync = originalRename;
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
