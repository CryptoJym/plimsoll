import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const sessionId = "70000000-0000-4000-8000-000000000007";
const line = (id: string, at: Date) => JSON.stringify({ type: "assistant", timestamp: at.toISOString(),
  message: { id, model: "claude-opus-5", usage: { input_tokens: 10, output_tokens: 0 } } }) + "\n";

async function main() {
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
    "pr426-r11-exactly-once-")));
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
      installKey: "fixture-install-key", captureRoots: [codexCaptureRoot, captureRoot] });
    const priorId = "83000000-0000-4000-8000-000000000008";
    const old = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
    assert.equal(old.append(aiInteractionEventSchema.parse({ id: priorId, tenantId: workspace,
      source: "claude_code", dataMode: "metadata", eventType: "assistant_response",
      observedAt: new Date(Date.now() - 86_400_000).toISOString(),
      sessionId: "71000000-0000-4000-8000-000000000007",
      inputTokens: 1, outputTokens: 0,
      metadata: { installationEpochId: epoch, sourceEventId: "archive-control" } })), true);
    old.close();

    const originalRename = fs.renameSync;
    let gapCreated = false;
    let oldWriterBlocked = false;
    fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
      if (String(source) === `${ledgerPath}.replacement-stage` && String(destination) === ledgerPath) {
        // This callback runs after final inventory and before the active rename.
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, line("forward-gap", new Date(Date.now() + 3_600_000)));
        gapCreated = true;
        let competitor: Database.Database | null = null;
        try {
          competitor = new Database(ledgerPath, { fileMustExist: true, timeout: 0 });
          competitor.exec("BEGIN IMMEDIATE");
          competitor.exec("ROLLBACK");
        } catch (error) {
          assert.equal((error as { code?: string }).code, "SQLITE_BUSY");
          oldWriterBlocked = true;
        } finally { competitor?.close(); }
      }
      return originalRename(source, destination);
    }) as typeof fs.renameSync;
    try {
      switchFreshLedger({ ledgerPath, archivePath, config,
        authorityRoot: path.join(fixture, "lifecycle-authority") });
    } finally { fs.renameSync = originalRename; }
    assert.equal(gapCreated, true);
    assert.equal(oldWriterBlocked, true, "the archive inode must reject a competing writer in the gap");
    const marker = readReplacementLedgerMarker(ledgerPath)!;
    if (process.argv.includes("--require-window")) {
      assert.ok(marker.inventoryToRenameDelayMs !== null && marker.inventoryToRenameDelayMs >= 0,
        "the switch records a conservative upper bound on the forward gap");
    }
    fs.appendFileSync(file, line("post-cutover", new Date(Date.parse(marker.switchedAt) + 1)));

    const fresh = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    const rollout = new RolloutTailer(fresh, undefined, () => [], undefined, [codexCaptureRoot]);
    const tailer = new TranscriptTailer(fresh, undefined, undefined, [captureRoot]);
    try {
      const boundaryRows = (fresh.database.prepare("select count(*) as n from replacement_file_boundaries")
        .get() as { n: number }).n;
      assert.equal(boundaryRows, 0, "the gap path is absent from final inventory");
      for (let pass = 0; pass < 30 && captureBaselineStatus(fresh.database).status !== "complete"; pass++) {
        await rollout.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
        await tailer.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
      }
      assert.equal(captureBaselineStatus(fresh.database).status, "complete");
      for (let pass = 0; pass < 12; pass++) {
        await tailer.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
      }
      const archived = new Database(archivePath, { readonly: true, fileMustExist: true });
      const archiveIds = (archived.prepare("select id from buffered_events").all() as Array<{ id: string }>)
        .map(row => row.id).sort();
      archived.close();
      const freshIds = (fresh.database.prepare("select id from buffered_events").all() as Array<{ id: string }>)
        .map(row => row.id).sort();
      const gapId = deterministicEventId(["claude-transcript", sessionId, "forward-gap"]);
      const postId = deterministicEventId(["claude-transcript", sessionId, "post-cutover"]);
      assert.deepEqual(archiveIds, [priorId]);
      assert.deepEqual(freshIds, [gapId, postId].sort());
      assert.equal(new Set([...archiveIds, ...freshIds]).size, 3,
        "each archive, forward-skew and post-cutover event occurs in exactly one ledger");
      console.log(JSON.stringify({ oldWriterBlocked, boundaryRows, cutoverAt: marker.switchedAt,
        renameToSampleDelayMs: marker.renameToSampleDelayMs,
        inventoryToRenameDelayMs: marker.inventoryToRenameDelayMs, archiveIds, freshIds,
        forwardSkew: { archive: archiveIds.filter(id => id === gapId).length,
          fresh: freshIds.filter(id => id === gapId).length },
        postCutover: { archive: archiveIds.filter(id => id === postId).length,
          fresh: freshIds.filter(id => id === postId).length } }));
    } finally { tailer.close(); rollout.close(); fresh.close(); }
  } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
