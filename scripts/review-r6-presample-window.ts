import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { readReplacementLedgerMarker, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { DEFAULT_JSONL_TAILER_IO, type JsonlTailerIo } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const variant: string = process.argv[2] ?? "rename_before";
const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
  `pr426-r5-timing-${variant}-`)));
const ledgerPath = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const codex = path.join(fixture, "codex");
const claude = path.join(fixture, "claude");
const file = path.join(claude, "project", "70000000-0000-4000-8000-000000000007.jsonl");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const line = (id: string, timestamp = new Date()) => JSON.stringify({ type: "assistant", timestamp: timestamp.toISOString(),
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
let staleGenerationCursorPassed = false;
async function main() {
  try {
    fs.writeFileSync(file, line("archive-generation"));
    buffer = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
    const io: JsonlTailerIo = { ...DEFAULT_JSONL_TAILER_IO,
      readTail: (candidate, stat, cursor, limits) => {
        const current = fs.lstatSync(candidate, { bigint: true });
        const identity = `${current.dev}:${current.ino}:${current.birthtimeNs}`;
        if (cursor?.fileIdentity && cursor.fileIdentity !== identity) staleGenerationCursorPassed = true;
        return DEFAULT_JSONL_TAILER_IO.readTail(candidate, stat, cursor, limits);
      } };
    transcript = new TranscriptTailer(buffer, undefined, io, [roots[1]!]);
    await transcript.scan({ scope: "full" });
    assert.equal((buffer.database.prepare("select count(*) as n from rollout_scan_state")
      .get() as { n: number }).n, 1);
    transcript.close(); transcript = undefined;
    buffer.close(); buffer = undefined;
    const oldInode = fs.statSync(file).ino;
    const replace = () => {
      const next = `${file}.new`;
      // Historical content in a replacement generation must not be admitted
      // merely because the replacement happens after the ledger rename.
      fs.writeFileSync(next, line("before-fence", new Date(Date.now() - 3_600_000)));
      fs.renameSync(next, file);
      assert.notEqual(fs.statSync(file).ino, oldInode);
    };
    const input = { ledgerPath, archivePath: path.join(archiveDirectory, "old-ledger.sqlite"),
      config, authorityRoot: path.join(fixture, "lifecycle-authority") };
    const timing = process.argv[2] ?? "rename_before";
    assert.ok(["stage_bound", "archive_linked", "final_stat", "rename_before",
      "rename_after", "post_stat", "fsync_after"].includes(timing));
    const originalRename = fs.renameSync;
    const originalLstat = fs.lstatSync;
    const originalFsync = fs.fsyncSync;
    let injected = false;
    let fsyncRecord = false;
    let armedFinalStat = false;
    let renamed = false;
    let preSampleAt: string | null = null;
    const inject = () => { replace(); injected = true; };
    fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
      if (String(source) === `${ledgerPath}.replacement-stage` && String(destination) === ledgerPath) {
        if (timing === "rename_before") inject();
        const result = originalRename(source, destination);
        renamed = true;
        if (timing === "rename_after" || timing === "fsync_after") inject();
        if (timing === "rename_after") {
          const stamp = new Date();
          preSampleAt = stamp.toISOString();
          fs.appendFileSync(file, line("post-rename-pre-sample", stamp));
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 35);
        }
        return result;
      }
      return originalRename(source, destination);
    }) as typeof fs.renameSync;
    (fs as any).lstatSync = ((candidate: fs.PathLike, options?: any) => {
      const stat = originalLstat(candidate, options);
      if (String(candidate) === file && !injected &&
          ((timing === "final_stat" && armedFinalStat) ||
           (timing === "post_stat" && renamed))) inject();
      return stat;
    }) as typeof fs.lstatSync;
    fs.fsyncSync = ((fd: number) => {
      if (timing === "fsync_after" && renamed && injected && !fsyncRecord &&
          fs.fstatSync(fd).isDirectory()) {
        fsyncRecord = true;
        fs.appendFileSync(file, line("during-fsync"));
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30);
      }
      return originalFsync(fd);
    }) as typeof fs.fsyncSync;
    let switchReceipt: ReturnType<typeof switchFreshLedger> | undefined;
    try { switchReceipt = switchFreshLedger({ ...input, onStep: step => {
      if (step === "stage_bound" && timing === "stage_bound") inject();
      if (step === "archive_linked") {
        if (timing === "archive_linked") inject();
        if (timing === "final_stat") armedFinalStat = true;
      }
    } }); }
    finally { fs.renameSync = originalRename; (fs as any).lstatSync = originalLstat;
      fs.fsyncSync = originalFsync; }
    assert.equal(injected, true);
    const marker = readReplacementLedgerMarker(ledgerPath)!;
    const cutoverAt = marker.switchedAt;
    assert.ok(preSampleAt && Date.parse(preSampleAt) < Date.parse(cutoverAt),
      "the writer's post-rename record precedes the sampled cutover without clock skew");
    const gapMs = Date.parse(cutoverAt) - Date.parse(preSampleAt);
    const measuredDelayMs = switchReceipt?.renameToSampleDelayMs ?? -1;
    assert.ok(Number.isFinite(measuredDelayMs) && measuredDelayMs >= gapMs - 5,
      "the cutover receipt must report an upper bound including rename scheduling delay");
    assert.equal(marker.renameToSampleDelayMs, measuredDelayMs,
      "the measured delay must survive a fresh read of the durable marker");
    console.log(JSON.stringify({ preSampleAt, cutoverAt, gapMs,
      renameToSampleDelayMs: measuredDelayMs }));
    if (timing === "fsync_after") assert.equal(fsyncRecord, true);
    fs.appendFileSync(file, line("post-swap-early"));
    if (variant === "replace-after-rename") replace();
    if (variant === "append-before-first-scan") fs.appendFileSync(file, line("after-fence"));
    buffer = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    rollout = new RolloutTailer(buffer, undefined, () => [], undefined, [roots[0]!]);
    transcript = new TranscriptTailer(buffer, undefined, io, [roots[1]!]);
    for (let pass = 0; pass < 30 && captureBaselineStatus(buffer.database).status !== "complete"; pass++) {
      for (const tailer of [rollout, transcript])
        await tailer.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
    }
    assert.equal(captureBaselineStatus(buffer.database).status, "complete");
    for (let pass = 0; pass < 8; pass++)
      await transcript.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
    const earlyRows = (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
    console.log(JSON.stringify({ injected, earlyRows, fileSize: fs.statSync(file).size,
      cursor: buffer.database.prepare("select file_identity,committed_offset,size from rollout_scan_state").all(),
      carried: buffer.database.prepare("select file_identity,committed_offset from replacement_capture_cursors").all(),
      fences: buffer.database.prepare("select baseline_size from replacement_unseen_file_fences").all(),
      baseline: buffer.database.prepare("select baseline_size from automatic_capture_baseline_generations").all() }));
    fs.appendFileSync(file, line("post-swap-late"));
    for (let pass = 0; pass < 8; pass++)
      await transcript.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
    const rows = buffer.database.prepare("select payload_json from buffered_events").all() as
      Array<{ payload_json: string }>;
    console.log(JSON.stringify({ variant, injected, earlyRows, stored: rows.length, staleGenerationCursorPassed,
      fileSize: fs.statSync(file).size, rows: rows.map(row => row.payload_json) }));
    assert.equal(earlyRows, timing === "fsync_after" ? 2 : 1,
      "all records written after rename and stamped after cutover must be captured");
    assert.equal(rows.length, timing === "fsync_after" ? 3 : 2,
      `${variant}: historical replacement records are excluded and later records admitted`);
    const capturedIds = rows.map(row => (JSON.parse(row.payload_json) as {id:string}).id).sort();
    const expectedIds = [...(timing === "fsync_after" ? ["during-fsync"] : []),
      "post-swap-early", "post-swap-late"].map(id =>
      deterministicEventId(["claude-transcript", path.basename(file, ".jsonl"), id])).sort();
    assert.deepEqual(capturedIds, expectedIds,
      "only the two post-switch records may be stored, never the old or pre-switch records");
    if (variant === "replace-after-rename") {
      assert.equal(staleGenerationCursorPassed, false,
        "a carried cursor from the old generation must never be applied to the post-rename file");
    }
  } finally {
    transcript?.close(); rollout?.close(); buffer?.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
