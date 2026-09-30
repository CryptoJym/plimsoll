import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { DEFAULT_JSONL_TAILER_IO, type JsonlTailerIo } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const variant: string = process.argv[2] ?? "race-replacement";
const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
  `pr426-r7-${variant}-`)));
const ledgerPath = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const codex = path.join(fixture, "codex");
const claude = path.join(fixture, "claude");
const file = path.join(claude, "project", "70000000-0000-4000-8000-000000000007.jsonl");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const line = (id: string) => JSON.stringify({ type: "assistant", timestamp: new Date().toISOString(),
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
      fs.writeFileSync(next, line("before-fence"));
      fs.renameSync(next, file);
      assert.notEqual(fs.statSync(file).ino, oldInode);
    };
    const input = { ledgerPath, archivePath: path.join(archiveDirectory, "old-ledger.sqlite"),
      config, authorityRoot: path.join(fixture, "lifecycle-authority") };
    const originalRename = fs.renameSync;
    let injected = false;
    fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
      if (String(source) === `${ledgerPath}.replacement-stage` && String(destination) === ledgerPath) {
        const next = `${file}.writer-new`;
        fs.writeFileSync(next, line("pre-swap-replacement"));
        originalRename(next, file);
        injected = true;
      }
      return originalRename(source, destination);
    }) as typeof fs.renameSync;
    try { switchFreshLedger(input); } finally { fs.renameSync = originalRename; }
    assert.equal(injected, true);
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
    assert.equal(earlyRows, 1, "a post-switch append before first scan must be captured");
    assert.equal(rows.length, 2,
      `${variant}: first-scan baseline fences existing bytes and admits only later growth`);
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
