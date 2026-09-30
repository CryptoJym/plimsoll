import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { readReplacementLedgerMarker, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const sessionId = "70000000-0000-4000-8000-000000000007";

function line(id: string, timestamp?: Date): string {
  return JSON.stringify({ type: "assistant", ...(timestamp ? { timestamp: timestamp.toISOString() } : {}),
    message: { id, model: "claude-opus-5", usage: { input_tokens: 10, output_tokens: 0 } } }) + "\n";
}

function roots(fixture: string) {
  return [
    { source: "codex" as const, rootId: "codex-root", profileId: "codex-profile",
      directory: path.join(fixture, "codex"), installationEpochId: epoch },
    { source: "claude_code" as const, rootId: "claude-root", profileId: "claude-profile",
      directory: path.join(fixture, "claude"), installationEpochId: epoch },
  ];
}

async function child(fixture: string, variant: string) {
  const ledger = path.join(fixture, "work-ledger.sqlite");
  const file = path.join(fixture, "claude", "project", `${sessionId}.jsonl`);
  const buffer = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  const configured = roots(fixture);
  const rollout = new RolloutTailer(buffer, undefined, () => [], undefined, [configured[0]!]);
  const transcript = new TranscriptTailer(buffer, undefined, undefined, [configured[1]!]);
  try {
    const boundaryRows = (buffer.database.prepare("select count(*) as n from replacement_file_boundaries")
      .get() as { n: number }).n;
    assert.equal(boundaryRows, 0, "the file must be absent from the pre-rename inventory");
    for (let pass = 0; pass < 30 && captureBaselineStatus(buffer.database).status !== "complete"; pass++) {
      for (const tailer of [rollout, transcript]) {
        await tailer.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
      }
    }
    assert.equal(captureBaselineStatus(buffer.database).status, "complete");
    let excluded = 0;
    for (let pass = 0; pass < 12; pass++) {
      const scan = await transcript.scan({ scope: "recent",
        automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
      excluded += scan.enrollmentExcludedEvents ?? 0;
    }
    const early = buffer.database.prepare("select payload_json from buffered_events").all() as
      Array<{ payload_json: string }>;
    assert.equal(early.length, 1, "the new session's first post-switch record must survive baseline");
    assert.equal(excluded, 1, `${variant}: one historical or unstamped record must be counted`);

    const marker = readReplacementLedgerMarker(ledger)!;
    const later = new Date(Math.max(Date.now(), Date.parse(marker.switchedAt) + 1));
    fs.appendFileSync(file, line("post-switch-late", later));
    for (let pass = 0; pass < 12; pass++) {
      await transcript.scan({ scope: "recent",
        automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
    }
    const rows = buffer.database.prepare("select payload_json from buffered_events").all() as
      Array<{ payload_json: string }>;
    assert.equal(rows.length, 2, "the ordinary cursor must admit later growth once");
    assert.deepEqual(rows.map(row => (JSON.parse(row.payload_json) as {id:string}).id).sort(),
      ["post-switch-early", "post-switch-late"].map(id =>
        deterministicEventId(["claude-transcript", sessionId, id])).sort());
    console.log(JSON.stringify({ variant, boundaryRows, excluded, earlyRows: early.length,
      stored: rows.length, cutoverAt: marker.switchedAt }));
  } finally {
    transcript.close(); rollout.close(); buffer.close();
  }
}

function parent(variant: string) {
  assert.ok(variant === "restart" || variant === "untimestamped");
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
    `pr426-r10-${variant}-`)));
  try {
    const configured = roots(fixture);
    fs.mkdirSync(configured[0]!.directory);
    fs.mkdirSync(configured[1]!.directory);
    const archiveDirectory = path.join(fixture, "archive");
    fs.mkdirSync(archiveDirectory, { mode: 0o700 });
    const ledgerPath = path.join(fixture, "work-ledger.sqlite");
    const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
      installKey: "fixture-install-key", captureRoots: configured });
    const old = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
    old.close();
    switchFreshLedger({ ledgerPath, archivePath: path.join(archiveDirectory, "old-ledger.sqlite"),
      config, authorityRoot: path.join(fixture, "lifecycle-authority") });
    const marker = readReplacementLedgerMarker(ledgerPath)!;
    const file = path.join(configured[1]!.directory, "project", `${sessionId}.jsonl`);
    fs.mkdirSync(path.dirname(file));
    const excluded = variant === "untimestamped"
      ? line("no-timestamp") : line("before-cutover", new Date(Date.parse(marker.switchedAt) - 3_600_000));
    const postSwitch = new Date(Math.max(Date.now(), Date.parse(marker.switchedAt) + 1));
    fs.writeFileSync(file, excluded + line("post-switch-early", postSwitch));
    // The child is a fresh collector process. Its first operation opens the
    // replacement ledger; no in-memory cutover state crosses this boundary.
    const run = spawnSync(process.execPath, ["--import", "tsx", import.meta.filename,
      "--child", fixture, variant], { cwd: process.cwd(), env: process.env, encoding: "utf8",
      timeout: 120_000 });
    process.stdout.write(run.stdout ?? "");
    process.stderr.write(run.stderr ?? "");
    assert.equal(run.status, 0, `fresh collector process failed: ${run.error ?? run.stderr}`);
  } finally {
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

if (process.argv[2] === "--child") {
  void child(process.argv[3]!, process.argv[4]!).catch(error => {
    console.error(error); process.exitCode = 1;
  });
} else {
  try { parent(process.argv[2]!); } catch (error) { console.error(error); process.exitCode = 1; }
}
