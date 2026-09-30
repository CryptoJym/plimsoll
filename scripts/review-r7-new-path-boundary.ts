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

const variant = process.argv[2] ?? "pre_rename_missed";
assert.ok(["pre_rename_missed", "during_rename", "future_skew", "future_skew_only", "stamp_boundary", "backward_skew", "hook_first"].includes(variant));
const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr426-r7-boundary-")));
const ledgerPath = path.join(fixture, "work-ledger.sqlite");
const archivePath = path.join(fixture, "archive", "old-ledger.sqlite");
const codex = path.join(fixture, "codex");
const claude = path.join(fixture, "claude");
const sessionId = "70000000-0000-4000-8000-000000000007";
const file = path.join(claude, "project", `${sessionId}.jsonl`);
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const line = (id: string, at: Date) => JSON.stringify({ type: "assistant", timestamp: at.toISOString(),
  message: { id, model: "claude-opus-5", usage: { input_tokens: 10, output_tokens: 0 } } }) + "\n";

async function main() {
  fs.mkdirSync(codex);
  fs.mkdirSync(claude);
  fs.mkdirSync(path.dirname(archivePath), { mode: 0o700 });
  const roots = [
    { source: "codex" as const, rootId: "codex-root", profileId: "codex-profile", directory: codex, installationEpochId: epoch },
    { source: "claude_code" as const, rootId: "claude-root", profileId: "claude-profile", directory: claude, installationEpochId: epoch },
  ];
  const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
    installKey: "fixture-install-key", captureRoots: roots });
  const old = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
  old.close();

  const originalRename = fs.renameSync;
  let injected = false;
  let physicalPreRename = false;
  fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
    if (String(source) === `${ledgerPath}.replacement-stage` && String(destination) === ledgerPath) {
      if (variant !== "during_rename") {
        physicalPreRename = true;
        const at = variant.startsWith("future_skew") ? new Date(Date.now() + 3_600_000) : new Date(Date.now() - 3_600_000);
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, line("old-before-rename", at));
        injected = true;
      }
      const result = originalRename(source, destination);
      if (variant === "during_rename") {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, line("during-rename-before-sample", new Date()));
        injected = true;
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 20);
      }
      return result;
    }
    return originalRename(source, destination);
  }) as typeof fs.renameSync;
  try {
    switchFreshLedger({ ledgerPath, archivePath, config,
      authorityRoot: path.join(fixture, "lifecycle-authority") });
  } finally { fs.renameSync = originalRename; }
  assert.equal(injected, true);
  const marker = readReplacementLedgerMarker(ledgerPath)!;
  const archive = new Database(archivePath, { readonly: true, fileMustExist: true });
  const archivedIds = (archive.prepare("select id from buffered_events where source='claude_code'")
    .all() as Array<{ id: string }>).map(row => row.id);
  archive.close();
  const postAt = variant === "stamp_boundary" ? new Date(marker.switchedAt)
    : variant === "backward_skew" ? new Date(Date.parse(marker.switchedAt) - 1)
    : new Date(Math.max(Date.now(), Date.parse(marker.switchedAt) + 1));
  if (variant !== "future_skew_only") fs.appendFileSync(file, line("written-after-cutover", postAt));

  const buffer = new LocalEventBuffer(ledgerPath, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  if (variant === "hook_first") {
    const hook = aiInteractionEventSchema.parse({ id: "83000000-0000-4000-8000-000000000008",
      tenantId: workspace, source: "codex", dataMode: "metadata", eventType: "assistant_response",
      observedAt: new Date(Math.max(Date.now(), Date.parse(marker.switchedAt) + 1)).toISOString(),
      sessionId: "84000000-0000-4000-8000-000000000009", inputTokens: 1, outputTokens: 0,
      metadata: { installationEpochId: epoch, sourceEventId: "fixture-hook-before-baseline" } });
    assert.equal(buffer.append(hook), true, "hook intake precedes the first baseline");
  }
  const rollout = new RolloutTailer(buffer, undefined, () => [], undefined, [roots[0]!]);
  const transcript = new TranscriptTailer(buffer, undefined, undefined, [roots[1]!]);
  try {
    const boundaryRows = (buffer.database.prepare("select count(*) as n from replacement_file_boundaries")
      .get() as { n: number }).n;
    assert.equal(boundaryRows, 0, "the first file must be absent from the final inventory");
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
    const rows = buffer.database.prepare("select payload_json from buffered_events where source='claude_code'").all() as Array<{ payload_json: string }>;
    const ids = rows.map(row => (JSON.parse(row.payload_json) as { id: string }).id).sort();
    const oldId = deterministicEventId(["claude-transcript", sessionId, "old-before-rename"]);
    const postId = deterministicEventId(["claude-transcript", sessionId, "written-after-cutover"]);
    console.log(JSON.stringify({ variant, physicalPreRename, boundaryRows, cutoverAt: marker.switchedAt,
      postAt: postAt.toISOString(), excluded, archivedIds, ids, oldId, postId }));
    assert.deepEqual(archivedIds, [], "the stopped collector cannot capture the newly created path into the archive");
    if (variant === "future_skew_only") assert.deepEqual(ids, [oldId],
      "the forward-stamped record is admitted exactly once, in the fresh ledger");
    else if (variant === "future_skew") assert.deepEqual(ids, [postId, oldId].sort(),
      "forward-stamped and genuine post-cutover records are each admitted once");
    else if (variant === "backward_skew") assert.equal(ids.length, 0, "the documented backward-skew window excludes the post-cutover record");
    else assert.deepEqual(ids, [postId], "a pre-cutover record must stay excluded and the post-cutover record must survive");
    if (variant !== "future_skew_only") assert.equal(excluded,
      variant === "backward_skew" ? 2 : variant === "future_skew" ? 0 : 1);
  } finally { transcript.close(); rollout.close(); buffer.close(); }
}

void main().catch(error => { console.error(error); process.exitCode = 1; })
  .finally(() => { fs.rmSync(fixture, { recursive: true, force: true }); });
