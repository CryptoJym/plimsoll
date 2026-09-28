import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer as NewBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { switchFreshLedger, assertReplacementRuntimeCompatible } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { TranscriptTailer as NewTranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r2-min-version-")));
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
let initial: NewBuffer | undefined;
let initialTailer: NewTranscriptTailer | undefined;
async function main() {
  try {
    initial = new NewBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 2 * 86_400_000) });
    initialTailer = new NewTranscriptTailer(initial, undefined, undefined, [roots[1]!]);
    await initialTailer.scan({ scope: "full" });
    assert.equal((initial.database.prepare("select count(*) as n from rollout_scan_state").get() as { n: number }).n, 1);
    initialTailer.close(); initialTailer = undefined;
    initial.close(); initial = undefined;
    switchFreshLedger({ ledgerPath: ledger, archivePath, config,
      authorityRoot: path.join(fixture, "lifecycle-authority") });
    assert.throws(() => assertReplacementRuntimeCompatible(ledger, "0.7.45"),
      /replacement_ledger_requires_archive_restore_before_downgrade/);
    assert.doesNotThrow(() => assertReplacementRuntimeCompatible(ledger, "0.7.46"));
    console.log(JSON.stringify({ markerMinimum: "0.7.46", version045Refused: true }));
  } finally {
    initialTailer?.close(); initial?.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
