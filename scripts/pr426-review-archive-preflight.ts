import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

async function main() {
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
    "pr426-archive-preflight-")));
  const home = path.join(fixture, "home");
  const plimsoll = path.join(home, ".plimsoll");
  const root = path.join(home, "claude");
  const archiveDir = path.join(fixture, "archive");
  const ledger = path.join(plimsoll, "work-ledger.sqlite");
  const archivePath = path.join(archiveDir, "old-ledger.sqlite");
  const epoch = "10000000-0000-4000-8000-000000000001";
  const workspace = "30000000-0000-4000-8000-000000000003";
  const device = "40000000-0000-4000-8000-000000000004";
  fs.mkdirSync(plimsoll, { recursive: true, mode: 0o700 });
  fs.mkdirSync(root);
  fs.mkdirSync(archiveDir, { mode: 0o700 });
  const file = path.join(root, "70000000-0000-4000-8000-000000000007.jsonl");
  fs.writeFileSync(file, JSON.stringify({ type: "assistant", timestamp: new Date().toISOString(),
    message: { id: "old", model: "claude-opus-5", usage: { input_tokens: 1, output_tokens: 0 } } }) + "\n");
  const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
    installKey: "fixture-install-key", managed: true, port: 49131,
    captureRoots: [{ source: "claude_code", rootId: "root", profileId: "profile",
      directory: root, installationEpochId: epoch }] });
  fs.writeFileSync(path.join(plimsoll, "collector.config.json"), JSON.stringify(config));
  let old: LocalEventBuffer | undefined;
  let tailer: TranscriptTailer | undefined;
  try {
    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 86_400_000) });
    tailer = new TranscriptTailer(old, undefined, undefined, config.captureRoots);
    await tailer.scan({ scope: "full" });
    tailer.close(); tailer = undefined;
    const original = old.database.prepare(`select committed_offset as offset from rollout_scan_state`)
      .get() as { offset: number };
    old.database.prepare(`update rollout_scan_state set committed_offset=size+1`).run();
    old.close(); old = undefined;
    const input = { ledgerPath: ledger, archivePath, config };
    const corrupt = planFreshLedgerCutover(input);
    assert.equal(corrupt.reason, "archive_cursor_state_inconsistent");
    const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
    const cli = path.resolve("packages/collector-cli/src/cli.ts");
    const processResult = spawnSync(process.execPath,
      ["--import", loader, cli, "capture-roots", "epoch-plan", "--archive", archivePath, "--json"],
      { cwd: path.resolve("."), encoding: "utf8", timeout: 120_000,
        env: { ...process.env, HOME: home, USERPROFILE: home, PLIMSOLL_HOME: plimsoll,
          CODEX_HOME: path.join(home, ".codex"), CLAUDE_CONFIG_DIR: path.join(home, ".claude") } });
    assert.equal(processResult.status, 1, processResult.stderr);
    assert.equal(JSON.parse(processResult.stdout).reason, "archive_cursor_state_inconsistent");
    assert.equal(fs.existsSync(archivePath), false);
    assert.equal(fs.existsSync(`${ledger}.replacement-stage`), false);

    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    old.database.prepare(`update rollout_scan_state set committed_offset=?`).run(original.offset);
    old.database.prepare(`insert into maintenance_state(key,value,updated_at) values(?,?,?)
      on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at`)
      .run("account_assertion_adapters_v1", "{broken", new Date().toISOString());
    old.close(); old = undefined;
    const unreadable = planFreshLedgerCutover(input);
    assert.equal(unreadable.status, "refused");
    assert.equal(unreadable.reason, "archive_live_authorization_unreadable");
    assert.equal(fs.existsSync(archivePath), false);
    assert.equal(fs.existsSync(ledger), true);
    console.log(JSON.stringify({ cursorReason: corrupt.reason, cliReason: "archive_cursor_state_inconsistent",
      authorizationReason: unreadable.reason, archiveCreated: false, replacementCreated: false }));
  } finally {
    tailer?.close(); old?.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
