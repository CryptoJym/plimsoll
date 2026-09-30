import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
if (process.argv[2] === "--child") {
  const fixture = process.argv[3]!;
  const ledger = path.join(fixture, "work-ledger.sqlite");
  const stage = `${ledger}.replacement-stage`;
  const original = fs.renameSync;
  fs.renameSync = ((source: fs.PathLike, destination: fs.PathLike) => {
    const result = original(source, destination);
    if (String(source) === stage && String(destination) === ledger) {
      process.kill(process.pid, "SIGKILL");
    }
    return result;
  }) as typeof fs.renameSync;
  switchFreshLedger({ ledgerPath: ledger,
    archivePath: path.join(fixture, "archive", "old-ledger.sqlite"),
    config: collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
      installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
        profileId: "profile", directory: path.join(fixture, "codex"),
        installationEpochId: epoch }] }),
    authorityRoot: path.join(fixture, "lifecycle-authority") });
  process.exit(90);
} else {
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
    "r5-pending-startup-")));
  const ledger = path.join(fixture, "work-ledger.sqlite");
  try {
    fs.mkdirSync(path.join(fixture, "codex"));
    fs.mkdirSync(path.join(fixture, "archive"), { mode: 0o700 });
    const old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    old.close();
    const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
    const child = spawnSync(process.execPath,
      ["--import", loader, import.meta.filename, "--child", fixture],
      { encoding: "utf8", timeout: 120_000 });
    assert.equal(child.signal, "SIGKILL", child.stderr);
    const db = new Database(ledger, { readonly: true, fileMustExist: true });
    const row = db.prepare("select post_switch_fence_pending as pending from collector_replacement_ledger")
      .get() as { pending: number };
    db.close();
    let opened: LocalEventBuffer | undefined;
    let error: string | null = null;
    try { opened = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch }); }
    catch (caught) { error = caught instanceof Error ? caught.message : String(caught); }
    finally { opened?.close(); }
    console.log(JSON.stringify({ pending: row.pending, error,
      archivePresent: fs.existsSync(path.join(fixture, "archive", "old-ledger.sqlite")) }));
    assert.equal(row.pending, 1);
    assert.equal(error, "replacement_post_switch_fence_pending");
  } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
}
