import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, restoreArchivedLedger,
  switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
const fixture = process.argv[2] === "--child" ? process.argv[3]! :
  fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(),
    "r5-two-restores-")));
const ledger = path.join(fixture, "work-ledger.sqlite");
const archive = path.join(fixture, "archive", "old-ledger.sqlite");
const fresh = path.join(fixture, "archive", "fresh-attempt.sqlite");
const authorityRoot = path.join(fixture, "lifecycle-authority");
if (process.argv[2] === "--child") {
  const label = process.argv[4]!;
  fs.writeFileSync(path.join(fixture, `ready-${label}`), "ready");
  while (!fs.existsSync(path.join(fixture, "start"))) {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  }
  try {
    const receipt = restoreArchivedLedger({ ledgerPath: ledger, archivePath: archive,
      freshAttemptPath: fresh, authorityRoot });
    console.log(JSON.stringify({ label, result: "restored", archivePreserved: receipt.archivePreserved }));
  } catch (error) {
    console.log(JSON.stringify({ label, result: "refused", reason: String(error) }));
    process.exitCode = 1;
  }
} else {
  async function main() {
    let children: Array<{ child: ReturnType<typeof spawn>;
      done: Promise<{ label: string; code: number | null; out: string; err: string }> }> = [];
    try {
      fs.mkdirSync(path.join(fixture, "codex"));
      fs.mkdirSync(path.dirname(archive), { mode: 0o700 });
      const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
        installKey: "fixture-install-key", captureRoots: [{ source: "codex", rootId: "root",
          profileId: "profile", directory: path.join(fixture, "codex"),
          installationEpochId: epoch }] });
      const old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
        freshCaptureRootEpoch: epoch });
      old.database.prepare("insert into maintenance_state(key,value,updated_at) values(?,?,?)")
        .run("archive-only-sentinel", "must-survive", new Date().toISOString());
      old.close();
      switchFreshLedger({ ledgerPath: ledger, archivePath: archive, config, authorityRoot });
      const archiveInode = fs.statSync(archive).ino;
      const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
      children = ["a", "b"].map(label => {
        const child = spawn(process.execPath, ["--import", loader, import.meta.filename,
          "--child", fixture, label], { stdio: ["ignore", "pipe", "pipe"] });
        let out = "", err = "";
        child.stdout.on("data", data => { out += String(data); });
        child.stderr.on("data", data => { err += String(data); });
        const done = new Promise<{ label: string; code: number | null; out: string; err: string }>(resolve => {
          child.on("close", code => resolve({ label, code, out, err }));
        });
        return { child, done };
      });
      const deadline = Date.now() + 20_000;
      while (!fs.existsSync(path.join(fixture, "ready-a")) ||
             !fs.existsSync(path.join(fixture, "ready-b"))) {
        assert.ok(Date.now() < deadline, "children did not reach barrier");
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      fs.writeFileSync(path.join(fixture, "start"), "go");
      const results = await Promise.all(children.map(row => row.done));
      const active = new Database(ledger, { readonly: true, fileMustExist: true });
      const sentinel = (active.prepare("select value from maintenance_state where key=?")
        .get("archive-only-sentinel") as { value: string } | undefined)?.value ?? null;
      active.close();
      console.log(JSON.stringify({ results, successes: results.filter(row => row.code === 0).length,
        marker: readReplacementLedgerMarker(ledger), sentinel,
        archiveSameInode: fs.statSync(archive).ino === archiveInode,
        freshPresent: fs.existsSync(fresh), stagePresent: fs.existsSync(`${ledger}.restore-stage`) }));
      assert.ok(results.some(row => row.code === 0));
      assert.equal(readReplacementLedgerMarker(ledger), null);
      assert.equal(sentinel, "must-survive");
      assert.equal(fs.statSync(archive).ino, archiveInode);
      assert.equal(fs.existsSync(fresh), true);
      assert.equal(fs.existsSync(`${ledger}.restore-stage`), false);
    } finally {
      for (const { child } of children) if (child.exitCode === null) child.kill("SIGTERM");
      await Promise.all(children.map(row => row.done));
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  }
  void main().catch(error => { console.error(error); process.exitCode = 1; });
}
