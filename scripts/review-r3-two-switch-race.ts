import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const fixture = process.argv[2] === "--child" ? process.argv[3]! :
  fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r3-two-switches-")));
const ledgerPath = path.join(fixture, "work-ledger.sqlite");
const archivePath = path.join(fixture, "archive", "old-ledger.sqlite");
const root = path.join(fixture, "codex");
const authorityRoot = path.join(fixture, "lifecycle-authority");
const config = collectorConfigSchema.parse({
  tenantId: "30000000-0000-4000-8000-000000000003",
  deviceId: "40000000-0000-4000-8000-000000000004",
  installKey: "fixture-install-key",
  captureRoots: [{ source: "codex", rootId: "root", profileId: "profile", directory: root,
    installationEpochId: "10000000-0000-4000-8000-000000000001" }],
});

if (process.argv[2] === "--child") {
  const label = process.argv[4]!;
  fs.writeFileSync(path.join(fixture, `ready-${label}`), "ready");
  while (!fs.existsSync(path.join(fixture, "start")))
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  try {
    switchFreshLedger({ ledgerPath, archivePath, config, authorityRoot,
      onStep: step => {
        if (step === "old_locked") Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
      } });
    console.log(JSON.stringify({ label, result: "switched" }));
  } catch (error) {
    console.log(JSON.stringify({ label, result: "refused", reason: String(error) }));
    process.exitCode = 1;
  }
} else {
  async function main() {
    let spawned: Array<{ child: ReturnType<typeof spawn>; done: Promise<unknown> }> = [];
    try {
      fs.mkdirSync(root);
      fs.mkdirSync(path.dirname(archivePath), { mode: 0o700 });
      const old = new LocalEventBuffer(ledgerPath, { workspaceId: config.tenantId,
        deviceId: config.deviceId, freshCaptureRootEpoch: config.captureRoots![0]!.installationEpochId });
      old.close();
      const oldInode = fs.statSync(ledgerPath).ino;
      const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
      const children = ["a", "b"].map(label => {
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
      spawned = children;
      const deadline = Date.now() + 20_000;
      while (!fs.existsSync(path.join(fixture, "ready-a")) || !fs.existsSync(path.join(fixture, "ready-b"))) {
        assert.ok(Date.now() < deadline, "children did not reach barrier");
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      fs.writeFileSync(path.join(fixture, "start"), "go");
      const results = await Promise.all(children.map(child => child.done));
      const successes = results.filter(result => result.code === 0);
      console.log(JSON.stringify({ results, successes: successes.length,
        marker: readReplacementLedgerMarker(ledgerPath)?.minCollectorVersion,
        archiveSameOldInode: fs.statSync(archivePath).ino === oldInode }));
      assert.equal(successes.length, 1);
      assert.equal(fs.statSync(archivePath).ino, oldInode);
      assert.equal(readReplacementLedgerMarker(ledgerPath)?.minCollectorVersion, "0.7.46");
    } finally {
      for (const { child } of spawned) if (child.exitCode === null) child.kill("SIGTERM");
      await Promise.all(spawned.map(({ done }) => done));
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  }
  void main().catch(error => { console.error(error); process.exitCode = 1; });
}
