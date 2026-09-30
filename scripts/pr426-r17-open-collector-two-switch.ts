import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readReplacementLedgerMarker, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";

const childMode = process.argv[2] === "--child";
const fixture = childMode ? process.argv[3]! :
  fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r17-open-collector-")));
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

if (childMode) {
  const label = process.argv[4]!;
  fs.writeFileSync(path.join(fixture, `ready-${label}`), "ready");
  while (!fs.existsSync(path.join(fixture, "start")))
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
  try {
    switchFreshLedger({ ledgerPath, archivePath, config, authorityRoot });
    console.log(JSON.stringify({ label, result: "switched" }));
  } catch (error) {
    console.log(JSON.stringify({ label, result: "refused", reason: String(error) }));
    process.exitCode = 1;
  }
} else {
  async function main() {
    let collector: LocalEventBuffer | undefined;
    const children: Array<{ child: ReturnType<typeof spawn>; done: Promise<{
      label: string; code: number | null; out: string; err: string }> }> = [];
    try {
      fs.mkdirSync(root);
      fs.mkdirSync(path.dirname(archivePath), { mode: 0o700 });
      collector = new LocalEventBuffer(ledgerPath, { workspaceId: config.tenantId,
        deviceId: config.deviceId, freshCaptureRootEpoch: config.captureRoots![0]!.installationEpochId });
      const oldInode = fs.statSync(ledgerPath).ino;
      const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
      for (const label of ["a", "b"]) {
        const child = spawn(process.execPath, ["--import", loader, import.meta.filename,
          "--child", fixture, label], { stdio: ["ignore", "pipe", "pipe"] });
        let out = "", err = "";
        child.stdout.on("data", data => { out += String(data); });
        child.stderr.on("data", data => { err += String(data); });
        const done = new Promise<{ label: string; code: number | null; out: string; err: string }>(resolve => {
          child.on("close", code => resolve({ label, code, out, err }));
        });
        children.push({ child, done });
      }
      while (!["a", "b"].every(label => fs.existsSync(path.join(fixture, `ready-${label}`))))
        await new Promise(resolve => setTimeout(resolve, 10));
      fs.writeFileSync(path.join(fixture, "start"), "start");
      const results = await Promise.all(children.map(({ done }) => done));
      assert.equal(results.length, 2);
      for (const result of results) {
        assert.equal(result.code, 1, `${result.label}: ${result.out} ${result.err}`);
        const refusal = JSON.parse(result.out.trim()) as { result: string; reason: string };
        assert.equal(refusal.result, "refused");
        assert.match(refusal.reason, /ledger_quiescence_unproven/);
      }
      assert.equal(fs.statSync(ledgerPath).ino, oldInode);
      assert.equal(fs.existsSync(archivePath), false);
      assert.equal(readReplacementLedgerMarker(ledgerPath), null);
      console.log(JSON.stringify({ results, oldInodeActive: true, archiveExists: false }));
    } finally {
      for (const { child } of children) if (child.exitCode === null) child.kill("SIGTERM");
      await Promise.all(children.map(({ done }) => done));
      collector?.close();
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  }
  void main().catch(error => { console.error(error); process.exitCode = 1; });
}
