import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";

if (process.argv[2] === "--child") {
  const file = process.argv[3]!;
  const barrier = process.argv[4]!;
  process.stdout.write("ready\n");
  while (!fs.existsSync(barrier)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 25);
  const buffer = new LocalEventBuffer(file, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  const binding = buffer.workspaceBinding();
  buffer.close();
  console.log(JSON.stringify(binding));
  process.exit(0);
}

async function main() {
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr426-race-")));
  const file = path.join(fixture, "replacement.sqlite");
  const barrier = path.join(fixture, "go");
  const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
  const children = Array.from({ length: 2 }, () => spawn(process.execPath,
    ["--import", loader, import.meta.filename, "--child", file, barrier],
    { stdio: ["ignore", "pipe", "pipe"] }));
  let timeoutId: NodeJS.Timeout | undefined;
  try {
    const results = children.map(child => new Promise<{ code: number | null; out: string; err: string }>((resolve, reject) => {
      let out = "", err = "";
      child.stdout.setEncoding("utf8").on("data", chunk => { out += chunk; });
      child.stderr.setEncoding("utf8").on("data", chunk => { err += chunk; });
      child.once("error", reject);
      child.once("close", code => resolve({ code, out, err }));
    }));
    await new Promise(resolve => setTimeout(resolve, 500));
    fs.writeFileSync(barrier, "go");
    const settled = await Promise.race([Promise.all(results), new Promise<never>((_, reject) => {
      timeoutId = setTimeout(() => reject(new Error("race_timeout")), 30_000);
    })]);
    // The constructor's long-standing WAL pragma can reject one co-starter
    // with SQLITE_BUSY. The ledger must still have one durable, correct binding.
    assert.ok(settled.some(result => result.code === 0), JSON.stringify(settled));
    assert.ok(settled.every(result => result.code === 0 ||
      (result.code === 1 && result.err.includes("SQLITE_BUSY"))), JSON.stringify(settled));
    const reopened = new LocalEventBuffer(file, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    try {
      assert.equal(reopened.workspaceBinding()!.currentInstallationEpochId, epoch);
      console.log(JSON.stringify({ status: "PASS", processResults: settled.map(result => result.code),
        busyCoStarters: settled.filter(result => result.err.includes("SQLITE_BUSY")).length,
        binding: reopened.workspaceBinding() }));
    } finally { reopened.close(); }
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
    for (const child of children) if (child.exitCode === null && child.pid) child.kill("SIGTERM");
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
