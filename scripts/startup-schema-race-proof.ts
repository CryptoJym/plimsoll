import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { createProofCompletion } from "./lib/proof-completion";

const workspaceId = "30000000-0000-4000-8000-000000000003";
const deviceId = "40000000-0000-4000-8000-000000000004";
const sleepArray = new Int32Array(new SharedArrayBuffer(4));
const trials = Number(process.env.STARTUP_SCHEMA_RACE_TRIALS ?? 6);
assert.ok(Number.isInteger(trials) && trials >= 1 && trials <= 20);

if (process.argv[2] === "--child") {
  const [file, startBarrier, schemaMarkers, name] = process.argv.slice(3);
  assert.ok(file && startBarrier && schemaMarkers && name);

  // The unfixed open path reads this schema outside a writer transaction.
  // Hold both readers after the read so their subsequent ALTERs use the same
  // stale result. The fixed path owns BEGIN IMMEDIATE before this read.
  const prototype = Database.prototype as unknown as { pragma: (...args: unknown[]) => unknown };
  const originalPragma = prototype.pragma;
  prototype.pragma = function (sql: unknown, ...args: unknown[]) {
    const result = Reflect.apply(originalPragma, this, [sql, ...args]);
    if (sql === "table_info(buffered_events)" && !(this as Database.Database).inTransaction) {
      fs.writeFileSync(path.join(schemaMarkers, name), "read\n");
      const deadline = Date.now() + 20_000;
      while (fs.readdirSync(schemaMarkers).length < 2) {
        if (Date.now() > deadline) throw new Error("schema_read_barrier_timeout");
        Atomics.wait(sleepArray, 0, 0, 10);
      }
    }
    return result;
  };

  process.stdout.write("ready\n");
  while (!fs.existsSync(startBarrier)) Atomics.wait(sleepArray, 0, 0, 10);
  const buffer = new LocalEventBuffer(file, { workspaceId, deviceId });
  try {
    assert.equal(buffer.workspaceBinding()?.currentWorkspaceId, workspaceId);
    assert.equal(buffer.workspaceBinding()?.currentDeviceId, deviceId);
    console.log(JSON.stringify({ status: "opened", binding: buffer.workspaceBinding() }));
  } finally {
    buffer.close();
  }
} else {
  void runProof().catch(error => { console.error(error); process.exitCode = 1; });
}

async function runProof() {
  const completion = createProofCompletion("startup-schema-race", trials);
  const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
  const script = import.meta.filename;
  for (let trial = 1; trial <= trials; trial++) {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "startup-schema-race-")));
    const file = path.join(fixture, "ledger.sqlite");
    const startBarrier = path.join(fixture, "go");
    const schemaMarkers = path.join(fixture, "schema-markers");
    fs.mkdirSync(schemaMarkers);
    const children = ["a", "b"].map(name => spawn(process.execPath,
      ["--import", loader, script, "--child", file, startBarrier, schemaMarkers, name],
      { stdio: ["ignore", "pipe", "pipe"] }));
    let timer: NodeJS.Timeout | undefined;
    try {
      const results = children.map(child => new Promise<{ code: number | null; out: string; err: string }>((resolve, reject) => {
        let out = "", err = "";
        child.stdout.setEncoding("utf8").on("data", chunk => { out += chunk; });
        child.stderr.setEncoding("utf8").on("data", chunk => { err += chunk; });
        child.once("error", reject);
        child.once("close", code => resolve({ code, out, err }));
      }));
      await new Promise(resolve => setTimeout(resolve, 500));
      fs.writeFileSync(startBarrier, "go\n");
      const settled = await Promise.race([Promise.all(results), new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("startup_schema_race_timeout")), 30_000);
      })]);
      console.log(JSON.stringify({ trial, processResults: settled }));
      assert.deepEqual(settled.map(result => result.code), [0, 0],
        "both fresh openers must succeed after the same startup barrier");
      const reopened = new LocalEventBuffer(file, { workspaceId, deviceId });
      try {
        assert.equal(reopened.workspaceBinding()?.currentWorkspaceId, workspaceId);
        assert.ok(reopened.workspaceBinding()?.currentInstallationEpochId);
      } finally { reopened.close(); }
      completion.check(`fresh_open_${trial}`);
    } finally {
      if (timer) clearTimeout(timer);
      for (const child of children) if (child.exitCode === null && child.pid) child.kill("SIGTERM");
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  }
  completion.complete();
}
