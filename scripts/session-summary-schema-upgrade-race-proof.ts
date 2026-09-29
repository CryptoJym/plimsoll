import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { createProofCompletion } from "./lib/proof-completion";

const workspace = "00000000-0000-4000-8000-000000000108";
const sleepArray = new Int32Array(new SharedArrayBuffer(4));
const script = fileURLToPath(import.meta.url);
const repo = path.resolve(path.dirname(script), "..");
const oldCommit = "62d7452094fed23f5acdb28548e477fa042d3283";

async function child(repo: string, dbPath: string, markers: string, name: string) {
  const require = createRequire(path.join(repo, "package.json"));
  const Database = require("better-sqlite3");
  const db = new Database(dbPath);
  const original = db.exec.bind(db);
  let synchronized = false;
  db.exec = (sql: string) => {
    if (sql.includes("add column state_generation")) {
      synchronized = true;
      // On the old code, both workers reach ALTER without a writer lock, so
      // force that interleaving. With the fix, the first ALTER is already
      // inside an immediate transaction and the second waits for that lock.
      if (!db.inTransaction) {
        fs.writeFileSync(path.join(markers, name), "ready\n");
        const deadline = Date.now() + 30_000;
        while (!(fs.existsSync(path.join(markers, "a")) &&
                 fs.existsSync(path.join(markers, "b")))) {
          if (Date.now() > deadline) throw new Error("schema_race_barrier_timeout");
          Atomics.wait(sleepArray, 0, 0, 10);
        }
      }
    }
    return original(sql);
  };
  const summary = await import(pathToFileURL(path.join(repo,
    "packages/collector-cli/src/session-summary.ts")).href);
  try {
    summary.ensureSessionSummarySchema(db);
    console.log(JSON.stringify({ worker: name, outcome: "success", synchronized }));
  } catch (error) {
    console.log(JSON.stringify({ worker: name, outcome: "error", synchronized,
      message: error instanceof Error ? error.message : String(error) }));
    process.exitCode = 1;
  } finally { db.close(); }
}

function launch(repo: string, dbPath: string, markers: string, name: string) {
  return new Promise<{ name: string; code: number | null; output: string }>((resolve, reject) => {
    const childTmp = path.join(markers, `tmp-${name}`);
    fs.mkdirSync(childTmp);
    const process = spawn(globalThis.process.execPath,
      ["--import", path.join(repo, "node_modules/tsx/dist/loader.mjs"),
        script,
        repo, dbPath, markers, name, "child"],
      { cwd: repo, env: { ...globalThis.process.env,
        TMPDIR: childTmp, TMP: childTmp, TEMP: childTmp } });
    let output = "";
    process.stdout.on("data", chunk => { output += chunk; });
    process.stderr.on("data", chunk => { output += chunk; });
    process.once("error", reject);
    process.once("close", code => resolve({ name, code, output }));
  });
}

async function main() {
  const args = process.argv.slice(2);
  if (args.at(-1) === "child") {
    await child(args[0]!, args[1]!, args[2]!, args[3]!);
    return;
  }
  const completion = createProofCompletion("session-summary-schema-upgrade-race", 2);
  const root = process.env.PLIMSOLL_PROOF_ROOT!;
  const baseRepo = path.join(root, "collector-before-generation");
  const dbPath = path.join(root, "schema-race.sqlite");
  const markers = path.join(root, "schema-markers");
  fs.mkdirSync(markers);
  execFileSync("git", ["worktree", "add", "--detach", "--quiet", baseRepo, oldCommit], { cwd: repo });
  try {
    fs.symlinkSync(path.join(repo, "node_modules"), path.join(baseRepo, "node_modules"), "dir");
    const baseBuffer = await import(pathToFileURL(path.join(baseRepo,
      "packages/collector-cli/src/buffer.ts")).href);
    const baseSummary = await import(pathToFileURL(path.join(baseRepo,
      "packages/collector-cli/src/session-summary.ts")).href);
    const buffer = new baseBuffer.LocalEventBuffer(dbPath, { workspaceId: workspace });
    try { baseSummary.ensureSessionSummarySchema(buffer.database); }
    finally { buffer.close(); }
    const results = await Promise.all([launch(repo, dbPath, markers, "a"),
                                       launch(repo, dbPath, markers, "b")]);
    console.log(JSON.stringify({ source: repo, oldCommit, results }));
    assert.deepEqual(results.map(row => row.code), [0, 0],
      "both collectors must open an old ledger without a schema race");
    completion.check("both_upgrade_workers_succeed");
    const Database = createRequire(path.join(repo, "package.json"))("better-sqlite3");
    const db = new Database(dbPath);
    try {
      const columns = new Set((db.prepare("pragma table_info(session_sync_summary_state)").all() as
        Array<{ name: string }>).map(row => row.name));
      assert.ok(columns.has("state_generation"));
    } finally { db.close(); }
    completion.check("generation_column_installed_once");
    completion.complete();
  } finally {
    execFileSync("git", ["worktree", "remove", "--force", baseRepo], { cwd: repo });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
