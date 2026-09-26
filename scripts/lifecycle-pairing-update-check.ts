/** Local CI-layout check against a real 0.7.41 ledger and packaged lifecycle commands. */
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import Database from "better-sqlite3";
import { build } from "esbuild";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorBufferPath, collectorConfigPath } from "../packages/collector-cli/src/config";
import { parseCompletionReceipt } from "../packages/collector-cli/src/lifecycle";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";

const root = path.resolve(import.meta.dirname, "..");
const oldRoot = path.resolve(root, "../plimsoll-0.7.41");
const baseRoot = path.resolve(root, "../plimsoll-0.7.42-base");
const oldCommit = "03445d3ade34fb6178a1a0a018e6b92bbd2b69da";
const baseCommit = "4982bb62613427cf9c0a3953adca966eb75fc095";
const createdWorktrees: string[] = [];
const temporaryRoot = process.env.TMPDIR;
if (!temporaryRoot || !fs.realpathSync(temporaryRoot).startsWith(path.resolve(root, "../..", "ci-home") + path.sep)) {
  throw new Error("CI-layout TMPDIR is required");
}
for (const [source, commit] of [[oldRoot, oldCommit], [baseRoot, baseCommit]]) {
  if (!fs.existsSync(path.join(source, "packages", "collector-cli", "src", "buffer.ts"))) {
    const created = spawnSync("git", ["worktree", "add", "--detach", source, commit], {
      cwd: root, encoding: "utf8", timeout: 60_000,
    });
    if (created.status !== 0) throw new Error(`cannot create pinned fixture worktree: ${created.stderr}`);
    createdWorktrees.push(source);
  }
  if (!fs.existsSync(path.join(source, "node_modules"))) {
    fs.symlinkSync(path.relative(source, path.join(root, "node_modules")), path.join(source, "node_modules"));
  }
  const head = spawnSync("git", ["rev-parse", "HEAD"], { cwd: source, encoding: "utf8", timeout: 10_000 });
  if (head.status !== 0 || head.stdout.trim() !== commit) {
    throw new Error("fixture worktree is not the pinned commit");
  }
}
const fixtureHome = fs.mkdtempSync(path.join(temporaryRoot, "plimsoll-pairing-update-"));
const priorPlimsollHome = process.env.PLIMSOLL_HOME;
process.env.PLIMSOLL_HOME = path.join(fixtureHome, ".plimsoll");
const oldBundle = path.join(root, "packages", "collector-cli", "dist-old-0.7.41", "cli.mjs");
const baseBundle = path.join(root, "packages", "collector-cli", "dist-old-0.7.42", "cli.mjs");
const newBundle = path.join(root, "packages", "collector-cli", "dist", "cli.mjs");
const ledger = collectorBufferPath(fixtureHome);
const lifecycleRoot = path.join(fixtureHome, ".plimsoll", "lifecycle");
const stubBin = path.join(fixtureHome, "stubbin");
const launchctlLog = path.join(stubBin, "launchctl.log");
const checks: string[] = [];
let holderPid: number | null = null;

function check(name: string, valid: unknown, detail: unknown = null) {
  console.log(`${valid ? "PASS" : "FAIL"} ${name}`);
  if (!valid) throw new Error(`${name}: ${JSON.stringify(detail)}`);
  checks.push(name);
}

function cli(bundle: string, args: string[], nodeArgs: string[] = []) {
  const result = spawnSync(process.execPath, [...nodeArgs, bundle, ...args], {
    cwd: fixtureHome,
    env: {
      ...process.env,
      HOME: fixtureHome,
      USERPROFILE: fixtureHome,
      PLIMSOLL_HOME: path.join(fixtureHome, ".plimsoll"),
      CODEX_HOME: path.join(fixtureHome, ".codex"),
      CLAUDE_CONFIG_DIR: path.join(fixtureHome, ".claude"),
      XDG_CONFIG_HOME: path.join(fixtureHome, ".config"),
      XDG_CACHE_HOME: path.join(fixtureHome, ".cache"),
      XDG_STATE_HOME: path.join(fixtureHome, ".local", "state"),
      TMPDIR: path.join(fixtureHome, "tmp"),
      PATH: `${stubBin}:${process.env.PATH ?? ""}`,
    },
    encoding: "utf8", timeout: 240_000,
  });
  if (result.status !== 0) {
    throw new Error(`lifecycle command failed (${result.status}): ${(result.stderr ?? "").slice(0, 1400)}`);
  }
  return JSON.parse(result.stdout) as { receipt: Record<string, any> };
}

const attr = (key: string, value: string | number) => ({ key,
  value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value } });
const nano = (ms: number) => String(BigInt(Math.round(ms)) * 1_000_000n);
const resource = { attributes: [attr("service.name", "codex-app-server"), attr("service.version", "0.156.0")] };

function responsePair(at: number) {
  const log = explodeOtlpPayload({ resourceLogs: [{ resource, scopeLogs: [{ logRecords: [{
    timeUnixNano: nano(at), attributes: [
      attr("event.name", "codex.sse_event"), attr("event.kind", "response.completed"),
      attr("input_token_count", "24261"), attr("output_token_count", "148"),
      attr("cached_token_count", 11776), attr("conversation.id", "019e9100-0000-7000-8000-00000000000a"),
      attr("user.account_id", "synthetic-account"), attr("model", "gpt-5.1-codex-max"),
    ],
  }] }] }] }, { source: "codex" });
  const span = explodeOtlpPayload({ resourceSpans: [{ resource, scopeSpans: [{ spans: [{
    traceId: "00000000000000000000000000000001", spanId: "0000000000000001",
    name: "handle_responses", kind: 1,
    startTimeUnixNano: nano(at - 1_500), endTimeUnixNano: nano(at + 20),
    attributes: [attr("gen_ai.usage.input_tokens", 24261),
      attr("gen_ai.usage.cache_read.input_tokens", 11776),
      attr("gen_ai.usage.output_tokens", 148)],
  }] }] }] }, { source: "codex" });
  if (log.events.length !== 1 || span.events.length !== 1) throw new Error("Codex fixture parse failed");
  return [log.events[0]!, span.events[0]!] as const;
}

async function main() {
  fs.mkdirSync(stubBin, { recursive: true, mode: 0o700 });
  fs.mkdirSync(path.join(fixtureHome, "tmp"), { mode: 0o700 });
  fs.writeFileSync(path.join(stubBin, "launchctl"), `#!/bin/sh\necho called >> ${JSON.stringify(launchctlLog)}\nexit 97\n`, { mode: 0o700 });
  for (const [source, bundle] of [
    [oldRoot, oldBundle], [baseRoot, baseBundle], [root, newBundle],
  ]) {
    fs.mkdirSync(path.dirname(bundle), { recursive: true });
    await build({ entryPoints: [path.join(source, "packages", "collector-cli", "src", "cli.ts")],
      bundle: true, platform: "node", target: "node20", format: "esm", outfile: bundle,
      external: ["better-sqlite3"], logLevel: "silent" });
    fs.chmodSync(bundle, 0o755);
    fs.copyFileSync(path.join(source, "packages", "collector-cli", "src", "dashboard.html"),
      path.join(path.dirname(bundle), "dashboard.html"));
  }
  fs.mkdirSync(path.dirname(collectorConfigPath(fixtureHome)), { recursive: true, mode: 0o700 });
  fs.writeFileSync(collectorConfigPath(fixtureHome), "{}\n", { mode: 0o600 });
  const old = await import(pathToFileURL(path.join(oldRoot, "packages", "collector-cli", "src", "buffer.ts")).href);
  const oldBuffer = new old.LocalEventBuffer(ledger, {
    workspaceId: "b2b2b2b2-2222-4222-8222-222222222222",
    enrollmentNow: () => new Date("2026-09-25T17:00:00.000Z"),
  });
  oldBuffer.close();
  const before = new Database(ledger, { readonly: true });
  const indexCountBefore = (before.prepare(`select count(*) as n from sqlite_master
    where name in ('idx_codex_usage_span_backfill', 'idx_codex_usage_span_match', 'idx_codex_usage_log_match')`).get() as { n: number }).n;
  before.close();
  check("actual_0_7_41_ledger_starts_without_pairing_indexes", indexCountBefore === 0, indexCountBefore);

  const installed = cli(oldBundle, ["lifecycle", "update", "--operation-id", "install-041", "--artifact", "self"]);
  check("fixture_home_has_0_7_41_runtime", installed.receipt.status === "completed" &&
    installed.receipt.toVersion === "0.7.41", installed.receipt);

  const upgraded = cli(newBundle, ["lifecycle", "update", "--operation-id", "upgrade-042", "--artifact", "self"]);
  const after = new Database(ledger, { readonly: true });
  const indexCountAfter = (after.prepare(`select count(*) as n from sqlite_master
    where name in ('idx_codex_usage_span_backfill', 'idx_codex_usage_span_match', 'idx_codex_usage_log_match')`).get() as { n: number }).n;
  after.close();
  check("plain_update_builds_indexes_and_records_it", upgraded.receipt.status === "completed" &&
    upgraded.receipt.fromVersion === "0.7.41" && upgraded.receipt.pairingIndexes?.status === "applied" &&
    indexCountAfter === 3 && parseCompletionReceipt(upgraded.receipt, "upgrade-042") !== null,
    { receipt: upgraded.receipt, indexCountAfter });
  const older042 = cli(baseBundle, ["lifecycle", "pairing-indexes"]) as unknown as
    { enabled?: boolean; missingIndexes?: string[] };
  check("base_0_7_42_reads_upgraded_ledger", older042.enabled === true &&
    older042.missingIndexes?.length === 0, older042);

  const buffer = new LocalEventBuffer(ledger, {
    workspaceId: "b2b2b2b2-2222-4222-8222-222222222222",
    enrollmentNow: () => new Date("2026-09-25T17:00:00.000Z"),
  });
  try {
    for (const entry of responsePair(Date.parse("2026-09-25T18:00:00.000Z"))) {
      if (!buffer.append(entry.event, entry.suppressedFields)) throw new Error("Codex fixture append refused");
    }
    const counts = buffer.database.prepare(`select count(*) as raw,
      sum(case when usage_duplicate_reason is null and event_type = 'assistant_response'
        and input_tokens is not null then 1 else 0 end) as countable,
      sum(case when usage_duplicate_reason = 'codex_sse_event_span' then 1 else 0 end) as duplicates
      from buffered_events where source = 'codex'`).get() as
      { raw: number; countable: number; duplicates: number };
    check("codex_log_and_span_count_once_after_update", counts.raw === 2 &&
      counts.countable === 1 && counts.duplicates === 1,
      { counts, rows: buffer.database.prepare(`select event_type as eventType, input_tokens as inputTokens,
        output_tokens as outputTokens, usage_duplicate_reason as duplicateReason from buffered_events`).all() });
  } finally { buffer.close(); }

  const beforeBusy = new Database(ledger);
  beforeBusy.exec(`drop index idx_codex_usage_span_backfill;
    drop index idx_codex_usage_span_match;
    drop index idx_codex_usage_log_match;`);
  beforeBusy.close();
  const readyFile = path.join(fixtureHome, "holder-ready");
  const pidFile = path.join(fixtureHome, "holder-pid");
  const holderScript = path.join(fixtureHome, "holder.mjs");
  const binding = createRequire(import.meta.url).resolve("better-sqlite3");
  fs.writeFileSync(holderScript, `import fs from 'node:fs';\n` +
    `import { createRequire } from 'node:module';\n` +
    `const Database = createRequire(import.meta.url)(${JSON.stringify(binding)});\n` +
    `const db = new Database(${JSON.stringify(ledger)});\n` +
    `db.prepare('select 1 from sqlite_master limit 1').get();\n` +
    `fs.writeFileSync(${JSON.stringify(readyFile)}, 'ready');\n` +
    `process.on('SIGTERM', () => { db.close(); process.exit(0); });\n` +
    `setInterval(() => {}, 1000);\n`);
  const hook = path.join(fixtureHome, "hold-after-switch.mjs");
  fs.writeFileSync(hook, `import fs from 'node:fs';\n` +
    `import { spawn } from 'node:child_process';\n` +
    `import { syncBuiltinESMExports } from 'node:module';\n` +
    `const rename = fs.renameSync; let started = false;\n` +
    `fs.renameSync = function (...args) {\n` +
    `  const result = rename.apply(this, args);\n` +
    `  if (!started && String(args[1]) === ${JSON.stringify(path.join(lifecycleRoot, "journal.json"))}) {\n` +
    `    const journal = JSON.parse(fs.readFileSync(args[1], 'utf8'));\n` +
    `    if (journal.phase === 'switched') {\n` +
    `      started = true;\n` +
    `      const holder = spawn(process.execPath, [${JSON.stringify(holderScript)}], { stdio: 'ignore' });\n` +
    `      holder.unref();\n` +
    `      fs.writeFileSync(${JSON.stringify(pidFile)}, String(holder.pid));\n` +
    `      const sleep = new Int32Array(new SharedArrayBuffer(4));\n` +
    `      const deadline = Date.now() + 5000;\n` +
    `      while (!fs.existsSync(${JSON.stringify(readyFile)}) && Date.now() < deadline) Atomics.wait(sleep, 0, 0, 25);\n` +
    `      if (!fs.existsSync(${JSON.stringify(readyFile)})) throw new Error('holder did not open ledger');\n` +
    `    }\n` +
    `  }\n` +
    `  return result;\n` +
    `};\n` +
    `syncBuiltinESMExports();\n`);
  const busy = cli(newBundle, ["lifecycle", "update", "--operation-id", "busy-upgrade", "--artifact", "self"],
    ["--import", hook]);
  holderPid = Number(fs.readFileSync(pidFile, "utf8"));
  const busyPersisted = JSON.parse(fs.readFileSync(path.join(lifecycleRoot, "receipts", "busy-upgrade-update.json"), "utf8")) as
    { pairingIndexes?: { status?: string; reason?: string; attempts?: number; elapsedMs?: number } };
  check("late_ledger_holder_yields_bounded_reported_skip", busy.receipt.status === "completed" &&
    busy.receipt.pairingIndexes?.status === "skipped" &&
    busy.receipt.pairingIndexes?.reason === "ledger_in_use" &&
    busy.receipt.pairingIndexes?.attempts === 2 &&
    busy.receipt.pairingIndexes?.elapsedMs >= 5_000 &&
    busy.receipt.pairingIndexes?.elapsedMs < 180_000 &&
    busyPersisted.pairingIndexes?.reason === "ledger_in_use" &&
    parseCompletionReceipt(busy.receipt, "busy-upgrade") !== null,
    { command: busy.receipt, persisted: busyPersisted });
  const stillMissing = new Database(ledger, { readonly: true });
  const missingCount = (stillMissing.prepare(`select count(*) as n from sqlite_master
    where name in ('idx_codex_usage_span_backfill', 'idx_codex_usage_span_match', 'idx_codex_usage_log_match')`).get() as
    { n: number }).n;
  stillMissing.close();
  check("busy_skip_does_not_build_partial_indexes", missingCount === 0, missingCount);
  try { process.kill(holderPid, "SIGTERM"); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
    holderPid = null;
  }
  for (let attempt = 0; holderPid !== null && attempt < 60; attempt += 1) {
    try { process.kill(holderPid, 0); } catch { holderPid = null; break; }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  if (holderPid !== null) throw new Error("holder did not stop");

  const retry = cli(newBundle, ["lifecycle", "update", "--operation-id", "retry-upgrade", "--artifact", "self"]);
  check("next_plain_update_builds_missing_indexes", retry.receipt.status === "completed" &&
    retry.receipt.pairingIndexes?.status === "applied", retry.receipt);
  const repeat = cli(newBundle, ["lifecycle", "update", "--operation-id", "repeat-upgrade", "--artifact", "self"]);
  const repeated = new Database(ledger, { readonly: true });
  const repeatedState = repeated.prepare(`select
    (select count(*) from sqlite_master where name in
      ('idx_codex_usage_span_backfill', 'idx_codex_usage_span_match', 'idx_codex_usage_log_match')) as indexes,
    (select count(*) from buffered_events where source = 'codex' and event_type = 'assistant_response'
      and input_tokens is not null and usage_duplicate_reason is null) as countable,
    (select count(*) from buffered_events where usage_duplicate_reason = 'codex_sse_event_span') as duplicates`).get() as
    { indexes: number; countable: number; duplicates: number };
  repeated.close();
  check("repeated_update_keeps_pairing_idempotent", repeat.receipt.status === "completed" &&
    repeat.receipt.pairingIndexes?.status === "applied" && repeatedState.indexes === 3 &&
    repeatedState.countable === 1 && repeatedState.duplicates === 1,
    { receipt: repeat.receipt, state: repeatedState });
  check("update_never_invoked_launchctl", !fs.existsSync(launchctlLog));
  console.log(JSON.stringify({ proof: "lifecycle-pairing-update", checks: checks.length,
    passed: checks.length, failed: [], liveStateTouched: false,
    receipts: { upgraded: upgraded.receipt.pairingIndexes, busy: busy.receipt.pairingIndexes,
      retried: retry.receipt.pairingIndexes, repeated: repeat.receipt.pairingIndexes },
    pairedRows: repeatedState }));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
}).finally(() => {
  if (holderPid !== null) {
    try { process.kill(holderPid, "SIGKILL"); } catch { /* Already stopped. */ }
  }
  fs.rmSync(fixtureHome, { recursive: true, force: true });
  if (priorPlimsollHome === undefined) delete process.env.PLIMSOLL_HOME;
  else process.env.PLIMSOLL_HOME = priorPlimsollHome;
  for (const source of createdWorktrees) {
    const removed = spawnSync("git", ["worktree", "remove", "--force", source], {
      cwd: root, encoding: "utf8", timeout: 60_000,
    });
    if (removed.status !== 0) console.error(`could not remove fixture worktree: ${removed.stderr}`);
  }
});
