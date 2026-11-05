import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { utcWeekStart } from "../packages/collector-cli/src/weekly-tool-stats";
import { createProfileCapture } from "../packages/collector-cli/src/profile-capture";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";

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
  const device = "dev_n50_fixture";
  const cloudDevice = "40000000-0000-4000-8000-000000000004";
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
  fs.writeFileSync(path.join(plimsoll, "collector.config.json"), JSON.stringify(config), { mode: 0o600 });
  let old: LocalEventBuffer | undefined;
  let tailer: TranscriptTailer | undefined;
  const server = http.createServer(async (request, response) => {
    for await (const _chunk of request) { /* consume the report */ }
    const db = new Database(ledger, { readonly: true });
    const row = db.prepare("select week_start,report_sequence,digest from weekly_tool_stats_uploads where delivered=0")
      .get() as { week_start: string; report_sequence: number; digest: string };
    db.close();
    response.end(JSON.stringify({ schema: "fleet-device-report-receipt/v1", deviceId: cloudDevice,
      reportSequence: row.report_sequence, disposition: "accepted", toolStatsDigest: row.digest,
      storedToolStatsReport: { weekStart: row.week_start, reportSequence: row.report_sequence, digest: row.digest } }));
  });
  try {
    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch, enrollmentNow: () => new Date(Date.now() - 86_400_000) });
    tailer = new TranscriptTailer(old, undefined, undefined, config.captureRoots);
    await tailer.scan({ scope: "full" });
    tailer.close(); tailer = undefined;
    const boundary = new Date(`${utcWeekStart(new Date())}T00:00:00.000Z`);
    boundary.setUTCDate(boundary.getUTCDate() + 7);
    const before = new Date(boundary.getTime() - 60_000);
    old.database.prepare("insert into weekly_tool_stats_control values(?,?,?)")
      .run(workspace, device, utcWeekStart(before));
    old.learningFacts.recordToolSignal({ kind: "attempt", operationId: "80000000-0000-5000-8000-000000000008", source: "claude_code",
      sessionId: path.basename(file, ".jsonl"), toolClass: "compute", toolName: "shell", startedAt: before.toISOString() });
    old.database.prepare("update rollout_scan_state set committed_prefix_hash=NULL,work_remaining=1").run();
    const row = old.database.prepare("select * from rollout_scan_state").get() as Record<string, unknown>;
    old.database.prepare(`insert into rollout_scan_state (${Object.keys(row).join(",")})
      values (${Object.keys(row).map(() => "?").join(",")})`).run(...Object.values({ ...row, file: "a".repeat(64) }));
    old.close(); old = undefined;
    const input = { ledgerPath: ledger, archivePath, config, now: () => boundary };
    assert.equal(planFreshLedgerCutover({ ...input, now: () => before }).reason, "current_utc_week_tool_attempts");
    assert.equal(planFreshLedgerCutover(input).reason, "prior_week_report_not_acknowledged");
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    config.uploadUrl = `http://127.0.0.1:${(server.address() as { port: number }).port}/api/work-intelligence/ingest`;
    config.cloudDeviceId = cloudDevice;
    fs.writeFileSync(path.join(plimsoll, "collector.config.json"), JSON.stringify(config));
    const clock = path.join(fixture, "clock.mjs");
    fs.writeFileSync(clock, `const OriginalDate = Date; globalThis.Date = class extends OriginalDate {
      constructor(...args) { super(...(args.length ? args : [${boundary.getTime()}])); }
      static now() { return ${boundary.getTime()}; }
    };`);
    const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
    const cli = path.resolve("packages/collector-cli/src/cli.ts");
    const processResult = await promisify(execFile)(process.execPath,
      ["--import", clock, "--import", loader, cli, "upload", "--tool-stats-weeks"],
      { cwd: path.resolve("."), encoding: "utf8", timeout: 120_000,
        env: { ...process.env, HOME: home, USERPROFILE: home, PLIMSOLL_HOME: plimsoll,
          CODEX_HOME: path.join(home, ".codex"), CLAUDE_CONFIG_DIR: path.join(home, ".claude") } });
    assert.deepEqual(JSON.parse(processResult.stdout), {
      status: "weekly_tool_stats_upload", result: "not_due", acceptedWeeks: 1, maxWeeks: 52 });
    const plan = planFreshLedgerCutover(input);
    assert.equal(plan.status, "ready", plan.reason ?? "");
    assert.equal(plan.invalidCursorsNotCarried, 2);
    assert.equal(plan.untrackedFileFences, 1);
    assert.deepEqual([plan.currentUtcWeekToolAttempts, plan.priorWeekReportAcknowledged], [0, true]);

    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
      freshCaptureRootEpoch: epoch });
    old.database.prepare(`insert into maintenance_state(key,value,updated_at) values(?,?,?)
      on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at`)
      .run("account_assertion_adapters_v1", "{broken", new Date().toISOString());
    old.close(); old = undefined;
    const unreadable = planFreshLedgerCutover(input);
    assert.equal(unreadable.status, "refused");
    assert.equal(unreadable.reason, "archive_live_authorization_unreadable");
    assert.equal(fs.existsSync(archivePath), false);
    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device });
    old.database.prepare("delete from maintenance_state where key='account_assertion_adapters_v1'").run();
    old.close(); old = undefined;
    const switched = switchFreshLedger(input);
    old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device });
    assert.deepEqual(old.database.prepare("select baseline_size from replacement_unseen_file_fences").all(),
      [{ baseline_size: fs.statSync(file).size }]);
    assert.equal((old.database.prepare("select count(*) as n from rollout_scan_state").get() as { n: number }).n, 0);
    const capture = createProfileCapture(old, config);
    try {
      for (let pass = 0; pass < 3; pass++) await capture.transcript.scan({ scope: "recent",
        automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
      assert.equal(captureBaselineStatus(old.database).status, "complete");
      const scan = await capture.transcript.scan({ scope: "recent",
        automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
      assert.equal(scan.recordsParsed, 0);
      assert.equal(scan.excludedGenerations, 1);
    } finally { capture.close(); }
    console.log(JSON.stringify({ legacyCursors: plan.invalidCursorsNotCarried, fencedFiles: plan.untrackedFileFences,
      weeklyUpload: JSON.parse(processResult.stdout), plan: plan.status, switched: switched.status,
      rereadRecords: 0, authorizationRefusal: unreadable.reason }));
  } finally {
    server.close();
    tailer?.close(); old?.close();
    fs.rmSync(fixture, { recursive: true, force: true });
  }
}

void main().catch(error => { console.error(error); process.exitCode = 1; });
