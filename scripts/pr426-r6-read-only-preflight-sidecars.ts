import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { planFreshLedgerCutover, switchFreshLedger } from "../packages/collector-cli/src/fresh-ledger-cutover";
import { ensureJsonlScanState } from "../packages/collector-cli/src/jsonl-byte-tailer";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r2-query-plan-")));
const ledger = path.join(fixture, "work-ledger.sqlite");
const archiveDirectory = path.join(fixture, "archive");
const archivePath = path.join(archiveDirectory, "old-ledger.sqlite");
const root = path.join(fixture, "codex");
const epoch = "10000000-0000-4000-8000-000000000001";
const workspace = "30000000-0000-4000-8000-000000000003";
const device = "40000000-0000-4000-8000-000000000004";
fs.mkdirSync(root);
fs.mkdirSync(archiveDirectory, { mode: 0o700 });
const config = collectorConfigSchema.parse({ tenantId: workspace, deviceId: device,
  installKey: "fixture-install-key", captureRoots: [{ source: "codex",
    rootId: "root", profileId: "profile", directory: root, installationEpochId: epoch }] });
let old: LocalEventBuffer | undefined;
try {
  old = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device, freshCaptureRootEpoch: epoch });
  const db = old.database;
  ensureJsonlScanState(db);
  const insert = db.prepare("insert into session_usage_authority(source,session_id,authority,claimed_at) values('codex',?,'tailer',?)");
  const timestamp = new Date().toISOString();
  db.transaction(() => {
    for (let n = 0; n < 20_000; n++) insert.run(`session-${n}`, timestamp);
  })();
  const plans = Object.fromEntries([
    ["events_max", "select max(observed_at) from buffered_events"],
    ["event_created_max", "select max(created_at) from buffered_events"],
    ["metric_max", "select max(created_at) from metric_samples"],
    ["attempt_max", "select max(started_at) from tool_attempt_facts"],
    ["week_count", "select count(*) from tool_attempt_facts where started_at>='2026-09-28' and started_at<'2026-10-05'"],
    ["session_authority_carry", "select source,session_id,authority,claimed_at from session_usage_authority"],
    ["live_receipts_carry", "select scope_digest,kind,attachment_id,packet_key,packet_digest,receipt_json from codex_live_receipts"],
    ["cursor_carry", "select * from rollout_scan_state"],
  ].map(([name, sql]) => [name, db.prepare("explain query plan " + sql).all()]));
  old.close(); old = undefined;
  const archiveBytes = fs.statSync(ledger).size;
  const digestFiles = () => Object.fromEntries([ledger, `${ledger}-wal`, `${ledger}-shm`]
    .filter(file => fs.existsSync(file))
    .map(file => [path.basename(file), crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex")]));
  const beforePlan = digestFiles();
  const input = { ledgerPath: ledger, archivePath, config,
    authorityRoot: path.join(fixture, "lifecycle-authority") };
  const started = performance.now();
  const plan = planFreshLedgerCutover(input);
  const planMs = Math.round(performance.now() - started);
  const afterPlan = digestFiles();
  assert.deepEqual(afterPlan, beforePlan, "preflight must leave the SQLite file and sidecars byte-identical");
  const switchStarted = performance.now();
  if (plan.status === "ready") switchFreshLedger(input);
  const switchMs = Math.round(performance.now() - switchStarted);
  const replacement = new LocalEventBuffer(ledger, { workspaceId: workspace, deviceId: device,
    freshCaptureRootEpoch: epoch });
  const carried = (replacement.database.prepare("select count(*) as n from session_usage_authority").get() as { n: number }).n;
  replacement.close();
  console.log(JSON.stringify({ archiveBytes, sourceRows: 20_000, planStatus: plan.status,
    carriedRowsReported: plan.carriedRows.session_usage_authority, carriedRowsActual: carried,
    planMs, switchMs, preflightByteIdentical: true, plans }));
  assert.equal(carried, 20_000);
} finally {
  old?.close();
  fs.rmSync(fixture, { recursive: true, force: true });
}
