/** Synthetic-only regression proof for explicit pre-enrollment history import. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { CODEX_MODEL_WAIT_MS } from "../packages/collector-cli/src/codex-model-capture";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { estimateCostUsd } from "../packages/shared/src/index";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { captureFrontier, ensureCaptureFrontierSchema, CAPTURE_FRONTIER_SOURCES } from "../packages/collector-cli/src/capture-frontier";
import { captureRootDigest, captureRootObservationPayloadDigest, deriveCaptureRootIdentity, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { planCaptureHistory, applyCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { acknowledgingFetch } from "./fixtures/delivery-ack-fixture";
import { useFixtureRoot } from "./lib/fixture-root";

const WORKSPACE = "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10";
const DEVICE = "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d";
const BASELINE = "2026-01-01T00:00:00.000Z";
const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-history-import-proof-")));
const fixture = useFixtureRoot(root, { home: path.join(root, "home"),
  plimsollHome: path.join(root, "plimsoll-home") });
fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });
const checks: string[] = [];
function check(name: string, value: unknown) { assert.ok(value, name); checks.push(name); }
function codexFile(directory: string, id: string, amounts: Array<[number, number]>) {
  const day = path.join(directory, "2026", "01", "02");
  fs.mkdirSync(day, { recursive: true, mode: 0o700 });
  const file = path.join(day, `rollout-2026-01-02T00-00-00-${id}.jsonl`);
  const lines = [JSON.stringify({ type: "session_meta", timestamp: "2026-01-02T00:00:00.000Z", payload: { id } }),
    JSON.stringify({ type: "turn_context", timestamp: "2026-01-02T00:00:00.500Z", payload: { turn_id: "turn-a", model: "gpt-6-sol" } }),
    JSON.stringify({ type: "event_msg", timestamp: "2026-01-02T00:00:01.000Z", payload: { type: "token_count", info: { total_token_usage: { input_tokens: 0, output_tokens: 0 } } } }),
    ...amounts.map(([input, output], index) => JSON.stringify({
      type: "event_msg", timestamp: `2026-01-02T00:00:${String(index + 2).padStart(2, "0")}.000Z`,
      payload: { type: "token_count", info: { total_token_usage: { input_tokens: input, output_tokens: output } } },
    }))];
  fs.writeFileSync(file, `${lines.join("\n")}\n`, { mode: 0o600 });
  return file;
}
function claudeFile(directory: string, id: string) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = path.join(directory, `${id}.jsonl`);
  const usage = (second: number, messageId: string, input: number, output: number) => JSON.stringify({
    type: "assistant", sessionId: id,
    timestamp: `2026-01-02T00:00:${String(second).padStart(2, "0")}.000Z`,
    message: { id: messageId, model: "claude-sonnet-4-5", usage: { input_tokens: input, output_tokens: output } },
  });
  fs.writeFileSync(file, `${[
    usage(2, "message-a", 100, 10), usage(3, "message-a", 150, 15),
    usage(4, "message-b", 40, 4),
  ].join("\n")}\n`, { mode: 0o600 });
  return file;
}
function seal(buffer: LocalEventBuffer, source: "codex" | "claude_code", files: string[]) {
  const begun = beginAutomaticCaptureBaseline(buffer.database, source, { startedAt: BASELINE, filesDiscovered: 0 });
  completeAutomaticCaptureBaseline(buffer.database, source, { runId: begun.latestRun!.runId, completedAt: BASELINE });
  const observations = files.map(file => {
    const stat = fs.statSync(file, { bigint: true });
    return { path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs };
  });
  const receipt = sealCaptureBaselineGenerations(buffer.database, source, observations, "2026-01-03T00:00:00.000Z");
  assert.equal(receipt.generationsSealed, files.length);
}
function sealExtra(buffer: LocalEventBuffer, source: "codex" | "claude_code", file: string) {
  // Each staged scenario models a fresh root import against the accumulated
  // capture ledger. The source generations grow only in this synthetic proof;
  // a production retry is bound to the original fenced inventory.
  if (buffer.database.prepare(`select 1 from sqlite_master
    where type='table' and name='capture_history_import_runs'`).get())
    buffer.database.prepare(`delete from capture_history_import_runs where source=?`).run(source);
  const stat = fs.statSync(file, { bigint: true });
  const receipt = sealCaptureBaselineGenerations(buffer.database, source, [{
    path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs,
  }], "2026-01-03T00:00:00.000Z");
  assert.equal(receipt.generationsSealed, 1);
}
function priorTailerRow(buffer: LocalEventBuffer, session: string) {
  const id = deterministicEventId(["codex-rollout", session, "1"]);
  buffer.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json,
     suppressed_fields_json, created_at, session_id, model, input_tokens, output_tokens, uploaded_at)
    values (?, 'codex', 'usage_rollout', 'metadata', '2026-01-02T00:00:02.000Z', '{}', '[]',
      '2026-01-02T00:00:03.000Z', ?, 'gpt-6-sol', 100, 10, '2026-01-02T00:00:04.000Z')`).run(id, session);
  buffer.database.prepare(`insert into session_usage_authority values ('codex', ?, 'tailer', '2026-01-02T00:00:03.000Z')`).run(session);
}
function codexSightingDigest(session: string, id: string) {
  return captureRootObservationPayloadDigest({ source: "codex", id, sessionId: session,
    observedAt: "2026-01-02T00:00:02.000Z", inputTokens: 100, outputTokens: 10,
    model: "gpt-6-sol", cacheReadTokens: 0,
    costUsd: estimateCostUsd({ model: "gpt-6-sol", inputTokens: 100, outputTokens: 10,
      cacheReadTokens: 0 })?.costUsd });
}
async function main() {
  const home = path.join(root, "home");
  const directory = path.join(home, ".codex-profiles", "missed", "sessions");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const partial = "019d0000-0000-7000-8000-000000000001";
  const missing = "019d0000-0000-7000-8000-000000000002";
  const live = "019d0000-0000-7000-8000-000000000003";
  const files = [codexFile(directory, partial, [[100, 10], [150, 15]]),
    codexFile(directory, missing, [[100, 10], [150, 15]]),
    codexFile(directory, live, [[100, 10], [150, 15]])];
  const ledger = path.join(fixture.env.PLIMSOLL_HOME, "work-ledger.sqlite");
  const buffer = new LocalEventBuffer(ledger, { workspaceId: WORKSPACE, deviceId: DEVICE, delivery: { enabled: true } });
  try {
    const epoch = buffer.workspaceBinding()!.currentInstallationEpochId!;
    const captureRoot: CaptureRoot = { ...deriveCaptureRootIdentity("fixture", "codex", directory),
      source: "codex", directory, installationEpochId: epoch };
    seal(buffer, "codex", files);
    priorTailerRow(buffer, partial);
    // A legacy whole-session live marker cannot cover new native responses.
    buffer.database.prepare(`insert into session_usage_authority values ('codex', ?, 'live', '2026-01-02T00:00:03.000Z')`).run(live);
    const before = Number((buffer.database.prepare(`select count(*) as n from buffered_events`).get() as { n: number }).n);
    const preview = await planCaptureHistory(buffer.database, captureRoot);
    check("dry_run_is_value_blind", !JSON.stringify(preview).includes(directory) &&
      !JSON.stringify(preview).includes(partial) && !JSON.stringify(preview).includes(missing));
    check("dry_run_counts_only_missing_rows", preview.missingRows === 5 && preview.skippedLiveSessions === 0);
    check("dry_run_writes_nothing", Number((buffer.database.prepare(`select count(*) as n from buffered_events`).get() as { n: number }).n) === before &&
      !buffer.database.prepare(`select 1 from sqlite_master where name='capture_history_import_runs'`).get());
    const applied = await applyCaptureHistory(buffer, captureRoot);
    const sessionTotals = (session: string) => buffer.database.prepare(`select sum(input_tokens) as input,
      sum(output_tokens) as output from buffered_events where session_id=?`).get(session) as
        { input: number; output: number };
    check("partial_tailer_session_imports_only_missing_usage", applied.importedRows === 5 &&
      sessionTotals(partial).input === 150 && sessionTotals(partial).output === 15);
    check("never_captured_session_imports_once", sessionTotals(missing).input === 150 &&
      sessionTotals(missing).output === 15);
    check("legacy_live_marker_does_not_erase_native_responses",
      sessionTotals(live).input === 150 && sessionTotals(live).output === 15);
    const rerun = await applyCaptureHistory(buffer, captureRoot);
    check("rerun_imports_zero", rerun.importedRows === 0);
    fs.appendFileSync(files[1]!, `${JSON.stringify({ type: "event_msg",
      timestamp: "2026-01-04T00:00:00.000Z", payload: { type: "token_count",
        info: { total_token_usage: { input_tokens: 200, output_tokens: 20 } } } })}\n`);
    const grownPlan = await planCaptureHistory(buffer.database, captureRoot);
    const grownApply = await applyCaptureHistory(buffer, captureRoot);
    check("post_fence_append_does_not_change_import", grownPlan.missingRows === 0 &&
      grownApply.importedRows === 0);
    const partialFirst = deterministicEventId(["codex-rollout", partial, "1"]);
    buffer.database.prepare(`update buffered_events set input_tokens=101 where id=?`).run(partialFirst);
    let conflictRefused = false;
    try { await planCaptureHistory(buffer.database, captureRoot); }
    catch (error) { conflictRefused = String(error).includes("existing_event_conflict"); }
    check("existing_id_with_different_usage_refuses", conflictRefused);
    buffer.database.prepare(`update buffered_events set input_tokens=100 where id=?`).run(partialFirst);
    const ownerStart = spawnSync("/bin/ps", ["-p", String(process.pid), "-o", "lstart="],
      { encoding: "utf8", env: { ...process.env, TZ: "UTC", LC_ALL: "C" } }).stdout.trim();
    buffer.database.prepare(`insert into capture_history_import_lock
      (singleton,root_id,owner_pid,owner_start) values (1,'another-root',?,?)`)
      .run(process.pid, ownerStart);
    const beforeBlockedPrune = (buffer.database.prepare(`select count(*) as n from buffered_events`)
      .get() as { n: number }).n;
    let pruneBlocked = false;
    try { buffer.prune(1, { now: new Date("2026-02-01T00:00:00.000Z"), maxRows: 10 }); }
    catch (error) { pruneBlocked = String(error).includes("capture_history_import_active"); }
    check("retention_cannot_erase_evidence_during_import", pruneBlocked &&
      (buffer.database.prepare(`select count(*) as n from buffered_events`).get() as { n: number }).n ===
        beforeBlockedPrune);
    let concurrentPlanRefused = false;
    try { await planCaptureHistory(buffer.database, captureRoot); }
    catch (error) { concurrentPlanRefused = String(error).includes("another_import_holds_ledger"); }
    check("concurrent_import_refuses_dry_run", concurrentPlanRefused);
    let concurrentRefused = false;
    try { await applyCaptureHistory(buffer, captureRoot); }
    catch (error) { concurrentRefused = String(error).includes("import_in_progress"); }
    check("concurrent_import_refuses_ledger", concurrentRefused);
    buffer.database.prepare(`update capture_history_import_lock set owner_pid=0 where singleton=1`).run();
    let unknownHolderRefused = false;
    try { await applyCaptureHistory(buffer, captureRoot); }
    catch (error) { unknownHolderRefused = String(error).includes("import_holder_identity_unknown"); }
    check("unknown_lock_holder_fails_closed", unknownHolderRefused);
    buffer.database.prepare(`delete from capture_history_import_lock`).run();
    buffer.database.prepare(`insert into maintenance_state(key,value,updated_at)
      values ('session_summary_legacy_rebuild_v1','{"phase":"running"}',?)`).run(BASELINE);
    let rebuildPlanRefused = false;
    try { await planCaptureHistory(buffer.database, captureRoot); }
    catch (error) { rebuildPlanRefused = String(error).includes("maintenance_rebuild_active_or_unknown"); }
    check("active_rebuild_refuses_dry_run", rebuildPlanRefused);
    let rebuildRefused = false;
    try { await applyCaptureHistory(buffer, captureRoot); }
    catch (error) { rebuildRefused = String(error).includes("maintenance_rebuild_active_or_unknown"); }
    check("active_rebuild_refuses_import", rebuildRefused);
    buffer.database.prepare(`delete from maintenance_state where key='session_summary_legacy_rebuild_v1'`).run();
    const unfenced = codexFile(directory, "019d0000-0000-7000-8000-000000000013", [[100, 10]]);
    fs.utimesSync(unfenced, new Date("2026-01-02T00:00:00.000Z"),
      new Date("2026-01-02T00:00:00.000Z"));
    let unfencedRefused = false;
    try { await planCaptureHistory(buffer.database, captureRoot); }
    catch (error) { unfencedRefused = String(error).includes("unfenced_pre_enrollment_file"); }
    check("unfenced_pre_enrollment_file_refuses_root", unfencedRefused);
    fs.rmSync(unfenced);

    const hookOnly = "019d0000-0000-7000-8000-000000000012";
    sealExtra(buffer, "codex", codexFile(directory, hookOnly, [[100, 10], [150, 15]]));
    buffer.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at,
        session_id,input_tokens,output_tokens)
      values ('fixture-keyed-hook', 'codex', 'hook_event', 'metadata',
        '2026-01-02T00:00:02.000Z', '{}', '[]', '2026-01-02T00:00:03.000Z', ?, 150, 15)`)
      .run(hookOnly);
    const hookPlan = await planCaptureHistory(buffer.database, captureRoot);
    check("unqualified_keyed_hook_cannot_cover_native_responses", hookPlan.missingRows === 2 &&
      hookPlan.skippedLiveSessions === 0);
    const hookImport = await applyCaptureHistory(buffer, captureRoot);
    assert.equal(hookImport.importedRows, 2);
    assert.equal((buffer.database.prepare(`select sum(input_tokens) as n from buffered_events
      where session_id=? and event_type='usage_rollout'`).get(hookOnly) as { n: number | null } | undefined)?.n, 150);

    const pruned = "019d0000-0000-7000-8000-000000000004";
    const prunedFile = codexFile(directory, pruned, [[100, 10], [150, 15]]);
    sealExtra(buffer, "codex", prunedFile);
    const prunedId = deterministicEventId(["codex-rollout", pruned, "1"]);
    buffer.database.prepare(`insert into raw_retention_receipts
      (event_id, raw_rowid, raw_created_at, raw_generation, expired_at, reason)
      values (?, 987654, '2026-01-02T00:00:03.000Z', 'fixture',
        '2026-01-04T00:00:00.000Z', 'retention_window_elapsed')`).run(prunedId);
    const prunedPlan = await planCaptureHistory(buffer.database, captureRoot);
    check("pruned_tailer_receipt_is_existing_capture", prunedPlan.missingRows === 1 && prunedPlan.existingRows >= 1);
    const prunedImport = await applyCaptureHistory(buffer, captureRoot);
    check("pruned_tailer_row_is_not_reimported", prunedImport.importedRows === 1 &&
      !buffer.database.prepare(`select 1 from buffered_events where id=?`).get(prunedId));
    // Current tailers retain a root observation alongside the retention
    // receipt. Add it after proving the receipt-only compatibility path.
    buffer.database.prepare(`insert into capture_root_observations
      (root_digest,event_id,payload_digest,observed_at,state) values (?,?,?,?,?)`)
      .run(captureRootDigest(captureRoot), prunedId, codexSightingDigest(pruned, prunedId),
        "2026-01-02T00:00:02.000Z", "admitted");

    const uncertain = "019d0000-0000-7000-8000-000000000005";
    const uncertainFile = codexFile(directory, uncertain, [[100, 10], [150, 15]]);
    sealExtra(buffer, "codex", uncertainFile);
    buffer.database.prepare(`insert into raw_retention_receipts
      (event_id, raw_rowid, raw_created_at, raw_generation, expired_at, reason)
      values ('opaque-pruned-live-id', 987655, '2026-01-02T00:00:03.000Z', 'fixture',
        '2026-01-04T00:00:00.000Z', 'retention_window_elapsed')`).run();
    let ambiguityRefused = false;
    try { await planCaptureHistory(buffer.database, captureRoot); }
    catch (error) { ambiguityRefused = String(error).includes("unattributed_pruned_rows"); }
    check("unattributed_pruned_row_refuses_root", ambiguityRefused);
    buffer.database.prepare(`delete from raw_retention_receipts where event_id='opaque-pruned-live-id'`).run();
    buffer.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at,
        input_tokens,output_tokens)
      values ('opaque-unkeyed-hook', 'codex', 'hook_event', 'metadata',
        '2026-01-02T00:00:02.000Z', '{}', '[]', '2026-01-02T00:00:03.000Z', 1, 1)`)
      .run();
    let unkeyedRefused = false;
    try { await planCaptureHistory(buffer.database, captureRoot); }
    catch (error) { unkeyedRefused = String(error).includes("unkeyed_live_usage_overlap"); }
    check("unqualified_unkeyed_hook_cannot_erase_native_responses", !unkeyedRefused &&
      (await planCaptureHistory(buffer.database, captureRoot)).missingRows === 2);
    buffer.database.prepare(`delete from buffered_events where id='opaque-unkeyed-hook'`).run();

    const observedOnly = "019d0000-0000-7000-8000-000000000007";
    const outboxOnly = "019d0000-0000-7000-8000-000000000008";
    const uploadedOnly = "019d0000-0000-7000-8000-000000000009";
    for (const session of [observedOnly, outboxOnly, uploadedOnly])
      sealExtra(buffer, "codex", codexFile(directory, session, [[100, 10], [150, 15]]));
    const observedId = deterministicEventId(["codex-rollout", observedOnly, "1"]);
    buffer.database.prepare(`insert into capture_root_observations
      (root_digest,event_id,payload_digest,observed_at,state) values (?,?,?,?,?)`)
      .run(captureRootDigest(captureRoot), observedId, codexSightingDigest(observedOnly, observedId),
        "2026-01-02T00:00:02.000Z", "admitted");
    const outboxId = deterministicEventId(["codex-rollout", outboxOnly, "1"]);
    buffer.database.prepare(`insert into upload_outbox
      (delivery_id,base_envelope_json,base_bytes,state,next_attempt_at,created_at,updated_at)
      values (?, '{}', 2, 'pending', ?, ?, ?)`)
      .run(outboxId, BASELINE, BASELINE, BASELINE);
    const uploadedId = deterministicEventId(["codex-rollout", uploadedOnly, "1"]);
    buffer.database.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?, 'acknowledged', 'fixture', '2xx', 1, ?, ?)`)
      .run(uploadedId, BASELINE, BASELINE);
    const deliveryPlan = await planCaptureHistory(buffer.database, captureRoot);
    check("durable_dedupe_receipts_count_as_existing", deliveryPlan.existingRows >= 6 &&
      deliveryPlan.missingRows === 5);
    const deliveryApply = await applyCaptureHistory(buffer, captureRoot);
    const onlySecond = [observedOnly, outboxOnly, uploadedOnly].every(session => {
      const row = buffer.database.prepare(`select count(*) as n,sum(input_tokens) as input
        from buffered_events where session_id=?`).get(session) as { n: number; input: number };
      return row.n === 1 && row.input === 50;
    });
    check("observed_outbox_and_upload_receipts_dedupe", deliveryApply.importedRows === 5 && onlySecond);
    buffer.database.prepare(`update capture_root_observations set state='conflict' where event_id=?`)
      .run(observedId);
    let sightingConflictRefused = false;
    try { await planCaptureHistory(buffer.database, captureRoot); }
    catch (error) { sightingConflictRefused = String(error).includes("prior_root_observation_conflict"); }
    check("conflicted_source_receipt_refuses_root", sightingConflictRefused);
    buffer.database.prepare(`update capture_root_observations set state='admitted' where event_id=?`)
      .run(observedId);
    buffer.database.prepare(`update capture_root_observations set payload_digest='wrong' where event_id=?`)
      .run(observedId);
    let sightingDigestRefused = false;
    try { await planCaptureHistory(buffer.database, captureRoot); }
    catch (error) { sightingDigestRefused = String(error).includes("prior_root_observation_conflict"); }
    check("mismatched_source_receipt_refuses_root", sightingDigestRefused);
    buffer.database.prepare(`update capture_root_observations set payload_digest=? where event_id=?`)
      .run(codexSightingDigest(observedOnly, observedId), observedId);
    buffer.database.prepare(`insert into raw_retention_receipts
      (event_id,raw_rowid,raw_created_at,raw_generation,expired_at,reason)
      values (?, 987656, ?, 'fixture', ?, 'retention_window_elapsed')`)
      .run(observedId, BASELINE, BASELINE);

    const claudeSession = "019d0000-0000-7000-8000-000000000010";
    const claudeDirectory = path.join(home, ".claude-profiles", "missed", "projects");
    seal(buffer, "claude_code", [claudeFile(claudeDirectory, claudeSession)]);
    const claudeRoot: CaptureRoot = { ...deriveCaptureRootIdentity("fixture", "claude_code", claudeDirectory),
      source: "claude_code", directory: claudeDirectory, installationEpochId: epoch };
    const claudeFirst = deterministicEventId(["claude-transcript", claudeSession, "message-a"]);
    buffer.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at,
        session_id,model,input_tokens,output_tokens,uploaded_at)
      values (?, 'claude_code', 'usage_transcript', 'metadata', '2026-01-02T00:00:02.000Z',
        '{}', '[]', '2026-01-02T00:00:03.000Z', ?, 'claude-sonnet-4-5', 100, 10,
        '2026-01-02T00:00:04.000Z')`)
      .run(claudeFirst, claudeSession);
    buffer.database.prepare(`insert into session_usage_authority values
      ('claude_code', ?, 'tailer', '2026-01-02T00:00:03.000Z')`).run(claudeSession);
    const claudePlan = await planCaptureHistory(buffer.database, claudeRoot);
    check("other_root_observed_pruned_receipt_is_attributed", claudePlan.missingRows === 2);
    const claudeApply = await applyCaptureHistory(buffer, claudeRoot);
    const claudeTotal = buffer.database.prepare(`select sum(input_tokens) as input,sum(output_tokens) as output
      from buffered_events where session_id=?`).get(claudeSession) as { input: number; output: number };
    check("claude_revisions_import_only_missing_usage", claudeApply.importedRows === 2 &&
      claudeTotal.input === 190 && claudeTotal.output === 19);
    // This case checks an opaque filename's declared, previously unseen
    // session. Seen-session fragments are refused by the named F3 proofs.
    const agentSession = "019d0000-0000-7000-8000-000000000777";
    const agentFile = path.join(claudeDirectory, "agent-opaque.jsonl");
    fs.writeFileSync(agentFile, `${JSON.stringify({ type: "assistant", sessionId: agentSession,
      timestamp: "2026-01-02T00:00:05.000Z", message: { id: "message-c",
        usage: { input_tokens: 20, output_tokens: 2 } } })}\n`, { mode: 0o600 });
    sealExtra(buffer, "claude_code", agentFile);
    const agentPlan = await planCaptureHistory(buffer.database, claudeRoot);
    check("claude_agent_file_uses_declared_session", agentPlan.missingRows === 1);
    const agentApply = await applyCaptureHistory(buffer, claudeRoot);
    check("claude_agent_file_imports_once", agentApply.importedRows === 1 &&
      (buffer.database.prepare(`select sum(input_tokens) as input from buffered_events where session_id in (?,?)`)
        .get(claudeSession, agentSession) as { input: number }).input === 210);

    const duplicate = "019d0000-0000-7000-8000-000000000011";
    const original = codexFile(directory, duplicate, [[100, 10], [150, 15]]);
    const duplicateDay = path.join(directory, "2026", "01", "04");
    fs.mkdirSync(duplicateDay, { recursive: true, mode: 0o700 });
    const copy = path.join(duplicateDay, `rollout-2026-01-04T00-00-00-${duplicate}.jsonl`);
    fs.copyFileSync(original, copy);
    sealExtra(buffer, "codex", original);
    sealExtra(buffer, "codex", copy);
    const duplicatePlan = await planCaptureHistory(buffer.database, captureRoot);
    check("same_source_event_across_files_plans_once", duplicatePlan.missingRows === 2);
    const duplicateApply = await applyCaptureHistory(buffer, captureRoot);
    check("same_source_event_across_files_imports_once", duplicateApply.importedRows === 2 &&
      (buffer.database.prepare(`select count(*) as n from buffered_events where session_id=?`)
        .get(duplicate) as { n: number }).n === 2);

    const beforeClaim = captureFrontier(buffer.database);
    ensureCaptureFrontierSchema(buffer.database);
    const binding = buffer.workspaceBinding()!;
    const claimAt = new Date(Date.parse(binding.currentInstallationEpochStartedAt!) + 2 * 60 * 60 * 1000).toISOString();
    for (const source of CAPTURE_FRONTIER_SOURCES) buffer.database.prepare(`insert into capture_coverage_state
      (workspace_id,installation_epoch_id,source,checked_at,complete_through,updated_at)
      values (?,?,?,?,?,?)`).run(WORKSPACE, epoch, source, claimAt, claimAt, claimAt);
    const liveThrough = captureFrontier(buffer.database)?.capturedThrough;
    check("fixture_live_frontier_established", beforeClaim?.capturedThrough === null && liveThrough === claimAt);
    const claimSpool = { pendingFiles: 0, oldestPendingMs: null, losses: [], unreadable: false };
    buffer.database.prepare(`update upload_control set migration_complete=1 where singleton=1`).run();
    const claimBefore = buffer.delivery.captureClaim([], claimSpool, new Date(claimAt));

    const crash = "019d0000-0000-7000-8000-000000000006";
    const crashAmounts = Array.from({ length: 20 }, (_, index) => [100 + index * 10, 10 + index] as [number, number]);
    const crashFile = codexFile(directory, crash, crashAmounts);
    sealExtra(buffer, "codex", crashFile);
    let crashed = false;
    try { await applyCaptureHistory(buffer, captureRoot, { stopAfterSlices: 1 }); }
    catch (error) { crashed = String(error).includes("capture_history_injected_crash"); }
    check("injected_crash_after_committed_slice", crashed);
    const committedBeforeRecovery = (buffer.database.prepare(`select count(*) as n from buffered_events
      where session_id=?`).get(crash) as { n: number }).n;
    buffer.database.prepare(`update capture_history_import_lock set owner_pid=999999
      where singleton=1`).run();
    const originalCrashBytes = fs.readFileSync(crashFile);
    const changedCrashBytes = originalCrashBytes.toString("utf8").replace('"input_tokens":290', '"input_tokens":291');
    assert.notEqual(changedCrashBytes, originalCrashBytes.toString("utf8"));
    fs.writeFileSync(crashFile, changedCrashBytes);
    let changedSourceRefused = false;
    try { await applyCaptureHistory(buffer, captureRoot); }
    catch (error) { changedSourceRefused = String(error).includes("fenced_history_changed_since_import"); }
    check("crash_retry_refuses_changed_fenced_bytes", changedSourceRefused);
    fs.writeFileSync(crashFile, originalCrashBytes);
    let changedWindowRefused = false;
    try { await applyCaptureHistory(buffer, captureRoot, { since: "2026-01-02T00:00:05.000Z" }); }
    catch (error) { changedWindowRefused = String(error).includes("fenced_history_changed_since_import"); }
    check("crash_retry_refuses_changed_since_window", changedWindowRefused);
    const recovered = await applyCaptureHistory(buffer, captureRoot);
    const crashRows = (buffer.database.prepare(`select count(*) as n, sum(input_tokens) as input
      from buffered_events where session_id=?`).get(crash) as { n: number; input: number });
    check("crash_resumes_exactly_once", recovered.importedRows === 20 - committedBeforeRecovery &&
      crashRows.n === 20 &&
      crashRows.input === crashAmounts.at(-1)![0]);
    const claimAfter = buffer.delivery.captureClaim([], claimSpool, new Date(claimAt));
    check("import_does_not_move_live_claim_watermark", claimBefore?.through === claimAt &&
      claimAfter?.through === claimAt && captureFrontier(buffer.database)?.capturedThrough === claimAt);

    const cfg = collectorConfigSchema.parse({ tenantId: WORKSPACE, deviceId: DEVICE,
      installKey: "fixture-import-upload", uploadUrl: "http://127.0.0.1:49600/ingest",
      captureRoots: [captureRoot], delivery: { maxOldestAgeDays: 3650 } });
    const deliveries = new Map<string, string>();
    const deliveryCounts = new Map<string, number>();
    const fetchImpl = acknowledgingFetch(async (_url, init) => {
      const body = JSON.parse(String(init?.body ?? "{}")) as { events?: Array<{ event?: { id?: string; observedAt?: string } }> };
      for (const row of body.events ?? []) {
        if (row.event?.id && row.event.observedAt) {
          deliveries.set(row.event.id, row.event.observedAt);
          deliveryCounts.set(row.event.id, (deliveryCounts.get(row.event.id) ?? 0) + 1);
        }
      }
      return new Response(JSON.stringify({ accepted: body.events?.length ?? 0 }),
        { status: 200, headers: { "content-type": "application/json" } });
    });
    // Native history still observes the durable model-evidence hold. Move
    // the fixture clock past it; observedAt and every accounting assertion stay fixed.
    const flushAt = new Date(Date.now() + CODEX_MODEL_WAIT_MS + 1);
    for (let cycle = 0; cycle < 10; cycle += 1) {
      const sent = await uploadBufferedEvents(cfg, buffer, { fetchImpl, now: () => flushAt });
      if (sent.remainingDelivery === 0) break;
    }
    const importedDeliveryId = deterministicEventId(["codex-rollout", missing, "2"]);
    const uploadedOnce = deliveries.get(importedDeliveryId);
    check("imported_row_uploads_with_original_time", uploadedOnce === "2026-01-02T00:00:03.000Z");
    const sentCount = [...deliveryCounts.values()].reduce((sum, count) => sum + count, 0);
    await uploadBufferedEvents(cfg, buffer, { fetchImpl, now: () => flushAt });
    check("imported_row_uploads_once", deliveryCounts.get(importedDeliveryId) === 1 &&
      [...deliveryCounts.values()].reduce((sum, count) => sum + count, 0) === sentCount);

    fs.writeFileSync(path.join(fixture.env.PLIMSOLL_HOME, "collector.config.json"),
      `${JSON.stringify(collectorConfigSchema.parse({ tenantId: WORKSPACE, deviceId: DEVICE,
        installKey: "fixture-import-command", captureRoots: [captureRoot, claudeRoot] }))}\n`, { mode: 0o600 });
    const cli = path.join(import.meta.dirname, "..", "packages", "collector-cli", "src", "cli.ts");
    const invoke = (args: string[]) => spawnSync(process.execPath,
      ["--import", "tsx", cli, "capture-roots", "import-history", ...args],
      { cwd: path.join(import.meta.dirname, ".."), env: { ...process.env, ...fixture.env }, encoding: "utf8" });
    const cliPreview = invoke(["--root", captureRoot.rootId, "--json"]);
    const previewJson = JSON.parse(cliPreview.stdout || "{}") as { missingRows?: number; status?: string };
    check("explicit_cli_defaults_to_dry_run", cliPreview.status === 0 &&
      previewJson.status === "capture_roots_history_plan" && previewJson.missingRows === 0 &&
      !cliPreview.stdout.includes(directory));
    const cliApply = invoke(["--root", captureRoot.rootId, "--apply", "--json"]);
    const applyJson = JSON.parse(cliApply.stdout || "{}") as { importedRows?: number; receiptPath?: string };
    check("explicit_cli_apply_is_idempotent", cliApply.status === 0 && applyJson.importedRows === 0 &&
      Boolean(applyJson.receiptPath && fs.existsSync(applyJson.receiptPath)));
    const malformed = invoke(["--root", captureRoot.rootId, "--since", "--apply"]);
    check("cli_refuses_missing_since_value", malformed.status !== 0 &&
      malformed.stdout.includes("capture_roots_history_refused"));
    buffer.database.prepare(`update buffered_events set created_at='2026-01-02T00:00:04.000Z'
      where session_id=?`).run(missing);
    buffer.prune(1, { now: new Date("2026-02-01T00:00:00.000Z"), maxRows: 1000 });
    const expired = (buffer.database.prepare(`select count(*) as n from raw_retention_receipts
      where event_id in (?,?)`).get(
        deterministicEventId(["codex-rollout", missing, "1"]),
        deterministicEventId(["codex-rollout", missing, "2"]),
      ) as { n: number }).n;
    const afterPrune = await applyCaptureHistory(buffer, captureRoot);
    check("uploaded_then_pruned_import_stays_deduped", expired === 2 &&
      afterPrune.importedRows === 0);
    console.log(`capture-history-import-proof: ${checks.length} checks green`);
    for (const name of checks) console.log(`  ok ${name}`);
  } finally {
    buffer.close();
    fixture.restore();
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
