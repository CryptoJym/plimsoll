/** Temporary receipts and loopback server only; no provider, live ledger or hooks. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";
import Database from "better-sqlite3";
import { readJevAnalysis } from "../packages/collector-cli/src/jev-analysis";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";

async function main() {
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-jev-proof-"));
const databasePath = path.join(root, "inference.sqlite3");
const nowMs = Date.now();
const seconds = nowMs / 1000 - 10;
const sha = (value: string) => crypto.createHash("sha256").update(value).digest("hex");
const privateText = "PRIVATE_TASK_PROVIDER_ANSWER_OR_PATH";
const db = new Database(databasePath);
db.exec(`
  CREATE TABLE auto_events(id TEXT PRIMARY KEY,session_key TEXT,created REAL,state TEXT,receipt TEXT);
  CREATE TABLE auto_decisions(id TEXT PRIMARY KEY,session_key TEXT,mode TEXT,created REAL,result TEXT);
  CREATE TABLE auto_outcomes(id TEXT PRIMARY KEY,decision_id TEXT,created REAL,record TEXT);
  CREATE TABLE auto_offers(decision_id TEXT PRIMARY KEY,event_id TEXT,offered_at REAL);
  CREATE TABLE auto_sessions(key TEXT PRIMARY KEY,state TEXT);
  CREATE TABLE calls(key TEXT PRIMARY KEY,state TEXT,reserved REAL,estimated REAL,record TEXT);
`);
db.prepare("INSERT INTO auto_sessions VALUES (?,?)").run("private", privateText);
db.prepare("INSERT INTO calls VALUES (?,?,?,?,?)").run("private", "returned_valid", 0, 0, privateText);
let checks = 0;
const check = (name: string, test: () => void) => {
  test(); checks++; console.log(JSON.stringify({ check: name, passed: true }));
};
function add(name: string, options: { created?: number; session?: string; request?: string; result?: Record<string, unknown> } = {}) {
  const id = sha(name), eventId = sha(name + ":event"), sessionKey = sha("native-session");
  const created = options.created ?? seconds;
  const result = { schema: "jev-decision/v1", mode: "rescue", input_sha256: sha("input"),
    event_id: eventId, execution_authorized: false, holds: [],
    decision: { recommendation: "inspect_evidence", answer: privateText },
    inference: { state: "returned_valid", model: "jev", requested_model: "jev-1.13.0", request_sha256: sha(options.request ?? "shared-request"),
      cache_hit: false, usage: { input_tokens: 100, output_tokens: 5 }, latency_ms: 12,
      estimated_cost_usd: 0.0001, billed_cost_usd: null, answers: privateText },
    ...options.result };
  db.prepare("INSERT INTO auto_events VALUES (?,?,?,?,?)").run(eventId, sessionKey, created - 1, "complete", JSON.stringify({
    runtime: "claude-code", session: options.session ?? "native-session-123", workspace_sha256: sha("workspace"),
    prompt: privateText, tool: privateText,
  }));
  db.prepare("INSERT INTO auto_decisions VALUES (?,?,?,?,?)").run(id, sessionKey, "rescue", created, JSON.stringify(result));
  return { id, eventId, sessionKey, result };
}
const read = () => readJevAnalysis({ databasePath, nowMs, days: 30 });
const first = add("first", { session: "s".repeat(180) });
db.prepare("INSERT INTO auto_offers VALUES (?,?,?)").run(first.id, first.eventId, seconds + 1);
db.prepare("INSERT INTO auto_outcomes VALUES (?,?,?,?)").run(sha("outcome"), first.id, seconds + 2,
  JSON.stringify({ decision_id: first.id, action: "accepted", evidence_ref: privateText, verified_savings: 999 }));
add("cached", { result: { inference: { ...(first.result.inference as object), cache_hit: true } } });

try {
  check("native_join_privacy_and_owner_report_are_preserved_without_false_acceptance", () => {
    const snapshot = read();
    assert.equal(snapshot.state, "available");
    assert.equal(snapshot.scope, "local_machine");
    assert.equal(snapshot.decisions.length, 2);
    assert(!JSON.stringify(snapshot).includes(privateText));
    const decision = snapshot.decisions.find(item => item.id === first.id)!;
    assert.equal(decision.native.sessionId.length, 180);
    assert.equal(decision.native.eventId, first.eventId);
    assert.equal(decision.inference.requestedModel, "jev-1.13.0");
    assert(decision.offeredAt);
    assert.equal(decision.reportedActions[0].action, "accepted");
    assert.equal(decision.verifiedAcceptance, null);
    assert.equal(decision.measuredSavingsUsd, null);
    assert.equal(decision.workAttribution, "unavailable");
  });
  check("cached_and_repeated_decisions_do_not_multiply_request_cost", () => {
    assert.equal(read().overhead.uniqueRequests, 1);
    assert.equal(read().overhead.estimatedCostUsd, 0.0001);
    assert.equal(read().overhead.billedCostUsd, null);
  });
  check("conflicting_request_cost_stays_unknown", () => {
    const record = add("conflict", { result: { inference: { ...(first.result.inference as object), estimated_cost_usd: 0.001 } } });
    assert.equal(read().overhead.estimatedCostUsd, null);
    db.prepare("DELETE FROM auto_decisions WHERE id=?").run(record.id);
  });
  check("unknown_inference_and_incomplete_event_are_not_replayed_or_promoted", () => {
    const record = add("unknown", { result: { inference: { state: "unknown", request_sha256: sha("unknown") } } });
    db.prepare("UPDATE auto_events SET state='started' WHERE id=?").run(record.eventId);
    const snapshot = read();
    const decision = snapshot.decisions.find(item => item.id === record.id)!;
    assert.equal(decision.native.eventState, "started");
    assert.equal(decision.inference.inputTokens, null);
    assert.equal(decision.offeredAt, null);
    assert.equal(snapshot.overhead.estimatedCostUsd, null);
    db.prepare("DELETE FROM auto_decisions WHERE id=?").run(record.id);
  });
  for (const kind of ["session", "event", "offer", "outcome", "future", "content", "oversized", "order"]) {
    check("rejects_" + kind + "_mismatch_without_losing_other_valid_decisions", () => {
      const record = add(kind, kind === "future" ? { created: seconds + 1000 } : {});
      if (kind === "session") db.prepare("UPDATE auto_events SET session_key=? WHERE id=?").run(sha("other"), record.eventId);
      if (kind === "event") db.prepare("DELETE FROM auto_events WHERE id=?").run(record.eventId);
      if (kind === "offer") db.prepare("INSERT INTO auto_offers VALUES (?,?,?)").run(record.id, sha("absent"), seconds + 1);
      if (kind === "outcome") db.prepare("INSERT INTO auto_outcomes VALUES (?,?,?,?)").run(sha("bad-outcome"), record.id, seconds + 1,
        JSON.stringify({ decision_id: first.id, action: "accepted", evidence_ref: "somewhere" }));
      if (kind === "content") db.prepare("UPDATE auto_decisions SET result=? WHERE id=?").run(
        JSON.stringify({ ...record.result, decision: { recommendation: '<img onerror="attack()">' } }), record.id);
      if (kind === "oversized") db.prepare("UPDATE auto_decisions SET result=? WHERE id=?").run("x".repeat(65_000), record.id);
      if (kind === "order") db.prepare("UPDATE auto_events SET created=? WHERE id=?").run(seconds + 1, record.eventId);
      const snapshot = read();
      assert.equal(snapshot.state, "partial");
      assert.equal(snapshot.coverage.rejected, kind === "outcome" ? 0 : 1);
      assert.equal(snapshot.coverage.rejectedOutcomes, kind === "outcome" ? 1 : 0);
      assert.equal(snapshot.decisions.length, kind === "outcome" ? 3 : 2);
      if (kind === "outcome") assert.equal(snapshot.decisions.find(item => item.id === record.id)?.reportedActions.length, 0);
      db.prepare("DELETE FROM auto_decisions WHERE id=?").run(record.id);
    });
  }
  check("absent_unsupported_and_oversized_sources_fail_explicitly", () => {
    assert.equal(readJevAnalysis({ databasePath: path.join(root, "absent") }).state, "not_installed");
    assert.equal(readJevAnalysis({ databasePath, days: 0 }).reason, "unsupported_window");
    const large = path.join(root, "oversized.sqlite3");
    fs.writeFileSync(large, ""); fs.truncateSync(large, 65 * 1024 * 1024);
    assert.equal(readJevAnalysis({ databasePath: large }).reason, "source_outside_read_bounds");
    const link = path.join(root, "linked.sqlite3"); fs.symlinkSync(databasePath, link);
    assert.equal(readJevAnalysis({ databasePath: link }).reason, "source_outside_read_bounds");
  });
  check("bounded_recent_decisions_and_outcomes_report_truncation", () => {
    for (let index = 0; index < 55; index++) add("limit-" + index);
    const snapshot = read();
    assert.equal(snapshot.state, "partial");
    assert.equal(snapshot.decisions.length, 50);
    assert.equal(snapshot.coverage.truncated, true);
    db.prepare("DELETE FROM auto_decisions WHERE id NOT IN (?,?)").run(first.id, sha("cached"));
    for (let index = 0; index < 12; index++) db.prepare("INSERT INTO auto_outcomes VALUES (?,?,?,?)").run(sha("many-" + index), first.id,
      seconds + 3, JSON.stringify({ decision_id: first.id, action: "used", evidence_ref: "source" }));
    const decision = read().decisions.find(item => item.id === first.id)!;
    assert.equal(decision.reportedActions.length, 10);
    assert.equal(decision.outcomeRecordsTruncated, true);
  });
  check("reads_do_not_change_source_database", () => {
    const before = sha(fs.readFileSync(databasePath).toString("base64"));
    read(); read();
    assert.equal(sha(fs.readFileSync(databasePath).toString("base64")), before);
  });
  check("busy_source_fails_bounded_and_recovers_after_unlock", () => {
    db.exec("BEGIN EXCLUSIVE");
    const started = performance.now();
    try {
      const snapshot = read();
      assert.equal(snapshot.state, "unavailable");
      assert.equal(snapshot.reason, "receipt_store_busy");
      assert.equal(snapshot.decisions.length, 0);
      assert(performance.now() - started < 2_000);
    } finally { db.exec("ROLLBACK"); }
    assert.equal(read().state, "available");
  });
  check("window_excludes_old_receipts_without_claiming_hooks_inactive", () => {
    const record = add("old", { created: seconds - 31 * 86400 });
    assert(!read().decisions.some(item => item.id === record.id));
    db.prepare("DELETE FROM auto_decisions WHERE id=?").run(record.id);
  });

  const auth = loadOrCreateLocalIngestAuth(path.join(root, "auth"));
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, { localAuth: auth, jevDatabasePath: databasePath });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  try {
    const unauthenticated = await fetch(origin + "/api/jev-analysis");
    const producer = await fetch(origin + "/api/jev-analysis", { headers: { "x-plimsoll-token": auth.codexProducer } });
    check("only_management_credential_can_read_native_jev_sessions", () => {
      assert.equal(unauthenticated.status, 401);
      assert.equal(producer.status, 401);
    });
    const response = await fetch(origin + "/api/jev-analysis", { headers: { "x-plimsoll-token": auth.managementRead } });
    const snapshot = await response.json();
    check("authenticated_route_works_while_regular_projection_is_unavailable", () => {
      assert.equal(response.status, 200);
      assert.equal(response.headers.get("cache-control"), "no-store");
      assert.equal(snapshot.schema, "plimsoll.jev-analysis.v1");
      assert.equal(snapshot.decisions.length, 2);
      assert(!JSON.stringify(snapshot).includes(privateText));
    });
    const invalid = await fetch(origin + "/api/jev-analysis?days=0", { headers: { "x-plimsoll-token": auth.managementRead } });
    check("unsupported_window_is_not_silently_defaulted", () => assert.equal(invalid.status, 400));
    const changed = await fetch(origin + "/api/jev-analysis?days=90", { headers: { "x-plimsoll-token": auth.managementRead } });
    const changedBody = await changed.json();
    check("changing_window_does_not_bypass_source_read_interval", () => {
      assert.equal(changed.status, 503);
      assert.equal(changedBody.error, "jev_refresh_pending");
      assert(Number(changed.headers.get("retry-after")) <= 15);
      assert(!("decisions" in changedBody));
    });
    const html = await (await fetch(origin)).text();
    check("dashboard_uses_existing_text_only_rendering_and_csp", () => {
      assert(html.includes('id="jev-plate"'));
      assert(html.includes("renderJevAnalysis"));
      assert(!/\.innerHTML\s*=|insertAdjacentHTML|document\.write\(/.test(html));
    });
  } finally {
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    buffer.close();
  }
  const legacyBuffer = new LocalEventBuffer(path.join(root, "legacy.sqlite"));
  const legacy = createCollectorServer(collectorConfigSchema.parse({}), legacyBuffer, { jevDatabasePath: databasePath });
  await new Promise<void>(resolve => legacy.listen(0, "127.0.0.1", resolve));
  try {
    const response = await fetch(`http://127.0.0.1:${(legacy.address() as AddressInfo).port}/api/jev-analysis`);
    const body = await response.json();
    check("legacy_no_auth_install_never_discloses_native_session", () => {
      assert.equal(response.status, 503);
      assert.deepEqual(body, { error: "management_auth_required" });
    });
  } finally {
    await new Promise<void>(resolve => legacy.close(() => resolve()));
    legacyBuffer.close();
  }
  console.log(JSON.stringify({ proof: "jev-analysis", passed: checks, failed: 0 }));
} finally {
  db.close();
  fs.rmSync(root, { recursive: true, force: true });
}
}
main().catch(error => { console.error(error); process.exitCode = 1; });
