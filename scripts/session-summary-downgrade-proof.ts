import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { ensureSessionSummarySchema, updateSessionSummary } from "../packages/collector-cli/src/session-summary";
import { collectSessionSnapshots } from "../packages/collector-cli/src/session-sync";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-summary-downgrade", 7);
const root = process.env.PLIMSOLL_PROOF_ROOT!;
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const until = "2026-09-30T00:00:00.000Z";
const created = "2026-09-24T00:00:00.000Z";
const workspace = "00000000-0000-4000-8000-000000000742";
const session = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const event = (n: number) => `10000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const legacyTriggerNames = ["raw_update", "raw_delete", "outbox_insert", "outbox_update",
  "outbox_delete", "receipt_insert", "receipt_update", "receipt_delete"]
  .map((suffix) => `trg_session_summary_${suffix}`);

function put(buffer: LocalEventBuffer, sid: string, n: number): void {
  buffer.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json,
     suppressed_fields_json, created_at, session_id, input_tokens, output_tokens,
     workspace_id, privacy_generation)
    values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]', ?, ?, 2, 1, ?, ?)`)
    .run(event(n), created, created, sid, workspace, `generation-${n}`);
}

function terminalReceipt(buffer: LocalEventBuffer, n: number): void {
  buffer.database.prepare(`insert into upload_receipts
    (delivery_id, terminal_state, reason, status_class, attempt_count, created_at, terminal_at)
    values (?, 'dead', 'local_privacy_violation', 'local', 0, ?, ?)`)
    .run(event(n), created, created);
}

function read(db: LocalEventBuffer["database"]) {
  return async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> =>
    queries.flatMap((query) => db.prepare(query.sql).all(query.params) as T[]);
}

function triggerSql(db: LocalEventBuffer["database"], name: string): string | null {
  return (db.prepare("select sql from sqlite_master where type='trigger' and name=?")
    .get(name) as { sql: string } | undefined)?.sql ?? null;
}

function installOldSource(ref: string, name: string): string {
  const dir = path.join(root, name);
  execFileSync("git", ["worktree", "add", "--detach", "--quiet", dir, ref], { cwd: repo });
  fs.symlinkSync(path.join(repo, "node_modules"), path.join(dir, "node_modules"), "dir");
  return dir;
}

async function oldModule<T>(dir: string, file: string): Promise<T> {
  return await import(pathToFileURL(path.join(dir, "packages/collector-cli/src", file)).href) as T;
}

async function main(): Promise<void> {
  const installed: string[] = [];
  try {
    const old41 = installOldSource("03445d3ade34fb6178a1a0a018e6b92bbd2b69da", "collector-0741");
    installed.push(old41);
    const old39 = installOldSource("de20f7e", "collector-0739");
    installed.push(old39);
    const buffer41 = await oldModule<typeof import("../packages/collector-cli/src/buffer")>(old41, "buffer.ts");
    const summary41 = await oldModule<typeof import("../packages/collector-cli/src/session-summary")>(old41, "session-summary.ts");
    const sync41 = await oldModule<typeof import("../packages/collector-cli/src/session-sync")>(old41, "session-sync.ts");
    const buffer39 = await oldModule<typeof import("../packages/collector-cli/src/buffer")>(old39, "buffer.ts");
    const sync39 = await oldModule<typeof import("../packages/collector-cli/src/session-sync")>(old39, "session-sync.ts");
    const config = collectorConfigSchema.parse({ uploadUrl: "http://127.0.0.1:1/ingest",
      tenantId: workspace, installKey: "downgrade-proof", uploadSigningSecret: "fixture-only-secret" });

    // 0.7.41 must install its own old-name triggers when it starts a sync on
    // a head ledger. Those triggers advance the revision its worker fences.
    const file41 = path.join(root, "rollback-0741.sqlite");
    const sid41 = session(741);
    const tokens41 = session(742);
    const normal41 = session(740);
    const beforeInstall41 = session(744);
    const head41 = new LocalEventBuffer(file41, { workspaceId: workspace });
    ensureSessionSummarySchema(head41.database);
    put(head41, sid41, 1);
    put(head41, normal41, 3);
    put(head41, tokens41, 4);
    put(head41, beforeInstall41, 5);
    put(head41, beforeInstall41, 6);
    const partial = await updateSessionSummary(head41.database, beforeInstall41, until,
      { read: read(head41.database), maxRows: 1, maxMs: 1_000 });
    assert.equal(partial.complete, false);
    const headSql = new Map(legacyTriggerNames.map((name) =>
      [name, triggerSql(head41.database, `${name}_v42`)]));
    head41.close();

    const old = new buffer41.LocalEventBuffer(file41, { workspaceId: workspace });
    try {
      // This receipt precedes the old trigger install. The head cursor has
      // not counted row 6, so the old worker must read its new eligibility.
      terminalReceipt(old, 6);
      assert.equal(old.database.prepare(`select 1 from session_sync_summary_dirty where session_id=?`)
        .get(beforeInstall41), undefined);
      // The real 0.7.41 sync entry point installs its triggers before work.
      const warmup = await sync41.runSessionSync(config, { ledgerDb: old.database, incremental: true,
        sessionIds: [normal41], until, delayMs: 0, maxAttemptsPerBatch: 1, log: () => undefined,
        fetchImpl: (async (_input, init) => {
          const body = String(init?.body ?? "");
          return new Response(JSON.stringify(acceptedFixtureDelivery(body, config.installKey!)),
            { status: 200, headers: { "content-type": "application/json" } });
        }) as typeof fetch });
      assert.equal(warmup.sentSessions, 1);
      const oldSql = triggerSql(old.database, "trg_session_summary_raw_update");
      assert.ok(oldSql);
      const beforeInstallBodies: string[] = [];
      const resumed = await sync41.runSessionSync(config, { ledgerDb: old.database, incremental: true,
        sessionIds: [beforeInstall41], until, delayMs: 0, maxAttemptsPerBatch: 1, log: () => undefined,
        fetchImpl: (async (_input, init) => {
          const body = String(init?.body ?? "");
          beforeInstallBodies.push(body);
          return new Response(JSON.stringify(acceptedFixtureDelivery(body, config.installKey!)),
            { status: 200, headers: { "content-type": "application/json" } });
        }) as typeof fetch });
      const expectedBeforeInstall = sync41.buildSessionSyncRow(sync41.collectSessionSnapshots(old.database,
        { until, sessionIds: [beforeInstall41] })[0]!);
      assert.equal(expectedBeforeInstall.ok, true);
      assert.equal(resumed.sentSessions, 1);
      assert.equal(beforeInstallBodies.length, 1);
      assert.deepEqual(JSON.parse(beforeInstallBodies[0]!).sessions[0], expectedBeforeInstall.row);
      assert.equal(expectedBeforeInstall.row?.totals.events, 1);
      completion.check("0741_receipt_before_trigger_install_is_read_from_partial_cursor");
      let injected = false;
      const raced = await summary41.updateSessionSummary(old.database, sid41, until, {
        read: async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>) => {
          const rows = await read(old.database)<T>(queries);
          if (!injected && rows.some((row) => typeof (row as { outputTokens?: unknown }).outputTokens === "number")) {
            injected = true;
            terminalReceipt(old, 1);
          }
          return rows;
        },
      });
      assert.equal(injected, true, "the terminal receipt must land after the worker read");
      assert.equal(collectSessionSnapshots(old.database, { until, sessionIds: [sid41] }).length, 0);
      const bodies: string[] = [];
      const sent = await sync41.runSessionSync(config, { ledgerDb: old.database, incremental: true,
        sessionIds: [sid41], until, delayMs: 0, maxAttemptsPerBatch: 1, log: () => undefined,
        fetchImpl: (async (_input, init) => {
          const body = String(init?.body ?? "");
          bodies.push(body);
          return new Response(JSON.stringify(acceptedFixtureDelivery(body, config.installKey!)),
            { status: 200, headers: { "content-type": "application/json" } });
        }) as typeof fetch });
      console.log(JSON.stringify({ case: "0741-receipt-between-read-and-write", workerComplete: raced.complete,
        sends: bodies.length, scratchEvents: 0, oldTriggerInstalled: Boolean(triggerSql(old.database,
          "trg_session_summary_raw_update")) }));
      assert.equal(raced.complete, false, "0.7.41 must reject the raced read");
      assert.equal(bodies.length, 0, "0.7.41 must not POST a privacy-ineligible summary");
      assert.equal(sent.sentSessions, 0);
      assert.doesNotMatch(oldSql, /summary_scanned_aware_v1/);
      assert.ok(headSql.get("trg_session_summary_raw_update")?.includes("summary_scanned_aware_v1"));
      for (const name of legacyTriggerNames) {
        assert.ok(triggerSql(old.database, name), `0.7.41 did not install ${name}`);
        assert.ok(headSql.get(name), `head did not install ${name}_v42`);
      }
      completion.check("0741_sync_start_installs_legacy_triggers");
      completion.check("0741_terminal_receipt_race_cannot_send");

      let tokenInjected = false;
      const staleTokens = await summary41.updateSessionSummary(old.database, tokens41, until, {
        read: async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>) => {
          const rows = await read(old.database)<T>(queries);
          if (!tokenInjected && rows.some((row) => typeof (row as { outputTokens?: unknown }).outputTokens === "number")) {
            tokenInjected = true;
            old.database.prepare("update buffered_events set output_tokens=99 where id=?").run(event(4));
          }
          return rows;
        },
      });
      assert.equal(tokenInjected, true);
      assert.equal(staleTokens.complete, false);
      const correctedBodies: string[] = [];
      const corrected = await sync41.runSessionSync(config, { ledgerDb: old.database, incremental: true,
        sessionIds: [tokens41], until, delayMs: 0, maxAttemptsPerBatch: 1, log: () => undefined,
        fetchImpl: (async (_input, init) => {
          const body = String(init?.body ?? "");
          correctedBodies.push(body);
          return new Response(JSON.stringify(acceptedFixtureDelivery(body, config.installKey!)),
            { status: 200, headers: { "content-type": "application/json" } });
        }) as typeof fetch });
      assert.equal(corrected.sentSessions, 1);
      assert.equal(correctedBodies.length, 1);
      assert.equal(JSON.parse(correctedBodies[0]!).sessions[0].totals.outputTokens, 99);
      completion.check("0741_raw_edit_race_rebuilds_before_send");
    } finally { old.close(); }

    const again = new LocalEventBuffer(file41, { workspaceId: workspace });
    try {
      ensureSessionSummarySchema(again.database);
      for (const name of legacyTriggerNames) {
        assert.equal(triggerSql(again.database, name), null);
        assert.equal(triggerSql(again.database, `${name}_v42`), headSql.get(name));
      }
      completion.check("head_reupgrade_removes_0741_triggers_and_keeps_scanned_aware_sql");
    } finally { again.close(); }

    // 0.7.39 has no summary worker or state write. Race a head worker, then
    // open that ledger with the actual 0.7.39 binary: its fresh full query
    // must see the receipt and keep the session off the wire.
    const file39 = path.join(root, "rollback-0739.sqlite");
    const sid39 = session(739);
    const head39 = new LocalEventBuffer(file39, { workspaceId: workspace });
    ensureSessionSummarySchema(head39.database);
    put(head39, sid39, 2);
    let headInjected = false;
    const headRace = await updateSessionSummary(head39.database, sid39, until, {
      read: async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>) => {
        const rows = await read(head39.database)<T>(queries);
        if (!headInjected && rows.some((row) => typeof (row as { outputTokens?: unknown }).outputTokens === "number")) {
          headInjected = true;
          terminalReceipt(head39, 2);
        }
        return rows;
      },
    });
    assert.equal(headInjected, true);
    assert.equal(headRace.complete, false);
    head39.close();
    const rollback = new buffer39.LocalEventBuffer(file39, { workspaceId: workspace });
    try {
      const bodies: string[] = [];
      const sent = await sync39.runSessionSync(config, { ledgerDb: rollback.database,
        sessionIds: [sid39], until, delayMs: 0, maxAttemptsPerBatch: 1, log: () => undefined,
        fetchImpl: (async (_input, init) => {
          const body = String(init?.body ?? "");
          bodies.push(body);
          return new Response(JSON.stringify(acceptedFixtureDelivery(body, config.installKey!)),
            { status: 200, headers: { "content-type": "application/json" } });
        }) as typeof fetch });
      assert.equal(sent.sentSessions, 0);
      assert.equal(bodies.length, 0);
      assert.equal(sync39.collectSessionSnapshots(rollback.database, { until, sessionIds: [sid39] }).length, 0);
      completion.check("0739_full_query_after_head_race_cannot_send");
    } finally { rollback.close(); }

    const headAfter39 = new LocalEventBuffer(file39, { workspaceId: workspace });
    try {
      ensureSessionSummarySchema(headAfter39.database);
      assert.ok(triggerSql(headAfter39.database, "trg_session_summary_raw_update_v42"));
      completion.check("head_reupgrade_after_0739_keeps_scanned_aware_sql");
    } finally { headAfter39.close(); }
    completion.complete();
  } finally {
    for (const dir of installed.reverse()) {
      execFileSync("git", ["worktree", "remove", "--force", dir], { cwd: repo });
    }
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
