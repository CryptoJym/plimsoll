import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { once } from "node:events";
import { performance } from "node:perf_hooks";
import { pathToFileURL } from "node:url";
import type { AddressInfo } from "node:net";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { CollectorMaintenance, runRepricingMaintenance } from "../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { ensureSessionSummarySchema, updateSessionSummary } from "../packages/collector-cli/src/session-summary";
import { STATUS_MAX_AGE_MS } from "../packages/collector-cli/src/projection-validity";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { buildCodexUsagePairingIndexes } from "../packages/collector-cli/src/codex-usage-pairing";
import { createProofCompletion } from "./lib/proof-completion";

/**
 * Regression proof for the 0.7.46 canary failure (#437 follow-up).
 *
 * The fixture is deliberately a long-lived ledger shape: a complete summary
 * exists for one session, while an older usage row has no session id and is
 * still represented in the delivery outbox and receipt lineage. Repricing
 * updates that orphan row. On 0.7.46 the summary repair UPDATE trigger's
 * unparenthesized OR expression inserts NULL into its NOT NULL repair key.
 */
const otlpOnly = process.argv.includes("--otlp-only");
const completion = createProofCompletion("session-summary-repair-null-session", otlpOnly ? 1 : 9);
const root = process.env.PLIMSOLL_PROOF_ROOT!;
const workspace = "00000000-0000-4000-8000-000000000747";
const session = "00000000-0000-4000-8000-000000000701";
const orphanEvent = "10000000-0000-4000-8000-000000000701";
const sessionEvent = "10000000-0000-4000-8000-000000000702";
const created = "2026-09-30T00:00:00.000Z";

function fixturePath(name: string) {
  return path.join(root, `${name}.sqlite`);
}

const triggerNames = [
  "trg_session_summary_raw_update_v42",
  "trg_session_summary_repair_update_old_v1",
  "trg_session_summary_repair_update_new_v1",
];

function triggerDefinitions(db: { prepare: (sql: string) => { get: (name: string) => unknown } }) {
  return triggerNames.map((name) => db.prepare(
    "select name, sql from sqlite_master where type='trigger' and name=?",
  ).get(name) ?? null);
}

function captureFailure(action: () => unknown) {
  try {
    return { ok: true, result: action() };
  } catch (error) {
    return {
      ok: false,
      code: error && typeof error === "object" && "code" in error ? String(error.code) : null,
      message: error instanceof Error ? error.message : String(error),
    };
  }
}

function codexOtlpBody() {
  return {
    resourceLogs: [{
      resource: { attributes: [{ key: "service.name", value: { stringValue: "codex_exec" } }] },
      scopeLogs: [{ logRecords: [{
        timeUnixNano: "1781400010000000000",
        attributes: [
          { key: "event.name", value: { stringValue: "codex.sse_event" } },
          { key: "gen_ai.usage.input_tokens", value: { intValue: "10" } },
          { key: "gen_ai.usage.output_tokens", value: { intValue: "5" } },
        ],
      }] }],
    }],
    resourceSpans: [{
      resource: { attributes: [{ key: "service.name", value: { stringValue: "codex_exec" } }] },
      scopeSpans: [{ spans: [{
        name: "handle_responses",
        startTimeUnixNano: "1781400010500000000",
        attributes: [
          { key: "gen_ai.usage.input_tokens", value: { intValue: "10" } },
          { key: "gen_ai.usage.output_tokens", value: { intValue: "5" } },
        ],
      }] }],
    }],
  };
}

function insertRows(buffer: LocalEventBuffer) {
  buffer.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json,
     suppressed_fields_json, created_at, session_id, model, input_tokens,
     output_tokens, workspace_id, privacy_generation)
    values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]', ?, ?, null,
      2, 1, ?, 'fixture-session-generation')`).run(sessionEvent, created, created, session, workspace);
  // This is the upgraded-ledger row shape that matters: usage pricing can
  // repair it, but historical attribution never supplied a session id.
  buffer.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json,
     suppressed_fields_json, created_at, session_id, model, input_tokens,
     output_tokens, workspace_id, privacy_generation)
    values (?, 'codex', 'usage_rollout', 'metadata', ?, '{}', '[]', ?, null,
      'gpt-5.5', 10, 5, ?, 'fixture-orphan-generation')`).run(
    orphanEvent, created, created, workspace,
  );
}

function seedLineage(buffer: LocalEventBuffer) {
  const raw = buffer.database.prepare("select rowid, id, created_at, privacy_generation from buffered_events where id=?")
    .get(orphanEvent) as { rowid: number; id: string; created_at: string; privacy_generation: string };
  buffer.database.prepare(`insert into upload_outbox
    (delivery_id, raw_rowid, raw_id, raw_created_at, raw_generation, workspace_id,
     base_envelope_json, base_bytes, state, next_attempt_at, created_at, updated_at)
    values (?, ?, ?, ?, ?, ?, '{}', 2, 'pending', ?, ?, ?)`).run(
    `delivery-${orphanEvent}`, raw.rowid, raw.id, raw.created_at, raw.privacy_generation,
    workspace, created, created, created,
  );
  buffer.database.prepare(`insert into upload_receipts
    (delivery_id, raw_rowid, raw_id, raw_created_at, raw_generation, terminal_state,
     reason, status_class, attempt_count, created_at, terminal_at)
    values (?, ?, ?, ?, ?, 'dead', 'fixture_receipt', 'local', 0, ?, ?)`).run(
    `receipt-${orphanEvent}`, raw.rowid, raw.id, raw.created_at, raw.privacy_generation, created, created,
  );
}

async function makeLedger(name: string) {
  const buffer = new LocalEventBuffer(fixturePath(name), { workspaceId: workspace });
  ensureSessionSummarySchema(buffer.database);
  insertRows(buffer);
  seedLineage(buffer);
  const complete = await updateSessionSummary(buffer.database, session, "2026-10-01T00:00:00.000Z", {
    read: async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>) =>
      queries.flatMap((query) => buffer.database.prepare(query.sql).all(query.params) as T[]),
  });
  assert.equal(complete.complete, true);
  return buffer;
}

async function historicalUpgradeProof() {
  const repo = process.cwd();
  const cases = [
    ["v0745", "v0.7.45"],
    ["v0746", "82dc901cba64b7393c00a8e9ea9f72b2cef836cd"],
  ] as const;
  for (const [label, ref] of cases) {
    const source = path.join(root, `source-${label}`);
    const ledger = path.join(root, `ledger-${label}.sqlite`);
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", source, ref], { cwd: repo });
    const sourceNodeModules = path.join(source, "node_modules");
    if (!fs.existsSync(sourceNodeModules)) {
      fs.symlinkSync(path.join(repo, "node_modules"), sourceNodeModules, "dir");
    }
    try {
      const oldBufferModule = await import(pathToFileURL(
        path.join(source, "packages/collector-cli/src/buffer.ts"),
      ).href);
      const oldSummaryModule = await import(pathToFileURL(
        path.join(source, "packages/collector-cli/src/session-summary.ts"),
      ).href);
      const oldMaintenanceModule = await import(pathToFileURL(
        path.join(source, "packages/collector-cli/src/maintenance.ts"),
      ).href);
      const old = new oldBufferModule.LocalEventBuffer(ledger, { workspaceId: workspace });
      let before: Array<{ name: string; sql: string }>;
      let originalFailure: ReturnType<typeof captureFailure>;
      try {
        oldSummaryModule.ensureSessionSummarySchema(old.database);
        old.database.prepare(`insert into buffered_events
          (id, source, event_type, data_mode, observed_at, payload_json,
           suppressed_fields_json, created_at, session_id, model, input_tokens,
           output_tokens, workspace_id, privacy_generation)
          values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]', ?, ?, null,
            2, 1, ?, 'fixture-session-generation')`).run(
          sessionEvent, created, created, session, workspace,
        );
        old.database.prepare(`insert into buffered_events
          (id, source, event_type, data_mode, observed_at, payload_json,
           suppressed_fields_json, created_at, session_id, model, input_tokens,
           output_tokens, workspace_id, privacy_generation)
          values (?, 'codex', 'usage_rollout', 'metadata', ?, '{}', '[]', ?, null,
            'gpt-5.5', 10, 5, ?, 'fixture-orphan-generation')`).run(
          orphanEvent, created, created, workspace,
        );
        const raw = old.database.prepare(
          "select rowid, id, created_at, privacy_generation from buffered_events where id=?",
        ).get(orphanEvent) as { rowid: number; id: string; created_at: string; privacy_generation: string };
        old.database.prepare(`insert into upload_outbox
          (delivery_id, raw_rowid, raw_id, raw_created_at, raw_generation, workspace_id,
           base_envelope_json, base_bytes, state, next_attempt_at, created_at, updated_at)
          values (?, ?, ?, ?, ?, ?, '{}', 2, 'pending', ?, ?, ?)`).run(
          `delivery-${orphanEvent}`, raw.rowid, raw.id, raw.created_at, raw.privacy_generation,
          workspace, created, created, created,
        );
        old.database.prepare(`insert into upload_receipts
          (delivery_id, raw_rowid, raw_id, raw_created_at, raw_generation, terminal_state,
           reason, status_class, attempt_count, created_at, terminal_at)
          values (?, ?, ?, ?, ?, 'dead', 'fixture_receipt', 'local', 0, ?, ?)`).run(
          `receipt-${orphanEvent}`, raw.rowid, raw.id, raw.created_at, raw.privacy_generation,
          created, created,
        );
        const summary = await oldSummaryModule.updateSessionSummary(old.database, session, "2026-10-01T00:00:00.000Z", {
          read: async (queries: Array<{ sql: string; params: Record<string, unknown> }>) =>
            queries.flatMap((query) => old.database.prepare(query.sql).all(query.params)),
        });
        assert.equal(summary.complete, true);
        before = triggerDefinitions(old.database) as Array<{ name: string; sql: string }>;
        originalFailure = captureFailure(() => oldMaintenanceModule.runRepricingMaintenance(old.database));
        assert.equal(originalFailure.ok, false);
        assert.equal(originalFailure.code, "SQLITE_CONSTRAINT_NOTNULL");
      } finally {
        old.close();
      }

      const fixed = new LocalEventBuffer(ledger, { workspaceId: workspace });
      try {
        ensureSessionSummarySchema(fixed.database);
        const after = triggerDefinitions(fixed.database) as Array<{ name: string; sql: string }>;
        assert.equal(after.length, triggerNames.length);
        assert.ok(after.every((trigger) => trigger?.sql));
        assert.ok(after.some((trigger, index) => trigger.sql !== before[index]?.sql));
        assert.match(after.find((trigger) => trigger.name === "trg_session_summary_repair_update_old_v1")!.sql, /when\s+\(/i);
        const upgraded = captureFailure(() => runRepricingMaintenance(fixed.database));
        assert.equal(upgraded.ok, true, JSON.stringify(upgraded));
        const repriced = fixed.database.prepare(
          "select session_id, cost_usd, cost_kind from buffered_events where id=?",
        ).get(orphanEvent) as { session_id: string | null; cost_usd: number | null; cost_kind: string | null };
        assert.equal(repriced.session_id, null);
        assert.equal(repriced.cost_kind, "estimated");
        assert.equal(typeof repriced.cost_usd, "number");
        const repairKeys = fixed.database.prepare(
          "select session_id from session_sync_summary_repairs",
        ).all() as Array<{ session_id: string | null }>;
        assert.ok(repairKeys.every((row) => row.session_id !== null));
        const codexRoot = path.join(root, `codex-${label}`);
        const claudeRoot = path.join(root, `claude-${label}`);
        fs.mkdirSync(codexRoot);
        fs.mkdirSync(claudeRoot);
        const maintenance = new CollectorMaintenance(
          fixed,
          new RolloutTailer(fixed, codexRoot, () => []),
          new TranscriptTailer(fixed, claudeRoot),
        );
        const recent = await maintenance.runRecent();
        maintenance.close();
        assert.ok(recent);
        console.log(JSON.stringify({
          phase: "historical-ledger-upgrade",
          label,
          ref,
          originalFailure,
          before,
          after,
          upgraded,
          repriced,
          repairKeys,
          recent,
        }));
      } finally {
        fixed.close();
      }
    } finally {
      execFileSync("git", ["worktree", "remove", "--force", source], { cwd: repo });
      for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(ledger + suffix, { force: true });
    }
  }
}

async function statusFreshnessProof() {
  const buffer = new LocalEventBuffer(fixturePath("status-refresh"), { workspaceId: workspace });
  let refresh: ((failure?: "maintenance_failed") => boolean) | undefined;
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, {
    statusRefreshIntervalMs: 20,
    maintenanceStatus: () => ({ failed: true, stage: "recent_maintenance" }),
    registerStatusRefresher: (callback) => { refresh = callback; },
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = (server.address() as AddressInfo).port;
  try {
    const first = await fetch(`http://127.0.0.1:${port}/status`);
    assert.equal(first.status, 200);
    const initial = await first.json() as { statusFreshness: { cachedAt: string } };
    let refreshed = false;
    const before = Date.parse(initial.statusFreshness.cachedAt);
    const oldNow = Date.now;
    try {
      assert.equal(refresh?.("maintenance_failed"), true);
      Date.now = () => before + STATUS_MAX_AGE_MS + 1;
      await new Promise((resolve) => setTimeout(resolve, 80));
      const response = await fetch(`http://127.0.0.1:${port}/status`);
      assert.equal(response.status, 200);
      const body = await response.json() as {
        statusFreshness: { state: string; cachedAt: string; reason: string | null };
        maintenance: { failed: boolean };
      };
      refreshed = Date.parse(body.statusFreshness.cachedAt) > before &&
        body.statusFreshness.state === "last_coherent" &&
        body.statusFreshness.reason === "maintenance_failed" && body.maintenance.failed === true;
      assert.equal(refreshed, true, JSON.stringify(body.statusFreshness));
    } finally {
      Date.now = oldNow;
    }
  } finally {
    server.closeAllConnections();
    server.close();
    await once(server, "close");
    buffer.close();
  }
}

async function statusReadFailureRecoveryProof() {
  for (const code of ["SQLITE_BUSY", "fixture_read_failure"]) {
    const buffer = new LocalEventBuffer(fixturePath(`status-read-recovery-${code}`), { workspaceId: workspace });
    let refresh: ((failure?: "maintenance_failed") => boolean) | undefined;
    const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, {
      statusRefreshIntervalMs: 20,
      maintenanceStatus: () => ({ failed: true, stage: "recent_maintenance" }),
      registerStatusRefresher: (callback) => { refresh = callback; },
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const port = (server.address() as AddressInfo).port;
    try {
      assert.equal(refresh?.("maintenance_failed"), true);
      const beforeResponse = await fetch(`http://127.0.0.1:${port}/status`);
      const before = await beforeResponse.json() as {
        statusFreshness: { state: string; reason: string | null; cachedAt: string; lastGoodAt: string | null };
      };
      const original = buffer.projection.readSnapshot.bind(buffer.projection) as (...args: any[]) => any;
      let failed = false;
      (buffer.projection.readSnapshot as unknown as (...args: unknown[]) => unknown) = (...args) => {
        if (!failed) {
          failed = true;
          const error = new Error("fixture one failed status read") as Error & { code?: string };
          error.code = code;
          throw error;
        }
        return original(...args);
      };
      assert.equal(refresh?.(), false);
      await new Promise((resolve) => setTimeout(resolve, 80));
      const afterResponse = await fetch(`http://127.0.0.1:${port}/status`);
      const after = await afterResponse.json() as {
        statusFreshness: { state: string; reason: string | null; cachedAt: string; lastGoodAt: string | null };
        maintenance: { failed: boolean };
      };
      assert.equal(failed, true);
      assert.equal(after.statusFreshness.state, "last_coherent");
      assert.equal(after.statusFreshness.reason, "maintenance_failed");
      assert.ok(Date.parse(after.statusFreshness.cachedAt) > Date.parse(before.statusFreshness.cachedAt));
      assert.equal(after.maintenance.failed, true);
      assert.equal(after.statusFreshness.lastGoodAt, before.statusFreshness.lastGoodAt);
      console.log(JSON.stringify({ phase: "read-failure-heartbeat-recovery", code, before: before.statusFreshness, after: after.statusFreshness }));
    } finally {
      server.closeAllConnections();
      server.close();
      await once(server, "close");
      buffer.close();
    }
  }
}

async function heartbeatLatencyProof() {
  const buffer = new LocalEventBuffer(fixturePath("heartbeat-latency"), { workspaceId: workspace });
  ensureSessionSummarySchema(buffer.database);
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, {
    statusRefreshIntervalMs: 1,
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = (server.address() as AddressInfo).port;
  const postOtlp = async () => {
    const started = performance.now();
    const response = await fetch(`http://127.0.0.1:${port}/v1/logs`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-plimsoll-source": "codex" },
      body: JSON.stringify(codexOtlpBody()),
    });
    const result = await response.json() as { accepted?: boolean; events?: number };
    assert.equal(response.status, 202, JSON.stringify(result));
    assert.equal(result.accepted, true, JSON.stringify(result));
    return performance.now() - started;
  };
  try {
    const baseline = [await postOtlp(), await postOtlp(), await postOtlp()];
    const original = buffer.projection.readSnapshot.bind(buffer.projection) as (...args: any[]) => any;
    let projectionReads = 0;
    (buffer.projection.readSnapshot as unknown as (...args: unknown[]) => unknown) = (...args) => {
      projectionReads += 1;
      return original(...args);
    };
    buffer.database.prepare("delete from session_sync_summary_pending").run();
    buffer.database.prepare(`with recursive n(v) as (
      select 1 union all select v + 1 from n where v < ?
    ) insert into session_sync_summary_pending
      (session_id, reason, mutation_revision, queued_high_water,
       consecutive_zero_progress, updated_at)
      select printf('fixture-session-%d', v), 'rows_read_timeout', 0, 0, 0,
        '2026-10-01T00:00:00.000Z' from n`).run(1_000_000);
    await new Promise((resolve) => setTimeout(resolve, 30));
    const queued = [await postOtlp(), await postOtlp(), await postOtlp()];
    const median = (values: number[]) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]!;
    const baselineMedian = median(baseline);
    const queuedMedian = median(queued);
    assert.equal(projectionReads, 0, `heartbeat invoked the full status builder ${projectionReads} times`);
    assert.ok(queuedMedian < 250, `queued intake took ${queuedMedian.toFixed(1)}ms`);
    assert.ok(queuedMedian <= baselineMedian * 20 + 50,
      `queued ${queuedMedian.toFixed(1)}ms was not close to baseline ${baselineMedian.toFixed(1)}ms`);
    console.log(JSON.stringify({ phase: "bounded-heartbeat-intake", pendingRows: 1_000_000, baseline, queued,
      baselineMedian, queuedMedian, projectionReads }));
  } finally {
    server.closeAllConnections();
    server.close();
    await once(server, "close");
    buffer.close();
  }
}

async function codexOtlpProof() {
  const buffer = await makeLedger("codex-otlp");
  buildCodexUsagePairingIndexes(buffer.database);
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, {
    statusRefreshIntervalMs: 20,
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = (server.address() as AddressInfo).port;
  try {
    const body = codexOtlpBody();
    const response = await fetch(`http://127.0.0.1:${port}/v1/logs`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-plimsoll-source": "codex" },
      body: JSON.stringify(body),
    });
    const result = await response.json() as { accepted?: boolean; events?: number };
    assert.equal(response.status, 202, JSON.stringify(result));
    assert.equal(result.accepted, true, JSON.stringify(result));
    assert.equal(result.events, 2, JSON.stringify(result));
  } finally {
    server.closeAllConnections();
    server.close();
    await once(server, "close");
    buffer.close();
  }
}

async function main() {
  if (otlpOnly) {
    await codexOtlpProof();
    completion.check("codex_otlp_pairing_accepts_orphan_rows");
    completion.complete();
    return;
  }
  const buffer = await makeLedger("null-session-repair");
  try {
    let failure: unknown = null;
    try {
      runRepricingMaintenance(buffer.database);
    } catch (error) {
      failure = error;
    }
    assert.equal(failure, null, failure instanceof Error ? failure.message : String(failure));
    const orphan = buffer.database.prepare(
      "select session_id, cost_usd, cost_kind from buffered_events where id=?",
    ).get(orphanEvent) as { session_id: string | null; cost_usd: number | null; cost_kind: string | null };
    assert.equal(orphan.session_id, null);
    assert.equal(typeof orphan.cost_usd, "number");
    assert.equal(orphan.cost_kind, "estimated");
    const repairs = buffer.database.prepare("select session_id from session_sync_summary_repairs").all() as Array<{ session_id: string | null }>;
    assert.ok(repairs.every((row) => row.session_id !== null));
    const trigger = buffer.database.prepare(
      "select sql from sqlite_master where type='trigger' and name='trg_session_summary_repair_update_old_v1'",
    ).get() as { sql: string } | undefined;
    assert.ok(trigger?.sql, "the additive repair trigger must exist");
    assert.match(trigger.sql, /when\s+\(/i);
    completion.check("repricing_orphan_usage_row_never_inserts_null_repair_key");
  } finally {
    buffer.close();
  }

  await statusFreshnessProof();
  completion.check("status_cache_refreshes_during_failed_maintenance");
  await statusReadFailureRecoveryProof();
  completion.check("status_read_failure_preserves_maintenance_latch");
  await heartbeatLatencyProof();
  completion.check("bounded_heartbeat_does_not_block_queued_otlp_intake");
  await codexOtlpProof();
  completion.check("codex_otlp_pairing_accepts_orphan_rows");
  completion.check("fixture_contains_outbox_receipt_orphan_shape");
  completion.check("bounded_summary_repair_trigger_is_present");
  await historicalUpgradeProof();
  completion.check("v0745_upgrade_replaces_old_repair_triggers");
  completion.check("v0746_upgrade_replaces_old_repair_triggers");
  completion.complete();
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
