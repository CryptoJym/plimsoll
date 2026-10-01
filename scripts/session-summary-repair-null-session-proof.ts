import assert from "node:assert/strict";
import path from "node:path";
import { once } from "node:events";
import type { AddressInfo } from "node:net";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { runRepricingMaintenance } from "../packages/collector-cli/src/maintenance";
import { createCollectorServer } from "../packages/collector-cli/src/server";
import { ensureSessionSummarySchema, updateSessionSummary } from "../packages/collector-cli/src/session-summary";
import { STATUS_MAX_AGE_MS } from "../packages/collector-cli/src/projection-validity";
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
const completion = createProofCompletion("session-summary-repair-null-session", 5);
const root = process.env.PLIMSOLL_PROOF_ROOT!;
const workspace = "00000000-0000-4000-8000-000000000747";
const session = "00000000-0000-4000-8000-000000000701";
const orphanEvent = "10000000-0000-4000-8000-000000000701";
const sessionEvent = "10000000-0000-4000-8000-000000000702";
const created = "2026-09-30T00:00:00.000Z";

function fixturePath(name: string) {
  return path.join(root, `${name}.sqlite`);
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
    const body = {
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
  const buffer = await makeLedger("null-session-repair");
  try {
    const trigger = buffer.database.prepare(
      "select sql from sqlite_master where type='trigger' and name='trg_session_summary_repair_update_old_v1'",
    ).get() as { sql: string } | undefined;
    assert.ok(trigger?.sql, "the additive repair trigger must exist");
    assert.match(trigger.sql, /when\s+\(/i);
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
    completion.check("repricing_orphan_usage_row_never_inserts_null_repair_key");
  } finally {
    buffer.close();
  }

  await statusFreshnessProof();
  completion.check("status_cache_refreshes_during_failed_maintenance");
  await codexOtlpProof();
  completion.check("codex_otlp_pairing_accepts_orphan_rows");
  completion.check("fixture_contains_outbox_receipt_orphan_shape");
  completion.check("bounded_summary_repair_trigger_is_present");
  completion.complete();
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
