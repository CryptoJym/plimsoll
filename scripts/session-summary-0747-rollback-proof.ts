import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-summary-0747-rollback", 4);
const root = process.env.PLIMSOLL_PROOF_ROOT!;
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const baseRef = "82dc901cba64b7393c00a8e9ea9f72b2cef836cd";
const rollbackRef = "8d7068b";
const workspace = "00000000-0000-4000-8000-000000000747";
const sessionIds = [
  "00000000-0000-4000-8000-000000000751",
  "00000000-0000-4000-8000-000000000752",
  "00000000-0000-4000-8000-000000000753",
];
const churnWaveOne = [
  "00000000-0000-4000-8000-000000000754",
  "00000000-0000-4000-8000-000000000755",
  "00000000-0000-4000-8000-000000000756",
];
const churnWaveTwo = [
  "00000000-0000-4000-8000-000000000757",
  "00000000-0000-4000-8000-000000000758",
  "00000000-0000-4000-8000-000000000759",
];
const eventId = (session: string, n: number) => `${session.slice(0, -2)}${String(n).padStart(2, "0")}`;
const observedAt = "2026-09-30T00:00:00.000Z";
const until = "2026-10-01T00:00:00.000Z";

type Version = typeof import("../packages/collector-cli/src/buffer");
type SummaryVersion = typeof import("../packages/collector-cli/src/session-summary");
type SyncVersion = typeof import("../packages/collector-cli/src/session-sync");
type ConfigVersion = typeof import("../packages/collector-cli/src/config");

async function load(dir: string) {
  const source = path.join(dir, "packages/collector-cli/src");
  return {
    buffer: await import(pathToFileURL(path.join(source, "buffer.ts")).href) as Version,
    summary: await import(pathToFileURL(path.join(source, "session-summary.ts")).href) as SummaryVersion,
    sync: await import(pathToFileURL(path.join(source, "session-sync.ts")).href) as SyncVersion,
    config: await import(pathToFileURL(path.join(source, "config.ts")).href) as ConfigVersion,
  };
}

function read(buffer: { database: any }) {
  return async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>) =>
    queries.flatMap((query) => buffer.database.prepare(query.sql).all(query.params) as T[]);
}

function insertEvents(buffer: { database: any }, ids: string[] = sessionIds, eventOffset = 0) {
  const insert = buffer.database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json,
     suppressed_fields_json, created_at, session_id, input_tokens, output_tokens,
     cost_usd, workspace_id, privacy_generation)
    values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]', ?, ?, 2, 3,
      0.25, ?, ?)`);
  for (const [index, sessionId] of ids.entries()) {
    for (let n = 0; n < 2; n += 1) {
      insert.run(eventId(sessionId, eventOffset + index * 2 + n + 1), observedAt, observedAt,
        sessionId, workspace, `0746-generation-${index}-${n}`);
    }
  }
}

async function seedTouchedLedger(base: Awaited<ReturnType<typeof load>>, file: string) {
  const buffer = new base.buffer.LocalEventBuffer(file, { workspaceId: workspace });
  try {
    base.summary.ensureSessionSummarySchema(buffer.database);
    insertEvents(buffer);
    for (const sessionId of sessionIds) {
      const result = await base.summary.updateSessionSummary(buffer.database, sessionId, until, {
        read: read(buffer),
      });
      assert.equal(result.complete, true, `initial 0.7.46 summary incomplete: ${sessionId}`);
    }
    // A pre-canary edit to each complete session leaves one bounded segment in
    // the 0.7.46 repair queue, the same durable shape the rollback must drain.
    for (const sessionId of sessionIds) {
      const row = buffer.database.prepare(
        "select id from buffered_events where session_id=? order by rowid limit 1",
      ).get(sessionId) as { id: string };
      assert.equal(buffer.database.prepare("update buffered_events set output_tokens=9 where id=?")
        .run(row.id).changes, 1);
    }
    const queued = buffer.database.prepare(
      "select count(distinct session_id) as n from session_sync_summary_repairs",
    ).get() as { n: number };
    assert.equal(queued.n, sessionIds.length, "the fixture must be 0.7.46-touched");
  } finally {
    buffer.close();
  }
}

async function runSync(
  version: Awaited<ReturnType<typeof load>>,
  file: string,
  label: string,
  ids: string[] = sessionIds,
  addDuringFetch: string[] = [],
  producerOffset = 1000,
) {
  const buffer = new version.buffer.LocalEventBuffer(file, { workspaceId: workspace });
  try {
    version.summary.ensureSessionSummarySchema(buffer.database);
    const config = version.config.collectorConfigSchema.parse({
      uploadUrl: "http://127.0.0.1:1/fixture-ingest",
      tenantId: workspace,
      installKey: `rollback-${label}`,
      uploadSigningSecret: `fixture-${label}-secret`,
    });
    const bodies: unknown[] = [];
    let producerAdded = false;
    const result = await version.sync.runSessionSync(config, {
      ledgerDb: buffer.database,
      incremental: true,
      sessionIds: ids,
      until,
      delayMs: 0,
      maxAttemptsPerBatch: 1,
      log: () => undefined,
      fetchImpl: (async (_input, init) => {
        // Add the next wave while the current sync is on the wire. The next
        // daemon cycle must discover it; treating a transient pending count as
        // a permanent backlog would misread exactly this live-intake shape.
        if (!producerAdded && addDuringFetch.length > 0) {
          insertEvents(buffer, addDuringFetch, producerOffset);
          producerAdded = true;
        }
        const body = String(init?.body ?? "");
        bodies.push(JSON.parse(body));
        return new Response(JSON.stringify(acceptedFixtureDelivery(body, config.installKey!)), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }) as typeof fetch,
    });
    assert.equal(result.sentSessions, ids.length,
      `${label} did not send every queued session: ${JSON.stringify(result)}`);
    assert.equal(result.summaryComplete, true, `${label} summary remained incomplete`);
    assert.deepEqual(result.pendingSummarySessionIds, [], `${label} left pending summaries`);
    assert.equal(bodies.length, 1, `${label} should use one bounded upload batch`);
    const sent = (bodies[0] as { sessions?: unknown[] }).sessions ?? [];
    assert.equal(sent.length, ids.length, `${label} wire session count`);
    return {
      sentSessions: result.sentSessions,
      summaryComplete: result.summaryComplete,
      pendingSummarySessionIds: result.pendingSummarySessionIds,
      wireSessions: sent.length,
      producerAdded: producerAdded ? addDuringFetch.length : 0,
      ledgerSessionCount: (buffer.database.prepare(
        "select count(distinct session_id) as n from buffered_events where session_id is not null",
      ).get() as { n: number }).n,
      leasesRemaining: (buffer.database.prepare(
        "select count(*) as n from session_sync_upload_leases",
      ).get() as { n: number }).n,
      repairsRemaining: (buffer.database.prepare(
        "select count(*) as n from session_sync_summary_repairs",
      ).get() as { n: number }).n,
    };
  } finally {
    buffer.close();
  }
}

async function main() {
  const oldRepo = path.join(root, "collector-0745");
  const baseRepo = path.join(root, "collector-0746");
  execFileSync("git", ["worktree", "add", "--detach", "--quiet", oldRepo, rollbackRef], { cwd: repo });
  execFileSync("git", ["worktree", "add", "--detach", "--quiet", baseRepo, baseRef], { cwd: repo });
  fs.symlinkSync(path.join(repo, "node_modules"), path.join(oldRepo, "node_modules"), "dir");
  fs.symlinkSync(path.join(repo, "node_modules"), path.join(baseRepo, "node_modules"), "dir");
  try {
    const base = await load(baseRepo);
    const old = await load(oldRepo);
    const head = await load(repo);
    const baseLedger = path.join(root, "collector-0746-touched.sqlite");
    await seedTouchedLedger(base, baseLedger);
    completion.check("0746_touched_copy_has_three_queued_sessions");

    const oldLedger = path.join(root, "rollback-0745.sqlite");
    const fixedLedger = path.join(root, "rollback-0747.sqlite");
    fs.copyFileSync(baseLedger, oldLedger);
    fs.copyFileSync(baseLedger, fixedLedger);
    const oldResult = await runSync(old, oldLedger, "0745");
    const fixedResult = await runSync(head, fixedLedger, "0747");
    assert.equal(oldResult.sentSessions, sessionIds.length);
    assert.equal(fixedResult.sentSessions, sessionIds.length);
    assert.equal(oldResult.repairsRemaining, 0, JSON.stringify(oldResult));
    assert.equal(fixedResult.repairsRemaining, 0, JSON.stringify(fixedResult));
    console.log(JSON.stringify({ phase: "rollback-catchup", old: oldResult, fixed: fixedResult }));
    completion.check("0745_rollback_catches_up_every_queued_session");
    completion.check("0747_fix_catches_up_every_queued_session");

    // A live-intake fixture distinguishes a real backlog from the transient
    // "pending 3" observed just after the rollback. Each pass adds another
    // three sessions while its upload is in flight; the following pass must
    // process that wave. Both the verified 0.7.45 tag and the fix converge.
    const churnIds = [...sessionIds, ...churnWaveOne, ...churnWaveTwo];
    const churnRuns = async (
      version: Awaited<ReturnType<typeof load>>,
      label: string,
      ledger: string,
    ) => {
      const first = await runSync(version, ledger, `${label}-wave-1`, sessionIds, churnWaveOne);
      const second = await runSync(version, ledger, `${label}-wave-2`,
        [...sessionIds, ...churnWaveOne], churnWaveTwo, 2000);
      const final = await runSync(version, ledger, `${label}-wave-3`, churnIds);
      for (const [index, pass] of [first, second, final].entries()) {
        assert.equal(pass.sentSessions, [sessionIds, [...sessionIds, ...churnWaveOne], churnIds][index]!.length,
          `${label} wave ${index + 1} did not process its snapshot`);
      }
      assert.equal(first.producerAdded, churnWaveOne.length);
      assert.equal(second.producerAdded, churnWaveTwo.length);
      assert.equal(final.producerAdded, 0);
      assert.equal(final.ledgerSessionCount, churnIds.length);
      assert.equal(final.pendingSummarySessionIds.length, 0);
      assert.equal(final.repairsRemaining, 0);
      return { first, second, final };
    };
    const oldChurnLedger = path.join(root, "rollback-0745-live-intake.sqlite");
    const fixedChurnLedger = path.join(root, "rollback-0747-live-intake.sqlite");
    fs.copyFileSync(baseLedger, oldChurnLedger);
    fs.copyFileSync(baseLedger, fixedChurnLedger);
    const oldChurn = await churnRuns(old, "0745", oldChurnLedger);
    const fixedChurn = await churnRuns(head, "0747", fixedChurnLedger);
    console.log(JSON.stringify({ phase: "rollback-live-intake", old: oldChurn, fixed: fixedChurn }));
    completion.check("0745_rollback_drains_waves_added_during_sync");
  } finally {
    execFileSync("git", ["worktree", "remove", "--force", oldRepo], { cwd: repo });
    execFileSync("git", ["worktree", "remove", "--force", baseRepo], { cwd: repo });
  }
  completion.complete();
}

main().catch((error) => { console.error(error instanceof Error ? error.stack : error); process.exitCode = 1; });
