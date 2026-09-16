import fs from "node:fs";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  collectSessionSnapshots,
  commitDaemonSessionSyncFailure,
  commitDaemonSessionSyncSuccess,
  loadDaemonSessionSyncState,
  planDaemonSessionSync,
  runSessionSync,
  saveDaemonSessionSyncState,
  sessionIdsFromBatches,
} from "../packages/collector-cli/src/session-sync";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-sync-daemon", 13);
const root = process.env.PLIMSOLL_PROOF_ROOT!;
const installKey = "session-sync-daemon-proof-key";
const tenantId = "00000000-0000-4000-8000-000000000070";
const missedSession = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa70";
const otlpSession = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbb70";
const nowIso = () => new Date().toISOString();
const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest",
  tenantId,
  installKey,
  uploadSigningSecret: "session-sync-daemon-secret",
});

function uuid(n: number) {
  return `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
}

function openLedger(name: string) {
  return new LocalEventBuffer(path.join(root, `${name}.sqlite`), { workspaceId: tenantId });
}

function appendSession(
  buffer: LocalEventBuffer,
  sessionId: string,
  index: number,
  observedAt = new Date().toISOString(),
) {
  const appended = buffer.append(
    aiInteractionEventSchema.parse({
      id: uuid(index),
      sessionId,
      source: "codex",
      eventType: "assistant_response",
      observedAt,
      inputTokens: 3,
      outputTokens: 1,
    }),
  );
  if (!appended) {
    throw new Error(`fixture event ${index} was not admitted`);
  }
}

function legacyTouched(pending: string[], batches: Parameters<typeof sessionIdsFromBatches>[0]) {
  return [...new Set([...pending, ...sessionIdsFromBatches(batches)])];
}

function ingestFetch(options: { failTimes?: number; sent: string[][] }): typeof fetch {
  let remainingFails = options.failTimes ?? 0;
  return (async (_input, init) => {
    const raw = String(init?.body ?? "");
    const payload = JSON.parse(raw) as {
      kind?: string;
      sessions?: Array<{ session: { id: string } }>;
    };
    if (payload.kind !== "session_sync") {
      return new Response("unexpected", { status: 404 });
    }
    options.sent.push((payload.sessions ?? []).map((row) => row.session.id));
    if (remainingFails > 0) {
      remainingFails -= 1;
      return new Response("unavailable", { status: 503 });
    }
    return new Response(
      JSON.stringify({
        ...acceptedFixtureDelivery(raw, installKey),
        inserted: payload.sessions?.length ?? 0,
        updated: 0,
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  }) as typeof fetch;
}

async function daemonCycle(
  buffer: LocalEventBuffer,
  input: {
    batches?: Parameters<typeof sessionIdsFromBatches>[0];
    until: string;
    fetchImpl: typeof fetch;
  },
) {
  const state = loadDaemonSessionSyncState(buffer.database);
  const plan = planDaemonSessionSync({
    db: buffer.database,
    state,
    uploadedBatches: input.batches ?? [],
    until: input.until,
  });
  saveDaemonSessionSyncState(buffer.database, plan.state);
  if (plan.skip) {
    return { plan, result: null, state: loadDaemonSessionSyncState(buffer.database) };
  }
  const result = await runSessionSync(config, {
    ...(plan.sessionIds ? { sessionIds: plan.sessionIds } : {}),
    until: plan.until,
    ledgerDb: buffer.database,
    fetchImpl: input.fetchImpl,
    sleep: async () => undefined,
    delayMs: 0,
    maxAttemptsPerBatch: 1,
    log: () => undefined,
  });
  const next = result.ok
    ? commitDaemonSessionSyncSuccess(plan.state, plan.until)
    : commitDaemonSessionSyncFailure(plan.state, plan.sessionIds);
  saveDaemonSessionSyncState(buffer.database, next);
  return { plan, result, state: loadDaemonSessionSyncState(buffer.database) };
}

async function main() {
  const historical = openLedger("historical-miss");
  try {
    appendSession(historical, missedSession, 1);
    const emptyBatches: Parameters<typeof sessionIdsFromBatches>[0] = [];
    completion.check(
      "legacy_touched_window_misses_already_uploaded_session",
      legacyTouched([], emptyBatches).length === 0,
    );

    const catchup = planDaemonSessionSync({
      db: historical.database,
      state: loadDaemonSessionSyncState(historical.database),
      uploadedBatches: emptyBatches,
      until: nowIso(),
    });
    completion.check(
      "uncaught_horizon_walks_ledger_without_upload_history",
      catchup.reason === "full_catchup" && catchup.sessionIds === undefined && catchup.skip === false,
    );

    const sent: string[][] = [];
    const delivered = await daemonCycle(historical, {
      until: nowIso(),
      fetchImpl: ingestFetch({ sent }),
    });
    completion.check(
      "periodic_sync_delivers_missed_session_without_upload_history",
      delivered.result?.ok === true &&
        delivered.result.sentSessions === 1 &&
        sent.flat().includes(missedSession) &&
        delivered.state.caughtUp === true &&
        delivered.state.pendingSessionIds.length === 0,
    );
  } finally {
    historical.close();
  }

  const restart = openLedger("restart-503");
  try {
    appendSession(restart, missedSession, 2);
    const failedSent: string[][] = [];
    const failed = await daemonCycle(restart, {
      until: nowIso(),
      fetchImpl: ingestFetch({ failTimes: 1, sent: failedSent }),
    });
    completion.check(
      "push_failure_does_not_advance_horizon",
      failed.result?.ok === false &&
        failed.state.caughtUp === false &&
        failedSent.flat().includes(missedSession),
    );

    const ramPending: string[] = [];
    completion.check(
      "restart_drops_volatile_pending_but_durable_state_remains",
      ramPending.length === 0 && loadDaemonSessionSyncState(restart.database).caughtUp === false,
    );

    const recoveredSent: string[][] = [];
    const recovered = await daemonCycle(restart, {
      until: nowIso(),
      fetchImpl: ingestFetch({ sent: recoveredSent }),
    });
    completion.check(
      "restart_after_503_converges_on_next_periodic_cycle",
      recovered.result?.ok === true &&
        recoveredSent.flat().includes(missedSession) &&
        recovered.state.caughtUp === true,
    );
  } finally {
    restart.close();
  }

  const incremental = openLedger("incremental");
  try {
    appendSession(incremental, missedSession, 3);
    await daemonCycle(incremental, {
      until: nowIso(),
      fetchImpl: ingestFetch({ sent: [] }),
    });
    const caughtUp = loadDaemonSessionSyncState(incremental.database);
    const idle = planDaemonSessionSync({
      db: incremental.database,
      state: caughtUp,
      uploadedBatches: [],
      until: caughtUp.lastSuccessfulUntil ?? nowIso(),
    });
    completion.check(
      "caught_up_idle_cycle_skips_network",
      idle.skip === true && idle.reason === "skip",
    );

    await new Promise((resolve) => setTimeout(resolve, 5));
    appendSession(incremental, otlpSession, 4);
    const otlpPlan = planDaemonSessionSync({
      db: incremental.database,
      state: loadDaemonSessionSyncState(incremental.database),
      uploadedBatches: [],
      until: nowIso(),
    });
    completion.check(
      "otlp_style_ledger_session_enters_incremental_set",
      otlpPlan.reason === "incremental" &&
        otlpPlan.sessionIds?.includes(otlpSession) === true &&
        otlpPlan.skip === false,
    );

    const horizon = loadDaemonSessionSyncState(incremental.database).lastSuccessfulUntil;
    const failedIncremental = await daemonCycle(incremental, {
      until: nowIso(),
      fetchImpl: ingestFetch({ failTimes: 1, sent: [] }),
    });
    completion.check(
      "incremental_failure_keeps_horizon",
      failedIncremental.result?.ok === false &&
        failedIncremental.state.caughtUp === true &&
        failedIncremental.state.lastSuccessfulUntil === horizon,
    );
    completion.check(
      "empty_session_id_filter_is_zero_snapshots_not_a_full_walk",
      collectSessionSnapshots(incremental.database, {
        until: nowIso(),
        sessionIds: [],
      }).length === 0,
    );
  } finally {
    incremental.close();
  }

  const corrupt = openLedger("corrupt-state");
  try {
    appendSession(corrupt, missedSession, 5);
    corrupt.database.exec(`
      create table if not exists maintenance_state (
        key text primary key,
        value text not null,
        updated_at text not null
      );
    `);
    corrupt.database
      .prepare(
        `insert into maintenance_state (key, value, updated_at) values (?, ?, ?)`,
      )
      .run("session_sync_daemon_v1", "{not-json", "2026-09-16T00:00:00.000Z");
    const degraded = loadDaemonSessionSyncState(corrupt.database);
    const plan = planDaemonSessionSync({
      db: corrupt.database,
      state: degraded,
      uploadedBatches: [],
      until: nowIso(),
    });
    completion.check(
      "corrupt_state_degrades_to_full_catchup",
      degraded.caughtUp === false && plan.reason === "full_catchup",
    );
    saveDaemonSessionSyncState(corrupt.database, {
      schemaVersion: 1,
      caughtUp: true,
      lastSuccessfulUntil: nowIso(),
      pendingSessionIds: ["x".repeat(200)],
    });
    const overflow = loadDaemonSessionSyncState(corrupt.database);
    completion.check(
      "oversized_pending_save_drops_horizon",
      overflow.caughtUp === false && overflow.pendingSessionIds.length === 0,
    );
  } finally {
    corrupt.close();
  }

  const cliSource = fs.readFileSync(
    path.join(process.cwd(), "packages/collector-cli/src/cli.ts"),
    "utf8",
  );
  completion.check(
    "daemon_wires_durable_planner_and_keeps_retry_after_carry",
    cliSource.includes("planDaemonSessionSync") &&
      cliSource.includes("loadDaemonSessionSyncState") &&
      cliSource.includes("saveDaemonSessionSyncState") &&
      cliSource.includes("if (serverRetryAfterMs > 0) { carrySessions(); return; }") &&
      cliSource.includes("const touchedSessionIds =") &&
      cliSource.indexOf("const touchedSessionIds =") >
        cliSource.indexOf("if (serverRetryAfterMs > 0) { carrySessions(); return; }") &&
      /try \{\s*const sessionPlan = planDaemonSessionSync/.test(cliSource),
  );

  completion.complete();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
