#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";

const DAY_MS = 24 * 60 * 60 * 1000;
const NOW = new Date("2026-07-15T12:00:00.000Z");
const ADVANCED = new Date(NOW.getTime() + 2 * DAY_MS);
const SESSION = "convergence-fixture-session";

function seedLedger(databasePath: string) {
  const seed = new LocalEventBuffer(databasePath);
  seed.close();
  const db = new Database(databasePath);
  const insert = db.prepare(
    `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,
        created_at,session_id,action_class,input_tokens,output_tokens,cost_usd)
     values (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
  );
  const expiredAt = new Date(NOW.getTime() - 29 * DAY_MS).toISOString();
  const retainedAt = new Date(NOW.getTime() - 20 * DAY_MS).toISOString();
  db.transaction(() => {
    // The first 2,000 facts are older than the advanced 30-day cutoff; all
    // 3,501 are older than the 7-day cutoff. Expiry therefore stays active in
    // bounded slices, while the 1,501 facts that remain inside the 30-day
    // window make one session repair exceed its 1,000-row slice and expose the
    // cursor restart on every cadence.
    for (let index = 0; index < 3_501; index += 1) {
      const observedAt = index < 2_000 ? expiredAt : retainedAt;
      const id = `projection-convergence-${index}`;
      insert.run(
        id,
        "codex",
        "assistant_response",
        "metadata",
        observedAt,
        JSON.stringify({
          id,
          source: "codex",
          dataMode: "metadata",
          eventType: "assistant_response",
          observedAt,
          sessionId: SESSION,
          actionClass: "other",
          inputTokens: 1,
          outputTokens: 1,
          costUsd: 1,
        }),
        "[]",
        NOW.toISOString(),
        SESSION,
        "other",
        1,
        1,
        1,
      );
    }
  })();
  db.close();
}

function settle(buffer: LocalEventBuffer, now: Date, maxTicks: number) {
  for (let tick = 0; tick < maxTicks; tick += 1) {
    const status = buffer.projection.status();
    if (
      status.ready &&
      status.parityReady &&
      !status.dirty &&
      Object.values(status.backlog).every((value) => value === 0)
    ) {
      return tick;
    }
    buffer.projection.runMaintenance(now);
  }
  throw new Error(`projection did not settle: ${JSON.stringify(buffer.projection.status())}`);
}

function main() {
  const previousDateNow = Date.now;
  Date.now = () => NOW.getTime();
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-projection-convergence-")));
  const databasePath = path.join(root, "ledger.sqlite");
  seedLedger(databasePath);
  const buffer = new LocalEventBuffer(databasePath);
  try {
    settle(buffer, NOW, 80);
    const initial = buffer.projection.status();
    assert.equal(initial.ready, true, JSON.stringify(initial));
    assert.equal(initial.backlog.dirtySessions, 0, JSON.stringify(initial));
    const lastSuccessAt = initial.lastSuccessAt;

    for (let tick = 0; tick < 4; tick += 1) {
      buffer.projection.runMaintenance(ADVANCED);
    }
    const afterBoundedBudget = buffer.projection.status();
    const staleSnapshot = buffer.projection.readSnapshot(30);

    // This is the regression assertion. On 78daaca2 the expiry stage bumps
    // restart_revision each cadence, so the repair remains dirty after the
    // four bounded turns. The fixed projection clears the session repair
    // while expiry continues in its own bounded queue.
    assert.equal(afterBoundedBudget.backlog.dirtySessions, 0, JSON.stringify({
      initial,
      afterBoundedBudget,
    }));
    assert.equal(afterBoundedBudget.lastSuccessAt, lastSuccessAt, JSON.stringify(afterBoundedBudget));
    assert.equal(afterBoundedBudget.degradedReason, "projection_repair_backlog", JSON.stringify(afterBoundedBudget));
    assert.ok(afterBoundedBudget.backlog.expiryWindows > 0, JSON.stringify(afterBoundedBudget));
    assert.equal(staleSnapshot.kind, "ready", JSON.stringify(staleSnapshot));
    if (staleSnapshot.kind === "ready") {
      assert.equal(staleSnapshot.snapshot.projection.status, "stale", JSON.stringify(staleSnapshot.snapshot.projection));
      assert.equal(staleSnapshot.snapshot.projection.parityReady, false, JSON.stringify(staleSnapshot.snapshot.projection));
    }

    const ticksToReady = settle(buffer, ADVANCED, 20);
    const final = buffer.projection.status();
    assert.equal(final.ready, true, JSON.stringify(final));
    assert.equal(final.parityReady, true, JSON.stringify(final));
    assert.equal(final.dirty, false, JSON.stringify(final));
    assert.equal(final.degradedReason, null, JSON.stringify(final));
    assert.equal(final.backlog.dirtySessions, 0, JSON.stringify(final));
    assert.equal(final.backlog.expiryWindows, 0, JSON.stringify(final));
    assert.equal(final.lastSuccessAt, ADVANCED.toISOString(), JSON.stringify(final));
    console.log(JSON.stringify({
      check: "projection_converges_after_expiry_session_repair",
      boundedTicks: 4,
      ticksToReady,
      afterBoundedBudget: {
        dirtySessions: afterBoundedBudget.backlog.dirtySessions,
        expiryWindows: afterBoundedBudget.backlog.expiryWindows,
        lastSuccessAt: afterBoundedBudget.lastSuccessAt,
        degradedReason: afterBoundedBudget.degradedReason,
      },
      final: {
        ready: final.ready,
        parityReady: final.parityReady,
        dirty: final.dirty,
        degradedReason: final.degradedReason,
        lastSuccessAt: final.lastSuccessAt,
        backlog: final.backlog,
      },
    }));
  } finally {
    buffer.close();
    Date.now = previousDateNow;
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main();
