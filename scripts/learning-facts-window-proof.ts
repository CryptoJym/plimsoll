#!/usr/bin/env node
/** Regression cases from the independent 0.7.41 retention-window review. */

import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { deterministicLearningFactId, buildWorkEpisodeFact } from "../packages/collector-cli/src/learning-facts";
import { runLearningMaterialization } from "../packages/collector-cli/src/learning-materializer";
import { loadOrCreateLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";

const DAY = 86_400_000;
const HOUR = 3_600_000;
const UNTIL = Date.parse("2026-09-25T00:00:00.000Z");
const iso = (at: number) => new Date(at).toISOString();
const selected = process.argv[2] ?? "all";
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-window-r2-"));
fs.chmodSync(root, 0o700);

function buffer(file: string, limits: { attempts?: number; episodes?: number }) {
  return new LocalEventBuffer(file, { delivery: { enabled: false }, learningFacts: { limits } });
}

function attempt(store: LocalEventBuffer["learningFacts"], key: string, at: number,
  retryOf?: string, episodeId?: string) {
  return store.recordToolSignal({ kind: "attempt", operationId: deterministicLearningFactId(["r2", key]),
    source: "codex", sessionId: "r2-session", toolClass: "compute", toolName: "shell",
    startedAt: iso(at), ...(retryOf ? { retryOf } : {}), ...(episodeId ? { episodeId } : {}) });
}

function materialize(file: string, name: string, until = UNTIL) {
  return runLearningMaterialization({ ledgerPath: file, outcomeStorePath: null,
    statePath: path.join(root, `${name}-state.sqlite`), outPath: null,
    until: iso(until), windowDays: 7, maxNewUsageEvents: 0 });
}

async function status(store: LocalEventBuffer, name: string): Promise<any> {
  const home = path.join(root, `${name}-home`);
  fs.mkdirSync(home, { mode: 0o700 });
  const auth = loadOrCreateLocalIngestAuth(home);
  const server = createCollectorServer(collectorConfigSchema.parse({}), store, { localAuth: auth });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    return await new Promise<any>((resolve, reject) => {
      const request = http.get({ host: "127.0.0.1", port: address.port, path: "/status",
        headers: { "x-plimsoll-token": auth.managementRead } }, (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (part) => { body += part; });
        response.on("end", () => {
          try { assert.equal(response.statusCode, 200); resolve(JSON.parse(body)); }
          catch (error) { reject(error); }
        });
      });
      request.on("error", reject);
    });
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

async function w2() {
  const file = path.join(root, "w2.sqlite");
  const store = buffer(file, { attempts: 10 });
  try {
    // /status uses the process clock, so this fixture must use that clock too.
    const until = Date.now();
    for (let index = 0; index < 10; index += 1) attempt(store.learningFacts, `w2-${index}`,
      until - 2 * DAY + index * HOUR);
    const late = attempt(store.learningFacts, "w2-late", until - 3 * DAY);
    assert.equal(late.dropReason, "outside_retention_window");
    const receipt = materialize(file, "w2", until);
    const body = await status(store, "w2");
    const expected = iso(until - 3 * DAY + 1);
    assert.equal(receipt.window.requestedStartInclusive, iso(until - 7 * DAY));
    assert.equal(receipt.window.effectiveStartInclusive, expected);
    assert.equal(receipt.window.reason, "retention");
    assert.equal(body.learningFacts.analysisWindow.effectiveStartInclusive, expected);
    assert.equal(body.learningFacts.analysisWindow.reason, "retention");
    return { case: "W2", effectiveStart: expected, status: body.learningFacts.analysisWindow.reason };
  } finally { store.close(); }
}

function w2b() {
  const file = path.join(root, "w2b.sqlite");
  const store = buffer(file, { attempts: 10 });
  try {
    for (let index = 0; index < 10; index += 1) attempt(store.learningFacts, `w2b-${index}`,
      UNTIL - 5 * DAY + index * 6 * HOUR);
    attempt(store.learningFacts, "w2b-new1", UNTIL - 2 * HOUR);
    attempt(store.learningFacts, "w2b-new2", UNTIL - HOUR);
    const lateAt = UNTIL - 5 * DAY + 6 * HOUR + 30 * 60_000;
    const late = attempt(store.learningFacts, "w2b-late", lateAt);
    assert.equal(late.dropReason, "outside_retention_window");
    const receipt = materialize(file, "w2b");
    assert.equal(receipt.window.effectiveStartInclusive, iso(lateAt + 1));
    assert.equal(receipt.window.reason, "retention");
    return { case: "W2b", effectiveStart: receipt.window.effectiveStartInclusive };
  } finally { store.close(); }
}

function w3() {
  const file = path.join(root, "w3.sqlite");
  const seed = buffer(file, { episodes: 5 });
  const oldest = UNTIL - 5 * DAY;
  for (let index = 0; index < 5; index += 1) {
    seed.learningFacts.recordWorkEpisode(buildWorkEpisodeFact({ source: "codex",
      sessionId: `w3-session-${index}`, sourceEpisodeKey: "k", workClass: "other",
      complexityBand: "unknown", startedAt: iso(oldest + index * HOUR) }));
  }
  seed.close();
  const writer = buffer(file, { episodes: 5 });
  const originalPrepare = Database.prototype.prepare as any;
  let injected = false;
  let readerOldestAtInjection: string | null = null;
  (Database.prototype as any).prepare = function(this: Database.Database, sql: string) {
    if (!injected && /from work_episode_facts order by started_at, episode_id/.test(sql)) {
      injected = true;
      writer.learningFacts.recordWorkEpisode(buildWorkEpisodeFact({ source: "codex",
        sessionId: "w3-new", sourceEpisodeKey: "k", workClass: "other",
        complexityBand: "unknown", startedAt: iso(UNTIL - HOUR) }));
      readerOldestAtInjection = (originalPrepare.call(this,
        "select min(started_at) as at from work_episode_facts").get() as { at: string }).at;
    }
    return originalPrepare.call(this, sql);
  };
  let receipt: ReturnType<typeof materialize>;
  try { receipt = materialize(file, "w3"); }
  finally { (Database.prototype as any).prepare = originalPrepare; }
  try {
    assert.equal(injected, true);
    assert.equal(readerOldestAtInjection, iso(oldest),
      "window and facts were read from different ledger snapshots");
    assert.equal(receipt.window.shortenedByRetention, false);
    assert.equal(receipt.window.effectiveStartInclusive, iso(UNTIL - 7 * DAY));
    assert.equal(receipt.scanned.episodes, 5);
    return { case: "W3", readerOldestAtInjection,
      receiptStart: receipt.window.effectiveStartInclusive };
  } finally { writer.close(); }
}

function w6() {
  const file = path.join(root, "w6.sqlite");
  const until = Date.now() + DAY;
  const seed = buffer(file, { attempts: 5 });
  const rootId = deterministicLearningFactId(["r2", "w6-root"]);
  attempt(seed.learningFacts, "w6-root", until - 6 * DAY);
  attempt(seed.learningFacts, "w6-a1", until - 5 * DAY);
  attempt(seed.learningFacts, "w6-a2", until - 4 * DAY);
  attempt(seed.learningFacts, "w6-a3", until - 3 * DAY);
  const retryAt = until - 2 * DAY;
  attempt(seed.learningFacts, "w6-retry", retryAt, rootId);
  seed.close();
  const raw = new Database(file);
  try {
    raw.prepare("delete from tool_attempt_facts where operation_id in (?, ?)").run(
      rootId, deterministicLearningFactId(["r2", "w6-retry"]));
    raw.prepare(`update learning_fact_table_state set evicted_count = 2
      where table_name = 'tool_attempt_facts'`).run();
    const columns = (raw.pragma("table_info(learning_fact_table_state)") as Array<{ name: string }>)
      .map((row) => row.name);
    if (columns.includes("loss_through_ms")) raw.prepare(`update learning_fact_table_state
      set loss_through_ms = null, loss_evicted_count = 0
      where table_name = 'tool_attempt_facts'`).run();
  } finally { raw.close(); }
  const reopened = buffer(file, { attempts: 5 });
  try {
    const receipt = materialize(file, "w6", until);
    assert.ok(receipt.window.effectiveStartInclusive !== null &&
      receipt.window.effectiveStartInclusive > iso(retryAt),
      "legacy retry-descendant loss was left inside the reported window");
    assert.equal(receipt.window.reason, "retention");
    return { case: "W6", effectiveStart: receipt.window.effectiveStartInclusive,
      deletedRetryAt: iso(retryAt) };
  } finally { reopened.close(); }
}

function w7() {
  const file = path.join(root, "w7.sqlite");
  const seed = buffer(file, { attempts: 5 });
  attempt(seed.learningFacts, "w7", UNTIL - DAY);
  seed.close();
  const raw = new Database(file);
  try {
    const columns = (raw.pragma("table_info(learning_fact_table_state)") as Array<{ name: string }>)
      .map((row) => row.name);
    if (columns.includes("loss_through_ms")) {
      raw.exec("alter table learning_fact_table_state drop column loss_through_ms");
    }
  }
  finally { raw.close(); }
  const receipt = materialize(file, "w7");
  assert.equal(receipt.status, "blocked_dependencies");
  assert.equal(receipt.window.reason, "coverage_unknown");
  assert.equal(receipt.window.requestedStartInclusive, iso(UNTIL - 7 * DAY));
  assert.equal(receipt.window.effectiveStartInclusive, null);
  assert.equal(receipt.window.startInclusive, iso(UNTIL),
    "legacy readers must not see a full window when coverage is unknown");
  const absent = materialize(path.join(root, "absent-ledger.sqlite"), "w7-absent");
  assert.equal(absent.status, "blocked_dependencies");
  assert.equal(absent.window.reason, "coverage_unknown");
  assert.equal(absent.window.startInclusive, iso(UNTIL));
  return { case: "W7", status: receipt.status, start: receipt.window.startInclusive,
    absentLedger: absent.window.reason };
}

function w8() {
  const file = path.join(root, "w8.sqlite");
  const until = Date.now() + DAY;
  const seed = buffer(file, { attempts: 1 });
  attempt(seed.learningFacts, "w8", until - DAY);
  seed.close();
  const raw = new Database(file);
  try {
    raw.exec(`create table if not exists runtime_fact_drops (
      reason text primary key, dropped_count integer not null, last_dropped_at text not null)`);
    raw.prepare(`insert into runtime_fact_drops (reason, dropped_count, last_dropped_at)
      values ('outside_retention_window', 1, ?)
      on conflict(reason) do update set dropped_count = dropped_count + 1,
        last_dropped_at = excluded.last_dropped_at`).run(iso(Date.now()));
  } finally { raw.close(); }
  const before = materialize(file, "w8-before", until);
  assert.equal(before.status, "blocked_dependencies");
  assert.equal(before.window.reason, "coverage_unknown");
  assert.equal(before.window.startInclusive, iso(until));
  const reopened = buffer(file, { attempts: 1 });
  try {
    const after = materialize(file, "w8-after", until);
    assert.equal(after.window.reason, "retention");
    assert.ok(after.window.effectiveStartInclusive !== null &&
      after.window.effectiveStartInclusive > iso(until - DAY));
    return { case: "W8", before: before.window.reason,
      after: after.window.effectiveStartInclusive };
  } finally { reopened.close(); }
}

function x6a() {
  const file = path.join(root, "x6a.sqlite");
  const now = Date.now();
  const store = buffer(file, { attempts: 5 });
  try {
    for (let index = 0; index < 5; index += 1) {
      attempt(store.learningFacts, `x6a-${index}`, now - (6 - index) * DAY);
    }
    attempt(store.learningFacts, "x6a-new", now - 12 * HOUR);
    const lostAt = now - HOUR;
    const lost = attempt(store.learningFacts, "x6a-retry", lostAt,
      deterministicLearningFactId(["r2", "x6a-0"]));
    assert.equal(lost.dropReason, "retry_target_missing");
    const window = store.learningFacts.statusWithWindow(iso(now + HOUR), 7).analysisWindow;
    assert.ok(window.effectiveStartInclusive !== null &&
      window.effectiveStartInclusive > iso(lostAt));
    return { case: "X6a", lostAt: iso(lostAt), effectiveStart: window.effectiveStartInclusive };
  } finally { store.close(); }
}

function x6b() {
  const file = path.join(root, "x6b.sqlite");
  const now = Date.now();
  const store = buffer(file, { episodes: 2 });
  try {
    const episodes = [0, 1, 2].map((index) => buildWorkEpisodeFact({ source: "codex",
      sessionId: "r2-session", sourceEpisodeKey: `x6b-${index}`, workClass: "other",
      complexityBand: "unknown", startedAt: iso(now - (6 - index) * DAY) }));
    for (const episode of episodes) store.learningFacts.recordWorkEpisode(episode);
    const lostAt = now - HOUR;
    const lost = attempt(store.learningFacts, "x6b-lost", lostAt, undefined, episodes[0].episodeId);
    assert.equal(lost.dropReason, "stale_reference");
    const window = store.learningFacts.statusWithWindow(iso(now + HOUR), 7).analysisWindow;
    assert.ok(window.effectiveStartInclusive !== null &&
      window.effectiveStartInclusive > iso(lostAt));
    return { case: "X6b", lostAt: iso(lostAt), effectiveStart: window.effectiveStartInclusive };
  } finally { store.close(); }
}

function legacyClockFixture(name: string, now: number) {
  const file = path.join(root, `${name}.sqlite`);
  const store = buffer(file, { attempts: 5 });
  const rootId = deterministicLearningFactId(["r2", `${name}-root`]);
  const retryId = deterministicLearningFactId(["r2", `${name}-retry`]);
  attempt(store.learningFacts, `${name}-root`, now - 2 * DAY);
  attempt(store.learningFacts, `${name}-other`, now - DAY);
  const lostAt = now - HOUR;
  attempt(store.learningFacts, `${name}-retry`, lostAt, rootId);
  attempt(store.learningFacts, `${name}-newest`, now - 30 * 60_000);
  store.close();
  const raw = new Database(file);
  try {
    raw.prepare(`delete from tool_attempt_facts where operation_id in (?, ?)`).run(rootId, retryId);
    raw.prepare(`update learning_fact_table_state set evicted_count = 2,
      loss_evicted_count = 0, loss_through_ms = null
      where table_name = 'tool_attempt_facts'`).run();
  } finally { raw.close(); }
  return { file, lostAt };
}

function x5a() {
  const now = Date.now();
  const { file } = legacyClockFixture("x5a", now);
  const realNow = Date.now;
  Date.now = () => now + 30 * DAY;
  try { buffer(file, { attempts: 5 }).close(); }
  finally { Date.now = realNow; }
  const receipt = materialize(file, "x5a", now + HOUR);
  assert.equal(receipt.status, "blocked_dependencies");
  assert.equal(receipt.window.reason, "retention");
  assert.equal(receipt.window.effectiveStartInclusive, iso(now + HOUR));
  return { case: "X5a", status: receipt.status, reason: receipt.window.reason };
}

function x5b() {
  const now = Date.now();
  const { file, lostAt } = legacyClockFixture("x5b", now);
  const realNow = Date.now;
  Date.now = () => now - 3 * DAY;
  try { buffer(file, { attempts: 5 }).close(); }
  finally { Date.now = realNow; }
  const receipt = materialize(file, "x5b", now + HOUR);
  assert.equal(receipt.window.reason, "retention");
  assert.ok(receipt.window.effectiveStartInclusive !== null &&
    receipt.window.effectiveStartInclusive > iso(lostAt),
    "a clock behind the deleted retry must not claim to cover that retry");
  return { case: "X5b", lostAt: iso(lostAt),
    effectiveStart: receipt.window.effectiveStartInclusive };
}

function x6c() {
  const file = path.join(root, "x6c.sqlite");
  const now = Date.now();
  const store = buffer(file, {});
  const episode = buildWorkEpisodeFact({ source: "codex", sessionId: "r2-session",
    sourceEpisodeKey: "x6c", workClass: "other", complexityBand: "unknown",
    startedAt: iso(now - 2 * DAY) });
  store.learningFacts.recordWorkEpisode(episode);
  const lostAt = now - HOUR;
  attempt(store.learningFacts, "x6c-valid", lostAt, undefined, episode.episodeId);
  store.database.prepare(`update work_episode_facts set source = 'not-a-source',
    retention_verified = null where episode_id = ?`).run(episode.episodeId);
  store.close();
  const reopened = buffer(file, {});
  try {
    const left = (reopened.database.prepare(`select count(*) as n from tool_attempt_facts`
    ).get() as { n: number }).n;
    assert.equal(left, 0);
    const window = reopened.learningFacts.statusWithWindow(iso(now + HOUR), 7).analysisWindow;
    assert.ok(window.effectiveStartInclusive !== null &&
      window.effectiveStartInclusive > iso(lostAt),
      "an invalid raw episode deleted a valid attempt inside the claimed window");
    assert.equal(window.reason, "retention");
    return { case: "X6c", lostAt: iso(lostAt), effectiveStart: window.effectiveStartInclusive };
  } finally { reopened.close(); }
}

async function main() {
  try {
    const results: unknown[] = [];
    if (selected === "all" || selected === "w2") results.push(await w2());
    if (selected === "all" || selected === "w2b") results.push(w2b());
    if (selected === "all" || selected === "w3") results.push(w3());
    if (selected === "all" || selected === "w6") results.push(w6());
    if (selected === "all" || selected === "w7") results.push(w7());
    if (selected === "all" || selected === "w8") results.push(w8());
    if (selected === "all" || selected === "x6a") results.push(x6a());
    if (selected === "all" || selected === "x6b") results.push(x6b());
    if (selected === "all" || selected === "x5a") results.push(x5a());
    if (selected === "all" || selected === "x5b") results.push(x5b());
    if (selected === "all" || selected === "x6c") results.push(x6c());
    if (results.length === 0) throw new Error(`unknown case: ${selected}`);
    console.log(JSON.stringify({ proof: "learning-facts-window", passed: true, cases: results }));
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
