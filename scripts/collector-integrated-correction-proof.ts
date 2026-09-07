import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import {LocalEventBuffer} from "../packages/collector-cli/src/buffer";
import {aiInteractionEventSchema} from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-integrated-clock-"));
const originalNow = Date.now;
const initial = originalNow();
let tick = initial;
Date.now = () => tick;
const checks: Array<{name: string; passed: boolean; detail?: unknown; error?: string}> = [];
function read(buffer: LocalEventBuffer) {
  const result = buffer.projection.readSnapshot(30);
  assert.equal(result.kind, "ready");
  if (result.kind !== "ready") throw new Error("snapshot_missing");
  return result.snapshot;
}
function settle(buffer: LocalEventBuffer) {
  for (let i = 0; i < 100; i++) {
    buffer.projection.runMaintenance(new Date(tick));
    const state = buffer.projection.status();
    if (state.parityReady && !state.dirty && Object.values(state.backlog).every(n => n === 0)) return;
  }
  throw new Error(`fixture_did_not_settle:${JSON.stringify(buffer.projection.status())}`);
}
function check(name: string, run: () => unknown) {
  tick = initial;
  try {checks.push({name, passed: true, detail: run()});}
  catch (error) {checks.push({name, passed: false, error: String(error)});}
}
try {
  check("completed_clock_only_pass_advances_verified_window_without_rebuild", () => {
    const buffer = new LocalEventBuffer(path.join(root, "clock.sqlite"));
    try {
      const before = read(buffer); tick += 60_000;
      const pass = buffer.projection.runMaintenance(new Date(tick));
      const changes = (buffer.database.prepare("select total_changes() as n").get() as {n:number}).n;
      const after = read(buffer);
      assert.equal(after.generation, before.generation);
      assert.equal(pass.snapshotBuilds, 0);
      assert.equal(after.window.since, new Date(tick - 30 * 86_400_000).toISOString());
      assert.equal(after.summary.since, after.window.since);
      assert.equal((buffer.database.prepare("select total_changes() as n").get() as {n:number}).n, changes);
      return {generation: after.generation, snapshotBuilds: pass.snapshotBuilds, since: after.window.since, getWrites: 0};
    } finally {buffer.close();}
  });
  check("quiet_success_renews_freshness_but_missing_success_expires", () => {
    const buffer = new LocalEventBuffer(path.join(root, "quiet.sqlite"));
    try {
      const before = read(buffer); tick += 180_000;
      assert.equal(read(buffer).projection.parityReady, false);
      const pass = buffer.projection.runMaintenance(new Date(tick));
      const after = read(buffer);
      const projection = after.status.projection as ReturnType<typeof buffer.projection.status>;
      assert.equal(after.generation, before.generation);
      assert.equal(pass.snapshotBuilds, 0);
      assert.equal(after.projection.parityReady, true);
      assert.equal(projection.lastSuccessAt, new Date(tick).toISOString());
      tick += 180_000;
      assert.equal(read(buffer).projection.parityReady, false);
      return {generation: after.generation, successfulAt: projection.lastSuccessAt};
    } finally {buffer.close();}
  });
  check("partial_repair_never_relabels_historical_totals_or_renews_validity", () => {
    const buffer = new LocalEventBuffer(path.join(root, "dirty.sqlite"));
    try {
      for (let i = 0; i < 251; i++) buffer.append(aiInteractionEventSchema.parse({
        id: `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`, source: "codex",
        eventType: "assistant_response", dataMode: "metadata", sessionId: "integrated-correction",
        observedAt: new Date(tick - 86_400_000).toISOString(), actionClass: "other", inputTokens: 1,
      }));
      settle(buffer);
      const before = read(buffer), successfulAt = buffer.projection.status().lastSuccessAt;
      buffer.database.prepare("update buffered_events set input_tokens=9").run();
      tick += 60_000;
      const pass = buffer.projection.runMaintenance(new Date(tick));
      assert.ok(pass.backlog.repairs > 0);
      const changes = (buffer.database.prepare("select total_changes() as n").get() as {n:number}).n;
      const after = read(buffer);
      const projection = after.status.projection as ReturnType<typeof buffer.projection.status>;
      assert.equal(after.generation, before.generation);
      assert.equal(after.window.since, before.window.since);
      assert.equal(after.projection.parityReady, false);
      assert.equal(projection.parityReady, false);
      assert.equal(buffer.projection.status().lastSuccessAt, successfulAt);
      assert.equal((buffer.database.prepare("select total_changes() as n").get() as {n:number}).n, changes);
      settle(buffer);
      assert.equal(read(buffer).projection.parityReady, true);
      return {remainingAfterPartial: pass.backlog.repairs, getWrites: 0, historicalSince: after.window.since};
    } finally {buffer.close();}
  });
  for (const invalidation of ["generation_changed", "dirty_control"] as const) {
    check(`interleaved_${invalidation}_cannot_relabel_prior_generation`, () => {
      const file = path.join(root, `${invalidation}.sqlite`);
      const buffer = new LocalEventBuffer(file);
      const writer = new Database(file);
      const originalPrepare = buffer.database.prepare;
      const prepare = originalPrepare.bind(buffer.database);
      let interleaved = false;
      try {
        const before = read(buffer);
        const changes = (prepare("select total_changes() as n").get() as {n:number}).n;
        tick += 60_000;
        // A second connection commits after the payload/control reads but
        // before the verified-window lookup. No thread timing is assumed.
        buffer.database.prepare = ((sql: string) => {
          if (!interleaved && /select\s+(?:\w+\.)?cutoff_at\s+as\s+cutoffAt/i.test(sql)) {
            interleaved = true;
            writer.transaction(() => {
              writer.prepare("update dashboard_window_control set cutoff_at=? where days=30")
                .run(new Date(tick - 30 * 86_400_000).toISOString());
              writer.prepare(invalidation === "generation_changed"
                ? "update dashboard_projection_control set generation=generation+1 where singleton=1"
                : "update dashboard_projection_control set dirty=1 where singleton=1").run();
            })();
          }
          return prepare(sql);
        }) as typeof buffer.database.prepare;
        const after = read(buffer);
        assert.equal(interleaved, true);
        assert.equal(after.generation, before.generation);
        assert.equal(after.window.since, before.window.since);
        assert.equal(after.summary.since, before.summary.since);
        assert.equal((prepare("select total_changes() as n").get() as {n:number}).n, changes);
        return {interleaved, generation: after.generation, since: after.window.since, getWrites: 0};
      } finally {buffer.database.prepare = originalPrepare; writer.close(); buffer.close();}
    });
  }
  const receipt = {schema: "plimsoll-integrated-clock-proof/v1", complete: checks.length === 5,
    expected: 5, passed: checks.filter(c => c.passed).length, failed: checks.filter(c => !c.passed).length, checks};
  if (process.argv[2]) fs.writeFileSync(process.argv[2], JSON.stringify(receipt, null, 2) + "\n");
  console.log(JSON.stringify(receipt));
  if (!receipt.complete || receipt.failed) process.exitCode = 1;
} finally {Date.now = originalNow; fs.rmSync(root, {recursive:true, force:true});}
