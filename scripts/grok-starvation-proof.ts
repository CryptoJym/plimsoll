/**
 * Regression for eco-6hoxj.163.42.
 *
 * The fixture deliberately has the shape that exposed the Studio0 incident:
 * a busy Codex source plus roughly 100 Grok cwd groups and 40 sessions per
 * group. The Codex adapter is slowed with a bounded disk-delay shim so the
 * proof is deterministic and never touches an installed collector.
 *
 * Modes:
 *   --expect=red    pinned 0.7.37 must leave Grok at zero over the observed ticks
 *   --expect=green  the repaired scheduler must reach the newest sessions
 *   --equal-mtime  mutation probe: recency has been removed by making every
 *                   directory tie; the recent-first assertion must fail
 *
 * Fixture times are relative to now: the recent sessions were written an
 * hour ago, the rest weeks ago, so the proof means the same on any date.
 * Time is virtual (scripts/lib/virtual-clock.ts): the slow calls charge
 * their 220 ms and real work costs nothing, so a loaded host and a CI runner
 * reach the same result.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { GrokUsageTailer } from "../packages/collector-cli/src/grok-usage-tailer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { RolloutTailer, type RolloutScanOptions } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { DEFAULT_JSONL_TAILER_IO } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { installVirtualClock, spend } from "./lib/virtual-clock";

const mode = process.argv.find((arg) => arg.startsWith("--expect="))?.split("=", 2)[1] ?? "green";
const equalMtime = process.argv.includes("--equal-mtime");
const ticks = 6;
const groupCount = 100;
const sessionsPerGroup = 40;
const recentGroupIndex = groupCount - 1;
const recentSessionIndex = sessionsPerGroup - 1;
const recentFiles = 5;
const DAY_MS = 24 * 60 * 60 * 1_000;

function uuid(index: number) {
  return `7a1e0000-0000-4000-8000-${String(index).padStart(12, "0")}`;
}

function usageDocument(sessionId: string, endedAt: string) {
  return {
    sessionId,
    updatedAt: endedAt,
    session: {
      inputTokens: 11,
      outputTokens: 3,
      primaryModelId: "grok-4.7-build",
    },
    turns: [{
      turnNumber: 1,
      endedAt,
      inputTokens: 11,
      outputTokens: 3,
      reasoningTokens: 1,
      cachedReadTokens: 2,
      cacheCreationTokens: 0,
      costUsdTicks: 100,
      primaryModelId: "grok-4.7-build",
    }],
  };
}

function writeFixture(home: string) {
  const sessionsRoot = path.join(home, "sessions");
  fs.mkdirSync(sessionsRoot, { recursive: true, mode: 0o700 });
  const old = new Date(Date.now() - 23 * DAY_MS);
  const recent = new Date(Date.now() - 60 * 60 * 1_000);
  const recentPaths: string[] = [];
  const recentSessionIds: string[] = [];
  let ordinal = 0;
  for (let groupIndex = 0; groupIndex < groupCount; groupIndex += 1) {
    const group = path.join(sessionsRoot, `group-${String(groupIndex).padStart(3, "0")}`);
    fs.mkdirSync(group, { recursive: true, mode: 0o700 });
    for (let sessionIndex = 0; sessionIndex < sessionsPerGroup; sessionIndex += 1) {
      const sessionId = uuid(ordinal++);
      // Grok's real layout uses the session id as the directory name; keeping
      // that invariant makes the parser exercise its identity fence too.
      const session = path.join(group, sessionId);
      fs.mkdirSync(session, { recursive: true, mode: 0o700 });
      const file = path.join(session, "usage.json");
      fs.writeFileSync(file, `${JSON.stringify(usageDocument(sessionId, old.toISOString()))}\n`, { mode: 0o600 });
      fs.utimesSync(session, old, old);
      fs.utimesSync(file, old, old);
      if (groupIndex === recentGroupIndex && sessionIndex >= sessionsPerGroup - recentFiles) {
        recentPaths.push(file);
        recentSessionIds.push(sessionId);
        fs.utimesSync(session, recent, recent);
        fs.utimesSync(file, recent, recent);
      }
    }
    fs.utimesSync(group, groupIndex === recentGroupIndex ? recent : old, groupIndex === recentGroupIndex ? recent : old);
  }
  fs.utimesSync(sessionsRoot, recent, recent);
  return { sessionsRoot, recentPaths, recentSessionIds };
}

function slowCodexTailer(buffer: LocalEventBuffer, root: string) {
  const today = new Date().toISOString().slice(0, 10).split("-");
  const day = path.join(root, ...today);
  fs.mkdirSync(day, { recursive: true, mode: 0o700 });
  const timestamp = new Date().toISOString();
  // Several real JSONL rollout files make the Codex side a genuine discovery
  // tree. The read seam injects one busy-disk stall per admitted file; the
  // pinned scheduler exhausts before it reaches this seam, while the repaired
  // scheduler still admits a bounded Codex slice and then Grok.
  for (let index = 0; index < 16; index += 1) {
    const sessionId = uuid(9000 + index);
    const lines = [
      { type: "session_meta", timestamp, payload: { id: sessionId } },
      { type: "turn_context", timestamp, payload: { model: "gpt-5.5" } },
      { type: "event_msg", timestamp, payload: {
        type: "token_count", info: { total_token_usage: {
          input_tokens: 1, cached_input_tokens: 0, output_tokens: 1, reasoning_output_tokens: 0,
        } },
      } },
    ];
    fs.writeFileSync(path.join(day, `rollout-${sessionId}.jsonl`), `${lines.map(line => JSON.stringify(line)).join("\n")}\n`, { mode: 0o600 });
  }
  let slowReads = 0;
  const io = {
    ...DEFAULT_JSONL_TAILER_IO,
    readTail: (...args: Parameters<typeof DEFAULT_JSONL_TAILER_IO.readTail>) => {
      // A single synchronous filesystem call on Studio0 can exceed the 200 ms
      // cadence wall. Charging it here models that call without touching live data.
      slowReads += 1;
      spend(220);
      return DEFAULT_JSONL_TAILER_IO.readTail(...args);
    },
  };
  return { tailer: new RolloutTailer(buffer, root, undefined, io), files: Array.from({ length: 16 }, (_, index) => {
    const sessionId = uuid(9000 + index);
    return path.join(day, `rollout-${sessionId}.jsonl`);
  }), liveFile: path.join(day, `rollout-${uuid(9999)}.jsonl`), slowReads: () => slowReads };
}

function countGrokEvents(buffer: LocalEventBuffer) {
  return Number((buffer.database.prepare(
    "select count(*) as count from buffered_events where source = 'grok' and input_tokens is not null",
  ).get() as { count: number }).count);
}

function sourceTurn(buffer: LocalEventBuffer) {
  return (buffer.database.prepare(
    "select value from maintenance_state where key = 'automatic_capture_source_turn'",
  ).get() as { value?: string } | undefined)?.value ?? null;
}

async function main() {
  installVirtualClock();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-grok-starvation-proof-"));
  const home = path.join(root, ".grok");
  const codexRoot = path.join(root, ".codex", "sessions");
  const claudeRoot = path.join(root, ".claude");
  fs.mkdirSync(claudeRoot, { recursive: true, mode: 0o700 });
  const fixture = writeFixture(home);
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
  // Make the pre-capture repair stage consume the cadence on every tick. This
  // is the busy-host condition that leaves 0.7.37's guarded rotation marker
  // stuck at Codex before any source scan starts.
  const projection = buffer.projection as unknown as {
    runMaintenance: (...args: unknown[]) => unknown;
  };
  const realProjectionMaintenance = projection.runMaintenance.bind(projection);
  projection.runMaintenance = (...args: unknown[]) => {
    spend(220);
    return realProjectionMaintenance(...args);
  };
  const codexFixture = slowCodexTailer(buffer, codexRoot);
  const rollout = codexFixture.tailer;
  const transcript = new TranscriptTailer(buffer, claudeRoot);
  const grok = new GrokUsageTailer(buffer, home);
  const maintenance = new CollectorMaintenance(buffer, rollout, transcript, undefined, grok);
  const observations: Array<Record<string, unknown>> = [];
  let codexLiveGenerationWritten = false;
  try {
    if (equalMtime) {
      const tie = new Date(Date.now() - 14 * DAY_MS);
      for (const group of fs.readdirSync(fixture.sessionsRoot)) {
        const groupPath = path.join(fixture.sessionsRoot, group);
        fs.utimesSync(groupPath, tie, tie);
        for (const session of fs.readdirSync(groupPath)) {
          const sessionPath = path.join(groupPath, session);
          fs.utimesSync(sessionPath, tie, tie);
          fs.utimesSync(path.join(sessionPath, "usage.json"), tie, tie);
        }
      }
    }
    for (let tick = 0; tick < ticks; tick += 1) {
      const repairRaw = (buffer.database.prepare(
        "select value from maintenance_state where key = 'automatic_repair_service_v1'",
      ).get() as { value?: string } | undefined)?.value;
      if (repairRaw) {
        const repair = JSON.parse(repairRaw) as { cycles?: number } & Record<string, unknown>;
        repair.cycles = 0;
        buffer.database.prepare(
          "update maintenance_state set value = ?, updated_at = ? where key = 'automatic_repair_service_v1'",
        ).run(JSON.stringify(repair), new Date().toISOString());
      }
      const before = sourceTurn(buffer);
      const tickStartedAt = performance.now();
      const result = await maintenance.runRecent();
      const grokResult = result.grok;
      observations.push({
        tick,
        turnBefore: before,
        turnAfter: sourceTurn(buffer),
        grokFilesRead: grokResult?.filesRead ?? 0,
        grokFilesParsed: grokResult?.filesParsed ?? 0,
        grokEvents: grokResult?.eventsAppended ?? 0,
        grokEntries: grokResult?.activity.discoveryEntries ?? 0,
        grokDeferredBeforeIo: grokResult?.activity.scan.deferredBeforeIo ?? null,
        grokSeen: grokResult?.activity.scan.usageFiles.seen ?? 0,
        codexDeferredBeforeIo: result.rollout.activity.scan?.deferredBeforeIo ?? null,
        codexFilesRead: result.rollout.filesRead,
        codexFilesSeen: result.rollout.filesSeen,
        codexExcluded: result.rollout.excludedGenerations,
        codexDeferred: result.rollout.deferredGenerations,
        codexReadErrors: result.rollout.readErrors,
        codexParseErrors: result.rollout.parseErrors,
        codexBytesRead: result.rollout.bytesRead,
        codexSlowReads: codexFixture.slowReads(),
        claudeFilesRead: result.transcript.filesRead,
        claudeDeferredBeforeIo: result.transcript.activity.scan?.deferredBeforeIo ?? null,
        runWallMs: Number((performance.now() - tickStartedAt).toFixed(3)),
        baselineStatus: maintenance.status().baseline.status,
        budget: maintenance.status().budget,
        stageTimings: result.stageTimings,
      });
      // Baseline enrollment intentionally excludes files present at install.
      // Once the two-pass baseline is complete, rewrite the fixture generation
      // so the next capture turn has real Codex work to spend its slice on.
      if (!codexLiveGenerationWritten && maintenance.status().baseline.status === "complete") {
        // The fixture's live generation is installed at the same boundary as
        // a real producer's first post-enrollment write. Reset only the
        // in-memory discovery attempt so the next cadence enumerates it.
        rollout.close();
        const liveLine = JSON.stringify({
          type: "event_msg", timestamp: new Date().toISOString(), payload: {
            type: "token_count", info: { total_token_usage: {
              input_tokens: 2, cached_input_tokens: 0, output_tokens: 1, reasoning_output_tokens: 0,
            } },
          },
        });
        fs.writeFileSync(codexFixture.liveFile, `${liveLine}\n`, { mode: 0o600 });
        codexLiveGenerationWritten = true;
      }
    }
    const events = countGrokEvents(buffer);
    const firstRecentTick = observations.findIndex((row) => Number(row.grokEvents) > 0);
    const recentDiscovered = fixture.recentSessionIds.every((sessionId) => Boolean(buffer.database.prepare(
      "select 1 from grok_usage_turn_state where session_id = ? limit 1",
    ).get(sessionId)));
    const result = {
      schema: "eco-6hoxj.163.42.grok-starvation-proof.v1",
      mode,
      equalMtime,
      fixture: {
        groups: groupCount,
        sessionsPerGroup,
        sessions: groupCount * sessionsPerGroup,
        usageFiles: groupCount * sessionsPerGroup,
        recentFiles,
      },
      observations,
      grokEvents: events,
      grokFilesRead: observations.reduce((total, row) => total + Number(row.grokFilesRead), 0),
      grokTurnsAdmitted: observations.filter((row) => row.grokDeferredBeforeIo === false).length,
      activeWallMs: Number(observations.reduce((total, row) => total + Number(row.runWallMs), 0).toFixed(3)),
      firstRecentTick,
      recentFilesReachable: recentDiscovered,
      sourceTurnAfterTicks: sourceTurn(buffer),
    };
    if (mode === "red") {
      assert.equal(events, 0, "pinned 0.7.37 must reproduce Grok starvation");
      assert.ok(observations.every((row) => row.grokDeferredBeforeIo === true),
        "each pinned tick must explain the pre-I/O defer");
    } else {
      assert.ok(events > 0, "the repaired scheduler must append Grok token events");
      assert.ok(firstRecentTick >= 0 && firstRecentTick <= 2,
        `recent Grok sessions must be reached within three ticks (got ${firstRecentTick})`);
      assert.ok(observations.some((row) => row.codexDeferredBeforeIo === false),
        "Codex must continue to receive an admitted turn");
      assert.ok(observations.some((row) => Number(row.codexSlowReads) > 0),
        "Codex fixture capture must reach its slow filesystem seam");
      assert.ok(observations.some((row) => row.claudeDeferredBeforeIo === false),
        "Claude capture must continue to receive an admitted turn");
      assert.ok(recentDiscovered, "recent session metadata must be visible without content reads");
    }
    assert.ok(observations.every((row) => {
      const budget = row.budget as { bytesRead: number; maxBytes: number; recordsParsed: number;
        maxRecords: number; eventsAppended: number; maxEvents: number };
      return budget.bytesRead <= budget.maxBytes && budget.recordsParsed <= budget.maxRecords &&
        budget.eventsAppended <= budget.maxEvents;
    }), "every tick must stay within aggregate capture ceilings");
    console.log(JSON.stringify(result, null, 2));
  } finally {
    maintenance.close();
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
