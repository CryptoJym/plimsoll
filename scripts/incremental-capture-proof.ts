/**
 * Adversarial proof for issue 0041 / GitHub #77.
 *
 * Uses only temporary JSONL trees and a temporary SQLite ledger. It exercises
 * byte-offset suffix reads, partial-line deferral across restart, unchanged
 * scans, truncation recovery, continuity fingerprints, runtime checkpoint
 * validation, legacy state migration, deterministic replay, and the
 * metadata-only privacy boundary for both capture tailers.
 *
 * Run: pnpm exec tsx scripts/incremental-capture-proof.ts
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  beginAutomaticCaptureBaseline,
  classifyCaptureBaselineFile,
  completeAutomaticCaptureBaseline,
} from "../packages/collector-cli/src/capture-baseline";
import {
  historyCoverageStatus,
  recordExplicitFullHistoryCoverage,
} from "../packages/collector-cli/src/history-coverage";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { jsonlScanStateKey } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { runRepoEnrichmentMaintenance } from "../packages/collector-cli/src/maintenance";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { resolveRepoContextRequests } from "../packages/collector-cli/src/repo-context";
import { aiInteractionEventSchema, remoteLinkageHash } from "../packages/shared/src/index";

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-incremental-proof-"));
const buffer = new LocalEventBuffer(path.join(tempDir, "proof.sqlite"));
let bufferClosed = false;

const ROLLOUT_SESSION = "019e1111-2222-7333-8444-555555555555";
const ROTATED_SESSION = "019e2222-3333-7444-8555-666666666666";
const LEGACY_SESSION = "019e3333-4444-7555-8666-777777777777";
const CONTINUITY_SESSION = "019e4444-5555-7666-8777-888888888888";
const CONTINUITY_REPLACEMENT_SESSION = "019e5555-6666-7777-8888-999999999999";
const EQUAL_REWRITE_SESSION = "019e6666-7777-7888-8999-aaaaaaaaaaaa";
const EQUAL_REWRITE_REPLACEMENT_SESSION = "019e7777-8888-7999-8aaa-bbbbbbbbbbbb";
const TRANSCRIPT_SESSION = "44445555-6666-4777-8888-99990000aaaa";
const RAW_SENTINEL = "RAW_CONTENT_SENTINEL must never persist";

function rolloutLine(timestamp: string, type: string, payload: Record<string, unknown>) {
  return JSON.stringify({ timestamp, type, payload });
}

function tokenCountLine(timestamp: string, input: number, cached: number, output: number) {
  return rolloutLine(timestamp, "event_msg", {
    type: "token_count",
    info: {
      total_token_usage: {
        input_tokens: input,
        cached_input_tokens: cached,
        output_tokens: output,
        reasoning_output_tokens: 0,
        total_tokens: input + output,
      },
    },
    rate_limits: { plan_type: "pro" },
  });
}

function assistantLine(messageId: string, input: number, output: number) {
  return JSON.stringify({
    type: "assistant",
    sessionId: TRANSCRIPT_SESSION,
    cwd: path.join(tempDir, "not-a-repo"),
    timestamp: "2026-07-15T12:00:00.000Z",
    message: {
      id: messageId,
      model: "claude-sonnet-4-20250514",
      content: [{ type: "text", text: RAW_SENTINEL }],
      usage: {
        input_tokens: input,
        cache_read_input_tokens: 0,
        cache_creation_input_tokens: 0,
        output_tokens: output,
      },
    },
  });
}

async function proveRolloutTailing() {
  const rolloutDay = path.join(tempDir, "rollouts", "2026", "07", "15");
  fs.mkdirSync(rolloutDay, { recursive: true });
  const file = path.join(
    rolloutDay,
    `rollout-2026-07-15T10-00-00-${ROLLOUT_SESSION}.jsonl`,
  );
  const completePrefix = [
    rolloutLine("2026-07-15T10:00:00.000Z", "session_meta", {
      id: ROLLOUT_SESSION,
      cwd: path.join(tempDir, "not-a-repo"),
      originator: "proof",
    }),
    rolloutLine("2026-07-15T10:00:01.000Z", "turn_context", {
      model: "gpt-5.5",
      cwd: path.join(tempDir, "not-a-repo"),
    }),
    rolloutLine("2026-07-15T10:00:02.000Z", "event_msg", {
      type: "user_message",
      message: RAW_SENTINEL,
    }),
  ].join("\n") + "\n";
  const firstToken = tokenCountLine("2026-07-15T10:00:03.000Z", 100, 20, 10);
  const split = Math.floor(firstToken.length / 2);
  fs.writeFileSync(file, completePrefix + firstToken.slice(0, split));

  const first = await new RolloutTailer(buffer, path.join(tempDir, "rollouts"), () => []).scan({ scope: "full" });
  assert.equal(first.filesRead, 1);
  assert.equal(first.eventsAppended, 0, "partial token line must not commit");
  assert.equal(first.parseErrors, 0, "partial framing is not a JSON parse error");
  assert.equal(first.bytesDeferred, Buffer.byteLength(firstToken.slice(0, split)));
  const firstCursor = buffer.database
    .prepare(
      `select committed_offset as committedOffset, head_bytes as headBytes,
         continuity_bytes as continuityBytes
       from rollout_scan_state where file = ?`,
    )
    .get(jsonlScanStateKey(file)) as { committedOffset: number; headBytes: number; continuityBytes: number };
  assert.equal(firstCursor.committedOffset, Buffer.byteLength(completePrefix));

  fs.appendFileSync(file, firstToken.slice(split) + "\n");
  const firstCompletionSize = fs.statSync(file).size;
  const second = await new RolloutTailer(buffer, path.join(tempDir, "rollouts"), () => []).scan({ scope: "full" });
  const suffixFromCommit = firstCompletionSize - firstCursor.committedOffset;
  assert.equal(second.eventsAppended, 1);
  assert.equal(second.tokensAppended.input, 100);
  assert.ok(
    second.bytesRead <= suffixFromCommit + firstCursor.headBytes + firstCursor.continuityBytes,
    `read ${second.bytesRead} bytes for ${suffixFromCommit}-byte suffix plus bounded probes`,
  );

  const unchanged = await new RolloutTailer(buffer, path.join(tempDir, "rollouts"), () => []).scan({ scope: "full" });
  assert.equal(unchanged.filesRead, 0);
  assert.equal(unchanged.bytesRead, 0);
  assert.equal(unchanged.eventsAppended, 0);

  const secondToken = tokenCountLine("2026-07-15T10:00:04.000Z", 250, 50, 25) + "\n";
  const beforeOrdinaryAppend = fs.statSync(file);
  fs.appendFileSync(file, secondToken);
  const afterOrdinaryAppend = fs.statSync(file);
  assert.notEqual(afterOrdinaryAppend.ctimeMs, beforeOrdinaryAppend.ctimeMs);
  const appended = await new RolloutTailer(buffer, path.join(tempDir, "rollouts"), () => []).scan({ scope: "full" });
  assert.equal(appended.eventsAppended, 1);
  assert.equal(appended.filesReset, 0, "ordinary append growth must retain parser continuity");
  assert.equal(appended.tokensAppended.input, 150, "cumulative state must telescope across scans");
  assert.ok(appended.bytesRead <= Buffer.byteLength(secondToken) + 1024);

  const rolloutTotals = buffer.database
    .prepare(
      `select count(*) as events, sum(input_tokens) as inputTokens
       from buffered_events where event_type = 'usage_rollout' and session_id = ?`,
    )
    .get(ROLLOUT_SESSION) as { events: number; inputTokens: number };
  assert.deepEqual(rolloutTotals, { events: 2, inputTokens: 250 });

  // Persisted parser state is untrusted input. Empty objects, JSON null,
  // wrong field types, and an incompatible checkpoint version must all force
  // a deterministic rebuild instead of crashing or diffing from ZERO.
  buffer.database
    .prepare(`update rollout_scan_state set parser_state_json = ? where file = ?`)
    .run("{}", jsonlScanStateKey(file));
  const emptyStateRebuild = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(emptyStateRebuild.checkpointRebuilds, 1);
  assert.deepEqual(
    buffer.database
      .prepare(
        `select count(*) as events, sum(input_tokens) as inputTokens
         from buffered_events where event_type = 'usage_rollout' and session_id = ?`,
      )
      .get(ROLLOUT_SESSION),
    { events: 2, inputTokens: 250 },
  );

  buffer.database
    .prepare(`update rollout_scan_state set parser_state_json = ? where file = ?`)
    .run("null", jsonlScanStateKey(file));
  const thirdToken = tokenCountLine("2026-07-15T10:00:05.000Z", 400, 80, 40) + "\n";
  fs.appendFileSync(file, thirdToken);
  const nullStateRebuild = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(nullStateRebuild.checkpointRebuilds, 1);
  assert.deepEqual(
    buffer.database
      .prepare(
        `select count(*) as events, sum(input_tokens) as inputTokens
         from buffered_events where event_type = 'usage_rollout' and session_id = ?`,
      )
      .get(ROLLOUT_SESSION),
    { events: 3, inputTokens: 400 },
  );

  buffer.database
    .prepare(`update rollout_scan_state set parser_state_json = ? where file = ?`)
    .run(
      JSON.stringify({
        parserKind: "codex-rollout-v2",
        checkpointVersion: 2,
        previous: null,
        tokenCountIndex: "2",
      }),
      jsonlScanStateKey(file),
    );
  const wrongTypeRebuild = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(wrongTypeRebuild.checkpointRebuilds, 1);

  buffer.database
    .prepare(`update rollout_scan_state set checkpoint_version = ? where file = ?`)
    .run(999, jsonlScanStateKey(file));
  const wrongVersionRebuild = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(wrongVersionRebuild.checkpointRebuilds, 1);

  buffer.database
    .prepare(`update rollout_scan_state set parser_state_json = ? where file = ?`)
    .run("{malformed-json", jsonlScanStateKey(file));
  const malformedStateRebuild = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(malformedStateRebuild.checkpointRebuilds, 1);

  const rotated = [
    rolloutLine("2026-07-15T11:00:00.000Z", "session_meta", { id: ROTATED_SESSION }),
    rolloutLine("2026-07-15T11:00:01.000Z", "turn_context", { model: "gpt-5.5" }),
    tokenCountLine("2026-07-15T11:00:02.000Z", 40, 0, 4),
  ].join("\n") + "\n";
  fs.truncateSync(file, 0);
  fs.writeFileSync(file, rotated);
  const afterTruncation = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(afterTruncation.filesReset, 0);
  assert.equal(afterTruncation.unresolvedRecords, 1);
  assert.equal(afterTruncation.eventsAppended, 0);
  assert.equal(
    (buffer.database
      .prepare(`select count(*) as n from buffered_events where session_id = ?`)
      .get(ROTATED_SESSION) as { n: number }).n,
    0,
  );
  const rotatedGeneration = `${file}.replacement`;
  fs.writeFileSync(rotatedGeneration, rotated);
  fs.renameSync(rotatedGeneration, file);
  const afterReplacementGeneration = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(afterReplacementGeneration.filesReset, 1);
  assert.equal(afterReplacementGeneration.eventsAppended, 1);

  const legacyFile = path.join(
    rolloutDay,
    `rollout-2026-07-15T12-00-00-${LEGACY_SESSION}.jsonl`,
  );
  const legacyPrefix = [
    rolloutLine("2026-07-15T12:00:00.000Z", "session_meta", { id: LEGACY_SESSION }),
    rolloutLine("2026-07-15T12:00:01.000Z", "turn_context", { model: "gpt-5.5" }),
    tokenCountLine("2026-07-15T12:00:02.000Z", 10, 0, 1),
  ].join("\n") + "\n";
  fs.writeFileSync(legacyFile, legacyPrefix);
  buffer.database
    .prepare(`insert into rollout_scan_state (file, size, scanned_at) values (?, ?, ?)`)
    .run(legacyFile, fs.statSync(legacyFile).size, new Date().toISOString());
  const legacyUnchanged = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(legacyUnchanged.filesRead, 0, "unchanged legacy rows must not trigger a corpus rebuild");
  fs.appendFileSync(legacyFile, tokenCountLine("2026-07-15T12:00:03.000Z", 25, 0, 2) + "\n");
  const legacyGrowth = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(legacyGrowth.legacyRebuilds, 1);
  assert.equal(
    (buffer.database
      .prepare(`select count(*) as n from buffered_events where session_id = ?`)
      .get(LEGACY_SESSION) as { n: number }).n,
    2,
  );

  // Same-inode truncate-and-regrow can preserve the first 512 bytes and grow
  // beyond the prior size. The checkpoint-boundary continuity probe must see
  // that the already-committed region changed and fail closed. Only a new
  // physical generation may restart from byte zero.
  const continuityFile = path.join(
    rolloutDay,
    `rollout-2026-07-15T13-00-00-${CONTINUITY_SESSION}.jsonl`,
  );
  const sharedHead = rolloutLine("2026-07-15T13:00:00.000Z", "event_msg", {
    type: "user_message",
    message: `stable-head-${"h".repeat(900)}`,
  }) + "\n";
  const continuityOriginal = sharedHead + [
    rolloutLine("2026-07-15T13:00:01.000Z", "session_meta", { id: CONTINUITY_SESSION }),
    rolloutLine("2026-07-15T13:00:02.000Z", "turn_context", { model: "gpt-5.5" }),
    tokenCountLine("2026-07-15T13:00:03.000Z", 60, 0, 6),
  ].join("\n") + "\n";
  fs.writeFileSync(continuityFile, continuityOriginal);
  const continuityInitial = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(continuityInitial.eventsAppended, 1);
  const beforeRewrite = fs.statSync(continuityFile);
  const continuityReplacement = sharedHead + [
    rolloutLine("2026-07-15T13:01:01.000Z", "session_meta", {
      id: CONTINUITY_REPLACEMENT_SESSION,
    }),
    rolloutLine("2026-07-15T13:01:02.000Z", "event_msg", {
      type: "user_message",
      message: `replacement-padding-${"r".repeat(1200)}`,
    }),
    rolloutLine("2026-07-15T13:01:03.000Z", "turn_context", { model: "gpt-5.5" }),
    tokenCountLine("2026-07-15T13:01:04.000Z", 70, 0, 7),
  ].join("\n") + "\n";
  assert.deepEqual(
    Buffer.from(continuityOriginal).subarray(0, 512),
    Buffer.from(continuityReplacement).subarray(0, 512),
  );
  assert.ok(Buffer.byteLength(continuityReplacement) > beforeRewrite.size);
  fs.truncateSync(continuityFile, 0);
  fs.writeFileSync(continuityFile, continuityReplacement);
  assert.equal(fs.statSync(continuityFile).ino, beforeRewrite.ino, "fixture must preserve inode");
  const continuityReset = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(continuityReset.filesReset, 0);
  assert.equal(continuityReset.unresolvedRecords, 1);
  assert.equal(
    (buffer.database
      .prepare(`select count(*) as n from buffered_events where session_id = ?`)
      .get(CONTINUITY_REPLACEMENT_SESSION) as { n: number }).n,
    0,
  );
  const continuityNewGeneration = `${continuityFile}.replacement`;
  fs.writeFileSync(continuityNewGeneration, continuityReplacement);
  fs.renameSync(continuityNewGeneration, continuityFile);
  const continuityRecovered = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(continuityRecovered.filesReset, 1);
  assert.equal(
    (buffer.database
      .prepare(`select count(*) as n from buffered_events where session_id = ?`)
      .get(CONTINUITY_REPLACEMENT_SESSION) as { n: number }).n,
    1,
  );

  // A same-inode rewrite can deliberately restore the original size and
  // mtime. The persisted ctime/birthtime generation envelope must prevent
  // the unchanged fast path before bounded probes fail the generation closed.
  const equalRewriteFile = path.join(
    rolloutDay,
    `rollout-2026-07-15T14-00-00-${EQUAL_REWRITE_SESSION}.jsonl`,
  );
  const equalOriginal = [
    rolloutLine("2026-07-15T14:00:00.000Z", "session_meta", { id: EQUAL_REWRITE_SESSION }),
    rolloutLine("2026-07-15T14:00:01.000Z", "turn_context", { model: "gpt-5.5" }),
    tokenCountLine("2026-07-15T14:00:02.000Z", 60, 0, 6),
  ].join("\n") + "\n";
  const equalReplacement = [
    rolloutLine("2026-07-15T14:00:00.000Z", "session_meta", {
      id: EQUAL_REWRITE_REPLACEMENT_SESSION,
    }),
    rolloutLine("2026-07-15T14:00:01.000Z", "turn_context", { model: "gpt-5.5" }),
    tokenCountLine("2026-07-15T14:00:02.000Z", 70, 0, 7),
  ].join("\n") + "\n";
  assert.equal(Buffer.byteLength(equalReplacement), Buffer.byteLength(equalOriginal));
  fs.writeFileSync(equalRewriteFile, equalOriginal);
  const equalFixedTime = new Date("2026-07-15T14:00:03.000Z");
  fs.utimesSync(equalRewriteFile, equalFixedTime, equalFixedTime);
  const equalInitial = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(equalInitial.eventsAppended, 1);
  const equalBefore = fs.statSync(equalRewriteFile);
  await new Promise((resolve) => setTimeout(resolve, 5));
  fs.truncateSync(equalRewriteFile, 0);
  fs.writeFileSync(equalRewriteFile, equalReplacement);
  fs.utimesSync(equalRewriteFile, equalFixedTime, equalFixedTime);
  const equalAfter = fs.statSync(equalRewriteFile);
  assert.equal(equalAfter.ino, equalBefore.ino);
  assert.equal(equalAfter.size, equalBefore.size);
  assert.equal(equalAfter.mtimeMs, equalBefore.mtimeMs);
  assert.notEqual(equalAfter.ctimeMs, equalBefore.ctimeMs);
  const equalRewrite = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(equalRewrite.filesRead, 1);
  assert.equal(equalRewrite.filesReset, 0);
  assert.equal(equalRewrite.unresolvedRecords, 1);
  assert.equal(
    (buffer.database
      .prepare(`select count(*) as n from buffered_events where session_id = ?`)
      .get(EQUAL_REWRITE_REPLACEMENT_SESSION) as { n: number }).n,
    0,
  );
  const equalNewGeneration = `${equalRewriteFile}.replacement`;
  fs.writeFileSync(equalNewGeneration, equalReplacement);
  fs.utimesSync(equalNewGeneration, equalFixedTime, equalFixedTime);
  fs.renameSync(equalNewGeneration, equalRewriteFile);
  const equalRecovered = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan({ scope: "full" });
  assert.equal(equalRecovered.filesReset, 1);
  assert.equal(
    (buffer.database
      .prepare(`select count(*) as n from buffered_events where session_id = ?`)
      .get(EQUAL_REWRITE_REPLACEMENT_SESSION) as { n: number }).n,
    1,
  );

  // Forced replay is deterministic: deleting only the scan cursor cannot
  // create extra event rows.
  const beforeReplay = (buffer.database
    .prepare(`select count(*) as n from buffered_events where event_type = 'usage_rollout'`)
    .get() as { n: number }).n;
  buffer.database.prepare(`delete from rollout_scan_state where file = ?`).run(jsonlScanStateKey(legacyFile));
  await new RolloutTailer(buffer, path.join(tempDir, "rollouts"), () => []).scan({ scope: "full" });
  const afterReplay = (buffer.database
    .prepare(`select count(*) as n from buffered_events where event_type = 'usage_rollout'`)
    .get() as { n: number }).n;
  assert.equal(afterReplay, beforeReplay);

  return {
    first,
    second,
    unchanged,
    appended,
    emptyStateRebuild,
    nullStateRebuild,
    wrongTypeRebuild,
    wrongVersionRebuild,
    malformedStateRebuild,
    afterTruncation,
    legacyGrowth,
    continuityReset,
    equalRewrite,
  };
}

async function proveTranscriptTailing() {
  const projectsDir = path.join(tempDir, "transcripts");
  const projectDir = path.join(projectsDir, "proof-project");
  fs.mkdirSync(projectDir, { recursive: true });
  const file = path.join(projectDir, `${TRANSCRIPT_SESSION}.jsonl`);
  fs.writeFileSync(file, assistantLine("msg-1", 20, 2) + "\n");

  const first = await new TranscriptTailer(buffer, projectsDir).scan({ scope: "full" });
  assert.equal(first.eventsAppended, 1);
  const secondLine = assistantLine("msg-2", 30, 3);
  const split = Math.floor(secondLine.length / 2);
  fs.appendFileSync(file, secondLine.slice(0, split));
  const partial = await new TranscriptTailer(buffer, projectsDir).scan({ scope: "full" });
  assert.equal(partial.eventsAppended, 0);
  assert.equal(partial.parseErrors, 0);
  assert.equal(partial.bytesDeferred, Buffer.byteLength(secondLine.slice(0, split)));

  const unchanged = await new TranscriptTailer(buffer, projectsDir).scan({ scope: "full" });
  assert.equal(unchanged.filesRead, 0);
  assert.equal(unchanged.bytesRead, 0);

  fs.appendFileSync(file, secondLine.slice(split) + "\n");
  const completedAfterRestart = await new TranscriptTailer(buffer, projectsDir).scan({ scope: "full" });
  assert.equal(completedAfterRestart.eventsAppended, 1);
  const totals = buffer.database
    .prepare(
      `select count(*) as events, sum(input_tokens) as inputTokens
       from buffered_events where event_type = 'usage_transcript' and session_id = ?`,
    )
    .get(TRANSCRIPT_SESSION) as { events: number; inputTokens: number };
  assert.deepEqual(totals, { events: 2, inputTokens: 50 });

  buffer.database
    .prepare(`update rollout_scan_state set parser_state_json = ? where file = ?`)
    .run("{}", jsonlScanStateKey(file));
  const emptyStateRebuild = await new TranscriptTailer(buffer, projectsDir).scan({ scope: "full" });
  assert.equal(emptyStateRebuild.checkpointRebuilds, 1);
  assert.deepEqual(
    buffer.database
      .prepare(
        `select count(*) as events, sum(input_tokens) as inputTokens
         from buffered_events where event_type = 'usage_transcript' and session_id = ?`,
      )
      .get(TRANSCRIPT_SESSION),
    { events: 2, inputTokens: 50 },
  );

  buffer.database
    .prepare(`update rollout_scan_state set parser_state_json = ?, checkpoint_version = ? where file = ?`)
    .run("null", 999, jsonlScanStateKey(file));
  fs.appendFileSync(file, assistantLine("msg-3", 10, 1) + "\n");
  const invalidVersionRebuild = await new TranscriptTailer(buffer, projectsDir).scan({ scope: "full" });
  assert.equal(invalidVersionRebuild.checkpointRebuilds, 1);
  assert.deepEqual(
    buffer.database
      .prepare(
        `select count(*) as events, sum(input_tokens) as inputTokens
         from buffered_events where event_type = 'usage_transcript' and session_id = ?`,
      )
      .get(TRANSCRIPT_SESSION),
    { events: 3, inputTokens: 60 },
  );

  return {
    first,
    partial,
    unchanged,
    completedAfterRestart,
    emptyStateRebuild,
    invalidVersionRebuild,
  };
}

async function proveParseFailuresRemainUnresolved() {
  const root = path.join(tempDir, "parse-failure-durability");
  const ledger = path.join(root, "proof.sqlite");
  const rolloutRoot = path.join(root, "rollouts");
  const rolloutDay = path.join(rolloutRoot, "2026", "07", "19");
  const transcriptRoot = path.join(root, "transcripts");
  const transcriptProject = path.join(transcriptRoot, "proof-project");
  const rolloutFile = path.join(
    rolloutDay,
    "rollout-proof-019e6000-0000-7000-8000-000000000011.jsonl",
  );
  const transcriptFile = path.join(
    transcriptProject,
    "019e6000-0000-7000-8000-000000000012.jsonl",
  );
  fs.mkdirSync(rolloutDay, { recursive: true });
  fs.mkdirSync(transcriptProject, { recursive: true });
  const validRollout = `${JSON.stringify({
    timestamp: "2026-07-19T12:00:00.000Z",
    type: "event_msg",
    payload: { type: "token_count" },
  })}\n`;
  const validTranscript = `${JSON.stringify({
    type: "assistant",
    sessionId: "019e6000-0000-7000-8000-000000000012",
    timestamp: "2026-07-19T12:00:00.000Z",
    message: { id: "empty-usage", usage: {} },
  })}\n`;
  fs.writeFileSync(rolloutFile, validRollout);
  fs.writeFileSync(transcriptFile, validTranscript);

  let parseBuffer = new LocalEventBuffer(ledger);
  const scanRollout = () =>
    new RolloutTailer(parseBuffer, rolloutRoot, () => []).scan({ scope: "full" });
  const scanTranscript = () =>
    new TranscriptTailer(parseBuffer, transcriptRoot).scan({ scope: "full" });
  const cursor = (file: string) =>
    parseBuffer.database
      .prepare(
        `select size as observedSize, committed_offset as committedOffset,
                mtime_ms as mtimeMs
         from rollout_scan_state where file = ?`,
      )
      .get(jsonlScanStateKey(file)) as
      | { observedSize: number; committedOffset: number; mtimeMs: number }
      | undefined;

  try {
    const rolloutInitial = await scanRollout();
    recordExplicitFullHistoryCoverage(parseBuffer.database, "codex", rolloutInitial);
    const transcriptInitial = await scanTranscript();
    const initialCoverage = recordExplicitFullHistoryCoverage(
      parseBuffer.database,
      "claude_code",
      transcriptInitial,
    );
    assert.equal(initialCoverage.promoted, true);
    assert.equal(initialCoverage.coverage.status, "complete");
    const rolloutCursorBefore = cursor(rolloutFile);
    const transcriptCursorBefore = cursor(transcriptFile);
    assert.ok(rolloutCursorBefore && transcriptCursorBefore);

    fs.appendFileSync(
      rolloutFile,
      '{"type":"event_msg","payload":{"type":"token_count"\n',
    );
    fs.appendFileSync(
      transcriptFile,
      '{"type":"assistant","message":{"usage":\n',
    );
    const rolloutFailure = await scanRollout();
    const rolloutFailureCoverage = recordExplicitFullHistoryCoverage(
      parseBuffer.database,
      "codex",
      rolloutFailure,
    );
    const transcriptFailure = await scanTranscript();
    const transcriptFailureCoverage = recordExplicitFullHistoryCoverage(
      parseBuffer.database,
      "claude_code",
      transcriptFailure,
    );
    assert.equal(rolloutFailure.parseErrors, 1);
    assert.equal(transcriptFailure.parseErrors, 1);
    assert.equal(rolloutFailureCoverage.promoted, false);
    assert.equal(transcriptFailureCoverage.promoted, false);
    assert.equal(transcriptFailureCoverage.coverage.status, "complete");
    assert.equal(
      transcriptFailureCoverage.coverage.sources.find(
        (source) => source.source === "claude_code",
      )?.latestFullAttempt?.parseErrors,
      1,
    );
    assert.deepEqual(cursor(rolloutFile), rolloutCursorBefore);
    assert.deepEqual(cursor(transcriptFile), transcriptCursorBefore);

    const rolloutRetry = await scanRollout();
    recordExplicitFullHistoryCoverage(parseBuffer.database, "codex", rolloutRetry);
    const transcriptRetry = await scanTranscript();
    recordExplicitFullHistoryCoverage(parseBuffer.database, "claude_code", transcriptRetry);
    assert.equal(rolloutRetry.filesRead, 1);
    assert.equal(transcriptRetry.filesRead, 1);
    assert.equal(rolloutRetry.parseErrors, 1);
    assert.equal(transcriptRetry.parseErrors, 1);

    parseBuffer.close();
    parseBuffer = new LocalEventBuffer(ledger);
    const rolloutRestart = await scanRollout();
    recordExplicitFullHistoryCoverage(parseBuffer.database, "codex", rolloutRestart);
    const transcriptRestart = await scanTranscript();
    const restartCoverage = recordExplicitFullHistoryCoverage(
      parseBuffer.database,
      "claude_code",
      transcriptRestart,
    );
    assert.equal(rolloutRestart.parseErrors, 1);
    assert.equal(transcriptRestart.parseErrors, 1);
    assert.equal(restartCoverage.promoted, false);
    assert.equal(restartCoverage.coverage.status, "complete");

    const repairedRollout = `${rolloutFile}.replacement`;
    fs.writeFileSync(
      repairedRollout,
      `${JSON.stringify({
        timestamp: "2026-07-19T12:00:00.000Z",
        type: "event_msg",
        payload: { type: "token_count", repaired: true },
      })}\n`,
    );
    fs.renameSync(repairedRollout, rolloutFile);
    const repairedTranscript = `${transcriptFile}.replacement`;
    fs.writeFileSync(
      repairedTranscript,
      `${JSON.stringify({
        type: "assistant",
        sessionId: "019e6000-0000-7000-8000-000000000012",
        timestamp: "2026-07-19T12:00:00.000Z",
        message: { id: "empty-usage-repaired", usage: {}, repaired: true },
      })}\n`,
    );
    fs.renameSync(repairedTranscript, transcriptFile);
    const rolloutRepair = await scanRollout();
    const rolloutRepairCoverage = recordExplicitFullHistoryCoverage(
      parseBuffer.database,
      "codex",
      rolloutRepair,
    );
    const transcriptRepair = await scanTranscript();
    const transcriptRepairCoverage = recordExplicitFullHistoryCoverage(
      parseBuffer.database,
      "claude_code",
      transcriptRepair,
    );
    assert.equal(rolloutRepair.filesRead, 1);
    assert.equal(transcriptRepair.filesRead, 1);
    assert.equal(rolloutRepair.parseErrors, 0);
    assert.equal(transcriptRepair.parseErrors, 0);
    assert.equal(rolloutRepairCoverage.promoted, true);
    assert.equal(transcriptRepairCoverage.promoted, true);
    assert.equal(historyCoverageStatus(parseBuffer.database).status, "complete");
    return {
      bothTailersRetriedUnchanged: true,
      bothTailersRetriedAfterRestart: true,
      priorCompletionPreservedAndFailureDisclosed: true,
      repairedFilesRereadAndPromoted: true,
    };
  } finally {
    parseBuffer.close();
  }
}

async function proveTranscriptChunkParity() {
  const sessionId = "019e7000-0000-7000-8000-000000000001";
  const usageLine = (input: number, output: number) =>
    JSON.stringify({
      type: "assistant",
      sessionId,
      timestamp: "2026-07-19T12:00:00.000Z",
      message: {
        id: "streamed-message",
        model: "claude-sonnet-4-20250514",
        usage: {
          input_tokens: input,
          cache_read_input_tokens: 0,
          cache_creation_input_tokens: 0,
          output_tokens: output,
        },
      },
    });
  const run = async (name: string, lines: string[]) => {
    const root = path.join(tempDir, `chunk-parity-${name}`);
    const project = path.join(root, "project");
    fs.mkdirSync(project, { recursive: true });
    fs.writeFileSync(path.join(project, `${sessionId}.jsonl`), `${lines.join("\n")}\n`);
    const parityBuffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
    try {
      const scan = await new TranscriptTailer(parityBuffer, project).scan({ scope: "full" });
      const totals = parityBuffer.database
        .prepare(
          `select count(*) as events, coalesce(sum(input_tokens), 0) as inputTokens,
             coalesce(sum(output_tokens), 0) as outputTokens
           from buffered_events where event_type = 'usage_transcript'`,
        )
        .get() as { events: number; inputTokens: number; outputTokens: number };
      const events = parityBuffer.database.prepare(
        `select id, input_tokens as inputTokens, output_tokens as outputTokens
         from buffered_events where event_type = 'usage_transcript' order by id`,
      ).all();
      return { scan, totals, events };
    } finally {
      parityBuffer.close();
    }
  };

  const oneShot = await run("one-shot", [usageLine(10, 1), usageLine(20, 2)]);
  const chunked = await run("chunked", [
    ...Array.from({ length: 63 }, (_, index) =>
      JSON.stringify({ type: "user", index }),
    ),
    usageLine(10, 1),
    usageLine(20, 2),
  ]);
  const crossRoot = path.join(tempDir, "chunk-parity-cross-cadence");
  const crossProject = path.join(crossRoot, "project");
  fs.mkdirSync(crossProject, { recursive: true });
  const crossFile = path.join(crossProject, `${sessionId}.jsonl`);
  fs.writeFileSync(crossFile, `${usageLine(10, 1)}\n`);
  const crossBuffer = new LocalEventBuffer(path.join(crossRoot, "ledger.sqlite"));
  let crossCadence: { totals: { events: number; inputTokens: number; outputTokens: number }; events: unknown[] };
  try {
    await new TranscriptTailer(crossBuffer, crossProject).scan({ scope: "full" });
    fs.appendFileSync(crossFile, `${usageLine(20, 2)}\n`);
    await new TranscriptTailer(crossBuffer, crossProject).scan({ scope: "full" });
    crossCadence = {
      totals: crossBuffer.database.prepare(
        `select count(*) as events, coalesce(sum(input_tokens), 0) as inputTokens,
           coalesce(sum(output_tokens), 0) as outputTokens
         from buffered_events where event_type = 'usage_transcript'`,
      ).get() as { events: number; inputTokens: number; outputTokens: number },
      events: crossBuffer.database.prepare(
        `select id, input_tokens as inputTokens, output_tokens as outputTokens
         from buffered_events where event_type = 'usage_transcript' order by id`,
      ).all(),
    };
  } finally {
    crossBuffer.close();
  }
  assert.deepEqual(oneShot.totals, { events: 2, inputTokens: 20, outputTokens: 2 });
  assert.deepEqual(chunked.totals, oneShot.totals);
  assert.deepEqual(chunked.events, oneShot.events);
  assert.deepEqual(crossCadence!.totals, oneShot.totals);
  assert.deepEqual(crossCadence!.events, oneShot.events);
  assert.ok(chunked.scan.slicesCommitted >= 2, "fixture must cross a reader slice");
  return {
    oneShot: oneShot.totals,
    chunked: chunked.totals,
    crossCadence: crossCadence!.totals,
    chunkedSlices: chunked.scan.slicesCommitted,
  };
}

async function proveDeferredRepoContextOccurrences() {
  const root = path.join(tempDir, "deferred-repo-context-occurrences");
  fs.mkdirSync(root, { recursive: true });
  const occurrenceBuffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
  const cwdA = path.join(root, "repo-a");
  const cwdB = path.join(root, "repo-b");
  fs.mkdirSync(cwdA, { recursive: true });
  fs.mkdirSync(cwdB, { recursive: true });
  try {
    occurrenceBuffer.beginChildRepoContextRun();
    const rolloutRoot = path.join(root, "rollouts");
    const rolloutDay = path.join(rolloutRoot, "2026", "07", "20");
    fs.mkdirSync(rolloutDay, { recursive: true });
    const sessionId = "019e8000-0000-7000-8000-000000000001";
    const rolloutFile = path.join(
      rolloutDay,
      `rollout-proof-${sessionId}.jsonl`,
    );
    const ignored = Array.from({ length: 63 }, (_, index) => rolloutLine(
      `2026-07-20T12:00:${String(index % 60).padStart(2, "0")}.000Z`,
      "response_item",
      { index },
    ));
    fs.writeFileSync(
      rolloutFile,
      [
        rolloutLine("2026-07-20T11:59:59.000Z", "session_meta", {
          id: sessionId,
          cwd: cwdA,
        }),
        ...ignored,
        tokenCountLine("2026-07-20T12:01:00.000Z", 10, 0, 1),
        tokenCountLine("2026-07-20T12:01:01.000Z", 20, 0, 2),
        rolloutLine("2026-07-20T12:01:02.000Z", "turn_context", {
          model: "gpt-5.5",
          cwd: cwdB,
        }),
        tokenCountLine("2026-07-20T12:01:03.000Z", 30, 0, 3),
        rolloutLine("2026-07-20T12:01:04.000Z", "turn_context", {
          model: "gpt-5.5",
          cwd: cwdA,
        }),
        tokenCountLine("2026-07-20T12:01:05.000Z", 40, 0, 4),
      ].join("\n") + "\n",
    );
    const rolloutScan = await new RolloutTailer(
      occurrenceBuffer,
      rolloutRoot,
      () => [],
    ).scan({ scope: "full" });
    const rolloutRows = occurrenceBuffer.database.prepare(
      `select e.input_tokens as inputTokens, l.context_id as contextId
       from buffered_events e left join repo_context_event_links l on l.event_id = e.id
       where e.event_type = 'usage_rollout' and e.session_id = ?
       order by e.observed_at`,
    ).all(sessionId) as Array<{ inputTokens: number; contextId: string | null }>;
    assert.equal(rolloutScan.slicesCommitted >= 2, true);
    assert.equal(rolloutRows.length, 4);
    assert.ok(rolloutRows.every((row) => row.inputTokens === 10 && row.contextId));
    assert.equal(rolloutRows[0]!.contextId, rolloutRows[1]!.contextId);
    assert.notEqual(rolloutRows[1]!.contextId, rolloutRows[2]!.contextId);
    assert.notEqual(
      rolloutRows[0]!.contextId,
      rolloutRows[3]!.contextId,
      "the same cwd at a later occurrence must not reuse the earlier occurrence id",
    );
    const rolloutIdsBeforeReplay = rolloutRows.map((row) => row.contextId);
    occurrenceBuffer.database.prepare(
      `delete from rollout_scan_state where file = ?`,
    ).run(jsonlScanStateKey(rolloutFile));
    await new RolloutTailer(occurrenceBuffer, rolloutRoot, () => []).scan({ scope: "full" });
    const rolloutIdsAfterReplay = (occurrenceBuffer.database.prepare(
      `select l.context_id as contextId
       from buffered_events e left join repo_context_event_links l on l.event_id = e.id
       where e.event_type = 'usage_rollout' and e.session_id = ? order by e.observed_at`,
    ).all(sessionId) as Array<{ contextId: string | null }>).map((row) => row.contextId);
    assert.deepEqual(rolloutIdsAfterReplay, rolloutIdsBeforeReplay);

    const transcriptRoot = path.join(root, "transcripts");
    const transcriptProject = path.join(transcriptRoot, "project");
    fs.mkdirSync(transcriptProject, { recursive: true });
    const transcriptSession = "019e8000-0000-7000-8000-000000000002";
    const transcriptFile = path.join(transcriptProject, `${transcriptSession}.jsonl`);
    const transcriptLine = (
      messageId: string,
      input: number,
      cwd: string,
      timestamp: string,
    ) => JSON.stringify({
      type: "assistant",
      sessionId: transcriptSession,
      cwd,
      timestamp,
      message: {
        id: messageId,
        model: "claude-sonnet-4-20250514",
        usage: { input_tokens: input, output_tokens: input / 10 },
      },
    });
    fs.writeFileSync(
      transcriptFile,
      [
        transcriptLine("stable-message", 10, cwdA, "2026-07-20T13:00:00.000Z"),
        transcriptLine("stable-message", 20, cwdA, "2026-07-20T13:00:01.000Z"),
        transcriptLine("conflict-message", 10, cwdA, "2026-07-20T13:00:02.000Z"),
        transcriptLine("conflict-message", 20, cwdB, "2026-07-20T13:00:03.000Z"),
      ].join("\n") + "\n",
    );
    await new TranscriptTailer(occurrenceBuffer, transcriptRoot).scan({ scope: "full" });
    let stableRows = occurrenceBuffer.database.prepare(
      `select l.context_id as contextId
       from buffered_events e left join repo_context_event_links l on l.event_id = e.id
       where e.event_type = 'usage_transcript' and e.session_id = ?
         and e.observed_at in (?, ?) order by e.observed_at`,
    ).all(
      transcriptSession,
      "2026-07-20T13:00:00.000Z",
      "2026-07-20T13:00:01.000Z",
    ) as Array<{ contextId: string | null }>;
    const conflictingRows = occurrenceBuffer.database.prepare(
      `select l.context_id as contextId
       from buffered_events e left join repo_context_event_links l on l.event_id = e.id
       where e.event_type = 'usage_transcript' and e.session_id = ?
         and e.observed_at in (?, ?) order by e.observed_at`,
    ).all(
      transcriptSession,
      "2026-07-20T13:00:02.000Z",
      "2026-07-20T13:00:03.000Z",
    ) as Array<{ contextId: string | null }>;
    assert.equal(stableRows.length, 2);
    assert.ok(stableRows[0]!.contextId);
    assert.equal(stableRows[0]!.contextId, stableRows[1]!.contextId);
    assert.equal(conflictingRows.length, 2);
    assert.ok(conflictingRows.every((row) => row.contextId));
    assert.equal(conflictingRows[0]!.contextId, conflictingRows[1]!.contextId);

    // Equal usage with a different cwd emits no event, but must still make
    // the occurrence conflict terminal. A later revision cannot last-win.
    fs.appendFileSync(
      transcriptFile,
      transcriptLine("stable-message", 20, cwdB, "2026-07-20T13:00:04.000Z") + "\n",
    );
    const zeroDeltaConflict = await new TranscriptTailer(
      occurrenceBuffer,
      transcriptRoot,
    ).scan({ scope: "full" });
    assert.equal(zeroDeltaConflict.eventsAppended, 0);
    fs.appendFileSync(
      transcriptFile,
      transcriptLine("stable-message", 30, cwdA, "2026-07-20T13:00:05.000Z") + "\n",
    );
    await new TranscriptTailer(occurrenceBuffer, transcriptRoot).scan({ scope: "full" });
    stableRows = occurrenceBuffer.database.prepare(
      `select l.context_id as contextId
       from buffered_events e left join repo_context_event_links l on l.event_id = e.id
       where e.event_type = 'usage_transcript' and e.session_id = ?
         and e.observed_at in (?, ?, ?) order by e.observed_at`,
    ).all(
      transcriptSession,
      "2026-07-20T13:00:00.000Z",
      "2026-07-20T13:00:01.000Z",
      "2026-07-20T13:00:05.000Z",
    ) as Array<{ contextId: string | null }>;
    assert.equal(stableRows.length, 3);
    assert.equal(stableRows[0]!.contextId, stableRows[1]!.contextId);
    assert.equal(stableRows[2]!.contextId, stableRows[0]!.contextId);
    const conflicts = occurrenceBuffer.database.prepare(
      `select count(*) as count from transcript_usage_revision_state
       where session_id = ? and context_conflict = 1`,
    ).get(transcriptSession) as { count: number };
    assert.equal(conflicts.count, 2);

    const liveSession = "019e8000-0000-7000-8000-000000000003";
    occurrenceBuffer.database.prepare(
      `insert into session_usage_authority (source, session_id, authority, claimed_at)
       values ('codex', ?, 'live', ?)`,
    ).run(liveSession, "2026-07-20T13:30:00.000Z");
    fs.writeFileSync(
      path.join(rolloutDay, `rollout-proof-${liveSession}.jsonl`),
      [
        rolloutLine("2026-07-20T13:30:00.000Z", "session_meta", {
          id: liveSession,
          cwd: path.join(root, "live-session-unused-context"),
        }),
        tokenCountLine("2026-07-20T13:30:01.000Z", 99, 0, 9),
      ].join("\n") + "\n",
    );
    const liveCovered = await new RolloutTailer(
      occurrenceBuffer,
      rolloutRoot,
      () => [],
    ).scan({ scope: "full" });
    assert.equal(liveCovered.sessionsSkippedOtlpCovered, 1);
    assert.equal(occurrenceBuffer.repoContextInflightCount(), 3);

    const childRequests = occurrenceBuffer.finishChildRepoContextRun();
    assert.equal(
      childRequests.length,
      3,
      "the cross-slice transcript conflict must cancel its child-owned request",
    );
    assert.equal(occurrenceBuffer.repoContextInflightCount(), 3);
    const usageRowsBeforeResolution = (occurrenceBuffer.database.prepare(
      `select count(*) as count from buffered_events
       where event_type in ('usage_rollout', 'usage_transcript')`,
    ).get() as { count: number }).count;
    const unknownApply = occurrenceBuffer.applyRepoContextResults(
      resolveRepoContextRequests(childRequests),
    );
    assert.equal(unknownApply.unknownResults, 3);
    assert.equal(occurrenceBuffer.repoContextInflightCount(), 0);
    assert.equal(
      (occurrenceBuffer.database.prepare(
        `select count(*) as count from buffered_events
         where event_type in ('usage_rollout', 'usage_transcript')`,
      ).get() as { count: number }).count,
      usageRowsBeforeResolution,
    );
    assert.ok((occurrenceBuffer.database.prepare(
      `select count(*) as count from rollout_scan_state`,
    ).get() as { count: number }).count > 0);

    const durableState = JSON.stringify(occurrenceBuffer.database.prepare(
      `select parser_state_json as parserState from rollout_scan_state`,
    ).all());
    assert.equal(durableState.includes(cwdA), false);
    assert.equal(durableState.includes(cwdB), false);
    return {
      rolloutCrossSliceContextIdPersisted: true,
      rolloutPendingTokensBoundAtParseTime: true,
      rolloutRepeatedCwdHasDistinctOccurrence: true,
      rolloutReplayIdsStable: true,
      transcriptSameMessageSameCwdStable: true,
      transcriptDifferentCwdConflictUnknown: true,
      transcriptZeroDeltaConflictDetected: true,
      transcriptConflictCancelsChildInflight: true,
      liveCoveredSessionConsumesNoChildCapacity: true,
      resolverFailurePreservesTokensAndCursors: true,
      rawCwdAbsentFromParserState: true,
    };
  } finally {
    occurrenceBuffer.close();
  }
}

async function proveResolvedTranscriptConflictIsTerminal() {
  const root = path.join(tempDir, "resolved-transcript-context-conflict");
  const transcriptRoot = path.join(root, "transcripts");
  const transcriptProject = path.join(transcriptRoot, "project");
  const cwdA = path.join(root, "repo-a");
  const cwdB = path.join(root, "repo-b");
  for (const [cwd, remote, head] of [
    [cwdA, "https://example.invalid/team/resolved-a.git", "a".repeat(40)],
    [cwdB, "https://example.invalid/team/resolved-b.git", "b".repeat(40)],
  ] as const) {
    fs.mkdirSync(path.join(cwd, ".git", "refs", "heads"), { recursive: true });
    fs.writeFileSync(path.join(cwd, ".git", "HEAD"), "ref: refs/heads/main\n");
    fs.writeFileSync(path.join(cwd, ".git", "refs", "heads", "main"), `${head}\n`);
    fs.writeFileSync(
      path.join(cwd, ".git", "config"),
      `[remote "origin"]\n\turl = ${remote}\n`,
    );
  }
  fs.mkdirSync(transcriptProject, { recursive: true });
  const sessionId = "019e8000-0000-7000-8000-000000000099";
  const messageId = "resolved-then-conflicting-message";
  const transcriptFile = path.join(transcriptProject, `${sessionId}.jsonl`);
  const usageLine = (input: number, cwd: string, timestamp: string) => JSON.stringify({
    type: "assistant",
    sessionId,
    cwd,
    timestamp,
    message: {
      id: messageId,
      model: "claude-sonnet-4-20250514",
      usage: { input_tokens: input, output_tokens: input / 10 },
    },
  });
  fs.writeFileSync(
    transcriptFile,
    [
      usageLine(10, cwdA, "2026-07-20T15:00:00.000Z"),
      usageLine(20, cwdA, "2026-07-20T15:00:01.000Z"),
    ].join("\n") + "\n",
  );
  const conflictBuffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
    delivery: { enabled: true },
  });
  try {
    conflictBuffer.beginChildRepoContextRun();
    await new TranscriptTailer(conflictBuffer, transcriptRoot).scan({ scope: "full" });
    const requests = conflictBuffer.finishChildRepoContextRun();
    assert.equal(requests.length, 1);
    const apply = conflictBuffer.applyRepoContextResults(resolveRepoContextRequests(requests));
    assert.equal(apply.rowsFilled, 2);
    const linkedBefore = conflictBuffer.database.prepare(
      `select l.context_id as contextId, e.repo_hash as repoHash,
         e.branch_hash as branchHash, e.head_sha as headSha
       from buffered_events e left join repo_context_event_links l on l.event_id = e.id
       where e.session_id = ? order by e.observed_at`,
    ).all(sessionId) as Array<{
      contextId: string | null;
      repoHash: string | null;
      branchHash: string | null;
      headSha: string | null;
    }>;
    assert.equal(linkedBefore.length, 2);
    assert.ok(linkedBefore.every((row) => row.contextId && row.repoHash && row.branchHash && row.headSha));

    const lease = conflictBuffer.delivery.lease({ leaseId: "resolved-context-conflict-proof" });
    assert.equal(lease.items.length, 2);
    const sealedBefore = conflictBuffer.database.prepare(
      `select delivery_id as deliveryId, sealed_envelope_json as sealed, attempt_count as attempts
       from upload_outbox order by delivery_id`,
    ).all() as Array<{ deliveryId: string; sealed: string; attempts: number }>;
    assert.ok(sealedBefore.every((row) => row.sealed && row.attempts === 1));

    fs.appendFileSync(
      transcriptFile,
      usageLine(20, cwdB, "2026-07-20T15:00:02.000Z") + "\n",
    );
    const zeroDeltaConflict = await new TranscriptTailer(
      conflictBuffer,
      transcriptRoot,
    ).scan({ scope: "full" });
    assert.equal(zeroDeltaConflict.eventsAppended, 0);
    assert.deepEqual(conflictBuffer.drainRepoContextSuppressions(), {
      contextsVisited: 1,
      rowsVisited: 2,
      rowsCleared: 2,
    });
    const sealedAfter = conflictBuffer.database.prepare(
      `select delivery_id as deliveryId, sealed_envelope_json as sealed, attempt_count as attempts
       from upload_outbox order by delivery_id`,
    ).all() as Array<{ deliveryId: string; sealed: string; attempts: number }>;
    assert.deepEqual(sealedAfter, sealedBefore, "attempted outbound bytes must remain immutable");

    fs.appendFileSync(
      transcriptFile,
      usageLine(30, cwdA, "2026-07-20T15:00:03.000Z") + "\n",
    );
    await new TranscriptTailer(conflictBuffer, transcriptRoot).scan({ scope: "full" });
    const terminalRows = conflictBuffer.database.prepare(
      `select e.input_tokens as inputTokens, e.output_tokens as outputTokens,
         l.context_id as contextId, e.repo_hash as repoHash,
         e.branch_hash as branchHash, e.head_sha as headSha
       from buffered_events e left join repo_context_event_links l on l.event_id = e.id
       where e.session_id = ? order by e.observed_at`,
    ).all(sessionId) as Array<{
      inputTokens: number;
      outputTokens: number;
      contextId: string | null;
      repoHash: string | null;
      branchHash: string | null;
      headSha: string | null;
    }>;
    assert.equal(terminalRows.length, 3);
    assert.equal(terminalRows.reduce((sum, row) => sum + row.inputTokens, 0), 30);
    assert.equal(terminalRows.reduce((sum, row) => sum + row.outputTokens, 0), 3);
    assert.ok(terminalRows.every((row) =>
      row.repoHash === null && row.branchHash === null && row.headSha === null
    ));
    assert.equal(terminalRows[0]!.contextId, terminalRows[1]!.contextId);
    assert.equal(terminalRows[2]!.contextId, terminalRows[0]!.contextId);
    assert.equal((conflictBuffer.database.prepare(
      `select count(*) as count from repo_context_suppressions
       where reason = 'transcript_context_conflict'`,
    ).get() as { count: number }).count, 1);
    assert.equal((conflictBuffer.database.prepare(
      `select context_conflict as conflict from transcript_usage_revision_state
       where session_id = ?`,
    ).get(sessionId) as { conflict: number }).conflict, 1);
    assert.deepEqual(conflictBuffer.drainRepoContextFills(), { rowsVisited: 0, rowsFilled: 0 });
    const legacyDonor = aiInteractionEventSchema.parse({
      id: deterministicEventId(["resolved-conflict-legacy-donor", sessionId]),
      source: "codex",
      dataMode: "metadata",
      eventType: "assistant_response",
      observedAt: "2026-07-20T15:00:04.000Z",
      sessionId,
      inputTokens: 1,
      outputTokens: 1,
      actionClass: "other",
      metadata: {
        git: {
          remoteUrlHash: remoteLinkageHash("https://example.invalid/conflict-donor.git"),
        },
      },
    });
    conflictBuffer.append(legacyDonor);
    runRepoEnrichmentMaintenance(conflictBuffer.database, {
      legacyBackfillLimit: 32,
      sessionLimit: 8,
      eventLimit: 32,
    });
    const exactRowsAfterLegacyStitch = conflictBuffer.database.prepare(
      `select e.repo_hash as repoHash
       from buffered_events e join repo_context_event_links l on l.event_id = e.id
       where e.session_id = ? and e.event_type = 'usage_transcript'`,
    ).all(sessionId) as Array<{ repoHash: string | null }>;
    assert.equal(exactRowsAfterLegacyStitch.length, 3);
    assert.ok(exactRowsAfterLegacyStitch.every((row) => row.repoHash === null));
    return {
      priorResolvedRowsReturnedToUnknown: true,
      laterRevisionsRemainUnknown: true,
      tokenTruthPreserved: true,
      attemptedOutboundBytesImmutable: true,
      suppressionTombstoneDurable: true,
      terminalContextExcludedFromLegacyStitching: true,
    };
  } finally {
    conflictBuffer.close();
  }
}

function proveNanosecondGenerationIdentity() {
  const identityBuffer = new LocalEventBuffer(
    path.join(tempDir, "nanosecond-generation-ledger.sqlite"),
  );
  try {
    const observedAt = "2026-07-15T12:00:00.000Z";
    const codexPath = path.join(tempDir, "nanosecond-codex.jsonl");
    const claudePath = path.join(tempDir, "nanosecond-claude.jsonl");
    const firstBirthtimeNs = 1_234_000_001n;
    const collidingMillisecondBirthtimeNs = 1_234_999_999n;
    for (const [source, file, inode] of [
      ["codex", codexPath, 91n],
      ["claude_code", claudePath, 92n],
    ] as const) {
      const began = beginAutomaticCaptureBaseline(identityBuffer.database, source, {
        startedAt: observedAt,
        filesDiscovered: 0,
      });
      assert.ok(began.latestRun);
      completeAutomaticCaptureBaseline(identityBuffer.database, source, {
        runId: began.latestRun.runId,
        completedAt: observedAt,
        observations: [{
          path: file,
          device: 7n,
          inode,
          size: 10n,
          birthtimeMs: 1_234,
          birthtimeNs: firstBirthtimeNs,
        }],
      });
    }
    const sameGenerationGrowth = classifyCaptureBaselineFile(
      identityBuffer.database,
      "codex",
      {
        path: codexPath,
        device: 7n,
        inode: 91n,
        size: 11n,
        birthtimeMs: 1_234,
        birthtimeNs: firstBirthtimeNs,
      },
      { mode: "automatic", observedAt },
    );
    const sameMillisecondReplacement = classifyCaptureBaselineFile(
      identityBuffer.database,
      "codex",
      {
        path: codexPath,
        device: 7n,
        inode: 91n,
        size: 10n,
        birthtimeMs: 1_234,
        birthtimeNs: collidingMillisecondBirthtimeNs,
      },
      { mode: "automatic", observedAt },
    );
    assert.equal(sameGenerationGrowth.decision, "exclude");
    assert.equal(sameGenerationGrowth.observedGrowth, true);
    assert.equal(sameMillisecondReplacement.decision, "capture");
    return {
      sameGenerationGrowthExcluded: true,
      sameMillisecondDistinctNanosecondsCapturedFromByteZero: true,
    };
  } finally {
    identityBuffer.close();
  }
}

async function main() {
  try {
    const rollout = await proveRolloutTailing();
    const transcript = await proveTranscriptTailing();
    const parseFailureDurability = await proveParseFailuresRemainUnresolved();
    const transcriptChunkParity = await proveTranscriptChunkParity();
    const deferredRepoContextOccurrences = await proveDeferredRepoContextOccurrences();
    const resolvedTranscriptConflict = await proveResolvedTranscriptConflictIsTerminal();
    const nanosecondGenerationIdentity = proveNanosecondGenerationIdentity();

    const persistedEvents = JSON.stringify(
      buffer.database.prepare(`select payload_json from buffered_events`).all(),
    );
    const persistedState = JSON.stringify(
      buffer.database
        .prepare(`select parser_state_json as state from rollout_scan_state`)
        .all(),
    );
    assert.ok(!persistedEvents.includes(RAW_SENTINEL));
    assert.ok(!persistedState.includes(RAW_SENTINEL));
    assert.ok(!persistedState.includes(tempDir), "raw cwd paths must not persist in parser checkpoints");

    const physicalLegacySentinels = Array.from({ length: 300 }, (_, index) => path.join(
      tempDir,
      `LEGACY_CURSOR_PHYSICAL_SENTINEL-${String(index).padStart(3, "0")}.jsonl`,
    ));
    const insertLegacyCursor = buffer.database.prepare(
      `insert into rollout_scan_state (file, size, scanned_at) values (?, 0, ?)`,
    );
    buffer.database.transaction(() => {
      for (const sentinel of physicalLegacySentinels) {
        insertLegacyCursor.run(sentinel, new Date().toISOString());
      }
    })();
    const migrationTailer = new RolloutTailer(
      buffer,
      path.join(tempDir, "no-such-rollout-root"),
      () => [],
    );
    migrationTailer.close();
    assert.equal(
      (buffer.database.prepare(
        `select count(*) as count from rollout_scan_state
         where length(file) != 64 or file glob '*[^0-9a-f]*'`,
      ).get() as { count: number }).count,
      0,
    );
    buffer.close();
    bufferClosed = true;
    const ledgerArtifacts = ["proof.sqlite", "proof.sqlite-wal", "proof.sqlite-shm"]
      .map((name) => path.join(tempDir, name))
      .filter((file) => fs.existsSync(file));
    assert.ok(
      ledgerArtifacts.every(
        (file) => !fs.readFileSync(file).includes(Buffer.from("LEGACY_CURSOR_PHYSICAL_SENTINEL-")),
      ),
      "secure-delete cursor migration must scrub the legacy path from DB/WAL/SHM bytes",
    );

    process.stdout.write(
      `${JSON.stringify(
        {
          proof: "plimsoll-incremental-capture",
          passed: true,
          rollout,
          transcript,
          transcriptChunkParity,
          deferredRepoContextOccurrences,
          resolvedTranscriptConflict,
          nanosecondGenerationIdentity,
          parseFailureDurability,
          privacy: {
            rawContentPersisted: false,
            rawPathPersistedInCursor: false,
            legacyPathAbsentFromPhysicalLedger: true,
            legacyPathRowsMigratedInOnePreflight: physicalLegacySentinels.length,
          },
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    if (!bufferClosed) buffer.close();
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
