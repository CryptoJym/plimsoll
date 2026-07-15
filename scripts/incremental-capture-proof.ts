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
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-incremental-proof-"));
const buffer = new LocalEventBuffer(path.join(tempDir, "proof.sqlite"));

const ROLLOUT_SESSION = "019e1111-2222-7333-8444-555555555555";
const ROTATED_SESSION = "019e2222-3333-7444-8555-666666666666";
const LEGACY_SESSION = "019e3333-4444-7555-8666-777777777777";
const CONTINUITY_SESSION = "019e4444-5555-7666-8777-888888888888";
const CONTINUITY_REPLACEMENT_SESSION = "019e5555-6666-7777-8888-999999999999";
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

  const first = await new RolloutTailer(buffer, path.join(tempDir, "rollouts"), () => []).scan();
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
    .get(file) as { committedOffset: number; headBytes: number; continuityBytes: number };
  assert.equal(firstCursor.committedOffset, Buffer.byteLength(completePrefix));

  fs.appendFileSync(file, firstToken.slice(split) + "\n");
  const firstCompletionSize = fs.statSync(file).size;
  const second = await new RolloutTailer(buffer, path.join(tempDir, "rollouts"), () => []).scan();
  const suffixFromCommit = firstCompletionSize - firstCursor.committedOffset;
  assert.equal(second.eventsAppended, 1);
  assert.equal(second.tokensAppended.input, 100);
  assert.ok(
    second.bytesRead <= suffixFromCommit + firstCursor.headBytes + firstCursor.continuityBytes,
    `read ${second.bytesRead} bytes for ${suffixFromCommit}-byte suffix plus bounded probes`,
  );

  const unchanged = await new RolloutTailer(buffer, path.join(tempDir, "rollouts"), () => []).scan();
  assert.equal(unchanged.filesRead, 0);
  assert.equal(unchanged.bytesRead, 0);
  assert.equal(unchanged.eventsAppended, 0);

  const secondToken = tokenCountLine("2026-07-15T10:00:04.000Z", 250, 50, 25) + "\n";
  fs.appendFileSync(file, secondToken);
  const appended = await new RolloutTailer(buffer, path.join(tempDir, "rollouts"), () => []).scan();
  assert.equal(appended.eventsAppended, 1);
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
    .run("{}", file);
  const emptyStateRebuild = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan();
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
    .run("null", file);
  const thirdToken = tokenCountLine("2026-07-15T10:00:05.000Z", 400, 80, 40) + "\n";
  fs.appendFileSync(file, thirdToken);
  const nullStateRebuild = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan();
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
      file,
    );
  const wrongTypeRebuild = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan();
  assert.equal(wrongTypeRebuild.checkpointRebuilds, 1);

  buffer.database
    .prepare(`update rollout_scan_state set checkpoint_version = ? where file = ?`)
    .run(999, file);
  const wrongVersionRebuild = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan();
  assert.equal(wrongVersionRebuild.checkpointRebuilds, 1);

  buffer.database
    .prepare(`update rollout_scan_state set parser_state_json = ? where file = ?`)
    .run("{malformed-json", file);
  const malformedStateRebuild = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan();
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
  ).scan();
  assert.equal(afterTruncation.filesReset, 1);
  assert.equal(afterTruncation.eventsAppended, 1);
  assert.equal(
    (buffer.database
      .prepare(`select count(*) as n from buffered_events where session_id = ?`)
      .get(ROTATED_SESSION) as { n: number }).n,
    1,
  );

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
  ).scan();
  assert.equal(legacyUnchanged.filesRead, 0, "unchanged legacy rows must not trigger a corpus rebuild");
  fs.appendFileSync(legacyFile, tokenCountLine("2026-07-15T12:00:03.000Z", 25, 0, 2) + "\n");
  const legacyGrowth = await new RolloutTailer(
    buffer,
    path.join(tempDir, "rollouts"),
    () => [],
  ).scan();
  assert.equal(legacyGrowth.legacyRebuilds, 1);
  assert.equal(
    (buffer.database
      .prepare(`select count(*) as n from buffered_events where session_id = ?`)
      .get(LEGACY_SESSION) as { n: number }).n,
    2,
  );

  // Same-inode truncate-and-regrow can preserve the first 512 bytes and grow
  // beyond the prior size. The checkpoint-boundary continuity probe must see
  // that the already-committed region changed and restart from byte zero.
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
  ).scan();
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
  ).scan();
  assert.equal(continuityReset.filesReset, 1);
  assert.equal(
    (buffer.database
      .prepare(`select count(*) as n from buffered_events where session_id = ?`)
      .get(CONTINUITY_REPLACEMENT_SESSION) as { n: number }).n,
    1,
  );

  // Forced replay is deterministic: deleting only the scan cursor cannot
  // create extra event rows.
  const beforeReplay = (buffer.database
    .prepare(`select count(*) as n from buffered_events where event_type = 'usage_rollout'`)
    .get() as { n: number }).n;
  buffer.database.prepare(`delete from rollout_scan_state where file = ?`).run(legacyFile);
  await new RolloutTailer(buffer, path.join(tempDir, "rollouts"), () => []).scan();
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
  };
}

async function proveTranscriptTailing() {
  const projectsDir = path.join(tempDir, "transcripts");
  const projectDir = path.join(projectsDir, "proof-project");
  fs.mkdirSync(projectDir, { recursive: true });
  const file = path.join(projectDir, `${TRANSCRIPT_SESSION}.jsonl`);
  fs.writeFileSync(file, assistantLine("msg-1", 20, 2) + "\n");

  const first = await new TranscriptTailer(buffer, projectsDir).scan();
  assert.equal(first.eventsAppended, 1);
  const secondLine = assistantLine("msg-2", 30, 3);
  const split = Math.floor(secondLine.length / 2);
  fs.appendFileSync(file, secondLine.slice(0, split));
  const partial = await new TranscriptTailer(buffer, projectsDir).scan();
  assert.equal(partial.eventsAppended, 0);
  assert.equal(partial.parseErrors, 0);
  assert.equal(partial.bytesDeferred, Buffer.byteLength(secondLine.slice(0, split)));

  const unchanged = await new TranscriptTailer(buffer, projectsDir).scan();
  assert.equal(unchanged.filesRead, 0);
  assert.equal(unchanged.bytesRead, 0);

  fs.appendFileSync(file, secondLine.slice(split) + "\n");
  const completedAfterRestart = await new TranscriptTailer(buffer, projectsDir).scan();
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
    .run("{}", file);
  const emptyStateRebuild = await new TranscriptTailer(buffer, projectsDir).scan();
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
    .run("null", 999, file);
  fs.appendFileSync(file, assistantLine("msg-3", 10, 1) + "\n");
  const invalidVersionRebuild = await new TranscriptTailer(buffer, projectsDir).scan();
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

async function main() {
  try {
    const rollout = await proveRolloutTailing();
    const transcript = await proveTranscriptTailing();

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

    process.stdout.write(
      `${JSON.stringify(
        {
          proof: "plimsoll-incremental-capture",
          passed: true,
          rollout,
          transcript,
          privacy: { rawContentPersisted: false, rawPathPersistedInCursor: false },
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    buffer.close();
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
