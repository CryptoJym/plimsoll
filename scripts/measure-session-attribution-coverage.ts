#!/usr/bin/env node
/**
 * eco-6hoxj.163.21: how much recent repo-less token usage session inheritance
 * attributes, with the 0.7.36 session scan and with the capture-time context
 * index, measured on a disposable COPY of a real ledger.
 *
 *   pnpm measure:session-attribution -- --ledger /abs/ledger-copy.sqlite --confirm-copy \
 *     [--rowids /abs/sample.json] [--sample 400] [--batch-rows 5000]
 *
 * The copy is opened read-write: the open installs the index exactly as a
 * collector upgrade would, then the script runs the bounded backfill to
 * completion (timed, in the batches maintenance uses) and evaluates both
 * paths on the same sample. The sample is the population of the lead's
 * s0-attr-measure.py: token rows with a session and no repo_hash among the
 * last 3M rowids. `--rowids` takes that script's exact sample (see the lane's
 * s0-attr-sample.py); without it the script draws its own seeded sample.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  SESSION_INHERIT_MAX_CONTEXT_ROWS,
  SESSION_INHERIT_WINDOW_MS,
  SessionAttributionBatch,
} from "../packages/collector-cli/src/session-attribution";
import {
  backfillSessionContextIndex,
  sessionContextIndexState,
  sessionContextIndexStatus,
} from "../packages/collector-cli/src/session-context-index";
import type { AiInteractionEvent } from "../packages/shared/src/index";

function fail(message: string): never {
  console.error(message);
  process.exit(1);
}

function option(name: string) {
  const index = process.argv.indexOf(name);
  return index < 0 ? undefined : process.argv[index + 1];
}

const ledgerArgument = option("--ledger");
if (!ledgerArgument || !process.argv.includes("--confirm-copy")) {
  fail(
    "Usage: pnpm measure:session-attribution -- --ledger /absolute/ledger-copy.sqlite --confirm-copy " +
    "[--rowids /absolute/sample.json] [--sample 400] [--batch-rows 5000]\n" +
    "The command installs and backfills the context index in the supplied ledger. Never pass a live ledger.",
  );
}
if (!path.isAbsolute(ledgerArgument)) fail("--ledger must be an absolute path");
const ledgerInputStat = fs.lstatSync(ledgerArgument);
if (!ledgerInputStat.isFile() || ledgerInputStat.isSymbolicLink()) fail("--ledger must be a regular, non-symlink copy");
const ledgerPath = fs.realpathSync(ledgerArgument);
const ledgerStat = fs.statSync(ledgerPath);
for (const liveLedger of [
  path.join(os.homedir(), "Library", "Application Support", "Plimsoll", "work-ledger.sqlite"),
  ...(path.isAbsolute(process.env.PLIMSOLL_HOME ?? "") ? [path.join(process.env.PLIMSOLL_HOME!, "work-ledger.sqlite")] : []),
]) {
  if (path.resolve(ledgerPath) === path.resolve(liveLedger)) fail("refusing the live Plimsoll ledger; pass a disposable copy");
  if (fs.existsSync(liveLedger)) {
    const liveStat = fs.statSync(liveLedger);
    if (liveStat.dev === ledgerStat.dev && liveStat.ino === ledgerStat.ino) fail("refusing a hard link to the live ledger");
  }
}
const sampleSize = Math.max(1, Math.trunc(Number(option("--sample") ?? 400)));
const batchRows = Math.max(1, Math.trunc(Number(option("--batch-rows") ?? 5_000)));

function mulberry32(seed: number) {
  let state = seed;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let value = Math.imul(state ^ (state >>> 15), 1 | state);
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  };
}

type SampledRow = {
  rowid: number;
  sessionId: string;
  observedAt: string;
  payloadJson: string;
  repoHash: string | null;
  branchHash: string | null;
  tokens: number;
};

const opened = performance.now();
let installStepMs: number | null = null;
const buffer = new LocalEventBuffer(ledgerPath, {
  databaseBusyTimeoutMs: 5_000,
  onOpenStep: (step) => {
    if (step.step === "ledger.session_context_index") installStepMs = step.durationMs;
  },
});
const openMs = performance.now() - opened;
const db = buffer.database;
const installedState = sessionContextIndexState(db);

// The one-time backfill, in the transactions maintenance runs.
const backfillStarted = performance.now();
let batches = 0;
let slowestBatchMs = 0;
while (!process.argv.includes("--no-backfill") && sessionContextIndexState(db) === "backfilling") {
  const started = performance.now();
  backfillSessionContextIndex(db, batchRows);
  slowestBatchMs = Math.max(slowestBatchMs, performance.now() - started);
  batches += 1;
}
const backfillMs = performance.now() - backfillStarted;
const indexStatus = sessionContextIndexStatus(db);

const readRow = db.prepare(
  `select rowid, session_id as sessionId, observed_at as observedAt, payload_json as payloadJson,
     repo_hash as repoHash, branch_hash as branchHash,
     coalesce(input_tokens, 0) + coalesce(output_tokens, 0) as tokens
   from buffered_events where rowid = ?`,
);
const sample: SampledRow[] = [];
const rowidsFile = option("--rowids");
if (rowidsFile) {
  const parsed = JSON.parse(fs.readFileSync(rowidsFile, "utf8")) as { rows: Array<{ rowid: number }> };
  for (const { rowid } of parsed.rows) {
    const row = readRow.get(rowid) as SampledRow | undefined;
    if (row) sample.push(row);
  }
} else {
  const maxRowid = (db.prepare(`select max(rowid) as n from buffered_events`).get() as { n: number }).n;
  const low = Math.max(1, maxRowid - 3_000_000);
  const random = mulberry32(7);
  const seen = new Set<number>();
  for (let attempts = 0; attempts < 40_000 && sample.length < sampleSize; attempts += 1) {
    const rowid = low + Math.floor(random() * (maxRowid - low + 1));
    if (seen.has(rowid)) continue;
    seen.add(rowid);
    const row = readRow.get(rowid) as SampledRow | undefined;
    if (row?.sessionId && !row.repoHash && row.tokens > 0) sample.push(row);
  }
}

const countContexts = db.prepare(
  `select count(*) as n from (
     select 1 from session_repo_contexts
     where session_id = ? and observed_at >= ? and observed_at <= ? limit ?
   )`,
);
type Tally = Record<string, { rows: number; tokens: number }>;
const add = (tally: Tally, key: string, tokens: number) => {
  tally[key] ??= { rows: 0, tokens: 0 };
  tally[key]!.rows += 1;
  tally[key]!.tokens += tokens;
};
const session0736: Tally = {};
const contextIndex: Tally = {};
const indexUnallocatedReasons: Tally = {};
const transitions: Tally = {};
let overBound0736 = 0;
let indexPathNotUsed = 0;
let changedProject = 0;
let evaluationMs = 0;
for (const row of sample) {
  const event = JSON.parse(row.payloadJson) as AiInteractionEvent;
  const input = [{ event, repoHash: row.repoHash }];
  const started = performance.now();
  const scanned = new SessionAttributionBatch(db, input, { contextIndex: false });
  const indexed = new SessionAttributionBatch(db, input);
  const before = scanned.attribute(event, { repoHash: row.repoHash, branchHash: row.branchHash });
  const after = indexed.attribute(event, { repoHash: row.repoHash, branchHash: row.branchHash });
  evaluationMs += performance.now() - started;
  if (scanned.stats().boundReached > 0) overBound0736 += 1;
  if (!indexed.stats().contextIndex) indexPathNotUsed += 1;
  add(session0736, before.basis, row.tokens);
  add(contextIndex, after.basis, row.tokens);
  add(transitions, `${before.basis} -> ${after.basis}`, row.tokens);
  if (before.basis === "session_inherited" && after.basis === "session_inherited" &&
      before.event.projectKey !== after.event.projectKey) changedProject += 1;
  if (after.basis === "unallocated") {
    const at = Date.parse(event.observedAt);
    const contexts = Number.isFinite(at)
      ? (countContexts.get(event.sessionId, new Date(at - SESSION_INHERIT_WINDOW_MS).toISOString(),
        new Date(at + SESSION_INHERIT_WINDOW_MS).toISOString(), SESSION_INHERIT_MAX_CONTEXT_ROWS + 1) as { n: number }).n
      : 0;
    add(indexUnallocatedReasons,
      contexts === 0 ? "no_context_in_window"
        : contexts > SESSION_INHERIT_MAX_CONTEXT_ROWS ? "over_256_contexts"
          : "no_usable_context (invalid hash or only later repos in a multi-repo window)",
      row.tokens);
  }
}
buffer.close();

const share = (tally: Tally, key: string, total: { rows: number; tokens: number }) => ({
  rows: tally[key]?.rows ?? 0,
  rowShare: Number(((tally[key]?.rows ?? 0) / Math.max(1, total.rows)).toFixed(4)),
  tokens: tally[key]?.tokens ?? 0,
  tokenShare: Number(((tally[key]?.tokens ?? 0) / Math.max(1, total.tokens)).toFixed(4)),
});
const total = { rows: sample.length, tokens: sample.reduce((sum, row) => sum + row.tokens, 0) };
console.log(JSON.stringify({
  ledger: { bytes: ledgerStat.size },
  open: { stateAfterOpen: installedState, installStepMs, openMs: Math.round(openMs) },
  backfill: {
    batchRows,
    batches,
    elapsedMs: Math.round(backfillMs),
    slowestBatchMs: Math.round(slowestBatchMs),
    maintenanceJobsAt250Ms: Math.ceil(backfillMs / 250),
    status: indexStatus,
  },
  sample: {
    source: rowidsFile ? "rowids file" : "seeded self-sample",
    rows: total.rows,
    tokens: total.tokens,
    observedAtRange: sample.length ? [
      sample.reduce((min, row) => (row.observedAt < min ? row.observedAt : min), sample[0]!.observedAt),
      sample.reduce((max, row) => (row.observedAt > max ? row.observedAt : max), sample[0]!.observedAt),
    ] : null,
  },
  session0736: {
    windowOverScanBound: overBound0736,
    inherited: share(session0736, "session_inherited", total),
    unallocated: share(session0736, "unallocated", total),
  },
  contextIndex: {
    batchesNotOnIndexPath: indexPathNotUsed,
    inherited: share(contextIndex, "session_inherited", total),
    unallocated: share(contextIndex, "unallocated", total),
    unallocatedReasons: indexUnallocatedReasons,
  },
  transitions,
  inheritedByBothWithDifferentProject: changedProject,
  evaluationMs: Math.round(evaluationMs),
}, null, 1));
