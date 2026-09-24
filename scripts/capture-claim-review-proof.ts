/**
 * eco-6hoxj.163.18 r3 — the collector findings of the r2 review, each as one
 * check that fails on the reviewed claim (`20b80a9`) and passes after the
 * fix. It uses only what both versions have, and picks the claim call by its
 * arity, so the same scenarios run on both:
 *
 * - B1: the reviewer's Codex day-folder scenario (real tailers, real
 *   maintenance cadences), then the Claude transcript left unread past the
 *   48-hour window. Time passing is a shifted wall clock.
 * - B3: a claim that cannot attest says why, so the cloud withdraws.
 * - S2: dead letters are bounded gaps; privacy refusals are withheld, not gaps.
 * - S4: spooled push events bound the claim; rejected or expired ones are gaps.
 * - S5: a claim that cannot be computed never stops the upload.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import * as frontierModule from "../packages/collector-cli/src/capture-frontier";
import type { CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema, collectorHome } from "../packages/collector-cli/src/config";
import { hookSpoolDirectory, listHookSpoolFiles, rejectHookSpoolFile, writeHookSpoolFile } from "../packages/collector-cli/src/hook-spool";
import { DEFAULT_JSONL_TAILER_IO } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { OtlpIntakeSpool, otlpSpoolDirectory } from "../packages/collector-cli/src/otlp-spool";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acknowledgingFetch } from "./fixtures/delivery-ack-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("capture-claim-review", 6);
const results: Array<{ name: string; passed: boolean; detail: Record<string, unknown> }> = [];
const check = (name: string, passed: boolean, detail: Record<string, unknown>) => {
  completion.check(name, passed);
  results.push({ name, passed, detail });
};

type Claim = {
  through: string | null;
  unattested?: string;
  pending: number;
  dead: number;
  withheld?: number;
  gaps?: Array<{ from: string; to: string }>;
  gapSince?: string | null;
  cursor: number;
};
type FrontierApi = {
  advanceCaptureFrontier?: (
    database: LocalEventBuffer["database"],
    source: "codex" | "claude_code",
    snapshot: { complete: boolean; files: [] },
    startedAt: string,
  ) => string | null;
  recordCompleteCapturePass?: (
    database: LocalEventBuffer["database"],
    source: "codex" | "claude_code",
    startedAt: string,
    now?: Date,
  ) => boolean;
  CAPTURE_WRITE_LAG_MS?: number;
};
const frontierApi = frontierModule as unknown as FrontierApi;

// A wall clock that can be moved forward, for "time passes" without waiting.
const RealDate = Date;
let clockShiftMs = 0;
class ShiftedDate extends RealDate {
  constructor(...args: unknown[]) {
    if (args.length === 0) super(RealDate.now() + clockShiftMs);
    else super(...(args as [string | number | Date]));
  }
  static now() {
    return RealDate.now() + clockShiftMs;
  }
}
globalThis.Date = ShiftedDate as unknown as DateConstructor;

const HOUR = 3_600_000;
const DAY = 24 * HOUR;
const iso = (ms: number) => new Date(ms).toISOString();
const TENANT = "00000000-0000-4000-8000-0000000000c1";
const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capture-review-proof-")));
let ledgerIndex = 0;
const ledger = () => path.join(root, `ledger-${++ledgerIndex}.sqlite`);
const EMPTY_SPOOL = { pendingFiles: 0, oldestPendingMs: null, losses: [], unreadable: false };
let eventIndex = 0;

function event(observedMs: number) {
  eventIndex += 1;
  return aiInteractionEventSchema.parse({
    id: `00000000-0000-4000-8000-${String(700_000 + eventIndex).padStart(12, "0")}`,
    sessionId: `00000000-0000-4000-8000-${String(600_000 + eventIndex).padStart(12, "0")}`,
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: iso(observedMs),
    actionClass: "other",
    inputTokens: 3,
    outputTokens: 1,
    metadata: { proof: "capture-claim-review" },
  });
}

function managedBuffer(epochStartMs: number, maxActiveRows = 50_000) {
  return new LocalEventBuffer(ledger(), {
    workspaceId: TENANT,
    delivery: { enabled: true, limits: { maxOldestAgeDays: 3650, maxActiveRows } },
    enrollmentNow: () => new Date(epochStartMs),
  });
}

/** The claim as the upload path takes it (r3 passes the spool state; the reviewed claim has no such input). */
function claimOf(buffer: LocalEventBuffer, ids: string[]): Claim {
  const delivery = buffer.delivery as unknown as { captureClaim: (...args: unknown[]) => Claim | null };
  const claim = delivery.captureClaim.length >= 2 ? delivery.captureClaim(ids, EMPTY_SPOOL) : delivery.captureClaim(ids);
  if (!claim) throw new Error("no claim");
  return claim;
}

/** Make `throughMs` the attested frontier of both tailed sources, the way each version records one. */
function attestThrough(buffer: LocalEventBuffer, throughMs: number) {
  for (const source of ["codex", "claude_code"] as const) {
    if (frontierApi.advanceCaptureFrontier) {
      frontierApi.advanceCaptureFrontier(buffer.database, source, { complete: true, files: [] },
        iso(throughMs + frontierApi.CAPTURE_WRITE_LAG_MS!));
    } else {
      frontierApi.recordCompleteCapturePass!(buffer.database, source, iso(throughMs), new Date(Math.max(Date.now(), throughMs) + 60_000));
    }
  }
}

const outboxIds = (buffer: LocalEventBuffer) =>
  (buffer.database.prepare(`select delivery_id as id from upload_outbox`).all() as Array<{ id: string }>).map((row) => row.id);
const inGap = (claim: Claim, at: string) => (claim.gaps ?? []).some((gap) => gap.from <= at && at <= gap.to);
/** The claim vouches for `at` without naming it as missing. */
const attests = (claim: Claim, at: string) => claim.through !== null && claim.through > at && !inGap(claim, at);

/** A ledger with one Codex and one Claude capture root and real tailers, past its enrollment baseline. */
async function captureWorld(label: string) {
  const base = path.join(root, label);
  const epochStartMs = Date.now() - 10 * DAY;
  const buffer = managedBuffer(epochStartMs);
  const epochId = buffer.workspaceBinding()!.currentInstallationEpochId!;
  const roots: CaptureRoot[] = [
    { source: "codex", rootId: "codex-0", profileId: "profile-codex-0", directory: path.join(base, "codex"), installationEpochId: epochId },
    { source: "claude_code", rootId: "claude-0", profileId: "profile-claude-0", directory: path.join(base, "claude"), installationEpochId: epochId },
  ];
  for (const captureRoot of roots) fs.mkdirSync(captureRoot.directory, { recursive: true });
  const rollout = new RolloutTailer(buffer, undefined, () => [], DEFAULT_JSONL_TAILER_IO, roots.filter((r) => r.source === "codex"));
  const transcript = new TranscriptTailer(buffer, undefined, DEFAULT_JSONL_TAILER_IO, roots.filter((r) => r.source === "claude_code"));
  // The fixed collector checks coverage at most every 15 minutes; here, every
  // cadence. Collector 0.7.37 put its Grok tailer before the options.
  const options = { captureCoverageIntervalMs: 0 };
  const maintenance = CollectorMaintenance.length >= 5
    ? new CollectorMaintenance(buffer, rollout, transcript, undefined, undefined, options)
    : new CollectorMaintenance(buffer, rollout, transcript, undefined, options as never);
  const run = () => maintenance.runRecent({ onDurableCommit: () => true, onProgress: () => true });
  let cadences = 0;
  while (captureBaselineStatus(buffer.database).status !== "complete" && cadences < 40) {
    await run();
    cadences += 1;
  }
  const eventsFor = (session: string) =>
    (buffer.database.prepare(`select count(*) as n from buffered_events where session_id = ? or payload_json like ?`)
      .get(session, `%${session}%`) as { n: number }).n;
  const finalClaim = () => {
    buffer.delivery.migrateLegacy({ now: new Date() });
    return claimOf(buffer, outboxIds(buffer));
  };
  return { buffer, roots, maintenance, run, eventsFor, finalClaim, baseline: captureBaselineStatus(buffer.database).status };
}

async function b1CodexDayFolder() {
  const world = await captureWorld("b1-codex");
  const dayDir = (daysAgo: number) =>
    path.join(world.roots[0]!.directory, ...new Date(Date.now() - daysAgo * DAY).toISOString().slice(0, 10).split("-"));
  const writeSession = (daysAgo: number, session: string, startedAt: string, usageAt: string) => {
    const dir = dayDir(daysAgo);
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, `rollout-${startedAt.slice(0, 19).replace(/:/g, "-")}-${session}.jsonl`);
    const usage = (tokens: number) => ({ type: "event_msg", timestamp: usageAt, payload: { type: "token_count", info: {
      total_token_usage: { input_tokens: tokens, cached_input_tokens: 0, output_tokens: tokens, reasoning_output_tokens: 0 } } } });
    const lines = [
      { type: "session_meta", timestamp: startedAt, payload: { id: session } },
      { type: "turn_context", timestamp: startedAt, payload: { model: "gpt-5.5" } },
      usage(0), usage(1000), usage(2500),
    ];
    fs.writeFileSync(file, lines.map((line) => JSON.stringify(line)).join("\n") + "\n");
  };
  // Same moment of use: one session filed today, one filed three UTC days ago and still running.
  const usageAt = new Date().toISOString();
  const today = "11111111-1111-4111-8111-111111111111";
  const older = "22222222-2222-4222-8222-222222222222";
  writeSession(0, today, new Date(Date.now() - HOUR).toISOString(), usageAt);
  writeSession(3, older, new Date(Date.now() - 3 * DAY).toISOString(), usageAt);
  for (let i = 0; i < 3; i += 1) await world.run();
  const claimNow = world.finalClaim();
  // Three hours later, still never read: the automatic cadences keep running.
  clockShiftMs = 3 * HOUR;
  for (let i = 0; i < 3; i += 1) await world.run();
  const claimLater = world.finalClaim();
  clockShiftMs = 0;
  const detail = {
    baseline: world.baseline, usageAt, todayEvents: world.eventsFor(today), olderEvents: world.eventsFor(older),
    claimNow, claimLater,
  };
  world.maintenance.close();
  world.buffer.close();
  check("B1_codex_rollout_in_older_day_folder_is_never_attested",
    detail.todayEvents > 0 && detail.olderEvents === 0 && !attests(claimNow, usageAt) && !attests(claimLater, usageAt) &&
      claimLater.through !== null && claimLater.through > usageAt,
    detail);
}

async function b1ClaudeTranscriptPast48Hours() {
  const world = await captureWorld("b1-claude");
  // A Claude session writes while no cadence runs; the next cadence comes
  // more than 48 hours after its last write.
  const usageAt = new Date().toISOString();
  const session = "33333333-3333-4333-8333-333333333333";
  const project = path.join(world.roots[1]!.directory, "project-a");
  fs.mkdirSync(project, { recursive: true });
  fs.writeFileSync(path.join(project, `${session}.jsonl`), `${JSON.stringify({
    type: "assistant", timestamp: usageAt, sessionId: session,
    message: { id: `${session}-1`, model: "claude-opus-5", usage: { input_tokens: 100, output_tokens: 20 } },
  })}\n`);
  clockShiftMs = 50 * HOUR;
  for (let i = 0; i < 3; i += 1) await world.run();
  const claim = world.finalClaim();
  clockShiftMs = 0;
  const detail = { baseline: world.baseline, usageAt, sessionEvents: world.eventsFor(session), claim };
  world.maintenance.close();
  world.buffer.close();
  check("B1_claude_transcript_left_unread_past_48_hours_is_never_attested",
    detail.sessionEvents === 0 && !attests(claim, usageAt) && claim.through !== null && claim.through > usageAt,
    detail);
}

function b3ClaimThatCannotAttestSaysWhy() {
  // The reviewer's P1: attested T, then a history import of older events over the row budget.
  const now = Date.now();
  const buffer = managedBuffer(now - 40 * DAY, 100);
  const unknown = (() => {
    buffer.append(event(now - 10 * 60_000));
    buffer.delivery.migrateLegacy({ now: new Date() });
    return claimOf(buffer, []);
  })();
  const attestedAt = now - 60_000;
  attestThrough(buffer, attestedAt);
  const lease = buffer.delivery.lease({ maxRows: 100, now: new Date(now + 2 * HOUR) });
  buffer.delivery.acknowledge(lease.leaseId, lease.items.map((item) => item.deliveryId));
  const attested = claimOf(buffer, []);
  const older = Array.from({ length: 150 }, (_, index) => event(now - 30 * DAY + index * 60_000));
  for (const item of older) buffer.append(item);
  const overBudget = claimOf(buffer, older.slice(0, 10).map((item) => item.id));
  buffer.database.prepare(`update upload_control set migration_complete = 0 where singleton = 1`).run();
  const migrating = claimOf(buffer, []);
  buffer.close();
  check("B3_claim_that_cannot_attest_says_why",
    attested.through !== null && unknown.through === null && unknown.unattested === "frontier_unknown" &&
      overBudget.through === null && overBudget.unattested === "over_row_budget" && overBudget.pending === 140 &&
      migrating.through === null && migrating.unattested === "migration_incomplete",
    { unknown, attested, overBudget, migrating });
}

function s2DeadLettersAreBoundedGaps() {
  const now = Date.now();
  const epochStartMs = now - 20 * DAY;
  const buffer = managedBuffer(epochStartMs);
  const lostAt = now - 18 * DAY;
  const refusedAt = now - 17 * DAY;
  const lost = event(lostAt);
  const refused = event(refusedAt);
  const current = event(now - 2 * HOUR);
  for (const item of [lost, refused, current]) buffer.append(item);
  buffer.delivery.migrateLegacy({ now: new Date() });
  // A genuine loss: the cloud rejected it for good.
  const lease = buffer.delivery.lease({ maxRows: 10, now: new Date() });
  buffer.delivery.deadLetterRemote(lease.leaseId, [lost.id]);
  buffer.delivery.acknowledge(lease.leaseId, [current.id]);
  // A deliberate local refusal: the privacy policy keeps it on this machine.
  buffer.database.prepare(`delete from upload_outbox where delivery_id = ?`).run(refused.id);
  buffer.database.prepare(
    `insert into upload_receipts (delivery_id, terminal_state, reason, status_class, attempt_count, created_at, terminal_at)
     values (?, 'dead', 'local_privacy_violation', 'local', 0, ?, ?)`,
  ).run(refused.id, iso(now - 60_000), iso(now - 60_000));
  const attestedAt = now - HOUR;
  attestThrough(buffer, attestedAt);
  const claim = claimOf(buffer, []);
  buffer.close();
  check("S2_dead_letters_are_bounded_gaps_and_privacy_refusals_are_withheld",
    claim.through === iso(attestedAt) && claim.dead === 1 && claim.withheld === 1 &&
      JSON.stringify(claim.gaps) === JSON.stringify([{ from: iso(lostAt), to: iso(lostAt) }]) &&
      !inGap(claim, iso(refusedAt)),
    { claim, lostAt: iso(lostAt), refusedAt: iso(refusedAt), attestedAt: iso(attestedAt) });
}

function uploadConfig() {
  return collectorConfigSchema.parse({
    uploadUrl: "http://127.0.0.1:1/api/work-intelligence/ingest",
    tenantId: TENANT,
    installKey: "pli_capture_review_proof_install_00000",
    delivery: { maxOldestAgeDays: 3650, requestTimeoutSeconds: 2 },
  });
}

function recordingFetch(requests: Array<{ claim: Claim | null }>) {
  return acknowledgingFetch(async (_input, init) => {
    const header = new Headers(init?.headers).get("x-plimsoll-capture");
    requests.push({ claim: header ? JSON.parse(header) as Claim : null });
    const count = (JSON.parse(String(init?.body ?? "{}")) as { events?: unknown[] }).events?.length ?? 0;
    return new Response(JSON.stringify({ ok: true, accepted: count, inserted: count }), {
      status: 200, headers: { "content-type": "application/json" },
    });
  });
}

async function s4SpooledPushEvents() {
  const now = Date.now();
  const epochStartMs = now - 6 * HOUR;
  const home = collectorHome();
  const buffer = managedBuffer(epochStartMs);
  const queued = event(epochStartMs + 10 * 60_000);
  buffer.append(queued);
  buffer.delivery.migrateLegacy({ now: new Date() });
  attestThrough(buffer, epochStartMs + 5 * HOUR);
  // An OTLP export spooled an hour into the epoch and deleted at its age limit.
  const expiredAt = epochStartMs + HOUR;
  const otlpDirectory = otlpSpoolDirectory(home);
  fs.mkdirSync(otlpDirectory, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(otlpDirectory, `${expiredAt}-${process.pid}-1-abcdef12.json`), "{}\n", { mode: 0o600 });
  const otlp = new OtlpIntakeSpool({ home, nowMs: () => now + 8 * DAY, onWarning: () => undefined });
  const drained = await otlp.drain(buffer);
  otlp.stopDrain();
  // A hook event the replay rejected, and one still waiting in the spool.
  const rejectedAt = epochStartMs + 3.5 * HOUR;
  const waitingAt = epochStartMs + 4 * HOUR;
  writeHookSpoolFile({ home, source: "claude_code", body: "{}", nowMs: rejectedAt });
  const rejected = listHookSpoolFiles(home).find((file) => file.spooledAtMs === rejectedAt)!;
  rejectHookSpoolFile(home, rejected, "spool_untrusted");
  writeHookSpoolFile({ home, source: "claude_code", body: "{}", nowMs: waitingAt });
  const requests: Array<{ claim: Claim | null }> = [];
  await uploadBufferedEvents(uploadConfig(), buffer, { fetchImpl: recordingFetch(requests) });
  buffer.close();
  for (const directory of [hookSpoolDirectory(home), otlpDirectory]) fs.rmSync(directory, { recursive: true, force: true });
  const claim = requests[0]?.claim ?? null;
  const expected = {
    through: iso(waitingAt - HOUR),
    gaps: [
      { from: iso(epochStartMs), to: iso(expiredAt) },
      { from: iso(rejectedAt - HOUR), to: iso(rejectedAt) },
    ],
  };
  check("S4_spooled_push_events_bound_the_claim_and_their_losses_are_gaps",
    drained.expired === 1 && claim !== null && claim.through === expected.through &&
      JSON.stringify(claim.gaps) === JSON.stringify(expected.gaps) && claim.dead === 2 && claim.pending === 1,
    { expired: drained.expired, claim, expected });
}

async function s5ClaimFailureNeverStopsUpload() {
  // The reviewer's P3: one queued row with an unreadable envelope, not due this cycle.
  const now = Date.now();
  const buffer = managedBuffer(now - 40 * DAY);
  const due = event(now - 5 * 60_000);
  const parked = event(now - 4 * 60_000);
  buffer.append(due);
  buffer.append(parked);
  buffer.delivery.migrateLegacy({ now: new Date() });
  attestThrough(buffer, now - 60_000);
  buffer.database.prepare(`update upload_outbox set base_envelope_json = '{unreadable', state = 'retry', next_attempt_at = ? where delivery_id = ?`)
    .run(iso(now + HOUR), parked.id);
  const outcome = async () => {
    const requests: Array<{ claim: Claim | null }> = [];
    try {
      const uploaded = await uploadBufferedEvents(uploadConfig(), buffer, { fetchImpl: recordingFetch(requests) });
      return { threw: false, uploadedEvents: uploaded.uploadedEvents, requests };
    } catch (error) {
      return { threw: true, error: error instanceof Error ? error.message : String(error), requests };
    }
  };
  const unreadable = await outcome();
  // Any other failure inside the claim: the request goes out without one.
  const later = event(now - 3 * 60_000);
  buffer.append(later);
  const summaryTable = buffer.database.prepare(`select 1 from sqlite_master where type = 'table' and name = 'capture_dead_summary'`).get();
  if (summaryTable) buffer.database.exec(`drop table capture_dead_summary`);
  const failing = await outcome();
  buffer.close();
  check("S5_claim_failure_never_stops_the_upload",
    !unreadable.threw && unreadable.uploadedEvents === 1 && unreadable.requests.length === 1 &&
      unreadable.requests[0]!.claim !== null &&
      !failing.threw && failing.uploadedEvents === 1 && failing.requests.length === 1 && failing.requests[0]!.claim === null,
    { unreadable, failing, summaryTableDropped: Boolean(summaryTable) });
}

async function main() {
  for (const step of [b1CodexDayFolder, b1ClaudeTranscriptPast48Hours, b3ClaimThatCannotAttestSaysWhy,
    s2DeadLettersAreBoundedGaps, s4SpooledPushEvents, s5ClaimFailureNeverStopsUpload]) {
    try {
      await step();
    } catch (error) {
      clockShiftMs = 0;
      check(step.name, false, { error: error instanceof Error ? `${error.name}: ${error.message}` : String(error) });
    }
  }
  console.log(JSON.stringify({ proof: "capture-claim-review", results }, null, 2));
  fs.rmSync(root, { recursive: true, force: true });
  if (results.every((result) => result.passed)) completion.complete();
  else process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
