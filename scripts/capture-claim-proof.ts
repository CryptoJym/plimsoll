/**
 * eco-6hoxj.163.18 — capture watermark v1, collector side.
 *
 * Proves the claim the collector attaches to each upload request
 * (`x-plimsoll-capture`, documented in the cloud's
 * docs/capture-watermark-v1.md): it attests nothing, and says why, before both
 * tailed sources have a coverage-checked frontier; queued deliveries hold it
 * back to their observed time; a dead delivery is a bounded gap that holds
 * nothing back; the cursor strictly increases; the frontier holds for a file
 * with unread bytes and later reports it as a bounded gap; the claim is scoped
 * to the current installation epoch; over the queue's row budget it attests
 * nothing without reading queued envelopes; an unreadable envelope never fails
 * it; and on the wire it is a signed header while the batch body stays
 * byte-for-byte the shape an older, strict cloud accepts. The reviewer's
 * scenarios are proved in capture-claim-review-proof.ts.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  advanceCaptureFrontier,
  CAPTURE_WRITE_LAG_MS,
  captureFrontier,
  type CaptureCoverageFile,
  type CaptureFrontierSource,
} from "../packages/collector-cli/src/capture-frontier";
import type { CaptureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema, aiWorkIngestBatchSchema } from "../packages/shared/src/index";
import { acknowledgingFetch } from "./fixtures/delivery-ack-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("capture-claim", 13);
const check = (name: string, passed: boolean, detail: Record<string, unknown> = {}) => {
  completion.check(name, passed);
  if (!passed) throw new Error(`${name} failed: ${JSON.stringify(detail)}`);
};

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capture-claim-proof-"));
let ledgerIndex = 0;
const ledger = () => path.join(root, `ledger-${++ledgerIndex}.sqlite`);
const TENANT = "00000000-0000-4000-8000-0000000000c1";
const OTHER_TENANT = "00000000-0000-4000-8000-0000000000c2";
const SIGNING_SECRET = "capture-claim-proof-signing-secret";
const REAL_NOW = Date.now();
const EPOCH_START_MS = REAL_NOW - 6 * 3_600_000;
const iso = (ms: number) => new Date(ms).toISOString();
const HOUR = 3_600_000;
const NO_SPOOL: CaptureSpoolState = { pendingFiles: 0, oldestPendingMs: null, losses: [], unreadable: false };
// The enrollment clock the next ledger binding reads; a later epoch starts later.
let enrollmentClock = EPOCH_START_MS;
let eventIndex = 0;

function event(observedMs: number) {
  eventIndex += 1;
  return aiInteractionEventSchema.parse({
    id: `00000000-0000-4000-8000-${String(900_000 + eventIndex).padStart(12, "0")}`,
    sessionId: `00000000-0000-4000-8000-${String(800_000 + eventIndex).padStart(12, "0")}`,
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: iso(observedMs),
    actionClass: "other",
    inputTokens: 3,
    outputTokens: 1,
    metadata: { proof: "capture-claim" },
  });
}

function managedBuffer(workspaceId = TENANT) {
  return new LocalEventBuffer(ledger(), {
    workspaceId,
    delivery: { enabled: true, limits: { maxOldestAgeDays: 3650 } },
    enrollmentNow: () => new Date(enrollmentClock),
  });
}

const outboxIds = (buffer: LocalEventBuffer) =>
  (buffer.database.prepare(`select delivery_id as id from upload_outbox order by created_at, delivery_id`).all() as Array<{ id: string }>)
    .map((row) => row.id);

/** A complete coverage check whose start makes the frontier `throughMs` (the write lag later). */
function cover(buffer: LocalEventBuffer, source: CaptureFrontierSource, throughMs: number, files: CaptureCoverageFile[] = []) {
  return advanceCaptureFrontier(buffer.database, source, { complete: true, files }, iso(throughMs + CAPTURE_WRITE_LAG_MS));
}

function coverBoth(buffer: LocalEventBuffer, codexMs: number, claudeMs: number) {
  return cover(buffer, "codex", codexMs) !== null && cover(buffer, "claude_code", claudeMs) !== null;
}

const claimFor = (buffer: LocalEventBuffer, ids: string[]) => buffer.delivery.captureClaim(ids, NO_SPOOL)!;

async function main() {
  // 1. No coverage check yet: the claim carries the epoch and a cursor, attests
  //    nothing, and says why. (The upload path finishes the legacy migration
  //    before it leases; so does this.)
  const buffer = managedBuffer();
  const first = event(EPOCH_START_MS + 60_000);
  buffer.append(first);
  buffer.delivery.migrateLegacy({ now: new Date() });
  const binding = buffer.workspaceBinding()!;
  const claim0 = claimFor(buffer, outboxIds(buffer));
  check("no_coverage_check_attests_nothing_and_says_why",
    claim0.through === null && claim0.unattested === "frontier_unknown" && claim0.pending === 0 && claim0.dead === 0 &&
      claim0.withheld === 0 && claim0.gaps.length === 0 && claim0.epoch === binding.currentInstallationEpochId &&
      claim0.epochStartedAt === iso(EPOCH_START_MS) && claim0.cursor === 1,
    { claim0 });

  // 2. Only one tailed source checked: still nothing; both: the earlier frontier.
  cover(buffer, "codex", EPOCH_START_MS + HOUR);
  const oneSource = claimFor(buffer, outboxIds(buffer));
  coverBoth(buffer, EPOCH_START_MS + HOUR, EPOCH_START_MS + 2 * HOUR);
  const bothSources = claimFor(buffer, outboxIds(buffer));
  check("claim_bounded_by_both_sources_frontiers",
    oneSource.through === null && oneSource.unattested === "frontier_unknown" &&
      bothSources.through === iso(EPOCH_START_MS + HOUR) && bothSources.unattested === undefined &&
      oneSource.cursor === 2 && bothSources.cursor === 3,
    { oneSource, bothSources });

  // 3. A queued delivery outside this request holds the claim at its observed time.
  const queued = event(EPOCH_START_MS + 10 * 60_000);
  buffer.append(queued);
  const outside = claimFor(buffer, [first.id]);
  const inside = claimFor(buffer, [first.id, queued.id]);
  check("queued_delivery_outside_request_holds_claim",
    outside.through === iso(EPOCH_START_MS + 10 * 60_000) && outside.pending === 1 &&
      inside.through === iso(EPOCH_START_MS + HOUR) && inside.pending === 0,
    { outside, inside });

  // 4. A dead delivery is a bounded gap at its observed time and holds nothing
  //    back (review r2 S2); replay removes the gap, and the requeued delivery
  //    holds the claim until it is delivered.
  const lease = buffer.delivery.lease({ maxRows: 10, now: new Date() });
  buffer.delivery.deadLetterRemote(lease.leaseId, [queued.id]);
  buffer.delivery.retry(lease.leaseId, lease.items.filter((item) => item.deliveryId !== queued.id), "remote_transient", new Date());
  const deadClaim = claimFor(buffer, [first.id]);
  const deadAt = iso(EPOCH_START_MS + 10 * 60_000);
  check("dead_delivery_is_a_bounded_gap_that_holds_nothing_back",
    deadClaim.dead === 1 && JSON.stringify(deadClaim.gaps) === JSON.stringify([{ from: deadAt, to: deadAt }]) &&
      deadClaim.through === iso(EPOCH_START_MS + HOUR) && deadClaim.pending === 0 && deadClaim.withheld === 0,
    { deadClaim });
  const replay = buffer.delivery.replayDeadLetters({ reason: "remote_validation_rejected" });
  const replayed = claimFor(buffer, [first.id]);
  check("replayed_dead_delivery_clears_gap_but_holds_claim_until_delivered",
    replay.requeued === 1 && replayed.dead === 0 && replayed.gaps.length === 0 && replayed.pending === 1 &&
      replayed.through === deadAt,
    { replay, replayed });

  // 5. Delivered rows stop holding it; the cursor never repeats.
  const all = buffer.delivery.lease({ maxRows: 10, now: new Date(Date.now() + 3_600_000 * 2) });
  const acknowledged = buffer.delivery.acknowledge(all.leaseId, all.items.map((item) => item.deliveryId));
  const drained = claimFor(buffer, []);
  const cursors = [claim0, oneSource, bothSources, outside, inside, deadClaim, replayed, drained].map((claim) => claim.cursor);
  check("acknowledged_deliveries_release_claim_and_cursor_strictly_increases",
    acknowledged.acknowledged === 2 && drained.pending === 0 && drained.dead === 0 &&
      drained.through === iso(EPOCH_START_MS + HOUR) &&
      cursors.every((cursor, index) => index === 0 || cursor > cursors[index - 1]!),
    { acknowledged, drained, cursors });

  // 6. The legacy migration has not enqueued every ledger row: nothing is
  //    attested, and the claim says so, so the cloud withdraws (review r2 B3).
  buffer.database.prepare(`update upload_control set migration_complete = 0 where singleton = 1`).run();
  const migrating = claimFor(buffer, []);
  buffer.database.prepare(`update upload_control set migration_complete = 1 where singleton = 1`).run();
  check("incomplete_legacy_migration_attests_nothing_and_says_why",
    migrating.through === null && migrating.unattested === "migration_incomplete", { migrating });

  // 7. The frontier: an incomplete or out-of-order check changes nothing; a
  //    file with unread bytes holds it for up to the hold limit, then is
  //    reported as a bounded gap while the frontier moves on; reading the file
  //    to its end removes the gap. The frontier never moves backwards.
  const frontierAt = () => captureFrontier(buffer.database)!;
  const step = (startedMs: number, files: CaptureCoverageFile[], complete = true) =>
    advanceCaptureFrontier(buffer.database, "codex", { complete, files }, iso(startedMs));
  const unread = (fullyRead: boolean): CaptureCoverageFile => ({
    key: "a".repeat(64), size: 10, mtimeMs: EPOCH_START_MS + 4.5 * HOUR, birthtimeMs: EPOCH_START_MS,
    hasCursor: true, fullyRead,
  });
  const frontier = {
    base: step(EPOCH_START_MS + 4 * HOUR, []),
    incomplete: step(EPOCH_START_MS + 5 * HOUR, [], false),
    outOfOrder: step(EPOCH_START_MS + 3 * HOUR, []),
    held: step(EPOCH_START_MS + 5 * HOUR, [unread(false)]),
    heldGaps: frontierAt().gaps,
    passed: step(EPOCH_START_MS + 5 * HOUR + 30 * 60_000, [unread(false)]),
    passedGaps: frontierAt().gaps,
    read: step(EPOCH_START_MS + 5 * HOUR + 40 * 60_000, [unread(true)]),
    readGaps: frontierAt().gaps,
  };
  check("frontier_holds_unread_file_then_reports_it_as_a_bounded_gap",
    frontier.base === iso(EPOCH_START_MS + 3 * HOUR) && frontier.incomplete === frontier.base &&
      frontier.outOfOrder === frontier.base && frontier.held === frontier.base && frontier.heldGaps.length === 0 &&
      frontier.passed === iso(EPOCH_START_MS + 4 * HOUR + 30 * 60_000) &&
      JSON.stringify(frontier.passedGaps) === JSON.stringify([{ fromMs: EPOCH_START_MS + 3 * HOUR, toMs: EPOCH_START_MS + 4.5 * HOUR }]) &&
      frontier.read === iso(EPOCH_START_MS + 4 * HOUR + 40 * 60_000) && frontier.readGaps.length === 0,
    { frontier });

  // 8. A new epoch (the ledger joins another workspace) starts from nothing and
  //    does not count the previous epoch's rows.
  const leftover = event(EPOCH_START_MS + 20 * 60_000);
  buffer.append(leftover);
  const newEpochId = crypto.randomUUID();
  enrollmentClock = REAL_NOW - 60_000;
  buffer.transitionWorkspace(TENANT, OTHER_TENANT, undefined, newEpochId);
  const fresh = claimFor(buffer, []);
  check("new_epoch_starts_unattested_and_ignores_previous_epoch_rows",
    fresh.epoch === newEpochId && fresh.epochStartedAt === iso(REAL_NOW - 60_000) && fresh.through === null &&
      fresh.unattested !== undefined && fresh.pending === 0 && fresh.dead === 0 && fresh.gaps.length === 0,
    { fresh });
  buffer.close();

  // 9. On the wire: a signed header, and a body an older strict cloud accepts.
  enrollmentClock = EPOCH_START_MS;
  const wire = managedBuffer();
  const sent = event(EPOCH_START_MS + 30 * 60_000);
  wire.append(sent);
  coverBoth(wire, EPOCH_START_MS + 2 * HOUR, EPOCH_START_MS + 2 * HOUR);
  const requests: Array<{ headers: Headers; body: string }> = [];
  const config = collectorConfigSchema.parse({
    uploadUrl: "http://127.0.0.1:1/api/work-intelligence/ingest",
    tenantId: TENANT,
    installKey: "pli_capture_claim_proof_install_000000",
    uploadSigningSecret: SIGNING_SECRET,
    delivery: { maxOldestAgeDays: 3650, requestTimeoutSeconds: 2 },
  });
  const fetchImpl = acknowledgingFetch(async (_input, init) => {
    requests.push({ headers: new Headers(init?.headers), body: String(init?.body ?? "") });
    const count = (JSON.parse(String(init?.body ?? "{}")) as { events?: unknown[] }).events?.length ?? 0;
    return new Response(JSON.stringify({ ok: true, accepted: count, inserted: count }), {
      status: 200, headers: { "content-type": "application/json" },
    });
  });
  const uploaded = await uploadBufferedEvents(config, wire, { fetchImpl, spoolHome: path.join(root, "spools") });
  const request = requests[0];
  const header = request?.headers.get("x-plimsoll-capture") ?? "";
  const timestamp = request?.headers.get("x-plimsoll-upload-timestamp") ?? "";
  const expectedSignature = `sha256=${crypto.createHmac("sha256", SIGNING_SECRET)
    .update(`plimsoll-capture-v1\n${timestamp}\n${header}\n${request?.body ?? ""}`).digest("hex")}`;
  const claim = header ? JSON.parse(header) as Record<string, unknown> : {};
  check("upload_carries_one_signed_capture_claim_header",
    uploaded.uploadedEvents === 1 && requests.length === 1 &&
      request!.headers.get("x-plimsoll-capture-signature") === expectedSignature &&
      claim.v === 1 && claim.through === iso(EPOCH_START_MS + 2 * HOUR) && claim.pending === 0 &&
      typeof claim.cursor === "number" && header.length <= 1024,
    { uploaded: uploaded.uploadedEvents, requests: requests.length, claim });
  const body = JSON.parse(request!.body) as Record<string, unknown>;
  const strict = aiWorkIngestBatchSchema.safeParse(body);
  check("batch_body_unchanged_so_a_strict_older_cloud_accepts_it",
    strict.success && JSON.stringify(Object.keys(body)) === JSON.stringify(["tenantId", "installKey", "appVersion", "events"]),
    { keys: Object.keys(body), strict: strict.success });
  // After acknowledgement the next claim is fully drained.
  const afterUpload = claimFor(wire, []);
  check("claim_after_acknowledged_upload_is_drained",
    afterUpload.pending === 0 && afterUpload.dead === 0 && afterUpload.through === iso(EPOCH_START_MS + 2 * HOUR) &&
      afterUpload.cursor === (claim.cursor as number) + 1,
    { afterUpload });
  wire.close();

  // 10. An unreadable queued envelope dates its row to the epoch start instead
  //     of failing the claim (review r2 S5). Over the queue's row budget the
  //     claim attests nothing, says why, and counts from the gauges without
  //     reading the queue: a row appended before the epoch, which the scan
  //     would skip, is counted.
  enrollmentClock = EPOCH_START_MS;
  const bounded = new LocalEventBuffer(ledger(), {
    workspaceId: TENANT,
    delivery: { enabled: true, limits: { maxOldestAgeDays: 3650, maxActiveRows: 2 } },
    enrollmentNow: () => new Date(enrollmentClock),
  });
  const kept = event(EPOCH_START_MS + 40 * 60_000);
  const unreadable = event(EPOCH_START_MS + 50 * 60_000);
  bounded.append(kept);
  bounded.append(unreadable);
  bounded.delivery.migrateLegacy({ now: new Date() });
  coverBoth(bounded, EPOCH_START_MS + 2 * HOUR, EPOCH_START_MS + 2 * HOUR);
  bounded.database.prepare(`update upload_outbox set base_envelope_json = '{unreadable' where delivery_id = ?`).run(unreadable.id);
  const underBudget = claimFor(bounded, [kept.id]);
  const beforeEpoch = event(EPOCH_START_MS + 55 * 60_000);
  bounded.append(beforeEpoch);
  bounded.database.prepare(`update upload_outbox set created_at = ? where delivery_id = ?`).run(iso(EPOCH_START_MS - HOUR), beforeEpoch.id);
  const overBudget = claimFor(bounded, [kept.id]);
  check("unreadable_envelope_never_fails_claim_and_over_budget_never_reads_the_queue",
    underBudget.through === iso(EPOCH_START_MS) && underBudget.unattested === undefined && underBudget.pending === 1 &&
      overBudget.through === null && overBudget.unattested === "over_row_budget" && overBudget.pending === 2 &&
      overBudget.cursor === underBudget.cursor + 1,
    { underBudget, overBudget });
  bounded.close();
  fs.rmSync(root, { recursive: true, force: true });
  completion.complete();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
