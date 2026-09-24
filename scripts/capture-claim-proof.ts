/**
 * eco-6hoxj.163.18 — capture watermark v1, collector side.
 *
 * Proves the claim the collector attaches to each upload request
 * (`x-plimsoll-capture`, documented in the cloud's
 * docs/capture-watermark-v1.md): it attests nothing before both tailed sources
 * completed a capture pass; queued and dead deliveries hold it back to their
 * observed time; dead deliveries are named as a gap; the cursor strictly
 * increases; the claim is scoped to the current installation epoch; over the
 * queue's row budget it attests nothing without reading queued envelopes; and
 * on the wire it is a signed header while the batch body stays byte-for-byte
 * the shape an older, strict cloud accepts.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  captureFrontier,
  isCompleteCapturePass,
  recordCompleteCapturePass,
} from "../packages/collector-cli/src/capture-frontier";
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
const later = new Date(REAL_NOW + 60_000);
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

function bothPasses(buffer: LocalEventBuffer, codexMs: number, claudeMs: number) {
  return recordCompleteCapturePass(buffer.database, "codex", iso(codexMs), later) &&
    recordCompleteCapturePass(buffer.database, "claude_code", iso(claudeMs), later);
}

async function main() {
  // 1. No capture pass yet: the claim carries the epoch and a cursor, attests nothing.
  const buffer = managedBuffer();
  const first = event(EPOCH_START_MS + 60_000);
  buffer.append(first);
  const binding = buffer.workspaceBinding()!;
  const claim0 = buffer.delivery.captureClaim(outboxIds(buffer))!;
  check("no_capture_pass_attests_nothing", claim0.through === null && claim0.pending === 0 && claim0.dead === 0 &&
    claim0.epoch === binding.currentInstallationEpochId && claim0.epochStartedAt === iso(EPOCH_START_MS) && claim0.cursor === 1,
  { claim0 });

  // 2. Only one tailed source complete: still nothing; both: the earlier pass start.
  //    (The upload path finishes the legacy migration before it leases; so does this.)
  buffer.delivery.migrateLegacy({ now: new Date() });
  recordCompleteCapturePass(buffer.database, "codex", iso(EPOCH_START_MS + 3_600_000), later);
  const oneSource = buffer.delivery.captureClaim(outboxIds(buffer))!;
  bothPasses(buffer, EPOCH_START_MS + 3_600_000, EPOCH_START_MS + 2 * 3_600_000);
  const bothSources = buffer.delivery.captureClaim(outboxIds(buffer))!;
  check("claim_bounded_by_both_sources_capture_passes",
    oneSource.through === null && bothSources.through === iso(EPOCH_START_MS + 3_600_000) &&
      oneSource.cursor === 2 && bothSources.cursor === 3,
    { oneSource, bothSources });

  // 3. A queued delivery outside this request holds the claim at its observed time.
  const queued = event(EPOCH_START_MS + 10 * 60_000);
  buffer.append(queued);
  const outside = buffer.delivery.captureClaim([first.id])!;
  const inside = buffer.delivery.captureClaim([first.id, queued.id])!;
  check("queued_delivery_outside_request_holds_claim",
    outside.through === iso(EPOCH_START_MS + 10 * 60_000) && outside.pending === 1 &&
      inside.through === iso(EPOCH_START_MS + 3_600_000) && inside.pending === 0,
    { outside, inside });

  // 4. A dead delivery is a gap and holds the claim; replay clears the gap but still holds it.
  const lease = buffer.delivery.lease({ maxRows: 10, now: new Date() });
  buffer.delivery.deadLetterRemote(lease.leaseId, [queued.id]);
  buffer.delivery.retry(lease.leaseId, lease.items.filter((item) => item.deliveryId !== queued.id), "remote_transient", new Date());
  const deadClaim = buffer.delivery.captureClaim([first.id])!;
  check("dead_delivery_is_a_gap_and_holds_claim",
    deadClaim.dead === 1 && deadClaim.gapSince === iso(EPOCH_START_MS + 10 * 60_000) &&
      deadClaim.through === deadClaim.gapSince && deadClaim.pending === 0,
    { deadClaim });
  const replay = buffer.delivery.replayDeadLetters({ reason: "remote_validation_rejected" });
  const replayed = buffer.delivery.captureClaim([first.id])!;
  check("replayed_dead_delivery_clears_gap_but_holds_claim_until_delivered",
    replay.requeued === 1 && replayed.dead === 0 && replayed.gapSince === null && replayed.pending === 1 &&
      replayed.through === iso(EPOCH_START_MS + 10 * 60_000),
    { replay, replayed });

  // 5. Delivered rows stop holding it; the cursor never repeats.
  const all = buffer.delivery.lease({ maxRows: 10, now: new Date(Date.now() + 3_600_000 * 2) });
  const acknowledged = buffer.delivery.acknowledge(all.leaseId, all.items.map((item) => item.deliveryId));
  const drained = buffer.delivery.captureClaim([])!;
  const cursors = [claim0, oneSource, bothSources, outside, inside, deadClaim, replayed, drained].map((claim) => claim.cursor);
  check("acknowledged_deliveries_release_claim_and_cursor_strictly_increases",
    acknowledged.acknowledged === 2 && drained.pending === 0 && drained.dead === 0 &&
      drained.through === iso(EPOCH_START_MS + 3_600_000) &&
      cursors.every((cursor, index) => index === 0 || cursor > cursors[index - 1]!),
    { acknowledged, drained, cursors });

  // 6. The legacy migration has not enqueued every ledger row: nothing is attested.
  buffer.database.prepare(`update upload_control set migration_complete = 0 where singleton = 1`).run();
  const migrating = buffer.delivery.captureClaim([])!;
  buffer.database.prepare(`update upload_control set migration_complete = 1 where singleton = 1`).run();
  check("incomplete_legacy_migration_attests_nothing", migrating.through === null, { migrating });

  // 7. Capture frontier guards: incomplete passes never count; before-epoch and
  //    future passes are refused; the frontier never moves backwards.
  const clean = { aborted: false, bytesDeferred: 0, deferredGenerations: 0, parseErrors: 0, discoveryErrors: 0, statErrors: 0, readErrors: 0, activity: { truncated: false } };
  const guards = {
    clean: isCompleteCapturePass(clean),
    truncated: isCompleteCapturePass({ ...clean, activity: { truncated: true } }),
    aborted: isCompleteCapturePass({ ...clean, aborted: true }),
    deferred: isCompleteCapturePass({ ...clean, bytesDeferred: 1 }),
    readError: isCompleteCapturePass({ ...clean, readErrors: 1 }),
    parseError: isCompleteCapturePass({ ...clean, parseErrors: 1 }),
    beforeEpoch: recordCompleteCapturePass(buffer.database, "codex", iso(EPOCH_START_MS - 1), later),
    future: recordCompleteCapturePass(buffer.database, "codex", iso(later.getTime() + 1), later),
    backwards: recordCompleteCapturePass(buffer.database, "codex", iso(EPOCH_START_MS + 60_000), later),
  };
  const frontierAfter = captureFrontier(buffer.database);
  check("capture_frontier_counts_only_complete_passes_and_never_regresses",
    guards.clean && !guards.truncated && !guards.aborted && !guards.deferred && !guards.readError && !guards.parseError &&
      !guards.beforeEpoch && !guards.future && guards.backwards && frontierAfter?.capturedThrough === iso(EPOCH_START_MS + 3_600_000),
    { guards, frontierAfter });

  // 8. A new epoch (the ledger joins another workspace) starts from nothing and
  //    does not count the previous epoch's rows.
  const leftover = event(EPOCH_START_MS + 20 * 60_000);
  buffer.append(leftover);
  const newEpochId = crypto.randomUUID();
  enrollmentClock = REAL_NOW - 60_000;
  buffer.transitionWorkspace(TENANT, OTHER_TENANT, undefined, newEpochId);
  const fresh = buffer.delivery.captureClaim([])!;
  check("new_epoch_starts_unattested_and_ignores_previous_epoch_rows",
    fresh.epoch === newEpochId && fresh.epochStartedAt === iso(REAL_NOW - 60_000) && fresh.through === null &&
      fresh.pending === 0 && fresh.dead === 0,
    { fresh });
  buffer.close();

  // 9. On the wire: a signed header, and a body an older strict cloud accepts.
  enrollmentClock = EPOCH_START_MS;
  const wire = managedBuffer();
  const sent = event(EPOCH_START_MS + 30 * 60_000);
  wire.append(sent);
  bothPasses(wire, EPOCH_START_MS + 2 * 3_600_000, EPOCH_START_MS + 2 * 3_600_000);
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
  const uploaded = await uploadBufferedEvents(config, wire, { fetchImpl });
  const request = requests[0];
  const header = request?.headers.get("x-plimsoll-capture") ?? "";
  const timestamp = request?.headers.get("x-plimsoll-upload-timestamp") ?? "";
  const expectedSignature = `sha256=${crypto.createHmac("sha256", SIGNING_SECRET)
    .update(`plimsoll-capture-v1\n${timestamp}\n${header}\n${request?.body ?? ""}`).digest("hex")}`;
  const claim = header ? JSON.parse(header) as Record<string, unknown> : {};
  check("upload_carries_one_signed_capture_claim_header",
    uploaded.uploadedEvents === 1 && requests.length === 1 &&
      request!.headers.get("x-plimsoll-capture-signature") === expectedSignature &&
      claim.v === 1 && claim.through === iso(EPOCH_START_MS + 2 * 3_600_000) && claim.pending === 0 &&
      typeof claim.cursor === "number",
    { uploaded: uploaded.uploadedEvents, requests: requests.length, claim });
  const body = JSON.parse(request!.body) as Record<string, unknown>;
  const strict = aiWorkIngestBatchSchema.safeParse(body);
  check("batch_body_unchanged_so_a_strict_older_cloud_accepts_it",
    strict.success && JSON.stringify(Object.keys(body)) === JSON.stringify(["tenantId", "installKey", "appVersion", "events"]),
    { keys: Object.keys(body), strict: strict.success });
  // After acknowledgement the next claim is fully drained.
  const afterUpload = wire.delivery.captureClaim([])!;
  check("claim_after_acknowledged_upload_is_drained",
    afterUpload.pending === 0 && afterUpload.dead === 0 && afterUpload.through === iso(EPOCH_START_MS + 2 * 3_600_000) &&
      afterUpload.cursor === (claim.cursor as number) + 1,
    { afterUpload });
  wire.close();

  // 10. Over the queue's row budget the claim attests nothing and never reads
  //     the queued envelopes (a planted unreadable envelope would make that
  //     scan throw), so the upload path stays bounded however far behind it is.
  enrollmentClock = EPOCH_START_MS;
  const bounded = new LocalEventBuffer(ledger(), {
    workspaceId: TENANT,
    delivery: { enabled: true, limits: { maxOldestAgeDays: 3650, maxActiveRows: 1 } },
    enrollmentNow: () => new Date(enrollmentClock),
  });
  const kept = event(EPOCH_START_MS + 40 * 60_000);
  bounded.append(kept);
  bounded.delivery.migrateLegacy({ now: new Date() });
  bothPasses(bounded, EPOCH_START_MS + 2 * 3_600_000, EPOCH_START_MS + 2 * 3_600_000);
  const underBudget = bounded.delivery.captureClaim([kept.id])!;
  const overflow = event(EPOCH_START_MS + 50 * 60_000);
  bounded.append(overflow);
  bounded.database.prepare(`update upload_outbox set base_envelope_json = '{unreadable' where delivery_id = ?`).run(overflow.id);
  let plantedUnreadable = false;
  try {
    bounded.database.prepare(`select json_extract(base_envelope_json, '$.event.observedAt') from upload_outbox`).all();
  } catch {
    plantedUnreadable = true;
  }
  const overBudget = bounded.delivery.captureClaim([kept.id])!;
  check("over_row_budget_attests_nothing_without_reading_the_queue",
    plantedUnreadable && underBudget.through === iso(EPOCH_START_MS + 2 * 3_600_000) && underBudget.pending === 0 &&
      overBudget.through === null && overBudget.pending === 1 && overBudget.cursor === underBudget.cursor + 1,
    { plantedUnreadable, underBudget, overBudget });
  bounded.close();
  fs.rmSync(root, { recursive: true, force: true });
  completion.complete();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
