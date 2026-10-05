import assert from "node:assert/strict";
import Database from "better-sqlite3";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { createProofCompletion } from "./lib/proof-completion";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";

const completion = createProofCompletion("collector-named-usage-rollback", 13);
// Commit A is a reader-only tier release. Commit B enables tier capture and
// changes this proof's previous reader to the exact commit A, not stock 0.7.49.
const recordedTierWriter = false;
const previousReaderCommit = "275fce73c76f8d2a9898cbc52e8ddf5b7a128c9e";
const previousReaderLabel = "stock-0.7.49";
// Preserve PR #450's exact writer and all five rollback assertions while
// testing this scanner branch as an additional reader. No #450 runtime changes
// or release bump are required in the scanner branch.
const currentVersion: string = JSON.parse(fs.readFileSync(
  path.resolve("packages/collector-cli/package.json"), "utf8",
)).version;
const writerCommit = "9f75bdcdf6d19fd477caed838482bb0fe16c8d31";
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-named-rollback-"));
const ledgerPath = path.join(root, "rollback.sqlite");
const workspaceId = "00000000-0000-4000-8000-000000000001";
const deviceId = "rollback-fixture-device";
const accountId = "sha256:0123456789abcdef";
const namedId = "00000000-0000-4000-8000-000000000901";
const gapId = "00000000-0000-4000-8000-000000000902";
const baseMs = Date.parse("2026-10-03T00:00:00.000Z");

const closeQuietly = (file: string) => {
  try { execFileSync("git", ["worktree", "remove", "--force", file], { stdio: "ignore" }); } catch { /* cleanup is best effort */ }
};

function readSealed(db: Database.Database) {
  return (db.prepare(
    "select delivery_id as id, sealed_envelope_json as sealed from upload_outbox order by delivery_id",
  ).all() as Array<{ id: string; sealed: string | null }>).map((row) => ({
    id: row.id,
    sealed: row.sealed,
  }));
}

async function leaseWithReader(
  worktree: string,
  expectedVersion: string,
  nowMs: number,
  acknowledge: boolean,
  fixtureLedgerPath = ledgerPath,
) {
  const reader = await import(pathToFileURL(
    path.join(worktree, "packages/collector-cli/src/buffer.ts"),
  ).href);
  const now = new Date(nowMs);
  const buffer = new reader.LocalEventBuffer(fixtureLedgerPath, {
    workspaceId,
    deviceId,
    enrollmentNow: () => new Date(baseMs),
    delivery: { enabled: true, now: () => now },
  });
  try {
    const version = JSON.parse(fs.readFileSync(
      path.join(worktree, "packages/collector-cli/package.json"), "utf8",
    )).version;
    assert.equal(version, expectedVersion);
    const lease = buffer.delivery.lease({ now });
    assert.equal(lease.locallyDead, 0, `${expectedVersion} did not retire usage`);
    assert.equal(lease.items.length, 2, `${expectedVersion} read both sealed deliveries`);
    const named = lease.items.find((item: any) => item.deliveryId === namedId)?.envelope.event;
    const gap = lease.items.find((item: any) => item.deliveryId === gapId)?.envelope.event;
    assert.equal(named?.model, "gpt-6.1-sol");
    assert.equal(named?.inputTokens, 19);
    assert.equal(named?.outputTokens, 2);
    assert.equal(gap?.model, undefined);
    assert.equal(gap?.inputTokens, undefined);
    assert.equal(gap?.outputTokens, undefined);
    assert.equal(gap?.metadata.usageSource, "capture_gap");
    if (acknowledge) {
      const acknowledged = buffer.delivery.acknowledge(
        lease.leaseId,
        lease.items.map((item: any) => item.deliveryId),
        now,
      );
      assert.equal(acknowledged.locallyDead, 0);
      assert.equal(acknowledged.acknowledged, 2);
    }
    return {
      leaseId: lease.leaseId,
      itemIds: lease.items.map((item: any) => item.deliveryId),
      namedModel: named?.model,
      namedInput: named?.inputTokens,
      gapInput: gap?.inputTokens,
    };
  } finally {
    buffer.close();
  }
}

async function branchWriterStockReader(stockTree: string) {
  const { LocalEventBuffer } = await import("../packages/collector-cli/src/buffer");
  const branchLedger = path.join(root, "branch-writer.sqlite");
  const branchStart = baseMs + 2_000_000;
  const now = new Date(branchStart);
  const buffer = new LocalEventBuffer(branchLedger, {
    workspaceId, deviceId, enrollmentNow: () => now,
    delivery: { enabled: true, now: () => now },
  });
  try {
    assert.equal(buffer.append(aiInteractionEventSchema.parse({
      id: namedId, sessionId: "00000000-0000-4000-8000-000000000911",
      actorId: accountId, source: "codex", dataMode: "metadata",
      eventType: "assistant_response", observedAt: now.toISOString(),
      model: "gpt-6.1-sol", inputTokens: 19, outputTokens: 2,
    })), true);
    assert.equal(buffer.append(aiInteractionEventSchema.parse({
      id: gapId, sessionId: "00000000-0000-4000-8000-000000000912",
      source: "codex", dataMode: "metadata", eventType: "assistant_response",
      observedAt: now.toISOString(), metadata: { usageSource: "capture_gap" },
    })), true);
  } finally { buffer.close(); }
  const ownLease = await leaseWithReader(path.resolve("."), currentVersion,
    branchStart + 1_000, false, branchLedger);
  const before = new Database(branchLedger, { readonly: true });
  let sealed: ReturnType<typeof readSealed>;
  try { sealed = readSealed(before); } finally { before.close(); }
  const stockLease = await leaseWithReader(stockTree, "0.7.49",
    branchStart + 122_000, true, branchLedger);
  assert.notEqual(stockLease.leaseId, ownLease.leaseId);
  assert.deepEqual(stockLease.itemIds.sort(), ownLease.itemIds.sort());
  const after = new Database(branchLedger, { readonly: true });
  try {
    const counts = after.prepare(`select
      (select count(*) from upload_outbox) as outbox,
      (select count(*) from upload_receipts where terminal_state = 'dead') as dead,
      (select count(*) from buffered_events where uploaded_at is not null) as uploaded`).get();
    assert.deepEqual(counts, { outbox: 0, dead: 0, uploaded: 2 });
    assert.equal(sealed.length, 2);
    assert.equal((after.prepare("select input_tokens as input from buffered_events where id = ?")
      .get(namedId) as { input: number }).input, 19);
  } finally { after.close(); }
  completion.check("branch-writer-to-stock-0.7.49-expired-lease-preserves-all-deliveries");
  console.log(JSON.stringify({ branchWriterCommit: execFileSync("git", ["rev-parse", "HEAD"],
    { encoding: "utf8" }).trim(), stockReaderCommit: "275fce73c76f8d2a9898cbc52e8ddf5b7a128c9e",
    ownLeaseAtMs: branchStart + 1_000, stockLeaseAtMs: branchStart + 122_000, namedInput: 19,
    preservedDeliveries: stockLease.itemIds.length, dead: 0 }));
}

async function main() {
  let writerTree: string | undefined;
  let old049: string | undefined;
  let old048: string | undefined;
  let old047: string | undefined;
  try {
    try { execFileSync("git", ["cat-file", "-e", `${writerCommit}^{commit}`], { stdio: "ignore" }); }
    catch {
      execFileSync("git", ["-c", "credential.helper=", "fetch", "--no-tags",
        "https://github.com/CryptoJym/plimsoll.git", writerCommit], { stdio: "ignore", timeout: 60_000 });
    }
    writerTree = path.join(root, "collector-pr450-writer");
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", writerTree, writerCommit]);
    fs.symlinkSync(path.resolve("node_modules"), path.join(writerTree, "node_modules"), "dir");
    const { LocalEventBuffer } = await import(pathToFileURL(
      path.join(writerTree, "packages/collector-cli/src/buffer.ts"),
    ).href);
    const { captureCodexModel } = await import(pathToFileURL(
      path.join(writerTree, "packages/collector-cli/src/codex-model-capture.ts"),
    ).href);
    let now = new Date(baseMs);
    const buffer = new LocalEventBuffer(ledgerPath, {
      workspaceId,
      deviceId,
      enrollmentNow: () => new Date(baseMs),
      delivery: { enabled: true, now: () => now },
    });
    let claim: unknown;
    let sealedBefore: Array<{ id: string; sealed: string | null }>;
    try {
      const traceId = "c".repeat(32);
      const evidence = aiInteractionEventSchema.parse({
        id: "00000000-0000-4000-8000-000000000903",
        sessionId: "00000000-0000-4000-8000-000000000911",
        actorId: accountId,
        source: "codex", dataMode: "metadata", eventType: "otel_span",
        observedAt: new Date(baseMs).toISOString(), model: "gpt-6.1-sol",
        metadata: {
          otelEventName: "codex.sse_event", traceId, "user.account_id": accountId,
          "gen_ai.request.model": "gpt-6.1-sol",
        },
      });
      assert.equal(buffer.append(evidence), true);
      const evidenceLease = buffer.delivery.lease({ now });
      assert.equal(evidenceLease.items.length, 1);
      buffer.delivery.acknowledge(evidenceLease.leaseId, [evidence.id], now);
      const named = aiInteractionEventSchema.parse({
        id: namedId,
        sessionId: "00000000-0000-4000-8000-000000000911",
        actorId: accountId,
        source: "codex",
        dataMode: "metadata",
        eventType: "assistant_response",
        observedAt: new Date(baseMs).toISOString(),
        inputTokens: 19,
        outputTokens: 2,
        metadata: { traceId, "user.account_id": accountId },
      });
      const gap = aiInteractionEventSchema.parse({
        id: gapId,
        sessionId: "00000000-0000-4000-8000-000000000912",
        source: "codex",
        dataMode: "metadata",
        eventType: "assistant_response",
        observedAt: new Date(baseMs + 1_000).toISOString(),
        inputTokens: 7,
        outputTokens: 1,
        metadata: { otelEventName: "handle_responses" },
      });
      assert.equal(buffer.append(named), true);
      assert.equal(buffer.append(gap), true);

      now = new Date(baseMs + 61_000);
      const localCapture = captureCodexModel(buffer.database, named);
      assert.equal(localCapture.metadata.modelCaptureSource, "unique_trace_sse_event");
      const lease = buffer.delivery.lease({ now });
      assert.equal(lease.locallyDead, 0);
      assert.equal(lease.items.length, 2);
      const namedWire = lease.items.find((item: any) => item.deliveryId === namedId)?.envelope.event;
      const gapWire = lease.items.find((item: any) => item.deliveryId === gapId)?.envelope.event;
      assert.equal(namedWire?.model, "gpt-6.1-sol");
      assert.equal(namedWire?.inputTokens, 19);
      assert.equal(namedWire?.metadata.modelCaptureSource, undefined);
      assert.equal(gapWire?.metadata.usageSource, "capture_gap");
      assert.equal(gapWire?.inputTokens, undefined);
      completion.check("new-reader-seals-named-usage-and-tokenless-gap");

      claim = buffer.delivery.captureClaim(
        lease.items.map((item: any) => item.deliveryId),
        { pendingFiles: 0, oldestPendingMs: null, losses: [], unreadable: false },
        now,
      );
      assert.ok(claim);
      assert.ok((claim as { gaps: unknown[] }).gaps.length >= 1,
        "the model-capture gap is present in the capture claim");
      completion.check("new-reader-seals-gap-census-claim");
      sealedBefore = readSealed(buffer.database);
    } finally {
      buffer.close();
    }

    const oldRoot = path.join(root, "readers");
    fs.mkdirSync(oldRoot, { recursive: true });
    old049 = path.join(oldRoot, "collector-0.7.49");
    old048 = path.join(oldRoot, "collector-0.7.48");
    old047 = path.join(oldRoot, "collector-0.7.47");
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", old049,
      "275fce73c76f8d2a9898cbc52e8ddf5b7a128c9e"]);
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", old048,
      "34d58bcd90865679e09fcbd1ee1703de5effda97"]);
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", old047,
      "a60590559403cace3db7cbbda49812c9e3dbfe62"]);
    for (const worktree of [old049, old048, old047])
      fs.symlinkSync(path.resolve("node_modules"), path.join(worktree, "node_modules"), "dir");

    const current = await leaseWithReader(path.resolve("."), currentVersion, baseMs + 182_000, false);
    assert.deepEqual(current.itemIds.sort(), [gapId, namedId].sort());
    const afterCurrent = new Database(ledgerPath, { readonly: true });
    try { assert.deepEqual(readSealed(afterCurrent), sealedBefore); } finally { afterCurrent.close(); }
    completion.check("scanner-head-reader-preserves-exact-sealed-usage-and-gap");

    const stock = await leaseWithReader(old049, "0.7.49", baseMs + 303_000, false);
    assert.deepEqual(stock.itemIds.sort(), [gapId, namedId].sort());
    const after049 = new Database(ledgerPath, { readonly: true });
    try { assert.deepEqual(readSealed(after049), sealedBefore); } finally { after049.close(); }
    completion.check("rollback-reader-0.7.49-preserves-usage-after-lease-expiry");

    const first = await leaseWithReader(old048, "0.7.48", baseMs + 424_000, false);
    assert.deepEqual(first.itemIds.sort(), [gapId, namedId].sort());
    completion.check("rollback-reader-0.7.48-preserves-usage");

    const after048 = new Database(ledgerPath, { readonly: true });
    try { assert.deepEqual(readSealed(after048), sealedBefore); } finally { after048.close(); }

    const second = await leaseWithReader(old047, "0.7.47", baseMs + 545_000, true);
    assert.deepEqual(second.itemIds.sort(), [gapId, namedId].sort());
    completion.check("rollback-reader-0.7.47-preserves-usage");

    const finalDb = new Database(ledgerPath, { readonly: true });
    try {
      const counts = finalDb.prepare(`select
        (select count(*) from upload_outbox) as outbox,
        (select count(*) from upload_receipts where terminal_state='dead') as dead,
        (select count(*) from upload_receipts where reason='local_model_capture_gap') as captureGapDead,
        (select count(*) from buffered_events where uploaded_at is not null and id in (?,?)) as uploaded`).get(
        namedId, gapId,
      ) as { outbox: number; dead: number; captureGapDead: number; uploaded: number };
      assert.deepEqual(counts, { outbox: 0, dead: 0, captureGapDead: 0, uploaded: 2 });
      const raw = finalDb.prepare(
        "select id, input_tokens as input from buffered_events where id in (?,?) order by id",
      ).all(namedId, gapId) as Array<{ id: string; input: number | null }>;
      assert.deepEqual(raw, [{ id: namedId, input: 19 }, { id: gapId, input: 7 }]);
    } finally { finalDb.close(); }
    completion.check("rollback-outbox-receipt-state-is-consistent");

    // Retain every legacy PR #450 assertion, then use this branch's actual
    // OTLP writer. A -> stock and B -> A are distinct staged rollback gates.
    assert.equal(JSON.parse(fs.readFileSync(path.join(old049, "packages/collector-cli/package.json"), "utf8")).version, "0.7.49");
    const currentReader = await import(pathToFileURL(path.resolve("packages/collector-cli/src/buffer.ts")).href);
    const tierIds = ["00000000-0000-4000-8000-000000000921", "00000000-0000-4000-8000-000000000922",
      "00000000-0000-4000-8000-000000000923"];
    const tierStart = baseMs + 1_000_000;
    let tierNow = new Date(tierStart);
    const tierWriter = new currentReader.LocalEventBuffer(ledgerPath, { workspaceId, deviceId,
      enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => tierNow } });
    let tierSealed: Array<{ id: string; sealed: string | null }>;
    let tierClaim: { dead: number; cursor: number };
    try {
      for (const [i, id] of tierIds.entries()) {
        const fields = { "event.name": "codex.sse_event", "event.kind": "response.completed",
          "conversation.id": id, "user.account_id": accountId, model: "gpt-6.1-sol",
          input_token_count: "19", output_token_count: "2",
          ...(i === 2 ? {} : { cached_token_count: i === 0 ? 0 : 7,
            service_tier: i === 0 ? "fast" : "flex" }) };
        const parsed = explodeOtlpPayload({ resourceLogs: [{ resource: { attributes: [
          { key: "service.name", value: { stringValue: "codex-app-server" } } ] },
          scopeLogs: [{ logRecords: [{ timeUnixNano: String(BigInt(tierStart) * 1_000_000n),
            attributes: Object.entries(fields).map(([key, value]) => ({ key,
              value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value } }))
          }] }] }] }, { source: "codex", transportPath: "/v1/logs" });
        assert.equal(parsed.parseFailures, 0);
        assert.equal(parsed.events.length, 1);
        const event = aiInteractionEventSchema.parse({ ...parsed.events[0].event, id });
        assert.equal(event.metadata.serviceTier, recordedTierWriter && i !== 2 ? i === 0 ? "priority" : "flex" : undefined);
        assert.equal(event.metadata.service_tier, recordedTierWriter && i !== 2 ? i === 0 ? "fast" : "flex" : undefined);
        assert.equal(tierWriter.append(event), true);
      }
      tierNow = new Date(tierStart + 61_000);
      const lease = tierWriter.delivery.lease({ now: tierNow });
      assert.equal(lease.locallyDead, 0);
      assert.deepEqual(lease.items.map((item: any) => item.deliveryId).sort(), tierIds);
      assert.equal(lease.items.find((item: any) => item.deliveryId === tierIds[0]).envelope.event.metadata.serviceTier,
        recordedTierWriter ? "priority" : undefined);
      completion.check(recordedTierWriter ? "branch-writer-seals-zero-and-recorded-processing-tier" : "reader-first-writer-seals-zero-with-no-tier-key");
      tierClaim = tierWriter.delivery.captureClaim(tierIds, { pendingFiles: 0, oldestPendingMs: null, losses: [], unreadable: false }, tierNow);
      assert.ok(tierClaim);
      assert.equal(tierClaim.dead, 0);
      tierSealed = readSealed(tierWriter.database);
      completion.check("branch-writer-seals-capture-claim-with-no-dead-usage");
    } finally { tierWriter.close(); }
    const previous = await import(pathToFileURL(path.join(old049, "packages/collector-cli/src/buffer.ts")).href);
    tierNow = new Date(tierStart + 182_000); // the branch writer's lease has expired
    const previousReader = new previous.LocalEventBuffer(ledgerPath, { workspaceId, deviceId,
      enrollmentNow: () => new Date(baseMs), delivery: { enabled: true, now: () => tierNow } });
    try {
      const lease = previousReader.delivery.lease({ now: tierNow });
      assert.equal(lease.locallyDead, 0);
      assert.deepEqual(lease.items.map((item: any) => item.deliveryId).sort(), tierIds);
      for (const [i, id] of tierIds.entries()) {
        const event = lease.items.find((item: any) => item.deliveryId === id).envelope.event;
        assert.equal(event.model, "gpt-6.1-sol");
        assert.equal(event.inputTokens, 19);
        assert.equal(event.outputTokens, 2);
        assert.equal(event.cacheReadTokens, i === 2 ? undefined : i === 0 ? 0 : 7);
        assert.equal(event.metadata.serviceTier, recordedTierWriter && i !== 2 ? i === 0 ? "priority" : "flex" : undefined);
        assert.equal(event.metadata.service_tier, recordedTierWriter && i !== 2 ? i === 0 ? "fast" : "flex" : undefined);
      }
      completion.check(`${previousReaderLabel}-preserves-exact-metadata-and-unknown-count`);
      assert.deepEqual(readSealed(previousReader.database), tierSealed);
      const oldClaim = previousReader.delivery.captureClaim(tierIds,
        { pendingFiles: 0, oldestPendingMs: null, losses: [], unreadable: false }, tierNow);
      assert.equal(oldClaim.dead, 0);
      assert.ok(oldClaim.cursor > tierClaim.cursor);
      completion.check(`${previousReaderLabel}-keeps-frozen-envelopes-and-continues-claims`);
      const acknowledged = previousReader.delivery.acknowledge(lease.leaseId, tierIds, tierNow);
      assert.equal(acknowledged.locallyDead, 0);
      assert.equal(acknowledged.acknowledged, 3);
      assert.equal((previousReader.database.prepare("select count(*) as n from upload_receipts where terminal_state='dead'").get() as { n: number }).n, 0);
      assert.equal((previousReader.database.prepare("select count(*) as n from buffered_events where uploaded_at is not null and id in (?,?,?)").get(...tierIds) as { n: number }).n, 3);
      completion.check(`${previousReaderLabel}-delivers-all-new-usage-with-zero-retirements`);
    } finally { previousReader.close(); }
    await branchWriterStockReader(old049);
    console.log(JSON.stringify({
      proof: "collector-named-usage-rollback",
      writerCommit,
      readers: [`scanner-head-${currentVersion}`, "stock-0.7.49", "main-0.7.48", "0.7.47"],
      stagedRollback: { writerCommit: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
        previousReaderCommit, previousReaderLabel, recordedTierWriter, sameLedger: true, leaseExpired: true,
        locallyDead: 0, acknowledged: 3 },
      claim,
      tierClaim,
      frozenEnvelopes: sealedBefore.length,
    }));
    completion.complete();
  } finally {
    if (writerTree) closeQuietly(writerTree);
    if (old049) closeQuietly(old049);
    if (old048) closeQuietly(old048);
    if (old047) closeQuietly(old047);
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
