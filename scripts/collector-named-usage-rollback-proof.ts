import assert from "node:assert/strict";
import Database from "better-sqlite3";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureCodexModel } from "../packages/collector-cli/src/codex-model-capture";
import { createProofCompletion } from "./lib/proof-completion";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const completion = createProofCompletion("collector-named-usage-rollback", 5);
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
) {
  const reader = await import(pathToFileURL(
    path.join(worktree, "packages/collector-cli/src/buffer.ts"),
  ).href);
  const now = new Date(nowMs);
  const buffer = new reader.LocalEventBuffer(ledgerPath, {
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

async function main() {
  let old048: string | undefined;
  let old047: string | undefined;
  try {
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
        metadata: { otelEventName: "codex.sse_event", traceId, "user.account_id": accountId },
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
      const namedWire = lease.items.find((item) => item.deliveryId === namedId)?.envelope.event;
      const gapWire = lease.items.find((item) => item.deliveryId === gapId)?.envelope.event;
      assert.equal(namedWire?.model, "gpt-6.1-sol");
      assert.equal(namedWire?.inputTokens, 19);
      assert.equal(namedWire?.metadata.modelCaptureSource, undefined);
      assert.equal(gapWire?.metadata.usageSource, "capture_gap");
      assert.equal(gapWire?.inputTokens, undefined);
      completion.check("new-reader-seals-named-usage-and-tokenless-gap");

      claim = buffer.delivery.captureClaim(
        lease.items.map((item) => item.deliveryId),
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
    old048 = path.join(oldRoot, "collector-0.7.48");
    old047 = path.join(oldRoot, "collector-0.7.47");
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", old048,
      "34d58bcd90865679e09fcbd1ee1703de5effda97"]);
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", old047,
      "a60590559403cace3db7cbbda49812c9e3dbfe62"]);
    for (const worktree of [old048, old047])
      fs.symlinkSync(path.resolve("node_modules"), path.join(worktree, "node_modules"), "dir");

    const first = await leaseWithReader(old048, "0.7.48", baseMs + 182_000, false);
    assert.deepEqual(first.itemIds.sort(), [gapId, namedId].sort());
    completion.check("rollback-reader-0.7.48-preserves-usage");

    const after048 = new Database(ledgerPath, { readonly: true });
    try { assert.deepEqual(readSealed(after048), sealedBefore); } finally { after048.close(); }

    const second = await leaseWithReader(old047, "0.7.47", baseMs + 303_000, true);
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
    console.log(JSON.stringify({
      proof: "collector-named-usage-rollback",
      readers: ["0.7.48", "0.7.47"],
      claim,
      frozenEnvelopes: sealedBefore.length,
    }));
    completion.complete();
  } finally {
    if (old048) closeQuietly(old048);
    if (old047) closeQuietly(old047);
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
