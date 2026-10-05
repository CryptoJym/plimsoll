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

const completion = createProofCompletion("collector-named-usage-rollback", 12);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-named-rollback-"));
const currentVersion: string = JSON.parse(fs.readFileSync(
  path.resolve("packages/collector-cli/package.json"), "utf8",
)).version;
const released049 = "275fce73c76f8d2a9898cbc52e8ddf5b7a128c9e";
const released048 = "34d58bcd90865679e09fcbd1ee1703de5effda97";
// Main's #454 proof also protected the original PR writer through the scanner
// head and 0.7.48/0.7.47. Retain that independent fixture alongside the new
// head-writer rollback to the two most recent released readers.
const legacyWriter = "9f75bdcdf6d19fd477caed838482bb0fe16c8d31";
const released047 = "a60590559403cace3db7cbbda49812c9e3dbfe62";
const headCommit = execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
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
  ledgerPath: string,
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

async function rollbackFixture(
  name: string,
  writer: { LocalEventBuffer: typeof LocalEventBuffer; captureCodexModel: typeof captureCodexModel },
  writerCommit: string,
  firstTree: string,
  firstVersion: string,
  secondTree: string,
  secondVersion: string,
) {
  const ledgerPath = path.join(root, `${name}.sqlite`);
  const check = (label: string) => completion.check(`${name}:${label}`);
  {
    let now = new Date(baseMs);
    const buffer = new writer.LocalEventBuffer(ledgerPath, {
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
      const localCapture = writer.captureCodexModel(buffer.database, named);
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
      check("new-reader-seals-named-usage-and-tokenless-gap");

      claim = buffer.delivery.captureClaim(
        lease.items.map((item) => item.deliveryId),
        { pendingFiles: 0, oldestPendingMs: null, losses: [], unreadable: false },
        now,
      );
      assert.ok(claim);
      assert.ok((claim as { gaps: unknown[] }).gaps.length >= 1,
        "the model-capture gap is present in the capture claim");
      check("new-reader-seals-gap-census-claim");
      sealedBefore = readSealed(buffer.database);
    } finally {
      buffer.close();
    }

    const current = await leaseWithReader(ledgerPath, path.resolve("."), currentVersion,
      baseMs + 182_000, false);
    assert.deepEqual(current.itemIds.sort(), [gapId, namedId].sort());
    const afterCurrent = new Database(ledgerPath, { readonly: true });
    try { assert.deepEqual(readSealed(afterCurrent), sealedBefore); } finally { afterCurrent.close(); }
    check("scanner-head-reader-preserves-exact-sealed-usage-and-gap");

    const first = await leaseWithReader(ledgerPath, firstTree, firstVersion, baseMs + 303_000, false);
    assert.deepEqual(first.itemIds.sort(), [gapId, namedId].sort());
    check(`rollback-reader-${firstVersion}-preserves-usage`);

    const afterFirst = new Database(ledgerPath, { readonly: true });
    try { assert.deepEqual(readSealed(afterFirst), sealedBefore); } finally { afterFirst.close(); }

    const second = await leaseWithReader(ledgerPath, secondTree, secondVersion, baseMs + 424_000, true);
    assert.deepEqual(second.itemIds.sort(), [gapId, namedId].sort());
    check(`rollback-reader-${secondVersion}-preserves-usage`);

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
    check("rollback-outbox-receipt-state-is-consistent");
    console.log(JSON.stringify({
      proof: "collector-named-usage-rollback",
      fixture: name,
      writerCommit,
      readers: [`scanner-head-${currentVersion}`, firstVersion, secondVersion],
      claim,
      frozenEnvelopes: sealedBefore.length,
    }));
  }
}

async function main() {
  const trees: string[] = [];
  function checkout(label: string, commit: string) {
    const tree = path.join(root, label);
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", tree, commit]);
    trees.push(tree);
    fs.symlinkSync(path.resolve("node_modules"), path.join(tree, "node_modules"), "dir");
    return tree;
  }
  try {
    assert.equal(currentVersion, "0.7.49", "the release-owned package version is unchanged");
    const old049 = checkout("collector-0.7.49", released049);
    const old048 = checkout("collector-0.7.48", released048);
    const old047 = checkout("collector-0.7.47", released047);
    const legacyTree = checkout("legacy-pr450-writer", legacyWriter);
    const legacyBuffer = await import(pathToFileURL(
      path.join(legacyTree, "packages/collector-cli/src/buffer.ts"),
    ).href);
    const legacyCapture = await import(pathToFileURL(
      path.join(legacyTree, "packages/collector-cli/src/codex-model-capture.ts"),
    ).href);
    await rollbackFixture("head-writer-released-readers", { LocalEventBuffer, captureCodexModel },
      headCommit, old049, "0.7.49", old048, "0.7.48");
    await rollbackFixture("legacy-writer-scanner-reader", {
      LocalEventBuffer: legacyBuffer.LocalEventBuffer,
      captureCodexModel: legacyCapture.captureCodexModel,
    }, legacyWriter, old048, "0.7.48", old047, "0.7.47");
    completion.complete();
  } finally {
    for (const tree of trees.reverse()) closeQuietly(tree);
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
