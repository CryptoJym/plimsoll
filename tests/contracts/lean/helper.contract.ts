/**
 * Guard for the test helper (NOT pending; green today). Round 2 of B0 (review-r1 blocker 3): `event()` stamps a fixed 2026-09-25
 * `observedAt` while a managed buffer refuses anything observed before its enrollment epoch, which the shipped constructor starts
 * at open time; eight pending tests therefore appended nothing and could never pass. `openTempBuffer` now pins the epoch at
 * EPOCH_STARTED_AT. This file proves that every buffer configuration the pending tests use admits the fixture events, and that
 * the trap is real (a buffer without the pinned epoch refuses the same event as `before_enrollment`).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { LocalEventBuffer } from "../../../packages/collector-cli/src/buffer";
import { EPOCH_STARTED_AT, EVENT_OBSERVED_AT, event, openTempBuffer } from "./_pending";

type Probe = { eventAdmissionReason(observedAt: unknown): string | null };
const rows = (buffer: { database: { prepare(sql: string): { get(): unknown } } }) =>
  (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;

/** Every `openTempBuffer` configuration the pending tests use (actor-stamp, membership, retention-hold, receipts-and-ladder, capture-gaps, converter). */
const CONFIGURATIONS: Array<[string, Record<string, unknown>]> = [
  ["retention-hold / receipts-and-ladder: workspace + delivery", { workspaceId: "tenant-lean-contract", delivery: { enabled: true } }],
  ["actor-stamp / membership: workspace + lean.write", { workspaceId: "tenant-lean-contract", lean: { write: true } }],
  ["actor-stamp test 3: workspace + deviceId + lean.write", { workspaceId: "lean-contract", deviceId: "lean-device", lean: { write: true } }],
  ["capture-gaps: workspace only", { workspaceId: "tenant-lean-contract" }],
  ["converter: workspace + lean.write off", { workspaceId: "tenant-lean-contract", lean: { write: false } }],
  ["schema: no workspace (LOCAL tenant)", {}],
];

for (const [name, options] of CONFIGURATIONS) {
  test(`helper guard: ${name}: the fixture event is admitted (append true, rows > 0)`, () => {
    const { buffer, close } = openTempBuffer(options as never);
    try {
      const e = event();
      assert.equal(e.observedAt, EVENT_OBSERVED_AT);
      assert.ok(Date.parse(e.observedAt) > Date.parse(EPOCH_STARTED_AT), "observedAt is after the pinned epoch");
      assert.equal((buffer as unknown as Probe).eventAdmissionReason(e.observedAt), null, "no admission reason");
      assert.equal(buffer.append(e), true, "append admits the event");
      assert.equal(rows(buffer), 1, "the row is stored");
      const binding = buffer.workspaceBinding();
      if (options.workspaceId) assert.equal(binding?.currentInstallationEpochStartedAt, EPOCH_STARTED_AT, "the epoch is pinned");
    } finally { close(); }
  });
}

test("helper guard: an explicit observedAt at the epoch start (the ladder test's old rows) is admitted too", () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", delivery: { enabled: true } });
  try {
    const old = event({ observedAt: EPOCH_STARTED_AT });
    assert.equal(buffer.append(old), true);
    assert.equal(rows(buffer), 1);
  } finally { close(); }
});

test("helper guard: the trap is real: without the pinned epoch a managed buffer refuses the fixed observedAt as before_enrollment", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-lean-contract-trap-"));
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), { workspaceId: "tenant-lean-contract" });
  try {
    const e = event();
    assert.equal((buffer as unknown as Probe).eventAdmissionReason(e.observedAt), "before_enrollment");
    assert.equal(buffer.append(e), false);
    assert.equal(rows(buffer), 0);
  } finally {
    try { buffer.close(); } catch { /* closed */ }
    fs.rmSync(root, { recursive: true, force: true });
  }
});
