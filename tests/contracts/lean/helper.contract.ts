/**
 * Guard for the test helper (NOT pending; green today). Round 2 of B0 (review-r1 blocker 3): `event()` stamps a fixed 2026-09-25
 * `observedAt` while a managed buffer refuses anything observed before its enrollment epoch, which the shipped constructor starts
 * at open time; eight pending tests therefore appended nothing and could never pass. `openTempBuffer` now pins the epoch at
 * EPOCH_STARTED_AT. This file proves that every buffer configuration the pending tests use admits the fixture events, and that
 * the trap is real (a buffer without the pinned epoch refuses the same event as `before_enrollment`). Round 3 of B0 (review-r2
 * blocker 3): two more guards prove the premises the pending tests assume AFTER appending: the ladder test's old rows keep their
 * outbox lineage under the mocked clock (all four lease, three acknowledge), and the retention test's reject row, appended through
 * the buffer, is leased and acknowledged and would be deleted by today's prune at age (its red half). Round 4 of B0 (round 10): one
 * more guard proves the premise of actor-stamp test 8, which drives join.ts activation with a fake cloud: on today's code the join
 * redeems the token, runs its one-event handshake and activates the config and the active ledger, so the pending test can fail only at
 * the B2a surfaces it binds (joined_install, the pair, the join request's previousInstall proof), never at its set-up.
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

test("helper guard: the ladder test's premise (receipts-and-ladder 2): three rows appended under a Date mocked at the epoch start are old by created_at with a matching outbox lineage, so the lease sees all four and acknowledge acknowledges exactly three", (t) => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", delivery: { enabled: true } });
  try {
    const db = buffer.database;
    t.mock.timers.enable({ apis: ["Date"], now: new Date(EPOCH_STARTED_AT) });
    const old = [event({ observedAt: EPOCH_STARTED_AT }), event({ observedAt: EPOCH_STARTED_AT }), event({ observedAt: EPOCH_STARTED_AT })];
    for (const e of old) assert.equal(buffer.append(e), true);
    t.mock.timers.reset();
    const recent = event();
    assert.equal(buffer.append(recent), true);
    const lineage = db.prepare("select e.id as id, e.created_at as createdAt, o.raw_created_at as rawCreatedAt from buffered_events e join upload_outbox o on o.raw_id = e.id order by e.rowid").all() as Array<{ id: string; createdAt: string; rawCreatedAt: string }>;
    assert.equal(lineage.length, 4, "every appended row has an outbox row");
    assert.deepEqual(lineage.slice(0, 3).map((r) => [r.createdAt, r.rawCreatedAt]), old.map(() => [EPOCH_STARTED_AT, EPOCH_STARTED_AT]), "the old rows are old by created_at and the outbox lineage agrees");
    const lease = buffer.delivery.lease({ leaseId: "lean-contract-lease", now: new Date() });
    assert.deepEqual([lease.items.length, lease.locallyDead], [4, 0], "all four lease; none is dead-lettered as a lineage violation");
    const ack = buffer.delivery.acknowledge(lease.leaseId, [old[0].id, old[1].id, recent.id], new Date());
    assert.deepEqual([ack.acknowledged, ack.markedUploaded], [3, 3]);
    assert.equal((db.prepare("select count(*) as n from upload_receipts where terminal_state = 'acknowledged'").get() as { n: number }).n, 3);
    assert.equal((db.prepare("select count(*) as n from buffered_events where uploaded_at is not null").get() as { n: number }).n, 3);
  } finally { close(); }
});

test("helper guard: the retention test's premise (conversion-rejects 2): a raw row appended through the buffer is leased and acknowledged (uploaded_at set, an acknowledged receipt for its own id), and today's prune deletes it at age", () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", delivery: { enabled: true }, lean: { write: false } } as never);
  try {
    const db = buffer.database;
    const x1 = event({ eventType: "usage_rollout", observedAt: "2026-09-26T00:00:00.000Z" });
    assert.equal(buffer.append(x1), true);
    const lease = buffer.delivery.lease({ leaseId: "lean-contract-lease", now: new Date() });
    assert.equal(lease.items.length, 1);
    assert.equal(buffer.delivery.acknowledge(lease.leaseId, [x1.id], new Date()).acknowledged, 1);
    const row = db.prepare("select uploaded_at as uploadedAt from buffered_events where id = ?").get(x1.id) as { uploadedAt: string | null };
    assert.ok(row.uploadedAt, "uploaded_at is set");
    const receipt = db.prepare("select terminal_state as state from upload_receipts where delivery_id = ?").get(x1.id) as { state: string } | undefined;
    assert.equal(receipt?.state, "acknowledged", "an acknowledged receipt exists for the row's own id (the ladder's acknowledged-only rule)");
    assert.equal((buffer.prune(0, { maxRows: 100, now: new Date("2036-01-01T00:00:00.000Z") }) as { events: number }).events, 1, "today's prune deletes it at age: the red half the retention rule must refuse while a reject is open");
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

test("helper guard (round 4): join.ts activation completes on today's code with a fake cloud (a grant for install Z, a handshake answered with accepted: 1), leaving an active ledger bound to the tenant and the activated config", async () => {
  const [join, config] = await Promise.all([import("../../../packages/collector-cli/src/join"), import("../../../packages/collector-cli/src/config")]);
  const { acknowledgingFetch } = await import("../../../scripts/fixtures/delivery-ack-fixture");
  const homeDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-lean-join-guard-"));
  const previousPlimsollHome = process.env.PLIMSOLL_HOME;
  process.env.PLIMSOLL_HOME = path.join(homeDir, ".plimsoll");
  try {
    const INSTALL_X = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa001", INSTALL_Z = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa002";
    const old = config.collectorConfigSchema.parse({ tenantId: "tenant-lean-contract", installKey: "pli_previous_install_key_x", cloudDeviceId: INSTALL_X, uploadUrl: "https://cloud.example/api/work-intelligence/ingest", managed: true, port: 49123 });
    const configPath = config.collectorConfigPath(homeDir);
    fs.mkdirSync(path.dirname(configPath), { recursive: true, mode: 0o700 });
    fs.writeFileSync(configPath, `${JSON.stringify(old, null, 2)}\n`, { mode: 0o600 });
    const requests: string[] = [];
    const cloud = acknowledgingFetch((async (input, init) => {
      const url = typeof input === "string" ? new URL(input) : input instanceof URL ? input : new URL(input.url);
      requests.push(url.pathname);
      const json = (body: unknown, status: number) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
      if (url.pathname.endsWith("/join")) return json({ ok: true, tenantId: "tenant-lean-contract", deviceId: INSTALL_Z, installKey: "pli_new_install_key_z", uploadUrl: "https://cloud.example/api/work-intelligence/ingest" }, 201);
      void init;
      return json({ ok: true, accepted: 1, deviceId: INSTALL_Z, actorBindingVersion: 0 }, 200);
    }) as typeof fetch);
    const result = await join.performJoin({ target: "https://cloud.example#pljt_lean-contract-token", homeDir, reassign: true, fetchImpl: cloud, temporaryRoot: path.join(homeDir, "handshake-tmp") });
    assert.equal(result.joined, true, JSON.stringify(result));
    assert.deepEqual(requests, ["/api/work-intelligence/join", "/api/work-intelligence/ingest"], "the token is redeemed, then the one-event handshake runs");
    const activated = config.collectorConfigSchema.parse(JSON.parse(fs.readFileSync(configPath, "utf8")));
    assert.deepEqual([activated.cloudDeviceId, activated.installKey], [INSTALL_Z, "pli_new_install_key_z"], "the grant replaced the hosted credential set");
    const ledger = new LocalEventBuffer(config.collectorBufferPath(homeDir));
    try {
      const binding = ledger.workspaceBinding();
      assert.equal(binding?.currentWorkspaceId, "tenant-lean-contract");
      assert.ok(binding?.currentInstallationEpochId, "activation ran useWorkspace/transitionWorkspace with the join's installation epoch (join.ts:605-621), the step that will also record the joined install");
    } finally { ledger.close(); }
  } finally {
    if (previousPlimsollHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = previousPlimsollHome;
    fs.rmSync(homeDir, { recursive: true, force: true });
  }
});
