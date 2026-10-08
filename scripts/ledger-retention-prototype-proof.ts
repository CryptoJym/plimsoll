import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { planRetentionCopies, retentionRowIdentitiesMatch, type RetentionCopy, type RetentionCopyPolicy } from "./lib/ledger-retention-prototype";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("ledger-retention-prototype", 21);
const root = fs.mkdtempSync(path.join(process.env.TMPDIR!, "retention-prototype-"));
const digest = (value: string | Buffer) => `sha256:${createHash("sha256").update(value).digest("hex")}`;
function check(name: string, value: unknown) { completion.check(name, Boolean(value)); assert.ok(value, name); console.log(`PASS ${name}`); }
const policy: RetentionCopyPolicy = { enabled: true, retentionDays: 30, maxLocalBytes: 1_000_000,
  maxRows: 2, now: "2026-10-07T18:00:00Z", complete: true, retainedStateBytes: 64 };
const copy: RetentionCopy = { key: "id-created-generation-epoch-audience", createdAt: "2026-08-01T00:00:00Z", bytes: 4096,
  payloadDigest: digest("raw"), audienceDigest: digest("tenant-device"), pendingDelivery: false, replayHold: false, disputeHold: false,
  receipt: { verified: true, rawKey: "id-created-generation-epoch-audience", payloadDigest: digest("raw"),
    audienceDigest: digest("tenant-device"), acknowledgedAt: "2026-08-02T00:00:00Z" } };
try {
  check("retention is disabled when enablement is omitted", planRetentionCopies([copy], { ...policy, enabled: undefined }).candidateKeys.length === 0);
  check("exact verified copy becomes eligible after both windows", planRetentionCopies([copy], policy).candidateKeys[0] === copy.key);
  check("unverified cloud receipt cannot release local raw", planRetentionCopies([{ ...copy, receipt: null }], policy).candidateKeys.length === 0);
  check("raw incarnation mismatch refuses even a verified receipt", planRetentionCopies([{ ...copy, receipt: { ...copy.receipt!, rawKey: "other-incarnation" } }], policy).candidateKeys.length === 0);
  check("content mismatch refuses deletion", planRetentionCopies([{ ...copy, receipt: { ...copy.receipt!, payloadDigest: digest("changed") } }], policy).candidateKeys.length === 0);
  check("audience mismatch refuses deletion", planRetentionCopies([{ ...copy, receipt: { ...copy.receipt!, audienceDigest: digest("other-tenant") } }], policy).candidateKeys.length === 0);
  check("recent acknowledgment keeps an old offline copy", planRetentionCopies([{ ...copy, receipt: { ...copy.receipt!, acknowledgedAt: "2026-10-06T00:00:00Z" } }], policy).candidateKeys.length === 0);
  check("future or invalid timestamps are held", ["invalid", "2030-01-01T00:00:00Z"].every(createdAt => planRetentionCopies([{ ...copy, createdAt }], policy).candidateKeys.length === 0));
  check("delivery, dispute and replay holds cannot be overridden by cap", ["pendingDelivery", "disputeHold", "replayHold"].every(key => planRetentionCopies([{ ...copy, [key]: true }], { ...policy, maxLocalBytes: 1 }).candidateKeys.length === 0));
  const partial = planRetentionCopies([copy], { ...policy, complete: false });
  check("partial metadata never implies exact total or satisfied cap", partial.totalBytes === null && partial.projectedBytes === null && partial.capExceeded === null && partial.coverage === "partial");
  const bounded = planRetentionCopies([copy, { ...copy, key: "b", receipt: { ...copy.receipt!, rawKey: "b" } }], { ...policy, maxRows: 1 });
  check("bounded batches preserve a continuation", bounded.candidateKeys.length === 1 && bounded.hasMoreEligible);
  check("a cap cannot shorten the age window", planRetentionCopies([{ ...copy, createdAt: "2026-10-06T00:00:00Z" }], { ...policy, maxLocalBytes: 1 }).capExceeded === true);
  check("a renumbered implicit raw identity refuses the file swap", !retentionRowIdentitiesMatch(
    [{ table: "buffered_events", rowid: 42, key: copy.key }], [{ table: "buffered_events", rowid: 1, key: copy.key }]));

  // Private synthetic SQLite only: never imports a production buffer or lifecycle adapter.
  const live = path.join(root, "fixture.sqlite");
  const rollback = path.join(root, "rollback.sqlite");
  const staged = path.join(root, "compacted.sqlite");
  const db = new Database(live);
  // This positive fixture has stable INTEGER PRIMARY KEYs. Production's text
  // primary key schema needs a proven preservation strategy before any swap.
  db.exec(`create table raw_copy (raw_rowid integer primary key, key text unique, payload blob);
    create trigger immutable_raw before update on raw_copy begin select raise(abort,'raw is immutable'); end;
    create table continuity (epoch text, capture_cursor integer, usage_total integer);
    insert into continuity values ('epoch-a',4242,42);
    create table copy_expiry_receipts (key text primary key, digest text);
    create table durable_ack (key text primary key, digest text);
    create table pending_replay (key text primary key);`);
  const payload = Buffer.alloc(2 * 1024 * 1024, 65);
  const heldPayload = Buffer.from("pending delivery");
  db.prepare("insert into raw_copy (key,payload) values (?,?)").run(copy.key, payload);
  db.prepare("insert into raw_copy (key,payload) values (?,?)").run("held", heldPayload);
  db.prepare("insert into durable_ack values (?,?)").run(copy.key, digest(payload));
  db.prepare("insert into pending_replay values (?)").run("held");
  const rawHash = digest(payload);
  const continuity = JSON.stringify(db.prepare("select * from continuity").all());
  assert.throws(() => db.prepare("update raw_copy set payload=? where key=?").run("mutation", copy.key), /immutable/);
  check("prototype database rejects raw row edits", digest((db.prepare("select payload from raw_copy where key=?").get(copy.key) as {payload: Buffer}).payload) === rawHash);
  db.prepare("vacuum into ?").run(rollback);
  const beforeBytes = fs.statSync(live).size;
  const eligibleCopy = { ...copy, bytes: payload.length, payloadDigest: rawHash, receipt: { ...copy.receipt!, payloadDigest: rawHash } };
  const plan = planRetentionCopies([eligibleCopy], policy);
  db.transaction(() => {
    for (const key of plan.candidateKeys) {
      db.prepare("insert into copy_expiry_receipts values (?,?)").run(key, rawHash);
      db.prepare("delete from raw_copy where key=?").run(key);
    }
  }).immediate();
  check("deleted local copy leaves matching durable receipt", (db.prepare("select digest from copy_expiry_receipts where key=?").get(copy.key) as {digest: string}).digest === rawHash);
  const identities = db.prepare("select 'raw_copy' as \"table\",rowid,key from raw_copy").all() as Array<{table: string; rowid: number; key: string}>;
  db.prepare("vacuum into ?").run(staged);
  db.close();
  const candidate = new Database(staged, { readonly: true });
  check("candidate identity gate preserves a surviving rowid gap", retentionRowIdentitiesMatch(identities,
    candidate.prepare("select 'raw_copy' as \"table\",rowid,key from raw_copy").all() as typeof identities));
  candidate.close();
  fs.renameSync(staged, live);
  const compacted = new Database(live, { readonly: true });
  check("compaction reduces file bytes after copy expiry", fs.statSync(live).size < beforeBytes / 2);
  console.log(JSON.stringify({ fixtureOnly: true, beforeBytes, compactedBytes: fs.statSync(live).size }));
  check("compacted file passes integrity and preserves cursor and aggregate", compacted.pragma("integrity_check", { simple: true }) === "ok" && JSON.stringify(compacted.prepare("select * from continuity").all()) === continuity);
  check("pending replay copy remains byte identical", digest((compacted.prepare("select payload from raw_copy where key='held'").get() as {payload: Buffer}).payload) === digest(heldPayload));
  // A verified fixture cloud copy remains independently retrievable; expired local data is never invented.
  check("expired replay can verify an independent cloud copy", compacted.prepare("select * from raw_copy where key=?").get(copy.key) === undefined && (compacted.prepare("select digest from durable_ack where key=?").get(copy.key) as {digest: string}).digest === digest(payload));
  compacted.close();
  fs.renameSync(rollback, live);
  const restored = new Database(live, { readonly: true });
  check("rollback restores original raw bytes and continuity", restored.pragma("integrity_check", { simple: true }) === "ok" && digest((restored.prepare("select payload from raw_copy where key=?").get(copy.key) as {payload: Buffer}).payload) === rawHash && JSON.stringify(restored.prepare("select * from continuity").all()) === continuity);
  restored.close();
  completion.complete();
} finally { fs.rmSync(root, { recursive: true, force: true }); }
