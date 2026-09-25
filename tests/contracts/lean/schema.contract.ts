/**
 * B2a / B10a / B10b (collector): local DDL v3 (docs/lean/ARCHITECTURE.md §3, §2.4; CONTRACTS.md C3, C6).
 * Ports the schema halves of fixtures b1_membership_edges.py, b2_epoch_day_keys.py, b3_durable_target_refs.py,
 * sf5_unresolved_file_open_gap.py and b5_non_iso_day_facts.py. Pending until the named bead lands ensureLeanSchema.
 */
import assert from "node:assert/strict";
import test from "node:test";

import { columns, openTempBuffer, pending, tableSql } from "./_pending";

function withBuffer(run: (db: ReturnType<typeof openTempBuffer>["buffer"]["database"], buffer: ReturnType<typeof openTempBuffer>["buffer"]) => void) {
  const { buffer, close } = openTempBuffer();
  try { run(buffer.database, buffer); } finally { close(); }
}

test("B2a: summary_segments is keyed by an autoincrement target_ref with session, rollup and day kinds", pending("B2a"), () => withBuffer((db) => {
  const sql = tableSql(db, "summary_segments");
  assert.ok(sql, "summary_segments exists");
  assert.match(sql, /target_ref\s+integer\s+primary\s+key\s+autoincrement/i);
  for (const kind of ["'session'", "'rollup'", "'day'"]) assert.ok(sql.includes(kind), kind);
}));

test("B2a: summary_members carries one identity row per raw row with its row_class and actor_binding_version; edges and mutations exist", pending("B2a"), () => withBuffer((db) => {
  const members = tableSql(db, "summary_members");
  assert.ok(members, "summary_members exists");
  for (const cls of ["usage_session", "usage_sessionless", "activity_session", "activity_sessionless"]) assert.ok(members.includes(cls), cls);
  const names = columns(db, "summary_members").map((c) => c.name);
  for (const col of ["event_id", "raw_generation", "raw_rowid", "row_class", "state", "payload_digest16", "actor_binding_version", "deleted_at_ms", "readmissions"]) assert.ok(names.includes(col), col);
  assert.ok(tableSql(db, "summary_member_edges"), "summary_member_edges exists");
  assert.ok(tableSql(db, "sealed_member_mutations"), "sealed_member_mutations exists");
}));

test("B2a: the fact, turn, session, day and rollup tables exist; day tables are epoch-scoped (epoch_key first in the primary key)", pending("B2a"), () => withBuffer((db) => {
  for (const table of ["usage_facts_local", "turn_summaries", "session_summaries", "session_day_facts", "model_day_facts", "activity_day_facts", "activity_rollup_hourly"]) assert.ok(tableSql(db, table), table);
  for (const table of ["session_day_facts", "model_day_facts", "activity_day_facts"]) {
    const first = columns(db, table).find((c) => c.pk === 1);
    assert.equal(first?.name, "epoch_key", `${table}: primary key starts with epoch_key`);
  }
}));

test("B22: capture_gaps has an open-ended interval (nullable ended_at_ms, interval_basis epoch_open, resolved_at_ms) and capture_faults exists", pending("B22"), () => withBuffer((db) => {
  const sql = tableSql(db, "capture_gaps");
  assert.ok(sql, "capture_gaps exists");
  assert.ok(sql.includes("epoch_open") && sql.includes("fault_interval"), "interval_basis check names epoch_open and fault_interval");
  const cols = columns(db, "capture_gaps");
  const ended = cols.find((c) => c.name === "ended_at_ms");
  assert.ok(ended && ended.notnull === 0, "ended_at_ms is nullable (null = open)");
  for (const col of ["started_at_ms", "resolved_at_ms", "count_basis", "dropped_usage_rows", "file_key_digest", "unread_bytes"]) assert.ok(cols.some((c) => c.name === col), col);
  assert.ok(tableSql(db, "capture_faults"), "capture_faults exists");
}));

test("B2a C3: conversion_rejects is durable, identity-keyed, reasoned and linked to its counted gap", pending("B2a"), () => withBuffer((db) => {
  const sql = tableSql(db, "conversion_rejects");
  assert.ok(sql, "conversion_rejects exists");
  const cols = columns(db, "conversion_rejects");
  for (const col of ["event_id", "raw_generation", "raw_rowid", "epoch_key", "source", "reason", "detail", "observed_at_raw_digest", "gap_id", "first_seen_at_ms", "last_seen_at_ms", "attempts", "resolved_at_ms"]) assert.ok(cols.some((c) => c.name === col), col);
  assert.deepEqual(cols.filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk).map((c) => c.name), ["event_id", "raw_generation"]);
  for (const reason of ["contract_violation", "payload_unreadable", "day_key_unresolvable"]) assert.ok(sql.includes(reason), reason);
  assert.match(sql, /gap_id\s+text\s+not\s+null\s+references\s+capture_gaps/i);
}));

test("B10a: raw_retention_control.hold_reason and collector_workspace_binding.actor_binding_version exist", pending("B10a", "hold column; B2a for the binding version"), () => withBuffer((db) => {
  assert.ok(columns(db, "raw_retention_control").some((c) => c.name === "hold_reason"), "raw_retention_control.hold_reason");
  assert.ok(columns(db, "collector_workspace_binding").some((c) => c.name === "actor_binding_version"), "collector_workspace_binding.actor_binding_version");
}));

test("B10b: raw_retention_receipts accepts the three lean reasons beside retention_window_elapsed", pending("B10b"), () => withBuffer((db) => {
  const insert = db.prepare("insert into raw_retention_receipts (event_id, raw_rowid, raw_created_at, raw_generation, expired_at, reason) values (?, 1, '2026-01-01T00:00:00.000Z', 'g', '2026-09-25T00:00:00.000Z', ?)");
  for (const reason of ["retention_window_elapsed", "lean_proved_activity", "lean_proved_usage", "lean_hold_release_acked"]) {
    assert.doesNotThrow(() => insert.run(`receipt-${reason}`, reason), reason);
  }
}));

// Guard, not pending: better-sqlite3 opens every connection with foreign_keys ON, so the ledger connection already enforces
// them at 03445d3a (the round-6 note "no foreign_keys pragma in collector-cli/src; B2a sets it" was about the source, not the
// runtime). B2a must keep it on for the connection that writes the lean tables (CONTRACTS.md C6).
test("B2a guard: the ledger connection the lean writer will use enforces foreign keys (PRAGMA foreign_keys = 1)", () => withBuffer((db, buffer) => {
  const lean = ((buffer as unknown as { leanDatabase?: typeof db }).leanDatabase ?? db) as { pragma(name: string, options?: { simple: boolean }): unknown };
  assert.equal(lean.pragma("foreign_keys", { simple: true }), 1);
}));
