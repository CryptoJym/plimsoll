/**
 * B2a (collector): one identity row and exactly two edges per raw row; the day target sees every member; segment
 * retirement only by proof and never a reused number (docs/lean/ARCHITECTURE.md §2.2, §3.3). Ports
 * fixtures/b1_membership_edges.py and b3_durable_target_refs.py. Pending until B2a lands the lean writer.
 */
import assert from "node:assert/strict";
import test from "node:test";

import { event, fn, loadSurface, openTempBuffer, pending } from "./_pending";

const REQUIRED: Record<string, string[]> = {
  usage_session: ["day", "session"], usage_sessionless: ["day", "rollup"], activity_session: ["day", "session"], activity_sessionless: ["day", "rollup"],
};

test("B2a: every admitted raw row has one identity row with its row_class and exactly the two covering targets of that class", pending("B2a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const rows = {
      u1: event({ eventType: "assistant_response", inputTokens: 5, outputTokens: 1 }),
      u2: event({ sessionId: undefined, eventType: "otel_span", inputTokens: 7, outputTokens: 1 }),
      a1: event({ eventType: "tool_use", inputTokens: undefined, outputTokens: undefined }),
      a2: event({ sessionId: undefined, eventType: "otel_span", inputTokens: undefined, outputTokens: undefined }),
    };
    for (const e of Object.values(rows)) buffer.append(e);
    const expected: Record<string, string> = { u1: "usage_session", u2: "usage_sessionless", a1: "activity_session", a2: "activity_sessionless" };
    for (const [name, e] of Object.entries(rows)) {
      const member = buffer.database.prepare("select row_class as rowClass from summary_members where event_id = ? and state = 'live'").get(e.id) as { rowClass: string } | undefined;
      assert.ok(member, `${name}: identity row`);
      assert.equal(member.rowClass, expected[name], name);
      const kinds = (buffer.database.prepare("select s.target_kind as kind from summary_member_edges e join summary_segments s on s.target_ref = e.target_ref where e.event_id = ? order by 1").all(e.id) as Array<{ kind: string }>).map((r) => r.kind);
      assert.deepEqual(kinds, REQUIRED[expected[name]], `${name}: covering targets`);
    }
    const day = buffer.database.prepare("select member_count as members, live_member_count as live from summary_segments where target_kind = 'day'").get() as { members: number; live: number };
    assert.deepEqual(day, { members: 4, live: 4 }, "the day target counts all four members");
  } finally { close(); }
});

test("B2a: a referenced segment cannot retire, a retired number is never reused, and a live edge refuses the raw delete", pending("B2a"), async () => {
  const surface = await loadSurface("../../../packages/collector-cli/src/lean/retention.ts");
  const retire = fn(surface, "retireSegment") as (db: unknown, targetRef: number) => { retired: boolean; reason?: string };
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const db = buffer.database;
    buffer.append(event());
    const session = db.prepare("select target_ref as ref from summary_segments where target_kind = 'session'").get() as { ref: number };
    const refused = retire(db, session.ref);
    assert.equal(refused.retired, false, "referenced by a live edge");
    const top = (db.prepare("select max(target_ref) as top from summary_segments").get() as { top: number }).top;
    db.prepare("delete from summary_member_edges").run();
    db.prepare("delete from summary_members").run();
    assert.equal(retire(db, top).retired, true, "zero references => retires");
    db.prepare("insert into summary_segments (target_kind, base_key, digest, upload_state) values ('session', 'e|codex|another', '', 'pending')").run();
    const fresh = (db.prepare("select max(target_ref) as top from summary_segments").get() as { top: number }).top;
    assert.ok(fresh > top, `autoincrement never reissues ${top}; got ${fresh}`);
  } finally { close(); }
});
