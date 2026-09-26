/**
 * B10a (collector): the no-delete hold (docs/lean/ARCHITECTURE.md §2.4; PROOF.md §5 item 1). Pending until B10a lands
 * raw_retention_control.hold_reason, the prune early return and retention.hold in the status block.
 */
import assert from "node:assert/strict";
import test from "node:test";

import { event, openTempBuffer, pending } from "./_pending";

const tomorrow = () => new Date(Date.now() + 86_400_000);

test("B10a: while hold_reason is set the prune deletes nothing at any age and /status reports the hold", pending("B10a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", delivery: { enabled: true } });
  try {
    const pendingDelivery = event();
    buffer.append(pendingDelivery); // pending in the outbox: today's predicate would still delete it at age (the defect of PROOF §5 item 1)
    const unmarked = event();
    buffer.append(unmarked);
    buffer.database.prepare("update raw_retention_control set hold_reason = 'lean_migration' where singleton = 1").run();
    const held = buffer.prune(0, { maxRows: 100, now: tomorrow() });
    assert.equal(held.events, 0, "nothing deleted under the hold");
    assert.equal((buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n, 2);
    const status = buffer.retentionProgressStatus(0, tomorrow()) as unknown as { hold?: string | null };
    assert.equal(status.hold, "lean_migration");
  } finally { close(); }
});

test("B10a: with the hold cleared the old predicate still deletes the pending-outbox row at age (today's defect as the red half of PROOF §5 item 1; retired when B10b replaces the predicate)", pending("B10a"), () => {
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", delivery: { enabled: true } });
  try {
    buffer.append(event());
    buffer.database.prepare("update raw_retention_control set hold_reason = null where singleton = 1").run();
    const released = buffer.prune(0, { maxRows: 100, now: tomorrow() });
    assert.equal(released.events, 1);
  } finally { close(); }
});
