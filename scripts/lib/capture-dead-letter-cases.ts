import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { nativeCodexFixture } from "./native-codex-fixture";
import { LocalEventBuffer } from "../../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../../packages/shared/src/index";

const noSpool = { pendingFiles: 0, oldestPendingMs: null, losses: [], unreadable: false };
const tenant = "11111111-2222-4333-8444-555555555555";

export function verifyCaptureDeadLetterCases() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capture-dead-letter-"));
  const epoch = Date.now() - 3_600_000;
  let checks = 0;
  try {
    for (const scenario of ["plan_only", "mixed", "privacy_only"] as const) {
      const buffer = new LocalEventBuffer(path.join(root, `${scenario}.sqlite`), {
        workspaceId: tenant, enrollmentNow: () => new Date(epoch),
        delivery: { enabled: true, limits: { maxOldestAgeDays: 3650 } },
      });
      try {
        const rows = scenario === "mixed" ? [
          { type: "plan_limit_observation", reason: "remote_validation_rejected", usage: {} },
          { type: "assistant_response", reason: "remote_validation_rejected",
            usage: { inputTokens: 11, outputTokens: 6, costUsd: 0.01, costKind: "reported" } },
          { type: "assistant_response", reason: "local_payload_unparseable", usage: {} },
          { type: "tool_use", reason: "local_privacy_violation", usage: {} },
        ] : [{ type: scenario === "privacy_only" ? "tool_use" : "plan_limit_observation",
          reason: scenario === "privacy_only" ? "local_privacy_violation" : "remote_validation_rejected",
          usage: {} }];
        const ids = rows.map(row => {
          const id = crypto.randomUUID();
          buffer.append(aiInteractionEventSchema.parse({
            id, source: "codex", dataMode: "metadata", eventType: row.type,
            observedAt: new Date(epoch + 60_000).toISOString(), actionClass: "other",
            ...row.usage, ...("inputTokens" in row.usage ? nativeCodexFixture(id) : {metadata:{}}),
          }));
          return id;
        });
        buffer.delivery.migrateLegacy();
        const lease = buffer.delivery.lease({ maxRows: 10 });
        buffer.delivery.deadLetterRemote(lease.leaseId, ids);
        for (const [index, row] of rows.entries()) buffer.database.prepare(
          "update upload_receipts set reason = ? where delivery_id = ?",
        ).run(row.reason, ids[index]);
        if (scenario === "mixed") buffer.database.prepare(
          "update buffered_events set payload_json = '{' where id = ?",
        ).run(ids[2]);
        // A persisted 0.7.47 cache must not suppress the new census after upgrade.
        buffer.delivery.captureClaim([], noSpool);
        buffer.database.prepare("update capture_dead_summary set summary_json = ?").run(JSON.stringify({
          dead: rows.filter(row => row.reason !== "local_privacy_violation").length,
          withheld: rows.filter(row => row.reason === "local_privacy_violation").length,
          gaps: scenario === "privacy_only" ? [] : [{ fromMs: epoch + 60_000, toMs: epoch + 60_000 }],
        }));
        const claim = buffer.delivery.captureClaim([], noSpool)!;
        assert.equal(claim.withheld, scenario === "plan_only" ? 0 : 1);
        if (scenario === "privacy_only") {
          assert.equal(claim.dead, 0);
          assert.equal(claim.gaps.length, 0);
          checks += 1;
          continue;
        }
        assert.equal(claim.gaps.length, 1);
        const census = (claim.gaps[0] as { deadLetters?: Array<Record<string, unknown>> }).deadLetters;
        assert.ok(census, "dead-letter gaps carry a complete typed census");
        const planned = census.find(entry => entry.eventType === "plan_limit_observation");
        assert.deepEqual(planned, { eventType: "plan_limit_observation",
          reason: "remote_validation_rejected", count: 1, tokens: 0, costUsd: 0 });
        if (scenario === "mixed") {
          assert.equal(census.length, 4);
          assert.deepEqual(census.find(entry => entry.eventType === "assistant_response" &&
            entry.reason === "remote_validation_rejected"), {
            eventType: "assistant_response", reason: "remote_validation_rejected",
            count: 1, tokens: 17, costUsd: 0.01,
          });
          const unknown = census.find(entry => entry.reason === "local_payload_unparseable");
          assert.equal(unknown?.tokens, null);
          assert.equal(unknown?.costUsd, null);
          assert.ok(census.some(entry => entry.reason === "local_privacy_violation"));
          const overlap = buffer.delivery.captureClaim([], { ...noSpool,
            losses: [{ fromMs: epoch + 60_000, toMs: epoch + 60_000, count: 1 }],
          })!;
          assert.equal((overlap.gaps[0] as { deadLetters?: unknown }).deadLetters, undefined,
            "a dead-letter census never hides overlapping unknown spool loss");
        }
        checks += 1;
      } finally { buffer.close(); }
    }
    return { scenarios: checks };
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}
