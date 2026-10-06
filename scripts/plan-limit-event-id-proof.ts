import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { codexPlanLimitWindows, PlanLimitEmitter } from "../packages/collector-cli/src/plan-limit-observation";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";
import { MAX_PLAN_LIMIT_WINDOW_MINUTES, providerAccountKey } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "plimsoll-plan-limit-id-"));
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
try {
  const accountKey = providerAccountKey("fixture-plan-limit-account");
  const window = { window: "five_hour", minutes: 300, usedPercent: 72.4,
    resetsAt: "2026-09-29T14:00:00.000Z" };
  const observe = (emitter: PlanLimitEmitter, observedAt: string) => emitter.observe({
    source: "codex", accountKey, observedAt, window, planLimitSource: "codex_rollout",
  });
  const readings = () => (buffer.database.prepare(
    "select id, payload_json as payload from buffered_events where event_type='plan_limit_observation' order by rowid",
  ).all() as Array<{ id: string; payload: string }>);

  const emitter = new PlanLimitEmitter(buffer);
  assert.equal(observe(emitter, "2026-09-29T09:00:01.000Z"), true);
  assert.equal(observe(emitter, "2026-09-29T09:00:02.000Z"), false);
  assert.equal(readings().length, 1, "a replay in the first bucket adds no row");

  assert.equal(observe(emitter, "2026-09-29T09:15:01.000Z"), true,
    "an unchanged reading after 15 minutes becomes a new row");
  const two = readings();
  assert.equal(two.length, 2);
  assert.notEqual(two[0]?.id, two[1]?.id, "the later bucket has a distinct event id");
  assert.ok(two.every(row => {
    const event = JSON.parse(row.payload);
    return event.metadata["user.account_id"] === accountKey &&
      event.metadata.planLimitUsedPercent === 72.4 && event.inputTokens === undefined;
  }));

  // Simulate replay after losing only the throttle cursor: the event ID still protects the ledger.
  buffer.database.prepare("delete from plan_limit_emission_state").run();
  assert.equal(observe(new PlanLimitEmitter(buffer), "2026-09-29T09:15:02.000Z"), false);
  assert.equal(readings().length, 2, "a same-bucket replay deduplicates independently of throttle state");

  const invalidRateLimit = {
    primary: {
      used_percent: 42,
      window_minutes: MAX_PLAN_LIMIT_WINDOW_MINUTES + 1,
      resets_at: "2026-09-29T14:00:00.000Z",
    },
  };
  assert.deepEqual(codexPlanLimitWindows(invalidRateLimit), [],
    "an over-bound provider window is not admitted by the normalizer");
  assert.equal(emitter.observe({
    source: "codex", accountKey, observedAt: "2026-09-29T09:30:01.000Z",
    window: {
      window: `window_${MAX_PLAN_LIMIT_WINDOW_MINUTES + 1}m`,
      minutes: MAX_PLAN_LIMIT_WINDOW_MINUTES + 1,
      usedPercent: 42,
      resetsAt: "2026-09-29T14:00:00.000Z",
    },
    planLimitSource: "codex_rollout",
  }), false,
    "the emitter refuses an explicitly over-bound window");
  const invalidEnvelope = sealOutboundEnvelope({
    event: {
      id: "00000000-0000-4000-8000-000000000901",
      source: "codex",
      dataMode: "metadata",
      eventType: "plan_limit_observation",
      observedAt: "2026-09-29T09:30:01.000Z",
      metadata: {
        planLimitWindow: `window_${MAX_PLAN_LIMIT_WINDOW_MINUTES + 1}m`,
        planLimitWindowMinutes: MAX_PLAN_LIMIT_WINDOW_MINUTES + 1,
      },
    },
    suppressedFields: [],
  });
  assert.equal(invalidEnvelope.ok, false,
    "the outbound boundary refuses an over-bound plan reading");
  assert.equal(readings().length, 2,
    "refused plan readings cannot join a usage batch through the ledger");
  console.log(JSON.stringify({ proof: "plan-limit-event-id", checks: 12, passed: 12, failed: 0 }));
} finally {
  buffer.close();
  fs.rmSync(root, { recursive: true, force: true });
}
