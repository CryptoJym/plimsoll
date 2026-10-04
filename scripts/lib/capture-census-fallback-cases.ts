import assert from "node:assert/strict";
import crypto from "node:crypto";
import { postDeliveryWithCaptureCensusFallback } from "../../packages/collector-cli/src/capture-census-fallback";
import { deliveryAcknowledgement, deliveryExpectation } from "../../packages/collector-cli/src/delivery-ack";
import type { DeliveryCaptureClaim } from "../../packages/collector-cli/src/outbox";

/** HTTP transport is injected; no collector, cloud or provider is contacted. */
export async function verifyCaptureCensusFallbackCases() {
  const installKey = "invented-census-install-key";
  const secret = "invented-census-signing-secret";
  const claim: DeliveryCaptureClaim = {
    v: 1, epoch: "11111111-2222-4333-8444-555555555555",
    epochStartedAt: "2026-10-01T00:00:00.000Z", cursor: 1,
    through: null, pending: 0, dead: 1, withheld: 0,
    gaps: [{ from: "2026-10-01T01:00:00.000Z", to: "2026-10-01T01:00:00.000Z", deadLetters: [{
      eventType: "plan_limit_observation", reason: "remote_validation_rejected", count: 1, tokens: 0, costUsd: 0,
    }] }],
  };
  const body = JSON.stringify({ tenantId: claim.epoch, installKey,
    events: [{ event: { id: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee" } }] });
  const expected = deliveryExpectation(body, installKey);
  const issues = [{ code: "unrecognized_keys", path: ["gaps", 0], keys: ["deadLetters"] }];
  const reply = (status: number, value: unknown) => new Response(JSON.stringify(value), {
    status, headers: { "content-type": "application/json" },
  });
  const good = { ok: true, ack: deliveryAcknowledgement(expected, expected.itemIds) };
  const warnings: string[] = [];
  const originalWarn = console.warn;
  console.warn = (...values: unknown[]) => warnings.push(values.map(String).join(" "));
  try {
    for (const scenario of ["old_schema", "still_refused", "unrelated_issue", "first_ack", "unknown_send",
      "generic_refusal", "unnamed_refusal", "named_signature_refusal", "no_census"] as const) {
      let calls = 0;
      const invoke = () => postDeliveryWithCaptureCensusFallback({
        url: "http://127.0.0.1:1/fixture", body, installKey, signingSecret: secret,
        now: () => new Date("2026-10-03T00:00:00.000Z"), captureClaim: scenario === "no_census"
          ? { ...claim, gaps: claim.gaps.map(({ from, to }) => ({ from, to })) } : claim,
        fetchImpl: (async (_input, init) => {
          calls += 1;
          const headers = new Headers(init?.headers);
          const raw = headers.get("x-plimsoll-capture")!;
          const sent = JSON.parse(raw) as DeliveryCaptureClaim;
          assert.equal(String(init?.body), body);
          assert.equal(sent.cursor, claim.cursor);
          assert.deepEqual(sent.gaps.map(({ from, to }) => ({ from, to })),
            claim.gaps.map(({ from, to }) => ({ from, to })));
          assert.equal(headers.get("x-plimsoll-capture-signature"), `sha256=${crypto.createHmac("sha256", secret)
            .update(`plimsoll-capture-v1\n${headers.get("x-plimsoll-upload-timestamp")}\n${raw}\n${body}`).digest("hex")}`);
          if (scenario === "unknown_send") throw new Error("synthetic network failure");
          if (scenario === "unrelated_issue") return reply(400, { issues: [{ path: ["events", 0, "deadLetters"] }] });
          if (scenario === "named_signature_refusal") return reply(200, { ...good,
            captureClaim: { state: "refused", reason: "claim_signature_invalid" },
          });
          if (scenario === "no_census") return reply(200, { ...good,
            captureClaim: { state: "refused", reason: "claim_invalid" },
          });
          if (calls === 1) {
            assert.ok(sent.gaps[0]?.deadLetters);
            if (scenario === "generic_refusal" || scenario === "unnamed_refusal") return reply(200, { ...good,
              captureClaim: { state: "refused", ...(scenario === "generic_refusal" ? { reason: "claim_invalid" } : {}) },
            });
            return scenario === "first_ack" ? reply(200, { ...good,
              captureClaim: { state: "refused", issues },
            }) : reply(400, { issues });
          }
          assert.equal(sent.gaps[0]?.deadLetters, undefined);
          if (scenario === "first_ack") return reply(503, { error: "synthetic unavailable" });
          return scenario === "still_refused" ? reply(400, { issues }) : reply(200, good);
        }) as typeof fetch,
      });
      if (scenario === "unknown_send") {
        await assert.rejects(invoke);
        assert.equal(calls, 1, "an unknown send never authorizes another POST");
      } else {
        const result = await invoke();
        assert.equal(calls, ["unrelated_issue", "named_signature_refusal", "no_census"].includes(scenario) ? 1 : 2);
        assert.equal(result.ok, !["still_refused", "unrelated_issue"].includes(scenario));
        if (result.ok) assert.deepEqual(result.acknowledgement?.acceptedIds, expected.itemIds);
      }
    }
    assert.equal(warnings.length, 1);
    assert.ok(warnings[0]?.includes("capture_claim_census_version_mismatch"));
    assert.ok(!warnings.join("\n").includes(secret) && !warnings.join("\n").includes(installKey));
    return { scenarios: 9, warnings: 1 };
  } finally { console.warn = originalWarn; }
}
