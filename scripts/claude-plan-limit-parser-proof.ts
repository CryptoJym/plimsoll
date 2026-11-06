import assert from "node:assert/strict";
import { parseClaudeUsageResponse, CLAUDE_USAGE_MAX_BYTES, CLAUDE_USAGE_WINDOW_LABELS } from "../packages/collector-cli/src/provider-capacity-adapters";
import { claudeKeychainService, parseClaudeUsageCredential } from "../packages/collector-cli/src/claude-plan-limits";

const checks: string[] = [];
function prove(name: string, run: () => void) { run(); checks.push(name); }
const reset = "2026-11-07T11:00:00+01:00";
prove("endpoint utilization and known windows only", () => {
  const raw = Object.fromEntries(CLAUDE_USAGE_WINDOW_LABELS.map((label, i) => [label, { utilization: i === 5 ? 103.2 : i * 12.5, resets_at: reset }]));
  const parsed = parseClaudeUsageResponse(JSON.stringify({ ...raw, accessToken: "fixture-secret", arbitrary_window: raw.five_hour }));
  assert.equal(parsed.kind, "ok");
  if (parsed.kind !== "ok") throw new Error("parser_failed");
  assert.deepEqual(parsed.windows.map(w => w.window), [...CLAUDE_USAGE_WINDOW_LABELS]);
  assert.equal(parsed.windows[0]?.usedPercent, 0);
  assert.equal(parsed.windows[5]?.usedPercent, 103.2);
  assert.equal(parsed.windows[0]?.windowMinutes, 300);
  assert.equal(parsed.windows[1]?.windowMinutes, 10080);
  assert.ok(parsed.windows.every(w => w.resetsAt === "2026-11-07T10:00:00.000Z"));
  assert.ok(!JSON.stringify(parsed).includes("fixture-secret"));
});
prove("absent or invalid windows never become zero", () => {
  for (const utilization of [null, true, "50", -1]) {
    assert.deepEqual(parseClaudeUsageResponse(JSON.stringify({ five_hour: { utilization, resets_at: reset } })), { kind: "ok", windows: [] });
  }
  assert.deepEqual(parseClaudeUsageResponse('{"seven_day":null,"five_hour":{"used_percentage":12,"resets_at":"bad"}}'), { kind: "ok", windows: [] });
  for (const raw of ["not-json", "[]", "null", '{"error":{"message":"fixture-secret"}}', " ".repeat(CLAUDE_USAGE_MAX_BYTES + 1)])
    assert.deepEqual(parseClaudeUsageResponse(raw), { kind: "parse_failed" });
});
prove("credentials expire and keychain scopes do not fall back to another seat", () => {
  const now = new Date("2026-11-06T10:00:00Z");
  const oauth = { accessToken: "fixture-only-token", expiresAt: now.getTime() + 60_000, subscriptionType: "max", refreshToken: "never-retained" };
  const parsed = parseClaudeUsageCredential(JSON.stringify({ claudeAiOauth: oauth }), now);
  assert.deepEqual(parsed, { accessToken: oauth.accessToken, expiresAt: oauth.expiresAt, planType: "max" });
  assert.equal(parseClaudeUsageCredential(JSON.stringify({ claudeAiOauth: { ...oauth, expiresAt: now.getTime() } }), now), null);
  assert.equal(claudeKeychainService("/fixture/.claude", "/fixture/.claude"), "Claude Code-credentials");
  assert.match(claudeKeychainService("/fixture/.claude", "/fixture/.claude", true), /^Claude Code-credentials-[a-f0-9]{8}$/);
  assert.notEqual(claudeKeychainService("/fixture/seat-a", "/fixture/.claude"), claudeKeychainService("/fixture/seat-b", "/fixture/.claude"));
});
console.log(JSON.stringify({ proof: "claude-plan-limit-parser", checks: checks.length, passed: checks.length, failed: 0, names: checks }));
