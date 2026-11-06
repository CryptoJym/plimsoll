import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { ClaudePlanLimitCollector, CLAUDE_PLAN_LIMIT_POLL_MS, type ClaudePlanLimitsOptions } from "../packages/collector-cli/src/claude-plan-limits";
import { providerAccountKey, hashProtectedValue } from "../packages/shared/src/policy";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { sealOutboundEvent } from "../packages/collector-cli/src/outbound-envelope";

async function main() {
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-claude-limits-"));
const home = path.join(root, "collector");
const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"));
let clock = Date.parse("2026-11-06T10:00:00Z");
const now = () => new Date(clock);
const names = ["a", "b", "a-copy", "no-account", "no-credential", "expired", "mismatch"];
const roots = names.map(name => ({ source: "claude_code" as const, rootId: name, profileId: name,
  installationEpochId: "fixture-epoch", directory: path.join(root, name, "projects") }));
const uuid = (name: string) => `fixture-account-${name}`;
for (const name of names) {
  const identity = name === "a-copy" ? "a" : name;
  fs.mkdirSync(path.join(root, name), { recursive: true });
  if (name !== "no-account") fs.writeFileSync(path.join(root, name, ".claude.json"), JSON.stringify({ oauthAccount: {
    accountUuid: uuid(identity), emailAddress: `${identity}@fixture.invalid`, organizationUuid: "fixture-org" } }));
}
let requests = 0;
let mode: "ok" | "401" | "429" | "rotate" = "ok";
const options: ClaudePlanLimitsOptions = { home, homeDir: root, env: {}, captureRoots: roots, now,
  readCredential: async (dir) => {
    const name = path.basename(dir) === "a-copy" ? "a" : path.basename(dir);
    return name === "no-credential" ? null : { accessToken: `fixture-token-${name}`,
      expiresAt: name === "expired" ? clock - 1 : clock + 60_000, planType: "max" };
  },
  request: async (endpoint, token) => {
    requests++;
    const name = token.slice("fixture-token-".length);
    if (mode === "401") return { status: 401, body: "fixture-private-error" };
    if (mode === "429") return { status: 429, body: "fixture-private-error", retryAfterMs: 30 * 60_000 };
    if (endpoint === "/api/oauth/profile") return { status: 200, body: JSON.stringify({
      account: { uuid: uuid(name === "mismatch" ? "foreign" : name), email: `${name}@fixture.invalid` }, organization: { uuid: "fixture-org" } }) };
    if (mode === "rotate") fs.writeFileSync(path.join(root, name, ".claude.json"), JSON.stringify({ oauthAccount: { accountUuid: uuid("rotated") } }));
    return { status: 200, body: JSON.stringify({ accessToken: token,
      five_hour: { utilization: name === "a" ? 27.3 : 61, resets_at: "2026-11-06T14:00:00Z" },
      seven_day_oauth_apps: { utilization: 12, resets_at: "2026-11-12T10:00:00Z" },
      seven_day: { utilization: 47, resets_at: "2026-11-12T10:00:00Z" } }) };
  },
};
const rows = () => (buffer.database.prepare("select payload_json from buffered_events where event_type='plan_limit_observation' order by rowid")
  .all() as Array<{ payload_json: string }>).map(row => aiInteractionEventSchema.parse(JSON.parse(row.payload_json)));
let collector = new ClaudePlanLimitCollector(buffer, options);
const checks: string[] = [];
try {
  const pending = collector.tick();
  assert.equal(collector.tick(), pending, "single-flight tick returns the same work");
  const first = await pending;
  assert.equal(first.planLimitReadingsEmitted, 4);
  assert.equal(first.planLimitReadingsSkippedNoAccount, 1);
  assert.equal(first.planLimitReadingsSkippedNoCredential, 2);
  assert.equal(first.planLimitReadingsSkippedAccountMismatch, 1);
  assert.equal(requests, 5, "missing/expired credentials never make a request; mismatched profile never reads usage");
  for (const event of rows()) {
    assert.equal(event.source, "claude_code");
    assert.equal(event.metadata.planLimitSource, "claude_usage_api");
    assert.equal(event.metadata.planType, "max");
    assert.equal(event.inputTokens, undefined);
    assert.ok(sealOutboundEvent(event).ok, "existing upload privacy gate admits the reading");
  }
  assert.equal(rows()[0]!.metadata["user.account_uuid"], hashProtectedValue({ stringValue: uuid("a") }));
  assert.equal(rows()[0]!.metadata["user.account_uuid"], providerAccountKey(uuid("a")));
  assert.deepEqual(rows().slice(0, 2).map(e => [e.metadata.planLimitWindow, e.metadata.planLimitUsedPercent,
    e.metadata.planLimitResetsAt, e.metadata.planLimitWindowMinutes]), [
    ["five_hour", 27.3, "2026-11-06T14:00:00.000Z", 300], ["seven_day", 47, "2026-11-12T10:00:00.000Z", 10080] ]);
  assert.equal((buffer.database.prepare("select count(*) as n from plan_limit_emission_state").get() as { n: number }).n, 4);
  checks.push("two enrolled accounts reach emission state and upload gate with event account hashes");

  await collector.stop();
  collector = new ClaudePlanLimitCollector(buffer, options);
  assert.equal((await collector.tick()).planLimitReadingsSkippedBackoff, 7);
  assert.equal(requests, 5, "restart preserves success cadence and failure backoff");
  clock += CLAUDE_PLAN_LIMIT_POLL_MS;
  assert.equal((await collector.tick()).planLimitReadingsEmitted, 4, "unchanged percentages heartbeat inside fifteen minutes");
  assert.equal(rows().length, 8);
  assert.equal(collector.status().planLimitReadingsSkippedNoCredential, 2);
  checks.push("restart spacing and unchanged ten-minute heartbeat");

  await collector.stop();
  const oneSeat = { ...options, captureRoots: roots.slice(0, 1) };
  collector = new ClaudePlanLimitCollector(buffer, oneSeat);
  clock += CLAUDE_PLAN_LIMIT_POLL_MS; mode = "401";
  assert.equal((await collector.tick()).planLimitReadingsSkippedNoCredential, 1);
  const before = requests;
  await collector.tick(); assert.equal(requests, before);
  clock += CLAUDE_PLAN_LIMIT_POLL_MS; mode = "429";
  assert.equal((await collector.tick()).planLimitReadingsFailed, 1);
  clock += CLAUDE_PLAN_LIMIT_POLL_MS;
  assert.equal((await collector.tick()).planLimitReadingsSkippedBackoff, 1);
  const otherSeat = new ClaudePlanLimitCollector(buffer, { ...options, captureRoots: [roots[2]!] });
  assert.equal((await otherSeat.tick()).planLimitReadingsSkippedBackoff, 1, "Retry-After also spaces another seat after restart");
  await otherSeat.stop();
  clock += 20 * 60_000; mode = "rotate";
  assert.equal((await collector.tick()).planLimitReadingsSkippedAccountMismatch, 1);
  assert.equal(rows().length, 8);
  const stateDir = path.join(home, "claude-plan-limits");
  assert.equal(fs.existsSync(path.join(home, "capacity")), false, "the reader writes no adapter state or receipt");
  assert.equal(fs.statSync(stateDir).mode & 0o777, 0o700);
  for (const file of fs.readdirSync(stateDir)) assert.equal(fs.statSync(path.join(stateDir, file)).mode & 0o777, 0o600);
  const persisted = JSON.stringify(rows()) + fs.readdirSync(stateDir).map(file => fs.readFileSync(path.join(stateDir, file), "utf8")).join("");
  for (const secret of ["fixture-token", "fixture-private-error", "fixture-account", "@fixture.invalid"])
    assert.ok(!persisted.includes(secret), "only hashed identity and allowlisted facts persist");
  checks.push("401, Retry-After and identity rotation skip without leaking or emitting");
  await collector.stop();
  clock += CLAUDE_PLAN_LIMIT_POLL_MS;
  let release!: () => void;
  collector = new ClaudePlanLimitCollector(buffer, { ...options, captureRoots: [roots[2]!],
    readCredential: async () => { await new Promise<void>(resolve => { release = resolve; }); return null; } });
  const interruptedPoll = collector.tick();
  const restarted = new ClaudePlanLimitCollector(buffer, { ...options, captureRoots: [roots[2]!] });
  assert.equal((await restarted.tick()).planLimitReadingsSkippedBackoff, 1, "reservation survives a restart during credential I/O");
  await restarted.stop();
  let drained = false;
  const stopping = collector.stop().then(() => { drained = true; });
  await Promise.resolve();
  assert.equal(drained, false, "stop waits for the in-flight tick");
  release(); await stopping; await interruptedPoll;
  assert.equal(rows().length, 8, "a stopped read cannot emit after the ledger is released");
  const disabled = new ClaudePlanLimitCollector(buffer, { ...options, captureRoots: [] });
  assert.equal((await disabled.tick()).accountsChecked, 0);
  await disabled.stop();
  console.log(JSON.stringify({ proof: "claude-plan-limit-emission", checks: checks.length, passed: checks.length, failed: 0, names: checks }));
} finally { await collector.stop(); buffer.close(); fs.rmSync(root, { recursive: true, force: true }); }
}
void main().catch(error => { console.error(error); process.exitCode = 1; });
