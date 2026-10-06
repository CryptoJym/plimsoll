import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { buildIngestBatch } from "../packages/collector-cli/src/upload";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { readLocalIdentities } from "../packages/collector-cli/src/local-identity";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { hashProtectedValue, providerAccountKey, sanitizeForPolicy } from "../packages/shared/src/policy";

const codexId = "fixture-codex-account-one";
const claudeId = "fixture-claude-account-one";
const attr = (key: string, value: string | number) => ({ key, value: typeof value === "number" ? { intValue: String(value) } : { stringValue: value } });
const line = (timestamp: string, type: string, payload: object) => JSON.stringify({ timestamp, type, payload }) + "\n";
const counts = (timestamp: string, input: number, used: number, reset: number, secondary = true, primaryMinutes = 300, output = 1) => line(timestamp, "event_msg", {
  type: "token_count", info: { total_token_usage: { input_tokens: input, cached_input_tokens: 0, output_tokens: output, reasoning_output_tokens: 0 } },
  rate_limits: { plan_type: "pro", limit_id: "codex-limit-fixture", primary: { used_percent: used, window_minutes: primaryMinutes, resets_at: reset },
    ...(secondary ? { secondary: { used_percent: 103.2, window_minutes: 10080, resets_at: reset + 1000 } } : {}) },
});
const scanAt = (clock: string) => ({ scope: "full" as const, now: new Date(`2026-09-29T${clock}Z`) });
const rows = (buffer: LocalEventBuffer) => (buffer.database.prepare("select payload_json from buffered_events order by rowid").all() as Array<{ payload_json: string }>).map(row => JSON.parse(row.payload_json));

async function main() {
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "plimsoll-account-limit-"));
  const codexHome = path.join(root, ".codex");
  const sessions = path.join(codexHome, "sessions");
  const day = path.join(sessions, "2026", "09", "29");
  fs.mkdirSync(day, { recursive: true });
  const authPath = path.join(codexHome, "auth.json");
  const writeAuth = (id: string, mtime: number) => {
    fs.writeFileSync(authPath, JSON.stringify({ tokens: { account_id: id }, last_refresh: "2026-09-29T00:00:00.000Z" }));
    fs.utimesSync(authPath, mtime, mtime);
  };
  let buffer: LocalEventBuffer | undefined;
  let oldWorktree: string | undefined;
  try {
    writeAuth(codexId, 1_000_000);
    const key = providerAccountKey(codexId);
    assert.equal(key, hashProtectedValue({ stringValue: codexId }));
    const otlp = (source: "codex" | "claude_code", field: string, raw: string) => explodeOtlpPayload({ resourceLogs: [{
      resource: { attributes: [attr("service.name", source)] }, scopeLogs: [{ logRecords: [{
        attributes: [attr("event.name", "assistant_response"), attr(field, raw), attr("gen_ai.usage.input_tokens", 7), attr("model", source === "codex" ? "gpt-6-sol" : "claude-opus-5-5")],
      }] }],
    }] }, { source }).events[0]!.event;
    assert.equal((sanitizeForPolicy({ attributes: [attr("user.account_id", codexId)] }).value as any).attributes[0].value.stringValue, key);
    assert.equal(otlp("codex", "user.account_id", codexId).metadata["user.account_id"], key);
    assert.equal(otlp("claude_code", "user.account_uuid", claudeId).metadata["user.account_uuid"], providerAccountKey(claudeId));

    const claims = { "https://api.openai.com/auth": { chatgpt_account_id: codexId } };
    const jwt = `x.${Buffer.from(JSON.stringify(claims)).toString("base64url")}.x`;
    fs.writeFileSync(authPath, JSON.stringify({ tokens: { account_id: codexId, id_token: jwt } }));
    assert.equal(readLocalIdentities({ codexAuthPath: authPath, claudeConfigPath: null })[0]?.actorHash, key);
    writeAuth(codexId, 1_000_000);

    const session = "019e9999-1111-7222-8333-444444444444";
    const workspace = "11111111-1111-4111-8111-111111111111";
    buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
      workspaceId: workspace,
      deviceId: "fixture-device",
      enrollmentNow: () => new Date("2026-09-29T00:00:00.000Z"),
    });
    const tailer = new RolloutTailer(buffer, sessions, () => []);
    await tailer.scan(scanAt("08:00:00")); // account observed before these later events
    assert.equal((buffer.database.prepare("select count(*) as n from sqlite_master where name='plan_limit_emission_state'")
      .get() as { n: number }).n, 0, "an idle tailer does not create plan-limit state");
    const rollout = path.join(day, `rollout-2026-09-29T09-00-00-${session}.jsonl`);
    const reset = 1790676000;
    fs.writeFileSync(rollout, line("2026-09-29T09:00:00.000Z", "session_meta", { id: session }) +
      counts("2026-09-29T09:00:01.000Z", 0, 30.5, reset, true, 300, 0) +
      line("2026-09-29T09:00:02.000Z", "turn_context", { turn_id: "fixture-turn", model: "gpt-6-sol" }) +
      counts("2026-09-29T09:00:03.000Z", 10, 30.8, reset) +
      counts("2026-09-29T09:00:04.000Z", 20, 31.6, reset, false));
    await tailer.scan(scanAt("09:00:05"));
    let all = rows(buffer);
    const usage = all.filter(row => row.eventType === "usage_rollout");
    const readings = all.filter(row => row.eventType === "plan_limit_observation");
    assert.ok(usage.length >= 2);
    const pricedUsage = usage.filter(row => (row.inputTokens ?? 0) > 0 || (row.outputTokens ?? 0) > 0 ||
      (row.cacheReadTokens ?? 0) > 0 || (row.cacheCreationTokens ?? 0) > 0);
    assert.ok(pricedUsage.every(row => row.metadata["user.account_id"] === key && row.model === "gpt-6-sol"));
    assert.equal(otlp("codex", "user.account_id", codexId).metadata["user.account_id"], usage[0].metadata["user.account_id"]);
    assert.equal(readings.length, 3);
    assert.deepEqual(readings.map(row => row.metadata.planLimitWindow).sort(), ["five_hour", "five_hour", "weekly"]);
    assert.ok(readings.every(row => row.metadata["user.account_id"] === key && row.metadata.planLimitSource === "codex_rollout" &&
      row.metadata.planLimitResetsAt.endsWith("Z") && row.metadata.planType === "pro" && row.metadata.planLimitId === "codex-limit-fixture" &&
      row.inputTokens === undefined && row.outputTokens === undefined && row.cacheReadTokens === undefined));
    assert.equal(readings.find(row => row.metadata.planLimitWindow === "weekly")?.metadata.planLimitUsedPercent, 103.2);
    await tailer.scan(scanAt("09:00:06"));
    assert.equal(rows(buffer).length, all.length);

    fs.appendFileSync(rollout, counts("2026-09-29T09:20:00.000Z", 20, 32.0, reset, false));
    await tailer.scan(scanAt("09:20:01"));
    assert.equal(rows(buffer).filter(row => row.eventType === "plan_limit_observation").length, 4,
      "a later sub-point change emits after 15 minutes");
    fs.appendFileSync(rollout, counts("2026-09-29T09:21:00.000Z", 20, 32.2, reset + 3600, false));
    await tailer.scan(scanAt("09:21:01"));
    assert.equal(rows(buffer).filter(row => row.eventType === "plan_limit_observation").length, 5,
      "a reset change emits even when percent changes by less than one point");

    buffer.database.prepare("delete from rollout_scan_state").run();
    const beforeReplay = rows(buffer).length;
    await new RolloutTailer(buffer, sessions, () => []).scan(scanAt("09:21:02"));
    assert.equal(rows(buffer).length, beforeReplay, "a replay does not append duplicate usage or readings");

    const secondId = "fixture-codex-account-two";
    writeAuth(secondId, 1_000_000);
    await tailer.scan(scanAt("09:21:50"));
    fs.appendFileSync(rollout, counts("2026-09-29T09:22:00.000Z", 30, 32.4, reset, false));
    await tailer.scan(scanAt("09:22:05"));
    assert.equal(rows(buffer).filter(row => row.eventType === "usage_rollout").at(-1)?.metadata["user.account_id"], providerAccountKey(secondId),
      "a changed auth file is observed even when its mtime is unchanged");
    writeAuth(secondId, 1_000_100);
    await tailer.scan(scanAt("09:22:50"));
    fs.appendFileSync(rollout, counts("2026-09-29T09:23:00.000Z", 40, 32.6, reset, false));
    await tailer.scan(scanAt("09:23:05"));
    all = rows(buffer);
    assert.equal(all.filter(row => row.eventType === "usage_rollout").at(-1)?.metadata["user.account_id"], providerAccountKey(secondId));
    assert.ok(all.some(row => row.eventType === "plan_limit_observation" && row.metadata["user.account_id"] === providerAccountKey(secondId)));
    const usageBeforeLimitOnly = all.filter(row => row.eventType === "usage_rollout").length;
    fs.appendFileSync(rollout, line("2026-09-29T09:24:00.000Z", "event_msg", { type: "token_count", rate_limits: {
      primary: { used_percent: 33, window_minutes: 300, resets_at: reset + 7200 },
    } }));
    await tailer.scan(scanAt("09:24:05"));
    all = rows(buffer);
    assert.equal(all.filter(row => row.eventType === "usage_rollout").length, usageBeforeLimitOnly);
    assert.ok(all.some(row => row.eventType === "plan_limit_observation" &&
      row.metadata["user.account_id"] === providerAccountKey(secondId) &&
      row.metadata.planLimitUsedPercent === 33 && row.inputTokens === undefined));

    const usageBeforeInvalidWindow = all.filter(row => row.eventType === "usage_rollout").length;
    fs.appendFileSync(rollout, counts("2026-09-29T09:25:00.000Z", 50, 34, reset, false, 525_601));
    await tailer.scan(scanAt("09:25:05"));
    all = rows(buffer);
    assert.equal(all.filter(row => row.eventType === "usage_rollout").length,
      usageBeforeInvalidWindow + 1,
      "usage in the same capture survives an invalid plan-window reading");
    assert.equal(all.some(row => row.eventType === "plan_limit_observation" &&
      row.metadata.planLimitWindow === "window_525601m"), false,
      "an over-bound plan-window reading is never emitted");
    const invalidWindowBatch = buildIngestBatch(
      collectorConfigSchema.parse({
        tenantId: workspace,
        deviceId: "fixture-device",
        installKey: "fixture-install",
      }),
      buffer,
      { now: () => new Date(Date.now() + 61_000) },
    );
    assert.ok(invalidWindowBatch.batch?.events.some(event =>
      event.event.eventType === "usage_rollout" && event.event.inputTokens === 10),
      "usage from the same capture remains uploadable");
    tailer.close();

    const claudeDir = path.join(root, "claude-profile");
    const projects = path.join(claudeDir, "projects", "fixture-project");
    fs.mkdirSync(projects, { recursive: true });
    fs.writeFileSync(path.join(claudeDir, ".claude.json"), JSON.stringify({ oauthAccount: { accountUuid: claudeId } }));
    const transcriptTailer = new TranscriptTailer(buffer, path.join(claudeDir, "projects"));
    await transcriptTailer.scan(scanAt("08:00:00"));
    const claudeSession = "019eaaaa-1111-7222-8333-444444444444";
    fs.writeFileSync(path.join(projects, `${claudeSession}.jsonl`), JSON.stringify({
      type: "assistant", sessionId: claudeSession, timestamp: "2026-09-29T09:00:00.000Z",
      message: { id: "019eaaaa-2222-7333-8444-555555555555", model: "claude-opus-5-5",
        content: [{ type: "text", text: "fixture" }],
        usage: { input_tokens: 8, cache_read_input_tokens: 2, cache_creation_input_tokens: 0, output_tokens: 3 } },
    }) + "\n");
    await transcriptTailer.scan(scanAt("09:00:05"));
    transcriptTailer.close();
    const transcript = rows(buffer).find(row => row.eventType === "usage_transcript");
    assert.equal(transcript?.metadata["user.account_uuid"], providerAccountKey(claudeId));
    assert.equal(transcript?.model, "claude-opus-5-5");
    buffer.close(); buffer = undefined;

    oldWorktree = path.join(root, "collector-0.7.44");
    execFileSync("git", ["worktree", "add", "--detach", "--quiet", oldWorktree, "375f277b85f7d4ede7db77bf4359c371c0e8a4aa"]);
    fs.symlinkSync(path.resolve("node_modules"), path.join(oldWorktree, "node_modules"), "dir");
    assert.equal(JSON.parse(fs.readFileSync(path.join(oldWorktree, "packages/collector-cli/package.json"), "utf8")).version, "0.7.44");
    const oldModule = await import(pathToFileURL(path.join(oldWorktree, "packages/collector-cli/src/buffer.ts")).href);
    const oldBuffer = new oldModule.LocalEventBuffer(path.join(root, "ledger.sqlite"));
    try {
      assert.ok((oldBuffer.database.prepare("select count(*) as n from buffered_events where event_type='plan_limit_observation'").get() as { n: number }).n >= 6);
    } finally { oldBuffer.close(); }
    console.log(JSON.stringify({ proof: "account-plan-limit", checks: 26, passed: 26, failed: 0 }));
  } finally {
    buffer?.close();
    if (oldWorktree) {
      try { execFileSync("git", ["worktree", "remove", "--force", oldWorktree]); } catch { /* Preserve original failure. */ }
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
