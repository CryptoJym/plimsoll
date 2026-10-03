import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { providerAccountKey } from "../packages/shared/src/policy";

const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "account-binding-proof-"));
const codexHome = path.join(root, "codex");
const sessions = path.join(codexHome, "sessions");
const claudeHome = path.join(root, "claude");
const projects = path.join(claudeHome, "projects");
const auth = path.join(codexHome, "auth.json");
const claudeConfig = path.join(claudeHome, ".claude.json");
const ledger = path.join(root, "ledger.sqlite");
const codexIds = ["fixture-codex-before", "fixture-codex-after"];
const claudeIds = ["fixture-claude-before", "fixture-claude-after"];
const at = (clock: string) => new Date(`2026-09-29T${clock}:00.000Z`);
const scanAt = async (clock: string, buffer: LocalEventBuffer) => {
  const codex = new RolloutTailer(buffer, sessions, () => []);
  const claude = new TranscriptTailer(buffer, projects);
  try {
    await codex.scan({ scope: "full", now: at(clock) });
    await claude.scan({ scope: "full", now: at(clock) });
  } finally { codex.close(); claude.close(); }
};
const writeAccounts = (index: number, clock: string) => {
  fs.writeFileSync(auth, JSON.stringify({ tokens: { account_id: codexIds[index] },
    last_refresh: at(clock).toISOString() }));
  fs.writeFileSync(claudeConfig, JSON.stringify({ oauthAccount: { accountUuid: claudeIds[index] } }));
  const date = at(clock);
  fs.utimesSync(auth, date, date);
  fs.utimesSync(claudeConfig, date, date);
};
const line = (timestamp: string, type: string, payload: object) => JSON.stringify({ timestamp, type, payload }) + "\n";
const rollout = (session: string, clock: string, percent: number) => {
  const day = path.join(sessions, "2026", "09", "29");
  fs.mkdirSync(day, { recursive: true });
  const base = `2026-09-29T${clock}`;
  const file = path.join(day, `rollout-${base.replaceAll(":", "-")}-${session}.jsonl`);
  const count = (seconds: string, input: number, used: number) => line(`${base}:${seconds}.000Z`, "event_msg", {
    type: "token_count", info: { total_token_usage: { input_tokens: input,
      cached_input_tokens: 0, output_tokens: input ? 1 : 0, reasoning_output_tokens: 0 } },
    rate_limits: { primary: { used_percent: used, window_minutes: 300,
      resets_at: at("14:00").getTime() / 1000 } },
  });
  fs.writeFileSync(file,
    line(`${base}:00.000Z`, "session_meta", { id: session }) +
    count("01", 0, percent) + count("02", 7, percent + 2));
};
const transcript = (session: string, message: string, clock: string) => {
  const dir = path.join(projects, "fixture-project");
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, `${session}.jsonl`), JSON.stringify({
    type: "assistant", sessionId: session, timestamp: at(clock).toISOString(),
    message: { id: message, model: "claude-opus-5-5", content: [{ type: "text", text: "fixture" }],
      usage: { input_tokens: 8, cache_read_input_tokens: 2, cache_creation_input_tokens: 0, output_tokens: 3 } },
  }) + "\n");
};
const rows = (buffer: LocalEventBuffer) =>
  (buffer.database.prepare("select payload_json from buffered_events order by rowid").all() as
    Array<{ payload_json: string }>).map(row => JSON.parse(row.payload_json));
const first = "019e9999-1111-7222-8333-444444444444";
const second = "019e9999-1111-7222-8333-555555555555";
const imported = "019e9999-1111-7222-8333-666666666666";
const stamped = "019e9999-1111-7222-8333-777777777777";
const gap = "019e9999-1111-7222-8333-888888888888";

async function main() {
let buffer: LocalEventBuffer | undefined;
try {
  fs.mkdirSync(sessions, { recursive: true });
  fs.mkdirSync(projects, { recursive: true });
  writeAccounts(0, "08:00");
  buffer = new LocalEventBuffer(ledger);
  await scanAt("08:10", buffer); // first observed continuous window for account A
  rollout(stamped, "08:30", 10);
  transcript(stamped, "019eaaaa-2222-7333-8444-888888888888", "08:30");
  await scanAt("08:40", buffer);
  const confirmed = rows(buffer).filter(row => row.sessionId === stamped);
  assert.ok(confirmed.some(row => row.eventType === "plan_limit_observation"));
  assert.ok(confirmed.filter(row => row.source === "codex").every(row =>
    row.metadata["user.account_id"] === providerAccountKey(codexIds[0]!)));
  assert.ok(confirmed.filter(row => row.source === "claude_code").every(row =>
    row.metadata["user.account_uuid"] === providerAccountKey(claudeIds[0]!)));

  rollout(first, "09:00", 20);
  transcript(first, "019eaaaa-2222-7333-8444-555555555555", "09:00");
  writeAccounts(1, "09:45");
  await scanAt("10:00", buffer); // delayed scan after the switch
  let all = rows(buffer);
  const old = all.filter(row => row.sessionId === first);
  assert.ok(old.some(row => row.eventType === "usage_rollout"));
  assert.ok(old.some(row => row.eventType === "usage_transcript"));
  assert.equal(old.filter(row => row.eventType === "plan_limit_observation").length, 0,
    "an unproven historical account must not emit a plan-limit reading");
  assert.ok(old.every(row => row.metadata["user.account_id"] === undefined &&
    row.metadata["user.account_uuid"] === undefined));
  assert.deepEqual(all.filter(row => row.sessionId === stamped), confirmed);

  rollout(second, "10:10", 40);
  transcript(second, "019eaaaa-2222-7333-8444-666666666666", "10:10");
  await scanAt("10:20", buffer);
  all = rows(buffer);
  const newer = all.filter(row => row.sessionId === second);
  assert.ok(newer.filter(row => row.source === "codex").every(row =>
    row.metadata["user.account_id"] === providerAccountKey(codexIds[1]!)));
  assert.ok(newer.filter(row => row.source === "claude_code").every(row =>
    row.metadata["user.account_uuid"] === providerAccountKey(claudeIds[1]!)));

  // An account can switch away and back between scans. Its final key may be
  // unchanged, but a changed auth-file signature makes the gap unprovable.
  writeAccounts(0, "10:30");
  rollout(gap, "10:32", 60);
  transcript(gap, "019eaaaa-2222-7333-8444-999999999999", "10:32");
  writeAccounts(1, "10:35");
  await scanAt("10:40", buffer);
  all = rows(buffer);
  const gapRows = all.filter(row => row.sessionId === gap);
  assert.ok(gapRows.some(row => row.eventType === "usage_rollout"));
  assert.ok(gapRows.some(row => row.eventType === "usage_transcript"));
  assert.equal(gapRows.filter(row => row.eventType === "plan_limit_observation").length, 0,
    "an account-switch gap must not emit a keyless plan-limit reading");
  assert.ok(gapRows.every(row => row.metadata["user.account_id"] === undefined &&
    row.metadata["user.account_uuid"] === undefined));

  const beforeReplay = JSON.stringify(all.filter(row => [stamped, first, second, gap].includes(row.sessionId)));
  const rowCount = all.length;
  buffer.database.prepare("delete from rollout_scan_state").run();
  await scanAt("10:50", buffer);
  all = rows(buffer);
  assert.equal(all.length, rowCount);
  assert.equal(JSON.stringify(all.filter(row => [stamped, first, second, gap].includes(row.sessionId))), beforeReplay);

  rollout(imported, "07:00", 80);
  transcript(imported, "019eaaaa-2222-7333-8444-777777777777", "07:00");
  await scanAt("11:00", buffer);
  const history = rows(buffer).filter(row => row.sessionId === imported);
  assert.ok(history.some(row => row.eventType === "usage_rollout"));
  assert.ok(history.some(row => row.eventType === "usage_transcript"));
  assert.ok(history.every(row => row.metadata["user.account_id"] === undefined &&
    row.metadata["user.account_uuid"] === undefined));

  const windows = buffer.database.prepare("select source, account_key from account_binding_windows").all() as
    Array<{ source: string; account_key: string }>;
  assert.equal(windows.length, 6);
  assert.ok(windows.every(row => /^sha256:[a-f0-9]{16}$/.test(row.account_key)));
  const files = [ledger, `${ledger}-wal`, `${ledger}-shm`].filter(fs.existsSync);
  assert.ok(files.every(file => [...codexIds, ...claudeIds].every(id => !fs.readFileSync(file).includes(id))));
  const pendingRows = JSON.stringify(buffer.listUnuploaded());
  assert.ok([...codexIds, ...claudeIds].every(id => !pendingRows.includes(id)));
  buffer.close(); buffer = undefined;
  console.log(JSON.stringify({ proof: "account-binding-history", delayedScan: true, replay: true,
    historyImport: true, priorKeyPreserved: true, newKeyBound: true, unboundHistoryHasNoKey: true,
    unseenReturnToSameKeyUnbound: true, rawIdsAbsentFromLedgerAndPendingRows: true, filesScanned: files.length }));
} finally {
  buffer?.close();
  fs.rmSync(root, { recursive: true, force: true });
}
}

main().catch(error => { console.error(error); process.exitCode = 1; });
