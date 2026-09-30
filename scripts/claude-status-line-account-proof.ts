import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { providerAccountKey } from "../packages/shared/src/policy";

const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "plimsoll-claude-status-"));
const home = path.join(root, "home");
const defaultDir = path.join(root, "selected-default");
const alternate = path.join(root, "alternate");
const collectorHome = path.join(home, ".plimsoll");
const node = process.execPath;
const loader = path.resolve("node_modules/tsx/dist/loader.mjs");
const cli = path.resolve("packages/collector-cli/src/cli.ts");
const account = "fixture-claude-alternate-account";
const original = Buffer.from("{\n  \"statusLine\" : { \"type\": \"command\", \"command\": \"cat >/dev/null; printf 'EXACT STATUS'\" },\n  \"otherSetting\" : true\n}\n");
const env = { ...process.env, HOME: home, USERPROFILE: home, PLIMSOLL_HOME: collectorHome,
  PLIMSOLL_FIXTURE_ROOT: root,
  CLAUDE_CONFIG_DIR: defaultDir, XDG_CONFIG_HOME: path.join(home, ".config"),
  XDG_CACHE_HOME: path.join(home, ".cache"), XDG_STATE_HOME: path.join(home, ".local", "state"),
  TMPDIR: path.join(home, "tmp"), NEXT_TELEMETRY_DISABLED: "1" };
const runCli = (...args: string[]) => spawnSync(node, ["--import", loader, cli, ...args], { env, encoding: "utf8", timeout: 30_000 });

try {
  for (const dir of [home, defaultDir, alternate, collectorHome, env.TMPDIR]) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(defaultDir, ".claude.json"), JSON.stringify({ oauthAccount: { accountUuid: "fixture-claude-default-account" } }));
  fs.writeFileSync(path.join(alternate, ".claude.json"), JSON.stringify({ oauthAccount: { accountUuid: account } }));
  fs.writeFileSync(path.join(alternate, "settings.json"), original);
  const setup = runCli("setup", "claude-status-line", "--config-dir", alternate);
  assert.equal(setup.status, 0, setup.stderr);
  const defaultSettings = JSON.parse(fs.readFileSync(path.join(defaultDir, "settings.json"), "utf8"));
  const settings = JSON.parse(fs.readFileSync(path.join(alternate, "settings.json"), "utf8"));
  assert.match(defaultSettings.statusLine.command, /__plimsoll-capacity-statusline-proxy/);
  assert.match(settings.statusLine.command, /__plimsoll-capacity-statusline-proxy/);
  assert.equal(fs.existsSync(path.join(home, ".claude", "settings.json")), false);
  const input = JSON.stringify({ rate_limits: {
    five_hour: { used_percentage: 76.4, resets_at: 1790676000 },
    seven_day: { used_percentage: 47, resets_at: 1791000000 },
    seven_day_sonnet: { used_percentage: 5, resets_at: 1791000000 },
    seven_day_opus: { used_percentage: 101.3, resets_at: 1791000000 },
  } });
  const proxy = spawnSync("/bin/sh", ["-c", settings.statusLine.command], { env, input, encoding: "utf8", timeout: 30_000 });
  assert.equal(proxy.status, 0, proxy.stderr);
  assert.equal(proxy.stdout, "EXACT STATUS");
  const defaultProxy = spawnSync("/bin/sh", ["-c", defaultSettings.statusLine.command],
    { env, input, encoding: "utf8", timeout: 30_000 });
  assert.equal(defaultProxy.status, 0, defaultProxy.stderr);
  const buffer = new LocalEventBuffer(path.join(collectorHome, "work-ledger.sqlite"));
  try {
    const rows = (buffer.database.prepare("select payload_json from buffered_events where event_type='plan_limit_observation'").all() as Array<{ payload_json: string }>).map(row => JSON.parse(row.payload_json));
    const alternateRows = rows.filter(row => row.metadata["user.account_uuid"] === providerAccountKey(account));
    const defaultRows = rows.filter(row => row.metadata["user.account_uuid"] === providerAccountKey("fixture-claude-default-account"));
    assert.deepEqual(alternateRows.map(row => row.metadata.planLimitWindow).sort(), ["five_hour", "weekly", "weekly_opus", "weekly_sonnet"]);
    assert.equal(defaultRows.length, 4);
    assert.ok(rows.every(row =>
      row.metadata.planLimitSource === "claude_status_line" && row.inputTokens === undefined && row.outputTokens === undefined));
    assert.equal(alternateRows.find(row => row.metadata.planLimitWindow === "weekly_opus")?.metadata.planLimitUsedPercent, 101.3);
  } finally { buffer.close(); }
  const uninstall = runCli("setup", "claude-status-line", "--config-dir", alternate, "--uninstall");
  assert.equal(uninstall.status, 0, uninstall.stderr);
  assert.deepEqual(fs.readFileSync(path.join(alternate, "settings.json")), original);
  assert.equal(fs.existsSync(path.join(defaultDir, "settings.json")), false);
  const outside = `${root}-outside`;
  const refused = runCli("setup", "claude-status-line", "--config-dir", outside, "--uninstall");
  assert.notEqual(refused.status, 0);
  assert.equal(fs.existsSync(outside), false);
  console.log(JSON.stringify({ proof: "claude-status-line-account", checks: 14, passed: 14, failed: 0 }));
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
