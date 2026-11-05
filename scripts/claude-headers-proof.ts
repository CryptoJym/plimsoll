/**
 * Claude Code telemetry token out of settings.json (plimsoll N60, bead
 * eco-754vw6.46): one isolated flow over the real CLI and a loopback collector.
 *
 * `setup --yes` writes the private header (0600) and the OTLP helper (0700)
 * and token-free home and seat settings; a seat seeded with the inline http
 * form plus the legacy curl form is migrated with a backup and its operator
 * hook kept; the generated Stop hook posts 202 and writes the parity receipt;
 * the helper prints the headers Claude Code sends; doctor reads valid, then
 * incomplete after a public header, and `setup --reconcile` repairs it;
 * `rotate-producer-token --source claude_code` rewrites header and helper,
 * accepts the old token in grace and refuses it after the deadline while the
 * new one is accepted; a second `setup --yes` is a no-op; no command output
 * carries a token. Every path is under a disposable fixture root.
 */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { AddressInfo } from "node:net";

import { useFixtureRoot } from "./lib/fixture-root";
import { claudeTelemetryPaths } from "../packages/collector-config/src/index";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateLocalIngestAuth, readLocalIngestAuth } from "../packages/collector-cli/src/local-auth";
import { createCollectorServer } from "../packages/collector-cli/src/server";

const repoRoot = path.resolve(import.meta.dirname, "..");
const cli = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const loader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
const checks: Array<{ name: string; passed: true; detail: Record<string, unknown> }> = [];
const secrets: string[] = [];
const outputs: string[] = [];

function check(name: string, passed: boolean, detail: Record<string, unknown> = {}) {
  assert.ok(passed, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, passed: true, detail });
}

async function main() {
  const started = performance.now();
  // A home with a space and an apostrophe: the generated command and helper quote it.
  const sandbox = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "plimsoll-claude-headers-"));
  const home = path.join(sandbox, "home seat's");
  const store = path.join(home, ".plimsoll");
  const fixture = useFixtureRoot(sandbox, { home, plimsollHome: store });
  fs.mkdirSync(store, { recursive: true, mode: 0o700 });
  const settingsFile = path.join(home, ".claude", "settings.json");
  const files = claudeTelemetryPaths(settingsFile, store);
  const seatFile = path.join(home, ".claude-seats", "x", "settings.json");
  const parityFile = path.join(store, "producer-parity", "hooks.jsonl");
  const initial = loadOrCreateLocalIngestAuth(store);
  secrets.push(initial.claudeCodeProducer);
  const buffer = new LocalEventBuffer(path.join(sandbox, "ledger.sqlite"));
  let clockOffsetMs = 0;
  const server = createCollectorServer(collectorConfigSchema.parse({}), buffer, {
    localAuth: initial, localAuthHome: store, producerAuthNowMs: () => Date.now() + clockOffsetMs,
    env: { ...process.env, PLIMSOLL_HOOK_SPOOL: "off", PLIMSOLL_HOME: store },
  });
  const env = {
    PATH: process.env.PATH, LANG: "en_US.UTF-8", TZ: "UTC", ...fixture.env,
    PLIMSOLL_COLLECTOR_DOCTOR_TIMEOUT_MS: "500",
  };
  const run = (command: string, args: string[], input = "", secretOutput = false) => {
    const result = spawnSync(command, args, { cwd: repoRoot, env, input, encoding: "utf8", timeout: 180_000 });
    const out = `${result.stdout ?? ""}${result.stderr ?? ""}`;
    if (!secretOutput) {
      outputs.push(out);
      assert.ok(secrets.every((token) => !out.includes(token)), "command output is secret-free");
    }
    return { code: result.status, out, stdout: result.stdout ?? "" };
  };
  const plimsoll = (...args: string[]) => run(process.execPath, ["--import", loader, cli, ...args]);
  // The collector answers from this process, so a hook that must reach it runs
  // without blocking the event loop (a blocking spawn would starve the server).
  const runShell = (command: string, input: string) => new Promise<{ code: number | null; out: string }>((resolve, reject) => {
    const child = spawn("/bin/sh", ["-c", command], { cwd: repoRoot, env, stdio: ["pipe", "pipe", "pipe"] });
    let out = "";
    child.stdout.on("data", (chunk) => { out += chunk; });
    child.stderr.on("data", (chunk) => { out += chunk; });
    child.once("error", reject);
    child.once("close", (code) => {
      outputs.push(out);
      resolve({ code, out });
    });
    child.stdin.end(input);
  });
  const lastJson = (stdout: string) => {
    const start = stdout.lastIndexOf("\n{");
    return JSON.parse(start === -1 ? stdout.slice(stdout.indexOf("{")) : stdout.slice(start + 1)) as Record<string, any>;
  };
  const json = (file: string) => JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, any>;
  const mode = (file: string) => fs.statSync(file).mode & 0o777;
  const headerMatches = (token: string) => fs.readFileSync(files.claudeHeaderFile, "utf8") === `x-plimsoll-token: ${token}\n`;
  try {
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;
    fs.writeFileSync(path.join(store, "collector.config.json"), `${JSON.stringify({ port })}\n`, { mode: 0o600 });
    const url = `http://127.0.0.1:${port}/hooks/claude-code`;
    const legacy = `/usr/bin/curl -sS -X POST -H 'Content-Type: application/json' -H 'x-plimsoll-token: ${initial.claudeCodeProducer}' --data-binary @- ${url} || true`;
    const seeded = (matcher = false) => [{ ...(matcher ? { matcher: ".*" } : {}), hooks: [
      { type: "http", url, headers: { "x-plimsoll-source": "claude_code", "x-plimsoll-token": initial.claudeCodeProducer }, timeout: 5, retry: 2 },
      { type: "command", command: legacy },
      { type: "command", command: "printf operator-hook" },
    ] }];
    fs.mkdirSync(path.dirname(seatFile), { recursive: true, mode: 0o700 });
    fs.mkdirSync(path.join(home, ".claude-seats", ".claude.json.lock"), { mode: 0o700 });
    fs.writeFileSync(seatFile, `${JSON.stringify({
      env: { OTEL_EXPORTER_OTLP_HEADERS: `x-plimsoll-source=claude_code,x-plimsoll-token=${initial.claudeCodeProducer}` },
      hooks: { Stop: seeded(), UserPromptSubmit: seeded(), PostToolUse: seeded(true) },
      theme: "dark",
    }, null, 2)}\n`, { mode: 0o600 });

    const setup = plimsoll("setup", "--yes");
    check("setup_writes_header_helper_and_token_free_settings",
      setup.code === 0 && mode(files.claudeHeaderFile) === 0o600 && headerMatches(initial.claudeCodeProducer) &&
        mode(files.claudeOtelHeadersHelperFile) === 0o700 &&
        [settingsFile, seatFile].every((file) => !fs.readFileSync(file, "utf8").includes(initial.claudeCodeProducer)) &&
        json(settingsFile).env.OTEL_EXPORTER_OTLP_HEADERS === "x-plimsoll-source=claude_code",
      { exit: setup.code, seatSlugs: (lastJson(setup.stdout) as any) && Object.keys(lastJson(setup.stdout)).filter((key) => key.startsWith("claudeSeat[")) });
    const seat = json(seatFile);
    const handlers = (event: string) => (seat.hooks[event] as any[]).flatMap((group) => group.hooks as any[]);
    check("seat_migrates_inline_http_and_legacy_curl_hooks_keeps_the_operator_hook_and_backs_up",
      seat.theme === "dark" && seat.otelHeadersHelper === json(settingsFile).otelHeadersHelper &&
        ["Stop", "UserPromptSubmit", "PostToolUse"].every((event) =>
          handlers(event).length === 2 && handlers(event).every((handler) => handler.type === "command") &&
          handlers(event).some((handler) => handler.command === "printf operator-hook")) &&
        !Object.keys(seat.hooks).includes(".claude.json.lock") &&
        fs.readdirSync(path.dirname(seatFile)).some((name) => name.includes(".plimsoll-backup-")) &&
        !fs.existsSync(path.join(home, ".claude-seats", ".claude.json.lock", "settings.json")),
      { events: Object.keys(seat.hooks) });

    const helper = run("/bin/sh", ["-c", json(settingsFile).otelHeadersHelper], "", true);
    const printed = JSON.parse(helper.out) as Record<string, string>;
    check("otlp_helper_prints_the_source_and_current_token_as_json",
      helper.code === 0 && printed["x-plimsoll-source"] === "claude_code" && printed["x-plimsoll-token"] === initial.claudeCodeProducer,
      { keys: Object.keys(printed).sort() });

    const body = JSON.stringify({ hook_event_name: "Stop", session_id: crypto.randomUUID(), cwd: home, stop_hook_active: false });
    const post = async (token: string) => {
      const response = await fetch(url, { method: "POST", headers: { "content-type": "application/json", "x-plimsoll-source": "claude_code", "x-plimsoll-token": token }, body });
      await response.arrayBuffer();
      return response.status;
    };
    // The live collector is long-running and warm; the first write to a fresh
    // ledger can take longer than the hook's 2-second curl deadline.
    const warmStarted = performance.now();
    const warm = await post(initial.claudeCodeProducer);
    const warmMs = Math.round(performance.now() - warmStarted);
    const hook = () => runShell(json(settingsFile).hooks.Stop[0].hooks[0].command, body);
    const first = await hook();
    const receipt = JSON.parse(fs.readFileSync(parityFile, "utf8").trim().split("\n").pop()!) as Record<string, unknown>;
    check("generated_stop_hook_posts_202_and_writes_the_parity_receipt",
      first.code === 0 && receipt.http === "202" && receipt.src === "claude_code" && receipt.v === 1,
      { exit: first.code, http: receipt.http, curlExit: receipt.exit, warm, warmMs });

    const doctor = () => lastJson(plimsoll("doctor", "--read-only", "--json").stdout);
    check("doctor_reads_claude_valid", doctor().telemetry.claude.status === "valid");
    fs.chmodSync(files.claudeHeaderFile, 0o644);
    const exposed = doctor();
    check("doctor_reads_incomplete_when_the_header_is_public",
      exposed.telemetry.claude.status === "incomplete" && exposed.telemetry.claude.missing.includes("headerFile"),
      { missing: exposed.telemetry.claude.missing });
    const reconcile = plimsoll("setup", "--reconcile");
    check("reconcile_repairs_the_header_mode", reconcile.code === 0 && mode(files.claudeHeaderFile) === 0o600, { exit: reconcile.code });

    const rotate = plimsoll("rotate-producer-token", "--source", "claude_code", "--grace-seconds", "60");
    const rotated = readLocalIngestAuth(store)!;
    secrets.push(rotated.claudeCodeProducer);
    const rotatedHelper = run("/bin/sh", ["-c", json(settingsFile).otelHeadersHelper], "", true);
    check("rotation_rewrites_the_header_keeps_the_old_token_in_grace_and_the_helper_follows",
      rotate.code === 0 && lastJson(rotate.stdout).status === "rotation_applied" &&
        rotated.claudeCodeProducer !== initial.claudeCodeProducer && headerMatches(rotated.claudeCodeProducer) &&
        rotated.rotations?.claude_code?.token === initial.claudeCodeProducer &&
        await post(initial.claudeCodeProducer) === 202 && (await hook()).code === 0 &&
        (JSON.parse(rotatedHelper.out) as Record<string, string>)["x-plimsoll-token"] === rotated.claudeCodeProducer,
      { status: lastJson(rotate.stdout).status, targets: (lastJson(rotate.stdout).targets as any[]).map((target) => target.status) });
    clockOffsetMs = rotated.rotations!.claude_code!.expiresAt + 1 - Date.now();
    check("superseded_token_is_refused_after_the_deadline_and_the_current_one_accepted",
      await post(initial.claudeCodeProducer) === 401 && await post(rotated.claudeCodeProducer) === 202);
    clockOffsetMs = 0;
    const again = plimsoll("setup", "--yes");
    check("a_second_setup_is_a_no_op", again.code === 0 && lastJson(again.stdout).status === "setup_noop", { status: lastJson(again.stdout).status });
    check("no_settings_file_or_command_output_carries_a_token",
      [settingsFile, seatFile].every((file) => secrets.every((token) => !fs.readFileSync(file, "utf8").includes(token))) &&
        outputs.every((out) => secrets.every((token) => !out.includes(token))),
      { outputs: outputs.length });
    const elapsed = performance.now() - started;
    check("the_proof_runs_under_sixty_seconds", elapsed < 60_000, { ms: Math.round(elapsed) });
    console.log(JSON.stringify({ proof: "claude-headers", checks }, null, 2));
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    buffer.close();
    fixture.restore();
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
