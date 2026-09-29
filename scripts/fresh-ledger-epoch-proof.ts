/** Copy-only rehearsal of the Studio0 B1 switch. Never reads a real provider home. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { loadOrCreateDeviceIdentity } from "../packages/collector-cli/src/device-identity";
import { createProfileCapture } from "../packages/collector-cli/src/profile-capture";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { grokUsageDocument } from "./lib/grok-usage-fixture";
import { useFixtureRoot } from "./lib/fixture-root";

const repo = path.resolve(import.meta.dirname, "..");
const cli = path.join(repo, "packages/collector-cli/src/cli.ts");
const tsx = path.join(repo, "node_modules/tsx/dist/loader.mjs");
const base = fs.realpathSync(process.env.PLIMSOLL_PROOF_HOME ?? os.tmpdir());
const sandbox = fs.mkdtempSync(path.join(base, "fresh-ledger-epoch-"));
const fixture = useFixtureRoot(sandbox, { home: path.join(sandbox, "home") });
const home = fixture.home;
const data = fixture.env.PLIMSOLL_HOME;
const ledger = path.join(data, "work-ledger.sqlite");
const tenant = "6f4dbf9e-2d9b-4a61-a379-670bc742918a";
const device = "dev_fixture-studio0";
const keyId = "key_fixture-studio0";
const session = (n: number) => `019e9000-0000-7000-8000-${String(n).padStart(12, "0")}`;
const checks: Array<{ name: string; pass: boolean; detail?: unknown }> = [];
function check(name: string, pass: boolean, detail?: unknown) {
  checks.push({ name, pass, ...(detail === undefined ? {} : { detail }) });
}
const sha = (bytes: Buffer) => crypto.createHash("sha256").update(bytes).digest("hex");
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function cliRun(...args: string[]) {
  return spawnSync(process.execPath, ["--import", tsx, cli, ...args], {
    cwd: repo, env: { ...process.env, ...fixture.env }, encoding: "utf8", timeout: 45_000,
  });
}

function writeClaude(root: CaptureRoot, id: string, at: string, tokens: number) {
  const file = path.join(root.directory, "proof-project", `${id}.jsonl`);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify({ type: "assistant", sessionId: id, timestamp: at,
    message: { id: `message-${id}`, model: "claude-opus-5", usage: { input_tokens: tokens, output_tokens: 1 } } })}\n`);
}

function writeCodex(root: CaptureRoot, id: string, at: string, tokens: number) {
  const file = path.join(root.directory, ...at.slice(0, 10).split("-"), `rollout-${id}.jsonl`);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, [
    { type: "session_meta", timestamp: at, payload: { id } },
    { type: "turn_context", timestamp: at, payload: { model: "gpt-5.5" } },
    ...[0, tokens].map((input) => ({ type: "event_msg", timestamp: at,
      payload: { type: "token_count", info: { total_token_usage: {
        input_tokens: input, cached_input_tokens: 0, output_tokens: 1, reasoning_output_tokens: 0,
      } } } })),
  ].map((row) => JSON.stringify(row)).join("\n") + "\n");
}

async function main() {
  fs.mkdirSync(data, { recursive: true, mode: 0o700 });
  loadOrCreateDeviceIdentity(home, { seed: { deviceId: device, keyId } });
  const identityPath = path.join(data, "device.identity.json");
  const identityBefore = JSON.parse(fs.readFileSync(identityPath, "utf8")) as { deviceId: string; keyId: string };
  const roots: CaptureRoot[] = Array.from({ length: 23 }, (_, index) => {
    const source = index < 12 ? "claude_code" : "codex";
    const directory = path.join(home, "synthetic-profiles", `profile-${index}`,
      source === "codex" ? "sessions" : "projects");
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    return { rootId: `root-${index}`, profileId: `profile-${index}`, installationEpochId: "",
      source, directory };
  });
  const oldStart = new Date(Date.now() - 3_600_000);
  const old = new LocalEventBuffer(ledger, { workspaceId: tenant, deviceId: device,
    enrollmentNow: () => oldStart });
  const epoch = old.workspaceBinding()!.currentInstallationEpochId!;
  check("old_ledger_has_epoch", Boolean(epoch));
  old.close();
  for (const root of roots) root.installationEpochId = epoch;
  const oldEventAt = new Date(Date.now() - 30_000).toISOString();
  writeClaude(roots[0]!, session(1), oldEventAt, 101);
  writeCodex(roots[12]!, session(2), oldEventAt, 102);
  const grokHome = path.join(home, ".grok");
  const grokSession = session(3);
  const grokDir = path.join(grokHome, "sessions", "%2F", grokSession);
  fs.mkdirSync(grokDir, { recursive: true });
  fs.writeFileSync(path.join(grokDir, "usage.json"), JSON.stringify(grokUsageDocument({
    sessionId: grokSession, updatedAt: oldEventAt, shape: "modern",
    turns: [{ turnNumber: 1, endedAt: oldEventAt, models: [{ model: "grok-4.7-build",
      input: 103, output: 1, cachedRead: 0, cacheCreation: 0, reasoning: 0, modelCalls: 1, costTicks: 1000 }] }],
  })));
  const config = collectorConfigSchema.parse({ port: 48319, tenantId: tenant, deviceId: device, keyId,
    installKey: "fixture-install-only", managed: true, captureRoots: roots });
  const configPath = path.join(data, "collector.config.json");
  fs.writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
  const configBefore = sha(fs.readFileSync(configPath));
  const oldOpen = new LocalEventBuffer(ledger, { workspaceId: tenant, deviceId: device });
  try {
    for (const [index, source] of (["claude_code", "codex"] as const).entries()) {
      const prior = aiInteractionEventSchema.parse({
        id: crypto.randomUUID(), tenantId: tenant, source,
        dataMode: "metadata", eventType: "assistant_response", observedAt: oldEventAt,
        sessionId: session(index + 1), inputTokens: 101 + index, outputTokens: 1,
        metadata: { installationEpochId: epoch, sourceEventId: `archived-${index}` },
      });
      assert.equal(oldOpen.append(prior), true);
    }
    const history = oldOpen.database.prepare("select count(*) as n from buffered_events where input_tokens > 0").get() as { n: number };
    check("old_ledger_contains_archived_provider_history", history.n === 2, { rows: history.n });
  } finally { oldOpen.close(); }
  // Step 4's keep-all ledger copy, then step 5's same-volume archive rename.
  const snapshot = path.join(sandbox, "update-snapshot");
  const archive = path.join(sandbox, "archived-old-ledger");
  fs.mkdirSync(snapshot); fs.mkdirSync(archive);
  fs.copyFileSync(ledger, path.join(snapshot, "work-ledger.sqlite"));
  const oldSha = sha(fs.readFileSync(ledger));
  for (const suffix of ["", "-wal", "-shm"]) {
    if (fs.existsSync(`${ledger}${suffix}`)) fs.renameSync(`${ledger}${suffix}`, path.join(archive, `work-ledger.sqlite${suffix}`));
  }
  check("old_ledger_archived_unchanged", oldSha === sha(fs.readFileSync(path.join(archive, "work-ledger.sqlite"))) &&
    oldSha === sha(fs.readFileSync(path.join(snapshot, "work-ledger.sqlite"))));
  // Step 6: only the preflight; epoch adoption happens when step 7 opens the ledger.
  const plan = cliRun("capture-roots", "epoch-plan", "--json");
  let planned: Record<string, unknown> = {};
  try { planned = JSON.parse(plan.stdout); } catch { /* main's command is absent */ }
  check("step6_preflight_agrees_on_23_roots_and_absent_ledger", plan.status === 0 &&
    planned.status === "capture_roots_epoch_plan" && planned.rootCount === 23 &&
    planned.installationEpochId === epoch && planned.ledgerAbsent === true,
    { code: plan.status, status: planned.status });
  const opened = cliRun("export", "--limit", "1");
  check("step7_cli_opens_fresh_ledger", opened.status === 0 && fs.existsSync(ledger),
    { code: opened.status, stderr: opened.stderr.slice(-250) });
  if (!fs.existsSync(ledger)) throw new Error(`fresh ledger did not open: ${opened.stderr.slice(-1500)} ${opened.stdout.slice(-500)}`);
  const fresh = new LocalEventBuffer(ledger, { workspaceId: tenant, deviceId: device });
  const binding = fresh.workspaceBinding()!;
  check("fresh_ledger_adopts_old_root_epoch_with_new_cutoff",
    binding.currentInstallationEpochId === epoch &&
    Date.parse(binding.currentInstallationEpochStartedAt!) > Date.parse(oldEventAt),
    { expectedEpoch: epoch, actualEpoch: binding.currentInstallationEpochId,
      oldStart: oldStart.toISOString(), freshStart: binding.currentInstallationEpochStartedAt });
  const identityAfter = JSON.parse(fs.readFileSync(identityPath, "utf8")) as { deviceId: string; keyId: string };
  check("config_device_and_keys_unchanged", sha(fs.readFileSync(configPath)) === configBefore &&
    identityAfter.deviceId === identityBefore.deviceId && identityAfter.keyId === identityBefore.keyId);
  const capture = createProfileCapture(fresh, config);
  try {
    let sweeps = 0;
    while (captureBaselineStatus(fresh.database).status !== "complete" && sweeps < 30) {
      for (const tailer of [capture.rollout, capture.transcript]) {
        await tailer.scan({ scope: "recent", automatic: { phase: "baseline", budget: new CaptureWorkBudget() } });
      }
      sweeps++;
    }
    const baseline = captureBaselineStatus(fresh.database);
    check("two_sweep_baseline_excludes_preexisting_files", baseline.status === "complete" &&
      baseline.sources.every((source) => source.excludedGenerations >= 1),
      { sweeps, sources: baseline.sources.map((source) => ({ source: source.source,
        status: source.status, excluded: source.excludedGenerations })) });
    await capture.rollout.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
    await capture.transcript.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
    const beforeNew = fresh.database.prepare("select count(*) as n from buffered_events").get() as { n: number };
    check("baseline_does_not_reread_old_root_history", beforeNew.n === 0, { rows: beforeNew.n });
    const grok = await capture.grok.scan({ budget: new CaptureWorkBudget() });
    const grokRows = fresh.database.prepare("select count(*) as n from buffered_events where source='grok'").get() as { n: number };
    check("grok_rewalk_refused_before_enrollment", grok.eventsAppended === 0 &&
      grok.enrollmentExcludedEvents >= 1 && grokRows.n === 0,
      { scanned: grok.recordsParsed, refused: grok.enrollmentExcludedEvents, rows: grokRows.n });
    await sleep(30);
    const forwardAt = new Date().toISOString();
    for (const [index, root] of roots.entries()) {
      if (root.source === "claude_code") writeClaude(root, session(4 + index), forwardAt, 104 + index);
      else writeCodex(root, session(4 + index), forwardAt, 104 + index);
    }
    for (let turn = 0; turn < 30; turn++) {
      await capture.rollout.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
      await capture.transcript.scan({ scope: "recent", automatic: { phase: "capture", budget: new CaptureWorkBudget() } });
      const count = fresh.database.prepare("select count(*) as n from buffered_events where source in ('codex','claude_code') and input_tokens > 0").get() as { n: number };
      if (count.n >= roots.length) break;
    }
    const rows = fresh.database.prepare(`select source, input_tokens as tokens, installation_epoch_id as epoch
      from buffered_events where source in ('codex','claude_code') and input_tokens > 0 order by source`).all() as
      Array<{ source: string; tokens: number; epoch: string }>;
    const reasons = roots.map((root) => fresh.eventAdmissionReason(forwardAt, root.installationEpochId));
    check("forward_all_23_roots_capture_with_zero_epoch_mismatch", rows.length === roots.length &&
      rows.every((row) => row.epoch === epoch) && new Set(rows.map((row) => row.tokens)).size === roots.length &&
      reasons.every((reason) => reason === null),
      { rows, reasons });
  } finally { capture.close(); fresh.close(); }
  const existingPath = path.join(sandbox, "existing-ledger.sqlite");
  const existing = new LocalEventBuffer(existingPath, { workspaceId: tenant, deviceId: device });
  const existingEpoch = existing.workspaceBinding()!.currentInstallationEpochId;
  existing.close();
  const reopened = new LocalEventBuffer(existingPath, { workspaceId: tenant, deviceId: device,
    freshCaptureRootEpoch: null });
  check("existing_ledger_binding_wins_over_conflicting_config",
    reopened.workspaceBinding()!.currentInstallationEpochId === existingEpoch);
  reopened.close();
  check("new_ledger_refuses_mixed_root_epochs", (() => {
    try {
      new LocalEventBuffer(path.join(sandbox, "mixed-ledger.sqlite"), { workspaceId: tenant,
        freshCaptureRootEpoch: null });
      return false;
    } catch (error) { return error instanceof Error && error.message === "fresh_ledger_capture_root_epochs_conflict"; }
  })());
  const originalConfigBytes = fs.readFileSync(configPath);
  config.captureRoots![0]!.installationEpochId = crypto.randomUUID();
  fs.writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`);
  const mixedPlan = cliRun("capture-roots", "epoch-plan", "--json");
  fs.writeFileSync(configPath, originalConfigBytes);
  let mixedPayload: Record<string, unknown> = {};
  try { mixedPayload = JSON.parse(mixedPlan.stdout); } catch { /* unchanged main lacks the preflight */ }
  check("step6_preflight_refuses_mixed_roots", mixedPlan.status === 1 &&
    mixedPayload.reason === "capture_root_epochs_conflict", { code: mixedPlan.status, reason: mixedPayload.reason });
  console.log(JSON.stringify({ schema: "plimsoll.fresh-ledger-epoch-proof/v1", variant: process.env.PROOF_VARIANT ?? "lane",
    oldEpoch: epoch, checks, passed: checks.filter((row) => row.pass).length,
    failed: checks.filter((row) => !row.pass).length }, null, 2));
  if (checks.some((row) => !row.pass)) process.exitCode = 1;
}

main().catch((error) => { console.error(error); process.exitCode = 1; }).finally(() => {
  fixture.restore();
  fs.rmSync(sandbox, { recursive: true, force: true });
});
