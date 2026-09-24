/**
 * eco-6hoxj.163.18 r5 — the r4 delta review's R4-S1: no tailer reads a
 * session entry through a symlink, so the capture claim must never vouch for
 * one. The tailers still follow no link; the coverage walk reports each link,
 * unfollowed, as uncovered. Each R4_S1 check fails on r4 (68ec5293) and passes
 * on r5. Real tailers, the maintenance loop's own coverage turn and the claim.
 *
 * - A symlinked Grok sessions directory, Claude project directory, Claude
 *   transcript and Codex day folder: the usage behind each lies in a gap.
 * - The walk records 60,000 links in turns of at most 250 ms.
 * - Not a gap: a symlinked `memory` folder in a Claude project (Claude Code's
 *   Markdown notes, the one link found on the fleet). Passes on both.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { captureFrontier } from "../packages/collector-cli/src/capture-frontier";
import type { CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { GrokUsageTailer } from "../packages/collector-cli/src/grok-usage-tailer";
import { DEFAULT_JSONL_TAILER_IO } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { grokUsageDocument } from "./lib/grok-usage-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("capture-claim-review-r5", 6);
const results: Array<{ name: string; passed: boolean; detail: Record<string, unknown> }> = [];
const check = (name: string, passed: boolean, detail: Record<string, unknown>) => {
  completion.check(name, passed);
  results.push({ name, passed, detail });
};

type Claim = {
  through: string | null;
  unattested?: string;
  pending: number;
  dead: number;
  gaps: Array<{ from: string; to: string }>;
};

// A wall clock that can be moved forward, for "time passes" without waiting.
const RealDate = Date;
let clockShiftMs = 0;
class ShiftedDate extends RealDate {
  constructor(...args: unknown[]) {
    if (args.length === 0) super(RealDate.now() + clockShiftMs);
    else super(...(args as [string | number | Date]));
  }
  static now() {
    return RealDate.now() + clockShiftMs;
  }
}
globalThis.Date = ShiftedDate as unknown as DateConstructor;

const HOUR = 3_600_000;
const DAY = 24 * HOUR;
const TENANT = "00000000-0000-4000-8000-0000000000c1";
const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capture-review-r5-proof-")));
let ledgerIndex = 0;

function ledger() {
  const epochStartMs = Date.now() - 10 * DAY;
  return new LocalEventBuffer(path.join(root, `ledger-${++ledgerIndex}.sqlite`), {
    workspaceId: TENANT, delivery: { enabled: true, limits: { maxOldestAgeDays: 3650 } },
    enrollmentNow: () => new Date(epochStartMs),
  });
}

function claimOf(buffer: LocalEventBuffer): Claim {
  buffer.delivery.migrateLegacy({ now: new Date() });
  const ids = (buffer.database.prepare(`select delivery_id as id from upload_outbox`).all() as Array<{ id: string }>).map((row) => row.id);
  const claim = (buffer.delivery as unknown as { captureClaim(...args: unknown[]): Claim | null })
    .captureClaim(ids, { pendingFiles: 0, oldestPendingMs: null, losses: [], unreadable: false });
  if (!claim) throw new Error("no claim");
  return claim;
}
const inGap = (claim: Claim, at: string) => claim.gaps.some((gap) => gap.from <= at && at <= gap.to);
/** The claim vouches for `at` without naming it as missing. */
const attests = (claim: Claim, at: string) => claim.through !== null && claim.through > at && !inGap(claim, at);
const captured = (buffer: LocalEventBuffer, needle: string) =>
  (buffer.database.prepare(`select count(*) as n from buffered_events where payload_json like ?`).get(`%${needle}%`) as { n: number }).n;

/** A Codex and a Claude capture root, their tailers, and two maintenance loops: one for cadences, one for coverage checks. */
function world(label: string, grok?: (buffer: LocalEventBuffer, base: string) => GrokUsageTailer) {
  const base = path.join(root, label);
  const buffer = ledger();
  const epochId = buffer.workspaceBinding()!.currentInstallationEpochId!;
  const list: CaptureRoot[] = [
    { source: "codex", rootId: "codex-0", profileId: "profile-codex-0", directory: path.join(base, "codex"), installationEpochId: epochId },
    { source: "claude_code", rootId: "claude-0", profileId: "profile-claude-0", directory: path.join(base, "claude"), installationEpochId: epochId },
  ];
  for (const captureRoot of list) fs.mkdirSync(captureRoot.directory, { recursive: true });
  const rollout = new RolloutTailer(buffer, undefined, () => [], DEFAULT_JSONL_TAILER_IO, list.filter((r) => r.source === "codex"));
  const transcript = new TranscriptTailer(buffer, undefined, DEFAULT_JSONL_TAILER_IO, list.filter((r) => r.source === "claude_code"));
  const grokTailer = grok?.(buffer, base);
  // 0.7.37 put a Grok tailer before the options.
  const maintenance = (intervalMs: number) => new CollectorMaintenance(buffer, rollout, transcript, undefined, grokTailer,
    { captureCoverageIntervalMs: intervalMs });
  const cadences = maintenance(365 * DAY);
  const checks = maintenance(0);
  /** One coverage turn of the maintenance loop: at most 250 ms of walking. */
  const turn = () => (checks as unknown as { checkCaptureCoverage(): void }).checkCaptureCoverage();
  const elsewhere = path.join(base, "elsewhere");
  fs.mkdirSync(elsewhere, { recursive: true });
  return {
    base, buffer, codex: list[0]!.directory, claude: list[1]!.directory, elsewhere, grok: grokTailer, turn,
    cadence: () => cadences.runRecent({ onDurableCommit: () => true, onProgress: () => true }),
    /** Coverage checks, each walked to completion (the fixtures are small). */
    coverage: () => {
      for (let index = 0; index < 4; index += 1) turn();
    },
    close: () => {
      cadences.close();
      checks.close();
      buffer.close();
    },
  };
}
type World = ReturnType<typeof world>;

async function completeBaseline(scene: World) {
  for (let cadence = 0; cadence < 40 && captureBaselineStatus(scene.buffer.database).status !== "complete"; cadence += 1) {
    await scene.cadence();
  }
}

/**
 * The usage is written, the tailers' cadences run, a check sees the link, and
 * three hours later another check runs: long past the hold limit.
 */
async function laterClaim(scene: World) {
  for (let cadence = 0; cadence < 4; cadence += 1) await scene.cadence();
  scene.coverage();
  clockShiftMs = 3 * HOUR;
  for (let cadence = 0; cadence < 2; cadence += 1) await scene.cadence();
  scene.coverage();
  const claim = claimOf(scene.buffer);
  clockShiftMs = 0;
  return claim;
}

function transcriptLine(session: string, at: string) {
  return `${JSON.stringify({ type: "assistant", timestamp: at, sessionId: session,
    message: { id: `${session}-1`, model: "claude-opus-5", usage: { input_tokens: 700, output_tokens: 90 } } })}\n`;
}

async function grokSessionsDirectory() {
  const scene = world("grok-sessions", (buffer, base) => new GrokUsageTailer(buffer, path.join(base, "grok-home")));
  // ~/.grok/sessions moved to another volume and linked back.
  const target = path.join(scene.elsewhere, "grok-sessions");
  fs.mkdirSync(target, { recursive: true });
  fs.mkdirSync(path.join(scene.base, "grok-home"), { recursive: true });
  fs.symlinkSync(target, path.join(scene.base, "grok-home", "sessions"));
  const endedAt = new Date().toISOString();
  const sessionId = "88888888-8888-4888-8888-888888888888";
  const session = path.join(target, "%2FUsers%2Ffixture%2Fproject", sessionId);
  fs.mkdirSync(session, { recursive: true });
  fs.writeFileSync(path.join(session, "usage.json"), JSON.stringify(grokUsageDocument({
    sessionId, updatedAt: endedAt, shape: "modern",
    turns: [{ turnNumber: 1, endedAt, models: [{ model: "grok-4", input: 5_000, cachedRead: 0, cacheCreation: 0,
      output: 900, reasoning: 0, modelCalls: 1, costTicks: 9_000_000 }] }],
  })));
  const scan = await scene.grok!.scan({ budget: new CaptureWorkBudget(), now: new Date() });
  const claim = await laterClaim(scene);
  const grokEvents = (scene.buffer.database.prepare(`select count(*) as n from buffered_events where source = 'grok'`).get() as { n: number }).n;
  scene.close();
  check("R4_S1_symlinked_grok_sessions_directory_is_never_attested",
    grokEvents === 0 && claim.through !== null && claim.through > endedAt && inGap(claim, endedAt),
    { endedAt, grokFilesSeen: scan.filesSeen, grokEvents, attested: attests(claim, endedAt), claim });
}

async function claudeProjectDirectory() {
  const scene = world("claude-project");
  await completeBaseline(scene);
  const session = "bbbbbbbb-2222-4222-8222-222222222222";
  const usedAt = new Date().toISOString();
  const target = path.join(scene.elsewhere, "project");
  fs.mkdirSync(target, { recursive: true });
  fs.writeFileSync(path.join(target, `${session}.jsonl`), transcriptLine(session, usedAt));
  fs.symlinkSync(target, path.join(scene.claude, "-Users-fixture-linked"));
  const claim = await laterClaim(scene);
  const events = captured(scene.buffer, session);
  scene.close();
  check("R4_S1_symlinked_claude_project_directory_is_never_attested",
    events === 0 && claim.through !== null && claim.through > usedAt && inGap(claim, usedAt),
    { usedAt, captured: events, attested: attests(claim, usedAt), claim });
}

async function claudeTranscriptFile() {
  const scene = world("claude-transcript");
  await completeBaseline(scene);
  const session = "cccccccc-3333-4333-8333-333333333333";
  const usedAt = new Date().toISOString();
  const project = path.join(scene.claude, "-Users-fixture-project");
  fs.mkdirSync(project, { recursive: true });
  const target = path.join(scene.elsewhere, `${session}.jsonl`);
  fs.writeFileSync(target, transcriptLine(session, usedAt));
  fs.symlinkSync(target, path.join(project, `${session}.jsonl`));
  const claim = await laterClaim(scene);
  const events = captured(scene.buffer, session);
  scene.close();
  check("R4_S1_symlinked_claude_transcript_is_never_attested",
    events === 0 && claim.through !== null && claim.through > usedAt && inGap(claim, usedAt),
    { usedAt, captured: events, attested: attests(claim, usedAt), claim });
}

async function codexDayFolder() {
  const scene = world("codex-day");
  await completeBaseline(scene);
  const session = "dddddddd-4444-4444-8444-444444444444";
  const usedAt = new Date().toISOString();
  const target = path.join(scene.elsewhere, "day");
  fs.mkdirSync(target, { recursive: true });
  const lines = [
    { type: "session_meta", timestamp: usedAt, payload: { id: session } },
    { type: "turn_context", timestamp: usedAt, payload: { model: "gpt-5.5" } },
    { type: "event_msg", timestamp: usedAt, payload: { type: "token_count", info: {
      total_token_usage: { input_tokens: 800, cached_input_tokens: 0, output_tokens: 80, reasoning_output_tokens: 0 } } } },
  ];
  fs.writeFileSync(path.join(target, `rollout-${usedAt.slice(0, 19).replace(/:/g, "-")}-${session}.jsonl`),
    lines.map((line) => JSON.stringify(line)).join("\n") + "\n");
  const [year, month, day] = usedAt.slice(0, 10).split("-") as [string, string, string];
  fs.mkdirSync(path.join(scene.codex, year, month), { recursive: true });
  fs.symlinkSync(target, path.join(scene.codex, year, month, day));
  const claim = await laterClaim(scene);
  const events = captured(scene.buffer, session);
  scene.close();
  check("R4_S1_symlinked_codex_day_folder_is_never_attested",
    events === 0 && claim.through !== null && claim.through > usedAt && inGap(claim, usedAt),
    { usedAt, captured: events, attested: attests(claim, usedAt), claim });
}

async function manyLinks() {
  // 20,000 links per source, where each tailer would read: symlinked rollouts,
  // transcripts, and Grok session directories.
  const perSource = 20_000;
  const scene = world("many-links", (buffer, base) => new GrokUsageTailer(buffer, path.join(base, "grok-home")));
  const targetFile = path.join(scene.elsewhere, "target.jsonl");
  const targetDirectory = path.join(scene.elsewhere, "session");
  fs.writeFileSync(targetFile, "{}\n");
  fs.mkdirSync(targetDirectory, { recursive: true });
  const built = performance.now();
  const now = Date.now();
  for (let index = 0; index < perSource; index += 1) {
    const group = Math.floor(index / 100);
    const codexDay = path.join(scene.codex, ...new Date(now - (1 + group) * DAY).toISOString().slice(0, 10).split("-"));
    const claudeProject = path.join(scene.claude, `project-${group}`);
    const grokGroup = path.join(scene.base, "grok-home", "sessions", `group-${group}`);
    if (index % 100 === 0) for (const directory of [codexDay, claudeProject, grokGroup]) fs.mkdirSync(directory, { recursive: true });
    fs.symlinkSync(targetFile, path.join(codexDay, `rollout-2026-01-01T00-00-00-${String(index).padStart(12, "0")}.jsonl`));
    fs.symlinkSync(targetFile, path.join(claudeProject, `${String(index).padStart(8, "0")}-0000-4000-8000-000000000000.jsonl`));
    fs.symlinkSync(targetDirectory, path.join(grokGroup, `session-${index}`));
  }
  const buildSeconds = Number(((performance.now() - built) / 1000).toFixed(1));
  const turns: number[] = [];
  let frontier: { capturedThrough: string | null } | null = null;
  for (let turn = 0; turn < 400; turn += 1) {
    const started = performance.now();
    scene.turn();
    turns.push(Number((performance.now() - started).toFixed(1)));
    frontier = captureFrontier(scene.buffer.database);
    if (frontier?.capturedThrough) break;
  }
  const rows = (scene.buffer.database.prepare(`select count(*) as n from capture_uncovered_files`).get() as { n: number }).n;
  const maxTurnMs = Math.max(...turns);
  scene.close();
  check("R4_S1_walk_records_60000_links_in_turns_of_at_most_250ms",
    maxTurnMs <= 250 && frontier?.capturedThrough != null && rows === 3 * perSource && turns.length > 1,
    { linksPerSource: perSource, buildSeconds, turns: turns.length, maxTurnMs,
      totalMs: Number(turns.reduce((total, ms) => total + ms, 0).toFixed(1)), uncoveredRows: rows,
      frontier: frontier?.capturedThrough ?? null });
}

async function claudeMemoryNotes() {
  const scene = world("claude-memory");
  await completeBaseline(scene);
  // Studio0's shape: a project whose `memory` notes folder is a symlink.
  const session = "eeeeeeee-5555-4555-8555-555555555555";
  const usedAt = new Date().toISOString();
  const project = path.join(scene.claude, "-Users-fixture");
  fs.mkdirSync(project, { recursive: true });
  fs.writeFileSync(path.join(project, `${session}.jsonl`), transcriptLine(session, usedAt));
  const notes = path.join(scene.elsewhere, "notes");
  fs.mkdirSync(notes, { recursive: true });
  fs.writeFileSync(path.join(notes, "MEMORY.md"), "# Notes\n");
  fs.symlinkSync(notes, path.join(project, "memory"));
  const claim = await laterClaim(scene);
  const events = captured(scene.buffer, session);
  scene.close();
  check("Symlinked_memory_notes_folder_in_a_claude_project_is_not_a_gap",
    events > 0 && attests(claim, usedAt) && claim.gaps.length === 0,
    { usedAt, captured: events, claim });
}

async function main() {
  for (const step of [grokSessionsDirectory, claudeProjectDirectory, claudeTranscriptFile, codexDayFolder, manyLinks,
    claudeMemoryNotes]) {
    try {
      await step();
    } catch (error) {
      clockShiftMs = 0;
      check(step.name, false, { error: error instanceof Error ? `${error.name}: ${error.message}` : String(error) });
    }
  }
  console.log(JSON.stringify({ proof: "capture-claim-review-r5", results }, null, 2));
  fs.rmSync(root, { recursive: true, force: true });
  if (results.every((result) => result.passed)) completion.complete();
  else process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
