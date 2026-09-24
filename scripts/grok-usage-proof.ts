/**
 * Grok usage capture proof (bead eco-6hoxj.163.20).
 *
 * Grok Build writes its own billed usage to
 * `<GROK_HOME>/sessions/<url-encoded cwd>/<session id>/usage.json`. This proof
 * builds a Grok home from generated documents with the documented key/type
 * shapes, runs the production maintenance wiring (profile capture plus the
 * worker's CollectorMaintenance construction) and proves: first-run backfill,
 * exact totals against Grok's own numbers, incremental turns, rewrites,
 * multi-model turns, incomplete usage, bounds and deferral, planted-content
 * privacy, project attribution from the encoded directory, and acceptance by
 * the shared ingest schema the cloud validates.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { createProofCompletion } from "./lib/proof-completion";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import {
  GROK_SESSION_CONTENT_FILES,
  GROK_USAGE_DOCUMENTED_SHAPES,
  canonicalShape,
  grokUsageDocument,
  valueBlindShape,
  type FixtureModelUsage,
  type FixtureSession,
  type FixtureTurn,
} from "./lib/grok-usage-fixture";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { AUTOMATIC_CAPTURE_LIMITS, CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { resolveGitContextUncached } from "../packages/collector-cli/src/git-context";
import { historyCoverageStatus } from "../packages/collector-cli/src/history-coverage";
import { CollectorMaintenance, type CollectorMaintenanceRunResult } from "../packages/collector-cli/src/maintenance";
import { projectMaintenanceResult } from "../packages/collector-cli/src/maintenance-protocol";
import { deterministicEventId } from "../packages/collector-cli/src/normalizer";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";
import { createProfileCapture } from "../packages/collector-cli/src/profile-capture";
import { resolveMaintenanceRepoContexts } from "../packages/collector-cli/src/maintenance-worker";
import { uploadBufferedEvents } from "../packages/collector-cli/src/upload";
import { aiInteractionEventSchema, aiWorkIngestBatchSchema } from "../packages/shared/src/index";

const EXPECTED_CHECKS = 40;
const completion = createProofCompletion("grok-usage", EXPECTED_CHECKS);
const SENTINEL = "PLIMSOLL_GROK_CONTENT_SENTINEL_5c1e";
const TICKS_PER_USD = 10_000_000_000;
const results: Array<{ name: string; detail: Record<string, unknown> }> = [];
let failures = 0;

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  completion.check(name, Boolean(condition));
  results.push({ name, detail });
  if (!condition) {
    failures += 1;
    console.error(JSON.stringify({ failed: name, detail }, null, 2));
  }
}

type GrokRow = {
  id: string;
  sessionId: string;
  model: string | null;
  eventType: string;
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheCreationTokens: number | null;
  costUsd: number | null;
  costKind: string | null;
  observedAt: string;
  repoHash: string | null;
  payloadJson: string;
  suppressedFieldsJson: string;
};

function grokRows(buffer: LocalEventBuffer, sessionId?: string): GrokRow[] {
  return buffer.database.prepare(
    `select id, session_id as sessionId, model, event_type as eventType,
       input_tokens as inputTokens, output_tokens as outputTokens,
       cache_read_tokens as cacheReadTokens, cache_creation_tokens as cacheCreationTokens,
       cost_usd as costUsd, cost_kind as costKind, observed_at as observedAt,
       repo_hash as repoHash, payload_json as payloadJson,
       suppressed_fields_json as suppressedFieldsJson
     from buffered_events
     where source = 'grok' and (input_tokens is not null or output_tokens is not null)
       ${sessionId ? "and session_id = ?" : ""}
     order by observed_at, id`,
  ).all(...(sessionId ? [sessionId] : [])) as GrokRow[];
}

function metadataOf(row: GrokRow) {
  return (JSON.parse(row.payloadJson) as { metadata: Record<string, unknown> }).metadata;
}

type Sums = { input: number; cachedRead: number; cacheCreation: number; output: number; reasoning: number; ticks: number };

function ledgerSums(rows: GrokRow[]): Sums {
  const sums: Sums = { input: 0, cachedRead: 0, cacheCreation: 0, output: 0, reasoning: 0, ticks: 0 };
  for (const row of rows) {
    sums.input += row.inputTokens ?? 0;
    sums.cachedRead += row.cacheReadTokens ?? 0;
    sums.cacheCreation += row.cacheCreationTokens ?? 0;
    sums.output += row.outputTokens ?? 0;
    sums.reasoning += Number(metadataOf(row).reasoningOutputTokens ?? 0);
    sums.ticks += Math.round((row.costUsd ?? 0) * TICKS_PER_USD);
  }
  return sums;
}

function rowSums(rows: FixtureModelUsage[], billable = true): Sums {
  return rows.reduce<Sums>((sums, row) => ({
    input: sums.input + row.input,
    cachedRead: sums.cachedRead + row.cachedRead,
    cacheCreation: sums.cacheCreation + row.cacheCreation,
    output: sums.output + row.output,
    reasoning: sums.reasoning + row.reasoning,
    ticks: sums.ticks + (billable && !row.incomplete ? row.costTicks ?? 0 : 0),
  }), { input: 0, cachedRead: 0, cacheCreation: 0, output: 0, reasoning: 0, ticks: 0 });
}

function sameSums(left: Sums, right: Sums) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function usage(model: string, scale: number, extra: Partial<FixtureModelUsage> = {}): FixtureModelUsage {
  return {
    model,
    input: 1_200 * scale + 17,
    cachedRead: 800 * scale,
    cacheCreation: scale % 3 === 0 ? 32 * scale : 0,
    output: 90 * scale + 3,
    reasoning: 40 * scale,
    modelCalls: scale,
    costTicks: 7_654_321 * scale,
    ...extra,
  };
}

function minutesAgo(minutes: number) {
  return new Date(Date.now() - minutes * 60_000).toISOString();
}

function writeSession(sessionsRoot: string, group: string, session: FixtureSession, options: {
  content?: boolean; unreadableContent?: boolean; raw?: string;
} = {}) {
  const directory = path.join(sessionsRoot, group, session.sessionId);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = path.join(directory, "usage.json");
  fs.writeFileSync(file, options.raw ?? JSON.stringify(grokUsageDocument(session), null, 1), { mode: 0o600 });
  if (options.content !== false) {
    for (const name of GROK_SESSION_CONTENT_FILES) {
      const content = path.join(directory, name);
      fs.writeFileSync(content, `{"text":"${SENTINEL}","cwd":"${SENTINEL}"}\n`, { mode: 0o600 });
      if (options.unreadableContent) fs.chmodSync(content, 0o000);
    }
    fs.mkdirSync(path.join(directory, "terminal"), { recursive: true });
    fs.writeFileSync(path.join(directory, "terminal", "output.log"), `${SENTINEL}\n`);
  }
  return { directory, file };
}

type Expected = { sessionId: string; turn: FixtureTurn; perModel: boolean; model: string | null };

/** The events one first sight of a turn must produce (per model when the rows reconcile). */
function expectedEvents(session: FixtureSession, turn: FixtureTurn, reconciles: boolean): Expected[] {
  if (session.shape === "modern" && reconciles) {
    return turn.models.map((row) => ({ sessionId: session.sessionId, turn, perModel: true, model: row.model }));
  }
  const model = session.shape === "modern"
    ? turn.primaryModelId ?? turn.models[0]!.model
    : null;
  return [{ sessionId: session.sessionId, turn, perModel: false, model }];
}

/** The highest ancestor (the directory itself included) that holds a `.git` entry. */
function outermostGitOwner(directory: string) {
  let owner: string | null = null;
  for (let current = directory; ; current = path.dirname(current)) {
    try {
      fs.lstatSync(path.join(current, ".git"));
      owner = current;
    } catch {
      // No Git entry here.
    }
    if (path.dirname(current) === current) return owner;
  }
}

/**
 * A fresh directory outside every Git worktree. Git resolution walks every
 * ancestor, and a local proof layout can place TMPDIR inside a checkout, so
 * a directory under the proof root is not reliably "not a repo". Step out
 * above the outermost worktree that holds TMPDIR, else use the system temp.
 */
function nonGitDirectory(label: string) {
  const temp = fs.realpathSync(os.tmpdir());
  const owner = outermostGitOwner(temp);
  const bases = [owner ? path.dirname(owner) : temp, fs.realpathSync("/tmp")];
  for (const base of bases) {
    if (outermostGitOwner(base)) continue;
    try {
      return fs.realpathSync(fs.mkdtempSync(path.join(base, `${label}-`)));
    } catch {
      // Not writable here; try the next base.
    }
  }
  throw new Error("grok_usage_proof_no_directory_outside_git");
}

function spyOnFilesystem() {
  const calls: Array<{ method: string; target: string }> = [];
  const methods = ["openSync", "readFileSync", "readdirSync", "lstatSync", "statSync", "opendirSync",
    "existsSync", "accessSync", "realpathSync", "createReadStream"] as const;
  const originals = new Map<string, unknown>();
  const target = fs as unknown as Record<string, (...args: unknown[]) => unknown>;
  for (const method of methods) {
    const original = target[method]!;
    originals.set(method, original);
    target[method] = function (this: unknown, ...args: unknown[]) {
      if (typeof args[0] === "string") calls.push({ method, target: args[0] });
      return original.apply(this, args);
    } as never;
  }
  return {
    calls,
    restore() {
      for (const [method, original] of originals) target[method] = original as never;
    },
  };
}

function snapshotHealth(buffer: LocalEventBuffer) {
  for (let slice = 0; slice < 200; slice += 1) {
    const state = buffer.projection.status();
    if (state.ready && !state.dirty && state.backfill.complete && state.backfill.parityComplete &&
      Object.values(state.backlog).every((value) => value === 0)) break;
    buffer.projection.runMaintenance(new Date());
  }
  const read = buffer.projection.readSnapshot(30);
  if (read.kind !== "ready") {
    console.error(JSON.stringify({ snapshot: read.kind }));
    return null;
  }
  return (read.snapshot.status as { health: { sources: Array<Record<string, unknown>> } }).health;
}

function drainRepoContexts(buffer: LocalEventBuffer) {
  let applied = 0;
  for (let round = 0; round < 50; round += 1) {
    const batch = buffer.takeRepoContextBatch();
    if (batch.length === 0) break;
    const begun = buffer.beginRepoContextResolution(batch);
    // The maintenance worker's own resolver, with its source gate.
    const resolved = resolveMaintenanceRepoContexts(begun, {
      quarantine: null,
      reportProgress: () => true,
      recordRepoLabel: (repoHash, label) => buffer.recordRepoLabel(repoHash, label),
    });
    const receipt = buffer.applyRepoContextResults(resolved);
    applied += receipt.resultsInserted;
  }
  for (let round = 0; round < 50; round += 1) {
    if (buffer.drainRepoContextFills().rowsFilled === 0) break;
  }
  return applied;
}

function maintenanceState(buffer: LocalEventBuffer, key: string) {
  return (buffer.database.prepare(`select value from maintenance_state where key = ?`).get(key) as
    | { value: string }
    | undefined)?.value;
}

async function main() {
  const root = process.env.PLIMSOLL_PROOF_ROOT!;
  const grokHome = process.env.GROK_HOME!;
  const work = path.join(root, "tmp", "grok-usage-proof");
  fs.mkdirSync(work, { recursive: true, mode: 0o700 });

  // --- The fixture holds the documented shapes -------------------------------
  const shapeRow = (model: string, scale: number) => usage(model, scale);
  const shapeDocuments = [
    grokUsageDocument({ sessionId: "s1", updatedAt: minutesAgo(9), shape: "modern",
      turns: [{ turnNumber: 1, endedAt: minutesAgo(9), models: [shapeRow("grok-4.6-build", 1)] }] }),
    grokUsageDocument({ sessionId: "s2", updatedAt: minutesAgo(9), shape: "modern",
      turns: [{ turnNumber: 1, endedAt: minutesAgo(9), models: [shapeRow("grok-4.7-build", 1)] }] }),
    grokUsageDocument({ sessionId: "s3", updatedAt: minutesAgo(9), shape: "legacy",
      turns: [{ turnNumber: 1, endedAt: minutesAgo(9), models: [{ ...shapeRow("grok-4.7-build", 1), costTicks: undefined }] }] }),
    grokUsageDocument({ sessionId: "s4", updatedAt: minutesAgo(9), shape: "modern", sessionIncomplete: true,
      sessionOnly: [shapeRow("grok-4.6-build", 2)],
      turns: [{ turnNumber: 1, endedAt: minutesAgo(9), models: [shapeRow("grok-4.7-build", 1)] }] }),
  ];
  check("fixture_documents_match_the_four_documented_grok_usage_shapes",
    shapeDocuments.every((document, index) =>
      JSON.stringify(canonicalShape(valueBlindShape(document))) ===
        JSON.stringify(GROK_USAGE_DOCUMENTED_SHAPES[index])),
    { shapes: GROK_USAGE_DOCUMENTED_SHAPES.length });

  // --- Production wiring: first-run backfill ---------------------------------
  const sessionsRoot = path.join(grokHome, "sessions");
  const repoDirectory = path.join(work, "projects", "grok-fixture-repo");
  fs.mkdirSync(path.join(repoDirectory, ".git", "refs", "heads"), { recursive: true });
  fs.writeFileSync(path.join(repoDirectory, ".git", "HEAD"), "ref: refs/heads/main\n");
  fs.writeFileSync(path.join(repoDirectory, ".git", "refs", "heads", "main"), `${"b".repeat(40)}\n`);
  fs.writeFileSync(path.join(repoDirectory, ".git", "config"),
    `[remote "origin"]\n\turl = https://example.invalid/team/grok-fixture.git\n`);
  const plainDirectory = nonGitDirectory("plimsoll-grok-not-a-repo");
  process.once("exit", () => fs.rmSync(plainDirectory, { recursive: true, force: true }));
  const rootGroup = "%2F";
  const repoGroup = encodeURIComponent(repoDirectory);
  const plainGroup = encodeURIComponent(plainDirectory);
  const uuid = (index: number) => `6a0e${String(index).padStart(4, "0")}-5e2d-4c1a-9b7f-${String(index).padStart(12, "0")}`;
  const at = (minutes: number) => minutesAgo(minutes);
  const corpus: Array<{ group: string; session: FixtureSession; reconcile: boolean[] }> = [
    { group: rootGroup, reconcile: [true, true, true], session: { sessionId: uuid(1), updatedAt: at(300), shape: "modern",
      turns: [1, 2, 3].map((n) => ({ turnNumber: n, endedAt: at(330 - n), models: [usage("grok-4.7-build", n)] })) } },
    { group: rootGroup, reconcile: [true], session: { sessionId: uuid(2), updatedAt: at(290), shape: "modern",
      turns: [{ turnNumber: 1, endedAt: at(291), models: [usage("grok-4.6-build", 4)] }] } },
    { group: rootGroup, reconcile: [false, false], session: { sessionId: uuid(3), updatedAt: at(280), shape: "legacy",
      turns: [1, 2].map((n) => ({ turnNumber: n, endedAt: at(285 - n),
        models: [{ ...usage("grok-4.7-build", n + 1), costTicks: undefined }] })) } },
    { group: rootGroup, reconcile: [true], session: { sessionId: uuid(4), updatedAt: at(270), shape: "modern",
      sessionIncomplete: true, sessionOnly: [usage("grok-4.6-build", 2)],
      turns: [{ turnNumber: 1, endedAt: at(271), models: [usage("grok-4.7-build", 1)] }] } },
    { group: rootGroup, reconcile: [true, true], session: { sessionId: uuid(5), updatedAt: at(260), shape: "modern",
      turns: [
        { turnNumber: 1, endedAt: at(262), models: [usage("grok-4.6-build", 2), usage("grok-4.7-build", 3)] },
        { turnNumber: 2, endedAt: at(261), models: [usage("grok-4.7-build", 1)] },
      ] } },
    { group: rootGroup, reconcile: [false], session: { sessionId: uuid(6), updatedAt: at(250), shape: "modern",
      turns: [{ turnNumber: 1, endedAt: at(251), primaryModelId: "grok-4.7-build",
        models: [usage("grok-4.6-build", 1), usage("grok-4.7-build", 2)],
        totalsOverride: { input: usage("grok-4.6-build", 1).input + usage("grok-4.7-build", 2).input + 11 } }] } },
    { group: repoGroup, reconcile: [true, true], session: { sessionId: uuid(7), updatedAt: at(2), shape: "modern",
      turns: [1, 2].map((n) => ({ turnNumber: n, endedAt: at(1.5 - n / 2), models: [usage("grok-4.7-build", n + 2)] })) } },
    { group: plainGroup, reconcile: [true], session: { sessionId: uuid(8), updatedAt: at(200), shape: "modern",
      turns: [{ turnNumber: 1, endedAt: at(201), models: [usage("grok-4.7-build", 5)] }] } },
    { group: rootGroup, reconcile: [true], session: { sessionId: uuid(9), updatedAt: at(190), shape: "modern",
      turns: [{ turnNumber: 1, endedAt: at(191), incomplete: true,
        models: [usage("grok-4.7-build", 6, { costTicks: undefined, incomplete: true })] }] } },
    { group: rootGroup, reconcile: [true], session: { sessionId: uuid(10), updatedAt: at(180), shape: "modern",
      extra: { notes: SENTINEL, prompt: SENTINEL, cwd: SENTINEL },
      turns: [{ turnNumber: 1, endedAt: at(181), models: [usage("grok-4.6-build", 7)] }] } },
  ];
  for (const [index, entry] of corpus.entries()) {
    writeSession(sessionsRoot, entry.group, entry.session, { unreadableContent: index % 2 === 0 });
  }
  // A file beside the session directories, as Grok's own prompt history is.
  fs.writeFileSync(path.join(sessionsRoot, rootGroup, "prompt_history.jsonl"), `${SENTINEL}\n`);

  const config = collectorConfigSchema.parse({
    managed: true,
    uploadUrl: "http://127.0.0.1/fake-ingest",
    installKey: "grok-usage-proof-install",
  });
  const buffer = new LocalEventBuffer(path.join(work, "ledger.sqlite"), {
    delivery: { enabled: true, limits: config.delivery },
  });
  const capture = createProfileCapture(buffer, config) as ReturnType<typeof createProfileCapture> & {
    grok?: unknown;
  };
  // Exactly the maintenance worker's construction (cli.ts `__maintenance_worker`).
  const maintenance = new CollectorMaintenance(
    buffer, capture.rollout, capture.transcript, undefined, capture.grok as never,
  );
  const turns: string[] = [];
  const runs: CollectorMaintenanceRunResult[] = [];
  const spy = spyOnFilesystem();
  try {
    for (let run = 0; run < 12; run += 1) {
      turns.push(maintenanceState(buffer, "automatic_capture_source_turn") ?? "unset");
      runs.push(await maintenance.runRecent());
      const marker = maintenanceState(buffer, "grok_usage_backfill_v1");
      if (marker && JSON.parse(marker).completedAt && run >= 3) break;
    }
  } finally {
    spy.restore();
  }
  const expected = corpus.flatMap((entry) => entry.session.turns.flatMap((turn, index) =>
    expectedEvents(entry.session, turn, entry.reconcile[index]!)));
  const expectedIds = expected.map((item) =>
    deterministicEventId(["grok-usage", item.sessionId, String(item.turn.turnNumber), item.model ?? ""]));
  const allRows = grokRows(buffer);
  const ids = new Set(allRows.map((row) => row.id));
  check("production_wiring_backfills_every_grok_turn_on_first_run",
    allRows.length === expected.length && expectedIds.every((id) => ids.has(id)) &&
      allRows.every((row) => row.eventType === "usage_transcript"),
    { events: allRows.length, expected: expected.length, runs: runs.length,
      grokEvents: runs.map((run) => run.grok?.eventsAppended ?? null) });

  const perSession = corpus.map((entry) => {
    const rows = grokRows(buffer, entry.session.sessionId);
    const turnRows = entry.session.turns.flatMap((turn) => turn.totalsOverride
      ? [{ ...rowSums(turn.models), input: turn.totalsOverride.input ?? rowSums(turn.models).input }]
      : [rowSums(turn.models)]);
    const turnTotals = turnRows.reduce<Sums>((sum, row) => ({
      input: sum.input + row.input, cachedRead: sum.cachedRead + row.cachedRead,
      cacheCreation: sum.cacheCreation + row.cacheCreation, output: sum.output + row.output,
      reasoning: sum.reasoning + row.reasoning,
      ticks: sum.ticks + (entry.session.shape === "modern" ? row.ticks : 0),
    }), { input: 0, cachedRead: 0, cacheCreation: 0, output: 0, reasoning: 0, ticks: 0 });
    const document = grokUsageDocument(entry.session) as { session: Record<string, number> };
    return { entry, rows, ledger: ledgerSums(rows), turnTotals, grokSession: document.session };
  });
  check("ledger_token_totals_equal_grok_turn_records_for_every_session",
    perSession.every((item) => {
      const { ticks: _ledgerTicks, ...ledgerTokens } = item.ledger;
      const { ticks: _turnTicks, ...turnTokens } = item.turnTotals;
      return JSON.stringify(ledgerTokens) === JSON.stringify(turnTokens);
    }),
    { sessions: perSession.map((item) => ({ ledger: item.ledger, turns: item.turnTotals })) });
  check("ledger_cost_equals_grok_billed_ticks_and_is_reported",
    perSession.every((item) => item.ledger.ticks === item.turnTotals.ticks) &&
      allRows.every((row) => row.costUsd === null || row.costKind === "reported"),
    { ticks: perSession.map((item) => [item.ledger.ticks, item.turnTotals.ticks]) });
  const completeSessions = perSession.filter((item) => !item.entry.session.sessionOnly);
  const incompleteSession = perSession.find((item) => item.entry.session.sessionOnly)!;
  const sessionOnly = rowSums(incompleteSession.entry.session.sessionOnly!);
  const lastGrok = runs.map((run) => run.grok).filter(Boolean).at(-1)!;
  check("ledger_matches_grok_session_totals_except_usage_grok_left_outside_every_turn",
    completeSessions.every((item) =>
      item.ledger.input === item.grokSession.inputTokens && item.ledger.output === item.grokSession.outputTokens &&
        item.ledger.cachedRead === item.grokSession.cachedReadTokens) &&
      incompleteSession.grokSession.inputTokens - incompleteSession.ledger.input === sessionOnly.input &&
      incompleteSession.grokSession.outputTokens - incompleteSession.ledger.output === sessionOnly.output &&
      lastGrok.activity.scan.usageFiles.sessionOnlyTokens === sessionOnly.input + sessionOnly.output,
    { sessionOnlyTokens: lastGrok.activity.scan.usageFiles.sessionOnlyTokens,
      expected: sessionOnly.input + sessionOnly.output });

  const multiModel = grokRows(buffer, uuid(5)).filter((row) => metadataOf(row).turnIndex === 1);
  const multiTurn = corpus[4]!.session.turns[0]!;
  check("multi_model_turn_is_one_event_per_model_with_exact_turn_totals",
    multiModel.length === 2 && new Set(multiModel.map((row) => row.model)).size === 2 &&
      sameSums(ledgerSums(multiModel), rowSums(multiTurn.models)) &&
      multiModel.every((row) => {
        const source = multiTurn.models.find((model) => model.model === row.model)!;
        return row.inputTokens === source.input && Math.round((row.costUsd ?? 0) * TICKS_PER_USD) === source.costTicks;
      }),
    { models: multiModel.map((row) => row.model) });
  const unreconciled = grokRows(buffer, uuid(6));
  check("model_rows_that_do_not_add_up_become_one_turn_total_event_under_the_primary_model",
    unreconciled.length === 1 && unreconciled[0]!.model === "grok-4.7-build" &&
      unreconciled[0]!.inputTokens === corpus[5]!.session.turns[0]!.totalsOverride!.input,
    { rows: unreconciled.map((row) => ({ model: row.model, input: row.inputTokens })) });
  const legacy = grokRows(buffer, uuid(3));
  check("legacy_documents_without_model_rows_or_cost_count_tokens_unpriced",
    legacy.length === 2 && legacy.every((row) => row.model === null && row.costUsd === null),
    { rows: legacy.length });
  const flagged = [...grokRows(buffer, uuid(9)), ...grokRows(buffer, uuid(4))];
  check("incomplete_usage_is_kept_and_labelled_and_a_partial_bill_is_never_the_cost",
    flagged.length === 2 && flagged.every((row) => metadataOf(row).usageSource === "grok_usage_incomplete") &&
      grokRows(buffer, uuid(9))[0]!.costUsd === null && grokRows(buffer, uuid(9))[0]!.inputTokens! > 0 &&
      grokRows(buffer, uuid(4))[0]!.costUsd !== null &&
      allRows.filter((row) => ![uuid(9), uuid(4)].includes(row.sessionId))
        .every((row) => metadataOf(row).usageSource === "grok_usage"),
    { labels: flagged.map((row) => metadataOf(row).usageSource) });
  const reasoningRows = allRows.filter((row) => (metadataOf(row).reasoningOutputTokens as number) > 0);
  check("reasoning_follows_the_rollout_convention_inside_output_and_in_metadata",
    reasoningRows.length === allRows.length &&
      allRows.every((row) => Number(metadataOf(row).reasoningOutputTokens) <= (row.outputTokens ?? 0)) &&
      allRows.every((row) => (row.cacheReadTokens ?? 0) <= (row.inputTokens ?? 0)),
    { rows: allRows.length });

  const coverage = historyCoverageStatus(buffer.database);
  const grokCoverage = coverage.sources.find((source) => source.source === "grok");
  check("history_coverage_reports_the_grok_usage_backfill_complete_without_changing_the_verdict",
    grokCoverage?.status === "complete" && grokCoverage.reason === null &&
      grokCoverage.lastFullScan?.filesSeen === corpus.length &&
      (grokCoverage.usageBackfill?.sweeps ?? 0) >= 1 && coverage.status === "incomplete" &&
      coverage.sources.filter((source) => source.source !== "grok").every((source) => source.status === "incomplete"),
    { grok: grokCoverage, aggregate: coverage.status });

  const rotation = turns.slice(0, 4);
  check("maintenance_rotates_the_shared_capture_allowance_over_three_sources",
    JSON.stringify(rotation) === JSON.stringify(["unset", "claude_code", "grok", "codex"]),
    { rotation });
  const projected = projectMaintenanceResult(runs[0]!);
  check("worker_outcome_carries_grok_counts_and_raw_writes_include_grok",
    projected.grok !== undefined && projected.grok.eventsAppended === runs[0]!.grok!.eventsAppended &&
      runs[0]!.rawEventWrites === runs[0]!.rollout.eventsAppended + runs[0]!.transcript.eventsAppended +
        runs[0]!.grok!.eventsAppended && runs[0]!.captureAdvanced === true,
    { projected: projected.grok, rawEventWrites: runs[0]!.rawEventWrites });

  const grokFiles = new Set(corpus.map((entry) =>
    path.join(sessionsRoot, entry.group, entry.session.sessionId, "usage.json")));
  const grokDirectories = new Set([sessionsRoot, ...corpus.flatMap((entry) => [
    path.join(sessionsRoot, entry.group),
    path.join(sessionsRoot, entry.group, entry.session.sessionId),
  ])]);
  const underGrok = spy.calls.filter((call) => call.target === grokHome || call.target.startsWith(`${grokHome}${path.sep}`));
  const outside = underGrok.filter((call) => !grokFiles.has(call.target) && !grokDirectories.has(call.target));
  const opened = underGrok.filter((call) => call.method === "openSync").map((call) => call.target);
  check("the_tailer_touches_only_usage_json_and_the_directories_above_it",
    underGrok.length > 0 && outside.length === 0 && opened.every((file) => grokFiles.has(file)) &&
      opened.length === grokFiles.size,
    { calls: underGrok.length, outside: outside.slice(0, 5), opened: opened.length });
  const scanErrors = runs.reduce((total, run) => total + (run.grok
    ? run.grok.discoveryErrors + run.grok.statErrors + run.grok.readErrors + run.grok.parseErrors
    : 0), 0);
  check("unreadable_content_files_beside_usage_json_cause_no_error",
    scanErrors === 0 && fs.statSync(path.join(sessionsRoot, rootGroup, uuid(1), "chat_history.jsonl")).mode % 0o1000 === 0,
    { scanErrors });

  // --- Project attribution from the encoded working directory ---------------
  drainRepoContexts(buffer);
  const repoHash = resolveGitContextUncached(repoDirectory)?.remoteUrlHash;
  const linked = (sessionId: string) => grokRows(buffer, sessionId).map((row) => ({
    repoHash: row.repoHash,
    link: (buffer.database.prepare(`select context_id as contextId from repo_context_event_links where event_id = ?`)
      .get(row.id) as { contextId: string } | undefined)?.contextId ?? null,
  }));
  const repoRows = linked(uuid(7));
  const plainRows = linked(uuid(8));
  const rootRows = corpus.filter((entry) => entry.group === rootGroup).flatMap((entry) => linked(entry.session.sessionId));
  check("encoded_directory_gives_grok_turns_their_git_project",
    typeof repoHash === "string" && repoRows.length === 2 && repoRows.every((row) => row.repoHash === repoHash && row.link),
    { repoRows, repoHash });
  check("the_plain_directory_fixture_is_outside_every_git_worktree",
    outermostGitOwner(plainDirectory) === null && resolveGitContextUncached(plainDirectory) === undefined,
    { insideWorktree: outermostGitOwner(plainDirectory) !== null });
  check("root_and_non_git_directories_stay_unallocated",
    rootRows.length > 0 && rootRows.every((row) => row.repoHash === null && row.link === null) &&
      plainRows.length === 1 && plainRows.every((row) => row.repoHash === null && row.link !== null),
    { rootRows: rootRows.length, plainRows });

  // --- Capture health for the new source -------------------------------------
  const health = snapshotHealth(buffer);
  const grokHealth = health?.sources.find((source) => source.source === "grok") as Record<string, any> | undefined;
  check("capture_health_judges_grok_by_its_usage_scan_with_file_counters",
    grokHealth?.capture === "local_scan" && grokHealth.status === "green" &&
      grokHealth.activityState?.scan?.usageFiles?.seen === corpus.length &&
      grokHealth.activityState?.scan?.usageFiles?.errors === 0 &&
      Number(grokHealth.tokenSessionsToday) >= 1,
    { grok: grokHealth && { capture: grokHealth.capture, status: grokHealth.status, reason: grokHealth.reason,
      usageFiles: grokHealth.activityState?.scan?.usageFiles } });

  // --- Upload: the shared ingest schema accepts every event, content-free ----
  const sealed = allRows.map((row) => sealOutboundEnvelope({
    event: JSON.parse(row.payloadJson),
    suppressedFields: JSON.parse(row.suppressedFieldsJson),
  }));
  const bodies: string[] = [];
  const upload = await uploadBufferedEvents(config, buffer, {
    fetchImpl: async (_input, init) => {
      bodies.push(String(init?.body ?? ""));
      return new Response(JSON.stringify(acceptedFixtureDelivery(String(init?.body ?? ""), config.installKey)),
        { status: 200, headers: { "content-type": "application/json" } });
    },
  });
  const uploaded = bodies.flatMap((body) => {
    const parsed = aiWorkIngestBatchSchema.safeParse(JSON.parse(body));
    return parsed.success ? parsed.data.events : [];
  });
  const uploadedGrok = uploaded.filter((envelope) => envelope.event.source === "grok" &&
    envelope.event.eventType === "usage_transcript");
  check("every_grok_usage_event_passes_the_shared_ingest_schema_and_outbound_seal",
    sealed.every((outcome) => outcome.ok &&
      Object.keys(outcome.envelope.event.metadata).every((key) =>
        ["usageSource", "turnIndex", "reasoningOutputTokens"].includes(key)) &&
      outcome.envelope.suppressedFields.length === 0) &&
      bodies.length > 0 && bodies.every((body) => aiWorkIngestBatchSchema.safeParse(JSON.parse(body)).success) &&
      uploadedGrok.length === allRows.length &&
      uploadedGrok.every((envelope) => aiInteractionEventSchema.safeParse(envelope.event).success &&
        typeof envelope.event.inputTokens === "number"),
    { sealed: sealed.length, uploadedGrok: uploadedGrok.length, uploadedEvents: upload.uploadedEvents });

  buffer.database.pragma("wal_checkpoint(TRUNCATE)");
  const ledgerBytes = [path.join(work, "ledger.sqlite"), path.join(work, "ledger.sqlite-wal")]
    .filter((file) => fs.existsSync(file)).map((file) => fs.readFileSync(file));
  const scanJson = JSON.stringify(runs.map((run) => run.grok));
  check("planted_content_never_reaches_ledger_scan_receipts_or_upload",
    ledgerBytes.every((bytes) => !bytes.includes(SENTINEL)) &&
      allRows.every((row) => !row.payloadJson.includes(SENTINEL)) &&
      bodies.every((body) => !body.includes(SENTINEL)) && !scanJson.includes(SENTINEL),
    { ledgerFiles: ledgerBytes.length, bodies: bodies.length });
  check("working_directories_never_rest_in_the_ledger_or_cross_the_upload",
    ledgerBytes.every((bytes) => !bytes.includes(repoDirectory) && !bytes.includes(repoGroup) &&
      !bytes.includes(plainDirectory)) &&
      bodies.every((body) => !body.includes(repoDirectory) && !body.includes(plainDirectory)),
    { checked: ["repo", "plain"] });
  capture.close();
  buffer.close();

  // --- Unit behaviour through the tailer itself --------------------------------
  const { GrokUsageTailer, GROK_USAGE_LIMITS } = await import("../packages/collector-cli/src/grok-usage-tailer");
  const unitHome = (name: string) => {
    const home = path.join(work, "unit", name, ".grok");
    fs.mkdirSync(path.join(home, "sessions"), { recursive: true, mode: 0o700 });
    return home;
  };
  const unitBuffer = (name: string) => new LocalEventBuffer(path.join(work, "unit", `${name}.sqlite`));
  const budget = (limits: Partial<typeof AUTOMATIC_CAPTURE_LIMITS> = {}) =>
    new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, ...limits });

  // Incremental turns, idempotence, rewrites.
  const incHome = unitHome("incremental");
  const incBuffer = unitBuffer("incremental");
  const incSession: FixtureSession = { sessionId: uuid(101), updatedAt: at(60), shape: "modern",
    turns: [1, 2, 3].map((n) => ({ turnNumber: n, endedAt: at(70 - n), models: [usage("grok-4.7-build", n)] })) };
  const incFile = writeSession(path.join(incHome, "sessions"), rootGroup, incSession).file;
  let tailer = new GrokUsageTailer(incBuffer, incHome);
  const first = await tailer.scan({ budget: budget() });
  const unchangedSpy = spyOnFilesystem();
  let unchanged;
  try {
    unchanged = await tailer.scan({ budget: budget() });
  } finally {
    unchangedSpy.restore();
  }
  check("an_unchanged_usage_file_costs_one_lstat_and_no_open_or_parse",
    first.eventsAppended === 3 && unchanged.filesUnchanged === 1 && unchanged.filesRead === 0 &&
      unchanged.recordsParsed === 0 && unchanged.eventsAppended === 0 &&
      !unchangedSpy.calls.some((call) => call.method === "openSync" && call.target === incFile),
    { first: first.eventsAppended, unchanged: unchanged.filesUnchanged });
  fs.utimesSync(incFile, new Date(), new Date(Date.now() + 1_000));
  const touched = await tailer.scan({ budget: budget() });
  check("identical_bytes_under_a_new_mtime_are_read_but_not_parsed",
    touched.filesRead === 1 && touched.recordsParsed === 0 && touched.eventsAppended === 0,
    { filesRead: touched.filesRead, recordsParsed: touched.recordsParsed });
  const grown: FixtureSession = { ...incSession, updatedAt: at(50),
    turns: [...incSession.turns, { turnNumber: 4, endedAt: at(51), models: [usage("grok-4.7-build", 9)] }] };
  writeSession(path.join(incHome, "sessions"), rootGroup, grown, { content: false });
  const grew = await tailer.scan({ budget: budget() });
  const turnFourId = deterministicEventId(["grok-usage", uuid(101), "4", "grok-4.7-build"]);
  check("a_file_that_grows_by_one_turn_emits_only_that_turn",
    grew.eventsAppended === 1 && grokRows(incBuffer).length === 4 &&
      grokRows(incBuffer).some((row) => row.id === turnFourId),
    { eventsAppended: grew.eventsAppended });
  const raised: FixtureSession = { ...grown, updatedAt: at(40), turns: grown.turns.map((turn) => turn.turnNumber === 2
    ? { ...turn, models: [{ ...turn.models[0]!, input: turn.models[0]!.input + 500, output: turn.models[0]!.output + 20,
      costTicks: turn.models[0]!.costTicks! + 1_000 }] }
    : turn) };
  writeSession(path.join(incHome, "sessions"), rootGroup, raised, { content: false });
  const raisedPass = await tailer.scan({ budget: budget() });
  const turnTwo = grokRows(incBuffer).filter((row) => metadataOf(row).turnIndex === 2);
  const raisedTwo = raised.turns[1]!.models[0]!;
  const revision = turnTwo.find((row) => row.id !== deterministicEventId(["grok-usage", uuid(101), "2", "grok-4.7-build"]));
  check("a_rewrite_that_raises_a_turn_appends_only_the_increase",
    raisedPass.eventsAppended === 1 && raisedPass.turnsRevised === 1 && turnTwo.length === 2 &&
      revision?.inputTokens === 500 && revision.outputTokens === 20 &&
      Math.round((revision.costUsd ?? 0) * TICKS_PER_USD) === 1_000 &&
      sameSums(ledgerSums(turnTwo), rowSums([raisedTwo])),
    { revision: revision && { input: revision.inputTokens, output: revision.outputTokens } });
  const lowered: FixtureSession = { ...raised, updatedAt: at(30), turns: raised.turns.map((turn) => turn.turnNumber === 1
    ? { ...turn, models: [{ ...turn.models[0]!, input: turn.models[0]!.input - 100 }] }
    : turn) };
  writeSession(path.join(incHome, "sessions"), rootGroup, lowered, { content: false });
  const before = ledgerSums(grokRows(incBuffer));
  const loweredPass = await tailer.scan({ budget: budget() });
  check("a_rewrite_that_lowers_a_turn_is_refused_and_the_turn_is_counted_once",
    loweredPass.eventsAppended === 0 && loweredPass.turnRewritesRefused === 1 &&
      sameSums(ledgerSums(grokRows(incBuffer)), before),
    { refused: loweredPass.turnRewritesRefused });
  tailer.close();
  tailer = new GrokUsageTailer(incBuffer, incHome);
  fs.writeFileSync(incFile, JSON.stringify(grokUsageDocument(raised)));
  const restarted = await tailer.scan({ budget: budget() });
  check("a_restarted_tailer_rereading_every_document_never_recounts",
    restarted.filesRead === 1 && restarted.recordsParsed === 4 && restarted.eventsAppended === 0 &&
      sameSums(ledgerSums(grokRows(incBuffer)), before),
    { restarted: { read: restarted.filesRead, events: restarted.eventsAppended } });
  const lateBillSession: FixtureSession = { sessionId: uuid(102), updatedAt: at(20), shape: "modern",
    turns: [{ turnNumber: 1, endedAt: at(21), incomplete: true,
      models: [usage("grok-4.6-build", 3, { costTicks: undefined, incomplete: true })] }] };
  writeSession(path.join(incHome, "sessions"), rootGroup, lateBillSession, { content: false });
  await tailer.scan({ budget: budget() });
  const completedSession: FixtureSession = { ...lateBillSession, updatedAt: at(10),
    turns: [{ turnNumber: 1, endedAt: at(21), models: [usage("grok-4.6-build", 3)] }] };
  writeSession(path.join(incHome, "sessions"), rootGroup, completedSession, { content: false });
  const completedPass = await tailer.scan({ budget: budget() });
  const billRows = grokRows(incBuffer, uuid(102));
  check("a_turn_completed_later_adds_its_bill_once_without_recounting_tokens",
    billRows.length === 2 && completedPass.eventsAppended === 1 &&
      metadataOf(billRows.find((row) => row.costUsd === null)!).usageSource === "grok_usage_incomplete" &&
      billRows.some((row) => row.inputTokens === 0 && row.outputTokens === 0 &&
        Math.round((row.costUsd ?? 0) * TICKS_PER_USD) === usage("grok-4.6-build", 3).costTicks &&
        metadataOf(row).usageSource === "grok_usage") &&
      ledgerSums(billRows).input === usage("grok-4.6-build", 3).input,
    { rows: billRows.map((row) => ({ input: row.inputTokens, cost: row.costUsd })) });
  tailer.close();
  incBuffer.close();

  // Bounds and deferral.
  const boundHome = unitHome("bounds");
  const boundBuffer = unitBuffer("bounds");
  for (let index = 0; index < 20; index += 1) {
    writeSession(path.join(boundHome, "sessions"), index < 10 ? rootGroup : repoGroup, {
      sessionId: uuid(200 + index), updatedAt: at(100), shape: "modern",
      turns: [{ turnNumber: 1, endedAt: at(101), models: [usage("grok-4.7-build", 1 + index)] }],
    }, { content: false });
  }
  const oneFile = fs.statSync(path.join(boundHome, "sessions", rootGroup, uuid(200), "usage.json")).size;
  const boundTailer = new GrokUsageTailer(boundBuffer, boundHome);
  const tight = () => budget({ maxBytes: oneFile * 4 + 2_048 });
  const firstBound = await boundTailer.scan({ budget: tight() });
  let boundPasses = 1;
  let maxBytesRead = firstBound.automaticBudget!.bytesRead;
  for (; boundPasses < 30 && grokRows(boundBuffer).length < 20; boundPasses += 1) {
    const next = await boundTailer.scan({ budget: tight() });
    maxBytesRead = Math.max(maxBytesRead, next.automaticBudget!.bytesRead);
  }
  const boundIds = grokRows(boundBuffer).map((row) => row.id);
  check("a_tight_budget_defers_files_instead_of_blocking_and_later_passes_finish",
    firstBound.eventsAppended > 0 && firstBound.eventsAppended < 20 && firstBound.deferredGenerations > 0 &&
      firstBound.activity.truncated && maxBytesRead <= oneFile * 4 + 2_048 &&
      boundIds.length === 20 && new Set(boundIds).size === 20 && boundPasses > 1,
    { first: firstBound.eventsAppended, deferred: firstBound.deferredGenerations, passes: boundPasses });
  boundTailer.close();
  boundBuffer.close();

  const entryHome = unitHome("entries");
  const entryBuffer = unitBuffer("entries");
  for (let index = 0; index < 10; index += 1) {
    writeSession(path.join(entryHome, "sessions"), index % 2 ? rootGroup : plainGroup, {
      sessionId: uuid(300 + index), updatedAt: at(100), shape: "modern",
      turns: [{ turnNumber: 1, endedAt: at(101), models: [usage("grok-4.6-build", 2)] }],
    }, { content: false });
  }
  const entryTailer = new GrokUsageTailer(entryBuffer, entryHome, { ...GROK_USAGE_LIMITS, entriesPerPass: 3 });
  const entryPasses = [];
  for (let pass = 0; pass < 20; pass += 1) {
    const result = await entryTailer.scan({ budget: budget() });
    entryPasses.push(result.activity.discoveryEntries);
    if (result.exhaustive) break;
  }
  check("discovery_is_bounded_per_pass_and_the_sweep_resumes_to_completion",
    entryPasses.every((entries) => entries <= 3) && entryPasses.length > 3 && grokRows(entryBuffer).length === 10,
    { passes: entryPasses });
  entryTailer.close();
  entryBuffer.close();

  const bigHome = unitHome("oversized");
  const bigBuffer = unitBuffer("oversized");
  const smallDoc = writeSession(path.join(bigHome, "sessions"), rootGroup, { sessionId: uuid(400), updatedAt: at(100),
    shape: "modern", turns: [{ turnNumber: 1, endedAt: at(101), models: [usage("grok-4.7-build", 1)] }] }, { content: false });
  const bigDoc = writeSession(path.join(bigHome, "sessions"), rootGroup, { sessionId: uuid(401), updatedAt: at(100),
    shape: "modern", turns: Array.from({ length: 12 }, (_, index) => ({ turnNumber: index + 1, endedAt: at(101),
      models: [usage("grok-4.7-build", index + 1)] })) }, { content: false });
  const ceiling = fs.statSync(smallDoc.file).size + 16;
  const bigTailer = new GrokUsageTailer(bigBuffer, bigHome, { ...GROK_USAGE_LIMITS, maxFileBytes: ceiling });
  const bigSpy = spyOnFilesystem();
  let bigPass;
  try {
    bigPass = await bigTailer.scan({ budget: budget() });
  } finally {
    bigSpy.restore();
  }
  check("a_document_larger_than_the_ceiling_is_reported_and_never_opened",
    fs.statSync(bigDoc.file).size > ceiling && bigPass.filesOversized === 1 && bigPass.unresolvedRecords === 1 &&
      !bigSpy.calls.some((call) => call.method === "openSync" && call.target === bigDoc.file) &&
      grokRows(bigBuffer).length === 1 && !bigPass.exhaustive &&
      bigPass.activity.scan.usageFiles.oversized === 1,
    { oversized: bigPass.filesOversized });
  bigTailer.close();
  bigBuffer.close();

  const longHome = unitHome("long");
  const longBuffer = unitBuffer("long");
  const longSession: FixtureSession = { sessionId: uuid(500), updatedAt: at(100), shape: "modern",
    turns: Array.from({ length: 150 }, (_, index) => ({ turnNumber: index + 1, endedAt: at(200 - index / 2),
      models: [usage("grok-4.7-build", 1 + (index % 7))] })) };
  writeSession(path.join(longHome, "sessions"), rootGroup, longSession, { content: false });
  const longTailer = new GrokUsageTailer(longBuffer, longHome);
  const longPasses: Array<{ events: number; records: number }> = [];
  for (let pass = 0; pass < 10 && grokRows(longBuffer).length < 150; pass += 1) {
    const result = await longTailer.scan({ budget: budget() });
    longPasses.push({ events: result.eventsAppended, records: result.automaticBudget!.recordsParsed });
  }
  const longIds = grokRows(longBuffer).map((row) => row.id);
  check("a_long_document_resumes_after_its_committed_turns_without_recounting",
    longPasses.length >= 2 && longPasses[0]!.events === AUTOMATIC_CAPTURE_LIMITS.sliceRecords &&
      longPasses.every((pass) => pass.records <= AUTOMATIC_CAPTURE_LIMITS.maxRecords) &&
      longIds.length === 150 && new Set(longIds).size === 150 &&
      sameSums(ledgerSums(grokRows(longBuffer)), rowSums(longSession.turns.flatMap((turn) => turn.models))),
    { passes: longPasses });
  longTailer.close();
  longBuffer.close();

  // Links and identity.
  const linkHome = unitHome("links");
  const linkBuffer = unitBuffer("links");
  const outsideDoc = path.join(work, "outside-usage.json");
  fs.writeFileSync(outsideDoc, JSON.stringify(grokUsageDocument({ sessionId: uuid(600), updatedAt: at(10),
    shape: "modern", turns: [{ turnNumber: 1, endedAt: at(11), models: [usage("grokoutsidemodel", 1)] }] })));
  const linkedDirectory = path.join(linkHome, "sessions", rootGroup, uuid(600));
  fs.mkdirSync(linkedDirectory, { recursive: true });
  fs.symlinkSync(outsideDoc, path.join(linkedDirectory, "usage.json"));
  const outsideSessions = path.join(work, "outside-sessions");
  writeSession(outsideSessions, rootGroup, { sessionId: uuid(601), updatedAt: at(10), shape: "modern",
    turns: [{ turnNumber: 1, endedAt: at(11), models: [usage("grokoutsidemodel", 2)] }] }, { content: false });
  fs.symlinkSync(path.join(outsideSessions, rootGroup, uuid(601)), path.join(linkHome, "sessions", rootGroup, uuid(601)));
  fs.symlinkSync(path.join(outsideSessions, rootGroup), path.join(linkHome, "sessions", "%2Flinked-group"));
  const mismatch = writeSession(path.join(linkHome, "sessions"), rootGroup, { sessionId: uuid(602), updatedAt: at(10),
    shape: "modern", turns: [{ turnNumber: 1, endedAt: at(11), models: [usage("grok-4.7-build", 1)] }] }, { content: false });
  fs.writeFileSync(mismatch.file, JSON.stringify(grokUsageDocument({ sessionId: uuid(603), updatedAt: at(10),
    shape: "modern", turns: [{ turnNumber: 1, endedAt: at(11), models: [usage("grok-4.7-build", 1)] }] })));
  const linkTailer = new GrokUsageTailer(linkBuffer, linkHome);
  const linkPass = await linkTailer.scan({ budget: budget() });
  check("symlinked_usage_files_sessions_and_groups_are_never_followed",
    linkPass.statErrors === 1 && grokRows(linkBuffer).length === 0 &&
      !JSON.stringify(linkBuffer.database.prepare(`select payload_json from buffered_events`).all()).includes("grokoutsidemodel"),
    { statErrors: linkPass.statErrors });
  check("a_document_whose_session_id_disagrees_with_its_directory_is_refused",
    linkPass.parseErrors === 1 && grokRows(linkBuffer, uuid(603)).length === 0,
    { parseErrors: linkPass.parseErrors });
  linkTailer.close();
  linkBuffer.close();

  // A failure inside one document is contained to it and retried later.
  const faultHome = unitHome("fault");
  const faultBuffer = unitBuffer("fault");
  for (const index of [0, 1]) {
    writeSession(path.join(faultHome, "sessions"), rootGroup, { sessionId: uuid(800 + index), updatedAt: at(10),
      shape: "modern", turns: [{ turnNumber: 1, endedAt: at(11), models: [usage("grok-4.7-build", 1 + index)] }] },
    { content: false });
  }
  const faultTailer = new GrokUsageTailer(faultBuffer, faultHome);
  const realAppend = faultBuffer.append.bind(faultBuffer);
  (faultBuffer as unknown as { append: typeof realAppend }).append = ((event, suppressed, options) => {
    if ((event as { sessionId?: string }).sessionId === uuid(800)) throw new Error("injected_append_fault");
    return realAppend(event, suppressed, options as never);
  }) as typeof realAppend;
  const faultPass = await faultTailer.scan({ budget: budget() });
  (faultBuffer as unknown as { append: typeof realAppend }).append = realAppend;
  const retryPass = await faultTailer.scan({ budget: budget() });
  check("a_failure_inside_one_document_is_contained_and_retried_by_the_next_sweep",
    faultPass.readErrors === 1 && grokRows(faultBuffer, uuid(801)).length === 1 &&
      faultPass.eventsAppended === 1 && !faultPass.exhaustive &&
      retryPass.eventsAppended === 1 && grokRows(faultBuffer, uuid(800)).length === 1 && retryPass.exhaustive,
    { fault: { readErrors: faultPass.readErrors, events: faultPass.eventsAppended },
      retry: { events: retryPass.eventsAppended, exhaustive: retryPass.exhaustive } });
  faultTailer.close();
  faultBuffer.close();

  // The Grok scan announces itself to the maintenance boundary and honours
  // a quarantine of its own stage, like the Codex and Claude scans.
  const frameHome = unitHome("frames");
  writeSession(path.join(frameHome, "sessions"), rootGroup, { sessionId: uuid(850), updatedAt: at(10),
    shape: "modern", turns: [{ turnNumber: 1, endedAt: at(11), models: [usage("grok-4.7-build", 1)] }] },
  { content: false });
  const frameBuffer = unitBuffer("frames");
  const emptyRoot = path.join(work, "unit", "frames-empty");
  fs.mkdirSync(emptyRoot, { recursive: true });
  const frameMaintenance = new CollectorMaintenance(frameBuffer,
    new (await import("../packages/collector-cli/src/rollout-tailer")).RolloutTailer(frameBuffer, emptyRoot, () => []),
    new (await import("../packages/collector-cli/src/transcript-tailer")).TranscriptTailer(frameBuffer, emptyRoot),
    undefined, new GrokUsageTailer(frameBuffer, frameHome));
  const frames: Array<{ source: string; stage: string }> = [];
  const quarantined = await frameMaintenance.runRecent({
    quarantine: { source: "grok", stage: "source_scan", candidateHash: null },
    onProgress: (progress) => {
      frames.push({ source: progress.source, stage: progress.stage });
      return true;
    },
  });
  const grokFrameIndex = frames.findIndex((frame) => frame.source === "grok" && frame.stage === "source_scan");
  const released = await frameMaintenance.runRecent({ onProgress: () => true });
  check("the_grok_scan_announces_its_stage_and_honours_its_quarantine",
    grokFrameIndex >= 0 && quarantined.grok?.eventsAppended === 0 &&
      quarantined.grok.activity.scan.deferredBeforeIo === true &&
      (released.grok?.eventsAppended ?? 0) === 1,
    { frames: frames.slice(0, 6), quarantinedEvents: quarantined.grok?.eventsAppended,
      releasedEvents: released.grok?.eventsAppended });
  frameMaintenance.close();
  frameBuffer.close();

  // A session whose usage already arrived live stays with the live path.
  const liveHome = unitHome("live");
  const liveBuffer = unitBuffer("live");
  liveBuffer.append(aiInteractionEventSchema.parse({
    id: deterministicEventId(["grok-usage-proof-live", uuid(700)]),
    source: "grok", dataMode: "metadata", eventType: "assistant_response", observedAt: at(30),
    sessionId: uuid(700), inputTokens: 10, outputTokens: 2, actionClass: "other", metadata: {},
  }));
  writeSession(path.join(liveHome, "sessions"), rootGroup, { sessionId: uuid(700), updatedAt: at(10), shape: "modern",
    turns: [{ turnNumber: 1, endedAt: at(11), models: [usage("grok-4.7-build", 1)] }] }, { content: false });
  const liveTailer = new GrokUsageTailer(liveBuffer, liveHome);
  const livePass = await liveTailer.scan({ budget: budget() });
  check("a_session_with_live_grok_usage_is_left_to_the_live_path",
    livePass.sessionsSkippedLiveCovered === 1 && livePass.eventsAppended === 0 &&
      grokRows(liveBuffer).every((row) => row.eventType === "assistant_response"),
    { skipped: livePass.sessionsSkippedLiveCovered });
  liveTailer.close();
  liveBuffer.close();

  // An invalid GROK_HOME disables only the Grok scan.
  const invalidBuffer = unitBuffer("invalid-home");
  const priorHome = process.env.GROK_HOME;
  process.env.GROK_HOME = "relative/grok";
  let invalidRun: CollectorMaintenanceRunResult | undefined;
  try {
    const invalidCapture = createProfileCapture(invalidBuffer, collectorConfigSchema.parse({}));
    const invalidMaintenance = new CollectorMaintenance(invalidBuffer, invalidCapture.rollout,
      invalidCapture.transcript, undefined, invalidCapture.grok);
    for (let run = 0; run < 3; run += 1) invalidRun = await invalidMaintenance.runRecent();
    invalidCapture.close();
  } finally {
    process.env.GROK_HOME = priorHome;
  }
  check("an_invalid_grok_home_disables_the_grok_scan_without_failing_capture",
    invalidRun?.grok?.home === "invalid" && invalidRun.grok.discoveryErrors === 1 &&
      invalidRun.grok.eventsAppended === 0 && invalidRun.rollout !== undefined,
    { home: invalidRun?.grok?.home });
  invalidBuffer.close();

  // The deterministic identity is (session id, turn number, model).
  check("event_ids_are_deterministic_over_session_turn_and_model",
    expectedIds.length === new Set(expectedIds).size && expectedIds.every((id) => ids.has(id)),
    { ids: expectedIds.length });

  console.log(JSON.stringify({ ok: failures === 0, checks: results.length, results }, null, 2));
  if (failures === 0) completion.complete();
  else process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
