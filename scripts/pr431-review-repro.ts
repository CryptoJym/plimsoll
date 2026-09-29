/** Independent synthetic reproducers for PR #431. Each case asserts the required invariant. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, captureBaselineExcludedReceipt, captureBaselineStatus, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { applyCaptureHistory, planCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { bindCaptureInventory, deriveCaptureRootIdentity, inspectCaptureRoots, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { sealOutboundEnvelope } from "../packages/collector-cli/src/outbound-envelope";
import { useFixtureRoot } from "./lib/fixture-root";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { aiInteractionEventSchema } from "../packages/shared/src/schemas";

const WORKSPACE = "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10";
const DEVICE = "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d";
const START = "2026-01-01T00:00:00.000Z";
const FENCE = "2026-01-03T00:00:00.000Z";
const SESSION = "019d0000-0000-7000-8000-000000000101";

function setup(deliveryEnabled = false) {
  const scratch = fs.realpathSync(fs.mkdtempSync(path.join(path.resolve(process.cwd(), ".."), "pr431-review-")));
  const fixture = useFixtureRoot(scratch, { home: path.join(scratch, "home"),
    plimsollHome: path.join(scratch, "plimsoll-home") });
  fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
  fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });
  const buffer = new LocalEventBuffer(path.join(fixture.env.PLIMSOLL_HOME, "work-ledger.sqlite"), {
    workspaceId: WORKSPACE, deviceId: DEVICE, enrollmentNow: () => new Date(START),
    delivery: { enabled: deliveryEnabled },
  });
  return { scratch, fixture, buffer, close() {
    buffer.close(); fixture.restore(); fs.rmSync(scratch, { recursive: true, force: true });
  } };
}
function captureRoot(directory: string, source: "codex" | "claude_code", buffer: LocalEventBuffer): CaptureRoot {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  return { ...deriveCaptureRootIdentity("review", source, directory), source, directory,
    installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! };
}
function seal(buffer: LocalEventBuffer, source: "codex" | "claude_code", files: string[]) {
  const baseline = beginAutomaticCaptureBaseline(buffer.database, source, { startedAt: START, filesDiscovered: 0 });
  completeAutomaticCaptureBaseline(buffer.database, source, { runId: baseline.latestRun!.runId, completedAt: START });
  const observations = files.map(file => {
    const stat = fs.statSync(file, { bigint: true });
    return { path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs };
  });
  const receipt = sealCaptureBaselineGenerations(buffer.database, source, observations, FENCE);
  assert.equal(receipt.generationsSealed, files.length);
}
function codexFile(directory: string, session: string, totals: number[]) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = path.join(directory, `rollout-2026-01-02T00-00-00-${session}.jsonl`);
  const lines = [JSON.stringify({ type: "session_meta", payload: { id: session } }),
    ...totals.map((value, index) => JSON.stringify({ type: "event_msg",
      timestamp: `2026-01-02T00:00:${String(index + 1).padStart(2, "0")}.000Z`,
      payload: { type: "token_count", info: { total_token_usage: {
        input_tokens: value, output_tokens: value } } },
    }))];
  fs.writeFileSync(file, `${lines.join("\n")}\n`, { mode: 0o600 });
  return file;
}
function claudeLine(value: number, at: string) {
  return JSON.stringify({ type: "assistant", sessionId: SESSION, timestamp: at,
    message: { id: "message-a", model: "claude-sonnet-4-5", usage: {
      input_tokens: value, output_tokens: value } } });
}
function inputTotal(buffer: LocalEventBuffer) {
  return (buffer.database.prepare("select coalesce(sum(input_tokens),0) as n from buffered_events where session_id=?")
    .get(SESSION) as { n: number }).n;
}

async function splitCodexSession() {
  const fixture = setup();
  try {
    const a = captureRoot(path.join(fixture.fixture.home, "profile-a", "sessions"), "codex", fixture.buffer);
    const b = captureRoot(path.join(fixture.fixture.home, "profile-b", "sessions"), "codex", fixture.buffer);
    const files = [codexFile(a.directory, SESSION, [0, 100]), codexFile(b.directory, SESSION, [150])];
    seal(fixture.buffer, "codex", files);
    const first = await applyCaptureHistory(fixture.buffer, a);
    const secondPlan = await planCaptureHistory(fixture.buffer.database, b);
    const second = await applyCaptureHistory(fixture.buffer, b);
    const actual = inputTotal(fixture.buffer);
    console.log(JSON.stringify({ case: "split_codex_session", firstImported: first.importedRows,
      secondPlannedTokens: secondPlan.tokens.input, secondImported: second.importedRows,
      expectedInputTokens: 150, actualInputTokens: actual }));
    assert.equal(actual, 150, "a continuing Codex counter must retain the preceding root's baseline");
    assert.equal((await applyCaptureHistory(fixture.buffer, a)).importedRows, 0);
    assert.equal((await applyCaptureHistory(fixture.buffer, b)).importedRows, 0);
    assert.equal(inputTotal(fixture.buffer), 150, "cross-root replay must keep first-seen ordering");
  } finally { fixture.close(); }
}

async function splitClaudeSession() {
  const fixture = setup();
  try {
    const a = captureRoot(path.join(fixture.fixture.home, "profile-a", "projects"), "claude_code", fixture.buffer);
    const b = captureRoot(path.join(fixture.fixture.home, "profile-b", "projects"), "claude_code", fixture.buffer);
    const firstFile = path.join(a.directory, `${SESSION}.jsonl`);
    const secondFile = path.join(b.directory, `${SESSION}.jsonl`);
    fs.writeFileSync(firstFile, `${claudeLine(100, "2026-01-02T00:00:01.000Z")}\n`, { mode: 0o600 });
    fs.writeFileSync(secondFile, `${claudeLine(150, "2026-01-02T00:00:02.000Z")}\n`, { mode: 0o600 });
    seal(fixture.buffer, "claude_code", [firstFile, secondFile]);
    await applyCaptureHistory(fixture.buffer, a);
    let refusal = "none";
    try { await applyCaptureHistory(fixture.buffer, b); }
    catch (error) { refusal = String(error); }
    const actual = inputTotal(fixture.buffer);
    console.log(JSON.stringify({ case: "split_claude_session", secondRootRefusal: refusal,
      expectedInputTokens: 150,
      actualInputTokens: actual }));
    assert.equal(actual, 150, "a Claude revision in another root must import its marginal 50 tokens");
    assert.equal((await applyCaptureHistory(fixture.buffer, a)).importedRows, 0);
    assert.equal((await applyCaptureHistory(fixture.buffer, b)).importedRows, 0);
    assert.equal(inputTotal(fixture.buffer), 150, "cross-root replay must keep revision ordering");
  } finally { fixture.close(); }
}

async function claudeGrowthAfterImport() {
  const fixture = setup();
  let tailer: TranscriptTailer | null = null;
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "projects"), "claude_code", fixture.buffer);
    const file = path.join(root.directory, `${SESSION}.jsonl`);
    fs.writeFileSync(file, `${claudeLine(100, "2026-01-02T00:00:01.000Z")}\n${claudeLine(150, "2026-01-02T00:00:02.000Z")}\n`,
      { mode: 0o600 });
    bindCaptureInventory(fixture.buffer.database, "claude_code", [root], inspectCaptureRoots([root]));
    seal(fixture.buffer, "claude_code", [file]);
    const codexBaseline = beginAutomaticCaptureBaseline(fixture.buffer.database, "codex", {
      startedAt: START, filesDiscovered: 0,
    });
    completeAutomaticCaptureBaseline(fixture.buffer.database, "codex", {
      runId: codexBaseline.latestRun!.runId, completedAt: START,
    });
    const imported = await applyCaptureHistory(fixture.buffer, root);
    const before = inputTotal(fixture.buffer);
    fs.appendFileSync(file, `${claudeLine(200, "2026-01-04T00:00:03.000Z")}\n`);
    const fileStat = fs.statSync(file, { bigint: true });
    const baseline = captureBaselineExcludedReceipt(fixture.buffer.database, "claude_code", {
      path: file, device: fileStat.dev, inode: fileStat.ino, size: fileStat.size,
      birthtimeNs: fileStat.birthtimeNs,
    });
    tailer = new TranscriptTailer(fixture.buffer, undefined, undefined, [root]);
    let appended = 0;
    let errors = 0;
    const scans: unknown[] = [];
    for (let n = 0; n < 20 && appended === 0; n++) {
      const result = await tailer.scan({ scope: "recent", automatic: {
        phase: "capture", budget: new CaptureWorkBudget() } });
      if (n < 3) scans.push({ filesSeen: result.filesSeen, filesRead: result.filesRead,
        bytesRead: result.bytesRead, recordsParsed: result.recordsParsed,
        excludedGenerations: result.excludedGenerations, deferredGenerations: result.deferredGenerations,
        filesSkippedOutsideRecentWindow: result.filesSkippedOutsideRecentWindow,
        continuationReasons: result.continuationReasons, eventsAppended: result.eventsAppended,
        statErrors: result.statErrors, discoveryErrors: result.discoveryErrors,
        readErrors: result.readErrors, parseErrors: result.parseErrors,
        baselineStatus: captureBaselineStatus(fixture.buffer.database).status,
        sourceStatus: captureBaselineStatus(fixture.buffer.database).sources.find(s => s.source === "claude_code")?.status });
      appended += result.eventsAppended;
      errors += result.readErrors + result.parseErrors;
    }
    const actual = inputTotal(fixture.buffer);
    console.log(JSON.stringify({ case: "claude_growth_after_import", importedRows: imported.importedRows,
      before, tailerAppended: appended, tailerErrors: errors, scans,
      baselineLimit: baseline?.baselineSize ?? null, grownSize: Number(fileStat.size),
      expectedInputTokens: 200, actualInputTokens: actual }));
    assert.equal(appended, 1, "the tailer must capture post-fence growth");
    assert.equal(actual, 200, "the tailer must subtract the imported latest Claude revision");
  } finally { tailer?.close(); fixture.close(); }
}

async function codexGrowthAfterImport() {
  const fixture = setup();
  let tailer: RolloutTailer | null = null;
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const day = new Date().toISOString().slice(0, 10).split("-");
    const file = codexFile(path.join(root.directory, ...day), SESSION, [0, 100]);
    bindCaptureInventory(fixture.buffer.database, "codex", [root], inspectCaptureRoots([root]));
    seal(fixture.buffer, "codex", [file]);
    const claudeBaseline = beginAutomaticCaptureBaseline(fixture.buffer.database, "claude_code", {
      startedAt: START, filesDiscovered: 0,
    });
    completeAutomaticCaptureBaseline(fixture.buffer.database, "claude_code", {
      runId: claudeBaseline.latestRun!.runId, completedAt: START,
    });
    const imported = await applyCaptureHistory(fixture.buffer, root);
    const before = inputTotal(fixture.buffer);
    fs.appendFileSync(file, `${JSON.stringify({ type: "event_msg", timestamp: "2026-01-04T00:00:03.000Z",
      payload: { type: "token_count", info: { total_token_usage: {
        input_tokens: 150, output_tokens: 150 } } } })}\n`);
    tailer = new RolloutTailer(fixture.buffer, undefined, undefined, undefined, [root]);
    let appended = 0;
    let errors = 0;
    const scans: unknown[] = [];
    for (let n = 0; n < 20 && appended === 0; n++) {
      const result = await tailer.scan({ scope: "recent", automatic: {
        phase: "capture", budget: new CaptureWorkBudget() } });
      if (n < 3) scans.push({ filesSeen: result.filesSeen, filesRead: result.filesRead,
        bytesRead: result.bytesRead, recordsParsed: result.recordsParsed,
        excludedGenerations: result.excludedGenerations, deferredGenerations: result.deferredGenerations,
        eventsAppended: result.eventsAppended, statErrors: result.statErrors,
        discoveryErrors: result.discoveryErrors, readErrors: result.readErrors,
        parseErrors: result.parseErrors, baselineStatus: captureBaselineStatus(fixture.buffer.database).status });
      appended += result.eventsAppended;
      errors += result.readErrors + result.parseErrors;
    }
    const actual = inputTotal(fixture.buffer);
    console.log(JSON.stringify({ case: "codex_growth_after_import", importedRows: imported.importedRows,
      before, tailerAppended: appended, tailerErrors: errors, scans,
      expectedInputTokens: 150, actualInputTokens: actual }));
    assert.equal(appended, 1, "the tailer must capture exactly one post-fence delta");
    assert.equal(actual, 150, "the tailer must subtract the imported latest Codex counter");
  } finally { tailer?.close(); fixture.close(); }
}

async function codexGrowthDuringImport() {
  const fixture = setup();
  let tailer: RolloutTailer | null = null;
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const day = new Date().toISOString().slice(0, 10).split("-");
    const file = codexFile(path.join(root.directory, ...day), SESSION, [0, 100]);
    bindCaptureInventory(fixture.buffer.database, "codex", [root], inspectCaptureRoots([root]));
    seal(fixture.buffer, "codex", [file]);
    const baseline = beginAutomaticCaptureBaseline(fixture.buffer.database, "claude_code", {
      startedAt: START, filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(fixture.buffer.database, "claude_code", {
      runId: baseline.latestRun!.runId, completedAt: START });
    fs.appendFileSync(file, `${JSON.stringify({ type: "event_msg", timestamp: "2026-01-04T00:00:03.000Z",
      payload: { type: "token_count", info: { total_token_usage: {
        input_tokens: 150, output_tokens: 150 } } } })}\n`);
    tailer = new RolloutTailer(fixture.buffer, undefined, undefined, undefined, [root]);
    const originalOpen = fs.openSync;
    let readOpens = 0;
    let scanPromise: Promise<Awaited<ReturnType<RolloutTailer["scan"]>>> | null = null;
    try {
      fs.openSync = ((target: fs.PathLike, flags: fs.OpenMode, mode?: fs.Mode) => {
        if (String(target) === file &&
            (flags === "r" || flags === (fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW)) &&
            ++readOpens === 3) {
          setImmediate(() => {
            scanPromise = tailer!.scan({ scope: "recent", automatic: {
              phase: "capture", budget: new CaptureWorkBudget() } });
          });
        }
        return originalOpen(target, flags, mode);
      }) as typeof fs.openSync;
      const imported = await applyCaptureHistory(fixture.buffer, root);
      await new Promise<void>(resolve => setImmediate(resolve));
      const scheduledScan = scanPromise as Promise<Awaited<ReturnType<RolloutTailer["scan"]>>> | null;
      assert.ok(scheduledScan, "Codex tailer must start during import");
      let scan: Awaited<ReturnType<RolloutTailer["scan"]>> = await scheduledScan;
      const firstDeferred = scan.deferredGenerations;
      for (let n = 0; n < 10 && scan.eventsAppended === 0; n++)
        scan = await tailer!.scan({ scope: "recent", automatic: {
          phase: "capture", budget: new CaptureWorkBudget() } });
      const actual = inputTotal(fixture.buffer);
      console.log(JSON.stringify({ case: "codex_growth_during_import", readOpens,
        importedRows: imported.importedRows, firstDeferred, tailerAppended: scan.eventsAppended,
        expectedInputTokens: 150, actualInputTokens: actual }));
      assert.equal(scan.eventsAppended, 1);
      assert.equal(actual, 150);
    } finally { fs.openSync = originalOpen; }
  } finally { tailer?.close(); fixture.close(); }
}

async function tailerDuringImport() {
  const fixture = setup();
  let tailer: TranscriptTailer | null = null;
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "projects"), "claude_code", fixture.buffer);
    const file = path.join(root.directory, `${SESSION}.jsonl`);
    const historical = Array.from({ length: 20 }, (_, index) => JSON.stringify({
      type: "assistant", sessionId: SESSION,
      timestamp: `2026-01-02T00:00:${String(index + 1).padStart(2, "0")}.000Z`,
      message: { id: `message-${index + 1}`, model: "claude-sonnet-4-5",
        usage: { input_tokens: 10, output_tokens: 10 } },
    }));
    fs.writeFileSync(file, `${historical.join("\n")}\n`, { mode: 0o600 });
    bindCaptureInventory(fixture.buffer.database, "claude_code", [root], inspectCaptureRoots([root]));
    seal(fixture.buffer, "claude_code", [file]);
    const codexBaseline = beginAutomaticCaptureBaseline(fixture.buffer.database, "codex", {
      startedAt: START, filesDiscovered: 0,
    });
    completeAutomaticCaptureBaseline(fixture.buffer.database, "codex", {
      runId: codexBaseline.latestRun!.runId, completedAt: START,
    });
    fs.appendFileSync(file, `${JSON.stringify({ type: "assistant", sessionId: SESSION,
      timestamp: "2026-01-04T00:00:01.000Z",
      message: { id: "message-1", model: "claude-sonnet-4-5",
        usage: { input_tokens: 20, output_tokens: 20 } } })}\n`);
    tailer = new TranscriptTailer(fixture.buffer, undefined, undefined, [root]);
    const originalOpen = fs.openSync;
    let readOpens = 0;
    let concurrentLockSeen = false;
    let cursorAtTailerStart: number | null = null;
    let tailerError = "none";
    const tailerScan: { promise: Promise<Awaited<ReturnType<TranscriptTailer["scan"]>> | null> | null } = { promise: null };
    try {
      fs.openSync = ((target: fs.PathLike, flags: fs.OpenMode, mode?: fs.Mode) => {
        if (String(target) === file && (flags === "r" || flags === (fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW)) && ++readOpens === 3) {
          setImmediate(() => {
            concurrentLockSeen = Boolean(fixture.buffer.database.prepare(
              "select 1 from capture_history_import_lock where singleton=1").get());
            cursorAtTailerStart = (fixture.buffer.database.prepare(
              "select resume_candidate_index as n from capture_history_import_runs where root_id=?")
              .get(root.rootId) as { n: number }).n;
            tailerScan.promise = tailer!.scan({ scope: "recent", automatic: {
              phase: "capture", budget: new CaptureWorkBudget() } }).catch(error => {
              tailerError = String(error);
              return null;
            });
          });
        }
        return originalOpen(target, flags, mode);
      }) as typeof fs.openSync;
      const receipt = await applyCaptureHistory(fixture.buffer, root);
      await new Promise<void>(resolve => setImmediate(resolve));
      assert.ok(tailerScan.promise, "tailer scan was scheduled during import");
      let scan = await tailerScan.promise;
      const firstDeferred = scan?.deferredGenerations ?? 0;
      for (let n = 0; n < 10 && scan && scan.eventsAppended === 0; n++)
        scan = await tailer!.scan({ scope: "recent", automatic: {
          phase: "capture", budget: new CaptureWorkBudget() } });
      const total = inputTotal(fixture.buffer);
      console.log(JSON.stringify({ case: "tailer_during_import", readOpens, concurrentLockSeen,
        cursorAtTailerStart, importedRows: receipt.importedRows, tailerAppended: scan?.eventsAppended ?? 0,
        firstDeferred,
        tailerReadErrors: scan?.readErrors ?? null, tailerParseErrors: scan?.parseErrors ?? null,
        tailerError,
        expectedInputTokens: 210, actualInputTokens: total }));
      assert.ok(concurrentLockSeen, "the tailer must run while import holds the lock");
      assert.ok(cursorAtTailerStart !== null && cursorAtTailerStart >= 0 && cursorAtTailerStart < 20,
        "the tailer must start before import completion");
      assert.ok(scan, "concurrent tailer scan must complete");
      assert.equal(scan.eventsAppended, 1);
      assert.equal(total, 210);
    } finally { fs.openSync = originalOpen; }
  } finally { tailer?.close(); fixture.close(); }
}

async function changedPrefixDuringSecondScan() {
  const fixture = setup(true);
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, ...Array.from({ length: 24 }, (_, i) => i + 1)]);
    seal(fixture.buffer, "codex", [file]);
    const originalOpen = fs.openSync;
    let readOpens = 0;
    let refusal = "none";
    try {
      fs.openSync = ((target: fs.PathLike, flags: fs.OpenMode, mode?: fs.Mode) => {
        if (String(target) === file && (flags === "r" || flags === (fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW)) && ++readOpens === 3) {
          const old = fs.readFileSync(file, "utf8");
          const changed = old.replace('"input_tokens":24', '"input_tokens":25');
          assert.notEqual(changed, old);
          fs.writeFileSync(file, changed);
        }
        return originalOpen(target, flags, mode);
      }) as typeof fs.openSync;
      try { await applyCaptureHistory(fixture.buffer, root); }
      catch (error) { refusal = String(error); }
    } finally { fs.openSync = originalOpen; }
    const committed = (fixture.buffer.database.prepare("select count(*) as n from buffered_events where session_id=?")
      .get(SESSION) as { n: number }).n;
    const queued = (fixture.buffer.database.prepare("select count(*) as n from upload_outbox")
      .get() as { n: number }).n;
    const cursor = fixture.buffer.database.prepare("select resume_candidate_index as n from capture_history_import_runs where root_id=?")
      .get(root.rootId) as { n: number } | undefined;
    console.log(JSON.stringify({ case: "changed_prefix_during_second_scan", readOpens,
      refusedChangedPrefix: refusal.includes("fenced_prefix_changed"), committedRows: committed,
      queuedUploadRows: queued, durableCandidateIndex: cursor?.n ?? null }));
    assert.ok(refusal.includes("fenced_prefix_changed"), "changed prefix must refuse");
    assert.equal(committed, 0, "a refused changed prefix must leave no imported rows");
    assert.equal(queued, 0, "a refused changed prefix must leave no eligible uploads");
    const retry = await applyCaptureHistory(fixture.buffer, root);
    assert.equal(retry.importedRows, 24, "a stable changed prefix must re-preflight from byte zero");
    assert.equal(inputTotal(fixture.buffer), 25);
  } finally { fixture.close(); }
}

async function changedLaterFileAfterEarlierPublication() {
  const fixture = setup(true);
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const laterSession = "019d0000-0000-7000-8000-000000000102";
    const firstFile = codexFile(root.directory, SESSION, [0, 1]);
    const laterFile = codexFile(root.directory, laterSession, [0, 2]);
    seal(fixture.buffer, "codex", [firstFile, laterFile]);
    const originalOpen = fs.openSync;
    let readOpens = 0;
    let refusal = "none";
    try {
      fs.openSync = ((target: fs.PathLike, flags: fs.OpenMode, mode?: fs.Mode) => {
        if (String(target) === laterFile &&
            (flags === "r" || flags === (fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW)) &&
            ++readOpens === 3) {
          const old = fs.readFileSync(laterFile, "utf8");
          const changed = old.replace('"input_tokens":2', '"input_tokens":3')
            .replace('"output_tokens":2', '"output_tokens":3');
          assert.notEqual(changed, old);
          fs.writeFileSync(laterFile, changed);
        }
        return originalOpen(target, flags, mode);
      }) as typeof fs.openSync;
      try { await applyCaptureHistory(fixture.buffer, root); }
      catch (error) { refusal = String(error); }
    } finally { fs.openSync = originalOpen; }
    const rowCount = () => (fixture.buffer.database.prepare("select count(*) as n from buffered_events")
      .get() as { n: number }).n;
    const queuedCount = () => (fixture.buffer.database.prepare("select count(*) as n from upload_outbox")
      .get() as { n: number }).n;
    const beforeRetry = { rows: rowCount(), queued: queuedCount() };
    assert.ok(refusal.includes("fenced_prefix_changed"), "later changed file must refuse");
    assert.deepEqual(beforeRetry, { rows: 1, queued: 1 },
      "only the earlier verified file may have published rows");
    const retry = await applyCaptureHistory(fixture.buffer, root);
    const afterRetry = { rows: rowCount(), queued: queuedCount() };
    console.log(JSON.stringify({ case: "changed_later_file_after_earlier_publication", readOpens,
      refusal, beforeRetry, retryImported: retry.importedRows, afterRetry }));
    assert.equal(retry.importedRows, 1, "a stable uncommitted file must re-preflight from byte zero");
    assert.deepEqual(afterRetry, { rows: 2, queued: 2 });
  } finally { fixture.close(); }
}

async function noSchemaWorkInsideWriterSlice() {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, 1]);
    seal(fixture.buffer, "codex", [file]);
    const db = fixture.buffer.database;
    const originalExec = db.exec;
    let schemaCallsInsideWriter = 0;
    try {
      db.exec = ((sql: string) => {
        if (db.inTransaction && sql.includes("create table if not exists capture_root_observations"))
          schemaCallsInsideWriter += 1;
        return originalExec.call(db, sql);
      }) as typeof db.exec;
      const receipt = await applyCaptureHistory(fixture.buffer, root);
      console.log(JSON.stringify({ case: "no_schema_work_inside_writer_slice",
        importedRows: receipt.importedRows, schemaCallsInsideWriter }));
      assert.equal(receipt.importedRows, 1);
      assert.equal(schemaCallsInsideWriter, 0,
        "history writer slices must not repeat schema DDL for every row");
    } finally { db.exec = originalExec; }
  } finally { fixture.close(); }
}

async function resumeCursorBinding() {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, ...Array.from({ length: 20 }, (_, i) => i + 1)]);
    seal(fixture.buffer, "codex", [file]);
    let injectedCrash = false;
    try { await applyCaptureHistory(fixture.buffer, root, { stopAfterSlices: 1 }); }
    catch (error) { injectedCrash = String(error).includes("capture_history_injected_crash"); }
    assert.ok(injectedCrash);
    const run = fixture.buffer.database.prepare(`select resume_candidate_index as candidateIndex,
      resume_candidate_digest as candidateDigest from capture_history_import_runs where root_id=?`)
      .get(root.rootId) as { candidateIndex: number; candidateDigest: string };
    const committed = () => (fixture.buffer.database.prepare("select count(*) as n from buffered_events where session_id=?")
      .get(SESSION) as { n: number }).n;
    const initialRows = committed();
    fixture.buffer.database.prepare("update capture_history_import_runs set resume_candidate_digest=? where root_id=?")
      .run("0".repeat(64), root.rootId);
    let digestRefusal = "none";
    try { await applyCaptureHistory(fixture.buffer, root); }
    catch (error) { digestRefusal = String(error); }
    const afterDigest = committed();
    fixture.buffer.database.prepare(`update capture_history_import_runs set resume_candidate_index=?,
      resume_candidate_digest=? where root_id=?`).run(999, run.candidateDigest, root.rootId);
    let positionRefusal = "none";
    try { await applyCaptureHistory(fixture.buffer, root); }
    catch (error) { positionRefusal = String(error); }
    const afterPosition = committed();
    console.log(JSON.stringify({ case: "resume_cursor_binding", initialCandidateIndex: run.candidateIndex,
      initialRows, digestRefusal, afterDigest, positionRefusal, afterPosition }));
    assert.ok(digestRefusal.includes("resume_cursor_digest_changed"), "tampered candidate digest must refuse");
    assert.equal(afterDigest, initialRows, "tampered candidate digest must commit no additional rows");
    assert.ok(positionRefusal.includes("resume_cursor_evidence_lost"), "tampered candidate position must refuse");
    assert.equal(afterPosition, initialRows, "tampered candidate position must commit no additional rows");
  } finally { fixture.close(); }
}

async function prewriteCrashRecovery() {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, 10]);
    seal(fixture.buffer, "codex", [file]);
    const originalOpen = fs.openSync;
    let readOpens = 0;
    let interrupted = false;
    try {
      fs.openSync = ((target: fs.PathLike, flags: fs.OpenMode, mode?: fs.Mode) => {
        if (String(target) === file && (flags === "r" || flags === (fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW)) && ++readOpens === 2)
          throw new Error("review_injected_prewrite_crash");
        return originalOpen(target, flags, mode);
      }) as typeof fs.openSync;
      try { await applyCaptureHistory(fixture.buffer, root); }
      catch (error) { interrupted = String(error).includes("review_injected_prewrite_crash"); }
    } finally { fs.openSync = originalOpen; }
    const beforeRows = (fixture.buffer.database.prepare("select count(*) as n from buffered_events where session_id=?")
      .get(SESSION) as { n: number }).n;
    const runExists = Boolean(fixture.buffer.database.prepare(
      "select 1 from capture_history_import_runs where root_id=?").get(root.rootId));
    const resumed = await applyCaptureHistory(fixture.buffer, root);
    console.log(JSON.stringify({ case: "prewrite_crash_recovery", readOpens, interrupted,
      runExists, beforeRows, resumedRows: resumed.importedRows, totalRows: resumed.totalImportedRows }));
    assert.ok(interrupted);
    assert.ok(runExists);
    assert.equal(beforeRows, 0);
    assert.equal(resumed.importedRows, 1);
    assert.equal(resumed.totalImportedRows, 1);
  } finally { fixture.close(); }
}

async function sameProcessConcurrentImport() {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, ...Array.from({ length: 20 }, (_, i) => i + 1)]);
    seal(fixture.buffer, "codex", [file]);
    const results = await Promise.allSettled([
      applyCaptureHistory(fixture.buffer, root), applyCaptureHistory(fixture.buffer, root),
    ]);
    const statuses = results.map(result => result.status);
    const refusals = results.map(result => result.status === "rejected" ? String(result.reason) : null);
    const imported = results.map(result => result.status === "fulfilled" ? result.value.importedRows : null);
    const actual = inputTotal(fixture.buffer);
    console.log(JSON.stringify({ case: "same_process_concurrent_import", statuses, refusals,
      imported, actualInputTokens: actual }));
    assert.equal(statuses.filter(status => status === "fulfilled").length, 1,
      "only one concurrent import may own the ledger");
    assert.deepEqual(imported.filter((value): value is number => value !== null), [20],
      "the sole accepted import must account for every row it commits");
    assert.ok(refusals.some(value => value?.includes("import_in_progress")),
    "the losing import must refuse before it writes");
    assert.equal(actual, 20, "the accepted import must finish without losing rows");
  } finally { fixture.close(); }
}

async function outboundProvenance(requireImportMarker = false) {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, 10]);
    seal(fixture.buffer, "codex", [file]);
    const applied = await applyCaptureHistory(fixture.buffer, root);
    const row = fixture.buffer.database.prepare("select payload_json as payload from buffered_events where session_id=?")
      .get(SESSION) as { payload: string };
    const local = JSON.parse(row.payload) as { id: string; observedAt: string; metadata: Record<string, unknown> };
    const sealed = sealOutboundEnvelope({ event: local, suppressedFields: [] });
    assert.equal(sealed.ok, true);
    if (!sealed.ok) return;
    const wire = sealed.envelope.event;
    const metadata = wire.metadata;
    console.log(JSON.stringify({ case: requireImportMarker ? "outbound_history_marker" : "outbound_provenance",
      idStable: wire.id === local.id,
      originalTime: wire.observedAt, captureRootId: metadata.captureRootId,
      logicalSourceEventId: metadata.logicalSourceEventId,
      sourceIdentityEvidenceRef: metadata.sourceIdentityEvidenceRef,
      installationEpochId: metadata.installationEpochId,
      localHistoryMarker: local.metadata.historyImport,
      outboundHistoryMarker: metadata.historyImport ?? null,
      suppressedHistoryKey: sealed.envelope.suppressedFields.includes("historyImport"),
      sourcePathPresent: JSON.stringify(sealed.envelope).includes(root.directory),
      sourcePathInReceipt: JSON.stringify(applied).includes(root.directory),
      sessionIdInReceipt: JSON.stringify(applied).includes(SESSION) }));
    assert.equal(wire.id, local.id);
    assert.equal(wire.observedAt, "2026-01-02T00:00:02.000Z");
    assert.equal(metadata.captureRootId, root.rootId);
    assert.equal(metadata.logicalSourceEventId, wire.id);
    assert.equal(metadata.sourceIdentityEvidenceRef, "native_runtime_event_v1");
    assert.equal(metadata.installationEpochId, root.installationEpochId);
    assert.ok(!JSON.stringify(sealed.envelope).includes(root.directory));
    assert.ok(!JSON.stringify(applied).includes(root.directory),
      "import receipt must contain no source path");
    assert.ok(!JSON.stringify(applied).includes(SESSION));
    if (requireImportMarker) assert.equal(metadata.historyImport, local.metadata.historyImport,
      "an uploaded import row must retain its explicit history provenance marker");
  } finally { fixture.close(); }
}

async function applyReceiptOverwrite() {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, 10]);
    seal(fixture.buffer, "codex", [file]);
    const config = collectorConfigSchema.parse({ tenantId: WORKSPACE, deviceId: DEVICE,
      installKey: "fixture-review-receipt", captureRoots: [root] });
    fs.writeFileSync(path.join(fixture.fixture.env.PLIMSOLL_HOME, "collector.config.json"),
      `${JSON.stringify(config)}\n`, { mode: 0o600 });
    fixture.buffer.close();
    const repo = path.resolve(import.meta.dirname, "..");
    const cli = path.join(repo, "packages/collector-cli/src/cli.ts");
    const invoke = () => spawnSync(process.execPath,
      ["--import", "tsx", cli, "capture-roots", "import-history", "--root", root.rootId,
        "--apply", "--json"], { cwd: repo, env: { ...process.env, ...fixture.fixture.env }, encoding: "utf8" });
    const first = invoke();
    assert.equal(first.status, 0, `${first.stdout}\n${first.stderr}`);
    const firstJson = JSON.parse(first.stdout) as { importedRows: number; receiptPath: string; runId: string };
    const firstBytes = fs.readFileSync(firstJson.receiptPath, "utf8");
    const second = invoke();
    assert.equal(second.status, 0, `${second.stdout}\n${second.stderr}`);
    const secondJson = JSON.parse(second.stdout) as { importedRows: number; receiptPath: string; runId: string };
    const after = fs.readFileSync(firstJson.receiptPath, "utf8");
    console.log(JSON.stringify({ case: "apply_receipt_overwrite", firstImported: firstJson.importedRows,
      secondImported: secondJson.importedRows, sameRunId: firstJson.runId === secondJson.runId,
      sameReceiptPath: firstJson.receiptPath === secondJson.receiptPath,
      firstReceiptPreserved: firstBytes === after }));
    assert.ok(firstJson.receiptPath !== secondJson.receiptPath,
      "each apply attempt needs a distinct durable receipt for the runbook audit trail");
    assert.equal(firstBytes, after, "a later apply must preserve its predecessor's receipt");
    assert.ok(fs.existsSync(firstJson.receiptPath.replace(/\.json$/, ".started.json")));
    assert.ok(fs.existsSync(secondJson.receiptPath.replace(/\.json$/, ".started.json")));
  } finally {
    // The CLI proof closes this buffer before invoking its own process.
    fixture.fixture.restore();
    fs.rmSync(fixture.scratch, { recursive: true, force: true });
  }
}

async function privateCliFailure() {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, 10]);
    seal(fixture.buffer, "codex", [file]);
    const config = collectorConfigSchema.parse({ tenantId: WORKSPACE, deviceId: DEVICE,
      installKey: "fixture-review-private-failure", captureRoots: [root] });
    fs.writeFileSync(path.join(fixture.fixture.env.PLIMSOLL_HOME, "collector.config.json"),
      `${JSON.stringify(config)}\n`, { mode: 0o600 });
    fixture.buffer.close();
    fs.chmodSync(file, 0o000);
    const repo = path.resolve(import.meta.dirname, "..");
    const cli = path.join(repo, "packages/collector-cli/src/cli.ts");
    const attempt = spawnSync(process.execPath,
      ["--import", "tsx", cli, "capture-roots", "import-history", "--root", root.rootId,
        "--apply", "--json"], { cwd: repo, env: { ...process.env, ...fixture.fixture.env }, encoding: "utf8" });
    const output = `${attempt.stdout}${attempt.stderr}`;
    console.log(JSON.stringify({ case: "private_cli_failure", exit: attempt.status,
      pathLeaked: output.includes(file) || output.includes(root.directory),
      sessionIdLeaked: output.includes(SESSION),
      contentLeaked: output.includes("token_count"),
      genericRefusal: output.includes("import_failed") }));
    assert.equal(attempt.status, 1);
    const refused = JSON.parse(attempt.stdout) as { status: string; reason: string;
      receiptPath?: string; attemptId?: string };
    assert.equal(refused.status, "capture_roots_history_refused");
    assert.equal(refused.reason, "import_failed");
    assert.ok(refused.receiptPath && fs.existsSync(refused.receiptPath),
      "a refused apply must retain its own receipt");
    assert.ok(fs.existsSync(refused.receiptPath.replace(/\.json$/, ".started.json")),
      "an interrupted apply must leave an append-only start marker");
    assert.ok(output.includes("import_failed"), "an unexpected read error must be redacted");
    assert.ok(!output.includes(file) && !output.includes(root.directory), "CLI failure must hide session paths");
    assert.ok(!output.includes(SESSION) && !output.includes("token_count"), "CLI failure must hide session data");
  } finally {
    fixture.fixture.restore();
    fs.rmSync(fixture.scratch, { recursive: true, force: true });
  }
}

async function sinceWindow() {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, 10, 20]);
    seal(fixture.buffer, "codex", [file]);
    const since = "2026-01-02T00:00:03.000Z";
    const plan = await planCaptureHistory(fixture.buffer.database, root, { since });
    const applied = await applyCaptureHistory(fixture.buffer, root, { since });
    const row = fixture.buffer.database.prepare("select observed_at as at, input_tokens as tokens from buffered_events where session_id=?")
      .get(SESSION) as { at: string; tokens: number };
    const replay = await applyCaptureHistory(fixture.buffer, root, { since });
    let changedWindowRefused = false;
    try { await applyCaptureHistory(fixture.buffer, root, { since: "2026-01-02T00:00:02.000Z" }); }
    catch (error) { changedWindowRefused = String(error).includes("fenced_history_changed_since_import"); }
    console.log(JSON.stringify({ case: "since_window", plannedRows: plan.missingRows,
      plannedTokens: plan.tokens.input, importedRows: applied.importedRows,
      originalTime: row.at, importedTokens: row.tokens,
      replayRows: replay.importedRows, changedWindowRefused }));
    assert.equal(plan.missingRows, 1);
    assert.equal(plan.tokens.input, 10);
    assert.equal(applied.importedRows, 1);
    assert.equal(row.at, since);
    assert.equal(row.tokens, 10);
    assert.equal(replay.importedRows, 0);
    assert.equal(changedWindowRefused, true);
  } finally { fixture.close(); }
}

async function originalObservedTime() {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, 10]);
    seal(fixture.buffer, "codex", [file]);
    const receipt = await applyCaptureHistory(fixture.buffer, root);
    const row = fixture.buffer.database.prepare("select observed_at as at from buffered_events where session_id=?")
      .get(SESSION) as { at: string };
    console.log(JSON.stringify({ case: "original_observed_time", importedRows: receipt.importedRows,
      expected: "2026-01-02T00:00:02.000Z", actual: row.at }));
    assert.equal(receipt.importedRows, 1);
    assert.equal(row.at, "2026-01-02T00:00:02.000Z", "import must retain the source record time");
  } finally { fixture.close(); }
}

async function ordinaryCaptureFence() {
  const fixture = setup();
  try {
    const before = aiInteractionEventSchema.parse({ id: crypto.randomUUID(), tenantId: "local",
      source: "codex", dataMode: "metadata", eventType: "usage_rollout",
      observedAt: "2025-12-31T23:59:59.000Z", sessionId: SESSION,
      actionClass: "other", inputTokens: 1, outputTokens: 1, metadata: {} });
    const after = aiInteractionEventSchema.parse({ ...before, id: crypto.randomUUID(),
      observedAt: "2026-01-02T00:00:01.000Z" });
    const beforeAccepted = fixture.buffer.append(before, []);
    const afterAccepted = fixture.buffer.append(after, []);
    const count = (fixture.buffer.database.prepare("select count(*) as n from buffered_events where session_id=?")
      .get(SESSION) as { n: number }).n;
    console.log(JSON.stringify({ case: "ordinary_capture_fence", beforeAccepted, afterAccepted,
      rows: count }));
    assert.equal(beforeAccepted, false, "ordinary capture must refuse a pre-enrollment row");
    assert.equal(afterAccepted, true);
    assert.equal(count, 1);
  } finally { fixture.close(); }
}

async function symlinkAndReplacement() {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, 10]);
    seal(fixture.buffer, "codex", [file]);
    const alias = path.join(root.directory, "alias.jsonl");
    fs.symlinkSync(file, alias);
    let symlinkRefusal = "none";
    try { await planCaptureHistory(fixture.buffer.database, root); }
    catch (error) { symlinkRefusal = String(error); }
    fs.rmSync(alias);
    const original = fs.readFileSync(file);
    fs.renameSync(file, `${file}.old`);
    fs.writeFileSync(file, original, { mode: 0o600 });
    let replacementRefusal = "none";
    try { await planCaptureHistory(fixture.buffer.database, root); }
    catch (error) { replacementRefusal = String(error); }
    const committed = (fixture.buffer.database.prepare("select count(*) as n from buffered_events where session_id=?")
      .get(SESSION) as { n: number }).n;
    console.log(JSON.stringify({ case: "symlink_and_replacement",
      symlinkRefused: symlinkRefusal.includes("root_symlink_entry"),
      replacementRefused: replacementRefusal.includes("no_fenced_generation_evidence"),
      committedRows: committed }));
    assert.ok(symlinkRefusal.includes("root_symlink_entry"));
    assert.ok(replacementRefusal.includes("no_fenced_generation_evidence"));
    assert.equal(committed, 0);
  } finally { fixture.close(); }
}

async function symlinkSwapDuringSecondScan() {
  const fixture = setup();
  try {
    const root = captureRoot(path.join(fixture.fixture.home, "profile", "sessions"), "codex", fixture.buffer);
    const file = codexFile(root.directory, SESSION, [0, 10]);
    seal(fixture.buffer, "codex", [file]);
    const moved = path.join(fixture.scratch, "moved-original.jsonl");
    const originalOpen = fs.openSync;
    let readOpens = 0;
    let refusal = "none";
    try {
      fs.openSync = ((target: fs.PathLike, flags: fs.OpenMode, mode?: fs.Mode) => {
        if (String(target) === file && (flags === "r" || flags === (fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW)) && ++readOpens === 3) {
          fs.renameSync(file, moved);
          fs.symlinkSync(moved, file);
        }
        return originalOpen(target, flags, mode);
      }) as typeof fs.openSync;
      try { await applyCaptureHistory(fixture.buffer, root); }
      catch (error) { refusal = String(error); }
    } finally { fs.openSync = originalOpen; }
    const committed = (fixture.buffer.database.prepare("select count(*) as n from buffered_events where session_id=?")
      .get(SESSION) as { n: number }).n;
    console.log(JSON.stringify({ case: "symlink_swap_during_second_scan", readOpens,
      pathNowSymlink: fs.lstatSync(file).isSymbolicLink(), refusal, committedRows: committed }));
    assert.notEqual(refusal, "none", "a file swapped to a symlink after discovery must refuse");
    assert.equal(committed, 0);
  } finally { fixture.close(); }
}

const cases: Record<string, () => Promise<void>> = {
  split: splitCodexSession, split_claude: splitClaudeSession,
  growth: claudeGrowthAfterImport, growth_codex: codexGrowthAfterImport,
  growth_codex_race: codexGrowthDuringImport,
  tailer_race: tailerDuringImport,
  changed: changedPrefixDuringSecondScan,
  changed_later: changedLaterFileAfterEarlierPublication, cursor: resumeCursorBinding,
  writer_schema: noSchemaWorkInsideWriterSlice,
  prewrite_crash: prewriteCrashRecovery,
  concurrent: sameProcessConcurrentImport, provenance: outboundProvenance,
  provenance_marker: () => outboundProvenance(true),
  receipt: applyReceiptOverwrite, privacy_cli: privateCliFailure,
  since: sinceWindow, original_time: originalObservedTime,
  ordinary_fence: ordinaryCaptureFence,
  symlink: symlinkAndReplacement, symlink_race: symlinkSwapDuringSecondScan,
};
const groups: Record<string, string[]> = {
  f1: ["changed", "changed_later"],
  f2: ["growth", "growth_codex", "growth_codex_race", "tailer_race"],
  f3: ["split", "split_claude"],
  f4: ["concurrent"],
  f5: ["provenance_marker"],
  f6: ["receipt", "privacy_cli"],
  f7: ["symlink_race", "symlink"],
};
const selected = process.argv[2];
if (!selected || (!cases[selected] && !groups[selected])) throw new Error("choose a review case or f1 through f7");
(async () => {
  for (const name of groups[selected] ?? [selected]) await cases[name]!();
})().catch(error => { console.error(error); process.exitCode = 1; });
