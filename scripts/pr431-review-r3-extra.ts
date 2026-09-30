/** Independent round-3 edge cases for copied history and parent paths. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { applyCaptureHistory, planCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { deriveCaptureRootIdentity, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { useFixtureRoot } from "./lib/fixture-root";

const SESSION = "019d0000-0000-7000-8000-000000000101";
const START = "2026-01-01T00:00:00.000Z";
const FENCE = "2026-01-03T00:00:00.000Z";
type Source = "codex" | "claude_code";

function setup() {
  const scratch = fs.realpathSync(fs.mkdtempSync(path.join(path.resolve(process.cwd(), ".."), "pr431-r3-")));
  const fixture = useFixtureRoot(scratch, { home: path.join(scratch, "home"),
    plimsollHome: path.join(scratch, "plimsoll-home") });
  fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
  fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });
  const buffer = new LocalEventBuffer(path.join(fixture.env.PLIMSOLL_HOME, "work-ledger.sqlite"), {
    workspaceId: "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10",
    deviceId: "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d",
    enrollmentNow: () => new Date(START),
  });
  return { scratch, fixture, buffer, close() {
    buffer.close(); fixture.restore(); fs.rmSync(scratch, { recursive: true, force: true });
  } };
}
function root(directory: string, source: Source, buffer: LocalEventBuffer): CaptureRoot {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  return { ...deriveCaptureRootIdentity("review", source, directory), source, directory,
    installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! };
}
function seal(buffer: LocalEventBuffer, source: Source, files: string[]) {
  const baseline = beginAutomaticCaptureBaseline(buffer.database, source,
    { startedAt: START, filesDiscovered: 0 });
  completeAutomaticCaptureBaseline(buffer.database, source,
    { runId: baseline.latestRun!.runId, completedAt: START });
  sealCaptureBaselineGenerations(buffer.database, source, files.map(file => {
    const stat = fs.statSync(file, { bigint: true });
    return { path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs };
  }), FENCE);
}
function codexLines(values: number[]) {
  return [JSON.stringify({ type: "session_meta", payload: { id: SESSION } }),
    ...values.map((value, index) => JSON.stringify({ type: "event_msg",
      timestamp: `2026-01-02T00:00:${String(index + 1).padStart(2, "0")}.000Z`,
      payload: { type: "token_count", info: { total_token_usage: {
        input_tokens: value, output_tokens: value } } },
    }))];
}
function claudeLines(values: number[]) {
  return values.map((value, index) => JSON.stringify({ type: "assistant", sessionId: SESSION,
    timestamp: `2026-01-02T00:00:${String(index + 1).padStart(2, "0")}.000Z`,
    message: { id: "message-a", model: "claude-sonnet-4-5", usage: {
      input_tokens: value, output_tokens: value } } }));
}
function write(root: CaptureRoot, values: number[], filler?: string, tail?: number) {
  const file = root.source === "codex"
    ? path.join(root.directory, `rollout-2026-01-02T00-00-00-${SESSION}.jsonl`)
    : path.join(root.directory, `${SESSION}.jsonl`);
  const lines = root.source === "codex" ? codexLines(values) : claudeLines(values);
  if (filler !== undefined) lines.push(JSON.stringify(root.source === "codex"
    ? { type: "turn_context", payload: { model: `gpt-5.${filler}-codex` } }
    : { type: "message", payload: { content: filler } }));
  if (tail !== undefined) lines.push(...(root.source === "codex" ? codexLines([...values, tail]).slice(values.length + 1)
    : claudeLines([...values, tail]).slice(values.length)));
  fs.writeFileSync(file, `${lines.join("\n")}\n`, { mode: 0o600 });
  return file;
}
function inputTotal(buffer: LocalEventBuffer) {
  return (buffer.database.prepare("select coalesce(sum(input_tokens),0) as n from buffered_events where session_id=?")
    .get(SESSION) as { n: number }).n;
}
function rowCount(buffer: LocalEventBuffer) {
  return (buffer.database.prepare("select count(*) as n from buffered_events where session_id=?")
    .get(SESSION) as { n: number }).n;
}
async function threeFolders(source: Source) {
  const permutations = [[0, 1, 2], [0, 2, 1], [1, 0, 2],
    [1, 2, 0], [2, 0, 1], [2, 1, 0]];
  const rows = [];
  for (const order of permutations) {
    const f = setup();
    try {
      const folder = source === "codex" ? "sessions" : "projects";
      const roots = [0, 1, 2].map(index => root(path.join(f.fixture.home,
        `profile-${index}`, folder), source, f.buffer));
      const files = roots.map((r, index) => write(r, source === "codex"
        ? [0, ...[100, 150, 200].slice(0, index + 1)]
        : [100, 150, 200].slice(0, index + 1)));
      seal(f.buffer, source, files);
      const firstPass = [];
      for (const index of order) firstPass.push((await applyCaptureHistory(f.buffer, roots[index]!)).importedRows);
      const replays = [];
      for (const r of roots) replays.push((await applyCaptureHistory(f.buffer, r)).importedRows);
      const total = inputTotal(f.buffer);
      rows.push({ order, firstPass, replays, total });
      assert.equal(total, 200);
      assert.deepEqual(replays, [0, 0, 0]);
    } finally { f.close(); }
  }
  console.log(JSON.stringify({ case: "three_folders", source, rows }));
}
async function oneByteUsage(source: Source) {
  const f = setup();
  try {
    const folder = source === "codex" ? "sessions" : "projects";
    const a = root(path.join(f.fixture.home, "a", folder), source, f.buffer);
    const b = root(path.join(f.fixture.home, "b", folder), source, f.buffer);
    const first = write(a, source === "codex" ? [0, 100, 150] : [100, 150]);
    const second = write(b, source === "codex" ? [0, 100, 150, 200] : [100, 150, 200]);
    // Exactly one timestamp byte differs in the already imported prefix.
    const original = fs.readFileSync(second, "utf8");
    const changed = original.replace("00:00:02.000Z", "00:00:12.000Z");
    assert.notEqual(changed, original);
    fs.writeFileSync(second, changed);
    seal(f.buffer, source, [first, second]);
    await applyCaptureHistory(f.buffer, a);
    let refusal = "none";
    try { await applyCaptureHistory(f.buffer, b); }
    catch (error) { refusal = String(error); }
    const result = { case: "one_byte_usage", source, refusal, total: inputTotal(f.buffer) };
    console.log(JSON.stringify(result));
    assert.match(refusal, /counter_regression/);
    assert.equal(result.total, 150);
  } finally { f.close(); }
}
async function oneByteEqualCounter(source: Source, changedPrefix = true) {
  const f = setup();
  try {
    const folder = source === "codex" ? "sessions" : "projects";
    const a = root(path.join(f.fixture.home, "a", folder), source, f.buffer);
    const b = root(path.join(f.fixture.home, "b", folder), source, f.buffer);
    const first = write(a, [150]);
    const second = write(b, [150, 200]);
    const original = fs.readFileSync(second, "utf8");
    const changed = changedPrefix
      ? original.replace("00:00:01.000Z", "00:00:11.000Z") : original;
    if (changedPrefix) assert.notEqual(original, changed);
    fs.writeFileSync(second, changed);
    const originalPrefix = fs.readFileSync(first, "utf8");
    const copiedPrefix = changed.slice(0, originalPrefix.length);
    assert.equal([...originalPrefix].filter((byte, i) => byte !== copiedPrefix[i]).length,
      changedPrefix ? 1 : 0);
    seal(f.buffer, source, [first, second]);
    const firstRows = (await applyCaptureHistory(f.buffer, a)).importedRows;
    const before = inputTotal(f.buffer);
    const checkpointRows = (f.buffer.database.prepare(`select count(*) as n
      from capture_history_import_prefixes where source=? and session_id=?`)
      .get(source, SESSION) as { n: number }).n;
    let refusal = "none";
    let secondRows = 0;
    try { secondRows = (await applyCaptureHistory(f.buffer, b)).importedRows; }
    catch (error) { refusal = String(error); }
    const replayRows = refusal === "none" ? (await applyCaptureHistory(f.buffer, b)).importedRows : null;
    const result = { case: changedPrefix ? "one_byte_equal_counter" : "identical_equal_counter",
      source, firstRows, before, checkpointRows,
      refusal, secondRows, replayRows, total: inputTotal(f.buffer),
      originalPrefixSha256: crypto.createHash("sha256").update(originalPrefix).digest("hex"),
      copiedPrefixSha256: crypto.createHash("sha256").update(copiedPrefix).digest("hex") };
    console.log(JSON.stringify(result));
    if (changedPrefix) {
      assert.match(refusal, /counter_regression/,
        "a changed imported usage record with the same durable counter must refuse");
      assert.equal(result.total, before);
    } else {
      assert.equal(refusal, "none");
      assert.equal(result.total, before + 50);
      assert.equal(replayRows, 0);
    }
  } finally { f.close(); }
}
async function claudeChangedMessageId() {
  const f = setup();
  try {
    const a = root(path.join(f.fixture.home, "a", "projects"), "claude_code", f.buffer);
    const b = root(path.join(f.fixture.home, "b", "projects"), "claude_code", f.buffer);
    const first = write(a, [150]);
    const second = write(b, [150, 200]);
    const original = fs.readFileSync(second, "utf8");
    const changed = original.replaceAll("message-a", "message-b");
    const firstBytes = fs.readFileSync(first, "utf8");
    const prefix = changed.slice(0, firstBytes.length);
    assert.equal([...firstBytes].filter((byte, i) => byte !== prefix[i]).length, 1);
    fs.writeFileSync(second, changed);
    seal(f.buffer, "claude_code", [first, second]);
    await applyCaptureHistory(f.buffer, a);
    const planned = await planCaptureHistory(f.buffer.database, b);
    let refusal = "none";
    let secondRows = 0;
    try { secondRows = (await applyCaptureHistory(f.buffer, b)).importedRows; }
    catch (error) { refusal = String(error); }
    const replayRows = refusal === "none" ? (await applyCaptureHistory(f.buffer, b)).importedRows : null;
    const result = { case: "claude_changed_message_id", refusal,
      plannedRows: planned.missingRows, plannedInputTokens: planned.tokens.input,
      secondRows, replayRows,
      total: inputTotal(f.buffer), rows: rowCount(f.buffer),
      originalPrefixSha256: crypto.createHash("sha256").update(firstBytes).digest("hex"),
      copiedPrefixSha256: crypto.createHash("sha256").update(prefix).digest("hex") };
    console.log(JSON.stringify(result));
    assert.match(refusal, /counter_regression/,
      "a copied Claude message with a changed ID must not import the old usage twice");
    assert.equal(result.total, 150);
  } finally { f.close(); }
}
async function oneByteAfterLastUsage(source: Source, changed = true) {
  const f = setup();
  try {
    const folder = source === "codex" ? "sessions" : "projects";
    const a = root(path.join(f.fixture.home, "a", folder), source, f.buffer);
    const b = root(path.join(f.fixture.home, "b", folder), source, f.buffer);
    const values = source === "codex" ? [0, 100, 150] : [100, 150];
    const first = write(a, values, source === "codex" ? "3" : "X");
    const second = write(b, values, source === "codex" ? (changed ? "4" : "3")
      : (changed ? "Y" : "X"), 200);
    const firstBytes = fs.readFileSync(first, "utf8");
    const secondBytes = fs.readFileSync(second, "utf8");
    // The shared-length prefix is identical or changes exactly one byte.
    const prefix = secondBytes.slice(0, firstBytes.length);
    assert.equal(firstBytes.length, prefix.length);
    assert.equal([...firstBytes].filter((byte, i) => byte !== prefix[i]).length, changed ? 1 : 0);
    seal(f.buffer, source, [first, second]);
    await applyCaptureHistory(f.buffer, a);
    let refusal = "none";
    let secondRows = 0;
    try { secondRows = (await applyCaptureHistory(f.buffer, b)).importedRows; }
    catch (error) { refusal = String(error); }
    const replayRows = refusal === "none" ? (await applyCaptureHistory(f.buffer, b)).importedRows : null;
    const suffix = f.buffer.database.prepare(`select model from buffered_events
      where session_id=? and input_tokens=50 order by observed_at desc limit 1`)
      .get(SESSION) as { model: string | null } | undefined;
    const result = { case: changed ? "one_byte_after_last_usage" : "identical_prefix_after_last_usage",
      source, refusal, secondRows, replayRows,
      total: inputTotal(f.buffer), rows: rowCount(f.buffer), changedBytes: changed ? 1 : 0,
      originalPrefixSha256: crypto.createHash("sha256").update(firstBytes).digest("hex"),
      copiedPrefixSha256: crypto.createHash("sha256").update(prefix).digest("hex"),
      suffixModel: suffix?.model ?? null };
    console.log(JSON.stringify(result));
    if (changed) {
      assert.match(refusal, /counter_regression/, "a changed byte in the copied, already fenced prefix must refuse");
      assert.equal(result.total, 150);
    } else {
      assert.equal(refusal, "none");
      assert.equal(result.total, 200);
      assert.equal(replayRows, 0);
    }
  } finally { f.close(); }
}
async function swappedParent(depth: number) {
  const f = setup();
  const originalOpenDir = fs.opendirSync;
  try {
    const r = root(path.join(f.fixture.home, "profile", "sessions"), "codex", f.buffer);
    const folders = Array.from({ length: depth }, (_, i) => `level-${i + 1}`);
    const nested = path.join(r.directory, ...folders);
    fs.mkdirSync(nested, { recursive: true });
    const file = write({ ...r, directory: nested }, [0, 10]);
    seal(f.buffer, "codex", [file]);
    const swap = path.join(r.directory, ...folders.slice(0, depth));
    const moved = path.join(f.scratch, `moved-depth-${depth}`);
    let opens = 0;
    let refusal = "none";
    try {
      fs.opendirSync = ((target: fs.PathLike, options?: unknown) => {
        if (String(target) === swap && ++opens === 2) {
          fs.renameSync(swap, moved);
          fs.symlinkSync(moved, swap, "dir");
        }
        return originalOpenDir(target, options as any);
      }) as typeof fs.opendirSync;
      try { await applyCaptureHistory(f.buffer, r); }
      catch (error) { refusal = String(error); }
    } finally { fs.opendirSync = originalOpenDir; }
    const result = { case: "swapped_parent", depth, opens, refusal,
      link: fs.lstatSync(swap).isSymbolicLink(), rows: rowCount(f.buffer) };
    console.log(JSON.stringify(result));
    assert.match(refusal, /root_parent_changed|root_symlink_entry/);
    assert.equal(result.rows, 0);
  } finally { fs.opendirSync = originalOpenDir; f.close(); }
}
async function linkedParent(depth: number) {
  const f = setup();
  try {
    const r = root(path.join(f.fixture.home, "profile", "sessions"), "codex", f.buffer);
    const folders = Array.from({ length: depth }, (_, i) => `level-${i + 1}`);
    const nested = path.join(r.directory, ...folders);
    fs.mkdirSync(nested, { recursive: true });
    const file = write({ ...r, directory: nested }, [0, 10]);
    seal(f.buffer, "codex", [file]);
    const moved = path.join(f.scratch, `linked-depth-${depth}`);
    fs.renameSync(nested, moved);
    fs.symlinkSync(moved, nested, "dir");
    let refusal = "none";
    try { await planCaptureHistory(f.buffer.database, r); }
    catch (error) { refusal = String(error); }
    const result = { case: "linked_parent", depth, refusal, rows: rowCount(f.buffer) };
    console.log(JSON.stringify(result));
    assert.match(refusal, /root_symlink_entry|root_parent_changed/);
    assert.equal(result.rows, 0);
  } finally { f.close(); }
}
async function linkedConfiguredRoot() {
  const f = setup();
  try {
    const r = root(path.join(f.fixture.home, "profile", "sessions"), "codex", f.buffer);
    const file = write(r, [0, 10]);
    seal(f.buffer, "codex", [file]);
    const moved = path.join(f.scratch, "moved-configured-root");
    fs.renameSync(r.directory, moved);
    fs.symlinkSync(moved, r.directory, "dir");
    let refusal = "none";
    try { await planCaptureHistory(f.buffer.database, r); }
    catch (error) { refusal = String(error); }
    const result = { case: "linked_configured_root", refusal, rows: rowCount(f.buffer) };
    console.log(JSON.stringify(result));
    assert.notEqual(refusal, "none");
    assert.equal(result.rows, 0);
  } finally { f.close(); }
}
async function digestCost() {
  const samples = [];
  for (const fillerRows of [10_000, 250_000]) {
    const f = setup();
    try {
      const r = root(path.join(f.fixture.home, "profile", "sessions"), "codex", f.buffer);
      const file = write(r, [0, 100]);
      const filler = `${JSON.stringify({ type: "message", payload: { content: "x".repeat(360) } })}\n`;
      const stream = fs.openSync(file, "a");
      try {
        for (let i = 0; i < fillerRows; i += 1000) fs.writeSync(stream, filler.repeat(1000));
        fs.writeSync(stream, `${codexLines([0, 100, 200]).at(-1)!}\n`);
      } finally { fs.closeSync(stream); }
      seal(f.buffer, "codex", [file]);
      const bytes = fs.statSync(file).size;
      const hashCpuStart = process.cpuUsage();
      const hashStarted = performance.now();
      const hash = crypto.createHash("sha256");
      const fd = fs.openSync(file, "r");
      try {
        const chunk = Buffer.alloc(1024 * 1024);
        for (let offset = 0; offset < bytes;) {
          const count = fs.readSync(fd, chunk, 0, Math.min(chunk.length, bytes - offset), offset);
          assert.ok(count > 0);
          hash.update(chunk.subarray(0, count));
          offset += count;
        }
      } finally { fs.closeSync(fd); }
      const sha256 = hash.digest("hex");
      const hashWallMs = performance.now() - hashStarted;
      const hashCpu = process.cpuUsage(hashCpuStart);
      const cpuStart = process.cpuUsage();
      const started = performance.now();
      const plan = await planCaptureHistory(f.buffer.database, r);
      const wallMs = performance.now() - started;
      const cpu = process.cpuUsage(cpuStart);
      samples.push({ fillerRows, bytes, sha256, hashWallMs,
        hashCpuMs: (hashCpu.user + hashCpu.system) / 1000,
        planWallMs: wallMs, planCpuMs: (cpu.user + cpu.system) / 1000,
        missingRows: plan.missingRows, tokens: plan.tokens.input });
    } finally { f.close(); }
  }
  console.log(JSON.stringify({ case: "digest_cost", samples }));
  assert.equal(samples.length, 2);
  assert.ok(samples.every(sample => sample.missingRows === 2 && sample.tokens === 200));
}

const cases: Record<string, () => Promise<void>> = {
  three_codex: () => threeFolders("codex"), three_claude: () => threeFolders("claude_code"),
  one_byte_codex: () => oneByteUsage("codex"), one_byte_claude: () => oneByteUsage("claude_code"),
  equal_counter_byte_codex: () => oneByteEqualCounter("codex"),
  equal_counter_byte_claude: () => oneByteEqualCounter("claude_code"),
  equal_counter_identical_codex: () => oneByteEqualCounter("codex", false),
  equal_counter_identical_claude: () => oneByteEqualCounter("claude_code", false),
  claude_changed_message_id: claudeChangedMessageId,
  trailing_byte_codex: () => oneByteAfterLastUsage("codex"),
  trailing_byte_claude: () => oneByteAfterLastUsage("claude_code"),
  trailing_identical_codex: () => oneByteAfterLastUsage("codex", false),
  trailing_identical_claude: () => oneByteAfterLastUsage("claude_code", false),
  parent_depth_1: () => swappedParent(1), parent_depth_3: () => swappedParent(3),
  linked_depth_1: () => linkedParent(1), linked_depth_3: () => linkedParent(3),
  linked_root: linkedConfiguredRoot, digest_cost: digestCost,
};
const selected = process.argv[2];
if (!selected || !cases[selected]) throw new Error(`choose ${Object.keys(cases).join(", ")}`);
cases[selected]().catch(error => { console.error(error); process.exitCode = 1; });
