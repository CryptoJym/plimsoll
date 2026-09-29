/** A copied file must prove every imported byte, including non-usage records. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { applyCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { deriveCaptureRootIdentity, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { useFixtureRoot } from "./lib/fixture-root";

const SESSION = "019d0000-0000-7000-8000-000000000701";
const START = "2026-01-01T00:00:00.000Z";
const FENCE = "2026-01-03T00:00:00.000Z";
type Source = "codex" | "claude_code";
type Case = "first" | "middle" | "last" | "exact" | "nonusage";

function usage(source: Source, value: number, second: number) {
  const timestamp = `2026-01-02T00:00:${String(second).padStart(2, "0")}.000Z`;
  return source === "codex" ? { type: "event_msg", timestamp,
    payload: { type: "token_count", info: { total_token_usage: {
      input_tokens: value, output_tokens: value } } } }
    : { type: "assistant", sessionId: SESSION, timestamp, message: {
      id: "message-a", model: "claude-sonnet-4-5", usage: {
        input_tokens: value, output_tokens: value } } };
}
function prefix(source: Source) {
  return (source === "codex" ? [
    { type: "session_meta", payload: { id: SESSION, originator: "first-a" } },
    usage(source, 0, 1), usage(source, 100, 2),
    { type: "message", content: "middle-a" }, usage(source, 150, 3),
    { type: "turn_context", payload: { model: "gpt-5.3-codex", marker: "last-a" } },
  ] : [
    { type: "message", sessionId: SESSION, content: "first-a" },
    usage(source, 100, 2), { type: "message", content: "middle-a" },
    usage(source, 150, 3), { type: "message", content: "last-a" },
  ]).map(row => JSON.stringify(row)).join("\n") + "\n";
}

async function prove(source: Source, which: Case) {
  const scratch = fs.realpathSync(fs.mkdtempSync(path.join(path.resolve(process.cwd(), ".."), "history-bytes-")));
  const fixture = useFixtureRoot(scratch, { home: path.join(scratch, "home"),
    plimsollHome: path.join(scratch, "ledger") });
  fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
  fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });
  const buffer = new LocalEventBuffer(path.join(fixture.env.PLIMSOLL_HOME, "work-ledger.sqlite"), {
    workspaceId: "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10",
    deviceId: "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d", enrollmentNow: () => new Date(START),
  });
  try {
    const roots: CaptureRoot[] = ["a", "b"].map(name => {
      const directory = path.join(fixture.home, name, source === "codex" ? "sessions" : "projects");
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      return { ...deriveCaptureRootIdentity("byte-proof", source, directory), source, directory,
        installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! };
    });
    const original = prefix(source);
    const needle = which === "nonusage" ? "middle-a" : `${which}-a`;
    const copied = which === "exact" ? original : original.replace(needle, needle.slice(0, -1) + "b");
    const originalBytes = Buffer.from(original); const copiedBytes = Buffer.from(copied);
    assert.equal(copiedBytes.length, originalBytes.length);
    assert.equal(originalBytes.reduce((count, byte, index) => count + Number(byte !== copiedBytes[index]), 0),
      which === "exact" ? 0 : 1);
    const files = roots.map((root, index) => {
      const name = source === "codex" ? `rollout-2026-01-02-${SESSION}.jsonl` : `${SESSION}.jsonl`;
      const file = path.join(root.directory, name);
      const tail = index === 1 && which !== "exact" && which !== "nonusage"
        ? JSON.stringify(usage(source, 200, 4)) + "\n" : "";
      fs.writeFileSync(file, (index === 0 ? original : copied) + tail, { mode: 0o600 });
      return file;
    });
    const baseline = beginAutomaticCaptureBaseline(buffer.database, source, { startedAt: START, filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(buffer.database, source, { runId: baseline.latestRun!.runId, completedAt: START });
    sealCaptureBaselineGenerations(buffer.database, source, files.map(file => {
      const stat = fs.statSync(file, { bigint: true });
      return { path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs };
    }), FENCE);
    await applyCaptureHistory(buffer, roots[0]!);
    const total = () => (buffer.database.prepare(`select coalesce(sum(input_tokens),0) as n
      from buffered_events where session_id=?`).get(SESSION) as { n: number }).n;
    assert.equal(total(), 150);
    const before = (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
    let refusal = "none"; let added = 0;
    try { added = (await applyCaptureHistory(buffer, roots[1]!)).importedRows; }
    catch (error) { refusal = String(error); }
    const after = (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
    if (which === "exact") {
      assert.equal(refusal, "none"); assert.equal(added, 0);
      for (const root of roots) assert.equal((await applyCaptureHistory(buffer, root)).importedRows, 0);
    } else {
      assert.match(refusal, /capture_history_refused:/, `changed ${which} line must refuse`);
    }
    assert.equal(total(), 150); assert.equal(after, before);
    console.log(JSON.stringify({ proof: "copied_byte_prefix", source, which, storedBytes: originalBytes.length,
      changedBytes: which === "exact" ? 0 : 1, refusal, added, total: total(), before, after }));
  } finally { buffer.close(); fixture.restore(); fs.rmSync(scratch, { recursive: true, force: true }); }
}
const [which, kind] = (process.argv[2] ?? "").split("_");
if (!["first", "middle", "last", "exact", "nonusage"].includes(which ?? "") ||
    !["codex", "claude"].includes(kind ?? "")) throw new Error("choose first|middle|last|exact|nonusage_codex|claude");
void prove(kind === "codex" ? "codex" : "claude_code", which as Case)
  .catch(error => { console.error(error); process.exitCode = 1; });
