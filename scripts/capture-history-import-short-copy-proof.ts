/** Byte evidence for shorter copies and files containing only later records. */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { beginAutomaticCaptureBaseline, completeAutomaticCaptureBaseline,
  sealCaptureBaselineGenerations } from "../packages/collector-cli/src/capture-baseline";
import { applyCaptureHistory, planCaptureHistory } from "../packages/collector-cli/src/capture-history-import";
import { deriveCaptureRootIdentity, type CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { useFixtureRoot } from "./lib/fixture-root";
const SESSION = "019d0000-0000-7000-8000-000000000702";
const START = "2026-01-01T00:00:00.000Z", FENCE = "2026-01-03T00:00:00.000Z";
type Source = "codex" | "claude_code";
const cases = ["short_match", "short_changed", "short_after_usage", "seen_suffix", "new_suffix",
  "missing_fingerprint", "missing_record", "short_invalid_after_usage", "invalid_prefix", "new_invalid"] as const;
type Case = typeof cases[number];
const sha = (bytes: Buffer) => crypto.createHash("sha256").update(bytes).digest("hex");
const line = (row: unknown) => JSON.stringify(row) + "\n";
function usage(source: Source, value: number, second: number) {
  const timestamp = `2026-01-02T00:00:${String(second).padStart(2, "0")}.000Z`;
  return source === "codex" ? { type: "event_msg", timestamp,
    payload: { type: "token_count", info: { total_token_usage: { input_tokens: value, output_tokens: value } } } }
    : { type: "assistant", sessionId: SESSION, timestamp, message: { id: "message-a",
      model: "claude-sonnet-4-5", usage: { input_tokens: value, output_tokens: value } } };
}
async function prove(source: Source, which: Case) {
  const scratch = fs.realpathSync(fs.mkdtempSync(path.join(path.resolve(process.cwd(), ".."), "history-short-")));
  const fixture = useFixtureRoot(scratch, { home: path.join(scratch, "home"), plimsollHome: path.join(scratch, "ledger") });
  fs.mkdirSync(fixture.home, { recursive: true, mode: 0o700 });
  fs.mkdirSync(fixture.env.PLIMSOLL_HOME, { recursive: true, mode: 0o700 });
  const buffer = new LocalEventBuffer(path.join(fixture.env.PLIMSOLL_HOME, "work-ledger.sqlite"), {
    workspaceId: "3f2ba2c4-7d0e-4a5a-9b2c-1d6f5c8e7a10",
    deviceId: "dev_0d1c2b3a-4e5f-4a6b-8c9d-0e1f2a3b4c5d", enrollmentNow: () => new Date(START) });
  try {
    const roots: CaptureRoot[] = ["a", "b"].map(name => {
      const directory = path.join(fixture.home, name, source === "codex" ? "sessions" : "projects");
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      return { ...deriveCaptureRootIdentity("short-proof", source, directory), source, directory,
        installationEpochId: buffer.workspaceBinding()!.currentInstallationEpochId! };
    });
    const head = line(source === "codex" ? { type: "session_meta", payload: { id: SESSION, marker: "first-a" } }
      : { type: "message", sessionId: SESSION, marker: "first-a" });
    const values = source === "codex" ? [0, 100, 150] : [100, 150];
    const throughUsage = head + values.map((value, index) => line(usage(source, value, index + 1))).join("");
    const short = throughUsage + line({ type: "message", content: "after-a" });
    const original = short + line(usage(source, 200, 5)) + line({ type: "message", content: "last-a" });
    let copy = short;
    if (which === "short_changed") copy = short.replace("first-a", "first-b");
    if (which === "short_after_usage") copy = short.replace("after-a", "after-b");
    if (which === "seen_suffix" || which === "new_suffix") copy = line(usage(source, 250, 6));
    if (["missing_fingerprint", "invalid_prefix", "new_invalid"].includes(which)) copy = original;
    const copiedBytes = Buffer.from(copy);
    if (["short_invalid_after_usage", "invalid_prefix", "new_invalid"].includes(which)) {
      const needle = which === "short_invalid_after_usage" ? "after-a" : "first-a";
      const position = copiedBytes.indexOf(needle) + needle.length - 1;
      assert.ok(position >= needle.length - 1); copiedBytes[position] = 255;
    }
    const files = roots.map((root, index) => {
      const file = path.join(root.directory, source === "codex" ? `rollout-2026-01-02-${SESSION}.jsonl` : `${SESSION}.jsonl`);
      fs.writeFileSync(file, index === 0 ? original : copiedBytes, { mode: 0o600 }); return file;
    });
    const baseline = beginAutomaticCaptureBaseline(buffer.database, source, { startedAt: START, filesDiscovered: 0 });
    completeAutomaticCaptureBaseline(buffer.database, source, { runId: baseline.latestRun!.runId, completedAt: START });
    sealCaptureBaselineGenerations(buffer.database, source, files.map(file => {
      const stat = fs.statSync(file, { bigint: true });
      return { path: file, device: stat.dev, inode: stat.ino, size: stat.size, birthtimeNs: stat.birthtimeNs };
    }), FENCE);
    const total = () => (buffer.database.prepare(`select coalesce(sum(input_tokens),0) as n
      from buffered_events where session_id=?`).get(SESSION) as { n: number }).n;
    if (which !== "new_suffix" && which !== "new_invalid") {
      await applyCaptureHistory(buffer, roots[0]!); assert.equal(total(), 200);
      if (which === "missing_fingerprint") buffer.database.exec("drop table if exists capture_history_session_bytes");
      if (which === "missing_record" && buffer.database.prepare("select 1 from sqlite_master where name='capture_history_record_bytes'").get())
        buffer.database.prepare(`delete from capture_history_record_bytes where source=? and session_id=? and record_index=?`)
          .run(source, SESSION, values.length - 1);
    }
    const before = (buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
    const plan = await planCaptureHistory(buffer.database, roots[1]!);
    const refused = ["short_changed", "seen_suffix", "missing_fingerprint", "missing_record", "invalid_prefix", "new_invalid"].includes(which);
    const reasons = (plan as typeof plan & { refusals?: Array<{ reason: string }> }).refusals ?? [];
    if (refused) { assert.equal(plan.missingRows, 0); assert.equal(reasons.length, 1); }
    else assert.equal(reasons.length, 0);
    let reason = "none", added = 0;
    try { added = (await applyCaptureHistory(buffer, roots[1]!)).importedRows; }
    catch (error) { reason = String(error).replace(/^Error: capture_history_refused:/, ""); }
    if (refused) {
      assert.equal(reason, reasons[0]!.reason); assert.equal(added, 0); assert.equal(total(), which === "new_invalid" ? 0 : 200);
      assert.equal((buffer.database.prepare("select count(*) as n from buffered_events").get() as { n: number }).n, before);
    } else {
      assert.equal(reason, "none"); assert.equal(added, which === "new_suffix" ? 1 : 0);
      assert.equal(total(), which === "new_suffix" ? (source === "codex" ? 0 : 250) : 200);
      assert.equal((await applyCaptureHistory(buffer, roots[1]!)).importedRows, 0);
    }
    if (["short_match", "short_after_usage", "short_invalid_after_usage"].includes(which)) {
      const saved = buffer.database.prepare(`select byte_offset as offset,prefix_digest as digest
        from capture_history_record_bytes where source=? and session_id=? and record_index=?`)
        .get(source, SESSION, values.length - 1) as { offset: number; digest: string };
      assert.equal(saved.offset, Buffer.byteLength(throughUsage)); assert.equal(saved.digest, sha(Buffer.from(throughUsage)));
      const count = buffer.database.prepare(`select count(*) as n from capture_history_record_bytes
        where source=? and session_id=?`).get(source, SESSION) as { n: number };
      assert.equal(count.n, values.length + 1, "store every usage record, including the zero record");
    }
    console.log(JSON.stringify({ proof: which, source, storedBytes: Buffer.byteLength(original), copyBytes: copiedBytes.length,
      lastShortUsageOffset: Buffer.byteLength(throughUsage), originalSha256: sha(Buffer.from(original)), copySha256: sha(copiedBytes),
      plannedRows: plan.missingRows, reasons, reason, added, total: total() }));
  } finally { buffer.close(); fixture.restore(); fs.rmSync(scratch, { recursive: true, force: true }); }
}
const argument = process.argv[2] ?? "";
const source = argument.endsWith("_codex") ? "codex" : argument.endsWith("_claude") ? "claude_code" : undefined;
const which = argument.replace(/_(codex|claude)$/, "") as Case;
if (!source || !cases.includes(which)) throw new Error("choose a short-copy case and _codex or _claude");
void prove(source, which).catch(error => { console.error(error); process.exitCode = 1; });
