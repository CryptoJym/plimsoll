/** Exact read-bound and later-readable rollout tails. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { discoverCaptureRootCandidates } from
  "../../packages/collector-cli/src/capture-root-inventory";

const root = fs.mkdtempSync(path.join(process.cwd(), "pr428-r6-rollout-bound-"));
const id = "12345678-1234-4234-8234-123456789abc";
const name = `rollout-2026-09-29T00-00-00-${id}.jsonl`;
const meta = (originator: string) => `${JSON.stringify({ type: "session_meta",
  timestamp: "2026-09-29T00:00:00Z", payload: { id, originator } })}\n`;
function fixture(label: string, body: string) {
  const home = path.join(root, label);
  const dir = path.join(home,
    ".clientai/studio/borg/conductors/unknown/profile/sessions/2026/09/29");
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const file = path.join(dir, name);
  fs.writeFileSync(file, body, { mode: 0o600 });
  return { home, file };
}
function enrolled(home: string) {
  return discoverCaptureRootCandidates(home).find((entry) =>
    entry.shape === "studio_codex_conductor")?.autoEnroll ?? false;
}
function padTo(size: number, prefix: string, suffix = "") {
  const emptyRow = `${JSON.stringify({ type: "fixture_padding", pad: "" })}\n`;
  const count = size - Buffer.byteLength(prefix) - Buffer.byteLength(suffix) -
    Buffer.byteLength(emptyRow);
  assert.ok(count >= 0);
  return prefix + `${JSON.stringify({ type: "fixture_padding", pad: "x".repeat(count) })}\n` + suffix;
}
try {
  const exact = fixture("exact-positive", padTo(16 * 1024, meta("codex_exec")));
  const exactConflict = fixture("exact-conflict", padTo(16 * 1024,
    meta("codex_exec"), meta("claude_code")));
  const exactPositiveBeforeAppend = enrolled(exact.home);
  fs.appendFileSync(exact.file, meta("claude_code"));
  const exactPositiveAfterAppend = enrolled(exact.home);

  const longPositive = meta("codex_exec") +
    `${JSON.stringify({ type: "fixture_padding", pad: "x".repeat(20_000) })}\n`;
  const foreignTail = fixture("foreign-tail", longPositive + meta("claude_code"));
  const foreignBefore = enrolled(foreignTail.home);
  fs.writeFileSync(foreignTail.file, meta("codex_exec") + meta("claude_code"));
  const foreignAfter = enrolled(foreignTail.home);
  const codexTail = fixture("codex-tail", longPositive + meta("codex_exec"));
  const codexBefore = enrolled(codexTail.home);
  fs.writeFileSync(codexTail.file, meta("codex_exec") + meta("codex_exec"));
  const codexAfter = enrolled(codexTail.home);
  const observed = { exactBytes: 16 * 1024, exactPositiveBeforeAppend,
    exactPositiveAfterAppend, exactConflict: enrolled(exactConflict.home),
    foreignBefore, foreignAfter, codexBefore, codexAfter };
  console.log(JSON.stringify(observed));
  assert.equal(fs.statSync(exactConflict.file).size, 16 * 1024);
  assert.deepEqual(observed, { exactBytes: 16 * 1024,
    exactPositiveBeforeAppend: true, exactPositiveAfterAppend: false,
    exactConflict: false, foreignBefore: false, foreignAfter: false,
    codexBefore: false, codexAfter: true });
} finally { fs.rmSync(root, { recursive: true, force: true }); }
