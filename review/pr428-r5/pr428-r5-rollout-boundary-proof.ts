/** A bounded read must not certify a rollout with unexamined conflicting metadata. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { discoverCaptureRootCandidates } from "../../packages/collector-cli/src/capture-root-inventory";

const root = fs.mkdtempSync(path.join(process.cwd(), "pr428-r5-rollout-boundary-"));
const id = "12345678-1234-4234-8234-123456789abc";
const name = `rollout-2026-09-29T00-00-00-${id}.jsonl`;
const meta = (originator: string) => JSON.stringify({
  type: "session_meta", timestamp: "2026-09-29T00:00:00Z",
  payload: { id, originator },
});
function fixture(label: string, body: string) {
  const home = path.join(root, label);
  const dir = path.join(home, ".clientai/studio/borg/conductors/unrelated/profile/sessions/2026/09/29");
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const file = path.join(dir, name);
  fs.writeFileSync(file, body, { mode: 0o600 });
  return { home, file };
}
function enrolled(home: string) {
  return discoverCaptureRootCandidates(home).find((entry) =>
    entry.shape === "studio_codex_conductor")?.autoEnroll ?? false;
}
try {
  const filler = JSON.stringify({ type: "fixture_other", padding: "x".repeat(20_000) });
  const beyond = fixture("foreign-after-read", `${meta("codex_exec")}\n${filler}\n${meta("claude_code")}\n`);
  const nested = fixture("nested-originator", `${JSON.stringify({ type: "session_meta",
    timestamp: "2026-09-29T00:00:00Z", payload: { id, nested: { originator: "codex_exec" } } })}\n`);
  const truncated = fixture("truncated", `${meta("codex_exec")}\n{"type":"session_meta",`);
  const growing = fixture("later-codex", `${JSON.stringify({ type: "fixture_other" })}\n`);
  const beforeAppend = enrolled(growing.home);
  fs.appendFileSync(growing.file, `${meta("codex_exec")}\n`);
  const afterAppend = enrolled(growing.home);
  const foreignGrowing = fixture("foreign-then-codex", `${meta("claude_code")}\n`);
  fs.appendFileSync(foreignGrowing.file, `${meta("codex_exec")}\n`);
  const observed = { foreignAfterBoundedRead: enrolled(beyond.home),
    nestedOriginator: enrolled(nested.home), truncatedMetadata: enrolled(truncated.home),
    laterCodexBefore: beforeAppend, laterCodexAfter: afterAppend,
    foreignThenCodex: enrolled(foreignGrowing.home),
    fileBytes: fs.statSync(beyond.file).size };
  console.log(JSON.stringify(observed));
  assert.equal(observed.nestedOriginator, false);
  assert.equal(observed.truncatedMetadata, false);
  assert.equal(observed.laterCodexBefore, false);
  assert.equal(observed.laterCodexAfter, true);
  assert.equal(observed.foreignThenCodex, false);
  assert.equal(observed.foreignAfterBoundedRead, false,
    "an unexamined later foreign session_meta was automatically enrolled");
} finally { fs.rmSync(root, { recursive: true, force: true }); }
