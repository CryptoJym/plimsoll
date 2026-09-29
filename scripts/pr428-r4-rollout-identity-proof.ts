/** Directed Codex discovery identity variants for PR #428. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { discoverCaptureRootCandidates } from "../packages/collector-cli/src/capture-root-inventory";

const repo = path.resolve(import.meta.dirname, "..");
const root = fs.mkdtempSync(path.join(repo, "pr428-r4-identity-"));
const id = "12345678-1234-4234-8234-123456789abc";
const other = "22345678-1234-4234-8234-123456789abc";
const meta = (identity: string, originator?: string) => JSON.stringify({
  type: "session_meta", timestamp: "2026-09-28T00:00:00Z",
  payload: { id: identity, ...(originator ? { originator } : {}) },
});
function candidate(name: string, filename: string, content: string) {
  const home = path.join(root, name);
  const sessions = path.join(home,
    ".clientai/studio/borg/conductors/not-an-agent/profile/sessions/2026/09/28");
  fs.mkdirSync(sessions, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(sessions, filename), content, { mode: 0o600 });
  return discoverCaptureRootCandidates(home).find((entry) =>
    entry.shape === "studio_codex_conductor");
}
try {
  const name = `rollout-2026-09-28T00-00-00-${id}.jsonl`;
  const truncated = candidate("truncated", name, '{"type":"session_meta",');
  const mixed = candidate("mixed-case", `rollout-2026-09-28T00-00-00-${id.toUpperCase()}.jsonl`,
    `${meta(id, "codex_exec")}\n`);
  const duplicated = candidate("duplicated", name,
    `${meta(id, "codex_exec")}\n${meta(other, "codex_exec")}\n`);
  const foreign = candidate("foreign-tool", name, `${meta(id, "claude_code")}\n`);
  const missing = candidate("missing-originator", name, `${meta(id)}\n`);
  const laterForeign = candidate("later-foreign", name,
    `${meta(id, "codex_exec")}\n${meta(id, "claude_code")}\n`);
  console.log(JSON.stringify({ truncated: truncated?.autoEnroll, mixedCase: mixed?.autoEnroll,
    duplicatedConflictingMetadata: duplicated?.autoEnroll,
    explicitForeignOriginator: foreign?.autoEnroll, missingOriginator: missing?.autoEnroll,
    laterForeignOriginator: laterForeign?.autoEnroll }));
  assert.equal(truncated?.autoEnroll, false);
  assert.equal(mixed?.autoEnroll, true);
  if (process.env.PR428_REVIEW_VARIANT !== "foreign")
    assert.equal(duplicated?.autoEnroll, false,
      "a rollout with conflicting duplicate session metadata was automatically enrolled");
  if (process.env.PR428_REVIEW_VARIANT !== "duplicate")
    assert.equal(foreign?.autoEnroll, false,
      "a rollout explicitly marked as another tool was automatically enrolled");
  assert.equal(missing?.autoEnroll, false,
    "a rollout without positive Codex originator was automatically enrolled");
  assert.equal(laterForeign?.autoEnroll, false,
    "a later foreign session metadata line was ignored");
} finally { fs.rmSync(root, { recursive: true, force: true }); }
