/** Regression: an older collector must still discover a pending pause retry. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { listHookSpoolFiles, writeHookSpoolEnvelope } from "../packages/collector-cli/src/hook-spool";

async function main() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "pr424-downgrade-")));
  try {
    // CI checks out full ancestry. Import the actual 0.7.44 source from it,
    // without relying on a sibling worktree or copying a reader implementation.
    const archived = spawnSync("git", ["archive", "375f277", "packages/collector-cli/src", "packages/shared/src"],
      { maxBuffer: 16 * 1024 * 1024 });
    assert.equal(archived.status, 0, archived.stderr?.toString());
    const extracted = spawnSync("tar", ["-xf", "-", "-C", root], { input: archived.stdout });
    assert.equal(extracted.status, 0, extracted.stderr?.toString());
    fs.symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"), "dir");
    const olderModule = path.join(root, "packages/collector-cli/src/hook-spool.ts");
    const older = await import(pathToFileURL(olderModule).href) as typeof import("../packages/collector-cli/src/hook-spool");
    const written = writeHookSpoolEnvelope({ home: root, source: "claude_code",
      body: JSON.stringify({ hook_event_name: "UserPromptSubmit", session_id: "fixture" }),
      cause: "maintenance_rebuild" });
    assert.equal(written.ok, true);
    const headVisible = listHookSpoolFiles(root).length;
    const olderVisible = older.listHookSpoolFiles(root).length;
    const olderReadable = olderVisible === 1 && older.readHookSpoolFile(written.path).ok;
    console.log(JSON.stringify({ check: "downgrade_discovers_pending_retry", headVisible,
      olderVisible, olderReadable }));
    assert.equal(headVisible, 1);
    assert.equal(olderVisible, 1, "the pre-PR drain must discover the pending retry after downgrade");
    assert.equal(olderReadable, true, "the pre-PR reader must decode the pending retry");
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
