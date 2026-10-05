import { fixtureEpochId } from "./lib/fixture-epoch-id";
/** PR #429 review fixtures and a deterministic bound on the dispatch hot path. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import {
  currentDispatchBindingSnapshot,
  dispatchBindingForSession,
  dispatchBindingSchema,
  rootEventMetadata,
} from "../packages/collector-cli/src/capture-root-inventory";
import { normalizeForwardedHook } from "../packages/collector-cli/src/forwarder";
import { createProofCompletion, requireIsolatedProofEnvironment } from "./lib/proof-completion";

const root = requireIsolatedProofEnvironment();
const proof = createProofCompletion("claude-dispatch-review", 5);
const repo = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");

function privateHome(name: string) {
  const fixture = path.join(root, name);
  const home = path.join(fixture, "home");
  const plimsoll = path.join(home, ".plimsoll");
  for (const directory of [home, plimsoll, path.join(fixture, "tmp")]) {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  }
  return { fixture, home, plimsoll };
}

for (const script of [
  "pr429-cross-root-review.ts",
  "pr429-allowlist-review.ts",
  "pr429-privacy-review.ts",
  "pr429-restamp-state-review.ts",
]) {
  const { fixture, home, plimsoll } = privateHome(path.basename(script, ".ts"));
  const child = spawnSync(process.execPath,
    ["--import", path.join(repo, "node_modules/tsx/dist/loader.mjs"), path.join(repo, "scripts", script)], {
      cwd: repo,
      encoding: "utf8",
      timeout: 120_000,
      env: {
        ...process.env,
        HOME: home,
        USERPROFILE: home,
        PLIMSOLL_HOME: plimsoll,
        CODEX_HOME: path.join(home, ".codex"),
        CLAUDE_CONFIG_DIR: path.join(home, ".claude"),
        XDG_CONFIG_HOME: path.join(home, ".config"),
        XDG_CACHE_HOME: path.join(home, ".cache"),
        XDG_STATE_HOME: path.join(home, ".local/state"),
        TMPDIR: path.join(fixture, "tmp"),
      },
    });
  console.log(JSON.stringify({ script, exitCode: child.status, stdout: child.stdout.trim(),
    stderr: child.stderr.trim(), error: child.error?.message ?? null }));
  assert.equal(child.status, 0, `${script} failed`);
  proof.check(script);
}

// The paired benchmark is the machine-specific numerical gate. CI checks the
// structural bound instead: one config read and only the target session's 23
// intervals are consulted, independently of wall-clock scheduling noise.
const { home, plimsoll } = privateHome("indexed-hot-path");
process.env.HOME = home;
process.env.USERPROFILE = home;
process.env.PLIMSOLL_HOME = plimsoll;
const now = Date.now();
const observedAt = new Date(now - 60_000).toISOString();
const binding = dispatchBindingSchema.parse({
  sessionId: "other-0000", workItemId: "beads:eco-6hoxj.165.97",
  projectKey: `sha256:${"a".repeat(64)}`, companyRef: null,
  attemptId: "11111111-1111-4111-8111-111111111111",
  parentAttemptId: null, acceptedOutcomeId: null,
  validFrom: new Date(now - 120_000).toISOString(), validUntil: null,
  evidenceRef: "dispatch:synthetic-hot-path",
});
const roots = Array.from({ length: 23 }, (_, index) => ({
  rootId: `root-${index}`, profileId: `profile-${index}`, installationEpochId: fixtureEpochId(`epoch-${index}`),
  source: "claude_code" as const,
  directory: path.join(home, ".claude-seats", `seat-${index}`, "projects"),
  dispatch: Array.from({ length: 1000 }, (_, offset) => ({ ...binding,
    sessionId: offset === 999 ? "target-session" : `other-${String(offset).padStart(4, "0")}` })),
}));
const config = collectorConfigSchema.parse({ deviceId: "dev_pr429-index",
  uploadUrl: "http://127.0.0.1:1/unused", captureRoots: roots });
const configFile = path.join(plimsoll, "collector.config.json");
fs.writeFileSync(configFile, `${JSON.stringify(config)}\n`, { mode: 0o600 });
// The private reader uses bounded descriptor reads, so count image opens
// rather than the former pathname-based readFileSync implementation.
let configReads = 0;
const originalOpen = fs.openSync;
fs.openSync = ((file: fs.PathLike, ...args: unknown[]) => {
  if (file === configFile) configReads += 1;
  return (originalOpen as (...openArgs: unknown[]) => unknown)(file, ...args);
}) as typeof fs.openSync;
try {
  const snapshot = currentDispatchBindingSnapshot();
  assert.equal(configReads, 1);
  assert.equal(snapshot.bySession.get("claude_code\0target-session")?.length, 23);
  assert.equal(dispatchBindingForSession("claude_code", "target-session", observedAt,
    snapshot.roots)?.workItemId, binding.workItemId);
  const hook = { id: "synthetic-hook", hook_event_name: "AssistantResponse",
    session_id: "target-session", timestamp: observedAt };
  for (let index = 0; index < 100; index += 1) {
    assert.strictEqual(currentDispatchBindingSnapshot(), snapshot);
    dispatchBindingForSession("claude_code", "target-session", observedAt, snapshot.roots);
    rootEventMetadata(snapshot.roots[0], `synthetic-${index}`, observedAt, "target-session",
      true, snapshot);
    normalizeForwardedHook(hook, { config, source: "claude_code", now: () => now,
      dispatchSnapshot: snapshot });
  }
  assert.equal(configReads, 1, "config read occurred on a repeated event");
  const changed = structuredClone(config);
  changed.captureRoots![0].dispatch![999].workItemId = "beads:eco-6hoxj.165.98";
  const replacement = `${configFile}.next`;
  fs.writeFileSync(replacement, `${JSON.stringify(changed)}\n`, { mode: 0o600 });
  fs.renameSync(replacement, configFile);
  const next = currentDispatchBindingSnapshot();
  assert.notStrictEqual(next, snapshot, "next batch did not observe config replacement");
  assert.equal(configReads, 2);
  assert.equal(dispatchBindingForSession("claude_code", "target-session", observedAt,
    next.roots), null, "next batch did not observe conflicting bindings");
  console.log(JSON.stringify({ roots: 23, bindingsPerRoot: 1000,
    targetCandidates: snapshot.bySession.get("claude_code\0target-session")?.length,
    repeatedEvents: 100, configReads, configUpdateVisibleNextBatch: true }));
  proof.check("indexed_hot_path_is_bounded_and_refreshes_on_next_batch");
} finally {
  fs.openSync = originalOpen;
}
proof.complete();
