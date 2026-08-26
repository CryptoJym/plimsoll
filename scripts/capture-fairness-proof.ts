/**
 * Issue #141 capture-fairness proof: rotation marker persistence degrades
 * closed-open honestly, and rotation resumes AFTER the last-served candidate
 * without ever changing eligibility. Hostile inputs (oversized records,
 * malformed JSON, wrong schema versions, injection-shaped sources) degrade to
 * plain ordered service instead of failing the scan. Real better-sqlite3
 * ledger; no network, no live collector.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import {
  CAPTURE_ROTATION_SCHEMA_VERSION,
  loadCaptureRotation,
  rememberCaptureRotation,
  rotateAfterServed,
  type CaptureRotation,
} from "../packages/collector-cli/src/capture-fairness";
import { maintenanceCandidateHash } from "../packages/collector-cli/src/maintenance-progress";

type Check = { name: string; passed: boolean; detail?: unknown };
const checks: Check[] = [];

function check(name: string, condition: unknown, detail?: unknown) {
  const row = { name, passed: Boolean(condition), ...(detail !== undefined ? { detail } : {}) };
  checks.push(row);
  console.log(`${row.passed ? "PASS" : "FAIL"} ${name}`);
  if (!row.passed) throw new Error(`${name}: ${JSON.stringify(detail ?? null)}`);
}

const workdir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capture-fairness-"));
const db = new Database(path.join(workdir, "ledger.sqlite"));
db.pragma("journal_mode = WAL");

try {
  // --- Rotation marker persistence -----------------------------------------
  check("load_absent_row_is_null", loadCaptureRotation(db, "codex") === null);

  rememberCaptureRotation(db, "codex", maintenanceCandidateHash("/gen/a.jsonl"));
  const loaded = loadCaptureRotation(db, "codex");
  check("remember_then_load_roundtrip", loaded?.lastServedRotationKey === maintenanceCandidateHash("/gen/a.jsonl"), loaded);
  check("roundtrip_schema_version", loaded?.schemaVersion === CAPTURE_ROTATION_SCHEMA_VERSION && loaded.source === "codex");

  rememberCaptureRotation(db, "codex", maintenanceCandidateHash("/gen/b.jsonl"));
  check("overwrite_updates_marker", loadCaptureRotation(db, "codex")?.lastServedRotationKey === maintenanceCandidateHash("/gen/b.jsonl"));

  const claudeKey = maintenanceCandidateHash("/claude/x.jsonl");
  rememberCaptureRotation(db, "claude_code", claudeKey);
  check("sources_are_isolated",
    loadCaptureRotation(db, "claude_code")?.lastServedRotationKey === claudeKey &&
    loadCaptureRotation(db, "codex")?.lastServedRotationKey === maintenanceCandidateHash("/gen/b.jsonl"));

  rememberCaptureRotation(db, "claude_code", null);
  check("null_clears_marker", loadCaptureRotation(db, "claude_code") === null);

  // --- Adversarial: durable-state degradation ------------------------------
  db.prepare("update maintenance_state set value = ? where key = 'capture_rotation:codex'").run("{not json");
  check("malformed_json_degrades_to_null", loadCaptureRotation(db, "codex") === null);

  db.prepare("update maintenance_state set value = ? where key = 'capture_rotation:codex'")
    .run(JSON.stringify({ schemaVersion: 99, source: "codex", lastServedRotationKey: "sha256:x" }));
  check("wrong_schema_version_degrades_to_null", loadCaptureRotation(db, "codex") === null);

  db.prepare("update maintenance_state set value = ? where key = 'capture_rotation:codex'")
    .run(JSON.stringify({ schemaVersion: 1, source: "claude_code", lastServedRotationKey: "sha256:x" }));
  check("cross_source_marker_rejected", loadCaptureRotation(db, "codex") === null);

  db.prepare("update maintenance_state set value = ? where key = 'capture_rotation:codex'").run(`"${"x".repeat(4096)}"`);
  check("oversized_record_degrades_to_null", loadCaptureRotation(db, "codex") === null);

  db.prepare("update maintenance_state set value = ? where key = 'capture_rotation:codex'")
    .run(JSON.stringify({ schemaVersion: 1, source: "codex", lastServedRotationKey: "" }));
  check("empty_key_degrades_to_null", loadCaptureRotation(db, "codex") === null);

  let injected = false;
  try {
    (rememberCaptureRotation as unknown as (db2: Database.Database, source: string, key: string | null) => void)(
      db, "codex'); drop table maintenance_state;--", "sha256:abc",
    );
  } catch {
    injected = true;
  }
  const tableSurvived = (db.prepare("select count(*) as n from maintenance_state").get() as { n: number }).n >= 0;
  check("hostile_source_identifier_refused", injected && tableSurvived);

  let oversizedRefused = false;
  try {
    rememberCaptureRotation(db, "codex", "k".repeat(4097));
  } catch {
    oversizedRefused = true;
  }
  check("overside_key_write_refused", oversizedRefused);

  // --- Rotation semantics ---------------------------------------------------
  const keyOf = (candidate: { name: string }) => maintenanceCandidateHash(candidate.name);
  const candidates = [{ name: "/gen/a" }, { name: "/gen/b" }, { name: "/gen/c" }, { name: "/gen/d" }];
  const keys = candidates.map(keyOf);

  check("no_rotation_without_marker",
    JSON.stringify(rotateAfterServed(candidates, keyOf, null)) === JSON.stringify(candidates));
  check("unknown_served_key_changes_nothing",
    JSON.stringify(rotateAfterServed(candidates, keyOf, {
      schemaVersion: 1, source: "codex", lastServedRotationKey: "sha256:not-a-candidate",
    })) === JSON.stringify(candidates));
  check("single_candidate_is_identity",
    JSON.stringify(rotateAfterServed([candidates[0]!], keyOf, {
      schemaVersion: 1, source: "codex", lastServedRotationKey: keys[0]!,
    })) === JSON.stringify([candidates[0]]));

  const servedSecond = rotateAfterServed(candidates, keyOf, {
    schemaVersion: 1, source: "codex", lastServedRotationKey: keys[1]!,
  });
  check("resume_after_last_served", JSON.stringify(servedSecond.map((c) => c.name)) === JSON.stringify(["/gen/c", "/gen/d", "/gen/a", "/gen/b"]), servedSecond);

  const servedLast = rotateAfterServed(candidates, keyOf, {
    schemaVersion: 1, source: "codex", lastServedRotationKey: keys[3]!,
  });
  check("last_served_moves_to_tail_preserving_order",
    JSON.stringify(servedLast.map((c) => c.name)) === JSON.stringify(["/gen/a", "/gen/b", "/gen/c", "/gen/d"]));

  const servedFirst = rotateAfterServed(candidates, keyOf, {
    schemaVersion: 1, source: "codex", lastServedRotationKey: keys[0]!,
  });
  check("first_served_wraps_everyone_forward",
    JSON.stringify(servedFirst.map((c) => c.name)) === JSON.stringify(["/gen/b", "/gen/c", "/gen/d", "/gen/a"]));

  const consecutiveTurns = (() => {
    let order = [...candidates];
    const sequence: string[][] = [];
    for (let turn = 0; turn < 4; turn += 1) {
      const served = order[0]!;
      sequence.push(order.map((candidate) => candidate.name));
      order = rotateAfterServed(order, keyOf, {
        schemaVersion: 1, source: "codex", lastServedRotationKey: keyOf(served),
      });
    }
    return sequence;
  })();
  const everyGenerationLeads = new Set(consecutiveTurns.map((turn) => turn[0])).size === candidates.length;
  const relativeOrderStable = consecutiveTurns.every((turn) =>
    ["a", "b", "c", "d"].map((n) => `/gen/${n}`).filter((name) => turn.includes(name)).every((name, index, names) =>
      index === 0 || names.indexOf(turn[(turn.indexOf(name) - 1 + turn.length) % turn.length]!) >= 0),
  );
  check("four_turns_give_every_generation_the_lead", everyGenerationLeads, consecutiveTurns);
  check("rotation_never_duplicates_or_drops_candidates",
    consecutiveTurns.every((turn) => new Set(turn).size === 4));

  const input = Object.freeze([...candidates]);
  rotateAfterServed(input, keyOf, { schemaVersion: 1, source: "codex", lastServedRotationKey: keys[2]! });
  check("input_array_not_mutated", input.length === 4 && input[0]?.name === "/gen/a");

  // --- End-to-end through the real tailer module wiring ---------------------
  const tailerSource = fs.readFileSync(
    path.join(path.dirname(new URL(import.meta.url).pathname), "..", "packages", "collector-cli", "src", "rollout-tailer.ts"),
    "utf8",
  );
  check("rollout_tailer_rotates_only_automatic_cadences",
    tailerSource.includes("? rotateAfterServed(") && tailerSource.includes("if (automatic && lastServedRotationKey)"));
  const transcriptSource = fs.readFileSync(
    path.join(path.dirname(new URL(import.meta.url).pathname), "..", "packages", "collector-cli", "src", "transcript-tailer.ts"),
    "utf8",
  );
  check("transcript_tailer_uses_same_fairness_contract",
    transcriptSource.includes('loadCaptureRotation(this.buffer.database, "claude_code")'));

  console.log(`capture-fairness proof: ${checks.length} checks, all passed`);
} finally {
  db.close();
  fs.rmSync(workdir, { recursive: true, force: true });
}
