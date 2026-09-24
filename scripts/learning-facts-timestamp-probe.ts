#!/usr/bin/env node

// Seeded compatibility/property probe. Run `seed-old` from a real 0.7.38
// checkout, then `upgrade-new` from the current checkout on the same ledger.
// `direct-new` exercises current admission; downgrade/reupgrade exercise the
// same admitted spellings after the new stored-key schema is present.
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";

const [mode, ledgerPath] = process.argv.slice(2);
assert.ok(["seed-old", "upgrade-new", "direct-new", "downgrade-old", "reupgrade-new"].includes(mode));
assert.ok(ledgerPath, "ledger path required");

type FilePathError = NodeJS.ErrnoException;

function sameOrInside(root: string, candidate: string) {
  const relative = path.relative(root, candidate);
  return relative !== "" && relative !== ".." &&
    !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}

function sameOrAncestor(candidate: string, root: string) {
  return candidate === root || sameOrInside(candidate, root);
}

const configuredTempDir = process.env.TMPDIR;
const requestedTempDir = path.resolve(configuredTempDir || os.tmpdir());
let tempRootStat: fs.Stats;
try {
  tempRootStat = fs.lstatSync(requestedTempDir);
} catch (error) {
  const detail = error as FilePathError;
  assert.fail(`isolated temp folder is not accessible: ${requestedTempDir} (${detail.code ?? "unknown"})`);
}
assert.ok(tempRootStat!.isDirectory(), `isolated temp folder must be a directory: ${requestedTempDir}`);
if (tempRootStat!.isSymbolicLink()) {
  const systemTempAlias = path.join(path.parse(requestedTempDir).root, "tmp");
  assert.equal(
    requestedTempDir,
    systemTempAlias,
    `isolated temp folder must not be a symlink: ${requestedTempDir}`,
  );
}
const isolatedTempDir = fs.realpathSync.native(requestedTempDir);
const filesystemRoot = path.parse(isolatedTempDir).root;
assert.notEqual(isolatedTempDir, filesystemRoot, "isolated temp folder must not be the filesystem root");
const homeDir = fs.realpathSync.native(os.homedir());
assert.notEqual(isolatedTempDir, homeDir, "isolated temp folder must not be the home directory");
const systemTempAlias = path.join(path.parse(requestedTempDir).root, "tmp");
let systemTempRoot: string | undefined;
try { systemTempRoot = fs.realpathSync.native(systemTempAlias); } catch { /* checked below */ }
const isSystemTempRoot = systemTempRoot !== undefined && isolatedTempDir === systemTempRoot &&
  (requestedTempDir === systemTempAlias || requestedTempDir === systemTempRoot);
const tempPathComponents = isolatedTempDir.split(path.sep).filter(Boolean);
assert.ok(
  !configuredTempDir || requestedTempDir === systemTempAlias ||
    tempPathComponents.some((component) => /^(?:tmp|temp|t)$/i.test(component)),
  `isolated temp folder must be a temp path: ${isolatedTempDir}`,
);
const realTempRootStat = fs.statSync(isolatedTempDir);
assert.ok(realTempRootStat.isDirectory(), `isolated temp folder must be a directory: ${isolatedTempDir}`);
const rootMode = realTempRootStat.mode & 0o7777;
assert.ok(
  (rootMode & 0o002) === 0 || isSystemTempRoot,
  `isolated temp folder must not be a shared world-writable directory: ${isolatedTempDir}`,
);
const currentUid = typeof process.getuid === "function" ? process.getuid() : undefined;
assert.ok(
  currentUid === undefined || realTempRootStat.uid === currentUid || isSystemTempRoot,
  `isolated temp folder must be owned by the current user: ${isolatedTempDir}`,
);

function nearestExisting(candidate: string) {
  let current = path.resolve(candidate);
  while (true) {
    try {
      const stat = fs.lstatSync(current);
      return { path: current, stat };
    } catch (error) {
      const detail = error as FilePathError;
      if (detail.code !== "ENOENT" && detail.code !== "ENOTDIR") throw error;
      const parent = path.dirname(current);
      assert.notEqual(parent, current, `path has no existing parent: ${candidate}`);
      current = parent;
    }
  }
}

function inspectExistingComponents(candidate: string) {
  const absolute = path.resolve(candidate);
  const existing = nearestExisting(absolute);
  let current = path.parse(absolute).root;
  const components = absolute.slice(current.length).split(path.sep).filter(Boolean);
  for (const component of components) {
    current = path.join(current, component);
    let stat: fs.Stats;
    try {
      stat = fs.lstatSync(current);
    } catch (error) {
      const detail = error as FilePathError;
      if (detail.code === "ENOENT" || detail.code === "ENOTDIR") break;
      throw error;
    }
    if (!stat.isSymbolicLink()) continue;
    const realComponent = fs.realpathSync.native(current);
    const realParent = fs.realpathSync.native(path.dirname(current));
    assert.ok(
      sameOrInside(isolatedTempDir, realComponent) || sameOrAncestor(realComponent, isolatedTempDir),
      `symlink component escapes isolated temp folder: ${current} -> ${realComponent}`,
    );
    assert.ok(
      !sameOrInside(isolatedTempDir, realParent),
      `symlink component inside isolated temp folder is refused: ${current}`,
    );
  }
  assert.ok(existing.stat.isDirectory() || existing.path === absolute,
    `ledger parent is not a directory: ${existing.path}`);
  return { absolute, existing };
}

function assertSafeLedgerPath(candidate: string) {
  const { absolute, existing } = inspectExistingComponents(candidate);
  const parent = path.dirname(absolute);
  const parentInfo = nearestExisting(parent);
  assert.ok(parentInfo.stat.isDirectory(), `ledger parent is not a directory: ${parentInfo.path}`);
  const parentReal = fs.realpathSync.native(parentInfo.path);
  const relativeMissing = path.relative(parentInfo.path, absolute);
  const resolvedPath = path.resolve(parentReal, relativeMissing);
  assert.ok(
    sameOrInside(isolatedTempDir, resolvedPath),
    `ledger path must be inside isolated temp folder ${isolatedTempDir}: ${candidate}`,
  );
  if (existing.path === absolute) {
    assert.ok(!existing.stat.isSymbolicLink(), `ledger path must not be a symlink: ${candidate}`);
    const realPath = fs.realpathSync.native(absolute);
    assert.ok(
      sameOrInside(isolatedTempDir, realPath),
      `ledger path resolves outside isolated temp folder ${isolatedTempDir}: ${candidate}`,
    );
  }
}

function openDatabase(candidate: string) {
  assertSafeLedgerPath(candidate);
  let descriptor: number | undefined;
  try {
    try {
      descriptor = fs.openSync(candidate, fs.constants.O_RDWR | fs.constants.O_NOFOLLOW);
    } catch (error) {
      const detail = error as FilePathError;
      if (detail.code !== "ENOENT") throw error;
      descriptor = fs.openSync(
        candidate,
        fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW,
        0o600,
      );
    }
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
  }
  assertSafeLedgerPath(candidate);
  return new Database(candidate);
}

const probeLedgerPaths = [ledgerPath, `${ledgerPath}.tie.sqlite`, `${ledgerPath}.top.sqlite`];
for (const probePath of probeLedgerPaths) assertSafeLedgerPath(probePath);
const requireFromRepo = createRequire(path.join(process.cwd(), "package.json"));
const Database = requireFromRepo("better-sqlite3");
const facts = requireFromRepo(path.join(process.cwd(), "packages/collector-cli/src/learning-facts.ts"));
const schemas = requireFromRepo(path.join(process.cwd(), "packages/shared/src/index.ts"));

let seed = 0x5eed4070;
function random(max: number) {
  seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
  return seed % max;
}
const pad = (value: number, width = 2) => String(value).padStart(width, "0");
function formatted(index: number): { label: string; timestamp: string } {
  // The local wall-clock parts are deliberately displaced across day/month
  // boundaries before applying a zone. These cases are not all SQLite dates.
  const epoch = Date.UTC(2026, index % 2 === 0 ? 7 : 8,
    index % 2 === 0 ? 31 : 1, random(24), random(60), random(60), random(1000));
  const offsetMinutes = [-720, -360, -300, 0, 120, 330, 840][random(7)];
  const local = new Date(epoch + offsetMinutes * 60_000);
  const date = `${pad(local.getUTCFullYear(), 4)}-${pad(local.getUTCMonth() + 1)}-${pad(local.getUTCDate())}`;
  const zone = offsetMinutes === 0 && index % 4 === 0 ? "Z" :
    `${offsetMinutes < 0 ? "-" : "+"}${pad(Math.floor(Math.abs(offsetMinutes) / 60))}:${pad(Math.abs(offsetMinutes) % 60)}`;
  const separator = ["T", "t", " "][index % 3];
  const seconds = index % 7 === 0 ? "" : `:${pad(local.getUTCSeconds())}`;
  const fraction = seconds && index % 5 !== 0
    ? `.${pad(local.getUTCMilliseconds(), 3)}${"0".repeat(index % 7)}` : "";
  return { label: `generated-${index}`, timestamp:
    `${date}${separator}${pad(local.getUTCHours())}:${pad(local.getUTCMinutes())}${seconds}${fraction}${zone}` };
}

const candidates = [
  { label: "lowercase-t", timestamp: "2026-09-01t00:00:00Z" },
  { label: "uppercase-t", timestamp: "2026-09-01T00:00:00Z" },
  { label: "space-separator", timestamp: "2026-09-01 00:00:00Z" },
  { label: "no-seconds", timestamp: "2026-09-01T00:00Z" },
  { label: "fraction-one", timestamp: "2026-09-01T00:00:00.5Z" },
  { label: "fraction-three", timestamp: "2026-09-01T00:00:00.500Z" },
  { label: "fraction-nine", timestamp: "2026-09-01T00:00:00.500123456Z" },
  { label: "offset-colon", timestamp: "2026-08-31T18:00:00-06:00" },
  { label: "offset-no-colon", timestamp: "2026-08-31T18:00:00-0600" },
  { label: "lowercase-z", timestamp: "2026-09-01T00:00:00z" },
  { label: "slash-date", timestamp: "09/01/2026 00:00:00Z" },
  { label: "trimmed", timestamp: " 2026-09-01T00:00:00Z " },
  ...Array.from({ length: 192 }, (_, index) => formatted(index)),
];

type Sample = { label: string; timestamp: string; milliseconds: number;
  operationId: string; episodeId: string; attempt: Record<string, unknown>;
  episode: Record<string, unknown> };
const samples: Sample[] = [];
const rejected: string[] = [];
for (const [index, candidate] of candidates.entries()) {
  const sessionId = `timestamp-property-${index}`;
  const attempt = {
    kind: "attempt", source: "codex", sessionId,
    operationId: facts.deterministicToolOperationId({
      source: "codex", sessionId, sourceOperationKey: `timestamp-${index}`,
    }),
    toolClass: "compute", toolName: "shell", startedAt: candidate.timestamp,
  };
  const parsed = schemas.toolAttemptStartSignalSchema.safeParse(attempt);
  if (!parsed.success) { rejected.push(candidate.label); continue; }
  const episode = facts.buildWorkEpisodeFact({ source: "codex", sessionId,
    sourceEpisodeKey: `timestamp-episode-${index}`, workClass: "review",
    complexityBand: "medium", startedAt: candidate.timestamp });
  samples.push({ label: candidate.label, timestamp: parsed.data.startedAt,
    milliseconds: Date.parse(parsed.data.startedAt), operationId: parsed.data.operationId,
    episodeId: episode.episodeId, attempt: parsed.data, episode });
}
assert.ok(samples.length >= 100, `too few admitted forms: ${samples.length}`);
assert.ok(samples.some((s) => s.label === "lowercase-t"));
assert.ok(samples.some((s) => s.label === "space-separator"));
assert.ok(samples.some((s) => s.label === "no-seconds"));
assert.ok(samples.some((s) => s.label === "slash-date"));
assert.ok(rejected.includes("offset-no-colon") && rejected.includes("lowercase-z"));
const tieSamples = samples.filter((sample) => [
  "lowercase-t", "uppercase-t", "space-separator", "no-seconds", "offset-colon",
].includes(sample.label));
assert.equal(tieSamples.length, 5);
assert.equal(new Set(tieSamples.map((sample) => sample.milliseconds)).size, 1);

const downgradeTimestamps = [
  "2026-09-02t00:00:00Z", "2026-09-02 00:00:01Z",
  "2026-09-02T00:02Z", "2026-09-02T00:03:00.5Z",
  "2026-09-01t18:04:00-06:00",
];

function seedLedger(db: any) {
  const store = new facts.LearningFactStore(db, { attempts: 512, episodes: 512 });
  for (const sample of samples) {
    assert.equal(store.recordToolSignal(sample.attempt).inserted, true, sample.label);
    assert.equal(store.recordWorkEpisode(sample.episode).inserted, true, sample.label);
  }
  assert.equal(db.prepare("select count(*) as n from tool_attempt_facts").get().n, samples.length);
  assert.equal(db.prepare("select count(*) as n from work_episode_facts").get().n, samples.length);
}
function compareFull(db: any) {
  for (const [table, idColumn, timestampColumn, sampleId] of [
    ["tool_attempt_facts", "operation_id", "started_at", "operationId"],
    ["work_episode_facts", "episode_id", "started_at", "episodeId"],
  ]) {
    const rows = db.prepare(`select ${idColumn} as id, ${timestampColumn} as timestamp,
      retention_ms as milliseconds from ${table}`).all() as Array<{
        id: string; timestamp: string; milliseconds: number }>;
    assert.equal(rows.length, samples.length);
    for (const row of rows) {
      const sample = samples.find((s) => s[sampleId as "operationId" | "episodeId"] === row.id);
      assert.ok(sample);
      assert.equal(row.timestamp, sample.timestamp, sample.label);
      assert.equal(row.milliseconds, sample.milliseconds, sample.label);
    }
  }
}
function topThree(idField: "operationId" | "episodeId") {
  return [...samples].sort((a, b) => b.milliseconds - a.milliseconds ||
    (a[idField] < b[idField] ? 1 : a[idField] > b[idField] ? -1 : 0))
    .slice(0, 3).map((s) => s[idField]);
}
function compareTop(db: any) {
  for (const [table, idColumn, field] of [
    ["tool_attempt_facts", "operation_id", "operationId"],
    ["work_episode_facts", "episode_id", "episodeId"],
  ] as const) {
    const ids = (db.prepare(`select ${idColumn} as id from ${table}
      order by retention_ms desc, ${idColumn} desc`).all() as Array<{ id: string }>).map((r) => r.id);
    assert.deepEqual(ids, topThree(field), table);
  }
}
function seedTie(db: any, limit: number) {
  const store = new facts.LearningFactStore(db, { attempts: limit, episodes: limit });
  for (const sample of tieSamples) {
    store.recordToolSignal(sample.attempt);
    store.recordWorkEpisode(sample.episode);
  }
}
function compareTie(db: any) {
  for (const [table, idColumn, field] of [
    ["tool_attempt_facts", "operation_id", "operationId"],
    ["work_episode_facts", "episode_id", "episodeId"],
  ] as const) {
    const actual = (db.prepare(`select ${idColumn} as id from ${table}
      order by retention_ms desc, ${idColumn} desc`).all() as Array<{id:string}>).map((row) => row.id);
    const expected = tieSamples.map((sample) => sample[field]).sort().reverse().slice(0, 2);
    assert.deepEqual(actual, expected, `${table} must break equal-instant ties by identity`);
  }
}

const db = openDatabase(ledgerPath);
try {
  if (mode === "seed-old") {
    seedLedger(db);
    assert.equal((db.pragma("table_xinfo(tool_attempt_facts)") as Array<{name:string}>).some(
      (column) => column.name === "retention_ms"), false, "seed must use real 0.7.38 schema");
    const tieDb = openDatabase(`${ledgerPath}.tie.sqlite`);
    try { seedTie(tieDb, 10); } finally { tieDb.close(); }
  } else if (mode === "direct-new") {
    seedLedger(db);
    compareFull(db);
    const topDb = openDatabase(`${ledgerPath}.top.sqlite`);
    try {
      const topStore = new facts.LearningFactStore(topDb, { attempts: 3, episodes: 3 });
      for (const sample of samples) {
        topStore.recordToolSignal(sample.attempt);
        topStore.recordWorkEpisode(sample.episode);
      }
      compareTop(topDb);
    } finally { topDb.close(); }
    const tieDb = openDatabase(`${ledgerPath}.tie.sqlite`);
    try { seedTie(tieDb, 2); compareTie(tieDb); } finally { tieDb.close(); }
  } else if (mode === "upgrade-new") {
    new facts.LearningFactStore(db, { attempts: 512, episodes: 512 });
    compareFull(db);
  } else if (mode === "downgrade-old") {
    const store = new facts.LearningFactStore(db, { attempts: 512, episodes: 512 });
    for (const [index, timestamp] of downgradeTimestamps.entries()) {
      const sessionId = `timestamp-downgrade-${index}`;
      const attempt = { kind: "attempt", source: "codex", sessionId,
        operationId: facts.deterministicToolOperationId({
          source: "codex", sessionId, sourceOperationKey: "downgrade",
        }), toolClass: "compute", toolName: "shell", startedAt: timestamp };
      const episode = facts.buildWorkEpisodeFact({ source: "codex", sessionId,
        sourceEpisodeKey: "downgrade", workClass: "review", complexityBand: "medium",
        startedAt: timestamp });
      assert.equal(store.recordToolSignal(attempt).inserted, true);
      assert.equal(store.recordWorkEpisode(episode).inserted, true);
    }
    const nullKeys = db.prepare(`select count(*) as n from tool_attempt_facts
      where session_id like 'timestamp-downgrade-%' and retention_ms is null`).get().n;
    assert.ok(nullKeys >= 1, "old writer must be allowed to leave unfamiliar key NULL");
    console.log(JSON.stringify({ mode, insertedPerTable: downgradeTimestamps.length, nullKeys }));
  } else {
    new facts.LearningFactStore(db, { attempts: 512, episodes: 512 });
    for (const table of ["tool_attempt_facts", "work_episode_facts"]) {
      const rows = db.prepare(`select started_at as timestamp, retention_ms as milliseconds
        from ${table} where session_id like 'timestamp-downgrade-%'`).all() as Array<{
          timestamp: string; milliseconds: number }>;
      assert.equal(rows.length, downgradeTimestamps.length);
      for (const row of rows) assert.equal(row.milliseconds, Date.parse(row.timestamp));
    }
    const before = Object.fromEntries(["tool_attempt_facts", "work_episode_facts"].map((table) => [
      table, (db.prepare(`select ${table === "tool_attempt_facts" ? "operation_id" : "episode_id"} as id
        from ${table} order by retention_ms desc,
        ${table === "tool_attempt_facts" ? "operation_id" : "episode_id"} desc limit 3`)
        .all() as Array<{id:string}>).map((row) => row.id),
    ]));
    db.close();
    const trimmed = openDatabase(ledgerPath);
    try {
      new facts.LearningFactStore(trimmed, { attempts: 3, episodes: 3 });
      for (const table of ["tool_attempt_facts", "work_episode_facts"]) {
        const ids = (trimmed.prepare(`select ${table === "tool_attempt_facts" ? "operation_id" : "episode_id"} as id
          from ${table} order by retention_ms desc,
          ${table === "tool_attempt_facts" ? "operation_id" : "episode_id"} desc`)
          .all() as Array<{id:string}>).map((row) => row.id);
        assert.deepEqual(ids, before[table], table);
      }
    } finally { trimmed.close(); }
    console.log(JSON.stringify({ mode, repairedPerTable: downgradeTimestamps.length,
      retainedPerTable: 3 }));
  }
  console.log(JSON.stringify({ mode, seed: "0x5eed4070", generated: candidates.length,
    admitted: samples.length, rejected, topAttempts: topThree("operationId"),
    topEpisodes: topThree("episodeId") }));
} finally { if (db.open) db.close(); }

if (mode === "upgrade-new") {
  const trimmed = new Database(ledgerPath);
  try {
    new facts.LearningFactStore(trimmed, { attempts: 3, episodes: 3 });
    compareTop(trimmed);
    console.log(JSON.stringify({ mode: "upgrade-trim", retainedPerTable: 3 }));
  } finally { trimmed.close(); }
  const tieDb = openDatabase(`${ledgerPath}.tie.sqlite`);
  try {
    new facts.LearningFactStore(tieDb, { attempts: 2, episodes: 2 });
    compareTie(tieDb);
    console.log(JSON.stringify({ mode: "upgrade-tie", equalInstantSpellingCount: tieSamples.length,
      retainedPerTable: 2 }));
  } finally { tieDb.close(); }
}
