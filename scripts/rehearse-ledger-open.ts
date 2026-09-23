#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function fail(message: string): never {
  console.error(message);
  process.exit(1);
}

function option(name: string) {
  const index = process.argv.indexOf(name);
  return index < 0 ? undefined : process.argv[index + 1];
}

function safeDirectory(directory: string) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const resolved = fs.realpathSync(directory);
  const stat = fs.lstatSync(resolved);
  if (!stat.isDirectory() || stat.isSymbolicLink()) fail(`${directory} must be a regular directory`);
  if (typeof process.getuid === "function" && stat.uid !== process.getuid()) {
    fail(`${directory} must be owned by the current user`);
  }
  if ((stat.mode & 0o7077) !== 0) fail(`${directory} must have mode 0700 with no special bits`);
  return resolved;
}

const ledgerArgument = option("--ledger");
const homeArgument = option("--home");
const collectorArgument = option("--collector");
if (!ledgerArgument || !homeArgument || !process.argv.includes("--confirm-copy")) {
  fail(
    "Usage: pnpm rehearse:ledger-open -- --ledger /absolute/copied-ledger.sqlite " +
    "--home /absolute/sandbox-home --confirm-copy [--collector /absolute/cli.mjs]\n" +
    "The command opens and migrates the supplied ledger read-write. Never pass a live ledger.",
  );
}
if (!path.isAbsolute(ledgerArgument) || !path.isAbsolute(homeArgument)) {
  fail("--ledger and --home must be absolute paths");
}

const ledgerInputStat = fs.lstatSync(ledgerArgument);
if (!ledgerInputStat.isFile() || ledgerInputStat.isSymbolicLink()) {
  fail("--ledger must be a regular, non-symlink copied ledger");
}
const ledgerPath = fs.realpathSync(ledgerArgument);
const ledgerStat = fs.statSync(ledgerPath);
const liveLedgers = new Set([
  path.join(os.homedir(), "Library", "Application Support", "Plimsoll", "work-ledger.sqlite"),
  ...(path.isAbsolute(process.env.PLIMSOLL_HOME ?? "")
    ? [path.join(process.env.PLIMSOLL_HOME!, "work-ledger.sqlite")]
    : []),
]);
for (const liveLedger of liveLedgers) {
  if (path.resolve(ledgerPath) === path.resolve(liveLedger)) {
    fail("refusing the current user's live Plimsoll ledger; pass a disposable copy");
  }
  if (fs.existsSync(liveLedger)) {
    const liveStat = fs.statSync(liveLedger);
    if (liveStat.dev === ledgerStat.dev && liveStat.ino === ledgerStat.ino) {
      fail("refusing a hard link to the current user's live Plimsoll ledger");
    }
  }
}

const sandboxHome = safeDirectory(homeArgument);
if (sandboxHome === fs.realpathSync(os.homedir())) {
  fail("--home must be a sandbox, not the current user's real HOME");
}
const plimsollHome = safeDirectory(path.join(sandboxHome, ".plimsoll"));
for (const directory of [".codex", ".claude", "tmp", ".config", ".cache", ".local/state"]) {
  safeDirectory(path.join(sandboxHome, directory));
}

const collectorPath = path.resolve(
  collectorArgument ?? path.join(repoRoot, "packages", "collector-cli", "dist", "cli.mjs"),
);
if (!fs.existsSync(collectorPath)) {
  fail(`built collector not found at ${collectorPath}; run pnpm --dir packages/collector-cli build first`);
}

const result = spawnSync(process.execPath, [
  collectorPath,
  "__rehearse_ledger_open",
  "--ledger",
  ledgerPath,
], {
  cwd: repoRoot,
  env: {
    ...process.env,
    HOME: sandboxHome,
    USERPROFILE: sandboxHome,
    PLIMSOLL_HOME: plimsollHome,
    CODEX_HOME: path.join(sandboxHome, ".codex"),
    CLAUDE_CONFIG_DIR: path.join(sandboxHome, ".claude"),
    XDG_CONFIG_HOME: path.join(sandboxHome, ".config"),
    XDG_CACHE_HOME: path.join(sandboxHome, ".cache"),
    XDG_STATE_HOME: path.join(sandboxHome, ".local", "state"),
    TMPDIR: path.join(sandboxHome, "tmp"),
    PLIMSOLL_REHEARSAL: "copied-ledger-v1",
  },
  encoding: "utf8",
  stdio: ["ignore", "pipe", "pipe"],
});
process.stdout.write(result.stdout ?? "");
process.stderr.write(result.stderr ?? "");
if (result.error) fail(result.error.message);
process.exit(result.status ?? 1);
