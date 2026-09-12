/**
 * Fixture-root guard proof (issue 0071).
 *
 * Four lane runs rewrote a real ~/.grok/hooks/plimsoll.json because a config
 * harness let apply default to the process home. This proof pins that shut from
 * both sides under a sentinel HOME that must stay byte-identical throughout:
 *
 *   a) with PLIMSOLL_FIXTURE_ROOT unset, every managed apply — Grok hooks, the
 *      Grok header file, Codex config.toml, Claude settings.json, a fleet
 *      Claude seat settings.json, Gemini settings.json, and the real `setup`
 *      command in a child process — fails closed with zero writes under the
 *      sentinel HOME;
 *   b) with PLIMSOLL_FIXTURE_ROOT set, the same applies succeed and write only
 *      inside the fixture root, and a target outside it is still refused.
 *
 * Nothing here reads or writes the operator's real tool config: the one case
 * that names a real-home path asserts the refusal and never touches the path.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { useFixtureRoot } from "./lib/fixture-root";
import {
  FIXTURE_ROOT_ENV,
  ManagedConfigTargetError,
  applyClaudeSettings,
  applyCodexConfig,
  applyGeminiSettings,
  applyGrokHookFile,
  applyCodexHookHeaderFile,
  applyGrokHookHeaderFile,
  generateClaudeCodeSettings,
  generateCodexConfigToml,
  generateGeminiCliSettings,
  generateCodexHookHeader,
  generateGrokHookHeader,
  generateGrokHookSettings,
  managedConfigProofContext,
} from "../packages/collector-config/src/index";

type Check = { name: string; passed: true; detail: unknown };

const repoRoot = path.resolve(import.meta.dirname, "..");
const cli = path.join(repoRoot, "packages", "collector-cli", "src", "cli.ts");
const loader = path.join(repoRoot, "node_modules", "tsx", "dist", "loader.mjs");
const checks: Check[] = [];

function check(name: string, condition: unknown, detail: unknown) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, passed: true, detail });
}

/** Byte-exact tree digest: any created, changed or removed entry changes it. */
function treeDigest(directory: string) {
  const entries: string[] = [];
  const walk = (current: string) => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const absolute = path.join(current, entry.name);
      const relative = path.relative(directory, absolute);
      if (entry.isDirectory()) {
        entries.push(`d ${relative}`);
        walk(absolute);
      } else if (entry.isSymbolicLink()) {
        entries.push(`l ${relative} ${fs.readlinkSync(absolute)}`);
      } else {
        entries.push(`f ${relative} ${crypto.createHash("sha256").update(fs.readFileSync(absolute)).digest("hex")}`);
      }
    }
  };
  walk(directory);
  return crypto.createHash("sha256").update(entries.join("\n")).digest("hex");
}

function refusal(run: () => unknown) {
  try {
    run();
  } catch (error) {
    if (error instanceof ManagedConfigTargetError) return error.code;
    return `unexpected:${error instanceof Error ? error.message : String(error)}`;
  }
  return "no_refusal";
}

// Synthetic, never a real credential: the header template requires a
// 43-character URL-safe producer token and this proof never prints it.
const syntheticGrokToken = `fixture-root-guard-synthetic-grok-token${"-".repeat(4)}`;
const syntheticCodexToken = `fixture-root-guard-synthetic-codex-token${"-".repeat(3)}`;

const options = {
  repoRoot: "/synthetic/plimsoll/source",
  port: 49171,
  dataMode: "metadata" as const,
  grokProducerToken: syntheticGrokToken,
  codexProducerToken: syntheticCodexToken,
};

/** Synthetic seat slug: named under fixture and sentinel homes only. */
const GUARD_SEAT_SLUG = "fixture-root-guard-seat";

/** Every managed target a `setup` run owns, keyed by the entry that applies it. */
function managedTargets(home: string) {
  const grokHeaderFile = path.join(home, ".grok", "hooks", "plimsoll.headers");
  const codexHeaderFile = path.join(home, ".codex", "plimsoll.headers");
  const generated = { ...options, grokHeaderFile, codexHeaderFile };
  return [
    {
      name: "claude",
      file: path.join(home, ".claude", "settings.json"),
      apply: (file: string) => applyClaudeSettings(file, generateClaudeCodeSettings(generated)),
    },
    {
      // Fleet Claude seat (bead eco-6hoxj.48): setup manages every
      // ~/.claude-seats/<slug>/settings.json, so the guard owns them too.
      name: "claudeSeat",
      file: path.join(home, ".claude-seats", GUARD_SEAT_SLUG, "settings.json"),
      apply: (file: string) => applyClaudeSettings(file, generateClaudeCodeSettings(generated), {
        managedTarget: `claudeSeat[${GUARD_SEAT_SLUG}]`,
      }),
    },
    {
      name: "gemini",
      file: path.join(home, ".gemini", "settings.json"),
      apply: (file: string) => applyGeminiSettings(file, generateGeminiCliSettings(generated)),
    },
    {
      name: "grokHeaders",
      file: grokHeaderFile,
      apply: (file: string) => applyGrokHookHeaderFile(file, generateGrokHookHeader(generated)),
    },
    {
      name: "grok",
      file: path.join(home, ".grok", "hooks", "plimsoll.json"),
      apply: (file: string) => applyGrokHookFile(file, generateGrokHookSettings(generated)),
    },
    {
      name: "codexHeaders",
      file: codexHeaderFile,
      apply: (file: string) => applyCodexHookHeaderFile(file, generateCodexHookHeader(generated)),
    },
    {
      name: "codex",
      file: path.join(home, ".codex", "config.toml"),
      apply: (file: string) => applyCodexConfig(file, generateCodexConfigToml(generated)),
    },
  ];
}

function runSetup(env: NodeJS.ProcessEnv) {
  const result = spawnSync(process.execPath, ["--import", loader, cli, "setup", "--yes"], {
    cwd: repoRoot,
    env: { PATH: process.env.PATH, LANG: "en_US.UTF-8", TZ: "UTC", ...env },
    encoding: "utf8",
    timeout: 120_000,
  });
  return { code: result.status, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}

function main() {
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-fixture-root-guard-"));
  const previousEnv = new Map<string, string | undefined>();
  for (const key of [FIXTURE_ROOT_ENV, "PLIMSOLL_PROOF_ROOT", "HOME", "USERPROFILE",
    "PLIMSOLL_HOME", "CODEX_HOME", "GROK_HOME", "CLAUDE_CONFIG_DIR"]) {
    previousEnv.set(key, process.env[key]);
  }
  try {
    // The sentinel HOME is what a forgotten fixture would have rewritten. It is
    // pre-populated so an empty-tree digest cannot hide a write.
    const sentinelHome = path.join(sandbox, "sentinel-home");
    for (const directory of [".claude", ".codex", ".gemini", path.join(".grok", "hooks"),
      path.join(".claude-seats", GUARD_SEAT_SLUG)]) {
      fs.mkdirSync(path.join(sentinelHome, directory), { recursive: true, mode: 0o700 });
    }
    fs.writeFileSync(path.join(sentinelHome, ".claude-seats", GUARD_SEAT_SLUG, "settings.json"),
      '{"hooks":{"SessionStart":[]}}\n', { mode: 0o600 });
    fs.writeFileSync(path.join(sentinelHome, ".grok", "hooks", "operator-owned.json"),
      '{"hooks":{"SessionStart":[]}}\n', { mode: 0o600 });
    fs.writeFileSync(path.join(sentinelHome, ".codex", "auth.json"),
      '{"synthetic":"must-remain"}\n', { mode: 0o600 });
    const sentinelBefore = treeDigest(sentinelHome);

    check(
      "a_proof_context_is_detected_from_the_proof_entry_alone",
      managedConfigProofContext({}, [process.execPath, "scripts/fixture-root-guard-proof.ts"]),
      { argv: "scripts/fixture-root-guard-proof.ts" },
    );

    // (a) Fixture contract unset, sentinel HOME: every apply must fail closed.
    delete process.env[FIXTURE_ROOT_ENV];
    delete process.env.PLIMSOLL_PROOF_ROOT;
    process.env.HOME = sentinelHome;
    process.env.USERPROFILE = sentinelHome;
    process.env.GROK_HOME = path.join(sentinelHome, ".grok");
    process.env.CODEX_HOME = path.join(sentinelHome, ".codex");
    process.env.CLAUDE_CONFIG_DIR = path.join(sentinelHome, ".claude");
    process.env.PLIMSOLL_HOME = path.join(sentinelHome, ".plimsoll");
    check(
      "a_sentinel_home_is_the_process_home_for_the_unset_contract_cases",
      os.homedir() === sentinelHome && process.env[FIXTURE_ROOT_ENV] === undefined,
      { home: os.homedir() },
    );

    const unsetRefusals = managedTargets(sentinelHome)
      .map((target) => ({ name: target.name, code: refusal(() => target.apply(target.file)) }));
    check(
      "a_every_managed_apply_refuses_without_a_fixture_root",
      unsetRefusals.every((entry) => entry.code === "FIXTURE_ROOT_REQUIRED"),
      unsetRefusals,
    );
    check(
      "a_refused_applies_wrote_zero_bytes_under_the_sentinel_home",
      treeDigest(sentinelHome) === sentinelBefore,
      { before: sentinelBefore, after: treeDigest(sentinelHome) },
    );

    // The same refusal must reach a child `setup`, which is how the incident
    // ran. PLIMSOLL_PROOF_ROOT marks the child as a proof without declaring a
    // fixture root, exactly the state the harnesses were in.
    const childSetup = runSetup({
      HOME: sentinelHome,
      GROK_HOME: path.join(sentinelHome, ".grok"),
      PLIMSOLL_HOME: path.join(sentinelHome, ".plimsoll"),
      PLIMSOLL_PROOF_ROOT: sandbox,
    });
    check(
      "a_child_setup_under_a_proof_context_fails_closed_without_a_fixture_root",
      childSetup.code !== 0 &&
        `${childSetup.stdout}${childSetup.stderr}`.includes("MANAGED_CONFIG_FIXTURE_ROOT_REQUIRED") &&
        treeDigest(sentinelHome) === sentinelBefore,
      { code: childSetup.code, sentinelUnchanged: treeDigest(sentinelHome) === sentinelBefore },
    );

    // (b) Fixture root declared: the same applies write, and only inside it.
    const fixture = useFixtureRoot(path.join(sandbox, "fixture"));
    const fixtureTargets = managedTargets(fixture.home);
    for (const target of fixtureTargets) fs.mkdirSync(path.dirname(target.file), { recursive: true, mode: 0o700 });
    const applied = fixtureTargets.map((target) => ({
      name: target.name,
      changed: target.apply(target.file).changed,
      exists: fs.existsSync(target.file),
    }));
    check(
      "b_every_managed_apply_writes_inside_the_declared_fixture_root",
      applied.every((entry) => entry.changed && entry.exists),
      applied,
    );
    check(
      "b_declared_fixture_root_applies_left_the_sentinel_home_byte_identical",
      treeDigest(sentinelHome) === sentinelBefore,
      { before: sentinelBefore, after: treeDigest(sentinelHome) },
    );

    const outsideRefusals = managedTargets(sentinelHome)
      .map((target) => ({ name: target.name, code: refusal(() => target.apply(target.file)) }));
    check(
      "b_targets_outside_the_declared_fixture_root_are_refused_without_writes",
      outsideRefusals.every((entry) => entry.code === "TARGET_OUTSIDE_FIXTURE_ROOT") &&
        treeDigest(sentinelHome) === sentinelBefore,
      outsideRefusals,
    );

    // A fixture root that resolves to the operator home, or a target inside the
    // operator's real managed directories, is refused before any filesystem
    // call — the path below is named, never created, read or written.
    const operatorHome = os.userInfo().homedir;
    const operatorGrokHook = path.join(operatorHome, ".grok", "hooks", "plimsoll.json");
    const operatorDocument = generateGrokHookSettings({
      ...options,
      grokHeaderFile: path.join(fixture.home, ".grok", "hooks", "plimsoll.headers"),
    });
    const operatorSeat = path.join(operatorHome, ".claude-seats", GUARD_SEAT_SLUG, "settings.json");
    const insideRealHome = refusal(() => applyGrokHookFile(operatorGrokHook, operatorDocument));
    const seatInsideRealHome = refusal(() => applyClaudeSettings(
      operatorSeat,
      generateClaudeCodeSettings({ ...options, grokHeaderFile: undefined, codexHeaderFile: undefined }),
      { managedTarget: `claudeSeat[${GUARD_SEAT_SLUG}]` },
    ));
    process.env[FIXTURE_ROOT_ENV] = operatorHome;
    const rootIsRealHome = refusal(() => applyGrokHookFile(operatorGrokHook, operatorDocument));
    process.env[FIXTURE_ROOT_ENV] = fixture.root;
    check(
      "b_the_operator_home_is_refused_as_both_target_and_fixture_root",
      insideRealHome === "TARGET_INSIDE_REAL_HOME" &&
        seatInsideRealHome === "TARGET_INSIDE_REAL_HOME" &&
        rootIsRealHome === "FIXTURE_ROOT_IS_REAL_HOME" &&
        !fs.existsSync(path.dirname(operatorSeat)),
      { insideRealHome, seatInsideRealHome, rootIsRealHome },
    );

    // `setup` merges into an existing Codex root rather than creating one, the
    // same as a real machine that already runs Codex.
    const childFixtureHome = path.join(fixture.root, "child-setup-home");
    fs.mkdirSync(path.join(childFixtureHome, ".codex"), { recursive: true, mode: 0o700 });
    const childApplied = runSetup({
      ...fixture.env,
      HOME: childFixtureHome,
      GROK_HOME: path.join(childFixtureHome, ".grok"),
      PLIMSOLL_HOME: path.join(childFixtureHome, ".plimsoll"),
    });
    check(
      "b_child_setup_with_the_fixture_contract_applies_only_under_the_fixture",
      childApplied.code === 0 &&
        childApplied.stdout.includes('"status": "setup_applied"') &&
        fs.existsSync(path.join(childFixtureHome, ".grok", "hooks", "plimsoll.json")) &&
        fs.existsSync(path.join(childFixtureHome, ".claude", "settings.json")) &&
        fs.existsSync(path.join(childFixtureHome, ".codex", "config.toml")) &&
        treeDigest(sentinelHome) === sentinelBefore,
      { code: childApplied.code, sentinelUnchanged: treeDigest(sentinelHome) === sentinelBefore },
    );

    // Production parity: a packaged `plimsoll setup` is not a proof context, so
    // the guard never runs for a real operator.
    check(
      "c_production_setup_is_not_a_proof_context",
      !managedConfigProofContext({ HOME: "/Users/operator", PATH: "/usr/bin" },
        ["/usr/local/bin/node", "/usr/local/lib/plimsoll/plimsoll.mjs", "setup", "--yes"]) &&
        !managedConfigProofContext({ HOME: "/Users/operator" },
          ["/usr/local/bin/node", "/repo/packages/collector-cli/src/cli.ts", "setup"]),
      { entries: ["plimsoll.mjs setup --yes", "cli.ts setup"] },
    );

    check(
      "sentinel_home_is_byte_identical_after_every_case",
      treeDigest(sentinelHome) === sentinelBefore,
      { digest: sentinelBefore },
    );
    console.log(JSON.stringify({ issue: 71, ok: true, checks }, null, 2));
  } finally {
    for (const [key, value] of previousEnv) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main();
