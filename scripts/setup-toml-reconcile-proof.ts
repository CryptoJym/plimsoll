import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import { parse as parseToml } from "smol-toml";

import {
  applyCodexConfig,
  generateCodexConfigToml,
} from "../packages/collector-config/src/index";

type Check = { name: string; passed: true; detail: unknown };

const root = path.resolve(import.meta.dirname, "..");
const fixture = (name: string) => path.join(root, "scripts", "fixtures", name);
const checks: Check[] = [];
const syntheticToken = "synthetic-codex-producer-token";
const generated = generateCodexConfigToml({
  repoRoot: root,
  port: 48271,
  dataMode: "metadata",
  codexProducerToken: syntheticToken,
});

function check(name: string, condition: unknown, detail: unknown) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, passed: true, detail });
}

function copyFixture(sandbox: string, name: string) {
  const target = path.join(sandbox, name);
  fs.copyFileSync(fixture(name), target);
  return target;
}

function plimsollCommands(document: Record<string, any>, event: string) {
  const commands: string[] = [];
  const visit = (value: unknown) => {
    if (Array.isArray(value)) return value.forEach(visit);
    if (!value || typeof value !== "object") return;
    for (const [key, entry] of Object.entries(value)) {
      if (key === "command" && typeof entry === "string" && entry.includes("/hooks/codex")) {
        commands.push(entry);
      }
      visit(entry);
    }
  };
  visit(document.hooks?.[event]);
  return commands;
}

function operation(result: ReturnType<typeof applyCodexConfig>, key: string) {
  return result.plan?.find((entry) => entry.key === key)?.action;
}

function runCli(home: string, args: string[]) {
  const cli = path.join(root, "packages", "collector-cli", "src", "cli.ts");
  const loader = path.join(root, "node_modules", "tsx", "dist", "loader.mjs");
  const result = spawnSync(process.execPath, ["--import", loader, cli, ...args], {
    cwd: root,
    env: {
      PATH: process.env.PATH,
      LANG: "en_US.UTF-8",
      TZ: "UTC",
      HOME: path.join(home, "operator-home-must-remain-absent"),
      PLIMSOLL_HOME: path.join(home, "plimsoll-home"),
    },
    encoding: "utf8",
    timeout: 120_000,
  });
  return { code: result.status, stdout: result.stdout, stderr: result.stderr };
}

function main() {
  check("proof_runs_on_node_22", Number(process.versions.node.split(".")[0]) === 22, process.versions.node);
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-setup-toml-reconcile-"));
  try {
    const macbook = copyFixture(sandbox, "macbook-codex-config-hooks-desktop-layout.toml");
    const macbookBefore = fs.readFileSync(macbook, "utf8");
    const macbookPlan = applyCodexConfig(macbook, generated, { dryRun: true });
    check(
      "macbook_inline_arrays_are_reconcilable_with_named_plan",
      macbookPlan.changed &&
        operation(macbookPlan, "hooks.Stop") === "added" &&
        operation(macbookPlan, "hooks.UserPromptSubmit") === "added" &&
        operation(macbookPlan, "hooks.PostToolUse") === "added",
      macbookPlan,
    );
    check(
      "macbook_dry_run_is_byte_noop_and_secret_free",
      fs.readFileSync(macbook, "utf8") === macbookBefore &&
        !JSON.stringify(macbookPlan).includes(syntheticToken),
      macbookPlan,
    );

    const macbookApplied = applyCodexConfig(macbook, generated);
    const macbookAfter = fs.readFileSync(macbook, "utf8");
    const macbookDocument = parseToml(macbookAfter) as Record<string, any>;
    check(
      "macbook_apply_preserves_foreign_hooks_state_comments_and_desktop",
      macbookApplied.changed &&
        macbookAfter.includes("mem0-fleet-hook end") &&
        macbookAfter.includes("inbox-native --runtime codex") &&
        macbookAfter.includes('[hooks.state."/Users/HOST_USER/.codex/hooks.json:pre_tool_use:0:0"]') &&
        macbookAfter.includes("dock-icon-preference = \"codex-system\"") &&
        ["Stop", "UserPromptSubmit", "PostToolUse"].every((event) =>
          plimsollCommands(macbookDocument, event).length === 1 &&
          plimsollCommands(macbookDocument, event)[0] ===
            plimsollCommands(parseToml(generated) as Record<string, any>, event)[0]
        ),
      {
        bytes: Buffer.byteLength(macbookAfter),
        hookCounts: Object.fromEntries(
          ["Stop", "UserPromptSubmit", "PostToolUse"].map((event) => [
            event,
            plimsollCommands(macbookDocument, event).length,
          ]),
        ),
      },
    );
    const secondBytes = fs.readFileSync(macbook, "utf8");
    const second = applyCodexConfig(macbook, generated);
    check(
      "macbook_second_run_is_exact_noop",
      !second.changed && fs.readFileSync(macbook, "utf8") === secondBytes,
      second,
    );

    const macbookCliRoot = path.join(sandbox, "macbook-cli");
    fs.mkdirSync(macbookCliRoot);
    const macbookCli = path.join(macbookCliRoot, "config.toml");
    fs.copyFileSync(fixture("macbook-codex-config-hooks-desktop-layout.toml"), macbookCli);
    const macbookCliBefore = fs.readFileSync(macbookCli, "utf8");
    const macbookCliPlan = runCli(macbookCliRoot, [
      "setup",
      "--claude-settings", path.join(macbookCliRoot, "claude.json"),
      "--gemini-settings", path.join(macbookCliRoot, "gemini.json"),
      "--codex-config", macbookCli,
      "--dry-run",
    ]);
    const expectedCodexPlan = [
      "otel.environment added",
      "otel.log_user_prompt added",
      "otel.exporter.otlp-http.endpoint added",
      "otel.exporter.otlp-http.protocol added",
      "otel.exporter.otlp-http.headers added",
      "otel.trace_exporter.otlp-http.endpoint added",
      "otel.trace_exporter.otlp-http.protocol added",
      "otel.trace_exporter.otlp-http.headers added",
      "otel.metrics_exporter.otlp-http.endpoint added",
      "otel.metrics_exporter.otlp-http.protocol added",
      "otel.metrics_exporter.otlp-http.headers added",
      "features.hooks added",
      "hooks.UserPromptSubmit added",
      "hooks.PostToolUse added",
      "hooks.Stop added",
    ].map((entry) => `${macbookCli}: ${entry}`);
    const actualCodexPlan = macbookCliPlan.stdout.split("\n")
      .filter((line) => line.startsWith(`${macbookCli}: `));
    check(
      "macbook_cli_dry_run_prints_exact_secret_free_plan_and_exits_zero",
      macbookCliPlan.code === 0 &&
        isDeepStrictEqual(actualCodexPlan, expectedCodexPlan) &&
        fs.readFileSync(macbookCli, "utf8") === macbookCliBefore &&
        !macbookCliPlan.stdout.includes(syntheticToken) &&
        !macbookCliPlan.stderr.includes(syntheticToken),
      { code: macbookCliPlan.code, actualCodexPlan, stderr: macbookCliPlan.stderr },
    );

    const studioFixture = fs.readFileSync(
      fixture("studio0-codex-config-hooks-after-manual-fix.toml"),
      "utf8",
    ).replaceAll("PLACEHOLDER_CODEX_PRODUCER_TOKEN", syntheticToken);
    const generatedWithoutHooks = generated.slice(0, generated.indexOf("[hooks]"));
    const studio = path.join(sandbox, "studio0-complete.toml");
    fs.writeFileSync(studio, generatedWithoutHooks + studioFixture);
    const studioBefore = fs.readFileSync(studio, "utf8");
    const studioPlan = applyCodexConfig(studio, generated, { dryRun: true });
    check(
      "studio0_post_fix_shape_is_recognized_unchanged",
      !studioPlan.changed &&
        ["Stop", "UserPromptSubmit", "PostToolUse"].every((event) =>
          operation(studioPlan, `hooks.${event}`) === "unchanged"
        ) &&
        fs.readFileSync(studio, "utf8") === studioBefore,
      studioPlan,
    );

    const preToken = copyFixture(sandbox, "codex-inline-pre-token.toml");
    const preTokenPlan = applyCodexConfig(preToken, generated, { dryRun: true });
    check(
      "pre_token_inline_hooks_plan_updates_in_place",
      ["Stop", "UserPromptSubmit", "PostToolUse"].every((event) =>
        operation(preTokenPlan, `hooks.${event}`) === "updated"
      ),
      preTokenPlan,
    );
    applyCodexConfig(preToken, generated);
    const preTokenAfter = fs.readFileSync(preToken, "utf8");
    const preTokenDocument = parseToml(preTokenAfter) as Record<string, any>;
    check(
      "pre_token_inline_hooks_replace_only_owned_groups",
      preTokenAfter.includes("printf foreign-stop") &&
        ["Stop", "UserPromptSubmit", "PostToolUse"].every((event) =>
          plimsollCommands(preTokenDocument, event).length === 1 &&
          plimsollCommands(preTokenDocument, event)[0]?.includes("x-plimsoll-token")
        ),
      {
        foreignStopPreserved: preTokenAfter.includes("printf foreign-stop"),
        hookCounts: Object.fromEntries(
          ["Stop", "UserPromptSubmit", "PostToolUse"].map((event) => [
            event,
            plimsollCommands(preTokenDocument, event).length,
          ]),
        ),
      },
    );

    const mixedRoot = path.join(sandbox, "mixed-cli");
    fs.mkdirSync(mixedRoot);
    const mixed = path.join(mixedRoot, "config.toml");
    fs.copyFileSync(fixture("codex-inline-mixed-pre-token.toml"), mixed);
    const mixedBefore = fs.readFileSync(mixed, "utf8");
    const mixedForeignFragment =
      '},  { type = "command", command = "printf foreign-inside-owned-group" }';
    const mixedArgs = [
      "setup",
      "--claude-settings", path.join(mixedRoot, "claude.json"),
      "--gemini-settings", path.join(mixedRoot, "gemini.json"),
      "--codex-config", mixed,
      "--yes",
    ];
    const mixedApplied = runCli(mixedRoot, mixedArgs);
    const mixedAfter = fs.readFileSync(mixed, "utf8");
    const mixedBackups = fs.readdirSync(mixedRoot).filter((name) => name.startsWith("config.toml.plimsoll-backup-"));
    const mixedAuth = JSON.parse(fs.readFileSync(path.join(mixedRoot, "plimsoll-home", "local-ingest-auth.json"), "utf8"));
    const mixedTokens = Object.values(mixedAuth).filter((value): value is string => typeof value === "string");
    check(
      "mixed_inline_group_real_cli_updates_only_owned_nested_hook_bytes",
      mixedApplied.code === 0 &&
        mixedAfter.includes(mixedForeignFragment) &&
        mixedBackups.length === 1 &&
        fs.readFileSync(path.join(mixedRoot, mixedBackups[0]!), "utf8") === mixedBefore &&
        mixedTokens.every((token) => !mixedApplied.stdout.includes(token) && !mixedApplied.stderr.includes(token)),
      {
        code: mixedApplied.code,
        foreignFragmentPreserved: mixedAfter.includes(mixedForeignFragment),
        backupCount: mixedBackups.length,
      },
    );
    const mixedSecond = runCli(mixedRoot, mixedArgs);
    check(
      "mixed_inline_group_second_real_cli_run_is_byte_noop",
      mixedSecond.code === 0 &&
        fs.readFileSync(mixed, "utf8") === mixedAfter &&
        fs.readdirSync(mixedRoot).filter((name) => name.startsWith("config.toml.plimsoll-backup-")).length === 1 &&
        mixedTokens.every((token) => !mixedSecond.stdout.includes(token) && !mixedSecond.stderr.includes(token)),
      { code: mixedSecond.code },
    );

    const unsafeMixedRoot = path.join(sandbox, "unsafe-mixed-cli");
    fs.mkdirSync(unsafeMixedRoot);
    const unsafeMixed = path.join(unsafeMixedRoot, "config.toml");
    fs.copyFileSync(fixture("codex-inline-mixed-unsafe-pre-token.toml"), unsafeMixed);
    const unsafeMixedBefore = fs.readFileSync(unsafeMixed, "utf8");
    const unsafeMixedResult = runCli(unsafeMixedRoot, [
      "setup",
      "--claude-settings", path.join(unsafeMixedRoot, "claude.json"),
      "--gemini-settings", path.join(unsafeMixedRoot, "gemini.json"),
      "--codex-config", unsafeMixed,
      "--yes",
    ]);
    const unsafeMixedAuth = JSON.parse(
      fs.readFileSync(path.join(unsafeMixedRoot, "plimsoll-home", "local-ingest-auth.json"), "utf8"),
    );
    const unsafeMixedTokens = Object.values(unsafeMixedAuth)
      .filter((value): value is string => typeof value === "string");
    check(
      "unsafe_mixed_inline_group_real_cli_refuses_exact_key_without_codex_write",
      unsafeMixedResult.code !== 0 &&
        unsafeMixedResult.stdout.includes("hooks.Stop cannot safely update") &&
        unsafeMixedResult.stdout.includes('"status": "refused"') &&
        fs.readFileSync(unsafeMixed, "utf8") === unsafeMixedBefore &&
        !fs.readdirSync(unsafeMixedRoot).some((name) => name.startsWith("config.toml.plimsoll-backup-")) &&
        fs.existsSync(path.join(unsafeMixedRoot, "claude.json")) &&
        fs.existsSync(path.join(unsafeMixedRoot, "gemini.json")) &&
        unsafeMixedTokens.every((token) =>
          !unsafeMixedResult.stdout.includes(token) && !unsafeMixedResult.stderr.includes(token)
        ),
      {
        code: unsafeMixedResult.code,
        codexUnchanged: fs.readFileSync(unsafeMixed, "utf8") === unsafeMixedBefore,
        backups: fs.readdirSync(unsafeMixedRoot).filter((name) => name.startsWith("config.toml.plimsoll-backup-")).length,
      },
    );

    const arrayTables = copyFixture(sandbox, "codex-hooks-array-of-tables.toml");
    const arrayTablePlan = applyCodexConfig(arrayTables, generated, { dryRun: true });
    check(
      "array_of_tables_layout_is_reconciled_by_appending_owned_groups",
      ["Stop", "UserPromptSubmit", "PostToolUse"].every((event) =>
        operation(arrayTablePlan, `hooks.${event}`) === "added"
      ),
      arrayTablePlan,
    );
    applyCodexConfig(arrayTables, generated);
    const arrayTableDocument = parseToml(fs.readFileSync(arrayTables, "utf8")) as Record<string, any>;
    check(
      "array_of_tables_foreign_groups_survive",
      ["Stop", "UserPromptSubmit", "PostToolUse"].every((event) =>
        arrayTableDocument.hooks[event].length === 2 && plimsollCommands(arrayTableDocument, event).length === 1
      ),
      Object.fromEntries(
        ["Stop", "UserPromptSubmit", "PostToolUse"].map((event) => [
          event,
          { groups: arrayTableDocument.hooks[event].length, owned: plimsollCommands(arrayTableDocument, event).length },
        ]),
      ),
    );

    const oldGenerated = generateCodexConfigToml({
      repoRoot: root,
      port: 49999,
      dataMode: "metadata",
      codexProducerToken: "old-synthetic-token",
    });
    const oldCommand = plimsollCommands(parseToml(oldGenerated) as Record<string, any>, "Stop")[0]!;
    const currentCommand = plimsollCommands(parseToml(generated) as Record<string, any>, "Stop")[0]!;
    const exporter = path.join(sandbox, "exporter-update.toml");
    const foreignHeaderFragment = '"x-foreign"   =   "keep" ,';
    fs.writeFileSync(
      exporter,
      oldGenerated
        .replaceAll(JSON.stringify(oldCommand), JSON.stringify(currentCommand))
        .replaceAll(
          'headers = { "x-plimsoll-source" = "codex", "x-plimsoll-token" = "old-synthetic-token" }',
          `headers = { ${foreignHeaderFragment} "x-plimsoll-source" = "codex", "x-plimsoll-token" = "old-synthetic-token" }`,
        ),
    );
    const foreignHeaderBytesBefore = fs.readFileSync(exporter, "utf8").split("\n")
      .filter((line) => line.includes('"x-foreign"'))
      .map((line) => line.slice(line.indexOf('"x-foreign"'), line.indexOf('"x-plimsoll-source"')));
    const exporterPlan = applyCodexConfig(exporter, generated, { dryRun: true });
    check(
      "all_three_exporters_plan_endpoint_and_header_updates",
      ["exporter", "trace_exporter", "metrics_exporter"].every((name) =>
        operation(exporterPlan, `otel.${name}.otlp-http.endpoint`) === "updated" &&
        operation(exporterPlan, `otel.${name}.otlp-http.headers`) === "updated"
      ),
      exporterPlan,
    );
    applyCodexConfig(exporter, generated);
    const exporterAfter = fs.readFileSync(exporter, "utf8");
    const exporterDocument = parseToml(exporterAfter) as Record<string, any>;
    const foreignHeaderBytesAfter = exporterAfter.split("\n")
      .filter((line) => line.includes('"x-foreign"'))
      .map((line) => line.slice(line.indexOf('"x-foreign"'), line.indexOf('"x-plimsoll-source"')));
    check(
      "all_three_exporters_update_in_place_and_preserve_foreign_headers",
      isDeepStrictEqual(foreignHeaderBytesAfter, foreignHeaderBytesBefore) &&
        exporterAfter.split(foreignHeaderFragment).length - 1 === 3 &&
        ["exporter", "trace_exporter", "metrics_exporter"].every((name) => {
        const table = exporterDocument.otel[name]["otlp-http"];
        return table.endpoint.includes(":48271/") &&
          table.headers["x-foreign"] === "keep" &&
          table.headers["x-plimsoll-token"] === syntheticToken;
      }),
      {
        foreignHeaderBytesEqual: isDeepStrictEqual(foreignHeaderBytesAfter, foreignHeaderBytesBefore),
        exporters: Object.fromEntries(
          ["exporter", "trace_exporter", "metrics_exporter"].map((name) => [
            name,
            {
              endpoint: exporterDocument.otel[name]["otlp-http"].endpoint,
              foreignHeaderPreserved: exporterDocument.otel[name]["otlp-http"].headers["x-foreign"] === "keep",
              tokenPresent: typeof exporterDocument.otel[name]["otlp-http"].headers["x-plimsoll-token"] === "string",
            },
          ]),
        ),
      },
    );

    const irreconcilable = copyFixture(sandbox, "codex-irreconcilable.toml");
    const refused = applyCodexConfig(irreconcilable, generated, { dryRun: true });
    check(
      "irreconcilable_layout_names_exact_blocked_key",
      refused.conflict?.includes("otel.exporter.otlp-http") === true,
      refused,
    );

    const independent = path.join(sandbox, "independent");
    fs.mkdirSync(independent);
    const claude = path.join(independent, "claude.json");
    const gemini = path.join(independent, "gemini.json");
    const codex = path.join(independent, "codex.toml");
    fs.copyFileSync(fixture("codex-irreconcilable.toml"), codex);
    const args = [
      "setup",
      "--claude-settings", claude,
      "--gemini-settings", gemini,
      "--codex-config", codex,
    ];
    const dry = runCli(independent, [...args, "--dry-run"]);
    check(
      "dry_run_prints_other_target_plans_before_refusal_and_keeps_files_absent",
      dry.code !== 0 &&
        dry.stdout.includes(`${claude}: claude.env.CLAUDE_CODE_ENABLE_TELEMETRY added`) &&
        dry.stdout.includes(`${gemini}: gemini.telemetry.enabled added`) &&
        dry.stdout.includes('"status":"would_apply"') &&
        dry.stdout.includes('"status":"refused"') &&
        !fs.existsSync(claude) && !fs.existsSync(gemini) &&
        !fs.existsSync(path.join(independent, "plimsoll-home")),
      dry,
    );
    const applied = runCli(independent, [...args, "--yes"]);
    const authFile = path.join(independent, "plimsoll-home", "local-ingest-auth.json");
    const tokenBytes = Object.values(JSON.parse(fs.readFileSync(authFile, "utf8")))
      .filter((value): value is string => typeof value === "string");
    check(
      "apply_writes_reconcilable_targets_reports_each_and_never_prints_tokens",
      applied.code !== 0 &&
        fs.existsSync(claude) && fs.existsSync(gemini) &&
        fs.readFileSync(codex, "utf8") === fs.readFileSync(fixture("codex-irreconcilable.toml"), "utf8") &&
        applied.stdout.includes('"claude": {') && applied.stdout.includes('"status": "applied"') &&
        applied.stdout.includes('"codex": {') && applied.stdout.includes('"status": "refused"') &&
        tokenBytes.every((token) => !applied.stdout.includes(token) && !applied.stderr.includes(token)),
      { ...applied, tokenCount: tokenBytes.length },
    );

    console.log(JSON.stringify({ ok: true, checks }, null, 2));
  } finally {
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main();
