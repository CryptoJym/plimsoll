/**
 * Focused synthetic proof for GitHub #123.
 *
 * Every path is under a temporary directory. The proof never reads or writes
 * the operator's HOME, Codex config, credentials, Plimsoll ledger, or service.
 */
import assert from "node:assert/strict";
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
const fixturePath = path.join(root, "scripts", "fixtures", "codex-partial-legacy.toml");
const checks: Check[] = [];

function check(name: string, condition: unknown, detail: unknown) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, passed: true, detail });
}

function backupFiles(directory: string) {
  return fs.readdirSync(directory).filter((name) => name.includes(".plimsoll-backup-")).sort();
}

function expectRejected(file: string, generated: string, expectedMessage: RegExp) {
  const before = fs.readFileSync(file, "utf8");
  let message = "";
  try {
    applyCodexConfig(file, generated);
  } catch (error) {
    message = error instanceof Error ? error.message : String(error);
  }
  check(
    `rejects_${path.basename(file, ".toml")}_before_mutation`,
    expectedMessage.test(message) && fs.readFileSync(file, "utf8") === before && backupFiles(path.dirname(file)).length === 0,
    { message, backups: backupFiles(path.dirname(file)) },
  );
}

function main() {
  check("proof_runs_on_node_22", Number(process.versions.node.split(".")[0]) === 22, process.versions.node);
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-codex-config-proof-"));
  try {
    const port = 49123;
    const generated = generateCodexConfigToml({
      repoRoot: "/synthetic/plimsoll/source",
      port,
      dataMode: "metadata",
    });
    const partial = fs.readFileSync(fixturePath, "utf8").replaceAll("__PLIMSOLL_PORT__", String(port));
    const config = path.join(sandbox, "partial", "config.toml");
    fs.mkdirSync(path.dirname(config), { recursive: true });
    fs.writeFileSync(config, partial);

    const dryPlan = applyCodexConfig(config, generated, { dryRun: true });
    check(
      "dry_run_reports_complete_legacy_reconciliation",
      dryPlan.changed &&
        dryPlan.changes.some((change) => change.includes("otel.environment replace")) &&
        dryPlan.changes.filter((change) => change.includes("replace legacy x-cfo-one-source")).length === 3 &&
        dryPlan.changes.some((change) => change.includes("features.hooks +")) &&
        ["UserPromptSubmit", "PostToolUse", "Stop"].every((event) =>
          dryPlan.changes.some((change) => change.includes(`hooks.${event} +`))
        ),
      dryPlan,
    );
    check(
      "dry_run_is_byte_noop_without_backup",
      fs.readFileSync(config, "utf8") === partial && backupFiles(path.dirname(config)).length === 0,
      { backups: backupFiles(path.dirname(config)) },
    );

    const applied = applyCodexConfig(config, generated);
    check(
      "dry_run_and_apply_report_identical_changes",
      applied.changed && isDeepStrictEqual(applied.changes, dryPlan.changes),
      { dryRun: dryPlan.changes, apply: applied.changes },
    );
    check(
      "apply_backed_up_exact_preimage_before_write",
      Boolean(applied.backupPath) &&
        fs.existsSync(applied.backupPath!) &&
        fs.readFileSync(applied.backupPath!, "utf8") === partial,
      applied.backupPath,
    );

    const reconciledText = fs.readFileSync(config, "utf8");
    const reconciled = parseToml(reconciledText) as Record<string, any>;
    const expected = parseToml(generated) as Record<string, any>;
    const exporters = ["exporter", "trace_exporter", "metrics_exporter"];
    check(
      "partial_endpoint_config_reaches_complete_generated_subset",
      reconciled.otel.environment === expected.otel.environment &&
        reconciled.otel.log_user_prompt === expected.otel.log_user_prompt &&
        exporters.every((name) =>
          reconciled.otel[name]["otlp-http"].endpoint === expected.otel[name]["otlp-http"].endpoint &&
          reconciled.otel[name]["otlp-http"].protocol === expected.otel[name]["otlp-http"].protocol &&
          reconciled.otel[name]["otlp-http"].headers["x-plimsoll-source"] === "codex" &&
          !("x-cfo-one-source" in reconciled.otel[name]["otlp-http"].headers)
        ) &&
        reconciled.features.hooks === true &&
        ["UserPromptSubmit", "PostToolUse", "Stop"].every((event) =>
          expected.hooks[event].every((expectedEntry: unknown) =>
            reconciled.hooks[event].some((actualEntry: unknown) => isDeepStrictEqual(actualEntry, expectedEntry))
          )
        ),
      reconciled,
    );
    check(
      "unrelated_config_comments_headers_and_hooks_survive",
      reconciled.operator_preferences.preserve_me === "synthetic-operator-value" &&
        reconciled.otel.exporter["otlp-http"].headers["x-synthetic-operator"] === "keep" &&
        reconciled.features.responses_websockets === true &&
        reconciled.hooks.state === "synthetic-existing-state" &&
        reconciled.hooks.UserPromptSubmit.some((entry: Record<string, any>) =>
          entry.name === "synthetic-operator-hook" &&
          entry.hooks?.[0]?.command === "printf synthetic-operator-hook"
        ) &&
        reconciled.mcp_servers.synthetic.command === "synthetic-mcp-command" &&
        reconciledText.includes("# This comment must survive byte-for-byte.") &&
        reconciledText.includes("# Keep the unrelated header and comment."),
      reconciledText,
    );

    const beforeSecond = fs.readFileSync(config, "utf8");
    const backupsAfterFirst = backupFiles(path.dirname(config));
    const second = applyCodexConfig(config, generated);
    check(
      "second_run_is_exact_noop_without_duplicate_or_backup",
      !second.changed &&
        second.changes.length === 0 &&
        fs.readFileSync(config, "utf8") === beforeSecond &&
        isDeepStrictEqual(backupFiles(path.dirname(config)), backupsAfterFirst),
      { second, backups: backupFiles(path.dirname(config)) },
    );

    const malformedDir = path.join(sandbox, "malformed");
    fs.mkdirSync(malformedDir);
    const malformed = path.join(malformedDir, "malformed.toml");
    fs.writeFileSync(malformed, "[otel]\nenvironment = \"first\"\n[otel]\nenvironment = \"duplicate\"\n");
    expectRejected(malformed, generated, /existing Codex config\.toml is invalid/);

    const ambiguousDir = path.join(sandbox, "ambiguous");
    fs.mkdirSync(ambiguousDir);
    const ambiguous = path.join(ambiguousDir, "ambiguous.toml");
    const withoutCanonicalOtel = partial.replace(
      '[otel]\nenvironment = "synthetic-legacy-environment" # Plimsoll owns this key; replace only its value.\nlog_user_prompt = false\n',
      "",
    );
    fs.writeFileSync(
      ambiguous,
      `otel.environment = "synthetic-legacy-environment"\notel.log_user_prompt = false\n${withoutCanonicalOtel}`,
    );
    expectRejected(ambiguous, generated, /dotted, inline, or implicit managed keys/);

    const hookConflictDir = path.join(sandbox, "hook-conflict");
    fs.mkdirSync(hookConflictDir);
    const hookConflict = path.join(hookConflictDir, "hook-conflict.toml");
    fs.writeFileSync(
      hookConflict,
      `${partial}\n[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype = "command"\ncommand = "curl http://127.0.0.1:49999/hooks/codex"\ntimeout = 5\n`,
    );
    expectRejected(hookConflict, generated, /already contains a different Plimsoll Codex hook/);

    const foreignDir = path.join(sandbox, "foreign");
    fs.mkdirSync(foreignDir);
    const foreign = path.join(foreignDir, "foreign.toml");
    const foreignSource = '[otel]\nexporter = "operator-owned"\n';
    fs.writeFileSync(foreign, foreignSource);
    const foreignResult = applyCodexConfig(foreign, generated);
    check(
      "foreign_otel_remains_a_precise_no_write_conflict",
      !foreignResult.changed &&
        Boolean(foreignResult.conflict?.includes("without this Plimsoll collector endpoint")) &&
        fs.readFileSync(foreign, "utf8") === foreignSource &&
        backupFiles(foreignDir).length === 0,
      foreignResult,
    );

    console.log(JSON.stringify({ issue: 123, ok: true, fixture: path.relative(root, fixturePath), checks }, null, 2));
  } finally {
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main();
