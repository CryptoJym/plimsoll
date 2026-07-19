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

function tempFiles(directory: string) {
  return fs.readdirSync(directory).filter((name) => name.includes(".plimsoll-tmp-")).sort();
}

function recursiveTempFiles(directory: string): string[] {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(directory, entry.name);
    if (entry.isDirectory()) return recursiveTempFiles(entryPath);
    return entry.name.includes(".plimsoll-tmp-") ? [entryPath] : [];
  }).sort();
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
    expectedMessage.test(message) &&
      fs.readFileSync(file, "utf8") === before &&
      backupFiles(path.dirname(file)).length === 0 &&
      tempFiles(path.dirname(file)).length === 0,
    { message, backups: backupFiles(path.dirname(file)), temps: tempFiles(path.dirname(file)) },
  );
}

function ownedHeaderNames(headers: Record<string, unknown>) {
  return Object.keys(headers).filter((name) => {
    const folded = name.toLowerCase();
    return folded === "x-cfo-one-source" || folded === "x-plimsoll-source";
  });
}

function main() {
  check("proof_runs_on_node_22", Number(process.versions.node.split(".")[0]) === 22, process.versions.node);
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-codex-config-proof-"));
  const operatorHome = process.env.HOME;
  const syntheticHome = path.join(sandbox, "must-remain-absent-home");
  process.env.HOME = syntheticHome;
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
        dryPlan.changes.length === 8 &&
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

    const preCommitInode = fs.lstatSync(config).ino;
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
    check(
      "apply_atomically_replaces_inode_and_visible_path_matches_plan",
      fs.lstatSync(config).ino !== preCommitInode && tempFiles(path.dirname(config)).length === 0,
      { before: preCommitInode, after: fs.lstatSync(config).ino, temps: tempFiles(path.dirname(config)) },
    );
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

    const appendDir = path.join(sandbox, "concurrent-append");
    fs.mkdirSync(appendDir);
    const appendConfig = path.join(appendDir, "config.toml");
    const appendedBytes = "\n# synthetic concurrent append must survive\n";
    fs.writeFileSync(appendConfig, partial);
    let appendMessage = "";
    try {
      applyCodexConfig(appendConfig, generated, {
        transactionHooks: {
          beforeCommit: () => fs.appendFileSync(appendConfig, appendedBytes),
        },
      });
    } catch (error) {
      appendMessage = error instanceof Error ? error.message : String(error);
    }
    const appendBackups = backupFiles(appendDir);
    check(
      "concurrent_append_immediately_before_commit_is_rejected_and_preserved",
      /bound config\.toml content changed before commit/.test(appendMessage) &&
        fs.readFileSync(appendConfig, "utf8") === partial + appendedBytes &&
        appendBackups.length === 1 &&
        fs.readFileSync(path.join(appendDir, appendBackups[0]!), "utf8") === partial &&
        tempFiles(appendDir).length === 0,
      { message: appendMessage, backups: appendBackups, temps: tempFiles(appendDir) },
    );

    const sameInodeDir = path.join(sandbox, "same-inode-content-swap");
    fs.mkdirSync(sameInodeDir);
    const sameInodeConfig = path.join(sameInodeDir, "config.toml");
    const sameInodeReplacement = `${partial}\n# same inode replacement\n`;
    fs.writeFileSync(sameInodeConfig, partial);
    const sameInodeBefore = fs.lstatSync(sameInodeConfig).ino;
    let sameInodeAfter = 0;
    let sameInodeMessage = "";
    try {
      applyCodexConfig(sameInodeConfig, generated, {
        transactionHooks: {
          afterBackup: () => {
            fs.writeFileSync(sameInodeConfig, sameInodeReplacement);
            sameInodeAfter = fs.lstatSync(sameInodeConfig).ino;
          },
        },
      });
    } catch (error) {
      sameInodeMessage = error instanceof Error ? error.message : String(error);
    }
    const sameInodeBackups = backupFiles(sameInodeDir);
    check(
      "post_backup_same_inode_content_swap_is_rejected_and_preserved",
      sameInodeAfter === sameInodeBefore &&
        /bound config\.toml content changed before commit/.test(sameInodeMessage) &&
        fs.readFileSync(sameInodeConfig, "utf8") === sameInodeReplacement &&
        sameInodeBackups.length === 1 &&
        fs.readFileSync(path.join(sameInodeDir, sameInodeBackups[0]!), "utf8") === partial &&
        tempFiles(sameInodeDir).length === 0,
      {
        message: sameInodeMessage,
        beforeInode: sameInodeBefore,
        afterInode: sameInodeAfter,
        backups: sameInodeBackups,
        temps: tempFiles(sameInodeDir),
      },
    );

    const preCommitSwapDir = path.join(sandbox, "pre-commit-path-swap");
    fs.mkdirSync(preCommitSwapDir);
    const preCommitSwapConfig = path.join(preCommitSwapDir, "config.toml");
    const preCommitDetached = path.join(preCommitSwapDir, "config.detached.toml");
    const preCommitVisibleReplacement = "# synthetic visible replacement before commit\n";
    fs.writeFileSync(preCommitSwapConfig, partial);
    let preCommitSwapMessage = "";
    try {
      applyCodexConfig(preCommitSwapConfig, generated, {
        transactionHooks: {
          beforeCommit: () => {
            fs.renameSync(preCommitSwapConfig, preCommitDetached);
            fs.writeFileSync(preCommitSwapConfig, preCommitVisibleReplacement);
          },
        },
      });
    } catch (error) {
      preCommitSwapMessage = error instanceof Error ? error.message : String(error);
    }
    const preCommitSwapBackups = backupFiles(preCommitSwapDir);
    check(
      "pathname_replacement_immediately_before_commit_is_rejected_without_detached_inode_write",
      /config\.toml was replaced after planning/.test(preCommitSwapMessage) &&
        fs.readFileSync(preCommitDetached, "utf8") === partial &&
        fs.readFileSync(preCommitSwapConfig, "utf8") === preCommitVisibleReplacement &&
        preCommitSwapBackups.length === 1 &&
        fs.readFileSync(path.join(preCommitSwapDir, preCommitSwapBackups[0]!), "utf8") === partial &&
        tempFiles(preCommitSwapDir).length === 0,
      { message: preCommitSwapMessage, backups: preCommitSwapBackups, temps: tempFiles(preCommitSwapDir) },
    );

    const postCommitSwapDir = path.join(sandbox, "post-commit-visible-swap");
    fs.mkdirSync(postCommitSwapDir);
    const postCommitSwapConfig = path.join(postCommitSwapDir, "config.toml");
    const postCommitDetached = path.join(postCommitSwapDir, "config.committed.toml");
    const postCommitVisibleReplacement = "# synthetic visible replacement after commit\n";
    fs.writeFileSync(postCommitSwapConfig, partial);
    let postCommitReturned = false;
    let postCommitSwapMessage = "";
    try {
      applyCodexConfig(postCommitSwapConfig, generated, {
        transactionHooks: {
          afterCommit: () => {
            fs.renameSync(postCommitSwapConfig, postCommitDetached);
            fs.writeFileSync(postCommitSwapConfig, postCommitVisibleReplacement);
          },
        },
      });
      postCommitReturned = true;
    } catch (error) {
      postCommitSwapMessage = error instanceof Error ? error.message : String(error);
    }
    const postCommitSwapBackups = backupFiles(postCommitSwapDir);
    check(
      "visible_path_replacement_after_commit_never_reports_success",
      !postCommitReturned &&
        /visible config\.toml identity does not match the committed plan/.test(postCommitSwapMessage) &&
        fs.readFileSync(postCommitDetached, "utf8") === reconciledText &&
        fs.readFileSync(postCommitSwapConfig, "utf8") === postCommitVisibleReplacement &&
        postCommitSwapBackups.length === 1 &&
        fs.readFileSync(path.join(postCommitSwapDir, postCommitSwapBackups[0]!), "utf8") === partial &&
        tempFiles(postCommitSwapDir).length === 0,
      {
        message: postCommitSwapMessage,
        returned: postCommitReturned,
        backups: postCommitSwapBackups,
        temps: tempFiles(postCommitSwapDir),
      },
    );

    const caseAliasDir = path.join(sandbox, "case-alias");
    fs.mkdirSync(caseAliasDir);
    const caseAlias = path.join(caseAliasDir, "case-alias.toml");
    const caseAliasSource = partial.replaceAll('"x-cfo-one-source"', '"X-CFO-One-Source"');
    fs.writeFileSync(caseAlias, caseAliasSource);
    const caseAliasResult = applyCodexConfig(caseAlias, generated);
    const caseAliasDocument = parseToml(fs.readFileSync(caseAlias, "utf8")) as Record<string, any>;
    check(
      "case_insensitive_legacy_aliases_collapse_to_one_canonical_owned_header",
      caseAliasResult.changed &&
        caseAliasResult.changes.length === 8 &&
        caseAliasResult.changes.filter((change) => change.includes("replace legacy x-cfo-one-source")).length === 3 &&
        ["exporter", "trace_exporter", "metrics_exporter"].every((name) => {
          const headers = caseAliasDocument.otel[name]["otlp-http"].headers as Record<string, unknown>;
          return isDeepStrictEqual(ownedHeaderNames(headers), ["x-plimsoll-source"]) &&
            headers["x-plimsoll-source"] === "codex";
        }),
      { changes: caseAliasResult.changes, document: caseAliasDocument },
    );

    const confusableDir = path.join(sandbox, "unicode-confusable");
    fs.mkdirSync(confusableDir);
    const confusable = path.join(confusableDir, "unicode-confusable.toml");
    fs.writeFileSync(
      confusable,
      partial.replaceAll('"x-cfo-one-source"', '"ｘ-cfo-one-source"'),
    );
    expectRejected(confusable, generated, /non-ASCII header name/);

    const leafTargetDir = path.join(sandbox, "leaf-symlink-target");
    const leafLinkDir = path.join(sandbox, "leaf-symlink-link");
    fs.mkdirSync(leafTargetDir);
    fs.mkdirSync(leafLinkDir);
    const leafTarget = path.join(leafTargetDir, "target.toml");
    const leafLink = path.join(leafLinkDir, "config.toml");
    fs.writeFileSync(leafTarget, partial);
    fs.symlinkSync(leafTarget, leafLink);
    let leafLinkMessage = "";
    try {
      applyCodexConfig(leafLink, generated);
    } catch (error) {
      leafLinkMessage = error instanceof Error ? error.message : String(error);
    }
    check(
      "leaf_symlink_is_rejected_without_target_or_backup_mutation",
      /config\.toml is a symbolic link/.test(leafLinkMessage) &&
        fs.lstatSync(leafLink).isSymbolicLink() &&
        fs.readFileSync(leafTarget, "utf8") === partial &&
        backupFiles(leafTargetDir).length === 0 &&
        backupFiles(leafLinkDir).length === 0,
      { message: leafLinkMessage },
    );

    const ancestorTargetDir = path.join(sandbox, "ancestor-symlink-target");
    const ancestorLink = path.join(sandbox, "ancestor-symlink-link");
    fs.mkdirSync(ancestorTargetDir);
    const ancestorTarget = path.join(ancestorTargetDir, "config.toml");
    fs.writeFileSync(ancestorTarget, partial);
    fs.symlinkSync(ancestorTargetDir, ancestorLink);
    let ancestorLinkMessage = "";
    try {
      applyCodexConfig(path.join(ancestorLink, "config.toml"), generated);
    } catch (error) {
      ancestorLinkMessage = error instanceof Error ? error.message : String(error);
    }
    check(
      "ancestor_symlink_is_rejected_without_target_or_backup_mutation",
      /ancestor .* is a symbolic link/.test(ancestorLinkMessage) &&
        fs.readFileSync(ancestorTarget, "utf8") === partial &&
        backupFiles(ancestorTargetDir).length === 0,
      { message: ancestorLinkMessage },
    );

    const leafSwapDir = path.join(sandbox, "leaf-swap");
    const leafSwapTargetDir = path.join(sandbox, "leaf-swap-target");
    fs.mkdirSync(leafSwapDir);
    fs.mkdirSync(leafSwapTargetDir);
    const leafSwap = path.join(leafSwapDir, "config.toml");
    const leafSwapPreserved = path.join(leafSwapDir, "config.pre-swap.toml");
    const leafSwapTarget = path.join(leafSwapTargetDir, "target.toml");
    fs.writeFileSync(leafSwap, partial);
    check("leaf_swap_dry_run_plans_without_mutation", applyCodexConfig(leafSwap, generated, { dryRun: true }).changed, leafSwap);
    fs.renameSync(leafSwap, leafSwapPreserved);
    fs.writeFileSync(leafSwapTarget, partial);
    fs.symlinkSync(leafSwapTarget, leafSwap);
    let leafSwapMessage = "";
    try {
      applyCodexConfig(leafSwap, generated);
    } catch (error) {
      leafSwapMessage = error instanceof Error ? error.message : String(error);
    }
    check(
      "leaf_swap_after_dry_run_is_rejected_without_target_or_backup_mutation",
      /config\.toml is a symbolic link/.test(leafSwapMessage) &&
        fs.readFileSync(leafSwapPreserved, "utf8") === partial &&
        fs.readFileSync(leafSwapTarget, "utf8") === partial &&
        backupFiles(leafSwapDir).length === 0 &&
        backupFiles(leafSwapTargetDir).length === 0,
      { message: leafSwapMessage },
    );

    const ancestorSwapRoot = path.join(sandbox, "ancestor-swap");
    const ancestorSwapParent = path.join(ancestorSwapRoot, "config-parent");
    const ancestorSwapPreserved = path.join(ancestorSwapRoot, "config-parent.pre-swap");
    const ancestorSwapTarget = path.join(ancestorSwapRoot, "swap-target");
    fs.mkdirSync(ancestorSwapParent, { recursive: true });
    fs.mkdirSync(ancestorSwapTarget);
    const ancestorSwapConfig = path.join(ancestorSwapParent, "config.toml");
    fs.writeFileSync(ancestorSwapConfig, partial);
    check(
      "ancestor_swap_dry_run_plans_without_mutation",
      applyCodexConfig(ancestorSwapConfig, generated, { dryRun: true }).changed,
      ancestorSwapConfig,
    );
    fs.renameSync(ancestorSwapParent, ancestorSwapPreserved);
    const ancestorSwapTargetConfig = path.join(ancestorSwapTarget, "config.toml");
    fs.writeFileSync(ancestorSwapTargetConfig, partial);
    fs.symlinkSync(ancestorSwapTarget, ancestorSwapParent);
    let ancestorSwapMessage = "";
    try {
      applyCodexConfig(ancestorSwapConfig, generated);
    } catch (error) {
      ancestorSwapMessage = error instanceof Error ? error.message : String(error);
    }
    check(
      "ancestor_swap_after_dry_run_is_rejected_without_target_or_backup_mutation",
      /ancestor .* is a symbolic link/.test(ancestorSwapMessage) &&
        fs.readFileSync(path.join(ancestorSwapPreserved, "config.toml"), "utf8") === partial &&
        fs.readFileSync(ancestorSwapTargetConfig, "utf8") === partial &&
        backupFiles(ancestorSwapPreserved).length === 0 &&
        backupFiles(ancestorSwapTarget).length === 0,
      { message: ancestorSwapMessage },
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

    for (const event of ["UserPromptSubmit", "PostToolUse", "Stop"] as const) {
      for (const [kind, endpoint] of [
        ["case", "/HOOKS/CODEX"],
        ["unicode", "/ｈｏｏｋｓ/ｃｏｄｅｘ"],
      ] as const) {
        const hookAliasDir = path.join(sandbox, `hook-${kind}-${event}`);
        fs.mkdirSync(hookAliasDir);
        const hookAlias = path.join(hookAliasDir, `hook-${kind}-${event}.toml`);
        fs.writeFileSync(
          hookAlias,
          `${reconciledText}\n[[hooks.${event}]]\n[[hooks.${event}.hooks]]\ntype = "command"\n` +
          `command = "curl http://127.0.0.1:${port}${endpoint}"\ntimeout = 5\n`,
        );
        expectRejected(hookAlias, generated, /non-canonical owned alias/);
      }
    }

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

    check(
      "all_rejections_leave_synthetic_home_absent",
      !fs.existsSync(syntheticHome),
      syntheticHome,
    );
    check(
      "all_success_and_rejection_paths_leave_no_transaction_temp_artifacts",
      recursiveTempFiles(sandbox).length === 0,
      recursiveTempFiles(sandbox),
    );

    console.log(JSON.stringify({ issue: 123, ok: true, fixture: path.relative(root, fixturePath), checks }, null, 2));
  } finally {
    if (operatorHome === undefined) delete process.env.HOME;
    else process.env.HOME = operatorHome;
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main();
