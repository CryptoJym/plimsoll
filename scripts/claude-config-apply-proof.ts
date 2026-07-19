/**
 * Focused hostile proof for GitHub #130.
 *
 * Every path and config is synthetic and rooted below a temporary directory.
 * The proof never resolves the operator HOME or reads real Claude credentials.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  applyClaudeSettings,
  generateClaudeCodeSettings,
} from "../packages/collector-config/src/index";

type Check = { name: string; passed: true; detail: Record<string, unknown> };

const checks: Check[] = [];

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, passed: true, detail });
}

function sha256(value: string | Buffer) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function artifacts(directory: string, marker: string) {
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory).filter((name) => name.includes(marker)).sort();
}

function backups(directory: string) {
  return artifacts(directory, ".plimsoll-backup-");
}

function temps(directory: string) {
  return artifacts(directory, ".plimsoll-tmp-");
}

function errorMessage(action: () => unknown) {
  try {
    action();
    return "";
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
}

function writeJson(file: string, value: unknown, mode = 0o600) {
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`, { mode });
}

function main() {
  check("proof_runs_on_node_22", Number(process.versions.node.split(".")[0]) === 22, {
    nodeMajor: Number(process.versions.node.split(".")[0]),
  });

  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-claude-config-proof-"));
  const syntheticHome = path.join(sandbox, "synthetic-home");
  const operatorHome = process.env.HOME;
  process.env.HOME = path.join(sandbox, "must-remain-absent-operator-home");
  const generated = generateClaudeCodeSettings({
    repoRoot: "/synthetic/plimsoll/source",
    port: 49130,
    dataMode: "metadata",
  });
  const secretSentinel = "SYNTHETIC_SECRET_MUST_NEVER_APPEAR_IN_RECEIPTS";

  try {
    const fresh = path.join(syntheticHome, ".claude", "settings.json");
    const dryFresh = applyClaudeSettings(fresh, generated, { dryRun: true });
    check(
      "fresh_absent_parent_preview_is_byte_noop",
      dryFresh.changed && !fs.existsSync(syntheticHome),
      { changed: dryFresh.changed, createdEntries: 0 },
    );
    const freshApplied = applyClaudeSettings(fresh, generated);
    const freshSource = fs.readFileSync(fresh, "utf8");
    const freshMode = fs.statSync(fresh).mode & 0o777;
    const freshParentMode = fs.statSync(path.dirname(fresh)).mode & 0o777;
    check(
      "fresh_apply_creates_private_parent_and_atomic_file",
      freshApplied.changed &&
        freshApplied.backupPath === undefined &&
        freshMode === 0o600 &&
        freshParentMode === 0o700 &&
        temps(path.dirname(fresh)).length === 0 &&
        backups(path.dirname(fresh)).length === 0,
      { fileMode: freshMode, parentMode: freshParentMode, temps: temps(path.dirname(fresh)).length },
    );
    const freshSecond = applyClaudeSettings(fresh, generated);
    check(
      "fresh_second_apply_is_exact_noop_without_backup_churn",
      !freshSecond.changed &&
        fs.readFileSync(fresh, "utf8") === freshSource &&
        backups(path.dirname(fresh)).length === 0,
      { changed: freshSecond.changed, backups: backups(path.dirname(fresh)).length },
    );

    const parentCreateRoot = path.join(sandbox, "parent-create-race");
    const parentCreateDir = path.join(parentCreateRoot, ".claude");
    const parentCreateMoved = path.join(parentCreateRoot, ".claude-created");
    const parentCreateFile = path.join(parentCreateDir, "settings.json");
    const parentCreateError = errorMessage(() => applyClaudeSettings(parentCreateFile, generated, {
      transactionHooks: {
        afterParentCreate: () => {
          fs.renameSync(parentCreateDir, parentCreateMoved);
          fs.mkdirSync(parentCreateDir, { mode: 0o700 });
        },
      },
    }));
    check(
      "fresh_parent_swap_after_private_creation_is_rejected",
      parentCreateError === "CLAUDE_CONFIG_ANCESTOR_CHANGED" &&
        !fs.existsSync(parentCreateFile) &&
        !fs.existsSync(path.join(parentCreateMoved, "settings.json")) &&
        backups(parentCreateDir).length === 0,
      { error: parentCreateError, settingsCreated: false },
    );

    const existingDir = path.join(sandbox, "existing");
    fs.mkdirSync(existingDir, { mode: 0o700 });
    const existing = path.join(existingDir, "settings.json");
    const canonicalUrl = "http://127.0.0.1:49130/hooks/claude-code";
    const preimage = {
      theme: "dark",
      operatorSecret: secretSentinel,
      env: { USER_SETTING: "preserve", OTEL_LOGS_EXPORTER: "legacy" },
      hooks: {
        UserPromptSubmit: [
          {
            hooks: [
              { type: "command", command: "synthetic-foreign-hook" },
              { type: "http", url: canonicalUrl },
            ],
          },
        ],
        ForeignEvent: [{ marker: secretSentinel }],
      },
    };
    writeJson(existing, preimage, 0o644);
    const preimageSource = fs.readFileSync(existing, "utf8");
    const preview = applyClaudeSettings(existing, generated, { dryRun: true });
    check(
      "existing_preview_preserves_bytes_mode_and_has_content_free_receipt",
      preview.changed &&
        fs.readFileSync(existing, "utf8") === preimageSource &&
        (fs.statSync(existing).mode & 0o777) === 0o644 &&
        backups(existingDir).length === 0 &&
        !JSON.stringify(preview.changes).includes(secretSentinel) &&
        !JSON.stringify(preview.changes).includes(canonicalUrl) &&
        !JSON.stringify(preview.changes).includes(existing),
      { changes: preview.changes.length, backups: backups(existingDir).length },
    );
    const beforeInode = fs.statSync(existing).ino;
    const result = applyClaudeSettings(existing, generated);
    const reconciledSource = fs.readFileSync(existing, "utf8");
    const reconciled = JSON.parse(reconciledSource) as Record<string, any>;
    check(
      "apply_preserves_unrelated_json_and_reconciles_partial_owned_hook",
      result.changed &&
        reconciled.theme === "dark" &&
        reconciled.operatorSecret === secretSentinel &&
        reconciled.env.USER_SETTING === "preserve" &&
        reconciled.env.OTEL_LOGS_EXPORTER === "otlp" &&
        reconciled.hooks.ForeignEvent[0].marker === secretSentinel &&
        reconciled.hooks.UserPromptSubmit.length === 2 &&
        reconciled.hooks.UserPromptSubmit[0].hooks[0].command === "synthetic-foreign-hook" &&
        reconciled.hooks.UserPromptSubmit[1].hooks[0].timeout === 5,
      { changes: result.changes.length, ownedHooks: reconciled.hooks.UserPromptSubmit.length - 1 },
    );
    const backupPath = result.backupPath!;
    check(
      "apply_backs_up_exact_preimage_once_and_atomically_replaces_destination",
      Boolean(backupPath) &&
        fs.readFileSync(backupPath, "utf8") === preimageSource &&
        sha256(fs.readFileSync(backupPath)) === sha256(preimageSource) &&
        (fs.statSync(backupPath).mode & 0o777) === 0o644 &&
        (fs.statSync(existing).mode & 0o777) === 0o644 &&
        fs.statSync(existing).ino !== beforeInode &&
        backups(existingDir).length === 1 &&
        temps(existingDir).length === 0,
      { backups: backups(existingDir).length, mode: fs.statSync(existing).mode & 0o777 },
    );
    const afterFirstHash = sha256(reconciledSource);
    const afterFirstBackups = backups(existingDir);
    const idempotent = applyClaudeSettings(existing, generated);
    check(
      "existing_second_apply_is_byte_noop_without_backup_churn",
      !idempotent.changed &&
        sha256(fs.readFileSync(existing)) === afterFirstHash &&
        JSON.stringify(backups(existingDir)) === JSON.stringify(afterFirstBackups),
      { changed: idempotent.changed, backups: backups(existingDir).length },
    );

    const appendDir = path.join(sandbox, "concurrent-append");
    fs.mkdirSync(appendDir, { mode: 0o700 });
    const appendFile = path.join(appendDir, "settings.json");
    writeJson(appendFile, preimage);
    const appendPreimage = fs.readFileSync(appendFile, "utf8");
    const appendBytes = " \n";
    const appendError = errorMessage(() => applyClaudeSettings(appendFile, generated, {
      transactionHooks: { beforeCommit: () => fs.appendFileSync(appendFile, appendBytes) },
    }));
    check(
      "concurrent_append_before_commit_is_rejected_and_preserved",
      appendError === "CLAUDE_CONFIG_BOUND_CONTENT_CHANGED" &&
        fs.readFileSync(appendFile, "utf8") === appendPreimage + appendBytes &&
        backups(appendDir).length === 1 &&
        fs.readFileSync(path.join(appendDir, backups(appendDir)[0]!), "utf8") === appendPreimage &&
        temps(appendDir).length === 0,
      { error: appendError, backups: backups(appendDir).length },
    );

    const rewriteDir = path.join(sandbox, "same-inode-rewrite");
    fs.mkdirSync(rewriteDir, { mode: 0o700 });
    const rewriteFile = path.join(rewriteDir, "settings.json");
    writeJson(rewriteFile, preimage);
    const rewritePreimage = fs.readFileSync(rewriteFile, "utf8");
    const rewriteReplacement = `${rewritePreimage} `;
    const rewriteInode = fs.statSync(rewriteFile).ino;
    const rewriteError = errorMessage(() => applyClaudeSettings(rewriteFile, generated, {
      transactionHooks: { afterBackup: () => fs.writeFileSync(rewriteFile, rewriteReplacement) },
    }));
    check(
      "same_inode_rewrite_after_backup_is_rejected_and_preserved",
      rewriteError === "CLAUDE_CONFIG_BOUND_CONTENT_CHANGED" &&
        fs.statSync(rewriteFile).ino === rewriteInode &&
        fs.readFileSync(rewriteFile, "utf8") === rewriteReplacement &&
        fs.readFileSync(path.join(rewriteDir, backups(rewriteDir)[0]!), "utf8") === rewritePreimage &&
        temps(rewriteDir).length === 0,
      { error: rewriteError, backups: backups(rewriteDir).length },
    );

    const swapDir = path.join(sandbox, "leaf-swap");
    fs.mkdirSync(swapDir, { mode: 0o700 });
    const swapFile = path.join(swapDir, "settings.json");
    const detachedFile = path.join(swapDir, "settings.detached.json");
    writeJson(swapFile, preimage);
    const swapPreimage = fs.readFileSync(swapFile, "utf8");
    const replacement = `${JSON.stringify({ replacement: true })}\n`;
    const swapError = errorMessage(() => applyClaudeSettings(swapFile, generated, {
      transactionHooks: {
        beforeCommit: () => {
          fs.renameSync(swapFile, detachedFile);
          fs.writeFileSync(swapFile, replacement, { mode: 0o600 });
        },
      },
    }));
    check(
      "leaf_path_swap_before_commit_never_overwrites_replacement",
      swapError === "CLAUDE_CONFIG_VISIBLE_IDENTITY_MISMATCH" &&
        fs.readFileSync(detachedFile, "utf8") === swapPreimage &&
        fs.readFileSync(swapFile, "utf8") === replacement &&
        temps(swapDir).length === 0,
      { error: swapError, replacementPreserved: true },
    );

    const commitWindowDir = path.join(sandbox, "commit-window-replacement");
    fs.mkdirSync(commitWindowDir, { mode: 0o700 });
    const commitWindowFile = path.join(commitWindowDir, "settings.json");
    const commitWindowDetached = path.join(commitWindowDir, "settings.original.json");
    writeJson(commitWindowFile, preimage);
    const commitWindowSource = fs.readFileSync(commitWindowFile, "utf8");
    const commitWindowReplacement = `${JSON.stringify({ concurrent: secretSentinel })}\n`;
    const originalRename = fs.renameSync;
    let injectedCommitWindow = false;
    let commitWindowError = "";
    try {
      fs.renameSync = ((oldPath: fs.PathLike, newPath: fs.PathLike) => {
        if (
          !injectedCommitWindow &&
          String(oldPath) === commitWindowFile &&
          String(newPath).includes(".plimsoll-claim-")
        ) {
          injectedCommitWindow = true;
          originalRename(commitWindowFile, commitWindowDetached);
          fs.writeFileSync(commitWindowFile, commitWindowReplacement, { mode: 0o600 });
        }
        return originalRename(oldPath, newPath);
      }) as typeof fs.renameSync;
      commitWindowError = errorMessage(() => applyClaudeSettings(commitWindowFile, generated));
    } finally {
      fs.renameSync = originalRename;
    }
    check(
      "commit_window_path_replacement_is_restored_and_never_clobbered",
      injectedCommitWindow &&
        commitWindowError === "CLAUDE_CONFIG_COMMIT_CLAIM_MISMATCH" &&
        fs.readFileSync(commitWindowDetached, "utf8") === commitWindowSource &&
        fs.readFileSync(commitWindowFile, "utf8") === commitWindowReplacement,
      { error: commitWindowError, replacementPreserved: true },
    );

    const postDir = path.join(sandbox, "post-commit-swap");
    fs.mkdirSync(postDir, { mode: 0o700 });
    const postFile = path.join(postDir, "settings.json");
    const postDetached = path.join(postDir, "settings.committed.json");
    writeJson(postFile, preimage);
    let postReturned = false;
    const postError = errorMessage(() => {
      const value = applyClaudeSettings(postFile, generated, {
        transactionHooks: {
          afterCommit: () => {
            fs.renameSync(postFile, postDetached);
            fs.writeFileSync(postFile, replacement, { mode: 0o600 });
          },
        },
      });
      postReturned = Boolean(value);
    });
    check(
      "visible_path_swap_after_commit_never_reports_success",
      !postReturned &&
        postError === "CLAUDE_CONFIG_VISIBLE_IDENTITY_MISMATCH" &&
        fs.readFileSync(postFile, "utf8") === replacement &&
        JSON.parse(fs.readFileSync(postDetached, "utf8")).env.OTEL_LOGS_EXPORTER === "otlp" &&
        temps(postDir).length === 0,
      { error: postError, returned: postReturned },
    );

    const interruptDir = path.join(sandbox, "interruption");
    fs.mkdirSync(interruptDir, { mode: 0o700 });
    const interruptFile = path.join(interruptDir, "settings.json");
    writeJson(interruptFile, preimage);
    const interruptSource = fs.readFileSync(interruptFile, "utf8");
    const interruptError = errorMessage(() => applyClaudeSettings(interruptFile, generated, {
      transactionHooks: { afterBackup: () => { throw new Error(secretSentinel); } },
    }));
    check(
      "interruption_after_durable_backup_is_content_free_and_no_false_success",
      interruptError === "CLAUDE_CONFIG_TRANSACTION_ABORTED" &&
        !interruptError.includes(secretSentinel) &&
        !interruptError.includes(interruptFile) &&
        fs.readFileSync(interruptFile, "utf8") === interruptSource &&
        backups(interruptDir).length === 1 &&
        fs.readFileSync(path.join(interruptDir, backups(interruptDir)[0]!), "utf8") === interruptSource &&
        temps(interruptDir).length === 0,
      { error: interruptError, backups: backups(interruptDir).length },
    );

    const backupRaceDir = path.join(sandbox, "backup-hardlink-race");
    fs.mkdirSync(backupRaceDir, { mode: 0o700 });
    const backupRaceFile = path.join(backupRaceDir, "settings.json");
    const backupAlias = path.join(backupRaceDir, "backup-alias");
    writeJson(backupRaceFile, preimage);
    const backupRaceSource = fs.readFileSync(backupRaceFile, "utf8");
    const backupRaceError = errorMessage(() => applyClaudeSettings(backupRaceFile, generated, {
      transactionHooks: {
        afterBackup: () => {
          const backup = backups(backupRaceDir)[0];
          assert.ok(backup);
          fs.linkSync(path.join(backupRaceDir, backup), backupAlias);
        },
      },
    }));
    check(
      "backup_hardlink_change_after_durability_is_rejected_before_commit",
      backupRaceError === "CLAUDE_CONFIG_UNSAFE_LEAF_LINK_COUNT" &&
        fs.readFileSync(backupRaceFile, "utf8") === backupRaceSource &&
        backups(backupRaceDir).length === 0 &&
        fs.readFileSync(backupAlias, "utf8") === backupRaceSource &&
        temps(backupRaceDir).length === 0,
      { error: backupRaceError, backups: backups(backupRaceDir).length },
    );

    const preparedLinkDir = path.join(sandbox, "prepared-hardlink");
    fs.mkdirSync(preparedLinkDir, { mode: 0o700 });
    const preparedLinkFile = path.join(preparedLinkDir, "settings.json");
    const preparedAlias = path.join(preparedLinkDir, "prepared-alias");
    writeJson(preparedLinkFile, preimage);
    const preparedError = errorMessage(() => applyClaudeSettings(preparedLinkFile, generated, {
      transactionHooks: {
        afterPrepare: () => {
          const temp = temps(preparedLinkDir)[0];
          assert.ok(temp);
          fs.linkSync(path.join(preparedLinkDir, temp), preparedAlias);
        },
      },
    }));
    check(
      "prepared_file_hardlink_change_is_rejected_before_backup_or_commit",
      preparedError === "CLAUDE_CONFIG_UNSAFE_LEAF_LINK_COUNT" &&
        backups(preparedLinkDir).length === 0 &&
        fs.readFileSync(preparedLinkFile, "utf8") === `${JSON.stringify(preimage, null, 2)}\n`,
      { error: preparedError, backups: backups(preparedLinkDir).length },
    );
    fs.unlinkSync(preparedAlias);
    for (const temp of temps(preparedLinkDir)) fs.unlinkSync(path.join(preparedLinkDir, temp));

    const ancestorSwapRoot = path.join(sandbox, "ancestor-swap-after-prepare");
    const ancestorSwapDir = path.join(ancestorSwapRoot, "claude");
    const ancestorSwapMoved = path.join(ancestorSwapRoot, "claude-detached");
    fs.mkdirSync(ancestorSwapDir, { recursive: true, mode: 0o700 });
    const ancestorSwapFile = path.join(ancestorSwapDir, "settings.json");
    writeJson(ancestorSwapFile, preimage);
    const ancestorSwapSource = fs.readFileSync(ancestorSwapFile, "utf8");
    const ancestorReplacement = `${JSON.stringify({ replacement: secretSentinel })}\n`;
    const ancestorSwapError = errorMessage(() => applyClaudeSettings(ancestorSwapFile, generated, {
      transactionHooks: {
        afterPrepare: () => {
          fs.renameSync(ancestorSwapDir, ancestorSwapMoved);
          fs.mkdirSync(ancestorSwapDir, { mode: 0o700 });
          fs.writeFileSync(ancestorSwapFile, ancestorReplacement, { mode: 0o600 });
        },
      },
    }));
    check(
      "ancestor_swap_after_preparation_is_rejected_without_false_success",
      ancestorSwapError === "CLAUDE_CONFIG_ANCESTOR_CHANGED" &&
        fs.readFileSync(path.join(ancestorSwapMoved, "settings.json"), "utf8") === ancestorSwapSource &&
        fs.readFileSync(ancestorSwapFile, "utf8") === ancestorReplacement &&
        backups(ancestorSwapDir).length === 0 &&
        backups(ancestorSwapMoved).length === 0,
      { error: ancestorSwapError, replacementPreserved: true },
    );
    for (const temp of temps(ancestorSwapMoved)) {
      fs.unlinkSync(path.join(ancestorSwapMoved, temp));
    }

    const leafLinkDir = path.join(sandbox, "leaf-link");
    fs.mkdirSync(leafLinkDir, { mode: 0o700 });
    const leafTarget = path.join(leafLinkDir, "target.json");
    const leafLink = path.join(leafLinkDir, "settings.json");
    writeJson(leafTarget, preimage);
    fs.symlinkSync(leafTarget, leafLink);
    const leafLinkError = errorMessage(() => applyClaudeSettings(leafLink, generated));
    check(
      "leaf_symlink_is_rejected_without_target_mutation",
      leafLinkError === "CLAUDE_CONFIG_UNSAFE_LEAF_SYMLINK" &&
        fs.readFileSync(leafTarget, "utf8") === `${JSON.stringify(preimage, null, 2)}\n` &&
        backups(leafLinkDir).length === 0,
      { error: leafLinkError },
    );

    const hardlinkDir = path.join(sandbox, "leaf-hardlink");
    fs.mkdirSync(hardlinkDir, { mode: 0o700 });
    const hardlinkTarget = path.join(hardlinkDir, "target.json");
    const hardlinkFile = path.join(hardlinkDir, "settings.json");
    writeJson(hardlinkTarget, preimage);
    fs.linkSync(hardlinkTarget, hardlinkFile);
    const hardlinkError = errorMessage(() => applyClaudeSettings(hardlinkFile, generated));
    check(
      "leaf_hardlink_is_rejected_without_backup_or_mutation",
      hardlinkError === "CLAUDE_CONFIG_UNSAFE_LEAF_LINK_COUNT" &&
        fs.readFileSync(hardlinkTarget, "utf8") === `${JSON.stringify(preimage, null, 2)}\n` &&
        backups(hardlinkDir).length === 0,
      { error: hardlinkError },
    );

    const ancestorTarget = path.join(sandbox, "ancestor-target");
    const ancestorLink = path.join(sandbox, "ancestor-link");
    fs.mkdirSync(ancestorTarget, { mode: 0o700 });
    const ancestorTargetFile = path.join(ancestorTarget, "settings.json");
    writeJson(ancestorTargetFile, preimage);
    fs.symlinkSync(ancestorTarget, ancestorLink);
    const ancestorError = errorMessage(() =>
      applyClaudeSettings(path.join(ancestorLink, "settings.json"), generated));
    check(
      "ancestor_symlink_is_rejected_without_target_mutation",
      ancestorError === "CLAUDE_CONFIG_UNSAFE_ANCESTOR_SYMLINK" &&
        fs.readFileSync(ancestorTargetFile, "utf8") === `${JSON.stringify(preimage, null, 2)}\n` &&
        backups(ancestorTarget).length === 0,
      { error: ancestorError },
    );

    const writableAncestor = path.join(sandbox, "writable-ancestor");
    const privateChild = path.join(writableAncestor, "claude");
    fs.mkdirSync(privateChild, { recursive: true, mode: 0o700 });
    fs.chmodSync(writableAncestor, 0o777);
    const writableAncestorFile = path.join(privateChild, "settings.json");
    const writableAncestorError = errorMessage(() => applyClaudeSettings(writableAncestorFile, generated));
    check(
      "operator_owned_writable_intermediate_ancestor_fails_closed",
      writableAncestorError === "CLAUDE_CONFIG_UNSAFE_ANCESTOR_MODE" &&
        !fs.existsSync(writableAncestorFile) &&
        backups(privateChild).length === 0,
      { error: writableAncestorError, created: fs.existsSync(writableAncestorFile) },
    );
    fs.chmodSync(writableAncestor, 0o700);

    const unsafeModeDir = path.join(sandbox, "unsafe-mode");
    fs.mkdirSync(unsafeModeDir, { mode: 0o700 });
    const unsafeModeFile = path.join(unsafeModeDir, "settings.json");
    writeJson(unsafeModeFile, preimage, 0o666);
    fs.chmodSync(unsafeModeFile, 0o666);
    const unsafeModeError = errorMessage(() => applyClaudeSettings(unsafeModeFile, generated));
    check(
      "group_or_world_writable_leaf_fails_closed",
      unsafeModeError === "CLAUDE_CONFIG_UNSAFE_LEAF_MODE" && backups(unsafeModeDir).length === 0,
      { error: unsafeModeError },
    );

    const noWriteDir = path.join(sandbox, "no-write-parent");
    fs.mkdirSync(noWriteDir, { mode: 0o500 });
    const noWriteFile = path.join(noWriteDir, "settings.json");
    const noWriteError = errorMessage(() => applyClaudeSettings(noWriteFile, generated));
    check(
      "unwritable_parent_failure_is_symbolic_and_does_not_claim_success",
      /^CLAUDE_CONFIG_[A-Z_]+$/.test(noWriteError) &&
        !noWriteError.includes(noWriteFile) &&
        !fs.existsSync(noWriteFile) &&
        backups(noWriteDir).length === 0,
      { error: noWriteError, created: fs.existsSync(noWriteFile) },
    );
    fs.chmodSync(noWriteDir, 0o700);

    const malformedDir = path.join(sandbox, "malformed");
    fs.mkdirSync(malformedDir, { mode: 0o700 });
    const malformed = path.join(malformedDir, "settings.json");
    fs.writeFileSync(malformed, `{ "token": "${secretSentinel}",`);
    const malformedSource = fs.readFileSync(malformed, "utf8");
    const malformedError = errorMessage(() => applyClaudeSettings(malformed, generated));
    check(
      "malformed_json_fails_before_backup_with_content_free_error",
      malformedError === "CLAUDE_CONFIG_MALFORMED_JSON" &&
        !malformedError.includes(secretSentinel) &&
        !malformedError.includes(malformed) &&
        fs.readFileSync(malformed, "utf8") === malformedSource &&
        backups(malformedDir).length === 0,
      { error: malformedError },
    );

    const aliasCases: Array<[string, (document: Record<string, any>) => void, string]> = [
      ["env-case", (document) => { document.env = { otel_logs_exporter: "alias" }; }, "CLAUDE_CONFIG_ENV_KEY_ALIAS"],
      ["env-nfkc", (document) => { document.env = { "ＯTEL_LOGS_EXPORTER": "alias" }; }, "CLAUDE_CONFIG_ENV_KEY_ALIAS"],
      ["event-case", (document) => { document.hooks = { userpromptsubmit: [] }; }, "CLAUDE_CONFIG_HOOK_EVENT_ALIAS"],
      ["url-case", (document) => {
        document.hooks = { UserPromptSubmit: [{ hooks: [{ type: "http", url: "http://127.0.0.1:49130/HOOKS/CLAUDE-CODE" }] }] };
      }, "CLAUDE_CONFIG_HOOK_URL_ALIAS"],
      ["url-nfkc", (document) => {
        document.hooks = { UserPromptSubmit: [{ hooks: [{ type: "http", url: "http://127.0.0.1:49130/ｈｏｏｋｓ/claude-code" }] }] };
      }, "CLAUDE_CONFIG_HOOK_URL_ALIAS"],
    ];
    for (const [name, mutate, expectedError] of aliasCases) {
      const directory = path.join(sandbox, name);
      fs.mkdirSync(directory, { mode: 0o700 });
      const file = path.join(directory, "settings.json");
      const document: Record<string, any> = { preserve: secretSentinel };
      mutate(document);
      writeJson(file, document);
      const source = fs.readFileSync(file, "utf8");
      const message = errorMessage(() => applyClaudeSettings(file, generated));
      check(
        `${name}_alias_fails_closed_without_receipt_content`,
        message === expectedError &&
          !message.includes(secretSentinel) &&
          fs.readFileSync(file, "utf8") === source &&
          backups(directory).length === 0,
        { error: message },
      );
    }

    const freshRaceDir = path.join(sandbox, "fresh-race");
    fs.mkdirSync(freshRaceDir, { mode: 0o700 });
    const freshRace = path.join(freshRaceDir, "settings.json");
    const foreignFresh = `${JSON.stringify({ foreign: secretSentinel })}\n`;
    const freshRaceError = errorMessage(() => applyClaudeSettings(freshRace, generated, {
      transactionHooks: { beforeCommit: () => fs.writeFileSync(freshRace, foreignFresh, { mode: 0o600 }) },
    }));
    check(
      "fresh_destination_creation_race_never_clobbers_operator_file",
      freshRaceError === "CLAUDE_CONFIG_PATH_CHANGED" &&
        fs.readFileSync(freshRace, "utf8") === foreignFresh &&
        backups(freshRaceDir).length === 0 &&
        temps(freshRaceDir).length === 0,
      { error: freshRaceError, foreignPreserved: true },
    );

    check(
      "proof_never_touched_operator_home",
      !fs.existsSync(process.env.HOME!),
      { operatorHomeEntriesCreated: 0 },
    );
    console.log(JSON.stringify({ issue: 130, ok: true, checks }, null, 2));
  } finally {
    if (operatorHome === undefined) delete process.env.HOME;
    else process.env.HOME = operatorHome;
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main();
