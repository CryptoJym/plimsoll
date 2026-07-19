import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  installLaunchAgent,
  LAUNCH_AGENT_LABEL,
  launchAgentPlistPath,
  renderLaunchAgentPlist,
  uninstallLaunchAgent,
  type LaunchAgentOptions,
} from "../packages/collector-cli/src/launch-agent";

type Check = { name: string; passed: true; details: Record<string, unknown> };

const checks: Check[] = [];
const PERMISSION_MODE_MASK = 0o7777;
const SPECIAL_MANIFEST_MODES = [0o4600, 0o2600, 0o1600] as const;
let temporaryHomes = 0;

function check(name: string, condition: unknown, details: Record<string, unknown> = {}) {
  if (!condition) throw new Error(`${name} failed: ${JSON.stringify(details)}`);
  checks.push({ name, passed: true, details });
}

function sha256(value: string | Buffer) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function permissionMode(file: string) {
  return fs.lstatSync(file).mode & PERMISSION_MODE_MASK;
}

function formatMode(mode: number) {
  return (mode & PERMISSION_MODE_MASK).toString(8).padStart(4, "0");
}

function errorCode(action: () => unknown) {
  try {
    action();
    return "";
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
}

function treeDigest(root: string) {
  if (!fs.existsSync(root)) return "absent";
  const rows: string[] = [];
  const visit = (directory: string) => {
    for (const name of fs.readdirSync(directory).sort()) {
      const file = path.join(directory, name);
      const relative = path.relative(root, file);
      const stat = fs.lstatSync(file);
      if (stat.isSymbolicLink()) {
        rows.push(`${relative}|link|${stat.mode & PERMISSION_MODE_MASK}|${fs.readlinkSync(file)}`);
      } else if (stat.isDirectory()) {
        rows.push(`${relative}|dir|${stat.mode & PERMISSION_MODE_MASK}`);
        visit(file);
      } else if (stat.isFile()) {
        rows.push(`${relative}|file|${stat.mode & PERMISSION_MODE_MASK}|${stat.nlink}|${sha256(fs.readFileSync(file))}`);
      } else {
        rows.push(`${relative}|other|${stat.mode & PERMISSION_MODE_MASK}`);
      }
    }
  };
  visit(root);
  return sha256(rows.join("\n"));
}

function freshHome(sandbox: string, name: string) {
  const home = path.join(sandbox, name);
  fs.mkdirSync(home, { mode: 0o700 });
  temporaryHomes += 1;
  return home;
}

function parseJson(stdout: string) {
  const first = stdout.indexOf("{");
  if (first === -1) throw new Error("CLI emitted no JSON receipt.");
  return JSON.parse(stdout.slice(first)) as Record<string, unknown>;
}

function command(
  executable: string,
  args: string[],
  options: { cwd: string; env: NodeJS.ProcessEnv },
) {
  const result = spawnSync(executable, args, {
    cwd: options.cwd,
    env: options.env,
    encoding: "utf8",
    timeout: 20_000,
  });
  return {
    code: result.status,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
    error: result.error?.message ?? null,
  };
}

function hiddenFiles(directory: string, marker: string) {
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory).filter((name) => name.includes(marker)).sort();
}

function main() {
  check("proof_runs_on_exact_node_22", process.versions.node.split(".")[0] === "22", {
    nodeMajor: Number(process.versions.node.split(".")[0]),
  });
  const root = path.resolve(import.meta.dirname, "..");
  const tsx = path.join(root, "node_modules", "tsx", "dist", "cli.mjs");
  const cli = path.join(root, "packages", "collector-cli", "src", "cli.ts");
  const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-launch-agent-proof-"));
  const originalHome = process.env.HOME;
  const originalPlimsollHome = process.env.PLIMSOLL_HOME;
  const originalSecret = process.env.PLIMSOLL_SYNTHETIC_HOSTED_TOKEN;
  const privateSentinel = "SYNTHETIC_HOSTED_TOKEN_MUST_STAY_OUTSIDE_MANIFEST_AND_RECEIPTS";
  process.env.HOME = path.join(sandbox, "operator-home-must-stay-absent");
  process.env.PLIMSOLL_HOME = path.join(sandbox, "synthetic-collector-home");
  process.env.PLIMSOLL_SYNTHETIC_HOSTED_TOKEN = privateSentinel;
  const syntheticPnpm = path.join(sandbox, "runtime", "pnpm");
  const options = (homeDir: string, repo = path.join(sandbox, "source-a")): LaunchAgentOptions => ({
    homeDir,
    repoRoot: repo,
    pnpmPath: syntheticPnpm,
  });

  try {
    const previewHome = freshHome(sandbox, "preview-home");
    const beforePreview = treeDigest(previewHome);
    const preview = installLaunchAgent({ ...options(previewHome), dryRun: true });
    const previewJson = JSON.stringify(preview.receipt);
    check(
      "fresh_preview_is_byte_noop_and_symbolic_content_free",
      preview.receipt.status === "preview" &&
        preview.receipt.wouldChange === true &&
        treeDigest(previewHome) === beforePreview &&
        !fs.existsSync(path.join(previewHome, "Library")) &&
        !previewJson.includes(previewHome) &&
        !previewJson.includes(privateSentinel) &&
        !previewJson.includes("source-a"),
      { status: preview.receipt.status, homeTreeUnchanged: true },
    );

    const cliPreviewState = path.join(sandbox, "cli-preview-state-must-stay-absent");
    const cliPreview = command(process.execPath, [
      tsx,
      cli,
      "install-launch-agent",
      "--dev",
      "--repo-root",
      path.join(sandbox, "source-a"),
      "--pnpm",
      syntheticPnpm,
      "--dry-run",
      "--load",
    ], {
      cwd: root,
      env: {
        HOME: previewHome,
        PLIMSOLL_HOME: cliPreviewState,
        PATH: `${path.dirname(process.execPath)}${path.delimiter}/usr/bin${path.delimiter}/bin`,
      },
    });
    const cliPreviewReceipt = parseJson(cliPreview.stdout);
    check(
      "cli_preview_creates_no_config_manifest_or_launchctl_effect",
      cliPreview.code === 0 &&
        cliPreviewReceipt.status === "preview" &&
        cliPreviewReceipt.loadIntent === "would_load_after_visible_postcondition" &&
        treeDigest(previewHome) === beforePreview &&
        !fs.existsSync(cliPreviewState) &&
        !JSON.stringify(cliPreviewReceipt).includes(previewHome),
      { status: cliPreviewReceipt.status, collectorStateCreated: false, homeTreeUnchanged: true },
    );

    const fresh = installLaunchAgent(options(previewHome));
    const plist = fs.readFileSync(fresh.plistPath, "utf8");
    const plistStat = fs.lstatSync(fresh.plistPath);
    check(
      "fresh_apply_is_private_owned_atomic_and_allowlisted",
      fresh.receipt.status === "installed" &&
        (plistStat.mode & PERMISSION_MODE_MASK) === 0o600 &&
        plistStat.nlink === 1 &&
        (typeof process.getuid !== "function" || plistStat.uid === process.getuid()) &&
        (fs.statSync(path.dirname(fresh.plistPath)).mode & PERMISSION_MODE_MASK) === 0o700 &&
        plist.includes("<key>PLIMSOLL_COLLECTOR_DATA_MODE</key>") &&
        plist.includes("<string>metadata</string>") &&
        !plist.includes(privateSentinel) &&
        !plist.includes("PLIMSOLL_SYNTHETIC_HOSTED_TOKEN") &&
        hiddenFiles(path.dirname(fresh.plistPath), "plimsoll-prepared").length === 0 &&
        hiddenFiles(path.dirname(fresh.plistPath), "plimsoll-commit").length === 0 &&
        hiddenFiles(path.dirname(fresh.plistPath), "plimsoll-claim").length === 0,
      { mode: plistStat.mode & PERMISSION_MODE_MASK, links: plistStat.nlink, environmentKeys: fresh.receipt.environmentKeys },
    );

    const freshInode = plistStat.ino;
    const freshDigest = sha256(plist);
    const freshTree = treeDigest(previewHome);
    const repeated = installLaunchAgent(options(previewHome));
    check(
      "repeated_install_is_exact_noop_without_restart_or_backup_churn",
      repeated.receipt.status === "unchanged" &&
        repeated.receipt.changed === false &&
        fs.statSync(fresh.plistPath).ino === freshInode &&
        sha256(fs.readFileSync(fresh.plistPath)) === freshDigest &&
        treeDigest(previewHome) === freshTree &&
        hiddenFiles(path.dirname(fresh.plistPath), "plimsoll-rollback").length === 0,
      { status: repeated.receipt.status, inodePreserved: true, rollbackFiles: 0 },
    );

    const specialNoopResults = SPECIAL_MANIFEST_MODES.map((specialMode, index) => {
      const home = freshHome(sandbox, `special-noop-home-${index}`);
      const installed = installLaunchAgent(options(home, path.join(sandbox, `special-noop-${index}`)));
      const beforeBytes = fs.readFileSync(installed.plistPath);
      const beforeInode = fs.lstatSync(installed.plistPath).ino;
      fs.chmodSync(installed.plistPath, specialMode);
      const beforeAttempt = treeDigest(home);
      const error = errorCode(() => installLaunchAgent(
        options(home, path.join(sandbox, `special-noop-${index}`)),
      ));
      return {
        mode: formatMode(specialMode),
        error,
        modePreserved: permissionMode(installed.plistPath) === specialMode,
        inodePreserved: fs.lstatSync(installed.plistPath).ino === beforeInode,
        bytesPreserved: fs.readFileSync(installed.plistPath).equals(beforeBytes),
        treePreserved: treeDigest(home) === beforeAttempt,
      };
    });
    check(
      "special_permission_bits_never_take_byte_identical_unchanged_path",
      specialNoopResults.every((entry) =>
        entry.error === "LAUNCH_AGENT_UNSAFE_LEAF_MODE" &&
        entry.modePreserved &&
        entry.inodePreserved &&
        entry.bytesPreserved &&
        entry.treePreserved
      ),
      { modes: specialNoopResults.map(({ mode, error }) => ({ mode, error })) },
    );

    const specialPreparedResults = SPECIAL_MANIFEST_MODES.map((specialMode, index) => {
      const home = freshHome(sandbox, `special-prepared-home-${index}`);
      const plistPath = launchAgentPlistPath(home);
      const parent = path.dirname(plistPath);
      const error = errorCode(() => installLaunchAgent({
        ...options(home, path.join(sandbox, `special-prepared-${index}`)),
        transactionHooks: {
          afterPrepare: () => {
            const prepared = hiddenFiles(parent, "plimsoll-prepared");
            if (prepared.length !== 1) throw new Error("prepared fixture missing");
            fs.chmodSync(path.join(parent, prepared[0]!), specialMode);
          },
        },
      }));
      return {
        mode: formatMode(specialMode),
        error,
        manifestAbsent: !fs.existsSync(plistPath),
        preparedCleaned: hiddenFiles(parent, "plimsoll-prepared").length === 0,
      };
    });
    check(
      "special_permission_bits_on_prepared_object_fail_apply_and_leave_no_manifest",
      specialPreparedResults.every((entry) =>
        entry.error === "LAUNCH_AGENT_UNSAFE_LEAF_MODE" &&
        entry.manifestAbsent &&
        entry.preparedCleaned
      ),
      { modes: specialPreparedResults.map(({ mode, error }) => ({ mode, error })) },
    );

    const specialPostconditionResults = SPECIAL_MANIFEST_MODES.map((specialMode, index) => {
      const home = freshHome(sandbox, `special-postcondition-home-${index}`);
      const original = installLaunchAgent(options(home, path.join(sandbox, `special-postcondition-old-${index}`)));
      const originalBytes = fs.readFileSync(original.plistPath);
      const originalInode = fs.lstatSync(original.plistPath).ino;
      let returned = false;
      const error = errorCode(() => {
        const result = installLaunchAgent({
          ...options(home, path.join(sandbox, `special-postcondition-new-${index}`)),
          transactionHooks: {
            afterCommit: () => fs.chmodSync(original.plistPath, specialMode),
          },
        });
        returned = Boolean(result);
      });
      return {
        mode: formatMode(specialMode),
        error,
        returned,
        originalRestored: fs.readFileSync(original.plistPath).equals(originalBytes) &&
          fs.lstatSync(original.plistPath).ino === originalInode &&
          permissionMode(original.plistPath) === 0o600,
        claimsCleaned: hiddenFiles(path.dirname(original.plistPath), "plimsoll-claim").length === 0,
      };
    });
    check(
      "special_permission_bits_after_publication_fail_postcondition_and_restore_owned_preimage",
      specialPostconditionResults.every((entry) =>
        !entry.returned &&
        entry.error === "LAUNCH_AGENT_UNSAFE_LEAF_MODE" &&
        entry.originalRestored &&
        entry.claimsCleaned
      ),
      { modes: specialPostconditionResults.map(({ mode, error, returned }) => ({ mode, error, returned })) },
    );

    const previewUninstallTree = treeDigest(previewHome);
    const uninstallPreview = uninstallLaunchAgent({ homeDir: previewHome, dryRun: true });
    check(
      "uninstall_preview_is_byte_noop",
      uninstallPreview.receipt.status === "preview" &&
        uninstallPreview.receipt.wouldChange === true &&
        treeDigest(previewHome) === previewUninstallTree,
      { status: uninstallPreview.receipt.status, homeTreeUnchanged: true },
    );

    const symlinkHome = freshHome(sandbox, "symlink-home");
    const symlinkParent = path.dirname(launchAgentPlistPath(symlinkHome));
    fs.mkdirSync(symlinkParent, { recursive: true, mode: 0o700 });
    const symlinkTarget = path.join(sandbox, "symlink-target");
    fs.writeFileSync(symlinkTarget, "operator-symlink-target\n", { mode: 0o600 });
    fs.symlinkSync(symlinkTarget, launchAgentPlistPath(symlinkHome));
    const symlinkError = errorCode(() => installLaunchAgent(options(symlinkHome)));
    check(
      "destination_symlink_is_rejected_without_target_mutation",
      symlinkError === "LAUNCH_AGENT_UNSAFE_LEAF_SYMLINK" &&
        fs.readFileSync(symlinkTarget, "utf8") === "operator-symlink-target\n" &&
        fs.lstatSync(launchAgentPlistPath(symlinkHome)).isSymbolicLink(),
      { error: symlinkError, targetPreserved: true },
    );

    const ancestorLinkHome = freshHome(sandbox, "ancestor-link-home");
    const ancestorLibrary = path.join(ancestorLinkHome, "Library");
    const externalLaunchAgents = path.join(sandbox, "external-launch-agents");
    fs.mkdirSync(ancestorLibrary, { mode: 0o700 });
    fs.mkdirSync(externalLaunchAgents, { mode: 0o700 });
    fs.symlinkSync(externalLaunchAgents, path.join(ancestorLibrary, "LaunchAgents"));
    const ancestorLinkError = errorCode(() => installLaunchAgent(options(ancestorLinkHome)));
    check(
      "launchagents_ancestor_symlink_is_rejected_without_external_write",
      ancestorLinkError === "LAUNCH_AGENT_UNSAFE_ANCESTOR_SYMLINK" &&
        fs.readdirSync(externalLaunchAgents).length === 0,
      { error: ancestorLinkError, externalEntries: 0 },
    );

    const hardlinkHome = freshHome(sandbox, "hardlink-home");
    const hardlinkParent = path.dirname(launchAgentPlistPath(hardlinkHome));
    fs.mkdirSync(hardlinkParent, { recursive: true, mode: 0o700 });
    const hardlinkTarget = path.join(hardlinkParent, "operator-target");
    fs.writeFileSync(hardlinkTarget, "operator-hardlink-target\n", { mode: 0o600 });
    fs.linkSync(hardlinkTarget, launchAgentPlistPath(hardlinkHome));
    const hardlinkError = errorCode(() => installLaunchAgent(options(hardlinkHome)));
    check(
      "destination_hardlink_is_rejected_without_mutation",
      hardlinkError === "LAUNCH_AGENT_UNSAFE_LEAF_LINK_COUNT" &&
        fs.readFileSync(hardlinkTarget, "utf8") === "operator-hardlink-target\n" &&
        fs.statSync(hardlinkTarget).nlink === 2,
      { error: hardlinkError, links: fs.statSync(hardlinkTarget).nlink },
    );

    const modeHome = freshHome(sandbox, "unsafe-mode-home");
    const modeParent = path.dirname(launchAgentPlistPath(modeHome));
    fs.mkdirSync(modeParent, { recursive: true, mode: 0o700 });
    fs.writeFileSync(launchAgentPlistPath(modeHome), "operator-mode-target\n", { mode: 0o660 });
    fs.chmodSync(launchAgentPlistPath(modeHome), 0o660);
    const modeError = errorCode(() => installLaunchAgent(options(modeHome)));
    check(
      "unsafe_destination_mode_is_rejected_without_repermissioning",
      modeError === "LAUNCH_AGENT_UNSAFE_LEAF_MODE" &&
        permissionMode(launchAgentPlistPath(modeHome)) === 0o660,
      { error: modeError, mode: permissionMode(launchAgentPlistPath(modeHome)) },
    );

    const ownerHome = freshHome(sandbox, "unsafe-owner-home");
    const ownerInstall = installLaunchAgent(options(ownerHome));
    const ownerBytes = fs.readFileSync(ownerInstall.plistPath);
    const originalLstat = fs.lstatSync;
    const mutableFs = fs as unknown as { lstatSync: typeof fs.lstatSync };
    let ownerError = "";
    try {
      mutableFs.lstatSync = ((candidate: fs.PathLike, statOptions?: unknown) => {
        const stat = originalLstat(candidate, statOptions as never);
        if (String(candidate) !== ownerInstall.plistPath) return stat;
        return new Proxy(stat, {
          get(target, property) {
            if (property === "uid") return target.uid + 1;
            const value = Reflect.get(target, property, target);
            return typeof value === "function" ? value.bind(target) : value;
          },
        });
      }) as typeof fs.lstatSync;
      ownerError = errorCode(() => installLaunchAgent(options(ownerHome)));
    } finally {
      mutableFs.lstatSync = originalLstat;
    }
    check(
      "foreign_owned_destination_is_rejected_without_mutation",
      ownerError === "LAUNCH_AGENT_UNSAFE_LEAF_OWNER" &&
        fs.readFileSync(ownerInstall.plistPath).equals(ownerBytes),
      { error: ownerError, bytesPreserved: true },
    );

    const parentModeHome = freshHome(sandbox, "unsafe-parent-mode-home");
    const unsafeParent = path.dirname(launchAgentPlistPath(parentModeHome));
    fs.mkdirSync(unsafeParent, { recursive: true, mode: 0o700 });
    fs.chmodSync(unsafeParent, 0o777);
    const parentModeError = errorCode(() => installLaunchAgent(options(parentModeHome)));
    check(
      "unsafe_launchagents_parent_mode_is_rejected",
      parentModeError === "LAUNCH_AGENT_UNSAFE_ANCESTOR_MODE" &&
        !fs.existsSync(launchAgentPlistPath(parentModeHome)),
      { error: parentModeError, manifestCreated: false },
    );

    const aliasHome = freshHome(sandbox, "plist-alias-home");
    const aliasParent = path.dirname(launchAgentPlistPath(aliasHome));
    fs.mkdirSync(aliasParent, { recursive: true, mode: 0o700 });
    const canonical = renderLaunchAgentPlist(options(aliasHome));
    const alias = canonical.replace("<key>Label</key>", "<key>Ｌabel</key>");
    fs.writeFileSync(launchAgentPlistPath(aliasHome), alias, { mode: 0o600 });
    const aliasError = errorCode(() => installLaunchAgent(options(aliasHome)));
    check(
      "unicode_plist_key_alias_is_never_adopted_or_overwritten",
      aliasError === "LAUNCH_AGENT_PLIST_KEYS_UNEXPECTED" &&
        fs.readFileSync(launchAgentPlistPath(aliasHome), "utf8") === alias,
      { error: aliasError, bytesPreserved: true },
    );

    const entityHome = freshHome(sandbox, "plist-entity-home");
    const entityParent = path.dirname(launchAgentPlistPath(entityHome));
    fs.mkdirSync(entityParent, { recursive: true, mode: 0o700 });
    const entity = canonical.replace(
      '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
      '<!DOCTYPE plist [<!ENTITY alias "com.plimsoll.collector">]>',
    );
    fs.writeFileSync(launchAgentPlistPath(entityHome), entity, { mode: 0o600 });
    const entityError = errorCode(() => installLaunchAgent(options(entityHome)));
    check(
      "plist_entity_alias_is_rejected_without_expansion_or_mutation",
      entityError === "LAUNCH_AGENT_PLIST_ALIAS_OR_ENTITY" &&
        fs.readFileSync(launchAgentPlistPath(entityHome), "utf8") === entity,
      { error: entityError, bytesPreserved: true },
    );

    const unrelatedHome = freshHome(sandbox, "unrelated-home");
    const unrelatedParent = path.dirname(launchAgentPlistPath(unrelatedHome));
    fs.mkdirSync(unrelatedParent, { recursive: true, mode: 0o700 });
    const unrelated = "operator-owned-unrelated-launch-agent\n";
    fs.writeFileSync(launchAgentPlistPath(unrelatedHome), unrelated, { mode: 0o600 });
    const unrelatedInstallError = errorCode(() => installLaunchAgent(options(unrelatedHome)));
    const unrelatedUninstallError = errorCode(() => uninstallLaunchAgent({ homeDir: unrelatedHome }));
    check(
      "unrelated_manifest_is_never_adopted_overwritten_or_uninstalled",
      unrelatedInstallError === "LAUNCH_AGENT_PLIST_PREAMBLE_INVALID" &&
        unrelatedUninstallError === "LAUNCH_AGENT_PLIST_PREAMBLE_INVALID" &&
        fs.readFileSync(launchAgentPlistPath(unrelatedHome), "utf8") === unrelated,
      { installError: unrelatedInstallError, uninstallError: unrelatedUninstallError, bytesPreserved: true },
    );

    const replacementHome = freshHome(sandbox, "replacement-home");
    const first = installLaunchAgent(options(replacementHome, path.join(sandbox, "source-old")));
    fs.chmodSync(first.plistPath, 0o644);
    const preimage = fs.readFileSync(first.plistPath);
    const preimageInode = fs.statSync(first.plistPath).ino;
    const replacement = installLaunchAgent(options(replacementHome, path.join(sandbox, "source-new")));
    const rollbackReceipt = replacement.rollbackFiles
      ? JSON.parse(fs.readFileSync(replacement.rollbackFiles.receiptPath, "utf8")) as Record<string, unknown>
      : null;
    check(
      "owned_replacement_preserves_exact_private_preimage_and_receipt",
      replacement.receipt.status === "installed" &&
        replacement.receipt.rollback?.available === true &&
        Boolean(replacement.rollbackFiles) &&
        fs.readFileSync(replacement.rollbackFiles!.preimagePath).equals(preimage) &&
        permissionMode(replacement.rollbackFiles!.preimagePath) === 0o600 &&
        permissionMode(replacement.rollbackFiles!.receiptPath) === 0o600 &&
        rollbackReceipt?.preimageDigest === sha256(preimage) &&
        rollbackReceipt?.preimageMode === "0644" &&
        fs.statSync(first.plistPath).ino !== preimageInode &&
        permissionMode(first.plistPath) === 0o600,
      {
        preimageExact: true,
        preimageMode: replacement.receipt.rollback?.preimageMode,
        installedMode: permissionMode(first.plistPath),
      },
    );

    const rollbackSwapHome = freshHome(sandbox, "rollback-receipt-swap-home");
    const rollbackSwapFirst = installLaunchAgent(options(rollbackSwapHome, path.join(sandbox, "rollback-old")));
    const rollbackSwapBytes = fs.readFileSync(rollbackSwapFirst.plistPath);
    let mutatedReceipt = "";
    const rollbackSwapError = errorCode(() => installLaunchAgent({
      ...options(rollbackSwapHome, path.join(sandbox, "rollback-new")),
      transactionHooks: {
        afterRollback: () => {
          const directory = path.dirname(rollbackSwapFirst.plistPath);
          const receiptName = hiddenFiles(directory, "plimsoll-rollback")
            .find((name) => name.endsWith(".receipt.json"))!;
          mutatedReceipt = path.join(directory, receiptName);
          fs.appendFileSync(mutatedReceipt, "operator-concurrent-receipt-change\n");
        },
      },
    }));
    check(
      "rollback_receipt_same_inode_change_blocks_commit",
      rollbackSwapError === "LAUNCH_AGENT_VISIBLE_IDENTITY_MISMATCH" &&
        fs.readFileSync(rollbackSwapFirst.plistPath).equals(rollbackSwapBytes) &&
        fs.readFileSync(mutatedReceipt, "utf8").endsWith("operator-concurrent-receipt-change\n"),
      { error: rollbackSwapError, manifestPreserved: true, falseSuccess: false },
    );

    const appendHome = freshHome(sandbox, "concurrent-append-home");
    const appendFirst = installLaunchAgent(options(appendHome, path.join(sandbox, "append-old")));
    const appendPreimage = fs.readFileSync(appendFirst.plistPath, "utf8");
    const append = "\n";
    const appendError = errorCode(() => installLaunchAgent({
      ...options(appendHome, path.join(sandbox, "append-new")),
      transactionHooks: { afterRollback: () => fs.appendFileSync(appendFirst.plistPath, append) },
    }));
    check(
      "same_inode_content_change_before_commit_is_preserved_and_never_false_success",
      appendError === "LAUNCH_AGENT_VISIBLE_IDENTITY_MISMATCH" &&
        fs.readFileSync(appendFirst.plistPath, "utf8") === appendPreimage + append &&
        hiddenFiles(path.dirname(appendFirst.plistPath), "plimsoll-rollback").length === 2,
      { error: appendError, replacementPreserved: true, durableRollbackFiles: 2 },
    );

    const leafSwapHome = freshHome(sandbox, "leaf-swap-home");
    const leafSwapFirst = installLaunchAgent(options(leafSwapHome, path.join(sandbox, "leaf-old")));
    const leafDetached = path.join(path.dirname(leafSwapFirst.plistPath), "detached.plist");
    const leafReplacement = "operator-concurrent-replacement\n";
    const leafSwapError = errorCode(() => installLaunchAgent({
      ...options(leafSwapHome, path.join(sandbox, "leaf-new")),
      transactionHooks: {
        beforeCommit: () => {
          fs.renameSync(leafSwapFirst.plistPath, leafDetached);
          fs.writeFileSync(leafSwapFirst.plistPath, leafReplacement, { mode: 0o600 });
        },
      },
    }));
    check(
      "leaf_swap_before_commit_preserves_concurrent_object",
      leafSwapError === "LAUNCH_AGENT_VISIBLE_IDENTITY_MISMATCH" &&
        fs.readFileSync(leafSwapFirst.plistPath, "utf8") === leafReplacement &&
        fs.readFileSync(leafDetached, "utf8").includes("leaf-old"),
      { error: leafSwapError, concurrentObjectPreserved: true },
    );

    const ancestorSwapHome = freshHome(sandbox, "ancestor-swap-home");
    const ancestorDir = path.dirname(launchAgentPlistPath(ancestorSwapHome));
    const ancestorMoved = path.join(ancestorSwapHome, "LaunchAgents-detached");
    const ancestorError = errorCode(() => installLaunchAgent({
      ...options(ancestorSwapHome),
      transactionHooks: {
        afterPrepare: () => {
          fs.renameSync(ancestorDir, ancestorMoved);
          fs.mkdirSync(ancestorDir, { mode: 0o700 });
        },
      },
    }));
    check(
      "ancestor_swap_after_prepare_is_rejected_without_visible_manifest",
      ancestorError === "LAUNCH_AGENT_ANCESTOR_CHANGED" &&
        !fs.existsSync(launchAgentPlistPath(ancestorSwapHome)) &&
        !fs.existsSync(path.join(ancestorMoved, `${LAUNCH_AGENT_LABEL}.plist`)),
      { error: ancestorError, falseSuccess: false },
    );

    const preparedLinkHome = freshHome(sandbox, "prepared-link-home");
    let preparedAlias = "";
    const preparedLinkError = errorCode(() => installLaunchAgent({
      ...options(preparedLinkHome),
      transactionHooks: {
        afterPrepare: () => {
          const directory = path.dirname(launchAgentPlistPath(preparedLinkHome));
          const preparedName = hiddenFiles(directory, "plimsoll-prepared")[0]!;
          preparedAlias = path.join(directory, "prepared-hardlink-alias");
          fs.linkSync(path.join(directory, preparedName), preparedAlias);
        },
      },
    }));
    check(
      "prepared_object_hardlink_is_rejected_before_publication",
      preparedLinkError === "LAUNCH_AGENT_UNSAFE_LEAF_LINK_COUNT" &&
        !fs.existsSync(launchAgentPlistPath(preparedLinkHome)) &&
        fs.existsSync(preparedAlias),
      { error: preparedLinkError, manifestCreated: false },
    );

    const parentCreateHome = freshHome(sandbox, "parent-create-swap-home");
    const createdParent = path.dirname(launchAgentPlistPath(parentCreateHome));
    const createdMoved = path.join(parentCreateHome, "LaunchAgents-created");
    const parentCreateError = errorCode(() => installLaunchAgent({
      ...options(parentCreateHome),
      transactionHooks: {
        afterParentCreate: () => {
          fs.renameSync(createdParent, createdMoved);
          fs.mkdirSync(createdParent, { mode: 0o700 });
        },
      },
    }));
    check(
      "fresh_parent_swap_after_creation_is_rejected_before_prepare",
      parentCreateError === "LAUNCH_AGENT_ANCESTOR_CHANGED" &&
        !fs.existsSync(launchAgentPlistPath(parentCreateHome)) &&
        hiddenFiles(createdMoved, "plimsoll-prepared").length === 0,
      { error: parentCreateError, manifestCreated: false },
    );

    const commitWindowHome = freshHome(sandbox, "commit-window-home");
    const commitFirst = installLaunchAgent(options(commitWindowHome, path.join(sandbox, "commit-old")));
    const commitDetached = path.join(path.dirname(commitFirst.plistPath), "commit-original-detached");
    const commitReplacement = "operator-commit-window-replacement\n";
    const originalRename = fs.renameSync;
    let commitInjected = false;
    let commitWindowError = "";
    try {
      fs.renameSync = ((oldPath: fs.PathLike, newPath: fs.PathLike) => {
        if (
          !commitInjected &&
          String(oldPath) === commitFirst.plistPath &&
          String(newPath).includes("plimsoll-claim")
        ) {
          commitInjected = true;
          originalRename(commitFirst.plistPath, commitDetached);
          fs.writeFileSync(commitFirst.plistPath, commitReplacement, { mode: 0o600 });
        }
        return originalRename(oldPath, newPath);
      }) as typeof fs.renameSync;
      commitWindowError = errorCode(() => installLaunchAgent(
        options(commitWindowHome, path.join(sandbox, "commit-new")),
      ));
    } finally {
      fs.renameSync = originalRename;
    }
    check(
      "commit_window_replacement_is_restored_and_never_clobbered",
      commitInjected &&
        commitWindowError === "LAUNCH_AGENT_COMMIT_CLAIM_MISMATCH" &&
        fs.readFileSync(commitFirst.plistPath, "utf8") === commitReplacement &&
        fs.readFileSync(commitDetached, "utf8").includes("commit-old"),
      { error: commitWindowError, concurrentObjectPreserved: true },
    );

    const postCommitHome = freshHome(sandbox, "post-commit-home");
    const postFirst = installLaunchAgent(options(postCommitHome, path.join(sandbox, "post-old")));
    const postDetached = path.join(path.dirname(postFirst.plistPath), "post-committed-detached");
    const postReplacement = "operator-post-commit-replacement\n";
    let postReturned = false;
    const postCommitError = errorCode(() => {
      const result = installLaunchAgent({
        ...options(postCommitHome, path.join(sandbox, "post-new")),
        transactionHooks: {
          afterCommit: () => {
            fs.renameSync(postFirst.plistPath, postDetached);
            fs.writeFileSync(postFirst.plistPath, postReplacement, { mode: 0o600 });
          },
        },
      });
      postReturned = Boolean(result);
    });
    check(
      "post_commit_swap_never_reports_success_or_clobbers_replacement",
      !postReturned &&
        postCommitError === "LAUNCH_AGENT_VISIBLE_IDENTITY_MISMATCH" &&
        fs.readFileSync(postFirst.plistPath, "utf8") === postReplacement &&
        fs.readFileSync(postDetached, "utf8").includes("post-new"),
      { error: postCommitError, returned: postReturned, concurrentObjectPreserved: true },
    );

    const loadHome = freshHome(sandbox, "load-home");
    installLaunchAgent(options(loadHome, path.join(sandbox, "load-source")));
    const stubDir = path.join(sandbox, "stub-bin");
    fs.mkdirSync(stubDir, { mode: 0o700 });
    const launchctlLog = path.join(sandbox, "launchctl.log");
    const launchctlState = path.join(sandbox, "launchctl.state");
    const launchctl = path.join(stubDir, "launchctl");
    fs.writeFileSync(launchctl, `#!/bin/sh
printf '%s\\n' "$1" >> "$PLIMSOLL_TEST_LAUNCHCTL_LOG"
case "$1" in
  print) test -f "$PLIMSOLL_TEST_LAUNCHCTL_STATE" && ! grep -qx 'booted_out' "$PLIMSOLL_TEST_LAUNCHCTL_STATE" ;;
  bootstrap)
    if [ "\${PLIMSOLL_TEST_LAUNCHCTL_FAIL:-0}" = "1" ]; then exit 42; fi
    : > "$PLIMSOLL_TEST_LAUNCHCTL_STATE"
    if [ -n "\${PLIMSOLL_TEST_SWAP_MANIFEST:-}" ]; then
      printf '%s\n' 'operator-bootstrap-window-replacement' > "$PLIMSOLL_TEST_SWAP_MANIFEST"
      chmod 600 "$PLIMSOLL_TEST_SWAP_MANIFEST"
    fi
    exit 0
    ;;
  bootout)
    if [ "\${PLIMSOLL_TEST_BOOTOUT_FAIL:-0}" = "1" ]; then exit 43; fi
    printf '%s\n' 'booted_out' > "$PLIMSOLL_TEST_LAUNCHCTL_STATE"
    exit 0
    ;;
  *) exit 64 ;;
esac
`, { mode: 0o700 });
    const cliHome = path.join(sandbox, "cli-plimsoll-home");
    const baseEnv = {
      HOME: loadHome,
      PLIMSOLL_HOME: cliHome,
      PATH: `${stubDir}${path.delimiter}${path.dirname(process.execPath)}${path.delimiter}/usr/bin${path.delimiter}/bin`,
      PLIMSOLL_TEST_LAUNCHCTL_LOG: launchctlLog,
      PLIMSOLL_TEST_LAUNCHCTL_STATE: launchctlState,
    };
    const failedLoad = command(process.execPath, [tsx, cli, "load-launch-agent"], {
      cwd: root,
      env: { ...baseEnv, PLIMSOLL_TEST_LAUNCHCTL_FAIL: "1" },
    });
    const failedLoadReceipt = parseJson(failedLoad.stdout);
    check(
      "launchctl_load_failure_is_literal_and_never_claims_active",
      failedLoad.code === 42 &&
        failedLoadReceipt.loaded === false &&
        failedLoadReceipt.status === "launchctl_failed" &&
        !fs.existsSync(launchctlState) &&
        !fs.existsSync(cliHome),
      { exitCode: failedLoad.code, status: failedLoadReceipt.status, activeClaimed: false },
    );

    fs.rmSync(launchctlLog, { force: true });
    const firstLoad = command(process.execPath, [tsx, cli, "load-launch-agent"], {
      cwd: root,
      env: baseEnv,
    });
    const secondLoad = command(process.execPath, [tsx, cli, "load-launch-agent"], {
      cwd: root,
      env: baseEnv,
    });
    const firstLoadReceipt = parseJson(firstLoad.stdout);
    const secondLoadReceipt = parseJson(secondLoad.stdout);
    const launchctlCalls = fs.readFileSync(launchctlLog, "utf8").trim().split("\n");
    check(
      "repeated_load_converges_without_second_bootstrap",
      firstLoad.code === 0 &&
        firstLoadReceipt.loaded === true &&
        firstLoadReceipt.status === "bootstrap_succeeded" &&
        secondLoad.code === 0 &&
        secondLoadReceipt.loaded === true &&
        secondLoadReceipt.status === "already_loaded" &&
        launchctlCalls.filter((entry) => entry === "bootstrap").length === 1 &&
        launchctlCalls.filter((entry) => entry === "print").length === 2,
      {
        firstStatus: firstLoadReceipt.status,
        secondStatus: secondLoadReceipt.status,
        printCalls: launchctlCalls.filter((entry) => entry === "print").length,
        bootstrapCalls: launchctlCalls.filter((entry) => entry === "bootstrap").length,
      },
    );

    const swapLoadHome = freshHome(sandbox, "load-bootstrap-swap-home");
    const swapLoadInstall = installLaunchAgent(options(swapLoadHome, path.join(sandbox, "load-bootstrap-swap")));
    const swapLaunchctlLog = path.join(sandbox, "launchctl-swap.log");
    const swapLaunchctlState = path.join(sandbox, "launchctl-swap.state");
    const swapLoad = command(process.execPath, [tsx, cli, "load-launch-agent"], {
      cwd: root,
      env: {
        ...baseEnv,
        HOME: swapLoadHome,
        PLIMSOLL_TEST_LAUNCHCTL_LOG: swapLaunchctlLog,
        PLIMSOLL_TEST_LAUNCHCTL_STATE: swapLaunchctlState,
        PLIMSOLL_TEST_SWAP_MANIFEST: swapLoadInstall.plistPath,
      },
    });
    const swapLoadReceipt = parseJson(swapLoad.stdout);
    const swapCleanup = swapLoadReceipt.cleanup as Record<string, unknown>;
    const swapCalls = fs.readFileSync(swapLaunchctlLog, "utf8").trim().split("\n");
    check(
      "bootstrap_window_manifest_swap_fails_and_bootout_cleanup_is_proved",
      swapLoad.code === 1 &&
        swapLoadReceipt.loaded === false &&
        swapLoadReceipt.status === "post_bootstrap_manifest_changed" &&
        typeof swapLoadReceipt.manifestDigest === "string" &&
        typeof swapLoadReceipt.manifestIdentityDigest === "string" &&
        swapLoadReceipt.postBootstrapManifestDigest === null &&
        swapLoadReceipt.postBootstrapManifestIdentityDigest === null &&
        swapCleanup.bootoutAttempted === true &&
        swapCleanup.bootoutSucceeded === true &&
        swapCleanup.labelReportedAfterBootout === false &&
        swapCleanup.labelQueryExitCode === 1 &&
        swapCleanup.labelQueryErrorCode === null &&
        swapCleanup.labelState === "not_reported" &&
        swapCleanup.status === "bootout_succeeded_label_not_reported" &&
        fs.readFileSync(swapLaunchctlState, "utf8").trim() === "booted_out" &&
        fs.readFileSync(swapLoadInstall.plistPath, "utf8").trim() === "operator-bootstrap-window-replacement" &&
        swapCalls.join(",") === "print,bootstrap,bootout,print",
      {
        exitCode: swapLoad.code,
        status: swapLoadReceipt.status,
        cleanupStatus: swapCleanup.status,
        loadedClaimed: swapLoadReceipt.loaded,
      },
    );

    const failedCleanupHome = freshHome(sandbox, "load-bootstrap-swap-cleanup-fail-home");
    const failedCleanupInstall = installLaunchAgent(
      options(failedCleanupHome, path.join(sandbox, "load-bootstrap-swap-cleanup-fail")),
    );
    const failedCleanupLog = path.join(sandbox, "launchctl-swap-cleanup-fail.log");
    const failedCleanupState = path.join(sandbox, "launchctl-swap-cleanup-fail.state");
    const failedCleanupLoad = command(process.execPath, [tsx, cli, "load-launch-agent"], {
      cwd: root,
      env: {
        ...baseEnv,
        HOME: failedCleanupHome,
        PLIMSOLL_TEST_LAUNCHCTL_LOG: failedCleanupLog,
        PLIMSOLL_TEST_LAUNCHCTL_STATE: failedCleanupState,
        PLIMSOLL_TEST_SWAP_MANIFEST: failedCleanupInstall.plistPath,
        PLIMSOLL_TEST_BOOTOUT_FAIL: "1",
      },
    });
    const failedCleanupReceipt = parseJson(failedCleanupLoad.stdout);
    const failedCleanup = failedCleanupReceipt.cleanup as Record<string, unknown>;
    check(
      "bootstrap_window_cleanup_failure_preserves_literal_bootout_truth",
      failedCleanupLoad.code === 43 &&
        failedCleanupReceipt.loaded === false &&
        failedCleanupReceipt.status === "post_bootstrap_manifest_changed" &&
        failedCleanup.bootoutAttempted === true &&
        failedCleanup.bootoutSucceeded === false &&
        failedCleanup.labelReportedAfterBootout === true &&
        failedCleanup.labelQueryExitCode === 0 &&
        failedCleanup.labelState === "reported" &&
        failedCleanup.status === "bootout_failed" &&
        fs.existsSync(failedCleanupState) &&
        fs.readFileSync(failedCleanupInstall.plistPath, "utf8").trim() === "operator-bootstrap-window-replacement",
      {
        exitCode: failedCleanupLoad.code,
        status: failedCleanupReceipt.status,
        cleanupStatus: failedCleanup.status,
        labelReportedAfterBootout: failedCleanup.labelReportedAfterBootout,
      },
    );

    const uninstallSwapHome = freshHome(sandbox, "uninstall-swap-home");
    const uninstallSwapInstall = installLaunchAgent(options(uninstallSwapHome));
    const uninstallDetached = path.join(path.dirname(uninstallSwapInstall.plistPath), "uninstall-detached");
    const uninstallReplacement = "operator-uninstall-replacement\n";
    const uninstallSwapError = errorCode(() => uninstallLaunchAgent({
      homeDir: uninstallSwapHome,
      transactionHooks: {
        beforeCommit: () => {
          fs.renameSync(uninstallSwapInstall.plistPath, uninstallDetached);
          fs.writeFileSync(uninstallSwapInstall.plistPath, uninstallReplacement, { mode: 0o600 });
        },
      },
    }));
    check(
      "uninstall_path_swap_preserves_concurrent_object_and_fails_closed",
      uninstallSwapError === "LAUNCH_AGENT_VISIBLE_IDENTITY_MISMATCH" &&
        fs.readFileSync(uninstallSwapInstall.plistPath, "utf8") === uninstallReplacement &&
        fs.readFileSync(uninstallDetached, "utf8").includes(LAUNCH_AGENT_LABEL),
      { error: uninstallSwapError, concurrentObjectPreserved: true },
    );

    const removed = uninstallLaunchAgent({ homeDir: previewHome });
    check(
      "owned_uninstall_uses_atomic_claim_and_visible_absence_postcondition",
      removed.receipt.status === "removed" &&
        removed.receipt.changed === true &&
        !fs.existsSync(fresh.plistPath) &&
        hiddenFiles(path.dirname(fresh.plistPath), "plimsoll-remove").length === 0,
      { status: removed.receipt.status, visibleManifestAbsent: true },
    );

    const serializedChecks = JSON.stringify(checks);
    check(
      "proof_receipt_contains_no_private_or_credential_sentinel",
      !serializedChecks.includes(privateSentinel) &&
        !serializedChecks.includes(sandbox) &&
        !serializedChecks.includes("PLIMSOLL_SYNTHETIC_HOSTED_TOKEN"),
      { privatePaths: 0, credentialValues: 0, credentialNames: 0 },
    );

    const receipt = {
      schema: "plimsoll.launch-agent-transaction-proof.v1",
      issue: 132,
      ok: checks.every((entry) => entry.passed),
      node: { major: 22, version: process.versions.node },
      isolation: {
        temporaryHomes,
        realLaunchAgentsTouched: 0,
        realLaunchctlCalls: 0,
        credentialOperations: 0,
      },
      checks,
    };
    const evidence = path.join(root, "evidence", "launch-agent-transaction-proof.json");
    fs.mkdirSync(path.dirname(evidence), { recursive: true });
    fs.writeFileSync(evidence, `${JSON.stringify(receipt, null, 2)}\n`);
    console.log(JSON.stringify(receipt, null, 2));
  } finally {
    if (originalHome === undefined) delete process.env.HOME;
    else process.env.HOME = originalHome;
    if (originalPlimsollHome === undefined) delete process.env.PLIMSOLL_HOME;
    else process.env.PLIMSOLL_HOME = originalPlimsollHome;
    if (originalSecret === undefined) delete process.env.PLIMSOLL_SYNTHETIC_HOSTED_TOKEN;
    else process.env.PLIMSOLL_SYNTHETIC_HOSTED_TOKEN = originalSecret;
    fs.rmSync(sandbox, { recursive: true, force: true });
  }
}

main();
