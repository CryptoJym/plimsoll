/** Independent PR #428 privacy and machine-label regression witnesses. */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  captureRootsDeriveFrom,
  deriveCaptureRootIdentity,
  discoverCaptureRootCandidates,
  resolveCaptureRootMachineLabel,
} from "../packages/collector-cli/src/capture-root-inventory";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { installLaunchAgent, inspectLaunchAgentOwnership, launchAgentOwnerEditedKeys } from
  "../packages/collector-cli/src/launch-agent";

const root = fs.mkdtempSync(path.join(process.cwd(), "pr428-review-"));
const failures: string[] = [];
function check(name: string, actual: unknown, expected: unknown) {
  try {
    assert.deepEqual(actual, expected);
    console.log(`PASS ${name}`);
  } catch {
    failures.push(name);
    console.log(`FAIL ${name}: actual=${JSON.stringify(actual)} expected=${JSON.stringify(expected)}`);
  }
}

try {
  const home = path.join(root, "home");
  const unrelated = path.join(home, "Documents", "private-sessions");
  const linked = path.join(home, ".clientai/studio/borg/conductors/primary/profile/sessions");
  fs.mkdirSync(unrelated, { recursive: true });
  fs.mkdirSync(path.dirname(linked), { recursive: true });
  fs.symlinkSync(unrelated, linked);
  const symlinkCandidates = discoverCaptureRootCandidates(home);
  check("studio_symlink_does_not_enroll_unrelated_physical_folder",
    symlinkCandidates.some((entry) => entry.shape === "studio_codex_conductor" && entry.directory === unrelated), false);

  const otherHome = path.join(root, "other-user-home", "sessions");
  const outsideLink = path.join(home, ".clientai/studio/borg/conductors/other/profile/sessions");
  fs.mkdirSync(otherHome, { recursive: true });
  fs.mkdirSync(path.dirname(outsideLink), { recursive: true });
  fs.symlinkSync(otherHome, outsideLink);
  check("studio_symlink_outside_operator_home_is_rejected",
    discoverCaptureRootCandidates(home).some((entry) => entry.directory === otherHome), false);

  const other = path.join(home, ".clientai/studio/borg/conductors/not-an-agent/profile/sessions");
  fs.mkdirSync(other, { recursive: true });
  fs.writeFileSync(path.join(path.dirname(other), "config.toml"), 'model = "gpt-6-sol"\n');
  fs.writeFileSync(path.join(other, "rollout-2026-09-27T00-00-00-12345678-1234-4234-8234-123456789abc.jsonl"),
    '{}\n');
  const nameOnlyCandidates = discoverCaptureRootCandidates(home);
  check("studio_name_only_folder_is_preview_only",
    nameOnlyCandidates.find((entry) => entry.shape === "studio_codex_conductor" && entry.directory === other)?.autoEnroll,
    false);

  const machine = "fleet-label-not-the-hostname";
  const directory = path.join(home, ".codex/sessions");
  const identity = deriveCaptureRootIdentity(machine, "codex", directory);
  const roots = [{ ...identity, source: "codex" as const, directory,
    installationEpochId: randomUUID() }];
  const hostname = os.hostname();
  const candidates = [...new Set([hostname, hostname.split(".")[0] ?? "", hostname.toLowerCase(),
    (hostname.split(".")[0] ?? "").toLowerCase()])].filter(Boolean);
  check("nonhostname_label_cannot_be_guessed_from_hostname",
    resolveCaptureRootMachineLabel(roots, candidates), null);
  const persisted = collectorConfigSchema.parse({ captureRoots: roots, enrollmentMachineLabel: machine });
  check("join_can_recover_existing_nonhostname_machine_label",
    resolveCaptureRootMachineLabel(roots, [persisted.enrollmentMachineLabel ?? ""]) === machine &&
      captureRootsDeriveFrom(roots, persisted.enrollmentMachineLabel ?? ""), true);

  const rolloutId = "12345678-1234-4234-8234-123456789abc";
  const rolloutName = `rollout-2026-09-28T00-00-00-${rolloutId}.jsonl`;
  const rollout = `${JSON.stringify({ type: "session_meta", timestamp: "2026-09-28T00:00:00Z",
    payload: { id: rolloutId, originator: "codex_exec" } })}\n`;
  const withoutConfig = path.join(root, "codex-without-optional-config");
  const currentDay = path.join(withoutConfig, ".codex/sessions/2026/09/28");
  fs.mkdirSync(currentDay, { recursive: true });
  fs.writeFileSync(path.join(currentDay, rolloutName), rollout);
  check("valid_codex_rollout_without_optional_config_enrolls",
    discoverCaptureRootCandidates(withoutConfig).find((entry) => entry.shape === "codex_home")?.autoEnroll, true);

  const manyDates = path.join(root, "codex-many-dates");
  const sessions = path.join(manyDates, ".codex/sessions");
  for (let month = 1; month <= 5; month += 1) {
    for (let day = 1; day <= 30; day += 1) {
      fs.mkdirSync(path.join(sessions, "2026", String(month).padStart(2, "0"),
        String(day).padStart(2, "0")), { recursive: true });
    }
  }
  fs.writeFileSync(path.join(sessions, "2026/05/30", rolloutName), rollout);
  check("newest_valid_rollout_survives_many_date_directories",
    discoverCaptureRootCandidates(manyDates).find((entry) => entry.shape === "codex_home")?.autoEnroll, true);

  const exhaustedHome = path.join(root, "codex-exhausted");
  const exhaustedDay = path.join(exhaustedHome, ".codex/sessions/2026/09/28");
  fs.mkdirSync(exhaustedDay, { recursive: true });
  for (let index = 0; index < 130; index += 1) {
    const id = `12345678-1234-4234-8234-${String(index).padStart(12, "0")}`;
    fs.writeFileSync(path.join(exhaustedDay, `rollout-2026-09-28T00-00-00-${id}.jsonl`), "{}\n");
  }
  check("exhausted_rollout_search_is_preview_not_absence",
    discoverCaptureRootCandidates(exhaustedHome).find((entry) => entry.shape === "codex_home")?.reason,
    "codex_evidence_exhausted");

  const agentHome = path.join(root, "launch-agent-home");
  fs.mkdirSync(agentHome, { recursive: true, mode: 0o700 });
  const runtime = path.join(process.cwd(), "packages/collector-cli/dist/cli.mjs");
  const install = { homeDir: agentHome, repoRoot: path.dirname(runtime),
    workingDirectory: path.dirname(runtime), programArguments: [process.execPath, runtime, "start"] };
  const installed = installLaunchAgent(install);
  const priorPath = process.env.PATH;
  try {
    process.env.PATH = `${priorPath}:/opt/new-toolchain`;
    check("shell_path_drift_does_not_look_like_owner_edit",
      launchAgentOwnerEditedKeys({ homeDir: agentHome }), []);
  } finally { process.env.PATH = priorPath; }
  const before = fs.readFileSync(installed.plistPath, "utf8");
  const edited = before.replace(`<string>${runtime}</string>`,
    `<string>${path.join(path.dirname(runtime), "custom-cli.mjs")}</string>`);
  if (edited === before) throw new Error("launch agent program fixture edit did not apply");
  fs.writeFileSync(installed.plistPath, edited, { mode: 0o600 });
  check("owner_edited_program_argument_is_detected",
    launchAgentOwnerEditedKeys({ homeDir: agentHome }).includes("ProgramArguments"), true);
  const legacyHome = path.join(root, "pre-template-launch-agent-home");
  fs.mkdirSync(legacyHome, { recursive: true, mode: 0o700 });
  const legacyInstall = installLaunchAgent({ ...install, homeDir: legacyHome });
  fs.rmSync(`${legacyInstall.plistPath}.plimsoll-owned-template.json`, { force: true });
  const legacyOriginal = fs.readFileSync(legacyInstall.plistPath, "utf8");
  const trustedRuntime = { programArguments: install.programArguments,
    workingDirectory: install.workingDirectory };
  check("pre_template_uses_verified_runtime_without_false_edit",
    inspectLaunchAgentOwnership({ homeDir: legacyHome, legacyRuntime: trustedRuntime }).ownerEditedKeys, []);
  fs.writeFileSync(legacyInstall.plistPath,
    legacyOriginal.replace(`<string>${runtime}</string>`,
      `<string>${path.join(path.dirname(runtime), "custom-cli.mjs")}</string>`), { mode: 0o600 });
  check("pre_template_install_uses_runtime_path_not_edited_manifest_argument",
    inspectLaunchAgentOwnership({ homeDir: legacyHome,
      legacyRuntime: trustedRuntime }).ownerEditedKeys.includes("ProgramArguments"), true);
  const movedDirectory = path.join(path.dirname(runtime), "alternate-runtime");
  const movedManifest = legacyOriginal.replace(`<string>${runtime}</string>`,
    `<string>${path.join(movedDirectory, "cli.mjs")}</string>`).replace(
      `<key>WorkingDirectory</key>\n  <string>${path.dirname(runtime)}</string>`,
      `<key>WorkingDirectory</key>\n  <string>${movedDirectory}</string>`);
  fs.writeFileSync(legacyInstall.plistPath, movedManifest, { mode: 0o600 });
  const movedKeys = inspectLaunchAgentOwnership({ homeDir: legacyHome,
    legacyRuntime: trustedRuntime }).ownerEditedKeys;
  check("pre_template_edited_working_directory_is_detected",
    movedKeys.includes("ProgramArguments") && movedKeys.includes("WorkingDirectory"), true);
  const editedNode = legacyOriginal.replace(`<string>${process.execPath}</string>`,
    `<string>${path.join(path.dirname(process.execPath), "node-owner-edit")}</string>`);
  if (editedNode === legacyOriginal) throw new Error("legacy node executable fixture edit did not apply");
  fs.writeFileSync(legacyInstall.plistPath, editedNode, { mode: 0o600 });
  check("pre_template_edited_node_executable_is_detected",
    inspectLaunchAgentOwnership({ homeDir: legacyHome,
      legacyRuntime: trustedRuntime }).ownerEditedKeys.includes("ProgramArguments"), true);
  // The packaged join proof checks local event and hosted unique-event totals
  // across the first-contact replay; this unit witness never fakes an append.
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}

console.log(JSON.stringify({ proof: "pr428-review-regressions", failures }));
if (failures.length) process.exitCode = 1;
