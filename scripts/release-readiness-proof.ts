/**
 * Issue 0001 proof: keep the README command contract aligned with the
 * executable CLI, package manifest, root scripts, and source installer.
 *
 * This is deliberately a source-only check. It does not invoke npx, touch a
 * user's home, start a collector, or claim clean-Mac/live-provider evidence.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

type PackageJson = {
  name?: unknown;
  scripts?: Record<string, unknown>;
  bin?: Record<string, unknown>;
  files?: unknown;
};

export type ReadmeContractInput = {
  readme: string;
  cliSource: string;
  efficiencyReportSource: string;
  installerSource: string;
  rootPackage: PackageJson;
  cliPackage: PackageJson;
};

const CLI_COMMANDS = new Set([
  "start",
  "status",
  "join",
  "doctor",
  "export",
  "forward-hook",
  "self-test-hook",
  "generate-config",
  "setup",
  "upload",
  "upload-history",
  "push-repo-labels",
  "sync-outcomes",
  "backfill-outcome-timeline",
  "backfill-outcome-performance",
  "weekly-performance-rollup",
  "scan-rollouts",
  "scan-transcripts",
  "drain-projections",
  "install-launch-agent",
  "load-launch-agent",
  "unload-launch-agent",
  "uninstall-launch-agent",
  "lifecycle",
  "label",
  "priority",
  "purge-local-data",
  "stop",
]);

const LIFECYCLE_ACTIONS = new Set([
  "update",
  "rollback",
  "uninstall",
  "purge",
  "support-bundle",
]);

const REQUIRED_CLI_FLAGS: Record<string, readonly string[]> = {
  doctor: ["--read-only", "--json"],
  "install-launch-agent": ["--dev", "--repo-root", "--pnpm", "--load"],
};

function shellCommandLines(markdown: string): string[] {
  const commands: string[] = [];
  for (const match of markdown.matchAll(/```[^\n]*\n([\s\S]*?)```/g)) {
    for (const rawLine of match[1]!.split("\n")) {
      let line = rawLine.trim();
      if (line.startsWith("$")) line = line.slice(1).trim();
      if (!line || line.startsWith("#")) continue;
      line = line.replace(/\s+#.*$/, "").replace(/\\\s*$/, "").trim();
      if (/^(?:npx|pnpm|git|cd|\.\/install\.sh)\b/.test(line)) {
        commands.push(line);
      }
    }
  }
  return commands;
}

function words(command: string): string[] {
  return command.match(/"[^"]*"|'[^']*'|\S+/g)?.map((word) =>
    word.replace(/^['"]|['"]$/g, ""),
  ) ?? [];
}

function sourceHasCommand(cliSource: string, command: string): boolean {
  return cliSource.includes(`command === "${command}"`);
}

function requiredFlagsPresent(commandWords: readonly string[], command: string): boolean {
  return (REQUIRED_CLI_FLAGS[command] ?? []).every((flag) => commandWords.includes(flag));
}

/** Returns contract failures rather than throwing so adversarial mutations can be tested. */
export function validateReadmeContract(input: ReadmeContractInput): string[] {
  const failures: string[] = [];
  const commands = shellCommandLines(input.readme);
  const rootScripts = input.rootPackage.scripts ?? {};

  if (commands.length === 0) failures.push("README contains no shell commands");
  if (input.cliPackage.name !== "@plimsoll/cli") failures.push("package name is not @plimsoll/cli");
  if (input.cliPackage.bin?.plimsoll !== "./dist/cli.mjs") {
    failures.push("packaged plimsoll bin is not ./dist/cli.mjs");
  }
  if (!Array.isArray(input.cliPackage.files) || !input.cliPackage.files.includes("dist")) {
    failures.push("package does not ship dist");
  }

  for (const command of commands) {
    const commandWords = words(command);
    const executable = commandWords[0];
    if (executable === "npx") {
      const packageIndex = commandWords.findIndex((word) => word === "@plimsoll/cli" || word.startsWith("@plimsoll/cli@"));
      const packageName = packageIndex === -1 ? undefined : commandWords[packageIndex];
      const cliCommand = packageIndex === undefined || packageIndex === -1
        ? undefined
        : commandWords[packageIndex + 1];
      if (!packageName) {
        failures.push(`npx command does not invoke @plimsoll/cli: ${command}`);
        continue;
      }
      if (!cliCommand || !CLI_COMMANDS.has(cliCommand) || !sourceHasCommand(input.cliSource, cliCommand)) {
        failures.push(`npx command is not an executable CLI command: ${command}`);
        continue;
      }
      if (cliCommand === "lifecycle") {
        const action = commandWords[packageIndex + 2];
        if (!action || !LIFECYCLE_ACTIONS.has(action)) {
          failures.push(`npx lifecycle action is unsupported: ${command}`);
        }
      }
      if (!requiredFlagsPresent(commandWords, cliCommand)) {
        failures.push(`npx command is missing required flags: ${command}`);
      }
      continue;
    }

    if (executable === "pnpm") {
      let scriptIndex = 1;
      if (commandWords[scriptIndex] === "--dir") scriptIndex += 2;
      const script = commandWords[scriptIndex];
      if (!script) {
        failures.push(`pnpm command has no script: ${command}`);
        continue;
      }
      if (script === "install") continue;
      if (script === "collector") {
        const cliCommand = commandWords[scriptIndex + 1];
        if (!cliCommand || !CLI_COMMANDS.has(cliCommand) || !sourceHasCommand(input.cliSource, cliCommand)) {
          failures.push(`pnpm collector command is not executable: ${command}`);
          continue;
        }
        if (!requiredFlagsPresent(commandWords, cliCommand)) {
          failures.push(`pnpm collector command is missing required flags: ${command}`);
        }
        continue;
      }
      if (typeof rootScripts[script] !== "string") {
        failures.push(`pnpm script is missing from package.json: ${command}`);
        continue;
      }
      if (script === "report" && !commandWords.includes("--repository")) {
        failures.push(`report command must name a repository: ${command}`);
      }
      if (script === "report" && !input.efficiencyReportSource.includes("--repository")) {
        failures.push("report source does not expose --repository");
      }
      continue;
    }

    if (executable === "git" && commandWords[1] === "clone") continue;
    if (executable === "cd") continue;
    if (executable === "./install.sh") {
      for (const argument of commandWords.slice(1)) {
        if (!input.installerSource.includes(`${argument})`)) {
          failures.push(`installer argument is not supported: ${command}`);
        }
      }
      continue;
    }
    failures.push(`unsupported README executable: ${command}`);
  }

  return failures;
}

function readJson(file: string): PackageJson {
  return JSON.parse(fs.readFileSync(file, "utf8")) as PackageJson;
}

function main(): void {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const input: ReadmeContractInput = {
    readme: fs.readFileSync(path.join(root, "README.md"), "utf8"),
    cliSource: fs.readFileSync(path.join(root, "packages/collector-cli/src/cli.ts"), "utf8"),
    efficiencyReportSource: fs.readFileSync(path.join(root, "scripts/efficiency-report.ts"), "utf8"),
    installerSource: fs.readFileSync(path.join(root, "install.sh"), "utf8"),
    rootPackage: readJson(path.join(root, "package.json")),
    cliPackage: readJson(path.join(root, "packages/collector-cli/package.json")),
  };
  const commands = shellCommandLines(input.readme);
  const failures = validateReadmeContract(input);
  assert.deepEqual(failures, [], failures.join("\n"));
  console.log(`release readiness proof: README command contract passed (${commands.length} commands)`);

  const typo = input.readme.replace("npx -y @plimsoll/cli setup", "npx -y @plimsoll/cli steup");
  assert.ok(
    validateReadmeContract({ ...input, readme: typo }).some((failure) => failure.includes("not an executable CLI command")),
    "a typo'd CLI command must fail the contract",
  );
  const missingScript = input.readme.replace("pnpm report -- --repository", "pnpm repot -- --repository");
  assert.ok(
    validateReadmeContract({ ...input, readme: missingScript }).some((failure) => failure.includes("missing from package.json")),
    "a typo'd pnpm script must fail the contract",
  );
  const removedRepository = input.readme.replace("pnpm report -- --repository your-org/your-repo", "pnpm report");
  assert.ok(
    validateReadmeContract({ ...input, readme: removedRepository }).some((failure) => failure.includes("must name a repository")),
    "a report without repository scope must fail the contract",
  );
  const missingBin = validateReadmeContract({
    ...input,
    cliPackage: { ...input.cliPackage, bin: { plimsoll: "./dist/missing.mjs" } },
  });
  assert.ok(missingBin.some((failure) => failure.includes("packaged plimsoll bin")), "a broken package bin must fail");
  console.log("release readiness proof: adversarial contract mutations rejected (4/4)");
}

main();
