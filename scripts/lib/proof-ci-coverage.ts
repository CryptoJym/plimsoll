import fs from "node:fs";
import path from "node:path";

/**
 * Which package.json proof scripts CI actually runs (eco-6hoxj.163.23).
 *
 * A proof that CI never runs rots silently: proof:enrollment-privacy failed
 * from 2026-09-07 until eco-6hoxj.163.22 while the published privacy spec
 * cited it. A proof script counts as run by CI only when a step of
 * .github/workflows/proof.yml invokes it (`pnpm <script>`, `pnpm run <script>`,
 * or node/tsx on its entry file) on its own shell line, in a step whose failure
 * fails the job. Every other proof script must be listed, with a reviewed
 * reason, in scripts/proof-local-only.json.
 */

export const PROOF_WORKFLOW = ".github/workflows/proof.yml";
export const PROOF_LOCAL_ONLY = "scripts/proof-local-only.json";

const PROOF_SCRIPT = /^proof(?::|$)/;

export type WorkflowStep = {
  name: string;
  line: number;
  run: string | null;
  ifCondition: string | null;
  continueOnError: string | null;
};

export type ProofInvocation = {
  step: string;
  line: number;
  via: "script" | "entry";
  command: string;
};

export type IgnoredInvocation = ProofInvocation & { reason: string };

export type ProofScriptCoverage = {
  script: string;
  command: string;
  entry: string | null;
  invocations: ProofInvocation[];
  ignored: IgnoredInvocation[];
  localOnly: string | null;
};

export type ProofCiCoverage = {
  scripts: ProofScriptCoverage[];
  /** Proof scripts that proof.yml does not run and proof-local-only.json does not list. */
  uncovered: string[];
  /** Listed as local-only, yet proof.yml runs them: the list is stale. */
  localOnlyButRun: string[];
  /** Local-only entries that name no package.json proof script, or give no reason. */
  invalidLocalOnly: string[];
  /** proof.yml invokes `pnpm proof…` names that package.json does not define. */
  unknownInvocations: string[];
};

export type ProofCiCoverageInput = {
  scripts: Record<string, string>;
  workflow: string;
  localOnly: Record<string, unknown>;
};

const indentOf = (line: string) => line.length - line.trimStart().length;

function scalar(value: string) {
  const trimmed = value.trim();
  if (
    trimmed.length >= 2 &&
    ((trimmed.startsWith('"') && trimmed.endsWith('"')) ||
      (trimmed.startsWith("'") && trimmed.endsWith("'")))
  ) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

/**
 * The steps of every `steps:` list in a GitHub workflow: name, `if`,
 * `continue-on-error` and the `run` script (inline or a `|`/`>` block).
 * Deliberately small: it reads only the step keys this gate needs.
 */
export function parseWorkflowSteps(text: string): WorkflowStep[] {
  const lines = text.split(/\r?\n/);
  const steps: Array<WorkflowStep & { keyIndent: number }> = [];
  for (let index = 0; index < lines.length; index += 1) {
    const stepsKey = /^(\s*)steps:\s*$/.exec(lines[index]!);
    if (!stepsKey) continue;
    const listIndent = stepsKey[1]!.length;
    let step: (WorkflowStep & { keyIndent: number }) | null = null;
    let cursor = index + 1;
    while (cursor < lines.length) {
      const raw = lines[cursor]!;
      if (raw.trim() === "" || raw.trimStart().startsWith("#")) {
        cursor += 1;
        continue;
      }
      const indent = indentOf(raw);
      if (indent <= listIndent) break;
      let keyLine = raw;
      let keyIndent = indent;
      const item = /^(\s*)-\s+(.*)$/.exec(raw);
      if (item && (step === null || item[1]!.length < step.keyIndent)) {
        if (step) steps.push(step);
        keyIndent = item[1]!.length + 2;
        keyLine = `${" ".repeat(keyIndent)}${item[2]}`;
        step = { name: "", line: cursor + 1, run: null, ifCondition: null, continueOnError: null, keyIndent };
      }
      cursor += 1;
      if (!step || keyIndent !== step.keyIndent) continue;
      const key = /^\s*([A-Za-z0-9_-]+):(?:\s+(.*))?$/.exec(keyLine);
      if (!key) continue;
      const name = key[1]!;
      const value = key[2] ?? "";
      let content = value;
      if (/^[|>][-+]?$/.test(value.trim())) {
        const block: string[] = [];
        while (cursor < lines.length) {
          const next = lines[cursor]!;
          if (next.trim() !== "" && indentOf(next) <= keyIndent) break;
          block.push(next);
          cursor += 1;
        }
        const nonEmpty = block.filter((line) => line.trim() !== "");
        const blockIndent = nonEmpty.length > 0 ? Math.min(...nonEmpty.map(indentOf)) : 0;
        content = block.map((line) => line.slice(blockIndent)).join("\n");
      } else {
        content = scalar(value);
      }
      if (name === "name") step.name = content;
      else if (name === "run") step.run = content;
      else if (name === "if") step.ifCondition = content;
      else if (name === "continue-on-error") step.continueOnError = content;
    }
    if (step) steps.push(step);
    index = cursor - 1;
  }
  return steps.map(({ keyIndent: _keyIndent, ...step }) => step);
}

/** The file a package.json proof command runs (its last script path). */
export function proofEntry(command: string): string | null {
  const files = command
    .split(/\s+/)
    .map((token) => token.replace(/^\.\//, ""))
    .filter((token) => /^(?:scripts|packages)\/\S+\.[cm]?[jt]s$/.test(token));
  return files.at(-1) ?? null;
}

/** Shell lines of a run script: comments dropped, `\` continuations joined. */
function shellLines(run: string) {
  const joined = run.replace(/\\\r?\n/g, " ");
  return joined
    .split(/\r?\n/)
    .map((line) => line.replace(/(^|\s)#.*$/, "").trim())
    .filter((line) => line.length > 0);
}

/**
 * A line whose exit status could be swallowed: `||`, `&&`, `;`, a pipe or a
 * background `&` outside quotes, `${{ }}` expressions and fd redirections.
 */
function hasControlOperator(line: string) {
  const bare = line
    .replace(/\$\{\{[\s\S]*?\}\}/g, "EXPR")
    .replace(/'[^']*'/g, "''")
    .replace(/"(?:[^"\\]|\\.)*"/g, '""')
    .replace(/\d*>&\d+|&>>?/g, " ");
  return /[;&|]/.test(bare);
}

function stepIgnoredReason(step: WorkflowStep) {
  if (step.continueOnError !== null && scalar(step.continueOnError) !== "false") {
    return `step sets continue-on-error: ${step.continueOnError}`;
  }
  if (step.ifCondition !== null && /^(?:false|\$\{\{\s*false\s*\}\})$/.test(step.ifCondition.trim())) {
    return "step never runs (if: false)";
  }
  return null;
}

export function proofCiCoverage(input: ProofCiCoverageInput): ProofCiCoverage {
  const steps = parseWorkflowSteps(input.workflow);
  const proofScripts = Object.keys(input.scripts).filter((name) => PROOF_SCRIPT.test(name)).sort();
  const coverage = new Map<string, ProofScriptCoverage>();
  for (const script of proofScripts) {
    const command = input.scripts[script]!;
    const localOnly = input.localOnly[script];
    coverage.set(script, {
      script,
      command,
      entry: proofEntry(command),
      invocations: [],
      ignored: [],
      localOnly: typeof localOnly === "string" && localOnly.trim() !== "" ? localOnly : null,
    });
  }
  const unknownInvocations = new Set<string>();
  const runnerLine = /(?:^|\s)(?:\S*\/)?(?:node|tsx)(?:\s|$)|tsx\/dist\/cli\.mjs/;
  for (const step of steps) {
    if (step.run === null) continue;
    const ignoredBecause = stepIgnoredReason(step);
    for (const line of shellLines(step.run)) {
      const found: Array<{ script: string; via: ProofInvocation["via"] }> = [];
      for (const match of line.matchAll(/(?:^|[\s;&|(])pnpm\s+(?:run\s+)?(proof(?::[A-Za-z0-9:_.-]+)?)(?=$|[\s;&|)])/g)) {
        const script = match[1]!;
        if (coverage.has(script)) found.push({ script, via: "script" });
        else unknownInvocations.add(script);
      }
      if (runnerLine.test(line)) {
        const tokens = new Set(line.split(/\s+/).map((token) => scalar(token).replace(/^\.\//, "")));
        for (const entry of coverage.values()) {
          if (entry.entry && tokens.has(entry.entry)) found.push({ script: entry.script, via: "entry" });
        }
      }
      const reason = ignoredBecause ?? (hasControlOperator(line) ? "exit status can be swallowed on this line" : null);
      for (const { script, via } of found) {
        const invocation: ProofInvocation = { step: step.name, line: step.line, via, command: line };
        const entry = coverage.get(script)!;
        if (reason) entry.ignored.push({ ...invocation, reason });
        else entry.invocations.push(invocation);
      }
    }
  }
  const scripts = [...coverage.values()];
  return {
    scripts,
    uncovered: scripts
      .filter((entry) => entry.invocations.length === 0 && entry.localOnly === null)
      .map((entry) => entry.script),
    localOnlyButRun: scripts
      .filter((entry) => entry.invocations.length > 0 && entry.localOnly !== null)
      .map((entry) => entry.script),
    invalidLocalOnly: Object.entries(input.localOnly)
      .filter(([script, reason]) => !coverage.has(script) || typeof reason !== "string" || reason.trim() === "")
      .map(([script]) => script)
      .sort(),
    unknownInvocations: [...unknownInvocations].sort(),
  };
}

/** Read package.json, proof.yml and proof-local-only.json from a checkout. */
export function readProofCiCoverageInput(repoRoot: string): ProofCiCoverageInput {
  const pkg = JSON.parse(fs.readFileSync(path.join(repoRoot, "package.json"), "utf8")) as {
    scripts?: Record<string, string>;
  };
  const localOnlyPath = path.join(repoRoot, PROOF_LOCAL_ONLY);
  const localOnly = fs.existsSync(localOnlyPath)
    ? (JSON.parse(fs.readFileSync(localOnlyPath, "utf8")) as Record<string, unknown>)
    : {};
  return {
    scripts: pkg.scripts ?? {},
    workflow: fs.readFileSync(path.join(repoRoot, PROOF_WORKFLOW), "utf8"),
    localOnly,
  };
}
