/**
 * The only command forms proof:ci-coverage counts as running a proof
 * (eco-6hoxj.163.23).
 *
 * This is an allow-list, not a shell parser. Two independent reviews found
 * that each shell feature the gate modelled by hand left another way to make
 * a proof look run when it was not. So a command either matches one of the
 * exact forms below, or it is not canonical and the gate fails.
 *
 * A package script (pnpm runs it with `sh -c`) is canonical when it is
 * exactly one of:
 *   tsx <file> [args]
 *   node [--expose-gc] [--max-old-space-size=N] --import tsx <file> [args]
 *   node ./node_modules/tsx/dist/cli.mjs <file> [args]
 *   pnpm <script>                                     (a pure alias)
 * where <file> is a repository .ts/.js file under scripts/ or packages/ and
 * `tsx scripts/run-proof.ts [--direct-node] <file> [args]` runs <file>. Every
 * word must be unquoted and made only of the characters [A-Za-z0-9_./:@%+,=-],
 * so a second statement, `;`, `&&`, `||`, a pipe, a redirection, quoting, an
 * expansion, an environment prefix, `exit`, `trap` or any other flag (such as
 * `node --check`) makes the script non-canonical.
 *
 * A workflow `run:` line (GitHub runs the step with bash and errexit) is
 * canonical when it is exactly one of:
 *   [NAME=value ...] pnpm|node <args> [> file | >> file]
 *   export NAME=value
 * Args are plain words; values may also be "$VAR" or "$(mktemp -d <template>)".
 * The inert GitHub expressions allowed as arguments are the head commit SHA,
 * `github.sha`, `runner.temp` and `github.workspace` (including the combined
 * head-SHA expression):
 * GitHub pastes every `${{ }}` into the script before bash reads it, and that
 * one always expands to 40 hex digits. Blank lines and whole-line comments
 * are skipped; anything else (other expressions, operators, heredocs, other
 * quoting or expansions, line continuations, other commands, trailing
 * comments) is not canonical.
 */

export const LITERAL_WORD = /^[A-Za-z0-9_./:@%+,=-]+$/;
export const RUN_PROOF_WRAPPER = "scripts/run-proof.ts";
/** Expressions that GitHub resolves without changing which proof runs. */
export const INERT_EXPRESSIONS = {
  "${{ github.event.pull_request.head.sha || github.sha }}": "<head-sha>",
  "${{ github.sha }}": "<github-sha>",
  "${{ runner.temp }}": "<runner-temp>",
  "${{ github.workspace }}": "<github-workspace>",
} as const;
const TSX_CLI = "node_modules/tsx/dist/cli.mjs";
const REPO_FILE = /^(?:scripts|packages)\/(?:[A-Za-z0-9_.-]+\/)*[A-Za-z0-9_.-]+\.[cm]?[jt]s$/;
const ASSIGNMENT = /^[A-Za-z_][A-Za-z0-9_]*=/;

export type Problem = { problem: string };
export type RunnerInvocation = { file: string; args: string[] };
export type ScriptForm = RunnerInvocation | { alias: string };

export const normalizeFile = (word: string) => word.replace(/^\.\//, "");

/** A repository script path (scripts/ or packages/, no `.` or `..` segments). */
export function isRepoFile(word: string) {
  const file = normalizeFile(word);
  return REPO_FILE.test(file) && !file.split("/").some((segment) => segment === "." || segment === "..");
}

/** The file one runner command executes, or why its words are not a canonical runner invocation. */
export function runnerInvocation(words: string[]): RunnerInvocation | Problem {
  const [head, ...rest] = words;
  let fileAt = 0;
  if (head === "node") {
    if (rest[0] !== undefined && normalizeFile(rest[0]) === TSX_CLI) {
      fileAt = 1;
    } else {
      let loadsTsx = false;
      while (rest[fileAt]?.startsWith("-")) {
        if (rest[fileAt] === "--import" && rest[fileAt + 1] === "tsx" && !loadsTsx) {
          loadsTsx = true;
          fileAt += 2;
        } else if (rest[fileAt] === "--expose-gc" || /^--max-old-space-size=[1-9][0-9]*$/.test(rest[fileAt] ?? "")) {
          fileAt += 1;
        } else {
          return { problem: `node flag \`${rest[fileAt]}\` is not allowed (only --import tsx, --expose-gc and --max-old-space-size=N)` };
        }
      }
      if (!loadsTsx) return { problem: "node must load tsx (`--import tsx`) or run ./node_modules/tsx/dist/cli.mjs" };
    }
  } else if (head !== "tsx") {
    return { problem: `\`${head ?? ""}\` is not an allowed runner (tsx, node or a pure \`pnpm <script>\` alias)` };
  }
  const file = rest[fileAt];
  if (file === undefined) return { problem: `${head} runs no file` };
  if (file.startsWith("-")) return { problem: `${head} flag \`${file}\` is not allowed` };
  if (!isRepoFile(file)) return { problem: `\`${file}\` is not a script file under scripts/ or packages/` };
  const args = rest.slice(fileAt + 1);
  if (normalizeFile(file) !== RUN_PROOF_WRAPPER) return { file: normalizeFile(file), args };
  const entryArgs = args[0] === "--direct-node" ? args.slice(1) : args;
  const entry = entryArgs[0];
  if (entry === undefined || !isRepoFile(entry) || normalizeFile(entry) === RUN_PROOF_WRAPPER) {
    return { problem: `${RUN_PROOF_WRAPPER} must be given one script file (optionally after --direct-node)` };
  }
  return { file: normalizeFile(entry), args: entryArgs.slice(1) };
}

/** The canonical form of a package script, or why it is not canonical. */
export function packageScriptForm(text: string): ScriptForm | Problem {
  const words = text.trim().split(/[ \t]+/);
  if (text.trim() === "") return { problem: "the script is empty" };
  const plain = words.find((word) => !LITERAL_WORD.test(word));
  if (plain !== undefined) {
    return { problem: `\`${plain}\` is not a plain word (no operators, redirections, quoting, expansions or newlines)` };
  }
  if (ASSIGNMENT.test(words[0]!)) return { problem: `\`${words[0]}\` sets the environment before the command` };
  if (words[0] === "pnpm") {
    const alias = words[1] === "run" ? words[2] : words[1];
    const pureAlias = typeof alias === "string" && !alias.startsWith("-");
    return (((words.length === 2 && words[1] !== "run") || (words.length === 3 && words[1] === "run")) && pureAlias)
      ? { alias: alias! }
      : { problem: "pnpm is allowed only as a pure alias, `pnpm <script>` or `pnpm run <script>`" };
  }
  return runnerInvocation(words);
}

/**
 * Every word-like token of arbitrary shell text. Over-approximate on
 * purpose: it decides what must be canonical and what the inventory holds,
 * and never counts anything as run.
 */
export function mentionedWords(text: string): string[] {
  return text.split(/[\s;&|()<>'"`$={}\\]+/).filter(Boolean);
}

export const INERT_EXPRESSION = "${{ github.event.pull_request.head.sha || github.sha }}";

export type WorkflowLine =
  | { kind: "export"; name: string }
  | { kind: "command"; assignments: string[]; words: string[] };

type Segment = "literal" | "sha" | "inert" | "var" | "mktemp";
type LineWord = { text: string; segments: Set<Segment> } | { redirect: ">" | ">>" };

function outsideForm(rest: string): Problem {
  if (rest.startsWith("${{") || rest.startsWith('"${{')) {
    return { problem: "GitHub pastes `${{ … }}` into the script before bash reads it; only the documented inert expressions are allowed" };
  }
  if (/^\d*<</.test(rest)) return { problem: "heredocs are not allowed" };
  if (rest.startsWith("#")) return { problem: "trailing comments are not allowed" };
  if (rest === "\\") return { problem: "line continuations are not allowed" };
  if (/^[;&|]/.test(rest)) return { problem: `operator \`${rest.slice(0, 2).trim()}\` is not allowed` };
  return { problem: `\`${rest.slice(0, 32)}\` is outside the canonical form` };
}

function scanLine(line: string): LineWord[] | Problem {
  const words: LineWord[] = [];
  let i = 0;
  while (i < line.length) {
    if (line[i] === " " || line[i] === "\t") {
      i += 1;
      continue;
    }
    const redirect = /^>>?(?=[ \t])/.exec(line.slice(i));
    if (redirect) {
      words.push({ redirect: redirect[0] as ">" | ">>" });
      i += redirect[0].length;
      continue;
    }
    let text = "";
    const segments = new Set<Segment>();
    while (i < line.length && line[i] !== " " && line[i] !== "\t") {
      const rest = line.slice(i);
      const literal = /^[A-Za-z0-9_./:@%+,=-]+/.exec(rest);
      const variable = /^"\$(?:([A-Za-z_][A-Za-z0-9_]*)|\{([A-Za-z_][A-Za-z0-9_]*)\})"/.exec(rest);
      const mktemp = /^"\$\(mktemp -d [A-Za-z0-9_./-]+\)"/.exec(rest);
      let taken: string;
      if (literal) {
        taken = literal[0];
        text += taken;
        segments.add("literal");
      } else if (variable) {
        taken = variable[0];
        text += `$${variable[1] ?? variable[2]}`;
        segments.add("var");
      } else if (mktemp) {
        taken = mktemp[0];
        text += "<mktemp>";
        segments.add("mktemp");
      } else {
        const expression = Object.entries(INERT_EXPRESSIONS).find(([candidate]) =>
          rest.startsWith(`"${candidate}"`) || rest.startsWith(candidate),
        );
        if (expression) {
          const [candidate, replacement] = expression;
          taken = rest.startsWith(`"${candidate}"`) ? `"${candidate}"` : candidate;
          text += replacement;
          segments.add(candidate === INERT_EXPRESSION ? "sha" : "inert");
        } else {
          return outsideForm(rest);
        }
      }
      i += taken.length;
    }
    words.push({ text, segments });
  }
  return words;
}

const only = (word: LineWord, allowed: Segment[]) =>
  "text" in word && [...word.segments].every((segment) => allowed.includes(segment));

/** The canonical form of one workflow `run:` line, or why it is not canonical. */
export function workflowLineForm(line: string): WorkflowLine | Problem {
  const scanned = scanLine(line);
  if ("problem" in scanned) return scanned;
  const words = [...scanned];
  const first = words[0];
  if (first && "text" in first && first.text === "export") {
    const assignment = words[1];
    if (words.length !== 2 || !assignment || !("text" in assignment) || !ASSIGNMENT.test(assignment.text)) {
      return { problem: "`export` must set exactly one NAME=value" };
    }
    if (!only(assignment, ["literal", "var", "mktemp"])) return { problem: "`export` value must be a word, \"$VAR\" or \"$(mktemp -d …)\"" };
    return { kind: "export", name: assignment.text.slice(0, assignment.text.indexOf("=")) };
  }
  const assignments: string[] = [];
  while (words[0] && "text" in words[0] && ASSIGNMENT.test(words[0].text)) {
    if (!only(words[0], ["literal", "var"])) return { problem: "an assignment value must be a word or \"$VAR\"" };
    assignments.push(words[0].text.slice(0, words[0].text.indexOf("=")));
    words.shift();
  }
  const redirect = words.at(-2);
  if (redirect && "redirect" in redirect) {
    if (!only(words.at(-1)!, ["literal"])) return { problem: "a redirection must name a plain file" };
    words.splice(-2, 2);
  }
  if (words.some((word) => "redirect" in word)) return { problem: "the only redirection allowed is a final `> file` or `>> file`" };
  const [command, ...args] = words as Array<Extract<LineWord, { text: string }>>;
  if (!command || !only(command, ["literal"]) || (command.text !== "pnpm" && command.text !== "node")) {
    return { problem: `\`${command?.text ?? ""}\` is not an allowed command (pnpm or node)` };
  }
  const bad = args.find((arg) => !only(arg, ["literal", "sha", "inert"]));
  if (bad) return { problem: `argument \`${bad.text}\` must be a plain word or an inert GitHub expression` };
  return { kind: "command", assignments, words: [command.text, ...args.map((arg) => arg.text)] };
}
