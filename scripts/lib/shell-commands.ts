/**
 * A small, deliberately conservative reading of shell text (eco-6hoxj.163.23).
 *
 * The proof CI-coverage gate must only count a command that really executes
 * and whose failure really fails its caller. This module does not try to be a
 * shell: it recognises plain simple commands and stops at anything it cannot
 * model (control structures, `cd`, `exit`, `set +e`, operators that can swallow
 * an exit status, ...). Commands after a stop never count as executed.
 */

export type ShellCommand = {
  /** Static word values (quotes removed); null where a word expands at run time. */
  words: Array<string | null>;
  /** 1-based line within the analysed text. */
  line: number;
  text: string;
};

export type ShellAnalysis = {
  /** Simple commands that provably execute and whose failure propagates. */
  executed: ShellCommand[];
  /** Every simple command seen, executed or not (for inventory). */
  seen: ShellCommand[];
  /** Why analysis stopped early, if it did: nothing after `stoppedAt` counts. */
  stoppedAt: { line: number; reason: string } | null;
};

type Token =
  | { kind: "word"; value: string; dynamic: boolean; line: number; raw: string }
  | { kind: "op"; value: string; line: number }
  | { kind: "redirect"; value: string; line: number }
  | { kind: "newline"; line: number };

const OPERATORS = ["&&", "||", ";;", "|&", ";", "|", "&", "(", ")"];

/** Split shell text into words, operators, redirections and newlines; heredoc bodies are skipped. */
export function tokenizeShell(text: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;
  let line = 1;
  const pendingHeredocs: Array<{ delimiter: string; stripTabs: boolean }> = [];
  let word = "";
  let raw = "";
  let dynamic = false;
  let inWord = false;
  let wordLine = 1;

  const flushWord = () => {
    if (!inWord) return;
    tokens.push({ kind: "word", value: word, dynamic, line: wordLine, raw });
    word = "";
    raw = "";
    dynamic = false;
    inWord = false;
  };
  const startWord = () => {
    if (!inWord) {
      inWord = true;
      wordLine = line;
    }
  };
  const skipBalanced = (open: string, close: string) => {
    // Consumes a $( ... ) or ${ ... } body including nested pairs and quotes.
    let depth = 1;
    while (i < text.length && depth > 0) {
      const c = text[i]!;
      if (c === "\\") {
        raw += text.slice(i, i + 2);
        i += 2;
        continue;
      }
      if (c === "'" ) {
        const end = text.indexOf("'", i + 1);
        const stop = end === -1 ? text.length : end + 1;
        raw += text.slice(i, stop);
        i = stop;
        continue;
      }
      if (c === "\n") line += 1;
      if (c === open) depth += 1;
      if (c === close) depth -= 1;
      raw += c;
      i += 1;
    }
  };
  const readHeredocBodies = () => {
    // Called right after a newline token: consume every pending heredoc body.
    for (const heredoc of pendingHeredocs.splice(0)) {
      while (i < text.length) {
        const end = text.indexOf("\n", i);
        const bodyLine = text.slice(i, end === -1 ? text.length : end);
        i = end === -1 ? text.length : end + 1;
        line += 1;
        const candidate = heredoc.stripTabs ? bodyLine.replace(/^\t+/, "") : bodyLine;
        if (candidate === heredoc.delimiter) break;
      }
    }
  };

  while (i < text.length) {
    const c = text[i]!;
    if (c === "\\" && text[i + 1] === "\n") {
      i += 2;
      line += 1;
      continue;
    }
    if (c === "\n") {
      flushWord();
      tokens.push({ kind: "newline", line });
      i += 1;
      line += 1;
      if (pendingHeredocs.length > 0) readHeredocBodies();
      continue;
    }
    if (c === " " || c === "\t" || c === "\r") {
      flushWord();
      i += 1;
      continue;
    }
    if (c === "#" && !inWord) {
      const end = text.indexOf("\n", i);
      i = end === -1 ? text.length : end;
      continue;
    }
    if (c === "\\") {
      startWord();
      word += text[i + 1] ?? "";
      raw += text.slice(i, i + 2);
      i += 2;
      continue;
    }
    if (c === "'") {
      startWord();
      const end = text.indexOf("'", i + 1);
      const stop = end === -1 ? text.length : end;
      word += text.slice(i + 1, stop);
      raw += text.slice(i, stop + 1);
      for (const ch of text.slice(i, stop)) if (ch === "\n") line += 1;
      i = stop + 1;
      continue;
    }
    if (c === '"') {
      startWord();
      raw += c;
      i += 1;
      while (i < text.length && text[i] !== '"') {
        const d = text[i]!;
        if (d === "\\" && i + 1 < text.length) {
          const next = text[i + 1]!;
          if (next === "\n") {
            line += 1;
          } else {
            word += '"\\$`'.includes(next) ? next : `\\${next}`;
          }
          raw += text.slice(i, i + 2);
          i += 2;
          continue;
        }
        if (d === "$" || d === "`") dynamic = true;
        if (d === "$" && text[i + 1] === "(") {
          raw += "$(";
          i += 2;
          skipBalanced("(", ")");
          continue;
        }
        if (d === "\n") line += 1;
        word += d;
        raw += d;
        i += 1;
      }
      raw += '"';
      i += 1;
      continue;
    }
    if (c === "$") {
      startWord();
      dynamic = true;
      if (text[i + 1] === "(") {
        raw += "$(";
        i += 2;
        skipBalanced("(", ")");
        continue;
      }
      if (text[i + 1] === "{") {
        raw += "${";
        i += 2;
        skipBalanced("{", "}");
        continue;
      }
      word += c;
      raw += c;
      i += 1;
      continue;
    }
    if (c === "`") {
      startWord();
      dynamic = true;
      const end = text.indexOf("`", i + 1);
      const stop = end === -1 ? text.length : end + 1;
      raw += text.slice(i, stop);
      i = stop;
      continue;
    }
    // Redirections, with an optional leading file-descriptor number.
    const redirect = /^(\d*)(<<<|<<-|<<|>>|&>>|&>|>&|<&|<>|>\||>|<)/.exec(text.slice(i));
    // Digits glued to a preceding word stay part of that word (`x2>f`).
    if (redirect && (!inWord || redirect[1] === "")) {
      if (inWord && /^\d+$/.test(word)) {
        // `2` then `>`: the digits were the file-descriptor prefix.
        word = "";
        raw = "";
        inWord = false;
      } else {
        flushWord();
      }
      const op = redirect[2]!;
      tokens.push({ kind: "redirect", value: op, line });
      i += redirect[0].length;
      if (op === "<<" || op === "<<-") {
        while (text[i] === " " || text[i] === "\t") i += 1;
        const delimiter = /^(['"]?)([^\s'"<>;&|()]+)\1/.exec(text.slice(i));
        if (delimiter) {
          pendingHeredocs.push({ delimiter: delimiter[2]!, stripTabs: op === "<<-" });
          i += delimiter[0].length;
        }
      }
      continue;
    }
    const op = OPERATORS.find((candidate) => text.startsWith(candidate, i));
    if (op) {
      flushWord();
      tokens.push({ kind: "op", value: op, line });
      i += op.length;
      continue;
    }
    startWord();
    word += c;
    raw += c;
    i += 1;
  }
  flushWord();
  return tokens;
}

type Statement = {
  line: number;
  /** Commands in order, each a list of word tokens (redirections removed). */
  commands: Array<Array<Extract<Token, { kind: "word" }>>>;
  /** Operators between/around commands in this statement. */
  operators: string[];
  redirects: string[];
};

function splitStatements(tokens: Token[], splitOnSemicolon: boolean): Statement[] {
  const statements: Statement[] = [];
  let current: Statement | null = null;
  let command: Array<Extract<Token, { kind: "word" }>> = [];
  let skipRedirectTarget = false;
  const finishCommand = () => {
    if (current && command.length > 0) current.commands.push(command);
    command = [];
  };
  const finishStatement = () => {
    finishCommand();
    if (current && (current.commands.length > 0 || current.operators.length > 0)) statements.push(current);
    current = null;
  };
  for (const token of tokens) {
    if (token.kind === "newline") {
      finishStatement();
      continue;
    }
    if (!current) current = { line: token.line, commands: [], operators: [], redirects: [] };
    if (token.kind === "redirect") {
      current.redirects.push(token.value);
      // Every redirection takes a target word except a heredoc, whose
      // delimiter the tokenizer already consumed.
      skipRedirectTarget = token.value !== "<<" && token.value !== "<<-";
      continue;
    }
    if (token.kind === "word") {
      if (skipRedirectTarget) {
        skipRedirectTarget = false;
        continue;
      }
      command.push(token);
      continue;
    }
    // Operator.
    if (splitOnSemicolon && token.value === ";") {
      finishStatement();
      continue;
    }
    current.operators.push(token.value);
    finishCommand();
  }
  finishStatement();
  return statements;
}

const ASSIGNMENT = /^[A-Za-z_][A-Za-z0-9_]*=/;
const RESERVED = new Set([
  "if", "then", "else", "elif", "fi", "for", "while", "until", "do", "done", "case", "esac",
  "select", "function", "time", "coproc", "{", "}", "!", "[[", "]]", "((", "))",
]);
/** Commands that can end the script, skip later lines, change directory or rebind names. */
const STOP_COMMANDS = new Set([
  "exit", "return", "exec", "eval", "source", ".", "trap", "shopt", "alias", "unalias", "cd",
  "pushd", "popd", "builtin", "command", "enable", "unset", "break", "continue", "logout",
  "kill", "disown", "suspend", "wait", "hash",
]);

function isBenignSet(args: string[]) {
  let expectOption = false;
  for (const arg of args) {
    if (expectOption) {
      if (!["pipefail", "errexit", "nounset", "xtrace", "verbose"].includes(arg)) return false;
      expectOption = false;
      continue;
    }
    if (!/^-[euxvo]+$/.test(arg)) return false;
    if (arg.endsWith("o")) expectOption = true;
  }
  return !expectOption;
}

/** Index of the command word: leading `NAME=value` words are assignments even when the value expands. */
function commandWordIndex(words: Array<Extract<Token, { kind: "word" }>>) {
  let index = 0;
  while (index < words.length && ASSIGNMENT.test(words[index]!.raw)) index += 1;
  return index;
}

function toCommand(words: Array<Extract<Token, { kind: "word" }>>, line: number): ShellCommand {
  const start = commandWordIndex(words);
  return {
    words: words.slice(start).map((word) => (word.dynamic ? null : word.value)),
    line,
    text: words.map((word) => word.raw).join(" "),
  };
}

/**
 * Commands a `bash -e` step script provably executes, in order. A statement
 * counts only when it is one simple command with no `&&`, `||`, `;`, pipe,
 * background or subshell; the first statement the model cannot vouch for
 * stops the analysis.
 */
export function analyzeErrexitScript(text: string): ShellAnalysis {
  const executed: ShellCommand[] = [];
  const statements = splitStatements(tokenizeShell(text), false);
  const commands = statements.map((statement) => statement.commands.map((words) => toCommand(words, statement.line)));
  const seen = commands.flat();
  for (const [index, statement] of statements.entries()) {
    const stop = (reason: string) => ({ executed, seen, stoppedAt: { line: statement.line, reason } });
    if (statement.operators.length > 0 || statement.commands.length !== 1) {
      return stop(`operator ${statement.operators.join(" ") || "(none)"} can skip later lines or swallow an exit status`);
    }
    const words = statement.commands[0]!;
    const start = commandWordIndex(words);
    if (start === words.length) continue; // bare assignments
    const head = words[start]!;
    if (head.dynamic) return stop("command word expands at run time");
    if (RESERVED.has(head.value)) return stop(`control structure \`${head.value}\``);
    if (/^[A-Za-z_][A-Za-z0-9_]*\(\)$/.test(head.value) || words[start + 1]?.value === "()") {
      return stop("function definition");
    }
    if (STOP_COMMANDS.has(head.value)) return stop(`\`${head.value}\` can end or redirect the script`);
    if (head.value === "set" && !isBenignSet(words.slice(start + 1).map((word) => word.value))) {
      return stop("`set` may disable errexit");
    }
    executed.push(commands[index]![0]!);
  }
  return { executed, seen, stoppedAt: null };
}

/**
 * Commands of a package script (run by `sh -c`, no errexit) whose failure sets
 * the script's exit status: every command of the final `&&`-only list. Earlier
 * lists, `||`, pipes, background jobs and compound commands do not propagate.
 */
export function analyzePackageScript(text: string): ShellAnalysis {
  const statements = splitStatements(tokenizeShell(text), true);
  const commands = statements.map((statement) => statement.commands.map((words) => toCommand(words, statement.line)));
  const seen = commands.flat();
  const last = statements.at(-1);
  if (!last) return { executed: [], seen, stoppedAt: null };
  if (last.operators.some((operator) => operator !== "&&")) {
    return { executed: [], seen, stoppedAt: { line: last.line, reason: `operator ${last.operators.join(" ")} does not propagate every failure` } };
  }
  for (const words of last.commands) {
    const start = commandWordIndex(words);
    const head = words[start];
    if (!head || head.dynamic || RESERVED.has(head.value) || STOP_COMMANDS.has(head.value)) {
      return { executed: [], seen, stoppedAt: { line: last.line, reason: "unmodelled command in the final list" } };
    }
  }
  return { executed: commands.at(-1)!, seen, stoppedAt: null };
}
