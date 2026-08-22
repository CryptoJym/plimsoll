import type { ChildProcess } from "node:child_process";

export type Receipt = Record<string, unknown>;

export type WatchedChild = {
  child: ChildProcess;
  errors: string[];
  exit: { code: number | null; signal: NodeJS.Signals | null } | null;
  output: string;
  receipts: Receipt[];
};

export function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

export function watch(child: ChildProcess): WatchedChild {
  const watched: WatchedChild = {
    child,
    errors: [],
    exit: null,
    output: "",
    receipts: [],
  };
  let stdoutRemainder = "";
  let jsonBuffer = "";
  let stderr = "";
  const consumeLine = (line: string) => {
    const trimmed = line.trim();
    if (!jsonBuffer && !trimmed.startsWith("{")) return;
    jsonBuffer = jsonBuffer ? jsonBuffer + "\n" + line : line;
    try {
      watched.receipts.push(JSON.parse(jsonBuffer) as Receipt);
      jsonBuffer = "";
    } catch {
      // Pretty-printed JSON is complete only after its closing brace.
    }
  };
  child.stdout?.setEncoding("utf8");
  child.stderr?.setEncoding("utf8");
  child.stdout?.on("data", (chunk: string) => {
    watched.output += chunk;
    stdoutRemainder += chunk;
    const lines = stdoutRemainder.split("\n");
    stdoutRemainder = lines.pop() ?? "";
    for (const line of lines) consumeLine(line);
  });
  child.stderr?.on("data", (chunk: string) => {
    stderr += chunk;
    watched.errors.push(chunk);
  });
  child.on("error", (error) => {
    watched.errors.push("spawn_error: " + error.message);
  });
  child.on("exit", (code, signal) => {
    watched.exit = { code, signal };
  });
  // Finalize only on 'close': Node emits 'exit' as soon as the process is
  // reaped, but its stdio pipes may still hold undelivered data (they can
  // even remain open in an inheriting process). 'close' fires once the
  // process has ended AND all stdio streams are fully drained, so consuming
  // the trailing stdout line and flushing stderr here cannot race the
  // receipt stream.
  child.on("close", (code, signal) => {
    if (stdoutRemainder) consumeLine(stdoutRemainder);
    if (stderr.trim() && watched.errors.length === 0) watched.errors.push(stderr);
    watched.exit = { code, signal };
  });
  return watched;
}

export function waitFor(
  predicate: () => boolean,
  message: string,
  timeoutMs = 15_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    const poll = () => {
      if (predicate()) {
        resolve();
        return;
      }
      if (Date.now() > deadline) {
        reject(new Error(message));
        return;
      }
      setTimeout(poll, 25);
    };
    poll();
  });
}

export function waitForExit(child: ChildProcess, timeoutMs = 10_000) {
  // 'exit' alone does not guarantee the child's stdio pipes have been
  // drained; assertions that read collected receipts must wait for 'close'.
  const exitedAndDrained = () =>
    (child.exitCode !== null || child.signalCode !== null) &&
    (child.stdout === null || child.stdout.readableEnded || child.stdout.destroyed) &&
    (child.stderr === null || child.stderr.readableEnded || child.stderr.destroyed);
  if (exitedAndDrained()) {
    return Promise.resolve({ code: child.exitCode, signal: child.signalCode });
  }
  return new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Child did not exit in time.")), timeoutMs);
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal });
    });
  });
}
