import type { ChildProcess } from "node:child_process";

/**
 * Bounded, path-free symbolic classes for the duplicate-start fixture's owner
 * shutdown stages (#162). Every terminal state of a fixture child must map to
 * exactly one of these classes or to a clean reap; none of them may carry
 * filesystem paths, PIDs, or free-form error text.
 */
export const OWNER_SHUTDOWN_FAILURE_CLASSES = [
  "OwnerEarlyExit",
  "StopCommandFailure",
  "OwnerShutdownTimeout",
  "CleanupFailure",
] as const;

export type OwnerShutdownFailureClass = (typeof OWNER_SHUTDOWN_FAILURE_CLASSES)[number];

/**
 * Proof-only symbolic error: `name` and `message` are both the bounded code,
 * so receipts can report `error.name` without leaking paths or raw text.
 * Fixes #162's "symbolic Error" receipts where `error.name` was literally
 * `"Error"` for every deadline failure.
 */
export class SymbolicProofError extends Error {
  code: string;
  constructor(code: string) {
    super(code);
    this.code = code;
    this.name = code;
  }
}

export function withSymbolicDeadline<T>(
  promise: Promise<T>,
  milliseconds: number,
  code: string,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new SymbolicProofError(code)), milliseconds);
    promise.then(
      (value) => {
        clearTimeout(timeout);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timeout);
        reject(error);
      },
    );
  });
}

function boundedSleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    // Deliberately ref'd: these budgets bound cleanup escalation, so they
    // must keep the event loop alive until they fire. An unref'd timer would
    // let Node exit mid-reap and silently abandon an unreaped child.
    setTimeout(resolve, milliseconds);
  });
}

export type ChildExitObservation =
  | { settled: true; code: number | null; signal: string | null }
  | { settled: false };

/**
 * Observe a fixture child's exit inside a bounded budget without ever
 * throwing. `{ settled: false }` means the child was still running when the
 * budget expired; reaping remains the caller's responsibility.
 */
export async function observeChildExit(
  exit: Promise<{ code: number | null; signal: NodeJS.Signals | null }>,
  milliseconds: number,
): Promise<ChildExitObservation> {
  try {
    const outcome = await withSymbolicDeadline(exit, milliseconds, "ChildExitObservationTimeout");
    return { settled: true, code: outcome.code, signal: outcome.signal };
  } catch (error) {
    if (error instanceof SymbolicProofError) return { settled: false };
    throw error;
  }
}

export type ReapOutcome =
  | "already_exited"
  | "reaped_after_term"
  | "reaped_after_kill"
  | "CleanupFailure";

export type ReapBudgets = { termMs: number; killMs: number };

/** Defaults mirror the previous boundedChildCleanup escalation windows. */
const DEFAULT_REAP_BUDGETS: ReapBudgets = { termMs: 3_000, killMs: 3_000 };

/**
 * Await and reap one fixture child with bounded SIGTERM -> SIGKILL
 * escalation on every path. Never throws; a child that survives SIGKILL
 * within the combined budget is reported as CleanupFailure so residue can
 * never pass silently.
 */
export async function reapFixtureChild(
  child: ChildProcess,
  exit: Promise<{ code: number | null; signal: NodeJS.Signals | null }>,
  budgets: Partial<ReapBudgets> = {},
): Promise<ReapOutcome> {
  const resolvedBudgets = { ...DEFAULT_REAP_BUDGETS, ...budgets };
  let exited = false;
  const onceExited = exit.then(
    () => {
      exited = true;
    },
    () => {
      exited = true;
    },
  );
  if (child.exitCode !== null || child.signalCode !== null) return "already_exited";
  try {
    child.kill("SIGTERM");
  } catch {
    // Fall through to the bounded waits; a kill failure surfaces as timeout.
  }
  await Promise.race([onceExited, boundedSleep(resolvedBudgets.termMs)]);
  if (exited || child.exitCode !== null || child.signalCode !== null) return "reaped_after_term";
  try {
    child.kill("SIGKILL");
  } catch {
    // Only the bounded waits remain; report honestly below.
  }
  await Promise.race([onceExited, boundedSleep(resolvedBudgets.killMs)]);
  if (exited || child.exitCode !== null || child.signalCode !== null) return "reaped_after_kill";
  return "CleanupFailure";
}

export type StopCommandObservation = {
  stopperSettled: boolean;
  stopperExitCode: number | null;
  stopperSignal: string | null;
  stopperReceiptParsed: boolean;
  stopReceiptReportedStopped: boolean;
};

export type StopCommandFailureReason =
  | "StopCommandTimeout"
  | "StopCommandExitNonZero"
  | "StopCommandSignalled"
  | "StopReceiptUnparseable"
  | "StopReceiptNotStopped";

export type StopCommandClassification =
  | { failed: false }
  | { failed: true; class: "StopCommandFailure"; reason: StopCommandFailureReason };

/** Classify the packaged stop command itself, independent of owner liveness. */
export function classifyStopCommand(
  observation: StopCommandObservation,
): StopCommandClassification {
  if (!observation.stopperSettled) {
    return { failed: true, class: "StopCommandFailure", reason: "StopCommandTimeout" };
  }
  if (observation.stopperSignal !== null) {
    return { failed: true, class: "StopCommandFailure", reason: "StopCommandSignalled" };
  }
  if (!observation.stopperReceiptParsed) {
    return { failed: true, class: "StopCommandFailure", reason: "StopReceiptUnparseable" };
  }
  if (observation.stopperExitCode !== 0) {
    return { failed: true, class: "StopCommandFailure", reason: "StopCommandExitNonZero" };
  }
  if (!observation.stopReceiptReportedStopped) {
    return { failed: true, class: "StopCommandFailure", reason: "StopReceiptNotStopped" };
  }
  return { failed: false };
}

export type OwnerShutdownClassification = {
  failureClass: OwnerShutdownFailureClass | null;
  reason: string;
};

/**
 * Classify the owner shutdown stage from plain observables, in causal order:
 * an owner that died before any stop existed is OwnerEarlyExit even when the
 * later stop reports success; otherwise a failed stop command explains the
 * shutdown; only a successful stop followed by a live owner past its bounded
 * budget is OwnerShutdownTimeout.
 */
export function classifyOwnerShutdown(input: {
  ownerExitedBeforeStopSpawned: boolean;
  stopFailed: boolean;
  stopFailureReason: string | null;
  ownerExitSettled: boolean;
}): OwnerShutdownClassification {
  if (input.ownerExitedBeforeStopSpawned) {
    return { failureClass: "OwnerEarlyExit", reason: "owner_exited_before_stop_command_spawned" };
  }
  if (input.stopFailed) {
    return { failureClass: "StopCommandFailure", reason: input.stopFailureReason ?? "unspecified" };
  }
  if (!input.ownerExitSettled) {
    return {
      failureClass: "OwnerShutdownTimeout",
      reason: "owner_live_after_bounded_shutdown_budget",
    };
  }
  return { failureClass: null, reason: "none" };
}
