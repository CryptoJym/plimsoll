import { spawnSync } from "node:child_process";
import { refreshSubscriptionTelemetry } from "../packages/collector-cli/src/subscription-refresh";

// Every run is bounded. The refresh is synchronous SQLite work, so no timer in
// this process could interrupt a stalled read; the parent runs the work in a
// child and kills it at the bound. launchd then starts the next minute's run
// instead of leaving one process live for hours (studio0, 2026-10-31 and
// 2026-11-01: runs sat in throttled disk reads for 100+ minutes).
const RUN_BOUND_MS = 10 * 60 * 1000;
const CHILD_FLAG = "PLIMSOLL_SUBSCRIPTION_REFRESH_CHILD";

if (process.env[CHILD_FLAG] === "1") {
  try {
    console.log(JSON.stringify(refreshSubscriptionTelemetry()));
  } catch {
    // No source values, native identifiers, or exception text enter service logs.
    console.error(JSON.stringify({status:"UNKNOWN",reason:"subscription_refresh_failed",observedAt:new Date().toISOString()}));
    process.exitCode=1;
  }
} else {
  const startedAt = new Date().toISOString();
  const child = spawnSync(process.execPath, [process.argv[1]!], {
    env: { ...process.env, [CHILD_FLAG]: "1" }, stdio: "inherit",
    timeout: RUN_BOUND_MS, killSignal: "SIGKILL",
  });
  if (child.error && (child.error as NodeJS.ErrnoException).code === "ETIMEDOUT") {
    console.error(JSON.stringify({status:"UNKNOWN",reason:"subscription_refresh_timeout",boundSeconds:RUN_BOUND_MS/1000,startedAt,observedAt:new Date().toISOString()}));
    process.exitCode=1;
  } else if (child.error) {
    console.error(JSON.stringify({status:"UNKNOWN",reason:"subscription_refresh_child_failed",observedAt:new Date().toISOString()}));
    process.exitCode=1;
  } else {
    process.exitCode = child.status ?? 1;
  }
}
