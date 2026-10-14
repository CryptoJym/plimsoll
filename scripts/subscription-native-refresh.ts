import { refreshSubscriptionTelemetry } from "../packages/collector-cli/src/subscription-refresh";

try {
  console.log(JSON.stringify(refreshSubscriptionTelemetry()));
} catch {
  // No source values, native identifiers, or exception text enter service logs.
  console.error(JSON.stringify({status:"UNKNOWN",reason:"subscription_refresh_failed",observedAt:new Date().toISOString()}));
  process.exitCode=1;
}
