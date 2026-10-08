import assert from "node:assert/strict";
import test from "node:test";

import { runLifecycleSnapshotCommand } from "../packages/collector-cli/src/lifecycle-command";
import type { LifecycleAdapter } from "../packages/collector-cli/src/lifecycle";

// Invalid invocations must fail before even reading the adapter or acquiring a lease.
const untouchedAdapter = new Proxy({} as LifecycleAdapter, {
  get() { throw new Error("adapter touched by invalid command"); },
});
for (const [name, argv, error] of [
  ["missing ID", [], /requires --id/],
  ["missing ID value", ["--id"], /distinct options/],
  ["another flag as ID", ["--id", "--apply"], /distinct options/],
  ["duplicated ID", ["--id", "a", "--id", "b"], /distinct options/],
  ["duplicate apply", ["--id", "a", "--apply", "--apply"], /unknown or repeated/],
  ["keep flag cannot target deletion", ["--id", "a", "--keep", "0"], /unknown or repeated/],
  ["unknown confirmation", ["--id", "a", "--confirm"], /unknown or repeated/],
  ["joined option", ["--id=a"], /unknown or repeated/],
  ["confirmation without apply", ["--id", "a", "--confirm-exact", "a"], /requires --apply/],
  ["traversal", ["--id", "../a"], /bounded identifier/],
  ["missing apply confirmation", ["--id", "a", "--apply"], /confirm-exact/],
  ["mismatched confirmation", ["--id", "a", "--apply", "--confirm-exact", "b"], /confirm-exact/],
  ["duplicate operation ID", ["--id", "a", "--operation-id", "x", "--operation-id", "y"], /distinct options/],
] as const) {
  test(`snapshot remove refuses ${name} without touching an adapter`, async () => {
    await assert.rejects(runLifecycleSnapshotCommand({ argv: ["snapshots", "remove", ...argv], adapter: untouchedAdapter }), error);
  });
}
