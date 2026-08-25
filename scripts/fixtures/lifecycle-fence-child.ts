/**
 * Cross-process fence-race child for the lifecycle-fence proof.
 * Acquires and releases COUNT leases from the authority root in argv[2],
 * printing one JSON line per acquired revision. No other output.
 */
import { LifecycleMutationAuthority } from "../../packages/collector-cli/src/lifecycle-authority";

const root = process.argv[2];
const count = Number(process.argv[3] ?? "1");
if (!root || !Number.isInteger(count) || count < 1) {
  console.error("usage: child-acquire ROOT COUNT");
  process.exit(2);
}
const authority = new LifecycleMutationAuthority(root, { defaultLeaseMs: 30_000 });
for (let index = 0; index < count; index += 1) {
  const acquisition = authority.acquire();
  if (acquisition.kind !== "acquired") {
    console.log(JSON.stringify({ kind: acquisition.kind }));
    process.exit(1);
  }
  const revision = acquisition.lease.revision;
  const released = acquisition.lease.release();
  if (released !== "released") {
    console.log(JSON.stringify({ kind: "release_failed", released }));
    process.exit(1);
  }
  console.log(JSON.stringify({ kind: "acquired", revision }));
}
