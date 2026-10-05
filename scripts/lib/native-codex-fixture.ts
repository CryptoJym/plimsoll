/** Native request attributes for proofs of transport/storage, not pairing.
 * A bare normalized model could be a .48 proximity guess. Each synthetic
 * request therefore reports its own model inside its unique native trace.
 * No derived capture decision or frozen-accounting witness is injected. */
export function nativeCodexFixture(id: string, model = "gpt-6-sol") {
  const traceId = createHash("sha256").update(`native-fixture:${id}`).digest("hex").slice(0, 32);
  return { model, metadata: { model, traceId } };
}
import { createHash } from "node:crypto";
