import { createHash } from "node:crypto";

/** Stable UUIDs for proof roots whose labels must remain distinct across restarts. */
export function fixtureEpochId(label: string): string {
  const hash = createHash("sha256").update(`fixture-epoch:${label}`).digest("hex");
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-4${hash.slice(13, 16)}-8${hash.slice(17, 20)}-${hash.slice(20, 32)}`;
}
