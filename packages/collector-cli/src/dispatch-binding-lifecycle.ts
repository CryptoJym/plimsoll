import crypto from "node:crypto";
import { performance } from "node:perf_hooks";
import { z } from "zod";

const id = z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
/** A router must authenticate this attestation; parsing is not authentication. */
export const dispatchTerminalProofSchema = z.object({
  schema: z.literal("dispatch-terminal-proof/v1"), proofId: id,
  authority: z.literal("authenticated-dispatch-lifecycle/v1"), authorityEvidenceSha256: digest,
  terminalScope: z.enum(["native_thread_terminal", "attempt_irrevocably_retired"]),
  continuationAllowed: z.literal(false), sessionId: id, attemptId: id, workItemId: z.string().min(1).max(256),
  terminalAt: z.iso.datetime(), issuedAt: z.iso.datetime(), expectedProfileSha256: digest,
  bindings: z.array(z.object({ rootDigest: digest, bindingSha256: digest }).strict()).min(1).max(64),
}).strict();
export type DispatchTerminalProof = z.infer<typeof dispatchTerminalProofSchema>;
export type DispatchTerminalAuthority = {
  proof: unknown;
  /** Synchronous authenticated router verifier. No collector discovery of native authority. */
  verify: (proof: DispatchTerminalProof) => boolean;
};
export const dispatchBindingProofDigest = (binding: unknown) =>
  crypto.createHash("sha256").update(JSON.stringify(binding)).digest("hex");

export function verifiedDispatchTerminalProof(authority: DispatchTerminalAuthority | undefined,
  now: Date, sourceSha256: string) {
  if (!Number.isFinite(now.getTime())) throw new Error("dispatch_clock_invalid");
  if (!authority || typeof authority.verify !== "function") throw new Error("dispatch_terminal_proof_required");
  const proof = dispatchTerminalProofSchema.parse(authority.proof);
  const terminalAt = Date.parse(proof.terminalAt), issuedAt = Date.parse(proof.issuedAt);
  if (!Number.isFinite(terminalAt) || !Number.isFinite(issuedAt) || terminalAt > issuedAt || issuedAt > now.getTime())
    throw new Error("dispatch_terminal_proof_clock_invalid");
  if (proof.expectedProfileSha256 !== sourceSha256) throw new Error("dispatch_terminal_proof_profile_mismatch");
  if (new Set(proof.bindings.map(row => row.rootDigest)).size !== proof.bindings.length)
    throw new Error("dispatch_terminal_proof_duplicate_root");
  const started = performance.now();
  if (authority.verify(proof) !== true) throw new Error("dispatch_terminal_proof_not_authoritative");
  // A noncooperating verifier's late answer never authorizes publication.
  if (performance.now() - started > 100) throw new Error("dispatch_terminal_proof_deadline_exceeded");
  return proof;
}
