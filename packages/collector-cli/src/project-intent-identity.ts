import fs from "node:fs";
import path from "node:path";
import { linkageHash } from "../../shared/src/linkage";
import { projectIntentReceiptSchema, type ProjectIntentReceipt } from "../../shared/src/project-intent";

export const INTENT_EVIDENCE_FIELDS = [
  "receiptId", "installId", "source", "sourceRootKey", "accountKey", "sessionId",
  "nativeSessionKey", "sessionEpochKey", "rootAttemptId", "attemptId", "parentAttemptId",
  "workItemKey", "projectKey", "projectRegistryRevision", "observedRepoKey",
  "effectiveFrom", "effectiveUntil", "basis", "adapterId", "adapterVersion",
] as const;
export type IntentSource = ProjectIntentReceipt["source"];
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export class IntentError extends Error {
  constructor(readonly code: string) { super(code); }
}

export function canonicalIdentity(value: string): string {
  if (!value || value.length > 4096 || /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(value))
    throw new IntentError("invalid_identity");
  return UUID.test(value) ? value.toLowerCase() : value.normalize("NFC");
}

export function intentDigest(kind: string, parts: readonly (string | number | null)[]): string {
  for (const part of parts) if (typeof part === "string") canonicalIdentity(part);
  return linkageHash(`plimsoll:project-intent:v1:${kind}\0${JSON.stringify(parts)}`)!;
}

/** Only the provider's actual state directory; never cwd or a repository. */
export function canonicalSourceRoot(directory: string | undefined): string | null {
  if (!directory) return null;
  if (!path.isAbsolute(directory)) throw new IntentError("source_root_must_be_absolute");
  try {
    let root = fs.realpathSync.native(directory);
    if (!fs.statSync(root).isDirectory()) return null;
    if (process.platform === "win32") {
      root = root.replace(/^\\\\\?\\UNC\\/, "\\\\").replace(/^\\\\\?\\/, "").replace(/\\/g, "/");
      root = root.replace(/^[A-Z]:/, drive => drive.toLowerCase());
    }
    return canonicalIdentity(root);
  } catch (error) {
    if (error instanceof IntentError) throw error;
    return null;
  }
}

export function intentAccountKey(source: IntentSource, principal: string | undefined): string | null {
  if (!principal) return null;
  if (principal.includes("@")) throw new IntentError("account_requires_opaque_principal");
  const realm = { codex: "openai", claude_code: "anthropic", gemini_cli: "google", grok: "xai" }[source];
  return intentDigest("account", [source, realm, canonicalIdentity(principal)]);
}

export function intentWorkItemKey(work: { authority: string; namespace: string; id: string } | undefined): string | null {
  if (!work) return null;
  if (!/^[a-z][a-z0-9-]{0,47}$/.test(work.authority) || /^[\/\\]|^[A-Za-z]:/.test(work.namespace))
    throw new IntentError("invalid_work_identity");
  return intentDigest("work-item", [work.authority, canonicalIdentity(work.namespace), canonicalIdentity(work.id)]);
}

/** Canonicalize every wire fact before hashing; evidence never hashes raw inputs. */
export function sealIntentReceipt(facts: Omit<ProjectIntentReceipt, "evidenceRef">): ProjectIntentReceipt {
  const canonical = projectIntentReceiptSchema.parse({ ...facts, evidenceRef: `sha256:${"0".repeat(64)}` });
  canonical.evidenceRef = intentDigest("evidence", INTENT_EVIDENCE_FIELDS.map(field => canonical[field]));
  return canonical;
}
