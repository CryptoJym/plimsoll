import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import {
  ACCOUNT_ASSERTION_SALT_FILE, deriveAccountActorHash, hashBindingRecord,
  readAccountAssertionSaltForTenant,
} from "./account-assertion";

export const NATIVE_ACCOUNT_DERIVATION = "account_assertion_actor_v1" as const;
export const NATIVE_PROVIDER_ACCOUNT_DERIVATION = "native_0_7_47_provider_account_boxed_sha16" as const;
export type NativeAccountDerivation = typeof NATIVE_ACCOUNT_DERIVATION | typeof NATIVE_PROVIDER_ACCOUNT_DERIVATION;
export type NativeIdentityProvenance = "native_feed_provider_account_id" | "registered_claude_profile_account_uuid" | "matching_local_claude_account_uuid";
export type NativeAccountLinkEvidence = {
  source: "codex" | "claude_code";
  accountRef: string;
  accountHash: string | null;
  state: "observed" | "UNKNOWN";
  reason: string | null;
  derivation: NativeAccountDerivation | null;
  identityProvenance?: NativeIdentityProvenance | null;
  evidenceRef: string;
  observedAt: string | null;
};

export function capturedAccountHash(value: unknown): string | null {
  if (typeof value !== "string" || !/^(?:sha256:[a-f0-9]{16}|(?:sha256:)?[a-f0-9]{64})$/i.test(value)) return null;
  return `sha256:${value.replace(/^sha256:/i, "").toLowerCase()}`;
}

function validNativeIdentity(value: unknown): value is string {
  return typeof value === "string" && Boolean(value) && value === value.trim() && value.length <= 512 &&
    !value.includes("@") && !/[\u0000-\u001f]/.test(value) && !/^(?:sk-|tok_|bearer\s|eyJ[A-Za-z0-9_-]+\.)/i.test(value);
}

/** The installed 0.7.47 providerAccountKey protocol, without variants or salt. */
export function deriveNativeProviderAccountHash(rawId: string): string {
  if (!validNativeIdentity(rawId)) throw new Error("native_provider_account_id_invalid");
  return `sha256:${crypto.createHash("sha256").update(JSON.stringify({ stringValue: rawId })).digest("hex").slice(0,16)}`;
}

type NativeIdentityOptions = {
  provider: string; accountRef: string | null; nativeIdentity: unknown;
  authenticated: boolean; matchesExpected: unknown; proof: string | null;
  observedAt: string | null; now: string;
  identityReason?: string | null; identityProvenance?: NativeIdentityProvenance | null;
};

function identityGate(options: NativeIdentityOptions): string | null {
  const age = options.observedAt ? Date.parse(options.now) - Date.parse(options.observedAt) : NaN;
  return options.matchesExpected === false ? "conflicting_native_identity" :
    options.identityReason ?? (!validNativeIdentity(options.nativeIdentity) ? "missing_or_invalid_native_identity" :
      options.provider === "anthropic" && !provedClaudeUuid(options.identityProvenance) ? "unproved_claude_account_uuid" :
      !options.authenticated || !options.proof || options.proof === "configured_only" || !Number.isFinite(age) || age < -300_000 ? "unproved_native_identity" : null);
}

function provedClaudeUuid(provenance: unknown): boolean {
  return provenance === "registered_claude_profile_account_uuid" || provenance === "matching_local_claude_account_uuid";
}

export function nativeProviderAccountLinkEvidence(options: NativeIdentityOptions): NativeAccountLinkEvidence | null {
  const source = options.provider === "openai" ? "codex" : options.provider === "anthropic" ? "claude_code" : null;
  if (!source || !options.accountRef) return null;
  const accountHash = validNativeIdentity(options.nativeIdentity) ? deriveNativeProviderAccountHash(options.nativeIdentity) : null;
  const reason = identityGate(options);
  const evidence: Omit<NativeAccountLinkEvidence,"evidenceRef"> = {
    source, accountRef: options.accountRef, accountHash, reason,
    state: reason === null ? "observed" : "UNKNOWN",
    derivation: accountHash ? NATIVE_PROVIDER_ACCOUNT_DERIVATION : null,
    identityProvenance: options.identityProvenance ?? null, observedAt: options.observedAt,
  };
  return { ...evidence, evidenceRef: hashBindingRecord({ ...evidence, proof: options.proof }) };
}

/** The existing reader checks ownership/modes. Preflight prevents its directory
 * helper from creating an absent home. No salt is minted, stored or returned to
 * inspection/report callers. */
export function readExistingNativeAccountSalt(home: string): Buffer | null {
  try {
    const directory = fs.lstatSync(home);
    const file = fs.lstatSync(path.join(home, ACCOUNT_ASSERTION_SALT_FILE));
    if (!directory.isDirectory() || directory.isSymbolicLink() || !file.isFile() || file.isSymbolicLink()) return null;
    return readAccountAssertionSaltForTenant(home);
  } catch { return null; }
}

/** A native feed proves only identity equivalence. Historical activity still
 * requires the event's own captured hash; login time/email/account labels do
 * not participate. The hash function is the capture assertion's exact one. */
export function nativeAccountLinkEvidence(options: NativeIdentityOptions, salt: Buffer | null): NativeAccountLinkEvidence | null {
  const source = options.provider === "openai" ? "codex" : options.provider === "anthropic" ? "claude_code" : null;
  if (!source || !options.accountRef) return null;
  const identityValid = validNativeIdentity(options.nativeIdentity);
  let reason = identityGate(options) ?? (!salt ? "missing_tenant_salt" : null);
  let accountHash: string | null = null;
  if (identityValid && salt) {
    try { accountHash = deriveAccountActorHash(options.nativeIdentity as string, salt); }
    catch { reason = "missing_tenant_salt"; }
  }
  const evidence: Omit<NativeAccountLinkEvidence, "evidenceRef"> = {
    source, accountRef: options.accountRef, accountHash,
    state: reason === null ? "observed" as const : "UNKNOWN" as const,
    reason, derivation: accountHash ? NATIVE_ACCOUNT_DERIVATION : null,
    identityProvenance: options.identityProvenance ?? null,
    observedAt: options.observedAt,
  };
  return { ...evidence, evidenceRef: hashBindingRecord({ ...evidence, proof: options.proof }) };
}

export function nativeAccountLinkResolver(evidence: NativeAccountLinkEvidence[]) {
  const byHash = new Map<string, NativeAccountLinkEvidence[]>();
  const matchingEvidence = new Map<string, NativeAccountLinkEvidence[]>();
  const byRef = new Map<string, Set<string>>();
  const conflicts = new Set<string>();
  const providersByHash = new Map<string, Set<string>>();
  for (const claim of evidence) {
    if (!["codex", "claude_code"].includes(claim.source)) continue;
    const refKey = JSON.stringify([claim.source, claim.accountRef]);
    if (claim.reason === "conflicting_native_identity") conflicts.add(refKey);
    // Pre-repair Claude claims hashed organizationId. They cannot certify the
    // personal account UUID, even if their old observation said "observed".
    if (claim.source === "claude_code" && !provedClaudeUuid(claim.identityProvenance)) continue;
    const accountHash = capturedAccountHash(claim.accountHash);
    if (!accountHash || !validDerivation(accountHash,claim.derivation)) continue;
    const key = JSON.stringify([claim.source, claim.derivation, accountHash]);
    matchingEvidence.set(key, [...(matchingEvidence.get(key) ?? []), claim]);
    if (claim.state !== "observed" || claim.reason !== null) continue;
    byHash.set(key, [...(byHash.get(key) ?? []), claim]);
    const semanticRefKey = JSON.stringify([claim.source,claim.accountRef,claim.derivation]);
    const hashes = byRef.get(semanticRefKey) ?? new Set<string>();
    hashes.add(accountHash); byRef.set(semanticRefKey, hashes);
    const semanticHashKey = JSON.stringify([claim.derivation,accountHash]);
    const providers = providersByHash.get(semanticHashKey) ?? new Set<string>();
    providers.add(claim.source); providersByHash.set(semanticHashKey, providers);
  }
  return (source: string, value: unknown, eventConflict = false, capturedDerivation?: NativeAccountDerivation | null) => {
    const accountHash = capturedAccountHash(value);
    // Legacy full assertion hashes have an established namespace. Only Codex
    // may value-match its legacy short actor; Claude's short user.id is generic.
    const derivation = capturedDerivation === undefined ?
      accountHash?.length === 71 ? NATIVE_ACCOUNT_DERIVATION : source === "codex" ? NATIVE_PROVIDER_ACCOUNT_DERIVATION : null : capturedDerivation;
    const key = JSON.stringify([source,derivation,accountHash]);
    const claims = accountHash && validDerivation(accountHash,derivation) ? byHash.get(key) ?? [] : [];
    const matching = accountHash && validDerivation(accountHash,derivation) ? matchingEvidence.get(key) ?? [] : [];
    const candidateAccountRefs = [...new Set(claims.map(claim => claim.accountRef))].sort();
    const reason = eventConflict ? "conflicting_captured_identity" : !accountHash ? "missing_captured_identity" :
      !["codex", "claude_code"].includes(source) ? "unsupported_source" :
      !validDerivation(accountHash,derivation) ? "missing_captured_account_evidence" :
      candidateAccountRefs.length > 1 ? "ambiguous_native_identity" :
      matching.some(claim => conflicts.has(JSON.stringify([source, claim.accountRef])) ||
        (byRef.get(JSON.stringify([source, claim.accountRef,derivation]))?.size ?? 0) > 1) ? "conflicting_native_identity" :
      !claims.length ? providersByHash.has(JSON.stringify([derivation,accountHash])) ? "cross_provider_identity" : "no_proven_native_identity" : null;
    return {
      accountHash, accountDerivation: derivation, accountRef: reason === null ? candidateAccountRefs[0] : null,
      subscriptionAccountJoin: reason === null ? "observed" as const : "UNKNOWN" as const,
      unknownReason: reason, candidateAccountRefs, accountLinkEvidence: matching,
    };
  };
}

function validDerivation(hash: string | null, derivation: NativeAccountDerivation | null): boolean {
  return hash !== null && (derivation === NATIVE_ACCOUNT_DERIVATION && hash.length === 71 ||
    derivation === NATIVE_PROVIDER_ACCOUNT_DERIVATION && hash.length === 23);
}
