/**
 * Bead eco-6hoxj.68, review r1 F1 — the measured spelling corpus behind the
 * hook-spool exemption, in ONE place, used by both the page that discloses it
 * (`scripts/privacy-spec.ts`) and the check that measures it
 * (`scripts/hook-spool-proof.ts`).
 *
 * Why the lists live here and not in `packages/collector-cli/src/hook-spool.ts`
 * with the rule they exemplify: they are not the rule. The rule is
 * `spoolKeepsProtectedIdentityRaw`, which is keyed on a normalizer and covers
 * infinitely many spellings; these are the spellings someone actually drove
 * through it, kept so the page can print a name an operator will recognize and
 * the proof can measure the same names the page prints. Nothing here is
 * consulted at runtime.
 *
 * The two classes exist because the collector's two rules cut a name
 * differently. `isSensitiveMetadataSemanticKey` (the DROP rule) splits a key
 * into WORDS and matches a word; `isProtectedMetadataFieldName` (the hash rule,
 * and rule 2 of the spool exemption) deletes every separator and lowercases.
 * So the same protected name lands on opposite sides depending on how it is
 * spelled, and the spool follows the ledger either way rather than picking one.
 */

/**
 * Spellings of a protected path/email name that the DROP rule's WORD SPLIT
 * still reaches. `sanitizeForPolicy` drops these outright, so the spool empties
 * them: they never reach the ledger's hash branch and nothing is lost by
 * emptying them.
 */
export const SPOOL_WORD_SPLIT_DROPPED_SPELLINGS: readonly string[] = [
  "user.email",
  "userEmail",
  "user_email",
  "EMAIL",
  "email_address",
  "emailAddress",
  "account_email",
  "accountEmail",
  "actor_email",
  "actorEmail",
  "owner_email",
  "ownerEmail",
  "transcript_path",
  "transcriptPath",
  "file_path",
  "filePath",
  "FILE_PATH",
  "full_path",
  "fullPath",
  "project_path",
  "projectPath",
  "repo_path",
  "repoPath",
  "repository_url",
  "repositoryUrl",
  "workspace_path",
  "workspacePath",
];

/**
 * The other side of the same names, and the defect review r1 F1 found: a
 * spelling with no separator for the word split to cut on. The DROP rule does
 * NOT fire, `isProtectedMetadataFieldName` DOES match, so the ledger HASHES the
 * value — and the spool therefore holds it raw, for exactly the fidelity reason
 * it holds `account_id` raw. An email address can rest in a spool file under
 * one of these spellings; that is disclosed rather than denied.
 *
 * The fifteen review r1 measured at `24606c3a`.
 */
export const SPOOL_UNSPLIT_PROTECTED_SPELLINGS: readonly string[] = [
  "USEREMAIL",
  "useremail",
  "e_mail",
  "EMAILADDRESS",
  "ACCOUNTEMAIL",
  "OWNEREMAIL",
  "TRANSCRIPTPATH",
  "transcriptpath",
  "FULLPATH",
  "PROJECTPATH",
  "REPOPATH",
  "WORKSPACEPATH",
  "REPOSITORYURL",
  "repositoryurl",
  "cWd",
];

/**
 * Spellings added in r2 to make the mirror check's corpus adversarial rather
 * than confirmatory. Each one is here because it lands somewhere the two lists
 * above do not:
 *
 *   - `CWD`, `WORKDIR` — case variants of an exact-name derivation input. Rule
 *     1 is exact, so it does not cover them; the DROP rule's word split does,
 *     and they are emptied. This is what rule 1's "a case variant is not a
 *     derivation input" costs, measured.
 *   - `workingdirectory`, `CURRENTWORKINGDIRECTORY` — the SAME derivation
 *     names spelled without separators, which is `cWd`'s case generalized:
 *     hashed by the ledger, held raw by the spool.
 *   - `org--id`, `AcCoUnT_Id`, `e-mail` — exempt under rule 2 alone, by an
 *     unusual separator, by case, and a protected name that is a near-miss for
 *     the word `email` respectively.
 *   - `user.0.id`, `u s e r n a m e`, `user@email`, `Transcript-Path` —
 *     separators that take a key AWAY from the protected set or into the DROP
 *     rule; all four are emptied.
 *   - `account_identifier`, `USEREMAILADDRESS` — near-misses the ledger neither
 *     drops nor hashes. It keeps them PLAIN, so the spool holding them raw is
 *     still a mirror; this is the class the page discloses as a value the
 *     ledger keeps and the metadata admission may later discard.
 */
export const SPOOL_MIRROR_PROBE_SPELLINGS: readonly string[] = [
  "CWD",
  "WORKDIR",
  "workingdirectory",
  "CURRENTWORKINGDIRECTORY",
  "org--id",
  "AcCoUnT_Id",
  "e-mail",
  "user.0.id",
  "u s e r n a m e",
  "user@email",
  "Transcript-Path",
  "account_identifier",
  "USEREMAILADDRESS",
];

/**
 * Review r1 F2 — the rule-2 disclosure, TYPED BY HAND on purpose.
 *
 * `SPOOL_PROTECTED_IDENTITY_KEYS` derives the canonical names rule 2 exempts
 * from `protectedMetadataFieldNames` by running the predicate over it, and the
 * privacy-spec table renders that derivation. Both move on their own the moment
 * someone adds a name to the shared list, which is why r1's completeness check
 * could not fail: every term in it was derived from the same code it was
 * checking.
 *
 * This list is the human decision that derivation is measured against — the
 * canonical identity names a person has agreed may rest RAW in a spool file
 * until the drain applies them. It is not generated and must not be: adding a
 * protected identity name to `packages/shared/src/policy.ts` must FAIL
 * `r_every_protected_identity_name_is_blanked_or_declared` until someone adds
 * it here too, and regenerating `docs/privacy-spec.md` must not be enough to
 * make that failure go away.
 */
export const SPOOL_RULE_TWO_DISCLOSED_CANONICAL_NAMES: readonly string[] = [
  "account_id",
  "account_uuid",
  "actor_id",
  "organization_id",
  "org_id",
  "workspace_root",
  "user.account_id",
  "user.account_uuid",
  "user.id",
  "user_id",
  "username",
];

/**
 * The ledger's own name-matching, copied — every non-alphanumeric character
 * removed, then lowercased. It must agree with `normalizeFieldName`
 * (`packages/shared/src/policy.ts`), which is not exported; rather than trust
 * that, `r_every_protected_identity_name_is_blanked_or_declared` asserts this
 * copy reproduces `isProtectedMetadataFieldName`'s verdict over every spelling
 * in the corpus, so a change to the real normalizer fails the check instead of
 * quietly widening the hand-maintained disclosure above.
 */
export function normalizedSpelling(field: string) {
  return field.replace(/[^a-zA-Z0-9]/g, "").toLowerCase();
}
