export type NativeProjectEvidence = {
  projectKey: string;
  projectLabel: string | null;
  state: string;
  evidence: string;
  observedEvents: number | null;
};

/** Explicit keys outrank repository suggestions; competing explicit keys or
 * competing repository suggestions cannot select a project. Every candidate
 * remains available for review, including lower-priority conflicts. */
export function nativeProjectLink(evidence: NativeProjectEvidence[]) {
  const candidates = evidence.filter(item => /^sha256:[a-f0-9]{64}$/.test(item.projectKey)).map(item => {
    const projectUrl = item.projectLabel && /^[a-z0-9.-]+\.[a-z]+\/[a-z0-9._-]+\/[a-z0-9._-]+$/.test(item.projectLabel)
      ? `https://${item.projectLabel}` : null;
    return { ...item, projectUrl, projectUrlState: projectUrl ? "inferred" as const : "UNKNOWN" as const };
  });
  const observed = [...new Set(candidates.filter(item => item.state === "observed").map(item => item.projectKey))];
  const inferred = [...new Set(candidates.filter(item => item.state === "inferred").map(item => item.projectKey))];
  const keys = [...new Set(candidates.map(item => item.projectKey))];
  const projectKey = observed.length === 1 ? observed[0] : observed.length === 0 && inferred.length === 1 && keys.length === 1 ? inferred[0] : null;
  const projectState = observed.length === 1 ? "observed" : projectKey ? "inferred" : "UNKNOWN";
  const labelled = projectKey ? candidates.find(item => item.projectKey === projectKey && item.projectUrl) : undefined;
  return {
    projectKey, projectState, projectLabel: labelled?.projectLabel ?? null,
    projectUrl: labelled?.projectUrl ?? null, projectUrlState: labelled?.projectUrlState ?? "UNKNOWN",
    projectConflict: keys.length > 1,
    projectCandidates: candidates,
  };
}
