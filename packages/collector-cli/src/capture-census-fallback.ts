import { postDelivery } from "./delivery-post";
import type { DeliveryCaptureClaim } from "./outbox";

export const CAPTURE_CLAIM_HEADER = "x-plimsoll-capture";
let warnedCensusVersionMismatch = false;
const record = (value: unknown): Record<string, unknown> => value !== null &&
  typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};

export function isCaptureCensusSchemaRefusal(response: { status: number; body: unknown }) {
  const body = record(response.body);
  const claim = record(body.captureClaim);
  const watermark = record(body.captureWatermark);
  const refusals = [body, claim, watermark].filter(value => value.state === "refused");
  const claimOnlyRefusal = response.status >= 200 && response.status < 300 &&
    refusals.length > 0;
  if (![400, 422].includes(response.status) && !claimOnlyRefusal) return false;
  if (refusals.some(value => typeof value.reason === "string" && value.reason !== "" &&
      value.reason !== "claim_invalid")) return false;
  // Main's strict reader returns only claim_invalid, without the Zod issues.
  // Strip the optional census once; all other claim fields remain guarded.
  if (refusals.length > 0) return true;
  for (const issues of [body.issues, record(body.error).issues, claim.issues, watermark.issues]) {
    if (!Array.isArray(issues)) continue;
    for (const raw of issues.slice(0, 128)) {
      const issue = record(raw);
      if (!Array.isArray(issue.path) || issue.path.length > 32 || !issue.path.includes("gaps")) continue;
      // Zod reports an unknown key at its parent path, with the key in `keys`.
      if (issue.path.includes("deadLetters") || (issue.code === "unrecognized_keys" &&
          Array.isArray(issue.keys) && issue.keys.includes("deadLetters"))) return true;
    }
  }
  return false;
}

/** One definite claim/census-schema refusal permits one resend of the same body and
 * claim cursor. Both attempts are signed over their exact header bytes. */
export async function postDeliveryWithCaptureCensusFallback(
  input: Parameters<typeof postDelivery>[0] & { captureClaim?: DeliveryCaptureClaim | null },
) {
  const { captureClaim, ...transport } = input;
  const send = (claim: DeliveryCaptureClaim | null | undefined) => postDelivery({
    ...transport, headers: { ...transport.headers,
      ...(claim ? { [CAPTURE_CLAIM_HEADER]: JSON.stringify(claim) } : {}),
    },
  });
  const first = await send(captureClaim);
  if (!captureClaim?.gaps.some(gap => gap.deadLetters !== undefined) || !isCaptureCensusSchemaRefusal(first)) return first;
  if (!warnedCensusVersionMismatch) {
    warnedCensusVersionMismatch = true;
    console.warn(JSON.stringify({ status: "capture_claim_census_version_mismatch",
      action: "resend_without_dead_letters", requiredCloudChange: "capture_dead_letter_census" }));
  }
  const plain = { ...captureClaim, gaps: captureClaim.gaps.map(({ from, to }) => ({ from, to })) };
  try {
    const second = await send(plain);
    // A valid first acknowledgement remains authoritative if only its claim
    // was refused and the fallback cannot be confirmed.
    return second.ok || !first.ok ? second : first;
  } catch (error) {
    if (first.ok) return first;
    throw error;
  }
}
