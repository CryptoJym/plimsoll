/**
 * A server delay is a lower bound, never a reason to retry early.
 *
 * The date form names an instant on the *server's* clock, so it is measured
 * against the server's own `Date` header when the response carried one; a
 * collector whose clock is an hour slow would otherwise turn a ten-minute wait
 * into seventy minutes and persist it (review r1, F3). Without a usable `Date`
 * the local clock remains the reference, as before.
 */
export function retryAfterMilliseconds(value: string | null, nowMs: number, dateHeader: string | null = null): number {
  if (value === null || !Number.isFinite(nowMs)) return 0;
  const text = value.trim();
  let target: number;
  let reference = nowMs;
  if (/^\d+$/.test(text)) {
    const seconds = Number(text);
    if (!Number.isSafeInteger(seconds)) return 0;
    target = nowMs + seconds * 1_000;
  } else if (/^[A-Za-z]{3}, .+ GMT$/.test(text)) {
    target = Date.parse(text);
    const sentAt = dateHeader === null ? Number.NaN : Date.parse(dateHeader.trim());
    if (Number.isSafeInteger(sentAt)) reference = sentAt;
  } else return 0;
  // Keep the timestamp representable by Date and the persisted ISO date format.
  if (!Number.isSafeInteger(target) || target > 253402300799999) return 0;
  return Math.max(0, target - reference);
}
