/** A server delay is a lower bound, never a reason to retry early. */
export function retryAfterMilliseconds(value: string | null, nowMs: number): number {
  if (value === null || !Number.isFinite(nowMs)) return 0;
  const text = value.trim();
  let target: number;
  if (/^\d+$/.test(text)) {
    const seconds = Number(text);
    if (!Number.isSafeInteger(seconds)) return 0;
    target = nowMs + seconds * 1_000;
  } else if (/^[A-Za-z]{3}, .+ GMT$/.test(text)) target = Date.parse(text);
  else return 0;
  // Keep the timestamp representable by Date and the persisted ISO date format.
  if (!Number.isSafeInteger(target) || target > 253402300799999) return 0;
  return Math.max(0, target - nowMs);
}
