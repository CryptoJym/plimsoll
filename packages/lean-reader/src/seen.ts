import type { State } from './types.js';

// 8-byte SHA-256 prefixes since state version 3: a busy machine holds about
// 400,000 IDs over the 8-day window, where a false match is near 1 in 10^9.
// Version 2 stored 12 bytes and filled two thirds of the 9 MiB state budget.
const WIDTH = 8, LEGACY_WIDTH = 12;
// SHA-256 prefixes, sorted inside UTC-hour buckets. Old JSON ID maps still
// read. Entire expired/backfilled hours can be removed without decoding IDs.
// IDs added during a pass stay out of the state until finish() packs them, so
// a pass is bounded by the size it will save, not by a JSON map in flight.

/** Rewrites version 2's 12-byte buckets as 8-byte prefixes, once, for both ID
 * sets whether or not evidence is enabled now. Prefixes of sorted IDs stay
 * sorted. A reader older than version 3 refuses the narrowed state. */
export function narrowPacked(state: State): void {
  if (state.version === 3) return;
  for (const field of ['seenPacked', 'evidenceSeenPacked'] as const) {
    for (const [hour, packed] of Object.entries(state[field] ?? {})) {
      const data = Buffer.from(packed, 'base64');
      if (data.length % LEGACY_WIDTH) throw new Error('invalid_state');
      const next = Buffer.allocUnsafe(data.length / LEGACY_WIDTH * WIDTH);
      for (let at = 0, write = 0; at < data.length; at += LEGACY_WIDTH, write += WIDTH) data.copy(next, write, at, at + WIDTH);
      state[field]![hour] = next.toString('base64');
    }
  }
  state.version = 3;
}

export class Seen {
  private buckets: Record<string, string>;
  private decoded = new Map<string, Buffer>();
  private added = new Map<string, number>();
  constructor(private state: State, private field: 'seen' | 'evidenceSeen', private packedField: 'seenPacked' | 'evidenceSeenPacked', cutoff: number) {
    this.buckets = state[packedField] ?? {};
    for (const [id, hour] of Object.entries(state[field] ?? {})) this.added.set(id, hour);
    state[field] = {};
    this.remove(hour => hour * 3_600_000 < cutoff);
  }
  /** Bytes the IDs added so far will take once packed: base64 of WIDTH bytes each. */
  get pendingBytes(): number { return this.added.size * WIDTH / 3 * 4; }
  private buffer(hour: string): Buffer {
    let data = this.decoded.get(hour);
    if (!data) {
      data = Buffer.from(this.buckets[hour] ?? '', 'base64');
      if (data.length % WIDTH) throw new Error('invalid_state');
      this.decoded.set(hour, data);
    }
    return data;
  }
  has(id: string): boolean {
    if (this.added.has(id)) return true;
    const key = Buffer.from(id, 'hex');
    for (const hour in this.buckets) {
      const data = this.buffer(hour);
      let low = 0, high = data.length / WIDTH - 1;
      while (low <= high) {
        const mid = (low + high) >>> 1, at = mid * WIDTH;
        const comparison = data.compare(key, 0, WIDTH, at, at + WIDTH);
        if (!comparison) return true;
        if (comparison < 0) low = mid + 1; else high = mid - 1;
      }
    }
    return false;
  }
  add(id: string, hour: number) { this.added.set(id, hour); }
  remove(predicate: (hour: number) => boolean) {
    for (const [id, hour] of this.added) if (predicate(hour)) this.added.delete(id);
    for (const hour in this.buckets) if (predicate(Number(hour))) { delete this.buckets[hour]; this.decoded.delete(hour); }
  }
  finish() {
    const groups = new Map<string, string[]>();
    for (const id of [...this.added.keys()].sort()) { const hour = String(this.added.get(id)); if (!groups.has(hour)) groups.set(hour, []); groups.get(hour)!.push(id); }
    for (const [hour, ids] of groups) {
      const data = this.buffer(hour), next = Buffer.allocUnsafe(data.length + ids.length * WIDTH);
      let old = 0, index = 0, write = 0;
      while (old < data.length || index < ids.length) {
        const key = index < ids.length ? Buffer.from(ids[index], 'hex') : null;
        if (old < data.length && (!key || data.compare(key, 0, WIDTH, old, old + WIDTH) < 0)) { data.copy(next, write, old, old + WIDTH); old += WIDTH; }
        else { key!.copy(next, write, 0, WIDTH); index++; }
        write += WIDTH;
      }
      this.buckets[hour] = next.toString('base64'); this.decoded.set(hour, next);
    }
    this.added.clear();
    this.state[this.field] = {};
    this.state[this.packedField] = this.buckets;
  }
}
