import type { State } from './types.js';

const WIDTH = 12;
// SHA-256 prefixes, sorted inside UTC-hour buckets. Old JSON ID maps still
// read. Entire expired/backfilled hours can be removed without decoding IDs.
export class Seen {
  private buckets: Record<string, string>;
  private decoded = new Map<string, Buffer>();
  constructor(private state: State, private field: 'seen' | 'evidenceSeen', packedField: 'seenPacked' | 'evidenceSeenPacked', cutoff: number) {
    this.buckets = state[packedField] ?? {};
    state[field] ??= {};
    this.remove(hour => hour * 3_600_000 < cutoff);
  }
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
    if (Object.hasOwn(this.state[this.field]!, id)) return true;
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
  add(id: string, hour: number) { this.state[this.field]![id] = hour; }
  remove(predicate: (hour: number) => boolean) {
    const pending = this.state[this.field]!;
    for (const id in pending) if (predicate(pending[id])) delete pending[id];
    for (const hour in this.buckets) if (predicate(Number(hour))) { delete this.buckets[hour]; this.decoded.delete(hour); }
  }
  finish() {
    const pending = this.state[this.field]!, groups = new Map<string, string[]>();
    for (const id of Object.keys(pending).sort()) { const hour = String(pending[id]); if (!groups.has(hour)) groups.set(hour, []); groups.get(hour)!.push(id); }
    for (const [hour, ids] of groups) {
      const data = this.buffer(hour), next = Buffer.allocUnsafe(data.length + ids.length * WIDTH);
      let old = 0, index = 0, write = 0;
      while (old < data.length || index < ids.length) {
        const key = index < ids.length ? Buffer.from(ids[index], 'hex') : null;
        if (old < data.length && (!key || data.compare(key, 0, WIDTH, old, old + WIDTH) < 0)) { data.copy(next, write, old, old + WIDTH); old += WIDTH; }
        else { key!.copy(next, write); index++; }
        write += WIDTH;
      }
      this.buckets[hour] = next.toString('base64'); this.decoded.set(hour, next);
    }
    this.state[this.field] = {};
    this.state[this.field === 'seen' ? 'seenPacked' : 'evidenceSeenPacked'] = this.buckets;
  }
}
