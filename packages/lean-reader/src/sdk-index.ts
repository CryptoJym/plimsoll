import type { State } from './types.js';

const WIDTH = 8;
function merge(a: Buffer, b: Buffer): Buffer {
  const out = Buffer.allocUnsafe(a.length + b.length);
  let i = 0, j = 0, at = 0;
  while (i < a.length && j < b.length) {
    const comparison = a.compare(b, j, j + WIDTH, i, i + WIDTH);
    if (comparison <= 0) { a.copy(out, at, i, i + WIDTH); i += WIDTH; if (!comparison) j += WIDTH; }
    else { b.copy(out, at, j, j + WIDTH); j += WIDTH; }
    at += WIDTH;
  }
  a.copy(out, at, i); at += a.length - i;
  b.copy(out, at, j); at += b.length - j;
  return out.subarray(0, at);
}
function contains(data: Buffer, key: Buffer): boolean {
  let low = 0, high = data.length / WIDTH - 1;
  while (low <= high) {
    const mid = (low + high) >>> 1, at = mid * WIDTH;
    const comparison = data.compare(key, 0, WIDTH, at, at + WIDTH);
    if (!comparison) return true;
    if (comparison < 0) low = mid + 1; else high = mid - 1;
  }
  return false;
}

/** Same global 8-byte reply identities as Seen, but binary sorted runs instead
 * of a day-sized JS Map. A file contributes one small sorted run; runs of the
 * same size merge, so every ID is copied logarithmically, not once per file. */
export class SdkIds {
  private base = new Map<string, Buffer>();
  private runs = new Map<string, (Buffer | undefined)[]>();
  private filter = Buffer.alloc(2 * 1024 * 1024);
  constructor(private state: State) {
    for (const hour in state.seenPacked ?? {}) this.mark(this.original(hour));
  }
  private mark(data: Buffer): void {
    for (let at = 0; at < data.length; at += 4) {
      const bit = data.readUInt32BE(at) & 0xffffff;
      this.filter[bit >>> 3] |= 1 << (bit & 7);
    }
  }
  private original(hour: string): Buffer {
    let data = this.base.get(hour);
    if (!data) {
      data = Buffer.from(this.state.seenPacked?.[hour] ?? '', 'base64');
      if (data.length % WIDTH) throw new Error('invalid_state');
      this.base.set(hour, data);
    }
    return data;
  }
  has(key: Buffer): boolean {
    // A negative is exact. Positives still check the sorted hashes, so this
    // fixed 2 MiB filter cannot change deduplication or introduce false matches.
    for (let at = 0; at < WIDTH; at += 4) {
      const bit = key.readUInt32BE(at) & 0xffffff;
      if (!(this.filter[bit >>> 3] & (1 << (bit & 7)))) return false;
    }
    for (const hour in this.state.seenPacked ?? {}) if (contains(this.original(hour), key)) return true;
    for (const levels of this.runs.values()) for (const data of levels) if (data && contains(data, key)) return true;
    return false;
  }
  add(packed: Record<string, string>): void {
    for (const [hour, text] of Object.entries(packed)) {
      let data: Buffer = Buffer.from(text, 'base64');
      if (!data.length || data.length % WIDTH) throw new Error('invalid_state');
      this.mark(data);
      const levels = this.runs.get(hour) ?? [];
      let level = Math.floor(Math.log2(data.length / WIDTH));
      while (levels[level]) { data = merge(levels[level]!, data); levels[level] = undefined; level = Math.floor(Math.log2(data.length / WIDTH)); }
      levels[level] = data; this.runs.set(hour, levels);
    }
  }
  finish(): void {
    const buckets = this.state.seenPacked ??= {};
    for (const [hour, levels] of this.runs) {
      let data = this.original(hour);
      for (const run of levels) if (run) data = merge(data, run);
      buckets[hour] = data.toString('base64');
    }
    this.base.clear(); this.runs.clear();
  }
}
