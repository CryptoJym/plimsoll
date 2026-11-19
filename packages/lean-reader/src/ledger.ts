import type { ReadEntry, ReadSummary } from './types.js';

// Where a pass read its bytes: totals per source and the largest reads, each
// with the offset it started from and why (append, new, reset, evidence
// replay, velocity replay, sdk). A pass over 1 GB is explained by its own
// reader.log line instead of a guess from the next fixer.
const TOP = 8;
export class ReadLedger {
  private sources: ReadSummary['sources'] = {};
  private top: ReadEntry[] = [];
  private source(name: string) { return this.sources[name] ??= { files: 0, read: 0, bytes: 0 }; }
  visit(source: string, files = 1): void { this.source(source).files += files; }
  /** A settled file back in the window unchanged: skipped, not read again. */
  settle(source: string): void { const total = this.source(source); total.settled = (total.settled ?? 0) + 1; }
  note(source: string, file: string, bytes: number, from: number, why: ReadEntry['why']): void {
    if (!(bytes > 0)) return;
    const total = this.source(source); total.read++; total.bytes += bytes;
    if (this.top.length < TOP || bytes > this.top[this.top.length - 1].bytes) {
      this.top.push({ file, bytes, from, why });
      this.top.sort((a, b) => b.bytes - a.bytes);
      if (this.top.length > TOP) this.top.length = TOP;
    }
  }
  summary(sdk?: ReadSummary['sdk']): ReadSummary { return sdk ? { sources: this.sources, top: this.top, sdk } : { sources: this.sources, top: this.top }; }
}
