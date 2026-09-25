/** Paths are held only in this worker's memory. The coverage walk supplies
 * them again after a restart; byte cursors remain durable in SQLite. */
export class CaptureRevisitQueue {
  private readonly paths = new Map<string, boolean>();

  constructor(private readonly maxPaths = 1024) {}

  offer(file: string) {
    if (this.paths.has(file)) return;
    if (this.paths.size >= this.maxPaths) {
      // A long unfinished file cannot permanently occupy one of the bounded
      // slots. Replace only a path selected in a prior turn; fresh paths get
      // their own chance before the next coverage sweep can replace them.
      let served: string | undefined;
      for (const [path, selected] of this.paths) if (selected) { served = path; break; }
      if (!served) return;
      this.paths.delete(served);
    }
    this.paths.set(file, false);
  }

  remove(file: string) { this.paths.delete(file); }
  clear() { this.paths.clear(); }

  /** Rotate before service so one large file cannot hide later paths. */
  next(limit = 16) {
    const files = [...this.paths.keys()].slice(0, limit);
    for (const file of files) {
      this.paths.delete(file);
      this.paths.set(file, true);
    }
    return files;
  }
}
