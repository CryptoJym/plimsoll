import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  assertPrivateStateDirectory, fsyncStateDirectory, readPrivateStateFile,
} from "./collector-state-io";
import { withCollectorConfigMutationLock } from "./config";
import { IntentError } from "./project-intent-identity";

/** Local launch evidence/transport queue, not another usage or session ledger. */
export const INTENT_STATE_DIRECTORY = "project-intents";
export const INTENT_STATE_LIMITS = { maxFileBytes: 1024 * 1024, maxFilesPerKind: 4096, replaySessions: 8 } as const;

export class IntentStore {
  readonly root: string;
  constructor(collectorDirectory: string) {
    assertPrivateStateDirectory(collectorDirectory);
    this.root = path.join(collectorDirectory, INTENT_STATE_DIRECTORY);
    fs.mkdirSync(this.root, { recursive: true, mode: 0o700 });
    assertPrivateStateDirectory(this.root);
    for (const kind of ["launches", "sessions", "defaults", "registry"]) {
      const directory = path.join(this.root, kind);
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      assertPrivateStateDirectory(directory);
    }
  }
  private file(kind: string, id: string) {
    if (!["launches", "sessions", "defaults", "registry"].includes(kind) || !/^[a-z0-9-]{1,80}$/.test(id))
      throw new IntentError("invalid_state_key");
    return path.join(this.root, kind, `${id}.json`);
  }
  read<T>(kind: string, id: string): T | null {
    const file = this.file(kind, id);
    if (!fs.existsSync(file)) return null;
    try { return JSON.parse(readPrivateStateFile(file, INTENT_STATE_LIMITS.maxFileBytes).toString("utf8")) as T; }
    catch { throw new IntentError("intent_state_unreadable"); }
  }
  mutate<T, R>(kind: string, id: string, action: (previous: T | null) => { state: T; result: R }): R {
    const file = this.file(kind, id);
    return withCollectorConfigMutationLock(file, () => {
      if (!fs.existsSync(file) && this.ids(kind).length >= INTENT_STATE_LIMITS.maxFilesPerKind)
        throw new IntentError("intent_state_capacity");
      const { state, result } = action(this.read<T>(kind, id));
      const bytes = Buffer.from(`${JSON.stringify(state)}\n`);
      if (bytes.length > INTENT_STATE_LIMITS.maxFileBytes) throw new IntentError("intent_state_capacity");
      const temporary = `${file}.${crypto.randomUUID()}.tmp`;
      try {
        const descriptor = fs.openSync(temporary, "wx", 0o600);
        try { fs.writeFileSync(descriptor, bytes); fs.fsyncSync(descriptor); }
        finally { fs.closeSync(descriptor); }
        assertPrivateStateDirectory(path.dirname(file));
        fs.renameSync(temporary, file);
        fsyncStateDirectory(path.dirname(file));
      } finally { fs.rmSync(temporary, { force: true }); }
      return result;
    });
  }
  ids(kind: string): string[] {
    const directory = path.dirname(this.file(kind, "inventory"));
    const names = fs.readdirSync(directory);
    if (names.length > INTENT_STATE_LIMITS.maxFilesPerKind * 5) throw new IntentError("intent_state_capacity");
    return names.filter(name => /^[a-z0-9-]{1,80}\.json$/.test(name)).map(name => name.slice(0, -5)).sort();
  }
}
