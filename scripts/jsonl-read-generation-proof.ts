import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { DEFAULT_JSONL_TAILER_IO, jsonlScanStateKey, loadJsonlScanCursor } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { readJsonlContinuation } from "../packages/collector-cli/src/jsonl-continuation";

async function main() {
  const home = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "jsonl-generation-proof-")));
  const receipts: unknown[] = [];
  const at = new Date().toISOString(), id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
  for (const provider of ["codex", "claude"] as const) {
    for (const mode of ["rewrite", "replacement", "append"] as const) {
      for (const boundary of ["reader", "provider"] as const) {
        const dir = path.join(home, provider, mode, boundary);
        const leaf = provider === "codex" ? path.join(dir, ...at.slice(0, 10).split("-")) : path.join(dir, "project");
        fs.mkdirSync(leaf, { recursive: true });
        const file = path.join(leaf, provider === "codex" ? `rollout-${id}.jsonl` : `${id}.jsonl`);
        const db = new LocalEventBuffer(path.join(home, `${provider}-${mode}-${boundary}.sqlite`));
        const tailer = provider === "codex" ? new RolloutTailer(db, dir, () => []) : new TranscriptTailer(db, dir);
        const usage = (n: number) => provider === "codex"
          ? { type: "event_msg", timestamp: at, payload: { type: "token_count", info: { total_token_usage: { input_tokens: n, output_tokens: 0, cached_input_tokens: 0, reasoning_output_tokens: 0 } } } }
          : { type: "assistant", sessionId: id, timestamp: at, message: { id: `m${n}`, model: "claude-opus-5", usage: { input_tokens: n, output_tokens: 0 } } };
        fs.writeFileSync(file, (provider === "codex" ? JSON.stringify({ type: "session_meta", timestamp: at, payload: { id } }) + "\n" : "") +
          JSON.stringify(usage(0)) + "\n" + JSON.stringify({ type: "fixture_ignored", padding: "p".repeat(700) }) + "\n");
        await tailer.scan({ scope: "full" });
        const row = () => db.database.prepare("select * from rollout_scan_state where file=?").get(jsonlScanStateKey(file)) as any;
        assert(row().committed_offset > 512);

        // A bounded escaped control must retain normal progress and usage.
        fs.appendFileSync(file, JSON.stringify({ type: "fixture_ignored", padding: "a".repeat(80 * 1024) + "\n" }) + "\n" + JSON.stringify(usage(100)) + "\n");
        const control = await tailer.scan({ scope: "full" });
        assert.equal(control.readErrors, 0);
        assert.equal(row().committed_offset, fs.statSync(file).size);
        assert.equal(control.eventsAppended, 1);
        fs.appendFileSync(file, JSON.stringify(usage(200)) + "\n");
        const original = fs.readFileSync(file, "utf8");
        const rewritten = original.replace('"input_tokens":200', '"input_tokens":900');
        assert.notEqual(original, rewritten);
        assert.equal(Buffer.byteLength(original), Buffer.byteLength(rewritten));
        const snapshot = () => {
          const tables = db.database.prepare("select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name").all() as { name: string }[];
          return JSON.stringify(tables.map(({ name }) => {
            const rows = db.database.prepare(`select * from "${name.replaceAll('"', '""')}"`).all() as Record<string, unknown>[];
            // Every scan records the retirement pass time, even with no rows.
            // Preserve its key/value and every capture table; ignore only that clock.
            return [name, name === "maintenance_state" ? rows.map(row => {
              if (row.key !== `jsonl_continuation_retirement:${provider}`) return row;
              const { updated_at: _retirementClock, ...state } = row;
              return state;
            }) : rows];
          }));
        };
        const before = snapshot();
        const cursor = loadJsonlScanCursor<any>(db.database, file, row().parser_kind, row().checkpoint_version, () => true)!;
        assert.equal(cursor.checkpointStatus, "valid");
        const originalRead = fs.readSync, originalOpen = fs.openSync, originalClose = fs.closeSync;
        const targetFds = new Set<number>();
        let reads = 0, afterMutation = 0, mutated = false;
        fs.openSync = ((...args: any[]) => { const fd = (originalOpen as any)(...args); if (String(args[0]) === file) targetFds.add(fd); return fd; }) as any;
        fs.closeSync = ((fd: number) => { targetFds.delete(fd); return originalClose(fd); }) as any;
        fs.readSync = ((...args: any[]) => {
          const target = targetFds.has(args[0]);
          if (target) { reads++; if (mutated) afterMutation++; }
          const n = (originalRead as any)(...args);
          if (target && !mutated) {
            if (mode === "rewrite") fs.writeFileSync(file, rewritten);
            else if (mode === "replacement") { fs.writeFileSync(file + ".replacement", rewritten); fs.renameSync(file + ".replacement", file); }
            else fs.appendFileSync(file, "{}\n");
            mutated = true;
          }
          return n;
        }) as any;
        let read: ReturnType<typeof readJsonlContinuation>;
        try {
          if (boundary === "reader") {
            read = readJsonlContinuation(file, fs.statSync(file), cursor, { maxBytes: 65536, maxRecords: 64 }, DEFAULT_JSONL_TAILER_IO,
              { database: db.database, provider, cursorKey: file, directory: dir, deadline: performance.now() + 200, eligible: () => true });
            assert(read);
            assert.equal(read.continuation?.reason, "source_changed");
            assert.equal(read.continuation?.action, "park");
            assert.equal(read.continuation?.scanBytesAdvanced, 0);
            assert.deepEqual(read.lines, []);
          } else {
            const scan = await tailer.scan({ scope: "full" });
            assert.equal(scan.eventsAppended, 0);
            assert.equal(scan.continuationBytesAdvanced ?? 0, 0);
          }
        } finally {
          read?.close();
          fs.readSync = originalRead; fs.openSync = originalOpen; fs.closeSync = originalClose;
        }
        assert(mutated, "fixture must mutate the targeted file after a real read");
        assert.equal(reads, 1);
        assert.equal(afterMutation, 0, "no further read may use the changed generation");
        assert.equal(targetFds.size, 0, "refusal must close the reader fd");
        const after = snapshot();
        const oldTables = new Map<string, unknown>(JSON.parse(before));
        const changedTables = (JSON.parse(after) as [string, unknown][]).filter(([name, rows]) => JSON.stringify(rows) !== JSON.stringify(oldTables.get(name))).map(([name]) => name);
        assert.deepEqual(changedTables, [], `${provider}/${mode}/${boundary}: durable tables changed`);
        receipts.push({ provider, mode, boundary, reads, afterMutation, captureTablesUnchanged: true, retirementTimestampExcluded: true, boundedEscapedControl: true });
        tailer.close(); db.close();
      }
    }
  }
  console.log(JSON.stringify({ status: "PASS", checks: receipts.length, receipts, fixture: home }));
}
main().catch(error => { console.error(error); process.exitCode = 1; });
