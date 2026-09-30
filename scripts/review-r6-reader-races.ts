import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { readJsonlTail } from "../packages/collector-cli/src/jsonl-byte-tailer";

const fixture = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r6-reader-races-")));
const file = path.join(fixture, "session.jsonl");
const line = (id: string) => JSON.stringify({ type: "assistant", timestamp: new Date().toISOString(),
  message: { id, model: "claude-opus-5", usage: { input_tokens: 10, output_tokens: 0 } } }) + "\n";
const originalOpen = fs.openSync;
const originalRead = fs.readSync;
try {
  for (const point of ["before_open", "before_read_check", "after_read_check", "before_commit"] as const) {
    fs.writeFileSync(file, line(`old-${point}`));
    const stat = fs.statSync(file);
    let injected = false;
    let targetFd: number | undefined;
    const replace = () => {
      const next = `${file}.new`;
      fs.writeFileSync(next, line(`new-${point}`));
      fs.renameSync(next, file);
      injected = true;
    };
    fs.openSync = ((candidate: fs.PathLike, flags: fs.OpenMode, mode?: fs.Mode) => {
      if (String(candidate) === file && point === "before_open" && !injected) replace();
      const fd = originalOpen(candidate, flags, mode);
      if (String(candidate) === file) targetFd = fd;
      return fd;
    }) as typeof fs.openSync;
    fs.readSync = ((fd: number, buffer: NodeJS.ArrayBufferView, offset: number,
      length: number, position: number | null) => {
      if (point === "after_read_check" && fd === targetFd && !injected) replace();
      return originalRead(fd, buffer, offset, length, position);
    }) as typeof fs.readSync;
    try {
      if (point === "before_commit" || point === "after_read_check") {
        const read = readJsonlTail(file, stat, undefined);
        assert.ok(read);
        if (point === "before_commit") replace();
        try { assert.throws(() => read.assertStableForCommit(), /generation changed/); }
        finally { read.close(); }
      } else {
        assert.throws(() => readJsonlTail(file, stat, undefined, {
          beforeRead: point === "before_read_check" ? () => { if (!injected) replace(); } : undefined,
        }), /generation changed/);
      }
      assert.equal(injected, true, `${point}: injection must fire`);
      console.log(JSON.stringify({ point, refused: true }));
    } finally {
      fs.openSync = originalOpen;
      fs.readSync = originalRead;
    }
  }
} finally {
  fs.openSync = originalOpen;
  fs.readSync = originalRead;
  fs.rmSync(fixture, { recursive: true, force: true });
}
