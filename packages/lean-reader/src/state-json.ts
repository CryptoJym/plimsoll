import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

// State consists of plain JSON values. Stream its encoding so budget checks and
// the two atomic saves do not each allocate another multi-megabyte state string.
function encode(value: any, emit: (piece: string) => void): void {
  if (value && typeof value === 'object') {
    const array = Array.isArray(value);
    emit(array ? '[' : '{');
    let first = true;
    const entry = (key: string | number) => {
      if (!first) emit(',');
      first = false;
      if (!array) { emit(JSON.stringify(key)); emit(':'); }
      encode(value[key], emit);
    };
    if (array) { for (let i = 0; i < value.length; i++) entry(i); }
    else { for (const key in value) if (Object.hasOwn(value, key) && value[key] !== undefined) entry(key); }
    emit(array ? ']' : '}');
  } else if (typeof value === 'string' && value.length > 64 * 1024 && /^[A-Za-z0-9+/=]+$/.test(value)) {
    emit('"');
    for (let at = 0; at < value.length; at += 32 * 1024) emit(value.slice(at, at + 32 * 1024));
    emit('"');
  } else emit(JSON.stringify(value) ?? 'null');
}
export function stateBytes(value: unknown, limit: number): number {
  let size = 0;
  encode(value, piece => { size += Buffer.byteLength(piece); if (size > limit) throw new Error('state_capacity'); });
  return size;
}
export function writeState(fd: number, value: unknown, limit: number): number {
  let size = 0, length = 0, parts: string[] = [];
  const flush = () => { if (parts.length) fs.writeFileSync(fd, parts.join('')); parts = []; length = 0; };
  encode(value, piece => {
    size += Buffer.byteLength(piece);
    if (size > limit) throw new Error('state_capacity');
    parts.push(piece); length += piece.length;
    if (length >= 32 * 1024) flush();
  });
  flush(); return size;
}
/** Atomic replace: a private temp file, fsynced, renamed over the target, then
 * the folder fsynced. */
export function saveJson(file: string, value: unknown, limit: number): number {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const temp = `${file}.${process.pid}.${crypto.randomBytes(8).toString('hex')}.tmp`;
  const fd = fs.openSync(temp, 'wx', 0o600);
  let size = 0;
  try {
    try { size = writeState(fd, value, limit); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    fs.renameSync(temp, file); const parent = fs.openSync(path.dirname(file), 'r'); try { fs.fsyncSync(parent); } finally { fs.closeSync(parent); }
  }
  finally { try { fs.unlinkSync(temp); } catch { /* Rename succeeded. */ } }
  return size;
}
