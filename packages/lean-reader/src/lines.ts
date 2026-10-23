import fs from 'node:fs';

// Only these leaves survive projection of an oversized record. Array contents,
// prompts, instructions and tool outputs never become part of the projection.
const paths = [
  'type', 'timestamp', 'sessionId', 'cwd', 'requestId', 'uuid', 'ordinal', 'summary',
  'message.id', 'message.model',
  ...['input_tokens', 'cache_read_input_tokens', 'cache_creation_input_tokens', 'output_tokens'].map(k => `message.usage.${k}`),
  ...['type', 'cwd', 'id', 'session_id', 'creator_account_id', 'model', 'response_id', 'turn_id'].map(k => `payload.${k}`),
  'payload.base_instructions.provenance.model',
  ...['input_tokens', 'cached_input_tokens', 'cache_write_input_tokens', 'output_tokens', 'reasoning_output_tokens', 'total_tokens'].flatMap(k => [`payload.usage.${k}`, `payload.info.total_token_usage.${k}`]),
  'payload.rate_limits.plan_type', 'payload.rate_limits.credits.balance',
  ...['used_percent', 'window_minutes', 'resets_at'].map(k => `payload.rate_limits.primary.${k}`),
];
const wanted = new Set(paths);
const containers = new Set(paths.flatMap(p => p.split('.').map((_, i, parts) => parts.slice(0, i).join('.')).filter(Boolean)));
const evidencePaths = [
  'gitBranch', 'payload.git.branch', 'payload.name', 'payload.call_id', 'payload.arguments', 'payload.input',
  ...['type', 'id', 'name'].map(k => `message.content[].${k}`),
  ...['file_path', 'filePath', 'path', 'notebook_path', 'target_file', 'workdir', 'cwd', 'working_directory', 'command', 'cmd', 'patch', 'input'].flatMap(k => [
    `message.content[].input.${k}`, `message.content[].input.edits[].${k}`, `payload.arguments.${k}`, `payload.input.${k}`,
  ]),
];
evidencePaths.push(...evidencePaths.filter(p => /\.(?:command|cmd)$/.test(p)).map(p => p + '[]'));
const evidenceWanted = new Set([...paths, ...evidencePaths]);
const evidenceContainers = new Set([...evidenceWanted].flatMap(p => p.replaceAll('[]', '.[]').split('.').map((_, i, parts) => parts.slice(0, i).join('.').replaceAll('.[]', '[]')).filter(Boolean)));
const inputWanted = new Set(evidencePaths.filter(p => p.startsWith('message.content[].input.')).map(p => p.slice('message.content[].input.'.length)));
const inputContainers = new Set([...inputWanted].flatMap(p => p.replaceAll('[]', '.[]').split('.').map((_, i, parts) => parts.slice(0, i).join('.').replaceAll('.[]', '[]')).filter(Boolean)));
type Frame = { kind: 'object' | 'array'; path: string; expect: 'key' | 'colon' | 'value' | 'comma'; key: string; count: number };

// Explicit selections stream even small records. A wildcard matches one object
// key (model IDs and auth issuers contain dots, so encode keys before matching).
function selection(paths: readonly string[]) {
  const matches = (patterns: string[]) => {
    const expressions = patterns.map(p => new RegExp('^' + p.split('.').map(k => k === '*' ? '[^.]+' : k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('\\.') + '$'));
    return { has: (p: string) => expressions.some(re => re.test(p)) };
  };
  return { wanted: matches([...paths]), containers: matches(paths.flatMap(p => p.split('.').map((_, i, parts) => parts.slice(0, i).join('.')).filter(Boolean))) };
}

class PatchPaths {
  private pending = ''; private dropped = false; private headers: string[] = []; private bytes = 0;
  private line() {
    if (!this.dropped && /^(?:\*\*\* (?:Add File|Update File|Delete File|Move to): |(?:---|\+\+\+) [ab]\/)/.test(this.pending) && this.bytes + this.pending.length <= 64 * 1024) { this.headers.push(this.pending); this.bytes += this.pending.length; }
    this.pending = ''; this.dropped = false;
  }
  feed(text: string) {
    let start = 0;
    for (;;) {
      const end = text.indexOf('\n', start), part = text.slice(start, end < 0 ? undefined : end);
      if (!this.dropped) { if (this.pending.length + part.length <= 8192) this.pending += part; else { this.pending = ''; this.dropped = true; } }
      if (end < 0) break;
      this.line(); start = end + 1;
    }
  }
  finish() { this.line(); return this.headers.join('\n'); }
}

/** A bounded JSON projection, including JSON validation, for arbitrarily long
 * strings. Ignored string spans are searched with a regex in each 64 KiB chunk;
 * their bytes are never concatenated. Nesting, keys and retained scalars are capped. */
class Projection {
  constructor(private evidence = false, private input = false, private selected?: ReturnType<typeof selection>) {}
  private selectedAssignments = 0;
  result: Record<string, any> = {};
  stack: Frame[] = [];
  mode: 'normal' | 'string' | 'scalar' = 'normal';
  token = ''; keep = false; keyToken = false; escaped = false;
  unicode = 0;
  unicodeValue = '';
  sink: Projection | PatchPaths | null = null;
  scalarPath = ''; rootDone = false; bad = false;
  valuePath() {
    const f = this.stack.at(-1);
    const key = f && this.selected ? encodeURIComponent(f.key).replaceAll('.', '%2E') : f?.key;
    return f ? (f.kind === 'array' ? `${f.path}[${this.evidence ? f.count : ''}]` : [f.path, key].filter(Boolean).join('.')) : '';
  }
  normalized(p: string) { return p.replace(/\[\d+\]/g, '[]'); }
  wanted() { return this.selected?.wanted ?? (this.input ? inputWanted : this.evidence ? evidenceWanted : wanted); }
  containers() { return this.selected?.containers ?? (this.input ? inputContainers : this.evidence ? evidenceContainers : containers); }
  bounded(p: string) { return !this.evidence || ![...p.matchAll(/\[(\d+)\]/g)].some(m => Number(m[1]) >= 256); }
  assign(p: string, value: unknown, container = false) {
    const parts = (this.evidence ? p.replace(/\[(\d+)\]/g, '.$1') : p).split('.');
    if (this.selected) {
      // Dynamic model/issuer keys are bounded too. Reject the whole record if
      // it exceeds the budget, rather than accepting incomplete usage totals.
      if (++this.selectedAssignments > 4096) { this.bad = true; return; }
      // Dynamic JSON keys must never traverse Object.prototype.
      let out = this.result;
      for (const part of parts.slice(0, -1)) {
        const key = decodeURIComponent(part);
        if (!Object.hasOwn(out, key)) Object.defineProperty(out, key, { value: {}, enumerable: true, writable: true, configurable: true });
        out = out[key];
      }
      const key = decodeURIComponent(parts.at(-1)!);
      if (!container || !Object.hasOwn(out, key)) Object.defineProperty(out, key, { value, enumerable: true, writable: true, configurable: true });
      return;
    }
    let out = this.result;
    for (let i = 0; i < parts.length - 1; i++) out = out[parts[i]] ??= /^\d+$/.test(parts[i + 1]) ? [] : {};
    if (!container || out[parts.at(-1)!] === undefined) out[parts.at(-1)!] = value;
  }
  valueDone() {
    const f = this.stack.at(-1);
    if (f) { f.expect = 'comma'; f.count++; } else this.rootDone = true;
  }
  set(value: unknown) {
    if (!this.wanted().has(this.normalized(this.scalarPath)) || !this.bounded(this.scalarPath)) return;
    this.assign(this.scalarPath, value);
  }
  feed(text: string) {
    if (this.bad) return;
    let i = 0;
    while (i < text.length && !this.bad) {
      if (this.mode === 'string') {
        const start = i;
        if (this.unicode) {
          if (!/[0-9a-fA-F]/.test(text[i])) { this.bad = true; continue; }
          if (this.keep) this.token += text[i];
          this.unicodeValue += text[i];
          this.unicode--; i++;
          if (!this.unicode) this.sink?.feed(String.fromCharCode(parseInt(this.unicodeValue, 16)));
          continue;
        }
        // A trailing escape is carried across chunks, including escaped quotes.
        if (this.escaped) {
          if (!/["\\/bfnrtu]/.test(text[i])) { this.bad = true; continue; }
          if (this.keep) this.token += text[i];
          if (text[i] === 'u') { this.unicode = 4; this.unicodeValue = ''; }
          else this.sink?.feed(({ '"': '"', '\\': '\\', '/': '/', b: '\b', f: '\f', n: '\n', r: '\r', t: '\t' } as Record<string, string>)[text[i]]);
          this.escaped = false; i++; continue;
        }
        const special = /["\\\x00-\x1f]/g; special.lastIndex = i;
        const match = special.exec(text);
        const end = match ? match.index : text.length;
        if (this.keep) this.token += text.slice(start, end);
        this.sink?.feed(text.slice(start, end));
        if (this.token.length > 8192) {
          if (this.evidence && !this.keyToken && !wanted.has(this.scalarPath)) { this.keep = false; this.token = ''; }
          else { this.bad = true; break; }
        }
        i = end;
        if (!match) continue;
        const c = text[i++];
        if (c === '\\') { if (this.keep) this.token += c; this.escaped = true; }
        else if (c === '"') {
          let value: string;
          try { value = this.keep ? JSON.parse('"' + this.token + '"') : ''; } catch { this.bad = true; break; }
          if (this.keyToken) {
            const f = this.stack.at(-1)!; f.key = value; f.expect = 'colon';
          } else {
            if (this.sink) { const projected = this.sink.finish(); if (projected !== null) this.set(projected); this.sink = null; }
            else if (this.keep) this.set(value);
            this.valueDone();
          }
          this.mode = 'normal'; this.token = '';
        } else this.bad = true;
        continue;
      }
      if (this.mode === 'scalar') {
        const c = text[i];
        if (/[\s,}\]]/.test(c)) {
          try { this.set(JSON.parse(this.token)); } catch { this.bad = true; break; }
          this.valueDone(); this.mode = 'normal'; this.token = ''; continue;
        }
        this.token += c; i++;
        if (this.token.length > 128) this.bad = true;
        continue;
      }
      const c = text[i++];
      if (/\s/.test(c)) continue;
      const f = this.stack.at(-1);
      if (c === '}' || c === ']') {
        if (!f || f.kind !== (c === '}' ? 'object' : 'array') || !(f.expect === 'comma' || (f.count === 0 && (f.expect === 'key' || f.expect === 'value')))) { this.bad = true; continue; }
        this.stack.pop(); this.valueDone(); continue;
      }
      if (f?.expect === 'colon') { if (c !== ':') this.bad = true; else f.expect = 'value'; continue; }
      if (f?.expect === 'comma') { if (c !== ',') this.bad = true; else f.expect = f.kind === 'object' ? 'key' : 'value'; continue; }
      if (f?.expect === 'key') {
        if (c !== '"') { this.bad = true; continue; }
        this.mode = 'string'; this.keyToken = true; this.keep = true; this.token = ''; continue;
      }
      if (this.rootDone) { this.bad = true; continue; }
      const p = this.valuePath();
      if (c === '{' || c === '[') {
        if (this.stack.length >= 64) { this.bad = true; continue; }
        if (c === '{' && this.containers().has(this.normalized(p)) && this.bounded(p)) this.assign(p, {}, true);
        this.stack.push({ kind: c === '{' ? 'object' : 'array', path: p, expect: c === '{' ? 'key' : 'value', key: '', count: 0 });
      } else if (c === '"') {
        this.mode = 'string'; this.keyToken = false; this.scalarPath = p; this.keep = this.wanted().has(this.normalized(p)) && this.bounded(p); this.token = '';
        if (this.evidence && this.keep) {
          if (!this.input && p === 'payload.arguments') this.sink = new Projection(true, true);
          else if (p.endsWith('.patch') || (this.input && p === 'input') || (p === 'payload.input' && /(?:^|[._])apply_patch$/.test(this.result.payload?.name ?? ''))) this.sink = new PatchPaths();
          if (this.sink) this.keep = false;
        }
      } else {
        this.mode = 'scalar'; this.scalarPath = p; this.token = c;
      }
    }
  }
  finish(): Record<string, any> | null {
    return !this.bad && this.rootDone && this.stack.length === 0 && this.mode === 'normal' ? this.result : null;
  }
}

const LIMIT = 1024 * 1024;
const buffer = Buffer.allocUnsafe(64 * 1024);
export function readLines(file: string, offset: number, end: number, visit: (record: Record<string, any>, offset: number) => void, evidence = false, selectedPaths?: readonly string[]): { offset: number; bytes: number } {
  const fd = fs.openSync(file, 'r');
  const selected = selectedPaths ? selection(selectedPaths) : undefined;
  let position = offset, committed = offset, size = 0, chunks: Buffer[] = [];
  let projector: Projection | null = null, dropped = false;
  // StringDecoder is unnecessary here: Buffer chunks are retained for small
  // lines; giant projections use a streaming decoder to preserve UTF-8 tags.
  let decoder: TextDecoder | null = null;
  const consume = (part: Buffer) => {
    if (dropped) return;
    if ((evidence || selected) && !projector) { projector = new Projection(evidence, false, selected); decoder = new TextDecoder(); }
    if (projector) { projector.feed(decoder!.decode(part, { stream: true })); return; }
    size += part.length;
    if (size <= LIMIT) { chunks.push(Buffer.from(part)); return; }
    const head = Buffer.concat(chunks, Math.min(size - part.length, 512)).subarray(0, 512).toString();
    if (!/"(?:assistant|usage|session_meta|turn_context|token_usage_record|token_count)"/.test(head) && !(evidence && /"response_item"/.test(head))) { chunks = []; dropped = true; return; }
    projector = new Projection(evidence); decoder = new TextDecoder();
    for (const chunk of chunks) projector.feed(decoder.decode(chunk, { stream: true }));
    chunks = []; projector.feed(decoder.decode(part, { stream: true }));
  };
  try {
    while (position < end) {
      const n = fs.readSync(fd, buffer, 0, Math.min(buffer.length, end - position), position);
      if (!n) break;
      let start = 0;
      for (;;) {
        const newline = buffer.indexOf(10, start);
        if (newline === -1 || newline >= n) break;
        consume(buffer.subarray(start, newline));
        let record: Record<string, any> | null = null;
        const completed = projector as Projection | null;
        if (completed) { completed.feed(decoder!.decode()); record = completed.finish(); }
        else if (!dropped) {
          const text = Buffer.concat(chunks).toString('utf8');
          if (/"(?:assistant|summary|session_meta|turn_context|token_usage_record|token_count)"/.test(text) || (evidence && /"response_item"/.test(text))) {
            try { record = JSON.parse(text); } catch { /* An invalid line is not usage. */ }
          }
        }
        committed = position + newline + 1;
        if (record && typeof record === 'object' && !Array.isArray(record)) visit(record, committed);
        chunks = []; size = 0; projector = null; decoder = null; dropped = false; start = newline + 1;
      }
      consume(buffer.subarray(start, n)); position += n;
    }
    // Leave an unfinished line at its starting byte; never persist raw text.
    return { offset: committed, bytes: position - offset };
  } finally { fs.closeSync(fd); }
}

// Metadata may contain credentials or generated prose. Never materialize the
// whole JSON object: retain only the explicitly requested leaves.
export function readJsonFields(file: string, paths: readonly string[], maxBytes = 1024 * 1024): Record<string, any> | null {
  const fd = fs.openSync(file, 'r'), chunk = Buffer.alloc(64 * 1024);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.size > maxBytes) return null;
    const projection = new Projection(false, false, selection(paths)), decoder = new TextDecoder();
    let position = 0;
    while (position < stat.size) {
      const n = fs.readSync(fd, chunk, 0, Math.min(chunk.length, stat.size - position), position);
      if (!n) return null;
      projection.feed(decoder.decode(chunk.subarray(0, n), { stream: true })); position += n;
    }
    projection.feed(decoder.decode());
    return projection.finish();
  } finally { chunk.fill(0); fs.closeSync(fd); }
}
