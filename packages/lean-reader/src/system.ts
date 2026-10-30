import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { scan, emptyState } from './reader.js';
import { sendPending } from './transport.js';
import { peakMemory, type State, type Config, type Options } from './types.js';
import { saveJson } from './state-json.js';
import { discover } from './discovery.js';
import { allowancePass } from './allowance.js';

const MAX_STATE = 9 * 1024 * 1024;
const execFileSync: typeof import('node:child_process').execFileSync = (...args: any[]): any => {
  return (process.getBuiltinModule('node:child_process') as any).execFileSync(...args);
};
export const stateDirectory = (home: string) => path.join(home, 'Library/Application Support/Plimsoll Lean');
export function readState(file: string): State {
  try {
    if (fs.statSync(file).size > MAX_STATE) throw new Error('state_capacity');
    const state = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (![1, 2, 3].includes(state.version) || !state.files || !state.hours || !state.seen || !state.pending || !state.sessions || !state.seats) throw new Error('invalid_state');
    state.velocity ??= {}; state.velocityPending ??= {};
    return state;
  } catch (e) { if ((e as NodeJS.ErrnoException).code === 'ENOENT') return emptyState(); throw new Error('invalid_state'); }
}
export const saveState = (file: string, state: State): number => saveJson(file, state, MAX_STATE);
export function acquireLock(file: string): () => void {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const token = crypto.randomBytes(16).toString('hex');
  const name = `${process.pid}-${token}.json`;
  const staging = fs.mkdtempSync(`${file}.${process.pid}-`);
  fs.chmodSync(staging, 0o700);
  fs.writeFileSync(path.join(staging, name), JSON.stringify({ pid: process.pid, token }), { flag: 'wx', mode: 0o600 });
  const dead = (pid: unknown) => {
    if (!Number.isSafeInteger(pid) || Number(pid) <= 0) return false;
    try { process.kill(Number(pid), 0); return false; } catch (e) { return (e as NodeJS.ErrnoException).code === 'ESRCH'; }
  };
  let published = false;
  try {
    for (let attempt = 0; attempt < 4; attempt++) {
      try {
        // Publish the directory with its owner ALREADY inside it. rename can
        // replace an empty abandoned directory, never a live nonempty lock.
        fs.renameSync(staging, file); published = true;
        return () => {
          try { fs.unlinkSync(path.join(file, name)); fs.rmdirSync(file); } catch { /* A successor owns the directory. */ }
        };
      } catch (error) {
        if (!['EEXIST', 'ENOTEMPTY', 'ENOTDIR', 'EISDIR'].includes((error as NodeJS.ErrnoException).code ?? '')) throw error;
      }
      try {
        const stat = fs.lstatSync(file);
        if (stat.isFile()) {
          if (!dead(JSON.parse(fs.readFileSync(file, 'utf8')).pid)) throw new Error('locked');
          fs.unlinkSync(file); continue;
        }
        if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error('locked');
        const owners = fs.readdirSync(file);
        for (const owner of owners) {
          if (!/^\d+-[a-f0-9]{32}\.json$/.test(owner) || !dead(JSON.parse(fs.readFileSync(path.join(file, owner), 'utf8')).pid)) throw new Error('locked');
        }
        // Delete only the exact dead tokens inspected above. An atomic rmdir
        // refuses a directory containing a concurrently published successor.
        for (const owner of owners) try { fs.unlinkSync(path.join(file, owner)); } catch (e) { if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e; }
        fs.rmdirSync(file);
      } catch (e) {
        if (['ENOENT', 'ENOTEMPTY', 'EEXIST'].includes((e as NodeJS.ErrnoException).code ?? '')) continue;
        throw e;
      }
    }
    throw new Error('locked');
  } finally {
    if (!published) { try { fs.unlinkSync(path.join(staging, name)); fs.rmdirSync(staging); } catch { /* Preserve the lock error. */ } }
  }
}

function log(home: string, row: unknown) {
  const file = path.join(home, 'Library/Logs/plimsoll-lean/reader.log'), line = JSON.stringify(row) + '\n';
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  try { if (fs.statSync(file).size + Buffer.byteLength(line) > 5_000_000) fs.renameSync(file, file + '.1'); } catch (e) { if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e; }
  fs.appendFileSync(file, line, { mode: 0o600 });
}
export async function runPass(options: Options = {}) {
  const started = performance.now(), home = options.home ?? os.homedir(), now = options.now ?? Date.now(), config = options.config ?? {};
  const dir = options.stateDir ?? stateDirectory(home), file = path.join(dir, 'state.json');
  const release = acquireLock(path.join(dir, 'reader.lock'));
  try {
    // Allowance goes first and on its own: one small file, its own uploads and
    // its own outbox, so a slow or failing usage pass never holds it up.
    if (!options.velocityOnly && (options.command ?? 'run') === 'run') {
      await allowancePass({ home, env: options.env ?? process.env, config, stateDir: dir, now, dryRun: (options.dryRun ?? config.dryRun) === true }).catch(() => 0);
    }
    const state = options.state ?? readState(file);
    // Velocity replay has its own bounded traversal in scan(): it admits
    // immutable archived Codex .jsonl.zst sources. The ordinary async
    // discovery pass intentionally filters those files for the five-second
    // incremental run, so do not replace the replay traversal with it.
    const velocityOnly = options.velocityOnly === true || options.command === 'velocity-backfill';
    const discovered = options.discovered ?? (velocityOnly ? undefined : await discover({ ...options, home, now, config }));
    const result = scan({ ...options, home, now, config, state, discovered, backfill: options.command === 'backfill' || options.backfill });
    // Persist the outbox before attempting any network operation. Replaying an
    // accepted batch after a crash is safe because cloud rows replace totals.
    result.stats.stateBytes = saveState(file, result.state);
    if (!(options.dryRun ?? config.dryRun)) result.stats.sent = await sendPending(result.state, config, { home, now, velocityOnly: options.velocityOnly === true || options.command === 'velocity-backfill' });
    if (result.state.lastError) result.stats.errors++;
    result.stats.milliseconds = performance.now() - started; result.stats.peakMemoryBytes = peakMemory();
    result.state.footprint = { milliseconds: result.stats.milliseconds, peakMemoryBytes: result.stats.peakMemoryBytes, stateBytes: result.stats.stateBytes };
    result.stats.stateBytes = saveState(file, result.state);
    log(home, { at: new Date(now).toISOString(), ...result.stats, error: result.state.lastError });
    return result;
  } catch (error) {
    const reason = error instanceof Error && ['state_capacity', 'invalid_state', 'invalid_window', 'source_unreadable'].includes(error.message) ? error.message : 'reader_error';
    try { log(home, { at: new Date(now).toISOString(), files: 0, bytesRead: 0, rows: 0, sent: 0, errors: 1, milliseconds: performance.now() - started, peakMemoryBytes: peakMemory(), error: reason }); } catch { /* Preserve the original failure. */ }
    throw new Error(reason);
  } finally { release(); }
}

const xmlEscape = (s: string) => s.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&apos;');
const xmlUnescape = (s: string) => s.replaceAll('&lt;', '<').replaceAll('&gt;', '>').replaceAll('&quot;', '"').replaceAll('&apos;', "'").replaceAll('&amp;', '&');
export function launchAgent(home: string, script: string): string {
  let node = process.execPath;
  const dir = path.join(home, 'Library/LaunchAgents');
  let candidates: string[] = [];
  try { candidates = fs.readdirSync(dir).filter(n => /plimsoll.*\.plist$/.test(n) && n !== 'com.plimsoll.lean-reader.plist').sort(); candidates.sort((a, b) => Number(b === 'com.plimsoll.collector.plist') - Number(a === 'com.plimsoll.collector.plist')); } catch { /* No collector agent. */ }
  for (const name of candidates) {
    try {
      let plist = fs.readFileSync(path.join(dir, name), 'utf8');
      if (!plist.startsWith('<?xml') && process.platform === 'darwin') plist = execFileSync('/usr/bin/plutil', ['-convert', 'xml1', '-o', '-', path.join(dir, name)], { encoding: 'utf8' });
      const args = plist.match(/<key>ProgramArguments<\/key>\s*<array>([\s\S]*?)<\/array>/)?.[1];
      const executable = args?.match(/<string>([\s\S]*?)<\/string>/)?.[1];
      if (executable && /(?:^|\/)node(?:-\d+)?$/.test(xmlUnescape(executable))) { node = xmlUnescape(executable); break; }
    } catch { /* Another optional agent. */ }
  }
  const env = ['CLAUDE_CONFIG_DIR', 'CODEX_HOME', 'GROK_HOME'].filter(k => process.env[k]).map(k => `<key>${k}</key><string>${xmlEscape(process.env[k]!)}</string>`).join('');
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>com.plimsoll.lean-reader</string>
<key>ProgramArguments</key><array><string>${xmlEscape(node)}</string><string>${xmlEscape(script)}</string><string>run</string></array>
<key>StartInterval</key><integer>300</integer><key>RunAtLoad</key><true/>
<key>ProcessType</key><string>Background</string><key>LowPriorityIO</key><true/><key>Nice</key><integer>10</integer>
<key>EnvironmentVariables</key><dict>${env}<key>NODE_OPTIONS</key><string>--jitless --max-old-space-size=48 --max-semi-space-size=1</string></dict>
</dict></plist>\n`;
}
export function install(home: string, script: string, remove = false) {
  if (process.platform !== 'darwin') throw new Error('macos_required');
  const file = path.join(home, 'Library/LaunchAgents/com.plimsoll.lean-reader.plist'), domain = `gui/${process.getuid!()}`;
  if (fs.existsSync(file)) {
    try { execFileSync('/bin/launchctl', ['bootout', domain, file], { stdio: 'ignore' }); } catch { /* Already unloaded. */ }
  }
  if (remove) { try { fs.unlinkSync(file); } catch (e) { if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e; } return; }
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, launchAgent(home, script), { mode: 0o600 });
  execFileSync('/bin/launchctl', ['bootstrap', domain, file], { stdio: 'ignore' });
}
export function loadConfig(file: string): Config {
  let input: Record<string, any>;
  try { if (fs.statSync(file).size > 1024 * 1024) throw new Error(); input = JSON.parse(fs.readFileSync(file, 'utf8')); } catch (e) { if ((e as NodeJS.ErrnoException).code === 'ENOENT') return { sendVelocity: true }; throw new Error('invalid_config'); }
  const config: Config = {};
  for (const key of ['allowanceFile', 'credentialsFrom', 'endpoint'] as const) if (typeof input[key] === 'string') config[key] = input[key];
  for (const key of ['rootsOnly', 'sendSessionTitles', 'sendFolderLabels', 'sendEvidence', 'sendVelocity', 'dryRun'] as const) if (typeof input[key] === 'boolean') config[key] = input[key];
  if (typeof input.velocityAccountMapFile === 'string' && input.velocityAccountMapFile.length <= 4096) config.velocityAccountMapFile = input.velocityAccountMapFile;
  // Velocity is additive and enabled for every installed reader. An explicit
  // false remains useful for a rollback or a local compatibility run.
  config.sendVelocity ??= true;
  if (Array.isArray(input.roots)) config.roots = input.roots.filter((r: any) => typeof r === 'string' || (r && typeof r.path === 'string' && ['codex', 'claude_code', 'grok'].includes(r.tool))).slice(0, 256).map((r: any) => typeof r === 'string' ? r : { tool: r.tool, path: r.path });
  return config;
}
