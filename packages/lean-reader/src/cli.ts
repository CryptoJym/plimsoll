#!/usr/bin/env node
import os from 'node:os';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadConfig, stateDirectory, readState, runPass, install } from './system.js';
export { scan, roots, emptyState, accountKey } from './reader.js';
export { readState, saveState, acquireLock, runPass, launchAgent } from './system.js';
export { makeBatches, sendPending } from './transport.js';
const print = (value: unknown, pretty = false) => process.stdout.write(JSON.stringify(value, null, pretty ? 2 : undefined) + '\n');

async function main() {
  const args = process.argv.slice(2), command = args.shift() ?? 'run';
  const flags = new Map<string, string | boolean>();
  for (let i = 0; i < args.length; i++) {
    const key = args[i];
    if (['--dry-run', '--json'].includes(key)) flags.set(key, true);
    else if (['--since', '--until', '--config', '--state-dir'].includes(key) && args[i + 1] && !args[i + 1].startsWith('--')) flags.set(key, args[++i]);
    else throw new Error('invalid_arguments');
  }
  const home = os.homedir(), config = loadConfig(String(flags.get('--config') ?? path.join(home, '.config/plimsoll-lean/config.json')));
  const stateDir = String(flags.get('--state-dir') ?? process.env.PLIMSOLL_LEAN_STATE_DIR ?? stateDirectory(home));
  if (command === 'print-config') {
    // Credentials are never loaded here. Only allowlisted nonsecret fields are
    // printable; endpoint userinfo, query strings and fragments are redacted.
    let endpoint: string | undefined;
    try { const u = new URL(config.endpoint!); endpoint = u.origin; } catch { endpoint = config.endpoint ? '[invalid]' : undefined; }
    print({ ...config, endpoint, sendSessionTitles: config.sendSessionTitles ?? false, sendFolderLabels: config.sendFolderLabels ?? true, dryRun: config.dryRun ?? false }, true); return;
  }
  if (command === 'status') {
    const state = readState(path.join(stateDir, 'state.json'));
    print({ filesTracked: Object.keys(state.files).length, pendingRows: Object.keys(state.pending).length, lastSend: state.lastSend, lastError: state.lastError, nextSend: state.nextSend ? new Date(state.nextSend).toISOString() : null, footprint: state.footprint }, true); return;
  }
  if (command === 'install' || command === 'uninstall') { install(home, fileURLToPath(import.meta.url), command === 'uninstall'); print({ status: command === 'install' ? 'installed' : 'uninstalled' }); return; }
  if (!['run', 'backfill'].includes(command)) throw new Error('invalid_command');
  if (command === 'backfill' && (!flags.has('--since') || !flags.has('--until'))) throw new Error('window_required');
  const result = await runPass({ home, config, stateDir, command, since: flags.get('--since') as string | undefined, until: flags.get('--until') as string | undefined, dryRun: flags.has('--dry-run') ? true : undefined });
  print(flags.has('--json') ? { run: result.stats, rows: result.rows, sessions: Object.values(result.state.sessions), seats: Object.values(result.state.seats) } : { ...result.stats, lastError: result.state.lastError });
}
function isMain() {
  try { return Boolean(process.argv[1]) && fs.realpathSync(process.argv[1]) === fs.realpathSync(fileURLToPath(import.meta.url)); }
  catch { return false; }
}
if (isMain()) {
  main().catch(error => { const code = error instanceof Error ? error.message : ''; console.error(JSON.stringify({ error: /^(invalid_config|invalid_state|state_capacity|source_unreadable|reader_error|locked|invalid_arguments|invalid_command|window_required|macos_required)$/.test(code) ? code : 'reader_error' })); process.exitCode = 1; });
}
