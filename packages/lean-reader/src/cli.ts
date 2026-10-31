#!/usr/bin/env node
import os from 'node:os';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadConfig, stateDirectory, readState, runPass, install } from './system.js';
import { nativeAccounts, outboxFile, readOutbox } from './allowance.js';
import { expand } from './reader.js';
export { scan, roots, emptyState, accountKey } from './reader.js';
export { readState, saveState, acquireLock, runPass, launchAgent } from './system.js';
export { makeBatches, sendPending } from './transport.js';
export { allowancePass, nativeAccounts, readOutbox } from './allowance.js';
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
    print({ ...config, endpoint, sendSessionTitles: config.sendSessionTitles ?? false, sendFolderLabels: config.sendFolderLabels ?? true, sendEvidence: config.sendEvidence ?? false, dryRun: config.dryRun ?? false }, true); return;
  }
  const allowanceStatus = () => {
    const outbox = readOutbox(outboxFile(stateDir));
    return { pendingRows: Object.keys(outbox.pending).length, windows: Object.keys(outbox.seen).length, lastSend: outbox.lastSend, lastError: outbox.lastError,
      readError: outbox.readError, nextSend: outbox.nextSend ? new Date(outbox.nextSend).toISOString() : null,
      accountStatus: { pendingRows: Object.keys(outbox.status.pending).length, accounts: Object.keys(outbox.status.seen).length, lastSend: outbox.status.lastSend,
        lastError: outbox.status.lastError, nextSend: outbox.status.nextSend ? new Date(outbox.status.nextSend).toISOString() : null } };
  };
  if (command === 'status') {
    const state = readState(path.join(stateDir, 'state.json'));
    print({ filesTracked: Object.keys(state.files).length, pendingRows: Object.keys(state.pending).length, velocityRows: Object.keys(state.velocity ?? {}).length, pendingVelocityRows: Object.keys(state.velocityPending ?? {}).length, lastSend: state.lastSend, lastError: state.lastError, nextSend: state.nextSend ? new Date(state.nextSend).toISOString() : null, footprint: state.footprint, allowance: config.allowanceFile ? allowanceStatus() : null }, true); return;
  }
  if (command === 'allowance-keys') {
    // Per native account: its provider, hashed key and whether usage already
    // carries that key. Never an email, provider ID or path.
    const accounts = config.allowanceFile ? nativeAccounts(expand(config.allowanceFile, home), home, process.env) : null;
    const state = readState(path.join(stateDir, 'state.json'));
    const seats = new Set(Object.values(state.seats).map(seat => seat.tool + ':' + seat.accountKey));
    const hours = new Set(Object.values(state.hours).map(row => row.tool + ':' + row.accountKey));
    const lines = (accounts ?? []).map(a => ({ tool: a.tool, accountKey: a.accountKey, state: a.state, windows: a.rows.length,
      seat: a.accountKey !== null && seats.has(a.tool + ':' + a.accountKey), hours: a.accountKey !== null && hours.has(a.tool + ':' + a.accountKey),
      signedIn: a.status?.signedIn ?? null, resets: a.status?.resets?.available ?? null }));
    print({ allowanceFile: !config.allowanceFile ? 'not_configured' : accounts ? 'read' : 'missing', accounts: lines.length,
      keyed: lines.filter(l => l.accountKey).length, withSeat: lines.filter(l => l.seat).length, withHours: lines.filter(l => l.hours).length, lines }, true); return;
  }
  if (command === 'install' || command === 'uninstall') { install(home, fileURLToPath(import.meta.url), command === 'uninstall'); print({ status: command === 'install' ? 'installed' : 'uninstalled' }); return; }
  if (['backfill', 'velocity-backfill'].includes(command) && (!flags.has('--since') || !flags.has('--until'))) throw new Error('window_required');
  if (!['run', 'backfill', 'velocity-backfill'].includes(command)) throw new Error('invalid_command');
  const result = await runPass({ home, config, stateDir, command, velocityOnly: command === 'velocity-backfill', since: flags.get('--since') as string | undefined, until: flags.get('--until') as string | undefined, dryRun: flags.has('--dry-run') ? true : undefined });
  print(flags.has('--json') ? { run: result.stats, rows: result.rows, velocity: Object.values(result.state.velocity), sessions: Object.values(result.state.sessions), seats: Object.values(result.state.seats) } : { ...result.stats, lastError: result.state.lastError });
}
function isMain() {
  try { return Boolean(process.argv[1]) && fs.realpathSync(process.argv[1]) === fs.realpathSync(fileURLToPath(import.meta.url)); }
  catch { return false; }
}
if (isMain()) {
  main().catch(error => { const code = error instanceof Error ? error.message : ''; console.error(JSON.stringify({ error: /^(invalid_config|invalid_state|state_capacity|source_unreadable|reader_error|locked|invalid_arguments|invalid_command|window_required|macos_required|allowance_unreadable)$/.test(code) ? code : 'reader_error' })); process.exitCode = 1; });
}
