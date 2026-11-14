import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { emptyState, saveState, readState, makeBatches, exportLegacyState } from '../dist/plimsoll-lean.mjs';
import { spawnSync } from 'node:child_process';

test('SDK growth does not enlarge the JSON checkpoint and saved pending totals survive reload', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lean-storage-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const file = path.join(dir, 'state.json');
  function fixture(count) {
    const state = emptyState(); state.sdk = { '2026-11-13': {} };
    for (let i = 0; i < count; i++) state.sdk['2026-11-13'][String(i).padStart(24, '0')] =
      ['fixture-' + i, 'unknown', 'fixture-project', 1794528000000, 1794528000000, { '[0,"fixture-model"]': [1, 2, 3, 4, 5] }, true];
    return state;
  }
  saveState(file, fixture(10)); const small = fs.statSync(file).size;
  saveState(file, fixture(1000));
  assert.ok(fs.statSync(file).size <= small + 64, 'SDK session totals must live outside the bounded JSON checkpoint');
  const restored = readState(file), batches = makeBatches(restored, 'fixture-tenant', 1794614400000);
  assert.equal(batches.flatMap(b => b.body.hours).reduce((n, h) => n + h.requests, 0), 1000);
  assert.ok(batches.flatMap(b => b.body.hours).every(h => h.accountKey === 'unknown'));
});

test('a lost JSON projection and a killed uncommitted write keep the atomic SDK baseline; rollback exports it', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lean-storage-crash-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const file = path.join(dir, 'state.json'), bundle = new URL('../dist/plimsoll-lean.mjs', import.meta.url).href;
  const child = source => {
    const r = spawnSync(process.execPath, ['--input-type=module', '-e', `import {emptyState,saveState,readState,makeBatches,exportLegacyState} from ${JSON.stringify(bundle)}; import fs from 'node:fs'; const file=${JSON.stringify(file)}; ${source}`], { encoding: 'utf8' });
    assert.equal(r.status, 0, r.stderr);
  };
  child(`const s=emptyState(); s.sdk={'2026-11-13':{a:['fixture','unknown',null,1794528000000,1794528000000,{'[0,"fixture"]':[1,2,3,4,5]},true]}};
    s.seenPacked={'498480':Buffer.alloc(8,1).toString('base64')}; s.version=3; saveState(file,s);`);
  fs.unlinkSync(file); // database commit happened, JSON rename did not
  child(`const s=readState(file); if(makeBatches(s,'fixture',1794614400000)[0].body.hours[0].requests!==1) process.exit(9);
    const {DatabaseSync}=process.getBuiltinModule('node:sqlite'); const db=new DatabaseSync(file+'.sdk.sqlite');
    db.exec('BEGIN; DELETE FROM sdk'); process.exit(0);`);
  child(`exportLegacyState(file);`);
  const legacy = JSON.parse(fs.readFileSync(file));
  assert.equal(legacy.version, 3); assert.equal(legacy.sdk['2026-11-13'].a[5]['[0,"fixture"]'][0], 1);
  assert.equal(legacy.sdk['2026-11-13'].a[6], true);
  assert.equal(legacy.seenPacked['498480'], Buffer.alloc(8,1).toString('base64'));
});
