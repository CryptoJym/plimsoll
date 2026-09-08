import assert from 'node:assert/strict';
import { LocalEventBuffer } from '../packages/collector-cli/src/buffer';
import { ensureJsonlContinuationStore } from '../packages/collector-cli/src/jsonl-continuation';
const buffer = new LocalEventBuffer(':memory:');
ensureJsonlContinuationStore(buffer.database);
assert(buffer.database.prepare("select name from sqlite_master where name='jsonl_continuations'").get());
buffer.close();
console.log('PASS oversized continuation storage seam');
