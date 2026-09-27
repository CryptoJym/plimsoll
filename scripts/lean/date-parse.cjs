// Parse a JSON array of stored timestamps with the same V8 Date.parse used by
// dashboard-projection.ts. This script is run with the collector's Node binary.
const fs = require('node:fs');
const values = JSON.parse(fs.readFileSync(0, 'utf8'));
if (!Array.isArray(values) || !values.every((value) => typeof value === 'string')) {
  throw new Error('expected a JSON array of timestamp strings');
}
process.stdout.write(JSON.stringify({
  node: process.version,
  parsed: values.map((value) => {
    const ms = Date.parse(value);
    return Number.isSafeInteger(ms) ? ms : null;
  }),
}));
