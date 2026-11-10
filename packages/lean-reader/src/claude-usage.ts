/*!
 * Adapted from T3 Code claudeUsage.ts at fa2f4ff2a2bac5ac92e07749f6b83b0003d642a0.
 * MIT License
 * Copyright (c) 2026 T3 Tools Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
import { integer, string, type Counts } from './types.js';

export type ClaudeUsage = { id: string; model: string; tokens: Counts; advisor: boolean };

/** Top-level usage belongs to the executor; advisor iterations are independent
 * requests. Keep Plimsoll's executor identity byte-for-byte for saved dedup state.
 * Advisor identities extend it with T3's iteration index, never content text. */
export function claudeUsage(record: Record<string, any>): ClaudeUsage[] {
  const msg = record.message, usage = msg?.usage;
  if (record.type !== 'assistant' || !usage || typeof usage !== 'object') return [];
  const id = JSON.stringify(msg.id || record.requestId || record.uuid || null);
  const tokens = (u: Record<string, any>): Counts => ({ requests: 1,
    freshInput: integer(u.input_tokens), cacheRead: integer(u.cache_read_input_tokens),
    cacheWrite: integer(u.cache_creation_input_tokens), output: integer(u.output_tokens) });
  const model = string(msg.model) || 'unknown';
  if (model === '<synthetic>') return [];
  const rows: ClaudeUsage[] = [{ id, model, tokens: tokens(usage), advisor: false }];
  for (const [index, item] of (Array.isArray(usage.iterations) ? usage.iterations : []).entries()) {
    if (!item || item.type !== 'advisor_message' || typeof item.model !== 'string' || !item.model || item.model === '<synthetic>') continue;
    rows.push({ id: `${id}:advisor:${index}`, model: string(item.model), tokens: tokens(item), advisor: true });
  }
  return rows;
}
