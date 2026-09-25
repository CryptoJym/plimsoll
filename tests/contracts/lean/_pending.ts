/**
 * Lean Plimsoll contract tests (eco-6hoxj.164.4, B0). See tests/contracts/lean/README.md and docs/lean/CONTRACTS.md C6.
 *
 * A pending test runs, prints its failure and counts under `# todo`, so the suite exits 0 until the named bead lands and
 * removes the marker in the same change. Surfaces that do not exist yet are loaded at run time so a missing module fails
 * as a test with its path. This directory is outside tsconfig `include` on purpose: a missing surface must never break
 * `tsc --noEmit`.
 */
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../../../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema, type AiInteractionEvent } from "../../../packages/shared/src/index";

export function pending(bead: string, note?: string) {
  return { todo: `pending until ${bead} lands${note ? `: ${note}` : ""}` };
}

export async function loadSurface(relative: string): Promise<Record<string, unknown>> {
  const specifier = new URL(relative, import.meta.url).href;
  try {
    return (await import(specifier)) as Record<string, unknown>;
  } catch (error) {
    throw new Error(`contract surface missing: ${relative} (${(error as Error).message})`);
  }
}

export function fn(surface: Record<string, unknown>, name: string): (...args: unknown[]) => unknown {
  const value = surface[name];
  if (typeof value !== "function") throw new Error(`contract surface missing export: ${name}`);
  return value as (...args: unknown[]) => unknown;
}

type BufferOptions = NonNullable<ConstructorParameters<typeof LocalEventBuffer>[1]> & { lean?: { write?: boolean } };
/** A buffer on a private temporary ledger; `lean.write` is the B2a flag (unknown to the shipped constructor today). */
export function openTempBuffer(options: BufferOptions = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-lean-contract-"));
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), options as ConstructorParameters<typeof LocalEventBuffer>[1]);
  const close = () => {
    try { buffer.close(); } catch { /* already closed */ }
    fs.rmSync(root, { recursive: true, force: true });
  };
  return { buffer, root, close };
}

let nextId = 1;
export function event(overrides: Record<string, unknown> = {}): AiInteractionEvent {
  const id = `00000000-0000-4000-8000-${String(nextId++).padStart(12, "0")}`;
  return aiInteractionEventSchema.parse({
    id,
    sessionId: id,
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: "2026-09-25T10:00:00.000Z",
    actionClass: "other",
    inputTokens: 1,
    outputTokens: 1,
    metadata: { contract: "lean" },
    ...overrides,
  });
}

export function tableSql(db: { prepare(sql: string): { get(...args: unknown[]): unknown } }, name: string): string | null {
  const row = db.prepare("select sql from sqlite_master where type = 'table' and name = ?").get(name) as { sql: string } | undefined;
  return row?.sql ?? null;
}
export function columns(db: { prepare(sql: string): { all(...args: unknown[]): unknown[] } }, name: string) {
  return db.prepare(`pragma table_info(${name})`).all() as Array<{ name: string; type: string; notnull: number; pk: number }>;
}

const MOD = BigInt(1) << BigInt(128);
/** H(id) = first 16 bytes of sha256(lower(trim(id))) as a 128-bit integer (ARCHITECTURE.md §2.2 "Digests"). */
export function memberHash(id: string): bigint {
  const digest = createHash("sha256").update(id.trim().toLowerCase()).digest();
  return BigInt(`0x${digest.subarray(0, 16).toString("hex")}`);
}
export function sumDigest(ids: Iterable<string>): string {
  let total = BigInt(0);
  for (const id of ids) total = (total + memberHash(id)) % MOD;
  return total.toString(16).padStart(32, "0");
}
