import { createHash } from "node:crypto";
import type { Period } from "./contracts";
export function reject(code: string): never { throw new Error(`economics_rejected:${code}`); }
export function timestamp(value: string): number {
  const ms=Date.parse(value);
  if(!Number.isFinite(ms)||new Date(ms).toISOString()!==value)
    reject("timestamp");
  return ms;
}
export function period(value: Period): {
  start: number;
  end: number;
} {
  const start=timestamp(value.start),end=timestamp(value.end);
  if(end<=start)
    reject("period");
  return { start,end };
}
export function identifier(value: string): string {
  if(typeof value!=="string"||!/^[A-Za-z0-9][A-Za-z0-9:._/#-]{0,255}$/.test(value))
    reject("identifier");
  return value;
}
export function minor(value: string): bigint {
  if(typeof value!=="string"||!/^-?(0|[1-9][0-9]{0,23})$/.test(value)||value==="-0")
    reject("minor_units");
  return BigInt(value);
}
export function count(value: number): number {
  if(!Number.isSafeInteger(value)||value<0)
    reject("count");
  return value;
}
export function digest(value: unknown): string {
  function canonical(v: unknown): string {
    if(v===null||typeof v!=="object")
      return JSON.stringify(v);
    if(Array.isArray(v))
      return `[${v.map(canonical).join(",")}]`;
    return `{${Object.entries(v).sort(([a],[b]) => a.localeCompare(b)).map(([k,v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(",")}}`;
  }
  return createHash("sha256").update(canonical(value)).digest("hex");
}
export function assertTenant(expected: string,actual: string): void {
  if(identifier(actual)!==identifier(expected))
    reject("tenant_mismatch");
}
