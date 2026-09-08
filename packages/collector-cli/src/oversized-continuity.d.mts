export type Fingerprint = {version:1;start:number;end:number;fullBytes:number;fullHash:string;partialHash:string|null};
export type Snapshot = {identity:string;size:number;mtimeNs:string;ctimeNs:string};
export const BLOCK_BYTES:number;
export function fingerprint(start?:number):Fingerprint;
export function equal(a:Fingerprint,b:Fingerprint):boolean;
export function extend(prior:Fingerprint,target:number,read:(at:number,length:number)=>Buffer,options?:{maxBytes?:number;deadline?:number}):{
 fingerprint:Fingerprint;bytesRead:number;status:'insufficient_budget'|'changed'|'complete'|'progress'|'yield';requiredMinimumBytes?:number};
export function resumePolicy(previous:Snapshot,current:Snapshot,eligible:boolean):'excluded'|'generation_changed'|'rewrite_ambiguous'|'resume'|'verify_prefix';
export function sameSnapshot(a:Snapshot,b:Snapshot):boolean;
