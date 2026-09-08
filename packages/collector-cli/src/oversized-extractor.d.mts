export type Parser = { version: 1; provider: 'codex'|'claude'; recordStart: number; scanOffset: number;
 status: 'scanning'|'ready'|'refused'; reason: string|null;
 slots: Array<{kind:string;begin:number;end:number}|null> };
export const LIMITS: Readonly<{sliceBytes:number;maxDepth:number;scalarBytes:number;numberBytes:number;stateBytes:number;projectionBytes:number;wallMs:number}>;
export function start(provider: Parser['provider'], recordStart?:number):Parser;
export function feed(state:Parser, bytes:Buffer, options?:{deadline?:number}):number;
export function checkpoint(state:Parser):string;
export function restore(saved:string):Parser;
export function project(state:Parser,read:(at:number,length:number)=>Buffer):Record<string,unknown>;
