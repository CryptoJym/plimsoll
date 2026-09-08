// Bounded prefix fingerprint: fixed 4KiB SHA256 chain + hashed partial block.
// Partial bytes are reread and verified, never serialized. Not a MAC/append oracle.
import { createHash } from 'node:crypto';
export const BLOCK_BYTES=4096;
const hash = b => createHash('sha256').update(b).digest('hex');
const safe = n => Number.isSafeInteger(n)&&n>=0;
export function fingerprint(start=0) {
  if(!safe(start))throw new Error('invalid_offset');
  return {version:1,start,end:start,fullBytes:0,fullHash:hash('plimsoll-prefix-v1:'+start),partialHash:null};
}
function validate(f) {
  if(!f||Object.keys(f).sort().join(',')!=='end,fullBytes,fullHash,partialHash,start,version'||f.version!==1||
    !safe(f.start)||!safe(f.end)||f.end<f.start||!safe(f.fullBytes)||f.fullBytes%BLOCK_BYTES||
    f.fullBytes>f.end-f.start||f.end-f.start-f.fullBytes>=BLOCK_BYTES||!/^[a-f0-9]{64}$/.test(f.fullHash)||
    (f.end-f.start===f.fullBytes?f.partialHash!==null:!(/^[a-f0-9]{64}$/).test(f.partialHash)))throw new Error('invalid_fingerprint');
}
export function equal(a,b) {validate(a);validate(b);return a.start===b.start&&a.end===b.end&&a.fullBytes===b.fullBytes&&a.fullHash===b.fullHash&&a.partialHash===b.partialHash;}
export function extend(prior,target,read,{maxBytes=65536,deadline=performance.now()+200}={}) {
  validate(prior);if(!safe(target)||target<prior.end||!safe(maxBytes)||maxBytes>65536)throw new Error('invalid_limit');
  const f={...prior};let bytesRead=0;
  // A partial block must be reread in full plus one new byte. Advertise the
  // admission requirement; callers must skip/back off without raising caps.
  const requiredMinimumBytes=prior.end-prior.start-prior.fullBytes+1;
  if(target>prior.end && maxBytes<requiredMinimumBytes)
    return {fingerprint:f,bytesRead,status:'insufficient_budget',requiredMinimumBytes};
  while(f.end<target && bytesRead<maxBytes && performance.now()<deadline) {
    const begin=f.start+f.fullBytes,oldPartial=f.end-begin;
    const length=Math.min(BLOCK_BYTES,target-begin,maxBytes-bytesRead);
    if(length<=oldPartial)break;
    const bytes=read(begin,length);bytesRead+=bytes.length;
    if(bytes.length!==length || (oldPartial && hash(bytes.subarray(0,oldPartial))!==f.partialHash))
      return {fingerprint:prior,bytesRead,status:'changed'};
    if(length===BLOCK_BYTES) {
      const position=Buffer.alloc(8);position.writeBigUInt64BE(BigInt(begin));
      f.fullHash=hash(Buffer.concat([Buffer.from(f.fullHash,'hex'),position,bytes]));
      f.fullBytes+=length;f.partialHash=null;
    } else f.partialHash=hash(bytes);
    f.end=begin+length;
  }
  return {fingerprint:f,bytesRead,status:f.end===target?'complete':f.end>prior.end?'progress':'yield'};
}
/** Classification BEFORE body I/O. The filesystem adapter supplies precise metadata.
 * Append requires bounded full-prefix verification; endpoint hashes cannot prove it.
 */
export function resumePolicy(previous,current,eligible) {
  if(!eligible)return 'excluded';
  if(previous.identity!==current.identity)return 'generation_changed';
  if(current.size<previous.size)return 'rewrite_ambiguous';
  if(current.size===previous.size) return current.mtimeNs===previous.mtimeNs&&current.ctimeNs===previous.ctimeNs?'resume':'rewrite_ambiguous';
  return 'verify_prefix';
}
export function sameSnapshot(a,b) {
  return a.identity===b.identity&&a.size===b.size&&a.mtimeNs===b.mtimeNs&&a.ctimeNs===b.ctimeNs;
}
