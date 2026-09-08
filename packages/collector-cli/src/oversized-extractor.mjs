// Research prototype: no filesystem/DB writes or unselected string buffers.
// Persistent state contains grammar states, allowlist node IDs and byte spans.
import { createHash } from 'node:crypto';
export const LIMITS = Object.freeze({ sliceBytes: 65536, maxDepth: 64, scalarBytes: 4096,
  numberBytes: 128, stateBytes: 32768, projectionBytes: 16384, wallMs: 200 });
export const PATHS = Object.freeze({
  codex: ['type','timestamp','payload','payload.id','payload.timestamp','payload.cwd',
    'payload.originator','payload.cli_version','payload.model','payload.type','payload.info',
    'payload.info.total_token_usage','payload.info.total_token_usage.input_tokens',
    'payload.info.total_token_usage.cached_input_tokens','payload.info.total_token_usage.output_tokens',
    'payload.info.total_token_usage.reasoning_output_tokens','payload.rate_limits','payload.rate_limits.plan_type'],
  claude: ['type','sessionId','timestamp','cwd','message','message.id','message.model','message.usage',
    'message.usage.input_tokens','message.usage.cache_read_input_tokens',
    'message.usage.cache_creation_input_tokens','message.usage.output_tokens'],
});
const schemas = Object.fromEntries(Object.entries(PATHS).map(([provider, paths]) => {
  const nodes = [{ path:'', key:'', parent:-1, children:[] }];
  for (const path of paths) {
    const parts=path.split('.'); const key=parts.pop();
    const parent=nodes.findIndex(n=>n.path===parts.join('.'));
    nodes.push({path,key,parent,children:[]}); nodes[parent].children.push(nodes.length-1);
  }
  return [provider,nodes];
}));
const digest = text => createHash('sha256').update(text).digest('hex');
const safe = n => Number.isSafeInteger(n) && n >= 0;
const ws = b => b===32 || b===9 || b===13;
const digit = b => b>=48 && b<=57;
const hex = b => b>=48&&b<=57 ? b-48 : b>=65&&b<=70 ? b-55 : b>=97&&b<=102 ? b-87 : -1;
const terminals = new Set(['zero','int','frac','expdigits']);
const reasons = ['malformed_json','invalid_utf8','max_depth','allowed_scalar_too_large',
  'allowed_number_too_large','allowed_projection_too_large','offset_overflow','newline_before_end'];
export function start(provider, recordStart=0) {
  if (!Object.hasOwn(schemas,provider) || !safe(recordStart)) throw new Error('invalid_start');
  return { version:1, provider, recordStart, scanOffset:recordStart, status:'scanning', reason:null,
    root:'value', stack:[], lex:null, slots:[] };
}
function refuse(s, reason) { s.status='refused'; s.reason=reason; }
function mark(s, node, kind, begin, end) {
  if (node>=0) s.slots[node] = {kind,begin,end};
}
function beginValue(s, node) {
  const nodes=schemas[s.provider];
  if(node>=0) for(let n=0;n<nodes.length;n++) {
    if(n===node || nodes[n].path.startsWith(nodes[node].path+'.')) s.slots[n]=null;
  }
  const parent=s.stack.at(-1);
  if(parent) parent.phase='comma'; else s.root='end';
}
function matchKey(s, l, code) {
  if(!l.isKey) return;
  const nodes=schemas[s.provider];
  l.matched=l.matched.filter(n=>nodes[n].key.charCodeAt(l.index)===code);
  // Once unmatched, arbitrary unknown key length is not accumulated.
  if(l.matched.length) l.index++;
}
function finishString(s) {
  const l=s.lex;
  if(l.isKey) {
    const p=s.stack.at(-1);
    p.key=l.matched.find(n=>schemas[s.provider][n].key.length===l.index) ?? -1;
    p.phase='colon';
  } else mark(s,l.node,'string',l.start,s.scanOffset+1);
  s.lex=null;
}
function stringByte(s,b) {
  const l=s.lex;
  if(l.node>0 && s.scanOffset-l.start+1>LIMITS.scalarBytes) return refuse(s,'allowed_scalar_too_large');
  if(l.utfLeft) {
    if(b<l.utfMin || b>l.utfMax) return refuse(s,'invalid_utf8');
    l.utfLeft--; l.utfMin=128; l.utfMax=191; return;
  }
  if(l.mode==='unicode') {
    const h=hex(b); if(h<0) return refuse(s,'malformed_json');
    if(l.isKey && l.matched.length) l.unicodeValue=l.unicodeValue*16+h;
    if(--l.unicodeLeft===0) { matchKey(s,l,l.unicodeValue); l.unicodeValue=0; l.mode='plain'; }
    return;
  }
  if(l.mode==='escape') {
    if(b===117) { l.mode='unicode'; l.unicodeLeft=4; return; }
    const escapes={34:34,92:92,47:47,98:8,102:12,110:10,114:13,116:9};
    if(!Object.hasOwn(escapes,b)) return refuse(s,'malformed_json');
    matchKey(s,l,escapes[b]); l.mode='plain'; return;
  }
  if(b===34) return finishString(s);
  if(b===92) { l.mode='escape'; return; }
  if(b<32) return refuse(s,'malformed_json');
  if(b<128) { matchKey(s,l,b); return; }
  matchKey(s,l,0xFFFD);
  l.utfMin=128; l.utfMax=191;
  if(b>=194&&b<=223) l.utfLeft=1;
  else if(b>=224&&b<=239) { l.utfLeft=2; if(b===224) l.utfMin=160; if(b===237) l.utfMax=159; }
  else if(b>=240&&b<=244) { l.utfLeft=3; if(b===240) l.utfMin=144; if(b===244) l.utfMax=143; }
  else refuse(s,'invalid_utf8');
}
function numberByte(s,b) {
  const l=s.lex; let next;
  switch(l.phase) {
    case 'minus': next=b===48?'zero':b>=49&&b<=57?'int':null; break;
    case 'zero': next=b===46?'dot':b===101||b===69?'exp':undefined; break;
    case 'int': next=digit(b)?'int':b===46?'dot':b===101||b===69?'exp':undefined; break;
    case 'dot': next=digit(b)?'frac':null; break;
    case 'frac': next=digit(b)?'frac':b===101||b===69?'exp':undefined; break;
    case 'exp': next=b===43||b===45?'expsign':digit(b)?'expdigits':null; break;
    case 'expsign': next=digit(b)?'expdigits':null; break;
    case 'expdigits': next=digit(b)?'expdigits':undefined; break;
  }
  if(next===null) { refuse(s,'malformed_json'); return true; }
  if(next===undefined) {
    if(!terminals.has(l.phase)) refuse(s,'malformed_json');
    else { mark(s,l.node,'number',l.start,s.scanOffset); s.lex=null; }
    return false; // delimiter must also pass the structural grammar
  }
  if(l.node>0 && s.scanOffset-l.start+1>LIMITS.numberBytes) refuse(s,'allowed_number_too_large');
  l.phase=next; return true;
}
function openString(s,node,isKey) {
  const parent=s.stack.at(-1);
  s.lex={kind:'string',node,start:s.scanOffset,isKey,mode:'plain',unicodeValue:0,unicodeLeft:0,
    utfLeft:0,utfMin:128,utfMax:191,index:0,
    matched:isKey&&parent.node>=0 ? [...schemas[s.provider][parent.node].children] : []};
}
function structureByte(s,b) {
  const p=s.stack.at(-1);
  if(b===10) {
    if(p || s.lex) refuse(s,'newline_before_end');
    else if(s.slots.reduce((n,x)=>n+(x&&['string','number'].includes(x.kind)?x.end-x.begin:0),0)>LIMITS.projectionBytes) refuse(s,'allowed_projection_too_large');
    else s.status='ready'; // blank lines are complete, irrelevant records
    return;
  }
  if(ws(b)) return;
  if(p?.phase==='colon') {
    if(b!==58) return refuse(s,'malformed_json'); p.phase='value'; return;
  }
  if(p?.phase==='comma') {
    if(b===(p.kind==='object'?125:93)) { s.stack.pop(); return; }
    if(b!==44) return refuse(s,'malformed_json');
    p.phase=p.kind==='object'?'key':'value'; return;
  }
  if(p?.kind==='object' && (p.phase==='key0'||p.phase==='key')) {
    if(b===125 && p.phase==='key0') { s.stack.pop(); return; }
    if(b!==34) return refuse(s,'malformed_json'); openString(s,-1,true); return;
  }
  if(p?.kind==='array' && p.phase==='value0' && b===93) { s.stack.pop(); return; }
  if(!p && s.root==='end') return refuse(s,'malformed_json');
  const node=p ? p.kind==='object'?p.key:-1 : 0;
  beginValue(s,node);
  if(b===123||b===91) {
    if(s.stack.length>=LIMITS.maxDepth) return refuse(s,'max_depth');
    const kind=b===123?'object':'array'; mark(s,node,kind,s.scanOffset,s.scanOffset+1);
    s.stack.push({kind,node:kind==='object'?node:-1,phase:kind==='object'?'key0':'value0',key:-1});
  } else if(b===34) openString(s,node>0?node:-1,false);
  else if(b===45||digit(b)) s.lex={kind:'number',node:node>0?node:-1,start:s.scanOffset,phase:b===45?'minus':b===48?'zero':'int'};
  else if(b===116||b===102||b===110) s.lex={kind:'literal',node,start:s.scanOffset,word:b===116?'true':b===102?'false':'null',index:1};
  else refuse(s,'malformed_json');
}
/** One bounded chunk, at most one completed record. Never changes recordStart. */
export function feed(s, bytes, {deadline=performance.now()+LIMITS.wallMs}={}) {
  if(!Buffer.isBuffer(bytes) || bytes.length>LIMITS.sliceBytes) throw new Error('chunk_limit');
  let used=0;
  while(used<bytes.length && s.status==='scanning') {
    if((used&255)===0 && performance.now()>=deadline) break;
    if(!safe(s.scanOffset+1)) { refuse(s,'offset_overflow'); break; }
    const b=bytes[used]; let consume=true;
    if(s.lex?.kind==='string') stringByte(s,b);
    else if(s.lex?.kind==='number') consume=numberByte(s,b);
    else if(s.lex?.kind==='literal') {
      const l=s.lex;
      if(b!==l.word.charCodeAt(l.index)) refuse(s,'malformed_json');
      else if(++l.index===l.word.length) { mark(s,l.node,l.word,l.start,s.scanOffset+1); s.lex=null; }
    } else structureByte(s,b);
    if(s.status==='refused') break;
    if(consume) { used++; s.scanOffset++; }
  }
  return used;
}
function exact(value, keys) {
  return value && !Array.isArray(value) && typeof value==='object' &&
    Object.keys(value).sort().join(',')===[...keys].sort().join(',');
}
function valid(s) {
  if(!exact(s,['version','provider','recordStart','scanOffset','status','reason','root','stack','lex','slots']) ||
    s.version!==1 || !Object.hasOwn(schemas,s.provider) || !safe(s.recordStart)||!safe(s.scanOffset)||s.scanOffset<s.recordStart ||
    !['scanning','ready','refused'].includes(s.status) || !['value','end'].includes(s.root) ||
    (s.status==='refused'?!reasons.includes(s.reason):s.reason!==null) ||
    !Array.isArray(s.stack)||s.stack.length>LIMITS.maxDepth || !Array.isArray(s.slots)||s.slots.length>schemas[s.provider].length) return false;
  const node=n=>Number.isInteger(n)&&n>=-1&&n<schemas[s.provider].length;
  if(!s.stack.every(f=>exact(f,['kind','node','phase','key']) && ['object','array'].includes(f.kind) && node(f.node) && node(f.key) &&
    (f.kind==='object'?['key0','key','colon','value','comma']:['value0','value','comma']).includes(f.phase))) return false;
  if(!s.slots.every(x=>x===null || (exact(x,['kind','begin','end']) && ['object','array','true','false','null','string','number'].includes(x.kind) &&
    safe(x.begin)&&safe(x.end)&&x.begin>=s.recordStart&&x.end>x.begin&&x.end<=s.scanOffset &&
    (x.kind==='string'?x.end-x.begin<=LIMITS.scalarBytes:x.kind==='number'?x.end-x.begin<=LIMITS.numberBytes:true)))) return false;
  const l=s.lex;
  if(l!==null) {
    if(!node(l.node)||!safe(l.start)||l.start<s.recordStart||l.start>s.scanOffset) return false;
    if(l.kind==='number') {
      if(!exact(l,['kind','node','start','phase'])||!['minus','zero','int','dot','frac','exp','expsign','expdigits'].includes(l.phase)) return false;
    } else if(l.kind==='literal') {
      if(!exact(l,['kind','node','start','word','index'])||!['true','false','null'].includes(l.word)||!safe(l.index)||l.index<1||l.index>=l.word.length) return false;
    } else if(l.kind==='string') {
      if(!exact(l,['kind','node','start','isKey','mode','unicodeValue','unicodeLeft','utfLeft','utfMin','utfMax','index','matched']) ||
        typeof l.isKey!=='boolean'||!['plain','escape','unicode'].includes(l.mode) || !safe(l.unicodeValue)||l.unicodeValue>65535 ||
        !safe(l.unicodeLeft)||l.unicodeLeft>4||!safe(l.utfLeft)||l.utfLeft>3||!safe(l.utfMin)||l.utfMin<128||l.utfMin>191||
        !safe(l.utfMax)||l.utfMax>191||l.utfMax<l.utfMin||!safe(l.index)||l.index>32||
        !Array.isArray(l.matched)||l.matched.length>schemas[s.provider].length||!l.matched.every(n=>node(n)&&n>0)) return false;
    } else return false;
  }
  return s.status!=='ready' || (s.stack.length===0 && s.lex===null);
}
export function checkpoint(s) {
  if(!valid(s)) throw new Error('invalid_checkpoint');
  const body=JSON.stringify(s); const saved=JSON.stringify({body:s,sha256:digest(body)});
  if(Buffer.byteLength(saved)>LIMITS.stateBytes) throw new Error('checkpoint_limit');
  return saved;
}
export function restore(saved) {
  if(typeof saved!=='string'||Buffer.byteLength(saved)>LIMITS.stateBytes) throw new Error('checkpoint_limit');
  const value=JSON.parse(saved);
  if(!exact(value,['body','sha256'])||digest(JSON.stringify(value.body))!==value.sha256||!valid(value.body)) throw new Error('invalid_checkpoint');
  return value.body;
}
/** Ephemeral only. Caller must validate generation, charge reads, and atomically ingest.
 * JSON.parse is applied ONLY to an allowlisted <=4096-byte scalar, never a record.
 */
export function project(s, readSpan) {
  if(s.status!=='ready'||!valid(s)) throw new Error('record_not_complete');
  const nodes=schemas[s.provider]; const values=[]; let total=0;
  for(let i=0;i<s.slots.length;i++) {
    const slot=s.slots[i]; if(!slot) continue;
    let value;
    if(slot.kind==='object') value={};
    else if(slot.kind==='array') value=[];
    else if(slot.kind==='true') value=true;
    else if(slot.kind==='false') value=false;
    else if(slot.kind==='null') value=null;
    else {
      const length=slot.end-slot.begin; total+=length;
      if(total>LIMITS.projectionBytes) throw new Error('projection_limit');
      const bytes=readSpan(slot.begin,length);
      if(!Buffer.isBuffer(bytes)||bytes.length!==length) throw new Error('span_changed');
      value=JSON.parse(bytes.toString('utf8'));
    }
    values[i]=value;
    const parent=values[nodes[i].parent];
    if(i>0 && parent && typeof parent==='object' && !Array.isArray(parent)) parent[nodes[i].key]=value;
  }
  return values[0] && typeof values[0]==='object' && !Array.isArray(values[0]) ? values[0] : {};
}
