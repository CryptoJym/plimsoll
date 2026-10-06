import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import crypto from 'node:crypto';
import ts from 'typescript';
import {collectorConfigSchema} from '../packages/collector-cli/src/config';
import {createProofCompletion} from './lib/proof-completion';

// Execute the actual private reader with synthetic I/O. No join, listener,
// readiness probe or real profile is started, including ordinary-port controls.
const completion=createProofCompletion('join-fixture-port',29);
const source=fs.readFileSync(path.join(process.cwd(),'packages/collector-cli/src/join.ts'),'utf8');
const ast=ts.createSourceFile('join.ts',source,ts.ScriptTarget.Latest,true);
const declaration=ast.statements.find((n):n is ts.FunctionDeclaration=>
 ts.isFunctionDeclaration(n)&&n.name?.text==='readConfigWithoutCreating');
assert.ok(declaration,'actual production config reader exists');
const text=declaration.getText(ast);
const compiled=ts.transpileModule(text+'\nthis.readConfigWithoutCreating=readConfigWithoutCreating;',
 {compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
type Case={name:string;override?:string;ordinary?:boolean;config?:Record<string,unknown>;
 expected?:number;reject?:boolean;rootEqualsHome?:boolean;outsideHome?:boolean;outsideConfig?:boolean;emptyRoot?:boolean};
const cases:Case[]=[
 ...['49373','49300','49399','49750','49773','49799'].map(port=>({name:'valid-'+port,override:port,expected:Number(port)})),
 ...['49749','49800','not-a-port',undefined,'','0','49773.5','48271','-1','Infinity'].map((port,i)=>
  ({name:'invalid-'+i,override:port,reject:true})),
 {name:'ordinary-default-ignores-invalid-override',ordinary:true,override:'49749',expected:48271},
 {name:'ordinary-default-no-override',ordinary:true,expected:48271},
 {name:'ordinary-configured-port',ordinary:true,config:{port:49123},expected:49123},
 {name:'fixture-configured-safe-port',config:{port:49788},expected:49788},
 {name:'fixture-configured-invalid-override',config:{port:49788},override:'not-a-port',reject:true},
 {name:'fixture-configured-missing-port',config:{},reject:true},
 {name:'fixture-root-equals-home',rootEqualsHome:true,override:'49773',expected:49773},
 {name:'fixture-sibling-home-refuses',outsideHome:true,override:'49773',reject:true},
 {name:'fixture-outside-config-refuses',outsideConfig:true,override:'49773',reject:true},
 {name:'fixture-empty-root-refuses',emptyRoot:true,override:'49773',reject:true},
 {name:'fixture-configured-live-port-refuses',config:{port:48271},reject:true},
 {name:'ordinary-configured-ignores-invalid-override',ordinary:true,config:{port:49123},override:'not-a-port',expected:49123},
 {name:'fixture-configured-unaccepted-port-refuses',config:{port:49123},reject:true},
];
assert.equal(cases.length,29);
const reports:unknown[]=[];
for(const c of cases){
 const home=path.resolve(process.env.HOME!),parent=path.dirname(home);
 const root=c.emptyRoot?'':c.rootEqualsHome?home:parent;
 const fakeHome=c.outsideHome?path.join(parent+'-lookalike','home'):home;
 const file=c.outsideConfig?path.join(parent+'-lookalike','config.json'):path.join(fakeHome,'.plimsoll','synthetic.config.json');
 let reads=0,exists=0;
 const context:{readConfigWithoutCreating?:(file:string)=>{port:number};[key:string]:unknown}={
  fs:{existsSync:()=>{exists++;return c.config!==undefined;},readFileSync:()=>{reads++;return JSON.stringify(c.config);}},
  os:{homedir:()=>fakeHome},path,collectorConfigSchema,
  process:{env:{...(!c.ordinary?{PLIMSOLL_FIXTURE_ROOT:root}:{}),
   ...(c.override!==undefined?{PLIMSOLL_PROOF_JOIN_PORT:c.override}:{})}},
 };
 vm.runInNewContext(compiled,context);
 assert.ok(context.readConfigWithoutCreating);
 let result:{port:number}|undefined,error:string|undefined;
 try{result=context.readConfigWithoutCreating(file);}catch(e){error=(e as Error).message;}
 if(c.reject){assert.match(error??'',/^Join fixture /,c.name);assert.equal(result,undefined,c.name);}
 else{assert.equal(error,undefined,c.name);assert.equal(result?.port,c.expected,c.name);}
 if(c.outsideHome||c.outsideConfig||c.emptyRoot||c.override==='not-a-port'&&!c.ordinary){
  assert.equal(exists,0,c.name+': refuse before filesystem lookup');assert.equal(reads,0,c.name+': refuse before file read');
 }
 assert.ok(reads<=1&&exists<=1,'bounded synthetic config I/O');
 reports.push({name:c.name,passed:true,selectedPort:result?.port??null,error:error??null,exists,reads,networkCalls:0});
 completion.check(c.name);
}
console.log(JSON.stringify({proof:'join-fixture-port',actualFunctionSha256:crypto.createHash('sha256').update(text).digest('hex'),
 method:'AST-extracted actual production reader; synthetic fs/os/process; no network or real profile access',reports}));
completion.complete();
