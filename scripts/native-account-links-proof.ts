import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { ACCOUNT_ASSERTION_SALT_FILE, deriveAccountActorHash } from "../packages/collector-cli/src/account-assertion";
import { capturedAccountHash, deriveNativeProviderAccountHash, nativeAccountLinkEvidence, nativeAccountLinkResolver, NATIVE_PROVIDER_ACCOUNT_DERIVATION, readExistingNativeAccountSalt } from "../packages/collector-cli/src/native-account-links";
import { collectNativeTelemetry, nativeTelemetryReport, persistNativeTelemetry, replayNativeTelemetryEvidence } from "../packages/collector-cli/src/native-telemetry-store";
import { readNativeLedger } from "../packages/collector-cli/src/native-ledger";
import { inspectNativeSubscriptionSurfaces, type NativeSubscriptionPaths } from "../packages/collector-cli/src/native-subscription";
import { terminalPrivacyEligibilitySql } from "../packages/collector-cli/src/privacy-disposition";
import { linkageHash, remoteLinkageHash } from "../packages/shared/src/linkage";

// Every native feed, identity, salt, transcript and database in this proof is
// synthetic and confined to this fixture root. No default native path is read.
const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-native-links-")));
const now = "2026-10-14T12:00:00.000Z";
const paths: NativeSubscriptionPaths = {
  nativeUsageFile: path.join(root,"feed.json"), codexAuthFile: path.join(root,"auth.json"),
  claudeConfigFile: path.join(root,"claude.json"), cursorCliConfigFile: path.join(root,"cursor.json"),
  claudeSeatsRoot: path.join(root,"claude-seats"),
  cursorTrackingDb: path.join(root,"cursor.sqlite"), grokConfigFile: path.join(root,"grok.toml"),
  collectorDb: path.join(root,"ledger.sqlite"), nativeSubscriptionDb: path.join(root,"sidecar.sqlite"),
  dashboardEvidenceFile: path.join(root,"dashboard.json"),
};
const options = { paths, now, codexStateFile: path.join(root,"state.sqlite"), claudeProjectsRoot: path.join(root,"transcripts") };
const salt = crypto.randomBytes(32);
const saltFile = path.join(root,ACCOUNT_ASSERTION_SALT_FILE);
const ids = { a:"fixture-native-alpha", b:"fixture-native-beta", c:"fixture-native-gamma" };
const hashes = { a:deriveAccountActorHash(ids.a,salt), b:deriveAccountActorHash(ids.b,salt), c:deriveAccountActorHash(ids.c,salt) };
const explicit = linkageHash("fixture-explicit-project")!;
const alternate = linkageHash("fixture-alternate-project")!;
const repository = remoteLinkageHash("https://github.com/example/fixture-repository.git")!;
const stateRepository = remoteLinkageHash("https://github.com/example/fixture-state.git")!;
const nativeAccount = (provider: string, key: string, id?: string, matchesExpected = true) => ({
  provider,accountKey:key,state:"OK",identity:{authenticated:true,matchesExpected,observedAt:now,
    proof:"provider_native_identity_response",planType:"paid",email:"fixture-private@example.invalid",
    ...(id ? provider === "codex" ? {providerAccountId:id} : {organizationId:`fixture-organization-${key}`} : {})},
  source:{state:"OK",observedAt:now,proof:"provider_native_identity_response",...(provider === "claude" ? {profile:key} : {})},
});
const accounts = [nativeAccount("codex","fixture-key-a",ids.a),nativeAccount("codex","fixture-key-b",ids.b),
  nativeAccount("claude","fixture-key-ca",ids.a),nativeAccount("claude","fixture-key-c",ids.c)];
const writeFeed = (items: ReturnType<typeof nativeAccount>[]) => fs.writeFileSync(paths.nativeUsageFile,
  JSON.stringify({generatedAt:now,state:"OK",accounts:items}));
const digestFile = (file: string) => crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
const databases: Database.Database[] = [];
const open = (file = ":memory:") => { const db = new Database(file); databases.push(db); return db; };

function native047Fixtures() {
  const home = path.join(root,"native047");
  fs.mkdirSync(home,{mode:0o700});
  const fixturePaths: NativeSubscriptionPaths = Object.fromEntries(Object.keys(paths).map(key =>
    [key,path.join(home,key)])) as NativeSubscriptionPaths;
  const fakeId = "fixture-native047-personal-account";
  const fakeOther = "fixture-native047-second-account";
  const fakeEmail = "fixture-native047@example.invalid";
  const fakeOrg = "fixture-native047-organization";
  const boxed = (id: string) => `sha256:${crypto.createHash("sha256").update(JSON.stringify({stringValue:id})).digest("hex").slice(0,16)}`;
  const generic = `sha256:${crypto.createHash("sha256").update(fakeId).digest("hex").slice(0,16)}`;
  assert.equal(deriveNativeProviderAccountHash(fakeId),boxed(fakeId));
  assert.notEqual(boxed(fakeId),generic);
  assert.notEqual(boxed(fakeId),deriveAccountActorHash(fakeId,salt).slice(0,23));
  assert.equal(capturedAccountHash("0123456789abcdef"),null);
  assert.equal(capturedAccountHash("sha256:" + "a".repeat(32)),null);
  const profileDir = path.join(fixturePaths.claudeSeatsRoot!,"registered");
  fs.mkdirSync(profileDir,{recursive:true,mode:0o700});
  const profileFile = path.join(profileDir,".claude.json");
  const config = { oauthAccount:{ accountUuid:fakeId,emailAddress:fakeEmail,organizationUuid:fakeOrg } };
  const writeConfig = (value = config) => fs.writeFileSync(profileFile,JSON.stringify(value),{mode:0o600});
  writeConfig();
  fs.writeFileSync(fixturePaths.claudeConfigFile,JSON.stringify(config),{mode:0o600});
  const nativeRows = [
    { provider:"codex",accountKey:"fixture-native047-codex",state:"OK",identity:{authenticated:true,
      providerAccountId:fakeId,email:fakeEmail,planType:"paid",proof:"provider_account_read",matchesExpected:true,observedAt:now},source:{state:"OK",observedAt:now} },
    { provider:"claude",accountKey:"fixture-native047-claude",state:"OK",identity:{authenticated:true,
      email:fakeEmail,organizationId:fakeOrg,planType:"max",proof:"provider_usage_response",matchesExpected:true,observedAt:now},
      source:{state:"OK",profile:"registered",observedAt:now} },
  ];
  const feed = (rows: unknown[] = nativeRows) => fs.writeFileSync(fixturePaths.nativeUsageFile,JSON.stringify({generatedAt:now,accounts:rows}));
  feed();
  const fixtureOptions = { paths:fixturePaths,now,codexStateFile:path.join(home,"missing-state"),claudeProjectsRoot:path.join(home,"missing-transcripts") };
  const raw = open(fixturePaths.collectorDb);
  raw.exec(`create table buffered_events(id text primary key,source text,event_type text,observed_at text,session_id text,
    model text,input_tokens integer,output_tokens integer,project_key text,repo_hash text,account_hash text,
    data_mode text,privacy_generation text,privacy_disposition text,usage_duplicate_reason text,payload_json text);
    create table session_usage_authority(source text,session_id text,authority text,primary key(source,session_id));
    create table upload_receipts(delivery_id text primary key,reason text);`);
  const add = (id: string,source: string,actor: string | null,metadata: unknown,mode = "metadata_only",payload?: string) => {
    raw.prepare("insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").run(id,source,"assistant_response",now,id,"fixture-model",10,2,
      id === "claude-captured" ? explicit : null,null,actor,mode,"fixture-generation",null,null,
      payload ?? JSON.stringify({metadata,prompt:"fixture-native047-body-sentinel",body:{"user.account_uuid":boxed(fakeOther)}}));
    raw.prepare("insert or ignore into session_usage_authority values (?,?,?)").run(source,id,"live");
  };
  add("codex-captured","codex",boxed(fakeId),{"user.account_id":boxed(fakeId)});
  add("claude-captured","claude_code",generic,{"user.id":generic,"user.account_id":boxed(fakeOther),"user.account_uuid":boxed(fakeId)});
  add("claude-actor-only","claude_code",boxed(fakeId),{"user.account_id":boxed(fakeId)});
  add("codex-legacy-short","codex",boxed(fakeId),{});
  add("missing","claude_code",null,{});
  add("mismatched","codex",boxed(fakeId),{"user.account_id":boxed(fakeOther)});
  add("wrong-codex-field","codex",null,{"user.account_uuid":boxed(fakeId)});
  add("object-value","claude_code",null,{"user.account_uuid":{stringValue:boxed(fakeId)}});
  add("raw-value","claude_code",fakeId,{"user.account_uuid":fakeId});
  add("array-value","claude_code",null,{"user.account_uuid":[boxed(fakeId)]});
  add("malformed","claude_code",null,{},"metadata_only","{not-json");
  add("rejected","claude_code",generic,{"user.account_uuid":boxed(fakeId)});
  raw.prepare("update session_usage_authority set authority='tailer' where session_id='rejected'").run();
  add("privacy-evidence-047","claude_code",generic,{"user.account_uuid":boxed(fakeId)},"evidence");
  add("privacy-quarantine-047","claude_code",generic,{"user.account_uuid":boxed(fakeId)});
  raw.prepare("insert into upload_receipts values (?,?)").run("privacy-quarantine-047","local_privacy_violation");
  add("privacy-duplicate-047","claude_code",generic,{"user.account_uuid":boxed(fakeId)});
  raw.prepare("update buffered_events set usage_duplicate_reason='duplicate' where id='privacy-duplicate-047'").run();
  const reading = readNativeLedger(fixturePaths.collectorDb,{limit:50_000});
  assert.equal(reading.state,"observed",reading.note);
  assert.equal(reading.rawRowCount,15);
  assert.equal(reading.events.length,12);
  const captured = reading.events.find(event => event.id === "claude-captured")!;
  assert.equal(captured.accountHash,generic);
  assert.equal(captured.providerAccountHash,boxed(fakeId));
  assert.equal(captured.providerAccountEvidence,"native047_metadata_user_account_uuid");
  for (const id of ["claude-actor-only","missing","wrong-codex-field","object-value","raw-value","array-value","malformed","rejected"]) {
    assert.equal(reading.events.find(event => event.id === id)?.providerAccountHash,null,id);
  }
  assert.equal(JSON.stringify(reading).includes("body-sentinel"),false);
  assert.equal(JSON.stringify(reading).includes(fakeId),false);
  const recent = readNativeLedger(fixturePaths.collectorDb,{limit:3});
  assert.equal(recent.rawRowCount,3);
  assert.equal(recent.events.length,0); // Never widen the window to find eligible rows.
  assert.throws(() => readNativeLedger(fixturePaths.collectorDb,{limit:50_001}),/limit_invalid/);
  assert.throws(() => readNativeLedger(fixturePaths.collectorDb,{timeoutMs:0}),/timeout_invalid/);
  const inspect = (rows: unknown[] = nativeRows,at = now) => {
    feed(rows);
    return inspectNativeSubscriptionSurfaces({paths:fixturePaths,now:at,ledgerReading:reading});
  };
  const inspection = inspect();
  const codex = inspection.accounts.find(account => account.provider === "openai")!;
  const claude = inspection.accounts.find(account => account.provider === "anthropic")!;
  const codexClaim = codex.accountLinks!.find(claim => claim.derivation === NATIVE_PROVIDER_ACCOUNT_DERIVATION)!;
  const claudeClaim = claude.accountLinks!.find(claim => claim.derivation === NATIVE_PROVIDER_ACCOUNT_DERIVATION)!;
  assert.equal(codexClaim.accountHash,boxed(fakeId));
  assert.equal(claudeClaim.accountHash,boxed(fakeId));
  assert.equal(claudeClaim.state,"observed");
  assert.equal(claudeClaim.identityProvenance,"registered_claude_profile_account_uuid");
  assert.notEqual(claudeClaim.accountHash,boxed(fakeOrg));
  const expectedRef = (key: string) => `native:${crypto.createHash("sha256").update(key).digest("hex").slice(0,32)}`;
  assert.equal(claude.accountRef,expectedRef(nativeRows[1].accountKey));
  assert.equal(fs.existsSync(path.join(home,ACCOUNT_ASSERTION_SALT_FILE)),false);
  const fullClaim = nativeAccountLinkEvidence({provider:"openai",accountRef:codex.accountRef,nativeIdentity:fakeId,
    authenticated:true,matchesExpected:true,proof:"provider_account_read",observedAt:now,now},salt)!;
  const resolver = nativeAccountLinkResolver([codexClaim,claudeClaim,fullClaim]);
  assert.equal(resolver("codex",boxed(fakeId)).accountRef,codex.accountRef);
  assert.equal(resolver("codex",fullClaim.accountHash).accountRef,codex.accountRef);
  assert.equal(resolver("claude_code",boxed(fakeId)).subscriptionAccountJoin,"UNKNOWN");
  assert.equal(resolver("claude_code",boxed(fakeId),false,NATIVE_PROVIDER_ACCOUNT_DERIVATION).accountRef,claude.accountRef);
  assert.equal(nativeAccountLinkResolver([claudeClaim])("codex",boxed(fakeId)).unknownReason,"cross_provider_identity");
  assert.equal(nativeAccountLinkResolver([codexClaim,{...codexClaim,accountRef:"native:fixture-second-ref"}])("codex",boxed(fakeId)).unknownReason,"ambiguous_native_identity");
  assert.equal(nativeAccountLinkResolver([codexClaim,{...codexClaim,accountHash:boxed(fakeOther)}])("codex",boxed(fakeId)).unknownReason,"conflicting_native_identity");
  assert.equal(nativeAccountLinkResolver([{...claudeClaim,identityProvenance:null}])("claude_code",boxed(fakeId),false,
    NATIVE_PROVIDER_ACCOUNT_DERIVATION).subscriptionAccountJoin,"UNKNOWN");
  const mixedStore = open();
  const mixedInspection = {...inspection,accounts:[{...codex,accountLink:fullClaim,accountLinks:[fullClaim,codexClaim]}],
    ledgerReading:{...reading,events:[{...reading.events[0],accountHash:fullClaim.accountHash,
      providerAccountHash:null,providerAccountDerivation:null,providerAccountEvidence:null}]}};
  persistNativeTelemetry(mixedStore,mixedInspection,fixtureOptions);
  persistNativeTelemetry(mixedStore,{...mixedInspection,ledgerReading:{...reading,events:[reading.events[0]]}},
    {...fixtureOptions,replayEvidenceOnly:true});
  assert.equal(nativeTelemetryReport(mixedStore).threads[0].accountRef,codex.accountRef,
    "salted and boxed representations of the same account must not create a replay conflict");

  const unknownClaude = (result: ReturnType<typeof inspect>) => assert.ok(result.accounts.find(account => account.provider === "anthropic")!
    .accountLinks!.every(claim => claim.state === "UNKNOWN"));
  writeConfig({oauthAccount:{...config.oauthAccount,emailAddress:"fixture-other@example.invalid"}}); unknownClaude(inspect());
  writeConfig({oauthAccount:{...config.oauthAccount,organizationUuid:"fixture-other-organization"}}); unknownClaude(inspect());
  writeConfig();
  unknownClaude(inspect([nativeRows[0],{...nativeRows[1],identity:{...nativeRows[1].identity,email:null}}]));
  unknownClaude(inspect([nativeRows[0],{...nativeRows[1],source:{...nativeRows[1].source,profile:"../escape"}}]));
  unknownClaude(inspect([nativeRows[0],{...nativeRows[1],source:{...nativeRows[1].source,profile:"missing"}}]));
  fs.chmodSync(profileFile,0o666); unknownClaude(inspect()); fs.chmodSync(profileFile,0o600);
  fs.chmodSync(profileDir,0o777); unknownClaude(inspect()); fs.chmodSync(profileDir,0o700);
  const actualProfile = path.join(profileDir,"saved-config");
  fs.renameSync(profileFile,actualProfile); fs.symlinkSync(actualProfile,profileFile); unknownClaude(inspect());
  fs.unlinkSync(profileFile); fs.renameSync(actualProfile,profileFile);
  const actualDir = path.join(fixturePaths.claudeSeatsRoot!,"saved-profile");
  fs.renameSync(profileDir,actualDir); fs.symlinkSync(actualDir,profileDir); unknownClaude(inspect());
  fs.unlinkSync(profileDir); fs.renameSync(actualDir,profileDir);
  const localOnly = {...nativeRows[1],source:{state:"OK",observedAt:now}};
  const localInspection = inspect([nativeRows[0],localOnly]);
  assert.ok(localInspection.accounts.find(account => account.provider === "anthropic")!.accountLinks!.some(claim => claim.state === "observed"));
  unknownClaude(inspect([nativeRows[0],{...localOnly,identity:{...localOnly.identity,email:"fixture-other@example.invalid"}}]));
  unknownClaude(inspect([nativeRows[0],{...nativeRows[1],identity:{...nativeRows[1].identity,matchesExpected:false}}]));
  unknownClaude(inspect([nativeRows[0],{...nativeRows[1],identity:{...nativeRows[1].identity,authenticated:false}}]));
  unknownClaude(inspect([nativeRows[0],{...nativeRows[1],identity:{...nativeRows[1].identity,proof:"configured_only"}}]));
  unknownClaude(inspect([nativeRows[0],{...nativeRows[1],identity:{...nativeRows[1].identity,proof:null}}]));
  unknownClaude(inspect([nativeRows[0],{...nativeRows[1],identity:{...nativeRows[1].identity,observedAt:null},source:{...nativeRows[1].source,observedAt:null}}]));
  const historical = inspect(nativeRows,"2026-10-15T12:00:00.000Z");
  assert.ok(historical.accounts.flatMap(account => account.accountLinks!).some(claim => claim.state === "observed"));
  assert.ok(historical.accounts.every(account => account.subscriptionState === "unknown"));
  unknownClaude(inspect(nativeRows,"2026-10-13T12:00:00.000Z"));

  const store = open();
  persistNativeTelemetry(store,{...inspection,ledgerReading:{...reading,events:reading.events.map(event => ({...event,
    providerAccountHash:null,providerAccountDerivation:null,providerAccountEvidence:null,
    ...(event.id === "codex-captured" ? {accountHash:null} : {}),projectKey:null,projectEvidence:null}))}},fixtureOptions);
  const thread = (id: string) => nativeTelemetryReport(store).threads.find(row => row.threadId === id)!;
  assert.equal(thread("claude-captured").subscriptionAccountJoin,"UNKNOWN");
  assert.equal(thread("codex-captured").subscriptionAccountJoin,"UNKNOWN");
  assert.equal(thread("codex-legacy-short").accountRef,codex.accountRef);
  assert.equal(thread("claude-actor-only").subscriptionAccountJoin,"UNKNOWN");
  const originalActor = store.prepare("select account_hash from native_thread_events where thread_id='claude-captured'").get();
  const tokenRows = () => store.prepare(`select event_key,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,
    reference_cost_usd,cost_kind,usage_authority,authority_accepted,observed_at from native_thread_events order by event_key`).all();
  const originalTokens = tokenRows();
  store.prepare("update native_capture_cursors set through_rowid=50000").run();
  const originalCursor = store.prepare("select * from native_capture_cursors").all();
  const originalReadFile = fs.readFileSync, originalReadDir = fs.readdirSync, originalLstat = fs.lstatSync;
  let authReads = 0, directoryReads = 0, unrelatedReads = 0;
  const unrelated = new Set([fixturePaths.cursorTrackingDb,fixturePaths.cursorCliConfigFile,fixturePaths.grokConfigFile,fixturePaths.dashboardEvidenceFile]);
  Reflect.set(fs,"lstatSync",((file: fs.PathLike,...args: unknown[]) => {
    if (unrelated.has(String(file))) { unrelatedReads += 1; throw new Error("forbidden_unrelated_surface_read"); }
    return (originalLstat as (...args: unknown[]) => unknown)(file,...args);
  }) as typeof fs.lstatSync);
  fs.readFileSync = ((file: fs.PathOrFileDescriptor,...args: unknown[]) => {
    if (String(file) === fixturePaths.codexAuthFile || String(file).endsWith(".credentials.json")) { authReads += 1; throw new Error("forbidden_auth_read"); }
    return (originalReadFile as (...args: unknown[]) => unknown)(file,...args);
  }) as typeof fs.readFileSync;
  fs.readdirSync = (() => { directoryReads += 1; throw new Error("forbidden_directory_scan"); }) as typeof fs.readdirSync;
  try {
    feed();
    const replay = replayNativeTelemetryEvidence(store,{paths:fixturePaths,now,limit:50_000});
    assert.equal(replay.receipt.ledgerInserted,0);
    assert.equal(replay.receipt.rawRowsSelected,15);
    assert.equal(replay.receipt.cursorUpdated,false);
    replayNativeTelemetryEvidence(store,{paths:fixturePaths,now,limit:50_000});
    assert.throws(() => replayNativeTelemetryEvidence(store,{paths:fixturePaths,now,limit:50_001}),/limit_invalid/);
  } finally { fs.readFileSync = originalReadFile; fs.readdirSync = originalReadDir; Reflect.set(fs,"lstatSync",originalLstat); }
  assert.equal(authReads,0); assert.equal(directoryReads,0); assert.equal(unrelatedReads,0);
  assert.deepEqual(store.prepare("select * from native_capture_cursors").all(),originalCursor);
  assert.deepEqual(tokenRows(),originalTokens);
  assert.deepEqual(store.prepare("select account_hash from native_thread_events where thread_id='claude-captured'").get(),originalActor);
  assert.equal(thread("claude-captured").accountRef,claude.accountRef);
  assert.equal(thread("codex-captured").accountRef,codex.accountRef);
  assert.equal(thread("claude-captured").projectState,"observed");
  assert.equal(thread("claude-actor-only").subscriptionAccountJoin,"UNKNOWN");
  assert.equal(thread("missing").subscriptionAccountJoin,"UNKNOWN");
  assert.equal(thread("mismatched").subscriptionAccountJoin,"UNKNOWN");
  const storedBefore = nativeTelemetryReport(store).observedTokenEvents;
  add("previously-unstored","codex",boxed(fakeId),{"user.account_id":boxed(fakeId)});
  const skipNew = replayNativeTelemetryEvidence(store,{paths:fixturePaths,now,limit:50_000});
  assert.equal(skipNew.receipt.ledgerSkippedUnstored,1);
  assert.equal(nativeTelemetryReport(store).observedTokenEvents,storedBefore);
  const replayEvent = (event: typeof captured) => persistNativeTelemetry(store,{...inspection,
    ledgerReading:{...reading,events:[event]}},{...fixtureOptions,replayEvidenceOnly:true});
  replayEvent({...captured,accountHash:boxed(fakeOther)});
  assert.equal(thread("claude-captured").accountRef,claude.accountRef); // Generic actor changes do not invalidate UUID evidence.
  replayEvent({...captured,providerAccountHash:boxed(fakeOther)});
  assert.equal(thread("claude-captured").accountActivity[0].unknownReason,"conflicting_captured_identity");
  replayEvent(captured);
  assert.equal(thread("claude-captured").subscriptionAccountJoin,"UNKNOWN");
  assert.equal(fs.existsSync(path.join(home,ACCOUNT_ASSERTION_SALT_FILE)),false);
  const serialized = JSON.stringify({inspection,report:nativeTelemetryReport(store),rows:store.prepare("select * from native_account_link_evidence").all()});
  for (const value of [fakeId,fakeOther,fakeEmail,fakeOrg,"body-sentinel"]) assert.equal(serialized.includes(value),false,value);
  return {
    exactBoxedSha16:true,noGuessedOrTruncatedHashes:true,rawActorPreserved:true,specificProviderScalarsOnly:true,
    malformedAndNonScalarMetadataExcluded:true,protectedValuesOnly:true,privacyAndAuthorityGates:true,
    rawRowWindowDoesNotExpand:true,deadlineAndRowLimitEnforced:true,providerScopedCollision:true,
    genericClaudeActorNotAccountUuid:true,claudeAccountIdNotAccountUuid:true,organizationNotAccountUuid:true,
    saltedAndBoxedNamespacesCoexist:true,replayAcrossDerivationNamespaces:true,legacyUnprovedClaudeClaimsRejected:true,
    sameNamespaceAmbiguityAndConflict:true,registeredProfileProof:true,
    emailAndOrganizationBothRequired:true,unsafeAndAbsentProfilesExcluded:true,secureFilesAndAncestors:true,
    currentLocalIdentityMustMatch:true,identityProofGates:true,historicalIdentityNotFreshAllowance:true,
    noAuthFilesOrDirectoryScan:true,noUnrelatedReplaySurfaceReads:true,noSaltMinted:true,boundedReplayEnrichesExistingEvidence:true,
    replayPreservesCursorCountersAndLegacyActor:true,replayDoesNotInsertUnstoredTokens:true,
    sameSemanticReplayConflictSticky:true,genericActorReplayNotUuidConflict:true,
    missingAndMismatchedCaptureStayUnknown:true,noPrivateIdentityOrBodyInOutput:true,
  };
}

try {
  // Installed 0.7.47 protects provider account fields with boxed SHA16.
  assert.equal(capturedAccountHash("sha256:0123456789abcdef"), "sha256:0123456789abcdef",
    "native 0.7.47 protected account hashes must survive the reader/store boundary");
  fs.chmodSync(root,0o700);
  for (const account of accounts.filter(item => item.provider === "claude")) {
    const profile = path.join(paths.claudeSeatsRoot!,account.accountKey);
    fs.mkdirSync(profile,{ recursive:true,mode:0o700 });
    const accountUuid = account.accountKey === "fixture-key-ca" ? ids.a : ids.c;
    fs.writeFileSync(path.join(profile,".claude.json"),JSON.stringify({ oauthAccount:{ accountUuid,
      emailAddress:account.identity.email,organizationUuid:`fixture-organization-${account.accountKey}` } }),{ mode:0o600 });
  }
  fs.writeFileSync(saltFile,salt,{mode:0o600});
  const saltDigest = digestFile(saltFile);
  writeFeed(accounts.map(item => nativeAccount(item.provider,item.accountKey)));
  const ledger = open(paths.collectorDb);
  ledger.exec(`create table buffered_events(id text primary key,source text,event_type text,observed_at text,
    session_id text,model text,input_tokens integer,output_tokens integer,cache_read_tokens integer,
    cache_creation_tokens integer,cost_usd real,cost_kind text,project_key text,repo_hash text,account_hash text,
    data_mode text,privacy_generation text,privacy_disposition text,usage_duplicate_reason text,payload_json text);
    create table session_usage_authority(source text,session_id text,authority text,primary key(source,session_id));
    create table upload_receipts(delivery_id text primary key,reason text);`);
  const add = (id: string, thread: string, accountHash: string | null, input: number,
    project: string | null = null, repo: string | null = null, source = "codex") => {
    ledger.prepare("insert into buffered_events values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").run(
      id,source,source === "codex" ? "usage_rollout" : "usage_transcript",now,thread,"fixture-model",input,input / 5,
      5,10,0.1,"reference",project,repo,accountHash,"metadata_only","fixture-generation",null,null,
      JSON.stringify({prompt:"fixture-private-body-sentinel"}));
    ledger.prepare("insert or ignore into session_usage_authority values (?,?,?)").run(source,thread,"tailer");
  };
  add("switch-a","switch",hashes.a,100,explicit);
  add("switch-b","switch",hashes.b,200,null,repository);
  add("switch-missing","switch",null,50);
  add("switch-unmatched","switch",linkageHash("fixture-unmatched-actor")!,60);
  add("complete-a","complete-switch",hashes.a,10);
  add("complete-b","complete-switch",hashes.b,20);
  add("claude-shared","claude-shared",hashes.a,30,null,null,"claude_code");
  add("cross-provider","cross-provider",hashes.c,40);
  add("unsalted-lookalike","unsalted-lookalike",linkageHash(ids.a)!,45);
  add("repository-only","repository-only",hashes.a,50,null,repository);
  add("metadata-project","metadata-project",hashes.a,55);
  add("claude-inferred-conflict","claude-inferred-conflict",hashes.c,65,null,null,"claude_code");
  add("explicit-one","explicit-conflict",hashes.a,60,explicit);
  add("explicit-two","explicit-conflict",hashes.a,70,alternate);
  add("unknown-project","unknown-project",hashes.a,80);
  add("authority-rejected","authority-rejected",hashes.a,900);
  ledger.prepare("update session_usage_authority set authority='live' where session_id='authority-rejected'").run();
  add("privacy-evidence","privacy",hashes.a,900);
  ledger.prepare("update buffered_events set data_mode='evidence' where id='privacy-evidence'").run();
  add("privacy-quarantined","privacy",hashes.a,900);
  ledger.prepare("insert into upload_receipts values (?,?)").run("privacy-quarantined","local_evidence_quarantined");
  add("usage-duplicate","privacy",hashes.a,900);
  ledger.prepare("update buffered_events set usage_duplicate_reason='fixture_duplicate' where id='usage-duplicate'").run();
  // Native telemetry also excludes usage duplicates, independently of privacy.
  const eligible = ledger.prepare(`select id from buffered_events where ${terminalPrivacyEligibilitySql(ledger)}
    and usage_duplicate_reason is null order by rowid`).all() as Array<{id:string}>;
  ledger.close();
  databases.splice(databases.indexOf(ledger),1);
  const ledgerDigest = digestFile(paths.collectorDb);
  const nativeState = open(options.codexStateFile);
  nativeState.exec("create table threads(id text primary key,cwd text,git_origin_url text)");
  nativeState.prepare("insert into threads values (?,?,?)").run("switch",root,"https://github.com/example/fixture-state.git");
  nativeState.prepare("insert into threads values (?,?,?)").run("metadata-project",root,"https://github.com/example/fixture-state.git");
  nativeState.close(); databases.splice(databases.indexOf(nativeState),1);
  fs.mkdirSync(path.join(options.claudeProjectsRoot,"fixture"),{recursive:true});
  const transcriptLines = ["one","two"].map(name => {
    const cwd = path.join(root,`repo-${name}`);
    fs.mkdirSync(path.join(cwd,".git"),{recursive:true});
    fs.writeFileSync(path.join(cwd,".git/HEAD"),"a".repeat(40)+"\n");
    fs.writeFileSync(path.join(cwd,".git/config"),`[remote "origin"]\nurl = https://github.com/example/fixture-${name}.git\n`);
    return JSON.stringify({sessionId:"claude-inferred-conflict",cwd,message:"fixture-private-body-sentinel"});
  });
  fs.writeFileSync(path.join(options.claudeProjectsRoot,"fixture/claude-inferred-conflict.jsonl"),transcriptLines.join("\n")+"\n");

  const sidecar = open(paths.nativeSubscriptionDb);
  const first = collectNativeTelemetry(sidecar,options);
  assert.deepEqual(first.inspection.ledgerReading.events.map(event => event.id),eligible.map(row => row.id));
  assert.equal(nativeTelemetryReport(sidecar).accountLinkCoverage.linkedEvents,0);
  assert.equal(first.inspection.ledgerReading.events.find(event => event.id === "repository-only")?.projectEvidence,"repository_hash");
  assert.equal(first.inspection.ledgerReading.events.find(event => event.id === "switch-a")?.projectEvidence,"explicit_project_key");

  // The next recurring collection has no new ledger events. New native identity
  // equivalence links existing captured hashes, never fills a missing one.
  writeFeed(accounts);
  const next = collectNativeTelemetry(sidecar,options);
  assert.equal(next.receipt.ledgerCoverage,"incremental");
  assert.equal(next.receipt.ledgerInserted,0);
  assert.equal(next.inspection.ledgerReading.events.length,0);
  const account = (provider: string, index = 0) => next.inspection.accounts.filter(item => item.provider === provider)[index].accountRef!;
  const report = nativeTelemetryReport(sidecar);
  const thread = (id: string, result = report) => { const item = result.threads.find(item => item.threadId === id); assert.ok(item); return item; };
  const switched = thread("switch");
  assert.equal(switched.subscriptionAccountJoin,"partial");
  assert.equal(switched.accountRef,null);
  assert.deepEqual(switched.accountRefs,[account("openai",0),account("openai",1)].sort());
  assert.equal(switched.spansAccounts,true);
  assert.equal(switched.accountLinkCoverage.linkedEvents,2);
  assert.equal(switched.unknownAccountEvents,2);
  assert.equal(switched.accountActivity.find(group => group.accountRef === account("openai",0))?.observedInputTokens,100);
  assert.equal(switched.accountActivity.find(group => group.accountRef === account("openai",1))?.observedInputTokens,200);
  assert.equal(switched.accountActivity.reduce((sum,group) => sum + (group.observedInputTokens ?? 0),0),410);
  assert.ok(switched.accountActivity.filter(group => group.accountRef).every(group => group.accountLinkEvidence.some(item => item.state === "observed" && item.derivation === "account_assertion_actor_v1")));
  const complete = thread("complete-switch");
  assert.equal(complete.subscriptionAccountJoin,"observed");
  assert.equal(complete.accountRef,null);
  assert.equal(complete.spansAccounts,true);
  assert.equal(thread("claude-shared").accountRef,account("anthropic",0));
  assert.equal(thread("cross-provider").subscriptionAccountJoin,"UNKNOWN");
  assert.equal(thread("cross-provider").accountActivity[0].unknownReason,"cross_provider_identity");
  assert.equal(thread("unsalted-lookalike").subscriptionAccountJoin,"UNKNOWN");
  assert.equal(switched.projectState,"observed");
  assert.equal(switched.projectKey,explicit);
  assert.equal(switched.projectUrl,null);
  assert.equal(switched.projectConflict,true);
  assert.ok(switched.projectCandidates.some(item => item.projectKey === stateRepository && item.state === "inferred"));
  assert.equal(thread("repository-only").projectState,"inferred");
  assert.equal(thread("repository-only").projectKey,repository);
  assert.equal(thread("metadata-project").projectState,"inferred");
  assert.equal(thread("metadata-project").projectUrl,"https://github.com/example/fixture-state");
  assert.equal(thread("metadata-project").projectUrlState,"inferred");
  assert.equal(thread("claude-inferred-conflict").projectState,"UNKNOWN");
  assert.equal(thread("claude-inferred-conflict").projectConflict,true);
  assert.equal(thread("claude-inferred-conflict").projectCandidates.length,2);
  assert.equal(thread("explicit-conflict").projectState,"UNKNOWN");
  assert.equal(thread("explicit-conflict").projectKey,null);
  assert.equal(thread("explicit-conflict").observedProjectKey,null);
  assert.equal(thread("explicit-conflict").projectConflict,true);
  assert.equal(thread("unknown-project").projectState,"UNKNOWN");
  assert.equal(report.threads.some(item => item.threadId === "authority-rejected"),false);
  assert.equal(report.accountLinkCoverage.totalEvents,report.observedTokenEvents - 1);
  assert.equal(report.accountLinkCoverage.totalEvents,report.accountLinkCoverage.linkedEvents + report.accountLinkCoverage.unknownEvents);
  assert.ok(report.threads.every(item => item.actualSubscriptionSpendUsd === null && item.cashState === "UNKNOWN" && item.tokenCompleteness === "UNKNOWN"));
  assert.equal(report.actualSubscriptionSpendUsd,null);

  // Switching today's available feed to B preserves the already proven A
  // equivalence. Uncaptured events are still UNKNOWN.
  writeFeed([accounts[1]]);
  collectNativeTelemetry(sidecar,options);
  const switchedAgain = thread("switch",nativeTelemetryReport(sidecar));
  assert.deepEqual(switchedAgain.accountRefs,switched.accountRefs);
  assert.equal(switchedAgain.unknownAccountEvents,2);
  assert.equal(sidecar.prepare("select account_hash from native_thread_events where account_hash is null").all().length,1);

  // Upgrade a pre-extension sidecar without replaying or rewriting its events.
  const legacy = open();
  legacy.exec(`create table native_thread_events(event_key text primary key,source text not null,event_type text,
    observed_at text,thread_id text,model text,input_tokens integer,output_tokens integer,cache_read_tokens integer,
    cache_write_tokens integer,reference_cost_usd real,cost_kind text,project_key text,account_hash text,
    usage_authority text not null,authority_accepted integer,updated_at text not null);`);
  legacy.prepare("insert into native_thread_events values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").run(
    "fixture-legacy-key","codex","usage_rollout",now,"legacy-thread","fixture-model",75,15,0,0,0.2,"reference",repository,
    hashes.a.slice(7).toUpperCase(),"tailer",1,now);
  const original = legacy.prepare("select event_key,account_hash,input_tokens,updated_at from native_thread_events").get();
  writeFeed(accounts);
  const withIdentity = inspectNativeSubscriptionSurfaces({paths,now,ledgerReading:next.inspection.ledgerReading});
  persistNativeTelemetry(legacy,withIdentity,options);
  assert.equal(thread("legacy-thread",nativeTelemetryReport(legacy)).accountRef,account("openai",0));
  assert.equal(thread("legacy-thread",nativeTelemetryReport(legacy)).projectState,"UNKNOWN");
  assert.deepEqual(legacy.prepare("select event_key,account_hash,input_tokens,updated_at from native_thread_events").get(),original);

  // Duplicate native identities and reused account refs must never pick a
  // winner, including claims discovered on a later empty recurring pass.
  writeFeed([...accounts,nativeAccount("codex","fixture-duplicate-key",ids.a)]);
  collectNativeTelemetry(sidecar,options);
  const ambiguous = thread("complete-switch",nativeTelemetryReport(sidecar)).accountActivity.find(group => group.accountHash === hashes.a)!;
  assert.equal(ambiguous.subscriptionAccountJoin,"UNKNOWN");
  assert.equal(ambiguous.unknownReason,"ambiguous_native_identity");
  assert.equal(ambiguous.candidateAccountRefs.length,2);
  writeFeed([nativeAccount("codex","fixture-key-b",ids.c)]);
  collectNativeTelemetry(sidecar,options);
  const conflicting = thread("complete-switch",nativeTelemetryReport(sidecar)).accountActivity.find(group => group.accountHash === hashes.b)!;
  assert.equal(conflicting.subscriptionAccountJoin,"UNKNOWN");
  assert.equal(conflicting.unknownReason,"conflicting_native_identity");

  const mismatch = open();
  writeFeed([nativeAccount("codex","fixture-key-a",ids.a,false)]);
  persistNativeTelemetry(mismatch,inspectNativeSubscriptionSurfaces({paths,now,ledgerReading:first.inspection.ledgerReading}),options);
  assert.equal(thread("complete-switch",nativeTelemetryReport(mismatch)).accountActivity.find(group => group.accountHash === hashes.a)?.unknownReason,"conflicting_native_identity");

  // A replay can add a captured hash to a previously empty sidecar cell; two
  // conflicting nonempty captured hashes invalidate that event permanently.
  const replayDb = open();
  writeFeed(accounts);
  const replayInspection = inspectNativeSubscriptionSurfaces({paths,now,ledgerReading:first.inspection.ledgerReading});
  const single = replayInspection.ledgerReading.events.find(event => event.id === "switch-a")!;
  const persistEvent = (accountHash: string | null, projectKey = single.projectKey) => persistNativeTelemetry(replayDb,
    {...replayInspection,ledgerReading:{...replayInspection.ledgerReading,events:[{...single,accountHash,projectKey}]}},options);
  persistEvent(null);
  persistEvent(hashes.a);
  assert.equal(thread("switch",nativeTelemetryReport(replayDb)).accountRef,account("openai",0));
  persistEvent(hashes.a.slice(7).toUpperCase());
  assert.equal(thread("switch",nativeTelemetryReport(replayDb)).accountRef,account("openai",0));
  persistEvent(hashes.b);
  assert.equal(thread("switch",nativeTelemetryReport(replayDb)).accountActivity[0].unknownReason,"conflicting_captured_identity");
  persistEvent(hashes.a);
  assert.equal(thread("switch",nativeTelemetryReport(replayDb)).subscriptionAccountJoin,"UNKNOWN");
  persistEvent(hashes.a,alternate);
  assert.equal(thread("switch",nativeTelemetryReport(replayDb)).projectState,"UNKNOWN");
  assert.equal(thread("switch",nativeTelemetryReport(replayDb)).projectCandidates.filter(item => item.state === "observed").length,2);

  assert.equal(digestFile(saltFile),saltDigest);
  assert.equal(digestFile(paths.collectorDb),ledgerDigest);
  fs.unlinkSync(saltFile);
  const noSalt = open();
  const noSaltInspection = inspectNativeSubscriptionSurfaces({paths,now,ledgerReading:first.inspection.ledgerReading});
  assert.ok(noSaltInspection.accounts.every(item => item.accountLink?.reason === "missing_tenant_salt"));
  persistNativeTelemetry(noSalt,noSaltInspection,options);
  assert.equal(nativeTelemetryReport(noSalt).accountLinkCoverage.linkedEvents,0);
  assert.equal(fs.existsSync(saltFile),false);
  const absentHome = path.join(root,"absent-home");
  assert.equal(readExistingNativeAccountSalt(absentHome),null);
  assert.equal(fs.existsSync(absentHome),false);
  fs.writeFileSync(saltFile,salt,{mode:0o600});
  writeFeed([nativeAccount("codex","fixture-key-missing")]);
  const noIdentity = open();
  const missingInspection = inspectNativeSubscriptionSurfaces({paths,now,ledgerReading:first.inspection.ledgerReading});
  assert.equal(missingInspection.accounts[0].accountLink?.reason,"missing_or_invalid_native_identity");
  persistNativeTelemetry(noIdentity,missingInspection,options);
  assert.equal(nativeTelemetryReport(noIdentity).accountLinkCoverage.linkedEvents,0);
  const serialized = JSON.stringify({inspection:withIdentity,report,stored:sidecar.prepare("select observation_json from native_account_link_evidence").all()});
  assert.equal(Object.values(ids).some(id => serialized.includes(id)),false);
  assert.equal(serialized.includes("fixture-private@example.invalid"),false);
  assert.equal(serialized.includes("fixture-private-body-sentinel"),false);
  assert.equal(serialized.includes(salt.toString("base64")),false);
  const native047Checks = native047Fixtures();
  console.log(JSON.stringify({schema:"plimsoll.native-account-links-proof.v1",native047Checks,checks:{
    capturedAccountSwitchesSplitActivity:true,missingCapturedHashStaysUnknown:true,
    emptyRecurringPassLinksExistingSidecar:true,currentLoginDoesNotRewriteHistory:true,
    crossProviderIsolation:true,duplicateNativeIdsStayAmbiguous:true,reusedRefsStayConflicting:true,
    expectedIdentityMismatchStaysUnknown:true,replayedIdentityConflictsStayUnknown:true,
    missingSaltAndNativeIdStayUnknown:true,saltIsNotCreatedOrModified:true,
    sourceLedgerIsUnchanged:true,privacyAndAuthorityExclusionsPreserved:true,
    legacySidecarUpgradePreservesUnknownProvenance:true,repositoryEvidenceIsInferred:true,
    explicitProjectOutranksRepository:true,explicitProjectConflictsVisible:true,
    inferredProjectConflictsStayUnknown:true,repositoryLinksUseCapturedMetadata:true,
    projectReplayRetainsConflictingEvidence:true,unsaltedLookalikesDoNotJoin:true,
    cashAndCompletenessStayUnknown:true,onlyHashedIdentitiesReachReports:true,
  }},null,2));
} finally {
  for (const db of databases) db.close();
  salt.fill(0);
  fs.rmSync(root,{recursive:true,force:true});
}
