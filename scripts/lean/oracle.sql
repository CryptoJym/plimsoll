-- B9a independent dashboard oracle. The harness attaches an immutable ledger as
-- "ledger", supplies V8 Date.parse/sanitizer scalar functions, expands the
-- compact gzip payloads into oracle_compact_items, and creates oracle_clock.
-- Every persistent write below is to TEMP tables in an in-memory connection.
-- No dashboard_* aggregate/fact/snapshot table is an input.

create temp table oracle_rows as
with raw as (
  select b.rowid as raw_rowid, b.id as event_id,
    coalesce(b.installation_epoch_id, '__unknown_epoch__') as epoch_key,
    case when b.source in ('anthropic_admin','anthropic_usage','claude_code','codex','grok','github','openai_usage','manual','unknown') then b.source else 'unknown' end as source,
    b.event_type,
    b.observed_at,
    safe_hash(b.session_id) as session_hash,
    case when b.action_class in ('continue','validate','test','edit','read','write','shell','mcp','browser','review','other') then b.action_class else 'other' end as action_class,
    case when b.event_type='usage_live' then null else safe_model(b.model) end as model,
    case when b.event_type in ('usage_rollout','usage_transcript') and b.session_id is not null
      and (b.input_tokens is not null or b.output_tokens is not null or b.cache_read_tokens is not null or b.cache_creation_tokens is not null or b.cost_usd is not null)
      and exists (select 1 from ledger.buffered_events live where live.source=b.source and live.session_id=b.session_id
        and live.event_type not in ('usage_rollout','usage_transcript')
        and (live.input_tokens is not null or live.output_tokens is not null or live.cache_read_tokens is not null
          or live.cache_creation_tokens is not null or live.cost_usd is not null)) then 1 else 0 end as suppress_usage,
    b.input_tokens,b.output_tokens,b.cache_read_tokens,b.cache_creation_tokens,
    case when b.event_type='usage_live' then null else cost_nanos(b.cost_usd) end as cost_nanos,
    case when b.event_type='usage_live' then null else canonical_linkage(b.repo_hash) end as repo_hash,
    case when b.event_type='usage_live' then null else canonical_linkage(b.branch_hash) end as branch_hash,
    safe_hash(b.machine) as machine_hash,
    case when b.event_type='usage_live' then null else account_alias(safe_hash(b.account_hash)) end as account_hash
  from ledger.buffered_events b
  where b.data_mode <> 'evidence' and b.privacy_disposition is null
    and b.usage_duplicate_reason is null and b.privacy_generation is not null
    and not exists (select 1 from ledger.upload_receipts p where p.delivery_id=b.id
      and p.reason in ('local_evidence_quarantined','local_privacy_violation'))
    and not exists (select 1 from ledger.upload_outbox o where o.raw_rowid=b.rowid
      and (o.raw_id is null or o.raw_created_at is null or o.raw_generation is null
        or o.raw_id is not b.id or o.raw_created_at is not b.created_at
        or o.raw_generation is not b.privacy_generation))
), items as (
  select raw_rowid,event_id,epoch_key,source,event_type,observed_at,session_hash,action_class,model,
    case when suppress_usage then null else input_tokens end as input_tokens,
    case when suppress_usage then null else output_tokens end as output_tokens,
    case when suppress_usage then null else cache_read_tokens end as cache_read_tokens,
    case when suppress_usage then null else cache_creation_tokens end as cache_creation_tokens,
    case when suppress_usage then null else cost_nanos end as cost_nanos,
    repo_hash,branch_hash,machine_hash,account_hash,'raw' as origin
  from raw
  union all
  select c.raw_rowid, 'compact:'||c.segment_id||':'||c.item_index,
    '__compact_unknown_epoch__',c.source,c.event_type,c.observed_at,null,
    coalesce(c.action_class,'other'),null,null,null,null,null,null,null,null,null,null,'compact'
  from oracle_compact_items c
  where not exists (select 1 from ledger.buffered_events b where b.rowid=c.raw_rowid
    and b.observed_at=c.observed_at and b.source=c.source and b.event_type=c.event_type
    and coalesce(b.action_class,'other')=coalesce(c.action_class,'other'))
    and not exists (select 1 from ledger.dashboard_compact_cancellations x
      where x.raw_rowid=c.raw_rowid and x.observed_at=c.observed_at and x.source=c.source
        and x.event_type=c.event_type and x.action_key=coalesce(c.action_class,''))
)
select i.*, parse_ms(i.observed_at) as observed_ms,
  strftime('%Y-%m-%d',parse_ms(i.observed_at)/1000.0,'unixepoch') as utc_day,
  substr(i.observed_at,1,10) as lexical_day,
  case when i.observed_at glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[01][0-9]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z'
      or i.observed_at glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T2[0-3]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z' then 1 else 0 end as canonical
from items i;

create index oracle_rows_ms on oracle_rows(observed_ms);
create index oracle_rows_session on oracle_rows(session_hash);

-- The ARCHITECTURE §3.5 rule is evaluated for EVERY non-canonical row at
-- EVERY cutoff, even when the conservative census class predicts no flip.
create temp table oracle_boundary as
select w.days,r.event_id,r.epoch_key,r.origin,r.observed_at,r.observed_ms,r.utc_day,r.lexical_day,
  case when r.observed_at glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'
    and r.lexical_day<>r.utc_day then 1 else 0 end as day_move,
  case when r.observed_at glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'
    and r.observed_at<r.lexical_day||'T00:00:00.000Z' then 1 else 0 end as before_own_midnight,
  case when r.observed_at not glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*' then 1 else 0 end as non_iso,
  case when r.observed_at>=w.cutoff_iso then 1 else 0 end as lexical_in,
  case when r.observed_ms>=w.cutoff_ms then 1 else 0 end as instant_in
from oracle_rows r cross join oracle_clock w where r.canonical=0;

create temp table oracle_window_rows as
select w.days,r.* from oracle_rows r cross join oracle_clock w
where r.observed_ms>=w.cutoff_ms and r.observed_ms<w.frozen_ms;
create index oracle_window_days_session on oracle_window_rows(days,session_hash);

-- Epoch keys are retained until the final fold to the schema-2 presentation.
create temp table oracle_epoch_values (
  days integer,epoch_key text,group_name text,group_key text,metric text,value
);
insert into oracle_epoch_values
select days,epoch_key,'totals','all','events',count(*) from oracle_window_rows group by days,epoch_key;
insert into oracle_epoch_values
select days,epoch_key,'totals','all','tokenEvents',sum(input_tokens is not null or output_tokens is not null) from oracle_window_rows group by days,epoch_key;
insert into oracle_epoch_values
select days,epoch_key,'totals','all','inputTokens',coalesce(sum(input_tokens),0) from oracle_window_rows group by days,epoch_key;
insert into oracle_epoch_values
select days,epoch_key,'totals','all','outputTokens',coalesce(sum(output_tokens),0) from oracle_window_rows group by days,epoch_key;
insert into oracle_epoch_values
select days,epoch_key,'totals','all','cacheReadTokens',coalesce(sum(cache_read_tokens),0) from oracle_window_rows group by days,epoch_key;
insert into oracle_epoch_values
select days,epoch_key,'totals','all','cacheCreationTokens',coalesce(sum(cache_creation_tokens),0) from oracle_window_rows group by days,epoch_key;
insert into oracle_epoch_values
select days,epoch_key,'totals','all','costNanos',coalesce(sum(cost_nanos),0) from oracle_window_rows group by days,epoch_key;
insert into oracle_epoch_values
select days,epoch_key,'bySource',source,'events',count(*) from oracle_window_rows group by days,epoch_key,source;
insert into oracle_epoch_values
select days,epoch_key,'bySource',source,'inputTokens',coalesce(sum(input_tokens),0) from oracle_window_rows group by days,epoch_key,source;
insert into oracle_epoch_values
select days,epoch_key,'bySource',source,'outputTokens',coalesce(sum(output_tokens),0) from oracle_window_rows group by days,epoch_key,source;
insert into oracle_epoch_values
select days,epoch_key,'bySource',source,'costNanos',coalesce(sum(cost_nanos),0) from oracle_window_rows group by days,epoch_key,source;
insert into oracle_epoch_values
select days,epoch_key,'daily',utc_day,'tokens',coalesce(sum(input_tokens),0)+coalesce(sum(output_tokens),0) from oracle_window_rows group by days,epoch_key,utc_day;
insert into oracle_epoch_values
select days,epoch_key,'daily',utc_day,'costNanos',coalesce(sum(cost_nanos),0) from oracle_window_rows group by days,epoch_key,utc_day;
insert into oracle_epoch_values
select days,epoch_key,'byModel',model,'calls',count(*) from oracle_window_rows
where model is not null and (input_tokens is not null or output_tokens is not null or cost_nanos is not null)
group by days,epoch_key,model;
insert into oracle_epoch_values
select days,epoch_key,'byModel',model,'unpricedCalls',sum(cost_nanos is null) from oracle_window_rows
where model is not null and (input_tokens is not null or output_tokens is not null or cost_nanos is not null)
group by days,epoch_key,model;
insert into oracle_epoch_values
select days,epoch_key,'byModel',model,'inputTokens',coalesce(sum(input_tokens),0) from oracle_window_rows
where model is not null and (input_tokens is not null or output_tokens is not null or cost_nanos is not null)
group by days,epoch_key,model;
insert into oracle_epoch_values
select days,epoch_key,'byModel',model,'outputTokens',coalesce(sum(output_tokens),0) from oracle_window_rows
where model is not null and (input_tokens is not null or output_tokens is not null or cost_nanos is not null)
group by days,epoch_key,model;
insert into oracle_epoch_values
select days,epoch_key,'byModel',model,'cacheReadTokens',coalesce(sum(cache_read_tokens),0) from oracle_window_rows
where model is not null and (input_tokens is not null or output_tokens is not null or cost_nanos is not null)
group by days,epoch_key,model;
insert into oracle_epoch_values
select days,epoch_key,'byModel',model,'cacheCreationTokens',coalesce(sum(cache_creation_tokens),0) from oracle_window_rows
where model is not null and (input_tokens is not null or output_tokens is not null or cost_nanos is not null)
group by days,epoch_key,model;
insert into oracle_epoch_values
select days,epoch_key,'byModel',model,'costNanos',coalesce(sum(cost_nanos),0) from oracle_window_rows
where model is not null and (input_tokens is not null or output_tokens is not null or cost_nanos is not null)
group by days,epoch_key,model;
insert into oracle_epoch_values
select days,epoch_key,'actionMix',action_class,'events',count(*) from oracle_window_rows
where event_type in ('tool_use','tool_result') group by days,epoch_key,action_class;

create temp table oracle_values as
select days,group_name,group_key,metric,sum(value) as value
from oracle_epoch_values group by days,group_name,group_key,metric;
create index oracle_values_key on oracle_values(days,group_name,group_key,metric);

-- Session and dimensional groups have epoch-scoped row identities. Their
-- presentation is subsequently summed by the five old dashboard windows.
create temp table oracle_session_source as
select days,epoch_key,session_hash,source,min(observed_at) as started_at,max(observed_at) as ended_at,
  count(*) as events,sum(input_tokens is not null) as token_events,
  coalesce(sum(input_tokens),0) as input_tokens,coalesce(sum(output_tokens),0) as output_tokens,
  coalesce(sum(cache_read_tokens),0) as cache_read_tokens,coalesce(sum(cost_nanos),0) as cost_nanos
from oracle_window_rows where session_hash is not null group by days,epoch_key,session_hash,source;
create temp table oracle_session_repo as
select days,epoch_key,session_hash,repo_hash,count(*) as events,
  coalesce(sum(input_tokens),0) as input_tokens,coalesce(sum(output_tokens),0) as output_tokens,
  coalesce(sum(cost_nanos),0) as cost_nanos,count(distinct branch_hash) as branches
from oracle_window_rows where session_hash is not null and repo_hash is not null
group by days,epoch_key,session_hash,repo_hash;
create temp table oracle_session_account as
select days,epoch_key,session_hash,account_hash,count(*) as events,coalesce(sum(cost_nanos),0) as cost_nanos
from oracle_window_rows where session_hash is not null and account_hash is not null
group by days,epoch_key,session_hash,account_hash;
create temp table oracle_session_root as
with roots as (
  select days,epoch_key,session_hash,max(source) as source,max(branch_hash) as branch_hash,
    count(*) as events,sum(input_tokens is not null) as token_events,
    coalesce(sum(input_tokens),0) as input_tokens,coalesce(sum(output_tokens),0) as output_tokens,
    coalesce(sum(cache_read_tokens),0) as cache_read_tokens,coalesce(sum(cost_nanos),0) as cost_nanos
  from oracle_window_rows where session_hash is not null group by days,epoch_key,session_hash
), ranked_repo as (
  select *,row_number() over(partition by days,epoch_key,session_hash order by events desc,branches desc,repo_hash) as rank
  from oracle_session_repo
), ranked_account as (
  select *,row_number() over(partition by days,epoch_key,session_hash order by cost_nanos desc,events desc,account_hash) as rank
  from oracle_session_account
)
select r.*,p.repo_hash as dominant_repo_hash,a.account_hash as dominant_account_hash,
  (select count(*) from oracle_session_repo x where x.days=r.days and x.epoch_key=r.epoch_key and x.session_hash=r.session_hash) as repo_count
from roots r left join ranked_repo p on p.days=r.days and p.epoch_key=r.epoch_key and p.session_hash=r.session_hash and p.rank=1
left join ranked_account a on a.days=r.days and a.epoch_key=r.epoch_key and a.session_hash=r.session_hash and a.rank=1;

insert into oracle_values
select days,'totals','all','sessions',count(*) from oracle_session_root group by days;
insert into oracle_values
select days,'totals','all','sessionsWithTokens',sum(token_events>0) from oracle_session_root group by days;
insert into oracle_values
select days,'bySource',source,'sessions',count(*) from oracle_session_source group by days,source;
insert into oracle_values
select days,'bySource',source,'sessionsWithTokens',sum(token_events>0) from oracle_session_source group by days,source;

-- The old snapshot exposes the top 60 (session,source) rows, with root repo
-- labels and counts. Epoch stays in the row key until presentation.
create temp table oracle_top_sessions as
select s.*,r.branch_hash,r.repo_count,r.dominant_repo_hash,
  row_number() over(partition by s.days order by s.cost_nanos desc,s.events desc) as display_rank
from oracle_session_source s join oracle_session_root r using(days,epoch_key,session_hash);

create temp table oracle_repo_sessions as
with assigned as (
  select days,epoch_key,session_hash,repo_hash,input_tokens,output_tokens,cost_nanos
  from oracle_session_repo
  union all
  select u.days,u.epoch_key,u.session_hash,r.dominant_repo_hash,
    coalesce(sum(u.input_tokens),0),coalesce(sum(u.output_tokens),0),coalesce(sum(u.cost_nanos),0)
  from oracle_window_rows u join oracle_session_root r using(days,epoch_key,session_hash)
  where u.repo_hash is null and u.session_hash is not null
  group by u.days,u.epoch_key,u.session_hash
)
select days,epoch_key,session_hash,repo_hash,
  coalesce(sum(input_tokens),0) as input_tokens,coalesce(sum(output_tokens),0) as output_tokens,
  coalesce(sum(cost_nanos),0) as cost_nanos
from assigned group by days,epoch_key,session_hash,repo_hash;
create temp table oracle_repos as
select x.days,x.repo_hash,count(*) as sessions,
  coalesce(sum(x.input_tokens),0) as input_tokens,coalesce(sum(x.output_tokens),0) as output_tokens,
  coalesce(sum(x.cost_nanos),0) as cost_nanos,
  (select count(distinct b.branch_hash) from oracle_window_rows b
    join oracle_session_root r using(days,epoch_key,session_hash)
    where b.days=x.days and coalesce(b.repo_hash,r.dominant_repo_hash) is x.repo_hash
      and b.branch_hash is not null) as branch_refs
from oracle_repo_sessions x group by x.days,x.repo_hash;

create temp table oracle_account_sessions as
select r.days,r.epoch_key,r.session_hash,r.dominant_account_hash as account_hash,
  r.dominant_repo_hash as repo_hash,r.source,r.cost_nanos,r.input_tokens,r.output_tokens
from oracle_session_root r;
create temp table oracle_accounts as
select s.days,s.account_hash,count(*) as sessions,
  coalesce(sum(s.cost_nanos),0) as cost_nanos,
  coalesce(sum(case when s.repo_hash is null then s.cost_nanos else 0 end),0) as unlinked_nanos,
  coalesce(sum(case when s.repo_hash in (select canonical_linkage(repo_hash) from ledger.priority_repos) then s.cost_nanos else 0 end),0) as priority_nanos,
  coalesce(sum(case when s.repo_hash is not null and s.repo_hash not in (select canonical_linkage(repo_hash) from ledger.priority_repos) then s.cost_nanos else 0 end),0) as other_nanos,
  coalesce(sum(case when s.source='claude_code' then s.cost_nanos else 0 end),0) as claude_nanos,
  coalesce(sum(case when s.source='codex' then s.cost_nanos else 0 end),0) as codex_nanos,
  coalesce(sum(s.input_tokens),0) as input_tokens,coalesce(sum(s.output_tokens),0) as output_tokens
from oracle_account_sessions s group by s.days,s.account_hash;
create temp table oracle_account_machines as
select distinct s.days,s.account_hash,r.machine_hash from oracle_account_sessions s
join oracle_window_rows r using(days,epoch_key,session_hash)
where r.machine_hash is not null;
