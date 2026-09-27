-- Read-only shape pass from plan-r6-checks/ledger-shape-census.log and
-- b5_lexical_boundary.py. The harness attaches the ledger as "ledger".
-- SQLite's parser is diagnostic only: V8 Date.parse supplies the authoritative
-- UTC day in the code pass, including shapes SQLite cannot parse.
select b.rowid, b.source, b.observed_at,
  case when b.observed_at glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[01][0-9]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z'
         or b.observed_at glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T2[0-3]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z'
       then 'canonical_T_3frac_Z'
       when b.observed_at not glob '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'
       then 'non_iso'
       when strftime('%Y-%m-%d', b.observed_at) is null then 'unparsed_by_sqlite'
       when substr(b.observed_at,1,10) <> strftime('%Y-%m-%d', b.observed_at) then 'day_move'
       when b.observed_at < substr(b.observed_at,1,10)||'T00:00:00.000Z' then 'before_own_midnight'
       else 'examine' end as sql_shape,
  strftime('%Y-%m-%d', b.observed_at) as sqlite_day
from ledger.buffered_events b;
