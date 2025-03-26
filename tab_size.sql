with t1 as ( 
  SELECT tabschema, tabname
        ,SUM(DATA_OBJECT_P_SIZE + INDEX_OBJECT_P_SIZE + LONG_OBJECT_P_SIZE + LOB_OBJECT_P_SIZE + XML_OBJECT_P_SIZE) as full_p_size
        ,SUM(DATA_OBJECT_P_SIZE)                      as DATA_T_P_SIZE
        ,SUM(INDEX_OBJECT_P_SIZE)                     as IDX_P_SIZE
        ,SUM(LONG_OBJECT_P_SIZE + LOB_OBJECT_P_SIZE)  as LOB_P_SIZE
        ,SUM(XML_OBJECT_P_SIZE)                       as XML_P_SIZE
        ,SUM(DICTIONARY_SIZE)                         as DICTIONARY_SIZE
  FROM SYSIBMADM.ADMINTABINFO GROUP BY TABSCHEMA, TABNAME
)
,t2 as (
select t1.*
      ,substr(t2.TBSPACE, 1, 24) as TBSPACE
      ,substr(t2.INDEX_TBSPACE, 1, 24) as INDEX_TBSPACE
      ,substr(t2.LONG_TBSPACE, 1, 24) as LONG_TBSPACE
      ,t2.AVGROWSIZE
      ,t2.CARD
      ,CAST(full_p_size*100./sum(full_p_size) OVER() as DECIMAL(5,2)) as psize_pct
      ,CAST(sum(full_p_size) OVER(order by full_p_size desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100./sum(full_p_size) OVER() as DECIMAL(5,2)) as full_p_size_pct_sum
--    ,t3.ROWS_SAMPLED, t3.PAGES_SAVED_PERCENT, t3.BYTES_SAVED_PERCENT, t3.AVG_COMPRESS_REC_LENGTH
from t1 join syscat.tables t2 on (t1.tabschema, t1.tabname)=(t2.tabschema, t2.tabname)
-- ,TABLE(SYSPROC.ADMIN_GET_TAB_COMPRESS_INFO(t1.tabschema, t1.tabname, 'ESTIMATE')) t3
where t1.tabschema not like 'SYS%'
)
-- select sum(full_p_size) from t2
select
  t2.*
from t2
order by full_p_size desc
fetch first 45 rows only
with ur;


with source(DATA) as (
  select <LOB_COL_NAME> as DATA from <TABNAME>
)
,size_intervals(start, end) as (
   values (cast(0 as bigint), cast(128 as bigint))
   union all
   select end, end*2 from size_intervals where end*2 <= 32*1024
)
,extended_size_interwals(start, end) as (
  values (-1,0), (32*1024, bigint(2)*1024*1024*1024)
  union all
  select * from size_intervals
)
,size_distribution as (
  select 
      start, end
     ,count(*) CNT
     ,sum(bigint(length(s.DATA))) as length_sum
  from extended_size_interwals sz join source s 
           on start < length(s.DATA) and length(s.DATA) <= end
  group by start, end
)
select 
    s.*
   ,sum(length_sum) OVER(order by start asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
    sum(CNT)        OVER(order by start asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    as avg_size
from size_distribution s
order by end asc
with ur
;


