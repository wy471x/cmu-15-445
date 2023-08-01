with recursive cte_titles_akas as (
select distinct a.title from akas a, titles t where t.primary_title like 'House of the Dragon' and t.type like 'tvSeries' and a.title_id = t.title_id order by a.title
)
,
cte_rk as (
  select rank() over (order by ctta.title) rk, ctta.title from cte_titles_akas ctta
)
,
cte_str(rk, title) as (
  select ctr.rk, ctr.title from cte_rk ctr where ctr.rk = 1
  union all
  select cte_str.rk + 1, title || ', ' || (select ctr.title from cte_rk ctr where ctr.rk = cte_str.rk + 1) from cte_str limit (select max(rk) from cte_rk)
)
select cts.title from cte_str cts order by cts.rk desc limit 1;