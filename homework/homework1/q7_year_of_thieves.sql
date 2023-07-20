with cte_titles as (
 select distinct t.premiered from titles t, akas a where t.title_id = a.title_id and a.title like 'Army of Thieves'
)
select count(*) from titles t, cte_titles ctt where t.premiered = ctt.premiered;