with cte_titles as (
 select t.* from titles t, crew c, people p where t.title_id = c.title_id and c.person_id = p.person_id and p.name like '%Cruise%' and p.born = 1962
)
select cta.title, r.votes from (select distinct ctt.title_id, a.title from cte_titles ctt, akas a where ctt.title_id = a.title_id) cta, ratings r where cta.title_id = r.title_id order by r.votes desc limit 10;