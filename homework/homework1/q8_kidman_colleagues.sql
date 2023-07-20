with cte_crew as (
 select t.title_id from titles t, crew c, people p 
 where t.title_id = c.title_id and c.person_id = p.person_id and p.name like 'Nicole Kidman' and p.born = 1967
)
select distinct p.name from people p, crew c where (c.category like 'actor' or c.category like 'actress') and c.title_id in (select title_id from cte_crew) and c.person_id = p.person_id order by p.name;