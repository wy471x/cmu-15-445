with cte_crew as (
 select distinct c.title_id from crew c, people p 
 where c.person_id = p.person_id and p.name like 'Nicole Kidman' and p.born = 1967
)
select 
  distinct p.name 
from people p, crew c 
where c.person_id = p.person_id and c.category in ('actor' , 'actress') 
and c.title_id in (select title_id from cte_crew) order by p.name;