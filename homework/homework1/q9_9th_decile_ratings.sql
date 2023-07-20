with cte_people as (
 select 
  p.name, c.title_id, p.person_id
 from people p, crew c 
 where p.person_id = c.person_id and p.born = 1955 and c.category in ('actor', 'actress')
),
cte_titles as (
 select 
   ctp.name name, round(avg(r.rating), 2) avg_rating
  from cte_people ctp, ratings r where ctp.title_id = r.title_id group by ctp.person_id order by avg_rating desc, name
),
cte_rk as (
 select ctt.name, ctt.avg_rating, ntile(10) over (order by ctt.avg_rating desc, ctt.name) rk from cte_titles ctt
)
select name, avg_rating from cte_rk where rk = 9;
