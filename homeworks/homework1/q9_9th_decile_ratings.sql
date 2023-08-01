with cte_people as (
 select 
  p.name, c.title_id, p.person_id
 from people p, crew c, titles t
 where p.person_id = c.person_id and t.title_id = c.title_id and t.type like 'movie' and p.born = 1955
)
,
cte_titles as (
 select 
   ctp.name name, round(avg(r.rating), 2) avg_rating
  from cte_people ctp, ratings r where ctp.title_id = r.title_id group by ctp.person_id
)
,
cte_rk as (
 select ctt.name, ctt.avg_rating, ntile(10) over (order by ctt.avg_rating) rk from cte_titles ctt
)
select name, avg_rating from cte_rk ctr where rk = 9 order by ctr.avg_rating desc, ctr.name;
