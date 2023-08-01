WITH cte_titles AS (
SELECT
t.*
FROM titles t, crew c, people p
WHERE t.title_id = c.title_id AND c.person_id = p.person_id AND p.name like '%Cruise%' AND p.born=1962
)
SELECT
ctt.primary_title,r.votes
FROM cte_titles ctt, ratings r
WHERE ctt.title_id= r.title_id order by r.votes desc limit 10;