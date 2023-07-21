SELECT
t.primary_title,
t.premiered,
t.runtime_minutes || ' (mins)'
FROM titles t
WHERE t.genres LIKE '%Sci-Fi%' ORDER BY t.runtime_minutes DESC, t.primary_title LIMIT 10;