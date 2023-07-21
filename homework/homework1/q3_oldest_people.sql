SELECT
p.name,
2022 - p.born age
FROM people p
WHERE p.born IS NOT NULL AND p.died IS NULL AND p.born >= 1900 ORDER BY age DESC, name LIMIT 20;