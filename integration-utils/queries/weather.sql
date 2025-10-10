SELECT
    count(*),
    "MinTemp"
FROM weather
GROUP BY "MinTemp"
