WITH RECURSIVE numbers(n) AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 3
) SELECT n FROM numbers