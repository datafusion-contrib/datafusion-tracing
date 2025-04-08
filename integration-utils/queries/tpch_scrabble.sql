-- Compute each nation's Scrabble score, the average score per region,
-- and each nation's rank within its region. Results are ordered by region and rank.
WITH scrabble_scores AS (
    -- Define the Scrabble point values for each letter using a VALUES clause
    SELECT letter, points
    FROM (
             VALUES
                 ('A', 1), ('B', 3), ('C', 3), ('D', 2), ('E', 1),
                 ('F', 4), ('G', 2), ('H', 4), ('I', 1), ('J', 8),
                 ('K', 5), ('L', 1), ('M', 3), ('N', 1), ('O', 1),
                 ('P', 3), ('Q', 10), ('R', 1), ('S', 1), ('T', 1),
                 ('U', 1), ('V', 4), ('W', 4), ('X', 8), ('Y', 4),
                 ('Z', 10)
         ) AS t(letter, points)
),

-- Precompute the nation information and the uppercase nation name,
-- along with an array of its letters.
     nation_letters AS (
         SELECT
             n.n_nationkey,
             n.n_name AS nation_name,
             r.r_name AS region_name,
             UPPER(n.n_name) AS up_name,
             STRING_TO_ARRAY(UPPER(n.n_name), NULL) AS letters
         FROM nation n
                  JOIN region r
                       ON n.n_regionkey = r.r_regionkey
     ),

-- Explode the array of letters.
     exploded_letters AS (
         SELECT
             n.n_nationkey,
             n.nation_name,
             n.region_name,
             UNNEST(n.letters) AS letter
         FROM nation_letters n
     ),

-- Compute the Scrabble score for each nation by summing points for each letter.
     nation_scores AS (
         SELECT
             el.region_name,
             el.n_nationkey,
             el.nation_name,
             SUM(s.points) AS scrabble_score
         FROM exploded_letters el
                  JOIN scrabble_scores s
                       ON el.letter = s.letter
         GROUP BY el.n_nationkey, el.nation_name, el.region_name
     ),

-- Calculate the average Scrabble score per region and rank the nations within each region by score.
     scored_nations AS (
         SELECT
             nation_name,
             region_name,
             scrabble_score,
             AVG(scrabble_score) OVER (PARTITION BY region_name) AS region_avg_scrabble_score,
                 RANK() OVER (PARTITION BY region_name ORDER BY scrabble_score DESC) AS nation_rank_in_region
         FROM nation_scores
     )

-- Final result: List nations along with their score, regional average, and rank.
SELECT
    region_name,
    nation_name,
    scrabble_score,
    region_avg_scrabble_score,
    nation_rank_in_region
FROM scored_nations
ORDER BY region_avg_scrabble_score, region_name, nation_rank_in_region, nation_name;