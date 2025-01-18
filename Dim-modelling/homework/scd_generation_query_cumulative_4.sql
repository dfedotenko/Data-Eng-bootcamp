-- Task 4: "scd generation query"
-- Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.

-- This is a backfill query i.e. it is cumulative
-- It populates SCD table for actors with information about continuously active streaks
-- (a film shot per a given year)
-- It also populates the average actor cumulative quality based on film ratings
insert into actors_history_scd
WITH streak_started AS (
    SELECT actor,
           quality_class,
		   is_active,
		   current_year,
           -- Use LAG to look back at one given record behind and update quality class
           -- upon change
           LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY current_year) <> quality_class
               OR LAG(actor, 1) OVER
               (PARTITION BY actor ORDER BY current_year) IS NULL
               AS did_change
    FROM actors
),
     streak_identified AS (
         SELECT
            	actor,
                quality_class,
				is_active,
                current_year,
            -- Indicate changes in rating over time
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY actor ORDER BY current_year) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            actor,
            quality_class,
			is_active,
			current_year,
            streak_identifier,
            MIN(current_year) AS start_year,
            MAX(current_year) AS end_year
         FROM streak_identified
         -- aggregate everything
         GROUP BY actor,quality_class,is_active,current_year,streak_identifier
     )
     SELECT actor, quality_class, is_active, start_year, end_year, current_year
     FROM aggregated
	 ORDER BY actor, start_year ASC