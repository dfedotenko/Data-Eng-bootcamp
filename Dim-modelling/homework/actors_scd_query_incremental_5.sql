-- Task 5: "incremental scd query"
-- Incremental query for actors_history_scd: 
-- Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.

create type actors_scd_type AS (
                    quality_class quality_class,
                    is_active boolean,
                    start_year INTEGER,
                    end_year INTEGER
                        )

with last_year_scd as (
    select * from actors_history_scd
    where current_year = 1970
    and end_year = 1970
),
     historical_scd as (
        select
               actor,
               quality_class,
               is_active,
               start_year,
               end_year
        from actors_history_scd
        where current_year = 1970
        and end_year < 1970
     ),
     this_year_data as (
         select * from actors
         where current_year = 1971
		 order by actor, current_year asc
     ),
     unchanged_records as (
         select
                ts.actor,
                ts.quality_class,
                ts.is_active,
                ls.start_year,
                ts.current_year as end_year
        from this_year_data ts
        join last_year_scd ls
        on ls.actor = ts.actor
         where ts.quality_class = ls.quality_class
         and ts.is_active = ls.is_active
     ),
     changed_records as (
        select
                ts.actor,
                UNNEST(ARRAY[
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_year,
                        ls.end_year
                        )::actors_scd_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.current_year,
                        ts.current_year
                        )::actors_scd_type
                ]) as records
        from this_year_data ts
        left join last_year_scd ls
        on ls.actor = ts.actor
         where (ts.quality_class <> ls.quality_class
          or ts.is_active <> ls.is_active)
     ),
     unnested_changed_records as (
         select actor,
                (records::actors_scd_type).quality_class,
                (records::actors_scd_type).is_active,
                (records::actors_scd_type).start_year,
                (records::actors_scd_type).end_year
                from changed_records
         ),
     new_records as (
         select
            ts.actor,
            ts.quality_class,
            ts.is_active,
            ts.current_year as start_year,
            ts.current_year as end_year
         from this_year_data ts
         left join last_year_scd ls
             on ts.actor = ls.actor
         where ls.actor is null

     )
select *, 1971 as current_year from (
                  select *
                  from historical_scd
                  union all
                  select *
                  from unchanged_records
                  union all
                  select *
                  from unnested_changed_records
                  union all
                  select *
                  from new_records
              ) a
			  order by actor, current_year asc