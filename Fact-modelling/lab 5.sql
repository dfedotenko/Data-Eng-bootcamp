-- lab 5


-- select
-- 	max(event_time),
-- 	min(event_time)
-- from events

-- create table users_cumulated (
-- 	user_id text,
-- -- the list of dates in the past where user was active
-- 	dates_active DATE[],
-- -- current active date
-- 	date DATE,
-- 	primary key (user_id, date)
-- )

-- insert into users_cumulated
-- with yesterday as (
-- 	select *
-- 	from users_cumulated
-- 	where date = DATE('2023-01-30')
-- ),
-- today as (
-- 	select
-- 		cast(user_id as text) as user_id,
-- 		DATE(CAST(event_time as TIMESTAMP)) as date_active
-- 	from events
-- 	where 
-- 		DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-31')
-- 	and user_id is not null
-- 	group by user_id, DATE(CAST(event_time as TIMESTAMP))
-- )

-- select 
-- 	coalesce(t.user_id, y.user_id) as user_id,
-- 	case when dates_active is null 
-- 	then array[t.date_active]
-- 	when t.date_active is null then y.dates_active
-- 	else array[t.date_active] || y.dates_active
-- 	end as dates_active,
-- 	coalesce(t.date_active,y.date + Interval '1 day') as date
-- 	from today t
-- 	full outer join yesterday y
-- 	on t.user_id = y.user_id

-- SELECT * FROM users_cumulated
-- WHERE '2023-01-31'::DATE = ANY(dates_active)

WITH users AS (
    select * from users_cumulated
    where date = '2023-01-31'
), series as (
    select * 
    from generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, '1 day') 
    as series_date
),
place_holder_ints as (
    select cast(
        case when dates_active @> ARRAY[date(series_date)]
        then 
        cast(pow(2, 32-(date - date(series_date))) as bigint)
        else 0
        end 
        as bit(32)) as placeholder_int_value,
        *
    from users cross join series
)
select 
        user_id,
        cast(cast(sum(placeholder_int_value::bigint) as bigint) as bit(32)) 
        as bitmask,
        bit_count(cast(cast(sum(placeholder_int_value::bigint) as bigint) as bit(32))) > 0 
        as dim_is_monthly_active,
        bit_count(cast('11111110000000000000000000000000' as bit(32)) &
        cast(cast(sum(placeholder_int_value::bigint) as bigint) as bit(32))) > 0
        as dim_is_weekly_active,
        bit_count(cast('10000000000000000000000000000000' as bit(32)) &
        cast(cast(sum(placeholder_int_value::bigint) as bigint) as bit(32))) > 0
        as dim_is_daily_active
        from place_holder_ints
        group by user_id