-- An incremental query that loads host_activity_reduced, day-by-day

insert into host_activity_reduced
with daily_aggregate as (
    select 
    host,
    DATE(event_time) as date,
    count(1) as num_site_hits,
    array_agg(DISTINCT user_id::text) as unique_visitors  -- Collect unique user_ids as text array
    from events
    where date(event_time) = date('2023-01-02')
    and host is not null
    group by host, DATE(event_time)
),
yesterday_array as (
    select *
    from host_activity_reduced
    where month_start = date('2023-01-01')
)
select 
    coalesce(da.host,ya.host) as host,
    coalesce(ya.month_start, date_trunc('month',da.date)) as month_start,
    case
     when ya.hit_array is not null 
    then
        ya.hit_array || ARRAY[coalesce(da.num_site_hits,0)::int]
    when ya.month_start is null 
    then
        ARRAY[coalesce(da.num_site_hits,0)::int]
    when ya.hit_array is null 
        then array_fill(0::int, array[coalesce(date - date(date_trunc('month',date)),0)]) ||
            ARRAY[coalesce(da.num_site_hits,0)::int]
    end as hit_array,
    case
     when ya.unique_visitors is not null 
    then
        ya.unique_visitors || da.unique_visitors  -- Concatenate text arrays of user_ids
    when ya.month_start is null 
    then
        da.unique_visitors
    when ya.unique_visitors is null 
        then da.unique_visitors
    end as unique_visitors
from daily_aggregate da
full outer join yesterday_array ya on da.host = ya.host\
-- this is upsert
on conflict (host, month_start)
do update set hit_array = excluded.hit_array, unique_visitors = excluded.unique_visitors;
