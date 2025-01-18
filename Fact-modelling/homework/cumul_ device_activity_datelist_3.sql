-- A cumulative query to generate device_activity_datelist from events

-- select
-- 	max(event_time),
-- 	min(event_time)
-- from events

-- dedup devices table to have 1:1 browser to device mapping
insert into user_devices_cumulated
with dedup as (
select distinct(device_id),
        browser_type
        from devices
),
-- join with events table so that we have browser type recorded against each event
-- for a given device_id
mapped as (
select  
        e.*,
        d.browser_type
    from events e
    left join dedup d
    on e.device_id = d.device_id
    -- the date must change according to the day you run it on
    where DATE(e.event_time) <= DATE('2023-01-31') and user_id is not null
),
-- create the date arrays for each browser_type
browser_date_arrays as (
    select 
        user_id,
        browser_type,
        ARRAY_AGG(event_time order by event_time) as date_array
    from mapped
    where browser_type is not null
    group by user_id, browser_type
),
-- create the JSONB object from the pre-aggregated arrays
final_agg as (
    select 
        user_id,
        jsonb_object_agg(
            browser_type, 
            to_jsonb(date_array)
        ) as browser_dates
    from browser_date_arrays
    group by user_id
)
select 
    user_id,
    ROW(browser_dates)::device_activity_ds as device_activity_datelist,
    DATE('2023-01-31')
from final_agg
on conflict (user_id, date) 
do update set 
    device_activity_datelist = excluded.device_activity_datelist