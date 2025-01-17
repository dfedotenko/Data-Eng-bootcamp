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
    where DATE(e.event_time) <= DATE('2023-01-31') and user_id is not null
),
-- First get the dates and calculate day offsets
dates_with_offset AS (
    select 
        user_id,
        browser_type,
        DATE(event_time) as date_active,
        -- Calculate days offset from start of month
        (DATE(event_time) - DATE('2023-01-01'))::integer as d_offset
    from mapped
    where browser_type is not null
),
-- Convert to bits
browser_date_arrays as (
    select 
        user_id,
        browser_type,
        -- Shift to set 1 at the position of each day
        SUM(power(2, d_offset)::bigint)::bigint::bit(32) as activity_days
    from dates_with_offset
    group by user_id, browser_type
),
-- create the JSONB object from aggregating by browser type
final_agg as (
    select 
        user_id,
        jsonb_object_agg(
            browser_type, 
            to_jsonb(activity_days)
        ) as browser_dates
    from browser_date_arrays
    group by user_id
)
select 
    user_id,
    ROW(browser_dates)::device_activity_ds as device_activity_datelist,
    DATE('2023-01-31')
from final_agg
on CONFLICT (user_id, date) 
DO update set
    device_activity_datelist = EXCLUDED.device_activity_datelist