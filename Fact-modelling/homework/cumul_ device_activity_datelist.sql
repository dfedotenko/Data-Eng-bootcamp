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
    where DATE(e.event_time) <= DATE('2023-01-31') and user_id is not null
),
-- create the date arrays for each browser_type
browser_date_arrays AS (
    SELECT 
        user_id,
        browser_type,
        ARRAY_AGG(event_time ORDER BY event_time) as date_array
    FROM mapped
    WHERE browser_type IS NOT NULL
    GROUP BY user_id, browser_type
),
-- create the JSONB object from the pre-aggregated arrays
final_agg AS (
    SELECT 
        user_id,
        jsonb_object_agg(
            browser_type, 
            to_jsonb(date_array)
        ) as browser_dates
    FROM browser_date_arrays
    GROUP BY user_id
)
SELECT 
    user_id,
    ROW(browser_dates)::device_activity_ds as device_activity_datelist,
    DATE('2023-01-31')
FROM final_agg
ON CONFLICT (user_id, date) 
DO UPDATE SET 
    device_activity_datelist = EXCLUDED.device_activity_datelist;