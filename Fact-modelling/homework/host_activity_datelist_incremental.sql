
INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT * FROM hosts_cumulated
    WHERE date = DATE('2023-01-02')
),
today as (
	select
		host,
		DATE(CAST(event_time as TIMESTAMP)) as date_active
	from events
	where 
		DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-03')
	and user_id is not null
	group by host, DATE(CAST(event_time as TIMESTAMP))
)
select 
	coalesce(t.host, y.host) as host,
	case when host_activity_datelist is null 
	then array[t.date_active]
	when t.date_active is null then y.host_activity_datelist
	else array[t.date_active] || y.host_activity_datelist
	end as host_activity_datelist,
	coalesce(t.date_active,y.date + Interval '1 day') as date
	from today t
	full outer join yesterday y
	on t.host = y.host
-- on CONFLICT (host, date) 
-- DO update set 
--     host_activity_datelist = EXCLUDED.host_activity_datelist