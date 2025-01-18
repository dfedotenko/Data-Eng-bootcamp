-- The incremental query to generate host_activity_datelist

insert into hosts_cumulated
with yesterday as (
    select * from hosts_cumulated
	-- the date changes according to the next day you run it on
    where date = DATE('2023-01-02')
),
today as (
	select
		host,
		DATE(CAST(event_time as TIMESTAMP)) as date_active
	from events
	where 
		-- the date changes according to the next day you run it on
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
-- this is upsert
on conflict (host, date) 
DO update set 
    host_activity_datelist = excluded.host_activity_datelist