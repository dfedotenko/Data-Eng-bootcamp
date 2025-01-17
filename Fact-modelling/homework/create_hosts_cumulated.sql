-- A DDL for hosts_cumulated table that has:

-- a host_activity_datelist which which logs to see 
-- which dates each host is experiencing any activity

create table hosts_cumulated (
    host text,
-- the list of dates in the past where this host was visited
-- by date
	host_activity_datelist DATE[],
-- current visited date
	date DATE,
	primary key (host, date)
)
