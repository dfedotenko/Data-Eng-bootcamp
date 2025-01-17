-- A DDL for an user_devices_cumulated table that has:

-- a device_activity_datelist which tracks a users active days by browser_type
-- data type is MAP<STRING, ARRAY[DATE]> i.e. browser_type -> dates

CREATE TYPE device_activity_ds AS (
    browser_dates JSONB
)

create table user_devices_cumulated (
	user_id numeric,
-- the list of dates in the past where this user was active
-- by browser type
	device_activity_datelist device_activity_ds,
-- current active date
	date DATE,
	primary key (user_id, date)
)