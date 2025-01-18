-- A DDL for an user_devices_cumulated table that has:

-- a device_activity_datelist which tracks a users active days by browser_type
-- data type is MAP<STRING, ARRAY[DATE]> i.e. browser_type -> dates

CREATE TYPE device_activity_ds AS (
-- this can hold anything but in our case,
-- it holds a browser name as a key mapped to an array of dates
-- note I am not using a MAP DdataStruct here
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