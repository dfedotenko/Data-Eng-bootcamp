-- The first year of the actors_film table is 1970
-- The last year of the actors_film table is 2021

-- Task 2: "pipeline query"
-- Cumulative table generation query: 
-- Write a query that populates the actors table one year at a time.

insert into actors
with last_year as (
		select * from actors
		-- the year will change based on the current pipeline state
		-- it could be a parameter into the query
		where current_year = 1969
	),
	this_year as (
	    select
	        distinct(actor),
			year,
			-- We are going to collect film_info struct into array
			-- so we have reduction of columns per actor
	        array_remove(
	            array_agg(
	                case
	                    when year is not null
	                        then row(
								film,
								votes,
								rating,
								filmid
							)::film_info
	                end)
	            over (partition by actor),
	            null
	        ) as films,
		avg(rating) over (partition by actor) as rating_avg
	    from actor_films
		where year = 1970
		order by actor asc
	)
select
	coalesce(ty.actor, ly.actor) as actor,
	-- add to the existing film_info array of structs continuously
	case when ly.films is NULL
		then ty.films
		when ty.year is not null then ly.films || ty.films
	else ly.films
	end as films,
	-- while doing it, create a rating per year per actor
	case when ty.year is not null then
		case when rating_avg > 8 then 'star'
			when (rating_avg > 7 and rating_avg <= 8) then 'good'
			when (rating_avg > 6 and rating_avg <= 7) then 'average'
			else 'bad'
		end::quality_class
		else ly.quality_class
	end as quality_class,
	case when ty.year is not null then TRUE
		else FALSE
	end as is_active,
	-- track the active status of an actor as a number of years since last film
	case when ty.year is not null then 0
		else ly.years_since_last_film + 1
	end as years_since_last_film,
	-- update the last year column i.e. the last year we ran the query on
	coalesce(ty.year, ly.current_year+1) as current_year
from this_year ty full outer join last_year ly
	on ty.actor = ly.actor