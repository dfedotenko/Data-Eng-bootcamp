-- A query to deduplicate game_details from Day 1 so there's no duplicates

with deduped as (
	select 
		game_details.*, 
		row_number() over(partition by game_details.game_id, game_details.team_id, game_details.player_id
		) as row_num
	from game_details
)
select * from deduped
    where row_num = 1
