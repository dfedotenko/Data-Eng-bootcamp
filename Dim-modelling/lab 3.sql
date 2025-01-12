-- lab3

-- CREATE TYPE vertex_type
--     AS ENUM('player', 'team', 'game');



-- CREATE TABLE vertices (
--     identifier TEXT,
--     type vertex_type,
--     properties JSON,
--     PRIMARY KEY (identifier, type)
-- );

-- CREATE TYPE edge_type AS
--     ENUM ('plays_against',
--           'shares_team',
--           'plays_in',
--           'plays_on'
--         );

-- CREATE TABLE edges (
--     subject_identifier TEXT,
--     subject_type vertex_type,
--     object_identifier TEXT,
--     object_type vertex_type,
--     edge_type edge_type,
--     properties JSON,
--     PRIMARY KEY (subject_identifier,
--                 subject_type,
--                 object_identifier,
--                 object_type,
--                 edge_type)
-- )

-- insert into vertices
-- select 
-- 	game_id as identifier,
-- 	'game'::vertex_type as type,
-- 	json_build_object(
-- 		'pts_home', pts_home,
-- 		'pts_away', pts_away,
-- 		'winning_team', case when home_team_wins = 1 then home_team_id else visitor_team_id end
-- 	) as properties
-- from games

-- insert into vertices
-- with players_agg as (
-- select 
-- 	player_id as identifier,
-- 	min(player_name) as player_name,
-- 	count(1) as number_of_games,
-- 	sum(pts) as total_points,
-- 	array_agg(distinct team_id) as teams
-- from game_details
-- group by player_id
-- )

-- select identifier, 
-- 		'player'::vertex_type,
-- 		json_build_object(
-- 			'player_name', player_name,
-- 			'number_of_games', number_of_games,
-- 			'total_points', total_points,
-- 			'teams', teams
-- 		)
-- from players_agg

-- insert into vertices
-- with teams_deduped as(
-- 	select *, row_number() over(partition by team_id) as row_num
-- 	from teams
-- )
-- select 
-- 	team_id as identifier,
-- 	'team'::vertex_type,
-- 	json_build_object(
-- 		'abbreviation', abbreviation,
-- 		'nickname', nickname,
-- 		'city', city,
-- 		'arena', arena,
-- 		'year_founded', yearfounded
-- 	)
-- from teams_deduped
-- where row_num = 1

-- select type, count(*)	
-- from vertices
-- group by type

-- insert into edges
-- with deduped as (
-- 	select*, row_number() over (partition by player_id, game_id) as row_num
-- 	from game_details
-- )
-- select
-- 	player_id as subject_identfier,
-- 	'player'::vertex_type as subject_type,
-- 	game_id as object_identifier,
-- 	'game'::vertex_type as object_type,
-- 	'plays_in'::edge_type as edge_type,
-- 	json_build_object(
-- 		'sart_position', start_position,
-- 		'pts', pts,
-- 		'team_id', team_id,
-- 		'team_abbreviation', team_abbreviation
-- 	) as properties
-- from deduped
-- where row_num = 1

-- select player_id, game_id, count(*) as num
-- from game_details
-- group by 1,2
-- having count(*) > 1

-- select
-- 	v.properties->>'player_name',
-- 	max(cast(e.properties->>'pts' as integer))
-- from vertices v join edges e
-- on e.subject_identifier = v.identifier
-- and e.subject_type = v.type 
-- group by 1
-- order by 2 desc

-- 
select
	v.properties->>'player_name',
	e.object_identifier,
	cast(v.properties->>'number_of_games' as real)/
	case when cast(v.properties->>'total_points' as real) = 0 then 1 
	else cast(v.properties->>'total_points' as real) end,
	e.properties->>'subject_points',
	e.properties->>'num_games' 
from vertices v join edges e
	on v.identifier = e.subject_identifier
	and v.type = e.subject_type
where e.object_type = 'player'::vertex_type