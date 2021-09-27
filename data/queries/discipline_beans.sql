select
	evt,
	perceived_at,
	ge.game_id,
	ge.season,
	ge.day,
	pl.player_id,
	pl.player_name
from (select unnest(event_text) as evt, * from data.game_events where array_to_string(event_text, ';') like '% hits % with a pitch! % is now %') ge
join data.games on ge.game_id=games.game_id
join data.team_roster tr on (tr.team_id=games.home_team or tr.team_id=games.away_team)
	and tr.position_type_id<2 -- excludes shadows
	and tr.valid_from <= ge.perceived_at
	and (tr.valid_until > ge.perceived_at or tr.valid_until is null)
join data.players pl on pl.player_id=tr.player_id
	and pl.valid_from <= ge.perceived_at
	and (pl.valid_until > ge.perceived_at or pl.valid_until is null)
where games.season < 11
	and position('hits ' || pl.player_name || ' with a pitch!' in ge.evt) > 0

-- The datablase doesn't have Day X, but there was a bean
union select
    'Jaylen Hotdogfingers hits Wyatt Quitter with a pitch! Wyatt Quitter is now Repeating!' as evt,
	-- timestamp comes from the bossfight equivalent of game_events
	timestamp without time zone '2020-10-11T02:38:59.705Z' as perceived_at,
	'3e2882a7-1553-49bd-b271-49cab930d9fc' as game_id, -- technically a fight id but i'll deal with that later
	8 as season,
	-1 as day, -- uhhhhh
	'5ca7e854-dc00-4955-9235-d7fcd732ddcf' as player_id,
	'Wyatt Quitter' as player_name

order by perceived_at