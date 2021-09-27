select
	evt,
	perceived_at,
	ge.game_id,
	ge.season,
	ge.day,
	pl.player_id,
	pl.player_name
from (select unnest(event_text) as evt, * from data.game_events where array_to_string(event_text, ';') like '%is Partying!%') ge
join data.games on ge.game_id=games.game_id
join data.team_roster tr on (tr.team_id=games.home_team or tr.team_id=games.away_team)
	and tr.position_type_id<2 -- excludes shadows
	and tr.valid_from <= ge.perceived_at
	and (tr.valid_until > ge.perceived_at or tr.valid_until is null)
join data.players pl on pl.player_id=tr.player_id
	and pl.valid_from <= ge.perceived_at
	and (pl.valid_until > ge.perceived_at or pl.valid_until is null)
where games.season < 11
	and position(pl.player_name || ' is Partying!' in ge.evt) > 0

-- These updates were missed, but the game outcomes tell us the parties happened
union select
    'Gallup Crueller is Partying!' as evt,
	-- timestamp comes from the chron record
	timestamp without time zone '2020-09-18T16:25:00.65' as perceived_at,
	'53f05bb4-9c8a-4128-b97e-97105cd2519b' as game_id,
	6 as season,
	94 as day,
	'e7ecf646-e5e4-49ef-a0e3-10a78e87f5f4' as player_id,
	'Gallup Crueller' as player_name

union select
    'Hiroto Cerna is Partying!' as evt,
	-- timestamp comes from the chron record
	timestamp without time zone '2020-09-18T17:25:12.164' as perceived_at,
	'2587657a-f6e7-4986-9f10-1a27238e1158' as game_id,
	6 as season,
	95 as day,
	'd51f1fe8-4ab8-411e-b836-5bba92984d32' as player_id,
	'Hiroto Cerna' as player_name

union select
    'Wyatt Pothos is Partying!' as evt,
	-- timestamp comes from the chron record
	timestamp without time zone '2020-09-18T18:05:00.635' as perceived_at,
	'0cd09810-2885-4b1d-9ba0-ad7eb399fd26' as game_id,
	6 as season,
	96 as day,
	'ea44bd36-65b4-4f3b-ac71-78d87a540b48' as player_id,
	'Wyatt Pothos' as player_name


order by perceived_at