-- select unnest(event_text) as evt, * from data.game_events where array_to_string(event_text, ';') like '%Magmatic%' and season < 11
select
	evt,
	perceived_at,
	ge.game_id,
	ge.season,
	ge.day,
	draining_pl.player_id as drainer_id,
	draining_pl.player_name as drainer_name,
	drained_pl.player_id as drained_id,
	drained_pl.player_name as drained_name
from (select unnest(event_text) as evt, * from data.game_events where array_to_string(event_text, ';') like '%The Blooddrain gurgled!%') ge
join data.games on ge.game_id=games.game_id
join data.team_roster drained_tr on (drained_tr.team_id=games.home_team or drained_tr.team_id=games.away_team)
	and drained_tr.position_type_id<2 -- excludes shadows
	and drained_tr.valid_from <= ge.perceived_at
	and (drained_tr.valid_until > ge.perceived_at or drained_tr.valid_until is null)
join data.players drained_pl on drained_pl.player_id=drained_tr.player_id
	and drained_pl.valid_from <= ge.perceived_at
	and (drained_pl.valid_until > ge.perceived_at or drained_pl.valid_until is null)
join data.team_roster draining_tr on (draining_tr.team_id=games.home_team or draining_tr.team_id=games.away_team)
	and draining_tr.position_type_id<2 -- excludes shadows
	and draining_tr.valid_from <= ge.perceived_at
	and (draining_tr.valid_until > ge.perceived_at or draining_tr.valid_until is null)
join data.players draining_pl on draining_pl.player_id=draining_tr.player_id
	and draining_pl.valid_from <= ge.perceived_at
	and (draining_pl.valid_until > ge.perceived_at or draining_pl.valid_until is null)
where games.season < 11
	and position(draining_pl.player_name || ' siphoned some of ' in ge.evt) > 0
	and position('siphoned some of ' || drained_pl.player_name || '''s' in ge.evt) > 0
order by perceived_at