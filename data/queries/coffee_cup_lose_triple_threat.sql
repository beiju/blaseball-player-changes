select
	evt,
	perceived_at,
	ge.game_id,
	ge.season,
	ge.day,
	pl.player_id,
	pl.player_name
from (select unnest(event_text) as evt, * from data.game_events where array_to_string(event_text, ';') like '% is no longer a Triple Threat%') ge
join data.games on ge.game_id=games.game_id
join data.team_roster tr on (tr.team_id=games.home_team or tr.team_id=games.away_team)
	and tr.position_type_id=1 -- pitchers only
	and tr.valid_from <= ge.perceived_at
	and (tr.valid_until > ge.perceived_at or tr.valid_until is null)
join data.players pl on pl.player_id=tr.player_id
	and pl.valid_from <= ge.perceived_at
	and (pl.valid_until > ge.perceived_at or pl.valid_until is null)
where games.season < 11
	and position(pl.player_name || ' is no longer a Triple Threat' in ge.evt) > 0
order by perceived_at