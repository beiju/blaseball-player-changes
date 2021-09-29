select
	evt,
	perceived_at,
	ge.game_id,
	ge.season,
	ge.day,
	pl.player_id,
	pl.player_name
from (select unnest(event_text) as evt, * from data.game_events where array_to_string(event_text, ';') like '% a Third Wave of Coffee!%') ge
join data.games on ge.game_id=games.game_id
join data.team_roster tr on (tr.team_id=games.home_team or tr.team_id=games.away_team)
	and tr.position_type_id=1 -- pitchers only
	and tr.valid_from <= ge.perceived_at
	and (tr.valid_until > ge.perceived_at or tr.valid_until is null)
join data.players pl on pl.player_id=tr.player_id
	and pl.valid_from <= ge.perceived_at
	and (pl.valid_until > ge.perceived_at or pl.valid_until is null)
-- player 2 won't be used in the query but it will be used in the string matching
-- hopefully this means i get every pemutation of 2 pitchers
join data.team_roster tr2 on (tr2.team_id=games.home_team or tr2.team_id=games.away_team)
	and tr2.position_type_id=1 -- pitchers only
	and tr2.valid_from <= ge.perceived_at
	and (tr2.valid_until > ge.perceived_at or tr2.valid_until is null)
join data.players pl2 on pl2.player_id=tr2.player_id
	and pl2.valid_from <= ge.perceived_at
	and (pl2.valid_until > ge.perceived_at or pl2.valid_until is null)
where games.season < 11
	and ((position(pl.player_name || ' chugs a Third Wave of Coffee!' in ge.evt) > 0
		  -- this is a hack to limit pl2 to one option so there aren't a bunch of rows with the same pl
		  and pl.player_id=pl2.player_id)
	     or position(pl.player_name || ' and ' || pl2.player_name || ' chug a Third Wave of Coffee!' in ge.evt) > 0
		 or position(pl2.player_name || ' and ' || pl.player_name || ' chug a Third Wave of Coffee!' in ge.evt) > 0)
order by perceived_at