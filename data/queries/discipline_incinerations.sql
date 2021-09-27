-- select * from data.game_events where pitcher_id='c6e2e389-ed04-4626-a5ba-fe398fe89568'
-- order by perceived_at asc
-- limit 10
select
	evt,
	perceived_at,
	ge.game_id,
	ge.season,
	ge.day,
	victim_pl.player_id as victim_id,
	victim_pl.player_name as victim_name,
	replacement_pl.player_id as replacement_id,
	replacement_pl.player_name as replacement_name
from (select unnest(event_text) as evt, * from data.game_events where array_to_string(event_text, ';') like '%Rogue Umpire incinerated %! Replaced by %') ge
join data.games on ge.game_id=games.game_id
join data.team_roster victim_tr on (victim_tr.team_id=games.home_team or victim_tr.team_id=games.away_team)
	and victim_tr.position_type_id<2 -- excludes shadows
	and victim_tr.valid_from <= ge.perceived_at
	and (victim_tr.valid_until > ge.perceived_at or victim_tr.valid_until is null)
join data.players victim_pl on victim_pl.player_id=victim_tr.player_id
	and victim_pl.valid_from <= ge.perceived_at
	and (victim_pl.valid_until > ge.perceived_at or victim_pl.valid_until is null)
join data.team_roster replacement_tr on (replacement_tr.team_id=games.home_team or replacement_tr.team_id=games.away_team)
	and replacement_tr.position_type_id<2 -- excludes shadows
	and replacement_tr.valid_from <= (ge.perceived_at + interval '1' hour)
	and (replacement_tr.valid_until > (ge.perceived_at + interval '1' hour) or replacement_tr.valid_until is null)
join data.players replacement_pl on replacement_pl.player_id=replacement_tr.player_id
	and replacement_pl.valid_from <= (ge.perceived_at + interval '1' hour)
	and (replacement_pl.valid_until > (ge.perceived_at + interval '1' hour) or replacement_pl.valid_until is null)
where games.season < 11
	and position(victim_pl.player_name || '! Replaced by' in ge.evt) > 0
	and position('Replaced by ' || replacement_pl.player_name in ge.evt) > 0

union select 'Rogue Umpire incinerated Millennials pitcher Scrap Murphy! Replaced by Felix Garbage' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-29T08:31:00.000Z' as perceived_at,
	'995f84d2-ee86-4af6-a81a-45c279aba2d7' as game_id,
	1 as season,
	39 as day,
	'40db1b0b-6d04-4851-adab-dd6320ad2ed9' as victim_id,
	'Scrap Murphy' as victim_name,
	'18af933a-4afa-4cba-bda5-45160f3af99b' as replacement_id,
	'Felix Garbage' as replacement_name

union select 'Rogue Umpire incinerated Steaks hitter Lars Mendoza! Replaced by Marco Stink' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-29T22:25:00.000Z' as perceived_at,
	'653cf39c-0ffb-41ea-be6b-c0dafa7b0ddb' as game_id,
	1 as season,
	51 as day,
	'76c4853b-7fbc-4688-8cda-c5b8de1724e4' as victim_id,
	'Lars Mendoza' as victim_name,
	'87e6ae4b-67de-4973-aa56-0fc9835a1e1e' as replacement_id,
	'Marco Stink' as replacement_name

union select 'Rogue Umpire incinerated Magic hitter Sosa Elftower! Replaced by Halexandrey Walton' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-30T10:12:00.000Z' as perceived_at,
	'646a3d03-5304-4bfc-a4c0-8076e05077de' as game_id,
	1 as season,
	63 as day,
	'c86b5add-6c9a-40e0-aa43-e4fd7dd4f2c7' as victim_id,
	'Sosa Elftower' as victim_name,
	'03b80a57-77ea-4913-9be4-7a85c3594745' as replacement_id,
	'Halexandrey Walton' as replacement_name

union select 'Rogue Umpire incinerated Magic pitcher Famous Oconnor! Replaced by Cory Twelve' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-30T11:29:00.000Z' as perceived_at,
	'fef507f9-0573-4849-b463-c99cce6842ce' as game_id,
	1 as season,
	64 as day,
	'bca38809-81de-42ff-94e3-1c0ebfb1e797' as victim_id,
	'Famous Oconnor' as victim_name,
	'2da49de2-34e5-49d0-b752-af2a2ee061be' as replacement_id,
	'Cory Twelve' as replacement_name

union select 'Rogue Umpire incinerated Spies hitter Dickerson Greatness! Replaced by Collins Melon' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-30T11:31:00.000Z' as perceived_at,
	'fe71e327-6a78-4395-aefb-27cf057e2a15' as game_id,
	1 as season,
	64 as day,
	'3afb30c1-1b12-466a-968a-5a9a21458c7f' as victim_id,
	'Dickerson Greatness' as victim_name,
	'ef9f8b95-9e73-49cd-be54-60f84858a285' as replacement_id,
	'Collins Melon' as replacement_name

union select 'Rogue Umpire incinerated Moist Talkers hitter Trevino Merritt! Replaced by Simon Haley' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-30T18:16:00.000Z' as perceived_at,
	'bd6d87c3-5b20-4d3a-8492-1d03f0406c51' as game_id,
	1 as season,
	71 as day,
	'70a458ed-25ca-4ff8-97fc-21cbf58f2c2a' as victim_id,
	'Trevino Merritt' as victim_name,
	'020ed630-8bae-4441-95cc-0e4ecc27253b' as replacement_id,
	'Simon Haley' as replacement_name

union select 'Rogue Umpire incinerated Steaks hitter Zi Delacruz! Replaced by Thomas Kirby' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-30T18:19:00.000Z' as perceived_at,
	'a840778d-2946-4f3a-b10e-ddd54b541d1c' as game_id,
	1 as season,
	71 as day,
	'c83a13f6-ee66-4b1c-9747-faa67395a6f1' as victim_id,
	'Zi Delacruz' as victim_name,
	'f73009c5-2ede-4dc4-b96d-84ba93c8a429' as replacement_id,
	'Thomas Kirby' as replacement_name

union select 'Rogue Umpire incinerated Fridays hitter Jessi Wise! Replaced by York Silk' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-30T20:24:00.000Z' as perceived_at,
	'63f7e78e-9559-4f81-8db9-98c7a13003d3' as game_id,
	1 as season,
	73 as day,
	'57448b62-f952-40e2-820c-48d8afe0f64d' as victim_id,
	'Jessi Wise' as victim_name,
	'86d4e22b-f107-4bcf-9625-32d387fcb521' as replacement_id,
	'York Silk' as replacement_name

union select 'Rogue Umpire incinerated Flowers hitter Hurley Pacheco! Replaced by Nic Winkler' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-30T22:30:00.000Z' as perceived_at,
	'0a3500f2-1d5c-4838-a8fb-d1ae136f50ee' as game_id,
	1 as season,
	75 as day,
	'b86237bb-ade6-4b1d-9199-a3cc354118d9' as victim_id,
	'Hurley Pacheco' as victim_name,
	'855775c1-266f-40f6-b07b-3a67ccdf8551' as replacement_id,
	'Nic Winkler' as replacement_name

union select 'Rogue Umpire incinerated Jazz Hands hitter Alexandria Dracaena! Replaced by Hendricks Richardson' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-31T02:12:00.000Z' as perceived_at,
	'c5fc64df-319d-4120-9ea3-07f0ca36cc37' as game_id,
	1 as season,
	79 as day,
	'262c49c6-8301-487d-8356-747023fa46a9' as victim_id,
	'Alexandria Dracaena' as victim_name,
	'cf8e152e-2d27-4dcc-ba2b-68127de4e6a4' as replacement_id,
	'Hendricks Richardson' as replacement_name

union select 'Rogue Umpire incinerated Dalé hitter Aldon Anthony! Replaced by Murray Pony' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-31T10:24:00.000Z' as perceived_at,
	'04250880-4f38-4bda-af2c-0ccc42360f9e' as game_id,
	1 as season,
	87 as day,
	'4bda6584-6c21-4185-8895-47d07e8ad0c0' as victim_id,
	'Aldon Anthony' as victim_name,
	'2ca0c790-e1d5-4a14-ab3c-e9241c87fc23' as replacement_id,
	'Murray Pony' as replacement_name

union select 'Rogue Umpire incinerated Pies hitter Cedric Gonzalez! Replaced by Dan Holloway' as evt,
	-- Date comes from trawling discord
	timestamp without time zone '2020-07-31T15:30:00.000Z' as perceived_at,
	'e252c9c0-48af-4a6e-9a35-a03b3d672022' as game_id,
	1 as season,
	92 as day,
	'6fc3689f-bb7d-4382-98a2-cf6ddc76909d' as victim_id,
	'Cedric Gonzalez' as victim_name,
	'667cb445-c288-4e62-b603-27291c1e475d' as replacement_id,
	'Dan Holloway' as replacement_name

-- This one happened during the Grand Unslam, the event text was never observed but presumably it fits the pattern
union select 'Rogue Umpire incinerated Garages hitter Shaquille Torres! Replaced by Cedric Spliff' as evt,
	-- Date is when the site came back after the unslam
	timestamp without time zone '2020-08-06T23:13:00.000Z' as perceived_at,
	'72673e9e-b0af-48e2-863c-cf35c8b0e0fd' as game_id,
	2 as season,
	71 as day,
	'495a6bdc-174d-4ad6-8d51-9ee88b1c2e4a' as victim_id,
	'Shaquille Torres' as victim_name,
	'c31d874c-1b4d-40f2-a1b3-42542e934047' as replacement_id,
	'Cedric Spliff' as replacement_name

-- The game event for this one was missed
union select 'Rogue Umpire incinerated Garages hitter Shaquille Torres! Replaced by Cedric Spliff' as evt,
	-- Date is when the replacement player was first seen
	timestamp without time zone '2020-08-07T05:20:18.606' as perceived_at,
	'41fbc590-a6fa-4e01-aaa6-7d0dcbda680d' as game_id,
	2 as season,
	79 as day,
	'bd9d1d6e-7822-4ad9-bac4-89b8afd8a630' as victim_id,
	'Derrick Krueger' as victim_name,
	'c6e2e389-ed04-4626-a5ba-fe398fe89568' as replacement_id,
	'Henry Marshallow' as replacement_name
order by perceived_at