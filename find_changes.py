from datetime import timedelta, datetime
from functools import lru_cache
from typing import List, Optional, Set, Iterator

import pandas as pd
import requests_cache
from blaseball_mike.chronicler import paged_get
from blaseball_mike.eventually import search as feed_search
from blaseball_mike.session import _SESSIONS_BY_EXPIRY
from dateutil.parser import isoparse

from Change import Change, JsonDict
from ChangeSource import ChangeSource, ChangeSourceType, \
    UnknownTimeChangeSource, GameEventChangeSource, ElectionChangeSource

# CHRON_START_DATE = '2020-09-13T19:20:00Z'
CHRON_START_DATE = '2020-07-29T08:12:22'
CHRON_VERSIONS_URL = "https://api.sibr.dev/chronicler/v2/versions"
CHRON_GAMES_URL = 'https://api.sibr.dev/chronicler/v1/games'

NEGATIVE_ATTRS = {'tragicness', 'patheticism'}
PARTY_ATTRS = {'thwackability', 'buoyancy', 'musclitude', 'continuation',
               'indulgence', 'divinity', 'chasiness', 'omniscience',
               'shakespearianism', 'unthwackability', 'laserlikeness',
               'watchfulness', 'suppression', 'overpowerment', 'martyrdom',
               'groundFriction', 'anticapitalism', 'baseThirst', 'patheticism',
               'moxie', 'tenaciousness', 'tragicness', 'cinnamon', 'coldness',
               'ruthlessness'}
# Peanut attrs are party attrs plus fingers minus cinnamon(?)
PEANUT_ATTRS = PARTY_ATTRS.union({'totalFingers'}).difference({'cinnamon'})

# Set of sets of attributes that were added at once
NEW_ATTR_SETS = {
    frozenset({'cinnamon', 'bat', 'fate', 'peanutAllergy'}),
    frozenset({'hittingRating', 'baserunningRating',
               'defenseRating', 'pitchingRating'}),
    frozenset({'armor', 'coffee', 'ritual', 'blood'}),
    frozenset({'seasAttr', 'permAttr', 'gameAttr', 'weekAttr'}),
}

# These elections will be handled manually once I figure out the election format
PRE_FEED_ELECTIONS = {
    # season: (start time, end time)
    1: ('2020-08-02T19:09:05', '2020-08-02T19:09:08'),
    2: ('2020-08-09T19:27:41', '2020-08-09T19:27:47'),
    3: ('2020-08-30T19:18:18', '2020-08-30T19:18:28'),
}

EPS = 1e-10

session = requests_cache.CachedSession("blaseball-player-changes",
                                       backend="sqlite", expire_after=None)
_SESSIONS_BY_EXPIRY[None] = session

team_rosters = pd.read_csv('data/team_rosters.csv')
modifications = pd.read_csv('data/modifications.csv', index_col='modification')
prev_for_player = {}
creeping_peanut = {}

discipline_peanuts = pd.read_csv('data/discipline_peanuts.csv')
discipline_feedbacks = pd.read_csv('data/discipline_feedbacks.csv')


def get_keys_changed(before: Optional[dict], after: dict) -> Set[str]:
    if before is None:
        return set(after['data'].keys())

    a = after['data']
    b = before['data']
    return {key for key in set(a.keys()).union(set(b.keys()))
            if key not in a or key not in b or a[key] != b[key]}


def get_change(after):
    before = prev_for_player.get(after['entityId'], None)
    prev_for_player[after['entityId']] = after

    sources: List[ChangeSource] = []
    changed_keys = get_keys_changed(before, after)

    for change_finder in CHANGE_FINDERS:
        for source in change_finder(before, after, changed_keys):
            # Source should be derived from ChangeSource but not the base type
            assert isinstance(source, ChangeSource)
            assert not type(source) is ChangeSource
            sources.append(source)

        if not changed_keys:
            return Change(before, after, sources)

    return Change(before, after, [
        UnknownTimeChangeSource(ChangeSourceType.UNKNOWN, changed_keys)
    ])
    # This is to have the game in the local variable for debugging
    games = get_game(after['entityId'], after['validFrom'])
    raise RuntimeError("Can't identify change")


def find_manual_fixes(before: Optional[JsonDict], after: JsonDict,
                      changed_keys: Set[str]) -> Iterator[ChangeSource]:
    # The infamous Chorby Soul soul edit
    if (after['entityId'] == 'a1628d97-16ca-4a75-b8df-569bae02bef9' and
            after['validFrom'] == '2020-08-03T04:23:53.241Z'):
        changed_keys.remove('soul')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'soul'})

    # Renaming York's bat
    if (after['entityId'] == '86d4e22b-f107-4bcf-9625-32d387fcb521' and
            after['validFrom'] == '2020-08-09T06:23:26.778Z'):
        changed_keys.remove('bat')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'bat'})

    # Giving axel his bat, I guess? Seems like this was well post-election
    if (after['entityId'] == '3af96a6b-866c-4b03-bc14-090acf6ecee5' and
            after['validFrom'] == '2020-08-10T18:47:01.611Z'):
        changed_keys.remove('bat')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'bat'})

    # Manually Wyatts Masoning Marco Stink
    if (after['entityId'] == '87e6ae4b-67de-4973-aa56-0fc9835a1e1e' and
            after['validFrom'] == '2020-08-12T20:25:33.308Z'):
        changed_keys.remove('name')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'name'})

    # Manually Wyatt Masoning Summers Preston
    if (after['entityId'] == '80e474a3-7d2b-431d-8192-2f1e27162607' and
            after['validFrom'] == '2020-08-12T20:25:34.306Z'):
        changed_keys.remove('name')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'name'})

    # Manually Wyatt Masonsing Rivers Clembons
    if (after['entityId'] == 'af6b3edc-ed52-4edc-b0c9-14e0a5ae0ee3' and
            after['validFrom'] == '2020-08-12T20:25:34.726Z'):
        changed_keys.remove('name')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'name'})

    # I'm taking a stand here: the wyatt masoning doesn't get its own type.
    if before is not None and changed_keys == {'name'} and (
            before['data']['name'] == "Wyatt Mason" or
            before['data']['name'] == "Wyatts Mason" or
            before['data']['name'] == "Wyatt Masons" or
            before['data']['name'] == "Wyatt Breadwinner"):
        changed_keys.remove('name')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'name'})

    # Thomas England -> Sixpack Dogwalker
    if (after['entityId'] == '3a96d76a-c508-45a0-94a0-8f64cd6beeb4' and
            after['validFrom'] == '2020-08-28T19:54:23.418Z'):
        keys = {'name', 'ritual', 'bat', 'thwackability'}
        changed_keys.difference_update(keys)
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL, keys)

    # The waveback event undid Baldwin's fate change from feedback
    if (after['entityId'] == 'e4034192-4dc6-4901-bb30-07fe3cf77b5e' and
            after['validFrom'] == '2020-08-28T19:54:23.418Z'):
        changed_keys.remove('fate')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'fate'})

    # Shortly after fixing the waveback event they thought of a better joke for
    # the name of new Sixpack's bat
    if (after['entityId'] == '3a96d76a-c508-45a0-94a0-8f64cd6beeb4' and
            after['validFrom'] == '2020-08-28T21:02:34.226Z'):
        changed_keys.remove('bat')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'bat'})

    # Change Tot Fox's ritual for, I think, cultural sensitivity
    if (after['entityId'] == '90c2cec7-0ed5-426a-9de8-754f34d59b39' and
            after['validFrom'] == '2020-08-30T07:26:01.225Z'):
        changed_keys.remove('ritual')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'ritual'})


def find_chron_start(before: Optional[JsonDict], after: JsonDict,
                     changed_keys: Set[str]) -> Iterator[ChangeSource]:
    # Use startswith because chron proper includes milliseconds but VCR doesn't
    if after['validFrom'].startswith(CHRON_START_DATE):
        assert before is None
        changed_keys.clear()  # Signal that the change is fully accounted for
        yield UnknownTimeChangeSource(ChangeSourceType.CHRON_START,
                                      keys_changed=changed_keys)


def find_rename_attribute(_: Optional[JsonDict], __: JsonDict,
                          changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if changed_keys.issubset({'id', '_id'}):
        changed_keys.remove('id')
        changed_keys.remove('_id')
        yield UnknownTimeChangeSource(ChangeSourceType.RENAMED_ATTRIBUTES,
                                      keys_changed={'id', '_id'})


def find_change_attribute_format(_: Optional[JsonDict], after: JsonDict,
                                 changed_keys: Set[str]) -> \
        Iterator[ChangeSource]:
    # Changed bat attribute from the bat name to a bat id
    if 'bat' in changed_keys and (
            after['validFrom'] == '2020-08-30T07:25:59.724Z' or
            after['validFrom'] == '2020-08-30T07:26:00.713Z' or
            # they forgot about axel...
            after['validFrom'] == '2020-08-30T20:18:56.326Z'):
        changed_keys.discard('bat')
        yield UnknownTimeChangeSource(ChangeSourceType.CHANGED_ATTRIBUTE_FORMAT,
                                      keys_changed={'bat'})


def find_traj_reset(_: Optional[JsonDict], after: JsonDict,
                    changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if 'tragicness' in changed_keys and (after['data']['tragicness'] == 0 or
                                         after['data']['tragicness'] == 0.1):
        changed_keys.remove('tragicness')
        yield UnknownTimeChangeSource(ChangeSourceType.TRAJ_RESET,
                                      keys_changed={'tragicness'})


def find_attributes_capped(before: JsonDict, after: JsonDict,
                           changed_keys: Set[str]) -> Iterator[ChangeSource]:
    capped_keys = {k for k in changed_keys
                   if (after['data'][k] == 0.01 and before['data'][k] < 0.01)
                   or (after['data'][k] == 0.001 and before['data'][k] < 0.001)
                   or (k in NEGATIVE_ATTRS and
                       after['data'][k] == 0.99 and before['data'][k] > 0.99)
                   or (k in NEGATIVE_ATTRS and
                       after['data'][k] == 0.999 and before['data'][k] > 0.999)}
    if capped_keys:
        [changed_keys.discard(k) for k in capped_keys]
        yield UnknownTimeChangeSource(ChangeSourceType.ATTRIBUTES_CAPPED,
                                      keys_changed=capped_keys)


def find_hits_tracker(_: Optional[JsonDict], __: JsonDict,
                      changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if hit_keys := changed_keys.intersection({'hitStreak', 'consecutiveHits'}):
        changed_keys.discard('hitStreak')
        changed_keys.discard('consecutiveHits')
        yield UnknownTimeChangeSource(ChangeSourceType.HITS_TRACKER,
                                      keys_changed=hit_keys)


def time_str(timestamp: datetime):
    return timestamp.isoformat().replace('+00:00', 'Z')


def find_from_feed(before: JsonDict, after: JsonDict,
                   changed_keys: Set[str]) -> Iterator[ChangeSource]:
    timestamp = isoparse(after['validFrom'])
    events = feed_search(cache_time=None, limit=-1, query={
        'playerTags': after['entityId'],
        'before': time_str(timestamp - timedelta(seconds=180)),
        'after': time_str(timestamp + timedelta(seconds=180)),
    })
    for event in events:
        pass
        yield None


# The arguments are used in the pandas query, through some arcane magic
# noinspection PyUnusedLocal
def get_player_team_id(player_id: str, timestamp: str):
    timestamp = timestamp.replace('T', ' ')
    result = team_rosters.query(
        'player_id==@player_id and valid_from<=@timestamp and '
        '(valid_until>@timestamp or valid_until.isnull())')
    assert len(result) == 1
    return result.iloc[0]['team_id']


# This may be called many times with the same id and timestamp in a row, as it
# is used by many find_* functions, but then once it's called with a new value
# it will never be called with the old value again. So only store the previously
# computed value.
@lru_cache(maxsize=1)
def get_game(player_id: str, timestamp: str):
    team_id = get_player_team_id(player_id, timestamp)
    return paged_get(CHRON_GAMES_URL, {
        "team": team_id,
        "before": timestamp,
        "order": "desc"
    }, session, total_count=1)[0]


# Early chron has some large gaps between incinerations happening and the
# replacement being recorded, so get a bunch of games leading up to this. I've
# found you need up to at least 5 for Nic Winkler. This is only needed for
# incinerations, since that's the only Blaseball(tm) mechanic that existed at
# that time.
@lru_cache(maxsize=1)
def get_possible_games(player_id: str, timestamp: str):
    team_id = get_player_team_id(player_id, timestamp)
    return paged_get(CHRON_GAMES_URL, {
        "team": team_id,
        "before": timestamp,
        "order": "desc"
    }, session, total_count=10)


def find_weekly_mods_wear_off(_: JsonDict, after: JsonDict,
                              changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if 'weekAttr' in changed_keys and len(after['data']['weekAttr']) == 0:
        # Is probably weekly mods wearing off. Get game day to be sure.
        game_data = get_game(after['entityId'], after['validFrom'])['data']
        if (game_data['day'] + 1) % 9 == 0:
            changed_keys.remove('weekAttr')
            yield GameEventChangeSource(ChangeSourceType.WEEKLY_MODS_WEAR_OFF,
                                        keys_changed={'weekAttr'},
                                        season=game_data['season'],
                                        day=game_data['day'])


def find_weekly_mod_added(before: JsonDict, after: JsonDict,
                          changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if 'weekAttr' not in changed_keys:
        return
    # This function has to find all added mods because I have no infrastructure
    # to track which are accounted for and which are not.
    new_mods = set(after['data']['weekAttr']) - set(before['data']['weekAttr'])
    if new_mods:
        changed_keys.remove('weekAttr')
        game_data = get_game(after['entityId'], after['validFrom'])['data']
        player_name = before['data']['name']
        for mod_added in new_mods:
            mod_name = modifications.loc[mod_added, 'title']
            search_str = (f"hits {player_name} with a pitch! "
                          f"{player_name} is now {mod_name}!")
            if any(search_str in outcome for outcome in game_data['outcomes']):
                yield GameEventChangeSource(ChangeSourceType.HIT_BY_PITCH,
                                            keys_changed={'weekAttr'},
                                            season=game_data['season'],
                                            day=game_data['day'])
            else:
                raise RuntimeError("Mod added from unknown source")


# See comment on `get_game`
@lru_cache(maxsize=1)
def get_player_team(player_id: str, timestamp: str):
    team_id = get_player_team_id(player_id, timestamp)
    results = paged_get(CHRON_VERSIONS_URL, {
        "type": 'team',
        "id": team_id,
        "before": timestamp,
        "order": "desc",
    }, session, total_count=1)
    assert len(results) == 1
    return results[0]


def get_team_mods(team):
    return set(team['data']['permAttr']).union(team['data']['seasAttr'],
                                               team['data']['weekAttr'],
                                               team['data']['gameAttr'])


def find_incin_replacement(before: Optional[JsonDict], after: JsonDict,
                           changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if before is not None:
        return

    for game in get_possible_games(after['entityId'], after['validFrom']):
        if not any("Rogue Umpire incinerated" in outcome
                   and f"Replaced by {after['data']['name']}" in outcome
                   for outcome in game['data']['outcomes']):
            continue
        prev_keys = changed_keys.copy()
        changed_keys.clear()
        yield GameEventChangeSource(ChangeSourceType.INCINERATION_REPLACEMENT,
                                    keys_changed=prev_keys,
                                    season=game['data']['season'],
                                    day=game['data']['day'])


def find_party(before: JsonDict, after: JsonDict,
               changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if PARTY_ATTRS.issubset(changed_keys):
        # Check the game to see if there's a party
        game_data = get_game(after['entityId'], after['validFrom'])['data']
        player_name = before['data']['name']
        search_str = f"{player_name} is Partying!"
        if any(outcome == search_str for outcome in game_data['outcomes']):
            # See what type of party is expected
            team = get_player_team(after['entityId'], after['validFrom'])
            team_mods = get_team_mods(team)
            if 'PARTY_TIME' in team_mods and 'LIFE_OF_PARTY' in team_mods:
                multiplier = 1.2  # It says 1.1 but it's 1.2
            elif 'PARTY_TIME' in team_mods:
                multiplier = 1
            else:
                # Surely every party before game 27 is a hotel motel party
                assert game_data['day'] < 27
                multiplier = 0.5

            # Check that the rolls are in range
            for attr in PARTY_ATTRS:
                diff = after['data'][attr] - before['data'][attr]
                if attr in NEGATIVE_ATTRS:
                    # If floored, ignore
                    if after['data'][attr] == 0.01:
                        continue
                    # Else make the difference positive
                    diff *= -1
                if not 0.04 * multiplier < diff < 0.08 * multiplier:
                    return

            # If not returned, it was in range!
            [changed_keys.discard(attr) for attr in PARTY_ATTRS]
            yield GameEventChangeSource(ChangeSourceType.PARTY,
                                        keys_changed=PARTY_ATTRS,
                                        season=game_data['season'],
                                        day=game_data['day'])


def find_incineration_victim(_: JsonDict, after: JsonDict,
                             changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if 'deceased' not in changed_keys:
        return

    for game in get_possible_games(after['entityId'], after['validFrom']):
        if any("Rogue Umpire incinerated" in outcome
               and f"{after['data']['name']}! Replaced by" in outcome
               for outcome in game['data']['outcomes']):
            changed_keys.remove('deceased')
            yield GameEventChangeSource(ChangeSourceType.INCINERATED,
                                        keys_changed={'deceased'},
                                        season=game['data']['season'],
                                        day=game['data']['day'])


def find_new_attributes(before: Optional[JsonDict], _: JsonDict,
                        changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if before is None:
        return

    for attr_set in NEW_ATTR_SETS:
        if (attr_set.issubset(changed_keys) and
                all(before['data'].get(key, 0) == 0 for key in attr_set)):
            [changed_keys.discard(attr) for attr in attr_set]
            yield UnknownTimeChangeSource(ChangeSourceType.ADDED_ATTRIBUTES,
                                          keys_changed=set(attr_set))


def find_pre_feed_election(_: JsonDict, after: JsonDict,
                           changed_keys: Set[str]) -> Iterator[ChangeSource]:
    for season, (start_time, end_time) in PRE_FEED_ELECTIONS.items():
        if start_time <= after['validFrom'] <= end_time:
            prev_keys = changed_keys.copy()
            changed_keys.clear()
            yield ElectionChangeSource(ChangeSourceType.PRE_FEED_ELECTION,
                                       keys_changed=prev_keys,
                                       season=season)


def find_fateless_fated(before: Optional[JsonDict], after: JsonDict,
                        changed_keys: Set[str]) -> Iterator[ChangeSource]:
    # Restrict to creeping peanuts/fateless fated dates
    if not '2020-08-02T19:09:07' < after['validFrom'] < '2020-08-03T05:23:57':
        return

    # Only record this if it's _only_ fate (and maybe peanut allergy) changing
    if (before is not None and
            'fate' in changed_keys and
            changed_keys.issubset({'peanutAllergy', 'fate', 'tragicness'}) and
            before['data']['fate'] == 0 and
            after['data']['fate'] != 0):
        changed_keys.remove('fate')
        yield UnknownTimeChangeSource(ChangeSourceType.FATELESS_FATED,
                                      keys_changed={'fate'})


# noinspection PyUnusedLocal
def find_discipline_peanut(before: Optional[JsonDict], after: JsonDict,
                           changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if before is not None and changed_keys.issubset(PEANUT_ATTRS):
        player_id = after['entityId']
        before_time = before['validFrom'].replace('T', ' ')
        after_time = after['validFrom'].replace('T', ' ')
        possible_nuts = discipline_peanuts.query(
            'player_id==@player_id and '
            'perceived_at>=@before_time and perceived_at<=@after_time')
        if len(possible_nuts) == 1:
            [changed_keys.discard(attr) for attr in PEANUT_ATTRS]
            nut_row = possible_nuts.iloc[0]
            yield GameEventChangeSource(ChangeSourceType.PEANUT,
                                        keys_changed=PARTY_ATTRS,
                                        season=int(nut_row['season']),
                                        day=int(nut_row['day']))
        else:
            # 2 peanuts in one chron update? inconceivable!
            assert len(possible_nuts) == 0


def find_first_blood(before: Optional[JsonDict], _: JsonDict,
                     changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if (before is not None and 'blood' in changed_keys and
            before['data']['blood'] == 0):
        changed_keys.remove('blood')
        yield UnknownTimeChangeSource(ChangeSourceType.FIRST_BLOOD, {'blood'})


def find_interview(before: Optional[JsonDict], _: JsonDict,
                   changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if (before is not None and
            'coffee' in changed_keys and before['data']['coffee'] == 0 and
            'ritual' in changed_keys and before['data']['ritual'] == ''):
        changed_keys.remove('coffee')
        changed_keys.remove('ritual')
        yield UnknownTimeChangeSource(ChangeSourceType.INTERVIEW,
                                      keys_changed={'coffee', 'ritual'})


# noinspection PyUnusedLocal
def find_discipline_feedback_fate(before: Optional[JsonDict], after: JsonDict,
                                  changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if before is not None and 'fate' in changed_keys:
        player_id = after['entityId']
        before_time = before['validFrom'].replace('T', ' ')
        after_time = after['validFrom'].replace('T', ' ')
        possible_feedbacks = discipline_feedbacks.query(
            '(player_id==@player_id or player_id_2==@player_id) and '
            'perceived_at>=@before_time and perceived_at<=@after_time')
        if len(possible_feedbacks) == 1:
            changed_keys.discard('fate')
            feedback_row = possible_feedbacks.iloc[0]
            yield GameEventChangeSource(ChangeSourceType.FEEDBACK_FATE,
                                        keys_changed={'fate'},
                                        season=int(feedback_row['season']),
                                        day=int(feedback_row['day']))
        else:
            # 2 feedbacks in one chron update? inconceivable!
            assert len(possible_feedbacks) == 0


def find_creeping_peanuts(before: Optional[JsonDict], after: JsonDict,
                          changed_keys: Set[str]) -> Iterator[ChangeSource]:
    # Restrict to creeping peanuts/fateless fated dates
    if not '2020-08-02T19:09:07' < after['validFrom'] < '2020-08-03T05:23:57':
        return

    if (before is not None and
            'peanutAllergy' in changed_keys and
            changed_keys.issubset({'peanutAllergy', 'fate', 'tragicness'})):
        changed_keys.remove('peanutAllergy')
        if after['data']['peanutAllergy']:
            # Player was just made creepy-peanut
            assert after['entityId'] not in creeping_peanut
            creeping_peanut[after['entityId']] = True
            yield UnknownTimeChangeSource(
                ChangeSourceType.CREEPING_PEANUT_ALLERGY, {'peanutAllergy'})
        else:
            if not after['entityId'] in creeping_peanut:
                print(after['data']['name'],
                      "was un-allergized without being made allergic")
            elif not creeping_peanut[after['entityId']]:
                print(after['data']['name'],
                      "was un-allergized twice")
            creeping_peanut[after['entityId']] = False
            yield UnknownTimeChangeSource(
                ChangeSourceType.CREEPING_PEANUT_DEALLERGIZE, {'peanutAllergy'})


def find_precision_changed(before: JsonDict, after: JsonDict,
                           changed_keys: Set[str]) -> Iterator[ChangeSource]:
    precision_keys = {key for key in changed_keys
                      if (key in before['data'] and
                          key in after['data'] and
                          approximately_equal(after['data'][key],
                                              before['data'][key]))}
    if precision_keys:
        [changed_keys.discard(attr) for attr in precision_keys]
        yield UnknownTimeChangeSource(ChangeSourceType.PRECISION_CHANGE,
                                      keys_changed=precision_keys)


def approximately_equal(a, b):
    try:
        return abs(a - b) < EPS
    except TypeError:
        return a == b


CHANGE_FINDERS = [
    # Manual overrides
    find_manual_fixes,

    # First try finders that don't need to hit the network
    find_chron_start,
    find_rename_attribute,
    find_change_attribute_format,
    find_hits_tracker,
    find_new_attributes,
    find_pre_feed_election,
    find_creeping_peanuts,
    find_fateless_fated,
    find_discipline_peanut,
    find_first_blood,
    find_interview,
    find_discipline_feedback_fate,

    # Feed finder before the more specific finders, as it gives us the most
    # information when it works
    find_from_feed,

    # All new player finders should go here so the rest can assume that `before`
    # is populated
    find_incin_replacement,

    find_weekly_mods_wear_off,
    find_weekly_mod_added,
    find_party,
    find_incineration_victim,

    # I feel like this should go last just thematically
    find_precision_changed,

    # Only return these if no other change explains the data
    find_traj_reset,
    find_attributes_capped,
]
