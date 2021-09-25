import re
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

# Set of sets of attributes that were added at once
NEW_ATTR_SETS = {
    frozenset({'cinnamon', 'bat', 'fate', 'peanutAllergy'}),
    frozenset({'hittingRating', 'baserunningRating',
               'defenseRating', 'pitchingRating'})
}

# These elections will be handled manually once I figure out the election format
PRE_FEED_ELECTIONS = {
    # season: set[timestamp]
    1: {'2020-08-02T19:09:05', '2020-08-02T19:09:06', '2020-08-02T19:09:07'}
}

EPS = 1e-10

session = requests_cache.CachedSession("blaseball-player-changes",
                                       backend="sqlite", expire_after=None)
_SESSIONS_BY_EXPIRY[None] = session

team_rosters = pd.read_csv('data/team_rosters.csv')
modifications = pd.read_csv('data/modifications.csv', index_col='modification')
prev_for_player = {}
creeping_peanut = {}


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


def find_manual_fixes(_: Optional[JsonDict], after: JsonDict,
                      changed_keys: Set[str]) -> Iterator[ChangeSource]:
    # The infamous Chorby Soul soul edit
    if (after['entityId'] == 'a1628d97-16ca-4a75-b8df-569bae02bef9' and
            after['validFrom'] == '2020-08-03T04:23:53.241Z'):
        changed_keys.remove('soul')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'soul'})


def find_chron_start(before: Optional[JsonDict], after: JsonDict,
                     changed_keys: Set[str]) -> Iterator[ChangeSource]:
    # Use startswith because chron proper includes milliseconds but VCR doesn't
    if after['validFrom'].startswith(CHRON_START_DATE):
        assert before is None
        changed_keys.clear()  # Signal that the change is fully accounted for
        yield UnknownTimeChangeSource(ChangeSourceType.CHRON_START,
                                      keys_changed=changed_keys)


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
    if 'weekAttr' not in after['data']:
        return
    # This function has to find all added mods because I have no infrastructure
    # to track which are accounted for and which are not.
    new_mods = set(after['data']['weekAttr']) - set(before['data']['weekAttr'])
    if 'weekAttr' in changed_keys and new_mods:
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
    for season, timestamps in PRE_FEED_ELECTIONS.items():
        timestamp_no_ms = re.sub(r'(:?\.\d{1,3})?Z$', '', after['validFrom'])
        if timestamp_no_ms in timestamps:
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
    find_hits_tracker,
    find_new_attributes,
    find_pre_feed_election,
    find_creeping_peanuts,
    find_fateless_fated,

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
