from functools import lru_cache
from typing import List, Optional, Set, Iterator

import pandas as pd
from blaseball_mike.chronicler import paged_get
from blaseball_mike.session import session

from Change import Change, JsonDict
from ChangeSource import ChangeSource, ChangeSourceType, \
    UnknownTimeChangeSource, GameEventChangeSource

CHRON_START_DATE = '2020-09-13T19:20:00Z'
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
    frozenset({'hittingRating', 'baserunningRating',
               'defenseRating', 'pitchingRating'})
}

EPS = 1e-10

team_rosters = pd.read_csv('data/team_rosters.csv')
modifications = pd.read_csv('data/modifications.csv', index_col='modification')
prev_for_player = {}


def get_keys_changed(before: Optional[dict], after: dict) -> Set[str]:
    if before is None:
        return set(after.keys())

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

    # This is to have the game in the local variable for debugging
    game = get_game(after['entityId'], after['validFrom'])
    raise RuntimeError("Can't identify change")


def find_chron_start(before: Optional[JsonDict], after: JsonDict,
                     changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if after['validFrom'] == CHRON_START_DATE:
        assert before is None
        changed_keys.clear()  # Signal that the change is fully accounted for
        yield UnknownTimeChangeSource(ChangeSourceType.CHRON_START,
                                      keys_changed=changed_keys)


def find_traj_reset(_: Optional[JsonDict], after: JsonDict,
                    changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if 'tragicness' in changed_keys and after['data']['tragicness'] == 0.1:
        changed_keys.remove('tragicness')
        yield UnknownTimeChangeSource(ChangeSourceType.TRAJ_RESET,
                                      keys_changed=changed_keys)


def find_hits_tracker(_: Optional[JsonDict], __: JsonDict,
                      changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if hit_keys := changed_keys.intersection({'hitStreak', 'consecutiveHits'}):
        changed_keys.discard('hitStreak')
        changed_keys.discard('consecutiveHits')
        yield UnknownTimeChangeSource(ChangeSourceType.HITS_TRACKER,
                                      keys_changed=hit_keys)


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
        "order": "desc",
        "format": "json"
    }, session(None), total_count=1)[0]


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
    }, session(None), total_count=1)
    assert len(results) == 1
    return results[0]


def get_team_mods(team):
    return set(team['data']['permAttr']).union(team['data']['seasAttr'],
                                               team['data']['weekAttr'],
                                               team['data']['gameAttr'])


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


def find_new_attributes(before: JsonDict, _: JsonDict,
                        changed_keys: Set[str]) -> Iterator[ChangeSource]:
    for attr_set in NEW_ATTR_SETS:
        if (attr_set.issubset(changed_keys) and
                all(before['data'].get(key, 0) == 0 for key in attr_set)):
            [changed_keys.discard(attr) for attr in attr_set]
            yield UnknownTimeChangeSource(ChangeSourceType.ADDED_ATTRIBUTES,
                                          keys_changed=set(attr_set))


def find_precision_changed(before: JsonDict, after: JsonDict,
                           changed_keys: Set[str]) -> Iterator[ChangeSource]:
    precision_keys = {key for key in changed_keys
                      if (key in before['data'] and
                          key in after['data'] and
                          abs(before['data'][key] - after['data'][key]) < EPS)}
    if precision_keys:
        [changed_keys.discard(attr) for attr in precision_keys]
        yield UnknownTimeChangeSource(ChangeSourceType.PRECISION_CHANGE,
                                      keys_changed=precision_keys)


CHANGE_FINDERS = [
    # Order matters! Every new player gen finder should be before all other
    # finders, because the other finders don't check that before is not None
    find_chron_start,
    find_hits_tracker,
    find_weekly_mods_wear_off,
    find_weekly_mod_added,
    find_party,
    find_new_attributes,

    # I feel like this should go last (aside from traj reset) just thematically
    find_precision_changed,
    # I know a false traj reset is vanishingly unlikely, because nothing else
    # sets a value to exactly 0.1 and rolling exactly 0.1 randomly is
    # vanishingly unlikely. But it still makes me feel better to put this last.
    find_traj_reset,
]
