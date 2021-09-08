from typing import List, Optional, Set, Iterator

import pandas as pd
from blaseball_mike.chronicler import paged_get
from blaseball_mike.session import session

from Change import Change, JsonDict
from ChangeSource import ChangeSource, ChangeSourceType, \
    UnknownTimeChangeSource, GameEventChangeSource

CHRON_START_DATE = '2020-09-13T19:20:00Z'
CHRON_GAMES_URL = 'https://api.sibr.dev/chronicler/v1/games'

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
    game = get_game(after)
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


def get_player_team(after: JsonDict):
    # noinspection PyUnusedLocal
    player_id = after['entityId']
    # noinspection PyUnusedLocal
    timestamp = after['validFrom']
    result = team_rosters.query(
        'player_id==@player_id and valid_from<=@timestamp and '
        '(valid_until>@timestamp or valid_until.isnull())')
    assert len(result) == 1
    return result.iloc[0]['team_id']


def get_game(after: JsonDict):
    team_id = get_player_team(after)
    return paged_get(CHRON_GAMES_URL, {
        "team": team_id,
        "before": after['validFrom'],
        "order": "desc",
        "format": "json"
    }, session(None), total_count=1)[0]


def find_weekly_mods_wear_off(_: JsonDict, after: JsonDict,
                              changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if 'weekAttr' in changed_keys and len(after['data']['weekAttr']) == 0:
        # Is probably weekly mods wearing off. Get game day to be sure.
        game_data = get_game(after)['data']
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
        for mod_added in new_mods:
            game_data = get_game(after)['data']
            player_name = before['data']['name']
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


CHANGE_FINDERS = [
    # Order matters! Every new player gen finder should be before all other
    # finders, because the other finders don't check that before is not None
    find_chron_start,
    find_hits_tracker,
    find_weekly_mods_wear_off,
    find_weekly_mod_added,

    # I know a false traj reset is vanishingly unlikely, because nothing else
    # sets a value to exactly 0.1 and rolling exactly 0.1 randomly is
    # vanishingly unlikely. But it still makes me feel better to put this last.
    find_traj_reset,
]
