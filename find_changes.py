from typing import List, Optional, Set

from blaseball_mike.chronicler import paged_get
from blaseball_mike.session import session

from Change import Change, JsonDict
from ChangeSource import ChangeSource, ChangeSourceType, \
    UnknownTimeChangeSource, GameEventChangeSource

CHRON_START_DATE = '2020-09-13T19:20:00Z'
CHRON_GAMES_URL = 'https://api.sibr.dev/chronicler/v1/games'

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
        source = change_finder(before, after, changed_keys)
        if source is not None:
            # Source should be derived from ChangeSource but not the base type
            assert isinstance(source, ChangeSource)
            assert not type(source) is ChangeSource
            sources.append(source)

        if not changed_keys:
            return Change(before, after, sources)

    raise RuntimeError("Can't identify change")


def find_chron_start(before: Optional[JsonDict], after: JsonDict,
                     changed_keys: Set[str]) -> Optional[ChangeSource]:
    if after['validFrom'] == CHRON_START_DATE:
        assert before is None
        changed_keys.clear()  # Signal that the change is fully accounted for
        return UnknownTimeChangeSource(ChangeSourceType.CHRON_START,
                                       keys_changed=changed_keys)


def find_traj_reset(_: Optional[JsonDict], after: JsonDict,
                    changed_keys: Set[str]) -> Optional[ChangeSource]:
    if 'tragicness' in changed_keys and after['data']['tragicness'] == 0.1:
        changed_keys.remove('tragicness')
        return UnknownTimeChangeSource(ChangeSourceType.TRAJ_RESET,
                                       keys_changed=changed_keys)


def find_hits_tracker(_: Optional[JsonDict], __: JsonDict,
                      changed_keys: Set[str]) -> Optional[ChangeSource]:
    if hit_keys := changed_keys.intersection({'hitStreak', 'consecutiveHits'}):
        changed_keys.discard('hitStreak')
        changed_keys.discard('consecutiveHits')
        return UnknownTimeChangeSource(ChangeSourceType.HITS_TRACKER,
                                       keys_changed=hit_keys)


def find_weekly_mods_wear_off(_: JsonDict, after: JsonDict,
                              changed_keys: Set[str]) -> Optional[ChangeSource]:
    if 'weekAttr' in changed_keys and len(after['data']['weekAttr']) == 0:
        # Is probably weekly mods wearing off. Get game day to be sure.
        prev_game = paged_get(session(None), CHRON_GAMES_URL, {
            "order": "desc",
            "count": "1",
            "before": after['validFrom'],
            "format": "json"
        })[0]
        game_data = prev_game['data'][0]['data']
        if (game_data['day'] + 1) % 9 == 0:
            changed_keys.remove('weekAttr')
            return GameEventChangeSource(ChangeSourceType.WEEKLY_MODS_WEAR_OFF,
                                         keys_changed={'weekAttr'},
                                         season=game_data['season'],
                                         day=game_data['day'])


CHANGE_FINDERS = [
    # Order matters! Every new player gen finder should be before all other
    # finders, because the other finders don't check that before is not None
    find_chron_start,
    find_hits_tracker,
    find_weekly_mods_wear_off,

    # I know a false traj reset is vanishingly unlikely, because nothing else
    # sets a value to exactly 0.1 and rolling exactly 0.1 randomly is
    # vanishingly unlikely. But it still makes me feel better to put this last.
    find_traj_reset,
]
