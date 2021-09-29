import re
from collections import defaultdict
from datetime import timedelta, datetime
from functools import partial
from typing import List, Optional, Set, Iterator

import pandas as pd
import requests_cache
from blaseball_mike.eventually import search as feed_search
from blaseball_mike.session import _SESSIONS_BY_EXPIRY
from dateutil.parser import isoparse

from Change import Change, JsonDict
from ChangeSource import ChangeSource, ChangeSourceType, \
    UnknownTimeChangeSource, GameEventChangeSource, ElectionChangeSource, \
    EndseasonChangeSource, GameEndChangeSource

# CHRON_START_DATE = '2020-09-13T19:20:00Z'
CHRON_START_DATE = '2020-07-29T08:12:22'
FEED_START_DATE = '2021-03-01T03:37:36+00:00'

NEGATIVE_ATTRS = {'tragicness', 'patheticism'}
PARTY_ATTRS = {'thwackability', 'buoyancy', 'musclitude', 'continuation',
               'indulgence', 'divinity', 'chasiness', 'omniscience',
               'shakespearianism', 'unthwackability', 'laserlikeness',
               'watchfulness', 'suppression', 'overpowerment', 'martyrdom',
               'groundFriction', 'anticapitalism', 'baseThirst', 'patheticism',
               'moxie', 'tenaciousness', 'tragicness', 'cinnamon', 'coldness',
               'ruthlessness', 'hittingRating', 'defenseRating',
               'baserunningRating', 'pitchingRating'}
# Peanut attrs are party attrs plus fingers minus cinnamon(?)
PEANUT_ATTRS = PARTY_ATTRS.union({'totalFingers'}).difference({'cinnamon'})
INTERVIEW_ATTRS = {'blood', 'coffee', 'ritual'}
IDOLBOARD_ATTRS = {'permAttr', 'seasAttr', 'deceased'}
BLOODDRAIN_HITTING_ATTR = {
    'buoyancy', 'musclitude', 'moxie', 'divinity',
    'patheticism', 'tragicness', 'martyrdom', 'thwackability',
    'hittingRating'
}
BLOODDRAIN_BASERUNNING_ATTR = {
    'continuation', 'groundFriction', 'laserlikeness',
    'baseThirst', 'indulgence', 'baserunningRating'
}
BLOODDRAIN_PITCHING_ATTR = {
    'totalFingers', 'coldness', 'shakespearianism',
    'unthwackability', 'overpowerment', 'suppression',
    'ruthlessness', 'pitchingRating'
}
BLOODDRAIN_DEFENSE_ATTR = {'watchfulness', 'tenaciousness', 'omniscience',
                           'anticapitalism', 'chasiness', 'defenseRating'}

# Set of sets of attributes that were added at once
NEW_ATTR_SETS = [
    # Season 2 elections
    {'cinnamon', 'bat', 'fate', 'peanutAllergy'},
    # Behind-the-scenes
    {'hittingRating', 'baserunningRating', 'defenseRating', 'pitchingRating'},
    # Interviews elections
    {'armor', 'coffee', 'ritual', 'blood'},
    # Behind-the-scenes
    {'seasAttr', 'permAttr', 'gameAttr', 'weekAttr'},
    # Dead players who then play get cinnamon and fate together without bat and
    # peanutAllergy, for some reason
    {'cinnamon', 'fate'},
    # Before Coffee Cup
    {'tournamentTeamId', 'leagueTeamId'},
    {'leagueTeamId'},
    {'tournamentTeamId'},
    # Before season 12
    {'eDensity', 'state', 'evolution'}
]

# These elections will be handled manually once I figure out the election format
DISCIPLINE_ELECTION_TIMES = {
    # season: (start time, end time)
    1: ('2020-08-02T19:09:05', '2020-08-02T19:09:08'),
    2: ('2020-08-09T19:27:41', '2020-08-09T19:27:47'),
    3: ('2020-08-30T19:18:18', '2020-08-30T19:18:28'),
    4: ('2020-09-06T19:06:11', '2020-09-06T19:06:21'),
    5: ('2020-09-13T19:20:00', '2020-09-13T19:25:00'),
    # I think this election is effectively extra long because there was a mis-
    # fired blessing that needed to be fixed manually?
    6: ('2020-09-20T19:20:01', '2020-09-21T07:46:00'),
    7: ('2020-09-27T19:02:00', '2020-09-27T19:12:00'),
    8: ('2020-10-11T19:02:00', '2020-10-11T19:12:00'),
    # Did this one also need to be fixed manually?
    9: ('2020-10-18T19:00:13', '2020-10-18T21:51:00'),
}

DISCIPLINE_ENDSEASON_TIMES = {
    5: ('2020-09-11T19:05:00', '2020-09-11T19:05:10'),
    6: ('2020-09-18T21:29:18', '2020-09-18T21:35:12'),
    7: ('2020-09-25T19:15:33', '2020-09-25T19:25:33'),
    8: ('2020-10-09T19:00:00', '2020-10-09T19:10:00'),
    9: ('2020-10-16T20:00:00', '2020-10-16T20:10:00'),
    10: ('2020-10-23T19:36:08', '2020-10-23T19:38:08'),
}

EPS = 1e-10

session = requests_cache.CachedSession("blaseball-player-changes",
                                       backend="sqlite", expire_after=None)
_SESSIONS_BY_EXPIRY[None] = session

team_rosters = pd.read_csv('data/team_rosters.csv')
modifications = pd.read_csv('data/modifications.csv', index_col='modification')
prev_for_player = {}
creeping_peanut = {}
delayed_updates = defaultdict(lambda: set())

discipline_incinerations = pd.read_csv('data/discipline_incinerations.csv')
discipline_peanuts = pd.read_csv('data/discipline_peanuts.csv')
discipline_feedbacks = pd.read_csv('data/discipline_feedbacks.csv')
discipline_blooddrains = pd.read_csv('data/discipline_blooddrains.csv')
discipline_beans = pd.read_csv('data/discipline_beans.csv')
discipline_week_ends = pd.read_csv('data/discipline_week_ends.csv')
discipline_unshellings = pd.read_csv('data/discipline_unshellings.csv')
discipline_parties = pd.read_csv('data/discipline_parties.csv')
discipline_flame_eatings = pd.read_csv('data/discipline_flame_eatings.csv')
discipline_magmatic_hits = pd.read_csv('data/discipline_magmatic_hits.csv')
coffee_cup_coffee_beans = pd.read_csv('data/coffee_cup_coffee_beans.csv')
coffee_cup_percolations = pd.read_csv('data/coffee_cup_percolations.csv')
coffee_cup_refill_gained = pd.read_csv('data/coffee_cup_free_refill_gained.csv')
coffee_cup_refill_used = pd.read_csv('data/coffee_cup_free_refill_used.csv')
coffee_cup_gain_triple_threat = \
    pd.read_csv('data/coffee_cup_gain_triple_threat.csv')
coffee_cup_lose_triple_threat = \
    pd.read_csv('data/coffee_cup_lose_triple_threat.csv')

GET_EVENTS_CACHE = {}

SIPHON_BLOODDRAIN_RE = re.compile(r"ability to (?:add|remove) a")

DAY_X_FEEDBACKS = {
    # player update time: game event time
    '2020-10-18T00:44:00.159765Z': '2020-10-18T00:42:51.585707Z',
    '2020-10-18T00:44:02.534892Z': '2020-10-18T00:42:51.585707Z',
    '2020-10-18T00:48:00.118996Z': '2020-10-18T00:46:43.645184Z',
    '2020-10-18T00:48:00.505057Z': '2020-10-18T00:46:43.645184Z',
    '2020-10-18T00:50:00.574842Z': '2020-10-18T00:47:23.644101Z',
    '2020-10-18T00:52:00.127649Z': '2020-10-18T00:51:51.640298Z',
    '2020-10-18T00:52:00.495937Z': '2020-10-18T00:51:51.640298Z',
    '2020-10-18T00:56:00.106913Z': '2020-10-18T00:55:11.653861Z',
    '2020-10-18T00:56:00.481277Z': '2020-10-18T00:55:11.653861Z',
    '2020-10-18T00:58:00.114253Z': '2020-10-18T00:56:27.651097Z',
    '2020-10-18T00:58:00.489111Z': '2020-10-18T00:56:27.651097Z',
    '2020-10-18T01:00:00.215413Z': '2020-10-18T00:58:09.215Z',
    '2020-10-18T01:00:00.76539Z': '2020-10-18T00:58:09.215Z',
    '2020-10-18T01:04:00.120728Z': '2020-10-18T01:03:39.625145Z',
    '2020-10-18T01:04:00.486806Z': '2020-10-18T01:03:39.625145Z',
}


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

            # If a delayed update is accounted for, remove it from the list
            delayed_updates[after['entityId']].difference_update(
                source.keys_changed)

        if not changed_keys:
            return Change(before, after, sources)

    return Change(before, after, [
        UnknownTimeChangeSource(ChangeSourceType.UNKNOWN, changed_keys)
    ])
    raise RuntimeError("Can't identify change")


def get_events_from_record(data: pd.DataFrame, before: dict,
                           id_column: Optional[str] = 'player_id') \
        -> pd.DataFrame:
    return get_events(data,
                      player_id=before['entityId'],
                      before_time=before['validFrom'].replace('T', ' '),
                      after_time=before['validTo'].replace('T', ' '),
                      id_column=id_column)


# noinspection PyUnusedLocal
def get_events(data: pd.DataFrame, player_id: str, before_time: str,
               after_time: str, id_column: Optional[str] = 'player_id') \
        -> pd.DataFrame:
    if id_column is None:
        return data[(data['perceived_at'] >= before_time) &
                    (data['perceived_at'] <= after_time)]

    cache_key = (id(data), id_column)
    try:
        cached_frame = GET_EVENTS_CACHE[cache_key]
    except KeyError:
        cached_frame = {
            k: data.iloc[idx].set_index('perceived_at', drop=False)
            for k, idx in data.groupby(id_column, as_index=True).groups.items()}
        GET_EVENTS_CACHE[cache_key] = cached_frame

    try:
        player_data = cached_frame[player_id]
    except KeyError:
        return data.iloc[0:0]  # Empty data frame
    return player_data[before_time:after_time]


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

    # Nandy Fantastic's First Born mod was manually added
    if (after['entityId'] == 'ec68845f-3b26-412f-8446-4fef34e09c77' and
            after['validFrom'] == '2020-09-15T23:45:01.04Z'):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'permAttr'})

    # When Sexton was Shelled they, presumably accidentally, lost the Alternate
    # mod. This change put it back.
    if (after['entityId'] == '0bb35615-63f2-4492-80ec-b6b322dc5450' and
            after['validFrom'] == '2020-09-19T13:30:00.653Z'):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'permAttr'})

    # They let inky throw the blagonball
    if (after['entityId'] == 'b6aa8ce8-2587-4627-83c1-2a48d44afaee' and
            after['validFrom'] == '2020-09-20T06:55:00.646Z'):
        changed_keys.remove('bat')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'bat'})

    # It looks like Jaylen's second debt change, from the flickering kind to
    # the repeating kind, was manual
    if (after['entityId'] == '04e14d7b-5021-4250-a3cd-932ba8e0a889' and
            after['validFrom'] == '2020-10-05T06:18:00.276597Z'):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'permAttr'})

    # It looks like Jaylen's debt being cured was manual too? TODO Check this
    if (after['entityId'] == '04e14d7b-5021-4250-a3cd-932ba8e0a889' and
            after['validFrom'] == '2020-10-12T05:14:00.370476Z'):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'permAttr'})

    # Changing Pudge's last name for cultural sensitivity I think
    if (after['entityId'] == 'cc11963b-a05b-477b-b154-911dc31960df' and
            after['validFrom'] == '2020-10-18T21:52:14.633174Z'):
        changed_keys.remove('name')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'name'})

    # Changing Lotus's last name for cultural sensitivity I think
    if (after['entityId'] == '9f218ed1-d793-437d-a1b9-79f88f69154d' and
            after['validFrom'] == '2020-10-19T01:34:00.125772Z'):
        changed_keys.remove('name')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'name'})

    # Renaming the Ulrachers because the namesake is shitty
    if (after['entityId'] == 'ad8d15f4-e041-4a12-a10e-901e6285fdc5' and
            after['validFrom'] == '2020-10-19T19:36:00.432991Z'):
        changed_keys.remove('name')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'name'})
    if (after['entityId'] == 'cbd19e6f-3d08-4734-b23f-585330028665' and
            after['validFrom'] == '2020-10-19T19:36:00.721737Z'):
        changed_keys.remove('name')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'name'})

    # Tommy Drac's and Beans McBlase's trial settlement, except they gave it
    # to the wrong McBlase (Evelton)
    if ((after['entityId'] == '4b3e8e9b-6de1-4840-8751-b1fb45dc5605' or
         after['entityId'] == 'a5f8ce83-02b2-498c-9e48-533a1d81aebf') and
            after['validFrom'] == '2020-11-16T17:15:00.861234Z'):
        changed_keys.remove('laserlikeness')
        changed_keys.remove('baseThirst')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'laserlikeness',
                                                    'baseThirst'})
        delayed_updates[after['entityId']].add('baserunningRating')

    # I'm defining the coffee cup births as manual-ish
    if (before is None and
            '2020-11-16T17:22:43' < after['validFrom'] < '2020-11-17T07:30:02'):
        changed_keys_copy = changed_keys.copy()
        changed_keys.clear()
        yield UnknownTimeChangeSource(ChangeSourceType.COFFEE_CUP_BIRTH,
                                      keys_changed=changed_keys_copy)

    # Beans McBlase's actual trial settlement this time
    if (after['entityId'] == 'dddb6485-0527-4523-9bec-324a5b66bf37' and
            after['validFrom'] == '2020-11-17T03:15:00.925108Z'):
        changed_keys.remove('laserlikeness')
        changed_keys.remove('baseThirst')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'laserlikeness',
                                                    'baseThirst'})
        delayed_updates[after['entityId']].add('baserunningRating')

    # Changeup Liu given Observed mod. All other Real Game Band players were
    # given the mod at generation
    if (after['entityId'] == '82d5e79d-e125-4460-b1fa-d046ad7739e0' and
            after['validFrom'] == '2020-11-17T18:00:01.271342Z'):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'permAttr'})

    if before is None:
        return

    new_mods = (set(after['data'].get('permAttr', [])) -
                set(before['data'].get('permAttr', [])))

    # Coffee Cup special players given NON_IDOLIZED
    if (after['validFrom'].startswith('2021-03-01T04:15:') and
            new_mods == {'NON_IDOLIZED'}):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.MANUAL,
                                      keys_changed={'permAttr'})


def find_chron_start(before: Optional[JsonDict], after: JsonDict,
                     changed_keys: Set[str]) -> Iterator[ChangeSource]:
    # Use startswith because chron proper includes milliseconds but VCR doesn't
    if after['validFrom'].startswith(CHRON_START_DATE):
        assert before is None
        changed_keys_copy = changed_keys.copy()
        changed_keys.clear()  # Signal that the change is fully accounted for
        yield UnknownTimeChangeSource(ChangeSourceType.CHRON_START,
                                      keys_changed=changed_keys_copy)


def find_rename_attribute(_: Optional[JsonDict], __: JsonDict,
                          changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if {'id', '_id'}.issubset(changed_keys):
        changed_keys.remove('id')
        changed_keys.remove('_id')
        yield UnknownTimeChangeSource(ChangeSourceType.RENAMED_ATTRIBUTES,
                                      keys_changed={'id', '_id'})


def find_change_attribute_format(before: Optional[JsonDict], after: JsonDict,
                                 changed_keys: Set[str]) -> \
        Iterator[ChangeSource]:
    if before is None:
        return

        # Changed bat attribute from the bat name to a bat id
    if 'bat' in changed_keys and (
            after['validFrom'] == '2020-08-30T07:25:59.724Z' or
            after['validFrom'] == '2020-08-30T07:26:00.713Z' or
            # they forgot about axel...
            after['validFrom'] == '2020-08-30T20:18:56.326Z'):
        changed_keys.remove('bat')
        yield UnknownTimeChangeSource(ChangeSourceType.CHANGED_ATTRIBUTE_FORMAT,
                                      keys_changed={'bat'})

    # When the Hall opened(?) a bunch of players' bats changed from '' to None
    if ('bat' in changed_keys and 'bat' in before['data'] and
            before['data']['bat'] == '' and after['data']['bat'] is None):
        changed_keys.remove('bat')
        yield UnknownTimeChangeSource(ChangeSourceType.CHANGED_ATTRIBUTE_FORMAT,
                                      keys_changed={'bat'})

    # Players from the above who became Hall Stars then had their armor and bats
    # changed to ''
    keys = {key for key in ('armor', 'bat')
            if key in changed_keys and key in before['data'] and
            before['data'][key] is None and after['data'][key] == ''}
    if keys:
        changed_keys.difference_update(keys)
        yield UnknownTimeChangeSource(ChangeSourceType.CHANGED_ATTRIBUTE_FORMAT,
                                      keys_changed=keys)


def find_traj_reset(_: Optional[JsonDict], after: JsonDict,
                    changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if 'tragicness' in changed_keys and (after['data']['tragicness'] == 0 or
                                         after['data']['tragicness'] == 0.1):
        keys = changed_keys.intersection({'tragicness', 'hittingRating'})
        changed_keys.difference_update(keys)
        yield UnknownTimeChangeSource(ChangeSourceType.TRAJ_RESET,
                                      keys_changed=keys)


def find_attributes_capped(before: JsonDict, after: JsonDict,
                           changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if before is None:
        return

    capped_keys = {k for k in changed_keys
                   if (after['data'][k] == 0.01 and before['data'][k] < 0.01)
                   or (after['data'][k] == 0.001 and before['data'][k] < 0.001)
                   or (k in NEGATIVE_ATTRS and
                       after['data'][k] == 0.99 and before['data'][k] > 0.99)
                   or (k in NEGATIVE_ATTRS and
                       after['data'][k] == 0.999 and before['data'][k] > 0.999)}
    if capped_keys:
        changed_keys.difference_update(capped_keys)
        yield UnknownTimeChangeSource(ChangeSourceType.ATTRIBUTES_CAPPED,
                                      keys_changed=capped_keys)


def approximately_equal(a, b):
    try:
        return abs(a - b) < EPS
    except TypeError:
        return a == b


def find_precision_changed(before: JsonDict, after: JsonDict,
                           changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if before is None:
        return

    precision_keys = {key for key in changed_keys
                      if (key in before['data'] and
                          key in after['data'] and
                          approximately_equal(after['data'][key],
                                              before['data'][key]))}
    if precision_keys:
        [changed_keys.discard(attr) for attr in precision_keys]
        yield UnknownTimeChangeSource(ChangeSourceType.PRECISION_CHANGE,
                                      keys_changed=precision_keys)


def find_hits_tracker(before: Optional[JsonDict], __: JsonDict,
                      changed_keys: Set[str]) -> Iterator[ChangeSource]:
    # Prevent this from showing up for new births
    if before is None:
        return

    if hit_keys := changed_keys.intersection({'hitStreak', 'consecutiveHits'}):
        changed_keys.difference_update(hit_keys)
        yield UnknownTimeChangeSource(ChangeSourceType.HITS_TRACKER,
                                      keys_changed=hit_keys)


def find_new_attributes(before: Optional[JsonDict], _: JsonDict,
                        changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if before is None:
        return

    for attr_set in NEW_ATTR_SETS:
        if (attr_set.issubset(changed_keys) and
                all(not before['data'].get(key, None) for key in attr_set)):
            [changed_keys.discard(attr) for attr in attr_set]
            yield UnknownTimeChangeSource(ChangeSourceType.ADDED_ATTRIBUTES,
                                          keys_changed=set(attr_set))


def find_creeping_peanuts(before: Optional[JsonDict], after: JsonDict,
                          changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if before is None:
        return

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


def find_interview(before: Optional[JsonDict], after: JsonDict,
                   changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if before is None:
        return

    # It's an interview if you didn't have one of these values before, whether
    # it was missing or falsy, and you do have it now
    interview_changed_keys = {
        attr for attr in INTERVIEW_ATTRS
        if attr in changed_keys and not before['data'][attr]
    }
    if interview_changed_keys:
        changed_keys.difference_update(interview_changed_keys)
        yield UnknownTimeChangeSource(ChangeSourceType.INTERVIEW,
                                      keys_changed=interview_changed_keys)


def time_str(timestamp: datetime):
    return timestamp.isoformat().replace('+00:00', 'Z')


def find_from_feed(before: JsonDict, after: JsonDict,
                   changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if after['validFrom'] < FEED_START_DATE:
        return

    timestamp = isoparse(after['validFrom'])
    events = feed_search(cache_time=None, limit=-1, query={
        'playerTags': after['entityId'],
        'before': time_str(timestamp - timedelta(seconds=180)),
        'after': time_str(timestamp + timedelta(seconds=180)),
    })
    for event in events:
        pass
        yield None


def find_discipline_election(_: JsonDict, after: JsonDict,
                             changed_keys: Set[str]) -> Iterator[ChangeSource]:
    for season, (start_time, end_time) in DISCIPLINE_ELECTION_TIMES.items():
        if start_time <= after['validFrom'] <= end_time:
            changed_keys_copy = changed_keys.copy()
            changed_keys.clear()
            yield ElectionChangeSource(ChangeSourceType.PRE_FEED_ELECTION,
                                       keys_changed=changed_keys_copy,
                                       season=season)


def find_discipline_postseason_birth(before: Optional[JsonDict],
                                     after: JsonDict,
                                     changed_keys: Set[str]) -> \
        Iterator[ChangeSource]:
    if before is not None:
        return

    for season, (start_time, end_time) in DISCIPLINE_ENDSEASON_TIMES.items():
        if start_time <= after['validFrom'] <= end_time:
            changed_keys_copy = changed_keys.copy()
            changed_keys.clear()
            yield EndseasonChangeSource(ChangeSourceType.POSTSEASON_BIRTH,
                                        keys_changed=changed_keys_copy,
                                        season=season)


def find_discipline_idolboard_mod(_: Optional[JsonDict], after: JsonDict,
                                  changed_keys: Set[str]) -> \
        Iterator[ChangeSource]:
    idolboard_changed_keys = changed_keys.intersection(IDOLBOARD_ATTRS)
    if not idolboard_changed_keys:
        return

    for season, (start_time, end_time) in DISCIPLINE_ENDSEASON_TIMES.items():
        if start_time <= after['validFrom'] <= end_time:
            changed_keys.difference_update(idolboard_changed_keys)
            yield EndseasonChangeSource(ChangeSourceType.IDOLBOARD_MOD,
                                        keys_changed=idolboard_changed_keys,
                                        season=season)


def find_discipline_rare_events(before: Optional[JsonDict], after: JsonDict,
                                changed_keys: Set[str]) -> \
        Iterator[ChangeSource]:
    # These events all happened once or twice so a csv would be overkill

    # Birth of Pitching Machine
    if (after['entityId'] == 'de21c97e-f575-43b7-8be7-ecc5d8c4eaff' and
            after['validFrom'] == '2020-09-21T16:00:00.646Z'):
        changed_keys_copy = changed_keys.copy()
        changed_keys.clear()
        yield UnknownTimeChangeSource(ChangeSourceType.PITCHING_MACHINE_CREATED,
                                      keys_changed=changed_keys_copy)

    # Birth of Electric Kettle
    # Yeah, the coffee cup is not discipline era, but I want to be able to
    # assume before is not None for the rest of the finders
    if (after['entityId'] == 'a11242ae-936a-46b4-9101-be2cabafeed4' and
            after['validFrom'] == '2020-11-18T00:10:00.235845Z'):
        changed_keys_copy = changed_keys.copy()
        changed_keys.clear()
        # Making a stand here: Electric Kettle is a type of Pitching Machine
        yield UnknownTimeChangeSource(ChangeSourceType.PITCHING_MACHINE_CREATED,
                                      keys_changed=changed_keys_copy)

    # Everything after this point can assume before is not None
    if before is None:
        return

    # Don Mitchell getting Reverberating
    if (after['entityId'] == '11de4da3-8208-43ff-a1ff-0b3480a0fbf1' and
            after['validFrom'] == '2020-09-02T12:21:59.99Z'):
        changed_keys.remove('permAttr')
        yield GameEventChangeSource(ChangeSourceType.REVERBERATING_BESTOWED,
                                    keys_changed={'permAttr'},
                                    season=4, day=44,
                                    game='77d309a0-2c3f-4f86-be07-144bebe0887a',
                                    perceived_at='2020-09-02 12:08:07.892')

    # The only Soundproof in the discipline era
    if (after['entityId'] == '41949d4d-b151-4f46-8bf7-73119a48fac8' and
            after['validFrom'] == '2020-09-11T10:20:00.915Z'):
        changed_keys.remove('ruthlessness')
        yield GameEventChangeSource(ChangeSourceType.FEEDBACK_SOUNDPROOF,
                                    keys_changed={'ruthlessness'},
                                    season=5, day=90,
                                    game='71a34115-95a9-432f-b837-1c3d3d9ab4d9',
                                    perceived_at='2020-09-11 10:18:58.355')
    if (after['entityId'] == 'e6114fd4-a11d-4f6c-b823-65691bb2d288' and
            after['validFrom'] == '2020-09-11T10:20:00.915Z'):
        # Soundproof seems to affect the same attrs as peanuts
        soundproof_changed_keys = changed_keys.intersection(PEANUT_ATTRS)
        changed_keys.difference_update(soundproof_changed_keys)
        yield GameEventChangeSource(ChangeSourceType.FEEDBACK_SOUNDPROOF,
                                    keys_changed=soundproof_changed_keys,
                                    season=5, day=90,
                                    game='71a34115-95a9-432f-b837-1c3d3d9ab4d9',
                                    perceived_at='2020-09-11 10:18:58.355')

    # Jaylen came back from the dead and had a weird combo of attributes to add
    if (after['entityId'] == '04e14d7b-5021-4250-a3cd-932ba8e0a889' and
            after['validFrom'] == '2020-09-13T20:20:00.669Z'):
        changed_keys.difference_update({'ritual', 'peanutAllergy'})
        yield UnknownTimeChangeSource(ChangeSourceType.ADDED_ATTRIBUTES,
                                      keys_changed={'ritual', 'peanutAllergy'})

    # Failed incineration attempt on Beck Whitney resulted in them gaining stars
    if (after['entityId'] == '7a75d626-d4fd-474f-a862-473138d8c376' and
            after['validFrom'] == '2020-09-25T13:40:00.349Z'):
        changed_keys_copy = changed_keys.copy()
        changed_keys.clear()
        yield UnknownTimeChangeSource(ChangeSourceType.FAILED_INCINERATION,
                                      keys_changed=changed_keys_copy)

    # Receivers talked through their rituals for a bit
    if ('ritual' in changed_keys and 'permAttr' in after['data']
            and 'RECEIVER' in after['data']['permAttr']):
        changed_keys.remove('ritual')
        yield UnknownTimeChangeSource(ChangeSourceType.RECEIVER_RITUALS,
                                      keys_changed={'ritual'})

    # Did they maybe have to add more players to the hall or something?
    if after['validFrom'] == '2020-09-25T19:24:35.227636Z':
        changed_keys_copy = changed_keys.copy()
        changed_keys.clear()
        # TODO what
        yield UnknownTimeChangeSource(ChangeSourceType.UNKNOWN,
                                      keys_changed=changed_keys_copy)

    # Giant peanut shelled Wyatt Quitter
    if (after['entityId'] == '5ca7e854-dc00-4955-9235-d7fcd732ddcf' and
            after['validFrom'] == '2020-10-07T13:22:00.690582Z'):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.GIANT_PEANUT_SHELLING,
                                      keys_changed={'permAttr'})

    # Formation of the Pods
    new_mods = (set(after['data'].get('permAttr', [])) -
                set(before['data'].get('permAttr', [])))
    if (after['validFrom'] == '2020-10-11T02:24:00.397298Z' and
            new_mods == {'HONEY_ROASTED'}):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.JOINED_PODS,
                                      keys_changed={'permAttr'})

    # Shoe Thieves/Crabs being cursed by god
    if ((after['validFrom'] == '2020-10-18T01:08:00.640182Z' or
         after['validFrom'] == '2020-10-11T02:48:00.112997Z') and
            (new_mods == {'FLINCH'} or new_mods == {'WILD'})):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.CURSED_BY_GOD,
                                      keys_changed={'permAttr'})

    # Formation of the Hall Stars (and Axel joining later) (and Scrap also)
    if ((new_mods == {'SQUIDDISH'} or 'deceased' in changed_keys) and
            (after['validFrom'] == '2020-10-18T00:38:01.824262Z' or  # others
             after['validFrom'] == '2020-10-18T00:38:02.096209Z' or  # jaylen
             after['validFrom'] == '2020-10-18T00:48:00.505057Z' or  # axel
             after['validFrom'] == '2020-10-18T00:50:00.364837Z')):  # scrap
        keys = changed_keys.intersection({'permAttr', 'deceased'})
        changed_keys.difference_update(keys)
        yield UnknownTimeChangeSource(ChangeSourceType.JOINED_HALL_STARS,
                                      keys_changed=keys)

    # Seasonal player mods wore off. If this happened more than once I will
    # come back and write it properly
    if ((after['entityId'] == '4b3e8e9b-6de1-4840-8751-b1fb45dc5605' and
         after['validFrom'] == '2020-10-18T01:08:00.34729Z') or
            (after['entityId'] == 'f0bcf4bb-74b3-412e-a54c-04c12ad28ecb' and
             after['validFrom'] == '2020-10-18T01:08:00.500529Z')):
        changed_keys.remove('seasAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.SEASONAL_MODS_WEAR_OFF,
                                      keys_changed={'seasAttr'})

    # Hall Stars Released
    if (new_mods == {'RETIRED'} and
            after['validFrom'] == '2020-10-18T19:00:01.291576Z'):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.HALL_STARS_RELEASED,
                                      keys_changed={'permAttr'})

    # Coffee Cup winners got Perk
    if (new_mods == {'PERK'} and
            after['validFrom'].startswith('2020-12-09T00:28:00')):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.WON_TOURNAMENT,
                                      keys_changed={'permAttr'})


def find_discipline_incin_replacement(before: Optional[JsonDict],
                                      after: JsonDict, changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if before is not None:
        return

    possible_incins = get_events(discipline_incinerations,
                                 player_id=after['entityId'],
                                 before_time='0000-00-00 00:00:00.000Z',
                                 after_time=after['validTo'].replace('T', ' '),
                                 id_column='replacement_id')

    if len(possible_incins) == 1:
        incin = possible_incins.iloc[0]
        changed_keys_copy = changed_keys.copy()
        changed_keys.clear()
        yield GameEventChangeSource(ChangeSourceType.INCINERATION_REPLACEMENT,
                                    keys_changed=changed_keys_copy,
                                    season=incin['season'],
                                    day=incin['day'],
                                    game=incin['game_id'],
                                    perceived_at=incin['perceived_at'])
    else:
        assert len(possible_incins) == 0


def find_discipline_incin_victim(before: JsonDict, _: JsonDict,
                                 changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if 'deceased' not in changed_keys:
        return

    possible_incins = get_events_from_record(discipline_incinerations, before,
                                             id_column='victim_id')

    if len(possible_incins) == 1:
        incin = possible_incins.iloc[0]
        changed_keys.remove('deceased')
        yield GameEventChangeSource(ChangeSourceType.INCINERATED,
                                    keys_changed={'deceased'},
                                    season=incin['season'],
                                    day=incin['day'],
                                    game=incin['game_id'],
                                    perceived_at=incin['perceived_at'])
    else:
        assert len(possible_incins) == 0


def find_discipline_simple_event(event_type: ChangeSourceType,
                                 event_attrs: Set[str],
                                 event_data: pd.DataFrame,
                                 before: Optional[JsonDict],
                                 _: JsonDict,
                                 changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if event_changed_keys := changed_keys.intersection(event_attrs):
        possible_events = get_events_from_record(event_data, before)

        for _, event in possible_events.iterrows():
            # This only needs to run 0 or 1 times but it doesn't really matter
            # if it very occasionally runs 2 times
            changed_keys.difference_update(event_changed_keys)

            yield GameEventChangeSource(event_type,
                                        keys_changed=event_changed_keys,
                                        season=event['season'],
                                        day=event['day'],
                                        game=event['game_id'],
                                        perceived_at=event['perceived_at'])


# noinspection PyUnusedLocal
def find_discipline_feedback_fate(before: Optional[JsonDict], after: JsonDict,
                                  changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if not (before is not None and 'fate' in changed_keys):
        return

    if ((after['entityId'] == '04e14d7b-5021-4250-a3cd-932ba8e0a889' or
         after['entityId'] == '3af96a6b-866c-4b03-bc14-090acf6ecee5') and
            after['validFrom'] in DAY_X_FEEDBACKS):
        changed_keys.remove('fate')
        yield GameEventChangeSource(
            ChangeSourceType.FEEDBACK_FATE, keys_changed={'fate'},
            season=9, day=-1, game='9bb560d9-4925-4845-ad03-26012742ee23',
            perceived_at=DAY_X_FEEDBACKS[after['validFrom']])
        return

    player_id = after['entityId']
    before_time = before['validFrom'].replace('T', ' ')
    after_time = after['validFrom'].replace('T', ' ')
    possible_feedbacks = discipline_feedbacks.query(
        '(player_id==@player_id or player_id_2==@player_id) and '
        'perceived_at>=@before_time and perceived_at<=@after_time')
    # There actually have been 2 feedbacks in one chron update (Flickering
    # Eugenia Garbage). Assume it's the later one that caused the change.
    if len(possible_feedbacks) > 0:
        changed_keys.discard('fate')
        feedback = possible_feedbacks.iloc[-1]
        yield GameEventChangeSource(ChangeSourceType.FEEDBACK_FATE,
                                    keys_changed={'fate'},
                                    season=feedback['season'],
                                    day=feedback['day'],
                                    game=feedback['game_id'],
                                    perceived_at=feedback['perceived_at'])


def find_discipline_blooddrain(before: Optional[JsonDict], after: JsonDict,
                               changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if before is None:
        return

    for id_column in ('drainer_id', 'drained_id'):
        possible_blooddrains = get_events_from_record(discipline_blooddrains,
                                                      before, id_column)
        for _, blooddrain in possible_blooddrains.iterrows():
            # The draining player doesn't get any stat change from blooddrains
            # that just add an out, strike, etc.
            if (id_column == 'drainer_id' and
                    SIPHON_BLOODDRAIN_RE.search(blooddrain['evt'])):
                continue

            if "hitting ability" in blooddrain['evt']:
                expected_keys = BLOODDRAIN_HITTING_ATTR
            elif "baserunning ability" in blooddrain['evt']:
                expected_keys = BLOODDRAIN_BASERUNNING_ATTR
            elif "pitching ability" in blooddrain['evt']:
                expected_keys = BLOODDRAIN_PITCHING_ATTR
            elif "defensive ability" in blooddrain['evt']:
                expected_keys = BLOODDRAIN_DEFENSE_ATTR
            else:
                assert False
            if drain_changed_keys := changed_keys.intersection(expected_keys):
                changed_keys.difference_update(drain_changed_keys)
                yield GameEventChangeSource(
                    ChangeSourceType.BLOODDRAIN,
                    keys_changed=drain_changed_keys,
                    season=blooddrain['season'],
                    day=blooddrain['day'],
                    game=blooddrain['game_id'],
                    perceived_at=blooddrain['perceived_at'])


def find_discipline_weekly_mod_change(before: JsonDict, after: JsonDict,
                                      changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if 'weekAttr' not in changed_keys:
        return

    # See if the mods wore off
    if len(after['data']['weekAttr']) == 0:
        # Then the mod(s) was/were removed. Find the week end
        possible_week_ends = get_events_from_record(discipline_week_ends,
                                                    before, id_column=None)

        if len(possible_week_ends) == 1:
            week_end = possible_week_ends.iloc[0]
            changed_keys.remove('weekAttr')
            yield GameEndChangeSource(ChangeSourceType.WEEKLY_MODS_WEAR_OFF,
                                      keys_changed={'weekAttr'},
                                      season=week_end['season'],
                                      day=week_end['day'])
            return
        else:
            assert len(possible_week_ends) == 0

    # Find sources of added mods
    new_mods = set(after['data']['weekAttr']) - set(before['data']['weekAttr'])
    for mod_added in new_mods:
        mod_name = modifications.loc[mod_added, 'title']

        # Look for hit-by-pitch
        possible_beans = get_events_from_record(discipline_beans, before)
        possible_beans = possible_beans[
            possible_beans['evt'].str.contains(
                f"hits {before['data']['name']} with a pitch! "
                f"{before['data']['name']} is now {mod_name}!")
        ]

        if len(possible_beans) == 1:
            bean = possible_beans.iloc[0]
            changed_keys.remove('weekAttr')
            yield GameEventChangeSource(ChangeSourceType.HIT_BY_PITCH,
                                        keys_changed={'weekAttr'},
                                        season=bean['season'],
                                        day=bean['day'],
                                        game=bean['game_id'],
                                        perceived_at=bean['perceived_at'])
            continue
        else:
            # Hopefully a player doesn't get multiple HBPs in one chron update
            assert len(possible_beans) == 0

        # Look for unstable chain
        possible_chains = get_events_from_record(discipline_incinerations,
                                                 before, id_column=None)
        possible_chains = possible_chains[
            possible_chains['evt'].str.contains(
                r"The Instability (?:spreads|chains) to the [\w ]+'s " +
                before['data']['name'])
        ]
        if len(possible_chains) == 1:
            chain = possible_chains.iloc[0]
            changed_keys.remove('weekAttr')
            yield GameEventChangeSource(ChangeSourceType.UNSTABLE_CHAIN,
                                        keys_changed={'weekAttr'},
                                        season=chain['season'],
                                        day=chain['day'],
                                        game=chain['game_id'],
                                        perceived_at=chain['perceived_at'])
            continue
        else:
            # Hopefully a player doesn't get multiple HBPs in one chron update
            assert len(possible_chains) == 0

        # Loyalty mod, only procced during Day X
        if mod_added == 'SABOTEUR':
            changed_keys.remove('weekAttr')
            yield GameEventChangeSource(
                ChangeSourceType.LOYALTY, keys_changed={'weekAttr'},
                season=9, day=-1, game='9bb560d9-4925-4845-ad03-26012742ee23',
                perceived_at=DAY_X_FEEDBACKS[after['validFrom']]
            )
            continue

        # Subjection mod, ditto
        if mod_added == 'LIBERATED':
            changed_keys.remove('weekAttr')
            yield GameEventChangeSource(
                ChangeSourceType.SUBJECTION, keys_changed={'weekAttr'},
                season=9, day=-1, game='9bb560d9-4925-4845-ad03-26012742ee23',
                perceived_at=DAY_X_FEEDBACKS[after['validFrom']]
            )
            continue

        raise RuntimeError("Can't find source of weekly mod " + mod_name)


def find_discipline_unshelling(before: Optional[JsonDict], after: JsonDict,
                               changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if unshelling_changed_keys := changed_keys.intersection(
            {'peanutAllergy', 'permAttr'}):
        possible_unshellings = get_events_from_record(discipline_unshellings,
                                                      before)

        if len(possible_unshellings) == 1:
            changed_keys.discard('peanutAllergy')

            # Only mark permAttr as accounted for if superallergic was the only
            # change
            added_mods = (set(after['data']['permAttr']) -
                          set(before['data']['permAttr']))
            if not added_mods or added_mods == {'SUPERALLERGIC'}:
                changed_keys.remove('permAttr')

            unshelling = possible_unshellings.iloc[0]
            yield GameEventChangeSource(ChangeSourceType.UNSHELLING,
                                        keys_changed=unshelling_changed_keys,
                                        season=unshelling['season'],
                                        day=unshelling['day'],
                                        game=unshelling['game_id'],
                                        perceived_at=unshelling['perceived_at'])
        else:
            # 2 unshellings in one chron update? inconceivable!
            assert len(possible_unshellings) == 0


def find_discipline_spicy(before: Optional[JsonDict], after: JsonDict,
                          changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if 'permAttr' not in changed_keys:
        return

    changed_mods = set(before['data']['permAttr']).symmetric_difference(
        after['data']['permAttr'])
    if changed_mods.issubset({'HEATING_UP', 'ON_FIRE'}):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.SPICY,
                                      keys_changed={'permAttr'})


def find_discipline_magmatic(before: Optional[JsonDict], after: JsonDict,
                             changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if 'permAttr' not in changed_keys:
        return

    # The timing is actually so tight on this one magmatic hit that no
    # reasonable systematic fix will handle it. Easiest to just hard-code it.
    if (after['entityId'] == '2b157c5c-9a6a-45a6-858f-bf4cf4cbc0bd' and
            after['validFrom'] == '2020-10-14T05:22:00.640889Z'):
        changed_keys.remove('permAttr')
        yield GameEventChangeSource(ChangeSourceType.HIT_MAGMATIC_HOME_RUN,
                                    keys_changed={'permAttr'},
                                    season=9, day=36,
                                    game='abfd4b11-1e81-47be-887e-f06eba004d35',
                                    perceived_at='2020-10-14 05:21:56.113175')
        return

    mods_before = set(before['data']['permAttr'])
    mods_after = set(after['data']['permAttr'])
    if mods_after - mods_before == {'MAGMATIC'}:
        possible_flame_eatings = get_events_from_record(
            discipline_flame_eatings, before)

        if len(possible_flame_eatings) == 1:
            changed_keys.discard('permAttr')

            hit = possible_flame_eatings.iloc[0]
            yield GameEventChangeSource(ChangeSourceType.ATE_FIRE,
                                        keys_changed={'permAttr'},
                                        season=hit['season'],
                                        day=hit['day'],
                                        game=hit['game_id'],
                                        perceived_at=hit['perceived_at'])
        else:
            assert len(possible_flame_eatings) == 0
    elif mods_before - mods_after == {'MAGMATIC'}:
        possible_magmatic_hits = get_events_from_record(
            discipline_magmatic_hits, before)

        if len(possible_magmatic_hits) == 1:
            changed_keys.discard('permAttr')

            hit = possible_magmatic_hits.iloc[0]
            yield GameEventChangeSource(ChangeSourceType.HIT_MAGMATIC_HOME_RUN,
                                        keys_changed={'permAttr'},
                                        season=hit['season'],
                                        day=hit['day'],
                                        game=hit['game_id'],
                                        perceived_at=hit['perceived_at'])
        else:
            assert len(possible_magmatic_hits) == 0


def find_discipline_haunt(before: Optional[JsonDict], after: JsonDict,
                          changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if 'permAttr' not in changed_keys:
        return

    if (set(before['data']['permAttr'])
            .symmetric_difference(after['data']['permAttr']) == {'INHABITING'}):
        changed_keys.remove('permAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.INHABITING,
                                      keys_changed={'permAttr'})


missing_beans = []


def find_coffee_cup_game_mod_change(before: Optional[JsonDict], after: JsonDict,
                                    changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if 'gameAttr' not in changed_keys:
        return

    # In the coffee cup, these were the only game mods, I think
    assert set(after['data']['gameAttr']).issubset({'TIRED', 'WIRED'})
    # You could only have at most one at a time, I think
    assert len(after['data']['gameAttr']) <= 1

    possible_beans = get_events_from_record(coffee_cup_coffee_beans, before)

    if not after['data']['gameAttr']:
        # Then tired or wired was removed
        expected_str = " is no longer "
    elif after['data']['gameAttr'][0] == 'TIRED':
        # Then tired was added
        expected_str = " is now Tired"
    elif after['data']['gameAttr'][0] == 'WIRED':
        # Then wired was added
        expected_str = " is now Wired"
    else:
        # No other outcomes
        assert False
    possible_beans = possible_beans[
        possible_beans['evt'].str.contains(expected_str)]
    if len(possible_beans) > 0:
        bean = possible_beans.iloc[-1]
        changed_keys.remove('gameAttr')
        yield GameEventChangeSource(ChangeSourceType.COFFEE_BEAN,
                                    keys_changed={'gameAttr'},
                                    season=bean['season'],
                                    day=bean['day'],
                                    game=bean['game_id'],
                                    perceived_at=bean['perceived_at'])
    else:
        # There are many missing beans, and no more specific source of time
        # information that could be used to infer them manually. Just do the
        # easy(-ish) thing of finding which game it must've been in and use the
        # Chron time as perceived_at.
        # TODO Build spreadsheet of season/day/game for the missing beans once
        #   this list is complete
        missing_beans.append((after['entityId'], after['validFrom']))
        changed_keys.remove('gameAttr')
        yield UnknownTimeChangeSource(ChangeSourceType.COFFEE_BEAN,
                                      keys_changed={'gameAttr'})


def find_coffee_cup_percolation(before: Optional[JsonDict], after: JsonDict,
                                changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if 'permAttr' not in changed_keys:
        return

    attr_difference = set(before['data']['permAttr']).symmetric_difference(
        set(after['data']['permAttr']))

    # Percolation should only add the Percolated mod, and I don't have the
    # infrastructure to track multiple mods added from disparate effects
    if attr_difference != {'COFFEE_EXIT'}:
        return

    possible_percolations = \
        get_events_from_record(coffee_cup_percolations, before)

    if len(possible_percolations) > 0:
        bean = possible_percolations.iloc[-1]
        changed_keys.remove('permAttr')
        yield GameEventChangeSource(ChangeSourceType.PERCOLATION,
                                    keys_changed={'permAttr'},
                                    season=bean['season'],
                                    day=bean['day'],
                                    game=bean['game_id'],
                                    perceived_at=bean['perceived_at'])


def find_coffee_cup_free_refill(before: Optional[JsonDict], after: JsonDict,
                                changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if 'permAttr' not in changed_keys:
        return

    # Exactly one time, Sandie Turner used her free refill and gained one of the
    # Spicy-related mods in one hit. I don't have the infrastructure to track
    # two mod changes from different effects, and it happened exactly once, so I
    # hard-code it.
    if (after['entityId'] == '766dfd1e-11c3-42b6-a167-9b2d568b5dc0' and
            after['validFrom'] == '2020-12-02T00:22:00.131114Z'):
        changed_keys.remove('permAttr')
        yield GameEventChangeSource(ChangeSourceType.SPICY,
                                    keys_changed={'permAttr'},
                                    season=-1, day=12,
                                    game='4ad14ac7-667b-45e6-a3b6-6965e156704b',
                                    perceived_at='2020-12-02 00:19:50.620599')
        yield GameEventChangeSource(ChangeSourceType.FREE_REFILL,
                                    keys_changed={'permAttr'},
                                    season=-1, day=12,
                                    game='4ad14ac7-667b-45e6-a3b6-6965e156704b',
                                    perceived_at='2020-12-02 00:19:50.620599')
        return

    attr_difference = set(before['data']['permAttr']).symmetric_difference(
        set(after['data']['permAttr']))

    # Should only add/remove the COFFEE_RALLY mod
    if attr_difference != {'COFFEE_RALLY'}:
        return

    # This is a use-free-refill event, but Patty Fox got a new free refill so
    # quickly that chron delay places this event after that. It's easier to
    # hard-code one exception than to try to account for chron delay
    if (after['entityId'] == '81d7d022-19d6-427d-aafc-031fcb79b29e' and
            after['validFrom'] == '2020-11-25T01:14:00.379226Z'):
        changed_keys.remove('permAttr')
        yield GameEventChangeSource(ChangeSourceType.FREE_REFILL,
                                    keys_changed={'permAttr'},
                                    season=-1, day=8,
                                    game='fa398f5e-1ace-47d6-b449-afdf897a14c2',
                                    perceived_at='2020-11-25 01:11:59.610153')
        return
    if (after['entityId'] == 'cf8e152e-2d27-4dcc-ba2b-68127de4e6a4' and
            after['validFrom'] == '2020-11-25T02:08:00.38368Z'):
        changed_keys.remove('permAttr')
        yield GameEventChangeSource(ChangeSourceType.FREE_REFILL,
                                    keys_changed={'permAttr'},
                                    season=-1, day=9,
                                    game='1d198794-cbbc-4e7d-bb8f-8fd31a4ec055',
                                    perceived_at='2020-11-25 02:05:48.611981')
        return

    if 'COFFEE_RALLY' in after['data']['permAttr']:
        possible_events = get_events_from_record(coffee_cup_refill_gained,
                                                 before)
    else:
        possible_events = get_events_from_record(coffee_cup_refill_used,
                                                 before)

    if len(possible_events) > 0:
        event = possible_events.iloc[-1]
        changed_keys.remove('permAttr')
        yield GameEventChangeSource(ChangeSourceType.FREE_REFILL,
                                    keys_changed={'permAttr'},
                                    season=event['season'],
                                    day=event['day'],
                                    game=event['game_id'],
                                    perceived_at=event['perceived_at'])


def find_coffee_cup_triple_threat(before: Optional[JsonDict], after: JsonDict,
                                  changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if 'permAttr' not in changed_keys:
        return

    attr_difference = set(before['data']['permAttr']).symmetric_difference(
        set(after['data']['permAttr']))

    # Should only add/remove the TRIPLE_THREAT mod
    if attr_difference != {'TRIPLE_THREAT'}:
        return

    if 'TRIPLE_THREAT' in after['data']['permAttr']:
        possible_events = get_events_from_record(coffee_cup_gain_triple_threat,
                                                 before)
    else:
        possible_events = get_events_from_record(coffee_cup_lose_triple_threat,
                                                 before)

    if len(possible_events) > 0:
        event = possible_events.iloc[-1]
        changed_keys.remove('permAttr')
        yield GameEventChangeSource(ChangeSourceType.TRIPLE_THREAT,
                                    keys_changed={'permAttr'},
                                    season=event['season'],
                                    day=event['day'],
                                    game=event['game_id'],
                                    perceived_at=event['perceived_at'])


def find_delayed_star_recalculation(_: Optional[JsonDict], after: JsonDict,
                                    changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if (delayed_star_changed_keys := changed_keys.intersection(
            delayed_updates[after['entityId']])):
        changed_keys.difference_update(delayed_star_changed_keys)
        yield UnknownTimeChangeSource(
            ChangeSourceType.DELAYED_STAR_RECALCULATION,
            keys_changed=delayed_star_changed_keys)


CHANGE_FINDERS = [
    # Manual overrides
    find_manual_fixes,

    # Mechanical changes, not reflected in game
    find_chron_start,
    find_rename_attribute,
    find_change_attribute_format,

    # Mechanical attribute changes
    find_traj_reset,
    find_attributes_capped,
    find_precision_changed,

    # Tracker changes
    find_hits_tracker,

    # Changes associated with new features
    find_new_attributes,
    find_creeping_peanuts,
    find_fateless_fated,
    find_interview,

    # Feed finder handles nearly every user-visible change after discipline
    find_from_feed,

    # Discipline scheduled events
    find_discipline_election,
    find_discipline_postseason_birth,  # must be before idolboard_mod
    find_discipline_idolboard_mod,

    # Discipline in-game events
    find_discipline_rare_events,
    # Replacement first so the other finders can assume `before` is populated
    find_discipline_incin_replacement,
    find_discipline_incin_victim,
    partial(find_discipline_simple_event,
            ChangeSourceType.PEANUT, PEANUT_ATTRS, discipline_peanuts),
    find_discipline_feedback_fate,
    find_discipline_blooddrain,
    find_discipline_weekly_mod_change,
    find_discipline_unshelling,
    partial(find_discipline_simple_event,
            ChangeSourceType.PARTY, PARTY_ATTRS, discipline_parties),
    find_discipline_spicy,
    find_discipline_magmatic,
    find_discipline_haunt,

    # Coffee cup
    find_coffee_cup_game_mod_change,
    find_coffee_cup_percolation,
    find_coffee_cup_free_refill,
    find_coffee_cup_triple_threat,

    # These ones should only account for changes that aren't explained by
    # anything else
    find_delayed_star_recalculation,
]
