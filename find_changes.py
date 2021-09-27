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
    UnknownTimeChangeSource, GameEventChangeSource, ElectionChangeSource, \
    IdolBoardChangeSource, GameEndChangeSource

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
INTERVIEW_ATTRS = {'blood', 'coffee', 'ritual'}

# Set of sets of attributes that were added at once
NEW_ATTR_SETS = {
    frozenset({'cinnamon', 'bat', 'fate', 'peanutAllergy'}),
    frozenset({'hittingRating', 'baserunningRating',
               'defenseRating', 'pitchingRating'}),
    frozenset({'armor', 'coffee', 'ritual', 'blood'}),
    frozenset({'seasAttr', 'permAttr', 'gameAttr', 'weekAttr'}),
}

# These elections will be handled manually once I figure out the election format
DISCIPLINE_ELECTION_TIMES = {
    # season: (start time, end time)
    1: ('2020-08-02T19:09:05', '2020-08-02T19:09:08'),
    2: ('2020-08-09T19:27:41', '2020-08-09T19:27:47'),
    3: ('2020-08-30T19:18:18', '2020-08-30T19:18:28'),
    4: ('2020-09-06T19:06:11', '2020-09-06T19:06:21'),
    5: ('2020-09-13T19:20:00', '2020-09-13T19:25:00'),
}

DISCIPLINE_IDOLBOARD_TIMES = {
    5: ('2020-09-11T19:05:00', '2020-09-11T19:05:10'),
}

EPS = 1e-10

session = requests_cache.CachedSession("blaseball-player-changes",
                                       backend="sqlite", expire_after=None)
_SESSIONS_BY_EXPIRY[None] = session

team_rosters = pd.read_csv('data/team_rosters.csv')
modifications = pd.read_csv('data/modifications.csv', index_col='modification')
prev_for_player = {}
creeping_peanut = {}

discipline_incinerations = pd.read_csv('data/discipline_incinerations.csv')
discipline_peanuts = pd.read_csv('data/discipline_peanuts.csv')
discipline_feedbacks = pd.read_csv('data/discipline_feedbacks.csv')
discipline_blooddrains = pd.read_csv('data/discipline_blooddrains.csv')
discipline_beans = pd.read_csv('data/discipline_beans.csv')
discipline_week_ends = pd.read_csv('data/discipline_week_ends.csv')


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
    # Using python magic, these @ strings reference the function parameters
    q = 'perceived_at>=@before_time and perceived_at<=@after_time'

    if id_column is not None:
        q = f'{id_column}==@player_id and {q}'

    return data.query(q)


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


def find_hits_tracker(_: Optional[JsonDict], __: JsonDict,
                      changed_keys: Set[str]) -> Iterator[ChangeSource]:
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
                all(before['data'].get(key, 0) == 0 for key in attr_set)):
            [changed_keys.discard(attr) for attr in attr_set]
            yield UnknownTimeChangeSource(ChangeSourceType.ADDED_ATTRIBUTES,
                                          keys_changed=set(attr_set))


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


def find_interview(before: Optional[JsonDict], _: JsonDict,
                   changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if (before is not None and
            changed_keys.issubset(INTERVIEW_ATTRS) and
            ('coffee' not in changed_keys or before['data']['coffee'] == 0) and
            ('blood' not in changed_keys or before['data']['blood'] == 0) and
            ('ritual' not in changed_keys or before['data']['ritual'] == '')):
        interview_changed_keys = changed_keys.intersection(INTERVIEW_ATTRS)
        changed_keys.difference_update(interview_changed_keys)
        yield UnknownTimeChangeSource(ChangeSourceType.INTERVIEW,
                                      keys_changed=interview_changed_keys)


def find_discipline_election(_: JsonDict, after: JsonDict,
                             changed_keys: Set[str]) -> Iterator[ChangeSource]:
    for season, (start_time, end_time) in DISCIPLINE_ELECTION_TIMES.items():
        if start_time <= after['validFrom'] <= end_time:
            changed_keys_copy = changed_keys.copy()
            changed_keys.clear()
            yield ElectionChangeSource(ChangeSourceType.PRE_FEED_ELECTION,
                                       keys_changed=changed_keys_copy,
                                       season=season)


def find_discipline_idolboard_mod(_: Optional[JsonDict], after: JsonDict,
                                  changed_keys: Set[str]) -> \
        Iterator[ChangeSource]:
    if 'permAttr' not in changed_keys:
        return

    for season, (start_time, end_time) in DISCIPLINE_IDOLBOARD_TIMES.items():
        if start_time <= after['validFrom'] <= end_time:
            changed_keys.discard('permAttr')
            yield IdolBoardChangeSource(ChangeSourceType.IDOLBOARD_MOD,
                                        keys_changed={'permAttr'},
                                        season=season)


def find_discipline_rare_events(_: Optional[JsonDict], after: JsonDict,
                                changed_keys: Set[str]) -> \
        Iterator[ChangeSource]:
    # These events all happened once or twice so a csv would be overkill

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
        changed_keys_copy = changed_keys.copy()
        changed_keys.clear()
        yield GameEventChangeSource(ChangeSourceType.INCINERATED,
                                    keys_changed=changed_keys_copy,
                                    season=incin['season'],
                                    day=incin['day'],
                                    game=incin['game_id'],
                                    perceived_at=incin['perceived_at'])
    else:
        assert len(possible_incins) == 0


def find_discipline_peanut(before: Optional[JsonDict], _: JsonDict,
                           changed_keys: Set[str]) -> Iterator[ChangeSource]:
    if before is None:
        return

    if peanut_changed_keys := changed_keys.intersection(PEANUT_ATTRS):
        possible_nuts = get_events_from_record(discipline_peanuts, before)
        if len(possible_nuts) == 1:
            changed_keys.difference_update(peanut_changed_keys)
            nut = possible_nuts.iloc[0]
            yield GameEventChangeSource(ChangeSourceType.PEANUT,
                                        keys_changed=peanut_changed_keys,
                                        season=nut['season'],
                                        day=nut['day'],
                                        game=nut['game_id'],
                                        perceived_at=nut['perceived_at'])
        else:
            # 2 peanuts in one chron update? inconceivable!
            assert len(possible_nuts) == 0


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
            feedback = possible_feedbacks.iloc[0]
            yield GameEventChangeSource(ChangeSourceType.FEEDBACK_FATE,
                                        keys_changed={'fate'},
                                        season=feedback['season'],
                                        day=feedback['day'],
                                        game=feedback['game_id'],
                                        perceived_at=feedback['perceived_at'])
        else:
            # 2 feedbacks in one chron update? inconceivable!
            assert len(possible_feedbacks) == 0


# noinspection PyUnusedLocal
def find_discipline_blooddrain(before: Optional[JsonDict], after: JsonDict,
                               changed_keys: Set[str]) \
        -> Iterator[ChangeSource]:
    if before is None:
        return
    possible_blooddrains = get_events_from_record(discipline_blooddrains,
                                                  before)
    if len(possible_blooddrains) == 1:
        expected_keys = set()
        blooddrain = possible_blooddrains.iloc[0]
        if "hitting ability" in blooddrain['evt']:
            expected_keys = {
                'buoyancy', 'musclitude', 'moxie', 'divinity', 'patheticism',
                'tragicness', 'martyrdom', 'thwackability'
            }
        elif "baserunning ability" in blooddrain['evt']:
            expected_keys = {'continuation', 'groundFriction', 'laserlikeness',
                             'baseThirst', 'indulgence'}
        elif "pitching ability" in blooddrain['evt']:
            expected_keys = {
                'totalFingers', 'coldness', 'shakespearianism',
                'unthwackability', 'overpowerment', 'suppression',
                'ruthlessness'
            }
        elif "defensive ability" in blooddrain['evt']:
            expected_keys = {'watchfulness', 'tenaciousness', 'omniscience',
                             'anticapitalism', 'chasiness'}
        else:
            assert False
        if blooddrain_changed_keys := changed_keys.intersection(expected_keys):
            changed_keys.difference_update(blooddrain_changed_keys)
            yield GameEventChangeSource(ChangeSourceType.BLOODDRAIN,
                                        keys_changed=blooddrain_changed_keys,
                                        season=blooddrain['season'],
                                        day=blooddrain['day'],
                                        game=blooddrain['game_id'],
                                        perceived_at=blooddrain['perceived_at'])
    else:
        # 2 blooddrains in one chron update? inconceivable!
        assert len(possible_blooddrains) == 0


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

        raise RuntimeError("Can't find source of weekly mod " + mod_name)


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

    # Discipline scheduled events
    find_discipline_election,
    find_discipline_idolboard_mod,

    # Discipline in-game events
    find_discipline_rare_events,
    # Replacement first so the other finders can assume `before` is populated
    find_discipline_incin_replacement,
    find_discipline_incin_victim,
    find_discipline_peanut,
    find_discipline_feedback_fate,
    find_discipline_blooddrain,
    find_discipline_weekly_mod_change,

    # Feed finder handles everything(?) after discipline
    find_from_feed,
]
