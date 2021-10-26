from collections import defaultdict
from copy import copy, deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto, IntEnum
from typing import List, Tuple, Dict, Optional, Any
from dictdiffer import diff

from blaseball_mike.chronicler import get_entities

from ChangeSource import ChangeSource
from Player import Player


class TimestampSource(Enum):
    FEED = auto()
    CHRON_PLAYER = auto()
    CHRON_GAME_EVENT = auto()
    MANUAL = auto()


class ModDuration(IntEnum):
    PERMANENT = 0
    SEASON = 1
    WEEKLY = 2
    GAME = 3
    ITEM = 4
    LEAGUE = 5


class Effect:
    def apply(self, player: Player) -> None:
        raise NotImplementedError("Don't instantiate Effect")


def _duration_attribute(duration: ModDuration) -> Optional[str]:
    if duration == ModDuration.GAME:
        return "gameAttr"
    elif duration == ModDuration.WEEKLY:
        return "weekAttr"
    elif duration == ModDuration.SEASON:
        return "seasAttr"
    elif duration == ModDuration.PERMANENT:
        return "permAttr"
    return None


@dataclass
class ModEffect(Effect):
    from_mod: Optional[str]
    to_mod: Optional[str]
    type: ModDuration

    def apply(self, player: Player) -> None:
        attribute = _duration_attribute(self.type)

        if attribute is None:
            # This signifies that this mod effect is not stored on the player
            return

        if self.from_mod is not None:
            player.data[attribute].remove(self.from_mod)
        if self.to_mod is not None:
            player.data[attribute].append(self.to_mod)


@dataclass
class SetStateEffect(Effect):
    path: List[str]
    value: Any

    def apply(self, player: Player) -> None:
        player.set_state(self.path, self.value)

@dataclass
class IncrementCounterEffect(Effect):
    path: List[str]

    def apply(self, player: Player) -> None:
        player.increment_counter(self.path)

@dataclass
class ResetCounterEffect(Effect):
    path: List[str]

    def apply(self, player: Player) -> None:
        player.reset_counter(self.path)


@dataclass
class Change:
    source: ChangeSource
    timestamp: datetime
    timestamp_source: TimestampSource
    effects: List[Effect]

    def apply(self, player: Player) -> None:
        for effect in self.effects:
            effect.apply(player)


def _get_mod_effect(event: dict) -> ModEffect:
    metadata = event['metadata']
    if event['type'] == 106 or event['type'] == 146:
        return ModEffect(from_mod=None,
                         to_mod=metadata['mod'],
                         type=ModDuration(metadata['type']))
    elif event['type'] == 107 or event['type'] == 147:
        return ModEffect(from_mod=metadata['mod'],
                         to_mod=None,
                         type=ModDuration(metadata['type']))
    elif event['type'] == 148:
        return ModEffect(from_mod=metadata['from'],
                         to_mod=metadata['to'],
                         type=ModDuration(metadata['type']))

    raise ValueError("Not chron mod add/remove/change event")


def _player_id(event: dict) -> str:
    assert len(event['playerTags']) == 1
    return event['playerTags'][0]


def check_equality_recursive(chron: dict, ours: dict, path=""):
    if type(chron) != type(ours):
        raise RuntimeError(f"Mismatched type for {path}, expected " +
                           str(type(ours)) + " but chron has " +
                           str(type(chron)))

    if isinstance(chron, list):
        if len(chron) != len(ours):
            raise RuntimeError(f"Mismatched length for {path}, expected " +
                               str(len(ours)) + " but chron has " +
                               str(len(chron)))

        for i, (chron_elem, ours_elem) in enumerate(zip(chron, ours)):
            check_equality_recursive(chron_elem, ours_elem, f"{path}.{i}")

    if isinstance(chron, dict):
        chron_keys = set(chron.keys())
        our_keys = set(ours.keys())

        if chron_keys - our_keys:
            raise RuntimeError(f"Chron has additional key(s) for {path}: " +
                               ", ".join(chron_keys - our_keys))

        if our_keys - chron_keys:
            raise RuntimeError(f"Chron is missing key(s) for {path}: " +
                               ", ".join(our_keys - chron_keys))

        assert chron_keys == our_keys
        for key in chron_keys:
            check_equality_recursive(chron[key], ours[key], f"{path}.{key}")


class Players:
    def __init__(self, start_time: datetime):
        self.players: Dict[str, Player] = {}
        self.changes: Dict[str, List[Change]] = defaultdict(lambda: [])

        for player in get_entities("player",
                                   at=start_time,
                                   cache_time=None):
            self.players[player['entityId']] = Player(player)

    def associate_chron_updates(self, chron_updates: List[dict]):
        assert len(chron_updates) > 0
        chron_update_time = chron_updates[0]['validFrom']
        for chron_update in chron_updates:
            player_id = chron_update['entityId']

            player = deepcopy(self.players[player_id])
            last_matching_player, last_matching_i = None, None
            for i, change in enumerate(self.changes[player_id]):
                change.apply(player)

                if player.data == chron_update['data']:
                    last_matching_i = i
                    last_matching_player = deepcopy(player)

            if last_matching_i is None:
                print(list(diff(self.players[player_id].data,
                                chron_update['data'])))
                raise RuntimeError("Unable to account for chron change")

            # Changes up to last_matching_i are yielded, the rest are saved for
            # the next chron update
            last_matching_i += 1
            changes = self.changes[player_id][:last_matching_i]
            self.changes[player_id] = self.changes[player_id][last_matching_i:]

            # Verification
            for change in changes:
                change.apply(self.players[player_id])
            assert self.players[player_id].data == last_matching_player.data

            yield chron_update, changes

        for key, changes in self.changes.items():
            for change in changes:
                if chron_update_time - change.timestamp > timedelta(seconds=300):
                    raise RuntimeError("Chron update didn't account for "
                                       f"{len(changes)} changes to ${key}")

    def apply_event(self, event: dict) -> None:
        print("Applying:", event['description'])
        if 'parent' in event['metadata']:
            changes = Players._find_change_by_parent_type[
                event['metadata']['parent']['type']](self, event)
        else:
            changes = Players._find_change_by_own_type[
                event['type']](self, event)
        for player_id, change in changes:
            self.changes[player_id].append(change)

    def _find_change_superyummy(self, event: dict) -> List[Tuple[str, Change]]:
        mod_effect = _get_mod_effect(event)
        state_effect = SetStateEffect(path=['permModSources', mod_effect.to_mod],
                                      value=['SUPERYUMMY'])
        return [(_player_id(event),
                 Change(source=ChangeSource.SUPERYUMMY,
                        timestamp=event['created'],
                        timestamp_source=TimestampSource.FEED,
                        effects=[mod_effect, state_effect]))]

    def _find_recorded_change_from_score(self, event: dict) \
            -> List[Tuple[str, Change]]:
        if event['type'] == 107 and event['metadata']['mod'] == 'COFFEE_RALLY':
            return [(_player_id(event),
                     Change(source=ChangeSource.USE_FREE_REFILL,
                            timestamp=event['created'],
                            timestamp_source=TimestampSource.FEED,
                            effects=[_get_mod_effect(event)]))]

        raise RuntimeError("Didn't find change type from hit")

    def _find_unrecorded_change_from_hit(self, event: dict) \
            -> List[Tuple[str, Change]]:
        # I hope the player who hit the hit is guaranteed to be first.
        return [(event['playerTags'][0],
                 Change(source=ChangeSource.HIT,
                        timestamp=event['created'],
                        timestamp_source=TimestampSource.FEED,
                        effects=[IncrementCounterEffect(['consecutiveHits'])]))]

    def _find_unrecorded_change_from_non_hit(self, event: dict) \
            -> List[Tuple[str, Change]]:
        # TODO Get the player ID from blarser
        return [("",
                 Change(source=ChangeSource.NON_HIT,
                        timestamp=event['created'],
                        timestamp_source=TimestampSource.FEED,
                        effects=[ResetCounterEffect(['consecutiveHits'])]))]

    _find_change_by_parent_type = {
        92: _find_change_superyummy,
        4: _find_recorded_change_from_score,  # stolen base
        10: _find_recorded_change_from_score,  # hit
    }

    _find_change_by_own_type = {
        7: _find_unrecorded_change_from_non_hit,
        # 9 is a home run, which has the same effects as hit
        9: _find_unrecorded_change_from_hit,
        10: _find_unrecorded_change_from_hit,
    }
