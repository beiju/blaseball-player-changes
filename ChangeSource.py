from dataclasses import dataclass
from enum import Enum, auto
from typing import Set


class ChangeSourceType(Enum):
    # For players which existed when Chronicler's recording started, this is the
    # "change" corresponding to that player first being recorded.
    CHRON_START = auto()

    # In the discipline era, players tragicness was often reset to 0.1 for no
    # known reason.
    TRAJ_RESET = auto()

    # The player object tracks their consecutive hits and hit streak. Whenever
    # they get a hit, and whenever they fail to get a hit on the attempt after
    # they got a hit, there is a Chron change.
    HITS_TRACKER = auto()

    # Weekly mods wear off at the end of every 9th day
    WEEKLY_MODS_WEAR_OFF = auto()


@dataclass
class ChangeSource:
    source_type: ChangeSourceType
    keys_changed: Set[str]


@dataclass
class UnknownTimeChangeSource(ChangeSource):
    pass


@dataclass
class GameEventChangeSource(ChangeSource):
    season: int
    day: int
