from dataclasses import dataclass
from enum import Enum, auto
from typing import Set


class ChangeSourceType(Enum):
    # For debugging
    UNKNOWN = auto()

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

    # Various forms of Debt cause Debted players to hit others with pitches, and
    # that adds a weekly mod (e.g. Unstable)
    HIT_BY_PITCH = auto()

    # Is party!
    PARTY = auto()

    # Attributes were added that didn't used to exist. For example, star ratings
    # in the Expansion era.
    ADDED_ATTRIBUTES = auto()

    # At least once they renamed an attribute -- _id to id
    RENAMED_ATTRIBUTES = auto()

    # At least once they changed the format of the attributes (changed the
    # values without changing the meaning) -- for legacy bats
    CHANGED_ATTRIBUTE_FORMAT = auto()

    # For some reason, the precision changed
    PRECISION_CHANGE = auto()

    # A player was generated to replace an incinerated player
    INCINERATION_REPLACEMENT = auto()

    # A player was incinerated
    INCINERATED = auto()

    # A player had attributes above/below the caps, and the caps were applied
    ATTRIBUTES_CAPPED = auto()

    # Placeholder. These will be handled manually once I have nailed down the
    # election format from the post-feed elections.
    PRE_FEED_ELECTION = auto()

    # For a while after the Season 2 election, which introduced Peanuts, players
    # who weren't allergic to peanuts spontaneously became allergic. This was a
    # bug that was later fixed
    CREEPING_PEANUT_ALLERGY = auto()

    # This is when some peanut allergies were manually fixed
    CREEPING_PEANUT_DEALLERGIZE = auto()

    # For a while after the Season 2 election, which introduced Peanuts, players
    # who had a fate of 0 spontaneously gained a new fate. This was a bug that
    # was later fixed
    FATELESS_FATED = auto()

    # On rare occasions TGB has manually changed some stuff
    MANUAL = auto()

    # Players eat peanuts sometimes, often inadvisedly
    PEANUT = auto()

    # In the discipline era players weren't generated with coffee, blood, and
    # rituals (the three attributes added by the Interviews decree). They were
    # generated after some time
    INTERVIEW = auto()

    # When players get feedbacked, sometimes they change(d?) fate
    FEEDBACK_FATE = auto()

    # Very rarely, Reverb weather bestows the Reverberating mod
    REVERBERATING_BESTOWED = auto()

    # In Blooddrain weather, players drain blood
    BLOODDRAIN = auto()

    # When a Feedback is attempted but the target player is Soundproof, the
    # initiating player gets impaired a small amount
    FEEDBACK_SOUNDPROOF = auto()

    # The idol board sometimes bestows mods, e.g. Shelled
    IDOLBOARD_MOD = auto()

    # When an Unstable player is incinerated the instability chains to another
    # player
    UNSTABLE_CHAIN = auto()

    # Birds sometimes peck shelled players free
    UNSHELLING = auto()

    # If a team needs to play a game but they have no active (i.e. not Shelled,
    # Elsewhere, etc.) pitchers, a Pitching Machine is created and added to
    # their rotation
    PITCHING_MACHINE_CREATED = auto()

    # Spicy works by adding and removing some spicy-specific mods
    SPICY = auto()

    # In the Discipline era, or perhaps just because of the Iffey Jr., a failed
    # incineration resulted in the target gainign stars
    FAILED_INCINERATION = auto()

    # The mic spoke to us through the Receivers' rituals
    RECEIVER_RITUALS = auto()

    # For a short time giant peanuts would fall to the ground and exactly once
    # it hit a player, Wyatt Quitter
    GIANT_PEANUT_SHELLING = auto()

    # When a team makes the playoffs they get a player added to their shadows
    POSTSEASON_BIRTH = auto()

    # Shelled players became Honey Roasted when they joined the Pods
    JOINED_PODS = auto()

    # When you lose to a God team, you become cursed
    CURSED_BY_GOD = auto()

    # Fire Eaters become Magmatic when they eat fire
    ATE_FIRE = auto()

    # Fire Eaters use up their Magmatic when they hit the Magmatic homer
    HIT_MAGMATIC_HOME_RUN = auto()

    # Players stopped being deceased and became squiddish when they joined the
    # Hall Stars
    JOINED_HALL_STARS = auto()

    # Players leaving a team with Loyalty will gain the Saboteur modification.
    LOYALTY = auto()

    # Players leaving a Team with Subjection will gain the Liberated
    # modification.
    SUBJECTION = auto()

    # Seasonal mods wear off at the end of every season
    SEASONAL_MODS_WEAR_OFF = auto()

    # The Hall Stars were Released
    HALL_STARS_RELEASED = auto()

    # Ghosts can Inhabit Haunted players
    INHABITING = auto()


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
    game: str
    perceived_at: str  # should this be datetime?


@dataclass
class GameEndChangeSource(ChangeSource):
    season: int
    day: int


@dataclass
class ElectionChangeSource(ChangeSource):
    season: int


@dataclass
class EndseasonChangeSource(ChangeSource):
    season: int
