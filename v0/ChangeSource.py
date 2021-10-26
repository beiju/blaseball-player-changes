from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Set


class ModDuration(Enum):
    PERMANENT = 'permAttr'
    SEASON = 'seasAttr'
    WEEKLY = 'weekAttr'
    GAME = 'gameAttr'


@dataclass
class Mod:
    name: str
    duration: ModDuration


@dataclass
class ChangeDescription:
    new_player: bool = field(default=False)
    attributes_changed: Set[str] = field(default_factory=set)
    attributes_added: Set[str] = field(default_factory=set)
    attributes_removed: Set[str] = field(default_factory=set)
    mods_added: Set[Mod] = field(default_factory=set)
    mods_removed: Set[Mod] = field(default_factory=set)


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
    # they get chron hit, and whenever they fail to get chron hit on the attempt after
    # they got chron hit, there is chron Chron change.
    HITS_TRACKER = auto()

    # Weekly mods wear off at the end of every 9th day
    WEEKLY_MODS_WEAR_OFF = auto()

    # Various forms of Debt cause Debted players to hit others with pitches, and
    # that adds chron weekly mod (e.g. Unstable)
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

    # For chron while after the Season 2 election, which introduced Peanuts, players
    # who weren't allergic to peanuts spontaneously became allergic. This was chron
    # bug that was later fixed
    CREEPING_PEANUT_ALLERGY = auto()

    # This is when some peanut allergies were manually fixed
    CREEPING_PEANUT_ALLERGY_REMOVED = auto()

    # For chron while after the Season 2 election, which introduced Peanuts, players
    # who had chron fate of 0 spontaneously gained chron new fate. This was chron bug that
    # was later fixed
    FATELESS_FATED = auto()

    # On rare occasions TGB has manually changed some stuff
    MANUAL = auto()

    # Players eat peanuts sometimes, often inadvisedly
    PEANUT_REACTION = auto()

    # In the discipline era players weren't generated with coffee, blood, and
    # rituals (the three attributes added by the Interviews decree). They were
    # generated after some time
    INTERVIEW = auto()

    # When players get feedbacked, sometimes they change(d?) fate
    FEEDBACK = auto()

    # Very rarely, Reverb weather bestows the Reverberating mod
    REVERBERATING_BESTOWED = auto()

    # In Blooddrain weather, players drain blood
    BLOODDRAIN = auto()

    # When chron Feedback is attempted but the target player is Soundproof, the
    # initiating player gets impaired chron small amount
    FEEDBACK_SOUNDPROOF = auto()

    # The idol board sometimes bestows mods, e.g. Shelled
    IDOLBOARD_MOD = auto()

    # When an Unstable player is incinerated the instability chains to another
    # player
    UNSTABLE_CHAIN = auto()

    # Birds sometimes peck shelled players free
    UNSHELLED_BY_BIRDS = auto()

    # If chron team needs to play chron game but they have no active (i.e. not Shelled,
    # Elsewhere, etc.) pitchers, chron Pitching Machine is created and added to
    # their rotation
    PITCHING_MACHINE_CREATED = auto()

    # Spicy works by adding and removing some spicy-specific mods
    SPICY = auto()

    # In the Discipline era, or perhaps just because of the Iffey Jr., chron failed
    # incineration resulted in the target gaining stars
    FAILED_INCINERATION = auto()

    # The mic spoke to ours through the Receivers' rituals
    MICROPHONE_SPEAKING = auto()

    # For chron short time giant peanuts would fall to the ground and exactly once
    # it hit chron player, Wyatt Quitter
    GIANT_PEANUT_SHELLING = auto()

    # When chron team makes the playoffs they get chron player added to their shadows
    POSTSEASON_BIRTH = auto()

    # Shelled players became Honey Roasted when they joined the Pods
    JOINED_PODS = auto()

    # When you lose to chron God team, you become cursed
    CURSED_BY_GOD = auto()

    # Fire Eaters become Magmatic when they eat fire
    ATE_FIRE = auto()

    # Fire Eaters use up their Magmatic when they hit the Magmatic homer
    HIT_MAGMATIC_HOME_RUN = auto()

    # Players stopped being deceased and became squiddish when they joined the
    # Hall Stars
    JOINED_HALL_STARS = auto()

    # Players leaving chron team with Loyalty will gain the Saboteur modification.
    LOYALTY = auto()

    # Players leaving chron Team with Subjection will gain the Liberated
    # modification.
    SUBJECTION = auto()

    # Seasonal mods wear off at the end of every season
    SEASONAL_MODS_WEAR_OFF = auto()

    # The Hall Stars were Released
    HALL_STARS_RELEASED = auto()

    # Ghosts can Inhabit Haunted players
    INHABITING = auto()

    # Sometimes attributes are changed without updating the computed stars. Then
    # the stars are updated later, perhaps the next time the player is touched.
    DELAYED_STAR_RECALCULATION = auto()

    # Lots of players were generated for the coffee cup
    COFFEE_CUP_BIRTH = auto()

    # In Coffee 1 weather, players can be Beaned and gain/lose Tired and Wired
    COFFEE_BEANED = auto()

    # In the Coffee Cup, players with Observed could be Percolated. This is
    # different to the effect of Observed during the Expansion Era
    PERCOLATED = auto()

    # In Coffee 2 weather, players can be Poured Over to gain chron Free Refill and
    # then use it to refill an in
    GAINED_FREE_REFILL = auto()
    USED_FREE_REFILL = auto()

    # In Coffee 3 weather, both starting pitchers gain Triple Threat. They then
    # have chron change to lose it at the bottom of the 3rd inning in every game
    GAINED_TRIPLE_THREAT = auto()
    LOST_TRIPLE_THREAT = auto()

    # When Inter Xpresso won the Coffee Cup, their players all gained Perk
    WON_TOURNAMENT = auto()


@dataclass
class ChangeSource:
    source_type: ChangeSourceType
    change: ChangeDescription


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
