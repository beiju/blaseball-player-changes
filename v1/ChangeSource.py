from enum import Enum, auto


class ChangeSource(Enum):
    # Superyummy players gain Overperforming at the beginning of any game with
    # peanut weather or in a stadium with a peanut mister and Underperforming
    # at the beginning of every other game
    SUPERYUMMY = auto()

    # Players with a Free Refill can use it to Refill an In
    USE_FREE_REFILL = auto()

    # The state object records a players consecutive hits, which changes on
    # every hit
    HIT = auto()

    # The state object records a players consecutive hits, which changes on
    # every strikeout, flyout, or ground out
    NON_HIT = auto()

