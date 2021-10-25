from datetime import datetime
from backports.zoneinfo import ZoneInfo
from blaseball_mike.chronicler import get_entities

from Player import Player

EXPANSION_ERA_START = datetime(year=2021, month=3, day=1, hour=11,
                               tzinfo=ZoneInfo('US/Eastern'))


class Players:
    def __init__(self):
        self.players = {}
        for player in get_entities("player",
                                   at=EXPANSION_ERA_START,
                                   cache_time=None):
            self.players[player['entityId']] = Player(player)
