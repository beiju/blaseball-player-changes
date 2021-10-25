import requests_cache
from blaseball_mike.session import _SESSIONS_BY_EXPIRY

from v1.Players import Players

session = requests_cache.CachedSession("blaseball-player-changes",
                                       backend="sqlite", expire_after=None)
_SESSIONS_BY_EXPIRY[None] = session


def main():
    players = Players()


if __name__ == '__main__':
    main()
