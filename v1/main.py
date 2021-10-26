from datetime import datetime, timedelta
from heapq import merge
from typing import Iterator, List, Tuple, Any

from backports.zoneinfo import ZoneInfo

import requests_cache
from blaseball_mike import chronicler, eventually
from blaseball_mike.session import _SESSIONS_BY_EXPIRY
from dateutil.parser import isoparse

from v1.Players import Players

session = requests_cache.CachedSession("blaseball-player-changes",
                                       backend="sqlite", expire_after=None)
_SESSIONS_BY_EXPIRY[None] = session

EXPANSION_ERA_START = datetime(year=2021, month=3, day=1, hour=10,
                               tzinfo=ZoneInfo('US/Eastern'))
ONE_SECOND = timedelta(seconds=1)


def get_chron_batched() -> Iterator[Tuple[datetime, str, List[dict]]]:
    current_batch = []
    current_batch_date = None
    for chron_entry in chronicler.get_versions("player",
                                               after=EXPANSION_ERA_START,
                                               order='asc',
                                               cache_time=None):
        chron_entry['validFrom'] = isoparse(chron_entry['validFrom'])
        if current_batch_date is None:
            current_batch_date = chron_entry['validFrom']
        elif chron_entry['validFrom'] - current_batch_date > ONE_SECOND:
            yield current_batch_date, 'chron_updates', current_batch
            current_batch = []
            current_batch_date = chron_entry['validFrom']
        print("Batching chron entry for", chron_entry['data']['name'])
        current_batch.append(chron_entry)


def get_feed(query, query_name) -> Iterator[Tuple[datetime, dict]]:
    q = {
        'expand_parent': 'true',
        'after': EXPANSION_ERA_START.isoformat(),
        'sortorder': '{created}',
        **query
    }
    for event in eventually.search(cache_time=None, limit=-1, query=q):
        event['created'] = isoparse(event['created'])
        yield event['created'], query_name, event


def get_associations():
    players = Players(EXPANSION_ERA_START)

    iterators = [
        get_chron_batched(),
        get_feed({'category': '1'}, 'change'),  # Changes
        get_feed({'type': '6_or_7_or_8_or_9_or_10'}, 'hit_or_lack_thereof'),
    ]
    for _, source, item in merge(*iterators):
        if source == 'chron_updates':
            yield from players.associate_chron_updates(item)
        else:
            players.apply_event(item)


def main():
    for association in get_associations():
        pass


if __name__ == '__main__':
    main()
