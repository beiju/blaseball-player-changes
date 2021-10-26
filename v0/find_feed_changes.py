from typing import Optional, Set, Iterator

from ChangeSource import ChangeSource

PERFORMING = {'UNDERPERFORMING', 'OVERPERFORMING'}


def find_mod_added_by_mod(event: dict, before: Optional[dict], after: dict,
                          changed_keys: Set[str]) -> Iterator[ChangeSource]:
    raise NotImplementedError()
    # try:
    #     added_mod =
    #     if (event['metadata']['source'] == 'SUPERYUMMY' and
    #             event['metadata']['mod'] in PERFORMING):


FEED_CHANGE_FINDERS = {
    # feed duration: function
    146: find_mod_added_by_mod,
}
