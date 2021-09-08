from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Union, List, Optional

from dateutil.parser import isoparse

from ChangeSource import ChangeSource

JsonDict = Dict[str, Union[float, int, str, list, dict]]


@dataclass
class Change:
    player_id: str
    valid_from: datetime
    before: Optional[JsonDict]
    after: JsonDict

    sources: List[ChangeSource]

    def __init__(self, before: Optional[JsonDict], after: JsonDict,
                 sources: List[ChangeSource]):
        self.player_id = after['entityId']
        self.valid_from = isoparse(after['validFrom'])
        self.before = before['data'] if before is not None else None
        self.after = after['data']
        self.sources = sources
