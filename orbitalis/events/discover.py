import json
from dataclasses import dataclass, field
from typing import List
from busline.event.event import Event



@dataclass
class DiscoverEventContent:
    """
    core_identifier: identifier of core which has sent discover
    types: list of needed plugin types
    identifiers: list of needed plugin identifiers
    whitelist: (only if `types` is set) admitted plugin identifiers
    blacklist: (only if `types` is set) not admitted plugin identifiers

    Author: Nicola Ricciardi
    """

    core_identifier: str
    types: List[str] | None = field(default=None)
    identifiers: List[str] | None = field(default=None)
    whitelist: List[str] | None = field(default=None)
    blacklist: List[str] | None = field(default=None)


class DiscoverEvent(Event):
    def __init__(self, content: DiscoverEventContent):

        Event.__init__(self, json.dumps(content))