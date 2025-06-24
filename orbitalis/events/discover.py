import json
from dataclasses import dataclass, field
from typing import Set
from busline.event.event import Event

from orbitalis.core.configuration import Needs
from orbitalis.events.orb_event import OrbEvent
from orbitalis.events.wellknown_event import WellKnownEventType


@dataclass
class DiscoverEventContent:
    """
    core_identifier: identifier of core which has sent discover

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    needs_set: Set[Needs]


@dataclass(frozen=True)
class DiscoverEvent(OrbEvent):
    event_type = WellKnownEventType.DISCOVER.value

