from dataclasses import dataclass
from typing import Set

from orbitalis.core.configuration import ServiceNeed
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.orb_event import OrbEvent
from orbitalis.events.wellknown_event import WellKnownEventType


@dataclass
class DiscoverEventContent:
    """

    TODO

    Author: Nicola Ricciardi
    """

    core_descriptor: CoreDescriptor
    features_needs: Set[ServiceNeed]


@dataclass(frozen=True)
class DiscoverEvent(OrbEvent):
    content: DiscoverEventContent
    event_type = WellKnownEventType.DISCOVER.value

