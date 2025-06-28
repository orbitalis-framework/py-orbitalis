
from dataclasses import dataclass, field
from typing import Set, Optional

from orbitalis.core.configuration import ServiceNeed
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.orb_event import OrbEvent
from orbitalis.events.wellknown_event import WellKnownEventType
from orbitalis.plugin.descriptor import PluginDescriptor


@dataclass(frozen=True)
class OfferEventContent:
    """

    TODO

    Author: Nicola Ricciardi
    """

    plugin_descriptor: PluginDescriptor
    request_topic: str
    reject_topic: Optional[str] = field(default=None)
    allowlist: Optional[Set[str]] = field(default=None)
    blocklist: Optional[Set[str]] = field(default=None)



@dataclass(frozen=True)
class OfferEvent(OrbEvent):
    content: OfferEventContent
    event_type = WellKnownEventType.OFFER.value

