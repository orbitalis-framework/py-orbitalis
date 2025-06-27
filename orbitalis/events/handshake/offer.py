from dataclasses import dataclass
from typing import Set

from orbitalis.core.configuration import ServiceNeed
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.orb_event import OrbEvent
from orbitalis.events.wellknown_event import WellKnownEventType
from orbitalis.plugin.descriptor import PluginDescriptor


@dataclass
class OfferEventContent:
    """

    TODO

    Author: Nicola Ricciardi
    """

    plugin_descriptor: PluginDescriptor


@dataclass(frozen=True)
class OfferEvent(OrbEvent):
    content: OfferEventContent
    event_type = WellKnownEventType.OFFER.value

