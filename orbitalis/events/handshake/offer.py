from dataclasses import dataclass, field
from typing import Set, Optional

from busline.event import registry
from busline.event.avro_payload import AvroEventPayload

from orbitalis.core.configuration import ServiceNeed
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.wellknown_event import WellKnownEventType
from orbitalis.plugin.descriptor import PluginDescriptor


@dataclass(frozen=True)
@registry
class OfferMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    plugin_descriptor: PluginDescriptor
    reply_topic: str
    allowlist: Optional[Set[str]] = field(default=None)
    blocklist: Optional[Set[str]] = field(default=None)
    # TODO: interface