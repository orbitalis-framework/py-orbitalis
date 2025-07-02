from dataclasses import dataclass
from typing import Set

from busline.event import registry
from busline.event.avro_payload import AvroEventPayload

from orbitalis.core.configuration import ServiceNeed
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.wellknown_event import WellKnownEventType


@dataclass(frozen=True)
@registry
class DiscoverMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    core_descriptor: CoreDescriptor
    offer_topic: str
    needs: Set[ServiceNeed]
