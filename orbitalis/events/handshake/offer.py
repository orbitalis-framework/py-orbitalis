from dataclasses import dataclass, field
from typing import Set, Optional, Dict, List

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload

from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.wellknown_event import WellKnownEventType
from orbitalis.plugin.descriptor import PluginDescriptor

@dataclass(frozen=True)
class OfferedOperation:
    operation_name: str
    topic: str
    operation_input_schemas: List[Dict]


@dataclass(frozen=True)
@registry
class OfferMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    offered_operations: List[OfferedOperation]
    reply_topic: str