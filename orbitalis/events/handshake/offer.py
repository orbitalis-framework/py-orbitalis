from dataclasses import dataclass, field
from typing import Set, Optional, Dict, List

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload

from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.wellknown_event import WellKnownEventType
from orbitalis.plugin.descriptor import PluginDescriptor

@dataclass(frozen=True)
class OfferedOperation(AvroModel):
    operation_name: str
    topic: str
    operation_input_schema_fingerprints: List[str]


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