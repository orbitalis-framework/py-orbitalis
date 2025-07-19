from dataclasses import dataclass, field
from typing import Set, Optional, Dict, List

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.orbiter.schemaspec import Input, Output


@dataclass
class OfferedOperation(AvroModel):
    name: str
    input: Input
    output: Output

@dataclass(frozen=True)
@registry
class OfferMessage(AvroEventPayload):
    """

    Plugin --- offer ---> Core

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    offered_operations: List[OfferedOperation]
    reply_topic: str
    considered_dead_after: float
    plugin_keepalive_topic: str
    plugin_keepalive_request_topic: str