from dataclasses import dataclass, field
from typing import Set, Optional, Dict, List

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.orbiter.schemaspec import InputOutputSchemaSpec


@dataclass
class OfferedOperation(InputOutputSchemaSpec, AvroModel):
    operation_name: str

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