from dataclasses import dataclass, field
from typing import Set, Optional, Dict, List

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload


@dataclass(frozen=True)
class OfferedOperation(AvroModel):
    operation_name: str
    operation_input_schema: List[str]
    operation_output_schemas: List[str]


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