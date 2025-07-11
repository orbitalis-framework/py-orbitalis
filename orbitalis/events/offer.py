from dataclasses import dataclass, field
from typing import Set, Optional, Dict, List

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload


@dataclass(frozen=True)
class OfferedOperation(AvroModel):
    operation_name: str
    operation_input_schemas: List[str]
    operation_output_schemas: Optional[List[str]]

    def __post_init__(self):
        if self.operation_output_schemas is not None and len(self.operation_output_schemas) == 0:
            raise ValueError("missed output schemas")

        if len(self.operation_input_schemas) == 0:
            raise ValueError("missed input schemas")

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