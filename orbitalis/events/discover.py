from dataclasses import dataclass, field
from typing import Dict, Optional, List

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.utils.allowblocklist import AllowBlockListMixin


@dataclass(kw_only=True)
class NeededOperationInformation(AllowBlockListMixin, AvroModel):
    operation_name: str
    mandatory: Optional[List[str]]
    input_schemas: Optional[List[str]]
    output_schemas: Optional[List[str]]


@dataclass(frozen=True)
@registry
class DiscoverMessage(AvroEventPayload):
    """
    Core --- discover ---> Plugin

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    offer_topic: str
    needed_operations: Dict[str, NeededOperationInformation]   # operation_name => NeededOperationInformation
