
from dataclasses import dataclass, field
from typing import Optional, Dict, List, TypeVar, Generic

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.core.descriptor import CoreDescriptor


@dataclass(frozen=True, kw_only=True)
@registry
class RequestOperationMessage(AvroEventPayload):
    """
    Core --- request ---> Plugin

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    operation_name: str
    response_topic: str
    output_topic: Optional[str]
    core_side_close_operation_connection_topic: str
    setup_data: Optional[str]


@dataclass(frozen=True, kw_only=True)
@registry
class RejectOperationMessage(AvroEventPayload):
    """
    Core --- reject ---> Plugin

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    operation_name: str
    description: Optional[str] = field(default=None)
