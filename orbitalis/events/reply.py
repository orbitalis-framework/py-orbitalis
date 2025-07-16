
from dataclasses import dataclass, field
from typing import Optional, Dict, List, TypeVar, Generic

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.core.need import SetupData


@dataclass(kw_only=True)
class RequestedOperation(Generic[SetupData], AvroModel):
    name: str
    output_topic: Optional[str]
    setup_data: Optional[SetupData]
    core_close_connection_topic: str

    # TODO
    # core_keepalive_topic: str


@dataclass(frozen=True)
@registry
class RequestMessage(AvroEventPayload, Generic[SetupData]):
    """
    Core --- request ---> Plugin

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str

    requested_operations: Dict[str, RequestedOperation[SetupData]]  # operation_name => RequestedOperation
    response_topic: Optional[str] = field(default=None)
    setup_data: Optional[SetupData] = field(default=None)

    def __post_init__(self):
        if len(self.requested_operations) == 0:
            raise ValueError("requested operations missed")


@dataclass(frozen=True)
@registry
class RejectMessage(AvroEventPayload):
    """
    Core --- reject ---> Plugin

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    rejected_operations: List[str]
    description: Optional[str] = field(default=None)

    def __post_init__(self):
        if len(self.rejected_operations) == 0:
            raise ValueError("rejected operations missed")