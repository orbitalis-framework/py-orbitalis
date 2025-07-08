
from dataclasses import dataclass, field
from typing import Optional, Dict, List

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.core.descriptor import CoreDescriptor


@dataclass(frozen=True)
@registry
class RequestMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    core_descriptor: CoreDescriptor
    requested_operations: Dict[str, Optional[str]]      # operation_name => outcome topic
    response_topic: Optional[str] = field(default=None)

    def __post_init__(self):
        if len(self.requested_operations) == 0:
            raise ValueError("requested operations missed")


@dataclass(frozen=True)
@registry
class RejectMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    rejected_operations: List[str]
    description: Optional[str] = field(default=None)

    def __post_init__(self):
        if len(self.rejected_operations) == 0:
            raise ValueError("rejected operations missed")