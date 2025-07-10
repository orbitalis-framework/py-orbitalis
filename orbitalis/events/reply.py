
from dataclasses import dataclass, field
from typing import Optional, Dict, List

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.core.descriptor import CoreDescriptor


@dataclass(frozen=True)
@registry
class RequestMessage(AvroEventPayload):
    """
    Core --- request ---> Plugin

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    core_keepalive_topic: str
    core_general_purpose_hook: str
    requested_operations: Dict[str, str]      # operation_name => output_topic
    response_topic: Optional[str] = field(default=None)

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