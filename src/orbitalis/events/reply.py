
from dataclasses import dataclass, field
from typing import Optional

from busline.event.registry import add_to_registry
from busline.event.message.avro_message import AvroMessageMixin


@dataclass(frozen=True, kw_only=True)
class RequestOperationMessage(AvroMessageMixin):
    """
    Core --- request ---> Plugin

    Message used by core to formally request an operation. Every operation has own request.
    Core provides additional information to finalize the connection

    Author: Nicola Ricciardi
    """

    core_identifier: str
    operation_name: str
    response_topic: str
    output_topic: Optional[str]
    core_side_close_operation_connection_topic: str
    setup_data: Optional[bytes]


@dataclass(frozen=True, kw_only=True)
class RejectOperationMessage(AvroMessageMixin):
    """
    Core --- reject ---> Plugin

    Message used by core to formally reject an operation (e.g., not needed anymore). Every operation has own reject.

    Author: Nicola Ricciardi
    """

    core_identifier: str
    operation_name: str
    description: Optional[str] = field(default=None)
