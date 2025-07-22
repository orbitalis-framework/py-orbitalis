
from dataclasses import dataclass, field
from typing import Optional

from busline.event.registry import add_to_registry
from busline.event.message.avro_message import AvroMessageMixin


@dataclass(frozen=True, kw_only=True)
@add_to_registry
class RequestOperationMessage(AvroMessageMixin):
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
@add_to_registry
class RejectOperationMessage(AvroMessageMixin):
    """
    Core --- reject ---> Plugin

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    operation_name: str
    description: Optional[str] = field(default=None)
