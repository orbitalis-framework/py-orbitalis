
from dataclasses import dataclass
from typing import Optional

from busline.event.registry import add_to_registry
from busline.event.message.avro_message import AvroMessageMixin


@dataclass(frozen=True, kw_only=True)
@add_to_registry
class ConfirmConnectionMessage(AvroMessageMixin):
    """

    TODO

    Author: Nicola Ricciardi
    """

    plugin_identifier: str

    operation_name: str
    operation_input_topic: Optional[str]
    plugin_side_close_operation_connection_topic: str


@dataclass(frozen=True, kw_only=True)
@add_to_registry
class OperationNoLongerAvailableMessage(AvroMessageMixin):
    """
    TODO

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    operation_name: str