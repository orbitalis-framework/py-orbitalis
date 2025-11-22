
from dataclasses import dataclass
from typing import Optional

from busline.event.registry import add_to_registry
from busline.event.message.avro_message import AvroMessageMixin


@dataclass(frozen=True, kw_only=True)
class ConfirmConnectionMessage(AvroMessageMixin):
    """
    Plugin --- confirm connection ---> Core

    Message used by plugins to confirm the connection creation

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    operation_name: str
    operation_input_topic: Optional[str]
    plugin_side_close_operation_connection_topic: str


@dataclass(frozen=True, kw_only=True)
class OperationNoLongerAvailableMessage(AvroMessageMixin):
    """
    Plugin --- operation no longer available ---> Core

    Message used by plugins to notify core that operation is no longer available

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    operation_name: str