from dataclasses import dataclass
from typing import Optional, TypeVar, Generic

from dataclasses_avroschema import AvroModel

from busline.event.registry import add_to_registry
from busline.event.message.avro_message import AvroMessageMixin


@dataclass(frozen=True, kw_only=True)
class GracelessCloneConnectionMessage(AvroMessageMixin):
    """
    Orbiter A --- close ---> Orbiter B

    Message sent to close connection

    Author: Nicola Ricciardi
    """

    from_identifier: str
    operation_name: str
    data: Optional[bytes]


@dataclass(frozen=True, kw_only=True)
class GracefulCloseConnectionMessage(AvroMessageMixin):
    """
    Orbiter A --- close ---> Orbiter B

    Message sent to notify the desire to close the connection

    Author: Nicola Ricciardi
    """

    from_identifier: str
    operation_name: str
    ack_topic: str
    data: Optional[bytes]


@dataclass(frozen=True, kw_only=True)
class CloseConnectionAckMessage(AvroMessageMixin):
    """
    Orbiter A <--- close ack --- Orbiter B

    Message sent to confirm close connection

    Author: Nicola Ricciardi
    """

    from_identifier: str
    operation_name: str

