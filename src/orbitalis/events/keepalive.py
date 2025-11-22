from dataclasses import dataclass

from busline.event.message.avro_message import AvroMessageMixin
from busline.event.registry import add_to_registry


@dataclass(frozen=True, kw_only=True)
class KeepaliveRequestMessage(AvroMessageMixin):
    """
    Orbiter A --- keepalive_request ---> Orbiter B

    Message sent to request a keepalive

    Author: Nicola Ricciardi
    """

    from_identifier: str
    keepalive_topic: str


@dataclass(frozen=True, kw_only=True)
class KeepaliveMessage(AvroMessageMixin):
    """
    Orbiter A <--- keepalive --- Orbiter B

    Message sent to notify a keepalive

    Author: Nicola Ricciardi
    """

    from_identifier: str