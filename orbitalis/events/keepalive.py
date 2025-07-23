from dataclasses import dataclass

from busline.event.message.avro_message import AvroMessageMixin
from busline.event.registry import add_to_registry


@dataclass(frozen=True, kw_only=True)
class KeepaliveRequestMessage(AvroMessageMixin):
    """
    Orbiter A --- keepalive_request ---> Orbiter B

    TODO

    Author: Nicola Ricciardi
    """

    from_identifier: str
    keepalive_topic: str


@dataclass(frozen=True, kw_only=True)
class KeepaliveMessage(AvroMessageMixin):
    """
    Orbiter A <--- keepalive --- Orbiter B

    TODO

    Author: Nicola Ricciardi
    """

    from_identifier: str