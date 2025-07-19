from dataclasses import dataclass

from busline.event.avro_payload import AvroEventPayload
from busline.event.registry import registry


@dataclass(frozen=True, kw_only=True)
@registry
class KeepaliveRequestMessage(AvroEventPayload):
    """
    Orbiter A --- keepalive_request ---> Orbiter B

    TODO

    Author: Nicola Ricciardi
    """

    from_identifier: str
    keepalive_topic: str


@dataclass(frozen=True, kw_only=True)
@registry
class KeepaliveMessage(AvroEventPayload):
    """
    Orbiter A <--- keepalive --- Orbiter B

    TODO

    Author: Nicola Ricciardi
    """

    from_identifier: str