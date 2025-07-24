from dataclasses import dataclass
from typing import List

from dataclasses_avroschema import AvroModel

from busline.event.registry import add_to_registry
from busline.event.message.avro_message import AvroMessageMixin
from orbitalis.orbiter.schemaspec import Input, Output


@dataclass
class OfferedOperation(AvroModel):
    name: str
    input: Input
    output: Output

@dataclass(frozen=True)
class OfferMessage(AvroMessageMixin):
    """

    Plugin --- offer ---> Core

    Message used by plugins to response to discover message, providing their base information and a list of offered operations.
    List of offered operations can be smaller than fullset provided by discover

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    offered_operations: List[OfferedOperation]
    reply_topic: str
    considered_dead_after: float
    plugin_keepalive_topic: str
    plugin_keepalive_request_topic: str