from dataclasses import dataclass
from typing import Dict

from busline.event.registry import add_to_registry
from busline.event.message.avro_message import AvroMessageMixin
from orbitalis.core.need import Constraint


@dataclass(frozen=True)
@add_to_registry
class DiscoverMessage(AvroMessageMixin):
    """
    Core --- discover ---> Plugin

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    offer_topic: str
    core_keepalive_topic: str
    core_keepalive_request_topic: str
    considered_dead_after: float
    needed_operations: Dict[str, Constraint]   # operation_name => Need
