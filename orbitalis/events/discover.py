from dataclasses import dataclass, field
from typing import Dict, Optional, List

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.core.need import Constraint
from orbitalis.utils.allowblocklist import AllowBlockListMixin



@dataclass(frozen=True)
@registry
class DiscoverMessage(AvroEventPayload):
    """
    Core --- discover ---> Plugin

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    offer_topic: str
    needed_operations: Dict[str, Constraint]   # operation_name => Need
