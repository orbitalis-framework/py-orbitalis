from dataclasses import dataclass, field
from typing import Dict, Optional, List

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.core.need import ConstrainedNeed
from orbitalis.utils.allowblocklist import AllowBlockPriorityListMixin


@dataclass
class NeededOperationInformation(AvroModel):
    operation_name: str
    mandatory: Optional[List[str]] = field(default=None)
    schema_fingerprint: Optional[str] = field(default=None)
    blocklist: Optional[List[str]] = field(default=None)
    priorities: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if self.blocklist is not None and len(set(self.blocklist).intersection(set(self.priorities.keys()))) > 0:
            raise ValueError("Some blocked identifiers have a priority")


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
    needed_operations: Dict[str, NeededOperationInformation]   # operation_name => NeededOperationInformation
