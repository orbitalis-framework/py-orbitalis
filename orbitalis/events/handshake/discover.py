from dataclasses import dataclass
from typing import Dict

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.core.configuration import ConstrainedNeed
from orbitalis.core.core_service import CoreServiceNeed
from orbitalis.core.descriptor import CoreDescriptor




@dataclass(frozen=True)
@registry
class DiscoverMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    offer_topic: str
    needs: Dict[str, ConstrainedNeed]   # operation_name => ConstrainedNeed
