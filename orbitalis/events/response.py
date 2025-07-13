
from dataclasses import dataclass, field
from typing import Set, Optional, List, Dict

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload


@dataclass(frozen=True)
@registry
class ResponseMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    confirmed_operations: Dict[str, str]     # operation_name => input_topic
    operations_no_longer_available: List[str]
