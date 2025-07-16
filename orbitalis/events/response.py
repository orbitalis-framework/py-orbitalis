
from dataclasses import dataclass, field
from typing import Set, Optional, List, Dict

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload

@dataclass(kw_only=True)
class ConfirmedOperation(AvroModel):
    name: str
    input_topic: Optional[str]
    plugin_close_connection_topic: str

    # TODO
    # core_keepalive_topic: str


@dataclass(frozen=True)
@registry
class ResponseMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    confirmed_operations: Dict[str, ConfirmedOperation]     # operation_name => ConfirmedOperation
    operations_no_longer_available: List[str]