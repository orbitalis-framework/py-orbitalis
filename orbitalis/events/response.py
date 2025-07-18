
from dataclasses import dataclass, field
from typing import Set, Optional, List, Dict

from dataclasses_avroschema import AvroModel

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload
from orbitalis.orbiter.schemaspec import Input


@dataclass(frozen=True, kw_only=True)
@registry
class ConfirmConnectionMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    plugin_identifier: str

    operation_name: str
    operation_input_topic: Optional[str]
    plugin_side_close_operation_connection_topic: str


@dataclass(frozen=True, kw_only=True)
@registry
class OperationNoLongerAvailableMessage(AvroEventPayload):
    """
    TODO

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    operation_name: str