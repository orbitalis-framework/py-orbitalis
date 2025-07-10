from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from busline.event.avro_payload import AvroEventPayload
from busline.event.registry import registry


@dataclass(frozen=True)
@registry
class OperationResultMessage(AvroEventPayload):
    """
    Plugin --- result ---> Core

    TODO

    Author: Nicola Ricciardi
    """

    plugin_identifier: str
    operation_name: str
    avro_data: Optional[bytes] = field(default=None)
    avro_schema: Optional[str] = field(default=None)
    exception: Optional[str] = field(default=None)
    produced_at: datetime = field(default_factory=lambda: datetime.now())

    def __post_init__(self):
        if self.exception is None and self.avro_data is None:
            raise ValueError("Result missed")

        if self.avro_data is not None and self.avro_schema is None:
            raise ValueError("Schema missed")
