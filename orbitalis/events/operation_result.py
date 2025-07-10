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
    data: Optional[bytes] = field(default=None)
    exception: Optional[str] = field(default=None)
    produced_at: datetime = field(default_factory=lambda: datetime.now())

    def __post_init__(self):
        if self.exception is None and self.data is None:
            raise ValueError("Result missed")

        if self.exception is not None and self.data is not None:
            raise ValueError("You must provide data OR exception")