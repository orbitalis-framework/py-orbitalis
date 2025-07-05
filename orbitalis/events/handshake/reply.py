
from dataclasses import dataclass, field
from typing import Set, Optional

from busline.event.registry import registry
from busline.event.avro_payload import AvroEventPayload



@dataclass(frozen=True)
@registry
class ReplyMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    core_identifier: str
    plug_request: True
    response_topic: Optional[str] = field(default=None)
    description: Optional[str] = field(default=None)

    def __post_init__(self):
        if self.plug_request and self.response_topic is None:
            raise ValueError("Response topic is mandatory if plug request must be sent")

    def is_offer_request_discarded(self) -> bool:
        return not self.plug_request
