
from dataclasses import dataclass, field
from typing import Set, Optional

from busline.event import registry
from busline.event.avro_payload import AvroEventPayload

from orbitalis.core.configuration import ServiceNeed
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.events.wellknown_event import WellKnownEventType
from orbitalis.plugin.descriptor import PluginDescriptor


@dataclass(frozen=True)
@registry
class ReplyMessage(AvroEventPayload):
    """

    TODO

    Author: Nicola Ricciardi
    """

    plug_request: True
    response_topic: Optional[str] = field(default=None)
    description: Optional[str] = field(default=None)
    # TODO: interface

    def __post_init__(self):
        if self.plug_request and self.response_topic is None:
            raise ValueError("Response topic is mandatory if plug request must be sent")

    def is_offer_request_discarded(self) -> bool:
        return not self.plug_request
