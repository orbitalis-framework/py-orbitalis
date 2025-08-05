from dataclasses import dataclass, field
from typing import Dict

from busline.event.event import Event
from orbitalis.core.core import Core
from orbitalis.core.sink import sink
from tests.plugin.lamp.lamp_plugin import StatusMessage


@dataclass
class SmartHomeCore(Core):

    lamp_status: Dict[str, str] = field(default_factory=dict)

    @sink(
        operation_name="get_status"
    )
    async def get_status_sink(self, topic: str, event: Event[StatusMessage]):
        self.lamp_status[event.payload.lamp_identifier] = event.payload.status