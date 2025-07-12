from dataclasses import dataclass, field
from typing import override
from busline.event.event import Event
from orbitalis.plugin.operation import OperationSchema, Policy, operation
from test.plugin.lamp_plugin import LampPlugin, LampStatus


@dataclass
class LampXPlugin(LampPlugin):

    @operation(
        name="turn_on",
        input=OperationSchema.empty(),
        policy=Policy()
    )
    async def turn_on_event_handler(self, topic: str, event: Event):
        self._turn_on()

    @operation(
        name="turn_off",
        input=OperationSchema.empty(),
        policy=Policy()
    )
    async def turn_off_event_handler(self, topic: str, event: Event):
        self._turn_off()
