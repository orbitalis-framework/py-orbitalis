from dataclasses import dataclass, field
from typing import override
from busline.event.event import Event
from orbitalis.orbiter.schemaspec import SchemaSpec, Input, Output
from orbitalis.plugin.operation import Policy, operation
from tests.plugin.lamp_plugin import LampPlugin, LampStatus


@dataclass
class LampXPlugin(LampPlugin):

    @operation(
        name="turn_on",
        input=Input.empty()
    )
    async def turn_on_event_handler(self, topic: str, event: Event):
        self.turn_on()

    @operation(
        name="turn_off",
        input=Input.empty()
    )
    async def turn_off_event_handler(self, topic: str, event: Event):
        self.turn_off()
