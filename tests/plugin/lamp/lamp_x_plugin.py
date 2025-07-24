from dataclasses import dataclass
from busline.event.event import Event
from orbitalis.orbiter.schemaspec import Input
from orbitalis.plugin.operation import operation
from tests.plugin.lamp.lamp_plugin import LampPlugin


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
