from dataclasses import dataclass
from busline.event.event import Event
from orbitalis.orbiter.schemaspec import Input
from orbitalis.plugin.operation import operation
from tests.plugin.lamp.lamp_plugin import LampPlugin


@dataclass
class LampXPlugin(LampPlugin):
    """
    Specific plugin related to brand X of smart lamps.
    This type of lamps doesn't have additional features
    """

    @operation(     # add new operation with name: "turn_on"
        name="turn_on",
        input=Input.empty()     # accepts empty events
    )
    async def turn_on_event_handler(self, topic: str, event: Event):
        self.turn_on()

    @operation(     # add new operation with name: "turn_off"
        name="turn_off",
        input=Input.empty()     # accepts empty events
    )
    async def turn_off_event_handler(self, topic: str, event: Event):
        self.turn_off()
