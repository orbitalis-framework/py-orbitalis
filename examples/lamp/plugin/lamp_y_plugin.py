from dataclasses import dataclass, field
from datetime import datetime
from typing import override

from busline.event.message.avro_message import AvroMessageMixin
from busline.event.event import Event

from examples.lamp.plugin.lamp_plugin import LampPlugin, LampStatus
from orbitalis.orbiter.schemaspec import Input
from orbitalis.plugin.operation import operation

@dataclass(frozen=True)
class TurnOnLampYMessage(AvroMessageMixin):
    """
    Custom message to turn on plugin of brand Y.
    You can provide a "power" value which will be used to
    control brightness (and energy consumption)
    """

    power: float = field(default=1)

    def __post_init__(self):
        assert 0 < self.power <= 1

@dataclass(frozen=True)
class TurnOffLampYMessage(AvroMessageMixin):
    """
    Custom message to turn off plugin of brand Y.
    You can reset energy-meter setting True the flag
    """
    reset_consumption: bool = field(default=False)



@dataclass(kw_only=True)
class LampYPlugin(LampPlugin):
    """
    Specific plugin related to brand Y of smart lamps.
    These lamps are able to manage brightness level
    thanks to "power" attribute
    """

    power: float = field(default=1)

    @override
    def turn_off(self):
        """
        Overridden version to turn off the plugin and compute energy consumption
        also based on power field
        """

        self.status = LampStatus.OFF

        if self.on_at is not None:
            self.total_kwh += self.power * self.kw * (datetime.now() - self.on_at).total_seconds() / 3600

            self.on_at = None

    @operation(     # add new operation with name: "turn_on"
        name="turn_on",
        input=Input.from_message(TurnOnLampYMessage)   # accepts TurnOnLampYMessage messages (checking its Avro schema)
    )
    async def turn_on_event_handler(self, topic: str, event: Event[TurnOnLampYMessage]):
        self.turn_on()
        self.power = event.payload.power

    @operation(     # add new operation with name: "turn_off"
        name="turn_off",
        input=Input.from_schema(TurnOffLampYMessage.avro_schema())   # accepts TurnOffLampYMessage messages
    )
    async def turn_off_event_handler(self, topic: str, event: Event[TurnOffLampYMessage]):
        self.turn_off()

        if event.payload.reset_consumption:
            self.total_kwh = 0

