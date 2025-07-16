from dataclasses import dataclass, field
from datetime import datetime
from typing import override

from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.orbiter.schemaspec import SchemaSpec, Input
from orbitalis.plugin.operation import Policy, operation
from tests.plugin.lamp_plugin import LampPlugin, LampStatus

@dataclass(frozen=True)
class TurnOnLampYMessage(AvroEventPayload):
    power: float = field(default=1)

    def __post_init__(self):
        assert 0 < self.power <= 1

@dataclass(frozen=True)
class TurnOffLampYMessage(AvroEventPayload):
    reset_consumption: bool = field(default=False)



@dataclass(kw_only=True)
class LampYPlugin(LampPlugin):
    power: float = field(default=1)

    @override
    def turn_off(self):
        self.status = LampStatus.OFF

        if self.on_at is not None:
            self.total_kwh += self.power * self.kwh * (datetime.now() - self.on_at).total_seconds() / 3600

            self.on_at = None

    # === OPERATIONs ===

    @operation(
        name="turn_on",
        input=Input.from_schema(TurnOnLampYMessage.avro_schema())
    )
    async def turn_on_event_handler(self, topic: str, event: Event[TurnOnLampYMessage]):
        self.turn_on()
        self.power = event.payload.power

    @operation(
        name="turn_off",
        input=Input.from_schema(TurnOffLampYMessage.avro_schema())
    )
    async def turn_off_event_handler(self, topic: str, event: Event[TurnOffLampYMessage]):
        self.turn_off()

        if event.payload.reset_consumption:
            self.total_kwh = 0

