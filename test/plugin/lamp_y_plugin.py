from dataclasses import dataclass, field
from datetime import datetime
from typing import override

from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.plugin.operation import OperationSchema, Policy, operation
from test.plugin.lamp_plugin import LampPlugin, LampStatus

@dataclass(frozen=True)
class TurnOnMessage(AvroEventPayload):
    power: float = field(default=1)

    def __post_init__(self):
        assert 0 < self.power <= 1


@dataclass(kw_only=True)
class LampYPlugin(LampPlugin):
    power: float = field(default=1)

    @operation(
        name="turn_on",
        input=OperationSchema.from_schema(TurnOnMessage.avro_schema()),
        policy=Policy()
    )
    async def turn_on_event_handler(self, topic: str, event: Event[TurnOnMessage]):
        self._turn_on()
        self.power = event.payload.power

    @override
    def _turn_off(self):
        self.status = LampStatus.OFF

        if self.on_at is not None:
            self.total_kwh += self.power * self.kwh * (datetime.now() - self.on_at).total_seconds() / 3600

            self.on_at = None

    @operation(
        name="turn_off",
        input=OperationSchema.empty(),
        policy=Policy()
    )
    async def turn_off_event_handler(self, topic: str, event: Event):
        self._turn_off()
