from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import StrEnum
from datetime import datetime
from typing import Optional

from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.plugin.operation import operation, Policy, OperationSchema
from orbitalis.plugin.plugin import Plugin


@dataclass(frozen=True)
class StatusMessage(AvroEventPayload):
    status: str


class LampStatus(StrEnum):
    ON = "on"
    OFF = "off"


@dataclass
class LampPlugin(Plugin, ABC):
    kwh: float
    status: LampStatus = field(default_factory=lambda: LampStatus.OFF)
    on_at: Optional[datetime] = field(default=None)
    total_kwh: float = field(default=0.0)

    def _turn_on(self):
        self.status = LampStatus.ON

        if self.on_at is None:
            self.on_at = datetime.now()

    def _turn_off(self):
        self.status = LampStatus.OFF

        if self.on_at is not None:
            self.total_kwh += self.kwh * (datetime.now() - self.on_at).total_seconds() / 3600

            self.on_at = None

    @operation(
        name="get_status",
        input=OperationSchema.empty(),
        output=OperationSchema.from_payload(StatusMessage),
        policy=Policy()
    )
    async def get_status_event_handler(self, topic: str, event: Event):
        connections = self.retrieve_connections(input_topic=topic, operation_name="get_status")

        assert len(connections) == 1

        connection = connections[0]

        # TODO: sent result


    @abstractmethod
    async def turn_on_event_handler(self, topic: str, event: Event):
        raise NotImplemented()

    @abstractmethod
    async def turn_off_event_handler(self, topic: str, event: Event):
        raise NotImplemented()


