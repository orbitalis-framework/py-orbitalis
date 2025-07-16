from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import StrEnum
from datetime import datetime
from typing import Optional

from busline.event.avro_payload import AvroEventPayload
from busline.event.event import Event
from orbitalis.orbiter.schemaspec import SchemaSpec
from orbitalis.plugin.operation import operation, Policy
from orbitalis.plugin.plugin import Plugin


@dataclass
class StatusMessage(AvroEventPayload):
    plugin_identifier: str
    status: str
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


class LampStatus(StrEnum):
    ON = "on"
    OFF = "off"


@dataclass
class LampPlugin(Plugin, ABC):
    kwh: float
    status: LampStatus = field(default=LampStatus.OFF)
    on_at: Optional[datetime] = field(default=None)
    total_kwh: float = field(default=0.0)

    @property
    def is_on(self) -> bool:
        return self.status == LampStatus.ON

    @property
    def is_off(self) -> bool:
        return self.status == LampStatus.OFF

    def turn_on(self):
        self.status = LampStatus.ON

        if self.on_at is None:
            self.on_at = datetime.now()

    def turn_off(self):
        self.status = LampStatus.OFF

        if self.on_at is not None:
            self.total_kwh += self.kwh * (datetime.now() - self.on_at).total_seconds() / 3600

            self.on_at = None

    @operation(
        name="get_status",
        input=SchemaSpec.empty(),
        output=SchemaSpec.from_payload(StatusMessage)
    )
    async def get_status_event_handler(self, topic: str, event: Event):
        connections = self.retrieve_connections(input_topic=topic, operation_name="get_status")

        assert len(connections) == 1

        connection = connections[0]

        assert connection.output_topic is not None
        assert connection.has_output

        await self.eventbus_client.publish(
            connection.output_topic,
            StatusMessage(self.identifier, str(self.status)).into_event()
        )

        connection.touch()


    @abstractmethod
    async def turn_on_event_handler(self, topic: str, event: Event):
        raise NotImplemented()

    @abstractmethod
    async def turn_off_event_handler(self, topic: str, event: Event):
        raise NotImplemented()


