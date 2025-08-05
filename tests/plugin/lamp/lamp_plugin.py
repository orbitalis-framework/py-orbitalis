from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import StrEnum
from datetime import datetime
from typing import Optional

from busline.event.message.avro_message import AvroMessageMixin
from busline.event.event import Event
from orbitalis.orbiter.schemaspec import Input, Output
from orbitalis.plugin.operation import operation
from orbitalis.plugin.plugin import Plugin


class LampStatus(StrEnum):
    """
    Utility enum to define possible lamp statuses
    """

    ON = "on"
    OFF = "off"


@dataclass
class StatusMessage(AvroMessageMixin):
    """
    Custom message defined to share status of lamps
    """

    lamp_identifier: str
    status: str     # "on" or "off"
    created_at: datetime = None # it is an Avro message, avoid default of time-variant fields

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class LampPlugin(Plugin, ABC):
    """
    Plugin to control a smart lamp which has an energy-meter
    """

    # Custom plugin attributes
    kw: float  # lamp energy consumption
    status: LampStatus = field(default=LampStatus.OFF)
    on_at: Optional[datetime] = field(default=None) # datetime of on request
    total_kwh: float = field(default=0.0)   # total consumption history

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
        """
        Turn off this lamp and update consumption history
        """

        self.status = LampStatus.OFF

        if self.on_at is not None:
            # Update total consumption:
            self.total_kwh += self.kw * (datetime.now() - self.on_at).total_seconds() / 3600

            self.on_at = None

    @operation(
        name="get_status",
        input=Input.empty(),
        output=Output.from_message(StatusMessage)
    )
    async def get_status_event_handler(self, topic: str, event: Event):
        connections = await self._retrieve_and_touch_connections(input_topic=topic, operation_name="get_status")

        # Only one connection should be present on inbound topic
        assert len(connections) == 1

        connection = connections[0]

        assert connection.output_topic is not None
        assert connection.output.has_output

        # Manually touch the connection
        async with connection.lock:
            connection.touch()

        # Send output to core
        await self.eventbus_client.publish(
            connection.output_topic,
            StatusMessage(self.identifier, str(self.status))
        )

    @abstractmethod
    async def turn_on_event_handler(self, topic: str, event: Event):
        raise NotImplemented()

    @abstractmethod
    async def turn_off_event_handler(self, topic: str, event: Event):
        raise NotImplemented()


