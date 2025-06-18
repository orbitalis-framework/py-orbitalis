from abc import ABC, abstractmethod
from dataclasses import dataclass
from busline.client.client import EventBusClient
import logging
from orbitalis.orb.state import OrbState, Created, Running


@dataclass
class Orb(ABC):
    identifier: str
    eventbus_client: EventBusClient
    state: OrbState

    def __post_init__(self):
        self.state = Created()

    async def start(self, *args, **kwargs):
        logging.info(f"{self}: starting...")
        await self.on_starting(*args, **kwargs)
        await self._internal_start(*args, **kwargs)
        await self.on_started(*args, **kwargs)
        logging.info(f"{self}: started")


    async def on_starting(self, *args, **kwargs):
        """
        TODO
        """

    @abstractmethod
    async def _internal_start(self, *args, **kwargs):
        """
        TODO
        """

    async def on_started(self, *args, **kwargs):
        """
        TODO
        """

    def __repr__(self) -> str:
        return self.identifier
