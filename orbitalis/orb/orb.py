import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from busline.client.pubsub_client import PubTopicSubClient

import uuid

from orbitalis.orb.descriptor.descriptor import GenerateDescriptorMixin
from orbitalis.orb.state.state import Stopped, Created
from orbitalis.state_machine.state_machine import StateMachine



@dataclass(kw_only=True)
class Orb(GenerateDescriptorMixin, StateMachine, ABC):
    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))
    eventbus_client: PubTopicSubClient


    def __post_init__(self):
        self.state = Created(self)

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

        await self.eventbus_client.connect()

    async def on_started(self, *args, **kwargs):
        """
        TODO
        """

    async def stop(self, *args, **kwargs):
        logging.info(f"{self}: stopping...")
        await self.on_stopping(*args, **kwargs)
        await self._internal_stop(*args, **kwargs)
        await self.on_stopped(*args, **kwargs)
        logging.info(f"{self}: stopped")


    async def on_stopping(self, *args, **kwargs):
        """
        TODO
        """

    async def _internal_stop(self, *args, **kwargs):
        self.state = Stopped(self)

    async def on_stopped(self, *args, **kwargs):
        """
        TODO
        """


    def __repr__(self) -> str:
        return self.identifier

