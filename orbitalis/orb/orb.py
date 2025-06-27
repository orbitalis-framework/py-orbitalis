from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import override

from busline.client.pubsub_client import PubSubTopicClient
from orbitalis.descriptor.descriptor import GenerateDescriptorMixin
import uuid

from orbitalis.state_machine.state_machine import StateMachine


@dataclass(kw_only=True, frozen=True)
class Orb(GenerateDescriptorMixin, StateMachine, ABC):
    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))
    eventbus_client: PubSubTopicClient

    @override
    async def _internal_start(self, *args, **kwargs):
        await self.eventbus_client.connect()

    def __repr__(self) -> str:
        return self.identifier

