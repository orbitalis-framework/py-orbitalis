from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from busline.client.client import EventBusClient
from orbitalis.descriptor.descriptor import GenerateDescriptorMixin
import uuid


@dataclass
class Orb(GenerateDescriptorMixin, ABC):
    eventbus_client: EventBusClient     # TODO: creare topic client in py-busline
    identifier: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __repr__(self) -> str:
        return self.identifier
