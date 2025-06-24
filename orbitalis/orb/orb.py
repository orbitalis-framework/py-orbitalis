from abc import ABC, abstractmethod
from dataclasses import dataclass
from busline.client.client import EventBusClient
import logging
from orbitalis.descriptor.descriptor import GenerateDescriptorMixin
from orbitalis.state_machine.state_machine import StateMachine


@dataclass
class Orb(GenerateDescriptorMixin, StateMachine, ABC):
    identifier: str
    eventbus_client: EventBusClient     # TODO: creare topic client in py-busline

    def __repr__(self) -> str:
        return self.identifier
