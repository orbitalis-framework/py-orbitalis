from dataclasses import dataclass, field
from typing import override
from orbitalis.state_machine.state_machine import State


@dataclass
class Created(State):

    name: str = field(default="CREATED", init=False)

    @override
    async def handle(self, *args, **kwargs):
        raise NotImplemented(f"{self.name} state is able to handle *nothing*")


@dataclass
class Stopped(State):

    name: str = field(default="STOPPED", init=False)

    @override
    async def handle(self, *args, **kwargs):
        raise NotImplemented(f"{self.name} state is able to handle *nothing*")

