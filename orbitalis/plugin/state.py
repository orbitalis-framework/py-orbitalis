from dataclasses import dataclass, field
from typing import override, Self

from orbitalis.orb.state import OrbState
from orbitalis.plugin.plugin import Plugin


@dataclass
class Running(OrbState):
    name: str = field(default="RUNNING", init=False)
    context: Plugin

    def __post_init__(self):
        self.context.subscribe_on_discover()

    @override
    async def handle(self, *args, **kwargs):
        return self
