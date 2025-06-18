from dataclasses import dataclass
from typing import override, Self

from orbitalis.orb.state import OrbState


@dataclass
class NonCompliance(OrbState):

    @override
    async def handle(self, *args, **kwargs) -> Self:
        pass