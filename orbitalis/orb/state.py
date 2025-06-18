from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Self, override
import asyncio


@dataclass
class OrbState(ABC):

    @abstractmethod
    async def handle(self, *args, **kwargs) -> Self:
        raise NotImplemented()



@dataclass
class Created(OrbState):

    @override
    async def handle(self, *args, **kwargs) -> Self:
        raise NotImplemented()

@dataclass
class Running(OrbState):

    @override
    async def handle(self, *args, **kwargs) -> Self:
        raise NotImplemented()

