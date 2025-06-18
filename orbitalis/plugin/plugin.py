import asyncio
from typing import FrozenSet
from abc import ABC
from dataclasses import dataclass
from orbitalis.orb.orb import Orb


@dataclass
class Plugin(Orb, ABC):
    tags: FrozenSet[str]