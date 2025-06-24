from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Self, override
import asyncio

from orbitalis.state_machine.state_machine import State


@dataclass
class OrbState(State, ABC):
    """

    Author: Nicola Ricciardi
    """

