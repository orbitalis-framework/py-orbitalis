from dataclasses import dataclass
from typing import FrozenSet

from orbitalis.descriptor.descriptor import Descriptor


@dataclass(frozen=True, kw_only=True)
class CoreDescriptor(Descriptor):
    """

    Author: Nicola Ricciardi
    """
