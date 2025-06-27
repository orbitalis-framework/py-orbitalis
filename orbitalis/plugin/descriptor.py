from dataclasses import dataclass
from typing import FrozenSet

from orbitalis.descriptor.descriptor import Descriptor


@dataclass(frozen=True, kw_only=True)
class PluginDescriptor(Descriptor):
    categories: FrozenSet[str]
