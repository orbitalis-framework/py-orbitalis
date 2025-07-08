from dataclasses import dataclass
from typing import FrozenSet, Dict

from orbitalis.orb.descriptor import Descriptor


@dataclass(frozen=True, kw_only=True)
class PluginDescriptor(Descriptor):
    operations: Dict[str, Dict]   # operation_name => input_schema
