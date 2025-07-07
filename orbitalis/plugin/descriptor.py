from dataclasses import dataclass
from typing import FrozenSet, Dict

from orbitalis.orb.descriptor import Descriptor


@dataclass(frozen=True, kw_only=True)
class PluginDescriptor(Descriptor):
    categories: FrozenSet[str]
    features: Dict[str, Dict]   # feature_name => input_schema
