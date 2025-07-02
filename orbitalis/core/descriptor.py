from dataclasses import dataclass
from typing import Dict

from orbitalis.orb.descriptor.descriptor import Descriptor


@dataclass(frozen=True, kw_only=True)
class CoreDescriptor(Descriptor):
    """

    Author: Nicola Ricciardi
    """

    services: Dict[str, Dict[str, Dict]]   # service_name => { feature_name => input_schema }