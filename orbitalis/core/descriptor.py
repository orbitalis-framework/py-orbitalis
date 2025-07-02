from dataclasses import dataclass

from orbitalis.orb.descriptor.descriptor import Descriptor


@dataclass(frozen=True, kw_only=True)
class CoreDescriptor(Descriptor):
    """

    Author: Nicola Ricciardi
    """
