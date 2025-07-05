from dataclasses import dataclass, field
from typing import Optional, FrozenSet
from orbitalis.utils.policy import Policy


@dataclass(frozen=True)
class PluginConfiguration:
    """

    Author: Nicola Ricciardi
    """

    acceptance_policy: Optional[Policy] = field(default=None)
    categories: FrozenSet[str] = field(default_factory=frozenset)