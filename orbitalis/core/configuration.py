from dataclasses import dataclass, field
from typing import Optional, Set
from orbitalis.need.need import Needs


@dataclass
class Feature:
    name: str
    description: Optional[str] = field(default=None)

    needs: Needs = field(default_factory=Needs)



@dataclass
class CoreConfiguration:
    """
    TODO

    Author: Nicola Ricciardi
    """

    mandatory_features: Set[Feature]
    optional_features: Set[Feature]

    discovering_interval: int = field(default=2)

    @property
    def features(self) -> Set[Feature]:
        return self.mandatory_features.union(self.optional_features)