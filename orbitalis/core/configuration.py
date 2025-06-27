from dataclasses import dataclass, field
from typing import Optional, Set, Dict
from orbitalis.need.need import Need


@dataclass
class FeatureNeed:
    mandatory_plugins_by_identifier: Set[str] = field(default_factory=frozenset)
    optional_plugins_by_identifier: Set[str] = field(default_factory=frozenset)
    mandatory_plugins_by_category: Dict[str, Need] = field(default_factory=dict)
    optional_plugins_by_category: Dict[str, Need] = field(default_factory=dict)

    def __post_init__(self):
        if self.mandatory_plugins_by_identifier.intersection(self.optional_plugins_by_identifier):
            raise ValueError("A plugin identifier can be mandatory OR optional, not both")

        if set(self.mandatory_plugins_by_category.keys()).intersection(self.optional_plugins_by_category.keys()):
            raise ValueError("A plugin category can be mandatory OR optional, not both")

    def something_needed(self) -> bool:
        return len(self.mandatory_plugins_by_identifier) != 0 or \
                len(self.mandatory_plugins_by_category.keys()) != 0


@dataclass
class Feature:
    name: str
    description: Optional[str] = field(default=None)

    needs: FeatureNeed = field(default_factory=FeatureNeed)



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