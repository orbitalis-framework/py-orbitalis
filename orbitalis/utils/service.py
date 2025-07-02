from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Self, TypeVar, Generic
from abc import ABC, abstractmethod

from busline.client.subscriber.event_handler.schemafull_handler import SchemafullEventHandler

C = TypeVar('C')

@dataclass
class Feature(SchemafullEventHandler, Generic[C], ABC):
    context: Optional[C] = field(default=None, repr=False)
    description: Optional[str] = field(default=None)


@dataclass
class Service(ABC):
    """

    Author: Nicola Ricciardi
    """

    description: Optional[str] = field(default=None)
    features: Dict[str, Feature] = field(default_factory=dict)

    def __post_init__(self):
        self.refresh_context()

    @abstractmethod
    def add_context_to_feature(self, feature: Feature[C]):
        raise NotImplemented()

    def add_feature(self, name: str, feature: Feature):
        self.features[name] = feature
        self.add_context_to_feature(feature)

    def add_features(self, features: Dict[str, Feature]):
        for name, handler in features.items():
            self.add_feature(name, handler)

    def with_features(self, features: Dict[str, Feature]) -> Self:
        self.add_features(features)

        return self

    def refresh_context(self):
        for _, feature in self.features.items():
            self.add_context_to_feature(feature)

    def generate_features_description(self) -> Dict[str, Dict]:
        return dict([(name, handler.input_schema()) for name, handler in self.features.items()])



