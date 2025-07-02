from dataclasses import dataclass, field
from typing import Optional, Set, Dict, FrozenSet

from orbitalis.core.of_core import OfCoreMixin
from orbitalis.utils.policy import Policy
from orbitalis.plugin.descriptor import PluginDescriptor
from orbitalis.utils.service import Service, Feature, C


@dataclass
class CoreService(Service, OfCoreMixin):

    def add_context_to_feature(self, feature: Feature[C]):
        feature.context = self.context


@dataclass
class CoreServiceNeed:
    mandatory_plugins_by_identifier: Set[str] = field(default_factory=set)
    optional_plugins_by_identifier: Set[str] = field(default_factory=set)
    mandatory_plugins_by_category: Dict[str, Policy] = field(default_factory=dict)
    optional_plugins_by_category: Dict[str, Policy] = field(default_factory=dict)

    def __post_init__(self):
        if self.mandatory_plugins_by_identifier.intersection(self.optional_plugins_by_identifier):
            raise ValueError("A plugin identifier can be mandatory OR optional, not both")

        if set(self.mandatory_plugins_by_category.keys()).intersection(self.optional_plugins_by_category.keys()):
            raise ValueError("A plugin category can be mandatory OR optional, not both")

    def something_needed(self) -> bool:
        return len(self.mandatory_plugins_by_identifier) != 0 or \
                len(self.mandatory_plugins_by_category.keys()) != 0

    def is_plugin_pluggable(self, plugin_descriptor: PluginDescriptor) -> bool:

        if plugin_descriptor.identifier in self.mandatory_plugins_by_identifier:
            return True

        if plugin_descriptor.identifier in self.optional_plugins_by_identifier:
            return True

        for plugin_category in plugin_descriptor.categories:
            if plugin_category in self.mandatory_plugins_by_category:
                policy: Policy = self.mandatory_plugins_by_category[plugin_category]

                if not policy.slot_available():
                    return False

                if not policy.is_compliance(plugin_descriptor.identifier):
                    return False

            if plugin_category in self.optional_plugins_by_category:
                policy: Policy = self.optional_plugins_by_category[plugin_category]

                if not policy.slot_available():
                    return False

                if not policy.is_compliance(plugin_descriptor.identifier):
                    return False

        return True


@dataclass(frozen=True)
class CoreServiceDescriptor:
    service: CoreService
    mandatory: bool
    need: Optional[CoreServiceNeed] = field(default=None)