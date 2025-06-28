from dataclasses import dataclass, field
from typing import Optional, Set, Dict
from orbitalis.need.policy import Policy
from orbitalis.plugin.descriptor import PluginDescriptor


@dataclass
class ServiceNeed:
    mandatory_plugins_by_identifier: Set[str] = field(default_factory=frozenset)
    optional_plugins_by_identifier: Set[str] = field(default_factory=frozenset)
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


@dataclass
class Service:
    name: str
    description: Optional[str] = field(default=None)

    needs: ServiceNeed = field(default_factory=ServiceNeed)



@dataclass
class CoreConfiguration:
    """
    TODO

    Author: Nicola Ricciardi
    """

    mandatory_services: Set[Service]
    optional_services: Set[Service]

    discovering_interval: int = field(default=2)

    @property
    def services(self) -> Set[Service]:
        return self.mandatory_services.union(self.optional_services)

    def is_valid_service(self, service_name: str) -> bool:
        for service in self.mandatory_services.union(self.optional_services):
            if service.name == service_name:
                return True

        return False