import asyncio
from abc import ABC
from dataclasses import dataclass, field
from typing import override, Set, Optional, Dict
from orbitalis.core.configuration import CoreConfiguration, Feature, Need, Needs
from orbitalis.core.plugin_descriptor_manager import PluginDescriptorsManager
from orbitalis.core.state import NonCompliance
from orbitalis.descriptor.descriptors_manager import DescriptorsManager
from orbitalis.orb.orb import Orb
from orbitalis.plugin.descriptor import PluginDescriptor
import copy


@dataclass
class Core(Orb, ABC):
    configuration: CoreConfiguration
    plugins_by_feature: Dict[str, PluginDescriptorsManager] = field(default_factory=dict)       # feature.name => PluginDescriptor

    def _needs_for_feature(self, feature: Feature) -> Needs:
        """

        :param feature:
        :return: None is there are no needs, a Needed otherwise
        """

        plugins: PluginDescriptorsManager = self.plugins_by_feature[feature.name]

        needs: Needs = copy.deepcopy(feature.needs)

        needs.mandatory_plugins_by_identifier = needs.mandatory_plugins_by_identifier.difference(plugins.descriptors_by_identifier)
        needs.optional_plugins_by_identifier = needs.mandatory_plugins_by_identifier.difference(plugins.descriptors_by_identifier)

        for tag, need in needs.mandatory_plugins_by_tag.items():
            n_plugins = len(plugins.descriptors_by_tag[tag])

            need.maximum -= max(0, need.maximum - n_plugins)

            if need.maximum == 0:
                del needs.mandatory_plugins_by_tag[tag]

            need.minimum = max(0, need.minimum - n_plugins)

        for tag, need in needs.optional_plugins_by_tag.items():
            n_plugins = len(plugins.descriptors_by_tag[tag])

            need.maximum -= max(0, need.maximum - n_plugins)

            if need.maximum == 0:
                del needs.optional_plugins_by_tag[tag]

            need.minimum = max(0, need.minimum - n_plugins)

        return needs

    def _is_compliance_for_feature(self, feature: Feature) -> bool:
        """
        Return True if the plugged plugins are enough to perform given feature
        """

        return not self._need_for_feature(feature).something_needed()

    def _is_compliance(self) -> bool:
        """
        Return True if the plugged plugins are enough to perform all features
        """

        for feature in self.configuration.mandatory_features:
            if not self._is_compliance_for_feature(feature):
                return False

        return True

    @override
    async def _internal_start(self, *args, **kwargs):
        self.state = NonCompliance()

