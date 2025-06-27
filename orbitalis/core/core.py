import asyncio
from abc import ABC
from dataclasses import dataclass, field
from typing import override, Set, Optional, Dict

from pyexpat import features

from orbitalis.core.configuration import CoreConfiguration, Feature, Need, Needs
from orbitalis.core.plugin_descriptor_manager import PluginDescriptorsManager
from orbitalis.core.state import NonCompliance
from orbitalis.descriptor.descriptors_manager import DescriptorsManager
from orbitalis.events.discover import DiscoverEvent, DiscoverEventContent
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
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

        for plugins_by_tag, reference in zip([needs.mandatory_plugins_by_category, needs.optional_plugins_by_category], [needs.mandatory_plugins_by_category, needs.optional_plugins_by_category]):
            for tag, need in plugins_by_tag.items():
                n_plugins = len(plugins.descriptors_by_tag[tag])

                need.maximum -= max(0, need.maximum - n_plugins)

                if need.maximum == 0:
                    del reference[tag]

                need.minimum = max(0, need.minimum - n_plugins)

        return needs

    def _is_compliance_for_feature(self, feature: Feature) -> bool:
        """
        Return True if the plugged plugins are enough to perform given feature
        """

        return not self._needs_for_feature(feature).something_needed()

    def _is_compliance(self) -> bool:
        """
        Return True if the plugged plugins are enough to perform all features
        """

        for feature in self.configuration.mandatory_features:
            if not self._is_compliance_for_feature(feature):
                return False

        return True


    def send_discover(self) -> None:

        needs_set = set()
        for feature in self.configuration.features:
            needs = self._needs_for_feature(feature)
            needs_set.add(needs)

        discover_event = DiscoverEvent(
            content=DiscoverEventContent(self.identifier, needs_set)
        )

        self.eventbus_client.publish(
            WellKnownHandShakeTopic.build_discover_topic(self.identifier),
            discover_event
        )

    @override
    async def _internal_start(self, *args, **kwargs):
        self.state = NonCompliance()

