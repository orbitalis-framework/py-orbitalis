import asyncio
from abc import ABC
from dataclasses import dataclass, field
from typing import override, Set
from orbitalis.core.configuration import CoreConfiguration, Feature
from orbitalis.core.state import NonCompliance
from orbitalis.descriptor.descriptors_manager import DescriptorsManager
from orbitalis.orb.orb import Orb
from orbitalis.plugin.descriptor import PluginDescriptor


@dataclass
class Core(Orb, ABC):
    configuration: CoreConfiguration
    plugins: DescriptorsManager = field(default_factory=set)

    def _is_compliance_for_feature(self, feature: Feature) -> bool:
        """
        Return True if the plugged plugins are enough to perform given feature
        """

        for mandatory_plugin_id in feature.mandatory_plugins_by_identifier:
            if mandatory_plugin_id not in self.plugins.descriptors_by_identifier:
                return False

        # TODO: maybe return what is needed


        return True

    def _is_compliance(self) -> bool:
        """
        Return True if the plugged plugins are enough to perform all features
        """

        for feature in self.configuration.features:
            if not self._is_compliance_for_feature(feature):
                return False

        return True

    @override
    async def _internal_start(self, *args, **kwargs):
        self.state = NonCompliance()

