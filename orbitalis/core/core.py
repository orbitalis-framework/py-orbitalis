from abc import ABC
from dataclasses import dataclass, field
from typing import override, Dict

from orbitalis.core.configuration import CoreConfiguration, Service, ServiceNeed
from orbitalis.core.descriptor import CoreDescriptor
from orbitalis.core.plugin_descriptor_manager import PluginDescriptorsManager
from orbitalis.core.state import NonCompliance
from orbitalis.descriptor.descriptor import Descriptor
from orbitalis.events.handshake.discover import DiscoverEvent, DiscoverEventContent
from orbitalis.events.wellknown_topic import WellKnownHandShakeTopic
from orbitalis.orb.orb import Orb
import copy


@dataclass(kw_only=True, frozen=True)
class Core(Orb, ABC):
    configuration: CoreConfiguration
    plugins_by_service: Dict[str, PluginDescriptorsManager] = field(default_factory=dict)       # feature.name => PluginDescriptor

    def _needs_for_service(self, service: Service) -> ServiceNeed:
        """

        :param service:
        :return: None is there are no needs
        """

        plugins: PluginDescriptorsManager = self.plugins_by_service[service.name]

        needs: ServiceNeed = copy.deepcopy(service.needs)

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

    def _is_compliance_for_service(self, service: Service) -> bool:
        """
        Return True if the plugged plugins are enough to perform given service
        """

        return not self._needs_for_service(service).something_needed()

    def _is_compliance(self) -> bool:
        """
        Return True if the plugged plugins are enough to perform all services
        """

        for service in self.configuration.mandatory_services:
            if not self._is_compliance_for_service(service):
                return False

        return True


    def send_discover(self) -> None:

        needs_set = set()
        for service in self.configuration.services:
            needs = self._needs_for_service(service)
            needs_set.add(needs)

        discover_event = DiscoverEvent(
            content=DiscoverEventContent(self.generate_descriptor(), needs_set)
        )

        self.eventbus_client.publish(
            WellKnownHandShakeTopic.build_discover_topic(self.identifier),
            discover_event
        )


    @override
    def generate_descriptor(self) -> CoreDescriptor:
        return CoreDescriptor(identifier=self.identifier)

    @override
    async def _internal_start(self, *args, **kwargs):
        await super()._internal_start(*args, **kwargs)
        self.set_state(NonCompliance())

